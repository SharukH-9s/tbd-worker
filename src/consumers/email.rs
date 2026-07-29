use crate::config::WorkerConfig;
use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
    Channel,
};
use serde::Deserialize;
use uuid::Uuid;

const QUEUE_NAME: &str = "email_jobs";

/// Payload shape expected from the 'BookingCreated' outbox event.
#[derive(Debug, Deserialize)]
struct BookingCreatedPayload {
    outbox_id: Uuid, // used for idempotency — matches the outbox row's UUID
    booking_id: i64,
    user_email: String,
    contact_name: String,
    slot_start: String,
}

/// Subscribe to 'email_jobs' and process each message with manual ACK.
pub async fn consume_email_jobs(channel: Channel, config: WorkerConfig) {
    // Limit to 1 unacknowledged message at a time per consumer (fair dispatch).
    if let Err(e) = channel.basic_qos(1, BasicQosOptions::default()).await {
        tracing::error!(error = %e, "Email consumer: failed to set QoS");
        return;
    }

    let mut consumer = match channel
        .basic_consume(
            QUEUE_NAME,
            "tbd-worker-email", // consumer tag — unique identifier for this consumer
            BasicConsumeOptions {
                no_ack: false, // Manual ACK — we control when to ack
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "Email consumer: failed to start consuming");
            return;
        }
    };

    tracing::info!("Email consumer: listening on '{}'", QUEUE_NAME);

    while let Some(delivery_result) = consumer.next().await {
        // getting one message at a time because of QoS. Each message is the JSON payload of a single outbox row published by the relay.
        let delivery = match delivery_result {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(error = %e, "Email consumer: delivery error");
                break; // Exit loop — outer reconnect will re-subscribe
            }
        };

        // deserialize message
        let payload: BookingCreatedPayload = match serde_json::from_slice(&delivery.data) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "Email consumer: invalid JSON payload — NACKing without requeue");
                // Malformed message — send to DLX, do not requeue
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: false,
                        ..Default::default()
                    })
                    .await;
                continue;
            }
        };

        tracing::info!(
            booking_id = payload.booking_id,
            email = %payload.user_email,
            "Email consumer: sending booking confirmation email"
        );

        // ── Idempotency check ─────────────────────────────────────────────────
        // Atomically claim this (outbox_id, consumer) pair.
        // ON CONFLICT DO NOTHING means only one worker wins — the rest get None back.
        let claimed = sqlx::query!(
            "INSERT INTO processed_jobs (outbox_id, consumer)
             VALUES ($1, 'email')
             ON CONFLICT DO NOTHING
             RETURNING outbox_id",
            payload.outbox_id
        )
        .fetch_optional(&config.db)
        .await;

        match claimed {
            Ok(None) => {
                // query was successful but nothing was returned, because ON CONFLICT detected that the row already exists.
                // means this message was already processed
                tracing::warn!(
                    outbox_id = %payload.outbox_id,
                    "Email consumer: duplicate delivery detected — ACKing and skipping"
                );
                let _ = delivery.ack(BasicAckOptions::default()).await;
                continue;
            }
            Err(e) => {
                // DB error — don't process, requeue so we can retry.
                tracing::error!(
                    outbox_id = %payload.outbox_id,
                    error = %e,
                    "Email consumer: failed to claim job in processed_jobs — NACKing with requeue"
                );
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await;
                continue;
            }
            Ok(Some(_)) => {
                // We hold the exclusive claim — proceed with sending the email.
            }
        }

        match send_booking_email(&config, &payload).await {
            Ok(_) => {
                tracing::info!(
                    booking_id = payload.booking_id,
                    "Email consumer: email sent — ACKing"
                );
                let _ = delivery.ack(BasicAckOptions::default()).await; // RabbitMQ receives the ACK and permanently deletes the job from the queue.
            }
            Err(e) => {
                tracing::error!(
                    booking_id = payload.booking_id,
                    error = %e,
                    "Email consumer: failed to send email — NACKing with requeue"
                );
                // delete from processed_jobs so that it can be retried
                let _ = sqlx::query!(
                    "DELETE FROM processed_jobs WHERE outbox_id = $1 AND consumer = 'email'",
                    payload.outbox_id
                )
                .execute(&config.db)
                .await;

                // Transient error (e.g. Resend API down) — requeue for retry
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await; // RabbitMQ places the message back in the queue (or dead-letter queue) for redelivery.
            }
        }
    }

    tracing::warn!("Email consumer: stream ended — exiting");
}

async fn send_booking_email(
    config: &WorkerConfig,
    payload: &BookingCreatedPayload,
) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "from": "TBD <no-reply@yourdomain.com>",
        "to": [payload.user_email],
        "subject": format!("Booking Confirmed — {}", payload.slot_start),
        "html": format!(
            "<p>Hi {},</p><p>Your booking (#{}) is confirmed for <strong>{}</strong>.</p>",
            payload.contact_name, payload.booking_id, payload.slot_start
        )
    });

    let response = config
        .http_client
        .post("https://api.resend.com/emails")
        .header("Authorization", format!("Bearer {}", config.resend_api_key))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("Resend API error: {} — {}", status, text); // the function will return Err, which will cause the consumer to NACK the message with requeue. so we don't need to ack or nack here manually
    }

    Ok(())
}
