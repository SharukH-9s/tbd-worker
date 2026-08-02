use crate::config::WorkerConfig;
use crate::error::ConsumerError;
use base64::{engine::general_purpose, Engine as _};
use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
    Channel,
};
use serde::Deserialize;
use uuid::Uuid;

const QUEUE_NAME: &str = "booking_jobs";

/// Unified payload shape expected from the 'BookingCreated' outbox event.
#[derive(Debug, Deserialize)]
struct BookingCreatedPayload {
    outbox_id: Uuid, // used for idempotency — matches the outbox row's UUID
    booking_id: i64,
    user_email: String,
    contact_name: String,
    slot_start: String,
    amount: String,
}

/// Subscribe to 'booking_jobs' and process each message with manual ACK.
pub async fn consume_booking_jobs(channel: Channel, config: WorkerConfig) {
    if let Err(e) = channel.basic_qos(1, BasicQosOptions::default()).await {
        tracing::error!(error = %e, "Booking consumer: failed to set QoS");
        return;
    }

    let mut consumer = match channel
        .basic_consume(
            QUEUE_NAME,
            "tbd-worker-booking", // consumer tag
            BasicConsumeOptions {
                no_ack: false, // Manual ACK
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "Booking consumer: failed to start consuming");
            return;
        }
    };

    tracing::info!("Booking consumer: listening on '{}'", QUEUE_NAME);

    while let Some(delivery_result) = consumer.next().await {
        let delivery = match delivery_result {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(error = %e, "Booking consumer: delivery error");
                break;
            }
        };

        let payload: BookingCreatedPayload = match serde_json::from_slice(&delivery.data) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "Booking consumer: invalid JSON payload — NACKing without requeue");
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
            "Booking consumer: processing booking job"
        );

        // ── Idempotency check ─────────────────────────────────────────────────
        let claimed = sqlx::query!(
            "INSERT INTO processed_jobs (outbox_id, consumer)
             VALUES ($1, 'booking')
             ON CONFLICT DO NOTHING
             RETURNING outbox_id",
            payload.outbox_id
        )
        .fetch_optional(&config.db)
        .await;

        match claimed {
            Ok(None) => {
                tracing::warn!(
                    outbox_id = %payload.outbox_id,
                    "Booking consumer: duplicate delivery detected — ACKing and skipping"
                );
                let _ = delivery.ack(BasicAckOptions::default()).await;
                continue;
            }
            Err(e) => {
                tracing::error!(
                    outbox_id = %payload.outbox_id,
                    error = %e,
                    "Booking consumer: failed to claim job in processed_jobs — NACKing with requeue"
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
                // We hold the exclusive claim — proceed.
            }
        }

        match process_booking_job(&config, &payload).await {
            Ok(_) => {
                tracing::info!(
                    booking_id = payload.booking_id,
                    "Booking consumer: PDF generated and email sent — ACKing"
                );
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
            Err(ConsumerError::Permanent(msg)) => {
                tracing::error!(
                    booking_id = payload.booking_id,
                    error = %msg,
                    "Booking consumer: permanent failure — NACKing WITHOUT requeue (discarding)"
                );
                let _ = sqlx::query!(
                    "DELETE FROM processed_jobs WHERE outbox_id = $1 AND consumer = 'booking'",
                    payload.outbox_id
                )
                .execute(&config.db)
                .await;

                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: false,
                        ..Default::default()
                    })
                    .await;
            }
            Err(ConsumerError::Transient(e)) => {
                tracing::error!(
                    booking_id = payload.booking_id,
                    error = ?e,
                    "Booking consumer: transient failure — NACKing WITH requeue"
                );
                let _ = sqlx::query!(
                    "DELETE FROM processed_jobs WHERE outbox_id = $1 AND consumer = 'booking'",
                    payload.outbox_id
                )
                .execute(&config.db)
                .await;

                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await;
            }
        }
    }

    tracing::warn!("Booking consumer: stream ended — exiting");
}

async fn process_booking_job(
    config: &WorkerConfig,
    payload: &BookingCreatedPayload,
) -> Result<(), ConsumerError> {
    // 1. Generate PDF bytes via Gotenberg
    let pdf_bytes = generate_invoice_pdf(config, payload).await?;
    
    // 2. Base64-encode the PDF bytes
    let pdf_base64 = general_purpose::STANDARD.encode(&pdf_bytes);

    // 3. Send the email with the attached PDF
    send_booking_email(config, payload, pdf_base64).await?;

    Ok(())
}

async fn generate_invoice_pdf(
    config: &WorkerConfig,
    payload: &BookingCreatedPayload,
) -> Result<Vec<u8>, ConsumerError> {
    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Invoice #{}</title></head>
<body>
  <h1>TBD — Invoice</h1>
  <p>Client: <strong>{}</strong></p>
  <p>Booking ID: <strong>{}</strong></p>
  <p>Amount: <strong>BDT {}</strong></p>
</body>
</html>"#,
        payload.booking_id, payload.contact_name, payload.booking_id, payload.amount
    );

    let form = reqwest::multipart::Form::new().part(
        "files",
        reqwest::multipart::Part::bytes(html.into_bytes())
            .file_name("index.html")
            .mime_str("text/html")
            .map_err(|e| ConsumerError::Transient(e.into()))?,
    );

    let response = config
        .http_client
        .post(&format!("{}/forms/chromium/convert/html", config.gotenberg_url))
        .basic_auth("admin", Some("your_strong_secret_password"))
        .multipart(form)
        .send()
        .await
        .map_err(|e| ConsumerError::Transient(e.into()))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        if status.is_client_error() && status.as_u16() != 429 {
            return Err(ConsumerError::Permanent(format!(
                "Gotenberg permanent error: {} — {}",
                status, text
            )));
        }

        return Err(ConsumerError::Transient(anyhow::anyhow!(
            "Gotenberg transient error: {} — {}",
            status, text
        )));
    }

    let pdf_bytes = response
        .bytes()
        .await
        .map_err(|e| ConsumerError::Transient(e.into()))?
        .to_vec();

    Ok(pdf_bytes)
}

async fn send_booking_email(
    config: &WorkerConfig,
    payload: &BookingCreatedPayload,
    pdf_base64: String,
) -> Result<(), ConsumerError> {
    let body = serde_json::json!({
        "from": "TBD <onboarding@resend.dev>",
        "to": [payload.user_email],
        "subject": format!("Booking Confirmed — {}", payload.slot_start),
        "html": format!(
            "<p>Hi {},</p><p>Your booking (#{}) is confirmed for <strong>{}</strong>.</p><p>Please find your invoice attached.</p>",
            payload.contact_name, payload.booking_id, payload.slot_start
        ),
        "attachments": [
            {
                "filename": format!("invoice-{}.pdf", payload.booking_id),
                "content": pdf_base64
            }
        ]
    });

    let response = config
        .http_client
        .post("https://api.resend.com/emails")
        .header("Authorization", format!("Bearer {}", config.resend_api_key))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| ConsumerError::Transient(e.into()))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        if status.is_client_error() && status.as_u16() != 429 {
            return Err(ConsumerError::Permanent(format!(
                "Resend API permanent error: {} — {}",
                status, text
            )));
        }

        return Err(ConsumerError::Transient(anyhow::anyhow!(
            "Resend API transient error: {} — {}",
            status, text
        )));
    }

    Ok(())
}
