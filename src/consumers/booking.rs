use crate::config::WorkerConfig;
use crate::error::ConsumerError;
use base64::{engine::general_purpose, Engine as _};
use futures::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    Channel, ExchangeKind,
};
use serde::Deserialize;
use uuid::Uuid;

const QUEUE_NAME: &str = "booking_jobs";
const EXCHANGE_NAME: &str = "tbd.events";

/// Standard event envelope published by the outbox relay.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct EventEnvelope<T> {
    pub id: Uuid,                  // Matches outbox.id (used for idempotency)
    pub event_type: String,        // e.g. "BookingCreated"
    pub timestamp: Option<String>, // ISO 8601 string
    pub payload: T,                // Domain payload data
}

/// Domain payload shape for 'BookingCreated' event.
#[derive(Debug, Deserialize)]
pub struct BookingCreatedData {
    pub booking_id: i64,
    pub user_email: String,
    pub contact_name: String,
    pub slot_start: String,
    pub amount: String,
}

/// Ensure RabbitMQ topology (DLX, DLQ, Topic Exchange, Queue) exists idempotently before consuming.
async fn setup_booking_topology(ch: &Channel) -> Result<(), lapin::Error> {
    // 1. Declare DLX (Fanout)
    ch.exchange_declare(
        "tbd.dlx",
        ExchangeKind::Fanout,
        ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await?;

    // 2. Declare DLQ and bind to DLX
    ch.queue_declare(
        "tbd.dlq",
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await?;
    ch.queue_bind(
        "tbd.dlq",
        "tbd.dlx",
        "",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await?;

    // 3. Declare Main Topic Exchange
    ch.exchange_declare(
        EXCHANGE_NAME,
        ExchangeKind::Topic,
        ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await?;

    // 4. Declare booking_jobs queue with DLX
    let mut booking_args = FieldTable::default();
    booking_args.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString("tbd.dlx".into()),
    );

    ch.queue_declare(
        QUEUE_NAME,
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        booking_args,
    )
    .await?;

    // 5. Bind booking_jobs to receive all booking.* events
    ch.queue_bind(
        QUEUE_NAME,
        EXCHANGE_NAME,
        "booking.#",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await?;

    Ok(())
}

/// Subscribe to 'booking_jobs' and process each message with manual ACK.
pub async fn consume_booking_jobs(channel: Channel, config: WorkerConfig) {
    if let Err(e) = channel
        .basic_qos(config.amqp_prefetch_count, BasicQosOptions::default())
        .await
    {
        tracing::error!(error = %e, "Booking consumer: failed to set QoS");
        return;
    }

    // Ensure exchange and queue topology are declared idempotently
    if let Err(e) = setup_booking_topology(&channel).await {
        tracing::error!(error = %e, "Booking consumer: failed to declare AMQP topology");
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

        // Deserialize standardized EventEnvelope
        let envelope: EventEnvelope<BookingCreatedData> = match serde_json::from_slice(
            &delivery.data,
        ) {
            Ok(env) => env,
            Err(e) => {
                tracing::error!(error = %e, "Booking consumer: invalid JSON envelope — NACKing without requeue");
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
            outbox_id = %envelope.id,
            booking_id = envelope.payload.booking_id,
            email = %envelope.payload.user_email,
            "Booking consumer: processing booking job"
        );

        // ── Idempotency Pre-Check ─────────────────────────────────────────────
        let already_processed = sqlx::query_scalar::<_, i32>(
            "SELECT 1 FROM processed_jobs WHERE outbox_id = $1 AND consumer = 'booking'",
        )
        .bind(envelope.id)
        .fetch_optional(&config.db)
        .await;

        match already_processed {
            Ok(Some(_)) => {
                tracing::warn!(
                    outbox_id = %envelope.id,
                    "Booking consumer: duplicate delivery detected (already completed) — ACKing and skipping"
                );
                let _ = delivery.ack(BasicAckOptions::default()).await;
                continue;
            }
            Err(e) => {
                tracing::error!(
                    outbox_id = %envelope.id,
                    error = %e,
                    "Booking consumer: failed to check processed_jobs table — NACKing with requeue"
                );
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await;
                continue;
            }
            Ok(None) => {
                // Not yet processed — proceed to execute
            }
        }

        // ── Execute with retry & backoff ──────────────────────────────────────
        const MAX_ATTEMPTS: u32 = 3;
        let mut attempts = 0;

        let process_result = loop {
            attempts += 1;
            match process_booking_job(&config, &envelope.payload).await {
                Ok(()) => break Ok(()),
                Err(ConsumerError::Permanent(msg)) => {
                    break Err(ConsumerError::Permanent(msg));
                }
                Err(ConsumerError::Transient(e)) => {
                    if attempts >= MAX_ATTEMPTS {
                        tracing::error!(
                            booking_id = envelope.payload.booking_id,
                            attempts,
                            error = ?e,
                            "Booking consumer: max retries reached — moving to DLQ"
                        );
                        break Err(ConsumerError::Transient(e));
                    }
                    let backoff = std::time::Duration::from_secs(2u64.pow(attempts));
                    tracing::warn!(
                        booking_id = envelope.payload.booking_id,
                        attempt = attempts,
                        retry_in_secs = backoff.as_secs(),
                        error = ?e,
                        "Booking consumer: transient failure — retrying after backoff"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        };

        // ── Record Idempotency on Completion & ACK/NACK ───────────────────────
        match process_result {
            Ok(_) => {
                // Record into processed_jobs upon successful completion
                let insert_result = sqlx::query(
                    "INSERT INTO processed_jobs (outbox_id, consumer)
                     VALUES ($1, 'booking')
                     ON CONFLICT DO NOTHING",
                )
                .bind(envelope.id)
                .execute(&config.db)
                .await;

                if let Err(e) = insert_result {
                    tracing::error!(
                        outbox_id = %envelope.id,
                        error = %e,
                        "Booking consumer: warning — failed to record into processed_jobs"
                    );
                }

                tracing::info!(
                    booking_id = envelope.payload.booking_id,
                    "Booking consumer: PDF generated and email sent — ACKing"
                );
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
            Err(e) => {
                tracing::error!(
                    booking_id = envelope.payload.booking_id,
                    error = ?e,
                    "Booking consumer: failure — NACKing WITHOUT requeue (routing to DLQ)"
                );

                // Requeue: false sends message to tbd.dlx / tbd.dlq
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: false,
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
    payload: &BookingCreatedData,
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
    payload: &BookingCreatedData,
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

    let mut request = config
        .http_client
        .post(format!(
            "{}/forms/chromium/convert/html",
            config.gotenberg_url
        ))
        .multipart(form);

    // Only attach Basic Auth if credentials are configured.
    // Leave unset if your Gotenberg instance has no auth (avoids proxy 401/502).
    if let (Some(user), Some(pass)) = (&config.gotenberg_user, &config.gotenberg_password) {
        request = request.basic_auth(user, Some(pass));
    }

    let response = request
        .send()
        .await
        .map_err(|e| ConsumerError::Transient(e.into()))?;

    // Gotenberg error handling
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let text = if text.len() > 300 {
            format!(
                "{}... [truncated, total {} bytes]",
                &text[..300],
                text.len()
            )
        } else {
            text
        };

        if status.is_client_error() && status.as_u16() != 429 {
            return Err(ConsumerError::Permanent(format!(
                "Gotenberg permanent error: {} — {}",
                status, text
            )));
        }

        return Err(ConsumerError::Transient(anyhow::anyhow!(
            "Gotenberg transient error: {} — {}",
            status,
            text
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
    payload: &BookingCreatedData,
    pdf_base64: String,
) -> Result<(), ConsumerError> {
    let body = serde_json::json!({
        "from": config.resend_from_email.as_str(),
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
        let text = if text.len() > 300 {
            format!(
                "{}... [truncated, total {} bytes]",
                &text[..300],
                text.len()
            )
        } else {
            text
        };

        if status.is_client_error() && status.as_u16() != 429 {
            return Err(ConsumerError::Permanent(format!(
                "Resend API permanent error: {} — {}",
                status, text
            )));
        }

        return Err(ConsumerError::Transient(anyhow::anyhow!(
            "Resend API transient error: {} — {}",
            status,
            text
        )));
    }

    Ok(())
}
