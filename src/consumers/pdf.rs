use crate::config::WorkerConfig;
use crate::error::ConsumerError;
use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
    Channel,
};
use serde::Deserialize;
use uuid::Uuid;

const QUEUE_NAME: &str = "pdf_jobs";

/// Payload shape expected from the 'InvoiceRequested' outbox event.
#[derive(Debug, Deserialize)]
struct InvoiceRequestedPayload {
    outbox_id: Uuid, // used for idempotency — matches the outbox row's UUID
    booking_id: i64,
    #[allow(dead_code)]
    // present in the fanout payload for the email consumer; PDF does not use it
    user_email: String,
    contact_name: String,
    amount: String,
}

/// Subscribe to 'pdf_jobs' and process each message with manual ACK.
pub async fn consume_pdf_jobs(channel: Channel, config: WorkerConfig) {
    if let Err(e) = channel.basic_qos(1, BasicQosOptions::default()).await {
        tracing::error!(error = %e, "PDF consumer: failed to set QoS");
        return;
    }

    let mut consumer = match channel
        .basic_consume(
            QUEUE_NAME,
            "tbd-worker-pdf", // consumer tag
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
            tracing::error!(error = %e, "PDF consumer: failed to start consuming");
            return;
        }
    };

    tracing::info!("PDF consumer: listening on '{}'", QUEUE_NAME);

    while let Some(delivery_result) = consumer.next().await {
        let delivery = match delivery_result {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(error = %e, "PDF consumer: delivery error");
                break;
            }
        };

        let payload: InvoiceRequestedPayload = match serde_json::from_slice(&delivery.data) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "PDF consumer: invalid JSON payload — NACKing without requeue");
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
            "PDF consumer: generating invoice PDF via Gotenberg"
        );

        // ── Idempotency check ─────────────────────────────────────────────────
        // Composite PK (outbox_id, 'pdf') is independent from the email consumer's
        // (outbox_id, 'email') claim — both can proceed from the same outbox message.
        let claimed = sqlx::query!(
            "INSERT INTO processed_jobs (outbox_id, consumer)
             VALUES ($1, 'pdf')
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
                    "PDF consumer: duplicate delivery detected — ACKing and skipping"
                );
                let _ = delivery.ack(BasicAckOptions::default()).await;
                continue;
            }
            Err(e) => {
                tracing::error!(
                    outbox_id = %payload.outbox_id,
                    error = %e,
                    "PDF consumer: failed to claim job in processed_jobs — NACKing with requeue"
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
                // We hold the exclusive claim — proceed with PDF generation.
            }
        }

        match generate_invoice_pdf(&config, &payload).await {
            Ok(pdf_bytes) => {
                tracing::info!(
                    booking_id = payload.booking_id,
                    size_bytes = pdf_bytes.len(),
                    "PDF consumer: PDF generated — ACKing"
                );
                // TODO: upload pdf_bytes to S3 / storage before ACKing
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
            Err(ConsumerError::Permanent(msg)) => {
                tracing::error!(
                    booking_id = payload.booking_id,
                    error = %msg,
                    "PDF consumer: permanent generation failure — NACKing WITHOUT requeue (discarding)"
                );
                // delete from processed_jobs so that it can be retried (if we wanted to, but we are discarding it anyway, so deleting it keeps the db clean)
                let _ = sqlx::query!(
                    "DELETE FROM processed_jobs WHERE outbox_id = $1 AND consumer = 'pdf'",
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
                    error = %e,
                    "PDF consumer: transient generation failure — NACKing WITH requeue"
                );
                // delete from processed_jobs so that it can be retried
                let _ = sqlx::query!(
                    "DELETE FROM processed_jobs WHERE outbox_id = $1 AND consumer = 'pdf'",
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

    tracing::warn!("PDF consumer: stream ended — exiting");
}

async fn generate_invoice_pdf(
    config: &WorkerConfig,
    payload: &InvoiceRequestedPayload,
) -> Result<Vec<u8>, ConsumerError> {
    // HTML template for the invoice — Gotenberg renders this to PDF
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

    // Gotenberg's /forms/chromium/convert/html endpoint accepts multipart/form-data
    let form = reqwest::multipart::Form::new().part(
        "files",
        reqwest::multipart::Part::bytes(html.into_bytes())
            .file_name("index.html")
            .mime_str("text/html")
            .map_err(|e| ConsumerError::Transient(e.into()))?,
    );

    let response = config
        .http_client
        .post("https://gotenberg-8-atxh.onrender.com/forms/chromium/convert/html")
        .basic_auth("admin", Some("your_strong_secret_password"))
        .multipart(form)
        .send()
        .await
        .map_err(|e| ConsumerError::Transient(e.into()))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        // 4xx client errors (except 429) are permanent — retrying won't help
        if status.is_client_error() && status.as_u16() != 429 {
            return Err(ConsumerError::Permanent(format!(
                "Gotenberg permanent error: {} — {}",
                status, text
            )));
        }

        // 5xx server errors or 429 (rate limit) are transient — retry
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
