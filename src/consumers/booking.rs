use crate::config::WorkerConfig;
use crate::error::ConsumerError;
use askama::Template;
use base64::{engine::general_purpose, Engine as _};
use derive_typst_intoval::{IntoDict, IntoValue};
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
use typst::foundations::{Dict, IntoValue as _};
use typst_as_lib::{TypstAsLibError, TypstEngine};
use uuid::Uuid;

const QUEUE_NAME: &str = "booking_jobs";
const EXCHANGE_NAME: &str = "tbd.events";

// ── AMQP Envelope & Domain Payload ───────────────────────────────────────────

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
    pub contact_name: Option<String>,
    #[serde(default)]
    pub turf_name: Option<String>,
    #[serde(default)]
    pub game_name: Option<String>,
    pub slot_start: String,
    pub amount: String,
    #[serde(default)]
    pub total_price: Option<String>,
    #[serde(default)]
    pub paid_amount: Option<String>,
    #[serde(default)]
    pub due_amount: Option<String>,
    #[serde(default)]
    pub payment_status: Option<String>,
}

impl BookingCreatedData {
    pub fn display_contact_name(&self) -> &str {
        self.contact_name.as_deref().unwrap_or("Customer")
    }

    pub fn display_turf_name(&self) -> &str {
        self.turf_name.as_deref().unwrap_or("Turf BD Arena")
    }

    pub fn display_game_name(&self) -> &str {
        self.game_name.as_deref().unwrap_or("Pitch Reservation")
    }

    pub fn display_payment_status(&self) -> &str {
        match self.payment_status.as_deref() {
            Some("Fully_Paid") | Some("FullyPaid") => "Fully Paid",
            Some("Partially_Paid") | Some("PartiallyPaid") => "Partially Paid (Advance)",
            Some(other) => other,
            None => "Paid",
        }
    }

    pub fn is_partially_paid(&self) -> bool {
        matches!(
            self.payment_status.as_deref(),
            Some("Partially_Paid") | Some("PartiallyPaid")
        )
    }

    pub fn display_total_price(&self) -> &str {
        self.total_price.as_deref().unwrap_or(&self.amount)
    }

    pub fn display_paid_amount(&self) -> &str {
        self.paid_amount.as_deref().unwrap_or(&self.amount)
    }

    pub fn display_due_amount(&self) -> &str {
        self.due_amount.as_deref().unwrap_or("0")
    }
}

// ── Askama Email Template ─────────────────────────────────────────────────────

/// Askama template — maps 1:1 to `templates/email_booking.html`.
/// The template HTML is validated and parsed at compile time.
/// `.render()` at runtime is near-zero cost (just string interpolation).
#[derive(Template)]
#[template(path = "email_booking.html")]
struct BookingEmailTemplate<'a> {
    booking_id: i64,
    contact_name: &'a str,
    turf_name: &'a str,
    game_name: &'a str,
    slot_start: &'a str,
    paid_amount: &'a str,
    due_amount: &'a str,
    is_partially_paid: bool,
}

// ── Typst PDF Input Struct ────────────────────────────────────────────────────

/// Data injected into the Typst template via sys.inputs.
/// `derive_typst_intoval` generates the `IntoValue` + `IntoDict` impls
/// so this struct can be passed directly to `TypstEngine::compile_with_input()`.
#[derive(Debug, Clone, IntoValue, IntoDict)]
struct InvoiceInputs {
    booking_id: String,
    contact_name: String,
    user_email: String,
    turf_name: String,
    game_name: String,
    slot_start: String,
    total_price: String,
    paid_amount: String,
    due_amount: String,
    payment_status: String,
    is_partial: bool,
}

impl From<InvoiceInputs> for Dict {
    fn from(v: InvoiceInputs) -> Self {
        v.into_dict()
    }
}

// ── AMQP Topology ────────────────────────────────────────────────────────────

/// Ensure RabbitMQ topology (DLX, DLQ, Topic Exchange, Queue) exists
/// idempotently before consuming. Identical to the relay's topology.
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

// ── Consumer Entry Point ──────────────────────────────────────────────────────

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
                        "Booking consumer: failed to record into processed_jobs — NACKing with requeue"
                    );
                    if let Err(nack_error) = delivery
                        .nack(BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        })
                        .await
                    {
                        tracing::error!(
                            outbox_id = %envelope.id,
                            error = %nack_error,
                            "Booking consumer: failed to NACK after completion record error"
                        );
                    }
                    continue;
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
                // requeue: false sends message to tbd.dlx / tbd.dlq
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

// ── Job Orchestrator ──────────────────────────────────────────────────────────

async fn process_booking_job(
    config: &WorkerConfig,
    payload: &BookingCreatedData,
) -> Result<(), ConsumerError> {
    // Step 1: Render PDF bytes in-process via Typst (CPU-bound, sync)
    let pdf_bytes = generate_invoice_pdf(payload)?;

    // Step 2: Render email HTML via Askama (compile-time validated template)
    let html_body = render_email_html(payload)?;

    // Step 3: Build MIME email and send via Lettre → Brevo SMTP
    send_booking_email(config, payload, html_body, pdf_bytes).await?;

    Ok(())
}

// ── PDF Generation (Typst, in-process) ───────────────────────────────────────

/// Renders the Typst invoice template in-process and returns raw PDF bytes.
///
/// The `.typ` template is embedded into the binary at compile-time via
/// `include_str!`, so no disk read occurs at runtime and the binary is
/// self-contained (no external template file needed on Render).
///
/// Errors are classified as `Permanent` because a broken template or
/// invalid input will not fix itself on retry.
fn generate_invoice_pdf(payload: &BookingCreatedData) -> Result<Vec<u8>, ConsumerError> {
    // Embed the Typst template at compile-time — binary is fully self-contained.
    static TEMPLATE: &str = include_str!("../../templates/pdf_invoice.typ");

    // Build the input struct — derive_typst_intoval converts this to a typst Dict
    // that becomes accessible as `inputs.field_name` inside the .typ file.
    let inputs = InvoiceInputs {
        booking_id: payload.booking_id.to_string(),
        contact_name: payload.display_contact_name().to_string(),
        user_email: payload.user_email.clone(),
        turf_name: payload.display_turf_name().to_string(),
        game_name: payload.display_game_name().to_string(),
        slot_start: payload.slot_start.clone(),
        total_price: payload.display_total_price().to_string(),
        paid_amount: payload.display_paid_amount().to_string(),
        due_amount: payload.display_due_amount().to_string(),
        payment_status: payload.display_payment_status().to_string(),
        is_partial: payload.is_partially_paid(),
    };

    // Build the Typst engine with the embedded template source and fonts.
    let engine = TypstEngine::builder()
        .main_file(TEMPLATE)
        .search_fonts_with(typst_as_lib::typst_kit_options::TypstKitFontOptions::default())
        .build();

    // Compile the document, injecting `inputs` as sys.inputs
    let doc = engine.compile_with_input(inputs).output.map_err(|error| {
        let message = match error {
            TypstAsLibError::TypstSource(diagnostics) => diagnostics
                .iter()
                .map(|diagnostic| format!("{:?}", diagnostic.message))
                .collect::<Vec<_>>()
                .join("; "),
            other => other.to_string(),
        };
        ConsumerError::Permanent(format!("Typst compile error(s): {message}"))
    })?;

    // Export to PDF bytes (typst-pdf)
    let pdf_bytes = typst_pdf::pdf(&doc, &typst_pdf::PdfOptions::default())
        .map_err(|e| ConsumerError::Permanent(format!("Typst PDF export error: {:?}", e)))?;

    tracing::debug!(
        booking_id = payload.booking_id,
        pdf_size_bytes = pdf_bytes.len(),
        "PDF invoice generated"
    );

    Ok(pdf_bytes)
}

// ── Email HTML Rendering (Askama) ─────────────────────────────────────────────

/// Renders the Askama HTML email template to a `String`.
/// The template is validated at compile-time; rendering is near-zero cost.
fn render_email_html(payload: &BookingCreatedData) -> Result<String, ConsumerError> {
    let tmpl = BookingEmailTemplate {
        booking_id: payload.booking_id,
        contact_name: payload.display_contact_name(),
        turf_name: payload.display_turf_name(),
        game_name: payload.display_game_name(),
        slot_start: &payload.slot_start,
        paid_amount: payload.display_paid_amount(),
        due_amount: payload.display_due_amount(),
        is_partially_paid: payload.is_partially_paid(),
    };

    tmpl.render()
        .map_err(|e| ConsumerError::Permanent(format!("Askama template render error: {}", e)))
}

// ── Email Sending (Brevo HTTP API) ───────────────────────────────────────────

/// Dispatches the confirmation email with the Typst-generated PDF invoice
/// attached via Brevo's transactional email REST API (v3).
///
/// Brevo API endpoint: POST https://api.brevo.com/v3/smtp/email
/// Uses HTTP port 443 — guaranteed open on all cloud platforms (including Render free tier).
async fn send_booking_email(
    config: &WorkerConfig,
    payload: &BookingCreatedData,
    html_body: String,
    pdf_bytes: Vec<u8>,
) -> Result<(), ConsumerError> {
    let subject = format!(
        "Booking Confirmed #{} — {} ({})",
        payload.booking_id,
        payload.display_turf_name(),
        payload.display_game_name()
    );

    let pdf_base64 = general_purpose::STANDARD.encode(&pdf_bytes);
    let attachment_filename = format!("invoice-{}.pdf", payload.booking_id);

    let (from_name, from_email) = split_display_email(&config.email_from);

    let body = serde_json::json!({
        "sender": { "name": from_name, "email": from_email },
        "to": [{ "email": payload.user_email, "name": payload.display_contact_name() }],
        "subject": subject,
        "htmlContent": html_body,
        "attachment": [{ "name": attachment_filename, "content": pdf_base64 }],
    });

    let response = config
        .http_client
        .post("https://api.brevo.com/v3/smtp/email")
        .header("api-key", &config.brevo_api_key)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| ConsumerError::Transient(e.into()))?;

    let status = response.status();

    if status.is_success() {
        tracing::info!(
            booking_id = payload.booking_id,
            recipient = %payload.user_email,
            "Booking consumer: confirmation email dispatched via Brevo API"
        );
        return Ok(());
    }

    let err_text = response.text().await.unwrap_or_default();
    let err_text = if err_text.len() > 300 {
        format!("{}... [truncated]", &err_text[..300])
    } else {
        err_text
    };

    if status.is_client_error() && status.as_u16() != 429 {
        return Err(ConsumerError::Permanent(format!(
            "Brevo API permanent error {}: {}",
            status, err_text
        )));
    }

    Err(ConsumerError::Transient(anyhow::anyhow!(
        "Brevo API transient error {}: {}",
        status,
        err_text
    )))
}

fn split_display_email(s: &str) -> (String, String) {
    if let (Some(lt), Some(gt)) = (s.find('<'), s.find('>')) {
        let name = s[..lt].trim().to_string();
        let email = s[lt + 1..gt].trim().to_string();
        (name, email)
    } else {
        (s.to_string(), s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_pdf() {
        let payload = BookingCreatedData {
            booking_id: 1234,
            user_email: "test@example.com".to_string(),
            contact_name: Some("John Doe".to_string()),
            turf_name: Some("Green Field".to_string()),
            game_name: Some("Football 7v7".to_string()),
            slot_start: "2026-10-05 18:00".to_string(),
            amount: "1500".to_string(),
            total_price: Some("1500".to_string()),
            paid_amount: Some("500".to_string()),
            due_amount: Some("1000".to_string()),
            payment_status: Some("Partially_Paid".to_string()),
        };

        let pdf = generate_invoice_pdf(&payload).expect("PDF generation should succeed");
        assert!(!pdf.is_empty(), "PDF output should not be empty");
        assert!(
            pdf.len() > 10_000,
            "PDF should contain embedded font subsets and rendered text (got {} bytes)",
            pdf.len()
        );
    }
}

