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
    let status_badge_class = if payload.is_partially_paid() {
        "status-partial"
    } else {
        "status-paid"
    };

    let due_section = if payload.is_partially_paid() {
        format!(
            r#"<tr class="due-row">
                <td colspan="2"><strong>Amount Due at Venue</strong></td>
                <td class="text-right text-due"><strong>BDT {}</strong></td>
            </tr>"#,
            payload.display_due_amount()
        )
    } else {
        String::new()
    };

    let payment_note = if payload.is_partially_paid() {
        format!(
            r#"<div class="notice-card warning">
                <strong>⚠️ Advance Payment Acknowledged:</strong> BDT {} paid online. Please settle the remaining balance of <strong>BDT {}</strong> at the venue desk prior to match kickoff.
            </div>"#,
            payload.display_paid_amount(),
            payload.display_due_amount()
        )
    } else {
        r#"<div class="notice-card success">
            <strong>✅ Paid in Full:</strong> This reservation has been completely settled online. Please present this invoice at the venue for direct entry.
        </div>"#.to_string()
    };

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Invoice #{}</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      color: #1e293b;
      background-color: #ffffff;
      padding: 40px;
      font-size: 14px;
      line-height: 1.5;
    }}
    .invoice-card {{
      max-width: 680px;
      margin: 0 auto;
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      padding: 36px;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      border-bottom: 2px solid #0f172a;
      padding-bottom: 20px;
      margin-bottom: 24px;
    }}
    .brand {{
      font-size: 26px;
      font-weight: 800;
      letter-spacing: -0.5px;
      color: #0f172a;
    }}
    .brand span {{ color: #10b981; }}
    .invoice-title {{
      text-align: right;
    }}
    .invoice-title h2 {{
      font-size: 20px;
      color: #334155;
      text-transform: uppercase;
      letter-spacing: 1px;
    }}
    .invoice-title p {{
      color: #64748b;
      font-size: 13px;
    }}
    .grid-info {{
      display: flex;
      justify-content: space-between;
      margin-bottom: 24px;
      gap: 20px;
    }}
    .info-block h4 {{
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      color: #64748b;
      margin-bottom: 6px;
    }}
    .info-block p {{
      font-size: 14px;
      font-weight: 600;
      color: #1e293b;
    }}
    .info-block span {{
      color: #475569;
      font-weight: 400;
      display: block;
    }}
    .badge {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 9999px;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }}
    .status-paid {{
      background-color: #ecfdf5;
      color: #059669;
      border: 1px solid #a7f3d0;
    }}
    .status-partial {{
      background-color: #fffbeb;
      color: #d97706;
      border: 1px solid #fde68a;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-bottom: 24px;
    }}
    th {{
      background-color: #f8fafc;
      color: #475569;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      text-align: left;
      padding: 12px 14px;
      border-top: 1px solid #e2e8f0;
      border-bottom: 1px solid #e2e8f0;
    }}
    td {{
      padding: 14px;
      border-bottom: 1px solid #f1f5f9;
      color: #334155;
    }}
    .text-right {{ text-align: right; }}
    .text-due {{ color: #dc2626; }}
    .text-paid {{ color: #059669; }}
    .total-row td {{
      font-weight: 700;
      background-color: #f8fafc;
      border-top: 2px solid #e2e8f0;
    }}
    .due-row td {{
      background-color: #fef2f2;
      border-top: 1px solid #fecaca;
    }}
    .notice-card {{
      padding: 14px 18px;
      border-radius: 8px;
      font-size: 13px;
      margin-bottom: 24px;
      line-height: 1.6;
    }}
    .notice-card.warning {{
      background-color: #fffbeb;
      border: 1px solid #fef3c7;
      color: #92400e;
    }}
    .notice-card.success {{
      background-color: #ecfdf5;
      border: 1px solid #d1fae5;
      color: #065f46;
    }}
    .footer {{
      border-top: 1px solid #e2e8f0;
      padding-top: 18px;
      text-align: center;
      color: #94a3b8;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <div class="invoice-card">
    <div class="header">
      <div class="brand">TURF<span>BD</span></div>
      <div class="invoice-title">
        <h2>Tax Invoice</h2>
        <p>Booking #{}</p>
      </div>
    </div>

    <div class="grid-info">
      <div class="info-block">
        <h4>Customer Details</h4>
        <p>{}</p>
        <span>{}</span>
      </div>
      <div class="info-block">
        <h4>Venue & Pitch</h4>
        <p>{}</p>
        <span>{}</span>
      </div>
      <div class="info-block text-right">
        <h4>Match Schedule</h4>
        <p>{}</p>
        <span style="margin-top: 6px;"><span class="badge {}">{}</span></span>
      </div>
    </div>

    <table>
      <thead>
        <tr>
          <th>Description</th>
          <th>Rate / Unit</th>
          <th class="text-right">Amount</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>
            <strong>Slot Reservation</strong><br>
            <span style="color: #64748b; font-size: 12px;">{} &bull; {}</span>
          </td>
          <td>1 Match Slot</td>
          <td class="text-right">BDT {}</td>
        </tr>
        <tr class="total-row">
          <td colspan="2"><strong>Total Slot Price</strong></td>
          <td class="text-right"><strong>BDT {}</strong></td>
        </tr>
        <tr>
          <td colspan="2"><span class="text-paid">Amount Paid Online</span></td>
          <td class="text-right text-paid"><strong>BDT {}</strong></td>
        </tr>
        {}
      </tbody>
    </table>

    {}

    <div class="footer">
      Thank you for playing with Turf BD &bull; For questions, contact support@turfbd.com
    </div>
  </div>
</body>
</html>"#,
        payload.booking_id,
        payload.booking_id,
        payload.display_contact_name(),
        payload.user_email,
        payload.display_turf_name(),
        payload.display_game_name(),
        payload.slot_start,
        status_badge_class,
        payload.display_payment_status(),
        payload.display_turf_name(),
        payload.display_game_name(),
        payload.display_total_price(),
        payload.display_total_price(),
        payload.display_paid_amount(),
        due_section,
        payment_note
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
    let subject = format!(
        "Booking Confirmed #{} — {} ({})",
        payload.booking_id,
        payload.display_turf_name(),
        payload.display_game_name()
    );

    let payment_summary = if payload.is_partially_paid() {
        format!(
            r#"<div style="background: #fffbeb; border: 1px solid #fde68a; border-radius: 8px; padding: 14px; margin: 16px 0;">
                <p style="margin: 0; color: #92400e; font-weight: bold;">⚠️ Advance Payment Confirmed</p>
                <p style="margin: 6px 0 0 0; color: #b45309; font-size: 14px;">
                    Amount Paid: <strong>BDT {}</strong><br />
                    Amount Due at Turf: <strong style="color: #dc2626;">BDT {}</strong>
                </p>
            </div>"#,
            payload.display_paid_amount(),
            payload.display_due_amount()
        )
    } else {
        format!(
            r#"<div style="background: #ecfdf5; border: 1px solid #a7f3d0; border-radius: 8px; padding: 14px; margin: 16px 0;">
                <p style="margin: 0; color: #065f46; font-weight: bold;">✅ Payment Complete</p>
                <p style="margin: 6px 0 0 0; color: #047857; font-size: 14px;">Total Paid: <strong>BDT {}</strong> (Fully Paid)</p>
            </div>"#,
            payload.display_paid_amount()
        )
    };

    let html_content = format!(
        r#"<!DOCTYPE html>
<html>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #1e293b; line-height: 1.6; max-width: 600px; margin: 0 auto; padding: 20px;">
    <h2 style="color: #0f172a; margin-bottom: 8px;">Your Booking is Confirmed!</h2>
    <p>Hi <strong>{}</strong>,</p>
    <p>Great news! Your slot reservation has been successfully confirmed.</p>
    
    <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; margin: 16px 0;">
        <p style="margin: 0 0 8px 0;"><strong>Booking ID:</strong> #{}</p>
        <p style="margin: 0 0 8px 0;"><strong>Venue:</strong> {}</p>
        <p style="margin: 0 0 8px 0;"><strong>Pitch / Game:</strong> {}</p>
        <p style="margin: 0;"><strong>Match Schedule:</strong> {}</p>
    </div>

    {}

    <p style="color: #475569; font-size: 14px;">
        Your official tax invoice has been generated and attached as a PDF (<strong>invoice-{}.pdf</strong>).
    </p>
    
    <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 24px 0;" />
    <p style="font-size: 12px; color: #94a3b8; text-align: center;">
        Turf BD &bull; Instant Sports Booking
    </p>
</body>
</html>"#,
        payload.display_contact_name(),
        payload.booking_id,
        payload.display_turf_name(),
        payload.display_game_name(),
        payload.slot_start,
        payment_summary,
        payload.booking_id
    );

    let body = serde_json::json!({
        "from": config.resend_from_email.as_str(),
        "to": [payload.user_email],
        "subject": subject,
        "html": html_content,
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
