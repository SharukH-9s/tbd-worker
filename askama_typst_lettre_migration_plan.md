# Migration Plan: Askama + Typst + Lettre + Brevo
## Replace Gotenberg (PDF) + Resend (Email) in `tbd-worker`

> **Scope**: `tbd-worker` only. The outbox table, relay, RabbitMQ topology, idempotency logic, and
> `tbd-backend` are **completely untouched**.

---

## 0. What Is Being Replaced vs. Kept

| Component | Action | Reason |
|---|---|---|
| Gotenberg HTTP sidecar | ❌ Removed | PDF now rendered in-process via Typst |
| Resend API (HTTP) | ❌ Removed | Email now sent via Lettre SMTP to Brevo |
| `reqwest` crate | ❌ Removed | No more outgoing HTTP calls needed |
| `base64` crate | ❌ Removed | No longer Base64-encoding the PDF |
| RabbitMQ / `lapin` | ✅ Kept | Message broker stays exactly as-is |
| Outbox relay in `tbd-backend` | ✅ Kept | Unchanged |
| `processed_jobs` idempotency guard | ✅ Kept | Unchanged |
| `EventEnvelope<BookingCreatedData>` | ✅ Kept | Deserialization contract unchanged |
| ACK / NACK / DLQ routing logic | ✅ Kept | Unchanged |
| Retry loop (max 3 attempts, exponential backoff) | ✅ Kept | Unchanged |

---

## 1. New Dependency Stack

### 1.1 Crates to Add

| Crate | Version | Purpose |
|---|---|---|
| `askama` | `0.12` | Compile-time Jinja2-style HTML templates for email body |
| `typst` | `0.13` | In-process Typst compiler for PDF invoice generation |
| `typst-pdf` | `0.13` | Renders a compiled Typst document to PDF bytes |
| `typst-kit` | `0.13` | `FileSystemResolver` so Typst can load `.typ` template files |
| `lettre` | `0.11` | SMTP email client — builds MIME message, sends via Brevo |
| `comemo` | `0.4` | Required by Typst's compilation cache invalidation |
| `chrono` | _already present_ | Used for formatting `slot_start` in templates |

### 1.2 Crates to Remove

| Crate | Reason |
|---|---|
| `reqwest` | No more HTTP calls to Gotenberg or Resend |
| `base64` | No longer needed; Lettre handles binary attachments natively |

### 1.3 Final `Cargo.toml` `[dependencies]` Block

```toml
[dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# AMQP client — unchanged
lapin = "2"

# Serialization — unchanged
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = { version = "0.4", features = ["serde"] }

# UUID — unchanged
uuid = { version = "1", features = ["v4", "serde"] }

# Postgres idempotency — unchanged
sqlx = { version = "0.8", features = ["postgres", "runtime-tokio-rustls", "uuid", "macros"] }

# Structured logging — unchanged
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tracing-appender = "0.2"

# Error handling — unchanged
anyhow = "1"

# Env loading — unchanged
dotenvy = "0.15"
futures = "0.3"
axum = "0.8"

# NEW: Email + PDF Stack
askama = "0.12"
typst = "0.13"
typst-pdf = "0.13"
typst-kit = { version = "0.13", features = ["compile"] }
comemo = "0.4"
lettre = { version = "0.11", features = ["tokio1-native-tls", "builder"] }
```

---

## 2. File System Changes

### 2.1 New Files / Directories to Create

```
tbd-worker/
├── templates/                          <- NEW directory (Askama root)
│   ├── email_booking.html              <- NEW: Askama template for email body
│   └── pdf_invoice.typ                 <- NEW: Typst template for PDF invoice
└── src/
    ├── config.rs                       <- MODIFIED
    ├── error.rs                        <- UNCHANGED
    ├── main.rs                         <- MODIFIED
    └── consumers/
        ├── mod.rs                      <- UNCHANGED
        └── booking.rs                  <- MODIFIED (core changes here)
```

> **Askama default**: Askama looks for templates in a `templates/` folder at the **crate root**
> (sibling to `src/`). No extra configuration needed.

---

## 3. Environment Variables

### 3.1 Remove from `.env` and Render dashboard

```diff
- RESEND_API_KEY=re_QiktcA47_...
- RESEND_FROM_EMAIL=TBD <onboarding@resend.dev>
- GOTENBERG_URL=https://gotenberg-8-hgtn.onrender.com
- GOTENBERG_USER=admin
- GOTENBERG_PASSWORD=your_strong_secret_password
```

### 3.2 Add to `.env` and Render dashboard

```diff
+ BREVO_SMTP_HOST=smtp-relay.brevo.com
+ BREVO_SMTP_PORT=587
+ BREVO_SMTP_USER=<your_brevo_login_email>
+ BREVO_SMTP_PASSWORD=<your_brevo_smtp_key>
+ EMAIL_FROM=TBD <no-reply@yourdomain.com>
```

> **Brevo free tier**: 300 emails/day, 9,000/month. SMTP key is separate from API key — generate
> it at `app.brevo.com -> SMTP & API -> SMTP tab -> Generate a new SMTP key`.

### 3.3 Variables Retained (unchanged)

```
AMQP_URL
DATABASE_URL
AMQP_PREFETCH_COUNT
DATABASE_MAX_CONNECTIONS
DATABASE_ACQUIRE_TIMEOUT_SECS
HTTP_TIMEOUT_SECS       <- can be removed; no HTTP client anymore
PORT
APP_ENV
RUST_LOG
```

---

## 4. `src/config.rs` — Full Rewrite

Remove all Gotenberg and Resend fields. Add Brevo SMTP fields and a
`lettre::AsyncSmtpTransport` instead of a `reqwest::Client`.

**Before (relevant fields):**

```rust
pub resend_api_key: String,
pub resend_from_email: String,
pub gotenberg_url: String,
pub gotenberg_user: Option<String>,
pub gotenberg_password: Option<String>,
pub http_client: reqwest::Client,
pub db: sqlx::PgPool,
```

**After (full new struct):**

```rust
use lettre::AsyncSmtpTransport;
use lettre::Tokio1Executor;

#[derive(Clone)]
pub struct WorkerConfig {
    /// CloudAMQP connection URL — unchanged
    pub amqp_url: String,

    /// AMQP QoS prefetch count — unchanged
    pub amqp_prefetch_count: u16,

    /// Brevo SMTP hostname (smtp-relay.brevo.com)
    pub brevo_smtp_host: String,

    /// Brevo SMTP port (587)
    pub brevo_smtp_port: u16,

    /// Brevo SMTP login email (your Brevo account email)
    pub brevo_smtp_user: String,

    /// Brevo SMTP password (the generated SMTP key, NOT your account password)
    pub brevo_smtp_password: String,

    /// "From" display address — e.g. "TBD <no-reply@yourdomain.com>"
    pub email_from: String,

    /// Pre-built, cloneable SMTP transport — reuses the connection pool
    pub mailer: AsyncSmtpTransport<Tokio1Executor>,

    /// Postgres pool — unchanged
    pub db: sqlx::PgPool,
}
```

> `AsyncSmtpTransport<Tokio1Executor>` is cheaply `Clone`-able because it wraps an `Arc`
> internally. One pool is built at startup and shared across all consumer tasks.

---

## 5. `src/main.rs` — Targeted Changes

### 5.1 Remove These Blocks

```rust
// REMOVE: Resend
let resend_api_key = env::var("RESEND_API_KEY").expect("RESEND_API_KEY must be set")...;
let resend_from_email = env::var("RESEND_FROM_EMAIL").unwrap_or_else(...);

// REMOVE: Gotenberg
let gotenberg_url = env::var("GOTENBERG_URL").expect("GOTENBERG_URL must be set")...;
let gotenberg_user = env::var("GOTENBERG_USER").ok();
let gotenberg_password = env::var("GOTENBERG_PASSWORD").ok();

// REMOVE: reqwest HTTP client
let http_client = reqwest::Client::builder()
    .timeout(...)
    .build()
    .expect("Failed to build HTTP client");
```

### 5.2 Add These Blocks (after DB pool creation)

```rust
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, Tokio1Executor};

// Read Brevo SMTP Config
let brevo_smtp_host = env::var("BREVO_SMTP_HOST")
    .unwrap_or_else(|_| "smtp-relay.brevo.com".to_string());

let brevo_smtp_port: u16 = env::var("BREVO_SMTP_PORT")
    .ok().and_then(|v| v.parse().ok()).unwrap_or(587);

let brevo_smtp_user = env::var("BREVO_SMTP_USER")
    .expect("BREVO_SMTP_USER must be set")
    .trim().to_string();

let brevo_smtp_password = env::var("BREVO_SMTP_PASSWORD")
    .expect("BREVO_SMTP_PASSWORD must be set")
    .trim().to_string();

let email_from = env::var("EMAIL_FROM")
    .unwrap_or_else(|_| "TBD <no-reply@turfbd.com>".to_string());

// Build SMTP Transport (Lettre)
let creds = Credentials::new(brevo_smtp_user.clone(), brevo_smtp_password.clone());

let mailer: AsyncSmtpTransport<Tokio1Executor> =
    AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&brevo_smtp_host)
        .expect("Failed to build SMTP transport")
        .port(brevo_smtp_port)
        .credentials(creds)
        .build();

tracing::info!(
    smtp_host = %brevo_smtp_host,
    smtp_port = brevo_smtp_port,
    "tbd-worker: SMTP transport configured"
);
```

### 5.3 Update `WorkerConfig` Construction

```rust
// BEFORE
let worker_config = config::WorkerConfig {
    amqp_url,
    amqp_prefetch_count,
    resend_api_key,
    resend_from_email,
    gotenberg_url,
    gotenberg_user,
    gotenberg_password,
    http_client,
    db,
};

// AFTER
let worker_config = config::WorkerConfig {
    amqp_url,
    amqp_prefetch_count,
    brevo_smtp_host,
    brevo_smtp_port,
    brevo_smtp_user,
    brevo_smtp_password,
    email_from,
    mailer,
    db,
};
```

---

## 6. `templates/email_booking.html` — New Askama Template

Create `tbd-worker/templates/email_booking.html`. Askama uses Jinja2-like `{{ variable }}` and
`{% if condition %}` syntax. The struct fields in `booking.rs` map 1:1 to template variables.

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Booking Confirmed #{{ booking_id }}</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
             color: #1e293b; line-height: 1.6; max-width: 600px; margin: 0 auto; padding: 20px;">

  <h2 style="color: #0f172a; margin-bottom: 8px;">Your Booking is Confirmed!</h2>
  <p>Hi <strong>{{ contact_name }}</strong>,</p>
  <p>Great news! Your slot reservation has been successfully confirmed.</p>

  <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px;
              padding: 16px; margin: 16px 0;">
    <p style="margin: 0 0 8px 0;"><strong>Booking ID:</strong> #{{ booking_id }}</p>
    <p style="margin: 0 0 8px 0;"><strong>Venue:</strong> {{ turf_name }}</p>
    <p style="margin: 0 0 8px 0;"><strong>Pitch / Game:</strong> {{ game_name }}</p>
    <p style="margin: 0;"><strong>Match Schedule:</strong> {{ slot_start }}</p>
  </div>

  {% if is_partially_paid %}
  <div style="background: #fffbeb; border: 1px solid #fde68a; border-radius: 8px;
              padding: 14px; margin: 16px 0;">
    <p style="margin: 0; color: #92400e; font-weight: bold;">Advance Payment Confirmed</p>
    <p style="margin: 6px 0 0 0; color: #b45309; font-size: 14px;">
      Amount Paid: <strong>BDT {{ paid_amount }}</strong><br />
      Amount Due at Turf: <strong style="color: #dc2626;">BDT {{ due_amount }}</strong>
    </p>
  </div>
  {% else %}
  <div style="background: #ecfdf5; border: 1px solid #a7f3d0; border-radius: 8px;
              padding: 14px; margin: 16px 0;">
    <p style="margin: 0; color: #065f46; font-weight: bold;">Payment Complete</p>
    <p style="margin: 6px 0 0 0; color: #047857; font-size: 14px;">
      Total Paid: <strong>BDT {{ paid_amount }}</strong> (Fully Paid)
    </p>
  </div>
  {% endif %}

  <p style="color: #475569; font-size: 14px;">
    Your official tax invoice has been attached as a PDF
    (<strong>invoice-{{ booking_id }}.pdf</strong>).
  </p>

  <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 24px 0;" />
  <p style="font-size: 12px; color: #94a3b8; text-align: center;">
    Turf BD - Instant Sports Booking
  </p>
</body>
</html>
```

---

## 7. `templates/pdf_invoice.typ` — New Typst Template

Create `tbd-worker/templates/pdf_invoice.typ`. Variables are injected via the `sys.inputs`
dictionary at compile time in Rust. The `.typ` file reads them with `sys.inputs.at("key")`.

```typst
// pdf_invoice.typ
// Variables injected by Rust via sys.inputs dictionary

#let booking_id     = sys.inputs.at("booking_id")
#let contact_name   = sys.inputs.at("contact_name")
#let user_email     = sys.inputs.at("user_email")
#let turf_name      = sys.inputs.at("turf_name")
#let game_name      = sys.inputs.at("game_name")
#let slot_start     = sys.inputs.at("slot_start")
#let total_price    = sys.inputs.at("total_price")
#let paid_amount    = sys.inputs.at("paid_amount")
#let due_amount     = sys.inputs.at("due_amount")
#let payment_status = sys.inputs.at("payment_status")
#let is_partial     = sys.inputs.at("is_partial") == "true"

#set page(margin: (x: 2cm, y: 2cm), paper: "a4")
#set text(font: "Liberation Sans", size: 11pt, fill: rgb("#1e293b"))

// Header
#grid(
  columns: (1fr, 1fr),
  align(left)[
    #text(size: 22pt, weight: "bold")[
      TURF#text(fill: rgb("#10b981"))[BD]
    ]
  ],
  align(right)[
    #text(size: 16pt, weight: "bold", fill: rgb("#334155"))[TAX INVOICE]\
    #text(size: 11pt, fill: rgb("#64748b"))[Booking ##booking_id]
  ]
)

#line(stroke: 2pt + rgb("#0f172a"), length: 100%)
#v(0.5em)

// Info grid
#grid(
  columns: (1fr, 1fr, 1fr),
  gutter: 1em,
  [
    #text(size: 8pt, fill: rgb("#64748b"), weight: "bold")[CUSTOMER DETAILS]\
    #text(weight: "bold")[#contact_name]\
    #text(size: 10pt, fill: rgb("#475569"))[#user_email]
  ],
  [
    #text(size: 8pt, fill: rgb("#64748b"), weight: "bold")[VENUE & PITCH]\
    #text(weight: "bold")[#turf_name]\
    #text(size: 10pt, fill: rgb("#475569"))[#game_name]
  ],
  align(right)[
    #text(size: 8pt, fill: rgb("#64748b"), weight: "bold")[MATCH SCHEDULE]\
    #text(weight: "bold")[#slot_start]\
    #v(0.3em)
    #if is_partial [
      #box(
        fill: rgb("#fffbeb"), stroke: 1pt + rgb("#fde68a"),
        radius: 4pt, inset: (x: 8pt, y: 4pt)
      )[
        #text(size: 8pt, weight: "bold", fill: rgb("#d97706"))[PARTIALLY PAID]
      ]
    ] else [
      #box(
        fill: rgb("#ecfdf5"), stroke: 1pt + rgb("#a7f3d0"),
        radius: 4pt, inset: (x: 8pt, y: 4pt)
      )[
        #text(size: 8pt, weight: "bold", fill: rgb("#059669"))[FULLY PAID]
      ]
    ]
  ]
)

#v(1em)

// Line items table
#table(
  columns: (auto, 1fr, auto),
  stroke: none,
  fill: (col, row) => if row == 0 { rgb("#f8fafc") } else { white },
  table.header(
    [*Description*], [*Rate / Unit*], [*Amount*]
  ),
  [
    *Slot Reservation*\
    #text(size: 9pt, fill: rgb("#64748b"))[#turf_name - #game_name]
  ],
  [1 Match Slot],
  [BDT #total_price],
  table.hline(stroke: 2pt + rgb("#e2e8f0")),
  [*Total Slot Price*], [], [*BDT #total_price*],
  table.hline(stroke: 1pt + rgb("#e2e8f0")),
  [#text(fill: rgb("#059669"))[Amount Paid Online]],
  [],
  [#text(fill: rgb("#059669"), weight: "bold")[BDT #paid_amount]],
  ..if is_partial {(
    table.hline(stroke: 1pt + rgb("#fecaca")),
    [*Amount Due at Venue*], [], [#text(fill: rgb("#dc2626"), weight: "bold")[BDT #due_amount]]
  )} else {()}
)

#v(0.8em)

// Payment notice block
#if is_partial [
  #block(
    fill: rgb("#fffbeb"), stroke: 1pt + rgb("#fef3c7"),
    radius: 6pt, inset: 12pt, width: 100%
  )[
    #text(weight: "bold", fill: rgb("#92400e"))[Advance Payment Acknowledged:] \
    #text(fill: rgb("#92400e"))[
      BDT #paid_amount paid online. Please settle the remaining balance of
      *BDT #due_amount* at the venue desk prior to match kickoff.
    ]
  ]
] else [
  #block(
    fill: rgb("#ecfdf5"), stroke: 1pt + rgb("#d1fae5"),
    radius: 6pt, inset: 12pt, width: 100%
  )[
    #text(weight: "bold", fill: rgb("#065f46"))[Paid in Full:] \
    #text(fill: rgb("#065f46"))[
      This reservation has been completely settled online.
      Please present this invoice at the venue for direct entry.
    ]
  ]
]

#v(1fr)

// Footer
#line(stroke: 1pt + rgb("#e2e8f0"), length: 100%)
#align(center)[
  #text(size: 9pt, fill: rgb("#94a3b8"))[
    Thank you for playing with Turf BD - support@turfbd.com
  ]
]
```

---

## 8. `src/consumers/booking.rs` — Core Changes

The message loop, idempotency guard, retry loop, and ACK/NACK routing are **completely
unchanged**. Only the three I/O functions are touched.

### 8.1 Import Block Changes

**Remove:**

```rust
use base64::{engine::general_purpose, Engine as _};
```

**Add:**

```rust
use askama::Template;
use lettre::{
    message::{Attachment, MultiPart, SinglePart, header::ContentType},
    AsyncTransport, Message,
};
```

### 8.2 New Askama Template Struct — Add Near Top of File

Place this after the `BookingCreatedData` impl block:

```rust
/// Askama template — maps 1:1 to `templates/email_booking.html`.
/// Template is validated at compile-time; rendering is zero-cost at runtime.
#[derive(Template)]
#[template(path = "email_booking.html")]
struct BookingEmailTemplate<'a> {
    booking_id:        i64,
    contact_name:      &'a str,
    turf_name:         &'a str,
    game_name:         &'a str,
    slot_start:        &'a str,
    paid_amount:       &'a str,
    due_amount:        &'a str,
    is_partially_paid: bool,
}
```

### 8.3 `process_booking_job` — Change

The orchestrator now calls three synchronous steps:

```rust
async fn process_booking_job(
    config: &WorkerConfig,
    payload: &BookingCreatedData,
) -> Result<(), ConsumerError> {
    // Step 1: Generate PDF bytes in-process via Typst (sync, no HTTP)
    let pdf_bytes = generate_invoice_pdf(payload)?;

    // Step 2: Render email HTML via Askama (sync, compile-time validated)
    let html_body = render_email_html(payload)?;

    // Step 3: Send MIME email with PDF attachment via Lettre -> Brevo SMTP
    send_booking_email(config, payload, html_body, pdf_bytes).await?;

    Ok(())
}
```

> **Note**: Typst rendering is CPU-bound synchronous work. At prefetch = 1, this is fine on
> a worker process. If `AMQP_PREFETCH_COUNT` is ever raised, wrap `generate_invoice_pdf` in
> `tokio::task::spawn_blocking` to avoid blocking the async executor.

### 8.4 `generate_invoice_pdf` — Full Replacement

Replace the entire function (currently lines 386–720 in `booking.rs`):

```rust
/// Renders the Typst invoice template in-process and returns raw PDF bytes.
/// The template file is loaded from `templates/pdf_invoice.typ` at runtime.
/// Errors are classified as Permanent because a broken .typ file will not
/// fix itself on retry.
fn generate_invoice_pdf(payload: &BookingCreatedData) -> Result<Vec<u8>, ConsumerError> {
    use typst::foundations::{Dict, IntoValue};
    use typst::text::FontBook;
    use typst_kit::compile::{compile, CompileParams, Input};

    // Load template source from disk.
    // Working directory on Render is the project root, so this resolves correctly.
    let template_path = std::path::PathBuf::from("templates/pdf_invoice.typ");
    let source = std::fs::read_to_string(&template_path).map_err(|e| {
        ConsumerError::Permanent(format!(
            "Typst: cannot read template at {:?}: {}",
            template_path, e
        ))
    })?;

    // Build the sys.inputs dictionary injected into the Typst document.
    let mut inputs = Dict::new();
    let mut push = |k: &str, v: &str| {
        inputs.insert(k.into(), v.into_value());
    };
    push("booking_id",     &payload.booking_id.to_string());
    push("contact_name",   payload.display_contact_name());
    push("user_email",     &payload.user_email);
    push("turf_name",      payload.display_turf_name());
    push("game_name",      payload.display_game_name());
    push("slot_start",     &payload.slot_start);
    push("total_price",    payload.display_total_price());
    push("paid_amount",    payload.display_paid_amount());
    push("due_amount",     payload.display_due_amount());
    push("payment_status", payload.display_payment_status());
    push("is_partial",     if payload.is_partially_paid() { "true" } else { "false" });

    // Compile the Typst source to an intermediate document.
    let params = CompileParams {
        input: Input::Source(source),
        inputs,
        font_book: FontBook::new(), // uses Liberation Sans bundled in typst-kit
        ..Default::default()
    };

    let doc = compile(params).map_err(|errors| {
        let msgs: Vec<String> = errors.iter().map(|e| format!("{:?}", e)).collect();
        ConsumerError::Permanent(format!("Typst compile error(s): {}", msgs.join("; ")))
    })?;

    // Export to PDF bytes.
    let pdf_bytes = typst_pdf::pdf(&doc, &typst_pdf::PdfOptions::default())
        .map_err(|e| ConsumerError::Permanent(format!("Typst PDF export error: {:?}", e)))?;

    Ok(pdf_bytes)
}
```

### 8.5 `render_email_html` — New Function

Add this function. It replaces the inline `format!` call that used to build the HTML body
inside `send_booking_email`:

```rust
/// Renders the Askama HTML email template to a String.
/// Rendering is infallible if the template compiled successfully at build time.
fn render_email_html(payload: &BookingCreatedData) -> Result<String, ConsumerError> {
    let tmpl = BookingEmailTemplate {
        booking_id:        payload.booking_id,
        contact_name:      payload.display_contact_name(),
        turf_name:         payload.display_turf_name(),
        game_name:         payload.display_game_name(),
        slot_start:        &payload.slot_start,
        paid_amount:       payload.display_paid_amount(),
        due_amount:        payload.display_due_amount(),
        is_partially_paid: payload.is_partially_paid(),
    };

    tmpl.render().map_err(|e| {
        ConsumerError::Permanent(format!("Askama template render error: {}", e))
    })
}
```

### 8.6 `send_booking_email` — Full Replacement

Replace the entire `send_booking_email` function (currently lines 722–843):

```rust
/// Builds a MIME multipart email (HTML body + PDF attachment) and sends it
/// via Lettre through the Brevo SMTP relay.
async fn send_booking_email(
    config: &WorkerConfig,
    payload: &BookingCreatedData,
    html_body: String,
    pdf_bytes: Vec<u8>,
) -> Result<(), ConsumerError> {
    // Subject line
    let subject = format!(
        "Booking Confirmed #{} — {} ({})",
        payload.booking_id,
        payload.display_turf_name(),
        payload.display_game_name()
    );

    // PDF attachment — Lettre takes raw bytes, no base64 encoding needed
    let attachment_filename = format!("invoice-{}.pdf", payload.booking_id);
    let attachment = Attachment::new(attachment_filename)
        .body(pdf_bytes, ContentType::parse("application/pdf").unwrap());

    // Parse From/To as RFC 5321 mailboxes
    let from_addr = config.email_from
        .parse::<lettre::message::Mailbox>()
        .map_err(|e| ConsumerError::Permanent(format!("Invalid FROM address: {}", e)))?;

    let to_addr = payload.user_email
        .parse::<lettre::message::Mailbox>()
        .map_err(|e| ConsumerError::Permanent(
            format!("Invalid TO address '{}': {}", payload.user_email, e)
        ))?;

    // Build MIME message:
    //   multipart/mixed
    //     text/html         <- rendered by Askama
    //     application/pdf   <- invoice generated by Typst
    let email = Message::builder()
        .from(from_addr)
        .to(to_addr)
        .subject(subject)
        .multipart(
            MultiPart::mixed()
                .singlepart(
                    SinglePart::builder()
                        .header(ContentType::TEXT_HTML)
                        .body(html_body),
                )
                .singlepart(attachment),
        )
        .map_err(|e| ConsumerError::Permanent(
            format!("Failed to build MIME message: {}", e)
        ))?;

    // Send via Brevo SMTP
    config.mailer.send(email).await.map_err(|e| {
        let msg = e.to_string();
        // SMTP 5xx authentication or mailbox errors are permanent — no point retrying.
        // Connection drops, timeouts, and 421 (service temporarily unavailable) are transient.
        if msg.contains("550")
            || msg.contains("553")
            || msg.contains("Authentication")
            || msg.contains("535")
        {
            ConsumerError::Permanent(format!("SMTP permanent error: {}", msg))
        } else {
            ConsumerError::Transient(anyhow::anyhow!("SMTP transient error: {}", msg))
        }
    })?;

    tracing::info!(
        booking_id = payload.booking_id,
        recipient = %payload.user_email,
        "Booking consumer: email dispatched via Brevo SMTP"
    );

    Ok(())
}
```

---

## 9. Error Classification Table — Updated

| Error Scenario | Class | Action | Reason |
|---|---|---|---|
| Malformed JSON envelope | **Permanent** | `nack(requeue: false)` -> DLQ | Unchanged |
| Typst `.typ` file missing | **Permanent** | `nack(requeue: false)` -> DLQ | Deploy error; retry useless |
| Typst compile error | **Permanent** | `nack(requeue: false)` -> DLQ | Code bug; retry useless |
| Askama render error | **Permanent** | `nack(requeue: false)` -> DLQ | Compile-time template issue |
| Invalid `EMAIL_FROM` format | **Permanent** | `nack(requeue: false)` -> DLQ | Config error |
| Invalid recipient email format | **Permanent** | `nack(requeue: false)` -> DLQ | Bad payload data |
| SMTP auth failure (535) | **Permanent** | `nack(requeue: false)` -> DLQ | Wrong credentials |
| SMTP 550/553 (bad mailbox) | **Permanent** | `nack(requeue: false)` -> DLQ | Invalid address |
| SMTP connection timeout / drop | **Transient** | Retry w/ backoff -> DLQ if maxed | Network blip |
| SMTP 421 (service temporarily unavailable) | **Transient** | Retry w/ backoff -> DLQ if maxed | Brevo momentarily down |
| Postgres idempotency check error | **Transient** | `nack(requeue: true)` | Unchanged |
| Postgres `processed_jobs` insert error | **Transient** | `nack(requeue: true)` | Unchanged |

---

## 10. `tbd-backend` — Verify Only

**File:** [`src/api/utils/tester.rs`](file:///e:/projects/anti/TBD/src/api/utils/tester.rs)

The worker keep-alive ping (`GET {WORKER_URL}/healthz`) **stays** — `tbd-worker` still runs on
Render's free tier. Check if there is any Gotenberg-specific ping (there shouldn't be based on the
codebase review, but confirm):

```powershell
Select-String -Path "e:\projects\anti\TBD\src\*" -Pattern "gotenberg" -Recurse
```

If any result comes back, remove that code block. If not (expected), no action required.

---

## 11. Infrastructure Cleanup

### 11.1 Brevo Setup (One-Time, Do This First)

1. Sign up / log in at `app.brevo.com`.
2. Go to **Senders & IPs -> Senders** -> Add a verified sender domain (add the required DNS TXT/DKIM records).
3. Go to **SMTP & API -> SMTP tab** -> **Generate a new SMTP key**. This is your `BREVO_SMTP_PASSWORD`.
4. Note your Brevo account login email — this is your `BREVO_SMTP_USER`.
5. Free tier: 300 emails/day, 9,000/month — more than enough for a booking platform.

### 11.2 Render Dashboard Changes

On `render.com`:

1. **Delete** the `gotenberg-pdf` free web service (frees up one free-tier slot).
2. On `tbd-worker` service -> **Environment**:
   - **Delete**: `RESEND_API_KEY`, `RESEND_FROM_EMAIL`, `GOTENBERG_URL`, `GOTENBERG_USER`, `GOTENBERG_PASSWORD`, `HTTP_TIMEOUT_SECS`
   - **Add**: `BREVO_SMTP_HOST`, `BREVO_SMTP_PORT`, `BREVO_SMTP_USER`, `BREVO_SMTP_PASSWORD`, `EMAIL_FROM`
3. On `tbd-backend` service -> **Environment**: verify none of the Gotenberg vars were added there.

---

## 12. Implementation Order

Execute in this exact order to keep things compiling at every step:

1. **Set up Brevo** (§11.1) — get SMTP credentials before writing any code.
2. **Update `Cargo.toml`** (§1.3) — remove `reqwest` and `base64`; add new crates.
3. **Create `templates/email_booking.html`** (§6).
4. **Create `templates/pdf_invoice.typ`** (§7).
5. **Rewrite `src/config.rs`** (§4) — update the struct.
6. **Update `src/main.rs`** (§5) — replace bootstrap blocks and config construction.
7. **Update `src/consumers/booking.rs`** (§8) — update imports, add template struct, replace three functions.
8. **Compile check**: `cargo build` — fix any type errors.
9. **Update local `.env`** (§3) — swap env vars.
10. **Run locally** — trigger a test booking payment; verify email with PDF arrives.
11. **Update Render dashboard** (§11.2) — delete Gotenberg service, swap env vars on worker.
12. **Deploy** — push `tbd-worker` and trigger a Render deploy.
13. **Production smoke test** — trigger a real booking; verify email delivers correctly.
14. **Backend check** (§10) — confirm no Gotenberg-specific pings remain in `tbd-backend`.

---

## 13. `HTTP_TIMEOUT_SECS` — Remove

This variable controlled the `reqwest` client timeout, which no longer exists. Remove it from
`.env`, `.env.example`, and the Render environment. Lettre's built-in SMTP timeout (60s) is
appropriate. If you ever need to tune it, use `lettre::transport::smtp::SmtpTransportBuilder::timeout()`.

---

## 14. Summary of All Changed Files

| File | Change | What Changes |
|---|---|---|
| `tbd-worker/Cargo.toml` | Modified | Remove `reqwest`, `base64`; add `askama`, `typst`, `typst-pdf`, `typst-kit`, `comemo`, `lettre` |
| `tbd-worker/.env` | Modified | Remove 5 Resend/Gotenberg vars; add 5 Brevo vars |
| `tbd-worker/.env.example` | Modified | Same changes as `.env` |
| `tbd-worker/src/config.rs` | Rewritten | Replace HTTP/Resend/Gotenberg fields with Brevo SMTP + Lettre transport |
| `tbd-worker/src/main.rs` | Modified | Remove Gotenberg/Resend/reqwest bootstrap blocks; add Lettre SMTP construction |
| `tbd-worker/src/consumers/booking.rs` | Modified | Update imports; add `BookingEmailTemplate`; replace `generate_invoice_pdf`, `send_booking_email`; add `render_email_html` |
| `tbd-worker/templates/email_booking.html` | New file | Askama HTML template for email body |
| `tbd-worker/templates/pdf_invoice.typ` | New file | Typst template for PDF invoice |
| `tbd-backend/src/api/utils/tester.rs` | Verify only | Confirm no Gotenberg ping exists; worker keep-alive stays |
| `Render — Gotenberg service` | Delete | Free up one free-tier service slot |
| `Render — tbd-worker env` | Update | Swap 5 old vars for 5 new Brevo vars |

---

## 15. Current Implementation: Askama + Typst + Brevo HTTP REST API

> **Status: Implemented & Verified in `tbd-worker`**

### 15.1 Architectural Shift (Why HTTP REST API instead of Lettre SMTP)

During live deployment and testing on Render free tier, the worker encountered:
```
SMTP transient error: Connection error: connection timed out
```
**Root Cause**: Render free tier actively blocks outbound connections on standard SMTP ports (`25`, `465`, `587`) for abuse prevention.

**Solution**: Replaced the Lettre SMTP transport with **Brevo's transactional email REST API (v3)** via HTTP POST (`https://api.brevo.com/v3/smtp/email`).
- Communicates over standard HTTPS port `443` (always open across cloud platforms, including Render free tier).
- Uses `reqwest` with `rustls-tls` and `json`.
- PDF invoice generated in-process via Typst is Base64 encoded and attached directly in the JSON payload.

### 15.2 Active Stack & Dependencies (`Cargo.toml`)

- **HTML Templating**: `askama = "0.12"` (compile-time checked Jinja2-style templates)
- **PDF Generation**:
  - `typst = "0.15"`
  - `typst-as-lib = "0.16"`
  - `typst-pdf = "0.15"`
  - `derive_typst_intoval = "0.8"`
- **Email Delivery**:
  - `reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }`
  - `base64 = "0.22"`
  - *(Lettre was removed completely)*

### 15.3 Core Workflow in `src/consumers/booking.rs`

1. **`generate_invoice_pdf`**:
   - Uses `typst-as-lib::TypstEngine` in-memory.
   - Evaluates `templates/pdf_invoice.typ` with inputs bound via `derive_typst_intoval::IntoDict`.
   - Produces raw `Vec<u8>` PDF bytes directly in-process with zero network hops.
2. **`render_email_html`**:
   - Renders `templates/email_booking.html` via Askama compile-time template into a `String`.
3. **`send_booking_email`**:
   - Encodes PDF bytes into Base64 using `general_purpose::STANDARD.encode(&pdf_bytes)`.
   - Splits `config.email_from` into sender name and email.
   - Posts a JSON body to `https://api.brevo.com/v3/smtp/email` with header `api-key: <BREVO_API_KEY>`.
   - Maps 2xx to success, 4xx (except 429) to `ConsumerError::Permanent`, and 429/5xx/network drops to `ConsumerError::Transient` (triggering backoff retry).

### 15.4 Configuration & Environment

- **`WorkerConfig`** (`src/config.rs`):
  ```rust
  pub struct WorkerConfig {
      pub amqp_url: String,
      pub amqp_prefetch_count: u16,
      pub brevo_api_key: String,
      pub email_from: String,
      pub http_client: reqwest::Client,
      pub db: sqlx::PgPool,
  }
  ```
- **Environment Variables**:
  - `BREVO_API_KEY`: Brevo REST API key generated from **Brevo Dashboard → SMTP & API → API Keys** (starts with `xkeysib-...`, distinct from SMTP key).
  - `EMAIL_FROM`: Verified sender, e.g. `Turf BD <no-reply@turfbd.com>`.
  - Removed all `BREVO_SMTP_*` and `GOTENBERG_*` variables.

