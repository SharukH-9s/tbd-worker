/// Shared configuration passed to all consumer tasks.
/// Built once in main() and cheaply cloned into each spawned task.
/// `reqwest::Client` and `PgPool` both wrap `Arc` internally.
#[derive(Clone)]
pub struct WorkerConfig {
    /// CloudAMQP connection URL (amqps://...)
    pub amqp_url: String,

    /// AMQP prefetch count — how many unacked messages the broker sends at once.
    /// Override via AMQP_PREFETCH_COUNT (default: 1)
    pub amqp_prefetch_count: u16,

    /// Brevo API key — from Brevo dashboard → SMTP & API → API Keys tab.
    /// Used to call https://api.brevo.com/v3/smtp/email (HTTP, port 443).
    pub brevo_api_key: String,

    /// RFC 5321 "From" display address, e.g. "Turf BD <no-reply@turfbd.com>"
    /// Must be a verified sender in Brevo.
    pub email_from: String,

    /// Shared HTTP client — cheaply cloneable, reuses connection pool.
    pub http_client: reqwest::Client,

    /// Postgres connection pool — used for idempotency checks (processed_jobs table).
    pub db: sqlx::PgPool,
}
