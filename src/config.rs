use lettre::{AsyncSmtpTransport, Tokio1Executor};

/// Shared configuration passed to all consumer tasks.
/// Built once in main() and cheaply cloned into each spawned task.
/// `AsyncSmtpTransport` and `PgPool` both wrap `Arc` internally.
#[derive(Clone)]
pub struct WorkerConfig {
    /// CloudAMQP connection URL (amqps://...)
    pub amqp_url: String,

    /// AMQP prefetch count — how many unacked messages the broker sends at once.
    /// Override via AMQP_PREFETCH_COUNT (default: 1)
    pub amqp_prefetch_count: u16,

    /// RFC 5321 "From" display address, e.g. "Turf BD <no-reply@turfbd.com>"
    /// Must be a verified sender in Brevo.
    pub email_from: String,

    /// Pre-built, cloneable Lettre SMTP transport.
    /// Connection pooling is handled internally; no reconnect logic needed.
    pub mailer: AsyncSmtpTransport<Tokio1Executor>,

    /// Postgres connection pool — used for idempotency checks (processed_jobs table).
    pub db: sqlx::PgPool,
}
