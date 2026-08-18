/// Shared configuration passed to all consumer tasks.
/// Built once in main() and cloned into each spawned task.
#[derive(Clone)]
pub struct WorkerConfig {
    /// CloudAMQP connection URL (amqps://...)
    pub amqp_url: String,

    /// AMQP prefetch count — how many unacked messages the broker sends at once.
    /// Override via AMQP_PREFETCH_COUNT (default: 1)
    pub amqp_prefetch_count: u16,

    /// Resend API key for sending transactional emails
    pub resend_api_key: String,

    /// Resend sender address — override via RESEND_FROM_EMAIL
    /// Default: "TBD <onboarding@resend.dev>"
    pub resend_from_email: String,

    /// Gotenberg base URL (e.g. https://gotenberg.onrender.com)
    pub gotenberg_url: String,

    /// Gotenberg Basic Auth username — set GOTENBERG_USER if your instance requires auth.
    pub gotenberg_user: Option<String>,

    /// Gotenberg Basic Auth password — set GOTENBERG_PASSWORD if your instance requires auth.
    pub gotenberg_password: Option<String>,

    /// Shared HTTP client — cheaply cloneable, reuses connection pool.
    /// Timeout controlled by HTTP_TIMEOUT_SECS (default: 60 s).
    pub http_client: reqwest::Client,

    /// Postgres connection pool — used for idempotency checks (processed_jobs table).
    pub db: sqlx::PgPool,
}
