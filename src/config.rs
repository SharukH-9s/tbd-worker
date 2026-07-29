/// Shared configuration passed to all consumer tasks.
/// Built once in main() and cloned into each spawned task.
#[derive(Clone)]
pub struct WorkerConfig {
    /// CloudAMQP connection URL (amqps://...)
    pub amqp_url: String,

    /// Resend API key for sending transactional emails
    pub resend_api_key: String,

    /// Gotenberg base URL (e.g. http://gotenberg:3000)
    pub gotenberg_url: String,

    /// Shared HTTP client — cheaply cloneable, reuses connection pool
    pub http_client: reqwest::Client,

    /// Postgres connection pool — used for idempotency checks (processed_jobs table)
    /// PgPool is an Arc internally, so cloning is cheap and shares the same pool.
    pub db: sqlx::PgPool,
}
