mod config;
mod consumers;
pub mod error;

use dotenvy::dotenv;
use std::env;

#[tokio::main]
async fn main() {
    // Load .env file in local development. Silently ignored in production (Render).
    dotenv().ok();

    // ── Tracing Setup ─────────────────────────────────────────────────────────
    // EnvFilter reads RUST_LOG at runtime (defaults to "info" if not set).

    let (non_blocking_writer, _guard) = tracing_appender::non_blocking(std::io::stdout());
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let app_env = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());

    // Uses tracing_subscriber::fmt() convenience builder to construct a standalone FmtSubscriber.
    match app_env.as_str() {
        "production" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .with_writer(non_blocking_writer)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .compact()
                .with_env_filter(env_filter)
                .with_writer(non_blocking_writer)
                .init();
        }
    }

    tracing::info!(env = %app_env, "tbd-worker starting up");

    // ── Health Check Server (Render Free Tier Workaround) ─────────────────────
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await {
        Ok(listener) => {
            tracing::info!(port = %port, "Health check server bound successfully");
            tokio::spawn(async move {
                let app = axum::Router::new().route("/healthz", axum::routing::get(|| async { "OK" }));
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!(error = %e, "Health check server failed to run");
                }
            });
        }
        Err(e) => {
            tracing::error!(port = %port, error = %e, "Failed to bind health check server");
        }
    }


    // ── Read Required Config ───────────────────────────────────────────────
    let amqp_url = env::var("AMQP_URL")
        .expect("AMQP_URL must be set")
        .trim()
        .to_string();

    let resend_api_key = env::var("RESEND_API_KEY")
        .expect("RESEND_API_KEY must be set")
        .trim()
        .to_string();

    let gotenberg_url = env::var("GOTENBERG_URL")
        .expect("GOTENBERG_URL must be set")
        .trim()
        .trim_end_matches('/')
        .to_string();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set")
        .trim()
        .to_string();

    // ── Read Optional / Tunable Config ────────────────────────────────────
    let resend_from_email = env::var("RESEND_FROM_EMAIL")
        .unwrap_or_else(|_| "TBD <onboarding@resend.dev>".to_string());

    let gotenberg_user = env::var("GOTENBERG_USER").ok();
    let gotenberg_password = env::var("GOTENBERG_PASSWORD").ok();

    let amqp_prefetch_count: u16 = env::var("AMQP_PREFETCH_COUNT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(1);

    let http_timeout_secs: u64 = env::var("HTTP_TIMEOUT_SECS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(60);

    let max_connections: u32 = env::var("DATABASE_MAX_CONNECTIONS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(5);

    let acquire_timeout_secs: u64 = env::var("DATABASE_ACQUIRE_TIMEOUT_SECS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(3);

    // ── Connect to Neon (Postgres) with Pool Tuning ───────────────────────
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(std::time::Duration::from_secs(acquire_timeout_secs))
        .connect(&database_url)
        .await
        .expect("Failed to connect to Neon (DATABASE_URL)");

    tracing::info!("tbd-worker: connected to Neon (Postgres)");

    // ── Build Shared HTTP Client ───────────────────────────────────────────
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(http_timeout_secs))
        .build()
        .expect("Failed to build HTTP client");

    // ── Build Config ───────────────────────────────────────────────────────
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

    tracing::info!("tbd-worker config loaded — connecting to CloudAMQP...");

    // ── Start Consumer Loop ───────────────────────────────────────────────────
    // This runs forever, reconnecting on drop.
    consumers::run_consumers(worker_config).await;
}

