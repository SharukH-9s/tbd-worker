pub mod email;
pub mod pdf;

use crate::config::WorkerConfig;
use lapin::{Connection, ConnectionProperties};
use std::time::Duration;

/// Entry point for all consumers. Connects to CloudAMQP, then spawns
/// one task per queue. Loops on reconnect if the connection drops.
pub async fn run_consumers(config: WorkerConfig) {
    loop {
        match connect_and_subscribe(config.clone()).await {
            Ok(_) => tracing::warn!("Worker: AMQP connection closed — reconnecting in 5s..."),
            Err(e) => {
                tracing::error!(error = %e, "Worker: connection error — reconnecting in 5s...")
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await; // 5s delay before reconnecting
    }
}

async fn connect_and_subscribe(config: WorkerConfig) -> Result<(), lapin::Error> {
    let conn = Connection::connect(&config.amqp_url, ConnectionProperties::default()).await?;
    tracing::info!("Worker: connected to CloudAMQP");

    // Each consumer gets its own AMQP channel.
    // Channels are lightweight and independent — a failure on one does not affect the other.
    let email_channel = conn.create_channel().await?;
    let pdf_channel = conn.create_channel().await?;

    tracing::info!("Worker: starting consumers on 'email_jobs' and 'pdf_jobs'");

    // Spawn both consumers concurrently and wait for either to exit.
    // tokio::join! runs both futures in parallel on the same task.
    tokio::join!(
        email::consume_email_jobs(email_channel, config.clone()),
        pdf::consume_pdf_jobs(pdf_channel, config.clone()),
    );

    Ok(())
}
