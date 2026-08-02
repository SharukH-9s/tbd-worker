pub mod booking;

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

    let booking_channel = conn.create_channel().await?;

    tracing::info!("Worker: starting consumer on 'booking_jobs'");

    tokio::spawn(booking::consume_booking_jobs(booking_channel, config.clone())).await.unwrap();

    Ok(())
}
