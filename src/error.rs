#[derive(Debug)]
pub enum ConsumerError {
    /// Permanent failure — retrying will never fix this.
    /// e.g. 400 Bad Request, 401 Unauthorized, 403 Forbidden
    Permanent(String),

    /// Transient failure — retry is reasonable.
    /// e.g. 500 Server Error, 429 Rate Limited, network timeout
    Transient(anyhow::Error),
}
