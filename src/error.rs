use thiserror::Error;

#[derive(Debug, Error)]
pub enum BustubxError {
    #[error("Not implement: {0}")]
    NotImplement(String),
}
