use thiserror::Error;

pub type BustubxResult<T, E = BustubxError> = Result<T, E>;

#[derive(Debug, Error)]
pub enum BustubxError {
    #[error("Not implement: {0}")]
    NotImplement(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
