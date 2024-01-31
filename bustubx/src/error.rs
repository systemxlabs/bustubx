use thiserror::Error;

pub type BustubxResult<T, E = BustubxError> = Result<T, E>;

#[derive(Debug, Error)]
pub enum BustubxError {
    #[error("Not support: {0}")]
    NotSupport(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Plan error: {0}")]
    Plan(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parser error: {0}")]
    Parser(#[from] sqlparser::parser::ParserError),
}
