use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("database: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("not found")]
    NotFound,
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("runtime: {0}")]
    Runtime(String),
    #[error("layer fs: {0}")]
    LayerFs(String),
}
