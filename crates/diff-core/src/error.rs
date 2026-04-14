use thiserror::Error;

#[derive(Debug, Error)]
pub enum DiffCoreError {
    #[error("snapshot not found")]
    SnapshotNotFound,
    #[error("read beyond logical EOF")]
    Eof,
    #[error("seal requires single-threaded exclusive writer (reentrancy or lock held)")]
    Busy,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage: {0}")]
    Storage(String),
}
