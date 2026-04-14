//! Core types and in-memory differential storage backend.

mod error;
mod memory;

pub use error::DiffCoreError;
pub use memory::InMemoryBackend;

use bytes::Bytes;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LayerId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalRange {
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PhysicalLoc {
    pub layer_index: u32,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ReadContext {
    /// `None` = read the mutable tip (all sealed layers + active).
    pub snapshot_id: Option<SnapshotId>,
}

/// Storage presented as a single logical random-access file.
pub trait StorageBackend: Send + Sync {
    fn read(&self, logical: LogicalRange, ctx: ReadContext) -> Result<Bytes, DiffCoreError>;
    fn write(&self, logical_offset: u64, data: &[u8]) -> Result<(), DiffCoreError>;
    fn flush(&self) -> Result<(), DiffCoreError>;
    fn fsync(&self) -> Result<(), DiffCoreError>;
    /// Seal the active layer into a new snapshot; returns new snapshot id.
    fn seal(&self) -> Result<SnapshotId, DiffCoreError>;

    /// Shrink or grow the logical file if the engine issues `truncate` (default no-op).
    fn truncate(&self, _size: u64) -> Result<(), DiffCoreError> {
        Ok(())
    }
}
