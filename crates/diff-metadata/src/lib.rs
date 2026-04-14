//! PostgreSQL metadata repository for differential storage.

mod error;
pub mod gc;
mod pg;
mod pg_storage;

pub use error::MetadataError;
pub use pg::{release_write_lease, renew_write_lease, try_acquire_write_lease, PostgresMetadata};
pub use pg_storage::{bootstrap_openduck_database, BlobStore, PgStorageBackend};

use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct DatabaseRow {
    pub id: Uuid,
    pub name: String,
    pub current_tip_snapshot_id: Option<Uuid>,
}

#[async_trait::async_trait]
pub trait MetadataRepository: Send + Sync {
    async fn insert_db(&self, id: Uuid, name: &str) -> Result<(), MetadataError>;
    async fn get_db_by_name(&self, name: &str) -> Result<Option<DatabaseRow>, MetadataError>;
}
