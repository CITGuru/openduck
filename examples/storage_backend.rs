//! # Postgres Storage Backend Example
//!
//! Demonstrates the differential storage pipeline:
//! bootstrap a database, write data, seal a snapshot, and read it back.
//!
//! Requires Postgres running (see docker/docker-compose.yml).
//!
//! ```bash
//! export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
//! cargo run --example storage_backend
//! ```

use diff_core::{LogicalRange, ReadContext, StorageBackend};
use diff_metadata::{bootstrap_openduck_database, PgStorageBackend};
use sqlx::PgPool;
use tokio::runtime::Runtime;
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://openduck:openduck@localhost:5433/openduck_meta".into());

    let rt = Runtime::new()?;
    let pool = rt.block_on(PgPool::connect(&url))?;
    let db_name = format!("example_{}", Uuid::new_v4());
    let data_dir = tempfile::tempdir()?;
    let data_path = data_dir.path().to_path_buf();

    println!("=> Bootstrapping database '{db_name}'");
    rt.block_on(bootstrap_openduck_database(&pool, &db_name, &data_path))?;
    drop(rt);

    let backend = PgStorageBackend::connect_rw(&url, &db_name, data_path.clone())?;

    let payload = b"Hello, OpenDuck!";
    println!("=> Writing {} bytes at offset 0", payload.len());
    backend.write(0, payload)?;
    backend.flush()?;

    let data = backend.read(
        LogicalRange {
            offset: 0,
            len: payload.len() as u64,
        },
        ReadContext { snapshot_id: None },
    )?;
    println!("=> Read back: {:?}", std::str::from_utf8(&data)?);

    let snap = backend.seal()?;
    println!("=> Sealed snapshot: {:?}", snap.0);

    drop(backend);

    let ro = PgStorageBackend::connect_ro(&url, &db_name, data_path, snap)?;
    let snap_data = ro.read(
        LogicalRange {
            offset: 0,
            len: payload.len() as u64,
        },
        ReadContext {
            snapshot_id: Some(snap),
        },
    )?;
    println!("=> Snapshot read: {:?}", std::str::from_utf8(&snap_data)?);
    assert_eq!(&*data, &*snap_data);
    println!("=> Data matches. Done.");

    Ok(())
}
