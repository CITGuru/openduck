//! Requires `DATABASE_URL` and applied migrations (see CI).

use std::path::PathBuf;

use diff_core::{LogicalRange, ReadContext, StorageBackend};
use diff_metadata::{bootstrap_openduck_database, PgStorageBackend};
use sqlx::PgPool;
use tokio::runtime::Runtime;
use uuid::Uuid;

#[test]
fn pg_backend_write_seal_read_snapshot() {
    let url = match std::env::var("DATABASE_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("skip: DATABASE_URL not set");
            return;
        }
    };

    let rt = Runtime::new().expect("tokio runtime");
    let pool = rt.block_on(PgPool::connect(&url)).expect("connect");
    let name = format!("pgbe_{}", Uuid::new_v4());
    let data_dir = tempfile::tempdir().unwrap();
    let data_path: PathBuf = data_dir.path().to_path_buf();

    rt.block_on(bootstrap_openduck_database(&pool, &name, &data_path))
        .expect("bootstrap");

    let backend = PgStorageBackend::connect_rw(&url, &name, data_path.clone()).expect("connect_rw");
    backend.write(10, &[7, 8, 9]).expect("write");
    let snap = backend.seal().expect("seal");

    let old = backend
        .read(
            LogicalRange { offset: 10, len: 3 },
            ReadContext {
                snapshot_id: Some(snap),
            },
        )
        .expect("read snap");
    assert_eq!(&old[..], &[7, 8, 9]);

    backend.write(10, &[1, 1, 1]).expect("write after seal");
    let tip = backend
        .read(
            LogicalRange { offset: 10, len: 3 },
            ReadContext { snapshot_id: None },
        )
        .expect("read tip");
    assert_eq!(&tip[..], &[1, 1, 1]);

    let ro = PgStorageBackend::connect_ro(&url, &name, data_path, snap).expect("connect_ro");
    let frozen = ro
        .read(
            LogicalRange { offset: 10, len: 3 },
            ReadContext { snapshot_id: None },
        )
        .expect("ro read");
    assert_eq!(&frozen[..], &[7, 8, 9]);
}
