use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use diff_core::{SnapshotId, StorageBackend};
use diff_metadata::{
    bootstrap_openduck_database, try_acquire_write_lease, MetadataRepository, PgStorageBackend,
    PostgresMetadata,
};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "openduck-fuse", about = "Mount database.duckdb via FUSE (Linux)")]
struct Cli {
    #[arg(long)]
    db: String,

    #[arg(long, env = "DATABASE_URL")]
    postgres: String,

    #[arg(long)]
    data_dir: PathBuf,

    #[arg(long)]
    mountpoint: PathBuf,

    #[arg(long)]
    snapshot: Option<Uuid>,

    #[arg(long)]
    lease_holder: Option<String>,

    #[arg(long, default_value_t = 300)]
    lease_ttl_secs: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let pool = PgPool::connect(&cli.postgres).await?;
    bootstrap_openduck_database(&pool, &cli.db, &cli.data_dir).await?;

    if let Some(holder) = &cli.lease_holder {
        if cli.snapshot.is_none() {
            let meta = PostgresMetadata::new(pool.clone());
            let row = meta
                .get_db_by_name(&cli.db)
                .await?
                .ok_or("database row missing after bootstrap")?;
            if !try_acquire_write_lease(&pool, row.id, holder, cli.lease_ttl_secs).await? {
                return Err(
                    "write lease held by another process (or renew with same lease_holder)".into(),
                );
            }
        }
    }

    let backend: Arc<dyn StorageBackend> = if let Some(sid) = cli.snapshot {
        Arc::new(PgStorageBackend::connect_ro(
            &cli.postgres,
            &cli.db,
            cli.data_dir.clone(),
            SnapshotId(sid),
        )?)
    } else {
        Arc::new(PgStorageBackend::connect_rw(
            &cli.postgres,
            &cli.db,
            cli.data_dir.clone(),
        )?)
    };

    diff_fuse::mount_backend(backend, diff_fuse::MountOptions {
        mountpoint: &cli.mountpoint,
    })?;
    Ok(())
}
