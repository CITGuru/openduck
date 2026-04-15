//! OpenDuck execution worker — embedded DuckDB + gRPC `ExecutionService`.

use std::net::SocketAddr;
use std::path::PathBuf;

use exec_worker::WorkerConfig;
use tracing_subscriber::EnvFilter;

fn parse_addr() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let s = std::env::var("OPENDUCK_WORKER_LISTEN").unwrap_or_else(|_| "127.0.0.1:9898".into());
    Ok(s.parse()?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    if std::env::args().any(|a| a == "--smoke") {
        use duckdb::Connection;
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare("SELECT 1 AS x")?;
        let mut rows = stmt.query([])?;
        if let Some(r) = rows.next()? {
            let v: i32 = r.get(0)?;
            assert_eq!(v, 1);
        }
        tracing::info!(duckdb = exec_worker::DUCKDB_SEMVER, "smoke test passed");
        return Ok(());
    }

    let db_path = std::env::var("OPENDUCK_WORKER_DB")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);
    let ducklake_metadata = std::env::var("OPENDUCK_DUCKLAKE_METADATA")
        .ok()
        .filter(|s| !s.is_empty());
    let ducklake_data_path = std::env::var("OPENDUCK_DUCKLAKE_DATA")
        .ok()
        .filter(|s| !s.is_empty());
    let config = WorkerConfig {
        db_path,
        ducklake_metadata,
        ducklake_data_path,
        ..Default::default()
    };

    let addr = parse_addr()?;
    tracing::info!(
        duckdb = exec_worker::DUCKDB_SEMVER,
        db = ?config.db_path.as_deref().unwrap_or("in-memory".as_ref()),
        ducklake = if config.ducklake_metadata.is_some() { "configured" } else { "off" },
        "starting openduck-worker"
    );
    exec_worker::serve_with_config(addr, config).await?;
    Ok(())
}
