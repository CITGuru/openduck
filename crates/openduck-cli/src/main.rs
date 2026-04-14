use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use tracing_subscriber::EnvFilter;

#[derive(Clone, Debug, ValueEnum)]
enum StorageModeArg {
    /// No differential storage — local file or in-memory
    Direct,
    /// FUSE mount (Linux only) — requires running openduck-fuse
    Fuse,
    /// In-process StorageBackend via DuckDB FileSystem (cross-platform, experimental)
    InProcess,
}

#[derive(Parser)]
#[command(name = "openduck", version, about = "OpenDuck database service")]
struct Cli {
    /// Database name or file path
    #[arg(short = 'd', long)]
    db: Option<String>,

    /// Access token (overrides OPENDUCK_TOKEN)
    #[arg(short = 'p', long)]
    token: Option<String>,

    /// Listen address for the gateway
    #[arg(short = 'l', long, default_value = "0.0.0.0:7878")]
    listen: String,

    /// Internal worker listen address
    #[arg(long, default_value = "127.0.0.1:9898")]
    worker_listen: String,

    /// Storage mode: direct (default), fuse, or in-process
    #[arg(long, value_enum, default_value_t = StorageModeArg::Direct)]
    storage: StorageModeArg,

    /// FUSE mountpoint (required for --storage fuse)
    #[arg(long)]
    mountpoint: Option<String>,

    /// Postgres URL for differential storage metadata (used by fuse / in-process)
    #[arg(long, env = "DATABASE_URL")]
    postgres: Option<String>,

    /// Data directory for differential storage layers (used by fuse / in-process)
    #[arg(long)]
    data_dir: Option<String>,

    /// DuckLake metadata connection string
    #[arg(long)]
    ducklake_metadata: Option<String>,

    /// DuckLake data path (e.g. s3://bucket/prefix/)
    #[arg(long)]
    ducklake_data: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start only the gateway (connect to external workers)
    Gateway {
        /// Listen address
        #[arg(short = 'l', long, default_value = "0.0.0.0:7878")]
        listen: String,

        /// Comma-separated worker addresses
        #[arg(
            long,
            env = "OPENDUCK_WORKER_ADDRS",
            default_value = "http://127.0.0.1:9898"
        )]
        workers: String,
    },

    /// Start only a worker
    Worker {
        /// Listen address
        #[arg(short = 'l', long, default_value = "127.0.0.1:9898")]
        listen: String,

        /// Database file path (omit for in-memory)
        #[arg(short = 'd', long)]
        db: Option<String>,

        /// Storage mode: direct (default), fuse, or in-process
        #[arg(long, value_enum, default_value_t = StorageModeArg::Direct)]
        storage: StorageModeArg,

        /// FUSE mountpoint (required for --storage fuse)
        #[arg(long)]
        mountpoint: Option<String>,

        /// Postgres URL for differential storage metadata
        #[arg(long, env = "DATABASE_URL")]
        postgres: Option<String>,

        /// Data directory for differential storage layers
        #[arg(long)]
        data_dir: Option<String>,

        /// DuckLake metadata connection string
        #[arg(long)]
        ducklake_metadata: Option<String>,

        /// DuckLake data path
        #[arg(long)]
        ducklake_data: Option<String>,
    },
}

fn resolve_storage_mode(
    mode: &StorageModeArg,
    db: &Option<String>,
    mountpoint: &Option<String>,
    postgres: &Option<String>,
    data_dir: &Option<String>,
) -> Result<exec_worker::StorageMode, String> {
    match mode {
        StorageModeArg::Direct => Ok(exec_worker::StorageMode::Direct),
        StorageModeArg::Fuse => {
            let mp = mountpoint
                .as_ref()
                .ok_or("--mountpoint is required for --storage fuse")?;
            Ok(exec_worker::StorageMode::Fuse {
                mountpoint: PathBuf::from(mp),
            })
        }
        StorageModeArg::InProcess => {
            let db_name = db
                .as_ref()
                .ok_or("--db is required for --storage in-process")?;
            let pg = postgres
                .as_ref()
                .ok_or("--postgres is required for --storage in-process")?;
            let dd = data_dir
                .as_ref()
                .ok_or("--data-dir is required for --storage in-process")?;
            Ok(exec_worker::StorageMode::InProcess {
                db_name: db_name.clone(),
                postgres_url: pg.clone(),
                data_dir: PathBuf::from(dd),
            })
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();

    if let Some(tok) = &cli.token {
        std::env::set_var("OPENDUCK_TOKEN", tok);
    }

    match cli.command {
        Some(Commands::Gateway { listen, workers }) => run_gateway(&listen, &workers).await,
        Some(Commands::Worker {
            listen,
            db,
            storage,
            mountpoint,
            postgres,
            data_dir,
            ducklake_metadata,
            ducklake_data,
        }) => {
            let mode = resolve_storage_mode(&storage, &db, &mountpoint, &postgres, &data_dir)
                .map_err(|e| e.to_string())?;
            run_worker(&listen, db, mode, ducklake_metadata, ducklake_data).await
        }
        None => run_serve(cli).await,
    }
}

/// Default mode: start worker + gateway in a single process.
async fn run_serve(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let worker_addr: SocketAddr = cli.worker_listen.parse()?;
    let gateway_addr: SocketAddr = cli.listen.parse()?;

    let storage = resolve_storage_mode(
        &cli.storage,
        &cli.db,
        &cli.mountpoint,
        &cli.postgres,
        &cli.data_dir,
    )
    .map_err(|e| e.to_string())?;

    let config = exec_worker::WorkerConfig {
        db_path: cli.db.map(PathBuf::from),
        ducklake_metadata: cli.ducklake_metadata,
        ducklake_data_path: cli.ducklake_data,
        storage,
    };

    tracing::info!(
        storage = ?config.storage,
        db = ?config.db_path.as_deref().unwrap_or("in-memory".as_ref()),
        duckdb = exec_worker::DUCKDB_SEMVER,
        worker = %worker_addr,
        gateway = %gateway_addr,
        "starting openduck"
    );

    let worker_handle = tokio::spawn(async move {
        if let Err(e) = exec_worker::serve_with_config(worker_addr, config).await {
            tracing::error!(%e, "worker exited");
        }
    });

    // Brief pause so the worker binds before the gateway tries to connect.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let worker_url = format!("http://{worker_addr}");
    let gateway_handle = tokio::spawn(async move {
        if let Err(e) = exec_gateway::serve(gateway_addr, vec![worker_url]).await {
            tracing::error!(%e, "gateway exited");
        }
    });

    tokio::select! {
        _ = worker_handle => {},
        _ = gateway_handle => {},
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
        }
    }

    Ok(())
}

async fn run_gateway(listen: &str, workers: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = listen.parse()?;
    let worker_list: Vec<String> = workers
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    tracing::info!(%addr, ?worker_list, "starting gateway");
    exec_gateway::serve(addr, worker_list).await?;
    Ok(())
}

async fn run_worker(
    listen: &str,
    db: Option<String>,
    storage: exec_worker::StorageMode,
    ducklake_metadata: Option<String>,
    ducklake_data: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = listen.parse()?;
    let config = exec_worker::WorkerConfig {
        db_path: db.map(PathBuf::from),
        ducklake_metadata,
        ducklake_data_path: ducklake_data,
        storage,
    };

    tracing::info!(
        %addr,
        storage = ?config.storage,
        db = ?config.db_path.as_deref().unwrap_or("in-memory".as_ref()),
        duckdb = exec_worker::DUCKDB_SEMVER,
        "starting worker"
    );

    exec_worker::serve_with_config(addr, config).await?;
    Ok(())
}
