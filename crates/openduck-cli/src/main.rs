use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use diff_core::StorageBackend;
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

#[derive(Clone, Debug, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
    Csv,
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

    /// Enable hybrid execution (gateway splits LOCAL/REMOTE fragments)
    #[arg(long)]
    hybrid: bool,

    /// Max concurrent in-flight executions per gateway
    #[arg(long, default_value_t = 64)]
    max_in_flight: u32,

    /// Increase log verbosity (-v = debug, -vv = trace)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    verbosity: u8,

    /// Path to a TOML configuration file
    #[arg(long)]
    config: Option<PathBuf>,

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

        /// Gateway endpoint to self-register with on startup
        #[arg(long)]
        gateway: Option<String>,
    },

    /// Send a SQL query to a running gateway
    Query {
        /// SQL to execute
        sql: String,

        /// Gateway endpoint
        #[arg(long, default_value = "http://127.0.0.1:7878")]
        endpoint: String,

        /// Access token
        #[arg(long, env = "OPENDUCK_TOKEN")]
        token: Option<String>,

        /// Output format
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },

    /// Cancel a running execution
    Cancel {
        /// Execution ID to cancel
        execution_id: String,

        /// Gateway endpoint
        #[arg(long, default_value = "http://127.0.0.1:7878")]
        endpoint: String,

        /// Access token
        #[arg(long, env = "OPENDUCK_TOKEN")]
        token: Option<String>,
    },

    /// Check if the gateway is reachable
    Status {
        /// Gateway endpoint
        #[arg(long, default_value = "http://127.0.0.1:7878")]
        endpoint: String,

        /// Access token
        #[arg(long, env = "OPENDUCK_TOKEN")]
        token: Option<String>,
    },

    /// Differential storage snapshot operations
    Snapshot {
        #[command(subcommand)]
        action: SnapshotAction,
    },

    /// Garbage-collect unreferenced layers and compact extents
    Gc {
        /// Postgres URL for metadata
        #[arg(long, env = "DATABASE_URL")]
        postgres: String,

        /// Database name
        #[arg(long)]
        db: String,

        /// Data directory for storage layers (needed to delete segment files)
        #[arg(long)]
        data_dir: String,

        /// Dry-run: list candidates without deleting
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum SnapshotAction {
    /// Seal current data into an immutable snapshot
    Seal {
        /// Postgres URL for metadata
        #[arg(long, env = "DATABASE_URL")]
        postgres: String,

        /// Database name
        #[arg(long)]
        db: String,

        /// Data directory for storage layers
        #[arg(long)]
        data_dir: String,
    },

    /// List snapshots for a database
    List {
        /// Postgres URL for metadata
        #[arg(long, env = "DATABASE_URL")]
        postgres: String,

        /// Database name
        #[arg(long)]
        db: String,
    },
}

/// Optional TOML config file structure. CLI flags always take precedence.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct FileConfig {
    token: Option<String>,
    listen: Option<String>,
    worker_listen: Option<String>,
    hybrid: Option<bool>,
    max_in_flight: Option<u32>,
    postgres: Option<String>,
    data_dir: Option<String>,
    ducklake_metadata: Option<String>,
    ducklake_data: Option<String>,
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
    let mut cli = Cli::parse();

    // Load config file if provided, applying defaults for unset CLI fields.
    if let Some(cfg_path) = &cli.config {
        let contents =
            std::fs::read_to_string(cfg_path).map_err(|e| format!("read config: {e}"))?;
        let file_cfg: FileConfig =
            toml::from_str(&contents).map_err(|e| format!("parse config: {e}"))?;
        apply_file_config(&mut cli, &file_cfg);
    }

    let log_level = match cli.verbosity {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(log_level.parse()?))
        .init();

    // SAFETY: called before any worker threads are spawned; main thread only.
    if let Some(tok) = &cli.token {
        unsafe { std::env::set_var("OPENDUCK_TOKEN", tok) };
    }
    if cli.hybrid {
        unsafe { std::env::set_var("OPENDUCK_HYBRID", "1") };
    }
    if cli.max_in_flight != 64 {
        unsafe { std::env::set_var("OPENDUCK_MAX_IN_FLIGHT", cli.max_in_flight.to_string()) };
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
            gateway,
        }) => {
            let mode = resolve_storage_mode(&storage, &db, &mountpoint, &postgres, &data_dir)
                .map_err(|e| e.to_string())?;
            run_worker(&listen, db, mode, ducklake_metadata, ducklake_data, gateway).await
        }
        Some(Commands::Query {
            sql,
            endpoint,
            token,
            format,
        }) => run_query(&sql, &endpoint, token.as_deref().unwrap_or(""), &format).await,
        Some(Commands::Cancel {
            execution_id,
            endpoint,
            token,
        }) => run_cancel(&execution_id, &endpoint, token.as_deref().unwrap_or("")).await,
        Some(Commands::Status { endpoint, token }) => {
            run_status(&endpoint, token.as_deref().unwrap_or("")).await
        }
        Some(Commands::Snapshot { action }) => run_snapshot(action).await,
        Some(Commands::Gc {
            postgres,
            db,
            data_dir,
            dry_run,
        }) => run_gc(&postgres, &db, &data_dir, dry_run).await,
        None => run_serve(cli).await,
    }
}

/// Apply file config defaults for fields not set via CLI flags.
fn apply_file_config(cli: &mut Cli, cfg: &FileConfig) {
    if cli.token.is_none() {
        cli.token = cfg.token.clone();
    }
    if cli.postgres.is_none() {
        cli.postgres = cfg.postgres.clone();
    }
    if cli.data_dir.is_none() {
        cli.data_dir = cfg.data_dir.clone();
    }
    if cli.ducklake_metadata.is_none() {
        cli.ducklake_metadata = cfg.ducklake_metadata.clone();
    }
    if cli.ducklake_data.is_none() {
        cli.ducklake_data = cfg.ducklake_data.clone();
    }
    if !cli.hybrid {
        cli.hybrid = cfg.hybrid.unwrap_or(false);
    }
    if cli.max_in_flight == 64 {
        if let Some(n) = cfg.max_in_flight {
            cli.max_in_flight = n;
        }
    }
}

// ── Server modes ───────────────────────────────────────────────────────

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
        ..Default::default()
    };

    tracing::info!(
        storage = ?config.storage,
        db = ?config.db_path.as_deref().unwrap_or("in-memory".as_ref()),
        duckdb = exec_worker::DUCKDB_SEMVER,
        worker = %worker_addr,
        gateway = %gateway_addr,
        hybrid = cli.hybrid,
        max_in_flight = cli.max_in_flight,
        "starting openduck"
    );

    let (shutdown_tx, shutdown_rx_w) = tokio::sync::watch::channel(());
    let shutdown_rx_g = shutdown_tx.subscribe();

    let worker_handle = tokio::spawn(async move {
        if let Err(e) =
            exec_worker::serve_with_shutdown(worker_addr, config, Some(shutdown_rx_w)).await
        {
            tracing::error!(%e, "worker exited");
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let worker_url = format!("http://{worker_addr}");
    let gateway_handle = tokio::spawn(async move {
        if let Err(e) =
            exec_gateway::serve_with_shutdown(gateway_addr, vec![worker_url], Some(shutdown_rx_g))
                .await
        {
            tracing::error!(%e, "gateway exited");
        }
    });

    tokio::select! {
        _ = worker_handle => {},
        _ = gateway_handle => {},
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down — draining in-flight requests...");
            let _ = shutdown_tx.send(());
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
    gateway_endpoint: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = listen.parse()?;
    let config = exec_worker::WorkerConfig {
        db_path: db.map(PathBuf::from),
        ducklake_metadata,
        ducklake_data_path: ducklake_data,
        storage,
        ..Default::default()
    };

    tracing::info!(
        %addr,
        storage = ?config.storage,
        db = ?config.db_path.as_deref().unwrap_or("in-memory".as_ref()),
        duckdb = exec_worker::DUCKDB_SEMVER,
        "starting worker"
    );

    if let Some(gw) = gateway_endpoint {
        let worker_config = config.clone();
        let worker_addr = addr;
        tokio::spawn(async move {
            register_with_gateway(&gw, &worker_config, worker_addr).await;
        });
    }

    exec_worker::serve_with_config(addr, config).await?;
    Ok(())
}

/// Attempt to register this worker with the gateway, retrying with exponential
/// backoff, then send periodic heartbeats to keep the registration alive.
async fn register_with_gateway(
    gateway_url: &str,
    config: &exec_worker::WorkerConfig,
    listen_addr: SocketAddr,
) {
    use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
    use exec_proto::openduck::v1::{HeartbeatRequest, WorkerRegistration};

    let endpoint = format!("http://{listen_addr}");
    let worker_id = if config.worker_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        config.worker_id.clone()
    };

    let mut delay = std::time::Duration::from_millis(500);
    let max_delay = std::time::Duration::from_secs(30);

    let mut client = loop {
        tokio::time::sleep(delay).await;
        match ExecutionServiceClient::connect(gateway_url.to_string()).await {
            Ok(mut client) => {
                let reg = WorkerRegistration {
                    worker_id: worker_id.clone(),
                    endpoint: endpoint.clone(),
                    databases: config.databases.clone(),
                    compute_context: config.compute_context.clone(),
                    max_concurrency: config.max_concurrency,
                };
                match client.register_worker(tonic::Request::new(reg)).await {
                    Ok(reply) => {
                        if reply.into_inner().accepted {
                            tracing::info!(
                                gateway = gateway_url,
                                worker_id = %worker_id,
                                "registered with gateway"
                            );
                            break client;
                        } else {
                            tracing::warn!(gateway = gateway_url, "registration not accepted");
                            return;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(%e, "register_worker RPC failed, retrying...");
                    }
                }
            }
            Err(e) => {
                tracing::debug!(%e, "gateway not reachable, retrying...");
            }
        }
        delay = (delay * 2).min(max_delay);
    };

    let heartbeat_interval = std::time::Duration::from_secs(30);
    loop {
        tokio::time::sleep(heartbeat_interval).await;
        let req = HeartbeatRequest {
            worker_id: worker_id.clone(),
        };
        match client.heartbeat(tonic::Request::new(req)).await {
            Ok(reply) => {
                if !reply.into_inner().acknowledged {
                    tracing::warn!(
                        worker_id = %worker_id,
                        "heartbeat not acknowledged — re-registering"
                    );
                    Box::pin(register_with_gateway(gateway_url, config, listen_addr)).await;
                    return;
                }
                tracing::trace!(worker_id = %worker_id, "heartbeat acknowledged");
            }
            Err(e) => {
                tracing::warn!(%e, "heartbeat failed — gateway may be unreachable");
            }
        }
    }
}

// ── Client subcommands ─────────────────────────────────────────────────

async fn run_query(
    sql: &str,
    endpoint: &str,
    token: &str,
    format: &OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::ipc::reader::StreamReader;
    use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
    use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
    use exec_proto::openduck::v1::ExecuteFragmentRequest;

    let mut client = ExecutionServiceClient::connect(endpoint.to_string()).await?;
    let mut stream = client
        .execute_fragment(tonic::Request::new(ExecuteFragmentRequest {
            plan: sql.as_bytes().to_vec(),
            database: String::new(),
            snapshot_id: None,
            access_token: token.into(),
            execution_id: String::new(),
        }))
        .await?
        .into_inner();

    let mut all_batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();

    while let Some(chunk) = stream.message().await? {
        match chunk.payload {
            Some(Payload::ArrowBatch(b)) if !b.ipc_stream_payload.is_empty() => {
                let cursor = std::io::Cursor::new(b.ipc_stream_payload);
                let reader = StreamReader::try_new(cursor, None)?;
                for batch in reader {
                    all_batches.push(batch?);
                }
            }
            Some(Payload::Error(e)) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
            Some(Payload::Finished(true)) => break,
            _ => {}
        }
    }

    if all_batches.is_empty() {
        println!("(no results)");
        return Ok(());
    }

    match format {
        OutputFormat::Table => {
            arrow::util::pretty::print_batches(&all_batches)?;
        }
        OutputFormat::Json => {
            for batch in &all_batches {
                let buf = Vec::new();
                let mut writer = arrow::json::LineDelimitedWriter::new(buf);
                writer.write(batch)?;
                writer.finish()?;
                let bytes = writer.into_inner();
                print!("{}", String::from_utf8_lossy(&bytes));
            }
        }
        OutputFormat::Csv => {
            let mut first = true;
            for batch in &all_batches {
                let mut buf = Vec::new();
                {
                    let builder = arrow::csv::WriterBuilder::new().with_header(first);
                    let mut writer = builder.build(&mut buf);
                    writer.write(batch)?;
                }
                print!("{}", String::from_utf8_lossy(&buf));
                first = false;
            }
        }
    }

    Ok(())
}

async fn run_cancel(
    execution_id: &str,
    endpoint: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
    use exec_proto::openduck::v1::CancelRequest;

    let mut client = ExecutionServiceClient::connect(endpoint.to_string()).await?;
    let reply = client
        .cancel_execution(tonic::Request::new(CancelRequest {
            execution_id: execution_id.into(),
            access_token: token.into(),
        }))
        .await?
        .into_inner();

    if reply.acknowledged {
        println!("Cancelled execution {execution_id}");
    } else {
        println!("Execution {execution_id} not found or already completed");
    }
    Ok(())
}

async fn run_status(endpoint: &str, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
    use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
    use exec_proto::openduck::v1::ExecuteFragmentRequest;

    let start = Instant::now();
    let mut client = match ExecutionServiceClient::connect(endpoint.to_string()).await {
        Ok(c) => c,
        Err(e) => {
            println!("UNREACHABLE  {endpoint}  ({e})");
            std::process::exit(1);
        }
    };
    let connect_ms = start.elapsed().as_millis();

    let probe_start = Instant::now();
    let result = client
        .execute_fragment(tonic::Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1 AS health_check".to_vec(),
            database: String::new(),
            snapshot_id: None,
            access_token: token.into(),
            execution_id: String::new(),
        }))
        .await;

    match result {
        Ok(mut resp) => {
            let mut ok = false;
            let stream = resp.get_mut();
            while let Some(chunk) = stream.message().await? {
                match chunk.payload {
                    Some(Payload::Finished(true)) => {
                        ok = true;
                        break;
                    }
                    Some(Payload::Error(e)) => {
                        println!("UNHEALTHY  {endpoint}  error={e}");
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
            if ok {
                let query_ms = probe_start.elapsed().as_millis();
                println!("OK  {endpoint}  connect={connect_ms}ms  query={query_ms}ms");
            }
        }
        Err(e) => {
            println!("UNHEALTHY  {endpoint}  ({e})");
            std::process::exit(1);
        }
    }
    Ok(())
}

// ── Snapshot subcommands ───────────────────────────────────────────────

async fn run_snapshot(action: SnapshotAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        SnapshotAction::Seal {
            postgres,
            db,
            data_dir,
        } => {
            let backend = diff_metadata::PgStorageBackend::connect_rw(
                &postgres,
                &db,
                PathBuf::from(&data_dir),
            )
            .map_err(|e| format!("connect: {e}"))?;
            let snap_id = backend.seal().map_err(|e| format!("seal: {e}"))?;
            println!("{}", snap_id.0);
            Ok(())
        }
        SnapshotAction::List { postgres, db } => {
            let pool = sqlx::PgPool::connect(&postgres).await?;

            let db_row =
                sqlx::query_as::<_, (uuid::Uuid,)>("SELECT id FROM openduck_db WHERE name = $1")
                    .bind(&db)
                    .fetch_optional(&pool)
                    .await?;

            let Some((db_id,)) = db_row else {
                eprintln!("Database '{db}' not found");
                std::process::exit(1);
            };

            let rows = sqlx::query_as::<_, (uuid::Uuid, chrono::DateTime<chrono::Utc>)>(
                "SELECT id, created_at FROM openduck_snapshot \
                 WHERE database_id = $1 ORDER BY created_at DESC",
            )
            .bind(db_id)
            .fetch_all(&pool)
            .await?;

            if rows.is_empty() {
                println!("No snapshots for database '{db}'");
            } else {
                println!("{:<38}  CREATED AT", "SNAPSHOT ID");
                for (id, ts) in &rows {
                    println!("{id}  {ts}");
                }
            }
            Ok(())
        }
    }
}

// ── GC subcommand ──────────────────────────────────────────────────────

async fn run_gc(
    postgres: &str,
    db: &str,
    data_dir: &str,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::PgPool::connect(postgres).await?;

    let db_row = sqlx::query_as::<_, (uuid::Uuid,)>("SELECT id FROM openduck_db WHERE name = $1")
        .bind(db)
        .fetch_optional(&pool)
        .await?;

    let Some((db_id,)) = db_row else {
        tracing::error!(db, "database not found");
        std::process::exit(1);
    };

    let compacted = diff_metadata::gc::compact_extents(&pool, db_id).await?;
    if compacted > 0 {
        tracing::info!(compacted, "superseded extents marked");
    }

    let candidates = diff_metadata::gc::gc_candidates(&pool, db_id).await?;
    if candidates.is_empty() {
        println!("No GC candidates for database '{db}'");
        return Ok(());
    }

    println!(
        "{} GC candidate layer(s) for database '{db}':",
        candidates.len()
    );
    for layer in &candidates {
        println!("  {}  {}", layer.id, layer.storage_uri);
    }

    if dry_run {
        println!("(dry-run — no layers deleted)");
        return Ok(());
    }

    let data_path = PathBuf::from(data_dir);
    let mut deleted = 0u64;
    for layer in &candidates {
        let segment = data_path.join(&layer.storage_uri);
        if segment.exists() {
            if let Err(e) = std::fs::remove_file(&segment) {
                tracing::warn!(path = %segment.display(), %e, "failed to remove segment file");
                continue;
            }
        }
        diff_metadata::gc::delete_layer(&pool, layer.id).await?;
        deleted += 1;
        tracing::info!(layer_id = %layer.id, "deleted layer");
    }
    println!("Deleted {deleted}/{} layer(s)", candidates.len());

    Ok(())
}
