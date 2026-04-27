//! Embedded DuckDB worker — gRPC [`ExecutionService`] implementation (M2+).
//!
//! PR 2 adds:
//!   * Per-transaction connection registry with identity binding
//!     (`transactions` module).
//!   * `IngestData` handler with `Appender`-backed staging temp tables
//!     (`ingest` module).
//!   * Typed-error classification on every DuckDB error path (`errors`
//!     module). Both the legacy `string error` and the new
//!     `typed_error` field on `ExecuteFragmentChunk` are populated.
//!   * Idle reaper that rolls back + closes abandoned transactions.

pub mod errors;
pub mod ingest;
pub mod transactions;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::ipc::writer::StreamWriter;
use duckdb::Connection;
use exec_proto::auth::validate_token;
use exec_proto::execute_fragment_error::Kind;
use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use exec_proto::openduck::v1::{
    ArrowIpcBatch, BeginTransactionReply, BeginTransactionRequest, CancelReply, CancelRequest,
    CommitTransactionReply, CommitTransactionRequest, ExecuteFragmentChunk, ExecuteFragmentRequest,
    HeartbeatReply, HeartbeatRequest, IngestChunk, IngestReply, RegisterWorkerReply,
    RollbackTransactionReply, RollbackTransactionRequest, WorkerRegistration,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::errors::{classify, error_chunk, typed, typed_from_message};
use crate::transactions::{
    identity_hex, identity_of, spawn_reaper, ConnectionRegistry, DEFAULT_IDLE_TIMEOUT,
};

/// Pinned DuckDB version for the worker and C++ extension.
///
/// Must be kept in sync with:
///   - `extensions/openduck/DUCKDB_VERSION` (the git-submodule pin used
///     by the C++ extension build)
///   - the `duckdb` / `libduckdb-sys` crate version in
///     `crates/exec-worker/Cargo.toml` (the bundled Rust runtime).
///
/// The `duckdb` crate's version scheme is `1.M{minor:02d}{patch:02d}.X`,
/// so e.g. `1.10502.X` bundles DuckDB v1.5.2.
pub const DUCKDB_SEMVER: &str = "1.5.2";

#[derive(Default)]
struct WorkerState {
    cancellations: Mutex<HashMap<String, Arc<AtomicBool>>>,
    transactions: Arc<ConnectionRegistry>,
}

/// How the worker connects DuckDB to differential storage.
#[derive(Clone, Default)]
pub enum StorageMode {
    /// No differential storage — use db_path or in-memory (current default).
    #[default]
    Direct,
    /// Mount via FUSE, open the mounted path. The caller must ensure
    /// `openduck-fuse` is already running at the given mountpoint.
    /// Linux only.
    Fuse { mountpoint: PathBuf },
    /// In-process persistent storage at `data_dir/{db_name}.duckdb`.
    /// Cross-platform, no FUSE mount required. The worker opens a real
    /// DuckDB file at a deterministic path under `data_dir`.
    InProcess {
        db_name: String,
        postgres_url: String,
        data_dir: PathBuf,
    },
}

impl std::fmt::Debug for StorageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Direct => write!(f, "Direct"),
            Self::Fuse { mountpoint } => f
                .debug_struct("Fuse")
                .field("mountpoint", mountpoint)
                .finish(),
            Self::InProcess {
                db_name, data_dir, ..
            } => f
                .debug_struct("InProcess")
                .field("db_name", db_name)
                .field("postgres_url", &"[REDACTED]")
                .field("data_dir", data_dir)
                .finish(),
        }
    }
}

/// Worker configuration.
#[derive(Clone, Default)]
pub struct WorkerConfig {
    /// Database file path (used by `StorageMode::Direct`).
    pub db_path: Option<PathBuf>,
    /// DuckLake metadata connection string (e.g. `postgres://user:pass@host/ducklake_meta`).
    pub ducklake_metadata: Option<String>,
    /// DuckLake data path (e.g. `s3://bucket/prefix/`).
    pub ducklake_data_path: Option<String>,
    /// How the worker accesses differential storage.
    pub storage: StorageMode,
    /// Unique worker id (defaults to a random UUID at startup).
    pub worker_id: String,
    /// Databases this worker can serve (empty = any).
    pub databases: Vec<String>,
    /// Opaque compute context for affinity routing (e.g. "region=us-east-1").
    pub compute_context: String,
    /// Max concurrent executions (0 = unlimited).
    pub max_concurrency: u32,
    /// Tables this worker is authoritative for (used for planner co-location).
    pub tables: Vec<String>,
    /// Per-transaction idle timeout for the connection-registry reaper.
    /// `None` = use [`DEFAULT_IDLE_TIMEOUT`].
    pub transaction_idle_timeout: Option<Duration>,
}

#[derive(Clone)]
pub struct WorkerService {
    state: Arc<WorkerState>,
    config: WorkerConfig,
}

impl WorkerService {
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            state: Arc::new(WorkerState::default()),
            config,
        }
    }

    /// Exposed for tests that want to observe the live connection
    /// registry (e.g. assert registry size returns to zero).
    #[doc(hidden)]
    pub fn transaction_registry(&self) -> Arc<ConnectionRegistry> {
        self.state.transactions.clone()
    }
}

impl Default for WorkerService {
    fn default() -> Self {
        Self::new(WorkerConfig::default())
    }
}

pub(crate) fn open_connection(config: &WorkerConfig) -> Result<Connection, String> {
    let conn = match &config.storage {
        StorageMode::Direct => match &config.db_path {
            Some(p) => Connection::open(p).map_err(|e| e.to_string())?,
            None => Connection::open_in_memory().map_err(|e| e.to_string())?,
        },
        StorageMode::Fuse { mountpoint } => {
            let db_file = mountpoint.join("database.duckdb");
            if !db_file.exists() {
                return Err(format!(
                    "FUSE mount not ready: {} does not exist. \
                     Ensure openduck-fuse is running at {}",
                    db_file.display(),
                    mountpoint.display()
                ));
            }
            Connection::open(&db_file).map_err(|e| e.to_string())?
        }
        StorageMode::InProcess {
            db_name,
            postgres_url: _,
            data_dir,
        } => {
            if db_name.contains('/') || db_name.contains('\\') || db_name.contains("..") {
                return Err(format!(
                    "db_name contains path separators or '..': {db_name}"
                ));
            }
            std::fs::create_dir_all(data_dir)
                .map_err(|e| format!("create data_dir {}: {e}", data_dir.display()))?;
            let db_file = data_dir.join(format!("{db_name}.duckdb"));
            let is_new = !db_file.exists();
            let conn = Connection::open(&db_file).map_err(|e| e.to_string())?;
            tracing::info!(
                path = %db_file.display(),
                is_new,
                "InProcess storage"
            );
            conn
        }
    };

    if let (Some(meta_url), Some(data_path)) =
        (&config.ducklake_metadata, &config.ducklake_data_path)
    {
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .map_err(|e| format!("ducklake install/load: {e}"))?;
        let safe_data = data_path.replace('\'', "''");
        let safe_meta = meta_url.replace('\'', "''");
        let attach_sql = format!(
            "ATTACH 'ducklake:lake' (DATA_PATH '{safe_data}', METADATA_PATH '{safe_meta}');"
        );
        conn.execute_batch(&attach_sql)
            .map_err(|e| format!("ducklake attach: {e}"))?;
    }

    Ok(conn)
}

/// Max number of record batches to coalesce into a single Arrow IPC message.
/// Amortises the per-message schema header overhead while keeping memory bounded.
const BATCHES_PER_IPC_MESSAGE: usize = 8;

/// Execute `sql` on the given DuckDB connection, streaming result
/// batches back through `tx` as Arrow IPC payloads.
fn run_sql_arrow_batches(
    conn: &Connection,
    sql: &str,
    execution_id: &str,
    cancel: Arc<AtomicBool>,
    tx: mpsc::Sender<Result<ExecuteFragmentChunk, Status>>,
) -> Result<(), String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let arrow = stmt.query_arrow([]).map_err(|e| e.to_string())?;
    let schema_ref = arrow.get_schema();
    let handle = tokio::runtime::Handle::current();

    let mut pending: Vec<arrow::record_batch::RecordBatch> = Vec::new();
    let mut sent_any = false;

    for batch in arrow {
        if cancel.load(Ordering::Relaxed) {
            return Err(format!("execution cancelled: {execution_id}"));
        }
        pending.push(batch);

        if pending.len() >= BATCHES_PER_IPC_MESSAGE {
            let buf = encode_ipc_batch(&schema_ref, &pending)?;
            pending.clear();
            sent_any = true;
            let chunk = ExecuteFragmentChunk {
                payload: Some(Payload::ArrowBatch(ArrowIpcBatch {
                    ipc_stream_payload: buf,
                })),
                typed_error: None,
            };
            if handle.block_on(tx.send(Ok(chunk))).is_err() {
                return Ok(());
            }
        }
    }

    if !pending.is_empty() {
        let buf = encode_ipc_batch(&schema_ref, &pending)?;
        sent_any = true;
        let chunk = ExecuteFragmentChunk {
            payload: Some(Payload::ArrowBatch(ArrowIpcBatch {
                ipc_stream_payload: buf,
            })),
            typed_error: None,
        };
        if handle.block_on(tx.send(Ok(chunk))).is_err() {
            return Ok(());
        }
    }

    if !sent_any {
        let buf = encode_ipc_batch(&schema_ref, &[])?;
        let chunk = ExecuteFragmentChunk {
            payload: Some(Payload::ArrowBatch(ArrowIpcBatch {
                ipc_stream_payload: buf,
            })),
            typed_error: None,
        };
        let _ = handle.block_on(tx.send(Ok(chunk)));
    }

    Ok(())
}

fn encode_ipc_batch(
    schema: &arrow::datatypes::Schema,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema).map_err(|e| e.to_string())?;
    for batch in batches {
        writer.write(batch).map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;
    Ok(buf)
}

#[tonic::async_trait]
impl ExecutionService for WorkerService {
    type ExecuteFragmentStream =
        Pin<Box<dyn Stream<Item = Result<ExecuteFragmentChunk, Status>> + Send>>;

    async fn execute_fragment(
        &self,
        request: Request<ExecuteFragmentRequest>,
    ) -> Result<Response<Self::ExecuteFragmentStream>, Status> {
        let mut inner = request.into_inner();
        validate_token(&inner.access_token)?;

        if inner.execution_id.is_empty() {
            inner.execution_id = Uuid::new_v4().to_string();
        }
        let execution_id = inner.execution_id.clone();
        let sql = String::from_utf8(inner.plan).map_err(|_| {
            Status::invalid_argument("plan must be UTF-8 SQL for this worker build")
        })?;
        let transaction_id = inner.transaction_id.filter(|s| !s.is_empty());

        tracing::info!(
            execution_id = %execution_id,
            database = %inner.database,
            transaction_id = transaction_id.as_deref().unwrap_or(""),
            "worker received execute_fragment"
        );
        tracing::debug!(
            execution_id = %execution_id,
            sql = %sql,
            "worker query detail"
        );

        let (tx, rx) = mpsc::channel::<Result<ExecuteFragmentChunk, Status>>(16);
        let cancel = Arc::new(AtomicBool::new(false));
        {
            let mut guard = self
                .state
                .cancellations
                .lock()
                .map_err(|_| Status::internal("cancel lock poisoned"))?;
            guard.insert(execution_id.clone(), cancel.clone());
        }

        let state = self.state.clone();
        let config = self.config.clone();
        let identity = identity_of(&inner.access_token);
        let database = inner.database.clone();
        tokio::spawn(async move {
            let tx_for_worker = tx.clone();
            let execution_id_for_worker = execution_id.clone();
            // Blocking closures return typed errors so the `Kind`
            // chosen by the registry / open_connection paths survives
            // into the reply chunk — classification-by-prefix is only
            // used where no typed origin exists (run_sql_arrow_batches
            // surfaces raw DuckDB error strings).
            let res: Result<Result<(), exec_proto::ExecuteFragmentError>, _> = match transaction_id
                .clone()
            {
                Some(tx_id) => {
                    let registry = state.transactions.clone();
                    let sql_owned = sql.clone();
                    let cancel_clone = cancel.clone();
                    tokio::task::spawn_blocking(move || {
                        let entry_arc = registry.acquire(&tx_id, &identity, &database)?;
                        let mut entry = entry_arc.lock().map_err(|_| {
                            typed(Kind::Internal, "transaction entry lock poisoned")
                        })?;
                        entry.touch();
                        let result = run_sql_arrow_batches(
                            &entry.connection,
                            &sql_owned,
                            &execution_id_for_worker,
                            cancel_clone,
                            tx_for_worker,
                        )
                        .map_err(typed_from_message);
                        entry.touch();
                        result
                    })
                    .await
                }
                None => {
                    let sql_owned = sql.clone();
                    let config_clone = config.clone();
                    let cancel_clone = cancel.clone();
                    tokio::task::spawn_blocking(move || {
                        let conn = open_connection(&config_clone).map_err(typed_from_message)?;
                        run_sql_arrow_batches(
                            &conn,
                            &sql_owned,
                            &execution_id_for_worker,
                            cancel_clone,
                            tx_for_worker,
                        )
                        .map_err(typed_from_message)
                    })
                    .await
                }
            };

            match res {
                Ok(Ok(())) => {
                    tracing::info!(execution_id = %execution_id, "worker execution completed");
                }
                Ok(Err(err)) => {
                    tracing::error!(execution_id = %execution_id, error = %err.message, "worker execution failed");
                    let _ = tx.send(Ok(error_chunk(err))).await;
                }
                Err(e) => {
                    tracing::error!(execution_id = %execution_id, error = %e, "worker task join error");
                    let _ = tx
                        .send(Ok(error_chunk(typed(
                            Kind::Internal,
                            format!("worker join error: {e}"),
                        ))))
                        .await;
                }
            }
            if let Ok(mut guard) = state.cancellations.lock() {
                guard.remove(&execution_id);
            }
            let _ = tx
                .send(Ok(ExecuteFragmentChunk {
                    payload: Some(Payload::Finished(true)),
                    typed_error: None,
                }))
                .await;
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::ExecuteFragmentStream
        ))
    }

    async fn cancel_execution(
        &self,
        request: Request<CancelRequest>,
    ) -> Result<Response<CancelReply>, Status> {
        let cancel = request.into_inner();
        validate_token(&cancel.access_token)?;
        let id = cancel.execution_id;
        tracing::info!(execution_id = %id, "worker received cancel_execution");
        if id.is_empty() {
            return Ok(Response::new(CancelReply {
                acknowledged: false,
            }));
        }
        let cancelled = if let Ok(guard) = self.state.cancellations.lock() {
            if let Some(flag) = guard.get(&id) {
                flag.store(true, Ordering::Relaxed);
                true
            } else {
                false
            }
        } else {
            false
        };
        Ok(Response::new(CancelReply {
            acknowledged: cancelled,
        }))
    }

    async fn register_worker(
        &self,
        request: Request<WorkerRegistration>,
    ) -> Result<Response<RegisterWorkerReply>, Status> {
        let reg = request.into_inner();
        validate_token(&reg.access_token)?;
        tracing::info!(
            worker_id = %reg.worker_id,
            endpoint = %reg.endpoint,
            databases = ?reg.databases,
            "register_worker received (forwarded)"
        );
        Ok(Response::new(RegisterWorkerReply { accepted: true }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let hb = request.into_inner();
        validate_token(&hb.access_token)?;
        Ok(Response::new(HeartbeatReply {
            acknowledged: !hb.worker_id.is_empty(),
        }))
    }

    // ── Transactions / IngestData ───────────────────────────────────────

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionReply>, Status> {
        let req = request.into_inner();
        validate_token(&req.access_token)?;
        let identity = identity_of(&req.access_token);
        let database = req.database.clone();
        let config = self.config.clone();
        let registry = self.state.transactions.clone();

        // Identity-binding order (§5.2): open → BEGIN → only then register.
        // A failure in open or BEGIN MUST NOT leave a dangling registry entry.
        let outcome = tokio::task::spawn_blocking(move || {
            let conn = open_connection(&config).map_err(typed_from_message)?;
            conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
                let msg = e.to_string();
                typed(classify(&msg), msg)
            })?;
            Ok::<_, exec_proto::ExecuteFragmentError>(registry.insert(conn, identity, database))
        })
        .await;

        match outcome {
            Ok(Ok(tx_id)) => {
                tracing::info!(
                    transaction_id = %tx_id,
                    identity_hash = %identity_hex(&identity),
                    database = %req.database,
                    "worker opened transaction"
                );
                Ok(Response::new(BeginTransactionReply {
                    transaction_id: tx_id,
                    typed_error: None,
                }))
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    identity_hash = %identity_hex(&identity),
                    database = %req.database,
                    error = %err.message,
                    "BeginTransaction failed"
                );
                Ok(Response::new(BeginTransactionReply {
                    transaction_id: String::new(),
                    typed_error: Some(err),
                }))
            }
            Err(join) => Ok(Response::new(BeginTransactionReply {
                transaction_id: String::new(),
                typed_error: Some(typed(Kind::Internal, format!("worker join error: {join}"))),
            })),
        }
    }

    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionReply>, Status> {
        let req = request.into_inner();
        validate_token(&req.access_token)?;
        let identity = identity_of(&req.access_token);
        let registry = self.state.transactions.clone();
        let tx_id = req.transaction_id.clone();

        let outcome = tokio::task::spawn_blocking(move || registry.commit(&tx_id, &identity)).await;

        let typed_error = match outcome {
            Ok(Ok(())) => {
                tracing::info!(
                    transaction_id = %req.transaction_id,
                    identity_hash = %identity_hex(&identity),
                    "worker committed transaction"
                );
                None
            }
            Ok(Err(err)) => Some(err),
            Err(join) => Some(typed(Kind::Internal, format!("worker join error: {join}"))),
        };

        Ok(Response::new(CommitTransactionReply { typed_error }))
    }

    async fn rollback_transaction(
        &self,
        request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionReply>, Status> {
        let req = request.into_inner();
        validate_token(&req.access_token)?;
        let identity = identity_of(&req.access_token);
        let registry = self.state.transactions.clone();
        let tx_id = req.transaction_id.clone();

        let outcome =
            tokio::task::spawn_blocking(move || registry.rollback(&tx_id, &identity)).await;

        let typed_error = match outcome {
            Ok(Ok(())) => {
                tracing::info!(
                    transaction_id = %req.transaction_id,
                    identity_hash = %identity_hex(&identity),
                    "worker rolled back transaction"
                );
                None
            }
            Ok(Err(err)) => Some(err),
            Err(join) => Some(typed(Kind::Internal, format!("worker join error: {join}"))),
        };

        Ok(Response::new(RollbackTransactionReply { typed_error }))
    }

    async fn ingest_data(
        &self,
        request: Request<tonic::Streaming<IngestChunk>>,
    ) -> Result<Response<IngestReply>, Status> {
        ingest::handle_ingest_data(
            request,
            self.state.transactions.clone(),
            self.config.clone(),
        )
        .await
    }
}

/// Listen and serve the worker gRPC service (used by tests and CLI).
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    serve_with_config(addr, WorkerConfig::default()).await
}

/// Test-only: expose `open_connection` for integration tests.
#[doc(hidden)]
pub fn __test_open_connection(config: &WorkerConfig) -> Result<(), String> {
    let _conn = open_connection(config)?;
    Ok(())
}

/// Serve with explicit config (for workers backed by a real DB file).
pub async fn serve_with_config(
    addr: SocketAddr,
    config: WorkerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    serve_with_shutdown(addr, config, None).await
}

/// Serve with explicit config and optional graceful shutdown signal.
pub async fn serve_with_shutdown(
    addr: SocketAddr,
    config: WorkerConfig,
    shutdown: Option<tokio::sync::watch::Receiver<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    const MAX_MSG_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
    let idle_timeout = config
        .transaction_idle_timeout
        .unwrap_or(DEFAULT_IDLE_TIMEOUT);
    let service = WorkerService::new(config);
    let registry = service.state.transactions.clone();
    let svc = ExecutionServiceServer::new(service)
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE);

    // Idle reaper (§5.2): rolls back + closes transactions whose pinned
    // connection has been idle for longer than `idle_timeout`.
    let reaper = spawn_reaper(registry, idle_timeout, shutdown.clone());

    tracing::info!(%addr, ?idle_timeout, "openduck-worker listening");
    let serve_result = if let Some(mut rx) = shutdown {
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
            })
            .await
    } else {
        Server::builder().add_service(svc).serve(addr).await
    };
    reaper.abort();
    serve_result?;
    Ok(())
}
