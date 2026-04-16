//! Embedded DuckDB worker — gRPC [`ExecutionService`] implementation (M2).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::ipc::writer::StreamWriter;
use duckdb::Connection;
use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use exec_proto::openduck::v1::{
    ArrowIpcBatch, CancelReply, CancelRequest, ExecuteFragmentChunk, ExecuteFragmentRequest,
    HeartbeatReply, HeartbeatRequest, RegisterWorkerReply, WorkerRegistration,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use exec_proto::auth::validate_token;


/// Pinned DuckDB version for the worker and C++ extension (see `extensions/openduck/DUCKDB_VERSION`).
pub const DUCKDB_SEMVER: &str = "1.5.0";

#[derive(Default)]
struct WorkerState {
    cancellations: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

/// How the worker connects DuckDB to differential storage.
#[derive(Clone, Debug, Default)]
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
}

impl Default for WorkerService {
    fn default() -> Self {
        Self::new(WorkerConfig::default())
    }
}


fn open_connection(config: &WorkerConfig) -> Result<Connection, String> {
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

fn run_sql_arrow_batches(
    sql: &str,
    execution_id: &str,
    cancel: Arc<AtomicBool>,
    tx: mpsc::Sender<Result<ExecuteFragmentChunk, Status>>,
    config: &WorkerConfig,
) -> Result<(), String> {
    let conn = open_connection(config)?;
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let arrow = stmt.query_arrow([]).map_err(|e| e.to_string())?;
    let handle = tokio::runtime::Handle::current();

    let mut pending: Vec<arrow::record_batch::RecordBatch> = Vec::new();
    let mut schema_ref: Option<arrow::datatypes::SchemaRef> = None;

    for batch in arrow {
        if cancel.load(Ordering::Relaxed) {
            return Err(format!("execution cancelled: {execution_id}"));
        }
        if schema_ref.is_none() {
            schema_ref = Some(batch.schema());
        }
        pending.push(batch);

        if pending.len() >= BATCHES_PER_IPC_MESSAGE {
            let buf = encode_ipc_batch(schema_ref.as_ref().unwrap(), &pending)?;
            pending.clear();
            let chunk = ExecuteFragmentChunk {
                payload: Some(Payload::ArrowBatch(ArrowIpcBatch {
                    ipc_stream_payload: buf,
                })),
            };
            if handle.block_on(tx.send(Ok(chunk))).is_err() {
                return Ok(());
            }
        }
    }

    if !pending.is_empty() {
        let schema = schema_ref.as_ref().unwrap();
        let buf = encode_ipc_batch(schema, &pending)?;
        let chunk = ExecuteFragmentChunk {
            payload: Some(Payload::ArrowBatch(ArrowIpcBatch {
                ipc_stream_payload: buf,
            })),
        };
        if handle.block_on(tx.send(Ok(chunk))).is_err() {
            return Ok(());
        }
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
        tokio::spawn(async move {
            let tx_for_worker = tx.clone();
            let execution_id_for_worker = execution_id.clone();
            let res = tokio::task::spawn_blocking(move || {
                run_sql_arrow_batches(
                    &sql,
                    &execution_id_for_worker,
                    cancel,
                    tx_for_worker,
                    &config,
                )
            })
            .await;

            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = tx
                        .send(Ok(ExecuteFragmentChunk {
                            payload: Some(Payload::Error(e)),
                        }))
                        .await;
                }
                Err(e) => {
                    let _ = tx
                        .send(Ok(ExecuteFragmentChunk {
                            payload: Some(Payload::Error(format!("worker join error: {e}"))),
                        }))
                        .await;
                }
            }
            if let Ok(mut guard) = state.cancellations.lock() {
                guard.remove(&execution_id);
            }
            let _ = tx
                .send(Ok(ExecuteFragmentChunk {
                    payload: Some(Payload::Finished(true)),
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
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        Ok(Response::new(HeartbeatReply { acknowledged: true }))
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
    let svc = ExecutionServiceServer::new(WorkerService::new(config));
    tracing::info!(%addr, "openduck-worker listening");
    if let Some(mut rx) = shutdown {
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
            })
            .await?;
    } else {
        Server::builder().add_service(svc).serve(addr).await?;
    }
    Ok(())
}
