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
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Pinned DuckDB version for the worker and C++ extension (see `extensions/openduck/DUCKDB_VERSION`).
pub const DUCKDB_SEMVER: &str = "1.5.0";

#[derive(Default)]
struct WorkerState {
    cancellations: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

/// How the worker connects DuckDB to differential storage.
#[derive(Clone, Debug)]
pub enum StorageMode {
    /// No differential storage — use db_path or in-memory (current default).
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

impl Default for StorageMode {
    fn default() -> Self {
        Self::Direct
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

/// Validate `request_token` against `OPENDUCK_TOKEN`.
/// Dev mode (unset/empty `OPENDUCK_TOKEN`) accepts any token.
fn validate_token(request_token: &str) -> Result<(), Status> {
    let expected = std::env::var("OPENDUCK_TOKEN").unwrap_or_default();
    if expected.is_empty() {
        return Ok(());
    }
    if request_token.is_empty() {
        return Err(Status::unauthenticated(
            "access_token required when OPENDUCK_TOKEN is set",
        ));
    }
    if constant_time_eq(request_token.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(Status::unauthenticated("invalid access_token"))
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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
            eprintln!(
                "info: InProcess storage — {} DuckDB at {}",
                if is_new { "created" } else { "opened" },
                db_file.display(),
            );
            conn
        }
    };

    if let (Some(meta_url), Some(data_path)) =
        (&config.ducklake_metadata, &config.ducklake_data_path)
    {
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .map_err(|e| format!("ducklake install/load: {e}"))?;
        let attach_sql = format!(
            "ATTACH 'ducklake:lake' (DATA_PATH '{data_path}', METADATA_PATH '{meta_url}');"
        );
        conn.execute_batch(&attach_sql)
            .map_err(|e| format!("ducklake attach: {e}"))?;
    }

    Ok(conn)
}

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

    for batch in arrow {
        if cancel.load(Ordering::Relaxed) {
            return Err(format!("execution cancelled: {execution_id}"));
        }
        let schema = batch.schema();
        let mut buf = Vec::new();
        {
            let mut w =
                StreamWriter::try_new(&mut buf, schema.as_ref()).map_err(|e| e.to_string())?;
            w.write(&batch).map_err(|e| e.to_string())?;
            w.finish().map_err(|e| e.to_string())?;
        }
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
        let id = request.into_inner().execution_id;
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
    let svc = ExecutionServiceServer::new(WorkerService::new(config));
    println!("openduck-worker gRPC on {addr}");
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}
