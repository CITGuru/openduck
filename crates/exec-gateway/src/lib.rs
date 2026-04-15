//! OpenDuck execution gateway — authenticates and forwards `ExecuteFragment` to workers.
//!
//! When `OPENDUCK_HYBRID=1`, the gateway inspects incoming SQL for
//! `openduck_run('REMOTE', '...')` / `openduck_run('LOCAL', '...')` hints,
//! extracts the inner SQL, and logs an annotated hybrid plan.

pub mod hybrid;

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, sync::Mutex};

use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use exec_proto::openduck::v1::{
    CancelReply, CancelRequest, ExecuteFragmentChunk, ExecuteFragmentRequest, HeartbeatReply,
    HeartbeatRequest, RegisterWorkerReply, WorkerRegistration,
};
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Max concurrent fragment executions per gateway process (backpressure, M4).
pub fn max_in_flight() -> usize {
    std::env::var("OPENDUCK_MAX_IN_FLIGHT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64)
}

/// Whether hybrid planning is enabled (set `OPENDUCK_HYBRID=1`).
pub fn hybrid_enabled() -> bool {
    std::env::var("OPENDUCK_HYBRID")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Validate `request_token` against the expected token from `OPENDUCK_TOKEN`.
///
/// Delegates to the shared implementation in `exec_proto::validate_token`.
pub use exec_proto::validate_token;

pub fn worker_base_urls() -> Vec<String> {
    std::env::var("OPENDUCK_WORKER_ADDRS")
        .unwrap_or_else(|_| "http://127.0.0.1:9898".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

// ── Worker registry with database-affinity routing ─────────────────────

/// How long a worker can go without a heartbeat before being evicted.
const HEARTBEAT_TTL: std::time::Duration = std::time::Duration::from_secs(90);
/// How often the reaper checks for stale workers.
const REAPER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// A registered worker's capabilities, used for affinity routing.
#[derive(Debug, Clone)]
pub struct RegisteredWorker {
    pub worker_id: String,
    pub endpoint: String,
    pub databases: Vec<String>,
    pub compute_context: String,
    pub max_concurrency: u32,
    pub last_heartbeat: Instant,
}

/// Thread-safe worker registry. Workers register via `RegisterWorker` RPC;
/// the gateway routes fragments preferring workers that declare affinity for
/// the requested database.
#[derive(Default)]
pub struct WorkerRegistry {
    workers: Mutex<Vec<RegisteredWorker>>,
    next: AtomicUsize,
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, w: RegisteredWorker) {
        if let Ok(mut guard) = self.workers.lock() {
            guard.retain(|existing| existing.worker_id != w.worker_id);
            guard.push(w);
        }
    }

    /// Record a heartbeat for the given worker. Returns false if the worker
    /// is not registered.
    pub fn heartbeat(&self, worker_id: &str) -> bool {
        if let Ok(mut guard) = self.workers.lock() {
            if let Some(w) = guard.iter_mut().find(|w| w.worker_id == worker_id) {
                w.last_heartbeat = Instant::now();
                return true;
            }
        }
        false
    }

    /// Remove workers whose last heartbeat is older than `ttl`.
    pub fn evict_stale(&self, ttl: std::time::Duration) -> usize {
        let cutoff = Instant::now() - ttl;
        if let Ok(mut guard) = self.workers.lock() {
            let before = guard.len();
            guard.retain(|w| w.last_heartbeat >= cutoff);
            before - guard.len()
        } else {
            0
        }
    }

    /// Pick the best worker for `database`. Prefers workers that explicitly
    /// list the database; falls back to round-robin over all workers.
    pub fn pick(&self, database: &str) -> Option<String> {
        let guard = self.workers.lock().ok()?;
        if guard.is_empty() {
            return None;
        }

        if !database.is_empty() {
            let affinity: Vec<&RegisteredWorker> = guard
                .iter()
                .filter(|w| w.databases.iter().any(|d| d == database))
                .collect();
            if !affinity.is_empty() {
                let idx = self.next.fetch_add(1, Ordering::Relaxed) % affinity.len();
                return Some(affinity[idx].endpoint.clone());
            }
        }

        let idx = self.next.fetch_add(1, Ordering::Relaxed) % guard.len();
        Some(guard[idx].endpoint.clone())
    }

    pub fn len(&self) -> usize {
        self.workers.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ── Gateway implementation ─────────────────────────────────────────────

pub struct GatewayImpl {
    /// Static worker list from env (used as fallback when registry is empty).
    static_workers: Vec<String>,
    /// Dynamic registry populated by `RegisterWorker` RPCs.
    registry: Arc<WorkerRegistry>,
    next: AtomicUsize,
    sem: Arc<Semaphore>,
    executions: Arc<Mutex<HashMap<String, String>>>,
}

impl GatewayImpl {
    pub fn new(workers: Vec<String>) -> Self {
        let n = max_in_flight().max(1);
        Self {
            static_workers: workers,
            registry: Arc::new(WorkerRegistry::new()),
            next: AtomicUsize::new(0),
            sem: Arc::new(Semaphore::new(n)),
            executions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn from_env() -> Self {
        Self::new(worker_base_urls())
    }

    /// Select a worker endpoint: prefer registry affinity, then registry
    /// round-robin, then static list round-robin.
    fn select_worker(&self, database: &str) -> Option<String> {
        if let Some(ep) = self.registry.pick(database) {
            return Some(ep);
        }
        if self.static_workers.is_empty() {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.static_workers.len();
        Some(self.static_workers[idx].clone())
    }
}

#[tonic::async_trait]
impl ExecutionService for GatewayImpl {
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

        if hybrid_enabled() {
            if let Ok(sql) = std::str::from_utf8(&inner.plan) {
                if let Some(node) = hybrid::parse_openduck_run(sql) {
                    let rewritten = hybrid::insert_bridges(node.clone());
                    let explain = hybrid::explain_annotated(&rewritten);
                    tracing::debug!(plan = %explain, "hybrid plan");

                    if let hybrid::NodeKind::RunHint {
                        sql: inner_sql,
                        placement,
                    } = &node.kind
                    {
                        match placement {
                            hybrid::Placement::Remote => {
                                inner.plan = inner_sql.as_bytes().to_vec();
                            }
                            hybrid::Placement::Local => {
                                return Err(Status::invalid_argument(
                                    "openduck_run('LOCAL', ...) must execute client-side; \
                                     the gateway only handles REMOTE fragments",
                                ));
                            }
                            hybrid::Placement::Auto => {}
                        }
                    }
                } else if let Some(parsed) = hybrid::parse_compound_hint(sql) {
                    let explain = hybrid::explain_annotated(&parsed);
                    tracing::debug!(plan = %explain, "hybrid compound plan");
                    inner.plan = hybrid::extract_remote_sql(&parsed)
                        .unwrap_or(sql.to_string())
                        .as_bytes()
                        .to_vec();
                }
            }
        }

        let uri = self.select_worker(&inner.database).ok_or_else(|| {
            Status::failed_precondition(
                "no workers available; set OPENDUCK_WORKER_ADDRS or register workers via RegisterWorker RPC",
            )
        })?;

        let execution_id = inner.execution_id.clone();
        if let Ok(mut guard) = self.executions.lock() {
            guard.insert(execution_id.clone(), uri.clone());
        }

        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::internal("gateway shutting down"))?;

        let (tx, rx) = mpsc::channel::<Result<ExecuteFragmentChunk, Status>>(32);
        let routes = self.executions.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let mut client = match ExecutionServiceClient::connect(uri).await {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::unavailable(format!("worker connect: {e}"))))
                        .await;
                    return;
                }
            };

            let mut stream = match client.execute_fragment(Request::new(inner)).await {
                Ok(s) => s.into_inner(),
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };

            loop {
                match stream.message().await {
                    Ok(Some(chunk)) => {
                        if tx.send(Ok(chunk)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
            if let Ok(mut guard) = routes.lock() {
                guard.remove(&execution_id);
            }
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
        if cancel.execution_id.is_empty() {
            return Ok(Response::new(CancelReply {
                acknowledged: false,
            }));
        }
        let worker_uri = self
            .executions
            .lock()
            .ok()
            .and_then(|g| g.get(&cancel.execution_id).cloned());
        let Some(worker_uri) = worker_uri else {
            return Ok(Response::new(CancelReply {
                acknowledged: false,
            }));
        };

        let mut client = match ExecutionServiceClient::connect(worker_uri).await {
            Ok(c) => c,
            Err(_) => {
                return Ok(Response::new(CancelReply {
                    acknowledged: false,
                }));
            }
        };
        let reply = client.cancel_execution(Request::new(cancel)).await;
        match reply {
            Ok(r) => Ok(Response::new(r.into_inner())),
            Err(_) => Ok(Response::new(CancelReply {
                acknowledged: false,
            })),
        }
    }

    async fn register_worker(
        &self,
        request: Request<WorkerRegistration>,
    ) -> Result<Response<RegisterWorkerReply>, Status> {
        let reg = request.into_inner();
        if reg.endpoint.is_empty() {
            return Err(Status::invalid_argument("endpoint is required"));
        }
        let worker_id = if reg.worker_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            reg.worker_id.clone()
        };
        tracing::info!(
            worker_id = %worker_id,
            endpoint = %reg.endpoint,
            databases = ?reg.databases,
            compute_context = %reg.compute_context,
            "worker registered"
        );
        self.registry.register(RegisteredWorker {
            worker_id,
            endpoint: reg.endpoint,
            databases: reg.databases,
            compute_context: reg.compute_context,
            max_concurrency: reg.max_concurrency,
            last_heartbeat: Instant::now(),
        });
        Ok(Response::new(RegisterWorkerReply { accepted: true }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let hb = request.into_inner();
        let ack = self.registry.heartbeat(&hb.worker_id);
        if !ack {
            tracing::debug!(worker_id = %hb.worker_id, "heartbeat from unknown worker");
        }
        Ok(Response::new(HeartbeatReply { acknowledged: ack }))
    }
}

/// Run the gateway gRPC server (for tests and `openduck-gateway` binary).
pub async fn serve(
    addr: std::net::SocketAddr,
    workers: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    serve_with_shutdown(addr, workers, None).await
}

/// Run the gateway with an optional graceful shutdown signal.
pub async fn serve_with_shutdown(
    addr: std::net::SocketAddr,
    workers: Vec<String>,
    shutdown: Option<tokio::sync::watch::Receiver<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let gw = GatewayImpl::new(workers);
    let registry = gw.registry.clone();
    let svc = ExecutionServiceServer::new(gw);

    let reaper = tokio::spawn(async move {
        loop {
            tokio::time::sleep(REAPER_INTERVAL).await;
            let evicted = registry.evict_stale(HEARTBEAT_TTL);
            if evicted > 0 {
                tracing::info!(evicted, remaining = registry.len(), "evicted stale workers");
            }
        }
    });

    tracing::info!(%addr, "openduck-gateway listening");
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
    reaper.abort();
    Ok(())
}
