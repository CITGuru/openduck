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

pub use exec_proto::auth::validate_token;
use exec_proto::execute_fragment_error::Kind as ErrorKind;
use exec_proto::identity::{identity_hex, identity_of, IdentityHash};
use exec_proto::ingest_chunk::Payload as IngestPayload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use exec_proto::openduck::v1::{
    BeginTransactionReply, BeginTransactionRequest, CancelReply, CancelRequest,
    CommitTransactionReply, CommitTransactionRequest, ExecuteFragmentChunk, ExecuteFragmentError,
    ExecuteFragmentRequest, HeartbeatReply, HeartbeatRequest, IngestChunk, IngestReply,
    RegisterWorkerReply, RollbackTransactionReply, RollbackTransactionRequest, WorkerRegistration,
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
    pub tables: Vec<String>,
    pub last_heartbeat: Instant,
}

impl hybrid::FederationProvider for RegisteredWorker {
    fn id(&self) -> &str {
        &self.worker_id
    }
    fn endpoint(&self) -> &str {
        &self.endpoint
    }
    fn compute_context(&self) -> &str {
        &self.compute_context
    }
    fn databases(&self) -> &[String] {
        &self.databases
    }
    fn tables(&self) -> &[String] {
        &self.tables
    }
    fn max_concurrency(&self) -> u32 {
        self.max_concurrency
    }
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

    /// Remove workers whose last heartbeat is older than `ttl` and
    /// return the endpoints of the evicted workers so callers can sweep
    /// their own per-worker state (e.g. the transaction-affinity map).
    pub fn evict_stale(&self, ttl: std::time::Duration) -> Vec<String> {
        let cutoff = Instant::now() - ttl;
        if let Ok(mut guard) = self.workers.lock() {
            let mut evicted = Vec::new();
            guard.retain(|w| {
                if w.last_heartbeat < cutoff {
                    evicted.push(w.endpoint.clone());
                    false
                } else {
                    true
                }
            });
            evicted
        } else {
            Vec::new()
        }
    }

    /// Pick the best worker for the given `database` and `compute_context`.
    ///
    /// Priority: context+database match > context match > database match > round-robin.
    pub fn pick(&self, database: &str, compute_context: &str) -> Option<String> {
        let guard = self.workers.lock().ok()?;
        if guard.is_empty() {
            return None;
        }

        let has_ctx = !compute_context.is_empty();
        let has_db = !database.is_empty();

        // Tier 1: both context and database match
        if has_ctx && has_db {
            let both: Vec<&RegisteredWorker> = guard
                .iter()
                .filter(|w| {
                    w.compute_context == compute_context
                        && w.databases.iter().any(|d| d == database)
                })
                .collect();
            if !both.is_empty() {
                let idx = self.next.fetch_add(1, Ordering::Relaxed) % both.len();
                return Some(both[idx].endpoint.clone());
            }
        }

        // Tier 2: context match only
        if has_ctx {
            let ctx: Vec<&RegisteredWorker> = guard
                .iter()
                .filter(|w| w.compute_context == compute_context)
                .collect();
            if !ctx.is_empty() {
                let idx = self.next.fetch_add(1, Ordering::Relaxed) % ctx.len();
                return Some(ctx[idx].endpoint.clone());
            }
        }

        // Tier 3: database match only
        if has_db {
            let db: Vec<&RegisteredWorker> = guard
                .iter()
                .filter(|w| w.databases.iter().any(|d| d == database))
                .collect();
            if !db.is_empty() {
                let idx = self.next.fetch_add(1, Ordering::Relaxed) % db.len();
                return Some(db[idx].endpoint.clone());
            }
        }

        // Tier 4: round-robin over all workers
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % guard.len();
        Some(guard[idx].endpoint.clone())
    }

    /// Build a `TableSourceRegistry` from the live registered workers.
    /// This bridges the gap between the runtime worker registry and the
    /// planner's view of table locations.
    pub fn to_table_source_registry(&self) -> hybrid::TableSourceRegistry {
        let mut reg = hybrid::TableSourceRegistry::new();
        if let Ok(guard) = self.workers.lock() {
            let providers: Vec<&dyn hybrid::FederationProvider> = guard
                .iter()
                .map(|w| w as &dyn hybrid::FederationProvider)
                .collect();
            reg.sync_from_providers(&providers);
        }
        reg
    }

    pub fn len(&self) -> usize {
        self.workers.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ── Transaction affinity ───────────────────────────────────────────────

/// A pinned worker + the caller's identity fingerprint, stored for the
/// lifetime of a transaction. The gateway rejects cross-identity reuse
/// before forwarding to the worker (defense-in-depth — the worker also
/// performs its own authoritative check).
#[derive(Clone, Debug)]
struct AffinityEntry {
    worker: String,
    identity: IdentityHash,
}

/// In-memory, non-replicated transaction-affinity map. If this gateway
/// process restarts, every pinned transaction it knew about becomes
/// unroutable — callers see typed `INTERNAL` errors and the worker's
/// idle reaper eventually rolls the orphaned connections back.
/// Deployments that need zero-RPO during gateway failover should run a
/// single active gateway behind a load balancer with sticky sessions.
#[derive(Default)]
struct TransactionAffinity {
    entries: Mutex<HashMap<String, AffinityEntry>>,
}

impl TransactionAffinity {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&self, tx_id: String, worker: String, identity: IdentityHash) {
        if let Ok(mut guard) = self.entries.lock() {
            guard.insert(tx_id, AffinityEntry { worker, identity });
        }
    }

    fn get(&self, tx_id: &str) -> Option<AffinityEntry> {
        self.entries.lock().ok()?.get(tx_id).cloned()
    }

    fn remove(&self, tx_id: &str) {
        if let Ok(mut guard) = self.entries.lock() {
            guard.remove(tx_id);
        }
    }

    /// Sweep affinity entries pointing to any of `worker_endpoints`.
    /// Returns the transaction ids that were removed so the caller can
    /// log them — used by the worker-death handler in the heartbeat
    /// reaper.
    fn sweep_workers(&self, worker_endpoints: &[String]) -> Vec<String> {
        if worker_endpoints.is_empty() {
            return Vec::new();
        }
        let mut removed = Vec::new();
        if let Ok(mut guard) = self.entries.lock() {
            guard.retain(|tx_id, entry| {
                let keep = !worker_endpoints.iter().any(|w| w == &entry.worker);
                if !keep {
                    removed.push(tx_id.clone());
                }
                keep
            });
        }
        removed
    }

    /// Look up a pinned worker, enforcing the caller's identity. Used
    /// by every transactional `ExecuteFragment` / `IngestData` /
    /// `Commit` / `Rollback` call site before forwarding.
    fn route_for(
        &self,
        tx_id: &str,
        identity: &IdentityHash,
    ) -> Result<String, ExecuteFragmentError> {
        let entry = self.get(tx_id).ok_or_else(|| {
            typed_error(
                ErrorKind::Internal,
                format!(
                    "unknown transaction `{tx_id}`; the gateway has no affinity entry \
                     (the transaction may have been gateway-restart-orphaned or the \
                     pinned worker may have died)"
                ),
            )
        })?;
        if entry.identity != *identity {
            return Err(typed_error(
                ErrorKind::Permission,
                format!("transaction `{tx_id}` belongs to a different caller"),
            ));
        }
        Ok(entry.worker)
    }
}

fn typed_error(kind: ErrorKind, message: impl Into<String>) -> ExecuteFragmentError {
    ExecuteFragmentError {
        kind: kind as i32,
        message: message.into(),
        sql_state: None,
    }
}

/// Per-transaction request deadline. Transactional RPCs get a longer
/// ceiling than one-shot `ExecuteFragment` so long-running DDL / DML
/// doesn't hit the 30s default.
const TXN_RPC_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
const EXEC_RPC_DEADLINE: std::time::Duration = std::time::Duration::from_secs(300);
const CONNECT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

// ── Gateway implementation ─────────────────────────────────────────────

pub struct GatewayImpl {
    /// Static worker list from env (used as fallback when registry is empty).
    static_workers: Vec<String>,
    /// Dynamic registry populated by `RegisterWorker` RPCs.
    registry: Arc<WorkerRegistry>,
    next: AtomicUsize,
    sem: Arc<Semaphore>,
    executions: Arc<Mutex<HashMap<String, String>>>,
    /// Pins transactional RPCs to the worker that opened the transaction.
    transactions: Arc<TransactionAffinity>,
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
            transactions: Arc::new(TransactionAffinity::new()),
        }
    }

    pub fn from_env() -> Self {
        Self::new(worker_base_urls())
    }

    /// Select a worker endpoint: prefer registry affinity (context+database),
    /// then registry round-robin, then static list round-robin.
    fn select_worker(&self, database: &str, compute_context: &str) -> Option<String> {
        if let Some(ep) = self.registry.pick(database, compute_context) {
            return Some(ep);
        }
        if self.static_workers.is_empty() {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.static_workers.len();
        Some(self.static_workers[idx].clone())
    }
}

/// Open a gRPC client against the given worker endpoint with deadlines.
async fn open_worker_client(
    worker: &str,
    rpc_deadline: std::time::Duration,
) -> Result<ExecutionServiceClient<tonic::transport::Channel>, Status> {
    let channel = tonic::transport::Endpoint::from_shared(worker.to_string())
        .map(|ep| ep.connect_timeout(CONNECT_DEADLINE).timeout(rpc_deadline))
        .map_err(|e| Status::internal(format!("invalid worker endpoint: {e}")))?;
    ExecutionServiceClient::connect(channel)
        .await
        .map_err(|e| Status::unavailable(format!("worker connect: {e}")))
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

        let transaction_id = inner.transaction_id.clone().filter(|s| !s.is_empty());

        tracing::info!(
            execution_id = %inner.execution_id,
            database = %inner.database,
            transaction_id = transaction_id.as_deref().unwrap_or(""),
            "gateway received execute_fragment"
        );

        // Transactional path: bypass the normal worker selector and use
        // the pinned worker from the affinity map. Identity is
        // re-checked before forwarding so a stolen UUID alone is not
        // enough to hijack a transaction.
        if let Some(tx_id) = &transaction_id {
            let caller_identity = identity_of(&inner.access_token);
            let worker = match self.transactions.route_for(tx_id, &caller_identity) {
                Ok(w) => w,
                Err(err) => {
                    return Ok(typed_error_stream(err));
                }
            };

            tracing::info!(
                execution_id = %inner.execution_id,
                transaction_id = %tx_id,
                worker = %worker,
                "forwarding transactional fragment to pinned worker"
            );

            let execution_id = inner.execution_id.clone();
            if let Ok(mut guard) = self.executions.lock() {
                guard.insert(execution_id.clone(), worker.clone());
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
                forward_execute_fragment(worker, inner, tx.clone()).await;
                if let Ok(mut guard) = routes.lock() {
                    guard.remove(&execution_id);
                }
            });
            return Ok(Response::new(
                Box::pin(ReceiverStream::new(rx)) as Self::ExecuteFragmentStream
            ));
        }

        if hybrid_enabled() {
            let table_sources = self.registry.to_table_source_registry();
            if let Ok(sql) = std::str::from_utf8(&inner.plan) {
                if let Some(mut node) = hybrid::parse_openduck_run(sql) {
                    let remote_tables: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    hybrid::resolve_auto_with_registry(
                        &mut node,
                        &remote_tables,
                        Some(&table_sources),
                    );
                    hybrid::pushdown_federable_subplans(&mut node, &table_sources);

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
                } else if let Some(mut parsed) = hybrid::parse_compound_hint(sql) {
                    let remote_tables: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    hybrid::resolve_auto_with_registry(
                        &mut parsed,
                        &remote_tables,
                        Some(&table_sources),
                    );
                    hybrid::pushdown_federable_subplans(&mut parsed, &table_sources);

                    let explain = hybrid::explain_annotated(&parsed);
                    tracing::debug!(plan = %explain, "hybrid compound plan");
                    inner.plan = hybrid::extract_remote_sql(&parsed)
                        .unwrap_or(sql.to_string())
                        .as_bytes()
                        .to_vec();
                }
            }
        }

        let uri = self.select_worker(&inner.database, &inner.compute_context).ok_or_else(|| {
            tracing::error!(execution_id = %inner.execution_id, "no workers available");
            Status::failed_precondition(
                "no workers available; set OPENDUCK_WORKER_ADDRS or register workers via RegisterWorker RPC",
            )
        })?;

        tracing::info!(
            execution_id = %inner.execution_id,
            worker = %uri,
            "routing to worker"
        );

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
            forward_execute_fragment(uri, inner, tx.clone()).await;
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
        tracing::info!(execution_id = %cancel.execution_id, "gateway received cancel_execution");
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

        let channel = tonic::transport::Endpoint::from_shared(worker_uri).map(|ep| {
            ep.connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(10))
        });
        let mut client = match async { ExecutionServiceClient::connect(channel?).await }.await {
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
        validate_token(&reg.access_token)?;
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
            tables: reg.tables,
            last_heartbeat: Instant::now(),
        });
        Ok(Response::new(RegisterWorkerReply { accepted: true }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let hb = request.into_inner();
        validate_token(&hb.access_token)?;
        let ack = self.registry.heartbeat(&hb.worker_id);
        if !ack {
            tracing::debug!(worker_id = %hb.worker_id, "heartbeat from unknown worker");
        }
        Ok(Response::new(HeartbeatReply { acknowledged: ack }))
    }

    // ── Transactions / IngestData ──────────────────────────────────────

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionReply>, Status> {
        let inner = request.into_inner();
        validate_token(&inner.access_token)?;
        let caller_identity = identity_of(&inner.access_token);

        let worker = self.select_worker(&inner.database, "").ok_or_else(|| {
            Status::failed_precondition("no workers available for BeginTransaction")
        })?;
        tracing::info!(
            worker = %worker,
            database = %inner.database,
            identity_hash = %identity_hex(&caller_identity),
            "forwarding BeginTransaction"
        );

        let mut client = open_worker_client(&worker, TXN_RPC_DEADLINE).await?;
        let reply = client
            .begin_transaction(Request::new(inner))
            .await
            .map_err(|e| Status::unavailable(format!("worker BeginTransaction failed: {e}")))?
            .into_inner();

        // Only record affinity when the worker actually opened a txn.
        if !reply.transaction_id.is_empty() {
            self.transactions.insert(
                reply.transaction_id.clone(),
                worker.clone(),
                caller_identity,
            );
            tracing::info!(
                transaction_id = %reply.transaction_id,
                worker = %worker,
                identity_hash = %identity_hex(&caller_identity),
                "gateway pinned transaction"
            );
        }
        Ok(Response::new(reply))
    }

    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionReply>, Status> {
        let inner = request.into_inner();
        validate_token(&inner.access_token)?;
        let caller_identity = identity_of(&inner.access_token);
        let tx_id = inner.transaction_id.clone();

        let worker = match self.transactions.route_for(&tx_id, &caller_identity) {
            Ok(w) => w,
            Err(err) => {
                return Ok(Response::new(CommitTransactionReply {
                    typed_error: Some(err),
                }));
            }
        };

        let mut client = open_worker_client(&worker, TXN_RPC_DEADLINE).await?;
        let reply_result = client.commit_transaction(Request::new(inner)).await;
        // Whether Commit succeeded or not, the transaction is done as
        // far as the gateway is concerned — drop affinity.
        self.transactions.remove(&tx_id);

        match reply_result {
            Ok(r) => {
                tracing::info!(
                    transaction_id = %tx_id,
                    worker = %worker,
                    "forwarded CommitTransaction"
                );
                Ok(Response::new(r.into_inner()))
            }
            Err(e) => Ok(Response::new(CommitTransactionReply {
                typed_error: Some(typed_error(
                    ErrorKind::Internal,
                    format!("worker CommitTransaction failed: {e}"),
                )),
            })),
        }
    }

    async fn rollback_transaction(
        &self,
        request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionReply>, Status> {
        let inner = request.into_inner();
        validate_token(&inner.access_token)?;
        let caller_identity = identity_of(&inner.access_token);
        let tx_id = inner.transaction_id.clone();

        let worker = match self.transactions.route_for(&tx_id, &caller_identity) {
            Ok(w) => w,
            Err(err) => {
                return Ok(Response::new(RollbackTransactionReply {
                    typed_error: Some(err),
                }));
            }
        };

        let mut client = open_worker_client(&worker, TXN_RPC_DEADLINE).await?;
        let reply_result = client.rollback_transaction(Request::new(inner)).await;
        self.transactions.remove(&tx_id);

        match reply_result {
            Ok(r) => {
                tracing::info!(
                    transaction_id = %tx_id,
                    worker = %worker,
                    "forwarded RollbackTransaction"
                );
                Ok(Response::new(r.into_inner()))
            }
            Err(e) => Ok(Response::new(RollbackTransactionReply {
                typed_error: Some(typed_error(
                    ErrorKind::Internal,
                    format!("worker RollbackTransaction failed: {e}"),
                )),
            })),
        }
    }

    async fn ingest_data(
        &self,
        request: Request<tonic::Streaming<IngestChunk>>,
    ) -> Result<Response<IngestReply>, Status> {
        let mut stream = request.into_inner();

        // Peek the first chunk — MUST be IngestMetadata, mirroring the
        // worker-side contract. The gateway then uses the metadata to
        // resolve which worker to forward to (pinned for transactional
        // ingest, selected for implicit).
        let first = match stream.message().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => {
                return Ok(Response::new(IngestReply {
                    rows_ingested: 0,
                    typed_error: Some(typed_error(
                        ErrorKind::Binder,
                        "IngestData stream closed before any chunk was sent",
                    )),
                }));
            }
            Err(e) => return Err(e),
        };
        let meta = match &first.payload {
            Some(IngestPayload::Metadata(m)) => m.clone(),
            Some(IngestPayload::ArrowBatch(_)) => {
                return Ok(Response::new(IngestReply {
                    rows_ingested: 0,
                    typed_error: Some(typed_error(
                        ErrorKind::Binder,
                        "first IngestChunk must carry IngestMetadata; got arrow_batch",
                    )),
                }));
            }
            None => {
                return Ok(Response::new(IngestReply {
                    rows_ingested: 0,
                    typed_error: Some(typed_error(
                        ErrorKind::Binder,
                        "first IngestChunk has no payload",
                    )),
                }));
            }
        };

        validate_token(&meta.access_token)?;
        let caller_identity = identity_of(&meta.access_token);

        let worker = match meta.transaction_id.as_deref().filter(|s| !s.is_empty()) {
            Some(tx_id) => match self.transactions.route_for(tx_id, &caller_identity) {
                Ok(w) => w,
                Err(err) => {
                    return Ok(Response::new(IngestReply {
                        rows_ingested: 0,
                        typed_error: Some(err),
                    }));
                }
            },
            None => self.select_worker(&meta.database, "").ok_or_else(|| {
                Status::failed_precondition("no workers available for IngestData")
            })?,
        };

        tracing::info!(
            worker = %worker,
            database = %meta.database,
            transaction_id = meta.transaction_id.as_deref().unwrap_or(""),
            identity_hash = %identity_hex(&caller_identity),
            "forwarding IngestData to worker"
        );

        let mut client = open_worker_client(&worker, TXN_RPC_DEADLINE).await?;

        // Re-assemble the client stream: the first chunk we already
        // peeked, then relay subsequent chunks unchanged.
        let (fwd_tx, fwd_rx) = mpsc::channel::<IngestChunk>(32);
        fwd_tx
            .send(first)
            .await
            .map_err(|_| Status::internal("ingest forward channel closed"))?;
        tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(None) => break,
                    Ok(Some(chunk)) => {
                        if fwd_tx.send(chunk).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "ingest upstream recv error");
                        break;
                    }
                }
            }
        });
        let outbound = ReceiverStream::new(fwd_rx);

        let reply = client
            .ingest_data(Request::new(outbound))
            .await
            .map_err(|e| Status::unavailable(format!("worker IngestData failed: {e}")))?
            .into_inner();
        Ok(Response::new(reply))
    }
}

/// Alias matching `GatewayImpl::ExecuteFragmentStream`. Keeps the
/// ugly trait-object type out of free-function signatures.
type ExecFragmentStream = Pin<Box<dyn Stream<Item = Result<ExecuteFragmentChunk, Status>> + Send>>;

/// One-shot stream carrying a single typed-error chunk followed by
/// `finished=true`, used when the gateway rejects a transactional
/// `ExecuteFragment` before ever contacting a worker.
fn typed_error_stream(err: ExecuteFragmentError) -> Response<ExecFragmentStream> {
    let (tx, rx) = mpsc::channel::<Result<ExecuteFragmentChunk, Status>>(2);
    let err_msg = err.message.clone();
    tokio::spawn(async move {
        let _ = tx
            .send(Ok(ExecuteFragmentChunk {
                payload: Some(
                    exec_proto::openduck::v1::execute_fragment_chunk::Payload::Error(err_msg),
                ),
                typed_error: Some(err),
            }))
            .await;
        let _ = tx
            .send(Ok(ExecuteFragmentChunk {
                payload: Some(
                    exec_proto::openduck::v1::execute_fragment_chunk::Payload::Finished(true),
                ),
                typed_error: None,
            }))
            .await;
    });
    Response::new(Box::pin(ReceiverStream::new(rx)) as ExecFragmentStream)
}

/// Forward a fully-prepared `ExecuteFragmentRequest` to the given worker
/// endpoint and pump the response stream into `tx`.
async fn forward_execute_fragment(
    worker: String,
    inner: ExecuteFragmentRequest,
    tx: mpsc::Sender<Result<ExecuteFragmentChunk, Status>>,
) {
    let mut client = match open_worker_client(&worker, EXEC_RPC_DEADLINE).await {
        Ok(c) => c,
        Err(status) => {
            let _ = tx.send(Err(status)).await;
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
    let transactions = gw.transactions.clone();
    const MAX_MSG_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
    let svc = ExecutionServiceServer::new(gw)
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE);

    // Periodic reaper: drop workers whose heartbeat is stale, and sweep
    // any affinity entries pointing at them.
    let reaper_reg = registry.clone();
    let reaper_txn = transactions.clone();
    let reaper = tokio::spawn(async move {
        loop {
            tokio::time::sleep(REAPER_INTERVAL).await;
            let evicted = reaper_reg.evict_stale(HEARTBEAT_TTL);
            if !evicted.is_empty() {
                let orphaned = reaper_txn.sweep_workers(&evicted);
                tracing::info!(
                    evicted_workers = evicted.len(),
                    remaining = reaper_reg.len(),
                    orphaned_transactions = orphaned.len(),
                    "evicted stale workers"
                );
                for tx_id in orphaned {
                    tracing::warn!(
                        transaction_id = %tx_id,
                        "worker-death swept transaction affinity; \
                         subsequent calls will see typed INTERNAL \
                         'unknown transaction'"
                    );
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(token: &str) -> IdentityHash {
        identity_of(token)
    }

    #[test]
    fn affinity_insert_and_route() {
        let aff = TransactionAffinity::new();
        let id = ident("alpha");
        aff.insert("tx1".into(), "http://worker-1".into(), id);
        let worker = aff.route_for("tx1", &id).unwrap();
        assert_eq!(worker, "http://worker-1");
    }

    #[test]
    fn affinity_unknown_tx_returns_internal() {
        let aff = TransactionAffinity::new();
        let err = aff.route_for("nope", &ident("alpha")).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Internal as i32);
        assert!(err.message.contains("unknown transaction"));
    }

    #[test]
    fn affinity_cross_identity_rejected() {
        let aff = TransactionAffinity::new();
        aff.insert("tx1".into(), "http://w".into(), ident("alpha"));
        let err = aff.route_for("tx1", &ident("beta")).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Permission as i32);
        assert!(err.message.contains("different caller"));
    }

    #[test]
    fn affinity_remove_drops_entry() {
        let aff = TransactionAffinity::new();
        aff.insert("tx1".into(), "http://w".into(), ident("alpha"));
        aff.remove("tx1");
        assert!(aff.get("tx1").is_none());
    }

    #[test]
    fn affinity_sweep_workers_removes_matching() {
        let aff = TransactionAffinity::new();
        aff.insert("tx1".into(), "http://w1".into(), ident("a"));
        aff.insert("tx2".into(), "http://w1".into(), ident("b"));
        aff.insert("tx3".into(), "http://w2".into(), ident("c"));

        let removed = aff.sweep_workers(&["http://w1".into()]);
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&"tx1".to_string()));
        assert!(removed.contains(&"tx2".to_string()));

        assert!(aff.get("tx1").is_none());
        assert!(aff.get("tx2").is_none());
        assert!(aff.get("tx3").is_some());
    }

    #[test]
    fn affinity_sweep_workers_empty_input_is_noop() {
        let aff = TransactionAffinity::new();
        aff.insert("tx1".into(), "http://w1".into(), ident("a"));
        assert!(aff.sweep_workers(&[]).is_empty());
        assert!(aff.get("tx1").is_some());
    }

    #[test]
    fn worker_registry_evict_stale_returns_endpoints() {
        let reg = WorkerRegistry::new();
        let now = Instant::now();
        reg.register(RegisteredWorker {
            worker_id: "w-fresh".into(),
            endpoint: "http://fresh".into(),
            databases: vec![],
            compute_context: String::new(),
            max_concurrency: 0,
            tables: vec![],
            last_heartbeat: now,
        });
        reg.register(RegisteredWorker {
            worker_id: "w-stale".into(),
            endpoint: "http://stale".into(),
            databases: vec![],
            compute_context: String::new(),
            max_concurrency: 0,
            tables: vec![],
            last_heartbeat: now - std::time::Duration::from_secs(3600),
        });
        let evicted = reg.evict_stale(std::time::Duration::from_secs(60));
        assert_eq!(evicted, vec!["http://stale"]);
        assert_eq!(reg.len(), 1);
    }
}
