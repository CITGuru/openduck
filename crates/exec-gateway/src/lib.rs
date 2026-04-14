//! OpenDuck execution gateway — authenticates and forwards `ExecuteFragment` to workers.
//!
//! When `OPENDUCK_HYBRID=1`, the gateway inspects incoming SQL for
//! `openduck_run('REMOTE', '...')` / `openduck_run('LOCAL', '...')` hints,
//! extracts the inner SQL, and logs an annotated hybrid plan.

pub mod hybrid;

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};

use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use exec_proto::openduck::v1::{
    CancelReply, CancelRequest, ExecuteFragmentChunk, ExecuteFragmentRequest,
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
/// - If `OPENDUCK_TOKEN` is unset or empty: dev mode — any token (including empty) is accepted.
/// - If `OPENDUCK_TOKEN` is set: `request_token` must match (constant-time comparison).
#[allow(clippy::result_large_err)]
pub fn validate_token(request_token: &str) -> Result<(), Status> {
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

pub fn worker_base_urls() -> Vec<String> {
    std::env::var("OPENDUCK_WORKER_ADDRS")
        .unwrap_or_else(|_| "http://127.0.0.1:9898".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub struct GatewayImpl {
    workers: Vec<String>,
    next: AtomicUsize,
    sem: Arc<Semaphore>,
    executions: Arc<Mutex<HashMap<String, String>>>,
}

impl GatewayImpl {
    pub fn new(workers: Vec<String>) -> Self {
        let n = max_in_flight().max(1);
        Self {
            workers,
            next: AtomicUsize::new(0),
            sem: Arc::new(Semaphore::new(n)),
            executions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn from_env() -> Self {
        Self::new(worker_base_urls())
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

        if self.workers.is_empty() {
            return Err(Status::failed_precondition(
                "no workers configured; set OPENDUCK_WORKER_ADDRS",
            ));
        }

        if inner.execution_id.is_empty() {
            inner.execution_id = Uuid::new_v4().to_string();
        }

        if hybrid_enabled() {
            if let Ok(sql) = std::str::from_utf8(&inner.plan) {
                if let Some(node) = hybrid::parse_openduck_run(sql) {
                    let rewritten = hybrid::insert_bridges(node.clone());
                    let explain = hybrid::explain_annotated(&rewritten);
                    eprintln!("hybrid plan:\n{explain}");

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
                    eprintln!("hybrid compound plan:\n{explain}");
                    inner.plan = hybrid::extract_remote_sql(&parsed)
                        .unwrap_or(sql.to_string())
                        .as_bytes()
                        .to_vec();
                }
            }
        }

        let execution_id = inner.execution_id.clone();
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let uri = self.workers[idx].clone();
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
}

/// Run the gateway gRPC server (for tests and `openduck-gateway` binary).
pub async fn serve(
    addr: std::net::SocketAddr,
    workers: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let svc = ExecutionServiceServer::new(GatewayImpl::new(workers));
    println!("openduck-gateway on {addr}");
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}
