//! Shared helpers for OpenDuck end-to-end integration tests.
//!
//! Each test file includes this via `mod common;` so we don't duplicate
//! boilerplate across files. Ports are picked by binding to :0, grabbing
//! the kernel-assigned port, then dropping the listener and handing the
//! port to the worker/gateway — effectively `portpicker` without an
//! extra dev-dep.

#![allow(dead_code)]

use std::sync::Once;
use std::time::Duration;

use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_worker::WorkerConfig;

pub const TOKEN: &str = "test-token";

static INIT_TOKEN: Once = Once::new();

/// Call in every test's first line so `OPENDUCK_TOKEN` is stable even
/// when tests run in parallel. All tests use the same token except the
/// cross-identity ones, which always pass explicit access_token values.
pub fn init_token() {
    INIT_TOKEN.call_once(|| {
        std::env::set_var("OPENDUCK_TOKEN", TOKEN);
    });
}

/// Reserve a free TCP port by binding :0 and immediately releasing it.
/// There's a tiny TOCTOU window but it's acceptable for tests — the
/// worker/gateway rebinds within milliseconds.
pub fn pick_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// A running stack with a handle to the gateway's `ExecutionServiceClient`.
pub struct Stack {
    pub worker_port: u16,
    pub gateway_port: u16,
    /// Task handle for the worker — used by tests that need to kill
    /// the worker mid-flight (worker-death tests).
    pub worker_handle: tokio::task::JoinHandle<()>,
    #[allow(dead_code)]
    pub gateway_handle: tokio::task::JoinHandle<()>,
}

impl Stack {
    pub async fn new() -> Self {
        Self::with_config(WorkerConfig::default()).await
    }

    pub async fn with_config(config: WorkerConfig) -> Self {
        init_token();
        let worker_port = pick_port();
        let gateway_port = pick_port();

        let worker_addr = ([127, 0, 0, 1], worker_port).into();
        let worker_handle = tokio::spawn(async move {
            let _ = exec_worker::serve_with_config(worker_addr, config).await;
        });

        // Wait until the worker is accepting connections; retry the
        // gateway bind afterwards. Avoids the fixed-sleep flakiness of
        // the older harness on slow CI hardware.
        wait_for_tcp(worker_port).await;

        let gw_addr = ([127, 0, 0, 1], gateway_port).into();
        let workers = vec![format!("http://127.0.0.1:{worker_port}")];
        let gateway_handle = tokio::spawn(async move {
            let _ = exec_gateway::serve(gw_addr, workers).await;
        });
        wait_for_tcp(gateway_port).await;

        Self {
            worker_port,
            gateway_port,
            worker_handle,
            gateway_handle,
        }
    }

    /// Abort the worker task and wait for its port to stop accepting
    /// connections. Used by worker-death tests.
    pub async fn kill_worker(&self) {
        self.worker_handle.abort();
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", self.worker_port))
                .await
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Spin up only a worker (no gateway) for worker-direct tests.
    pub async fn worker_only(config: WorkerConfig) -> WorkerOnly {
        init_token();
        let port = pick_port();
        let addr = ([127, 0, 0, 1], port).into();
        tokio::spawn(async move {
            let _ = exec_worker::serve_with_config(addr, config).await;
        });
        wait_for_tcp(port).await;
        WorkerOnly { port }
    }

    pub fn gateway_uri(&self) -> String {
        format!("http://127.0.0.1:{}", self.gateway_port)
    }

    pub fn worker_uri(&self) -> String {
        format!("http://127.0.0.1:{}", self.worker_port)
    }

    pub async fn gateway_client(&self) -> ExecutionServiceClient<tonic::transport::Channel> {
        ExecutionServiceClient::connect(self.gateway_uri())
            .await
            .expect("gateway client connect")
    }

    pub async fn worker_client(&self) -> ExecutionServiceClient<tonic::transport::Channel> {
        ExecutionServiceClient::connect(self.worker_uri())
            .await
            .expect("worker client connect")
    }
}

pub struct WorkerOnly {
    pub port: u16,
}

impl WorkerOnly {
    pub fn uri(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub async fn client(&self) -> ExecutionServiceClient<tonic::transport::Channel> {
        ExecutionServiceClient::connect(self.uri())
            .await
            .expect("worker client connect")
    }
}

async fn wait_for_tcp(port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("service on port {port} never became reachable");
}
