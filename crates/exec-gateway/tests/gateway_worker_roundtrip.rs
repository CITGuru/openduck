//! End-to-end: gateway → worker → Arrow IPC chunks and cancellation.

use std::time::Duration;

use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::{CancelRequest, ExecuteFragmentRequest};
use tonic::Request;

async fn start_stack(worker_port: u16, gateway_port: u16) {
    std::env::set_var("OPENDUCK_TOKEN", "test-token");
    let worker = ([127, 0, 0, 1], worker_port).into();
    let _wh = tokio::spawn(async move {
        let _ = exec_worker::serve(worker).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let gw = ([127, 0, 0, 1], gateway_port).into();
    let workers = vec![format!("http://127.0.0.1:{worker_port}")];
    let _gh = tokio::spawn(async move {
        let _ = exec_gateway::serve(gw, workers).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn gateway_forwards_sql_and_arrow() {
    start_stack(45201, 45202).await;
    let mut client = ExecutionServiceClient::connect("http://127.0.0.1:45202")
        .await
        .expect("gateway client");

    let mut stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 42 AS v".to_vec(),
            database: String::new(),
            snapshot_id: None,
            access_token: "test-token".into(),
            execution_id: "gw-roundtrip-1".into(),
        }))
        .await
        .expect("execute_fragment")
        .into_inner();

    let mut saw_batch = false;
    let mut finished = false;
    while let Some(chunk) = stream.message().await.expect("message") {
        match chunk.payload {
            Some(Payload::ArrowBatch(b)) if !b.ipc_stream_payload.is_empty() => saw_batch = true,
            Some(Payload::Finished(true)) => finished = true,
            Some(Payload::Error(e)) => panic!("worker error: {e}"),
            _ => {}
        }
    }
    assert!(saw_batch, "expected at least one Arrow IPC batch");
    assert!(finished, "expected Finished chunk");
}

#[tokio::test]
async fn gateway_forwards_cancel_to_worker() {
    start_stack(45211, 45212).await;
    let mut client = ExecutionServiceClient::connect("http://127.0.0.1:45212")
        .await
        .expect("gateway client");

    let execution_id = "gw-cancel-1".to_string();
    let mut stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            // Generate enough output to increase cancel window.
            plan: b"SELECT * FROM range(500000000)".to_vec(),
            database: String::new(),
            snapshot_id: None,
            access_token: "test-token".into(),
            execution_id: execution_id.clone(),
        }))
        .await
        .expect("execute_fragment")
        .into_inner();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let cancel = client
        .cancel_execution(Request::new(CancelRequest {
            execution_id,
            access_token: "test-token".into(),
        }))
        .await
        .expect("cancel")
        .into_inner();
    assert!(cancel.acknowledged, "expected cancel acknowledgement");

    let mut saw_cancel_error = false;
    while let Some(chunk) = stream.message().await.expect("message") {
        if let Some(Payload::Error(e)) = chunk.payload {
            if e.contains("cancelled") {
                saw_cancel_error = true;
            }
        }
    }
    assert!(saw_cancel_error, "expected cancellation error chunk");
}

#[tokio::test]
async fn wrong_token_rejected() {
    start_stack(45221, 45222).await;
    // start_stack sets OPENDUCK_TOKEN to "test-token"; validate_token reads it per-request.

    let mut client = ExecutionServiceClient::connect("http://127.0.0.1:45222")
        .await
        .expect("gateway client");

    // Wrong token should be rejected
    let result = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1".to_vec(),
            database: String::new(),
            snapshot_id: None,
            access_token: "wrong-secret".into(),
            execution_id: "auth-test-bad".into(),
        }))
        .await;

    assert!(result.is_err(), "expected rejection with wrong token");
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::Unauthenticated,
        "expected Unauthenticated, got {:?}",
        status.code()
    );

    // Correct token ("test-token" set by start_stack) should be accepted
    let result = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 42".to_vec(),
            database: String::new(),
            snapshot_id: None,
            access_token: "test-token".into(),
            execution_id: "auth-test-ok".into(),
        }))
        .await;

    assert!(result.is_ok(), "expected success with correct token");
}
