//! # gRPC Round-Trip Example
//!
//! Starts a worker and gateway in-process, sends SQL through the gateway,
//! and prints the Arrow IPC results.
//!
//! ```bash
//! OPENDUCK_TOKEN=demo cargo run --example grpc_roundtrip
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::ExecuteFragmentRequest;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("OPENDUCK_TOKEN").unwrap_or_else(|_| "demo".into());

    let worker_addr: SocketAddr = "127.0.0.1:19898".parse()?;
    let gateway_addr: SocketAddr = "127.0.0.1:17878".parse()?;

    // Start worker in background.
    tokio::spawn(async move {
        let _ = exec_worker::serve(worker_addr).await;
    });

    // Start gateway pointing at the worker.
    let worker_url = format!("http://{worker_addr}");
    tokio::spawn(async move {
        let _ = exec_gateway::serve(gateway_addr, vec![worker_url]).await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect as a gRPC client.
    let mut client = ExecutionServiceClient::connect(format!("http://{gateway_addr}")).await?;

    let sql = "SELECT i AS id, i * 10 AS value FROM range(5) t(i)";
    println!("=> Sending SQL: {sql}");

    let request = ExecuteFragmentRequest {
        plan: sql.as_bytes().to_vec(),
        access_token: token,
        execution_id: "example-1".into(),
        ..Default::default()
    };

    let mut stream = client
        .execute_fragment(Request::new(request))
        .await?
        .into_inner();

    let mut batch_count = 0;
    while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
        let chunk = chunk?;
        match chunk.payload {
            Some(Payload::ArrowBatch(batch)) => {
                batch_count += 1;
                println!(
                    "   Arrow batch #{batch_count}: {} bytes IPC payload",
                    batch.ipc_stream_payload.len()
                );
            }
            Some(Payload::Error(e)) => {
                eprintln!("   Error: {e}");
            }
            Some(Payload::Finished(true)) => {
                println!("   Stream finished.");
            }
            _ => {}
        }
    }

    println!("=> Done. Received {batch_count} Arrow batch(es).\n");

    // ── Authentication demo ─────────────────────────────────────────────
    println!("=> Testing authentication: sending request with wrong token...");
    let bad_request = ExecuteFragmentRequest {
        plan: b"SELECT 1".to_vec(),
        access_token: "wrong-token".into(),
        execution_id: "example-bad-auth".into(),
        ..Default::default()
    };

    match client.execute_fragment(Request::new(bad_request)).await {
        Ok(_) => println!("   Unexpected success (OPENDUCK_TOKEN may be unset — dev mode)"),
        Err(status) => {
            println!(
                "   Rejected as expected: code={}, message={}",
                status.code(),
                status.message()
            );
        }
    }

    println!("\n=> Testing authentication: sending request with no token...");
    let empty_request = ExecuteFragmentRequest {
        plan: b"SELECT 1".to_vec(),
        access_token: String::new(),
        execution_id: "example-no-auth".into(),
        ..Default::default()
    };

    match client.execute_fragment(Request::new(empty_request)).await {
        Ok(_) => println!("   Unexpected success (OPENDUCK_TOKEN may be unset — dev mode)"),
        Err(status) => {
            println!(
                "   Rejected as expected: code={}, message={}",
                status.code(),
                status.message()
            );
        }
    }

    Ok(())
}
