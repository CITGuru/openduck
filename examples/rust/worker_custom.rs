//! # Custom Worker Configuration
//!
//! Shows how to start an OpenDuck worker with different configurations:
//! in-memory, file-backed, and DuckLake-backed.
//!
//! Then sends SQL through the worker's gRPC interface and prints results.
//!
//! ```bash
//! OPENDUCK_TOKEN=demo cargo run --example worker_custom
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::ExecuteFragmentRequest;
use exec_worker::WorkerConfig;
use tonic::Request;

async fn send_query(addr: &str, sql: &str, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ExecutionServiceClient::connect(addr.to_string()).await?;

    let request = ExecuteFragmentRequest {
        plan: sql.as_bytes().to_vec(),
        access_token: token.into(),
        execution_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    };

    let mut stream = client
        .execute_fragment(Request::new(request))
        .await?
        .into_inner();

    let mut batch_count = 0;
    let mut total_bytes = 0;
    while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
        let chunk = chunk?;
        match chunk.payload {
            Some(Payload::ArrowBatch(batch)) => {
                batch_count += 1;
                total_bytes += batch.ipc_stream_payload.len();
            }
            Some(Payload::Error(e)) => {
                eprintln!("   Error: {e}");
                return Ok(());
            }
            Some(Payload::Finished(true)) => break,
            _ => {}
        }
    }
    println!("   → {batch_count} batch(es), {total_bytes} bytes total");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("OPENDUCK_TOKEN").unwrap_or_else(|_| "demo".into());

    println!("=== OpenDuck Worker Configuration Examples ===\n");

    // ── 1. In-memory worker (default) ───────────────────────────────────────
    println!("── 1. In-memory worker ──");
    let addr: SocketAddr = "127.0.0.1:19001".parse()?;
    let config = WorkerConfig::default();
    println!("   Config: in-memory (no persistence)");

    tokio::spawn(async move {
        let _ = exec_worker::serve_with_config(addr, config).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("   Sending: SELECT 42 AS answer");
    send_query(&format!("http://{addr}"), "SELECT 42 AS answer", &token).await?;

    println!("   Sending: SELECT i, i*i AS square FROM range(10) t(i)");
    send_query(
        &format!("http://{addr}"),
        "SELECT i, i*i AS square FROM range(10) t(i)",
        &token,
    )
    .await?;

    println!("   Sending: CREATE + INSERT + SELECT");
    send_query(
        &format!("http://{addr}"),
        "CREATE TABLE test (x INT); INSERT INTO test VALUES (1),(2),(3); SELECT sum(x) FROM test",
        &token,
    )
    .await?;
    println!();

    // ── 2. File-backed worker ───────────────────────────────────────────────
    println!("── 2. File-backed worker (conceptual) ──");
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("worker.duckdb");
    let file_config = WorkerConfig {
        db_path: Some(db_path.clone()),
        ..Default::default()
    };
    println!("   Config: db_path = {}", db_path.display());

    let addr2: SocketAddr = "127.0.0.1:19002".parse()?;
    tokio::spawn(async move {
        let _ = exec_worker::serve_with_config(addr2, file_config).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("   Sending: SELECT version()");
    send_query(
        &format!("http://{addr2}"),
        "SELECT version() AS ver",
        &token,
    )
    .await?;
    println!();

    // ── 3. DuckLake-backed worker (config only) ─────────────────────────────
    println!("── 3. DuckLake-backed worker (config only — needs Postgres) ──");
    let ducklake_config = WorkerConfig {
        db_path: None,
        ducklake_metadata: Some("postgres://user:pass@localhost/ducklake_meta".into()),
        ducklake_data_path: Some("s3://my-bucket/ducklake/".into()),
        ..Default::default()
    };
    println!(
        "   metadata:  {}",
        ducklake_config.ducklake_metadata.as_ref().unwrap()
    );
    println!(
        "   data_path: {}",
        ducklake_config.ducklake_data_path.as_ref().unwrap()
    );
    println!("   (skipping startup — requires running Postgres + S3)\n");

    // ── 4. Gateway with multiple workers ────────────────────────────────────
    println!("── 4. Gateway routing across workers ──");
    let gw_addr: SocketAddr = "127.0.0.1:19003".parse()?;
    let workers = vec![format!("http://{addr}"), format!("http://{addr2}")];
    println!("   Workers: {:?}", workers);

    tokio::spawn(async move {
        let _ = exec_gateway::serve(gw_addr, workers).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 1..=4 {
        println!("   Query #{i} (round-robin): SELECT {i}");
        send_query(
            &format!("http://{gw_addr}"),
            &format!("SELECT {i} AS n"),
            &token,
        )
        .await?;
    }
    println!();

    // ── 5. Cancellation ─────────────────────────────────────────────────────
    println!("── 5. Execution cancellation ──");
    use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient as Client;
    use exec_proto::openduck::v1::CancelRequest;

    let mut client = Client::connect(format!("http://{gw_addr}")).await?;
    let reply = client
        .cancel_execution(Request::new(CancelRequest {
            execution_id: "nonexistent-id".into(),
            access_token: token.clone(),
        }))
        .await?;
    println!(
        "   Cancel 'nonexistent-id': acknowledged={}",
        reply.into_inner().acknowledged
    );
    println!("   (false expected — execution doesn't exist)\n");

    println!("=== Done ===");
    Ok(())
}
