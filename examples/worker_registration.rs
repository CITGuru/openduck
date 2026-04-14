//! # Worker Registration & Fallback Example
//!
//! Demonstrates:
//! - Workers registering with the gateway via `RegisterWorker` RPC
//! - Database-affinity routing (queries go to the worker that owns the data)
//! - Graceful fallback when a worker is unreachable
//!
//! ```bash
//! OPENDUCK_TOKEN=demo cargo run --example worker_registration
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::{ExecuteFragmentRequest, WorkerRegistration};
use tonic::Request;

use exec_gateway::hybrid;

async fn send_query_to(
    gateway: &str,
    sql: &str,
    database: &str,
    token: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut client = ExecutionServiceClient::connect(gateway.to_string()).await?;

    let request = ExecuteFragmentRequest {
        plan: sql.as_bytes().to_vec(),
        database: database.into(),
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
            Some(Payload::Error(e)) => return Ok(format!("error: {e}")),
            Some(Payload::Finished(true)) => break,
            _ => {}
        }
    }
    Ok(format!("{batch_count} batch(es), {total_bytes} bytes"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = "reg-demo-token";
    std::env::set_var("OPENDUCK_TOKEN", token);

    println!("=== OpenDuck Worker Registration & Fallback ===\n");

    // ── 1. Start two workers with different databases ────────────────────
    println!("── 1. Starting workers ──\n");

    let worker1_addr: SocketAddr = "127.0.0.1:19801".parse()?;
    let worker2_addr: SocketAddr = "127.0.0.1:19802".parse()?;

    tokio::spawn(async move {
        let _ = exec_worker::serve(worker1_addr).await;
    });
    tokio::spawn(async move {
        let _ = exec_worker::serve(worker2_addr).await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("   Worker A on {worker1_addr} (analytics database)");
    println!("   Worker B on {worker2_addr} (logging database)\n");

    // ── 2. Start gateway with NO static workers ─────────────────────────
    println!("── 2. Starting gateway (no static workers) ──\n");

    let gw_addr: SocketAddr = "127.0.0.1:19803".parse()?;
    let gw_url = format!("http://{gw_addr}");
    tokio::spawn(async move {
        let _ = exec_gateway::serve(gw_addr, vec![]).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Query should fail — no workers registered yet.
    println!("   Sending query before any workers register...");
    match send_query_to(&gw_url, "SELECT 1", "", token).await {
        Ok(r) => println!("   Unexpected: {r}"),
        Err(e) => println!("   Expected failure: {e}\n"),
    }

    // ── 3. Register workers with database affinity ───────────────────────
    println!("── 3. Registering workers via RegisterWorker RPC ──\n");

    let mut gw_client = ExecutionServiceClient::connect(gw_url.clone()).await?;

    let reply_a = gw_client
        .register_worker(Request::new(WorkerRegistration {
            worker_id: "worker-a".into(),
            endpoint: format!("http://{worker1_addr}"),
            databases: vec!["analytics".into()],
            compute_context: "us-east-1".into(),
            max_concurrency: 8,
        }))
        .await?;
    println!(
        "   Worker A registered: accepted={}",
        reply_a.into_inner().accepted
    );

    let reply_b = gw_client
        .register_worker(Request::new(WorkerRegistration {
            worker_id: "worker-b".into(),
            endpoint: format!("http://{worker2_addr}"),
            databases: vec!["logging".into()],
            compute_context: "eu-west-1".into(),
            max_concurrency: 4,
        }))
        .await?;
    println!(
        "   Worker B registered: accepted={}",
        reply_b.into_inner().accepted
    );
    println!();

    // ── 4. Queries route by database affinity ────────────────────────────
    println!("── 4. Affinity routing ──\n");

    let r1 = send_query_to(&gw_url, "SELECT 'hello from analytics'", "analytics", token).await?;
    println!("   database=analytics → Worker A: {r1}");

    let r2 = send_query_to(&gw_url, "SELECT 'hello from logging'", "logging", token).await?;
    println!("   database=logging   → Worker B: {r2}");

    let r3 = send_query_to(&gw_url, "SELECT 'any worker'", "unknown_db", token).await?;
    println!("   database=unknown   → round-robin fallback: {r3}");

    let r4 = send_query_to(&gw_url, "SELECT 'empty db field'", "", token).await?;
    println!("   database=<empty>   → round-robin fallback: {r4}");
    println!();

    // ── 5. Graceful fallback when worker is unreachable ──────────────────
    println!("── 5. Graceful fallback ──\n");

    let unreachable_url = "http://127.0.0.1:19999";
    println!("   Trying hybrid join against unreachable worker ({unreachable_url})...");

    let result = hybrid::execute_hybrid_join_with_fallback(
        "SELECT * FROM (VALUES (1, 100)) r(id, w)",
        "CREATE TABLE l (id INT, v INT); INSERT INTO l VALUES (1, 10);",
        "SELECT COALESCE(SUM(l.v + __remote.w), 0) FROM l JOIN __remote ON l.id = __remote.id",
        Some("SELECT COALESCE(SUM(v), 0) FROM l"),
        unreachable_url,
        token,
    )
    .await;

    match result {
        Ok(val) => {
            println!("   Fallback result: {val}");
            println!("   (Worker unreachable → fell back to local-only query)\n");
        }
        Err(e) => {
            println!("   Error: {e}\n");
        }
    }

    println!("   Trying without fallback SQL...");
    let result_no_fb = hybrid::execute_hybrid_join_with_fallback(
        "SELECT 1",
        "",
        "SELECT 1",
        None,
        unreachable_url,
        token,
    )
    .await;

    match result_no_fb {
        Ok(val) => println!("   Unexpected success: {val}"),
        Err(e) => println!("   Expected error (no fallback configured): {e}\n"),
    }

    // ── 6. Hybrid join succeeds via registered worker ────────────────────
    println!("── 6. Hybrid join via registered worker ──\n");

    let result = hybrid::execute_hybrid_join(
        "SELECT id, w FROM (VALUES (1, 100), (2, 200)) r(id, w)",
        "CREATE TABLE orders (id INT, v INT); INSERT INTO orders VALUES (1, 10), (2, 20);",
        "SELECT COALESCE(SUM(orders.v + __remote.w), 0) FROM orders JOIN __remote ON orders.id = __remote.id",
        &format!("http://{worker1_addr}"),
        token,
    )
    .await;

    match result {
        Ok(sum) => {
            println!("   Hybrid join result: {sum}");
            println!("   Expected: 330 (10+100 + 20+200)");
            assert_eq!(sum, 330);
            println!("   PASS\n");
        }
        Err(e) => println!("   Error: {e}\n"),
    }

    println!("=== Done ===");
    Ok(())
}
