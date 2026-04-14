//! # Hybrid Execution Example
//!
//! End-to-end demonstration of hybrid LOCAL + REMOTE query execution.
//! Starts a worker with remote data, then uses the gateway's hybrid engine
//! to join local data with remote data fetched via Arrow IPC.
//!
//! ```bash
//! OPENDUCK_TOKEN=demo cargo run --example hybrid_execution
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use exec_gateway::hybrid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenDuck Hybrid Execution ===\n");

    let token = "hybrid-demo-token";
    std::env::set_var("OPENDUCK_TOKEN", token);

    let worker_addr: SocketAddr = "127.0.0.1:19900".parse()?;
    let worker_url = format!("http://{worker_addr}");

    // Start a worker with some test data.
    println!("1. Starting worker on {worker_addr}...");
    tokio::spawn(async move {
        let _ = exec_worker::serve(worker_addr).await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("   Worker ready.\n");

    // ── Plan annotation ─────────────────────────────────────────────────
    println!("2. Building hybrid plan tree...\n");

    let plan = hybrid::PlanNode {
        id: 1,
        kind: hybrid::NodeKind::HashJoin {
            condition: "l.id = r.id".into(),
        },
        placement: hybrid::Placement::Local,
        children: vec![
            hybrid::PlanNode {
                id: 2,
                kind: hybrid::NodeKind::Scan {
                    table: "local_orders".into(),
                },
                placement: hybrid::Placement::Local,
                children: vec![],
            },
            hybrid::PlanNode {
                id: 3,
                kind: hybrid::NodeKind::Scan {
                    table: "remote_lineitem".into(),
                },
                placement: hybrid::Placement::Remote,
                children: vec![],
            },
        ],
    };

    let with_bridges = hybrid::insert_bridges(plan);
    let explain = hybrid::explain_annotated(&with_bridges);
    println!("   EXPLAIN (annotated):");
    for line in explain.lines() {
        println!("     {line}");
    }
    println!();

    // ── Actual hybrid execution ─────────────────────────────────────────
    println!("3. Executing hybrid join...\n");
    println!("   Remote fragment: SELECT id, w FROM (VALUES (1, 100), (3, 300)) r(id, w)");
    println!("   Local data:      INSERT INTO l VALUES (1, 10), (2, 20)");
    println!("   Join:            SELECT COALESCE(SUM(l.v + __remote.w), 0) FROM l JOIN __remote ON l.id = __remote.id\n");

    let result = hybrid::execute_hybrid_join(
        "SELECT id, w FROM (VALUES (1, 100), (3, 300)) r(id, w)",
        "CREATE TABLE l (id INT, v INT); INSERT INTO l VALUES (1, 10), (2, 20);",
        "SELECT COALESCE(SUM(l.v + __remote.w), 0) FROM l JOIN __remote ON l.id = __remote.id",
        &worker_url,
        token,
    )
    .await;

    match result {
        Ok(sum) => {
            println!("   Result: {sum}");
            println!("   Expected: 110 (only id=1 matches: 10 + 100 = 110)");
            assert_eq!(sum, 110, "golden parity check failed");
            println!("   PASS: hybrid result matches single-process baseline.\n");
        }
        Err(e) => {
            eprintln!("   ERROR: {e}");
            return Err(e.into());
        }
    }

    // ── Summary ─────────────────────────────────────────────────────────
    println!("4. How it works:");
    println!("   a) Gateway sends REMOTE SQL to worker via gRPC");
    println!("   b) Worker executes in DuckDB, streams Arrow IPC batches back");
    println!("   c) Gateway materializes remote batches into a temp table (__remote)");
    println!("   d) Gateway runs the LOCAL join SQL against local + materialized data");
    println!("   e) Result matches what a single-process DuckDB would produce\n");

    println!("=== Done ===");
    Ok(())
}
