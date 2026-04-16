//! # Federation Provider & Context-Aware Routing Example
//!
//! Demonstrates:
//! - The `FederationProvider` trait for self-describing workers
//! - Context-aware routing: gateway prefers workers matching `compute_context`
//! - `TableSourceRegistry::sync_from_providers()` to unify live workers with planner
//! - 4-tier routing priority: context+db > context > db > round-robin
//!
//! ```bash
//! cargo run --example federation_provider
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::{ExecuteFragmentRequest, WorkerRegistration};
use tonic::Request;

use exec_gateway::hybrid::{self, FederationProvider, TableSource, TableSourceRegistry};
use exec_gateway::RegisteredWorker;

async fn query_with_context(
    gateway: &str,
    sql: &str,
    database: &str,
    compute_context: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut client = ExecutionServiceClient::connect(gateway.to_string()).await?;
    let request = ExecuteFragmentRequest {
        plan: sql.as_bytes().to_vec(),
        database: database.into(),
        compute_context: compute_context.into(),
        ..Default::default()
    };
    let mut stream = client
        .execute_fragment(Request::new(request))
        .await?
        .into_inner();

    let mut payload = Vec::new();
    while let Some(chunk) = stream.message().await? {
        match chunk.payload {
            Some(Payload::ArrowBatch(b)) => payload.extend(b.ipc_stream_payload),
            Some(Payload::Error(e)) => return Err(e.into()),
            Some(Payload::Finished(true)) => break,
            _ => {}
        }
    }
    Ok(payload)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenDuck FederationProvider & Context-Aware Routing ===\n");

    // ── 1. FederationProvider trait ──────────────────────────────────────
    println!("── 1. FederationProvider trait ──\n");
    println!("   RegisteredWorker implements FederationProvider, exposing:");
    println!("   id(), endpoint(), compute_context(), databases(), tables(),");
    println!("   max_concurrency(), supports_sql()\n");

    let worker_us = RegisteredWorker {
        worker_id: "w-us-1".into(),
        endpoint: "http://10.0.1.5:9898".into(),
        databases: vec!["analytics".into()],
        compute_context: "us-east-1".into(),
        max_concurrency: 8,
        tables: vec!["sales".into(), "orders".into(), "products".into()],
        last_heartbeat: std::time::Instant::now(),
    };
    let worker_eu = RegisteredWorker {
        worker_id: "w-eu-1".into(),
        endpoint: "http://10.0.2.5:9898".into(),
        databases: vec!["logging".into()],
        compute_context: "eu-west-1".into(),
        max_concurrency: 4,
        tables: vec!["events".into(), "metrics".into()],
        last_heartbeat: std::time::Instant::now(),
    };

    println!(
        "   Worker US: id={}, ctx={}, tables={:?}",
        worker_us.id(),
        worker_us.compute_context(),
        worker_us.tables()
    );
    println!(
        "   Worker EU: id={}, ctx={}, tables={:?}",
        worker_eu.id(),
        worker_eu.compute_context(),
        worker_eu.tables()
    );

    // ── 2. sync_from_providers ──────────────────────────────────────────
    println!("\n── 2. Syncing live workers into TableSourceRegistry ──\n");

    let mut table_reg = TableSourceRegistry::new();
    let providers: Vec<&dyn FederationProvider> = vec![&worker_us, &worker_eu];
    table_reg.sync_from_providers(&providers);

    println!("   sales remote?    {}", table_reg.is_remote("sales"));
    println!("   events remote?   {}", table_reg.is_remote("events"));
    println!("   local_tab remote? {}", table_reg.is_remote("local_tab"));
    println!(
        "   sales & orders co-located? {}",
        table_reg.are_co_located("sales", "orders")
    );
    println!(
        "   sales & events co-located? {}",
        table_reg.are_co_located("sales", "events")
    );
    println!(
        "   us-east-1 tables: {:?}",
        table_reg
            .tables_for_context("us-east-1")
            .iter()
            .map(|t| &t.table)
            .collect::<Vec<_>>()
    );

    // ── 3. Compute pushdown with live worker data ───────────────────────
    println!("\n── 3. Compute pushdown using live worker metadata ──\n");

    let mut plan = hybrid::PlanNode {
        id: 1,
        kind: hybrid::NodeKind::HashJoin {
            condition: "s.product_id = p.id".into(),
        },
        placement: hybrid::Placement::Remote,
        children: vec![
            hybrid::PlanNode {
                id: 2,
                kind: hybrid::NodeKind::Filter {
                    predicate: "s.amount > 50".into(),
                },
                placement: hybrid::Placement::Remote,
                children: vec![hybrid::PlanNode {
                    id: 3,
                    kind: hybrid::NodeKind::Scan {
                        table: "sales".into(),
                    },
                    placement: hybrid::Placement::Remote,
                    children: vec![],
                }],
            },
            hybrid::PlanNode {
                id: 4,
                kind: hybrid::NodeKind::Scan {
                    table: "products".into(),
                },
                placement: hybrid::Placement::Remote,
                children: vec![],
            },
        ],
    };

    println!("   Before pushdown:");
    println!("   {}", hybrid::explain_annotated(&plan));

    hybrid::pushdown_federable_subplans(&mut plan, &table_reg);

    println!("   After pushdown (co-located → single RemoteSql):");
    println!("   {}", hybrid::explain_annotated(&plan));

    // ── 4. Context-aware routing ────────────────────────────────────────
    println!("── 4. Context-aware WorkerRegistry routing ──\n");

    let worker_a_addr: SocketAddr = "127.0.0.1:19901".parse()?;
    let worker_b_addr: SocketAddr = "127.0.0.1:19902".parse()?;
    let gw_addr: SocketAddr = "127.0.0.1:19903".parse()?;

    tokio::spawn(async move {
        let _ = exec_worker::serve(worker_a_addr).await;
    });
    tokio::spawn(async move {
        let _ = exec_worker::serve(worker_b_addr).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let workers: Vec<String> = vec![];
    tokio::spawn(async move {
        let _ = exec_gateway::serve(gw_addr, workers).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let gateway = format!("http://{gw_addr}");
    let mut client = ExecutionServiceClient::connect(gateway.clone()).await?;

    client
        .register_worker(Request::new(WorkerRegistration {
            worker_id: "w-us".into(),
            endpoint: format!("http://{worker_a_addr}"),
            databases: vec!["analytics".into()],
            compute_context: "us-east-1".into(),
            max_concurrency: 8,
            tables: vec!["sales".into()],
            ..Default::default()
        }))
        .await?;

    client
        .register_worker(Request::new(WorkerRegistration {
            worker_id: "w-eu".into(),
            endpoint: format!("http://{worker_b_addr}"),
            databases: vec!["logging".into()],
            compute_context: "eu-west-1".into(),
            max_concurrency: 4,
            tables: vec!["events".into()],
            ..Default::default()
        }))
        .await?;

    println!("   Registered w-us (us-east-1, analytics) and w-eu (eu-west-1, logging)");

    // Tier 1: context + database match
    let r = query_with_context(&gateway, "SELECT 'tier1'", "analytics", "us-east-1").await?;
    println!(
        "   context=us-east-1, db=analytics → {} bytes (Tier 1: ctx+db)",
        r.len()
    );

    // Tier 2: context match only
    let r = query_with_context(&gateway, "SELECT 'tier2'", "", "eu-west-1").await?;
    println!(
        "   context=eu-west-1, db=<empty>   → {} bytes (Tier 2: ctx only)",
        r.len()
    );

    // Tier 3: database match only
    let r = query_with_context(&gateway, "SELECT 'tier3'", "logging", "").await?;
    println!(
        "   context=<empty>,  db=logging     → {} bytes (Tier 3: db only)",
        r.len()
    );

    // Tier 4: round-robin
    let r = query_with_context(&gateway, "SELECT 'tier4'", "", "").await?;
    println!(
        "   context=<empty>,  db=<empty>     → {} bytes (Tier 4: round-robin)",
        r.len()
    );

    // ── 5. Manual TableSource registration still works ──────────────────
    println!("\n── 5. Manual TableSource registration ──\n");

    let mut manual_reg = TableSourceRegistry::new();
    manual_reg.register(TableSource {
        table: "custom_table".into(),
        provider: "custom-worker".into(),
        compute_context: "on-prem".into(),
        database: "warehouse".into(),
    });

    manual_reg.sync_from_providers(&providers);

    println!(
        "   custom_table remote? {} (manually registered)",
        manual_reg.is_remote("custom_table")
    );
    println!(
        "   sales remote?        {} (from live workers)",
        manual_reg.is_remote("sales")
    );
    println!("   Both sources coexist in the same registry.\n");

    println!("=== Done ===");
    Ok(())
}
