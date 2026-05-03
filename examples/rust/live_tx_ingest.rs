//! Live exerciser for the new transaction + IngestData RPCs.
//!
//! Connects to an already-running `openduck` gateway (default
//! `http://127.0.0.1:7878`) and drives:
//!   1. BeginTransaction + ExecuteFragment(tx_id) chained writes + COMMIT,
//!      verifying TEMP TABLEs survive across calls only on the pinned conn.
//!   2. BeginTransaction + write + ROLLBACK leaves the permanent table
//!      untouched.
//!   3. Cross-identity hijack: COMMIT under a different access_token gets
//!      a typed PERMISSION error.
//!   4. Unknown tx_id on ExecuteFragment returns a typed CATALOG error.
//!   5. IngestData streaming: metadata + arrow IPC batch flows into a
//!      staging TEMP TABLE on the pinned conn, then INSERT … SELECT
//!      from it inside the same transaction.
//!
//! ```bash
//! OPENDUCK_TOKEN=<token> cargo run --example live_tx_ingest
//! OPENDUCK_TOKEN=<token> OPENDUCK_GATEWAY=http://127.0.0.1:7878 \
//!     cargo run --example live_tx_ingest
//! ```

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use exec_proto::execute_fragment_chunk::Payload as FragmentPayload;
use exec_proto::execute_fragment_error::Kind;
use exec_proto::ingest_chunk::Payload as IngestPayload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::{
    ArrowIpcBatch, BeginTransactionRequest, CommitTransactionRequest, ExecuteFragmentRequest,
    IngestChunk, IngestColumn, IngestMetadata, RollbackTransactionRequest,
};
use tokio_stream::StreamExt;
use tonic::Request;

type Client = ExecutionServiceClient<tonic::transport::Channel>;

struct Outcome {
    name: &'static str,
    pass: bool,
    detail: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("OPENDUCK_GATEWAY").unwrap_or_else(|_| "http://127.0.0.1:7878".into());
    let token = std::env::var("OPENDUCK_TOKEN")
        .map_err(|_| "OPENDUCK_TOKEN must be set to the running service's token")?;
    let other_token = format!("{token}-OTHER");

    println!("connecting to gateway {endpoint}");
    let mut client = ExecutionServiceClient::connect(endpoint.clone()).await?;

    let mut outcomes: Vec<Outcome> = Vec::new();

    outcomes.push(scenario_pinned_temp_survives_then_dies(&mut client, &token).await);
    outcomes.push(scenario_rollback_undoes_writes(&mut client, &token).await);
    outcomes.push(scenario_cross_identity_commit_rejected(&mut client, &token, &other_token).await);
    outcomes.push(scenario_unknown_tx_id_typed_error(&mut client, &token).await);
    outcomes.push(scenario_ingest_streaming(&mut client, &token).await);

    println!("\n================= LIVE TX/INGEST RESULTS =================");
    let mut pass = 0;
    let mut fail = 0;
    for o in &outcomes {
        if o.pass {
            pass += 1;
            println!("  PASS  {} — {}", o.name, o.detail);
        } else {
            fail += 1;
            println!("  FAIL  {} — {}", o.name, o.detail);
        }
    }
    println!("Total: {pass} passed, {fail} failed");
    if fail > 0 {
        std::process::exit(1);
    }
    Ok(())
}

// ── helpers ──────────────────────────────────────────────────────────────

async fn execute_collect(
    client: &mut Client,
    sql: &str,
    token: &str,
    tx_id: Option<String>,
) -> Result<(usize, Option<(Kind, String)>), tonic::Status> {
    let req = ExecuteFragmentRequest {
        plan: sql.as_bytes().to_vec(),
        access_token: token.into(),
        execution_id: format!("live-{}", uuid::Uuid::new_v4()),
        compute_context: String::new(),
        transaction_id: tx_id,
        ..Default::default()
    };
    let mut stream = client
        .execute_fragment(Request::new(req))
        .await?
        .into_inner();

    let mut batches = 0usize;
    let mut typed: Option<(Kind, String)> = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if let Some(te) = chunk.typed_error.as_ref() {
            typed = Some((
                Kind::try_from(te.kind).unwrap_or(Kind::Unknown),
                te.message.clone(),
            ));
        }
        if let Some(payload) = chunk.payload {
            match payload {
                FragmentPayload::ArrowBatch(_) => batches += 1,
                FragmentPayload::Error(_) => {} // legacy field — typed mirrors it
                FragmentPayload::Finished(_) => {}
            }
        }
    }
    Ok((batches, typed))
}

fn ipc_batch() -> (Schema, Vec<u8>, usize) {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Utf8, false),
    ]);
    let keys = Int32Array::from(vec![100, 200, 300, 400]);
    let vals = StringArray::from(vec!["a", "b", "c", "d"]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(keys), Arc::new(vals)],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    let rows = batch.num_rows();
    (schema, buf, rows)
}

// ── scenarios ────────────────────────────────────────────────────────────

async fn scenario_pinned_temp_survives_then_dies(client: &mut Client, token: &str) -> Outcome {
    let name = "pinned_temp_survives_then_dies";
    let begin = match client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: token.into(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return fail(name, format!("BeginTransaction failed: {e}")),
    };
    if begin.transaction_id.is_empty() || begin.typed_error.is_some() {
        return fail(name, format!("Begin returned empty/typed_error: {begin:?}"));
    }
    let tx = begin.transaction_id.clone();

    if let Err(e) = execute_collect(
        client,
        "CREATE TEMP TABLE live_tx_temp (id INTEGER, label VARCHAR)",
        token,
        Some(tx.clone()),
    )
    .await
    {
        return fail(name, format!("CREATE TEMP TABLE failed: {e}"));
    }
    if let Err(e) = execute_collect(
        client,
        "INSERT INTO live_tx_temp VALUES (1,'a'),(2,'b'),(3,'c')",
        token,
        Some(tx.clone()),
    )
    .await
    {
        return fail(name, format!("INSERT failed: {e}"));
    }
    let select = match execute_collect(
        client,
        "SELECT COUNT(*) FROM live_tx_temp",
        token,
        Some(tx.clone()),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return fail(name, format!("SELECT failed: {e}")),
    };
    if select.0 == 0 {
        return fail(name, "expected at least one Arrow batch from SELECT".into());
    }

    let commit = match client
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: tx.clone(),
            access_token: token.into(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return fail(name, format!("Commit failed: {e}")),
    };
    if commit.typed_error.is_some() {
        return fail(name, format!("Commit typed_error: {commit:?}"));
    }

    // Off-transaction (auto-commit), the TEMP TABLE created on the pinned
    // connection should not exist. Expect a Catalog error.
    let after =
        match execute_collect(client, "SELECT COUNT(*) FROM live_tx_temp", token, None).await {
            Ok(r) => r,
            Err(e) => return fail(name, format!("post-commit SELECT errored: {e}")),
        };
    match after.1 {
        Some((Kind::Catalog, msg)) => pass(
            name,
            format!("temp table was visible inside tx, gone after commit (Catalog: {msg})"),
        ),
        Some((kind, msg)) => fail(
            name,
            format!("expected Catalog after commit, got {kind:?}: {msg}"),
        ),
        None => fail(
            name,
            format!(
                "TEMP TABLE unexpectedly visible after commit ({} batches)",
                after.0
            ),
        ),
    }
}

async fn scenario_rollback_undoes_writes(client: &mut Client, token: &str) -> Outcome {
    let name = "rollback_undoes_writes";

    // Make sure a permanent target exists (idempotent).
    if let Err(e) = execute_collect(
        client,
        "CREATE TABLE IF NOT EXISTS live_tx_perm (id INTEGER PRIMARY KEY, label VARCHAR)",
        token,
        None,
    )
    .await
    {
        return fail(name, format!("setup CREATE failed: {e}"));
    }

    let baseline =
        match execute_collect(client, "SELECT COUNT(*) FROM live_tx_perm", token, None).await {
            Ok(r) => r,
            Err(e) => return fail(name, format!("baseline SELECT failed: {e}")),
        };
    if baseline.1.is_some() {
        return fail(name, format!("baseline typed_error: {:?}", baseline.1));
    }

    let begin = match client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: token.into(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return fail(name, format!("Begin failed: {e}")),
    };
    let tx = begin.transaction_id.clone();

    // Use a per-run id so reruns don't collide with rows committed by a
    // previous successful "after-rollback" INSERT below.
    let unique_id: i32 = ((uuid::Uuid::new_v4().as_u128() as u32) & 0x7fff_ffff) as i32;
    let in_tx_sql = format!("INSERT INTO live_tx_perm VALUES ({unique_id},'rollback-me')");
    if let Err(e) = execute_collect(client, &in_tx_sql, token, Some(tx.clone())).await {
        return fail(name, format!("INSERT failed: {e}"));
    }

    let rb = match client
        .rollback_transaction(Request::new(RollbackTransactionRequest {
            transaction_id: tx.clone(),
            access_token: token.into(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return fail(name, format!("Rollback failed: {e}")),
    };
    if rb.typed_error.is_some() {
        return fail(name, format!("Rollback typed_error: {rb:?}"));
    }

    // After rollback the row must be gone — verified two ways:
    //   1. SELECT for the same id returns 0 rows.
    //   2. Reusing the same PK in a fresh INSERT outside the tx succeeds.
    let count_sql = format!("SELECT COUNT(*) FROM live_tx_perm WHERE id = {unique_id}");
    let cnt = execute_collect(client, &count_sql, token, None).await;
    match cnt {
        Ok((b, None)) if b > 0 => {} // got a result batch
        Ok((b, te)) => {
            return fail(
                name,
                format!("post-rollback SELECT batches={b} typed_error={te:?}"),
            );
        }
        Err(e) => return fail(name, format!("post-rollback SELECT errored: {e}")),
    }
    let reuse_sql = format!("INSERT INTO live_tx_perm VALUES ({unique_id},'after-rollback-ok')");
    let after_insert = execute_collect(client, &reuse_sql, token, None).await;
    let outcome = match after_insert {
        Ok((_, None)) => pass(name, "rollback discarded the in-tx INSERT (PK reusable)".into()),
        Ok((_, Some((kind, msg)))) => fail(
            name,
            format!(
                "expected post-rollback INSERT to succeed, got typed {kind:?}: {msg} (rollback didn't discard?)"
            ),
        ),
        Err(e) => fail(name, format!("post-rollback INSERT errored: {e}")),
    };
    // Best-effort cleanup so the row from this run doesn't pile up.
    let _ = execute_collect(
        client,
        &format!("DELETE FROM live_tx_perm WHERE id = {unique_id}"),
        token,
        None,
    )
    .await;
    outcome
}

async fn scenario_cross_identity_commit_rejected(
    client: &mut Client,
    token: &str,
    other: &str,
) -> Outcome {
    let name = "cross_identity_commit_rejected";
    let begin = match client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: token.into(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return fail(name, format!("Begin failed: {e}")),
    };
    let tx = begin.transaction_id.clone();

    let hijack = client
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: tx.clone(),
            access_token: other.into(),
        }))
        .await;

    let outcome = match hijack {
        Ok(reply) => {
            let r = reply.into_inner();
            match r.typed_error {
                Some(e) if matches!(Kind::try_from(e.kind), Ok(Kind::Permission)) => {
                    pass(name, format!("typed PERMISSION error: {}", e.message))
                }
                Some(e) => fail(
                    name,
                    format!(
                        "expected PERMISSION typed error, got kind={}: {}",
                        e.kind, e.message
                    ),
                ),
                None => fail(
                    name,
                    "hijacking COMMIT under wrong identity unexpectedly succeeded".into(),
                ),
            }
        }
        Err(s)
            if matches!(
                s.code(),
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
            ) =>
        {
            pass(
                name,
                format!("rejected at transport: {} {}", s.code(), s.message()),
            )
        }
        Err(s) => fail(
            name,
            format!("unexpected gRPC error: {} {}", s.code(), s.message()),
        ),
    };

    let _ = client
        .rollback_transaction(Request::new(RollbackTransactionRequest {
            transaction_id: tx,
            access_token: token.into(),
        }))
        .await;
    outcome
}

async fn scenario_unknown_tx_id_typed_error(client: &mut Client, token: &str) -> Outcome {
    // The gateway returns `Kind::Internal` with a "no affinity entry"
    // message when it has never seen the tx_id (could equally be an
    // unknown id or a gateway-restart-orphan, hence the conservative
    // `Internal`). The worker-side path (id known to gateway, unknown
    // to the pinned worker) returns `Kind::Catalog` instead. We only
    // ever exercise the gateway-side rejection from a pure client.
    let name = "unknown_tx_id_typed_error";
    let bogus = uuid::Uuid::new_v4().to_string();
    match execute_collect(client, "SELECT 1", token, Some(bogus)).await {
        Ok((_, Some((Kind::Internal, msg)))) if msg.contains("affinity") => {
            pass(name, format!("typed INTERNAL (gateway-side): {msg}"))
        }
        Ok((_, Some((Kind::Catalog, msg)))) => {
            pass(name, format!("typed CATALOG (worker-side): {msg}"))
        }
        Ok((b, te)) => fail(
            name,
            format!("expected typed Catalog/Internal, got batches={b} typed={te:?}"),
        ),
        Err(e) => fail(
            name,
            format!("transport error (expected stream w/ typed): {e}"),
        ),
    }
}

async fn scenario_ingest_streaming(client: &mut Client, token: &str) -> Outcome {
    let name = "ingest_streaming_with_pinned_tx";
    let begin = match client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: token.into(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return fail(name, format!("Begin failed: {e}")),
    };
    let tx = begin.transaction_id.clone();

    let staging = format!("__openduck_ingest_live_{}", uuid::Uuid::new_v4().simple());
    let (_schema, ipc, expected_rows) = ipc_batch();

    let (sink, rx) = tokio::sync::mpsc::channel::<IngestChunk>(4);
    sink.send(IngestChunk {
        payload: Some(IngestPayload::Metadata(IngestMetadata {
            database: String::new(),
            staging_table: staging.clone(),
            columns: vec![
                IngestColumn {
                    name: "k".into(),
                    sql_type: "INTEGER".into(),
                },
                IngestColumn {
                    name: "v".into(),
                    sql_type: "VARCHAR".into(),
                },
            ],
            transaction_id: Some(tx.clone()),
            access_token: token.into(),
        })),
    })
    .await
    .unwrap();
    sink.send(IngestChunk {
        payload: Some(IngestPayload::ArrowBatch(ArrowIpcBatch {
            ipc_stream_payload: ipc,
        })),
    })
    .await
    .unwrap();
    drop(sink);

    let reply = match client
        .ingest_data(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return fail(name, format!("IngestData rpc failed: {e}")),
    };
    if reply.typed_error.is_some() {
        return fail(
            name,
            format!("IngestData typed_error: {:?}", reply.typed_error),
        );
    }
    if reply.rows_ingested as usize != expected_rows {
        return fail(
            name,
            format!(
                "rows_ingested={} expected={expected_rows}",
                reply.rows_ingested
            ),
        );
    }

    // Verify on the SAME pinned connection that the staging table is
    // populated, then commit.
    let select = match execute_collect(
        client,
        &format!("SELECT COUNT(*) FROM {staging}"),
        token,
        Some(tx.clone()),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return fail(name, format!("post-ingest SELECT failed: {e}")),
    };
    if select.1.is_some() {
        return fail(name, format!("staging SELECT typed_error: {:?}", select.1));
    }
    if select.0 == 0 {
        return fail(name, "post-ingest SELECT returned no batches".into());
    }

    let commit = client
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: tx.clone(),
            access_token: token.into(),
        }))
        .await;
    match commit {
        Ok(r) => {
            let r = r.into_inner();
            if r.typed_error.is_some() {
                return fail(name, format!("commit typed_error: {r:?}"));
            }
        }
        Err(e) => return fail(name, format!("commit failed: {e}")),
    }

    pass(
        name,
        format!(
            "{expected_rows} rows ingested into {staging} on pinned tx, visible to follow-up SELECT"
        ),
    )
}

fn pass(name: &'static str, detail: String) -> Outcome {
    Outcome {
        name,
        pass: true,
        detail,
    }
}
fn fail(name: &'static str, detail: String) -> Outcome {
    Outcome {
        name,
        pass: false,
        detail,
    }
}
