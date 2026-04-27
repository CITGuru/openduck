//! End-to-end transaction lifecycle tests (Begin → Execute → Commit/Rollback)
//! exercising the full gateway → worker path over real gRPC.

mod common;

use std::time::Duration;

use exec_proto::execute_fragment_error::Kind;
use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::{
    BeginTransactionRequest, CommitTransactionRequest, ExecuteFragmentRequest,
    RollbackTransactionRequest,
};
use exec_worker::WorkerConfig;
use tonic::Request;

use crate::common::{Stack, TOKEN};

/// Pull every chunk off a streaming ExecuteFragment response and return
/// a `(saw_batch, typed_error, legacy_error)` summary. Panics on
/// transport errors (not on worker-side typed errors — those flow via
/// `typed_error`).
async fn drain_stream(
    mut stream: tonic::Streaming<exec_proto::ExecuteFragmentChunk>,
) -> (
    bool,
    Option<exec_proto::ExecuteFragmentError>,
    Option<String>,
) {
    let mut saw_batch = false;
    let mut typed: Option<exec_proto::ExecuteFragmentError> = None;
    let mut legacy: Option<String> = None;
    while let Some(chunk) = stream.message().await.expect("stream message") {
        if let Some(te) = chunk.typed_error.clone() {
            typed = Some(te);
        }
        match chunk.payload {
            Some(Payload::ArrowBatch(b)) if !b.ipc_stream_payload.is_empty() => {
                saw_batch = true;
            }
            Some(Payload::Error(e)) => legacy = Some(e),
            _ => {}
        }
    }
    (saw_batch, typed, legacy)
}

async fn run_sql_in_txn(
    client: &mut exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient<
        tonic::transport::Channel,
    >,
    sql: &str,
    tx_id: &str,
) -> (
    bool,
    Option<exec_proto::ExecuteFragmentError>,
    Option<String>,
) {
    let stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: sql.as_bytes().to_vec(),
            access_token: TOKEN.into(),
            transaction_id: Some(tx_id.to_string()),
            ..Default::default()
        }))
        .await
        .expect("execute_fragment")
        .into_inner();
    drain_stream(stream).await
}

#[tokio::test]
async fn begin_execute_commit_happy_path() {
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    let begin = client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: TOKEN.into(),
        }))
        .await
        .expect("begin")
        .into_inner();
    assert!(
        begin.typed_error.is_none(),
        "begin failed: {:?}",
        begin.typed_error
    );
    assert!(!begin.transaction_id.is_empty());

    // DDL + INSERT + SELECT inside the transaction.
    let (_, err, _) = run_sql_in_txn(
        &mut client,
        "CREATE TABLE kv (k INT, v VARCHAR)",
        &begin.transaction_id,
    )
    .await;
    assert!(err.is_none(), "CREATE TABLE: {err:?}");

    let (_, err, _) = run_sql_in_txn(
        &mut client,
        "INSERT INTO kv VALUES (1, 'alpha'), (2, 'beta')",
        &begin.transaction_id,
    )
    .await;
    assert!(err.is_none(), "INSERT: {err:?}");

    let (saw, err, _) = run_sql_in_txn(
        &mut client,
        "SELECT COUNT(*) FROM kv",
        &begin.transaction_id,
    )
    .await;
    assert!(err.is_none(), "SELECT: {err:?}");
    assert!(saw, "expected batch from SELECT");

    let commit = client
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: begin.transaction_id.clone(),
            access_token: TOKEN.into(),
        }))
        .await
        .expect("commit")
        .into_inner();
    assert!(
        commit.typed_error.is_none(),
        "commit failed: {:?}",
        commit.typed_error
    );
}

#[tokio::test]
async fn rollback_discards_worker_state() {
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    let begin = client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap()
        .into_inner();

    let (_, err, _) = run_sql_in_txn(
        &mut client,
        "CREATE TABLE ephemeral (x INT)",
        &begin.transaction_id,
    )
    .await;
    assert!(err.is_none(), "create: {err:?}");

    let rollback = client
        .rollback_transaction(Request::new(RollbackTransactionRequest {
            transaction_id: begin.transaction_id.clone(),
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        rollback.typed_error.is_none(),
        "rollback failed: {:?}",
        rollback.typed_error
    );

    // A fresh auto-commit call must NOT see the rolled-back table. We
    // assert that SELECT returns a CATALOG-kind typed error ("table
    // ephemeral does not exist").
    let (_, err, _) = {
        let stream = client
            .execute_fragment(Request::new(ExecuteFragmentRequest {
                plan: b"SELECT * FROM ephemeral".to_vec(),
                access_token: TOKEN.into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        drain_stream(stream).await
    };
    let err = err.expect("expected typed error on rolled-back table");
    assert_eq!(err.kind, Kind::Catalog as i32, "actual: {err:?}");
}

#[tokio::test]
async fn cross_identity_hijack_rejected_at_gateway() {
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    // Alice opens a transaction — the gateway stores her identity.
    let begin = client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap()
        .into_inner();

    // Bob presents Alice's transaction_id with his own (wrong) token.
    // With OPENDUCK_TOKEN set, the token validator runs BEFORE the
    // affinity check and rejects Bob outright. Temporarily clear the
    // env var so `validate_token` accepts any value — that lets us
    // verify the affinity-level identity check is the actual gate.
    std::env::remove_var("OPENDUCK_TOKEN");
    let bob = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1".to_vec(),
            access_token: "bob-token".into(),
            transaction_id: Some(begin.transaction_id.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    std::env::set_var("OPENDUCK_TOKEN", TOKEN);

    let (_, err, _) = drain_stream(bob).await;
    let err = err.expect("expected typed PERMISSION error");
    assert_eq!(err.kind, Kind::Permission as i32, "actual: {err:?}");
    assert!(err.message.contains("different caller"));

    // Original owner can still use the transaction.
    let (_, alice_err, _) = run_sql_in_txn(&mut client, "SELECT 1", &begin.transaction_id).await;
    assert!(alice_err.is_none());

    // Cleanup.
    client
        .rollback_transaction(Request::new(RollbackTransactionRequest {
            transaction_id: begin.transaction_id,
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn unknown_tx_id_rejected_at_gateway() {
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    let (_, err, _) = run_sql_in_txn(&mut client, "SELECT 1", "00000000-nope-not-a-txn").await;
    let err = err.expect("expected typed error");
    // Gateway emits INTERNAL ("no affinity entry") for an unknown tx_id
    // because it's a routing failure, not a worker catalog miss.
    assert_eq!(err.kind, Kind::Internal as i32, "actual: {err:?}");
    assert!(err.message.contains("unknown transaction"));
}

#[tokio::test]
async fn unknown_tx_id_rejected_at_worker() {
    // Bypassing the gateway — speak directly to the worker to exercise
    // its authoritative check. Any routing failure the gateway would
    // produce is out of scope here; we want the worker's own verdict.
    let stack = Stack::worker_only(WorkerConfig::default()).await;
    let mut client = stack.client().await;

    let stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1".to_vec(),
            access_token: TOKEN.into(),
            transaction_id: Some("bogus".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let (_, err, _) = drain_stream(stream).await;
    let err = err.expect("expected typed error");
    // Worker emits CATALOG for unknown txn — consistent with
    // `acquire()`'s return in `transactions::ConnectionRegistry`.
    assert_eq!(err.kind, Kind::Catalog as i32, "actual: {err:?}");
}

#[tokio::test]
async fn begin_commit_cycle_leak() {
    // Scaled-down registry-leak test: 200 cycles of Begin/Commit must
    // return the worker's connection registry to size zero.
    let stack = Stack::worker_only(WorkerConfig::default()).await;
    let mut client = stack.client().await;

    for i in 0..200 {
        let begin = client
            .begin_transaction(Request::new(BeginTransactionRequest {
                database: String::new(),
                access_token: TOKEN.into(),
            }))
            .await
            .unwrap_or_else(|e| panic!("begin {i}: {e}"))
            .into_inner();
        let commit = client
            .commit_transaction(Request::new(CommitTransactionRequest {
                transaction_id: begin.transaction_id,
                access_token: TOKEN.into(),
            }))
            .await
            .unwrap_or_else(|e| panic!("commit {i}: {e}"))
            .into_inner();
        assert!(
            commit.typed_error.is_none(),
            "commit {i}: {:?}",
            commit.typed_error
        );
    }

    // Give the worker a beat to drop the last entry.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // There's no public API to read the registry from outside the
    // process, so we probe by opening one more transaction and
    // confirming it still succeeds — a leak would eventually exhaust
    // DuckDB's connection budget or surface as BEGIN failures well
    // before 200 cycles.
    let probe = client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: TOKEN.into(),
        }))
        .await
        .expect("probe begin")
        .into_inner();
    assert!(probe.typed_error.is_none());
    client
        .rollback_transaction(Request::new(RollbackTransactionRequest {
            transaction_id: probe.transaction_id,
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap();
}
