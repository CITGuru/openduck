//! End-to-end tests for the worker idle reaper and cross-database
//! transaction-id binding.

mod common;

use std::time::Duration;

use exec_proto::execute_fragment_error::Kind;
use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::{
    BeginTransactionRequest, CommitTransactionRequest, ExecuteFragmentRequest,
};
use exec_worker::WorkerConfig;
use tonic::Request;

use crate::common::{Stack, TOKEN};

#[tokio::test]
async fn idle_reaper_rolls_back_abandoned_transaction() {
    // Use a short idle timeout so we don't need a long test.
    // REAPER_INTERVAL is 15s in code; override the idle timeout to 1s
    // and sleep 2s. The reaper scans once every 15s, so we poll for
    // up to 30s before concluding it didn't fire.
    let config = WorkerConfig {
        transaction_idle_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let stack = Stack::worker_only(config).await;
    let mut client = stack.client().await;

    let begin = client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap()
        .into_inner();
    let tx_id = begin.transaction_id.clone();
    assert!(!tx_id.is_empty());

    // Leave the transaction idle. The reaper scans every 15s so we
    // need to wait slightly longer than that.
    tokio::time::sleep(Duration::from_secs(17)).await;

    // Using the reaped tx_id must now return typed CATALOG
    // ("unknown transaction — likely idle-reaped"). We check the
    // message explicitly since the kind alone doesn't prove which
    // code path produced it.
    let stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1".to_vec(),
            access_token: TOKEN.into(),
            transaction_id: Some(tx_id.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let (_typed, err) = drain(stream).await;
    let err = err.expect("expected typed error");
    assert_eq!(err.kind, Kind::Catalog as i32, "actual: {err:?}");
    assert!(
        err.message.contains("unknown transaction") || err.message.contains("idle-reaped"),
        "message: {}",
        err.message,
    );

    // Commit on a reaped tx_id also surfaces a typed error (not a
    // silent success).
    let commit = client
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: tx_id,
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap()
        .into_inner();
    let err = commit
        .typed_error
        .expect("expected typed error on reaped commit");
    assert_eq!(err.kind, Kind::Catalog as i32);
}

async fn drain(
    mut stream: tonic::Streaming<exec_proto::ExecuteFragmentChunk>,
) -> (bool, Option<exec_proto::ExecuteFragmentError>) {
    let mut saw_batch = false;
    let mut err = None;
    while let Some(chunk) = stream.message().await.unwrap() {
        if let Some(te) = chunk.typed_error.clone() {
            err = Some(te);
        }
        match chunk.payload {
            Some(Payload::ArrowBatch(b)) if !b.ipc_stream_payload.is_empty() => {
                saw_batch = true;
            }
            _ => {}
        }
    }
    (saw_batch, err)
}

#[tokio::test]
async fn cross_database_transaction_reuse_is_binder() {
    // Workers accept any `database` by default (the in-memory DuckDB
    // handles the string however it likes), so the interesting check
    // is: a tx opened on db="A" and reused on db="B" must be rejected
    // by the worker's `acquire()` with typed BINDER.
    let stack = Stack::worker_only(WorkerConfig::default()).await;
    let mut client = stack.client().await;

    let begin = client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: "db-a".into(),
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap()
        .into_inner();

    let stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1".to_vec(),
            access_token: TOKEN.into(),
            database: "db-b".into(),
            transaction_id: Some(begin.transaction_id.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let (_, err) = drain(stream).await;
    let err = err.expect("expected typed error");
    assert_eq!(err.kind, Kind::Binder as i32, "actual: {err:?}");
    assert!(
        err.message.contains("bound to database") || err.message.contains("different"),
        "message: {}",
        err.message,
    );

    // Original database still works.
    let stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1".to_vec(),
            access_token: TOKEN.into(),
            database: "db-a".into(),
            transaction_id: Some(begin.transaction_id.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let (saw, err) = drain(stream).await;
    assert!(err.is_none(), "reuse on same db failed: {err:?}");
    assert!(saw);

    // Cleanup.
    client
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: begin.transaction_id,
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap();
}
