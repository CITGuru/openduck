//! End-to-end typed-error classification (attach-mutations design §6).
//!
//! Triggers each `ExecuteFragmentError::Kind` via a real worker and
//! asserts both the typed field and the legacy `string error` are
//! populated in lockstep.

mod common;

use exec_proto::execute_fragment_error::Kind;
use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::ExecuteFragmentRequest;
use tonic::Request;

use crate::common::{Stack, TOKEN};

async fn run_sql(
    stack: &Stack,
    sql: &str,
) -> (Option<exec_proto::ExecuteFragmentError>, Option<String>) {
    let mut client = stack.gateway_client().await;
    let mut stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: sql.as_bytes().to_vec(),
            access_token: TOKEN.into(),
            ..Default::default()
        }))
        .await
        .expect("execute_fragment")
        .into_inner();

    let mut typed: Option<exec_proto::ExecuteFragmentError> = None;
    let mut legacy: Option<String> = None;
    while let Some(chunk) = stream.message().await.expect("message") {
        if let Some(te) = chunk.typed_error.clone() {
            typed = Some(te);
        }
        if let Some(Payload::Error(e)) = chunk.payload {
            legacy = Some(e);
        }
    }
    (typed, legacy)
}

/// Assert that typed_error is present AND the legacy string error is
/// present AND they carry the same message — the "additivity invariant"
/// so downgraded (pre-typed-error) clients still see the same prose.
fn assert_additive(
    typed: Option<exec_proto::ExecuteFragmentError>,
    legacy: Option<String>,
    expected_kind: Kind,
) -> exec_proto::ExecuteFragmentError {
    let typed = typed.expect("typed_error must be populated");
    let legacy = legacy.expect("legacy string `error` must be populated");
    assert_eq!(
        typed.kind, expected_kind as i32,
        "expected kind={expected_kind:?}, got {typed:?}"
    );
    assert_eq!(typed.message, legacy, "typed.message must equal legacy");
    typed
}

#[tokio::test]
async fn unknown_table_is_catalog() {
    let stack = Stack::new().await;
    let (typed, legacy) = run_sql(&stack, "SELECT * FROM does_not_exist").await;
    let err = assert_additive(typed, legacy, Kind::Catalog);
    assert!(
        err.message.to_lowercase().contains("does_not_exist")
            || err.message.to_lowercase().contains("catalog"),
        "unexpected message: {}",
        err.message
    );
}

#[tokio::test]
async fn syntax_error_is_parser() {
    let stack = Stack::new().await;
    let (typed, legacy) = run_sql(&stack, "SELEKT 1").await;
    assert_additive(typed, legacy, Kind::Parser);
}

#[tokio::test]
async fn bad_cast_is_conversion() {
    let stack = Stack::new().await;
    let (typed, legacy) = run_sql(&stack, "SELECT CAST('foo' AS INTEGER)").await;
    assert_additive(typed, legacy, Kind::Conversion);
}

#[tokio::test]
async fn duplicate_primary_key_is_constraint() {
    let stack = Stack::new().await;
    // We need PK + duplicate INSERT in a single connection for the
    // second insert to conflict with the first. A transaction gets us
    // both statements on the same worker connection.
    use exec_proto::openduck::v1::{
        BeginTransactionRequest, CommitTransactionRequest, RollbackTransactionRequest,
    };
    let mut client = stack.gateway_client().await;
    let begin = client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap()
        .into_inner();

    // Helper: run SQL on this transaction and collect typed + legacy.
    async fn run_tx(
        client: &mut exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient<
            tonic::transport::Channel,
        >,
        tx: &str,
        sql: &str,
    ) -> (Option<exec_proto::ExecuteFragmentError>, Option<String>) {
        let mut stream = client
            .execute_fragment(Request::new(ExecuteFragmentRequest {
                plan: sql.as_bytes().to_vec(),
                access_token: TOKEN.into(),
                transaction_id: Some(tx.to_string()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let mut typed = None;
        let mut legacy = None;
        while let Some(chunk) = stream.message().await.unwrap() {
            if let Some(te) = chunk.typed_error.clone() {
                typed = Some(te);
            }
            if let Some(Payload::Error(e)) = chunk.payload {
                legacy = Some(e);
            }
        }
        (typed, legacy)
    }

    let (err, _) = run_tx(
        &mut client,
        &begin.transaction_id,
        "CREATE TABLE dup (id INT PRIMARY KEY)",
    )
    .await;
    assert!(err.is_none(), "CREATE TABLE failed: {err:?}");

    let (err, _) = run_tx(
        &mut client,
        &begin.transaction_id,
        "INSERT INTO dup VALUES (1)",
    )
    .await;
    assert!(err.is_none(), "first INSERT failed: {err:?}");

    let (typed, legacy) = run_tx(
        &mut client,
        &begin.transaction_id,
        "INSERT INTO dup VALUES (1)",
    )
    .await;
    let _ = client
        .rollback_transaction(Request::new(RollbackTransactionRequest {
            transaction_id: begin.transaction_id.clone(),
            access_token: TOKEN.into(),
        }))
        .await;
    let _ = (CommitTransactionRequest::default(),);

    assert_additive(typed, legacy, Kind::Constraint);
}

#[tokio::test]
async fn every_error_path_populates_both_fields() {
    // The proto-additivity invariant: no matter how the error arose
    // (classified or otherwise), both the typed field and the legacy
    // `string error` must be populated so pre-typed-error clients keep
    // working unchanged.
    let stack = Stack::new().await;
    for sql in [
        "SELECT * FROM does_not_exist",                   // Catalog
        "SELEKT 1",                                       // Parser
        "SELECT CAST('foo' AS INTEGER)",                  // Conversion
        "CREATE TABLE x (a INT); CREATE TABLE x (a INT)", // Catalog (duplicate table)
    ] {
        let (typed, legacy) = run_sql(&stack, sql).await;
        assert!(typed.is_some(), "typed_error missing for sql={sql}");
        assert!(legacy.is_some(), "legacy error missing for sql={sql}");
        assert_eq!(typed.unwrap().message, legacy.unwrap());
    }
}
