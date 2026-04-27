//! Worker-death test: kill the pinned worker mid-transaction, assert
//! subsequent tx_id calls surface a typed error cleanly (not a hang,
//! not a silent re-route).

mod common;

use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::{BeginTransactionRequest, ExecuteFragmentRequest};
use tonic::Request;

use crate::common::{Stack, TOKEN};

#[tokio::test]
async fn worker_death_surfaces_clean_error_on_pinned_txn() {
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
    assert!(begin.typed_error.is_none());

    // Confirm the happy path works while the worker is alive.
    let ok = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1".to_vec(),
            access_token: TOKEN.into(),
            transaction_id: Some(begin.transaction_id.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let (saw, typed, _) = drain(ok).await;
    assert!(saw);
    assert!(typed.is_none());

    // Kill the pinned worker.
    stack.kill_worker().await;

    // Subsequent use of that tx_id must surface a clean, typed error
    // (not a hang). The gateway's affinity entry still points at the
    // now-dead worker, so the RPC connect fails — the gateway
    // translates that to a gRPC Status::Unavailable, which the client
    // sees as a transport error, *not* a silent re-route to another
    // worker.
    let result = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: b"SELECT 1".to_vec(),
            access_token: TOKEN.into(),
            transaction_id: Some(begin.transaction_id.clone()),
            ..Default::default()
        }))
        .await;

    match result {
        Err(status) => {
            // Transport-level UNAVAILABLE is the expected shape.
            assert_eq!(
                status.code(),
                tonic::Code::Unavailable,
                "expected Unavailable, got {status:?}"
            );
        }
        Ok(resp) => {
            // Alternatively, the gateway might deliver the error as a
            // chunk with `typed_error`. Either shape is acceptable as
            // long as the client sees *some* error (no hang, no silent
            // success, no re-route).
            let (_, typed, legacy) = drain(resp.into_inner()).await;
            assert!(
                typed.is_some() || legacy.is_some(),
                "expected error chunk after worker death"
            );
        }
    }
}

async fn drain(
    mut stream: tonic::Streaming<exec_proto::ExecuteFragmentChunk>,
) -> (
    bool,
    Option<exec_proto::ExecuteFragmentError>,
    Option<String>,
) {
    let mut saw_batch = false;
    let mut typed = None;
    let mut legacy = None;
    loop {
        match stream.message().await {
            Ok(Some(chunk)) => {
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
            Ok(None) => break,
            Err(status) => {
                legacy = Some(status.message().to_string());
                break;
            }
        }
    }
    (saw_batch, typed, legacy)
}
