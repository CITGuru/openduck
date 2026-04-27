//! End-to-end IngestData tests: full metadata → Arrow batch → staging
//! table pipeline, plus the first-chunk-must-be-metadata and invalid-
//! token negative paths.

mod common;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use exec_proto::execute_fragment_error::Kind;
use exec_proto::ingest_chunk::Payload as IngestPayload;
use exec_proto::openduck::v1::execute_fragment_chunk::Payload as FragmentPayload;
use exec_proto::openduck::v1::{
    ArrowIpcBatch, BeginTransactionRequest, CommitTransactionRequest, ExecuteFragmentRequest,
    IngestChunk, IngestColumn, IngestMetadata, RollbackTransactionRequest,
};
use tokio_stream::StreamExt;
use tonic::Request;

use crate::common::{Stack, TOKEN};

fn encode_ipc(schema: &Schema, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema).expect("writer");
        writer.write(batch).expect("write batch");
        writer.finish().expect("finish");
    }
    buf
}

fn sample_batch() -> (Schema, RecordBatch) {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Utf8, false),
    ]);
    let keys = Int32Array::from(vec![1, 2, 3]);
    let vals = StringArray::from(vec!["alpha", "beta", "gamma"]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(keys), Arc::new(vals)],
    )
    .expect("batch");
    (schema, batch)
}

#[tokio::test]
async fn ingest_happy_path_rows_land_on_pinned_connection() {
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    // Open a transaction so the staging TEMP TABLE is visible to the
    // follow-up INSERT … SELECT on the same pinned connection.
    let begin = client
        .begin_transaction(Request::new(BeginTransactionRequest {
            database: String::new(),
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(begin.typed_error.is_none());

    let staging = "__openduck_ingest_test";
    let (schema, batch) = sample_batch();
    let ipc = encode_ipc(&schema, &batch);

    // Build the outbound client stream: metadata first, then one batch.
    let (tx, rx) = tokio::sync::mpsc::channel::<IngestChunk>(4);
    tx.send(IngestChunk {
        payload: Some(IngestPayload::Metadata(IngestMetadata {
            database: String::new(),
            staging_table: staging.into(),
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
            transaction_id: Some(begin.transaction_id.clone()),
            access_token: TOKEN.into(),
        })),
    })
    .await
    .unwrap();
    tx.send(IngestChunk {
        payload: Some(IngestPayload::ArrowBatch(ArrowIpcBatch {
            ipc_stream_payload: ipc,
        })),
    })
    .await
    .unwrap();
    drop(tx);

    let reply = client
        .ingest_data(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
        .await
        .expect("ingest_data")
        .into_inner();
    assert!(
        reply.typed_error.is_none(),
        "ingest failed: {:?}",
        reply.typed_error
    );
    assert_eq!(reply.rows_ingested, 3);

    // Verify the rows landed by querying the staging temp table on the
    // same pinned connection. This simultaneously proves: staging
    // TEMP TABLE was created, append_record_batch actually wrote, and
    // transaction pinning survives across RPC types.
    let stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: format!("SELECT COUNT(*) FROM {staging}").into_bytes(),
            access_token: TOKEN.into(),
            transaction_id: Some(begin.transaction_id.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let rows = read_single_int64(stream).await;
    assert_eq!(rows, Some(3), "expected 3 rows in staging table");

    // Roll back so the temp table disappears cleanly.
    client
        .rollback_transaction(Request::new(RollbackTransactionRequest {
            transaction_id: begin.transaction_id,
            access_token: TOKEN.into(),
        }))
        .await
        .unwrap();
    let _ = CommitTransactionRequest::default();
}

async fn read_single_int64(
    mut stream: tonic::Streaming<exec_proto::ExecuteFragmentChunk>,
) -> Option<i64> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    let mut result: Option<i64> = None;
    while let Some(chunk) = stream.message().await.unwrap() {
        if let Some(FragmentPayload::ArrowBatch(b)) = chunk.payload {
            if b.ipc_stream_payload.is_empty() {
                continue;
            }
            let reader = StreamReader::try_new(Cursor::new(b.ipc_stream_payload), None).unwrap();
            for batch in reader.flatten() {
                if batch.num_rows() == 0 {
                    continue;
                }
                let col = batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("expected BIGINT column");
                result = Some(arr.value(0));
            }
        }
    }
    result
}

#[tokio::test]
async fn ingest_first_chunk_must_be_metadata() {
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    let (schema, batch) = sample_batch();
    let ipc = encode_ipc(&schema, &batch);

    // Send an ArrowBatch as the first chunk — worker must reject.
    let (tx, rx) = tokio::sync::mpsc::channel::<IngestChunk>(1);
    tx.send(IngestChunk {
        payload: Some(IngestPayload::ArrowBatch(ArrowIpcBatch {
            ipc_stream_payload: ipc,
        })),
    })
    .await
    .unwrap();
    drop(tx);

    let reply = client
        .ingest_data(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
        .await
        .expect("ingest_data call must succeed at transport level")
        .into_inner();
    let err = reply.typed_error.expect("expected typed_error");
    assert_eq!(err.kind, Kind::Binder as i32, "actual: {err:?}");
    assert!(err.message.contains("IngestMetadata") || err.message.contains("metadata"));
    assert_eq!(reply.rows_ingested, 0);
}

#[tokio::test]
async fn ingest_empty_stream_yields_binder() {
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    // Client opens then immediately closes the stream without sending
    // anything. The gateway peeks — it sees None and must return
    // typed BINDER without touching the worker.
    let (tx, rx) = tokio::sync::mpsc::channel::<IngestChunk>(1);
    drop(tx);

    let reply = client
        .ingest_data(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
        .await
        .unwrap()
        .into_inner();
    let err = reply.typed_error.expect("expected typed_error");
    assert_eq!(err.kind, Kind::Binder as i32, "actual: {err:?}");
}

#[tokio::test]
async fn ingest_invalid_token_yields_permission_not_catalog() {
    // With OPENDUCK_TOKEN set, a bad token on IngestData must surface
    // as PERMISSION, never CATALOG — per §Security, leaking
    // table-existence info through CATALOG would be a bug.
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    let (schema, batch) = sample_batch();
    let ipc = encode_ipc(&schema, &batch);

    let (tx, rx) = tokio::sync::mpsc::channel::<IngestChunk>(2);
    tx.send(IngestChunk {
        payload: Some(IngestPayload::Metadata(IngestMetadata {
            database: String::new(),
            staging_table: "__bad_token_staging".into(),
            columns: vec![IngestColumn {
                name: "k".into(),
                sql_type: "INTEGER".into(),
            }],
            transaction_id: None,
            access_token: "wrong-token".into(),
        })),
    })
    .await
    .unwrap();
    tx.send(IngestChunk {
        payload: Some(IngestPayload::ArrowBatch(ArrowIpcBatch {
            ipc_stream_payload: ipc,
        })),
    })
    .await
    .unwrap();
    drop(tx);

    let result = client
        .ingest_data(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
        .await;
    // The gateway runs `validate_token` on the first chunk's metadata
    // access_token. With a wrong token it short-circuits at the
    // gRPC-Status level rather than returning a typed reply — the
    // important invariant is "never CATALOG".
    match result {
        Err(status) => {
            assert_eq!(
                status.code(),
                tonic::Code::Unauthenticated,
                "expected Unauthenticated, got {status:?}"
            );
        }
        Ok(resp) => {
            let err = resp.into_inner().typed_error.expect("typed_error");
            assert_eq!(err.kind, Kind::Permission as i32, "actual: {err:?}");
            assert_ne!(err.kind, Kind::Catalog as i32, "CATALOG leaks existence");
        }
    }
}

#[tokio::test]
async fn ingest_missing_staging_table_is_binder() {
    let stack = Stack::new().await;
    let mut client = stack.gateway_client().await;

    let (tx, rx) = tokio::sync::mpsc::channel::<IngestChunk>(1);
    tx.send(IngestChunk {
        payload: Some(IngestPayload::Metadata(IngestMetadata {
            database: String::new(),
            staging_table: String::new(), // empty — must be rejected
            columns: vec![IngestColumn {
                name: "k".into(),
                sql_type: "INTEGER".into(),
            }],
            transaction_id: None,
            access_token: TOKEN.into(),
        })),
    })
    .await
    .unwrap();
    drop(tx);

    let reply = client
        .ingest_data(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
        .await
        .unwrap()
        .into_inner();
    let err = reply.typed_error.expect("typed_error");
    assert_eq!(err.kind, Kind::Binder as i32, "actual: {err:?}");
}

// Suppress the `StreamExt` unused-import warning in the common path.
// (We don't drive streams directly here but keep the import for
// symmetry with other e2e tests.)
#[allow(dead_code)]
fn _use_stream_ext<T: StreamExt + Unpin>(_: T) {}
