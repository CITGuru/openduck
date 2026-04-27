//! Client-streaming `IngestData` handler.
//!
//! Wire contract: the first chunk on every stream is `IngestMetadata`
//! (database, staging table name, column list, optional `transaction_id`,
//! access token). Subsequent chunks are Arrow IPC stream payloads; the
//! handler decodes each, feeds the contained `RecordBatch`es to a DuckDB
//! `Appender` bound to a CREATE-TEMP-TABLE-staging table on the pinned
//! connection, and returns an `IngestReply { rows_ingested, typed_error? }`.
//!
//! The handler enforces identity binding on transactional ingests,
//! cleans up staging tables on stream-abort, and bounds memory via a
//! fixed-size channel between the tonic receive loop and the Appender
//! loop.

use std::io::Cursor;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use duckdb::Connection;
use exec_proto::execute_fragment_error::Kind;
use exec_proto::ingest_chunk::Payload as IngestPayload;
use exec_proto::{ExecuteFragmentError, IngestChunk, IngestMetadata, IngestReply};
use tokio::sync::mpsc;
use tonic::{Request, Status, Streaming};
use tracing::{debug, info, warn};

use crate::errors::{classify, typed};
use crate::transactions::{identity_hex, identity_of, ConnectionRegistry};
use crate::{open_connection, WorkerConfig};

/// Bounded channel depth between the async receive loop and the
/// blocking Appender loop. Backpressure propagates to the client once
/// 16 chunks are queued (`ingest_chunk_window`).
const INGEST_CHUNK_WINDOW: usize = 16;

/// Build the `CREATE TEMP TABLE` DDL from an `IngestMetadata`.
fn build_staging_ddl(meta: &IngestMetadata) -> Result<String, ExecuteFragmentError> {
    if meta.staging_table.is_empty() {
        return Err(typed(
            Kind::Binder,
            "IngestMetadata.staging_table must be non-empty",
        ));
    }
    if meta.columns.is_empty() {
        return Err(typed(
            Kind::Binder,
            "IngestMetadata.columns must be non-empty",
        ));
    }
    // DuckDB identifier quoting — `"` is the standard double-quote form.
    // Staging table names are client-generated from a UUID, so they
    // won't contain `"`; we still escape defensively.
    let quoted_name = quote_ident(&meta.staging_table);
    let cols: Vec<String> = meta
        .columns
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), c.sql_type))
        .collect();
    Ok(format!(
        "CREATE TEMP TABLE {} ({})",
        quoted_name,
        cols.join(", "),
    ))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn drop_staging(conn: &Connection, staging_table: &str) {
    let sql = format!("DROP TABLE IF EXISTS {}", quote_ident(staging_table));
    if let Err(e) = conn.execute_batch(&sql) {
        warn!(table = %staging_table, error = %e, "DROP TABLE IF EXISTS on staging failed");
    }
}

/// Run the blocking Appender pump: for every Arrow IPC payload received
/// on `rx`, decode it and `append_record_batch` into `staging_table`.
/// Returns the number of rows ingested (summed across all batches).
fn run_appender_loop(
    conn: &Connection,
    staging_table: &str,
    rx: &mut mpsc::Receiver<Vec<u8>>,
) -> Result<u64, ExecuteFragmentError> {
    let mut appender = conn.appender(staging_table).map_err(|e| {
        let msg = e.to_string();
        typed(classify(&msg), msg)
    })?;
    let mut rows_total: u64 = 0;

    while let Some(payload) = rx.blocking_recv() {
        let cursor = Cursor::new(payload);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            typed(
                Kind::Binder,
                format!("failed to decode ingest Arrow IPC stream: {e}"),
            )
        })?;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                typed(
                    Kind::Binder,
                    format!("failed to read ingest Arrow batch: {e}"),
                )
            })?;
            let rows = batch.num_rows() as u64;
            appender.append_record_batch(batch).map_err(|e| {
                let msg = e.to_string();
                typed(classify(&msg), msg)
            })?;
            rows_total += rows;
        }
    }

    appender.flush().map_err(|e| {
        let msg = e.to_string();
        typed(classify(&msg), msg)
    })?;
    Ok(rows_total)
}

/// Handle an `IngestData` client-streaming RPC. Returns an `IngestReply`
/// — errors populate `reply.typed_error` rather than a gRPC `Status`
/// so the client's typed-error path is uniform with `ExecuteFragment`.
pub async fn handle_ingest_data(
    request: Request<Streaming<IngestChunk>>,
    registry: Arc<ConnectionRegistry>,
    config: WorkerConfig,
) -> Result<tonic::Response<IngestReply>, Status> {
    let mut stream = request.into_inner();

    // 1. Wait for the first chunk. MUST be IngestMetadata.
    let meta = match stream.message().await {
        Ok(Some(IngestChunk {
            payload: Some(IngestPayload::Metadata(m)),
        })) => m,
        Ok(Some(IngestChunk {
            payload: Some(IngestPayload::ArrowBatch(_)),
        })) => {
            return Ok(tonic::Response::new(reply_err(typed(
                Kind::Binder,
                "first IngestChunk must carry IngestMetadata; got arrow_batch",
            ))));
        }
        Ok(Some(IngestChunk { payload: None })) => {
            return Ok(tonic::Response::new(reply_err(typed(
                Kind::Binder,
                "first IngestChunk must carry IngestMetadata; got empty payload",
            ))));
        }
        Ok(None) => {
            return Ok(tonic::Response::new(reply_err(typed(
                Kind::Binder,
                "IngestData stream closed before any IngestMetadata was sent",
            ))));
        }
        Err(e) => return Err(e),
    };

    // 2. Authorize. `validate_token` enforces OPENDUCK_TOKEN if set; the
    //    identity hash is then used to pin the txn (if any).
    if let Err(status) = exec_proto::auth::validate_token(&meta.access_token) {
        return Ok(tonic::Response::new(reply_err(typed(
            Kind::Permission,
            status.message(),
        ))));
    }
    let identity = identity_of(&meta.access_token);

    info!(
        staging_table = %meta.staging_table,
        database = %meta.database,
        transaction_id = meta.transaction_id.as_deref().unwrap_or(""),
        identity_hash = %identity_hex(&identity),
        columns = meta.columns.len(),
        "ingest stream opened"
    );

    // Validate DDL up front so a malformed metadata chunk doesn't open a
    // worker connection.
    let staging_ddl = match build_staging_ddl(&meta) {
        Ok(ddl) => ddl,
        Err(err) => return Ok(tonic::Response::new(reply_err(err))),
    };

    // 3. Resolve the connection: pinned if transaction_id set, else a
    //    fresh one bound to this stream's lifetime.
    match meta.transaction_id.as_deref().filter(|s| !s.is_empty()) {
        Some(tx_id) => handle_with_pinned(&meta, tx_id, identity, &staging_ddl, registry, stream)
            .await
            .map(tonic::Response::new),
        None => handle_with_implicit(&meta, &staging_ddl, config, stream)
            .await
            .map(tonic::Response::new),
    }
}

async fn handle_with_pinned(
    meta: &IngestMetadata,
    tx_id: &str,
    identity: [u8; 32],
    staging_ddl: &str,
    registry: Arc<ConnectionRegistry>,
    mut stream: Streaming<IngestChunk>,
) -> Result<IngestReply, Status> {
    let entry_arc = match registry.acquire(tx_id, &identity, &meta.database) {
        Ok(e) => e,
        Err(err) => return Ok(reply_err(err)),
    };

    // Spin up the blocking Appender pump on a dedicated thread. The
    // async side relays decoded IPC payloads through a bounded
    // channel, propagating backpressure to the client.
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(INGEST_CHUNK_WINDOW);
    let staging_table = meta.staging_table.clone();
    let staging_ddl_owned = staging_ddl.to_string();

    let worker = tokio::task::spawn_blocking(move || {
        let mut entry = entry_arc
            .lock()
            .map_err(|_| typed(Kind::Internal, "transaction entry lock poisoned"))?;
        entry.touch();
        entry
            .connection
            .execute_batch(&staging_ddl_owned)
            .map_err(|e| {
                let msg = e.to_string();
                typed(classify(&msg), msg)
            })?;
        let result = run_appender_loop(&entry.connection, &staging_table, &mut rx);
        if result.is_err() {
            drop_staging(&entry.connection, &staging_table);
        }
        entry.touch();
        result
    });

    // Relay subsequent chunks.
    let relay_outcome = relay_chunks(&mut stream, tx).await;
    let rows = match worker.await {
        Ok(Ok(rows)) => rows,
        Ok(Err(err)) => return Ok(reply_err(err)),
        Err(join) => {
            return Ok(reply_err(typed(
                Kind::Internal,
                format!("worker ingest thread join failed: {join}"),
            )));
        }
    };
    if let Err(err) = relay_outcome {
        return Ok(reply_err(err));
    }

    info!(
        staging_table = %meta.staging_table,
        transaction_id = %tx_id,
        rows_ingested = rows,
        "ingest stream completed (pinned)"
    );

    Ok(IngestReply {
        rows_ingested: rows,
        typed_error: None,
    })
}

async fn handle_with_implicit(
    meta: &IngestMetadata,
    staging_ddl: &str,
    config: WorkerConfig,
    mut stream: Streaming<IngestChunk>,
) -> Result<IngestReply, Status> {
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(INGEST_CHUNK_WINDOW);
    let staging_table = meta.staging_table.clone();
    let staging_ddl_owned = staging_ddl.to_string();

    let worker = tokio::task::spawn_blocking(move || {
        let conn = open_connection(&config).map_err(|e| typed(classify(&e), e))?;
        conn.execute_batch(&staging_ddl_owned).map_err(|e| {
            let msg = e.to_string();
            typed(classify(&msg), msg)
        })?;
        let result = run_appender_loop(&conn, &staging_table, &mut rx);
        // Implicit-ingest temp tables don't survive the connection anyway,
        // but dropping explicitly keeps the worker logs tidy if the
        // connection is ever reused.
        if result.is_err() {
            drop_staging(&conn, &staging_table);
        }
        result
    });

    let relay_outcome = relay_chunks(&mut stream, tx).await;
    let rows = match worker.await {
        Ok(Ok(rows)) => rows,
        Ok(Err(err)) => return Ok(reply_err(err)),
        Err(join) => {
            return Ok(reply_err(typed(
                Kind::Internal,
                format!("worker ingest thread join failed: {join}"),
            )));
        }
    };
    if let Err(err) = relay_outcome {
        return Ok(reply_err(err));
    }

    debug!(
        staging_table = %meta.staging_table,
        rows_ingested = rows,
        "ingest stream completed (implicit)"
    );

    Ok(IngestReply {
        rows_ingested: rows,
        typed_error: None,
    })
}

/// Forward subsequent IPC chunks from the gRPC stream into the bounded
/// channel feeding the Appender thread. Dropping `tx` signals EOS.
async fn relay_chunks(
    stream: &mut Streaming<IngestChunk>,
    tx: mpsc::Sender<Vec<u8>>,
) -> Result<(), ExecuteFragmentError> {
    loop {
        match stream.message().await {
            Ok(None) => {
                drop(tx);
                return Ok(());
            }
            Ok(Some(chunk)) => match chunk.payload {
                Some(IngestPayload::ArrowBatch(b)) => {
                    if tx.send(b.ipc_stream_payload).await.is_err() {
                        // Appender thread closed early (failure path).
                        return Ok(());
                    }
                }
                Some(IngestPayload::Metadata(_)) => {
                    return Err(typed(
                        Kind::Binder,
                        "IngestMetadata may only appear as the first chunk",
                    ));
                }
                None => {
                    return Err(typed(Kind::Binder, "IngestChunk has no payload"));
                }
            },
            Err(e) => {
                return Err(typed(
                    Kind::Io,
                    format!("IngestData stream receive error: {e}"),
                ));
            }
        }
    }
}

fn reply_err(err: ExecuteFragmentError) -> IngestReply {
    IngestReply {
        rows_ingested: 0,
        typed_error: Some(err),
    }
}
