//! DuckLake integration test.
//!
//! Requires:
//! - `OPENDUCK_DUCKLAKE_METADATA`: Postgres connection string for DuckLake metadata
//! - `OPENDUCK_DUCKLAKE_DATA`: S3/MinIO path for Parquet data files
//! - S3 credentials via standard AWS env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
//!   `AWS_ENDPOINT_URL` for MinIO)
//!
//! Run: `cargo test -p exec-worker -- --ignored ducklake`

use exec_worker::WorkerConfig;

fn ducklake_config() -> Option<WorkerConfig> {
    let meta = std::env::var("OPENDUCK_DUCKLAKE_METADATA").ok()?;
    let data = std::env::var("OPENDUCK_DUCKLAKE_DATA").ok()?;
    if meta.is_empty() || data.is_empty() {
        return None;
    }
    Some(WorkerConfig {
        db_path: None,
        ducklake_metadata: Some(meta),
        ducklake_data_path: Some(data),
        ..Default::default()
    })
}

#[test]
#[ignore = "requires ducklake extension + Postgres + S3/MinIO credentials"]
fn ducklake_open_connection_smoke() {
    let config = match ducklake_config() {
        Some(c) => c,
        None => {
            eprintln!("skip: OPENDUCK_DUCKLAKE_METADATA / OPENDUCK_DUCKLAKE_DATA not set");
            return;
        }
    };

    let result = exec_worker::__test_open_connection(&config);
    assert!(
        result.is_ok(),
        "ducklake connection failed: {:?}",
        result.err()
    );
}

#[tokio::test]
#[ignore = "requires ducklake extension + Postgres + S3/MinIO credentials"]
async fn ducklake_attach_and_query() {
    use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
    use exec_proto::openduck::v1::execution_service_server::ExecutionService;
    use exec_proto::openduck::v1::ExecuteFragmentRequest;
    use tonic::Request;

    let config = match ducklake_config() {
        Some(c) => c,
        None => {
            eprintln!("skip: OPENDUCK_DUCKLAKE_METADATA / OPENDUCK_DUCKLAKE_DATA not set");
            return;
        }
    };

    let svc = exec_worker::WorkerService::new(config);

    let req = ExecuteFragmentRequest {
        plan: b"SELECT 42 AS answer".to_vec(),
        access_token: "test".into(),
        execution_id: "ducklake-smoke-1".into(),
        ..Default::default()
    };

    let response = svc
        .execute_fragment(Request::new(req))
        .await
        .expect("execute_fragment");
    let mut stream = response.into_inner();

    let mut got_data = false;
    let mut got_finished = false;
    while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
        let chunk = chunk.expect("chunk");
        match chunk.payload {
            Some(Payload::ArrowBatch(_)) => got_data = true,
            Some(Payload::Finished(true)) => got_finished = true,
            Some(Payload::Error(e)) => panic!("worker error: {e}"),
            _ => {}
        }
    }
    assert!(got_data, "expected at least one Arrow batch");
    assert!(got_finished, "expected finished marker");
}
