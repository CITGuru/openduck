use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::ExecuteFragmentRequest;

#[tokio::test]
#[ignore = "run with: openduck-gateway & cargo test -p exec-gateway -- --ignored"]
async fn execute_fragment_requires_token() {
    std::env::remove_var("OPENDUCK_TOKEN");
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:7878")
        .connect()
        .await;
    if channel.is_err() {
        eprintln!("skip grpc_smoke: gateway not running on 7878");
        return;
    }
    let mut client = ExecutionServiceClient::new(channel.unwrap());
    let err = client
        .execute_fragment(ExecuteFragmentRequest {
            plan: vec![],
            database: "x".into(),
            snapshot_id: None,
            access_token: String::new(),
            execution_id: "smoke-unauth".into(),
        })
        .await
        .expect_err("expected unauthenticated");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
#[ignore = "run with: OPENDUCK_TOKEN=x openduck-gateway & cargo test -p exec-gateway -- --ignored"]
async fn execute_fragment_accepts_access_token() {
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:7878")
        .connect()
        .await;
    if channel.is_err() {
        eprintln!("skip grpc_smoke: gateway not running on 7878");
        return;
    }
    let mut client = ExecutionServiceClient::new(channel.unwrap());
    let mut stream = client
        .execute_fragment(ExecuteFragmentRequest {
            plan: vec![],
            database: "x".into(),
            snapshot_id: None,
            access_token: "test".into(),
            execution_id: "smoke-auth".into(),
        })
        .await
        .expect("stream")
        .into_inner();
    let _ = stream.message().await.expect("chunk");
}
