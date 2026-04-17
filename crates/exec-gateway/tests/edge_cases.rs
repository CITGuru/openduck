//! Edge-case integration tests: gateway → worker → Arrow IPC chunks.
//!
//! Tests that need stateful tables use a file-backed worker DB. Tests that
//! only need a single statement use the default in-memory worker.

use std::path::PathBuf;
use std::time::Duration;

use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::ExecuteFragmentRequest;
use tonic::Request;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_stack(worker_port: u16, gateway_port: u16) {
    start_stack_with_db(worker_port, gateway_port, None).await;
}

async fn start_stack_with_db(worker_port: u16, gateway_port: u16, db_path: Option<PathBuf>) {
    std::env::set_var("OPENDUCK_TOKEN", "test-token");
    let worker = ([127, 0, 0, 1], worker_port).into();
    let config = exec_worker::WorkerConfig {
        db_path,
        ..Default::default()
    };
    let _wh = tokio::spawn(async move {
        let _ = exec_worker::serve_with_config(worker, config).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let gw = ([127, 0, 0, 1], gateway_port).into();
    let workers = vec![format!("http://127.0.0.1:{worker_port}")];
    let _gh = tokio::spawn(async move {
        let _ = exec_gateway::serve(gw, workers).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
}

fn temp_db(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("openduck_tests");
    std::fs::create_dir_all(&dir).ok();
    let path = dir.join(format!("{name}.duckdb"));
    let _ = std::fs::remove_file(&path);
    path
}

struct RunResult {
    batches: Vec<Vec<u8>>,
    finished: bool,
    error: Option<String>,
}

async fn run_sql(
    client: &mut ExecutionServiceClient<tonic::transport::Channel>,
    sql: &str,
    exec_id: &str,
) -> RunResult {
    let mut stream = client
        .execute_fragment(Request::new(ExecuteFragmentRequest {
            plan: sql.as_bytes().to_vec(),
            access_token: "test-token".into(),
            execution_id: exec_id.into(),
            ..Default::default()
        }))
        .await
        .unwrap_or_else(|e| panic!("execute_fragment for '{exec_id}' failed: {e}"))
        .into_inner();

    let mut result = RunResult {
        batches: vec![],
        finished: false,
        error: None,
    };

    while let Some(chunk) = stream.message().await.expect("message") {
        match chunk.payload {
            Some(Payload::ArrowBatch(b)) if !b.ipc_stream_payload.is_empty() => {
                result.batches.push(b.ipc_stream_payload);
            }
            Some(Payload::Finished(true)) => result.finished = true,
            Some(Payload::Error(e)) => result.error = Some(e),
            _ => {}
        }
    }
    result
}

fn total_rows(result: &RunResult) -> usize {
    use arrow::ipc::reader::StreamReader;
    result
        .batches
        .iter()
        .flat_map(|b| {
            let reader = StreamReader::try_new(std::io::Cursor::new(b), None).expect("open IPC");
            reader
                .into_iter()
                .filter_map(|r| r.ok())
                .map(|batch| batch.num_rows())
                .collect::<Vec<_>>()
        })
        .sum()
}

fn assert_ok(r: &RunResult, label: &str) {
    assert!(r.error.is_none(), "{label} error: {:?}", r.error);
    assert!(r.finished, "{label} did not finish");
}

// ---------------------------------------------------------------------------
// Single-statement tests (in-memory, no persistent state needed)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_literal() {
    start_stack(46001, 46002).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46002")
        .await
        .unwrap();

    let r = run_sql(&mut c, "SELECT 42 AS v", "edge-literal").await;
    assert_ok(&r, "literal");
    assert_eq!(total_rows(&r), 1);
}

#[tokio::test]
async fn case_expression_and_cast() {
    start_stack(46011, 46012).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46012")
        .await
        .unwrap();

    let r = run_sql(
        &mut c,
        "SELECT
           CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END AS flag,
           CAST(42 AS VARCHAR) AS num_text,
           CAST('123' AS INTEGER) AS parsed",
        "edge-case-cast",
    )
    .await;
    assert_ok(&r, "case/cast");
    assert_eq!(total_rows(&r), 1);
}

#[tokio::test]
async fn order_by_limit_offset() {
    start_stack(46021, 46022).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46022")
        .await
        .unwrap();

    let r = run_sql(
        &mut c,
        "SELECT i FROM generate_series(1, 100) t(i) ORDER BY i DESC LIMIT 5 OFFSET 90",
        "edge-limit-offset",
    )
    .await;
    assert_ok(&r, "limit-offset");
    assert_eq!(total_rows(&r), 5);
}

#[tokio::test]
async fn large_result_multi_batch() {
    start_stack(46031, 46032).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46032")
        .await
        .unwrap();

    let r = run_sql(
        &mut c,
        "SELECT i AS id, 'row-' || i AS label FROM generate_series(1, 10000) t(i)",
        "edge-large",
    )
    .await;
    assert_ok(&r, "large-result");
    assert_eq!(total_rows(&r), 10000);
}

#[tokio::test]
async fn empty_result_from_false_filter() {
    start_stack(46041, 46042).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46042")
        .await
        .unwrap();

    let r = run_sql(
        &mut c,
        "SELECT i FROM generate_series(1, 10) t(i) WHERE 1 = 0",
        "edge-empty",
    )
    .await;
    assert_ok(&r, "empty");
    assert_eq!(total_rows(&r), 0);
}

#[tokio::test]
async fn ddl_error_propagates() {
    start_stack(46051, 46052).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46052")
        .await
        .unwrap();

    let r = run_sql(&mut c, "SELECT * FROM nonexistent_table_xyz", "edge-error").await;
    assert!(r.error.is_some(), "expected error for missing table");
}

// ---------------------------------------------------------------------------
// Stateful tests (file-backed worker DB)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn diverse_types_roundtrip() {
    let db = temp_db("edge_types");
    start_stack_with_db(46101, 46102, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46102")
        .await
        .unwrap();

    let r = run_sql(
        &mut c,
        "CREATE TABLE types_test (
           id INTEGER, flag BOOLEAN, tiny TINYINT, small SMALLINT,
           big BIGINT, price DOUBLE, label VARCHAR, ratio FLOAT
         )",
        "types-ddl",
    )
    .await;
    assert_ok(&r, "types DDL");

    let r = run_sql(
        &mut c,
        "INSERT INTO types_test VALUES
           (1, true,  127, 32000, 9223372036854775807, 3.14159, 'hello', 1.5),
           (2, false, -128, -32000, -1, 0.0, '', -0.0),
           (3, NULL,  NULL, NULL, NULL, NULL, NULL, NULL)",
        "types-insert",
    )
    .await;
    assert_ok(&r, "types INSERT");

    let r = run_sql(
        &mut c,
        "SELECT * FROM types_test ORDER BY id",
        "types-select",
    )
    .await;
    assert_ok(&r, "types SELECT");
    assert_eq!(total_rows(&r), 3);
}

#[tokio::test]
async fn null_aggregation() {
    let db = temp_db("edge_nulls");
    start_stack_with_db(46111, 46112, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46112")
        .await
        .unwrap();

    run_sql(
        &mut c,
        "CREATE TABLE nulls (id INTEGER, val VARCHAR)",
        "nulls-ddl",
    )
    .await;
    run_sql(
        &mut c,
        "INSERT INTO nulls VALUES (1, 'a'), (2, NULL), (3, 'c'), (4, NULL)",
        "nulls-insert",
    )
    .await;

    let r = run_sql(
        &mut c,
        "SELECT count(*) AS total, count(val) AS non_null FROM nulls",
        "nulls-agg",
    )
    .await;
    assert_ok(&r, "null-agg");
    assert_eq!(total_rows(&r), 1);
}

#[tokio::test]
async fn self_join() {
    let db = temp_db("edge_selfjoin");
    start_stack_with_db(46121, 46122, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46122")
        .await
        .unwrap();

    run_sql(
        &mut c,
        "CREATE TABLE items (id INTEGER, label VARCHAR)",
        "sj-ddl",
    )
    .await;
    run_sql(
        &mut c,
        "INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        "sj-insert",
    )
    .await;

    let r = run_sql(
        &mut c,
        "SELECT a.id AS id_a, b.id AS id_b FROM items a JOIN items b ON a.id = b.id",
        "sj-query",
    )
    .await;
    assert_ok(&r, "self-join");
    assert_eq!(total_rows(&r), 3);
}

#[tokio::test]
async fn cross_join_cartesian() {
    let db = temp_db("edge_cross");
    start_stack_with_db(46131, 46132, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46132")
        .await
        .unwrap();

    run_sql(&mut c, "CREATE TABLE t1 (x INTEGER)", "cross-ddl1").await;
    run_sql(&mut c, "CREATE TABLE t2 (y INTEGER)", "cross-ddl2").await;
    run_sql(&mut c, "INSERT INTO t1 VALUES (1),(2),(3)", "cross-ins1").await;
    run_sql(&mut c, "INSERT INTO t2 VALUES (10),(20)", "cross-ins2").await;

    let r = run_sql(&mut c, "SELECT * FROM t1, t2", "cross-select").await;
    assert_ok(&r, "cross-join");
    assert_eq!(total_rows(&r), 6);
}

#[tokio::test]
async fn cte_and_subquery() {
    let db = temp_db("edge_cte");
    start_stack_with_db(46141, 46142, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46142")
        .await
        .unwrap();

    run_sql(
        &mut c,
        "CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)",
        "cte-ddl",
    )
    .await;
    run_sql(
        &mut c,
        "INSERT INTO products VALUES (1,'Widget',9.99),(2,'Gadget',19.99),(3,'Doohickey',4.50)",
        "cte-insert",
    )
    .await;

    let r = run_sql(
        &mut c,
        "WITH expensive AS (SELECT * FROM products WHERE price > 10)
         SELECT name FROM expensive",
        "cte-query",
    )
    .await;
    assert_ok(&r, "CTE");
    assert_eq!(total_rows(&r), 1);

    let r = run_sql(
        &mut c,
        "SELECT name FROM products WHERE price = (SELECT max(price) FROM products)",
        "subquery",
    )
    .await;
    assert_ok(&r, "subquery");
    assert_eq!(total_rows(&r), 1);
}

#[tokio::test]
async fn window_functions() {
    let db = temp_db("edge_window");
    start_stack_with_db(46151, 46152, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46152")
        .await
        .unwrap();

    run_sql(
        &mut c,
        "CREATE TABLE scores (player VARCHAR, score INTEGER)",
        "win-ddl",
    )
    .await;
    run_sql(
        &mut c,
        "INSERT INTO scores VALUES ('alice',100),('bob',200),('charlie',150)",
        "win-insert",
    )
    .await;

    let r = run_sql(
        &mut c,
        "SELECT player, score,
                ROW_NUMBER() OVER (ORDER BY score DESC) AS rank,
                SUM(score) OVER () AS total
         FROM scores",
        "win-select",
    )
    .await;
    assert_ok(&r, "window-fn");
    assert_eq!(total_rows(&r), 3);
}

#[tokio::test]
async fn set_operations() {
    let db = temp_db("edge_sets");
    start_stack_with_db(46161, 46162, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46162")
        .await
        .unwrap();

    run_sql(&mut c, "CREATE TABLE s1 (v INTEGER)", "set-ddl1").await;
    run_sql(&mut c, "CREATE TABLE s2 (v INTEGER)", "set-ddl2").await;
    run_sql(&mut c, "INSERT INTO s1 VALUES (1),(2),(3)", "set-ins1").await;
    run_sql(&mut c, "INSERT INTO s2 VALUES (2),(3),(4)", "set-ins2").await;

    let r = run_sql(
        &mut c,
        "SELECT v FROM s1 UNION SELECT v FROM s2",
        "set-union",
    )
    .await;
    assert_ok(&r, "UNION");
    assert_eq!(total_rows(&r), 4);

    let r = run_sql(
        &mut c,
        "SELECT v FROM s1 INTERSECT SELECT v FROM s2",
        "set-intersect",
    )
    .await;
    assert_ok(&r, "INTERSECT");
    assert_eq!(total_rows(&r), 2);

    let r = run_sql(
        &mut c,
        "SELECT v FROM s1 EXCEPT SELECT v FROM s2",
        "set-except",
    )
    .await;
    assert_ok(&r, "EXCEPT");
    assert_eq!(total_rows(&r), 1);
}

#[tokio::test]
async fn aggregation_having_order_limit() {
    let db = temp_db("edge_having");
    start_stack_with_db(46171, 46172, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46172")
        .await
        .unwrap();

    run_sql(
        &mut c,
        "CREATE TABLE logs (category VARCHAR, val INTEGER)",
        "having-ddl",
    )
    .await;
    run_sql(
        &mut c,
        "INSERT INTO logs VALUES ('a',1),('a',2),('b',3),('c',4),('c',5),('c',6)",
        "having-insert",
    )
    .await;

    let r = run_sql(
        &mut c,
        "SELECT category, count(*) AS cnt
         FROM logs GROUP BY category HAVING count(*) >= 2
         ORDER BY cnt DESC LIMIT 1",
        "having-query",
    )
    .await;
    assert_ok(&r, "HAVING");
    assert_eq!(total_rows(&r), 1);
}

#[tokio::test]
async fn insert_select_across_tables() {
    let db = temp_db("edge_inssel");
    start_stack_with_db(46181, 46182, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46182")
        .await
        .unwrap();

    run_sql(
        &mut c,
        "CREATE TABLE src (id INTEGER, name VARCHAR)",
        "inssel-ddl",
    )
    .await;
    run_sql(
        &mut c,
        "INSERT INTO src VALUES (1,'alice'),(2,'bob')",
        "inssel-insert",
    )
    .await;
    run_sql(
        &mut c,
        "CREATE TABLE dst AS SELECT * FROM src",
        "inssel-ctas",
    )
    .await;

    let r = run_sql(&mut c, "SELECT * FROM dst", "inssel-verify").await;
    assert_ok(&r, "INSERT...SELECT");
    assert_eq!(total_rows(&r), 2);
}

#[tokio::test]
async fn multiple_aggregations() {
    let db = temp_db("edge_multiagg");
    start_stack_with_db(46191, 46192, Some(db)).await;
    let mut c = ExecutionServiceClient::connect("http://127.0.0.1:46192")
        .await
        .unwrap();

    run_sql(&mut c, "CREATE TABLE vals (v DOUBLE)", "multiagg-ddl").await;
    run_sql(
        &mut c,
        "INSERT INTO vals VALUES (10),(20),(30),(40),(50)",
        "multiagg-insert",
    )
    .await;

    let r = run_sql(
        &mut c,
        "SELECT count(*) AS n, min(v), max(v), avg(v), sum(v) FROM vals",
        "multiagg-query",
    )
    .await;
    assert_ok(&r, "multi-agg");
    assert_eq!(total_rows(&r), 1);
}
