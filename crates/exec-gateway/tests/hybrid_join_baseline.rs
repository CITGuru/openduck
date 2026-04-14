//! M3 hybrid join tests: baseline + actual hybrid execution via gateway → worker.
//!
//! `local_join_sum_matches_inline_data` — single-process baseline.
//! `hybrid_join_matches_baseline` — actual split execution through gRPC.

use std::time::Duration;

use duckdb::Connection;

#[test]
fn local_join_sum_matches_inline_data() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r"CREATE TABLE l (id INT, v INT);
          CREATE TABLE r (id INT, w INT);
          INSERT INTO l VALUES (1, 10), (2, 20);
          INSERT INTO r VALUES (1, 100), (3, 300);",
    )
    .unwrap();
    let sum: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(l.v + r.w), 0) FROM l JOIN r ON l.id = r.id",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sum, 110);
}

#[tokio::test]
async fn hybrid_join_matches_baseline() {
    std::env::set_var("OPENDUCK_TOKEN", "hybrid-test");

    let worker_addr: std::net::SocketAddr = "127.0.0.1:45301".parse().unwrap();
    let worker_url = format!("http://{worker_addr}");

    tokio::spawn(async move {
        let _ = exec_worker::serve(worker_addr).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let result = exec_gateway::hybrid::execute_hybrid_join(
        "SELECT * FROM (VALUES (1, 100), (3, 300)) r(id, w)",
        "CREATE TABLE l (id INT, v INT); INSERT INTO l VALUES (1, 10), (2, 20);",
        "SELECT COALESCE(SUM(l.v + __remote.w), 0) FROM l JOIN __remote ON l.id = __remote.id",
        &worker_url,
        "hybrid-test",
    )
    .await;

    let hybrid_sum = result.expect("hybrid join should succeed");

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r"CREATE TABLE l (id INT, v INT);
          CREATE TABLE r (id INT, w INT);
          INSERT INTO l VALUES (1, 10), (2, 20);
          INSERT INTO r VALUES (1, 100), (3, 300);",
    )
    .unwrap();
    let baseline: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(l.v + r.w), 0) FROM l JOIN r ON l.id = r.id",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(
        hybrid_sum, baseline,
        "hybrid execution ({hybrid_sum}) must match single-process baseline ({baseline})"
    );
}
