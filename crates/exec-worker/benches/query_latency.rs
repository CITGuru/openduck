//! Simple benchmark measuring query-to-Arrow round-trip latency.
//!
//! Run: `cargo bench -p exec-worker`

use std::time::Instant;

use duckdb::Connection;

const QUERIES: &[(&str, &str)] = &[
    ("point", "SELECT 42 AS answer"),
    ("range_1k", "SELECT * FROM range(1000)"),
    ("range_100k", "SELECT * FROM range(100000)"),
    (
        "aggregate_100k",
        "SELECT i % 100 AS bucket, SUM(i) FROM range(100000) t(i) GROUP BY bucket",
    ),
    (
        "join_10k",
        "WITH a AS (SELECT * FROM range(10000) t(i)), b AS (SELECT * FROM range(100) t(i)) SELECT a.i, b.i FROM a, b WHERE a.i % 100 = b.i",
    ),
];

fn main() {
    let warmup = 3;
    let iters = 10;

    println!("OpenDuck query latency benchmark");
    println!("  warmup={warmup}, iters={iters}");
    println!("{:<20} {:>12} {:>12} {:>12}", "query", "min_us", "avg_us", "max_us");
    println!("{}", "-".repeat(60));

    let conn = Connection::open_in_memory().expect("open duckdb");

    for (name, sql) in QUERIES {
        for _ in 0..warmup {
            let mut stmt = conn.prepare(sql).unwrap();
            let arrow = stmt.query_arrow([]).unwrap();
            let _: Vec<_> = arrow.collect();
        }

        let mut times = Vec::with_capacity(iters);
        for _ in 0..iters {
            let start = Instant::now();
            let mut stmt = conn.prepare(sql).unwrap();
            let arrow = stmt.query_arrow([]).unwrap();
            let _: Vec<_> = arrow.collect();
            times.push(start.elapsed());
        }

        let min = times.iter().min().unwrap().as_micros();
        let max = times.iter().max().unwrap().as_micros();
        let avg = times.iter().map(|t| t.as_micros()).sum::<u128>() / iters as u128;
        println!("{name:<20} {min:>12} {avg:>12} {max:>12}");
    }
}
