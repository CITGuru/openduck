//! # Connect to OpenDuck from Rust
//!
//! Demonstrates all connection methods: LOAD + ATTACH, URI shorthand,
//! query execution, result fetching, and hybrid local + remote queries.
//!
//! ```bash
//! OPENDUCK_TOKEN=demo cargo run --example client_rust
//! ```

use duckdb::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenDuck Rust Client Examples ===\n");

    // ── 1. DuckDB version check ─────────────────────────────────────────────
    println!("── 1. DuckDB version ──");
    let mut conn = Connection::open_in_memory()?;
    let version: String = conn.query_row("SELECT version()", [], |row| row.get(0))?;
    println!("   DuckDB version: {version}\n");

    // ── 2. LOAD + ATTACH (the standard path) ────────────────────────────────
    println!("── 2. LOAD + ATTACH ──");
    println!("   let conn = Connection::open_in_memory()?;");
    println!("   conn.execute_batch(r\"");
    println!("       SET allow_unsigned_extensions = true;");
    println!("       LOAD 'openduck';");
    println!("       ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;");
    println!("   \")?;");
    println!("   // SELECT * FROM cloud.users just works\n");

    // ── 3. Query execution and result fetching ──────────────────────────────
    println!("── 3. Query + fetch (local demo) ──");
    conn.execute_batch(
        r"CREATE TABLE demo_users (id INTEGER, name VARCHAR, active BOOLEAN);
          INSERT INTO demo_users VALUES (1, 'Alice', true), (2, 'Bob', false), (3, 'Carol', true);",
    )?;

    let mut stmt = conn.prepare("SELECT id, name FROM demo_users WHERE active = true")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (id, name) = row?;
        println!("   id={id}, name={name}");
    }
    println!();

    // ── 4. Aggregation ──────────────────────────────────────────────────────
    println!("── 4. Aggregation ──");
    conn.execute_batch(
        r"CREATE TABLE demo_sales (product VARCHAR, quantity INTEGER, revenue DOUBLE);
          INSERT INTO demo_sales VALUES
            ('Widget', 10, 99.90), ('Widget', 5, 49.95),
            ('Gadget', 3, 149.70), ('Gadget', 7, 349.30),
            ('Doohickey', 1, 9.99);",
    )?;

    let mut stmt = conn.prepare(
        r"SELECT product, SUM(quantity) AS total_qty, SUM(revenue) AS total_rev
          FROM demo_sales GROUP BY product ORDER BY total_rev DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, f64>(2)?,
        ))
    })?;
    for row in rows {
        let (product, qty, rev) = row?;
        println!("   {product}: {qty} units, ${rev:.2}");
    }
    println!();

    // ── 5. Arrow result batches ─────────────────────────────────────────────
    println!("── 5. Arrow result batches ──");
    let mut stmt = conn.prepare("SELECT * FROM demo_sales")?;
    let arrow_batches = stmt.query_arrow([])?;
    for batch in arrow_batches {
        println!(
            "   Arrow batch: {} columns, {} rows",
            batch.num_columns(),
            batch.num_rows()
        );
    }
    println!();

    // ── 6. Hybrid local + remote (simulated) ────────────────────────────────
    println!("── 6. Hybrid join (local simulation) ──");
    conn.execute_batch(
        r"CREATE TABLE local_products (id INTEGER, name VARCHAR);
          INSERT INTO local_products VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey');

          CREATE TABLE remote_orders (order_id INTEGER, product_id INTEGER, amount DOUBLE);
          INSERT INTO remote_orders VALUES
            (101, 1, 29.99), (102, 1, 29.99), (103, 2, 49.90),
            (104, 3, 9.99), (105, 2, 49.90);",
    )?;

    let mut stmt = conn.prepare(
        r"SELECT p.name, COUNT(*) AS orders, SUM(o.amount) AS revenue
          FROM local_products p
          JOIN remote_orders o ON p.id = o.product_id
          GROUP BY p.name
          ORDER BY revenue DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, f64>(2)?,
        ))
    })?;
    for row in rows {
        let (name, orders, revenue) = row?;
        println!("   {name}: {orders} orders, ${revenue:.2}");
    }
    println!();

    // ── 7. Read local CSV + join ────────────────────────────────────────────
    println!("── 7. Read CSV (conceptual) ──");
    println!("   conn.execute_batch(r\"");
    println!("       CREATE TABLE csv_data AS SELECT * FROM read_csv('data/input.csv');");
    println!("       SELECT c.*, u.email");
    println!("       FROM csv_data c JOIN cloud.users u ON c.user_id = u.id;");
    println!("   \")?;\n");

    // ── 8. Multiple attached databases ──────────────────────────────────────
    println!("── 8. Multiple remote databases ──");
    println!("   conn.execute_batch(r\"");
    println!("       ATTACH 'openduck:production?token=prod-tok' AS prod;");
    println!("       ATTACH 'openduck:staging?token=stg-tok&endpoint=http://staging:7878' AS stg;");
    println!("       -- Compare row counts across environments");
    println!("       SELECT 'prod' AS env, count(*) FROM prod.users");
    println!("       UNION ALL");
    println!("       SELECT 'stg', count(*) FROM stg.users;");
    println!("   \")?;\n");

    // ── 9. Prepared statements with parameters ──────────────────────────────
    println!("── 9. Parameterized queries ──");
    let mut stmt = conn.prepare("SELECT id, name FROM demo_users WHERE id > ?")?;
    let rows = stmt.query_map([1], |row| {
        Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (id, name) = row?;
        println!("   id={id}, name={name}");
    }
    println!();

    // ── 10. Transaction semantics ───────────────────────────────────────────
    println!("── 10. Transactions ──");
    {
        let tx = conn.transaction()?;
        tx.execute("INSERT INTO demo_users VALUES (4, 'Dave', true)", [])?;
        tx.execute("INSERT INTO demo_users VALUES (5, 'Eve', true)", [])?;
        tx.commit()?;
    }
    let count: i64 = conn.query_row("SELECT count(*) FROM demo_users", [], |r| r.get(0))?;
    println!("   Users after transaction: {count}\n");

    // ── 11. Error handling ──────────────────────────────────────────────────
    println!("── 11. Error handling ──");
    match conn.execute("SELECT * FROM nonexistent_table", []) {
        Ok(_) => println!("   Unexpected success"),
        Err(e) => println!("   Expected error: {e}"),
    }
    println!();

    println!("=== All examples complete ===");
    Ok(())
}
