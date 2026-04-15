//! # Secrets and Storage Configuration
//!
//! Demonstrates how OpenDuck's `openduck_storage` secret type integrates with
//! DuckDB's native secret management to configure the differential storage backend.
//!
//! No running services or Postgres required — this example uses a local DuckDB
//! connection with the extension loaded to show the secret lifecycle.
//!
//! ```bash
//! cargo run --example secrets_and_storage
//! ```

use duckdb::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenDuck Secrets & Storage Configuration ===\n");

    let conn = Connection::open_in_memory()?;

    // In production you would: INSTALL openduck; LOAD openduck;
    // For this example we show the SQL patterns that the extension enables.

    // ── 1. Secret type overview ─────────────────────────────────────────
    println!("1. The openduck_storage secret type");
    println!("   The extension registers a secret type called `openduck_storage`");
    println!("   with two parameters: `postgres_url` and `data_dir`.");
    println!("   `postgres_url` is automatically redacted in secret listings.\n");

    // ── 2. Creating secrets ─────────────────────────────────────────────
    println!("2. Creating secrets\n");

    println!("   -- Default secret (auto-discovered by OpenDuckFileSystem):");
    println!("   CREATE SECRET openduck_storage (");
    println!("       TYPE openduck_storage,");
    println!("       postgres_url 'postgres://localhost/openduck',");
    println!("       data_dir '/var/openduck'");
    println!("   );\n");

    println!("   -- Named secrets for multiple environments:");
    println!("   CREATE SECRET prod_storage (");
    println!("       TYPE openduck_storage,");
    println!("       postgres_url 'postgres://prod-host/openduck',");
    println!("       data_dir '/mnt/prod/openduck'");
    println!("   );\n");

    println!("   CREATE SECRET dev_storage (");
    println!("       TYPE openduck_storage,");
    println!("       postgres_url 'postgres://localhost/openduck',");
    println!("       data_dir '/tmp/openduck'");
    println!("   );\n");

    // ── 3. URI parameter usage ──────────────────────────────────────────
    println!("3. Using secrets with openduck:// URIs\n");

    println!("   -- No parameter: uses default secret 'openduck_storage', then env vars");
    println!("   ATTACH 'openduck://mydb/database.duckdb' AS local_db;\n");

    println!("   -- ?secret=NAME: uses a specific named secret");
    println!("   ATTACH 'openduck://mydb/database.duckdb?secret=prod_storage' AS prod_db;\n");

    println!("   -- ?data_dir=PATH: overrides data_dir from any source");
    println!("   ATTACH 'openduck://mydb/database.duckdb?secret=prod_storage&data_dir=/tmp/cache' AS cached;\n");

    // ── 4. Resolution cascade ───────────────────────────────────────────
    println!("4. Configuration resolution cascade\n");
    println!("   The OpenDuckFileSystem resolves storage config in this order:");
    println!("   ");
    println!("   1. ?secret=NAME in URI     → looks up the named secret");
    println!("   2. Default 'openduck_storage' → auto-discovered if it exists");
    println!("   3. OPENDUCK_POSTGRES_URL env → with OPENDUCK_DATA_DIR (or /tmp/openduck)");
    println!("   4. InMemoryStorage fallback  → no persistence, ephemeral");
    println!("   ");
    println!("   ?data_dir=PATH in the URI overrides data_dir from any source above.\n");

    // ── 5. Demonstrate with local DuckDB ────────────────────────────────
    println!("5. DuckDB secrets API demo (without extension)\n");

    conn.execute_batch(
        r"CREATE TABLE demo_config (step TEXT, description TEXT);
          INSERT INTO demo_config VALUES
            ('create', 'CREATE SECRET ... (TYPE openduck_storage, ...)'),
            ('list',   'SELECT * FROM duckdb_secrets()'),
            ('use',    'ATTACH ''openduck://mydb/db.duckdb?secret=name'''),
            ('drop',   'DROP SECRET name');",
    )?;

    let mut stmt = conn.prepare("SELECT step, description FROM demo_config ORDER BY rowid")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    for row in rows {
        let (step, desc) = row?;
        println!("   {step:8} → {desc}");
    }

    println!("\n=== Done ===");
    Ok(())
}
