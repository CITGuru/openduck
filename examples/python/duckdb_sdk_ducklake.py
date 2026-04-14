"""
Using OpenDuck with the DuckDB Python SDK and DuckLake.

This example shows the two main usage patterns:

  1. DuckDB SDK → openduck extension → remote OpenDuck gateway
     (the gateway's worker may itself be backed by DuckLake)

  2. DuckDB SDK → ducklake extension directly (local DuckLake, no gateway)

  3. Combining both: local DuckLake tables JOINed with remote OpenDuck tables

Prerequisites:
    pip install duckdb pandas pyarrow

    # Build the openduck extension (or set OPENDUCK_EXTENSION_PATH):
    #   cd extensions/openduck && make release

    # For pattern 1 — start the OpenDuck service with DuckLake backend:
    #   export OPENDUCK_TOKEN=dev-token
    #   export OPENDUCK_DUCKLAKE_METADATA=postgres://user:pass@localhost:5433/ducklake_meta
    #   export OPENDUCK_DUCKLAKE_DATA=s3://my-bucket/ducklake/
    #   cargo run --bin openduck-worker &
    #   cargo run --bin openduck-gateway &

    # For pattern 2 — no server needed, DuckLake runs in-process.

Run:
    python examples/python/duckdb_sdk_ducklake.py
"""

import os
from pathlib import Path

import duckdb


def _find_openduck_extension() -> str:
    """Locate the locally-built openduck extension binary.

    Resolution order:
      1. OPENDUCK_EXTENSION_PATH env var
      2. Standard build output under extensions/openduck/build/release/
    """
    env = os.environ.get("OPENDUCK_EXTENSION_PATH")
    if env and Path(env).exists():
        return env

    here = Path(__file__).resolve()
    candidate = (
        here.parents[2]
        / "extensions" / "openduck" / "build" / "release"
        / "extension" / "openduck" / "openduck.duckdb_extension"
    )
    if candidate.exists():
        return str(candidate)

    raise FileNotFoundError(
        "Cannot find openduck.duckdb_extension. Either:\n"
        "  • Build it:  cd extensions/openduck && make release\n"
        "  • Or set:    export OPENDUCK_EXTENSION_PATH=/path/to/openduck.duckdb_extension"
    )


def _load_openduck(con: duckdb.DuckDBPyConnection) -> None:
    """Load the openduck extension from the local build into a connection."""
    ext = _find_openduck_extension()
    con.execute(f"LOAD '{ext}';")


# ═══════════════════════════════════════════════════════════════════════════════
# Part 1: DuckDB SDK + openduck extension (remote queries via gateway)
# ═══════════════════════════════════════════════════════════════════════════════

def remote_via_openduck():
    """Query a remote OpenDuck gateway using the DuckDB Python SDK directly."""

    print("── 1. DuckDB SDK + openduck extension ──\n")

    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    _load_openduck(con)

    # Attach the remote database. The gateway routes SQL to a worker which
    # may be backed by DuckLake (Postgres metadata + S3 data).
    con.execute("""
        ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:7878&token=dev-token'
        AS remote;
    """)

    # --- Query remote tables ---
    con.sql("SHOW TABLES FROM remote").show()
    con.sql("SELECT * FROM remote.users LIMIT 10").show()

    # --- Aggregation ---
    df = con.sql("""
        SELECT department, count(*) AS cnt, avg(salary) AS avg_salary
        FROM remote.employees
        GROUP BY department
        ORDER BY cnt DESC
    """).fetchdf()
    print(df)

    # --- Arrow output ---
    arrow_table = con.sql("SELECT * FROM remote.events LIMIT 1000").arrow()
    print(f"Arrow schema: {arrow_table.schema}\n")

    con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Part 2: DuckDB SDK + DuckLake directly (local, no gateway)
# ═══════════════════════════════════════════════════════════════════════════════

def local_ducklake():
    """Use DuckLake directly in-process — no OpenDuck server required."""

    print("── 2. DuckDB SDK + DuckLake (local / in-process) ──\n")

    con = duckdb.connect(":memory:")
    con.execute("INSTALL ducklake; LOAD ducklake;")

    # Attach a DuckLake catalog. Metadata lives in Postgres (or SQLite/DuckDB);
    # data lives on S3, GCS, or the local filesystem.
    #
    # For local development you can use a DuckDB file as metadata storage
    # and a local directory for data — no external services needed:
    con.execute("""
        ATTACH 'ducklake:my_lake' (
            DATA_PATH    './ducklake_data/',
            METADATA_PATH 'ducklake_meta.duckdb'
        );
    """)

    # Switch into the DuckLake catalog so unqualified names resolve there.
    con.execute("USE my_lake;")

    # --- Create tables (stored as Parquet in DATA_PATH) ---
    con.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id        INTEGER,
            name      VARCHAR,
            email     VARCHAR,
            region    VARCHAR,
            joined_at TIMESTAMP DEFAULT current_timestamp
        );
    """)

    con.execute("""
        INSERT INTO customers VALUES
            (1, 'Alice',   'alice@example.com',   'us-east', '2025-01-15'),
            (2, 'Bob',     'bob@example.com',     'eu-west', '2025-02-20'),
            (3, 'Charlie', 'charlie@example.com', 'us-west', '2025-03-10'),
            (4, 'Diana',   'diana@example.com',   'eu-west', '2025-04-05'),
            (5, 'Eve',     'eve@example.com',     'us-east', '2025-05-01');
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id    INTEGER,
            customer_id INTEGER,
            amount      DECIMAL(10,2),
            product     VARCHAR,
            ordered_at  TIMESTAMP DEFAULT current_timestamp
        );
    """)

    con.execute("""
        INSERT INTO orders VALUES
            (101, 1, 250.00, 'Widget',     '2025-06-01'),
            (102, 2,  75.50, 'Gadget',     '2025-06-02'),
            (103, 1, 120.00, 'Doohickey',  '2025-06-03'),
            (104, 3, 340.00, 'Widget',     '2025-06-04'),
            (105, 4,  60.00, 'Thingamajig','2025-06-05'),
            (106, 5, 410.00, 'Widget',     '2025-06-06'),
            (107, 2, 190.00, 'Gadget',     '2025-06-07');
    """)

    print("Tables in DuckLake catalog:")
    con.sql("SHOW TABLES").show()

    # --- Queries against DuckLake ---
    print("Customer count by region:")
    con.sql("""
        SELECT region, count(*) AS n
        FROM customers
        GROUP BY region
        ORDER BY n DESC
    """).show()

    print("Revenue by product:")
    con.sql("""
        SELECT o.product, sum(o.amount) AS revenue, count(*) AS orders
        FROM orders o
        GROUP BY o.product
        ORDER BY revenue DESC
    """).show()

    print("Top customers by spend:")
    con.sql("""
        SELECT c.name, c.region, sum(o.amount) AS total_spend
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.name, c.region
        ORDER BY total_spend DESC
    """).show()

    # --- DuckLake snapshots (time travel) ---
    # DuckLake tracks every change. List snapshots:
    print("DuckLake snapshots:")
    con.sql("SELECT * FROM ducklake_snapshots()").show()

    # Read the table as of a specific snapshot (if available):
    # con.sql("FROM customers AT (VERSION => 1)").show()

    # --- Fetch as pandas / Arrow ---
    df = con.sql("SELECT * FROM customers").fetchdf()
    print(f"pandas DataFrame:\n{df}\n")

    arrow_tbl = con.sql("SELECT * FROM orders").arrow()
    print(f"Arrow table: {arrow_tbl.num_rows} rows, schema: {arrow_tbl.schema}\n")

    con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Part 3: Hybrid — local DuckLake + remote OpenDuck in the same connection
# ═══════════════════════════════════════════════════════════════════════════════

def hybrid_local_and_remote():
    """Attach both a local DuckLake catalog and a remote OpenDuck database,
    then JOIN across them."""

    print("── 3. Hybrid: local DuckLake + remote OpenDuck ──\n")

    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})

    # --- Attach local DuckLake ---
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute("""
        ATTACH 'ducklake:local_lake' (
            DATA_PATH    './ducklake_data/',
            METADATA_PATH 'ducklake_meta.duckdb'
        );
    """)

    # --- Attach remote OpenDuck ---
    _load_openduck(con)
    con.execute("""
        ATTACH 'openduck:warehouse?endpoint=http://127.0.0.1:7878&token=dev-token'
        AS remote;
    """)

    # Create a local reference table in DuckLake
    con.execute("""
        CREATE TABLE IF NOT EXISTS local_lake.region_labels (
            region VARCHAR,
            label  VARCHAR
        );
    """)
    con.execute("""
        INSERT INTO local_lake.region_labels VALUES
            ('us-east', 'US East Coast'),
            ('us-west', 'US West Coast'),
            ('eu-west', 'Europe West');
    """)

    # --- Cross-catalog JOIN: local DuckLake × remote OpenDuck ---
    con.sql("""
        SELECT
            r.name,
            r.email,
            l.label AS region_label,
            r.joined_at
        FROM remote.users     r
        JOIN local_lake.region_labels l ON r.region = l.region
        ORDER BY r.joined_at DESC
        LIMIT 20
    """).show()

    # --- Load remote data into local DuckLake for offline analysis ---
    con.execute("""
        CREATE TABLE local_lake.events_snapshot AS
        SELECT * FROM remote.events
        WHERE event_date >= '2025-06-01'
    """)

    print("Materialized remote events into local DuckLake:")
    con.sql("SELECT count(*) AS n FROM local_lake.events_snapshot").show()

    con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Part 4: Using the openduck Python wrapper (convenience layer)
# ═══════════════════════════════════════════════════════════════════════════════

def via_openduck_wrapper():
    """Same as Part 1 but using the openduck Python package, which handles
    extension loading and ATTACH for you."""

    print("── 4. openduck Python wrapper ──\n")

    # pip install -e clients/python
    import openduck

    con = openduck.connect(
        "mydb",
        token="dev-token",
        endpoint="http://127.0.0.1:7878",
    )

    # Queries resolve through the remote catalog automatically.
    con.sql("SELECT * FROM users LIMIT 5").show()

    # Access the raw DuckDB connection for local work or hybrid queries.
    con.raw.execute("CREATE TABLE local_tags (user_id INT, tag VARCHAR)")
    con.raw.execute("INSERT INTO local_tags VALUES (1,'vip'), (2,'new')")

    con.raw.sql("""
        SELECT u.id, u.name, t.tag
        FROM cloud.users u
        JOIN local_tags t ON u.id = t.user_id
    """).show()

    con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Part 5: DuckLake-backed OpenDuck worker (server-side configuration)
# ═══════════════════════════════════════════════════════════════════════════════

def ducklake_worker_setup():
    """Shows how to start an OpenDuck worker backed by DuckLake, then query it.

    This is a documentation-only section — the worker is a Rust binary.
    """

    print("── 5. DuckLake-backed worker (server-side setup) ──\n")

    print("""\
# Start an OpenDuck worker with DuckLake storage:
#
#   export OPENDUCK_TOKEN=dev-token
#   export OPENDUCK_DUCKLAKE_METADATA=postgres://user:pass@localhost:5433/ducklake_meta
#   export OPENDUCK_DUCKLAKE_DATA=s3://my-bucket/ducklake/
#
#   # For local dev with MinIO:
#   export AWS_ACCESS_KEY_ID=minioadmin
#   export AWS_SECRET_ACCESS_KEY=minioadmin
#   export AWS_ENDPOINT_URL=http://localhost:9000
#
#   cargo run --bin openduck-worker &
#   cargo run --bin openduck-gateway &
#
# The worker internally runs:
#   INSTALL ducklake; LOAD ducklake;
#   ATTACH 'ducklake:lake' (
#       DATA_PATH 's3://my-bucket/ducklake/',
#       METADATA_PATH 'postgres://user:pass@localhost:5433/ducklake_meta'
#   );
#
# Clients connect as usual — they don't need to know the backend is DuckLake:
""")

    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    _load_openduck(con)
    con.execute("""
        ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:7878&token=dev-token'
        AS cloud;
    """)

    # These queries hit the DuckLake-backed worker transparently.
    con.sql("SHOW TABLES FROM cloud").show()
    con.sql("SELECT * FROM cloud.lake.customers LIMIT 10").show()

    con.close()


# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    sections = {
        "remote":  remote_via_openduck,
        "local":   local_ducklake,
        "hybrid":  hybrid_local_and_remote,
        "wrapper": via_openduck_wrapper,
        "worker":  ducklake_worker_setup,
    }

    chosen = sys.argv[1] if len(sys.argv) > 1 else None

    if chosen and chosen in sections:
        sections[chosen]()
    elif chosen == "all":
        for fn in sections.values():
            fn()
    else:
        print("Usage: python duckdb_sdk_ducklake.py <section>\n")
        print("Sections:")
        print("  remote   — DuckDB SDK + openduck extension (needs running gateway)")
        print("  local    — DuckDB SDK + DuckLake in-process (no server needed)")
        print("  hybrid   — Local DuckLake + remote OpenDuck JOINed together")
        print("  wrapper  — openduck Python wrapper (convenience layer)")
        print("  worker   — DuckLake-backed worker setup (documentation)")
        print("  all      — Run all sections")
        print()
        print("Quick start (no server needed):")
        print("  python examples/python/duckdb_sdk_ducklake.py local")
