# OpenDuck Examples

Runnable examples demonstrating OpenDuck's core capabilities — client connections, differential storage, hybrid execution, gRPC round-trips, and worker configuration.

## Prerequisites

```bash
cargo build --workspace

# For storage_backend example only — needs Postgres:
docker compose -f docker/docker-compose.yml up -d
export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
for f in crates/diff-metadata/migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done
```

## Client examples

| Example | What it shows | Run |
|---------|---------------|-----|
| `rust/client_rust` | DuckDB Rust client — queries, aggregation, Arrow batches, transactions, hybrid joins | `cargo run --example client_rust` |
| `python/client_python.py` | Python client — all connection methods, pandas/Arrow, hybrid queries | `python examples/python/client_python.py` |
| `python/duckdb_sdk_ducklake.py` | DuckDB Python SDK + DuckLake — local lakehouse, remote gateway, hybrid joins | `python examples/python/duckdb_sdk_ducklake.py local` |

### Rust quick start

```rust
use duckdb::Connection;

let conn = Connection::open_in_memory()?;
conn.execute_batch(r"
    SET allow_unsigned_extensions = true;
    LOAD 'openduck';
    ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;
")?;

let mut stmt = conn.prepare("SELECT * FROM cloud.users LIMIT 10")?;
let rows = stmt.query_map([], |row| {
    Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
})?;
```

### Python quick start

```python
import openduck

con = openduck.connect("mydb", token="my-token")
con.sql("SELECT * FROM users").show()
```

### CLI quick start

```bash
export OPENDUCK_TOKEN=your-token
duckdb -unsigned -c "
  LOAD 'openduck';
  ATTACH 'openduck:mydb?token=your-token' AS cloud;
  SELECT * FROM cloud.users LIMIT 10;
"
```

## Infrastructure examples

| Example | What it shows | Run |
|---------|---------------|-----|
| `rust/fuse_concept` | How FUSE presents differential storage as a file — page writes, snapshots, isolation | `cargo run --example fuse_concept` |
| `fuse_duckdb.sh` | Full end-to-end: FUSE mount → DuckDB → create tables → query (Linux only) | `bash examples/fuse_duckdb.sh` |
| `rust/diff_storage_demo` | In-memory differential storage — writes, overlaps, snapshots, isolation | `cargo run --example diff_storage_demo` |
| `rust/grpc_roundtrip` | Start worker + gateway in-process, send SQL via gRPC, receive Arrow IPC | `OPENDUCK_TOKEN=demo cargo run --example grpc_roundtrip` |
| `rust/hybrid_plan` | Build a hybrid LOCAL/REMOTE plan, insert bridges, resolve AUTO, print EXPLAIN | `cargo run --example hybrid_plan` |
| `rust/worker_custom` | Worker configurations (in-memory, file-backed, DuckLake), gateway routing, cancellation | `OPENDUCK_TOKEN=demo cargo run --example worker_custom` |
| `rust/storage_backend` | Postgres-backed storage — bootstrap, write, seal, snapshot read (needs Postgres) | `cargo run --example storage_backend` |
| `rust/secrets_and_storage` | DuckDB secrets for storage configuration — create, list, redact, URI params | `cargo run --example secrets_and_storage` |
| `rust/bridge_storage` | C ABI bridge to Rust storage — write, read, fsync, overlay resolution (needs Postgres) | `cargo run --example bridge_storage` |
| `rust/hybrid_execution` | End-to-end hybrid LOCAL+REMOTE join — worker data + local data via Arrow IPC | `OPENDUCK_TOKEN=demo cargo run --example hybrid_execution` |
| `rust/snapshot_reads` | Point-in-time reads — write, seal, diverge, read-at-snapshot (needs Postgres) | `cargo run --example snapshot_reads` |

## What each example demonstrates

### `rust/client_rust` — DuckDB client usage

Shows 11 patterns: version check, LOAD + ATTACH, query + fetch, aggregation, Arrow result batches, hybrid local+remote join, CSV loading, multiple attached databases, parameterized queries, transactions, and error handling.

### `python/duckdb_sdk_ducklake.py` — DuckDB Python SDK + DuckLake

Five self-contained sections (run individually or all at once):

1. **`remote`** — DuckDB Python SDK with the openduck extension: `LOAD`, `ATTACH`, query remote tables, fetch as pandas/Arrow
2. **`local`** — DuckLake in-process with no server: create tables, insert data, aggregation, JOINs, snapshots (time travel), pandas/Arrow export
3. **`hybrid`** — Attach both a local DuckLake catalog and a remote OpenDuck gateway in the same connection, then cross-catalog JOIN
4. **`wrapper`** — Same as #1 but using the `openduck` Python package (handles extension loading and `ATTACH` automatically)
5. **`worker`** — Documents how to start a DuckLake-backed OpenDuck worker (Postgres metadata + S3 data) and query it from any client

### `rust/diff_storage_demo` — Differential storage internals

Walks through the core storage algorithm that powers OpenDuck:

1. Write and read bytes at arbitrary offsets
2. Overlapping writes (last write wins via extent map)
3. Seal a snapshot (freeze active layer into immutable)
4. Write after seal — snapshot remains frozen, tip reflects new data
5. Multiple snapshots — each sees its own consistent view
6. Sparse writes — unwritten regions read as zeroes
7. Large sequential writes — the pattern DuckDB uses through FUSE

### `rust/grpc_roundtrip` — End-to-end gRPC flow

Starts a worker and gateway in-process, sends SQL through the gateway's `ExecuteFragment` RPC, and streams Arrow IPC results. The minimal viable OpenDuck stack.

### `rust/hybrid_plan` — Query plan splitting

Builds a query plan tree with `LOCAL` and `REMOTE` scan nodes, inserts `Bridge(R→L)` operators at placement boundaries, resolves `AUTO` placement using catalog metadata, and prints `EXPLAIN`-style output showing where each operator runs.

### `rust/worker_custom` — Worker configurations

Shows how to configure workers for different environments:

- **In-memory** — default, no persistence (tests, development)
- **File-backed** — persistent DuckDB file
- **DuckLake-backed** — Postgres metadata + S3 data path
- **Gateway routing** — round-robin across multiple workers
- **Cancellation** — cancel running executions via gRPC

### `rust/fuse_concept` — Storage as a file

Simulates what the FUSE mount does, using the in-memory backend (no Linux/FUSE required):

1. DuckDB-style page writes (4KB blocks at specific offsets)
2. Reading pages back (extent resolution)
3. CHECKPOINT → seal snapshot → active layer frozen
4. Post-snapshot writes → old snapshot unaffected
5. Snapshot isolation: two readers see different consistent views
6. Explains the full FUSE mount flow with `openduck-fuse` CLI

### `fuse_duckdb.sh` — Full FUSE + DuckDB (Linux)

End-to-end shell script (Linux with FUSE3 + Postgres required):

1. Applies Postgres migrations
2. Mounts `openduck-fuse` with a fresh database
3. Opens DuckDB against the mounted `database.duckdb` file
4. Creates tables, inserts data, runs queries
5. Explains what happened under the hood (FUSE → layers → extents)
6. Cleans up (unmount + remove temp dirs)

### `rust/storage_backend` — Postgres storage pipeline

Full end-to-end with the Postgres-backed differential storage: bootstrap a database, write data, seal a snapshot, and verify reads match across tip and snapshot.

### `rust/secrets_and_storage` — Configuration via DuckDB secrets

Shows how the `openduck_storage` secret type integrates with DuckDB's native secret management:

1. Creating default and named secrets with `postgres_url` and `data_dir`
2. Using `?secret=NAME` and `?data_dir=` URI parameters
3. The resolution cascade: secret → env vars → in-memory fallback
4. Automatic redaction of `postgres_url` in secret listings

### `rust/bridge_storage` — C ABI bridge to Rust

Demonstrates the same C ABI that the C++ DuckDB extension uses:

1. `openduck_bridge_open` — connect to Postgres-backed storage
2. `openduck_bridge_write` — write pages at logical offsets
3. `openduck_bridge_read` — read back with extent resolution
4. `openduck_bridge_fsync` — flush to durable storage
5. Overlay writes — partial overwrites resolve correctly (newest wins)

### `rust/hybrid_execution` — End-to-end hybrid query

The full hybrid execution flow with real gRPC:

1. Start a worker with remote data
2. Gateway fetches remote data via Arrow IPC
3. Materializes remote results into a local temp table
4. Executes the local join against local + materialized data
5. Verifies the result matches a single-process baseline

### `rust/snapshot_reads` — Point-in-time consistency

Demonstrates snapshot isolation in differential storage:

1. Write data, seal a snapshot
2. Write more data (tip diverges)
3. Open a read-only handle at the snapshot UUID
4. Read from snapshot → sees pre-divergence state
5. Read from tip → sees all writes including post-snapshot
6. Shows `?snapshot=<uuid>` URI parameter usage

## Environment variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `OPENDUCK_TOKEN` | All gRPC examples | Authentication token (constant-time validated) |
| `OPENDUCK_ENDPOINT` | Client examples | Gateway endpoint |
| `DATABASE_URL` | `storage_backend`, `bridge_storage`, `snapshot_reads` | Postgres connection string |
| `OPENDUCK_POSTGRES_URL` | Extension | Postgres URL for storage (env-based fallback) |
| `OPENDUCK_DATA_DIR` | Extension | Local data directory (env-based fallback) |
| `OPENDUCK_HYBRID` | `hybrid_execution` | Set to `1` to enable hybrid planning in gateway |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Metrics | OpenTelemetry OTLP endpoint |
