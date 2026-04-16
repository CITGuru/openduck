# OpenDuck

An open-source implementation of the ideas pioneered by [MotherDuck](https://motherduck.com) — differential storage, hybrid (dual) execution, and transparent remote databases for DuckDB — available for anyone to run, extend, and build on.

MotherDuck showed that DuckDB can work beautifully in the cloud: `ATTACH 'md:mydb'`, and remote tables appear local. Queries split transparently across your laptop and the cloud. Storage is layered and snapshot-based. OpenDuck takes those architectural ideas — [differential storage](https://motherduck.com/blog/differential-storage-building-block-for-data-warehouse/), [dual execution](https://motherduck.com/videos/bringing-duckdb-to-the-cloud-dual-execution-explained/), the attach-based UX — and makes them open. Open protocol, open backend, open extension.

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD '/path/to/openduck.duckdb_extension';")
con.execute("ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;")

con.sql("SELECT * FROM cloud.users").show()                    # remote, transparent
con.sql("SELECT * FROM local.t JOIN cloud.t2 ON ...").show()   # hybrid, one query
```

## What OpenDuck does

### Differential storage

Append-only layers with PostgreSQL metadata. DuckDB sees a normal file; OpenDuck persists data as immutable sealed layers addressable from object storage. Snapshots give you consistent reads. One serialized write path, many concurrent readers.

### Hybrid (dual) execution

A single query can run partly on your machine and partly on a remote worker. The gateway splits the plan, labels each operator `LOCAL` or `REMOTE`, and inserts bridge operators at the boundaries. Only intermediate results cross the wire.

```
[LOCAL]  HashJoin(l.id = r.id)
  [LOCAL]  Scan(products)          ← your laptop
  [LOCAL]  Bridge(R→L)
    [REMOTE] Scan(sales)           ← remote worker
```

### DuckDB-native catalog

The extension implements DuckDB's `StorageExtension` and `Catalog` interfaces. Remote tables are first-class catalog entries, they participate in JOINs, CTEs, and the optimizer like local tables.

### Open protocol

OpenDuck's protocol is intentionally minimal and defined in [`execution.proto`](proto/openduck/v1/execution.proto). The data plane is two RPCs: one to execute a query and stream Arrow IPC batches back, another to cancel a running execution. Two additional RPCs handle worker lifecycle (registration and heartbeat) so the gateway can route queries by database affinity and compute context.

Because the protocol is open and simple, you're not locked into a single backend. Any service that speaks gRPC and returns Arrow can serve as an OpenDuck-compatible backend. Run the included Rust gateway, replace it with your own implementation, or plug in an entirely different execution engine — the client and extension don't care what's on the other side.

## Architecture

```
┌─────────────────────────────────────────────┐
│  DuckDB process (client)                    │
│                                             │
│  LOAD openduck                              │
│  ATTACH 'openduck:mydb' AS cloud            │
│                                             │
│  ┌─────────────────────────────────────┐    │
│  │ OpenDuckCatalog                     │    │
│  │  └─ OpenDuckSchemaEntry             │    │
│  │      └─ OpenDuckTableEntry (users)  │    │
│  │      └─ OpenDuckTableEntry (events) │    │
│  └──────────────┬──────────────────────┘    │
│                 │ gRPC + Arrow IPC          │
└─────────────────┼───────────────────────────┘
                  │
      ┌───────────▼───────────┐
      │  Gateway (Rust)       │
      │  - token auth         │
      │  - worker registry    │
      │  - affinity routing   │     ┌──────────────┐
      │  - plan splitting     │────▶│  Worker 1    │
      │  - backpressure       │◀────│  (DuckDB)    │
      │                       │     │  RegisterWorker
      │                       │     └──────────────┘
      │                       │     ┌──────────────┐
      │                       │────▶│  Worker N    │
      │                       │◀────│  (DuckDB)    │
      │                       │     │  Heartbeat   │
      └───────────────────────┘     └──────────────┘
              │
    ┌─────────┴─────────┐
    ▼                   ▼
┌──────────┐    ┌──────────────┐
│ Postgres │    │ Object store │
│ metadata │    │ sealed layers│
└──────────┘    └──────────────┘
```

## Quick start

### 1. Build the backend

```bash
cargo build --workspace
```

### 2. Build the DuckDB extension

The openduck extension is not yet published to DuckDB's extension repository, so you need to build it from source. See [`extensions/openduck/README.md`](extensions/openduck/README.md) for full prerequisites (vcpkg, bison on macOS).

```bash
cd extensions/openduck && make
```

This produces the loadable binary at:

```
extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension
```

### 3. Start the server

```bash
export OPENDUCK_TOKEN=your-token
cargo run -p openduck -- serve -d mydb -t your-token
```

### 4. Connect

Because the extension is unsigned, every DuckDB connection needs `allow_unsigned_extensions` enabled and an explicit `LOAD` with the full path to the built binary.

**Python (DuckDB SDK directly):**

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension';")
con.execute("ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=your-token' AS cloud;")

con.sql("SELECT * FROM cloud.users LIMIT 10").show()
```

You can also set `OPENDUCK_EXTENSION_PATH` to avoid hard-coding the path:

```bash
export OPENDUCK_EXTENSION_PATH=extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension
```

**Python (openduck wrapper — auto-detects the local build):**

```bash
pip install -e clients/python
export OPENDUCK_TOKEN=your-token
```

```python
import openduck

con = openduck.connect("mydb")
con.sql("SELECT 1 AS x").show()
```

The wrapper finds the extension automatically from the build tree or `OPENDUCK_EXTENSION_PATH`.

**CLI:**

```bash
duckdb -unsigned -c "
  LOAD 'extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension';
  ATTACH 'openduck:mydb?token=your-token' AS cloud;
  SELECT * FROM cloud.users LIMIT 10;
"
```

**Rust:**

```rust
use duckdb::Connection;

let conn = Connection::open_in_memory()?;
conn.execute_batch(r"
    SET allow_unsigned_extensions = true;
    LOAD 'extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension';
    ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;
")?;

let mut stmt = conn.prepare("SELECT * FROM cloud.users LIMIT 10")?;
```

> **Note:** Once the extension is published to the DuckDB extension repository, `INSTALL openduck; LOAD openduck;` will work without building from source or enabling unsigned extensions.

See [`examples/python/duckdb_sdk_ducklake.py`](examples/python/duckdb_sdk_ducklake.py) for a comprehensive walkthrough including DuckLake integration and hybrid local+remote queries.

## Layout

```
crates/
  exec-gateway/     Gateway — auth, worker registry, routing, hybrid plan splitting
  exec-worker/      Worker — embedded DuckDB, Arrow IPC streaming
  exec-proto/       Protobuf/tonic codegen + shared auth module
  openduck-cli/     Unified CLI (openduck serve|gateway|worker|query|cancel|snapshot)
  openduck-metrics/ OpenTelemetry metrics (optional OTLP exporter)
  diff-core/        Core types and StorageBackend trait
  diff-metadata/    Postgres metadata repo, GC, PgStorageBackend
  diff-layer-fs/    Append-only on-disk segment files
  diff-blob/        Sealed layer upload to S3-compatible object storage
  diff-bridge/      C ABI static library for the DuckDB extension
  diff-fuse/        Linux FUSE adapter over StorageBackend

extensions/
  openduck/         DuckDB C++ extension (StorageExtension + Catalog)

clients/
  python/           openduck Python package (pip install -e clients/python)

proto/
  openduck/v1/      Protocol definition (execution.proto)

```

## OpenDuck vs MotherDuck

MotherDuck is a commercial cloud service. OpenDuck is an open-source project inspired by its architecture.


|                          | MotherDuck            | OpenDuck                                 |
| ------------------------ | --------------------- | ---------------------------------------- |
| **What**                 | Managed cloud service | Self-hosted open-source                  |
| **Attach scheme**        | `md:`                 | `openduck:` / `od:`                      |
| **Auth**                 | `motherduck_token`    | `OPENDUCK_TOKEN`                         |
| **Differential storage** | Proprietary           | Open (Postgres metadata + object store)  |
| **Hybrid execution**     | Proprietary planner   | Open (gateway + plan splitting)          |
| **Protocol**             | Private wire format   | Open gRPC + Arrow IPC                    |
| **Backend**              | MotherDuck's cloud    | Anything implementing `ExecutionService` |
| **Extension**            | Bundled in DuckDB     | Separate loadable extension              |


OpenDuck is not wire-compatible with MotherDuck. It reimplements the same architectural ideas as an open protocol.

## OpenDuck vs Arrow Flight SQL

Arrow Flight SQL is a generic database protocol — "JDBC/ODBC over Arrow." OpenDuck is a DuckDB-specific system with a narrower scope but deeper integration.


|                      | Arrow Flight SQL                | OpenDuck                                     |
| -------------------- | ------------------------------- | -------------------------------------------- |
| **Scope**            | Any SQL database                | DuckDB-specific                              |
| **Integration**      | Separate client driver          | DuckDB StorageExtension + Catalog            |
| **Catalog**          | Server-side (`GetTables`, etc.) | Extension-side (DuckDB catalog entries)      |
| **Execution**        | Full query on server            | Hybrid — split across local and remote       |
| **Protocol surface** | ~15 RPCs                        | 4 RPCs (2 data plane + 2 worker lifecycle)   |
| **Plan format**      | SQL only                        | SQL (M2), structured plan IR (M3)            |
| **Optimizer**        | Client-side, unaware            | DuckDB optimizer sees remote tables natively |


## OpenDuck vs DuckLake

OpenDuck doesn't replace DuckLake — you use them together. They operate at different layers entirely.

DuckLake is a **lakehouse catalog**: it manages tables as Parquet files in object storage with transactional metadata in Postgres (or SQLite/DuckDB). It decides *where data lives* and *how tables are organized*.

OpenDuck is a **storage and execution layer** for DuckDB's own compute engine. It provides differential storage (append-only layers with snapshot isolation), hybrid query execution (split a single query across local and remote), and transparent remote attach (`ATTACH 'openduck:mydb'`).

If you're using DuckLake but still fall back to a `.duckdb` file for things DuckLake doesn't support yet (e.g. indexes, full-text search, or workloads that need DuckDB-native storage), OpenDuck makes that file concurrency-safe with snapshot isolation. And when you want to query a DuckLake catalog running on a remote server, OpenDuck is the transport — a worker backed by DuckLake serves queries over gRPC, and clients attach via the openduck extension without knowing or caring what the backend storage is.

|                      | DuckLake                              | OpenDuck                                    |
| -------------------- | ------------------------------------- | ------------------------------------------- |
| **Layer**            | Catalog (table → Parquet in S3)       | Storage + execution (DuckDB file I/O, gRPC) |
| **What it manages**  | Table metadata, Parquet data files    | DuckDB pages, layers, snapshots             |
| **Concurrency**      | Parquet files are immutable           | Snapshot isolation on `.duckdb` files        |
| **Remote access**    | Not built-in                          | `ATTACH 'openduck:...'` + hybrid execution  |
| **Together**         | DuckLake catalog on a remote worker → OpenDuck streams results to the client |


## Acknowledgments

OpenDuck's architecture draws heavily from MotherDuck's published work on [differential storage](https://motherduck.com/blog/differential-storage-building-block-for-data-warehouse/), [dual execution](https://motherduck.com/videos/bringing-duckdb-to-the-cloud-dual-execution-explained/), and [cloud-native DuckDB](https://motherduck.com/duckdb-book-summary-chapter7/). Credit to the MotherDuck team for pioneering these ideas.

## License

MIT