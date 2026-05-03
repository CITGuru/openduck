# Overview

OpenDuck is an open-source implementation of the architecture pioneered by [MotherDuck](https://motherduck.com): differential storage, dual (hybrid) execution, and transparent remote databases — built as a DuckDB extension plus a small set of Rust services.

You attach a remote database in one line:

```sql
ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;
SELECT * FROM cloud.users;
```

Tables resolve transparently. Joins between local and remote tables run as a single query. The protocol is open. The backend is open. The extension is open. There is no managed service to sign up for.

## What problems does it solve?

### 1. DuckDB on the network

DuckDB is a single-process embedded database. There is no built-in way to make a remote DuckDB look like a local one and have queries route across them. OpenDuck adds a `StorageExtension` and a `Catalog` that present remote tables as native DuckDB catalog entries — they participate in joins, CTEs, and the optimizer like local tables do.

### 2. DuckDB with cloud-shaped storage

The standard DuckDB storage format is a single mutable file. That works locally; it doesn't work for concurrent writers, point-in-time snapshots, or layered storage on object storage. OpenDuck implements **differential storage**: append-only sealed layers backed by Postgres metadata, with snapshot reads and a single serialized writer. DuckDB sees a regular file; OpenDuck handles the layering underneath.

### 3. Splitting one query across two machines

Sometimes the data is remote but small joining/filtering work belongs locally. Sometimes a heavy aggregation belongs on a beefy worker but you want the result in your local notebook. OpenDuck's gateway can split a single query plan: each operator is labeled `LOCAL` or `REMOTE`, bridge operators are inserted at boundaries, and only intermediate results cross the wire. This is the same pattern MotherDuck calls **dual execution**.

## Core capabilities

### Differential storage

Append-only layers, Postgres for metadata, local filesystem or S3-compatible object storage for sealed layers. Snapshot UUIDs for consistent reads. One serialized writer with leasing and fencing; many concurrent readers.

### Hybrid (dual) execution

The gateway parses `openduck_run('REMOTE', '...')` hints, splits plans across local and remote fragments, materializes remote Arrow IPC batches into a local temp table, and runs the local portion against the combined view. Output is guaranteed to match a single-process baseline (covered by a golden parity test).

### DuckDB-native catalog

The extension implements DuckDB's `StorageExtension` and `Catalog` interfaces. `OpenDuckCatalog` → `OpenDuckSchemaEntry` → `OpenDuckTableEntry`. Remote tables participate in the optimizer just like local ones.

### Open protocol

The wire format is defined in [`proto/openduck/v1/execution.proto`](../proto/openduck/v1/execution.proto). Eight RPCs total:

- **Data plane**: `ExecuteFragment`, `CancelExecution`, `IngestData`
- **Transactions**: `BeginTransaction`, `CommitTransaction`, `RollbackTransaction`
- **Lifecycle**: `RegisterWorker`, `Heartbeat`

Any service that speaks this protocol and returns Arrow IPC can serve as an OpenDuck-compatible backend.

### Three storage modes

| Mode | Platform | Differential storage | Notes |
|------|----------|----------------------|-------|
| **Direct** | Everywhere | No | Plain DuckDB file. The default. |
| **FUSE** | Linux | Yes | Mounts diff storage as a file. macFUSE experimental. |
| **In-Process** | Everywhere | Yes | Registers a DuckDB FileSystem in the worker process. |

See [Differential storage](guides/differential-storage.md) for setup.

### DuckLake interop

OpenDuck and DuckLake operate at different layers and complement each other. A DuckLake catalog can run on a worker that you reach via OpenDuck's `ATTACH 'openduck:...'`. OpenDuck handles the transport; DuckLake handles the table format.

## How it compares

### vs. MotherDuck

|                          | MotherDuck            | OpenDuck                                 |
|--------------------------|-----------------------|------------------------------------------|
| What                     | Managed cloud service | Self-hosted open-source                  |
| Attach scheme            | `md:`                 | `openduck:` / `od:`                      |
| Auth                     | `motherduck_token`    | `OPENDUCK_TOKEN`                         |
| Differential storage     | Proprietary           | Open (Postgres metadata + object store)  |
| Hybrid execution         | Proprietary planner   | Open (gateway + plan splitting)          |
| Protocol                 | Private wire format   | Open gRPC + Arrow IPC                    |
| Backend                  | MotherDuck's cloud    | Anything implementing `ExecutionService` |
| Extension                | Bundled in DuckDB     | Separate loadable extension              |

OpenDuck is **not** wire-compatible with MotherDuck — it reimplements the architecture as an open protocol.

### vs. Arrow Flight SQL

Arrow Flight SQL is a generic database protocol. OpenDuck is a DuckDB-specific system with a narrower scope and deeper integration.

|                  | Arrow Flight SQL                | OpenDuck                                     |
|------------------|---------------------------------|----------------------------------------------|
| Scope            | Any SQL database                | DuckDB-specific                              |
| Integration      | Separate client driver          | DuckDB StorageExtension + Catalog            |
| Catalog          | Server-side (`GetTables`, etc.) | Extension-side (DuckDB catalog entries)      |
| Execution        | Full query on server            | Hybrid — split across local and remote       |
| Protocol surface | ~15 RPCs                        | 8 RPCs                                       |
| Optimizer        | Client-side, unaware            | DuckDB optimizer sees remote tables natively |

### vs. DuckLake

DuckLake is a **lakehouse catalog** — tables as Parquet in object storage with transactional metadata. OpenDuck is a **storage and execution layer** for DuckDB's own engine. They are complementary, not alternatives.

|                  | DuckLake                              | OpenDuck                                    |
|------------------|---------------------------------------|---------------------------------------------|
| Layer            | Catalog (table → Parquet in S3)       | Storage + execution (DuckDB file I/O, gRPC) |
| What it manages  | Table metadata, Parquet data files    | DuckDB pages, layers, snapshots             |
| Concurrency      | Parquet files are immutable           | Snapshot isolation on `.duckdb` files       |
| Remote access    | Not built-in                          | `ATTACH 'openduck:...'` + hybrid execution  |
| Together         | DuckLake catalog on a remote worker → OpenDuck streams results to the client |

## When to use OpenDuck

Reach for OpenDuck when you want:

- **Remote DuckDB that feels local** — `ATTACH` and query, no driver glue.
- **Hybrid queries** — local and remote in one statement, optimizer-aware.
- **Snapshot isolation on a `.duckdb` file** — concurrent readers, sealed snapshots, point-in-time reads.
- **An open protocol** — write your own backend, bring your own catalog, swap the engine.
- **Operational ownership** — your hardware, your data, your auth, your auditing.

It is **not** a managed service, an OLTP database, or a replacement for DuckLake's table format. It is a thin transport and storage layer that makes DuckDB practical for cloud-shaped workloads without sacrificing the embedded-DB ergonomics.

## What's in the box

Differential storage with leasing and tiering, dual execution with bridge insertion and parity tests, security hardening (constant-time auth, path-traversal protections, message-size and timeout limits), and observability (OpenTelemetry OTLP, benchmark harness with regression thresholds).

## Where to go next

- New here? Start with [Getting started](guides/getting-started.md).
- Want to see the full design? [Architecture](architecture.md).
- Need to deploy? [Configuration](configuration.md) and [Deployment](guides/deployment.md).
- Want to read code? [`examples/`](../examples/README.md) has runnable demos for every feature.
