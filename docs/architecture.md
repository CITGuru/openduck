# Architecture

OpenDuck is three things in a trench coat:

1. A **DuckDB extension** that adds a `StorageExtension`, a `Catalog`, table functions (`openduck_remote`, `openduck_query`), and an `OpenDuckFileSystem` for in-process differential storage.
2. A **Rust gateway** that authenticates clients, routes queries to workers, splits hybrid plans, and bounds in-flight work.
3. A **Rust worker** that owns an embedded DuckDB, executes SQL, and streams Arrow IPC results back.

Underneath sits a **differential storage** layer with Postgres for metadata and a local filesystem (or S3-compatible object storage) for sealed layers.

## High-level diagram

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

## Component map

| Component | Crate / location | Role |
|-----------|------------------|------|
| Extension | `extensions/openduck/` | C++ DuckDB extension: catalog, secrets, FileSystem, table functions, BridgeStorage. |
| CLI | `crates/openduck-cli` | Single binary (`openduck`) for serve/gateway/worker/query/cancel/status/snapshot/gc. |
| Gateway | `crates/exec-gateway` | gRPC service; routing, plan splitting, backpressure, transaction pinning. |
| Worker | `crates/exec-worker` | gRPC service; embedded DuckDB; ingest; transactions; storage modes. |
| Protocol | `crates/exec-proto` + `proto/openduck/v1/execution.proto` | tonic-generated types and the shared `auth` module. |
| Diff core | `crates/diff-core` | `StorageBackend` trait + core types. |
| Diff metadata | `crates/diff-metadata` | Postgres metadata, leasing, GC, `PgStorageBackend`. |
| Diff layer FS | `crates/diff-layer-fs` | Append-only segment files on disk. |
| Diff blob | `crates/diff-blob` | S3-compatible upload of sealed layers. |
| Diff bridge | `crates/diff-bridge` | C ABI exposed to the C++ extension. |
| Diff FUSE | `crates/diff-fuse` | Linux FUSE adapter over `StorageBackend`. |
| Metrics | `crates/openduck-metrics` | OpenTelemetry OTLP exporter, query-latency histograms. |
| Python client | `clients/python/openduck` | Thin `duckdb` wrapper that loads + ATTACHes for you. |

## Data flow: a remote `SELECT`

1. **Client**: `con.sql("SELECT * FROM cloud.users")`.
2. **Catalog lookup**: DuckDB resolves `cloud.users` through `OpenDuckCatalog` → `OpenDuckSchemaEntry` → `OpenDuckTableEntry`. The entry knows it's remote and produces a scan that calls into the extension's gRPC client.
3. **gRPC**: The extension calls `ExecuteFragment` on the gateway with the SQL bytes, the database name, the access token, and an execution ID.
4. **Gateway**: Validates the token (`exec_proto::auth::check_token`), takes a slot from the in-flight semaphore, picks a worker.
5. **Worker selection**: The gateway uses tiered routing — database affinity → table co-location → compute_context → round-robin. See [Worker registration](#worker-registration--routing).
6. **Worker execution**: The worker runs the SQL on its embedded DuckDB. Results stream as Arrow IPC batches via `ExecuteFragmentChunk`.
7. **Streaming back**: The gateway proxies the stream to the client. The C++ extension drains all `ArrowIpcBatch` messages, deserializes them, and feeds DuckDB `DataChunk`s to the optimizer.
8. **Cleanup**: When the stream ends with `Finished(true)`, the gateway releases the semaphore slot.

## Protocol surface

`proto/openduck/v1/execution.proto` defines a single service with eight RPCs:

```
service ExecutionService {
  // Data plane
  rpc ExecuteFragment(ExecuteFragmentRequest) returns (stream ExecuteFragmentChunk);
  rpc CancelExecution(CancelRequest) returns (CancelReply);
  rpc IngestData(stream IngestChunk) returns (IngestReply);

  // Transactions
  rpc BeginTransaction(BeginTransactionRequest) returns (BeginTransactionReply);
  rpc CommitTransaction(CommitTransactionRequest) returns (CommitTransactionReply);
  rpc RollbackTransaction(RollbackTransactionRequest) returns (RollbackTransactionReply);

  // Worker lifecycle
  rpc RegisterWorker(WorkerRegistration) returns (RegisterWorkerReply);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatReply);
}
```

Two design choices to call out:

- **Plan format** is `bytes` in `ExecuteFragmentRequest`. The current encoding is a UTF-8 SQL string; the field is opaque to the wire format so the encoding can evolve without bumping the package version.
- **Errors** are returned as both a legacy `string error` field and a typed `ExecuteFragmentError { kind, message, sql_state? }`. Kinds map onto DuckDB's typed exceptions (`CATALOG`, `BINDER`, `CONSTRAINT`, `CONVERSION`, `PARSER`, `IO`, `PERMISSION`, `INTERNAL`).

## Authentication

Auth lives in `exec_proto::auth` and is shared by both gateway and worker.

- **Dev mode**: `OPENDUCK_TOKEN` unset on the server → any token (including empty) is accepted. Logs a startup warning.
- **Production**: `OPENDUCK_TOKEN` set → every RPC's `access_token` is compared with `subtle::ConstantTimeEq`. Mismatches return `Unauthenticated`.

The same check protects the data plane, transaction RPCs, ingest, `RegisterWorker`, and `Heartbeat`.

## Worker registration & routing

Workers self-register over `RegisterWorker`:

```
WorkerRegistration {
  worker_id:       "w-abc123",
  endpoint:        "http://10.0.1.5:9898",
  databases:       ["analytics", "events"],
  tables:          ["sales", "lineitem"],
  compute_context: "region=us-east-1",
  max_concurrency: 8,
  access_token:    "...",
}
```

Heartbeats every 30 s keep the registration alive. On heartbeat failure, the worker re-registers with exponential backoff.

The gateway routes each `ExecuteFragment` using a tiered preference order:

1. **Database affinity** — workers that declared the requested database.
2. **Table co-location** — workers authoritative for tables referenced in the query (`TableSourceRegistry`).
3. **Compute context** — match `request.compute_context` against worker `compute_context`.
4. **Fallback** — round-robin across all healthy workers.

The `FederationProvider` trait extends this with prefix-match (e.g. `analytics.*`) and custom routing tiers.

## Hybrid execution

When `OPENDUCK_HYBRID=1` (or `--hybrid`) is set, the gateway accepts queries that combine local and remote work. The `openduck_run('LOCAL'|'REMOTE'|'AUTO', '...')` hint marks fragments — see [Hybrid execution guide](guides/hybrid-execution.md) for syntax.

The planner builds a `PlanNode` tree where each node has a `Placement`:

| Placement | Where it runs |
|-----------|---------------|
| `Local` | Client-side DuckDB |
| `Remote` | Worker via gRPC |
| `Auto` | Resolved by `resolve_auto()` from catalog metadata |

`insert_bridges()` walks the tree and inserts a `Bridge` operator at every placement boundary:

```
[LOCAL] HashJoin(l.id = r.id)
  [LOCAL] Scan(orders)
  [LOCAL] Bridge(R→L)              ← inserted automatically
    [REMOTE] Scan(lineitem)
```

At runtime, `execute_hybrid_join()`:

1. Extracts each remote fragment from the tree.
2. Calls `ExecuteFragment` on the chosen worker.
3. Drains Arrow IPC batches and materializes them into a local DuckDB temp table (`__remote`).
4. Executes the local SQL referencing `__remote` alongside local data.
5. Returns the result.

The output is guaranteed to match the single-process baseline — the `hybrid_join_matches_baseline` parity test enforces this.

## Differential storage

Differential storage replaces "one mutable file" with "many append-only sealed layers + one active layer + Postgres metadata". The trait at the bottom of the stack:

```rust
pub trait StorageBackend: Send + Sync {
    fn read(&self, logical: LogicalRange, ctx: ReadContext) -> Result<Bytes>;
    fn write(&self, logical_offset: u64, data: &[u8]) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn fsync(&self) -> Result<()>;
    fn seal(&self) -> Result<SnapshotId>;
    fn truncate(&self, size: u64) -> Result<()>;
}
```

Implementations:

- `InMemoryBackend` — reference for tests.
- `PgStorageBackend` — production. Postgres metadata + local segment files, with optional sealed-layer upload to S3.

### Layers, extents, snapshots

- **Layer**: an append-only segment file. One active layer at a time; many sealed (immutable) layers.
- **Extent**: `(layer_id, file_offset, length, logical_offset)`. Reads are resolved by walking extents newest-first.
- **Snapshot**: a UUID that fixes the visible set of layers and extents. Sealing produces a new snapshot and starts a fresh active layer.
- **Refcounting + GC**: `gc::compact_extents` collapses superseded extents; `gc::gc_candidates` lists layers no longer referenced by any snapshot. The `openduck gc` CLI walks this and deletes the underlying segment files.

### Concurrency: leasing and fencing

A single writer at a time. The writer acquires a lease, renews it on a heartbeat, and the metadata layer fences operations from a stale lease. Multiple readers run concurrently. `examples/rust/live_tx_ingest.rs` and `crates/diff-metadata/src/pg_storage.rs` are good entry points to read.

### Tiering

When a blob store is attached (`PgStorageBackend::set_blob_store`), `seal()` automatically uploads the newly sealed layer to S3 and rewrites its `storage_uri` accordingly. Reads transparently fetch from S3 when the local file is gone. Configure with `AWS_*` env vars or DuckDB secrets for S3.

## Storage modes (extension side)

How DuckDB sees differential storage in the worker (or in your client process):

| Mode | Platform | Diff storage | Mechanism |
|------|----------|--------------|-----------|
| **Direct** | Everywhere | No | Plain `DuckDB::Open(path)`. |
| **FUSE** | Linux (macFUSE experimental) | Yes | `openduck-fuse` mounts a directory; DuckDB opens `database.duckdb` inside it; every I/O goes through the kernel FUSE driver into `StorageBackend`. |
| **In-Process** | Everywhere | Yes | The extension registers an `OpenDuckFileSystem` that intercepts `openduck://` paths and routes them directly to `BridgeStorage`, which calls into the Rust `StorageBackend` via the C ABI in `diff-bridge`. |

See [Differential storage guide](guides/differential-storage.md) for setup and trade-offs.

## Transactions and ingest

`BeginTransaction` opens a worker connection, runs `BEGIN TRANSACTION`, and returns an opaque `transaction_id`. Subsequent `ExecuteFragment` / `IngestData` calls carrying the same id are pinned to the same worker connection (via gateway affinity + a per-worker connection registry). `CommitTransaction` and `RollbackTransaction` release it.

`IngestData` is a client-streaming RPC. The first chunk carries `IngestMetadata` (database, staging table name, columns, optional `transaction_id`). Subsequent chunks carry Arrow IPC batches that the worker appends into a `TEMP TABLE` on the pinned connection. The client typically follows up with an `INSERT INTO target SELECT * FROM <staging>` `ExecuteFragment` in the same transaction.

## Backpressure and limits

- A semaphore on the gateway bounds in-flight executions (`OPENDUCK_MAX_IN_FLIGHT`, default `64`).
- gRPC encode/decode is capped at 64 MiB. Large result sets stream as multiple Arrow IPC batches.
- Connect timeout to a worker is 5 s; per-RPC `ExecuteFragment` is 300 s; `CancelExecution` is 10 s.

## Observability

`openduck-metrics` wires OpenTelemetry. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the worker exports:

- Query latency histograms (per-RPC, with success/error labels).
- Layer seal durations.
- Counters for in-flight executions, registrations, heartbeats.

The `cargo bench -p exec-worker` harness includes a query-latency baseline and regression thresholds.

## Security model

| Surface | Defence |
|---------|---------|
| Token auth on all RPCs | `subtle::ConstantTimeEq` constant-time comparison. |
| Path traversal via `db_name` | Rejects `/`, `\`, and `..` in in-process mode. |
| Path traversal via `storage_uri` | `resolve_path` rejects absolute paths and `..` before any file delete. |
| Arrow column injection | `quote_ident()` in `materialize_batches`. |
| Python client SQL injection | Strict identifier check on `alias`, single-quote escape for `db_name`/`token`/`endpoint`/`extension_path`. |
| DuckLake attach injection | Single-quote escape on metadata + data paths. |
| Credential leakage | `postgres_url` redacted in `Debug` and `LIST SECRETS`. |
| Resource exhaustion | In-flight semaphore + 64 MiB gRPC limit + per-RPC timeouts. |

## Where to dig in

- Read the protocol: [`proto/openduck/v1/execution.proto`](../proto/openduck/v1/execution.proto).
- Read the gateway: [`crates/exec-gateway/src/lib.rs`](../crates/exec-gateway/src/lib.rs), [`hybrid.rs`](../crates/exec-gateway/src/hybrid.rs).
- Read the worker: [`crates/exec-worker/src/lib.rs`](../crates/exec-worker/src/lib.rs), [`ingest.rs`](../crates/exec-worker/src/ingest.rs), [`transactions.rs`](../crates/exec-worker/src/transactions.rs).
- Read the storage: [`crates/diff-metadata/src/pg_storage.rs`](../crates/diff-metadata/src/pg_storage.rs).
- Run an example: every flow above has a runnable example in [`examples/`](../examples/README.md).
