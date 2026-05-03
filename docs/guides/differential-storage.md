# Differential storage

DuckDB normally stores a database as one mutable file. That works locally, but it doesn't give you concurrent writers, point-in-time snapshots, or layered storage on object storage.

OpenDuck adds **differential storage**: an append-only log of immutable sealed layers, plus one active layer at the tip, with Postgres holding the metadata. DuckDB still sees a regular file; OpenDuck handles the layering underneath.

This guide covers when to use differential storage, the three ways to plug DuckDB into it, and how to set each one up.

## Concepts in 30 seconds

- **Layer** — an append-only segment file. One active layer at a time; many sealed (immutable) layers per database.
- **Extent** — `(layer_id, file_offset, length, logical_offset)`. Reads are resolved by walking extents newest-first.
- **Snapshot** — a UUID that fixes the visible set of extents and layers. Sealing produces a new snapshot and starts a fresh active layer. Snapshot UUIDs are how you do point-in-time reads.
- **Lease** — a single writer holds a renewable lease; metadata fences operations from a stale lease. Many readers run concurrently with the writer.
- **GC** — `gc::compact_extents` collapses superseded extents; `gc::gc_candidates` lists layers no longer referenced by any snapshot. The `openduck gc` CLI walks both and deletes underlying segment files.
- **Tiering** — when a blob store is attached, `seal()` uploads the new layer to S3-compatible object storage and rewrites its `storage_uri`. Reads transparently fetch from S3 if the local file is gone.

## When to use it

| You want… | Use differential storage? |
|-----------|---------------------------|
| Quick local DuckDB, no Postgres, no S3 | No — the default `direct` mode is fine. |
| Concurrent readers safely while a writer is appending | Yes |
| Point-in-time snapshot reads (analytics on yesterday's data) | Yes |
| Sealed layers tiered to S3 | Yes |
| `.duckdb` semantics with cloud-shaped storage underneath | Yes |
| Lakehouse table format (Parquet, Iceberg-style metadata) | No — use [DuckLake](deployment.md#ducklake-attach). They compose. |

## Storage modes

There are three ways to plug DuckDB into a `StorageBackend`. They share the same trait under the hood; they differ in how DuckDB sees the file.

```
                     ┌─────────────────────────┐
                     │     StorageBackend      │
                     │  (InMemory, Pg, S3...)  │
                     └────────┬────────────────┘
                              │
            ┌─────────────────┼──────────────────┐
            │                 │                  │
   ┌────────▼─────┐  ┌────────▼───────┐  ┌──────▼──────────┐
   │   Direct     │  │  FUSE (Linux)  │  │  In-Process FS  │
   │  local file  │  │  kernel mount  │  │  DuckDB FileSystem│
   │  or memory   │  │  any process   │  │  same process    │
   └──────────────┘  └────────────────┘  └─────────────────┘
```

|                            | Direct | FUSE | In-Process |
|----------------------------|--------|------|------------|
| Platform                   | Everywhere | Linux (macFUSE experimental) | Everywhere |
| Differential storage       | No | Yes | Yes |
| Snapshots                  | No | Yes | Yes |
| Privileges                 | None | Root or `fuse` group | None |
| Latency per I/O            | Native | ~5–20 µs (kernel round-trip) | ~0.1 µs (function call) |
| Accessible to              | Only the worker | Any process on the system | Only this DuckDB instance |
| Setup                      | None | Mount/unmount lifecycle | Register at startup |
| Status                     | Stable | Stable on Linux | Implemented |

## Mode 1 — Direct (default)

The current default. DuckDB opens a local file (or runs in-memory). No differential storage, no Postgres, no metadata.

```bash
openduck -d /path/to/mydb.duckdb -p $OPENDUCK_TOKEN
```

Use this for development, single-node deployments, or any workload where snapshot isolation isn't required.

## Mode 2 — FUSE (Linux)

The `openduck-fuse` binary mounts a directory containing `database.duckdb`. DuckDB opens that file normally; every read/write goes through the kernel's FUSE driver to the `StorageBackend`.

### Setup

```bash
# 0. Bring up Postgres (only required for differential modes)
docker compose -f docker/docker-compose.yml up -d
export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
for f in crates/diff-metadata/migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done

# 1. Start the FUSE mount
openduck-fuse \
  --db mydb \
  --postgres "$DATABASE_URL" \
  --data-dir /var/openduck \
  --mountpoint /mnt/od

# 2. Point a worker at the mountpoint
openduck --storage fuse --mountpoint /mnt/od -p $OPENDUCK_TOKEN
```

The end-to-end script `examples/fuse_duckdb.sh` and the `examples/rust/fuse_concept` example are the fastest way to see what FUSE does without setting up the kernel module.

### Pros

- Any process on the host can open the mounted file (not just DuckDB).
- Standard filesystem semantics — `cat`, `cp`, `du`, `ls -la` all work.
- Can be mounted read-only at a specific snapshot.

### Cons

- Linux only. macFUSE is experimental and requires kernel extensions.
- Two context switches per I/O.
- You manage the mount/unmount lifecycle.

## Mode 3 — In-Process (cross-platform)

The extension registers an `OpenDuckFileSystem` with DuckDB's `FileSystem::RegisterSubSystem()`. Paths starting with `openduck://` are intercepted and routed directly to `BridgeStorage`, which calls into the Rust `StorageBackend` via the C ABI in `diff-bridge`.

### CLI form (worker-side)

```bash
openduck \
  --storage in-process \
  -d mydb \
  --postgres "$DATABASE_URL" \
  --data-dir /var/openduck \
  -p $OPENDUCK_TOKEN
```

### Extension-side configuration via DuckDB secrets

In a connected DuckDB session:

```sql
-- Default secret — auto-discovered, no ?secret= needed
CREATE SECRET openduck_storage (
    TYPE openduck_storage,
    postgres_url 'postgres://localhost/openduck',
    data_dir '/var/openduck'
);

ATTACH 'openduck://mydb/database.duckdb' AS local_db;
SELECT * FROM local_db.users;
```

Or a named secret picked via `?secret=`:

```sql
CREATE SECRET prod_storage (
    TYPE openduck_storage,
    postgres_url 'postgres://prod-host/openduck',
    data_dir '/mnt/prod/openduck'
);

ATTACH 'openduck://mydb/database.duckdb?secret=prod_storage' AS prod_db;
```

### Resolution cascade

1. `?secret=NAME` in the URI
2. Default secret named `openduck_storage`
3. Environment variables `OPENDUCK_POSTGRES_URL` + `OPENDUCK_DATA_DIR`
4. In-memory fallback (no persistence; logs a warning)

`?data_dir=PATH` always overrides whichever `data_dir` was resolved:

```sql
-- Use prod_storage's postgres_url but a local cache directory
ATTACH 'openduck://mydb/database.duckdb?secret=prod_storage&data_dir=/tmp/local-cache' AS db;
```

### Pros

- macOS, Linux, Windows — anywhere DuckDB runs.
- No kernel round-trips, no context switches.
- No mount privileges, no kernel extensions.
- Normal debugging tools work end-to-end.

### Cons

- Only the DuckDB instance that registered the FileSystem can use it.

## Snapshots

Sealing a snapshot freezes the active layer and starts a fresh one. The seal returns a UUID:

```bash
openduck snapshot seal --postgres "$DATABASE_URL" --db mydb --data-dir /var/openduck
# → 2f9c7c1a-3a08-4f2a-9d2c-8b4f1c2a9e3b

openduck snapshot list --postgres "$DATABASE_URL" --db mydb
# SNAPSHOT ID                            CREATED AT
# 2f9c7c1a-...                           2026-04-29 16:32:17 UTC
# 1a2b3c4d-...                           2026-04-22 09:11:02 UTC
```

To read at a snapshot — writes are rejected on the resulting handle:

```sql
ATTACH 'openduck://mydb/database.duckdb?snapshot=2f9c7c1a-...' AS snap_db;
SELECT * FROM snap_db.my_table;
```

For the sealing API, the reader/writer interaction, and a full walk-through, see [Snapshots and garbage collection](snapshots-and-gc.md).

## S3-compatible tiering

When a blob store is attached to the backend, `seal()` automatically uploads the newly sealed layer to object storage and rewrites its `storage_uri` accordingly. Reads transparently fetch from S3 when the local file isn't there.

For DuckLake-style configurations on MinIO or AWS S3, set the standard AWS env vars:

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://localhost:9000
```

The `examples/rust/storage_backend.rs` and `examples/rust/snapshot_reads.rs` examples exercise the upload + transparent read path.

## Concurrency: leases and fencing

Differential storage allows a single writer at a time. The writer acquires a lease, sends periodic `renew_write_lease` heartbeats, and releases it on shutdown. Postgres metadata fences operations originating from a stale lease — if a writer's lease expires (network blip, crash), the next writer gets a fresh lease and any straggler writes from the old one are rejected.

Readers don't need a lease; they take a snapshot view consistent with the metadata at read-start time.

## Garbage collection

```bash
# Dry-run — list candidate layers, no deletes
openduck gc --postgres "$DATABASE_URL" --db mydb --data-dir /var/openduck --dry-run

# Run for real
openduck gc --postgres "$DATABASE_URL" --db mydb --data-dir /var/openduck
```

GC validates every `storage_uri` before deletion (rejects absolute paths and `..` components) and only removes layers that aren't referenced by any snapshot. See [Snapshots and garbage collection](snapshots-and-gc.md) for the full lifecycle.

## Worth a look

- `examples/rust/diff_storage_demo` — in-memory differential storage walk-through (writes, overlaps, snapshots, isolation).
- `examples/rust/storage_backend` — Postgres-backed storage end-to-end (needs Postgres).
- `examples/rust/bridge_storage` — the C ABI bridge that the C++ extension uses.
- `examples/rust/secrets_and_storage` — DuckDB secret cascade with `?secret=` and `?data_dir=`.
- `examples/rust/snapshot_reads` — point-in-time reads via `?snapshot=<uuid>`.
- `examples/rust/fuse_concept` — what FUSE does, simulated with the in-memory backend (no Linux required).
- `examples/fuse_duckdb.sh` — full FUSE + DuckDB e2e (Linux + Postgres required).

## Where to next

- [Snapshots and garbage collection](snapshots-and-gc.md) — sealing, point-in-time reads, retention.
- [Deployment](deployment.md) — Postgres, MinIO, multi-worker, Docker Compose.
- [Configuration](../configuration.md) — every storage-related flag and env var.
