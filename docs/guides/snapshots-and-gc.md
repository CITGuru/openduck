# Snapshots and garbage collection

Differential storage gives you append-only sealed layers. Two operations are how you actually get value out of that:

- **Sealing a snapshot** — freeze the active layer into an immutable one and start a fresh active layer. The seal returns a UUID that names a consistent point-in-time view.
- **Garbage collection** — drop layers no longer referenced by any snapshot, and compact superseded extents.

This guide covers both, including the safe defaults around path validation and the read-only mode for snapshot reads.

## Mental model

```
                          time ─────►
        active layer               sealed layers
        ┌─────┐         seal       ┌─────┐
        │  L4 │ ──────────────────▶│  L4 │ (snapshot S2)
        └─────┘                    └─────┘
            ▲                          ▲
            │ writes                   │
            │ accumulate               │
        new active                 ┌─────┐
        layer L5                   │  L3 │ (snapshot S1)
                                   └─────┘
                                   ┌─────┐
                                   │  L2 │
                                   └─────┘
                                   ┌─────┐
                                   │  L1 │
                                   └─────┘
```

Each snapshot is a UUID + a frozen set of (layer, extent) refs. Reads at a snapshot resolve through that frozen set. Writes only go to the current active layer.

## Sealing

```bash
openduck snapshot seal \
  --postgres "$DATABASE_URL" \
  --db mydb \
  --data-dir /var/openduck
# → 2f9c7c1a-3a08-4f2a-9d2c-8b4f1c2a9e3b
```

Behind the scenes the writer:

1. Flushes any in-memory writes to the active layer file.
2. Calls `seal()` on the `StorageBackend`, which marks the active layer immutable, creates a snapshot row in Postgres referencing the visible extent set, and starts a fresh active layer.
3. (Optional) If a blob store is attached, uploads the newly sealed layer to S3 and rewrites its `storage_uri`.

Sealing is metadata-cheap; the bulk of the work is the optional upload. While a seal is in flight, readers continue to read; new writes wait briefly for the active-layer swap.

## Listing snapshots

```bash
openduck snapshot list \
  --postgres "$DATABASE_URL" \
  --db mydb
# SNAPSHOT ID                            CREATED AT
# 2f9c7c1a-3a08-4f2a-9d2c-8b4f1c2a9e3b   2026-04-29 16:32:17 UTC
# 1a2b3c4d-5e6f-7890-abcd-ef0123456789   2026-04-22 09:11:02 UTC
```

The CLI sorts newest first. Snapshots are durable until you GC their layers.

## Reading at a snapshot

The `?snapshot=<uuid>` URI parameter opens a read-only handle bound to that snapshot. Writes are rejected:

```sql
ATTACH 'openduck://mydb/database.duckdb?snapshot=2f9c7c1a-...' AS snap_db;

SELECT * FROM snap_db.users;        -- reads the snapshot view
SELECT * FROM snap_db.events
WHERE ts > '2025-01-01';            -- consistent point-in-time

-- Any write fails with a typed PermissionException:
INSERT INTO snap_db.users VALUES (...);  -- ERROR
```

Under the hood, `openduck_bridge_open_snapshot` opens the storage in read-only mode at that UUID. The C++ `BridgeStorage` enforces the read-only contract — any write attempt returns an error.

This is the primitive to build:

- Time-travel analytics ("what did the table look like Monday?").
- Reproducible reports ("query against the snapshot we sealed before the deploy").
- Concurrent readers with strict isolation guarantees.

See `examples/rust/snapshot_reads.rs` for an end-to-end walkthrough (write → seal → diverge tip → read at snapshot vs read at tip).

## Snapshots vs the writer lease

- A single writer holds a renewable lease (`acquire_write_lease` / `renew_write_lease` / `release_write_lease`).
- The lease fences operations from a stale writer: if a writer's lease expires (network blip, crash), the next writer gets a fresh lease and any straggler writes from the old one are rejected by Postgres metadata.
- Snapshots are independent of the lease — readers don't need one. Each snapshot is a frozen view that's safe to read concurrently with new writes.

## Garbage collection

GC has two halves:

1. **Extent compaction** — collapse extents superseded by newer writes. Reduces metadata overhead without touching files.
2. **Layer GC** — drop sealed layer files that aren't referenced by any snapshot.

Both are exposed via the `openduck gc` CLI:

```bash
# Dry-run — list candidate layers, don't delete anything
openduck gc \
  --postgres "$DATABASE_URL" \
  --db mydb \
  --data-dir /var/openduck \
  --dry-run

# Run for real
openduck gc \
  --postgres "$DATABASE_URL" \
  --db mydb \
  --data-dir /var/openduck
```

Output:

```
3 GC candidate layer(s) for database 'mydb':
  9d8a... segment-9d8a.bin
  e1f2... segment-e1f2.bin
  77c4... segment-77c4.bin
Deleted 3/3 layer(s)
```

A layer is a candidate when no snapshot row references any extent that lives in it. Compaction runs first to maximize the candidate set.

### Path validation

Before deleting, GC validates each layer's `storage_uri`:

- Absolute paths are rejected.
- Path components containing `..` are rejected.
- Anything that would resolve outside `data_dir` is logged and skipped.

The same validation lives in `diff-metadata::resolve_path` and is enforced for all storage operations, not just GC.

### Tiered layers (S3)

When sealed layers have been uploaded to object storage, GC removes both the local segment file (if present) and the metadata row. The blob store cleanup of the remote object is the operator's responsibility — set a lifecycle policy on the bucket, or extend the GC tool to call into `diff-blob` for deletes.

## Recommended cadence

The right schedule depends on workload, but a useful starting point:

| Schedule | Operation |
|----------|-----------|
| Continuously | Writes, reads. |
| Every N minutes / hours | `openduck snapshot seal` (driven by your data SLA, not a clock). |
| Daily | `openduck gc --dry-run` to a log; alert if the count is unexpectedly high. |
| Weekly | `openduck gc` to actually drop unreferenced layers. |
| As-needed | Snapshot reads via `?snapshot=` for analytics, repro, audit. |

Snapshots are cheap; GC is the lever for cost. Tune retention by deciding which snapshots you keep — anything not referenced by a kept snapshot becomes a GC candidate.

## Snapshot retention

OpenDuck doesn't ship a built-in retention policy — you decide which snapshots to keep. The pattern is to:

1. Tag snapshots externally (e.g. one snapshot per day labeled in your application).
2. Periodically delete snapshot rows you no longer want.
3. Run `openduck gc` to actually free the storage.

For multi-database deployments, run GC per database.

## Examples

- `examples/rust/snapshot_reads` — write → seal → diverge tip → read at snapshot vs tip.
- `examples/rust/diff_storage_demo` — in-memory snapshot algorithm walkthrough.
- `examples/rust/storage_backend` — Postgres-backed storage end-to-end.
- `examples/rust/gc_walkthrough` — narrative GC walkthrough on the in-memory backend.

## Where to next

- [Differential storage](differential-storage.md) — concepts and storage modes.
- [Deployment](deployment.md) — Postgres, S3, multi-worker, scheduling GC.
- [Configuration](../configuration.md) — every snapshot/GC flag and env var.
