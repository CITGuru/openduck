# Deployment

This guide covers the deployment topologies OpenDuck supports out of the box: single-process for development, multi-worker behind a gateway for production, Docker Compose for the full stack, and the bits you bolt on (DuckLake, S3 tiering, OpenTelemetry).

It assumes you've already worked through [Getting started](getting-started.md) and have a working `openduck` binary.

## Topologies

### 1. Single-process (default)

Gateway and worker in one process. Easiest to operate; fine for small teams, dev, demos, and any workload that fits one machine.

```bash
openduck -d /var/openduck/mydb -p $OPENDUCK_TOKEN
```

You get one gateway on `:7878`, one worker on `127.0.0.1:9898`, both bound to the same DuckDB file.

### 2. Gateway + N workers

For horizontal scale, run the gateway separately from one or more workers. Workers self-register with the gateway and the gateway picks one per query using affinity routing.

**Worker** (run multiple, one per host):

```bash
openduck worker \
  -l 0.0.0.0:9898 \
  -d /var/openduck/mydb \
  --gateway http://gateway.internal:7878 \
  -p $OPENDUCK_TOKEN
```

The `--gateway` flag makes the worker self-register on startup and send periodic heartbeats. Use this when workers know the gateway address.

**Gateway**:

```bash
openduck gateway \
  -l 0.0.0.0:7878 \
  --workers http://worker-1.internal:9898,http://worker-2.internal:9898
```

`--workers` (or `OPENDUCK_WORKER_ADDRS`) seeds the gateway with explicit endpoints. Self-registered workers add to this list dynamically.

For a multi-database deployment, declare each worker's databases / tables when registering — the planner uses that for affinity. See [Hybrid execution → affinity routing](hybrid-execution.md#worker-affinity-and-routing).

### 3. Differential storage worker

A worker using FUSE or in-process differential storage. Both modes need Postgres metadata and a data directory.

```bash
openduck worker \
  --storage in-process \
  -d mydb \
  --postgres "$DATABASE_URL" \
  --data-dir /var/openduck \
  --gateway http://gateway.internal:7878 \
  -p $OPENDUCK_TOKEN
```

For FUSE, run `openduck-fuse` first to mount the storage as a directory, then point the worker at the mount with `--storage fuse --mountpoint /mnt/od`. See [Differential storage](differential-storage.md).

## Docker Compose

Two compose files ship in `docker/`:

- [`docker/docker-compose.yml`](../../docker/docker-compose.yml) — Postgres + MinIO only. Use this when you're running `openduck` from the host.
- [`docker/docker-compose.full.yml`](../../docker/docker-compose.full.yml) — Postgres + MinIO + worker + gateway. Optional FUSE worker behind the `fuse` profile.

### Just the dependencies (Postgres + MinIO)

```bash
docker compose -f docker/docker-compose.yml up -d

export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
for f in crates/diff-metadata/migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done
```

Postgres is on host port `5433` (to avoid clashing with a local Postgres on `5432`). MinIO API on `9000`, console on `9001`.

### Full stack

```bash
docker compose -f docker/docker-compose.full.yml up
```

This brings up:

- Postgres on `5433`
- MinIO on `9000` (console `9001`)
- `openduck worker` on `9898`
- `openduck gateway` on `7878`

To include the FUSE worker (needs `--privileged` for `/dev/fuse`):

```bash
docker compose -f docker/docker-compose.full.yml --profile fuse up
```

### Connect to the stack

```python
import openduck
con = openduck.connect("mydb", token="demo", endpoint="http://localhost:7878")
con.sql("SELECT 1").show()
```

## Building images

The Dockerfile is multi-stage and ships three lightweight images:

```bash
# Builds with cargo + protoc + libfuse3 in a builder stage, then strips into slim images
docker build --target gateway -t openduck:gateway .
docker build --target worker  -t openduck:worker  .
docker build --target fuse    -t openduck:fuse    .
```

The runtime images are `debian:bookworm-slim` with just `ca-certificates` (and `fuse3` for the FUSE image). Each one defaults to its respective `openduck` subcommand as `ENTRYPOINT`.

## Authentication in production

- **Always** set `OPENDUCK_TOKEN`. Without it, the server runs in dev mode and accepts any token. The startup logs warn about this.
- Use a strong, random token. The server compares it with `subtle::ConstantTimeEq`; mismatches return `Unauthenticated`.
- Distribute the same token to clients (`OPENDUCK_TOKEN` env var or `?token=...` URI parameter) and to workers (so `RegisterWorker` / `Heartbeat` succeed).
- Rotate the token by rolling a new value through workers, then the gateway, then clients.

## TLS

OpenDuck uses tonic for gRPC. To run TLS, place the gateway behind a TLS-terminating load balancer (Envoy, nginx, GCP/AWS ALBs all work) and route `/openduck.v1.ExecutionService/*` to it. Workers normally live on a private network and don't need TLS termination.

If you need end-to-end TLS into the gateway directly, that requires a small change to `serve` in `crates/exec-gateway` — open an issue or PR; the tonic configuration is straightforward but isn't wired through the CLI today.

## DuckLake attach

Workers can attach a DuckLake catalog at startup. Configure via env vars or CLI flags:

```bash
openduck worker \
  -d analytics \
  --ducklake-metadata "postgres://lake:lake@db:5432/ducklake_meta" \
  --ducklake-data "s3://my-bucket/lake/" \
  --gateway http://gateway.internal:7878 \
  -p $OPENDUCK_TOKEN

# or
export OPENDUCK_DUCKLAKE_METADATA=postgres://lake:lake@db:5432/ducklake_meta
export OPENDUCK_DUCKLAKE_DATA=s3://my-bucket/lake/
```

When DuckLake is configured, `open_connection` auto-installs the DuckLake extension and runs `ATTACH 'ducklake:...'` against the worker's DuckDB instance. Clients query through the standard `openduck:` URI; the worker is the one that knows about DuckLake.

For S3:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL=https://s3.us-east-1.amazonaws.com    # optional
```

## Sealed-layer tiering to S3

When the worker has a blob store attached (via `PgStorageBackend::set_blob_store`), `seal()` automatically uploads the newly sealed layer to object storage and rewrites its `storage_uri`. Reads transparently fetch from S3 when the local file is gone.

This wiring lives in code today. To opt in without writing Rust, run the in-process or FUSE worker against `data_dir` and apply an S3 lifecycle policy on the bucket holding cold layers.

## Observability

`openduck-metrics` adds an OpenTelemetry OTLP exporter. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the worker exports:

- Query-latency histograms (per-RPC, with success/error labels).
- Layer seal duration histograms.
- Counters for in-flight executions, registrations, heartbeats.

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
openduck -d /var/openduck/mydb -p $OPENDUCK_TOKEN
```

Pipe the OTLP collector to your tracing/metrics backend of choice (Tempo, Grafana, Datadog, Honeycomb).

## Logging

`tracing-subscriber` reads `RUST_LOG`. The `-v` / `-vv` flags add directives on top:

```bash
RUST_LOG=info,exec_gateway=debug openduck -d mydb -p $OPENDUCK_TOKEN
openduck -vv -d mydb -p $OPENDUCK_TOKEN          # everything at trace
```

For machine-parseable logs, prefer `RUST_LOG_FORMAT=json` (when the binary is built with the json layer) or pipe through `jq` from a structured-format logger.

## Backpressure and capacity planning

| Knob | Default | Purpose |
|------|---------|---------|
| `OPENDUCK_MAX_IN_FLIGHT` (or `--max-in-flight`) | `64` | Per-gateway semaphore; bounds concurrent `ExecuteFragment` slots. |
| Worker `max_concurrency` (in `WorkerRegistration`) | `0` (unlimited) | Per-worker cap; the gateway routes to a worker that has free slots. |
| gRPC max message size | 64 MiB | Each Arrow IPC batch is bounded; larger results stream as multiple batches. |
| `ExecuteFragment` per-RPC timeout | 300 s | Per query. |
| `CancelExecution` per-RPC timeout | 10 s | |
| Connect timeout (gateway → worker) | 5 s | |

Capacity-plan the gateway by `concurrent_users * avg_in_flight_queries_per_user`. The worker side is bound by `max_concurrency` and the embedded DuckDB's parallelism settings.

## Backups

- **Postgres metadata** — back up like any production Postgres (e.g. `pg_dump`, WAL-G, managed snapshots).
- **Sealed layer files** — back up the contents of `data_dir`. They're immutable; a rolling sync to object storage suffices.
- **Active layer** — bound to a single writer; back it up by sealing first (`openduck snapshot seal`) and then including the freshly-sealed layer in your sync.
- **`.duckdb` files in direct mode** — back up the file with the worker stopped, or use `EXPORT DATABASE` while it's running.

## Upgrades

OpenDuck workers run an embedded DuckDB. Upgrading the worker upgrades DuckDB. The DuckDB storage format is forward-compatible within a major version; pin the worker DuckDB version explicitly in your deployment if you need cross-cluster consistency.

The gRPC protocol is versioned in the package name (`openduck.v1`). Breaking changes will bump the package; clients and servers can be rolled in either order within a version.

## Where to next

- [Configuration](../configuration.md) — every flag and env var.
- [Differential storage](differential-storage.md) — Postgres + S3 in detail.
- [Hybrid execution](hybrid-execution.md) — multi-worker affinity routing.
- [Troubleshooting](troubleshooting.md) — common deployment issues.
