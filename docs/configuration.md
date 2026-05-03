# Configuration

OpenDuck is configured through three layers, applied in this order of precedence (highest first):

1. **CLI flags** — explicit flags on `openduck` or its subcommands.
2. **Environment variables** — picked up automatically by both the server and the client.
3. **TOML config file** — supplied via `--config`, fills in unset CLI fields.

DuckDB's own [secret system](https://duckdb.org/docs/configuration/secrets_manager.html) is used for the in-process storage backend (extension side); see [Differential storage](guides/differential-storage.md).

## CLI overview

`openduck` is a single binary with subcommands:

```
openduck [OPTIONS]                       # default: gateway + worker in one process
openduck gateway [OPTIONS]                # gateway only (use with external workers)
openduck worker [OPTIONS]                 # worker only (register with a gateway)
openduck query <SQL> [OPTIONS]            # send a SQL query to a gateway
openduck cancel <EXEC_ID> [OPTIONS]       # cancel a running execution
openduck status [OPTIONS]                 # health-check a gateway
openduck snapshot seal|list [OPTIONS]     # snapshot operations
openduck gc [OPTIONS]                     # garbage-collect unreferenced layers
```

Run `openduck --help` for the full output, or `openduck <subcommand> --help`.

## Top-level flags (default mode)

These flags apply to the default `openduck` invocation that starts a gateway + worker in a single process.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-d`, `--db <NAME\|PATH>` | — | (in-memory) | Database name or DuckDB file path |
| `-p`, `--token <TOKEN>` | `OPENDUCK_TOKEN` | (dev mode) | Access token. Unset = dev mode (any token accepted) |
| `-l`, `--listen <ADDR>` | — | `0.0.0.0:7878` | Gateway listen address |
| `--worker-listen <ADDR>` | — | `127.0.0.1:9898` | Internal worker listen address |
| `--storage <MODE>` | — | `direct` | Storage mode: `direct`, `fuse`, `in-process` |
| `--mountpoint <PATH>` | — | — | FUSE mountpoint (required when `--storage fuse`) |
| `--postgres <URL>` | `DATABASE_URL` | — | Postgres URL for differential storage metadata |
| `--data-dir <PATH>` | — | — | Local directory holding sealed layers |
| `--ducklake-metadata <CONN>` | — | — | DuckLake metadata connection string |
| `--ducklake-data <PATH>` | — | — | DuckLake data path (e.g. `s3://bucket/prefix/`) |
| `--hybrid` | `OPENDUCK_HYBRID=1` | off | Enable hybrid execution (gateway splits LOCAL/REMOTE) |
| `--max-in-flight <N>` | `OPENDUCK_MAX_IN_FLIGHT` | `64` | Max concurrent in-flight executions per gateway |
| `-v`, `--verbose` | `RUST_LOG` | `info` | Repeat for more verbosity (`-v` = debug, `-vv` = trace) |
| `--config <PATH>` | — | — | TOML config file (CLI flags still take precedence) |

## Subcommand flags

### `openduck gateway`

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-l`, `--listen <ADDR>` | — | `0.0.0.0:7878` | Listen address |
| `--workers <ADDR,…>` | `OPENDUCK_WORKER_ADDRS` | `http://127.0.0.1:9898` | Comma-separated worker endpoints |

### `openduck worker`

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-l`, `--listen <ADDR>` | — | `127.0.0.1:9898` | Listen address |
| `-d`, `--db <PATH>` | — | (in-memory) | DuckDB file path |
| `--storage <MODE>` | — | `direct` | `direct`, `fuse`, or `in-process` |
| `--mountpoint <PATH>` | — | — | FUSE mountpoint (with `--storage fuse`) |
| `--postgres <URL>` | `DATABASE_URL` | — | Postgres URL (with `--storage in-process` / `fuse`) |
| `--data-dir <PATH>` | — | — | Local layer directory |
| `--ducklake-metadata <CONN>` | — | — | DuckLake metadata |
| `--ducklake-data <PATH>` | — | — | DuckLake data path |
| `--gateway <URL>` | — | — | Gateway endpoint to self-register with on startup |

### `openduck query`

```bash
openduck query "SELECT * FROM users LIMIT 10" \
  --endpoint http://127.0.0.1:7878 \
  --token $OPENDUCK_TOKEN \
  --format table   # or json | csv
```

### `openduck cancel`

```bash
openduck cancel <EXECUTION_ID> --endpoint http://127.0.0.1:7878 --token $OPENDUCK_TOKEN
```

### `openduck status`

```bash
openduck status --endpoint http://127.0.0.1:7878 --token $OPENDUCK_TOKEN
```

Prints `OK <endpoint> connect=<ms>ms query=<ms>ms` on success, exits non-zero otherwise.

### `openduck snapshot`

```bash
# Seal current data into an immutable snapshot, prints the snapshot UUID
openduck snapshot seal --postgres $DATABASE_URL --db mydb --data-dir /var/openduck

# List snapshots for a database
openduck snapshot list --postgres $DATABASE_URL --db mydb
```

### `openduck gc`

```bash
openduck gc --postgres $DATABASE_URL --db mydb --data-dir /var/openduck [--dry-run]
```

Compacts superseded extents, then deletes layers no longer referenced by any snapshot. With `--dry-run`, only candidates are listed.

## Environment variables

### Server-side (gateway / worker)

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENDUCK_TOKEN` | (dev mode) | Bearer token validated against every RPC. Unset = any token accepted. |
| `OPENDUCK_HYBRID` | `0` | Set to `1` to enable hybrid plan splitting in the gateway. |
| `OPENDUCK_MAX_IN_FLIGHT` | `64` | Max concurrent executions per gateway (semaphore-bounded). |
| `OPENDUCK_WORKER_ADDRS` | `http://127.0.0.1:9898` | Comma-separated worker endpoints (gateway only). |
| `DATABASE_URL` | — | Postgres URL for diff storage metadata. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | If set, metrics export to this OTLP endpoint. |

### Extension-side (DuckDB process)

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENDUCK_TOKEN` | — | Token for `ATTACH 'openduck:...'` when not in the URI. |
| `OPENDUCK_ENDPOINT` | `http://127.0.0.1:7878` | Gateway endpoint when not in the URI. |
| `OPENDUCK_EXTENSION_PATH` | auto-detect | Path to the built `.duckdb_extension` file (used by Python wrapper). |
| `OPENDUCK_POSTGRES_URL` | — | Fallback Postgres URL for in-process storage when no DuckDB secret is set. |
| `OPENDUCK_DATA_DIR` | — | Fallback data directory for in-process storage. |

### DuckLake (optional, server-side)

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENDUCK_DUCKLAKE_METADATA` | — | DuckLake metadata connection string. |
| `OPENDUCK_DUCKLAKE_DATA` | — | DuckLake data path (`s3://bucket/prefix/` or local path). |
| `AWS_ACCESS_KEY_ID` | — | S3 credential (for DuckLake on S3). |
| `AWS_SECRET_ACCESS_KEY` | — | S3 credential. |
| `AWS_ENDPOINT_URL` | — | S3 endpoint override (for MinIO). |

A complete reference template is checked in at [`examples/.env.example`](../examples/.env.example).

## TOML config file

Pass `--config /path/to/openduck.toml` to load defaults from a TOML file. CLI flags always take precedence; environment variables fill in fields the file omits.

```toml
# openduck.toml — every key is optional
token             = "my-secret-token"
listen            = "0.0.0.0:7878"
worker_listen     = "127.0.0.1:9898"
hybrid            = true
max_in_flight     = 128

postgres          = "postgres://openduck:openduck@db:5432/openduck_meta"
data_dir          = "/var/openduck"

ducklake_metadata = "postgres://lake:lake@db:5432/ducklake_meta"
ducklake_data     = "s3://my-bucket/lake/"
```

Fields recognised in the TOML file are exactly the ones listed in the [top-level flags](#top-level-flags-default-mode) table above (snake_case form of the long flag).

## URI format (`ATTACH`)

```
openduck:<database>?endpoint=<url>&token=<token>[&snapshot=<uuid>][&data_dir=<path>][&secret=<name>]
od:<database>?endpoint=<url>&token=<token>
```

| Form | Example |
|------|---------|
| Canonical | `ATTACH 'openduck:mydb?token=xxx' AS cloud;` |
| Short alias | `ATTACH 'od:mydb?token=xxx' AS cloud;` |
| Default DB | `ATTACH 'openduck:?token=xxx' AS cloud;` |
| Snapshot read | `ATTACH 'openduck://mydb/database.duckdb?snapshot=<uuid>' AS s;` |

### Token resolution (extension)

1. `?token=...` in the URI
2. `OPENDUCK_TOKEN` environment variable

Token validation uses constant-time comparison (`subtle::ConstantTimeEq`).

### Endpoint resolution (extension)

1. `?endpoint=...` in the URI
2. `OPENDUCK_ENDPOINT` environment variable
3. `http://127.0.0.1:7878`

## DuckDB secrets (in-process storage)

The extension registers a secret type called `openduck_storage`:

```sql
-- Default secret (no ?secret= needed)
CREATE SECRET openduck_storage (
    TYPE openduck_storage,
    postgres_url 'postgres://localhost/openduck',
    data_dir '/var/openduck'
);

-- Named secret (referenced via ?secret=)
CREATE SECRET prod_storage (
    TYPE openduck_storage,
    postgres_url 'postgres://prod-host/openduck',
    data_dir '/mnt/prod/openduck'
);

ATTACH 'openduck://mydb/database.duckdb?secret=prod_storage' AS prod_db;
```

Resolution order:

1. `?secret=NAME` in the URI
2. Default secret named `openduck_storage`
3. `OPENDUCK_POSTGRES_URL` + `OPENDUCK_DATA_DIR` env vars
4. In-memory storage (no persistence)

`?data_dir=PATH` always overrides whichever `data_dir` was resolved from a secret or env var. `postgres_url` values are redacted in `LIST SECRETS` output.

## Limits and timeouts

| Limit | Value | Notes |
|-------|-------|-------|
| Max in-flight per gateway | `64` (configurable) | Backpressure semaphore. |
| Max gRPC message size | 64 MiB | Encode and decode. Larger results stream as multiple Arrow IPC batches. |
| Connect timeout (gateway → worker) | 5 s | |
| Per-RPC `ExecuteFragment` timeout | 300 s | |
| Per-RPC `CancelExecution` timeout | 10 s | |
| Worker heartbeat interval | 30 s | Auto re-register on heartbeat failure. |

## Authentication

- **Dev mode** — `OPENDUCK_TOKEN` unset on the server: any token (including empty) is accepted. Logs a warning at startup.
- **Production** — `OPENDUCK_TOKEN` set: every RPC's `access_token` is compared against the server's value with constant-time comparison.

The same logic protects `ExecuteFragment`, `CancelExecution`, `RegisterWorker`, `Heartbeat`, `BeginTransaction`, `CommitTransaction`, `RollbackTransaction`, and `IngestData`.

## Logging

`tracing-subscriber` reads from `RUST_LOG`. The `-v` / `-vv` flags add directives on top:

```bash
# Debug for the gateway, info elsewhere
RUST_LOG=info,exec_gateway=debug openduck -d mydb -p $OPENDUCK_TOKEN

# Trace everything
openduck -vv -d mydb -p $OPENDUCK_TOKEN
```

## Metrics

When `openduck-metrics` is built into the binary and `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the worker exports query latency histograms and seal-duration metrics via OTLP. See [Deployment → Observability](guides/deployment.md#observability).

## Validation and safety

- `db_name` is checked for path separators and `..` before being joined into a file path (in-process mode).
- `storage_uri` values from Postgres are validated to reject absolute paths and `..` components before any file deletion.
- The Python client validates `alias` as a strict SQL identifier and single-quote-escapes `db_name`, `token`, `endpoint`, and `extension_path`.
- DuckLake `metadata` and `data_path` strings are single-quote-escaped before interpolation.
- `postgres_url` is redacted in `Debug` output and `LIST SECRETS`.

## Where to next

- [Getting started](guides/getting-started.md) for a full walk-through.
- [Differential storage](guides/differential-storage.md) for storage modes in depth.
- [Hybrid execution](guides/hybrid-execution.md) for `--hybrid` and `openduck_run`.
