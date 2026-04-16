# openduck CLI

Unified command-line interface for running and interacting with OpenDuck services.

## Installation

```bash
cargo install --path crates/openduck-cli
```

## Quick start

```bash
# Start gateway + worker in a single process (default mode)
openduck

# With a database file and verbose logging
openduck -d mydb.duckdb -v

# With hybrid execution enabled
openduck --hybrid --max-in-flight 128
```

## Subcommands

### `openduck` (default)

Starts a worker and gateway in a single process. Useful for local development.

```bash
openduck [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --db <PATH>` | in-memory | Database file path |
| `-p, --token <TOKEN>` | `$OPENDUCK_TOKEN` | Access token |
| `-l, --listen <ADDR>` | `0.0.0.0:7878` | Gateway listen address |
| `--worker-listen <ADDR>` | `127.0.0.1:9898` | Worker listen address |
| `--storage <MODE>` | `direct` | Storage mode: `direct`, `fuse`, `in-process` |
| `--mountpoint <PATH>` | | FUSE mountpoint (required for `--storage fuse`) |
| `--postgres <URL>` | `$DATABASE_URL` | Postgres URL for differential storage |
| `--data-dir <PATH>` | | Data directory for storage layers |
| `--ducklake-metadata <URL>` | | DuckLake metadata connection string |
| `--ducklake-data <PATH>` | | DuckLake data path (e.g. `s3://bucket/prefix/`) |
| `--hybrid` | off | Enable hybrid LOCAL/REMOTE execution |
| `--max-in-flight <N>` | `64` | Max concurrent gateway executions |
| `-v, --verbose` | info | `-v` = debug, `-vv` = trace |
| `--config <PATH>` | | Path to TOML config file |

### `openduck gateway`

Starts only the gateway, connecting to external workers.

```bash
openduck gateway -l 0.0.0.0:7878 --workers http://w1:9898,http://w2:9898
```

| Flag | Default | Description |
|------|---------|-------------|
| `-l, --listen <ADDR>` | `0.0.0.0:7878` | Listen address |
| `--workers <ADDRS>` | `$OPENDUCK_WORKER_ADDRS` | Comma-separated worker endpoints |

### `openduck worker`

Starts only a worker. Optionally self-registers with a gateway.

```bash
# Standalone worker
openduck worker -l 0.0.0.0:9898 -d mydb.duckdb

# Worker that auto-registers with a gateway
openduck worker -l 0.0.0.0:9898 --gateway http://gateway:7878
```

| Flag | Default | Description |
|------|---------|-------------|
| `-l, --listen <ADDR>` | `127.0.0.1:9898` | Listen address |
| `-d, --db <PATH>` | in-memory | Database file path |
| `--storage <MODE>` | `direct` | Storage mode |
| `--gateway <URL>` | | Gateway to self-register with on startup |

When `--gateway` is provided, the worker sends a `RegisterWorker` RPC to the gateway after binding its port. It retries with exponential backoff (up to 20 attempts) if the gateway is not yet available.

### `openduck query`

Sends a SQL query to a running gateway and prints results.

```bash
# Table output (default)
openduck query "SELECT * FROM my_table LIMIT 10"

# JSON output
openduck query "SELECT * FROM my_table" --format json

# CSV output to a different endpoint
openduck query "SELECT count(*) FROM orders" --endpoint http://prod:7878 --format csv
```

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint <URL>` | `http://127.0.0.1:7878` | Gateway endpoint |
| `--token <TOKEN>` | `$OPENDUCK_TOKEN` | Access token |
| `--format <FMT>` | `table` | Output: `table`, `json`, `csv` |

### `openduck cancel`

Cancels a running execution by ID.

```bash
openduck cancel <EXECUTION_ID> --endpoint http://127.0.0.1:7878
```

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint <URL>` | `http://127.0.0.1:7878` | Gateway endpoint |
| `--token <TOKEN>` | `$OPENDUCK_TOKEN` | Access token |

### `openduck status`

Health check: verifies gateway connectivity by running `SELECT 1`.

```bash
$ openduck status
OK  http://127.0.0.1:7878  connect=2ms  query=15ms

$ openduck status --endpoint http://unreachable:7878
UNREACHABLE  http://unreachable:7878  (...)
```

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint <URL>` | `http://127.0.0.1:7878` | Gateway endpoint |
| `--token <TOKEN>` | `$OPENDUCK_TOKEN` | Access token |

### `openduck snapshot`

Differential storage snapshot management. Requires a running Postgres instance with OpenDuck migrations applied.

#### `openduck snapshot seal`

Seals the current mutable tip into an immutable snapshot and prints the new snapshot UUID.

```bash
$ openduck snapshot seal --postgres postgres://user:pass@localhost/meta --db mydb --data-dir /var/openduck/data
a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

#### `openduck snapshot list`

Lists all snapshots for a database, newest first.

```bash
$ openduck snapshot list --postgres postgres://user:pass@localhost/meta --db mydb
SNAPSHOT ID                             CREATED AT
a1b2c3d4-e5f6-7890-abcd-ef1234567890  2026-03-30 14:22:01 UTC
f0e1d2c3-b4a5-6789-0123-456789abcdef  2026-03-29 09:15:33 UTC
```

## Configuration file

Use `--config <path>` to load defaults from a TOML file. CLI flags always take precedence over file values.

```toml
# openduck.toml
token = "my-secret-token"
hybrid = true
max_in_flight = 128
postgres = "postgres://user:pass@localhost/openduck_meta"
data_dir = "/var/openduck/data"
```

Supported keys: `token`, `listen`, `worker_listen`, `hybrid`, `max_in_flight`, `postgres`, `data_dir`, `ducklake_metadata`, `ducklake_data`.

## Environment variables

| Variable | Description |
|----------|-------------|
| `OPENDUCK_TOKEN` | Access token (overridden by `--token`) |
| `OPENDUCK_WORKER_ADDRS` | Comma-separated worker addresses for `gateway` subcommand |
| `OPENDUCK_HYBRID` | Set to `1` to enable hybrid execution (overridden by `--hybrid`) |
| `OPENDUCK_MAX_IN_FLIGHT` | Max concurrent executions (overridden by `--max-in-flight`) |
| `DATABASE_URL` | Postgres URL for differential storage (overridden by `--postgres`) |
| `RUST_LOG` | Standard tracing filter (overridden by `-v`/`-vv`) |

### `openduck gc`

Garbage-collects unreferenced layers and compacts extents. Requires a running Postgres instance with OpenDuck migrations applied.

```bash
# Dry run — list candidates without deleting
openduck gc --postgres postgres://user:pass@localhost/meta --db mydb --data-dir /var/openduck/data --dry-run

# Actually delete orphaned layers and compact extents
openduck gc --postgres postgres://user:pass@localhost/meta --db mydb --data-dir /var/openduck/data
```

| Flag | Default | Description |
|------|---------|-------------|
| `--postgres <URL>` | `$DATABASE_URL` | Postgres URL for metadata |
| `--db <NAME>` | | Database name |
| `--data-dir <PATH>` | | Data directory for storage layers (needed to delete segment files) |
| `--dry-run` | off | List candidates without deleting |

The GC command:

1. Compacts extents (merges adjacent extents in the same layer)
2. Identifies sealed layers with no snapshot references
3. Validates `storage_uri` paths (rejects absolute paths and `..` components)
4. Deletes orphaned layer files from disk and metadata from Postgres

## Graceful shutdown

In default mode (gateway + worker), pressing Ctrl-C sends a shutdown signal to both services. In-flight requests are given up to 5 seconds to drain before the process exits.

## Architecture

```
openduck (default mode)
├── Worker gRPC server  (127.0.0.1:9898)
└── Gateway gRPC server (0.0.0.0:7878)
    ├── Routes ExecuteFragment to workers
    ├── Forwards CancelExecution to workers
    └── Accepts RegisterWorker from workers

openduck query / cancel / status
└── gRPC client → Gateway
```
