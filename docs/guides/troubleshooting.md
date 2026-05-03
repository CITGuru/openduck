# Troubleshooting

A grab-bag of the errors people hit most often, and the one-line fixes.

If you see something not on this list, run with `-vv` (or `RUST_LOG=trace`) on the server and copy the relevant lines into a GitHub issue.

## Connection / authentication

### `Unauthenticated` from the gateway

You sent the wrong token (or none) when the server has `OPENDUCK_TOKEN` set.

```
Status { code: Unauthenticated, message: "invalid access token" }
```

- Confirm the server's `OPENDUCK_TOKEN` matches what the client sends.
- For the extension: check `?token=...` in the URI or `OPENDUCK_TOKEN` in the client's environment.
- For workers self-registering: their `OPENDUCK_TOKEN` must match the gateway's.

The server uses constant-time comparison; trailing whitespace, quotes, and case all matter.

### `transport error` / `Connection refused`

The gateway isn't reachable.

- Is `openduck` actually running? `openduck status --endpoint http://127.0.0.1:7878 --token $OPENDUCK_TOKEN` will say so quickly.
- Did you bind to `127.0.0.1` and try to reach it from another host? Use `-l 0.0.0.0:7878`.
- In Docker, are the ports actually published (`-p 7878:7878`) and is the host firewall happy?

### `deadline exceeded`

A query took longer than 300 s, or a worker is unresponsive.

- For long queries, split them or run them off-line.
- For an unresponsive worker, it'll be removed from the rotation after the next heartbeat fails. The 5 s connect timeout means a missing worker doesn't block other queries.

### `Error: extension "openduck" not found` / unsigned extension errors

DuckDB is refusing to load the extension because it's unsigned and `allow_unsigned_extensions` isn't set.

```python
# Right
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})

# Wrong — default config rejects unsigned
con = duckdb.connect()
```

If using the CLI: pass `-unsigned`. If using Rust: `SET allow_unsigned_extensions = true;` in your `execute_batch`. The Python wrapper handles this for you.

### `LOAD 'openduck'` fails with "extension not found"

DuckDB can't find the `.duckdb_extension` binary on its search path. Either:

- Pass the full path: `LOAD '/abs/path/to/openduck.duckdb_extension';`
- Or set `OPENDUCK_EXTENSION_PATH` and use the [Python wrapper](python-client.md), which handles the lookup.

The Python wrapper auto-detects a build under `extensions/openduck/build/` if you ran `make` in that directory.

## Build issues

### `bison: command not found` (macOS)

DuckDB's build needs a recent bison. macOS ships an old one; install Homebrew bison and put it on PATH.

```bash
brew install bison
export PATH="$(brew --prefix bison)/bin:$PATH"
```

### `protoc: command not found`

Install Protocol Buffers + gRPC libraries.

```bash
# macOS
brew install protobuf grpc apache-arrow

# Debian / Ubuntu
apt-get install -y protobuf-compiler libprotobuf-dev libgrpc++-dev
```

### `vcpkg` errors when building the extension

Set `VCPKG_TOOLCHAIN_PATH` before invoking `make`:

```bash
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
cd extensions/openduck && make
```

## Storage and Postgres

### `relation "openduck_db" does not exist`

You connected to a Postgres database that hasn't had migrations applied.

```bash
export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
for f in crates/diff-metadata/migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done
```

### `Database 'mydb' not found` from `snapshot list` / `gc`

Either the database has never been written to (no `openduck_db` row yet) or you connected to the wrong Postgres.

- Run a write first: a worker in differential mode (FUSE or in-process) bootstraps the row on first write.
- Confirm `--postgres` matches what your workers use.

### Falling back to in-memory storage with a warning

The extension couldn't resolve a `postgres_url` for in-process mode. The cascade is:

1. `?secret=NAME` in the URI.
2. Default secret named `openduck_storage`.
3. `OPENDUCK_POSTGRES_URL` + `OPENDUCK_DATA_DIR` env vars.
4. Fall back to in-memory.

Either create the secret (`CREATE SECRET openduck_storage (TYPE openduck_storage, postgres_url '...', data_dir '...');`) or set the env vars before launching DuckDB.

You can also see this when the Rust `diff-bridge` library wasn't linked into the extension. To enable bridge-backed storage:

```bash
cargo build -p diff-bridge --release
EXT_RELEASE_FLAGS="-DOPENDUCK_BRIDGE_LIB=$(pwd)/target/release/libdiff_bridge.a" \
  make -C extensions/openduck release
```

### Snapshot writes are rejected

You attached with `?snapshot=<uuid>`. Snapshot handles are read-only by design. To write, attach without the parameter.

### `gc` skips a layer with "suspicious storage_uri"

The `storage_uri` for that layer is absolute or contains `..`. This shouldn't happen in normal use; investigate how the row got there. The path-traversal validation in `resolve_path` is what stops a bad row from deleting an unrelated file.

## Hybrid execution

### Hybrid query runs as plain SQL with no splitting

Hybrid is **off** by default. Enable it on the gateway:

```bash
openduck -d mydb -p $OPENDUCK_TOKEN --hybrid
# or
OPENDUCK_HYBRID=1 openduck -d mydb -p $OPENDUCK_TOKEN
```

If you're on a multi-process deployment, set this on the **gateway** process, not the workers.

### Result columns come back as `VARCHAR` in hybrid mode

The Arrow → DuckDB type mapping covers common types (INT, BIGINT, FLOAT, DOUBLE, VARCHAR, BOOLEAN, DATE, TIMESTAMP). Complex / nested types fall back to `VARCHAR`. If this matters for your workload, narrow the cast on the worker side or open an issue with the schema.

### "Single remote fragment" limit

The gateway handles one remote subquery per hybrid query. Deeply nested multi-remote trees need to be decomposed manually for now.

## Workers and routing

### Queries always hit the same worker

Either you have one worker, or the gateway is routing by affinity. Workers register `databases` and `tables`; if those overlap, the gateway prefers the worker that declared the matching database first. To get round-robin, leave `databases` empty when registering, or have multiple workers declare the same database — the gateway then picks among matching workers.

### Newly-started worker isn't getting traffic

- Confirm the worker's `--gateway` URL is reachable.
- Look for `INFO ... registered with gateway` in the worker log.
- If the worker logs `register_worker RPC failed, retrying...`, fix the gateway URL or token.
- Heartbeats every 30 s — if you stop a worker, it leaves the rotation after the next heartbeat misses.

### `RegisterWorker` keeps being rejected

The `access_token` in the registration didn't match the gateway's `OPENDUCK_TOKEN`. The worker uses `OPENDUCK_TOKEN` from its own environment for self-registration; both processes need the same value.

## Performance

### gRPC message size limit (64 MiB) hit

Result sets larger than 64 MiB stream as multiple Arrow IPC batches automatically — you don't normally see this error. If you do, it usually means a single Arrow batch is being constructed > 64 MiB; tune the worker's batch size or split the query.

### Backpressure errors / `unavailable` under load

The gateway's in-flight semaphore is full. Default cap is 64. Raise it:

```bash
openduck -d mydb -p $OPENDUCK_TOKEN --max-in-flight 256
# or env
OPENDUCK_MAX_IN_FLIGHT=256 openduck -d mydb -p $OPENDUCK_TOKEN
```

Watch `OPENDUCK_MAX_IN_FLIGHT` and the worker's `max_concurrency` together — there's no point making the gateway accept 1000 in-flight if every worker caps at 8.

### Long tail latency

Profile worker DuckDB first. If a remote query is slow, it's almost always slow on the worker too. Use `EXPLAIN ANALYZE` in a worker-local DuckDB session against the same data.

For hybrid queries, watch the size of remote intermediate results — anything that crosses the wire is on the critical path. If the join is selective, push more of it to the worker with an explicit `openduck_run('REMOTE', ...)`.

## Python wrapper

### `ValueError: invalid alias`

The `alias` you passed to `openduck.connect(..., alias=...)` failed the strict identifier check (alphanumeric + underscores only). Pick a valid SQL identifier.

### `RuntimeError: could not find openduck.duckdb_extension`

The wrapper couldn't find the extension binary. Either:

- Run `make` in `extensions/openduck/` so it's discoverable under the build tree.
- Set `OPENDUCK_EXTENSION_PATH=/abs/path/to/openduck.duckdb_extension`.
- Pass `extension_path=` explicitly to `connect()`.

### Local tables disappear between calls

`con.sql(...)` runs against the attached remote (the default schema). Local tables you created with `con.raw.execute(...)` live in the underlying DuckDB. Use `con.raw.sql(...)` (note: `raw`) to query them, or qualify with a schema (`SELECT * FROM main.local_table`).

## Diagnostics checklist

When in doubt, gather this and post it in an issue:

```bash
openduck --version
openduck status --endpoint http://127.0.0.1:7878 --token $OPENDUCK_TOKEN

# Server log with verbose tracing
RUST_LOG=trace openduck -d mydb -p $OPENDUCK_TOKEN -vv 2>&1 | head -200

# DuckDB version + extension path on the client
python -c "import duckdb; print(duckdb.__version__)"
echo $OPENDUCK_EXTENSION_PATH
ls -l "$OPENDUCK_EXTENSION_PATH"

# OS / build
uname -a
rustc --version
```

## Where to next

- [Configuration](../configuration.md) — settings cheat-sheet.
- [Architecture](../architecture.md) — what's actually doing what.
- File an issue: <https://github.com/openduck/openduck/issues>.
