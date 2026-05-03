# Getting started

This guide takes you from a freshly cloned repo to running your first remote query in under ten minutes.

You will:

1. Build the Rust backend.
2. Build the DuckDB extension.
3. Start the gateway + worker.
4. Run a query from Python and from the CLI.

## Prerequisites

- **Rust** — latest stable. Install via [rustup](https://rustup.rs).
- **DuckDB build dependencies** — `cmake`, `make`, a C++17 compiler, and `vcpkg`. On macOS you also need `bison` (`brew install bison`).
- **Python 3.9+** (only if you plan to use the Python client).
- **Postgres** is optional and only needed if you want differential storage (skip for now).

## 1. Clone and build the backend

```bash
git clone https://github.com/openduck/openduck
cd openduck

cargo build --workspace --release
```

This produces a single binary at `target/release/openduck` (the unified CLI: serve / gateway / worker / query / cancel / status / snapshot / gc).

## 2. Build the DuckDB extension

The extension is not in DuckDB's extension repository, so you build it from source.

```bash
cd extensions/openduck
make    # honours vcpkg + bison; takes a few minutes the first time
cd -
```

The build produces:

```
extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension
```

Export this for the Python wrapper to find it automatically:

```bash
export OPENDUCK_EXTENSION_PATH=$(pwd)/extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension
```

> **Heads up**: the extension is unsigned, so every DuckDB connection needs `allow_unsigned_extensions = true`. The Python wrapper sets this for you. For raw `duckdb` or the CLI, you have to opt in.

## 3. Start the service

```bash
export OPENDUCK_TOKEN=demo-token
./target/release/openduck -d mydb -p $OPENDUCK_TOKEN
```

You should see:

```
INFO openduck: starting openduck storage=Direct db="mydb" duckdb=v1.x.x worker=127.0.0.1:9898 gateway=0.0.0.0:7878 hybrid=false max_in_flight=64
```

That single binary started a worker (on `127.0.0.1:9898`) and a gateway (on `0.0.0.0:7878`) in the same process, with a local `mydb` DuckDB file as storage.

In another terminal, sanity-check that it's reachable:

```bash
./target/release/openduck status \
  --endpoint http://127.0.0.1:7878 \
  --token $OPENDUCK_TOKEN
# → OK  http://127.0.0.1:7878  connect=12ms  query=4ms
```

## 4. Run your first query

### From the CLI

```bash
./target/release/openduck query \
  "CREATE TABLE users (id INTEGER, name VARCHAR); \
   INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace'); \
   SELECT * FROM users;" \
  --endpoint http://127.0.0.1:7878 \
  --token $OPENDUCK_TOKEN \
  --format table
```

```
┌────┬───────┐
│ id │ name  │
├────┼───────┤
│  1 │ Ada   │
│  2 │ Grace │
└────┴───────┘
```

### From Python (recommended)

Install the wrapper:

```bash
pip install -e clients/python
export OPENDUCK_TOKEN=demo-token
```

Then:

```python
import openduck

con = openduck.connect("mydb")            # uses OPENDUCK_TOKEN, defaults endpoint
con.sql("SELECT * FROM users").show()
```

The wrapper:

1. Opens a local DuckDB connection with `allow_unsigned_extensions = true`.
2. Loads the extension (auto-detects `OPENDUCK_EXTENSION_PATH` or finds it under `extensions/openduck/build/`).
3. `ATTACH 'openduck:mydb?...'` as the `cloud` alias and sets it as the default schema.

Tables resolve transparently — `SELECT * FROM users` works even though `users` lives on the remote worker.

### From plain DuckDB (no wrapper)

```python
import duckdb

ext = "extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension"
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"LOAD '{ext}';")
con.execute("ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=demo-token' AS cloud;")

con.sql("SELECT * FROM cloud.users").show()
```

### From the DuckDB CLI

```bash
duckdb -unsigned -c "
  LOAD 'extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension';
  ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=demo-token' AS cloud;
  SELECT * FROM cloud.users;
"
```

## 5. Try a hybrid query (optional)

Restart the service with `--hybrid`:

```bash
OPENDUCK_TOKEN=demo-token ./target/release/openduck -d mydb -p demo-token --hybrid
```

Then in Python:

```python
import openduck

with openduck.connect("mydb") as con:
    con.raw.execute("CREATE TABLE local.products(id INT, label VARCHAR);")
    con.raw.execute("INSERT INTO local.products VALUES (1, 'Widget'), (2, 'Gadget');")

    con.raw.sql("""
        SELECT p.label, u.name
        FROM local.products p
        JOIN cloud.users u ON p.id = u.id
    """).show()
```

The local table lives in the client's in-memory DuckDB; `cloud.users` lives on the worker. The gateway pulls just the `users` rows back as Arrow IPC and the join runs locally.

For the explicit `openduck_run` form and how the planner splits the work, see the [Hybrid execution guide](hybrid-execution.md).

## What's running where?

```
┌──────────────────────────┐         ┌─────────────────────────┐
│  Your Python process     │         │  openduck (one process) │
│                          │         │                         │
│  duckdb (in-memory)      │         │  ┌───────────────────┐  │
│   ├ openduck extension   │  gRPC   │  │   Gateway         │  │
│   ├ ATTACH cloud         │ ──────▶ │  │   :7878           │  │
│   └ SELECT FROM cloud... │         │  └────────┬──────────┘  │
└──────────────────────────┘         │           │             │
                                     │  ┌────────▼──────────┐  │
                                     │  │   Worker          │  │
                                     │  │   :9898           │  │
                                     │  │   embedded duckdb │  │
                                     │  │   mydb (file)     │  │
                                     │  └───────────────────┘  │
                                     └─────────────────────────┘
```

In the simplest setup the worker and gateway live in the same `openduck` process. To scale out, run them separately — see [Deployment](deployment.md).

## Where to next

- [Python client](python-client.md) — full API of the `openduck` Python package.
- [DuckDB extension](duckdb-extension.md) — `ATTACH` URI format, secrets, `openduck_remote()` and `openduck_query()` table functions.
- [Differential storage](differential-storage.md) — switch from a plain `.duckdb` file to layered, snapshot-isolated storage.
- [Hybrid execution](hybrid-execution.md) — split single queries across local and remote.
- [Configuration](../configuration.md) — every flag, env var, and TOML key in one place.
