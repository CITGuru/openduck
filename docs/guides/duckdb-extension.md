# DuckDB extension

The OpenDuck DuckDB extension makes a remote OpenDuck-compatible service look like a local DuckDB database.

```sql
LOAD 'openduck';
ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;
SELECT * FROM cloud.users LIMIT 10;
```

No special syntax, no wrapper functions, no driver glue. The extension implements DuckDB's `StorageExtension` and `Catalog` interfaces, so remote tables participate in joins, CTEs, and the optimizer just like local tables.

## What the extension provides

| Surface | Purpose |
|---------|---------|
| Storage scheme `openduck:` and `od:` | `ATTACH 'openduck:...' AS cloud` to pull a remote database into the catalog. |
| `OpenDuckCatalog` / `OpenDuckSchemaEntry` / `OpenDuckTableEntry` | Native DuckDB catalog chain for the attached remote. |
| `openduck_remote(endpoint, token, sql)` | Direct remote execution without `ATTACH`. |
| `openduck_query(alias, sql)` | Execute against an already-attached database. |
| `OpenDuckFileSystem` | Intercepts `openduck://` paths for in-process differential storage. |
| `openduck_storage` secret type | Configures the in-process storage backend (Postgres URL + data dir). |
| `BridgeStorage` (when built with the C ABI bridge) | C++ ↔ Rust handoff into `PgStorageBackend`. |

## Loading the extension

Because the extension is not in DuckDB's extension repository, every connection needs `allow_unsigned_extensions = true` and an explicit `LOAD` with the full path to the built binary.

### Python

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD '/path/to/openduck.duckdb_extension';")
```

If the path is annoying, set `OPENDUCK_EXTENSION_PATH` and use the [Python wrapper](python-client.md), which auto-detects it.

### CLI

```bash
duckdb -unsigned -c "LOAD '/path/to/openduck.duckdb_extension'; ..."
```

### Rust

```rust
use duckdb::Connection;

let conn = Connection::open_in_memory()?;
conn.execute_batch(r"
    SET allow_unsigned_extensions = true;
    LOAD '/path/to/openduck.duckdb_extension';
")?;
```

## ATTACH and the URI format

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

### Parameter resolution

| Parameter | Resolution order |
|-----------|------------------|
| `endpoint` | `?endpoint=` → `OPENDUCK_ENDPOINT` env → `http://127.0.0.1:7878` |
| `token` | `?token=` → `OPENDUCK_TOKEN` env |
| `database` | path after the scheme → `"default"` |
| `snapshot` | `?snapshot=<uuid>` → tip (read/write) |
| `data_dir` | `?data_dir=<path>` → secret → `OPENDUCK_DATA_DIR` env |
| `secret` | `?secret=<name>` → default `openduck_storage` secret → env vars → in-memory |

### What `ATTACH` does

```
ATTACH 'openduck:mydb?token=...' AS cloud
  └─ OpenDuckCatalog created (stores endpoint, token, database)

SELECT * FROM cloud.users
  ├─ DuckDB resolves cloud.main.users
  │    └─ OpenDuckSchemaEntry.LookupEntry("users")
  │         → gRPC: SELECT * FROM users LIMIT 0   (probe schema)
  │         → extracts column names + types from the Arrow schema
  │         → caches an OpenDuckTableEntry
  │
  └─ DuckDB calls GetScanFunction() on the table entry
       → gRPC: SELECT * FROM users                 (full stream)
       → deserializes Arrow IPC → DuckDB DataChunks
       → feeds back into DuckDB's execution engine
```

Schemas are probed lazily and cached. Each new column reference in a query goes through the same path; subsequent queries reuse the cached `OpenDuckTableEntry`.

## Table functions (no ATTACH)

Sometimes you want a one-shot query without `ATTACH`. Two table functions are registered:

### `openduck_remote(endpoint, token, sql)`

```sql
SELECT * FROM openduck_remote(
    'http://localhost:7878',
    'my-token',
    'SELECT * FROM users LIMIT 10'
);
```

Pure pass-through — endpoint, token, and SQL all explicit, nothing cached.

### `openduck_query(alias, sql)`

```sql
ATTACH 'openduck:mydb?token=xxx' AS cloud;
SELECT * FROM openduck_query('cloud', 'SELECT * FROM users LIMIT 10');
```

Uses the configuration of an already-`ATTACH`ed database — handy for SQL that you'd rather not rewrite as a table reference.

## Hybrid queries

When the gateway runs with `--hybrid`, you can mix local and remote work in one statement:

```sql
ATTACH 'openduck:mydb?token=xxx' AS cloud;
CREATE TABLE local.products(id INT, label VARCHAR);
INSERT INTO local.products VALUES (1, 'Widget'), (2, 'Gadget');

SELECT p.label, u.name
FROM local.products p
JOIN cloud.users u ON p.id = u.id;
```

For explicit `openduck_run('LOCAL'|'REMOTE'|'AUTO', '...')` placement, see the [Hybrid execution guide](hybrid-execution.md).

## In-process differential storage

The extension can also act as the storage layer itself, using `openduck://` as a DuckDB FileSystem path. This is the **In-Process** storage mode — no FUSE, no kernel round-trips, works on macOS / Linux / Windows.

```sql
CREATE SECRET openduck_storage (
    TYPE openduck_storage,
    postgres_url 'postgres://localhost/openduck',
    data_dir '/var/openduck'
);

ATTACH 'openduck://mydb/database.duckdb' AS local_db;
SELECT * FROM local_db.users;
```

See [Differential storage](differential-storage.md) for the full setup, secret resolution cascade, snapshot reads (`?snapshot=<uuid>`), and tradeoffs.

## Building the extension

### Prerequisites

- CMake 3.5+, a C++17 compiler.
- gRPC, Protobuf, Apache Arrow C++.
- vcpkg (recommended) or Homebrew on macOS.

```bash
# macOS
brew install protobuf grpc apache-arrow bison

# Other platforms — vcpkg
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build

```bash
cd extensions/openduck
make
```

Output:

```
build/release/duckdb                                          # Shell with extension linked
build/release/extension/openduck/openduck.duckdb_extension    # Loadable binary
```

### Build with the bridge (in-process storage)

To enable `BridgeStorage` (the C++ shim that calls into the Rust `PgStorageBackend`), build the bridge static library first and pass it to the extension build:

```bash
cargo build -p diff-bridge --release

EXT_RELEASE_FLAGS="-DOPENDUCK_BRIDGE_LIB=$(pwd)/target/release/libdiff_bridge.a" \
  make -C extensions/openduck release
```

Without the bridge, in-process storage falls back to in-memory storage with a warning.

### Test

```bash
cd extensions/openduck && make test
```

The extension's SQLLogicTests cover `ATTACH`, `LOAD`, secret creation/resolution, and the table functions.

## Errors

When something goes wrong, the extension surfaces typed DuckDB exceptions based on the gRPC `ExecuteFragmentError.kind`:

| Kind | DuckDB exception |
|------|------------------|
| `CATALOG` | `CatalogException` (table/schema/column not found, unknown txn) |
| `BINDER` | `BinderException` |
| `CONSTRAINT` | `ConstraintException` |
| `CONVERSION` | `ConversionException` |
| `PARSER` | `ParserException` |
| `IO` | `IOException` |
| `PERMISSION` | `PermissionException` |
| `INTERNAL` | `InternalException` |

This means downstream tooling (DBeaver, sqlc, ORM error handlers) gets the right exception class.

## Where to next

- [Differential storage](differential-storage.md) — `openduck://`, FUSE, secrets, snapshots.
- [Hybrid execution](hybrid-execution.md) — `openduck_run`, plan splitting.
- [Configuration](../configuration.md) — every URI parameter and env var.
