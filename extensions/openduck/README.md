# OpenDuck Extension

A DuckDB storage extension that makes remote databases feel local.

`ATTACH 'openduck:mydb'` and `SELECT * FROM users` — the extension resolves table schemas, streams data, and handles serialization transparently through DuckDB's native catalog system. No special query syntax, no wrapper functions, no separate client driver.

## Why not Arrow Flight SQL?

Arrow Flight SQL is a generic "send SQL, get Arrow back" protocol — it replaces JDBC/ODBC with Arrow-native transport. OpenDuck is different in three ways:

**1. DuckDB-native catalog integration, not a client-server bridge.**
The extension implements DuckDB's `StorageExtension`, `Catalog`, `SchemaCatalogEntry`, and `TableCatalogEntry` interfaces. Remote tables are first-class catalog entries. DuckDB's optimizer, type system, and query planner see them exactly like local tables — they participate in JOINs, CTEs, and subqueries without any wrapping.

**2. Hybrid execution — split a single query across local and remote.**
The gateway includes a plan splitter that assigns `LOCAL` or `REMOTE` placement to each operator and inserts bridge operators at the boundaries. A query like `SELECT * FROM local.products JOIN cloud.sales ON ...` doesn't round-trip all data — only the intermediate results cross the wire.

```
[LOCAL]  HashJoin(l.id = r.id)
  [LOCAL]  Scan(products)
  [LOCAL]  Bridge(R→L)
    [REMOTE] Scan(sales)
```

**3. Open protocol — anyone can implement the backend.**
Eight RPCs in a single `.proto` file: three data plane (`ExecuteFragment`, `CancelExecution`, `IngestData`), three transactions (`BeginTransaction`, `CommitTransaction`, `RollbackTransaction`), and two worker lifecycle (`RegisterWorker`, `Heartbeat`). Any service that implements them — accepting SQL and streaming Arrow IPC batches back — is a compatible backend. The extension is the universal DuckDB client.

## Usage

```python
import duckdb

con = duckdb.connect()
con.execute("LOAD 'openduck';")
con.execute("ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;")

# Tables resolve transparently through the remote catalog
con.sql("SELECT * FROM cloud.users LIMIT 10").show()
con.sql("SELECT count(*) FROM cloud.events WHERE ts > '2025-01-01'").fetchone()

# Hybrid: local data + remote data in one query
con.execute("CREATE TABLE local_products (id INT, name VARCHAR);")
con.sql("""
    SELECT p.name, s.revenue
    FROM local_products p
    JOIN cloud.sales s ON p.id = s.product_id
""").show()
```

## How it works

The extension registers `openduck:` and `od:` as DuckDB storage schemes (inspired by MotherDuck's `md:` scheme). When you `ATTACH`, it creates a full catalog chain:

```
ATTACH 'openduck:mydb?token=...' AS cloud
  │
  └─ OpenDuckCatalog created (stores endpoint, token, database)

SELECT * FROM cloud.users
  │
  ├─ DuckDB resolves cloud.main.users
  │    └─ OpenDuckSchemaEntry.LookupEntry("users")
  │         → gRPC: SELECT * FROM users LIMIT 0
  │         → extracts column names + types from Arrow schema
  │         → caches an OpenDuckTableEntry
  │
  └─ DuckDB calls GetScanFunction() on the table entry
       └─ Returns a TableFunction that:
            → gRPC: SELECT * FROM users (full stream)
            → deserializes Arrow IPC → DuckDB DataChunks
            → feeds back into DuckDB's execution engine
```

The extension implements:
- `StorageExtension` — registers the `openduck:` and `od:` schemes
- `Catalog` — database-level catalog for the attached remote
- `SchemaCatalogEntry` — discovers tables on demand via gRPC probes
- `TableCatalogEntry` — defines scan functions that stream from the remote
- `TransactionManager` — minimal implementation (transactions are remote-side)

This is the same integration pattern MotherDuck uses — remote tables are first-class catalog entries visible to DuckDB's optimizer, type system, and query planner.

## URI format

```
openduck:<database>?endpoint=<url>&token=<token>
od:<database>?endpoint=<url>&token=<token>
```

| Parameter | Resolution order |
|-----------|-----------------|
| `endpoint` | `?endpoint=` → `OPENDUCK_ENDPOINT` env → `http://127.0.0.1:7878` |
| `token` | `?token=` → `OPENDUCK_TOKEN` env |
| `database` | path after scheme → `"default"` |

## Registered functions

| Function | Arguments | Description |
|----------|-----------|-------------|
| `openduck_remote` | `(endpoint, token, sql)` | Direct remote execution (no ATTACH needed) |
| `openduck_query` | `(alias, sql)` | Execute using a stored ATTACH config |

## Registered storage schemes

| Scheme | Example | What it does |
|--------|---------|--------------|
| `openduck:` | `ATTACH 'openduck:mydb?endpoint=...&token=xxx' AS cloud;` | Gateway/worker remote attach. Stores in plain DuckDB files on the worker; **no differential storage in v0.1** (planned for v0.2 — see [`docs/internal/DIFFERENTIAL_STORAGE_E2E.md`](../../docs/internal/DIFFERENTIAL_STORAGE_E2E.md)). |
| `od:` | `ATTACH 'od:mydb?endpoint=...&token=xxx' AS cloud;` | Alias for `openduck:`. Same gateway/worker semantics. |
| `openduck://` | `ATTACH 'openduck://mydb/database.duckdb?secret=prod_storage' AS db;` | In-process FileSystem. Routes DuckDB I/O through differential storage when the extension is built with `OPENDUCK_BRIDGE_LIB`. Supports `?snapshot=<uuid>` for read-only point-in-time attaches. Falls back to in-memory storage (with a warning) when the bridge is not linked. |

> **Differential storage paths (v0.1):** the `openduck://` in-process scheme is the only path that touches `StorageBackend`. The gateway/worker `openduck:` path stores in plain DuckDB files; snapshot reads (`?snapshot=`), seal-on-commit, and `openduck_current_snapshot()` over the gateway path are tracked in [`docs/internal/DIFFERENTIAL_STORAGE_E2E.md`](../../docs/internal/DIFFERENTIAL_STORAGE_E2E.md) and ship in v0.2.

## Install

### Pre-built binaries (recommended)

Tagged releases ship pre-built `openduck.duckdb_extension` binaries for four
platforms — no toolchain install required. Download the asset for your
platform, verify the checksum, and `LOAD` it.

| Platform                | Asset                                                |
| ----------------------- | ---------------------------------------------------- |
| macOS (Apple Silicon)   | `openduck-<tag>-osx_arm64.duckdb_extension`          |
| macOS (Intel)           | `openduck-<tag>-osx_amd64.duckdb_extension`          |
| Linux x86_64            | `openduck-<tag>-linux_amd64.duckdb_extension`        |
| Linux arm64             | `openduck-<tag>-linux_arm64.duckdb_extension`        |

```sh
TAG=v0.1.0   # latest release tag
PLATFORM=osx_arm64   # one of: osx_arm64, osx_amd64, linux_amd64, linux_arm64
BASE="https://github.com/CITGuru/openduck/releases/download/${TAG}"

curl -L -o openduck.duckdb_extension \
  "${BASE}/openduck-${TAG}-${PLATFORM}.duckdb_extension"
curl -L -o SHA256SUMS.txt "${BASE}/openduck-${TAG}-SHA256SUMS.txt"

# Verify
grep "openduck-${TAG}-${PLATFORM}.duckdb_extension" SHA256SUMS.txt \
  | sed "s|openduck-${TAG}-${PLATFORM}.duckdb_extension|openduck.duckdb_extension|" \
  | sha256sum -c -
```

Then load it from any DuckDB client (the extension is unsigned, so the
`-unsigned` flag / `allow_unsigned_extensions` setting is required):

```sh
duckdb -unsigned -c "
  LOAD '$(pwd)/openduck.duckdb_extension';
  ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=...' AS cloud;
  SELECT * FROM cloud.users LIMIT 10;
"
```

> **Note:** Once the extension is published to DuckDB's community-extensions
> repository, `INSTALL openduck FROM community; LOAD openduck;` will work
> without any download or `-unsigned` flag.

### Build from source (fallback)

Use this path when there is no release for your platform or you need to test
local changes.

#### Prerequisites

- CMake 3.5+, C++17 compiler
- gRPC, Protobuf, and Apache Arrow C++ libraries

**macOS (Homebrew):**

```sh
brew install protobuf grpc apache-arrow
```

**Other platforms (vcpkg):**

```sh
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

#### Build

```sh
make
```

Output:

```
./build/release/duckdb                                          # Shell with extension linked
./build/release/extension/openduck/openduck.duckdb_extension    # Loadable binary
```

#### Test

```sh
make test
```

## Protocol

The extension communicates with backends using the OpenDuck Protocol defined in [`execution.proto`](../../proto/openduck/v1/execution.proto). One service, eight RPCs:

### Data plane

| RPC | Purpose |
|-----|---------|
| `ExecuteFragment` | Send SQL (or plan IR), stream back Arrow IPC batches |
| `CancelExecution` | Cancel a running execution by ID (requires `access_token`) |
| `IngestData` | Client-streaming: push Arrow IPC batches into a worker-side `TEMP TABLE` for cross-catalog writes |

### Transactions

| RPC | Purpose |
|-----|---------|
| `BeginTransaction` | Open a transaction; returns an opaque `transaction_id` pinned to a worker connection |
| `CommitTransaction` | Commit the pinned transaction |
| `RollbackTransaction` | Roll back the pinned transaction |

### Worker lifecycle (gateway-side)

| RPC | Purpose |
|-----|---------|
| `RegisterWorker` | Worker self-registers with database affinity and capabilities |
| `Heartbeat` | Worker sends periodic keepalives to maintain registration |

All RPCs validate `access_token` when `OPENDUCK_TOKEN` is set. The extension sends the token on every call.

Any service implementing these RPCs is a compatible backend. See the [top-level README](../../README.md) for comparisons with Arrow Flight SQL and MotherDuck.
