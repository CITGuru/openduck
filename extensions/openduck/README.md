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
The wire contract is two RPCs in a single `.proto` file. Any service that accepts SQL and streams Arrow IPC batches is a compatible backend. The extension is the universal DuckDB client.

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

| Scheme | Example |
|--------|---------|
| `openduck:` | `ATTACH 'openduck:mydb?token=xxx' AS cloud;` |
| `od:` | `ATTACH 'od:mydb?token=xxx' AS cloud;` |

## Building

### Prerequisites

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

### Build

```sh
make
```

Output:

```
./build/release/duckdb                                          # Shell with extension linked
./build/release/extension/openduck/openduck.duckdb_extension    # Loadable binary
```

### Test

```sh
make test
```

## Protocol

The extension communicates with backends using the OpenDuck Protocol defined in [`execution.proto`](../../proto/openduck/v1/execution.proto):

| RPC | Purpose |
|-----|---------|
| `ExecuteFragment` | Send SQL (M2) or plan IR (M3), stream back Arrow IPC batches |
| `CancelExecution` | Cancel a running execution by ID |

Any service implementing this interface is a compatible backend. See the [top-level README](../../README.md) for comparisons with Arrow Flight SQL and MotherDuck.
