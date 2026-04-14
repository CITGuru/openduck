# OpenDuck

An open-source implementation of the ideas pioneered by [MotherDuck](https://motherduck.com) — differential storage, hybrid (dual) execution, and transparent remote databases for DuckDB — available for anyone to run, extend, and build on.

MotherDuck showed that DuckDB can work beautifully in the cloud: `ATTACH 'md:mydb'`, and remote tables appear local. Queries split transparently across your laptop and the cloud. Storage is layered and snapshot-based. OpenDuck takes those architectural ideas — [differential storage](https://motherduck.com/blog/differential-storage-building-block-for-data-warehouse/), [dual execution](https://motherduck.com/videos/bringing-duckdb-to-the-cloud-dual-execution-explained/), the attach-based UX — and makes them open. Open protocol, open backend, open extension.

```python
import duckdb

con = duckdb.connect()
con.execute("LOAD 'openduck';")
con.execute("ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;")

con.sql("SELECT * FROM cloud.users").show()                    # remote, transparent
con.sql("SELECT * FROM local.t JOIN cloud.t2 ON ...").show()   # hybrid, one query

# direct connect using openduck python library

con = openduck.connect("od:mydb")
con = openduck.connect("openduck:myd")

# direct connect using duckb (TODO: needs duckdb to autoload openduck the same way motherduck works today)

con = duckdb.connect("od:mydb")
con = duckdb.connect("openduck:myd")
```

## What OpenDuck does

### Differential storage

Append-only layers with PostgreSQL metadata. DuckDB sees a normal file; OpenDuck persists data as immutable sealed layers addressable from object storage. Snapshots give you consistent reads. One serialized write path, many concurrent readers.

### Hybrid (dual) execution

A single query can run partly on your machine and partly on a remote worker. The gateway splits the plan, labels each operator `LOCAL` or `REMOTE`, and inserts bridge operators at the boundaries. Only intermediate results cross the wire.

```
[LOCAL]  HashJoin(l.id = r.id)
  [LOCAL]  Scan(products)          ← your laptop
  [LOCAL]  Bridge(R→L)
    [REMOTE] Scan(sales)           ← remote worker
```

### DuckDB-native catalog

The extension implements DuckDB's `StorageExtension` and `Catalog` interfaces. Remote tables are first-class catalog entries, they participate in JOINs, CTEs, and the optimizer like local tables.

### Open protocol

OpenDuck's protocol is intentionally minimal: two RPCs defined in [`execution.proto`](proto/openduck/v1/execution.proto). The first one to execute a query, and the other to stream results back as Arrow IPC batches.

Because the protocol is open and simple, you're not locked into a single backend. Any service that speaks gRPC and returns Arrow can serve as an OpenDuck-compatible backend. Run the included Rust gateway, replace it with your own implementation, or plug in an entirely different execution engine — the client and extension don't care what's on the other side.

## Architecture

```
┌─────────────────────────────────────────────┐
│  DuckDB process (client)                    │
│                                             │
│  LOAD openduck                              │
│  ATTACH 'openduck:mydb' AS cloud            │
│                                             │
│  ┌─────────────────────────────────────┐    │
│  │ OpenDuckCatalog                     │    │
│  │  └─ OpenDuckSchemaEntry             │    │
│  │      └─ OpenDuckTableEntry (users)  │    │
│  │      └─ OpenDuckTableEntry (events) │    │
│  └──────────────┬──────────────────────┘    │
│                 │ gRPC + Arrow IPC          │
└─────────────────┼───────────────────────────┘
                  │
      ┌───────────▼───────────┐
      │  Gateway (Rust)       │
      │  - auth, routing      │
      │  - plan splitting     │     ┌──────────────┐
      │  - backpressure       │────▶│  Worker 1    │
      │                       │     │  (DuckDB)    │
      │                       │     └──────────────┘
      │                       │     ┌──────────────┐
      │                       │────▶│  Worker N    │
      │                       │     │  (DuckDB)    │
      └───────────────────────┘     └──────────────┘
              │
    ┌─────────┴─────────┐
    ▼                   ▼
┌──────────┐    ┌──────────────┐
│ Postgres │    │ Object store │
│ metadata │    │ sealed layers│
└──────────┘    └──────────────┘
```

## Quick start

```bash
# Backend
cargo build --workspace
cargo run -p openduck -- serve -d mydb -t your-token

# Extension (requires vcpkg — see extensions/openduck/README.md)
cd extensions/openduck && make

# Python client
pip install -e clients/python
export OPENDUCK_TOKEN=your-token
python -c "
import openduck
con = openduck.connect('mydb')
con.sql('SELECT 1 AS x').show()
"
```

## Layout

```
crates/
  exec-gateway/     Gateway — auth, routing, hybrid plan splitting
  exec-worker/      Worker — embedded DuckDB, Arrow IPC streaming
  exec-proto/       Protobuf/tonic codegen
  openduck-cli/     Unified CLI (openduck serve|gateway|worker)
  diff-*/           Differential storage pipeline (layers, metadata, FUSE)

extensions/
  openduck/ DuckDB C++ extension (StorageExtension + Catalog)

clients/
  python/           openduck Python package (pip install -e clients/python)

proto/
  openduck/v1/      Protocol definition (execution.proto)
```

## OpenDuck vs MotherDuck

MotherDuck is a commercial cloud service. OpenDuck is an open-source project inspired by its architecture.


|                          | MotherDuck            | OpenDuck                                 |
| ------------------------ | --------------------- | ---------------------------------------- |
| **What**                 | Managed cloud service | Self-hosted open-source                  |
| **Attach scheme**        | `md:`                 | `openduck:` / `od:`                      |
| **Auth**                 | `motherduck_token`    | `OPENDUCK_TOKEN`                         |
| **Differential storage** | Proprietary           | Open (Postgres metadata + object store)  |
| **Hybrid execution**     | Proprietary planner   | Open (gateway + plan splitting)          |
| **Protocol**             | Private wire format   | Open gRPC + Arrow IPC                    |
| **Backend**              | MotherDuck's cloud    | Anything implementing `ExecutionService` |
| **Extension**            | Bundled in DuckDB     | Separate loadable extension              |


OpenDuck is not wire-compatible with MotherDuck. It reimplements the same architectural ideas as an open protocol.

## OpenDuck vs Arrow Flight SQL

Arrow Flight SQL is a generic database protocol — "JDBC/ODBC over Arrow." OpenDuck is a DuckDB-specific system with a narrower scope but deeper integration.


|                      | Arrow Flight SQL                | OpenDuck                                     |
| -------------------- | ------------------------------- | -------------------------------------------- |
| **Scope**            | Any SQL database                | DuckDB-specific                              |
| **Integration**      | Separate client driver          | DuckDB StorageExtension + Catalog            |
| **Catalog**          | Server-side (`GetTables`, etc.) | Extension-side (DuckDB catalog entries)      |
| **Execution**        | Full query on server            | Hybrid — split across local and remote       |
| **Protocol surface** | ~15 RPCs                        | 2 RPCs                                       |
| **Plan format**      | SQL only                        | SQL (M2), structured plan IR (M3)            |
| **Optimizer**        | Client-side, unaware            | DuckDB optimizer sees remote tables natively |


## Acknowledgments

OpenDuck's architecture draws heavily from MotherDuck's published work on [differential storage](https://motherduck.com/blog/differential-storage-building-block-for-data-warehouse/), [dual execution](https://motherduck.com/videos/bringing-duckdb-to-the-cloud-dual-execution-explained/), and [cloud-native DuckDB](https://motherduck.com/duckdb-book-summary-chapter7/). Credit to the MotherDuck team for pioneering these ideas.

## License

MIT