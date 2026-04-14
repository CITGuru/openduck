# OpenDuck Protocol v1

The wire contract for OpenDuck — intentionally minimal.

## `execution.proto`

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `ExecuteFragment` | `ExecuteFragmentRequest` | `stream ExecuteFragmentChunk` | Send SQL (or plan IR), receive Arrow IPC batches |
| `CancelExecution` | `CancelRequest` | `CancelReply` | Cancel a running execution |

**Why not Arrow Flight SQL?** Flight SQL defines ~15 RPCs for catalog introspection, prepared statements, transactions, etc. OpenDuck pushes catalog logic into the DuckDB extension's `Catalog`/`SchemaCatalogEntry` layer and keeps the wire contract to two RPCs. In M3, `ExecuteFragment` will carry structured plan IR with per-operator placement — partial execution trees, not complete SQL queries.

## Versioning

- Rust codegen: `tonic-build` / `prost` from `crates/exec-proto`
- C++ codegen: CMake protobuf generation in `extensions/openduck/CMakeLists.txt`
- Breaking changes bump the package (`openduck.v2`, etc.)

## Compatibility

This is the **OpenDuck** wire contract. It is not wire-compatible with MotherDuck or Arrow Flight SQL.
