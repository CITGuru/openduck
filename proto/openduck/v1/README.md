# OpenDuck Protocol v1

The wire contract for OpenDuck — intentionally minimal.

## `execution.proto`

### Data plane

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `ExecuteFragment` | `ExecuteFragmentRequest` | `stream ExecuteFragmentChunk` | Send SQL (or plan IR), receive Arrow IPC batches |
| `CancelExecution` | `CancelRequest` | `CancelReply` | Cancel a running execution by ID |

### Worker lifecycle

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `RegisterWorker` | `WorkerRegistration` | `RegisterWorkerReply` | Worker self-registers with the gateway on startup |
| `Heartbeat` | `HeartbeatRequest` | `HeartbeatReply` | Worker sends periodic keepalives to maintain registration |

### Key fields

| Message | Field | Purpose |
|---------|-------|---------|
| `ExecuteFragmentRequest` | `access_token` | Bearer-style auth token |
| `ExecuteFragmentRequest` | `execution_id` | Client-assigned ID for cancel routing |
| `ExecuteFragmentRequest` | `compute_context` | Routing hint (e.g. `region=us-east-1`) |
| `CancelRequest` | `access_token` | Required — cancel is authenticated |
| `WorkerRegistration` | `access_token` | Required — prevents rogue worker registration |
| `WorkerRegistration` | `databases`, `tables` | Affinity routing: gateway prefers workers that serve the requested database/tables |
| `WorkerRegistration` | `compute_context` | Workers with the same context are interchangeable |
| `WorkerRegistration` | `max_concurrency` | Capacity hint for load-aware routing |
| `HeartbeatRequest` | `access_token` | Required — heartbeats are authenticated |

**Why not Arrow Flight SQL?** Flight SQL defines ~15 RPCs for catalog introspection, prepared statements, transactions, etc. OpenDuck pushes catalog logic into the DuckDB extension's `Catalog`/`SchemaCatalogEntry` layer and keeps the wire contract small. The data plane is two RPCs (execute + cancel); the worker lifecycle adds two more (register + heartbeat).

## Authentication

All four RPCs require `access_token` validation when `OPENDUCK_TOKEN` is set:

- **Dev mode** (`OPENDUCK_TOKEN` unset): any token accepted
- **Production**: constant-time comparison via `subtle::ConstantTimeEq`; mismatches return gRPC `Unauthenticated`

Auth logic lives in `exec_proto::auth` and is shared by both gateway and worker.

## Message size limits

Gateway and worker servers set explicit `max_decoding_message_size` and `max_encoding_message_size` of **64 MiB** instead of relying on tonic defaults.

## Versioning

- Rust codegen: `tonic-build` / `prost` from `crates/exec-proto`
- C++ codegen: CMake protobuf generation in `extensions/openduck/CMakeLists.txt`
- Breaking changes bump the package (`openduck.v2`, etc.)

## Compatibility

This is the **OpenDuck** wire contract. It is not wire-compatible with MotherDuck or Arrow Flight SQL.
