# Changelog

## Unreleased

### Security

- **Auth on RegisterWorker / Heartbeat RPCs** — `RegisterWorker` and `Heartbeat`
were previously unauthenticated, allowing anyone who could reach the gateway
port to register a rogue worker and intercept queries and tokens. Both RPCs now
require `access_token` validation (new proto fields: `WorkerRegistration.access_token`,
`HeartbeatRequest.access_token`).
- **Arrow column name injection in hybrid DDL** — `materialize_batches` in the
hybrid planner interpolated Arrow schema field names directly into
`CREATE TEMP TABLE` DDL. A malicious worker could inject SQL via crafted column
names. Column names and table names are now double-quote-escaped via `quote_ident()`.
- **Postgres URL leaked in logs** — `StorageMode::InProcess` derived `Debug`,
which printed `postgres_url` (including credentials) at info-level on every
startup. Replaced with a custom `Debug` impl that redacts `postgres_url`.
- **Python client SQL injection** — `connect()` in the Python client interpolated
`alias`, `db_name`, `token`, `endpoint`, and `extension_path` into SQL without
escaping. `alias` is now validated as a strict identifier and double-quoted;
`db_name`, `token`, `endpoint`, and `extension_path` are single-quote-escaped.
Inspired by initial report from [@hobostay](https://github.com/hobostay)
([hobostay/openduck@f37ba17](https://github.com/hobostay/openduck/commit/f37ba17f779b4fb1927699eb7508624f769cc74e)).
- **DuckLake attach SQL injection** — `ducklake_metadata` and `ducklake_data_path`
were interpolated into an `ATTACH` statement via `format!()` without escaping.
Single quotes are now escaped (`'` → `''`) instead of rejecting the input, so
legitimate Postgres connection strings with special characters still work.
Inspired by initial report from [@hobostay](https://github.com/hobostay)
([hobostay/openduck@f37ba17](https://github.com/hobostay/openduck/commit/f37ba17f779b4fb1927699eb7508624f769cc74e)).
- **Path traversal via `storage_uri`** — `resolve_path` in `diff-metadata`
accepted absolute paths and `..` components from `storage_uri` values stored in
Postgres, allowing reads/deletes outside `data_dir`. Now rejects both. The GC
command in the CLI applies the same check before `remove_file`.
- **Path traversal via `db_name`** — `InProcess` storage mode joined `db_name`
into a file path without checking for path separators or `..`. Now rejected.
- **C++ cancel RPC missing token** — `GrpcClient::CancelExecution` did not set
`access_token`, so cancel requests were rejected when `OPENDUCK_TOKEN` was
enforced. Now requires and sends the token.

### Changed

- **Auth logic deduplicated** — `validate_token()` and `constant_time_eq()` were
copy-pasted between `exec-gateway` and `exec-worker`. Extracted into
`exec_proto::auth` module. Both crates now re-export or import from there.
Inspired by initial report from [@hobostay](https://github.com/hobostay)
([hobostay/openduck@f37ba17](https://github.com/hobostay/openduck/commit/f37ba17f779b4fb1927699eb7508624f769cc74e)).
- **Constant-time comparison hardened** — replaced hand-rolled `constant_time_eq`  
(vulnerable to compiler short-circuit optimizations) with the `subtle` crate's  
`ConstantTimeEq`, the Rust ecosystem standard used by `rustls`, `ring`, and  
`ed25519-dalek`. Inspired by initial report from [@hobostay](https://github.com/hobostay)  
([hobostay/openduck@f37ba17](https://github.com/hobostay/openduck/commit/f37ba17f779b4fb1927699eb7508624f769cc74e))
- **gRPC message size limits** — gateway and worker servers now set explicit
`max_decoding_message_size` and `max_encoding_message_size` (64 MiB) instead of
relying on tonic defaults.
- **Connect timeouts on worker clients** — gateway-to-worker connections now use
a 5-second connect timeout and per-RPC timeout (300s for execute, 10s for cancel)
instead of blocking indefinitely on unresponsive workers.

