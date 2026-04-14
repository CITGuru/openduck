#pragma once

/// C ABI bridge to Rust PgStorageBackend (diff-bridge crate).
///
/// When the DuckDB extension sees a URI like:
///   openduck://mydb/database.duckdb?postgres=postgres://...&data_dir=/var/...
/// it calls these functions instead of using the C++ InMemoryStorage.
///
/// Link libdiff_bridge.a (from `cargo build -p diff-bridge --release`)
/// and define OPENDUCK_HAS_BRIDGE=1 to enable.

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

/// Opaque handle to a Rust PgStorageBackend.
typedef struct BridgeHandle BridgeHandle;

/// Open a Postgres-backed storage backend (read-write at the mutable tip).
/// Returns NULL on failure — call openduck_bridge_last_error() for details.
BridgeHandle *openduck_bridge_open(const char *db_name, const char *postgres_url, const char *data_dir);

/// Open a Postgres-backed storage backend in read-only mode at a specific snapshot.
/// Returns NULL on failure — call openduck_bridge_last_error() for details.
BridgeHandle *openduck_bridge_open_snapshot(const char *db_name, const char *postgres_url,
                                            const char *data_dir, const char *snapshot_id);

/// Read `len` bytes at `offset` into `buf`. Returns 0 on success, -1 on error.
int openduck_bridge_read(BridgeHandle *handle, uint64_t offset, uint8_t *buf, uint64_t len);

/// Write `len` bytes from `buf` at `offset`. Returns 0 on success, -1 on error.
int openduck_bridge_write(BridgeHandle *handle, uint64_t offset, const uint8_t *buf, uint64_t len);

/// Flush and sync. Returns 0 on success, -1 on error.
int openduck_bridge_fsync(BridgeHandle *handle);

/// Get the last error message (thread-local). Returns NULL if no error.
const char *openduck_bridge_last_error(void);

/// Seal the active layer into a snapshot. Returns UUID string (must be freed
/// with openduck_bridge_free_string). Returns NULL on error.
char *openduck_bridge_seal(BridgeHandle *handle);

/// Free a string returned by openduck_bridge_seal.
void openduck_bridge_free_string(char *ptr);

/// Close and free the handle. Safe to call with NULL.
void openduck_bridge_close(BridgeHandle *handle);

#ifdef __cplusplus
}
#endif
