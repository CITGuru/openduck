#pragma once

#include "duckdb/common/exception.hpp"
#include <string>

// Forward-declare the generated protobuf type so callers who only need
// the helpers don't have to pull in the gRPC/protobuf headers.
namespace openduck {
namespace v1 {
class ExecuteFragmentError;
} // namespace v1
} // namespace openduck

namespace openduck {

class OpenDuckCatalog;

// ═══════════════════════════════════════════════════════════════════════════
// Typed error mapping
// ═══════════════════════════════════════════════════════════════════════════
//
// Every gRPC call site — ExecuteSQL, IngestData, BeginTransaction,
// CommitTransaction, RollbackTransaction — funnels error responses
// through these helpers so the exception surfaced to DuckDB carries the
// correct type and message.

/// Throw a DuckDB-native exception that corresponds to the given typed
/// error's `Kind`. Never returns (the function is `[[noreturn]]`).
[[noreturn]] void ThrowMappedError(const ::openduck::v1::ExecuteFragmentError &err);

/// Classify an untyped, human-readable error string (as would appear
/// pre-PR-1 in `ExecuteFragmentChunk.error`) and throw the matching
/// DuckDB exception.
[[noreturn]] void ThrowMappedErrorFromMessage(const std::string &message);

// ═══════════════════════════════════════════════════════════════════════════
// Read-only guard
// ═══════════════════════════════════════════════════════════════════════════

/// Throw `PermissionException` when `catalog` was attached `READ_ONLY`.
/// Must be called as the first line of every mutation-capable catalog
/// hook so no hook can forget it.
void EnsureWritable(OpenDuckCatalog &catalog);

} // namespace openduck
