#pragma once

#include <string>
#include <vector>

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace openduck {

struct ArrowSchemaInfo {
  std::vector<duckdb::LogicalType> types;
  std::vector<std::string> names;
};

/// Extract column names and DuckDB-mapped types from an Arrow IPC stream
/// payload. Only reads the schema; does not consume the record batch.
ArrowSchemaInfo ExtractSchema(const std::string &ipc_bytes);

/// Deserialize an Arrow IPC stream payload and copy the first record batch
/// into the given DataChunk. Returns the number of rows copied (0 means the
/// batch was empty or missing).
duckdb::idx_t CopyIpcToDataChunk(const std::string &ipc_bytes,
                                 duckdb::DataChunk &chunk);

} // namespace openduck
