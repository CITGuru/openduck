#pragma once

#include <deque>
#include <string>
#include <vector>

#include <arrow/record_batch.h>

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

/// Read ALL record batches from an Arrow IPC stream payload into a deque.
/// The worker may coalesce multiple batches per IPC message.
void ReadAllIpcBatches(const std::string &ipc_bytes,
                       std::deque<std::shared_ptr<arrow::RecordBatch>> &out);

/// Copy a single Arrow RecordBatch into a DuckDB DataChunk.
/// Returns the number of rows copied (0 if the batch is null/empty).
duckdb::idx_t CopyBatchToDataChunk(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    duckdb::DataChunk &chunk);

/// Copy with a column mapping: chunk_to_batch[i] gives the Arrow batch column
/// index for output chunk column i. A value of -1 means the column is a
/// virtual row_id and should be filled with zero.
duckdb::idx_t CopyBatchToDataChunkProjected(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    duckdb::DataChunk &chunk,
    const std::vector<int> &chunk_to_batch);

/// Convenience: deserialize an Arrow IPC stream payload and copy the first
/// record batch into the given DataChunk. Kept for backward compatibility.
duckdb::idx_t CopyIpcToDataChunk(const std::string &ipc_bytes,
                                 duckdb::DataChunk &chunk);

} // namespace openduck
