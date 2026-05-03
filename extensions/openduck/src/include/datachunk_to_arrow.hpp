#pragma once

#include <memory>
#include <string>
#include <vector>

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_properties.hpp"

#include <arrow/record_batch.h>

namespace duckdb {
class ClientContext;
} // namespace duckdb

namespace openduck {

// ═══════════════════════════════════════════════════════════════════════════
// DataChunkToArrow — encode a DuckDB DataChunk as an Arrow IPC stream
// ═══════════════════════════════════════════════════════════════════════════
//
// Used by `PhysicalOpenDuckIngestAndMutate` to ship local DataChunks to
// a remote worker's `IngestData` RPC. Every Encode() call returns a
// self-contained Arrow IPC stream payload (schema + one RecordBatch +
// EOS marker) so the worker's `StreamReader` can decode each IngestChunk
// independently.
//
// Type conversion is delegated to DuckDB's `ArrowConverter` (public
// `DUCKDB_API`) which we bridge to Arrow C++'s `ImportRecordBatch`. All
// built-in DuckDB LogicalTypes round-trip; user-defined types would need
// `ArrowTypeExtensionData` wiring which we punt until a concrete user
// surfaces.
class DataChunkToArrow {
public:
	DataChunkToArrow(duckdb::ClientContext &context,
	                 duckdb::vector<duckdb::LogicalType> types,
	                 duckdb::vector<std::string> names);
	~DataChunkToArrow();

	/// Encode `chunk` as a complete Arrow IPC stream payload. Empty
	/// chunks (num_rows == 0) return an empty string; the caller
	/// should skip sending those to the worker (the worker ignores
	/// them but they waste bandwidth).
	std::string Encode(duckdb::DataChunk &chunk);

	/// The Arrow schema derived from the DuckDB types. Exposed for
	/// tests and debug logging.
	const std::shared_ptr<arrow::Schema> &GetArrowSchema() const {
		return arrow_schema_;
	}

private:
	duckdb::vector<duckdb::LogicalType> types_;
	duckdb::vector<std::string> names_;
	duckdb::ClientProperties options_;
	std::shared_ptr<arrow::Schema> arrow_schema_;
};

} // namespace openduck
