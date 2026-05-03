#include "datachunk_to_arrow.hpp"

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_properties.hpp"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

namespace openduck {

using namespace duckdb;

DataChunkToArrow::DataChunkToArrow(ClientContext &context, vector<LogicalType> types,
                                   vector<string> names)
    : types_(std::move(types)), names_(std::move(names)),
      options_(context.GetClientProperties()) {
	if (types_.size() != names_.size()) {
		throw InternalException(
		    "DataChunkToArrow: types.size() (%llu) != names.size() (%llu)",
		    (unsigned long long)types_.size(), (unsigned long long)names_.size());
	}

	// Build the Arrow schema once via DuckDB's C-data-interface exporter
	// and hand it to Arrow C++ for the typed shared_ptr<arrow::Schema>
	// we'll reuse for every batch.
	ArrowSchema c_schema;
	ArrowConverter::ToArrowSchema(&c_schema, types_, names_, options_);
	auto imported = arrow::ImportSchema(&c_schema);
	if (!imported.ok()) {
		// `ImportSchema` already released the C struct on success;
		// on failure the caller owns cleanup. Call the release
		// callback explicitly if it's still there.
		if (c_schema.release) {
			c_schema.release(&c_schema);
		}
		throw InternalException(
		    "DataChunkToArrow: arrow::ImportSchema failed: %s",
		    imported.status().ToString());
	}
	arrow_schema_ = *std::move(imported);
}

DataChunkToArrow::~DataChunkToArrow() = default;

std::string DataChunkToArrow::Encode(DataChunk &chunk) {
	if (chunk.size() == 0) {
		return std::string();
	}

	// 1. DuckDB → C Data Interface.
	ArrowArray c_array;
	// `ToArrowArray` expects `ClientProperties` by value; pass a copy
	// so we keep our cached one pristine. The empty extension-type
	// map covers every built-in `LogicalType`; user-defined types
	// would need entries here — not handled in v1.
	ArrowConverter::ToArrowArray(
	    chunk, &c_array, options_,
	    unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>>{});

	// 2. C Data Interface → arrow::RecordBatch (arrow C++ takes
	//    ownership of `c_array` and calls its release callback).
	auto batch_result = arrow::ImportRecordBatch(&c_array, arrow_schema_);
	if (!batch_result.ok()) {
		if (c_array.release) {
			c_array.release(&c_array);
		}
		throw InternalException(
		    "DataChunkToArrow: arrow::ImportRecordBatch failed: %s",
		    batch_result.status().ToString());
	}
	auto batch = *std::move(batch_result);

	// 3. arrow::RecordBatch → Arrow IPC stream payload (schema + 1
	//    batch + EOS). A fresh StreamWriter per call keeps each
	//    payload self-contained so the worker's `StreamReader` can
	//    decode it independently.
	auto sink_result = arrow::io::BufferOutputStream::Create();
	if (!sink_result.ok()) {
		throw InternalException("DataChunkToArrow: BufferOutputStream::Create: %s",
		                        sink_result.status().ToString());
	}
	auto sink = *sink_result;

	auto writer_result = arrow::ipc::MakeStreamWriter(sink, arrow_schema_);
	if (!writer_result.ok()) {
		throw InternalException("DataChunkToArrow: MakeStreamWriter: %s",
		                        writer_result.status().ToString());
	}
	auto writer = *std::move(writer_result);

	auto st = writer->WriteRecordBatch(*batch);
	if (!st.ok()) {
		throw InternalException("DataChunkToArrow: WriteRecordBatch: %s",
		                        st.ToString());
	}
	st = writer->Close();
	if (!st.ok()) {
		throw InternalException("DataChunkToArrow: writer->Close: %s",
		                        st.ToString());
	}

	auto buf_result = sink->Finish();
	if (!buf_result.ok()) {
		throw InternalException("DataChunkToArrow: sink->Finish: %s",
		                        buf_result.status().ToString());
	}
	auto buf = *buf_result;
	return buf->ToString();
}

} // namespace openduck
