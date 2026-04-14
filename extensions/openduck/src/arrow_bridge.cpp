#include "arrow_bridge.hpp"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>

#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"

namespace openduck {

// ── Arrow type → DuckDB type mapping ────────────────────────────────────────

static duckdb::LogicalType
MapArrowType(const std::shared_ptr<arrow::DataType> &type) {
  switch (type->id()) {
  case arrow::Type::BOOL:
    return duckdb::LogicalType::BOOLEAN;
  case arrow::Type::INT8:
    return duckdb::LogicalType::TINYINT;
  case arrow::Type::INT16:
    return duckdb::LogicalType::SMALLINT;
  case arrow::Type::INT32:
    return duckdb::LogicalType::INTEGER;
  case arrow::Type::INT64:
    return duckdb::LogicalType::BIGINT;
  case arrow::Type::UINT8:
    return duckdb::LogicalType::UTINYINT;
  case arrow::Type::UINT16:
    return duckdb::LogicalType::USMALLINT;
  case arrow::Type::UINT32:
    return duckdb::LogicalType::UINTEGER;
  case arrow::Type::UINT64:
    return duckdb::LogicalType::UBIGINT;
  case arrow::Type::HALF_FLOAT:
  case arrow::Type::FLOAT:
    return duckdb::LogicalType::FLOAT;
  case arrow::Type::DOUBLE:
    return duckdb::LogicalType::DOUBLE;
  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    return duckdb::LogicalType::VARCHAR;
  case arrow::Type::BINARY:
  case arrow::Type::LARGE_BINARY:
    return duckdb::LogicalType::BLOB;
  case arrow::Type::DATE32:
  case arrow::Type::DATE64:
    return duckdb::LogicalType::DATE;
  case arrow::Type::TIMESTAMP:
    return duckdb::LogicalType::TIMESTAMP;
  case arrow::Type::TIME32:
  case arrow::Type::TIME64:
    return duckdb::LogicalType::TIME;
  case arrow::Type::INTERVAL_MONTHS:
  case arrow::Type::INTERVAL_DAY_TIME:
    return duckdb::LogicalType::INTERVAL;
  case arrow::Type::DECIMAL128:
  case arrow::Type::DECIMAL256: {
    auto dec = std::static_pointer_cast<arrow::DecimalType>(type);
    return duckdb::LogicalType::DECIMAL(dec->precision(), dec->scale());
  }
  default:
    return duckdb::LogicalType::VARCHAR;
  }
}

// ── IPC stream reader helper ────────────────────────────────────────────────

static std::shared_ptr<arrow::ipc::RecordBatchStreamReader>
OpenIpcStream(const std::string &ipc_bytes) {
  auto buffer = arrow::Buffer::FromString(ipc_bytes);
  auto input = std::make_shared<arrow::io::BufferReader>(buffer);
  auto result = arrow::ipc::RecordBatchStreamReader::Open(input);
  if (!result.ok()) {
    throw std::runtime_error("Arrow IPC open failed: " +
                             result.status().ToString());
  }
  return *result;
}

// ── ExtractSchema ───────────────────────────────────────────────────────────

ArrowSchemaInfo ExtractSchema(const std::string &ipc_bytes) {
  auto reader = OpenIpcStream(ipc_bytes);
  auto schema = reader->schema();

  ArrowSchemaInfo info;
  info.types.reserve(schema->num_fields());
  info.names.reserve(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); i++) {
    info.names.push_back(schema->field(i)->name());
    info.types.push_back(MapArrowType(schema->field(i)->type()));
  }
  return info;
}

// ── Column copiers ──────────────────────────────────────────────────────────

template <typename ArrowArrayT, typename CppT>
static void CopyFixedWidth(const std::shared_ptr<arrow::Array> &array,
                           duckdb::Vector &vec, duckdb::idx_t count) {
  auto typed = std::static_pointer_cast<ArrowArrayT>(array);
  auto data = duckdb::FlatVector::GetData<CppT>(vec);
  auto &validity = duckdb::FlatVector::Validity(vec);

  for (duckdb::idx_t i = 0; i < count; i++) {
    if (typed->IsNull(static_cast<int64_t>(i))) {
      validity.SetInvalid(i);
    } else {
      data[i] = static_cast<CppT>(typed->Value(static_cast<int64_t>(i)));
    }
  }
}

static void CopyBoolColumn(const std::shared_ptr<arrow::Array> &array,
                            duckdb::Vector &vec, duckdb::idx_t count) {
  auto typed = std::static_pointer_cast<arrow::BooleanArray>(array);
  auto data = duckdb::FlatVector::GetData<bool>(vec);
  auto &validity = duckdb::FlatVector::Validity(vec);

  for (duckdb::idx_t i = 0; i < count; i++) {
    if (typed->IsNull(static_cast<int64_t>(i))) {
      validity.SetInvalid(i);
    } else {
      data[i] = typed->Value(static_cast<int64_t>(i));
    }
  }
}

static void CopyStringColumn(const std::shared_ptr<arrow::Array> &array,
                              duckdb::Vector &vec, duckdb::idx_t count) {
  auto &validity = duckdb::FlatVector::Validity(vec);

  if (array->type_id() == arrow::Type::LARGE_STRING) {
    auto typed = std::static_pointer_cast<arrow::LargeStringArray>(array);
    for (duckdb::idx_t i = 0; i < count; i++) {
      if (typed->IsNull(static_cast<int64_t>(i))) {
        validity.SetInvalid(i);
      } else {
        auto sv = typed->GetView(static_cast<int64_t>(i));
        vec.SetValue(i, duckdb::Value(std::string(sv)));
      }
    }
  } else {
    auto typed = std::static_pointer_cast<arrow::StringArray>(array);
    for (duckdb::idx_t i = 0; i < count; i++) {
      if (typed->IsNull(static_cast<int64_t>(i))) {
        validity.SetInvalid(i);
      } else {
        auto sv = typed->GetView(static_cast<int64_t>(i));
        vec.SetValue(i, duckdb::Value(std::string(sv)));
      }
    }
  }
}

static void CopyBlobColumn(const std::shared_ptr<arrow::Array> &array,
                            duckdb::Vector &vec, duckdb::idx_t count) {
  auto typed = std::static_pointer_cast<arrow::BinaryArray>(array);
  auto &validity = duckdb::FlatVector::Validity(vec);

  for (duckdb::idx_t i = 0; i < count; i++) {
    if (typed->IsNull(static_cast<int64_t>(i))) {
      validity.SetInvalid(i);
    } else {
      auto sv = typed->GetView(static_cast<int64_t>(i));
      vec.SetValue(i, duckdb::Value::BLOB(std::string(sv)));
    }
  }
}

/// Fallback: render any Arrow array element as its string representation.
static void CopyAsString(const std::shared_ptr<arrow::Array> &array,
                          duckdb::Vector &vec, duckdb::idx_t count) {
  auto &validity = duckdb::FlatVector::Validity(vec);
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (array->IsNull(static_cast<int64_t>(i))) {
      validity.SetInvalid(i);
    } else {
      auto scalar = array->GetScalar(static_cast<int64_t>(i));
      if (scalar.ok()) {
        vec.SetValue(i, duckdb::Value((*scalar)->ToString()));
      } else {
        validity.SetInvalid(i);
      }
    }
  }
}

static void CopyColumn(const std::shared_ptr<arrow::Array> &array,
                        duckdb::Vector &vec, const duckdb::LogicalType &type,
                        duckdb::idx_t count) {
  switch (type.id()) {
  case duckdb::LogicalTypeId::BOOLEAN:
    CopyBoolColumn(array, vec, count);
    break;
  case duckdb::LogicalTypeId::TINYINT:
    CopyFixedWidth<arrow::Int8Array, int8_t>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::SMALLINT:
    CopyFixedWidth<arrow::Int16Array, int16_t>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::INTEGER:
    CopyFixedWidth<arrow::Int32Array, int32_t>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::BIGINT:
    CopyFixedWidth<arrow::Int64Array, int64_t>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::UTINYINT:
    CopyFixedWidth<arrow::UInt8Array, uint8_t>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::USMALLINT:
    CopyFixedWidth<arrow::UInt16Array, uint16_t>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::UINTEGER:
    CopyFixedWidth<arrow::UInt32Array, uint32_t>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::UBIGINT:
    CopyFixedWidth<arrow::UInt64Array, uint64_t>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::FLOAT:
    CopyFixedWidth<arrow::FloatArray, float>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::DOUBLE:
    CopyFixedWidth<arrow::DoubleArray, double>(array, vec, count);
    break;
  case duckdb::LogicalTypeId::BLOB:
    CopyBlobColumn(array, vec, count);
    break;
  case duckdb::LogicalTypeId::VARCHAR:
    if (array->type_id() == arrow::Type::STRING ||
        array->type_id() == arrow::Type::LARGE_STRING) {
      CopyStringColumn(array, vec, count);
    } else {
      CopyAsString(array, vec, count);
    }
    break;
  default:
    CopyAsString(array, vec, count);
    break;
  }
}

// ── ReadAllIpcBatches ───────────────────────────────────────────────────────

void ReadAllIpcBatches(
    const std::string &ipc_bytes,
    std::deque<std::shared_ptr<arrow::RecordBatch>> &out) {
  auto reader = OpenIpcStream(ipc_bytes);
  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = reader->ReadNext(&batch);
    if (!status.ok() || !batch) {
      break;
    }
    if (batch->num_rows() > 0) {
      out.push_back(std::move(batch));
    }
  }
}

// ── CopyBatchToDataChunk ────────────────────────────────────────────────────

duckdb::idx_t CopyBatchToDataChunk(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    duckdb::DataChunk &chunk) {
  if (!batch || batch->num_rows() == 0) {
    return 0;
  }

  auto count = static_cast<duckdb::idx_t>(batch->num_rows());
  chunk.SetCardinality(count);
  for (duckdb::idx_t col = 0; col < chunk.ColumnCount() &&
                              col < static_cast<duckdb::idx_t>(batch->num_columns());
       col++) {
    CopyColumn(batch->column(static_cast<int>(col)), chunk.data[col],
               chunk.data[col].GetType(), count);
  }
  return count;
}

// ── CopyIpcToDataChunk (backward compat) ────────────────────────────────────

duckdb::idx_t CopyIpcToDataChunk(const std::string &ipc_bytes,
                                 duckdb::DataChunk &chunk) {
  std::deque<std::shared_ptr<arrow::RecordBatch>> batches;
  ReadAllIpcBatches(ipc_bytes, batches);
  if (batches.empty()) {
    return 0;
  }
  return CopyBatchToDataChunk(batches.front(), chunk);
}

} // namespace openduck
