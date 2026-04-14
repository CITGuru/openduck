#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "arrow_bridge.hpp"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

#include "duckdb/common/allocator.hpp"

// ── helpers ──────────────────────────────────────────────────────────────────

static std::string MakeIpcPayload(
    const std::shared_ptr<arrow::Schema> &schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
  for (auto &b : batches) {
    REQUIRE(writer->WriteRecordBatch(*b).ok());
  }
  REQUIRE(writer->Close().ok());
  auto buf = sink->Finish().ValueOrDie();
  return buf->ToString();
}

static std::shared_ptr<arrow::Schema> IntStringSchema() {
  return arrow::schema(
      {arrow::field("id", arrow::int32()),
       arrow::field("name", arrow::utf8())});
}

static std::shared_ptr<arrow::RecordBatch>
MakeBatch(const std::shared_ptr<arrow::Schema> &schema,
          const std::vector<int32_t> &ids,
          const std::vector<std::string> &names) {
  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder;
  REQUIRE(id_builder.AppendValues(ids).ok());
  REQUIRE(name_builder.AppendValues(names).ok());
  return arrow::RecordBatch::Make(
      schema, static_cast<int64_t>(ids.size()),
      {id_builder.Finish().ValueOrDie(),
       name_builder.Finish().ValueOrDie()});
}

// ── ExtractSchema ────────────────────────────────────────────────────────────

TEST_CASE("ExtractSchema returns correct types and names", "[arrow_bridge]") {
  auto schema = IntStringSchema();
  auto batch = MakeBatch(schema, {1}, {"alice"});
  auto ipc = MakeIpcPayload(schema, {batch});

  auto info = openduck::ExtractSchema(ipc);
  REQUIRE(info.names.size() == 2);
  REQUIRE(info.names[0] == "id");
  REQUIRE(info.names[1] == "name");
  REQUIRE(info.types[0] == duckdb::LogicalType::INTEGER);
  REQUIRE(info.types[1] == duckdb::LogicalType::VARCHAR);
}

// ── ReadAllIpcBatches ────────────────────────────────────────────────────────

TEST_CASE("ReadAllIpcBatches reads single batch", "[arrow_bridge]") {
  auto schema = IntStringSchema();
  auto batch = MakeBatch(schema, {10, 20, 30}, {"a", "b", "c"});
  auto ipc = MakeIpcPayload(schema, {batch});

  std::deque<std::shared_ptr<arrow::RecordBatch>> out;
  openduck::ReadAllIpcBatches(ipc, out);

  REQUIRE(out.size() == 1);
  REQUIRE(out[0]->num_rows() == 3);
}

TEST_CASE("ReadAllIpcBatches reads multiple coalesced batches",
          "[arrow_bridge]") {
  auto schema = IntStringSchema();
  auto b1 = MakeBatch(schema, {1, 2}, {"alice", "bob"});
  auto b2 = MakeBatch(schema, {3}, {"charlie"});
  auto b3 = MakeBatch(schema, {4, 5, 6}, {"dave", "eve", "frank"});
  auto ipc = MakeIpcPayload(schema, {b1, b2, b3});

  std::deque<std::shared_ptr<arrow::RecordBatch>> out;
  openduck::ReadAllIpcBatches(ipc, out);

  REQUIRE(out.size() == 3);
  REQUIRE(out[0]->num_rows() == 2);
  REQUIRE(out[1]->num_rows() == 1);
  REQUIRE(out[2]->num_rows() == 3);
}

TEST_CASE("ReadAllIpcBatches skips empty batches", "[arrow_bridge]") {
  auto schema = IntStringSchema();
  auto b1 = MakeBatch(schema, {1}, {"x"});
  auto empty = MakeBatch(schema, {}, {});
  auto b2 = MakeBatch(schema, {2}, {"y"});
  auto ipc = MakeIpcPayload(schema, {b1, empty, b2});

  std::deque<std::shared_ptr<arrow::RecordBatch>> out;
  openduck::ReadAllIpcBatches(ipc, out);

  REQUIRE(out.size() == 2);
  REQUIRE(out[0]->num_rows() == 1);
  REQUIRE(out[1]->num_rows() == 1);
}

TEST_CASE("ReadAllIpcBatches handles 8 coalesced batches (BATCHES_PER_IPC_MESSAGE)",
          "[arrow_bridge]") {
  auto schema = IntStringSchema();
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (int i = 0; i < 8; i++) {
    batches.push_back(MakeBatch(schema, {i * 10, i * 10 + 1},
                                {"row" + std::to_string(i * 2),
                                 "row" + std::to_string(i * 2 + 1)}));
  }
  auto ipc = MakeIpcPayload(schema, batches);

  std::deque<std::shared_ptr<arrow::RecordBatch>> out;
  openduck::ReadAllIpcBatches(ipc, out);

  REQUIRE(out.size() == 8);
  int total_rows = 0;
  for (auto &b : out) {
    total_rows += static_cast<int>(b->num_rows());
  }
  REQUIRE(total_rows == 16);
}

// ── CopyBatchToDataChunk ─────────────────────────────────────────────────────

TEST_CASE("CopyBatchToDataChunk copies integer and string columns",
          "[arrow_bridge]") {
  auto schema = IntStringSchema();
  auto batch = MakeBatch(schema, {42, 99}, {"hello", "world"});

  duckdb::vector<duckdb::LogicalType> types = {
      duckdb::LogicalType::INTEGER, duckdb::LogicalType::VARCHAR};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyBatchToDataChunk(batch, chunk);

  REQUIRE(rows == 2);
  REQUIRE(chunk.data[0].GetValue(0).GetValue<int32_t>() == 42);
  REQUIRE(chunk.data[0].GetValue(1).GetValue<int32_t>() == 99);
  REQUIRE(chunk.data[1].GetValue(0).GetValue<duckdb::string>() == "hello");
  REQUIRE(chunk.data[1].GetValue(1).GetValue<duckdb::string>() == "world");
}

TEST_CASE("CopyBatchToDataChunk returns 0 for null batch", "[arrow_bridge]") {
  duckdb::vector<duckdb::LogicalType> types = {duckdb::LogicalType::INTEGER};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyBatchToDataChunk(nullptr, chunk);
  REQUIRE(rows == 0);
}

TEST_CASE("CopyBatchToDataChunk returns 0 for empty batch",
          "[arrow_bridge]") {
  auto schema = IntStringSchema();
  auto batch = MakeBatch(schema, {}, {});

  duckdb::vector<duckdb::LogicalType> types = {
      duckdb::LogicalType::INTEGER, duckdb::LogicalType::VARCHAR};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyBatchToDataChunk(batch, chunk);
  REQUIRE(rows == 0);
}

// ── CopyIpcToDataChunk (backward compat) ─────────────────────────────────────

TEST_CASE("CopyIpcToDataChunk copies first batch from multi-batch IPC",
          "[arrow_bridge]") {
  auto schema = IntStringSchema();
  auto b1 = MakeBatch(schema, {1, 2}, {"first", "batch"});
  auto b2 = MakeBatch(schema, {3, 4}, {"second", "batch"});
  auto ipc = MakeIpcPayload(schema, {b1, b2});

  duckdb::vector<duckdb::LogicalType> types = {
      duckdb::LogicalType::INTEGER, duckdb::LogicalType::VARCHAR};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyIpcToDataChunk(ipc, chunk);

  REQUIRE(rows == 2);
  REQUIRE(chunk.data[0].GetValue(0).GetValue<int32_t>() == 1);
  REQUIRE(chunk.data[1].GetValue(0).GetValue<duckdb::string>() == "first");
}

// ── Type coverage ────────────────────────────────────────────────────────────

TEST_CASE("ExtractSchema maps numeric Arrow types correctly",
          "[arrow_bridge]") {
  auto schema = arrow::schema({
      arrow::field("b", arrow::boolean()),
      arrow::field("i8", arrow::int8()),
      arrow::field("i16", arrow::int16()),
      arrow::field("i32", arrow::int32()),
      arrow::field("i64", arrow::int64()),
      arrow::field("f32", arrow::float32()),
      arrow::field("f64", arrow::float64()),
      arrow::field("d32", arrow::date32()),
      arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
  });

  arrow::BooleanBuilder bb;
  arrow::Int8Builder i8b;
  arrow::Int16Builder i16b;
  arrow::Int32Builder i32b;
  arrow::Int64Builder i64b;
  arrow::FloatBuilder f32b;
  arrow::DoubleBuilder f64b;
  arrow::Date32Builder d32b;
  arrow::TimestampBuilder tsb(arrow::timestamp(arrow::TimeUnit::MICRO),
                              arrow::default_memory_pool());

  REQUIRE(bb.Append(true).ok());
  REQUIRE(i8b.Append(1).ok());
  REQUIRE(i16b.Append(2).ok());
  REQUIRE(i32b.Append(3).ok());
  REQUIRE(i64b.Append(4).ok());
  REQUIRE(f32b.Append(5.0f).ok());
  REQUIRE(f64b.Append(6.0).ok());
  REQUIRE(d32b.Append(18000).ok());
  REQUIRE(tsb.Append(1000000).ok());

  auto batch = arrow::RecordBatch::Make(
      schema, 1,
      {bb.Finish().ValueOrDie(), i8b.Finish().ValueOrDie(),
       i16b.Finish().ValueOrDie(), i32b.Finish().ValueOrDie(),
       i64b.Finish().ValueOrDie(), f32b.Finish().ValueOrDie(),
       f64b.Finish().ValueOrDie(), d32b.Finish().ValueOrDie(),
       tsb.Finish().ValueOrDie()});

  auto ipc = MakeIpcPayload(schema, {batch});
  auto info = openduck::ExtractSchema(ipc);

  REQUIRE(info.types[0] == duckdb::LogicalType::BOOLEAN);
  REQUIRE(info.types[1] == duckdb::LogicalType::TINYINT);
  REQUIRE(info.types[2] == duckdb::LogicalType::SMALLINT);
  REQUIRE(info.types[3] == duckdb::LogicalType::INTEGER);
  REQUIRE(info.types[4] == duckdb::LogicalType::BIGINT);
  REQUIRE(info.types[5] == duckdb::LogicalType::FLOAT);
  REQUIRE(info.types[6] == duckdb::LogicalType::DOUBLE);
  REQUIRE(info.types[7] == duckdb::LogicalType::DATE);
  REQUIRE(info.types[8] == duckdb::LogicalType::TIMESTAMP);
}

// ── Null handling ────────────────────────────────────────────────────────────

TEST_CASE("CopyBatchToDataChunk handles null values", "[arrow_bridge]") {
  auto schema = arrow::schema(
      {arrow::field("id", arrow::int32()),
       arrow::field("name", arrow::utf8())});

  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder;
  REQUIRE(id_builder.Append(1).ok());
  REQUIRE(id_builder.AppendNull().ok());
  REQUIRE(id_builder.Append(3).ok());
  REQUIRE(name_builder.Append("alice").ok());
  REQUIRE(name_builder.Append("bob").ok());
  REQUIRE(name_builder.AppendNull().ok());

  auto batch = arrow::RecordBatch::Make(
      schema, 3,
      {id_builder.Finish().ValueOrDie(),
       name_builder.Finish().ValueOrDie()});

  duckdb::vector<duckdb::LogicalType> types = {
      duckdb::LogicalType::INTEGER, duckdb::LogicalType::VARCHAR};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyBatchToDataChunk(batch, chunk);
  REQUIRE(rows == 3);

  REQUIRE(chunk.data[0].GetValue(0).GetValue<int32_t>() == 1);
  REQUIRE(chunk.data[0].GetValue(1).IsNull());
  REQUIRE(chunk.data[0].GetValue(2).GetValue<int32_t>() == 3);

  REQUIRE(chunk.data[1].GetValue(0).GetValue<duckdb::string>() == "alice");
  REQUIRE(chunk.data[1].GetValue(1).GetValue<duckdb::string>() == "bob");
  REQUIRE(chunk.data[1].GetValue(2).IsNull());
}
