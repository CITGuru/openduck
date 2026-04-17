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

// ── CopyBatchToDataChunkProjected ────────────────────────────────────────────

TEST_CASE("Projected copy maps columns correctly", "[arrow_bridge]") {
  // Arrow batch: [id(int32), name(utf8), score(int64)]
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("name", arrow::utf8()),
      arrow::field("score", arrow::int64()),
  });

  arrow::Int32Builder id_b;
  arrow::StringBuilder name_b;
  arrow::Int64Builder score_b;
  REQUIRE(id_b.AppendValues({10, 20}).ok());
  REQUIRE(name_b.AppendValues({"alice", "bob"}).ok());
  REQUIRE(score_b.AppendValues({100, 200}).ok());

  auto batch = arrow::RecordBatch::Make(
      schema, 2,
      {id_b.Finish().ValueOrDie(), name_b.Finish().ValueOrDie(),
       score_b.Finish().ValueOrDie()});

  SECTION("identity mapping — all columns in order") {
    duckdb::vector<duckdb::LogicalType> types = {
        duckdb::LogicalType::INTEGER, duckdb::LogicalType::VARCHAR,
        duckdb::LogicalType::BIGINT};
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

    std::vector<int> mapping = {0, 1, 2};
    auto rows = openduck::CopyBatchToDataChunkProjected(batch, chunk, mapping);
    REQUIRE(rows == 2);
    REQUIRE(chunk.data[0].GetValue(0).GetValue<int32_t>() == 10);
    REQUIRE(chunk.data[1].GetValue(1).GetValue<duckdb::string>() == "bob");
    REQUIRE(chunk.data[2].GetValue(0).GetValue<int64_t>() == 100);
  }

  SECTION("subset — only name column") {
    duckdb::vector<duckdb::LogicalType> types = {duckdb::LogicalType::VARCHAR};
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

    std::vector<int> mapping = {1}; // batch column 1 = name
    auto rows = openduck::CopyBatchToDataChunkProjected(batch, chunk, mapping);
    REQUIRE(rows == 2);
    REQUIRE(chunk.data[0].GetValue(0).GetValue<duckdb::string>() == "alice");
    REQUIRE(chunk.data[0].GetValue(1).GetValue<duckdb::string>() == "bob");
  }

  SECTION("reordered — score, id (skip name)") {
    duckdb::vector<duckdb::LogicalType> types = {
        duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER};
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

    std::vector<int> mapping = {2, 0}; // batch col 2=score, 0=id
    auto rows = openduck::CopyBatchToDataChunkProjected(batch, chunk, mapping);
    REQUIRE(rows == 2);
    REQUIRE(chunk.data[0].GetValue(0).GetValue<int64_t>() == 100);
    REQUIRE(chunk.data[1].GetValue(0).GetValue<int32_t>() == 10);
  }
}

TEST_CASE("Projected copy handles row_id virtual column (-1)",
          "[arrow_bridge]") {
  auto schema = arrow::schema({arrow::field("name", arrow::utf8())});

  arrow::StringBuilder name_b;
  REQUIRE(name_b.AppendValues({"x", "y", "z"}).ok());
  auto batch = arrow::RecordBatch::Make(
      schema, 3, {name_b.Finish().ValueOrDie()});

  // Output chunk: [row_id(BIGINT), name(VARCHAR)]
  duckdb::vector<duckdb::LogicalType> types = {
      duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  std::vector<int> mapping = {-1, 0}; // -1 = row_id, 0 = name
  auto rows = openduck::CopyBatchToDataChunkProjected(batch, chunk, mapping);
  REQUIRE(rows == 3);

  // row_id should be filled with row indices 0, 1, 2
  REQUIRE(chunk.data[0].GetValue(0).GetValue<int64_t>() == 0);
  REQUIRE(chunk.data[0].GetValue(1).GetValue<int64_t>() == 1);
  REQUIRE(chunk.data[0].GetValue(2).GetValue<int64_t>() == 2);

  REQUIRE(chunk.data[1].GetValue(0).GetValue<duckdb::string>() == "x");
  REQUIRE(chunk.data[1].GetValue(1).GetValue<duckdb::string>() == "y");
  REQUIRE(chunk.data[1].GetValue(2).GetValue<duckdb::string>() == "z");
}

TEST_CASE("Projected copy returns 0 for null batch", "[arrow_bridge]") {
  duckdb::vector<duckdb::LogicalType> types = {duckdb::LogicalType::INTEGER};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  std::vector<int> mapping = {0};
  auto rows = openduck::CopyBatchToDataChunkProjected(nullptr, chunk, mapping);
  REQUIRE(rows == 0);
}

// ── Wide type coverage for CopyBatchToDataChunk ──────────────────────────────

TEST_CASE("CopyBatchToDataChunk handles all integer widths", "[arrow_bridge]") {
  auto schema = arrow::schema({
      arrow::field("i8", arrow::int8()),
      arrow::field("i16", arrow::int16()),
      arrow::field("i32", arrow::int32()),
      arrow::field("i64", arrow::int64()),
      arrow::field("u8", arrow::uint8()),
      arrow::field("u16", arrow::uint16()),
      arrow::field("u32", arrow::uint32()),
      arrow::field("u64", arrow::uint64()),
  });

  arrow::Int8Builder b1;
  arrow::Int16Builder b2;
  arrow::Int32Builder b3;
  arrow::Int64Builder b4;
  arrow::UInt8Builder b5;
  arrow::UInt16Builder b6;
  arrow::UInt32Builder b7;
  arrow::UInt64Builder b8;
  REQUIRE(b1.Append(-1).ok());
  REQUIRE(b2.Append(-2).ok());
  REQUIRE(b3.Append(-3).ok());
  REQUIRE(b4.Append(-4).ok());
  REQUIRE(b5.Append(5).ok());
  REQUIRE(b6.Append(6).ok());
  REQUIRE(b7.Append(7).ok());
  REQUIRE(b8.Append(8).ok());

  auto batch = arrow::RecordBatch::Make(
      schema, 1,
      {b1.Finish().ValueOrDie(), b2.Finish().ValueOrDie(),
       b3.Finish().ValueOrDie(), b4.Finish().ValueOrDie(),
       b5.Finish().ValueOrDie(), b6.Finish().ValueOrDie(),
       b7.Finish().ValueOrDie(), b8.Finish().ValueOrDie()});

  duckdb::vector<duckdb::LogicalType> types = {
      duckdb::LogicalType::TINYINT, duckdb::LogicalType::SMALLINT,
      duckdb::LogicalType::INTEGER, duckdb::LogicalType::BIGINT,
      duckdb::LogicalType::UTINYINT, duckdb::LogicalType::USMALLINT,
      duckdb::LogicalType::UINTEGER, duckdb::LogicalType::UBIGINT};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyBatchToDataChunk(batch, chunk);
  REQUIRE(rows == 1);
  REQUIRE(chunk.data[0].GetValue(0).GetValue<int8_t>() == -1);
  REQUIRE(chunk.data[1].GetValue(0).GetValue<int16_t>() == -2);
  REQUIRE(chunk.data[2].GetValue(0).GetValue<int32_t>() == -3);
  REQUIRE(chunk.data[3].GetValue(0).GetValue<int64_t>() == -4);
  REQUIRE(chunk.data[4].GetValue(0).GetValue<uint8_t>() == 5);
  REQUIRE(chunk.data[5].GetValue(0).GetValue<uint16_t>() == 6);
  REQUIRE(chunk.data[6].GetValue(0).GetValue<uint32_t>() == 7);
  REQUIRE(chunk.data[7].GetValue(0).GetValue<uint64_t>() == 8);
}

TEST_CASE("CopyBatchToDataChunk handles float and double", "[arrow_bridge]") {
  auto schema = arrow::schema({
      arrow::field("f", arrow::float32()),
      arrow::field("d", arrow::float64()),
  });

  arrow::FloatBuilder fb;
  arrow::DoubleBuilder db;
  REQUIRE(fb.Append(3.14f).ok());
  REQUIRE(db.Append(2.71828).ok());

  auto batch = arrow::RecordBatch::Make(
      schema, 1,
      {fb.Finish().ValueOrDie(), db.Finish().ValueOrDie()});

  duckdb::vector<duckdb::LogicalType> types = {
      duckdb::LogicalType::FLOAT, duckdb::LogicalType::DOUBLE};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyBatchToDataChunk(batch, chunk);
  REQUIRE(rows == 1);
  REQUIRE(chunk.data[0].GetValue(0).GetValue<float>() == Approx(3.14f));
  REQUIRE(chunk.data[1].GetValue(0).GetValue<double>() == Approx(2.71828));
}

TEST_CASE("CopyBatchToDataChunk handles boolean", "[arrow_bridge]") {
  auto schema = arrow::schema({arrow::field("b", arrow::boolean())});
  arrow::BooleanBuilder bb;
  REQUIRE(bb.Append(true).ok());
  REQUIRE(bb.Append(false).ok());
  REQUIRE(bb.AppendNull().ok());

  auto batch = arrow::RecordBatch::Make(
      schema, 3, {bb.Finish().ValueOrDie()});

  duckdb::vector<duckdb::LogicalType> types = {duckdb::LogicalType::BOOLEAN};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyBatchToDataChunk(batch, chunk);
  REQUIRE(rows == 3);
  REQUIRE(chunk.data[0].GetValue(0).GetValue<bool>() == true);
  REQUIRE(chunk.data[0].GetValue(1).GetValue<bool>() == false);
  REQUIRE(chunk.data[0].GetValue(2).IsNull());
}

TEST_CASE("CopyBatchToDataChunk handles large_string", "[arrow_bridge]") {
  auto schema = arrow::schema({arrow::field("s", arrow::large_utf8())});
  arrow::LargeStringBuilder sb;
  REQUIRE(sb.Append("hello world").ok());
  REQUIRE(sb.AppendNull().ok());

  auto batch = arrow::RecordBatch::Make(
      schema, 2, {sb.Finish().ValueOrDie()});

  duckdb::vector<duckdb::LogicalType> types = {duckdb::LogicalType::VARCHAR};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);

  auto rows = openduck::CopyBatchToDataChunk(batch, chunk);
  REQUIRE(rows == 2);
  REQUIRE(chunk.data[0].GetValue(0).GetValue<duckdb::string>() == "hello world");
  REQUIRE(chunk.data[0].GetValue(1).IsNull());
}
