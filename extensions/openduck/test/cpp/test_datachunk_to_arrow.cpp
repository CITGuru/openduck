#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "datachunk_to_arrow.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>

#include <memory>
#include <string>
#include <vector>

using namespace duckdb;
using openduck::DataChunkToArrow;

// ── helpers ────────────────────────────────────────────────────────────────

namespace {

struct Fixture {
	DuckDB db;
	Connection con;
	Fixture() : db(nullptr), con(db) {
	}

	shared_ptr<ClientContext> Ctx() {
		return con.context;
	}
};

static std::shared_ptr<arrow::RecordBatch>
DecodeSingleBatch(const std::string &ipc_payload) {
	auto buf = arrow::Buffer::FromString(ipc_payload);
	arrow::io::BufferReader reader(buf);
	auto stream_reader_result = arrow::ipc::RecordBatchStreamReader::Open(&reader);
	REQUIRE(stream_reader_result.ok());
	auto stream_reader = *stream_reader_result;
	std::shared_ptr<arrow::RecordBatch> batch;
	auto st = stream_reader->ReadNext(&batch);
	REQUIRE(st.ok());
	REQUIRE(batch != nullptr);
	return batch;
}

} // namespace

// ── tests ──────────────────────────────────────────────────────────────────

TEST_CASE("Encode empty DataChunk returns empty string", "[datachunk_to_arrow]") {
	Fixture fx;
	DataChunkToArrow enc(*fx.Ctx(), {LogicalType::INTEGER}, {"id"});

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
	chunk.SetCardinality(0);
	REQUIRE(enc.Encode(chunk).empty());
}

TEST_CASE("Encode INT + VARCHAR round-trips", "[datachunk_to_arrow]") {
	Fixture fx;
	DataChunkToArrow enc(*fx.Ctx(), {LogicalType::INTEGER, LogicalType::VARCHAR},
	                     {"id", "name"});

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(),
	                  {LogicalType::INTEGER, LogicalType::VARCHAR});
	chunk.SetCardinality(3);
	auto &ids = chunk.data[0];
	ids.SetValue(0, Value::INTEGER(1));
	ids.SetValue(1, Value::INTEGER(2));
	ids.SetValue(2, Value::INTEGER(3));
	auto &names = chunk.data[1];
	names.SetValue(0, Value("alpha"));
	names.SetValue(1, Value("beta"));
	names.SetValue(2, Value("gamma"));

	auto ipc = enc.Encode(chunk);
	REQUIRE_FALSE(ipc.empty());

	auto batch = DecodeSingleBatch(ipc);
	REQUIRE(batch->num_rows() == 3);
	REQUIRE(batch->num_columns() == 2);

	auto *id_arr = static_cast<arrow::Int32Array *>(batch->column(0).get());
	REQUIRE(id_arr->Value(0) == 1);
	REQUIRE(id_arr->Value(1) == 2);
	REQUIRE(id_arr->Value(2) == 3);

	auto *name_arr = static_cast<arrow::StringArray *>(batch->column(1).get());
	REQUIRE(name_arr->GetString(0) == "alpha");
	REQUIRE(name_arr->GetString(1) == "beta");
	REQUIRE(name_arr->GetString(2) == "gamma");
}

TEST_CASE("Encode BIGINT + DOUBLE round-trips", "[datachunk_to_arrow]") {
	Fixture fx;
	DataChunkToArrow enc(*fx.Ctx(), {LogicalType::BIGINT, LogicalType::DOUBLE},
	                     {"k", "v"});

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(),
	                  {LogicalType::BIGINT, LogicalType::DOUBLE});
	chunk.SetCardinality(2);
	chunk.data[0].SetValue(0, Value::BIGINT(100));
	chunk.data[0].SetValue(1, Value::BIGINT(-1));
	chunk.data[1].SetValue(0, Value::DOUBLE(3.14));
	chunk.data[1].SetValue(1, Value::DOUBLE(-2.718));

	auto ipc = enc.Encode(chunk);
	auto batch = DecodeSingleBatch(ipc);
	REQUIRE(batch->num_rows() == 2);

	auto *k_arr = static_cast<arrow::Int64Array *>(batch->column(0).get());
	REQUIRE(k_arr->Value(0) == 100);
	REQUIRE(k_arr->Value(1) == -1);

	auto *v_arr = static_cast<arrow::DoubleArray *>(batch->column(1).get());
	REQUIRE(v_arr->Value(0) == Approx(3.14));
	REQUIRE(v_arr->Value(1) == Approx(-2.718));
}

TEST_CASE("Encode NULLs preserved in validity mask", "[datachunk_to_arrow]") {
	Fixture fx;
	DataChunkToArrow enc(*fx.Ctx(), {LogicalType::INTEGER}, {"id"});

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
	chunk.SetCardinality(3);
	chunk.data[0].SetValue(0, Value::INTEGER(42));
	chunk.data[0].SetValue(1, Value());            // NULL
	chunk.data[0].SetValue(2, Value::INTEGER(99));

	auto ipc = enc.Encode(chunk);
	auto batch = DecodeSingleBatch(ipc);
	REQUIRE(batch->num_rows() == 3);

	auto col = batch->column(0);
	REQUIRE_FALSE(col->IsNull(0));
	REQUIRE(col->IsNull(1));
	REQUIRE_FALSE(col->IsNull(2));
	auto *arr = static_cast<arrow::Int32Array *>(col.get());
	REQUIRE(arr->Value(0) == 42);
	REQUIRE(arr->Value(2) == 99);
}

TEST_CASE("Encode BOOLEAN and BLOB round-trip", "[datachunk_to_arrow]") {
	Fixture fx;
	DataChunkToArrow enc(*fx.Ctx(),
	                     {LogicalType::BOOLEAN, LogicalType::BLOB},
	                     {"flag", "payload"});

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(),
	                  {LogicalType::BOOLEAN, LogicalType::BLOB});
	chunk.SetCardinality(2);
	chunk.data[0].SetValue(0, Value::BOOLEAN(true));
	chunk.data[0].SetValue(1, Value::BOOLEAN(false));
	const std::string blob_a = "\x00\x01\x02\xff";
	const std::string blob_b = "opaque";
	chunk.data[1].SetValue(0, Value::BLOB(reinterpret_cast<const_data_ptr_t>(blob_a.data()),
	                                        blob_a.size()));
	chunk.data[1].SetValue(1, Value::BLOB(reinterpret_cast<const_data_ptr_t>(blob_b.data()),
	                                        blob_b.size()));

	auto ipc = enc.Encode(chunk);
	auto batch = DecodeSingleBatch(ipc);
	REQUIRE(batch->num_rows() == 2);
	auto *flag_arr = static_cast<arrow::BooleanArray *>(batch->column(0).get());
	REQUIRE(flag_arr->Value(0) == true);
	REQUIRE(flag_arr->Value(1) == false);
	auto *blob_arr = static_cast<arrow::BinaryArray *>(batch->column(1).get());
	REQUIRE(blob_arr->GetString(0) == blob_a);
	REQUIRE(blob_arr->GetString(1) == blob_b);
}

TEST_CASE("Arrow schema names match the names passed to the encoder",
          "[datachunk_to_arrow][schema]") {
	Fixture fx;
	DataChunkToArrow enc(*fx.Ctx(),
	                     {LogicalType::INTEGER, LogicalType::VARCHAR},
	                     {"my_id", "my_name"});
	auto schema = enc.GetArrowSchema();
	REQUIRE(schema->num_fields() == 2);
	REQUIRE(schema->field(0)->name() == "my_id");
	REQUIRE(schema->field(1)->name() == "my_name");
}

TEST_CASE("Encoder reusable across multiple chunks", "[datachunk_to_arrow]") {
	Fixture fx;
	DataChunkToArrow enc(*fx.Ctx(), {LogicalType::INTEGER}, {"x"});
	for (int i = 0; i < 5; i++) {
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
		chunk.SetCardinality(2);
		chunk.data[0].SetValue(0, Value::INTEGER(i * 10));
		chunk.data[0].SetValue(1, Value::INTEGER(i * 10 + 1));
		auto ipc = enc.Encode(chunk);
		auto batch = DecodeSingleBatch(ipc);
		REQUIRE(batch->num_rows() == 2);
		auto *arr = static_cast<arrow::Int32Array *>(batch->column(0).get());
		REQUIRE(arr->Value(0) == i * 10);
		REQUIRE(arr->Value(1) == i * 10 + 1);
	}
}
