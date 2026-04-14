#include "openduck_table_entry.hpp"
#include "grpc_client.hpp"
#include "arrow_bridge.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include <deque>

namespace openduck {

using namespace duckdb;

// ── Scan bind/execute data ──────────────────────────────────────────────────

struct OpenDuckTableScanBindData : public TableFunctionData {
	string endpoint;
	string token;
	string database;
	string table_name;
};

struct OpenDuckTableScanGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<GrpcClient> client;
	std::unique_ptr<GrpcStream> stream;
	std::deque<std::shared_ptr<arrow::RecordBatch>> pending_batches;
	std::string execution_id;
	bool done = false;
	std::mutex lock;
};

static unique_ptr<GlobalTableFunctionState> OpenDuckTableScanInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<OpenDuckTableScanBindData>();
	auto state = make_uniq<OpenDuckTableScanGlobalState>();
	state->client = std::make_unique<GrpcClient>(bind_data.endpoint);
	auto sql = "SELECT * FROM " + bind_data.table_name;
	state->stream = state->client->ExecuteSQL(sql, bind_data.database, bind_data.token);
	return std::move(state);
}

static void OpenDuckTableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<OpenDuckTableScanGlobalState>();

	std::lock_guard<std::mutex> guard(gstate.lock);
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}

	while (gstate.pending_batches.empty()) {
		auto ipc_bytes = gstate.stream->Next();
		if (!ipc_bytes) {
			gstate.done = true;
			output.SetCardinality(0);
			return;
		}
		ReadAllIpcBatches(*ipc_bytes, gstate.pending_batches);
	}

	auto batch = std::move(gstate.pending_batches.front());
	gstate.pending_batches.pop_front();

	auto rows = CopyBatchToDataChunk(batch, output);
	if (rows == 0) {
		gstate.done = true;
		output.SetCardinality(0);
	}
}

// ── OpenDuckTableEntry ──────────────────────────────────────────────────────

OpenDuckTableEntry::OpenDuckTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                       AttachConfig config)
    : TableCatalogEntry(catalog, schema, info), config_(std::move(config)) {
}

unique_ptr<BaseStatistics> OpenDuckTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableFunction OpenDuckTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto data = make_uniq<OpenDuckTableScanBindData>();
	data->endpoint = config_.endpoint;
	data->token = config_.token;
	data->database = config_.database;
	data->table_name = name;

	bind_data = std::move(data);

	TableFunction scan("openduck_table_scan", {}, OpenDuckTableScanFunc, nullptr);
	scan.init_global = OpenDuckTableScanInitGlobal;
	return scan;
}

TableStorageInfo OpenDuckTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	return info;
}

} // namespace openduck
