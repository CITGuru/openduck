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
	vector<string> all_column_names;
};

struct OpenDuckTableScanGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<GrpcClient> client;
	std::unique_ptr<GrpcStream> stream;
	std::deque<std::shared_ptr<arrow::RecordBatch>> pending_batches;
	std::string execution_id;
	bool done = false;
	std::mutex lock;

	// Maps output chunk column index → Arrow batch column index.
	// -1 means the column is a virtual row_id (fill with 0).
	vector<int> chunk_to_batch;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<GlobalTableFunctionState> OpenDuckTableScanInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<OpenDuckTableScanBindData>();
	auto state = make_uniq<OpenDuckTableScanGlobalState>();
	state->client = std::make_unique<GrpcClient>(bind_data.endpoint);

	string cols;
	int batch_col_idx = 0;
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		auto col_id = input.column_ids[i];
		if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
			state->chunk_to_batch.push_back(-1);
			continue;
		}
		if (!cols.empty()) {
			cols += ", ";
		}
		cols += "\"" + bind_data.all_column_names[col_id] + "\"";
		state->chunk_to_batch.push_back(batch_col_idx++);
	}
	if (cols.empty()) {
		cols = "1";
	}

	auto sql = "SELECT " + cols + " FROM " + bind_data.table_name;
	state->stream = state->client->ExecuteSQL(sql, bind_data.database, bind_data.token);
	state->execution_id = state->stream->ExecutionId();
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

	auto rows = CopyBatchToDataChunkProjected(batch, output, gstate.chunk_to_batch);
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

	for (auto &col : columns.Logical()) {
		data->all_column_names.push_back(col.Name());
	}

	bind_data = std::move(data);

	TableFunction scan("openduck_table_scan", {}, OpenDuckTableScanFunc, nullptr);
	scan.init_global = OpenDuckTableScanInitGlobal;
	scan.projection_pushdown = true;
	return scan;
}

TableStorageInfo OpenDuckTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	return info;
}

} // namespace openduck
