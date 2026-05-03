#include "openduck_table_entry.hpp"
#include "openduck_scan_bind.hpp"
#include "grpc_client.hpp"
#include "arrow_bridge.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include <deque>

namespace openduck {

using namespace duckdb;

// ── Scan bind/execute data ──────────────────────────────────────────────────
//
// `OpenDuckTableScanBindData` lives in `openduck_scan_bind.hpp` so the
// optimizer extension can mutate it without friendship.

static duckdb::BindInfo OpenDuckTableScanGetBindInfo(
    const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
	if (!bind_data) {
		return duckdb::BindInfo(duckdb::ScanType::TABLE);
	}
	// The bind_data is a non-const pointer per the callback signature,
	// so we can reach the stored `optional_ptr<TableCatalogEntry>` and
	// hand it back to DuckDB's binder (which expects a non-const
	// reference).
	auto &data = bind_data->CastNoConst<OpenDuckTableScanBindData>();
	if (!data.table_entry) {
		return duckdb::BindInfo(duckdb::ScanType::TABLE);
	}
	return duckdb::BindInfo(*data.table_entry);
}

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

	// `limit_clause` is populated by `OpenDuckOptimizer` when a constant
	// LIMIT/OFFSET sits directly over this scan; empty otherwise.
	auto sql = "SELECT " + cols + " FROM " + bind_data.table_name + bind_data.limit_clause;
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
	data->table_entry = this;

	for (auto &col : columns.Logical()) {
		data->all_column_names.push_back(col.Name());
	}

	bind_data = std::move(data);

	TableFunction scan(kOpenDuckTableScanName, {}, OpenDuckTableScanFunc, nullptr);
	scan.init_global = OpenDuckTableScanInitGlobal;
	scan.projection_pushdown = true;
	// Wire `get_bind_info` so DuckDB's binder sees this scan as a
	// base table — enables DELETE / UPDATE on r.main.t and also lets
	// `HasNonRemoteReference` walk the plan without tripping over a
	// null TableCatalogEntry (it's us, and we live in the remote
	// catalog, so it's not "non-remote").
	scan.get_bind_info = OpenDuckTableScanGetBindInfo;
	return scan;
}

TableStorageInfo OpenDuckTableEntry::GetStorageInfo(ClientContext &context) {
	// Synthesize `IndexInfo` entries from the table's constraint list
	// so DuckDB's binder can resolve `INSERT ... ON CONFLICT` targets
	// without an explicit conflict-column list. The binder uses
	// `storage_info.index_info` (not `GetConstraints()`) — see
	// `Binder::GenerateMergeInto` in `bind_insert.cpp`. We map
	// PRIMARY KEY / UNIQUE constraints onto synthetic indexes; the
	// real index storage lives on the worker, but the binder only
	// needs the column-set + is_unique flag at bind time.
	TableStorageInfo info;
	for (auto &constraint : GetConstraints()) {
		if (constraint->type != duckdb::ConstraintType::UNIQUE) {
			continue;
		}
		auto &uc = constraint->Cast<duckdb::UniqueConstraint>();
		auto col_names = uc.GetColumnNames();
		duckdb::IndexInfo idx;
		idx.is_unique = true;
		idx.is_primary = uc.IsPrimaryKey();
		idx.is_foreign = false;
		// Resolve column NAMES → physical column indexes via the
		// table's columns list.
		idx_t i = 0;
		for (auto &col : columns.Logical()) {
			for (auto &name : col_names) {
				if (duckdb::StringUtil::CIEquals(col.Name(), name)) {
					idx.column_set.insert(static_cast<duckdb::column_t>(i));
				}
			}
			i++;
		}
		info.index_info.push_back(std::move(idx));
	}
	return info;
}

} // namespace openduck
