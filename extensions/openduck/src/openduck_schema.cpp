#include "openduck_schema.hpp"
#include "openduck_catalog.hpp"
#include "openduck_errors.hpp"
#include "openduck_sql_emit.hpp"
#include "openduck_table_entry.hpp"
#include "grpc_client.hpp"
#include "arrow_bridge.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <algorithm>
#include <deque>
#include <sstream>
#include <unordered_map>

#include <arrow/api.h>

namespace openduck {

using namespace duckdb;

OpenDuckSchemaEntry::OpenDuckSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {
}

OpenDuckCatalog &OpenDuckSchemaEntry::GetOpenDuckCatalog() {
	return catalog.Cast<OpenDuckCatalog>();
}

bool OpenDuckSchemaEntry::HasCachedTable(const std::string &table_name) const {
	return cached_tables_.find(table_name) != cached_tables_.end();
}

namespace {

/// Map a column NAME in `info` to its `LogicalIndex`. Case-insensitive,
/// matching DuckDB's identifier semantics.
LogicalIndex FindColumnIndex(const CreateTableInfo &info, const std::string &col_name) {
	idx_t i = 0;
	for (auto &col : info.columns.Logical()) {
		if (StringUtil::CIEquals(col.Name(), col_name)) {
			return LogicalIndex(i);
		}
		i++;
	}
	return LogicalIndex(DConstants::INVALID_INDEX);
}

} // namespace

void OpenDuckSchemaEntry::FetchAndAttachConstraints(GrpcClient &client,
                                                     const AttachConfig &config,
                                                     const std::string &table_name,
                                                     CreateTableInfo &info) {
	// Probe DuckDB's `duckdb_constraints()` system function for the
	// table. Each row in the result is one constraint: PRIMARY KEY /
	// UNIQUE / NOT NULL / FOREIGN KEY / CHECK. We materialize PRIMARY
	// KEY / UNIQUE / NOT NULL onto the local CreateTableInfo so the
	// client-side binder sees the same constraint shape the worker
	// enforces. CHECK / FOREIGN KEY are intentionally skipped — they
	// don't affect bind-time correctness for the supported mutation
	// shapes and they're harder to round-trip cleanly (CHECK
	// expressions need re-binding).
	std::ostringstream sql;
	sql << "SELECT constraint_type, constraint_column_names FROM duckdb_constraints() "
	    << "WHERE schema_name = " << QuoteLiteral(name) << " AND table_name = "
	    << QuoteLiteral(table_name);

	std::deque<std::shared_ptr<arrow::RecordBatch>> batches;
	try {
		auto stream = client.ExecuteSQL(sql.str(), config.database, config.token);
		while (true) {
			auto ipc = stream->Next();
			if (!ipc) {
				break;
			}
			ReadAllIpcBatches(*ipc, batches);
		}
	} catch (...) {
		// Best-effort: if the worker is too old to expose
		// `duckdb_constraints()` or the probe fails for any reason,
		// silently leave the table constraint-less. The worker still
		// enforces every constraint at apply time; the only loss is
		// client-side `ON CONFLICT` resolution.
		return;
	}

	for (auto &batch : batches) {
		if (!batch || batch->num_rows() == 0) {
			continue;
		}
		auto type_arr = batch->column(0); // VARCHAR
		auto cols_arr = batch->column(1); // LIST<VARCHAR>

		auto *type_strings = dynamic_cast<arrow::StringArray *>(type_arr.get());
		auto *cols_list = dynamic_cast<arrow::ListArray *>(cols_arr.get());
		if (!type_strings || !cols_list) {
			continue;
		}
		auto values = std::dynamic_pointer_cast<arrow::StringArray>(cols_list->values());
		if (!values) {
			continue;
		}

		for (int64_t r = 0; r < batch->num_rows(); r++) {
			if (type_arr->IsNull(r) || cols_list->IsNull(r)) {
				continue;
			}
			std::string ctype = type_strings->GetString(r);
			auto offset = cols_list->value_offset(r);
			auto length = cols_list->value_length(r);
			vector<string> cols;
			cols.reserve(length);
			for (int64_t i = 0; i < length; i++) {
				cols.push_back(values->GetString(offset + i));
			}
			if (cols.empty()) {
				continue;
			}

			if (ctype == "PRIMARY KEY") {
				info.constraints.push_back(make_uniq<UniqueConstraint>(cols, true));
			} else if (ctype == "UNIQUE") {
				info.constraints.push_back(make_uniq<UniqueConstraint>(cols, false));
			} else if (ctype == "NOT NULL") {
				// `NOT NULL` rows in `duckdb_constraints()` carry
				// exactly one column. We map it back to the
				// `LogicalIndex` of the matching column in `info`
				// and emit the constraint.
				auto idx = FindColumnIndex(info, cols.front());
				if (idx.index != DConstants::INVALID_INDEX) {
					info.constraints.push_back(make_uniq<NotNullConstraint>(idx));
				}
			}
			// CHECK / FOREIGN KEY: skipped (see header comment).
		}
	}
}

namespace {

// Per-table reflection row, accumulated while walking the batched
// (columns ∪ constraints) result.
struct TableReflection {
	// Preserves column_index order; entries may appear out of order in
	// the Arrow stream so we keep (column_index, name, type_sql) tuples
	// and sort at emit time.
	struct Column {
		int64_t column_index;
		std::string name;
		std::string type_sql;
	};
	struct Constraint {
		std::string type; // "PRIMARY KEY" | "UNIQUE" | "NOT NULL"
		std::vector<std::string> columns;
	};
	std::vector<Column> columns;
	std::vector<Constraint> constraints;
};

} // namespace

void OpenDuckSchemaEntry::EnsureSchemaLoaded(duckdb::ClientContext &context,
                                              GrpcClient &client,
                                              const AttachConfig &config) {
	if (schema_loaded_) {
		return;
	}

	// ONE batched remote query that UNIONs columns and constraints for
	// every table in this schema. `kind` distinguishes the two row
	// shapes; trailing ORDER BY guarantees we can walk column rows for
	// a table in column_index order before seeing its constraint rows.
	//
	// NULL-literals on the non-matching side of each UNION arm are
	// typed to match the other side so DuckDB's UNION type-resolver
	// doesn't complain: `column_index` as BIGINT (constraints emit
	// 9223372036854775807 as a "columns-last" sentinel), `data_type`
	// as VARCHAR, `constraint_column_names` as VARCHAR[].
	std::ostringstream sql;
	sql << "SELECT 'c' AS kind, table_name, "
	    << "CAST(column_index AS BIGINT) AS sort_idx, "
	    << "column_name, data_type, "
	    << "CAST(NULL AS VARCHAR) AS constraint_type, "
	    << "CAST(NULL AS VARCHAR[]) AS constraint_cols "
	    << "FROM duckdb_columns() "
	    << "WHERE schema_name = " << QuoteLiteral(name) << " AND internal = false "
	    << "UNION ALL "
	    << "SELECT 'k' AS kind, table_name, "
	    << "9223372036854775807 AS sort_idx, "
	    << "CAST(NULL AS VARCHAR) AS column_name, "
	    << "CAST(NULL AS VARCHAR) AS data_type, "
	    << "constraint_type, constraint_column_names "
	    << "FROM duckdb_constraints() "
	    << "WHERE schema_name = " << QuoteLiteral(name)
	    << " ORDER BY table_name, kind, sort_idx";

	std::deque<std::shared_ptr<arrow::RecordBatch>> batches;
	try {
		auto stream = client.ExecuteSQL(sql.str(), config.database, config.token);
		while (true) {
			auto ipc = stream->Next();
			if (!ipc) {
				break;
			}
			ReadAllIpcBatches(*ipc, batches);
		}
	} catch (...) {
		// Batched reflection failed — leave `schema_loaded_ = false`
		// so LookupEntry falls back to its per-table path. The worker
		// still works; we just don't get the N-to-1 round-trip win.
		return;
	}

	std::unordered_map<std::string, TableReflection> tables;

	for (auto &batch : batches) {
		if (!batch || batch->num_rows() == 0) {
			continue;
		}
		auto kind_arr = dynamic_cast<arrow::StringArray *>(batch->column(0).get());
		auto table_arr = dynamic_cast<arrow::StringArray *>(batch->column(1).get());
		auto sort_arr = dynamic_cast<arrow::Int64Array *>(batch->column(2).get());
		auto col_name_arr = dynamic_cast<arrow::StringArray *>(batch->column(3).get());
		auto data_type_arr = dynamic_cast<arrow::StringArray *>(batch->column(4).get());
		auto cons_type_arr = dynamic_cast<arrow::StringArray *>(batch->column(5).get());
		auto cons_cols_arr = dynamic_cast<arrow::ListArray *>(batch->column(6).get());
		if (!kind_arr || !table_arr || !sort_arr || !col_name_arr || !data_type_arr ||
		    !cons_type_arr || !cons_cols_arr) {
			// Unexpected schema; bail on the whole batch rather than
			// partially populating.
			return;
		}
		auto cons_values = std::dynamic_pointer_cast<arrow::StringArray>(cons_cols_arr->values());

		for (int64_t r = 0; r < batch->num_rows(); r++) {
			if (kind_arr->IsNull(r) || table_arr->IsNull(r)) {
				continue;
			}
			std::string kind = kind_arr->GetString(r);
			std::string tbl = table_arr->GetString(r);
			auto &ref = tables[tbl];

			if (kind == "c") {
				if (col_name_arr->IsNull(r) || data_type_arr->IsNull(r)) {
					continue;
				}
				TableReflection::Column c;
				c.column_index = sort_arr->IsNull(r) ? 0 : sort_arr->Value(r);
				c.name = col_name_arr->GetString(r);
				c.type_sql = data_type_arr->GetString(r);
				ref.columns.push_back(std::move(c));
			} else if (kind == "k") {
				if (cons_type_arr->IsNull(r) || cons_cols_arr->IsNull(r) || !cons_values) {
					continue;
				}
				TableReflection::Constraint k;
				k.type = cons_type_arr->GetString(r);
				auto offset = cons_cols_arr->value_offset(r);
				auto length = cons_cols_arr->value_length(r);
				k.columns.reserve(length);
				for (int64_t i = 0; i < length; i++) {
					k.columns.push_back(cons_values->GetString(offset + i));
				}
				ref.constraints.push_back(std::move(k));
			}
		}
	}

	// Resolve every distinct `data_type` string into a concrete
	// `LogicalType` via ONE local round-trip:
	//
	//     SELECT CAST(NULL AS <type1>) AS c0, CAST(NULL AS <type2>) AS c1, ...
	//
	// The client's own DuckDB binder runs the type resolution and we
	// read back the result schema. This is the simplest workaround
	// for `Parser::ParseColumnDefinition` returning UNBOUND types
	// (it's a parser, not a binder) and avoids needing private
	// access to `Binder::BindLogicalType`.
	std::unordered_map<std::string, LogicalType> type_map;
	{
		std::vector<std::string> distinct_types;
		std::unordered_map<std::string, idx_t> seen;
		for (auto &entry : tables) {
			for (auto &col : entry.second.columns) {
				if (seen.find(col.type_sql) == seen.end()) {
					seen[col.type_sql] = distinct_types.size();
					distinct_types.push_back(col.type_sql);
				}
			}
		}
		if (!distinct_types.empty()) {
			std::ostringstream cast_sql;
			cast_sql << "SELECT ";
			for (size_t i = 0; i < distinct_types.size(); i++) {
				if (i > 0) {
					cast_sql << ", ";
				}
				// `CAST(NULL AS <T>)` resolves <T> against the
				// client's catalog; failure throws BinderException
				// which we catch below and silently fall back.
				cast_sql << "CAST(NULL AS " << distinct_types[i] << ")";
			}
			try {
				duckdb::Connection con(*context.db);
				auto result = con.Query(cast_sql.str());
				if (result->HasError() || result->ColumnCount() != distinct_types.size()) {
					return;
				}
				for (size_t i = 0; i < distinct_types.size(); i++) {
					type_map.emplace(distinct_types[i], result->types[i]);
				}
			} catch (...) {
				return;
			}
		}
	}

	// Materialize every reflected table into a cached OpenDuckTableEntry.
	// If any single table fails to resolve (e.g., a type our local
	// catalog doesn't know about), we skip JUST that table — LookupEntry
	// will fall back to the per-table path for it next time it's asked.
	for (auto &entry : tables) {
		const std::string &tbl_name = entry.first;
		auto &ref = entry.second;
		if (ref.columns.empty()) {
			continue;
		}

		// Columns may arrive out of order across batches; sort by
		// column_index so the CreateTableInfo matches the worker's
		// physical layout.
		std::sort(ref.columns.begin(), ref.columns.end(),
		          [](const TableReflection::Column &a, const TableReflection::Column &b) {
			          return a.column_index < b.column_index;
		          });

		auto info = make_uniq<CreateTableInfo>();
		info->schema = name;
		info->table = tbl_name;

		bool parse_ok = true;
		for (auto &col : ref.columns) {
			auto it = type_map.find(col.type_sql);
			if (it == type_map.end() || it->second.id() == LogicalTypeId::UNKNOWN ||
			    it->second.id() == LogicalTypeId::INVALID ||
			    it->second.id() == LogicalTypeId::UNBOUND) {
				parse_ok = false;
				break;
			}
			info->columns.AddColumn(ColumnDefinition(col.name, it->second));
		}
		if (!parse_ok) {
			continue;
		}

		for (auto &k : ref.constraints) {
			if (k.columns.empty()) {
				continue;
			}
			// UniqueConstraint expects DuckDB's `vector<string>`;
			// convert from our std::vector<std::string>.
			vector<string> cols(k.columns.begin(), k.columns.end());
			if (k.type == "PRIMARY KEY") {
				info->constraints.push_back(make_uniq<UniqueConstraint>(std::move(cols), true));
			} else if (k.type == "UNIQUE") {
				info->constraints.push_back(make_uniq<UniqueConstraint>(std::move(cols), false));
			} else if (k.type == "NOT NULL") {
				auto idx = FindColumnIndex(*info, k.columns.front());
				if (idx.index != DConstants::INVALID_INDEX) {
					info->constraints.push_back(make_uniq<NotNullConstraint>(idx));
				}
			}
			// CHECK / FOREIGN KEY intentionally skipped, see the
			// FetchAndAttachConstraints header comment.
		}

		auto table_entry = make_uniq<OpenDuckTableEntry>(catalog, *this, *info, config);
		cached_tables_[tbl_name] = std::move(table_entry);
	}

	schema_loaded_ = true;
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                             const EntryLookupInfo &lookup_info) {
	auto entry_type = lookup_info.GetCatalogType();
	if (entry_type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}

	auto table_name = lookup_info.GetEntryName();

	auto it = cached_tables_.find(table_name);
	if (it != cached_tables_.end()) {
		return it->second.get();
	}

	auto &od_catalog = GetOpenDuckCatalog();
	auto &config = od_catalog.GetConfig();
	auto &client = od_catalog.GetClient();

	// First try the batched schema reflection. On cold lookups against
	// any schema with >1 table this is strictly fewer round-trips
	// than the per-table path below. Requires a `ClientContext` for
	// type resolution (see `EnsureSchemaLoaded` doc); when absent
	// (programmatic-API call sites) we skip straight to the fallback.
	if (transaction.context) {
		EnsureSchemaLoaded(*transaction.context, client, config);
		it = cached_tables_.find(table_name);
		if (it != cached_tables_.end()) {
			return it->second.get();
		}
	}

	// Cache miss after a successful batched load means the table
	// doesn't exist in the reflected snapshot OR was created after the
	// snapshot was taken. Fall back to the single-table lookup so
	// newly-created tables are still visible without a full re-scan.
	auto sql = "SELECT * FROM " + table_name + " LIMIT 0";
	try {
		auto stream = client.ExecuteSQL(sql, config.database, config.token);
		auto first_ipc = stream->Next();
		if (!first_ipc) {
			// Worker stream closed without emitting a schema batch.
			// This is "table genuinely doesn't exist" for any healthy
			// worker — silently return so callers (e.g. DuckDB's
			// pre-CREATE existence probe) don't see noise.
			return nullptr;
		}

		auto schema_info = ExtractSchema(*first_ipc);

		auto info = make_uniq<CreateTableInfo>();
		info->schema = name;
		info->table = table_name;
		for (idx_t i = 0; i < schema_info.names.size(); i++) {
			info->columns.AddColumn(ColumnDefinition(schema_info.names[i], schema_info.types[i]));
		}

		// Round-trip PRIMARY KEY / UNIQUE / NOT NULL constraints from
		// the worker so the client-side binder can resolve `INSERT ...
		// ON CONFLICT` targets and reject NULL inserts the same way the
		// worker would. Without this, `ON CONFLICT DO NOTHING/UPDATE`
		// fails at bind time with "no UNIQUE/PRIMARY KEY constraints
		// that refer to this table".
		FetchAndAttachConstraints(client, config, table_name, *info);

		auto table_entry = make_uniq<OpenDuckTableEntry>(catalog, *this, *info, config);
		auto result = table_entry.get();
		cached_tables_[table_name] = std::move(table_entry);
		return result;
	} catch (const GatewayUnavailableError &e) {
		// Real connectivity failure — keep this loud; users need to
		// know the gateway isn't reachable.
		fprintf(stderr, "[openduck] LookupEntry('%s'): gateway unavailable: %s\n",
		        table_name.c_str(), e.what());
		return nullptr;
	} catch (const CatalogException &) {
		// Worker confirmed the table doesn't exist. This is the
		// EXPECTED outcome of an existence probe — DuckDB calls
		// LookupEntry before every CREATE TABLE / DROP TABLE / etc.
		// to enforce IF [NOT] EXISTS / OR REPLACE semantics. Logging
		// here would scare users into thinking a CREATE failed when in
		// fact it's just the pre-flight check returning the right
		// answer. Silently return nullptr.
		return nullptr;
	} catch (const std::exception &e) {
		// Anything else (transport error, malformed reply, auth
		// failure, …) IS unexpected — surface it.
		fprintf(stderr, "[openduck] LookupEntry('%s'): error: %s\n",
		        table_name.c_str(), e.what());
		return nullptr;
	} catch (...) {
		fprintf(stderr, "[openduck] LookupEntry('%s'): unknown error\n", table_name.c_str());
		return nullptr;
	}
}

void OpenDuckSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}

	auto &od_catalog = GetOpenDuckCatalog();
	auto &config = od_catalog.GetConfig();
	auto &client = od_catalog.GetClient();

	// Primary path (one remote query): reflect every table in this
	// schema into `cached_tables_` via `EnsureSchemaLoaded`, then emit
	// each cached entry to the callback.
	try {
		EnsureSchemaLoaded(context, client, config);
	} catch (const GatewayUnavailableError &) {
		return;
	} catch (...) {
		// EnsureSchemaLoaded already swallows most errors; this
		// catch is belt-and-braces.
	}

	if (schema_loaded_) {
		for (auto &entry : cached_tables_) {
			if (entry.second) {
				callback(*entry.second);
			}
		}
		return;
	}

	// Fallback: batched reflection unavailable (older worker?).
	// List table names via information_schema and warm each one with
	// the per-table lookup — preserves pre-change behavior end-to-end.
	try {
		auto sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'";
		auto stream = client.ExecuteSQL(sql, config.database, config.token);

		std::vector<std::string> table_names;
		std::deque<std::shared_ptr<arrow::RecordBatch>> batches;
		while (true) {
			auto ipc = stream->Next();
			if (!ipc) {
				break;
			}
			ReadAllIpcBatches(*ipc, batches);
		}

		vector<LogicalType> types = {LogicalType::VARCHAR};
		for (auto &batch : batches) {
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), types);
			auto rows = CopyBatchToDataChunk(batch, chunk);
			for (idx_t i = 0; i < rows; i++) {
				table_names.push_back(chunk.data[0].GetValue(i).GetValue<string>());
			}
		}

		for (auto &tbl_name : table_names) {
			CatalogTransaction txn(catalog, context);
			EntryLookupInfo info(CatalogType::TABLE_ENTRY, tbl_name);
			auto entry = LookupEntry(txn, info);
			if (entry) {
				callback(*entry);
			}
		}
	} catch (const GatewayUnavailableError &) {
	} catch (...) {
	}
}

void OpenDuckSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
}

// ── Helpers ────────────────────────────────────────────────────────────────

namespace {

ClientContext &RequireContext(CatalogTransaction &transaction, const char *op_name) {
	if (!transaction.context) {
		throw BinderException(
		    "%s on an ATTACH OpenDuck catalog requires an active ClientContext "
		    "(programmatic call path is not supported). Workaround: "
		    "`openduck_remote('<SQL>')`.",
		    op_name);
	}
	return *transaction.context;
}

// The `catalog` member on a SchemaCatalogEntry is a `Catalog &`; we
// cast once and reuse.
OpenDuckCatalog &AsOpenDuckCatalog(Catalog &catalog) {
	return catalog.Cast<OpenDuckCatalog>();
}

} // namespace

// ── DDL hooks ──────────────────────────────────────────────────────────────

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateIndex(CatalogTransaction transaction,
                                                             CreateIndexInfo &info,
                                                             TableCatalogEntry &table) {
	auto &od_catalog = AsOpenDuckCatalog(catalog);
	EnsureWritable(od_catalog);
	auto &context = RequireContext(transaction, "CREATE INDEX");
	ForwardDDL(context, od_catalog);
	return nullptr;
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                CreateFunctionInfo &info) {
	// Intentional product decision: client-side CREATE FUNCTION has
	// no body to ship to the worker. Users wanting a remote UDF issue
	// `openduck_remote('CREATE FUNCTION ...')`.
	throw BinderException(
	    "CREATE FUNCTION is not supported on ATTACH OpenDuck catalogs: there is "
	    "no way to ship a function body to the worker. Issue "
	    "`openduck_remote('CREATE FUNCTION ...')` to run the DDL directly on the "
	    "worker's DuckDB.");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                             BoundCreateTableInfo &info) {
	auto &od_catalog = AsOpenDuckCatalog(catalog);
	EnsureWritable(od_catalog);
	auto &context = RequireContext(transaction, "CREATE TABLE");

	// SQL-text path (the hot one): re-parse `GetCurrentQuery()` and
	// forward via the rewriter. Programmatic-API path (when the user
	// constructs a `CreateTableInfo` and runs it directly via the
	// C API rather than as parsed SQL): reconstruct CREATE TABLE SQL
	// from the bound info struct's `ToString()`, which DuckDB
	// implements for every column type / constraint shape we'd
	// realistically receive.
	const auto &original = context.GetCurrentQuery();
	if (!original.empty()) {
		ForwardDDL(context, od_catalog);
	} else if (info.base) {
		// Strip the attached-catalog qualifier (Rule T) on the info
		// struct so the worker sees `<schema>.<table>` not
		// `<our_alias>.<schema>.<table>`.
		auto &create_info = info.base->Cast<CreateTableInfo>();
		if (StringUtil::CIEquals(create_info.catalog, od_catalog.GetName())) {
			create_info.catalog.clear();
		}
		auto sql = create_info.ToString();
		auto transaction_id = EnsureAcquired(context, od_catalog);
		auto &config = od_catalog.GetConfig();
		auto &client = od_catalog.GetClient();
		auto stream =
		    client.ExecuteSQL(sql, config.database, config.token, transaction_id);
		while (stream->Next()) {
		}
	} else {
		throw BinderException(
		    "CreateTable on ATTACH OpenDuck catalog `%s` was not issued "
		    "through a SQL-text call path AND no BoundCreateTableInfo was "
		    "provided. Workaround: `openduck_remote('CREATE TABLE ...')`.",
		    od_catalog.GetName());
	}

	// Evict any stale cache entry for this table so the next scan
	// re-probes and picks up the worker-side column definitions.
	// Also reset the schema-load flag so the next `Scan` (or cold
	// `LookupEntry`) re-reflects the schema in one batched query
	// and picks up this new table.
	if (info.base) {
		auto &create_info = info.base->Cast<CreateTableInfo>();
		cached_tables_.erase(create_info.table);
	}
	schema_loaded_ = false;
	return nullptr;
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateView(CatalogTransaction transaction,
                                                            CreateViewInfo &info) {
	auto &od_catalog = AsOpenDuckCatalog(catalog);
	EnsureWritable(od_catalog);
	auto &context = RequireContext(transaction, "CREATE VIEW");
	ForwardDDL(context, od_catalog);
	cached_tables_.erase(info.view_name);
	schema_loaded_ = false;
	return nullptr;
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                CreateSequenceInfo &info) {
	auto &od_catalog = AsOpenDuckCatalog(catalog);
	EnsureWritable(od_catalog);
	auto &context = RequireContext(transaction, "CREATE SEQUENCE");
	ForwardDDL(context, od_catalog);
	return nullptr;
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                     CreateTableFunctionInfo &info) {
	throw BinderException(
	    "CREATE TABLE FUNCTION is not supported on ATTACH OpenDuck catalogs: "
	    "table functions are local to a DuckDB process.");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                    CreateCopyFunctionInfo &info) {
	throw BinderException(
	    "CREATE COPY FUNCTION is not supported on ATTACH OpenDuck catalogs: "
	    "copy formats are registered per DuckDB process.");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                      CreatePragmaFunctionInfo &info) {
	throw BinderException(
	    "CREATE PRAGMA FUNCTION is not supported on ATTACH OpenDuck catalogs: "
	    "pragmas are a local-only DuckDB concept.");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                 CreateCollationInfo &info) {
	throw BinderException(
	    "CREATE COLLATION is not supported on ATTACH OpenDuck catalogs: not "
	    "meaningful over a SQL-forwarding proxy.");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateType(CatalogTransaction transaction,
                                                            CreateTypeInfo &info) {
	auto &od_catalog = AsOpenDuckCatalog(catalog);
	EnsureWritable(od_catalog);
	auto &context = RequireContext(transaction, "CREATE TYPE");
	ForwardDDL(context, od_catalog);
	return nullptr;
}

void OpenDuckSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	auto &od_catalog = AsOpenDuckCatalog(catalog);
	EnsureWritable(od_catalog);
	ForwardDDL(context, od_catalog);
	if (info.type == CatalogType::TABLE_ENTRY || info.type == CatalogType::VIEW_ENTRY) {
		cached_tables_.erase(info.name);
		schema_loaded_ = false;
	}
}

void OpenDuckSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	auto &od_catalog = AsOpenDuckCatalog(catalog);
	EnsureWritable(od_catalog);
	auto &context = RequireContext(transaction, "ALTER");
	ForwardDDL(context, od_catalog);
	// Invalidate the target table/view so subsequent reads re-probe.
	// Reset schema_loaded_ too: ALTER RENAME changes the table name,
	// ALTER ADD/DROP COLUMN changes the column set, both of which
	// affect the batched reflection snapshot.
	cached_tables_.erase(info.name);
	schema_loaded_ = false;
}

} // namespace openduck
