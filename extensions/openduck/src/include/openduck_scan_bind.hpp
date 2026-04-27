#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/function/table_function.hpp"

#include <string>
#include <vector>

namespace openduck {

/// Name of the TableFunction that backs a scan of an OpenDuck-attached
/// table. Exported for the `OpenDuckOptimizer` and any other consumer
/// that needs to identify our scans in a `LogicalGet`.
inline constexpr const char *kOpenDuckTableScanName = "openduck_table_scan";

/// Bind data attached to every `LogicalGet` that scans an attached
/// OpenDuck table. Held by DuckDB as a `unique_ptr<FunctionData>`;
/// kept in a shared header so `OpenDuckOptimizer` can mutate it
/// (e.g., stash a pushed-down LIMIT clause) without needing friendship
/// with `openduck_table_entry.cpp`.
struct OpenDuckTableScanBindData : public duckdb::TableFunctionData {
	duckdb::string endpoint;
	duckdb::string token;
	duckdb::string database;
	duckdb::string table_name;
	duckdb::vector<duckdb::string> all_column_names;
	/// Back-pointer to the catalog entry the scan was created from.
	/// DuckDB's binder uses `LogicalGet::GetTable()` (which calls
	/// `function.get_bind_info(bind_data).table`) to recognize a scan
	/// as a "base table" for DELETE / UPDATE planning.
	duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;

	/// Optional SQL fragment appended verbatim to the generated worker
	/// query after the FROM clause. Populated by `OpenDuckOptimizer`
	/// when a constant LIMIT / OFFSET sits over this scan. Leading
	/// whitespace is included by the setter; empty string means no
	/// pushdown.
	duckdb::string limit_clause;
};

} // namespace openduck
