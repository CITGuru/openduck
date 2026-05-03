#include "openduck_has_nonremote_reference.hpp"
#include "openduck_catalog.hpp"
#include "openduck_scan_bind.hpp"

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <cstring>

namespace openduck {

using namespace duckdb;

namespace {

// Returns true if `entry` lives in a catalog other than `remote_catalog`.
bool IsForeignEntry(const TableCatalogEntry &entry, OpenDuckCatalog &remote_catalog) {
	const Catalog &parent = entry.ParentCatalog();
	// Catalog addresses are unique per attached database — comparing
	// raw `this` pointers is the cheapest reliable identity check.
	return &parent != static_cast<const Catalog *>(&remote_catalog);
}

bool IsNonRemoteLogicalGet(const LogicalGet &get, OpenDuckCatalog &remote_catalog) {
	// FIRST: function-name fast path. By the time the catalog's
	// `PlanInsert / PlanDelete / PlanUpdate` runs, the LogicalGet's
	// source has already been physically planned, which can null out
	// `bind_data` on the still-attached logical node. The function
	// name on the GET, however, is preserved — and a GET whose
	// function is `openduck_table_scan` is, by construction, a scan
	// of an OpenDuck-attached table, i.e. always remote.
	//
	// We can't tell which OpenDuck catalog it belongs to without
	// `bind_data`, but for the cross-catalog detector this is fine:
	// "remote scan against SOME OpenDuck catalog" is the answer the
	// caller needs to decide between BuildMutate (forward-only) and
	// BuildCrossCatalog* (local-staging path). A scan of `cloud2.t`
	// inside a mutation against `cloud1.t` would still be flagged as
	// non-remote-with-respect-to-cloud1 by the table_entry branch
	// below — but that branch only fires when bind_data is populated,
	// which is the only time we can prove cross-OpenDuck-catalog
	// references anyway.
	if (std::strcmp(get.function.name.c_str(), kOpenDuckTableScanName) == 0) {
		return false;
	}

	// SECOND: bind_data-backed lookup. When present, we can prove
	// catalog identity by walking GetTable() → ParentCatalog().
	if (get.bind_data) {
		if (auto table_ptr = get.GetTable()) {
			return IsForeignEntry(*table_ptr, remote_catalog);
		}
		// bind_data set but no TableCatalogEntry → table-function GET
		// (read_csv, read_parquet, generate_series, user table function,
		// STDIN reader, …). All of those execute locally regardless of
		// where their output feeds, so they must force the cross-catalog
		// path.
		return true;
	}

	// THIRD: fallback for a non-OpenDuck function GET with null bind_data.
	// This is rare (the planner usually populates bind_data before
	// dispatch), but when it happens we can't tell the entry's catalog,
	// so we conservatively treat it as non-remote — same behavior the
	// original guard had.
	return true;
}

// Recursive walk. Stops early on the first hit.
bool Walk(LogicalOperator &op, OpenDuckCatalog &remote_catalog) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (IsNonRemoteLogicalGet(get, remote_catalog)) {
			return true;
		}
	}
	// NOTE: local user-defined scalar/aggregate/window functions
	// embedded inside `op.expressions` are not walked in v1. They are
	// rare in practice (the public API route for remote UDFs is
	// `openduck_remote('CREATE FUNCTION ...')`), and if one does slip
	// through, the worker simply reports an unresolved function and
	// the client surfaces a typed `CATALOG` / `BINDER` error. The v2
	// extension would run `ExpressionIterator` over `op.expressions`
	// here.
	for (auto &child : op.children) {
		if (child && Walk(*child, remote_catalog)) {
			return true;
		}
	}
	return false;
}

} // namespace

bool HasNonRemoteReference(LogicalOperator &op, OpenDuckCatalog &remote_catalog) {
	return Walk(op, remote_catalog);
}

} // namespace openduck
