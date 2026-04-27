#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "openduck_catalog.hpp"
#include "openduck_has_nonremote_reference.hpp"
#include "openduck_scan_bind.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <functional>
#include <memory>

using namespace duckdb;
using openduck::HasNonRemoteReference;
using openduck::OpenDuckCatalog;

// ── fixture ────────────────────────────────────────────────────────────────

namespace {

struct Fixture {
	DuckDB db;
	Connection con;
	Fixture() : db(nullptr), con(db) {
		con.Query("CREATE TABLE local_tbl (id INTEGER)");
	}
};

static bool any_get(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return true;
	}
	for (auto &ch : op.children) {
		if (ch && any_get(*ch)) {
			return true;
		}
	}
	return false;
}

} // namespace

// ── tests ──────────────────────────────────────────────────────────────────
//
// `HasNonRemoteReference` makes two decisions per LogicalGet:
//
//   1. If `GetTable()` is non-null, the entry is "remote" iff it lives
//      in `remote_catalog`. We can't directly construct a sibling
//      `OpenDuckCatalog` without a full gRPC stack + AttachedDatabase,
//      so we cover this case indirectly through the cross-catalog
//      branch of the full system via the Rust e2e tests.
//
//   2. If `GetTable()` is null (table-function scan), the function
//      unconditionally returns true — we cover this directly.
//
// We also cover the walker: a plan with no LogicalGet anywhere must
// short-circuit to `false`.

TEST_CASE("Table-function LogicalGet forces non-remote (always true)",
          "[has_nonremote_reference][table_function]") {
	Fixture fx;
	auto plan = fx.con.ExtractPlan("SELECT * FROM range(10)");
	REQUIRE(plan != nullptr);

	std::function<LogicalGet *(LogicalOperator &)> find_get;
	find_get = [&](LogicalOperator &op) -> LogicalGet * {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			return &op.Cast<LogicalGet>();
		}
		for (auto &ch : op.children) {
			if (ch) {
				if (auto *g = find_get(*ch)) {
					return g;
				}
			}
		}
		return nullptr;
	};
	auto *get = find_get(*plan);
	REQUIRE(get != nullptr);
	// Table-function scans have no TableCatalogEntry attached.
	REQUIRE(get->GetTable() == nullptr);

	// Any catalog pointer is acceptable here — table-function Get
	// returns true regardless. Use the address of the DuckDB instance's
	// own Catalog just so we have a valid, aligned one.
	auto *any_catalog = reinterpret_cast<OpenDuckCatalog *>(fx.db.instance.get());
	REQUIRE(HasNonRemoteReference(*plan, *any_catalog) == true);
}

TEST_CASE("Bound LogicalGet on a local table is flagged as non-remote",
          "[has_nonremote_reference][local_table]") {
	Fixture fx;
	auto plan = fx.con.ExtractPlan("SELECT * FROM local_tbl");
	REQUIRE(plan != nullptr);

	std::function<LogicalGet *(LogicalOperator &)> find_get;
	find_get = [&](LogicalOperator &op) -> LogicalGet * {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			return &op.Cast<LogicalGet>();
		}
		for (auto &ch : op.children) {
			if (ch) {
				if (auto *g = find_get(*ch)) {
					return g;
				}
			}
		}
		return nullptr;
	};
	auto *get = find_get(*plan);
	REQUIRE(get != nullptr);

	auto table_ptr = get->GetTable();
	REQUIRE(table_ptr != nullptr);
	// `local_tbl` lives in the default DuckDB catalog. A *different*
	// Catalog reference — here, the DatabaseInstance pointer — is not
	// the same Catalog, so the function must report non-remote.
	auto *other_catalog =
	    reinterpret_cast<OpenDuckCatalog *>(fx.db.instance.get());
	REQUIRE(HasNonRemoteReference(*plan, *other_catalog) == true);
}

TEST_CASE("Walker recurses through children and finds the LogicalGet",
          "[has_nonremote_reference][walker]") {
	Fixture fx;
	// `SELECT id FROM local_tbl` yields a projection over a Get in
	// this DuckDB version, exercising `Walk`'s recursion into
	// `op.children` rather than the top-level Get shortcut.
	auto plan = fx.con.ExtractPlan("SELECT id FROM local_tbl");
	REQUIRE(plan != nullptr);
	REQUIRE(any_get(*plan));

	// Regardless of the wrapping operators, the walker must reach the
	// LogicalGet and flag it (non-remote).
	auto *other_catalog = reinterpret_cast<OpenDuckCatalog *>(fx.db.instance.get());
	REQUIRE(HasNonRemoteReference(*plan, *other_catalog) == true);
}

// ── openduck_table_scan fast path (regression for cross-catalog DML routing) ──
//
// Background: by the time the catalog's `PlanInsert / PlanDelete /
// PlanUpdate` hooks run, the source plan has been physically planned
// and the surviving `LogicalGet` node may have its `bind_data` reset
// to null. The original code's conservative `if (!get.bind_data)
// return true;` then forced every mutation through the cross-catalog
// ingest path, which threw the misleading "no non-remote tables were
// found in the re-parsed statement" error for pure-remote
// UPDATE/DELETE.
//
// The fix is a function-name fast path: a `LogicalGet` whose
// `function.name == kOpenDuckTableScanName` is, by construction, a
// scan of an OpenDuck-attached table — REMOTE — regardless of
// `bind_data` state. These tests pin that contract.

namespace {
LogicalGet *find_get(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return &op.Cast<LogicalGet>();
	}
	for (auto &ch : op.children) {
		if (ch) {
			if (auto *g = find_get(*ch)) {
				return g;
			}
		}
	}
	return nullptr;
}
} // namespace

TEST_CASE("openduck_table_scan GET with null bind_data is REMOTE (function-name fast path)",
          "[has_nonremote_reference][openduck_table_scan][fast_path]") {
	using openduck::kOpenDuckTableScanName;

	Fixture fx;
	auto plan = fx.con.ExtractPlan("SELECT * FROM local_tbl");
	REQUIRE(plan != nullptr);
	auto *get = find_get(*plan);
	REQUIRE(get != nullptr);

	// Synthesize the post-physical-planning shape: function name is
	// our scan's, but bind_data has already been moved out.
	get->function.name = kOpenDuckTableScanName;
	get->bind_data.reset();

	// Any catalog pointer is fine here; the fast path doesn't deref it.
	auto *any_catalog =
	    reinterpret_cast<OpenDuckCatalog *>(fx.db.instance.get());
	// Without the fast path this would return TRUE (forcing the
	// cross-catalog ingest detour). With it, it must be FALSE so
	// the catalog dispatch falls through to BuildMutate.
	REQUIRE(HasNonRemoteReference(*plan, *any_catalog) == false);
}

TEST_CASE("openduck_table_scan GET with bind_data set is also REMOTE",
          "[has_nonremote_reference][openduck_table_scan][fast_path]") {
	using openduck::kOpenDuckTableScanName;

	Fixture fx;
	auto plan = fx.con.ExtractPlan("SELECT * FROM local_tbl");
	REQUIRE(plan != nullptr);
	auto *get = find_get(*plan);
	REQUIRE(get != nullptr);

	// Function name is the fast-path trigger — bind_data state should
	// not change the answer. (This guards against a regression where
	// someone moves the bind_data check above the fast path.)
	get->function.name = kOpenDuckTableScanName;

	auto *any_catalog =
	    reinterpret_cast<OpenDuckCatalog *>(fx.db.instance.get());
	REQUIRE(HasNonRemoteReference(*plan, *any_catalog) == false);
}

TEST_CASE("Non-openduck GET with null bind_data falls back to non-remote (preserves old guard)",
          "[has_nonremote_reference][fallback]") {
	Fixture fx;
	auto plan = fx.con.ExtractPlan("SELECT * FROM local_tbl");
	REQUIRE(plan != nullptr);
	auto *get = find_get(*plan);
	REQUIRE(get != nullptr);

	// NOT openduck_table_scan, AND bind_data is gone. The fast path
	// doesn't fire, the bind_data branch doesn't fire — we expect the
	// conservative "treat as non-remote" fallback to kick in (matching
	// the pre-fix behaviour for foreign / unknown GETs).
	get->function.name = "some_other_unknown_scan";
	get->bind_data.reset();

	auto *any_catalog =
	    reinterpret_cast<OpenDuckCatalog *>(fx.db.instance.get());
	REQUIRE(HasNonRemoteReference(*plan, *any_catalog) == true);
}
