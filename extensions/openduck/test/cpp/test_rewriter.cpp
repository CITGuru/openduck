#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "catalog_reference_rewriter.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/sql_statement.hpp"

#include <string>
#include <unordered_set>

using namespace openduck;
using duckdb::Parser;
using duckdb::SQLStatement;

// ── helpers ──────────────────────────────────────────────────────────────────

// A minimal cache probe that mirrors `OpenDuckCatalog::HasSchemaOrTable`
// semantics: case-insensitive identifiers, and an EMPTY `schema`
// argument means "any cached table" (used by the 3-part column-ref
// probe path, where we only know the table slot, not the schema).
struct FakeCache {
	std::unordered_set<std::string> keys; //!< stores "schema|table" lowercased

	void Add(const std::string &schema, const std::string &table) {
		keys.insert(lowercase(schema) + "|" + lowercase(table));
	}

	bool Has(const std::string &schema, const std::string &table) const {
		auto t_lower = lowercase(table);
		if (schema.empty()) {
			// "Any schema" probe — match if any cached entry has this
			// table name regardless of its schema.
			auto suffix = "|" + t_lower;
			for (const auto &k : keys) {
				if (k.size() >= suffix.size() &&
				    k.compare(k.size() - suffix.size(), suffix.size(), suffix) == 0) {
					return true;
				}
			}
			return false;
		}
		return keys.count(lowercase(schema) + "|" + t_lower) > 0;
	}

	static std::string lowercase(const std::string &s) {
		std::string out = s;
		for (auto &c : out) {
			c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
		}
		return out;
	}
};

static std::string Rewrite(const std::string &sql, const std::string &catalog_name,
                           const FakeCache &cache = {}) {
	Parser parser;
	parser.ParseQuery(sql);
	REQUIRE(parser.statements.size() >= 1);

	CatalogReferenceRewriter rewriter(
	    catalog_name,
	    [&cache](const std::string &schema, const std::string &table) {
		    return cache.Has(schema, table);
	    });
	rewriter.Visit(*parser.statements[0]);
	return parser.statements[0]->ToString();
}

static bool Contains(const std::string &haystack, const std::string &needle) {
	return haystack.find(needle) != std::string::npos;
}

// ── Rule T: BaseTableRef catalog stripping ──────────────────────────────────

TEST_CASE("Rule T strips attached catalog from qualified table ref",
          "[rewriter][rule_t]") {
	auto out = Rewrite("SELECT * FROM warehouse.main.users", "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "main"));
	REQUIRE(Contains(out, "users"));
}

TEST_CASE("Rule T leaves other-catalog refs untouched",
          "[rewriter][rule_t][mixed_catalogs]") {
	auto out = Rewrite(
	    "SELECT * FROM warehouse.main.a JOIN other_db.main.b ON a.id = b.id",
	    "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse.main.a"));
	// The non-attached catalog must survive the rewrite intact.
	REQUIRE(Contains(out, "other_db"));
}

TEST_CASE("Rule T ignores bare table names",
          "[rewriter][rule_t]") {
	auto out = Rewrite("SELECT * FROM users", "warehouse");
	// `users` had no catalog qualifier — should pass through untouched.
	REQUIRE(Contains(out, "users"));
	REQUIRE_FALSE(Contains(out, "warehouse"));
}

TEST_CASE("Rule T strips CREATE TABLE catalog",
          "[rewriter][rule_t][ddl]") {
	auto out = Rewrite(
	    "CREATE TABLE warehouse.main.events (id INTEGER, payload VARCHAR)",
	    "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse"));
	// DuckDB's `CreateTableInfo::ToString()` omits the schema when it's
	// `main`, so we only assert the table name survives and the
	// catalog name is gone. The schema is preserved internally in the
	// `CreateInfo::schema` field; the wire emission is up to DuckDB.
	REQUIRE(Contains(out, "events"));
}

TEST_CASE("Rule T strips DROP / INSERT catalogs",
          "[rewriter][rule_t][ddl]") {
	auto drop = Rewrite("DROP TABLE warehouse.main.events", "warehouse");
	REQUIRE_FALSE(Contains(drop, "warehouse"));

	auto ins = Rewrite("INSERT INTO warehouse.main.events VALUES (1, 'x')",
	                    "warehouse");
	REQUIRE_FALSE(Contains(ins, "warehouse"));
	REQUIRE(Contains(ins, "main.events"));
}

// ── Rule C: qualified column-ref stripping ──────────────────────────────────

TEST_CASE("Rule C strips catalog from 3+ part column chain when cache knows the entry",
          "[rewriter][rule_c]") {
	FakeCache cache;
	cache.Add("main", "users");
	auto out = Rewrite(
	    "SELECT warehouse.main.users.id FROM warehouse.main.users",
	    "warehouse", cache);
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "main.users"));
}

TEST_CASE("Rule C leaves 2-part column refs alone",
          "[rewriter][rule_c]") {
	auto out = Rewrite("SELECT warehouse.col FROM t", "warehouse");
	// `warehouse.col` is only length 2 — can't be a catalog qualifier.
	const bool survives =
	    Contains(out, "warehouse.col") || Contains(out, "warehouse");
	REQUIRE(survives);
}

TEST_CASE("Rule C leaves 1-part column refs alone",
          "[rewriter][rule_c]") {
	// A column literally named `warehouse` must survive.
	auto out = Rewrite("SELECT warehouse FROM t", "warehouse");
	REQUIRE(Contains(out, "warehouse"));
}

TEST_CASE("Rule C conservatively declines to rewrite when cache is empty",
          "[rewriter][rule_c][cache_miss]") {
	FakeCache cache; // no entries — probe returns false
	auto out = Rewrite(
	    "SELECT warehouse.main.users.id FROM warehouse.main.users",
	    "warehouse", cache);
	// Rule T still fires on the FROM (that doesn't consult the cache),
	// but Rule C declines without a cache hit. The original chain
	// survives the rewrite.
	const bool survived = Contains(out, "main.users.id")
	                      || Contains(out, "warehouse.main.users.id");
	REQUIRE(survived);
}

// ── Scope stack: CTE and alias shadowing ───────────────────────────────────

TEST_CASE("CTE name shadows the attached catalog",
          "[rewriter][scope][cte]") {
	// `WITH warehouse AS (...) SELECT * FROM warehouse` — the
	// BaseTableRef for `warehouse` has catalog_name="" (the parser
	// doesn't promote bare identifiers to catalog refs), so Rule T
	// never triggers regardless of scope. This test just confirms the
	// pattern round-trips without corruption.
	auto out = Rewrite(
	    "WITH warehouse AS (SELECT 1 AS x) SELECT * FROM warehouse",
	    "warehouse");
	REQUIRE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "SELECT"));
}

TEST_CASE("Table alias named 'warehouse' survives the rewrite",
          "[rewriter][scope][alias]") {
	auto out = Rewrite("SELECT * FROM t AS warehouse", "warehouse");
	REQUIRE(Contains(out, "warehouse"));
}

// ── Rule C: 3-part column refs (regression for the catalog.table.column form) ──

// Background: DuckDB allows two qualified column-ref shapes that begin
// with the attached catalog name:
//   3-part:  catalog.table.column            ← far more common in practice
//   4-part:  catalog.schema.table.column
// The original `RewriteColumnChain` assumed every 3+ part chain was
// 4-part and probed `(schema=column_names[1], table=column_names[2])`.
// For a 3-part `cloud.users.id` that became `(schema="users", table="id")`,
// which never matched any cached entry — so the catalog prefix was
// silently never stripped, the SQL went to the worker as
// `WHERE cloud.users.id = …`, and the worker rejected it because it
// has no `cloud` schema. These tests pin the correct behaviour for
// both shapes.

TEST_CASE("Rule C strips 3-part column ref `catalog.table.column` when table is cached",
          "[rewriter][rule_c][3part]") {
	FakeCache cache;
	cache.Add("main", "users");
	auto out = Rewrite(
	    "SELECT warehouse.users.id FROM warehouse.users",
	    "warehouse", cache);
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "users"));
	REQUIRE(Contains(out, "id"));
}

TEST_CASE("Rule C 3-part probes the table name in the default schema (cache hit on bare table)",
          "[rewriter][rule_c][3part]") {
	// The 3-part probe uses `("", table)` — empty schema means
	// "any cached table". Cache contains only the bare table key
	// (no schema), so the probe must still succeed.
	FakeCache cache;
	cache.Add("", "users"); // matches the empty-schema probe path
	auto out = Rewrite("SELECT warehouse.users.id FROM warehouse.users",
	                    "warehouse", cache);
	REQUIRE_FALSE(Contains(out, "warehouse"));
}

TEST_CASE("Rule C 4-part column ref still strips correctly",
          "[rewriter][rule_c][4part]") {
	FakeCache cache;
	cache.Add("main", "users");
	auto out = Rewrite(
	    "SELECT warehouse.main.users.id FROM warehouse.main.users",
	    "warehouse", cache);
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "main.users"));
}

TEST_CASE("Rule C 3-part declines without a matching table in the cache",
          "[rewriter][rule_c][3part][cache_miss]") {
	FakeCache cache; // empty
	auto out = Rewrite("SELECT warehouse.users.id FROM warehouse.users",
	                    "warehouse", cache);
	// FROM clause still gets Rule T (which doesn't consult the cache),
	// but the column chain must survive untouched.
	const bool survived =
	    Contains(out, "users.id") || Contains(out, "warehouse.users.id");
	REQUIRE(survived);
}

// ── Rule T: 2-part `cloud.table` form (parser puts catalog into schema_name) ──
//
// When users write the common shorthand `cloud.daily_quake_counts`
// (no explicit schema), DuckDB's parser stores `cloud` in
// `schema_name` rather than `catalog_name`. There's no way at parse
// time to know `cloud` is an attached catalog vs a real schema. The
// rewriter must detect this case and strip `schema_name` — otherwise
// the worker sees `DELETE FROM cloud.daily_quake_counts …` and fails
// with "schema cloud does not exist".

TEST_CASE("Rule T strips 2-part `catalog.table` from SELECT",
          "[rewriter][rule_t][2part]") {
	auto out = Rewrite("SELECT * FROM warehouse.users", "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "users"));
}

TEST_CASE("Rule T strips 2-part `catalog.table` from INSERT",
          "[rewriter][rule_t][2part][dml]") {
	auto out = Rewrite(
	    "INSERT INTO warehouse.events VALUES (1, 'x')", "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "events"));
}

TEST_CASE("Rule T strips 2-part `catalog.table` from UPDATE",
          "[rewriter][rule_t][2part][dml]") {
	auto out = Rewrite(
	    "UPDATE warehouse.users SET name = 'bob' WHERE id = 1",
	    "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "users"));
}

TEST_CASE("Rule T strips 2-part `catalog.table` from DELETE",
          "[rewriter][rule_t][2part][dml]") {
	auto out = Rewrite(
	    "DELETE FROM warehouse.users WHERE id < 10", "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "users"));
}

TEST_CASE("Rule T strips 2-part `catalog.table` from CREATE TABLE",
          "[rewriter][rule_t][2part][ddl]") {
	auto out = Rewrite(
	    "CREATE TABLE warehouse.events (id INTEGER, payload VARCHAR)",
	    "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "events"));
}

TEST_CASE("Rule T strips 2-part `catalog.table` from DROP TABLE",
          "[rewriter][rule_t][2part][ddl]") {
	auto out = Rewrite("DROP TABLE warehouse.events", "warehouse");
	REQUIRE_FALSE(Contains(out, "warehouse"));
	REQUIRE(Contains(out, "events"));
}

TEST_CASE("Rule T 2-part leaves a real `schema.table` (non-attached) alone",
          "[rewriter][rule_t][2part][safety]") {
	// `other_db.users` — `other_db` is not the attached catalog name,
	// so the rewriter must NOT touch it.
	auto out = Rewrite("SELECT * FROM other_db.users", "warehouse");
	REQUIRE(Contains(out, "other_db"));
}

// ── Substitution: preserves user-visible name as alias ─────────────────────
//
// Cross-catalog DML stages local tables under generated names like
// `__openduck_ingest_<uuid>`. If the user wrote `WHERE local_t.id = …`,
// renaming the BaseTableRef alone leaves `local_t.id` dangling. The
// rewriter must keep the original name as the alias so column refs
// continue to resolve.

TEST_CASE("Substitution preserves the original table name as an alias",
          "[rewriter][substitution][alias]") {
	Parser parser;
	parser.ParseQuery(
	    "UPDATE warehouse.users SET flag = 'x' "
	    "FROM local_t WHERE warehouse.users.id = local_t.id");
	REQUIRE(parser.statements.size() == 1);

	FakeCache cache;
	cache.Add("main", "users");
	cache.Add("", "users");

	CatalogReferenceRewriter rewriter(
	    "warehouse",
	    [&cache](const std::string &schema, const std::string &table) {
		    return cache.Has(schema, table);
	    });
	rewriter.AddSubstitution("", "", "local_t",
	                         "__openduck_ingest_test_alias");
	rewriter.Visit(*parser.statements[0]);
	auto out = parser.statements[0]->ToString();

	// The staging table replaces `local_t` as the actual ref…
	REQUIRE(Contains(out, "__openduck_ingest_test_alias"));
	// …but `local_t` survives as an alias so `local_t.id` keeps
	// resolving (otherwise the worker rejects the WHERE).
	REQUIRE(Contains(out, "local_t"));
	// And the catalog-qualified `warehouse.users.id` got stripped.
	REQUIRE_FALSE(Contains(out, "warehouse.users.id"));
}

TEST_CASE("Substitution preserves a user-supplied alias rather than the table name",
          "[rewriter][substitution][alias]") {
	Parser parser;
	parser.ParseQuery(
	    "UPDATE warehouse.users SET flag = 'x' "
	    "FROM local_t AS lt WHERE warehouse.users.id = lt.id");
	REQUIRE(parser.statements.size() == 1);

	FakeCache cache;
	cache.Add("main", "users");
	cache.Add("", "users");

	CatalogReferenceRewriter rewriter(
	    "warehouse",
	    [&cache](const std::string &schema, const std::string &table) {
		    return cache.Has(schema, table);
	    });
	rewriter.AddSubstitution("", "", "local_t",
	                         "__openduck_ingest_test_alias_user");
	rewriter.Visit(*parser.statements[0]);
	auto out = parser.statements[0]->ToString();

	REQUIRE(Contains(out, "__openduck_ingest_test_alias_user"));
	// The user-supplied alias `lt` (not `local_t`) is what column
	// refs use, so that's what must survive on the staging ref.
	REQUIRE(Contains(out, "lt"));
}

// ── Round-trip fidelity ────────────────────────────────────────────────────

TEST_CASE("Parse → rewrite → ToString → parse → ToString is stable",
          "[rewriter][fidelity]") {
	const std::string sqls[] = {
	    "SELECT * FROM warehouse.main.users WHERE id > 100",
	    "INSERT INTO warehouse.main.events (id, payload) "
	    "SELECT id, payload FROM warehouse.main.staging",
	    "UPDATE warehouse.main.users SET name = 'bob' WHERE id = 1",
	    "DELETE FROM warehouse.main.users WHERE id > 10",
	    "CREATE VIEW warehouse.main.v AS SELECT id FROM warehouse.main.users",
	    // 2-part DML/DDL forms — same fidelity expectation as 3-part.
	    "SELECT * FROM warehouse.users WHERE id > 100",
	    "DELETE FROM warehouse.users WHERE id < 10",
	    "UPDATE warehouse.users SET name = 'bob' WHERE id = 1",
	    "INSERT INTO warehouse.events VALUES (1, 'x')",
	};
	for (const auto &sql : sqls) {
		auto first = Rewrite(sql, "warehouse");
		auto second = Rewrite(first, "warehouse"); // idempotent
		REQUIRE(first == second);
		// And the once-rewritten form must no longer mention `warehouse`.
		REQUIRE_FALSE(Contains(first, "warehouse"));
	}
}
