#pragma once

#include "duckdb/common/types.hpp"

#include <string>

namespace openduck {

// ═══════════════════════════════════════════════════════════════════════════
// SQL text emission helpers
// ═══════════════════════════════════════════════════════════════════════════
//
// Narrow, single-purpose helpers for building SQL text the worker can
// re-parse. These used to live scattered across `openduck_schema.cpp`
// and `openduck_ingest.cpp`; consolidating them here makes
// quoting/schema-prefix behavior a single review target (§2 of
// `docs/design/learnings-from-duckdb-postgres.md`).
//
// All helpers are pure functions. Anything requiring a `ClientContext`
// or catalog state (e.g. `BuildForwardedSQL`) stays in its owning
// module.

/// Quote `name` as a DuckDB identifier — wraps in double-quotes and
/// doubles any embedded `"`. Suitable for any table / column /
/// schema / catalog name emitted into worker-facing SQL.
std::string QuoteIdentifier(const std::string &name);

/// Quote `s` as a SQL string literal — wraps in single-quotes and
/// doubles any embedded `'`. Used when embedding user-provided or
/// catalog-probe strings into worker-facing SQL (e.g., schema name
/// filters on `duckdb_columns()`).
std::string QuoteLiteral(const std::string &s);

/// Emit a qualified name `[catalog.][schema.]table` with each component
/// double-quoted. Empty components are skipped — `QuoteRef("", "", "t")`
/// returns `"t"`, `QuoteRef("", "main", "t")` returns `"main"."t"`.
std::string QuoteRef(const std::string &catalog, const std::string &schema,
                     const std::string &table);

/// Convert a DuckDB `LogicalType` into the SQL type string used inside
/// a `CREATE TEMP TABLE` on the worker. Today this is just
/// `type.ToString()` — kept as a named helper so future dialect
/// divergence (e.g., if we ever need to rewrite certain types) has
/// a single hook.
std::string TypeToSQL(const duckdb::LogicalType &type);

/// Generate a unique staging-table identifier for cross-catalog
/// ingest operations. The returned name is process-local-unique
/// within a worker connection for the lifetime of one statement;
/// doesn't need to be globally unique.
std::string GenerateStagingTable();

} // namespace openduck
