#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class SQLStatement;
class TableRef;
class ParsedExpression;
class QueryNode;
class CommonTableExpressionMap;
class SelectStatement;
class InsertStatement;
class UpdateStatement;
class DeleteStatement;
class CreateStatement;
class DropStatement;
class AlterStatement;
class CopyStatement;
class PrepareStatement;
class ExecuteStatement;
} // namespace duckdb

namespace openduck {

class OpenDuckCatalog;

// ═══════════════════════════════════════════════════════════════════════════
// CatalogReferenceRewriter
// ═══════════════════════════════════════════════════════════════════════════
//
// Walks a ParsedStatement tree and strips the attached-catalog qualifier
// (`A.`) from BaseTableRefs, CreateInfo / DropInfo / AlterInfo / CopyInfo
// `catalog` fields, and qualified `ColumnRefExpression::column_names`
// chains whose first element matches `A`. The statement then
// `ToString()`s into SQL the worker can run directly against its local
// catalog.
//
// Correctness rests on two rules:
//
//   Rule T: rewrite `BaseTableRef { catalog = A, ... }` → catalog = "".
//   Rule C: rewrite `[A, s, t, ...]` column chains → `[s, t, ...]`
//           IFF (s, t) is known to exist in catalog A's cache AND the
//           name `A` is NOT shadowed by the current scope stack.
//
// Both rules are gated by a scope stack that tracks CTE names, table
// aliases, and lambda parameter names — so `WITH A AS (...)`,
// `FROM t AS A`, and `list_transform(xs, A -> ...)` are all left
// untouched.
class CatalogReferenceRewriter {
public:
	/// Callback that reports whether `(schema, table)` names a
	/// currently-cached entry in the attached catalog. Consulted by
	/// Rule C only; never called for Rule T. Used as a seam so unit
	/// tests can inject a lambda instead of constructing a full
	/// `OpenDuckCatalog` / `AttachedDatabase`.
	using CacheProbe = std::function<bool(const std::string &schema,
	                                       const std::string &table)>;

	CatalogReferenceRewriter(std::string catalog_name, OpenDuckCatalog &catalog);

	/// Test-only constructor: inject an arbitrary schema cache probe
	/// without needing a real `OpenDuckCatalog` (which in turn needs
	/// an `AttachedDatabase`, `DatabaseInstance`, etc.).
	CatalogReferenceRewriter(std::string catalog_name, CacheProbe probe);

	/// Register a substitution: every `BaseTableRef` whose
	/// `(catalog, schema, table)` triple matches the key is replaced
	/// with `BaseTableRef(table = replacement_table, schema = "",
	/// catalog = "")`, preserving its alias. Used by the cross-catalog
	/// DML path to re-target local source refs at their staging
	/// temp-table equivalents. Matching is case-insensitive.
	///
	/// Substitutions are checked BEFORE Rule T, so a substitution for
	/// a local catalog's table pre-empts Rule T's "strip attached
	/// catalog" behavior.
	void AddSubstitution(const std::string &catalog, const std::string &schema,
	                     const std::string &table,
	                     const std::string &replacement_table);

	/// Rewrite every catalog reference in `stmt` in place.
	void Visit(duckdb::SQLStatement &stmt);

private:
	std::string catalog_name_; //!< The attached-catalog name (`A`).
	CacheProbe probe_;         //!< Rule C's cache-populated check.

	/// Per-visit scope stack of identifiers that shadow the catalog name.
	/// Pushed on WITH, table alias, and lambda parameter entry; popped on
	/// exit. Case-insensitive because SQL identifiers compare
	/// case-insensitively.
	duckdb::case_insensitive_set_t scope_stack_;

	/// Substitution map keyed on lowercase "<catalog>|<schema>|<table>".
	/// An empty catalog/schema component in the key matches a
	/// corresponding empty component on the ref. The associated value
	/// is the replacement bare table name.
	std::unordered_map<std::string, std::string> substitutions_;

	// ── Per-node dispatch ──
	void VisitStatement(duckdb::SQLStatement &stmt);
	void VisitSelect(duckdb::SelectStatement &stmt);
	void VisitInsert(duckdb::InsertStatement &stmt);
	void VisitUpdate(duckdb::UpdateStatement &stmt);
	void VisitDelete(duckdb::DeleteStatement &stmt);
	void VisitCreate(duckdb::CreateStatement &stmt);
	void VisitDrop(duckdb::DropStatement &stmt);
	void VisitAlter(duckdb::AlterStatement &stmt);
	void VisitCopy(duckdb::CopyStatement &stmt);
	void VisitPrepare(duckdb::PrepareStatement &stmt);
	void VisitExecute(duckdb::ExecuteStatement &stmt);

	void VisitQueryNode(duckdb::QueryNode &node);
	void VisitTableRef(duckdb::TableRef &ref);
	void VisitExpression(duckdb::ParsedExpression &expr);
	void VisitCTEMap(duckdb::CommonTableExpressionMap &cte_map);

	// ── Rewrite primitives ──
	void RewriteTableCatalog(std::string &catalog);
	void RewriteColumnChain(duckdb::ParsedExpression &expr);

	bool IsShadowed(const std::string &name) const;
	bool IsAttachedCatalog(const std::string &candidate) const;

	/// RAII helper: pushes an identifier onto the scope stack for the
	/// lifetime of the returned object, then pops it.
	class ScopePush {
	public:
		ScopePush(CatalogReferenceRewriter &parent, const std::string &name);
		~ScopePush();
		ScopePush(const ScopePush &) = delete;
		ScopePush &operator=(const ScopePush &) = delete;

	private:
		CatalogReferenceRewriter &parent_;
		std::string name_;
		bool pushed_;
	};
};

} // namespace openduck
