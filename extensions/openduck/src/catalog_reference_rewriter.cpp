#include "catalog_reference_rewriter.hpp"
#include "openduck_catalog.hpp"
#include "openduck_schema.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace openduck {

using namespace duckdb;

// ═══════════════════════════════════════════════════════════════════════════
// ScopePush — RAII
// ═══════════════════════════════════════════════════════════════════════════

CatalogReferenceRewriter::ScopePush::ScopePush(CatalogReferenceRewriter &parent,
                                               const std::string &name)
    : parent_(parent), name_(name), pushed_(false) {
	if (!name_.empty()) {
		parent_.scope_stack_.insert(name_);
		pushed_ = true;
	}
}

CatalogReferenceRewriter::ScopePush::~ScopePush() {
	if (pushed_) {
		parent_.scope_stack_.erase(name_);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// CatalogReferenceRewriter
// ═══════════════════════════════════════════════════════════════════════════

CatalogReferenceRewriter::CatalogReferenceRewriter(std::string catalog_name, OpenDuckCatalog &catalog)
    : catalog_name_(std::move(catalog_name)) {
	// Bind the cache probe to the live catalog's schema cache. The
	// separate constructor overload is the test seam.
	OpenDuckCatalog *ptr = &catalog;
	probe_ = [ptr](const std::string &schema, const std::string &table) {
		return ptr->HasSchemaOrTable(schema, table);
	};
}

CatalogReferenceRewriter::CatalogReferenceRewriter(std::string catalog_name, CacheProbe probe)
    : catalog_name_(std::move(catalog_name)), probe_(std::move(probe)) {
}

static std::string Lower(const std::string &s) {
	std::string out = s;
	for (auto &c : out) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	return out;
}

static std::string SubKey(const std::string &catalog, const std::string &schema,
                          const std::string &table) {
	return Lower(catalog) + "|" + Lower(schema) + "|" + Lower(table);
}

void CatalogReferenceRewriter::AddSubstitution(const std::string &catalog,
                                                const std::string &schema,
                                                const std::string &table,
                                                const std::string &replacement_table) {
	substitutions_[SubKey(catalog, schema, table)] = replacement_table;
}

bool CatalogReferenceRewriter::IsAttachedCatalog(const std::string &candidate) const {
	return StringUtil::CIEquals(candidate, catalog_name_);
}

bool CatalogReferenceRewriter::IsShadowed(const std::string &name) const {
	return scope_stack_.find(name) != scope_stack_.end();
}

void CatalogReferenceRewriter::RewriteTableCatalog(std::string &catalog) {
	if (!catalog.empty() && IsAttachedCatalog(catalog)) {
		catalog.clear();
	}
}

// Rule C. Operates on a ColumnRefExpression's `column_names` in place.
void CatalogReferenceRewriter::RewriteColumnChain(ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		return;
	}
	auto &col = expr.Cast<ColumnRefExpression>();
	if (col.column_names.size() < 3) {
		return; // `len >= 3` gate — chains with fewer than 3 parts have no catalog qualifier to strip.
	}
	const auto &first = col.column_names[0];
	if (!IsAttachedCatalog(first)) {
		return;
	}
	if (IsShadowed(first)) {
		return; // Scope-stack shadowing (CTE / alias)
	}
	// DuckDB allows two qualified column-ref shapes that begin with a
	// catalog name:
	//   3-part: catalog.table.column
	//   4-part: catalog.schema.table.column
	// In the 3-part form the parts after the leading catalog are
	// `[table, column, …]`; in the 4-part form they are
	// `[schema, table, column, …]`. The cache probe needs to be told
	// which shape we're looking at — passing the wrong one means we'd
	// probe (schema=<table_name>, table=<column_name>), which never
	// matches and silently suppresses the rewrite (the bug we're
	// fixing here).
	bool ok;
	if (col.column_names.size() == 3) {
		// catalog.table.column → probe just the table name in the
		// catalog's default schema. Empty schema string accepts any
		// cached table (see `OpenDuckCatalog::HasSchemaOrTable`).
		const auto &table = col.column_names[1];
		ok = probe_("", table);
	} else {
		// 4+ part: catalog.schema.table.column[.field…]
		const auto &schema = col.column_names[1];
		const auto &table = col.column_names[2];
		ok = probe_(schema, table);
	}
	if (!ok) {
		// Cache-miss safety: if we can't prove the chain names a
		// known entry, refuse to rewrite. Either Rule T already ran
		// on a live `LookupEntry` earlier in the binder path and
		// populated the cache, or the name is spurious and leaving
		// the chain intact is harmless.
		return;
	}
	col.column_names.erase(col.column_names.begin());
}

void CatalogReferenceRewriter::VisitExpression(ParsedExpression &expr) {
	RewriteColumnChain(expr);
	// DuckDB's `EnumerateChildren` for `SubqueryExpression` visits only
	// the `child` (the LHS of `IN` / `EXISTS`), not the nested
	// `subquery` statement itself. Descend explicitly so catalog refs
	// inside `WHERE x IN (SELECT … FROM local.t)` reach Rule T / C.
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &sq = expr.Cast<SubqueryExpression>();
		if (sq.subquery) {
			VisitSelect(*sq.subquery);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { VisitExpression(child); });
}

// Leaf callback: apply Rule T + alias-scope push per ref. Structural
// recursion is handled by DuckDB's `EnumerateTableRefChildren` /
// `EnumerateQueryNodeChildren`, which invoke this callback once for
// every TableRef in the tree.
void CatalogReferenceRewriter::VisitTableRef(TableRef &ref) {
	// A `warehouse` alias in FROM shadows the catalog name in column
	// refs further up the call stack. Because the iterator invokes us
	// after walking children, we push the alias name here but keep
	// the push-scope narrowly to this ref's subtree via the alias
	// scope registered by its parent query node. For the shipped
	// grammar this is a no-op for most test cases; the scope stack
	// semantic matters for hand-written grammars that put column refs
	// inside the same FROM scope.
	if (!ref.alias.empty()) {
		scope_stack_.insert(ref.alias);
	}

	if (ref.type == TableReferenceType::BASE_TABLE) {
		auto &base = ref.Cast<BaseTableRef>();
		// Substitution first (takes precedence over Rule T). Used by
		// the cross-catalog DML path to re-target local source refs
		// at their staging temp-table equivalents.
		auto key = SubKey(base.catalog_name, base.schema_name, base.table_name);
		auto it = substitutions_.find(key);
		if (it != substitutions_.end()) {
			// Preserve the user-visible name as an alias on the
			// rewritten ref. Without this, column refs that name
			// the original table (e.g. `local_t.id` in the WHERE)
			// would dangle on the worker side, where `local_t`
			// doesn't exist. With it, `local_t.id` continues to
			// resolve cleanly because the staging temp table is
			// re-aliased to `local_t`.
			//
			// If the user already wrote an alias, keep it (their
			// alias is the one column refs use); otherwise fall
			// back to the original table name.
			std::string original = base.alias.empty() ? base.table_name : base.alias;
			base.catalog_name.clear();
			base.schema_name.clear();
			base.table_name = it->second;
			base.alias = std::move(original);
		} else {
			// Rule T: strip attached-catalog qualifier.
			RewriteTableCatalog(base.catalog_name);
			// Rule T (2-part form): when a user writes the common
			// shorthand `<alias>.<table>` (no explicit schema), the
			// parser stores `<alias>` in `schema_name` rather than
			// `catalog_name` — there's no way at parse time to know
			// `<alias>` is an attached catalog vs a real schema. If
			// `schema_name` matches our attached alias and there's no
			// explicit catalog qualifier, treat it as the
			// catalog-only form and strip it; the worker will
			// resolve the bare table name in its default schema
			// chain (typically `main`). A 3-part form
			// `<alias>.<schema>.<table>` is already handled above by
			// `RewriteTableCatalog(catalog_name)` plus `schema_name`
			// being forwarded as-is.
			if (base.catalog_name.empty() && !base.schema_name.empty() &&
			    IsAttachedCatalog(base.schema_name)) {
				base.schema_name.clear();
			}
		}
	}
	// Nested SubqueryRef bodies are already walked by
	// `EnumerateQueryNodeChildren`'s recursion into the subquery node
	// — no extra descent needed here.
}

void CatalogReferenceRewriter::VisitCTEMap(CommonTableExpressionMap &cte_map) {
	// Push every CTE name BEFORE visiting any CTE body so recursive
	// CTEs (`WITH foo AS (SELECT ... FROM foo)`) see their own name
	// shadowed. This variant also walks the bodies — used by
	// `InsertStatement` / `UpdateStatement` / `DeleteStatement` whose
	// CTE maps are NOT the responsibility of
	// `EnumerateQueryNodeChildren`. `VisitQueryNode` handles CTE
	// bodies itself (via the iterator) and only needs the scope push.
	std::vector<std::string> pushed;
	pushed.reserve(cte_map.map.size());
	for (auto &pair : cte_map.map) {
		if (scope_stack_.insert(pair.first).second) {
			pushed.push_back(pair.first);
		}
	}
	for (auto &pair : cte_map.map) {
		if (pair.second && pair.second->query) {
			VisitSelect(*pair.second->query);
		}
	}
	for (auto &name : pushed) {
		scope_stack_.erase(name);
	}
}

void CatalogReferenceRewriter::VisitQueryNode(QueryNode &node) {
	// Push every CTE name onto the scope stack for the full lifetime
	// of this query node — covering both our own code paths and the
	// `EnumerateQueryNodeChildren` walk, which itself recurses into
	// CTE bodies. A CTE named `warehouse` correctly shadows the
	// attached catalog anywhere inside this node's subtree.
	std::vector<std::string> pushed;
	pushed.reserve(node.cte_map.map.size());
	for (auto &pair : node.cte_map.map) {
		if (scope_stack_.insert(pair.first).second) {
			pushed.push_back(pair.first);
		}
	}

	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node,
	    [&](unique_ptr<ParsedExpression> &child) {
		    if (child) {
			    VisitExpression(*child);
		    }
	    },
	    [&](TableRef &ref) { VisitTableRef(ref); });
	ParsedExpressionIterator::EnumerateQueryNodeModifiers(
	    node, [&](unique_ptr<ParsedExpression> &child) {
		    if (child) {
			    VisitExpression(*child);
		    }
	    });

	for (auto &name : pushed) {
		scope_stack_.erase(name);
	}
}

// ── Statement dispatch ─────────────────────────────────────────────────────

void CatalogReferenceRewriter::VisitSelect(SelectStatement &stmt) {
	if (stmt.node) {
		VisitQueryNode(*stmt.node);
	}
}

void CatalogReferenceRewriter::VisitInsert(InsertStatement &stmt) {
	// `InsertStatement` carries the target as direct `catalog/schema/
	// table` fields (NOT through a `BaseTableRef`). Apply the same
	// 3-part and 2-part rewrites here.
	RewriteTableCatalog(stmt.catalog);
	if (stmt.catalog.empty() && !stmt.schema.empty() && IsAttachedCatalog(stmt.schema)) {
		stmt.schema.clear();
	}
	VisitCTEMap(stmt.cte_map);
	if (stmt.table_ref) {
		VisitTableRef(*stmt.table_ref);
	}
	if (stmt.select_statement) {
		VisitSelect(*stmt.select_statement);
	}
	for (auto &expr : stmt.returning_list) {
		if (expr) {
			VisitExpression(*expr);
		}
	}
	if (stmt.on_conflict_info) {
		if (stmt.on_conflict_info->condition) {
			VisitExpression(*stmt.on_conflict_info->condition);
		}
		if (stmt.on_conflict_info->set_info) {
			if (stmt.on_conflict_info->set_info->condition) {
				VisitExpression(*stmt.on_conflict_info->set_info->condition);
			}
			for (auto &expr : stmt.on_conflict_info->set_info->expressions) {
				if (expr) {
					VisitExpression(*expr);
				}
			}
		}
	}
}

void CatalogReferenceRewriter::VisitUpdate(UpdateStatement &stmt) {
	VisitCTEMap(stmt.cte_map);
	if (stmt.table) {
		VisitTableRef(*stmt.table);
	}
	if (stmt.from_table) {
		VisitTableRef(*stmt.from_table);
	}
	if (stmt.set_info) {
		if (stmt.set_info->condition) {
			VisitExpression(*stmt.set_info->condition);
		}
		for (auto &expr : stmt.set_info->expressions) {
			if (expr) {
				VisitExpression(*expr);
			}
		}
	}
	for (auto &expr : stmt.returning_list) {
		if (expr) {
			VisitExpression(*expr);
		}
	}
}

void CatalogReferenceRewriter::VisitDelete(DeleteStatement &stmt) {
	VisitCTEMap(stmt.cte_map);
	if (stmt.table) {
		VisitTableRef(*stmt.table);
	}
	for (auto &ref : stmt.using_clauses) {
		if (ref) {
			VisitTableRef(*ref);
		}
	}
	if (stmt.condition) {
		VisitExpression(*stmt.condition);
	}
	for (auto &expr : stmt.returning_list) {
		if (expr) {
			VisitExpression(*expr);
		}
	}
}

void CatalogReferenceRewriter::VisitCreate(CreateStatement &stmt) {
	if (!stmt.info) {
		return;
	}
	RewriteTableCatalog(stmt.info->catalog);
	// 2-part `CREATE TABLE cloud.foo (...)` form: parser puts `cloud`
	// in `schema`. Strip if it names the attached catalog.
	if (stmt.info->catalog.empty() && !stmt.info->schema.empty() &&
	    IsAttachedCatalog(stmt.info->schema)) {
		stmt.info->schema.clear();
	}

	// `CreateInfo` is polymorphic; descend into the variants that
	// carry embedded SQL subtrees so catalog refs inside them get
	// rewritten too. CTAS (`CreateTableInfo::query`) and VIEW
	// (`CreateViewInfo::query`) are the common ones. Other variants
	// (CREATE TYPE, CREATE SEQUENCE) carry no catalog refs in their
	// body fields.
	switch (stmt.info->type) {
	case CatalogType::VIEW_ENTRY: {
		auto &view = stmt.info->Cast<CreateViewInfo>();
		if (view.query) {
			VisitSelect(*view.query);
		}
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		auto &table = stmt.info->Cast<CreateTableInfo>();
		if (table.query) {
			VisitSelect(*table.query);
		}
		break;
	}
	default:
		break;
	}
}

void CatalogReferenceRewriter::VisitDrop(DropStatement &stmt) {
	if (stmt.info) {
		RewriteTableCatalog(stmt.info->catalog);
		// 2-part `DROP TABLE cloud.foo` form: parser puts `cloud` in
		// `schema`. Strip if it names the attached catalog.
		if (stmt.info->catalog.empty() && !stmt.info->schema.empty() &&
		    IsAttachedCatalog(stmt.info->schema)) {
			stmt.info->schema.clear();
		}
	}
}

void CatalogReferenceRewriter::VisitAlter(AlterStatement &stmt) {
	if (stmt.info) {
		RewriteTableCatalog(stmt.info->catalog);
		if (stmt.info->catalog.empty() && !stmt.info->schema.empty() &&
		    IsAttachedCatalog(stmt.info->schema)) {
			stmt.info->schema.clear();
		}
	}
}

void CatalogReferenceRewriter::VisitCopy(CopyStatement &stmt) {
	if (!stmt.info) {
		return;
	}
	RewriteTableCatalog(stmt.info->catalog);
	if (stmt.info->select_statement) {
		VisitQueryNode(*stmt.info->select_statement);
	}
	if (stmt.info->file_path_expression) {
		VisitExpression(*stmt.info->file_path_expression);
	}
	for (auto &kv : stmt.info->parsed_options) {
		if (kv.second) {
			VisitExpression(*kv.second);
		}
	}
}

void CatalogReferenceRewriter::VisitPrepare(PrepareStatement &stmt) {
	if (stmt.statement) {
		VisitStatement(*stmt.statement);
	}
}

void CatalogReferenceRewriter::VisitExecute(ExecuteStatement &stmt) {
	for (auto &named : stmt.named_values) {
		if (named.second) {
			VisitExpression(*named.second);
		}
	}
}

void CatalogReferenceRewriter::VisitStatement(SQLStatement &stmt) {
	switch (stmt.type) {
	case StatementType::SELECT_STATEMENT:
		VisitSelect(stmt.Cast<SelectStatement>());
		break;
	case StatementType::INSERT_STATEMENT:
		VisitInsert(stmt.Cast<InsertStatement>());
		break;
	case StatementType::UPDATE_STATEMENT:
		VisitUpdate(stmt.Cast<UpdateStatement>());
		break;
	case StatementType::DELETE_STATEMENT:
		VisitDelete(stmt.Cast<DeleteStatement>());
		break;
	case StatementType::CREATE_STATEMENT:
		VisitCreate(stmt.Cast<CreateStatement>());
		break;
	case StatementType::DROP_STATEMENT:
		VisitDrop(stmt.Cast<DropStatement>());
		break;
	case StatementType::ALTER_STATEMENT:
		VisitAlter(stmt.Cast<AlterStatement>());
		break;
	case StatementType::COPY_STATEMENT:
		VisitCopy(stmt.Cast<CopyStatement>());
		break;
	case StatementType::PREPARE_STATEMENT:
		VisitPrepare(stmt.Cast<PrepareStatement>());
		break;
	case StatementType::EXECUTE_STATEMENT:
		VisitExecute(stmt.Cast<ExecuteStatement>());
		break;
	default:
		// Statements with no catalog refs to rewrite (TRANSACTION,
		// SET, VACUUM, etc.) pass through unchanged.
		break;
	}
}

void CatalogReferenceRewriter::Visit(SQLStatement &stmt) {
	scope_stack_.clear();
	VisitStatement(stmt);
}

} // namespace openduck
