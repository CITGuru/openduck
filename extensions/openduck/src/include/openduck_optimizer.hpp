#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace openduck {

/// Optimizer extension that runs AFTER DuckDB's built-in optimizers.
///
/// Current responsibilities (see
/// `docs/design/learnings-from-duckdb-postgres.md §8`):
///
///   1. Constant LIMIT/OFFSET pushdown into `openduck_table_scan`'s
///      bind data, so the remote worker query becomes
///      `SELECT ... FROM t LIMIT N [OFFSET M]` instead of shipping the
///      full table and discarding rows client-side. Mirrors
///      `PostgresOptimizer`'s `OptimizePostgresScanLimitPushdown`.
///
/// Registered via `OptimizerExtension::Register(config, ...)` from
/// `OpenDuckExtension::Load`.
class OpenDuckOptimizer {
public:
	/// Entry point installed as
	/// `OptimizerExtension::optimize_function`. Walks the plan in
	/// place, mutates `openduck_table_scan` bind data where pushdown
	/// is applicable, and peels off any `LogicalLimit` that got fully
	/// absorbed.
	static void Optimize(duckdb::OptimizerExtensionInput &input,
	                     duckdb::unique_ptr<duckdb::LogicalOperator> &plan);
};

} // namespace openduck
