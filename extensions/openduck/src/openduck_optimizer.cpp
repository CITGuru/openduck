#include "openduck_optimizer.hpp"

#include "openduck_scan_bind.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace openduck {

using namespace duckdb;

namespace {

/// Walk the plan. Whenever we find a `LogicalLimit` whose child is
/// (possibly through a chain of projections) a `LogicalGet` backed by
/// our `openduck_table_scan`, append the limit to the scan's bind
/// data and replace the LogicalLimit with its (unchanged) child.
///
/// The child-pointer `reference<LogicalOperator>` walk mirrors
/// `PostgresOptimizer::OptimizePostgresScanLimitPushdown` — projections
/// between the LIMIT and the GET are transparent for pushdown since
/// they don't filter rows.
void PushdownLimit(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			PushdownLimit(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != kOpenDuckTableScanName) {
			PushdownLimit(op->children[0]);
			return;
		}

		// Only push down when BOTH limit and offset are either a
		// concrete constant OR unset. Expressions, parameters, or
		// other dynamic forms get evaluated client-side.
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			PushdownLimit(op->children[0]);
			return;
		}
		switch (limit.offset_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			PushdownLimit(op->children[0]);
			return;
		}

		if (!get.bind_data) {
			PushdownLimit(op->children[0]);
			return;
		}
		auto &bind_data = get.bind_data->Cast<OpenDuckTableScanBindData>();
		// If a limit was already pushed down on this scan (e.g. the
		// optimizer ran twice, or there are nested LIMITs — we take
		// the outer one), bail: concatenating clauses would be
		// incorrect.
		if (!bind_data.limit_clause.empty()) {
			PushdownLimit(op->children[0]);
			return;
		}

		string clause;
		if (limit.limit_val.Type() != LimitNodeType::UNSET) {
			clause += " LIMIT " + std::to_string(limit.limit_val.GetConstantValue());
		}
		if (limit.offset_val.Type() != LimitNodeType::UNSET) {
			clause += " OFFSET " + std::to_string(limit.offset_val.GetConstantValue());
		}
		if (clause.empty()) {
			return;
		}

		bind_data.limit_clause = std::move(clause);

		// Absorb the LIMIT into the scan — the projections (if any)
		// between the LIMIT and the GET keep their place.
		op = std::move(op->children[0]);
		return;
	}

	for (auto &child : op->children) {
		PushdownLimit(child);
	}
}

} // namespace

void OpenDuckOptimizer::Optimize(OptimizerExtensionInput &input,
                                  unique_ptr<LogicalOperator> &plan) {
	(void)input;
	if (!plan) {
		return;
	}
	PushdownLimit(plan);
}

} // namespace openduck
