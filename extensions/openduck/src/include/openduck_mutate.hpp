#pragma once

#include <string>
#include <vector>

#include "duckdb/common/reference_map.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace openduck {

class OpenDuckCatalog;

// ═══════════════════════════════════════════════════════════════════════════
// PhysicalOpenDuckMutate
// ═══════════════════════════════════════════════════════════════════════════
//
// Pure-remote mutation operator. Runs a fully-rewritten SQL statement on
// the worker connection bound to the caller's current transaction (or
// a fresh auto-commit connection if there's no active
// `OpenDuckTransaction`) and streams the worker's Arrow IPC reply back
// as DataChunks.
//
// Output schema:
//   * `return_chunk == true`  → `op.types` (the RETURNING projection)
//   * `return_chunk == false` → `{ BIGINT }` (the rows-affected "Count" row
//                                             DuckDB's workers emit natively)
//
// Used for every mutation where `HasNonRemoteReference` returned false:
// INSERT / UPDATE / DELETE / CTAS with only remote scans, and COPY
// variants that are forwarded as-is.
class PhysicalOpenDuckMutate : public duckdb::PhysicalOperator {
public:
	static constexpr const duckdb::PhysicalOperatorType TYPE =
	    duckdb::PhysicalOperatorType::EXTENSION;

public:
	PhysicalOpenDuckMutate(duckdb::PhysicalPlan &physical_plan,
	                       duckdb::vector<duckdb::LogicalType> types,
	                       duckdb::idx_t estimated_cardinality,
	                       std::string worker_sql,
	                       OpenDuckCatalog &catalog);

	std::string GetName() const override {
		return "OPENDUCK_MUTATE";
	}

	// ── Source interface ──
	duckdb::unique_ptr<duckdb::GlobalSourceState>
	GetGlobalSourceState(duckdb::ClientContext &context) const override;

	duckdb::SourceResultType
	GetDataInternal(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
	                duckdb::OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	bool ParallelSource() const override {
		return false;
	}

	const std::string &WorkerSQL() const {
		return worker_sql_;
	}

private:
	std::string worker_sql_;
	duckdb::reference<OpenDuckCatalog> catalog_;
};

} // namespace openduck
