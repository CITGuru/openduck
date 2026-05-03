#pragma once

#include <string>
#include <vector>

#include "duckdb/common/reference_map.hpp"
#include "duckdb/execution/physical_operator.hpp"

#include "grpc_client.hpp" // for IngestColumnSpec

namespace duckdb {
class LogicalCreateTable;
class LogicalDelete;
class LogicalInsert;
class LogicalMergeInto;
class LogicalUpdate;
class PhysicalPlanGenerator;
} // namespace duckdb

namespace openduck {

class OpenDuckCatalog;

// ═══════════════════════════════════════════════════════════════════════════
// PhysicalOpenDuckIngestAndMutate
// ═══════════════════════════════════════════════════════════════════════════
//
// Cross-catalog DML operator. Acts as both a Sink (receiving DataChunks
// from the local source plan, encoded to Arrow IPC and streamed to the
// worker's `IngestData` RPC on a staging TEMP TABLE) and a Source
// (emitting the final apply-phase result — a BIGINT count row today;
// RETURNING rows in a future revision).
//
// Lifecycle:
//   1. `GetGlobalSinkState`: open gRPC client, resolve/acquire
//      `transaction_id`, open `IngestData` stream with metadata.
//   2. `Sink`: encode each incoming DataChunk → Arrow IPC, WriteBatch.
//   3. `Finalize`: close ingest stream (gets rows_ingested), run
//      `INSERT INTO target SELECT * FROM staging` on the pinned
//      connection, drop staging, commit if we opened the txn.
//   4. `GetDataInternal`: emit the single count row back to the executor.
//   5. Global sink destructor: if `Finalize` didn't succeed AND we
//      opened the txn, issue a best-effort rollback.
//
// V1 scope (documented in docs/design/attach-mutations.md §10.5):
//   * INSERT only. UPDATE / DELETE / CTAS with local sources still
//     throw `BinderException` with the `openduck_remote()` workaround
//     hint — their rewrite story is a further increment.
//   * RETURNING and ON CONFLICT are rejected at plan time with a
//     typed `BinderException`; they'd require either rewriting the
//     original SQL to reference staging (preserving those clauses) or
//     extending the apply-phase protocol, neither of which fit the
//     "land INSERT first" scope.
class PhysicalOpenDuckIngestAndMutate : public duckdb::PhysicalOperator {
public:
	static constexpr const duckdb::PhysicalOperatorType TYPE =
	    duckdb::PhysicalOperatorType::EXTENSION;

	PhysicalOpenDuckIngestAndMutate(
	    duckdb::PhysicalPlan &physical_plan,
	    duckdb::vector<duckdb::LogicalType> result_types,
	    duckdb::idx_t estimated_cardinality, OpenDuckCatalog &catalog,
	    std::string staging_table, std::vector<IngestColumnSpec> staging_cols,
	    std::string apply_sql, duckdb::vector<duckdb::LogicalType> child_types,
	    duckdb::vector<std::string> child_names);

	std::string GetName() const override {
		return "OPENDUCK_INGEST_AND_MUTATE";
	}

	// ── Sink interface ──
	duckdb::unique_ptr<duckdb::GlobalSinkState>
	GetGlobalSinkState(duckdb::ClientContext &context) const override;
	duckdb::unique_ptr<duckdb::LocalSinkState>
	GetLocalSinkState(duckdb::ExecutionContext &context) const override;
	duckdb::SinkResultType Sink(duckdb::ExecutionContext &context,
	                             duckdb::DataChunk &chunk,
	                             duckdb::OperatorSinkInput &input) const override;
	duckdb::SinkFinalizeType
	Finalize(duckdb::Pipeline &pipeline, duckdb::Event &event,
	         duckdb::ClientContext &context,
	         duckdb::OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	// ── Source interface ──
	duckdb::unique_ptr<duckdb::GlobalSourceState>
	GetGlobalSourceState(duckdb::ClientContext &context) const override;
	duckdb::SourceResultType
	GetDataInternal(duckdb::ExecutionContext &context,
	                duckdb::DataChunk &chunk,
	                duckdb::OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return false;
	}

	// Accessors used by the sink/source state classes defined in the
	// .cpp file.
	OpenDuckCatalog &Catalog() const {
		return catalog_.get();
	}
	const std::string &StagingTable() const {
		return staging_table_;
	}
	const std::vector<IngestColumnSpec> &StagingColumns() const {
		return staging_cols_;
	}
	const std::string &ApplySQL() const {
		return apply_sql_;
	}
	const duckdb::vector<duckdb::LogicalType> &ChildTypes() const {
		return child_types_;
	}
	const duckdb::vector<std::string> &ChildNames() const {
		return child_names_;
	}
	const duckdb::vector<duckdb::LogicalType> &ResultTypes() const {
		return types;
	}

private:
	duckdb::reference<OpenDuckCatalog> catalog_;
	std::string staging_table_;
	std::vector<IngestColumnSpec> staging_cols_;
	std::string apply_sql_;
	duckdb::vector<duckdb::LogicalType> child_types_;
	duckdb::vector<std::string> child_names_;
};

// ═══════════════════════════════════════════════════════════════════════════
// PhysicalOpenDuckSelectIngestAndMutate
// ═══════════════════════════════════════════════════════════════════════════
//
// Cross-catalog UPDATE / DELETE / CTAS operator. Unlike the INSERT path
// (which wires the local source plan as a Sink child), these statements
// have their local sources buried inside subqueries or WHERE clauses
// that the bound plan doesn't expose as a single streaming input.
//
// Lifecycle (pure Source, no Sink, no children):
//   1. On first `GetDataInternal`: open a fresh DuckDB Connection
//      against the same DatabaseInstance and, for each registered
//      local source, run `SELECT * FROM <qualified_local_ref>`,
//      encoding each emitted DataChunk into Arrow IPC and pushing
//      through an `IngestData` stream to a per-source staging
//      TEMP TABLE.
//   2. Run the rewritten apply SQL on the pinned transaction.
//   3. Drop all staging TEMP TABLEs.
//   4. Commit if we opened the transaction; rollback on failure
//      via the global-source-state destructor.
//   5. Buffer the apply-phase result chunks; subsequent calls yield them.

/// One local source to materialize into a staging TEMP TABLE.
struct SelectIngestSpec {
	/// Client-side SQL to execute (typically `SELECT * FROM <quoted
	/// catalog>.<schema>.<table>`).
	std::string local_select_sql;
	/// Name of the worker-side TEMP TABLE to create.
	std::string staging_table;
	/// Column spec for the TEMP TABLE.
	std::vector<IngestColumnSpec> columns;
	/// Arrow-encoder input types matching the local SELECT's output
	/// schema. We pass them explicitly (rather than inferring from
	/// the local QueryResult's types) so the encoder's Arrow schema
	/// is stable across iterations.
	std::vector<std::string> column_names_vec;
};

class PhysicalOpenDuckSelectIngestAndMutate : public duckdb::PhysicalOperator {
public:
	static constexpr const duckdb::PhysicalOperatorType TYPE =
	    duckdb::PhysicalOperatorType::EXTENSION;

	PhysicalOpenDuckSelectIngestAndMutate(
	    duckdb::PhysicalPlan &physical_plan,
	    duckdb::vector<duckdb::LogicalType> result_types,
	    duckdb::idx_t estimated_cardinality, OpenDuckCatalog &catalog,
	    std::vector<SelectIngestSpec> sources, std::string apply_sql);

	std::string GetName() const override {
		return "OPENDUCK_SELECT_INGEST_AND_MUTATE";
	}

	duckdb::unique_ptr<duckdb::GlobalSourceState>
	GetGlobalSourceState(duckdb::ClientContext &context) const override;
	duckdb::SourceResultType
	GetDataInternal(duckdb::ExecutionContext &context,
	                duckdb::DataChunk &chunk,
	                duckdb::OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return false;
	}

	OpenDuckCatalog &Catalog() const {
		return catalog_.get();
	}
	const std::vector<SelectIngestSpec> &Sources() const {
		return sources_;
	}
	const std::string &ApplySQL() const {
		return apply_sql_;
	}
	const duckdb::vector<duckdb::LogicalType> &ResultTypes() const {
		return types;
	}

private:
	duckdb::reference<OpenDuckCatalog> catalog_;
	std::vector<SelectIngestSpec> sources_;
	std::string apply_sql_;
};

// ═══════════════════════════════════════════════════════════════════════════
// Planner entry points (called from OpenDuckCatalog::Plan{Insert,Update,…})
// ═══════════════════════════════════════════════════════════════════════════

/// Build a cross-catalog INSERT operator.
duckdb::PhysicalOperator &BuildCrossCatalogInsert(
    duckdb::ClientContext &context,
    duckdb::PhysicalPlanGenerator &planner,
    duckdb::LogicalInsert &op,
    duckdb::PhysicalOperator &plan,
    OpenDuckCatalog &catalog);

/// Build a cross-catalog DELETE operator.
duckdb::PhysicalOperator &BuildCrossCatalogDelete(
    duckdb::ClientContext &context,
    duckdb::PhysicalPlanGenerator &planner,
    duckdb::LogicalDelete &op,
    OpenDuckCatalog &catalog);

/// Build a cross-catalog UPDATE operator.
duckdb::PhysicalOperator &BuildCrossCatalogUpdate(
    duckdb::ClientContext &context,
    duckdb::PhysicalPlanGenerator &planner,
    duckdb::LogicalUpdate &op,
    OpenDuckCatalog &catalog);

/// Build a cross-catalog CREATE TABLE AS SELECT operator.
duckdb::PhysicalOperator &BuildCrossCatalogCreateTableAs(
    duckdb::ClientContext &context,
    duckdb::PhysicalPlanGenerator &planner,
    duckdb::LogicalCreateTable &op,
    OpenDuckCatalog &catalog);

/// Build a cross-catalog `INSERT ... ON CONFLICT` (or explicit `MERGE
/// INTO`) operator. Re-parses the original SQL, substitutes the source
/// SELECT with a staging-table reference, and forwards the rewritten
/// statement (preserving the conflict target / DO clause / RETURNING
/// list) to the worker.
duckdb::PhysicalOperator &BuildCrossCatalogMerge(
    duckdb::ClientContext &context,
    duckdb::PhysicalPlanGenerator &planner,
    duckdb::LogicalMergeInto &op,
    duckdb::PhysicalOperator &plan,
    OpenDuckCatalog &catalog);

/// Legacy stub kept for call sites that still want the generic
/// "not-yet-implemented" error.
void ThrowCrossCatalogUnsupported(const char *op_name);

} // namespace openduck
