#include "openduck_ingest.hpp"

#include "arrow_bridge.hpp"
#include "catalog_reference_rewriter.hpp"
#include "datachunk_to_arrow.hpp"
#include "grpc_client.hpp"
#include "openduck_catalog.hpp"
#include "openduck_errors.hpp"
#include "openduck_extension.hpp"
#include "openduck_sql_emit.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

#include <arrow/record_batch.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>

namespace openduck {

using namespace duckdb;

// ═══════════════════════════════════════════════════════════════════════════
// Stubs for operations not yet wired to the cross-catalog path
// ═══════════════════════════════════════════════════════════════════════════

void ThrowCrossCatalogUnsupported(const char *op_name) {
	throw BinderException(
	    "cross-catalog %s on an ATTACH'ed OpenDuck database is not yet "
	    "supported by this client build (INSERT is handled; UPDATE / DELETE / "
	    "CTAS with local source plans still require rewriting the parsed "
	    "statement to reference a staging table and land in a follow-up). "
	    "Workaround: `openduck_remote('endpoint', 'token', '<SQL>')` forwards "
	    "raw SQL straight to the worker and covers every shape.",
	    op_name);
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

// SQL-text helpers (`GenerateStagingTable`, `QuoteIdentifier`,
// `QuoteLiteral`, `QuoteRef`, `TypeToSQL`) live in
// `openduck_sql_emit.{cpp,hpp}`. Including the header above brings
// them into scope in the `openduck` namespace.

// ═══════════════════════════════════════════════════════════════════════════
// Sink + Source state
// ═══════════════════════════════════════════════════════════════════════════

// Invariants for the cross-catalog ingest path
// (see docs/design/learnings-from-duckdb-postgres.md §5 + §6):
//
//  §5 ─ "drain-before-write":
//        The worker holds ONE `duckdb::Connection` per user transaction.
//        That connection cannot interleave a SELECT stream and an
//        IngestData write. Today's planner happens to satisfy this
//        because cross-catalog DML always materializes local sources
//        before opening the ingest stream (see
//        `BuildRewrittenSelectPlan` and the `materialize_local`
//        helpers below). Anyone adding a "scan-feeds-write on the
//        SAME catalog" plan shape MUST add an explicit materialization
//        guard or the worker connection will deadlock between the two
//        pipelines. duckdb-postgres has the same constraint and
//        enforces it via `MaterializePostgresScans`.
//
//  §6 ─ "single ingest stream per operator":
//        Each `PhysicalOpenDuckIngestAndMutate` instance opens its own
//        `IngestStream` in `GetGlobalSinkState` and closes it in
//        `Finalize`. This is fine when the planner emits one
//        cross-catalog mutation per query. If we ever fan out a MERGE
//        into multiple ingest-and-mutate ops sharing a transaction,
//        consider a `bool keep_ingest_open` knob on the op (mirroring
//        postgres' `keep_copy_alive`) so consecutive ops can reuse a
//        single staging stream.

namespace {

class IngestAndMutateGlobalSink : public GlobalSinkState {
public:
	IngestAndMutateGlobalSink(ClientContext &context,
	                          const PhysicalOpenDuckIngestAndMutate &op)
	    : op_ref(op) {
		auto &catalog = op.Catalog();
		auto &cfg = catalog.GetConfig();

		// 1. gRPC client + transaction pinning.
		client = std::make_unique<GrpcClient>(cfg.endpoint);
		transaction_id = EnsureAcquired(context, catalog);
		if (transaction_id.empty()) {
			// Outside a user BEGIN — open an implicit transaction for
			// the lifetime of this operator so the staging TEMP TABLE
			// + apply INSERT run on the same worker connection.
			transaction_id = client->BeginTransaction(cfg.database, cfg.token);
			owned_implicit_txn = true;
		}

		// 2. Open the IngestData stream. First chunk (metadata) is
		//    written eagerly here; subsequent batches are pumped in
		//    Sink().
		ingest = client->IngestData(cfg.database, op.StagingTable(),
		                             op.StagingColumns(), transaction_id, cfg.token);

		// 3. Prepare the DuckDB → Arrow encoder. We copy the types
		//    and names out of the operator so the encoder doesn't
		//    depend on the op's lifetime during Sink calls.
		encoder = std::make_unique<DataChunkToArrow>(context, op.ChildTypes(),
		                                              op.ChildNames());
	}

	~IngestAndMutateGlobalSink() override {
		// Best-effort rollback if we opened the txn but never reached
		// `Finalize`'s commit. Swallows errors — destructor.
		if (!finalized_ok && owned_implicit_txn && !transaction_id.empty()) {
			try {
				auto &cfg = op_ref.get().Catalog().GetConfig();
				client->RollbackTransaction(transaction_id, cfg.token);
			} catch (...) {
				// intentionally swallowed
			}
		}
	}

	void PumpChunk(DataChunk &chunk) {
		auto payload = encoder->Encode(chunk);
		if (!payload.empty()) {
			ingest->WriteBatch(payload);
		}
	}

	/// Close ingest stream, run apply INSERT on the pinned txn, drop
	/// staging, commit if we own the txn. On any error leaves
	/// `finalized_ok=false` so the destructor can rollback.
	void RunApplyAndCommit() {
		auto &catalog = op_ref.get().Catalog();
		auto &cfg = catalog.GetConfig();

		// Close the ingest stream — returns the total rows appended
		// into staging. Surfaces typed errors from the worker.
		rows_ingested = ingest->Finish();
		ingest.reset();

		// Run apply INSERT on the pinned connection. The result
		// stream is one Arrow IPC batch containing a single BIGINT
		// `Count` row.
		auto apply_stream = client->ExecuteSQL(
		    op_ref.get().ApplySQL(), cfg.database, cfg.token, transaction_id);
		while (auto ipc = apply_stream->Next()) {
			std::deque<std::shared_ptr<arrow::RecordBatch>> batches;
			ReadAllIpcBatches(*ipc, batches);
			for (auto &b : batches) {
				if (!b || b->num_rows() == 0) {
					continue;
				}
				auto out = make_uniq<DataChunk>();
				out->Initialize(Allocator::DefaultAllocator(),
				                 op_ref.get().ResultTypes());
				CopyBatchToDataChunk(b, *out);
				result_chunks.push_back(std::move(out));
			}
		}

		// Drop the staging TEMP TABLE. Belt-and-braces — it would
		// vanish on connection close anyway (TEMP TABLEs are
		// connection-scoped in DuckDB), but dropping explicitly keeps
		// the pinned connection tidy for callers that reuse it.
		auto drop_sql = std::string("DROP TABLE ") +
		                QuoteIdentifier(op_ref.get().StagingTable());
		auto drop_stream =
		    client->ExecuteSQL(drop_sql, cfg.database, cfg.token, transaction_id);
		while (drop_stream->Next()) {
		}

		if (owned_implicit_txn) {
			client->CommitTransaction(transaction_id, cfg.token);
		}
		finalized_ok = true;
	}

	duckdb::reference<const PhysicalOpenDuckIngestAndMutate> op_ref;

	std::unique_ptr<GrpcClient> client;
	std::unique_ptr<IngestStream> ingest;
	std::unique_ptr<DataChunkToArrow> encoder;

	std::string transaction_id;
	bool owned_implicit_txn = false;
	bool finalized_ok = false;

	std::mutex pump_lock;

	uint64_t rows_ingested = 0;
	std::deque<unique_ptr<DataChunk>> result_chunks;
};

class IngestAndMutateGlobalSource : public GlobalSourceState {
public:
	// The source state is a pure cursor over `sink_state.result_chunks`,
	// populated during Finalize.
	size_t cursor = 0;
	bool emitted_any = false;
};

class IngestAndMutateLocalSink : public LocalSinkState {};

} // namespace

// ═══════════════════════════════════════════════════════════════════════════
// PhysicalOpenDuckIngestAndMutate
// ═══════════════════════════════════════════════════════════════════════════

PhysicalOpenDuckIngestAndMutate::PhysicalOpenDuckIngestAndMutate(
    PhysicalPlan &physical_plan, vector<LogicalType> result_types,
    idx_t estimated_cardinality, OpenDuckCatalog &catalog, std::string staging_table,
    std::vector<IngestColumnSpec> staging_cols, std::string apply_sql,
    vector<LogicalType> child_types, vector<std::string> child_names)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION,
                        std::move(result_types), estimated_cardinality),
      catalog_(catalog), staging_table_(std::move(staging_table)),
      staging_cols_(std::move(staging_cols)), apply_sql_(std::move(apply_sql)),
      child_types_(std::move(child_types)), child_names_(std::move(child_names)) {
}

// ── Sink ──

unique_ptr<GlobalSinkState>
PhysicalOpenDuckIngestAndMutate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IngestAndMutateGlobalSink>(context, *this);
}

unique_ptr<LocalSinkState>
PhysicalOpenDuckIngestAndMutate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<IngestAndMutateLocalSink>();
}

SinkResultType PhysicalOpenDuckIngestAndMutate::Sink(ExecutionContext &context,
                                                      DataChunk &chunk,
                                                      OperatorSinkInput &input) const {
	auto &gsink = input.global_state.Cast<IngestAndMutateGlobalSink>();
	std::lock_guard<std::mutex> guard(gsink.pump_lock);
	gsink.PumpChunk(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalOpenDuckIngestAndMutate::Finalize(
    Pipeline &pipeline, Event &event, ClientContext &context,
    OperatorSinkFinalizeInput &input) const {
	auto &gsink = input.global_state.Cast<IngestAndMutateGlobalSink>();
	gsink.RunApplyAndCommit();
	return SinkFinalizeType::READY;
}

// ── Source ──

unique_ptr<GlobalSourceState>
PhysicalOpenDuckIngestAndMutate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<IngestAndMutateGlobalSource>();
}

SourceResultType
PhysicalOpenDuckIngestAndMutate::GetDataInternal(ExecutionContext &context,
                                                  DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &gsource = input.global_state.Cast<IngestAndMutateGlobalSource>();
	auto &gsink = sink_state->Cast<IngestAndMutateGlobalSink>();

	if (gsource.cursor < gsink.result_chunks.size()) {
		auto &next = gsink.result_chunks[gsource.cursor++];
		chunk.Move(*next);
		gsource.emitted_any = true;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	// Empty-stream defense: if the worker's apply reply arrived without
	// any rows (should not happen for a healthy worker), synthesize
	// `[rows_ingested]` as a BIGINT count row so downstream formatters
	// never see zero rows for an INSERT.
	if (!gsource.emitted_any && types.size() == 1 &&
	    types[0].id() == LogicalTypeId::BIGINT) {
		chunk.SetCardinality(1);
		chunk.data[0].SetValue(0, Value::BIGINT(static_cast<int64_t>(gsink.rows_ingested)));
		gsource.emitted_any = true;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

// ═══════════════════════════════════════════════════════════════════════════
// Planner entry
// ═══════════════════════════════════════════════════════════════════════════

namespace {

// Build the apply SQL by re-parsing the original INSERT, stripping the
// attached-catalog qualifier from the target, and replacing the source
// SELECT with `SELECT * FROM <staging>`. Preserves RETURNING, ON
// CONFLICT, column lists, and every other InsertStatement clause
// through `SQLStatement::ToString()`.
std::string BuildRewrittenInsertApplySQL(ClientContext &context, OpenDuckCatalog &catalog,
                                         const std::string &staging_table) {
	const auto &original = context.GetCurrentQuery();
	if (original.empty()) {
		throw BinderException(
		    "cross-catalog INSERT with RETURNING / ON CONFLICT on ATTACH "
		    "OpenDuck catalog `%s` was not issued through a SQL-text call "
		    "path; the programmatic-API fallback is not supported in this "
		    "build.",
		    catalog.GetName());
	}

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(original);
	if (parser.statements.empty()) {
		throw BinderException("unable to re-parse current query on OpenDuck catalog `%s`",
		                      catalog.GetName());
	}

	// Find the first INSERT in the script. Multi-statement scripts
	// where the hook fires on a later INSERT hit the same
	// statement-index caveat documented in §10.5 deviation #2; for
	// single-statement call paths (the hot path) this picks the right
	// one.
	InsertStatement *insert = nullptr;
	for (auto &stmt : parser.statements) {
		if (stmt && stmt->type == StatementType::INSERT_STATEMENT) {
			insert = &stmt->Cast<InsertStatement>();
			break;
		}
	}
	if (!insert) {
		throw BinderException(
		    "cross-catalog INSERT rewrite path could not locate an INSERT "
		    "statement in re-parsed SQL (catalog `%s`)",
		    catalog.GetName());
	}

	// Replace the source SELECT with `SELECT * FROM <staging>`.
	// The child physical plan in the operator produces data into
	// `<staging>` that matches what the original SELECT would have
	// produced; the rewritten SELECT just fans those rows back into
	// the worker-side INSERT.
	Parser staging_parser;
	staging_parser.ParseQuery("SELECT * FROM " + QuoteIdentifier(staging_table));
	if (staging_parser.statements.size() != 1 ||
	    staging_parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("failed to re-parse staging SELECT");
	}
	unique_ptr<SelectStatement> staging_select(
	    static_cast<SelectStatement *>(staging_parser.statements[0].release()));
	insert->select_statement = std::move(staging_select);
	insert->default_values = false;

	// Now run the CatalogReferenceRewriter to strip the attached
	// catalog from the INSERT target (and anything in RETURNING /
	// ON CONFLICT that references `r.col`-style qualified chains).
	CatalogReferenceRewriter rewriter(catalog.GetName(), catalog);
	rewriter.Visit(*insert);

	return insert->ToString();
}

} // namespace

PhysicalOperator &BuildCrossCatalogInsert(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalInsert &op, PhysicalOperator &plan,
                                          OpenDuckCatalog &catalog) {
	// Decide the apply-SQL shape first; the ResolveDefaultsProjection
	// policy depends on it.
	const bool complex_form =
	    op.return_chunk ||
	    op.on_conflict_info.action_type != OnConflictAction::THROW;

	// If the user omitted columns in the INSERT, DuckDB populates
	// `column_index_map` with per-column positions and expects
	// `ResolveDefaultsProjection` to fill the gaps with DEFAULT
	// expressions before the insert. For the synthesized apply
	// (`INSERT INTO target SELECT * FROM staging`) we need all
	// storage columns in storage order, so we run the projection. For
	// the rewrite-original apply the original INSERT's column list is
	// preserved verbatim, so the child plan's columns must match that
	// list — running ResolveDefaultsProjection would inflate staging
	// past what the rewritten INSERT expects.
	PhysicalOperator *child = &plan;
	if (!complex_form && !op.column_index_map.empty()) {
		child = &planner.ResolveDefaultsProjection(op, plan);
	}

	// Build the staging column spec from the child plan's output
	// types. Column names `col0, col1, ...` are arbitrary — the
	// apply SQL does positional `SELECT * FROM staging` so names
	// don't matter beyond keeping the worker's CREATE TEMP TABLE
	// parseable.
	std::vector<IngestColumnSpec> staging_cols;
	vector<std::string> child_names;
	staging_cols.reserve(child->types.size());
	child_names.reserve(child->types.size());
	for (idx_t i = 0; i < child->types.size(); i++) {
		IngestColumnSpec spec;
		spec.name = "col" + std::to_string(i);
		spec.sql_type = TypeToSQL(child->types[i]);
		staging_cols.push_back(spec);
		child_names.push_back(spec.name);
	}

	auto staging_table = GenerateStagingTable();

	// Two apply-SQL shapes:
	//   * "complex" (RETURNING / ON CONFLICT) → re-parse the original
	//     INSERT, substitute source with `SELECT * FROM <staging>`,
	//     preserve RETURNING / ON CONFLICT / column-list clauses via
	//     `SQLStatement::ToString()`.
	//   * "simple" (no RETURNING, no ON CONFLICT) → synthesize the
	//     apply directly from the target TableCatalogEntry. Avoids a
	//     re-parse and works even for programmatic-API INSERTs.
	std::string apply_sql;
	if (complex_form) {
		apply_sql = BuildRewrittenInsertApplySQL(context, catalog, staging_table);
	} else {
		auto &target = op.table;
		auto &target_schema = target.ParentSchema();
		std::ostringstream apply_ss;
		apply_ss << "INSERT INTO " << QuoteIdentifier(target_schema.name) << "."
		         << QuoteIdentifier(target.name) << " SELECT * FROM "
		         << QuoteIdentifier(staging_table);
		apply_sql = apply_ss.str();
	}

	// Result types: RETURNING projection when `return_chunk`, else a
	// single BIGINT count row (DuckDB's native "rows affected" shape).
	vector<LogicalType> result_types;
	if (op.return_chunk && !op.types.empty()) {
		result_types = op.types;
	} else {
		result_types = {LogicalType::BIGINT};
	}

	auto &result = planner.Make<PhysicalOpenDuckIngestAndMutate>(
	    std::move(result_types), op.estimated_cardinality, catalog,
	    std::move(staging_table), std::move(staging_cols), std::move(apply_sql),
	    vector<LogicalType>(child->types), std::move(child_names));
	result.children.push_back(*child);
	return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// PhysicalOpenDuckSelectIngestAndMutate (cross-catalog UPDATE/DELETE/CTAS)
// ═══════════════════════════════════════════════════════════════════════════

namespace {

/// Per-source local-select execution state. Collected into the global
/// source state, driven from a single-threaded `GetDataInternal`.
struct LocalSourceRuntime {
	SelectIngestSpec spec;
	std::vector<LogicalType> select_types;
	std::vector<std::string> select_names;
};

class SelectIngestAndMutateGlobalSource : public GlobalSourceState {
public:
	SelectIngestAndMutateGlobalSource(ClientContext &context,
	                                   const PhysicalOpenDuckSelectIngestAndMutate &op)
	    : op_ref(op), context(context) {
	}

	~SelectIngestAndMutateGlobalSource() override {
		if (!finalized_ok && owned_implicit_txn && !transaction_id.empty() && client) {
			try {
				auto &cfg = op_ref.get().Catalog().GetConfig();
				client->RollbackTransaction(transaction_id, cfg.token);
			} catch (...) {
				// intentionally swallowed
			}
		}
	}

	/// Run the full cross-catalog mutation on first entry. Subsequent
	/// entries (second and later `GetDataInternal`) just yield from
	/// the buffered result.
	void RunOnce() {
		if (initialized) {
			return;
		}
		initialized = true;
		auto &catalog = op_ref.get().Catalog();
		auto &cfg = catalog.GetConfig();

		// 1. gRPC client + transaction pinning.
		client = std::make_unique<GrpcClient>(cfg.endpoint);
		transaction_id = EnsureAcquired(context, catalog);
		if (transaction_id.empty()) {
			transaction_id = client->BeginTransaction(cfg.database, cfg.token);
			owned_implicit_txn = true;
		}

		// 2. For each local source: fresh local Connection, run SELECT,
		//    stream chunks into a per-source IngestData stream.
		for (const auto &spec : op_ref.get().Sources()) {
			IngestLocalSource(spec);
		}

		// 3. Run the apply on the pinned connection and buffer its
		//    results.
		auto apply_stream =
		    client->ExecuteSQL(op_ref.get().ApplySQL(), cfg.database, cfg.token,
		                        transaction_id);
		while (auto ipc = apply_stream->Next()) {
			std::deque<std::shared_ptr<arrow::RecordBatch>> batches;
			ReadAllIpcBatches(*ipc, batches);
			for (auto &b : batches) {
				if (!b || b->num_rows() == 0) {
					continue;
				}
				auto out = make_uniq<DataChunk>();
				out->Initialize(Allocator::DefaultAllocator(),
				                 op_ref.get().ResultTypes());
				CopyBatchToDataChunk(b, *out);
				result_chunks.push_back(std::move(out));
			}
		}

		// 4. Drop each staging TEMP TABLE.
		for (const auto &spec : op_ref.get().Sources()) {
			auto drop_sql =
			    std::string("DROP TABLE IF EXISTS ") + QuoteIdentifier(spec.staging_table);
			auto drop_stream =
			    client->ExecuteSQL(drop_sql, cfg.database, cfg.token, transaction_id);
			while (drop_stream->Next()) {
			}
		}

		// 5. Commit if we opened the transaction.
		if (owned_implicit_txn) {
			client->CommitTransaction(transaction_id, cfg.token);
		}
		finalized_ok = true;
	}

	void IngestLocalSource(const SelectIngestSpec &spec) {
		auto &catalog = op_ref.get().Catalog();
		auto &cfg = catalog.GetConfig();

		// Fresh Connection on the same DatabaseInstance. DuckDB's MVCC
		// means a concurrent reader is safe even while the enclosing
		// query is mid-execution.
		Connection sub(*context.db);
		auto result = sub.Query(spec.local_select_sql);
		if (result->HasError()) {
			throw BinderException(
			    "cross-catalog mutation: local SELECT failed: %s\n  SQL: %s",
			    result->GetError(), spec.local_select_sql);
		}

		// Open the IngestData stream with the per-source metadata.
		auto ingest = client->IngestData(cfg.database, spec.staging_table,
		                                  spec.columns, transaction_id, cfg.token);

		// Reuse the Arrow encoder. Types + names match the TEMP TABLE
		// DDL we just shipped in the metadata.
		// Use the live result's types (matches Arrow output exactly,
		// including DECIMAL scale). Names come from the staging spec
		// so they match the worker-side CREATE TEMP TABLE.
		duckdb::vector<LogicalType> types_copy;
		duckdb::vector<std::string> names_copy;
		types_copy.reserve(spec.columns.size());
		names_copy.reserve(spec.columns.size());
		for (auto &c : spec.columns) {
			names_copy.push_back(c.name);
		}
		for (idx_t i = 0; i < result->types.size(); i++) {
			types_copy.push_back(result->types[i]);
		}
		DataChunkToArrow encoder(context, types_copy, names_copy);

		while (true) {
			auto chunk = result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			auto ipc = encoder.Encode(*chunk);
			if (!ipc.empty()) {
				ingest->WriteBatch(ipc);
			}
		}
		ingest->Finish();
	}

	duckdb::reference<const PhysicalOpenDuckSelectIngestAndMutate> op_ref;
	ClientContext &context;

	std::unique_ptr<GrpcClient> client;
	std::string transaction_id;
	bool owned_implicit_txn = false;
	bool initialized = false;
	bool finalized_ok = false;

	std::deque<unique_ptr<DataChunk>> result_chunks;
	size_t cursor = 0;
	bool emitted_any = false;
};

} // namespace

PhysicalOpenDuckSelectIngestAndMutate::PhysicalOpenDuckSelectIngestAndMutate(
    PhysicalPlan &physical_plan, vector<LogicalType> result_types,
    idx_t estimated_cardinality, OpenDuckCatalog &catalog,
    std::vector<SelectIngestSpec> sources, std::string apply_sql)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION,
                        std::move(result_types), estimated_cardinality),
      catalog_(catalog), sources_(std::move(sources)),
      apply_sql_(std::move(apply_sql)) {
}

unique_ptr<GlobalSourceState>
PhysicalOpenDuckSelectIngestAndMutate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<SelectIngestAndMutateGlobalSource>(context, *this);
}

SourceResultType PhysicalOpenDuckSelectIngestAndMutate::GetDataInternal(
    ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gsource = input.global_state.Cast<SelectIngestAndMutateGlobalSource>();
	gsource.RunOnce();

	if (gsource.cursor < gsource.result_chunks.size()) {
		auto &next = gsource.result_chunks[gsource.cursor++];
		chunk.Move(*next);
		gsource.emitted_any = true;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	// Empty-stream defense for non-RETURNING mutations.
	if (!gsource.emitted_any && types.size() == 1 &&
	    types[0].id() == LogicalTypeId::BIGINT) {
		chunk.SetCardinality(1);
		chunk.data[0].SetValue(0, Value::BIGINT(0));
		gsource.emitted_any = true;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	chunk.SetCardinality(0);
	return SourceResultType::FINISHED;
}

// ═══════════════════════════════════════════════════════════════════════════
// Cross-catalog UPDATE/DELETE/CTAS planner entries
// ═══════════════════════════════════════════════════════════════════════════

namespace {

/// A single non-remote `LogicalGet` collected during planning.
struct LocalTableFound {
	std::string catalog;
	std::string schema;
	std::string table;
	std::vector<LogicalType> column_types;
	std::vector<std::string> column_names;
};

// Walk a parsed SQLStatement tree, emit every BaseTableRef's
// (catalog, schema, table) triple via `emit`. Recurses into CTE bodies,
// subqueries, joins, and every nested structural node the
// CatalogReferenceRewriter visits.
class BaseTableRefCollector {
public:
	explicit BaseTableRefCollector(std::function<void(BaseTableRef &)> emit)
	    : emit_(std::move(emit)) {
	}

	void Visit(SQLStatement &stmt) {
		VisitStatement(stmt);
	}

private:
	std::function<void(BaseTableRef &)> emit_;

	void VisitStatement(SQLStatement &stmt) {
		switch (stmt.type) {
		case StatementType::SELECT_STATEMENT:
			VisitSelect(stmt.Cast<SelectStatement>());
			break;
		case StatementType::INSERT_STATEMENT: {
			auto &ins = stmt.Cast<InsertStatement>();
			if (ins.table_ref) {
				VisitTableRef(*ins.table_ref);
			}
			if (ins.select_statement) {
				VisitSelect(*ins.select_statement);
			}
			VisitCTEMap(ins.cte_map);
			break;
		}
		case StatementType::UPDATE_STATEMENT: {
			auto &upd = stmt.Cast<UpdateStatement>();
			if (upd.table) {
				VisitTableRef(*upd.table);
			}
			if (upd.from_table) {
				VisitTableRef(*upd.from_table);
			}
			if (upd.set_info) {
				for (auto &expr : upd.set_info->expressions) {
					if (expr) {
						VisitExpr(*expr);
					}
				}
				if (upd.set_info->condition) {
					VisitExpr(*upd.set_info->condition);
				}
			}
			VisitCTEMap(upd.cte_map);
			break;
		}
		case StatementType::DELETE_STATEMENT: {
			auto &del = stmt.Cast<DeleteStatement>();
			if (del.table) {
				VisitTableRef(*del.table);
			}
			for (auto &u : del.using_clauses) {
				if (u) {
					VisitTableRef(*u);
				}
			}
			if (del.condition) {
				VisitExpr(*del.condition);
			}
			VisitCTEMap(del.cte_map);
			break;
		}
		case StatementType::CREATE_STATEMENT: {
			auto &cre = stmt.Cast<CreateStatement>();
			if (cre.info && cre.info->type == CatalogType::TABLE_ENTRY) {
				auto &ti = cre.info->Cast<CreateTableInfo>();
				if (ti.query) {
					VisitSelect(*ti.query);
				}
			}
			break;
		}
		default:
			break;
		}
	}

	void VisitSelect(SelectStatement &sel) {
		if (sel.node) {
			VisitQueryNode(*sel.node);
		}
	}

	void VisitQueryNode(QueryNode &node) {
		// Walk CTE bodies.
		for (auto &kv : node.cte_map.map) {
			if (kv.second && kv.second->query) {
				VisitSelect(*kv.second->query);
			}
		}
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    node,
		    [&](unique_ptr<ParsedExpression> &child) {
			    if (child) {
				    VisitExpr(*child);
			    }
		    },
		    [&](TableRef &ref) { VisitTableRef(ref); });
	}

	void VisitCTEMap(CommonTableExpressionMap &cte_map) {
		for (auto &kv : cte_map.map) {
			if (kv.second && kv.second->query) {
				VisitSelect(*kv.second->query);
			}
		}
	}

	void VisitTableRef(TableRef &ref) {
		if (ref.type == TableReferenceType::BASE_TABLE) {
			auto &base = ref.Cast<BaseTableRef>();
			emit_(base);
		} else if (ref.type == TableReferenceType::SUBQUERY) {
			auto &sq = ref.Cast<SubqueryRef>();
			if (sq.subquery) {
				VisitSelect(*sq.subquery);
			}
		}
	}

	void VisitExpr(ParsedExpression &expr) {
		// SubqueryExpression carries a SelectStatement whose body is
		// not reached by `EnumerateQueryNodeChildren`; we must descend
		// manually so a local BaseTableRef inside a
		// `WHERE id IN (SELECT … FROM local.t)` subquery is found.
		if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
			auto &sq = expr.Cast<SubqueryExpression>();
			if (sq.subquery) {
				VisitSelect(*sq.subquery);
			}
		}
		ParsedExpressionIterator::EnumerateChildren(
		    expr, [&](ParsedExpression &child) { VisitExpr(child); });
	}
};

/// Resolve a parsed (catalog, schema, table) triple against the client
/// context's catalog search path. Returns the TableCatalogEntry if it
/// resolves to a persistent table outside `remote_catalog`; returns
/// nullptr otherwise (including when the table is in `remote_catalog`).
optional_ptr<TableCatalogEntry>
ResolveLocalTable(ClientContext &context, OpenDuckCatalog &remote_catalog,
                   const std::string &cat, const std::string &schema,
                   const std::string &table) {
	try {
		// `Catalog::GetEntry` with a specific catalog name requires that
		// name to be non-empty; use the overload-friendly variant that
		// accepts empty catalog / schema (falls back to default search).
		auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, cat, schema, table);
		if (entry.type != CatalogType::TABLE_ENTRY) {
			return nullptr;
		}
		auto &table_entry = entry.Cast<TableCatalogEntry>();
		if (&table_entry.ParentCatalog() == static_cast<Catalog *>(&remote_catalog)) {
			return nullptr;
		}
		return &table_entry;
	} catch (...) {
		return nullptr;
	}
}

/// Walk parsed statement for local BaseTableRefs and return the
/// corresponding LocalTableFound records (deduped by catalog/schema/table).
std::vector<LocalTableFound>
CollectLocalTablesFromParsed(ClientContext &context, OpenDuckCatalog &remote_catalog,
                              SQLStatement &stmt) {
	std::unordered_map<std::string, LocalTableFound> dedup;
	std::vector<std::string> order;

	BaseTableRefCollector collector([&](BaseTableRef &ref) {
		auto entry = ResolveLocalTable(context, remote_catalog, ref.catalog_name,
		                                ref.schema_name, ref.table_name);
		if (!entry) {
			return;
		}
		auto &te = *entry;
		// Use the resolved catalog/schema names (which may differ from
		// the parsed ones — e.g. empty schema resolves to "main").
		auto resolved_cat = te.ParentCatalog().GetName();
		auto resolved_schema = te.ParentSchema().name;
		auto resolved_table = te.name;
		auto key = StringUtil::Lower(resolved_cat) + "|" +
		            StringUtil::Lower(resolved_schema) + "|" +
		            StringUtil::Lower(resolved_table);
		if (dedup.count(key)) {
			return;
		}
		LocalTableFound f;
		f.catalog = resolved_cat;
		f.schema = resolved_schema;
		f.table = resolved_table;
		for (auto &col : te.GetColumns().Physical()) {
			f.column_names.push_back(col.Name());
			f.column_types.push_back(col.Type());
		}
		dedup[key] = f;
		order.push_back(key);
	});
	collector.Visit(stmt);

	std::vector<LocalTableFound> result;
	result.reserve(order.size());
	for (auto &k : order) {
		result.push_back(dedup[k]);
	}
	return result;
}

/// Shared planner: re-parse the original SQL, run the rewriter with
/// per-local-table substitutions, produce (apply_sql, SelectIngestSpec[]).
/// Used by cross-catalog DELETE / UPDATE / CTAS.
struct CrossCatalogSelectPlan {
	std::string apply_sql;
	std::vector<SelectIngestSpec> sources;
};

CrossCatalogSelectPlan
BuildRewrittenSelectPlan(ClientContext &context, OpenDuckCatalog &catalog,
                          LogicalOperator &op, const char *op_label) {
	(void)op; // unused — local tables are discovered by re-parsing the SQL
	         // since `op.children[...].bind_data` has already been moved
	         // into the physical plan by the time PlanDelete / PlanUpdate
	         // / PlanCreateTableAs fires.

	const auto &original = context.GetCurrentQuery();
	if (original.empty()) {
		throw BinderException(
		    "cross-catalog %s on ATTACH OpenDuck catalog `%s` was not "
		    "issued through a SQL-text call path; the programmatic-API "
		    "fallback is not supported. Workaround: `openduck_remote('<SQL>')`.",
		    op_label, catalog.GetName());
	}

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(original);
	if (parser.statements.empty()) {
		throw BinderException(
		    "cross-catalog %s: unable to re-parse original query on catalog `%s`",
		    op_label, catalog.GetName());
	}
	auto &stmt = *parser.statements.front();

	auto locals = CollectLocalTablesFromParsed(context, catalog, stmt);
	if (locals.empty()) {
		throw BinderException(
		    "cross-catalog %s triggered but no non-remote tables were found "
		    "in the re-parsed statement. This can happen if the non-remote "
		    "reference is a table function (e.g. read_csv / read_parquet) — "
		    "not yet supported by the client-side ingest. Workaround: "
		    "materialize the local source into a real local table first, "
		    "or use `openduck_remote('<SQL>')`.",
		    op_label);
	}

	CrossCatalogSelectPlan plan;
	CatalogReferenceRewriter rewriter(catalog.GetName(), catalog);
	for (auto &f : locals) {
		SelectIngestSpec spec;
		spec.staging_table = GenerateStagingTable();
		spec.local_select_sql = "SELECT * FROM " + QuoteRef(f.catalog, f.schema, f.table);
		for (size_t i = 0; i < f.column_types.size(); i++) {
			IngestColumnSpec c;
			c.name = f.column_names[i];
			c.sql_type = TypeToSQL(f.column_types[i]);
			spec.columns.push_back(c);
			spec.column_names_vec.push_back(f.column_names[i]);
		}
		rewriter.AddSubstitution(f.catalog, f.schema, f.table, spec.staging_table);
		// Also register the BaseTableRef shape the user wrote. Tables
		// specified as bare `src` (with empty catalog/schema) resolve
		// to `<main_cat>.main.src` via the search path; the substitution
		// map needs BOTH the resolved triple AND the empty-catalog /
		// empty-schema variants so the rewriter catches every
		// BaseTableRef the user wrote.
		rewriter.AddSubstitution(std::string(), std::string(), f.table, spec.staging_table);
		rewriter.AddSubstitution(std::string(), f.schema, f.table, spec.staging_table);
		rewriter.AddSubstitution(f.catalog, std::string(), f.table, spec.staging_table);
		plan.sources.push_back(std::move(spec));
	}
	rewriter.Visit(stmt);
	plan.apply_sql = stmt.ToString();
	return plan;
}

} // namespace

PhysicalOperator &BuildCrossCatalogDelete(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalDelete &op,
                                          OpenDuckCatalog &catalog) {
	auto plan = BuildRewrittenSelectPlan(context, catalog, op, "DELETE");
	vector<LogicalType> result_types;
	if (op.return_chunk && !op.types.empty()) {
		result_types = op.types;
	} else {
		result_types = {LogicalType::BIGINT};
	}
	return planner.Make<PhysicalOpenDuckSelectIngestAndMutate>(
	    std::move(result_types), op.estimated_cardinality, catalog,
	    std::move(plan.sources), std::move(plan.apply_sql));
}

PhysicalOperator &BuildCrossCatalogUpdate(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalUpdate &op,
                                          OpenDuckCatalog &catalog) {
	auto plan = BuildRewrittenSelectPlan(context, catalog, op, "UPDATE");
	vector<LogicalType> result_types;
	if (op.return_chunk && !op.types.empty()) {
		result_types = op.types;
	} else {
		result_types = {LogicalType::BIGINT};
	}
	return planner.Make<PhysicalOpenDuckSelectIngestAndMutate>(
	    std::move(result_types), op.estimated_cardinality, catalog,
	    std::move(plan.sources), std::move(plan.apply_sql));
}

PhysicalOperator &BuildCrossCatalogCreateTableAs(ClientContext &context,
                                                  PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op,
                                                  OpenDuckCatalog &catalog) {
	auto plan = BuildRewrittenSelectPlan(context, catalog, op, "CREATE TABLE AS");
	// CTAS returns a single BIGINT "Count" row by convention.
	vector<LogicalType> result_types = {LogicalType::BIGINT};
	return planner.Make<PhysicalOpenDuckSelectIngestAndMutate>(
	    std::move(result_types), op.estimated_cardinality, catalog,
	    std::move(plan.sources), std::move(plan.apply_sql));
}

PhysicalOperator &BuildCrossCatalogMerge(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalMergeInto &op,
                                          PhysicalOperator &plan,
                                          OpenDuckCatalog &catalog) {
	// `INSERT ... ON CONFLICT` (and explicit `MERGE INTO`) reaches us
	// through `Catalog::PlanMergeInto`. The bound `plan` is the source
	// SELECT — but the merge planner appends extra trailing columns
	// (row_id_start, source_marker) that the INSERT-shaped apply SQL
	// doesn't expect. We re-parse the original INSERT to discover the
	// real source-column count (taken from the user's INSERT target
	// column list, or all-columns if implicit), and use only the
	// LEADING `n` columns of the bound child plan as the staging
	// schema.
	const auto &original = context.GetCurrentQuery();
	if (original.empty()) {
		throw BinderException(
		    "cross-catalog INSERT ... ON CONFLICT on ATTACH OpenDuck catalog "
		    "`%s` was not issued through a SQL-text call path; the "
		    "programmatic-API fallback is not supported in this build. "
		    "Workaround: `openduck_remote('<SQL>')`.",
		    catalog.GetName());
	}

	Parser src_parser(context.GetParserOptions());
	src_parser.ParseQuery(original);
	if (src_parser.statements.empty() ||
	    src_parser.statements.front()->type != StatementType::INSERT_STATEMENT) {
		throw BinderException(
		    "cross-catalog MERGE INTO bridge expected an `INSERT ... ON "
		    "CONFLICT` statement (the binder generates LogicalMergeInto for "
		    "those); got something else. Workaround: use `openduck_remote()`.");
	}
	auto &src_insert = src_parser.statements.front()->Cast<InsertStatement>();

	// Number of source columns the apply INSERT expects:
	//   * explicit column list (`INSERT INTO t (a, b)`) → that count.
	//   * implicit (`INSERT INTO t`) → number of physical columns on
	//     the target.
	idx_t source_col_count = src_insert.columns.empty()
	                              ? op.table.GetColumns().PhysicalColumnCount()
	                              : src_insert.columns.size();
	if (source_col_count == 0 || source_col_count > plan.types.size()) {
		throw BinderException(
		    "cross-catalog INSERT ... ON CONFLICT: source plan has %llu "
		    "columns but original INSERT expects %llu — refusing to ingest.",
		    (unsigned long long)plan.types.size(),
		    (unsigned long long)source_col_count);
	}

	auto staging_table = GenerateStagingTable();

	std::vector<IngestColumnSpec> staging_cols;
	vector<std::string> child_names;
	vector<LogicalType> child_types;
	staging_cols.reserve(source_col_count);
	child_names.reserve(source_col_count);
	child_types.reserve(source_col_count);
	for (idx_t i = 0; i < source_col_count; i++) {
		IngestColumnSpec spec;
		spec.name = "col" + std::to_string(i);
		spec.sql_type = TypeToSQL(plan.types[i]);
		staging_cols.push_back(spec);
		child_names.push_back(spec.name);
		child_types.push_back(plan.types[i]);
	}
	// We hand the operator the FULL `plan.types` for the encoder so
	// it doesn't reject merge-internal trailing columns; the encoder
	// simply truncates to the staging-column count via the per-spec
	// width, and downstream INSERT consumes the leading source_col_count.
	// Most cleanly: project the child to drop trailing columns.
	auto &projected = planner.Make<duckdb::PhysicalProjection>(
	    child_types,
	    [&]() {
		    vector<unique_ptr<duckdb::Expression>> exprs;
		    for (idx_t i = 0; i < source_col_count; i++) {
			    exprs.push_back(make_uniq<duckdb::BoundReferenceExpression>(plan.types[i], i));
		    }
		    return exprs;
	    }(),
	    op.estimated_cardinality);
	projected.children.push_back(plan);

	auto apply_sql = BuildRewrittenInsertApplySQL(context, catalog, staging_table);

	vector<LogicalType> result_types;
	if (op.return_chunk && !op.types.empty()) {
		result_types = op.types;
	} else {
		result_types = {LogicalType::BIGINT};
	}

	auto &result = planner.Make<PhysicalOpenDuckIngestAndMutate>(
	    std::move(result_types), op.estimated_cardinality, catalog,
	    std::move(staging_table), std::move(staging_cols), std::move(apply_sql),
	    std::move(child_types), std::move(child_names));
	result.children.push_back(projected);
	return result;
}

} // namespace openduck
