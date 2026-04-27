#include "openduck_catalog.hpp"
#include "openduck_schema.hpp"
#include "openduck_errors.hpp"
#include "openduck_has_nonremote_reference.hpp"
#include "openduck_ingest.hpp"
#include "openduck_mutate.hpp"
#include "catalog_reference_rewriter.hpp"
#include "grpc_client.hpp"
#include "arrow_bridge.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_context.hpp"

namespace openduck {

using namespace duckdb;

// ═══════════════════════════════════════════════════════════════════════════
// OpenDuckCatalog
// ═══════════════════════════════════════════════════════════════════════════

OpenDuckCatalog::OpenDuckCatalog(AttachedDatabase &db, AttachConfig config)
    : Catalog(db), config_(std::move(config)) {
}

OpenDuckCatalog::~OpenDuckCatalog() = default;

GrpcClient &OpenDuckCatalog::GetClient() {
	if (!client_) {
		client_ = std::make_unique<GrpcClient>(config_.endpoint);
	}
	return *client_;
}

void OpenDuckCatalog::Initialize(bool load_builtin) {
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	main_schema_ = make_uniq<OpenDuckSchemaEntry>(*this, info);
}

void OpenDuckCatalog::Initialize(optional_ptr<ClientContext> context, bool load_builtin) {
	Initialize(load_builtin);
}

optional_ptr<SchemaCatalogEntry> OpenDuckCatalog::LookupSchema(CatalogTransaction transaction,
                                                                const EntryLookupInfo &schema_lookup,
                                                                OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	if (schema_name == DEFAULT_SCHEMA || schema_name == "main") {
		return main_schema_.get();
	}
	if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException("Schema \"%s\" not found in OpenDuck catalog", schema_name);
	}
	return nullptr;
}

void OpenDuckCatalog::ScanSchemas(ClientContext &context,
                                   std::function<void(SchemaCatalogEntry &)> callback) {
	if (main_schema_) {
		callback(*main_schema_);
	}
}

bool OpenDuckCatalog::HasSchemaOrTable(const std::string &schema, const std::string &table) {
	if (!main_schema_) {
		return false;
	}
	// `main` is the only schema this catalog currently exposes
	// (LookupSchema aliases DEFAULT_SCHEMA → main_schema_). We match
	// either the literal `main` or DuckDB's DEFAULT_SCHEMA constant
	// case-insensitively.
	if (!schema.empty() && !StringUtil::CIEquals(schema, "main") &&
	    !StringUtil::CIEquals(schema, DEFAULT_SCHEMA)) {
		return false;
	}
	return main_schema_->HasCachedTable(table);
}

optional_ptr<CatalogEntry> OpenDuckCatalog::CreateSchema(CatalogTransaction transaction,
                                                          CreateSchemaInfo &info) {
	EnsureWritable(*this);
	if (!transaction.context) {
		throw BinderException("CREATE SCHEMA requires an active ClientContext on ATTACH "
		                      "OpenDuck catalogs; programmatic call path is not supported.");
	}
	ForwardDDL(*transaction.context, *this);
	// OpenDuckSchemaEntry currently only exposes `main`; the worker now
	// knows about the new schema, but LookupSchema still only returns
	// `main`. Users can still query the new schema via
	// `openduck_remote('<SQL>')`.
	return nullptr;
}

DatabaseSize OpenDuckCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	size.bytes = 0;
	size.wal_size = 0;
	return size;
}

bool OpenDuckCatalog::InMemory() {
	return true;
}

string OpenDuckCatalog::GetDBPath() {
	return config_.endpoint;
}

namespace {

// Output schema for a mutation op: `op.types` when RETURNING is in
// play, otherwise the single-column BIGINT rows-affected row that
// DuckDB's workers emit natively.
vector<LogicalType> MutationResultTypes(bool return_chunk, const vector<LogicalType> &op_types) {
	if (return_chunk && !op_types.empty()) {
		return op_types;
	}
	return {LogicalType::BIGINT};
}

PhysicalOperator &BuildMutate(ClientContext &context, PhysicalPlanGenerator &planner,
                              OpenDuckCatalog &catalog, vector<LogicalType> result_types,
                              idx_t estimated_cardinality) {
	auto worker_sql = BuildForwardedSQL(context, catalog);
	return planner.Make<PhysicalOpenDuckMutate>(std::move(result_types), estimated_cardinality,
	                                            std::move(worker_sql), catalog);
}

} // namespace

PhysicalOperator &OpenDuckCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                      LogicalCreateTable &op, PhysicalOperator &plan) {
	EnsureWritable(*this);
	if (HasNonRemoteReference(op, *this)) {
		return BuildCrossCatalogCreateTableAs(context, planner, op, *this);
	}
	return BuildMutate(context, planner, *this, {LogicalType::BIGINT},
	                   op.estimated_cardinality);
}

PhysicalOperator &OpenDuckCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	EnsureWritable(*this);
	if (HasNonRemoteReference(op, *this)) {
		if (!plan) {
			throw BinderException(
			    "cross-catalog INSERT requires a source plan; got INSERT ... "
			    "VALUES style without a child operator. This should not happen "
			    "for a bound INSERT — please file a bug with the failing SQL.");
		}
		return BuildCrossCatalogInsert(context, planner, op, *plan, *this);
	}
	return BuildMutate(context, planner, *this, MutationResultTypes(op.return_chunk, op.types),
	                   op.estimated_cardinality);
}

PhysicalOperator &OpenDuckCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalDelete &op, PhysicalOperator &plan) {
	EnsureWritable(*this);
	if (HasNonRemoteReference(op, *this)) {
		return BuildCrossCatalogDelete(context, planner, op, *this);
	}
	return BuildMutate(context, planner, *this, MutationResultTypes(op.return_chunk, op.types),
	                   op.estimated_cardinality);
}

PhysicalOperator &OpenDuckCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalUpdate &op, PhysicalOperator &plan) {
	EnsureWritable(*this);
	if (HasNonRemoteReference(op, *this)) {
		return BuildCrossCatalogUpdate(context, planner, op, *this);
	}
	return BuildMutate(context, planner, *this, MutationResultTypes(op.return_chunk, op.types),
	                   op.estimated_cardinality);
}

PhysicalOperator &OpenDuckCatalog::PlanMergeInto(ClientContext &context,
                                                  PhysicalPlanGenerator &planner,
                                                  LogicalMergeInto &op,
                                                  PhysicalOperator &plan) {
	EnsureWritable(*this);
	// `INSERT ... ON CONFLICT` (and explicit `MERGE INTO`) get bound
	// into a `LogicalMergeInto`. We forward the original SQL to the
	// worker via the cross-catalog ingest pipeline: stream the source
	// (the merge's child plan) into a staging TEMP TABLE, then apply
	// the rewritten `INSERT ... ON CONFLICT ...` (with source
	// re-pointed at staging) on the pinned connection. The worker's
	// DuckDB executes the MergeInto natively against the pinned
	// connection — preserving every ON CONFLICT semantic.
	return BuildCrossCatalogMerge(context, planner, op, plan, *this);
}

void OpenDuckCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	EnsureWritable(*this);
	ForwardDDL(context, *this);
}

// ── SQL-forwarding helpers ─────────────────────────────────────────────────

string BuildForwardedSQL(ClientContext &context, OpenDuckCatalog &catalog) {
	const auto &original = context.GetCurrentQuery();
	if (original.empty()) {
		throw BinderException(
		    "mutation against ATTACH OpenDuck catalog `%s` was not issued through "
		    "a SQL-text call path; the programmatic-API fallback is not "
		    "supported in this build. Workaround: `openduck_remote('<SQL>')`.",
		    catalog.GetName());
	}
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(original);
	if (parser.statements.empty()) {
		throw BinderException("unable to re-parse current query on OpenDuck catalog `%s`",
		                      catalog.GetName());
	}
	// For scripts containing multiple statements, DuckDB calls each
	// catalog hook exactly once per statement. `GetCurrentStatementIndex`
	// would be the ideal selector but isn't exposed on ClientContext;
	// we fall back to the first statement which is correct for every
	// single-statement call path (the overwhelming common case). A
	// future enhancement can swap in the precise index once DuckDB
	// exposes it.
	auto &stmt = *parser.statements.front();
	CatalogReferenceRewriter rewriter(catalog.GetName(), catalog);
	rewriter.Visit(stmt);
	return stmt.ToString();
}

void ForwardDDL(ClientContext &context, OpenDuckCatalog &catalog) {
	auto worker_sql = BuildForwardedSQL(context, catalog);
	auto transaction_id = EnsureAcquired(context, catalog);
	auto &config = catalog.GetConfig();
	auto &client = catalog.GetClient();
	auto stream = client.ExecuteSQL(worker_sql, config.database, config.token, transaction_id);
	// Drain the stream — every success path emits a single Arrow batch
	// we don't need to consume, then FINISHED. Any error will throw
	// a typed exception from `GrpcStream::Next`.
	while (stream->Next()) {
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// OpenDuckTransaction / EnsureAcquired
// ═══════════════════════════════════════════════════════════════════════════

OpenDuckTransaction::OpenDuckTransaction(TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

string EnsureAcquired(ClientContext &context, OpenDuckCatalog &catalog) {
	auto &transaction = Transaction::Get(context, catalog);
	auto &otx = transaction.Cast<OpenDuckTransaction>();
	if (otx.acquired) {
		return otx.transaction_id;
	}
	// Lazy acquisition: only fire `BeginTransaction` inside an explicit
	// user BEGIN/COMMIT. Outside of one, DuckDB wraps each statement
	// in its own auto-commit meta-transaction and the worker handles
	// auto-commit on its side.
	if (!context.transaction.IsAutoCommit()) {
		auto &config = catalog.GetConfig();
		auto &client = catalog.GetClient();
		otx.transaction_id = client.BeginTransaction(config.database, config.token);
		otx.acquired = true;
		return otx.transaction_id;
	}
	return string();
}

// ═══════════════════════════════════════════════════════════════════════════
// OpenDuckTransactionManager
// ═══════════════════════════════════════════════════════════════════════════

OpenDuckTransactionManager::OpenDuckTransactionManager(AttachedDatabase &db, OpenDuckCatalog &catalog)
    : TransactionManager(db), catalog_(catalog) {
}

Transaction &OpenDuckTransactionManager::StartTransaction(ClientContext &context) {
	// Lazy acquisition: no network call here. Allocate a local
	// `OpenDuckTransaction` with no `transaction_id`; the first remote
	// DDL/DML operation will call `BeginTransaction`.
	lock_guard<mutex> guard(transaction_lock_);
	auto transaction = make_uniq<OpenDuckTransaction>(*this, context);
	auto &result = *transaction;
	transactions_[context] = std::move(transaction);
	return result;
}

ErrorData OpenDuckTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &otx = transaction.Cast<OpenDuckTransaction>();
	// Remote commit (if acquired). Any exception is captured into an
	// ErrorData so DuckDB surfaces it through the normal commit path.
	if (otx.acquired) {
		try {
			auto &config = catalog_.GetConfig();
			catalog_.GetClient().CommitTransaction(otx.transaction_id, config.token);
		} catch (std::exception &e) {
			lock_guard<mutex> guard(transaction_lock_);
			transactions_.erase(context);
			return ErrorData(e.what());
		}
	}
	lock_guard<mutex> guard(transaction_lock_);
	transactions_.erase(context);
	return ErrorData();
}

void OpenDuckTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &otx = transaction.Cast<OpenDuckTransaction>();
	if (otx.acquired) {
		// Best-effort rollback. Swallow errors — the transaction is
		// being torn down anyway and a rollback-after-failure shouldn't
		// escalate into a second exception.
		try {
			auto &config = catalog_.GetConfig();
			catalog_.GetClient().RollbackTransaction(otx.transaction_id, config.token);
		} catch (...) {
			// intentionally swallowed
		}
	}
	lock_guard<mutex> guard(transaction_lock_);
	auto ctx = transaction.context.lock();
	if (ctx) {
		transactions_.erase(*ctx);
	}
}

void OpenDuckTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

} // namespace openduck
