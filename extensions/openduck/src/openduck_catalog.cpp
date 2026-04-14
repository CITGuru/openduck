#include "openduck_catalog.hpp"
#include "openduck_schema.hpp"
#include "grpc_client.hpp"
#include "arrow_bridge.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

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

optional_ptr<CatalogEntry> OpenDuckCatalog::CreateSchema(CatalogTransaction transaction,
                                                          CreateSchemaInfo &info) {
	throw NotImplementedException("CREATE SCHEMA is not supported on OpenDuck remote databases");
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

PhysicalOperator &OpenDuckCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                      LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("CREATE TABLE AS is not supported on OpenDuck remote databases");
}

PhysicalOperator &OpenDuckCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("INSERT is not supported on OpenDuck remote databases");
}

PhysicalOperator &OpenDuckCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException("DELETE is not supported on OpenDuck remote databases");
}

PhysicalOperator &OpenDuckCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalUpdate &op, PhysicalOperator &plan) {
	throw NotImplementedException("UPDATE is not supported on OpenDuck remote databases");
}

void OpenDuckCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DROP SCHEMA is not supported on OpenDuck remote databases");
}

// ═══════════════════════════════════════════════════════════════════════════
// OpenDuckTransaction
// ═══════════════════════════════════════════════════════════════════════════

OpenDuckTransaction::OpenDuckTransaction(TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

// ═══════════════════════════════════════════════════════════════════════════
// OpenDuckTransactionManager
// ═══════════════════════════════════════════════════════════════════════════

OpenDuckTransactionManager::OpenDuckTransactionManager(AttachedDatabase &db, OpenDuckCatalog &catalog)
    : TransactionManager(db), catalog_(catalog) {
}

Transaction &OpenDuckTransactionManager::StartTransaction(ClientContext &context) {
	lock_guard<mutex> guard(transaction_lock_);
	auto transaction = make_uniq<OpenDuckTransaction>(*this, context);
	auto &result = *transaction;
	transactions_[context] = std::move(transaction);
	return result;
}

ErrorData OpenDuckTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> guard(transaction_lock_);
	transactions_.erase(context);
	return ErrorData();
}

void OpenDuckTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> guard(transaction_lock_);
	auto ctx = transaction.context.lock();
	if (ctx) {
		transactions_.erase(*ctx);
	}
}

void OpenDuckTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

} // namespace openduck
