#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "grpc_client.hpp"
#include "openduck_extension.hpp"

namespace openduck {

class OpenDuckSchemaEntry;

struct OpenDuckCatalogInfo : public duckdb::StorageExtensionInfo {
	AttachConfig config;
};

class OpenDuckCatalog : public duckdb::Catalog {
public:
	OpenDuckCatalog(duckdb::AttachedDatabase &db, AttachConfig config);
	~OpenDuckCatalog() override;

	AttachConfig &GetConfig() {
		return config_;
	}
	GrpcClient &GetClient();

	void Initialize(bool load_builtin) override;
	void Initialize(duckdb::optional_ptr<duckdb::ClientContext> context, bool load_builtin) override;

	std::string GetCatalogType() override {
		return "openduck";
	}

	duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
	LookupSchema(duckdb::CatalogTransaction transaction, const duckdb::EntryLookupInfo &schema_lookup,
	             duckdb::OnEntryNotFound if_not_found) override;

	void ScanSchemas(duckdb::ClientContext &context,
	                 std::function<void(duckdb::SchemaCatalogEntry &)> callback) override;

	duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(duckdb::CatalogTransaction transaction,
	                                                         duckdb::CreateSchemaInfo &info) override;

	duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext &context) override;
	bool InMemory() override;
	std::string GetDBPath() override;

	duckdb::PhysicalOperator &PlanCreateTableAs(duckdb::ClientContext &context,
	                                             duckdb::PhysicalPlanGenerator &planner,
	                                             duckdb::LogicalCreateTable &op,
	                                             duckdb::PhysicalOperator &plan) override;
	duckdb::PhysicalOperator &PlanInsert(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                      duckdb::LogicalInsert &op,
	                                      duckdb::optional_ptr<duckdb::PhysicalOperator> plan) override;
	duckdb::PhysicalOperator &PlanDelete(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                      duckdb::LogicalDelete &op, duckdb::PhysicalOperator &plan) override;
	duckdb::PhysicalOperator &PlanUpdate(duckdb::ClientContext &context, duckdb::PhysicalPlanGenerator &planner,
	                                      duckdb::LogicalUpdate &op, duckdb::PhysicalOperator &plan) override;

	void DropSchema(duckdb::ClientContext &context, duckdb::DropInfo &info) override;

private:
	AttachConfig config_;
	std::unique_ptr<GrpcClient> client_;
	duckdb::unique_ptr<OpenDuckSchemaEntry> main_schema_;
};

class OpenDuckTransaction : public duckdb::Transaction {
public:
	OpenDuckTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context);
};

class OpenDuckTransactionManager : public duckdb::TransactionManager {
public:
	OpenDuckTransactionManager(duckdb::AttachedDatabase &db, OpenDuckCatalog &catalog);

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override;
	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override;
	void RollbackTransaction(duckdb::Transaction &transaction) override;
	void Checkpoint(duckdb::ClientContext &context, bool force) override;

private:
	OpenDuckCatalog &catalog_;
	duckdb::mutex transaction_lock_;
	duckdb::reference_map_t<duckdb::ClientContext, duckdb::unique_ptr<OpenDuckTransaction>> transactions_;
};

} // namespace openduck
