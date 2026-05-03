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

	/// Cache probe used by the `CatalogReferenceRewriter` Rule C.
	/// Returns true when `(schema, table)` names an entry currently
	/// cached on one of this catalog's schemas. Case-insensitive.
	/// Never issues a network call.
	bool HasSchemaOrTable(const std::string &schema, const std::string &table);

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
	duckdb::PhysicalOperator &PlanMergeInto(duckdb::ClientContext &context,
	                                         duckdb::PhysicalPlanGenerator &planner,
	                                         duckdb::LogicalMergeInto &op,
	                                         duckdb::PhysicalOperator &plan) override;

	void DropSchema(duckdb::ClientContext &context, duckdb::DropInfo &info) override;

private:
	AttachConfig config_;
	std::unique_ptr<GrpcClient> client_;
	duckdb::unique_ptr<OpenDuckSchemaEntry> main_schema_;
};

class OpenDuckTransaction : public duckdb::Transaction {
public:
	OpenDuckTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context);

	/// Server-issued transaction id. Empty until the first remote DDL
	/// or DML operation inside this transaction flips acquisition on
	/// via `EnsureAcquired` (lazy acquisition).
	std::string transaction_id;
	/// Flips true once `BeginTransaction` returns on the worker. While
	/// false, `CommitTransaction` / `RollbackTransaction` at the
	/// manager level skip the RPC entirely (no remote state exists).
	bool acquired = false;
};

/// Ensure the current user transaction (if any) has been acquired on
/// the worker. Returns the bound `transaction_id`, or an empty string
/// when the caller is in auto-commit mode (in which case the worker
/// runs each statement on a fresh connection).
///
/// Must be called from every DDL / DML hook before issuing any gRPC
/// call to the worker.
std::string EnsureAcquired(duckdb::ClientContext &context, OpenDuckCatalog &catalog);

/// Re-parse `context.GetCurrentQuery()`, strip the attached-catalog
/// qualifier from every table / column reference using
/// `CatalogReferenceRewriter`, and return the result as a SQL string
/// the worker can execute directly. Throws `BinderException` when the
/// current query text is empty (programmatic call path — the design
/// doc's §1 "Fallback for programmatic statements" is not yet
/// implemented in this build).
std::string BuildForwardedSQL(duckdb::ClientContext &context, OpenDuckCatalog &catalog);

/// Convenience: run `BuildForwardedSQL`, ship it to the worker on the
/// current transaction (via `EnsureAcquired`), and drain the result
/// stream. Used for DDL paths that don't return rows to the caller
/// (CREATE / DROP / ALTER / CREATE SCHEMA / DROP SCHEMA).
void ForwardDDL(duckdb::ClientContext &context, OpenDuckCatalog &catalog);

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
