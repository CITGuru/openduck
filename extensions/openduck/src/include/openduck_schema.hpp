#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {
struct CreateTableInfo;
} // namespace duckdb

namespace openduck {

class GrpcClient;
struct AttachConfig;
class OpenDuckCatalog;

class OpenDuckSchemaEntry : public duckdb::SchemaCatalogEntry {
public:
	OpenDuckSchemaEntry(duckdb::Catalog &catalog, duckdb::CreateSchemaInfo &info);

	void Scan(duckdb::ClientContext &context, duckdb::CatalogType type,
	          const std::function<void(duckdb::CatalogEntry &)> &callback) override;
	void Scan(duckdb::CatalogType type, const std::function<void(duckdb::CatalogEntry &)> &callback) override;

	duckdb::optional_ptr<duckdb::CatalogEntry>
	CreateIndex(duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo &info,
	            duckdb::TableCatalogEntry &table) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateFunction(duckdb::CatalogTransaction transaction,
	                                                           duckdb::CreateFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateTable(duckdb::CatalogTransaction transaction,
	                                                        duckdb::BoundCreateTableInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateView(duckdb::CatalogTransaction transaction,
	                                                       duckdb::CreateViewInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateSequence(duckdb::CatalogTransaction transaction,
	                                                           duckdb::CreateSequenceInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateTableFunction(duckdb::CatalogTransaction transaction,
	                                                                duckdb::CreateTableFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateCopyFunction(duckdb::CatalogTransaction transaction,
	                                                               duckdb::CreateCopyFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreatePragmaFunction(duckdb::CatalogTransaction transaction,
	                                                                 duckdb::CreatePragmaFunctionInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateCollation(duckdb::CatalogTransaction transaction,
	                                                            duckdb::CreateCollationInfo &info) override;
	duckdb::optional_ptr<duckdb::CatalogEntry> CreateType(duckdb::CatalogTransaction transaction,
	                                                       duckdb::CreateTypeInfo &info) override;

	duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntry(duckdb::CatalogTransaction transaction,
	                                                        const duckdb::EntryLookupInfo &lookup_info) override;

	void DropEntry(duckdb::ClientContext &context, duckdb::DropInfo &info) override;
	void Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo &info) override;

	OpenDuckCatalog &GetOpenDuckCatalog();

	/// Case-insensitive cache probe for the rewriter's Rule C.
	/// Returns true if `table_name` is currently in the local table
	/// cache; never makes a network call.
	bool HasCachedTable(const std::string &table_name) const;

private:
	/// Probe the worker's `duckdb_constraints()` for the given table
	/// and attach PRIMARY KEY / UNIQUE / NOT NULL constraints onto
	/// `info`. Best-effort: any failure (e.g. older worker without
	/// `duckdb_constraints()`) silently leaves `info` constraint-less.
	void FetchAndAttachConstraints(GrpcClient &client, const AttachConfig &config,
	                                const std::string &table_name,
	                                duckdb::CreateTableInfo &info);

	/// Best-effort: run ONE batched query against the worker combining
	/// `duckdb_columns()` and `duckdb_constraints()` filtered to this
	/// schema, populate `cached_tables_` with every table it finds,
	/// and set `schema_loaded_`. Safe to call repeatedly; skips work
	/// if `schema_loaded_` is already true.
	///
	/// Modeled on duckdb-postgres' `PostgresTableSet::GetInitializeQuery`
	/// pattern: one remote round-trip reflects every table in the schema
	/// at once, replacing the per-table `SELECT * FROM t LIMIT 0 +
	/// duckdb_constraints()` loop which is O(N) round-trips.
	///
	/// `context` is required because `duckdb_columns().data_type` returns
	/// type strings (e.g. `"INTEGER"`, `"DECIMAL(18,3)"`) that
	/// `Parser::ParseColumnDefinition` only resolves to
	/// `LogicalTypeId::UNBOUND`. We resolve them to concrete
	/// `LogicalType`s by issuing a single local
	/// `SELECT CAST(NULL AS t1), CAST(NULL AS t2), ...` against
	/// `context.db` and reading the result schema back. This is one
	/// extra in-process query per ATTACH-schema-load, no extra remote
	/// round-trips.
	///
	/// On any failure, the flag stays false and `LookupEntry` falls
	/// back to its per-table path — no user-visible error.
	void EnsureSchemaLoaded(duckdb::ClientContext &context,
	                        GrpcClient &client, const AttachConfig &config);

	duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CatalogEntry>> cached_tables_;
	bool schema_loaded_ = false;
};

} // namespace openduck
