#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "openduck_extension.hpp"

namespace openduck {

class OpenDuckTableEntry : public duckdb::TableCatalogEntry {
public:
	OpenDuckTableEntry(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info,
	                   AttachConfig config);

	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                          duckdb::column_t column_id) override;

	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                       duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;

	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

private:
	AttachConfig config_;
};

} // namespace openduck
