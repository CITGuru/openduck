#include "openduck_schema.hpp"
#include "openduck_catalog.hpp"
#include "openduck_table_entry.hpp"
#include "grpc_client.hpp"
#include "arrow_bridge.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include <deque>

namespace openduck {

using namespace duckdb;

OpenDuckSchemaEntry::OpenDuckSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {
}

OpenDuckCatalog &OpenDuckSchemaEntry::GetOpenDuckCatalog() {
	return catalog.Cast<OpenDuckCatalog>();
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                             const EntryLookupInfo &lookup_info) {
	auto entry_type = lookup_info.GetCatalogType();
	if (entry_type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}

	auto table_name = lookup_info.GetEntryName();

	auto it = cached_tables_.find(table_name);
	if (it != cached_tables_.end()) {
		return it->second.get();
	}

	auto &od_catalog = GetOpenDuckCatalog();
	auto &config = od_catalog.GetConfig();
	auto &client = od_catalog.GetClient();

	auto sql = "SELECT * FROM " + table_name + " LIMIT 0";
	try {
		auto stream = client.ExecuteSQL(sql, config.database, config.token);
		auto first_ipc = stream->Next();
		if (!first_ipc) {
			fprintf(stderr, "[openduck] LookupEntry('%s'): no IPC data returned\n", table_name.c_str());
			return nullptr;
		}

		auto schema_info = ExtractSchema(*first_ipc);
		fprintf(stderr, "[openduck] LookupEntry('%s'): got %zu columns\n",
		        table_name.c_str(), schema_info.names.size());

		auto info = make_uniq<CreateTableInfo>();
		info->schema = name;
		info->table = table_name;
		for (idx_t i = 0; i < schema_info.names.size(); i++) {
			info->columns.AddColumn(ColumnDefinition(schema_info.names[i], schema_info.types[i]));
		}

		auto table_entry = make_uniq<OpenDuckTableEntry>(catalog, *this, *info, config);
		auto result = table_entry.get();
		cached_tables_[table_name] = std::move(table_entry);
		return result;
	} catch (const GatewayUnavailableError &e) {
		fprintf(stderr, "[openduck] LookupEntry('%s'): gateway unavailable: %s\n",
		        table_name.c_str(), e.what());
		return nullptr;
	} catch (const std::exception &e) {
		fprintf(stderr, "[openduck] LookupEntry('%s'): error: %s\n",
		        table_name.c_str(), e.what());
		return nullptr;
	} catch (...) {
		fprintf(stderr, "[openduck] LookupEntry('%s'): unknown error\n", table_name.c_str());
		return nullptr;
	}
}

void OpenDuckSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}

	auto &od_catalog = GetOpenDuckCatalog();
	auto &config = od_catalog.GetConfig();
	auto &client = od_catalog.GetClient();

	try {
		auto sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'";
		auto stream = client.ExecuteSQL(sql, config.database, config.token);

		std::vector<std::string> table_names;
		std::deque<std::shared_ptr<arrow::RecordBatch>> batches;
		while (true) {
			auto ipc = stream->Next();
			if (!ipc) {
				break;
			}
			ReadAllIpcBatches(*ipc, batches);
		}

		vector<LogicalType> types = {LogicalType::VARCHAR};
		for (auto &batch : batches) {
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), types);
			auto rows = CopyBatchToDataChunk(batch, chunk);
			for (idx_t i = 0; i < rows; i++) {
				table_names.push_back(chunk.data[0].GetValue(i).GetValue<string>());
			}
		}

		for (auto &tbl_name : table_names) {
			CatalogTransaction txn(catalog, context);
			EntryLookupInfo info(CatalogType::TABLE_ENTRY, tbl_name);
			auto entry = LookupEntry(txn, info);
			if (entry) {
				callback(*entry);
			}
		}
	} catch (const GatewayUnavailableError &) {
	} catch (...) {
	}
}

void OpenDuckSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateIndex(CatalogTransaction transaction,
                                                             CreateIndexInfo &info,
                                                             TableCatalogEntry &table) {
	throw NotImplementedException("CREATE INDEX is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                CreateFunctionInfo &info) {
	throw NotImplementedException("CREATE FUNCTION is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                             BoundCreateTableInfo &info) {
	throw NotImplementedException("CREATE TABLE is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateView(CatalogTransaction transaction,
                                                            CreateViewInfo &info) {
	throw NotImplementedException("CREATE VIEW is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                CreateSequenceInfo &info) {
	throw NotImplementedException("CREATE SEQUENCE is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                     CreateTableFunctionInfo &info) {
	throw NotImplementedException("CREATE TABLE FUNCTION is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                    CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CREATE COPY FUNCTION is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                      CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CREATE PRAGMA is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                 CreateCollationInfo &info) {
	throw NotImplementedException("CREATE COLLATION is not supported on OpenDuck remote databases");
}

optional_ptr<CatalogEntry> OpenDuckSchemaEntry::CreateType(CatalogTransaction transaction,
                                                            CreateTypeInfo &info) {
	throw NotImplementedException("CREATE TYPE is not supported on OpenDuck remote databases");
}

void OpenDuckSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DROP is not supported on OpenDuck remote databases");
}

void OpenDuckSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("ALTER is not supported on OpenDuck remote databases");
}

} // namespace openduck
