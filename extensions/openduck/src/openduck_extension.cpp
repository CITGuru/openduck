#define DUCKDB_EXTENSION_MAIN

#include "openduck_extension.hpp"
#include "openduck_catalog.hpp"
#include "openduck_filesystem.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "arrow_bridge.hpp"
#include "grpc_client.hpp"

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <cstdlib>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

using namespace duckdb;

// ═══════════════════════════════════════════════════════════════════════════
// URI parsing and config registry (openduck namespace)
// ═══════════════════════════════════════════════════════════════════════════

namespace openduck {

static std::string EnvOrDefault(const char *key, const std::string &fallback) {
	const char *v = std::getenv(key);
	if (!v || !*v) {
		return fallback;
	}
	return std::string(v);
}

static std::unordered_map<std::string, std::string> ParseQuery(const std::string &query) {
	std::unordered_map<std::string, std::string> out;
	size_t start = 0;
	while (start < query.size()) {
		size_t end = query.find('&', start);
		if (end == std::string::npos) {
			end = query.size();
		}
		const auto part = query.substr(start, end - start);
		const auto eq = part.find('=');
		if (eq == std::string::npos) {
			out.emplace(part, "");
		} else {
			out.emplace(part.substr(0, eq), part.substr(eq + 1));
		}
		start = end + 1;
	}
	return out;
}

AttachConfig ResolveAttachConfig(const std::string &uri) {
	AttachConfig cfg;
	const auto qpos = uri.find('?');
	const auto head = uri.substr(0, qpos);
	const auto query = (qpos == std::string::npos) ? "" : uri.substr(qpos + 1);

	if (head.rfind("openduck:", 0) == 0) {
		cfg.scheme = "openduck";
		cfg.database = head.substr(std::string("openduck:").size());
	} else if (head.rfind("od:", 0) == 0) {
		cfg.scheme = "od";
		cfg.database = head.substr(std::string("od:").size());
	} else {
		// DuckDB strips the scheme before calling the storage extension attach
		// handler, so the head is just the database name.
		cfg.scheme = "openduck";
		cfg.database = head;
	}

	if (cfg.database.empty()) {
		cfg.database = "default";
	}

	auto params = ParseQuery(query);

	auto ep_it = params.find("endpoint");
	cfg.endpoint = (ep_it != params.end() && !ep_it->second.empty())
	                   ? ep_it->second
	                   : EnvOrDefault("OPENDUCK_ENDPOINT", "http://127.0.0.1:7878");

	auto tok_it = params.find("token");
	cfg.token = (tok_it != params.end() && !tok_it->second.empty())
	                ? tok_it->second
	                : EnvOrDefault("OPENDUCK_TOKEN", "");
	if (cfg.token.empty()) {
		cfg.error = "missing token (set OPENDUCK_TOKEN or URI ?token=...)";
		return cfg;
	}
	cfg.valid = true;
	return cfg;
}

static std::mutex g_config_mutex;
static std::unordered_map<std::string, AttachConfig> g_configs;

void StoreAttachConfig(const std::string &alias, AttachConfig cfg) {
	std::lock_guard<std::mutex> lock(g_config_mutex);
	g_configs[alias] = std::move(cfg);
}

AttachConfig LookupAttachConfig(const std::string &alias) {
	std::lock_guard<std::mutex> lock(g_config_mutex);
	auto it = g_configs.find(alias);
	if (it == g_configs.end()) {
		throw std::runtime_error("No ATTACH config for alias '" + alias +
		                         "'. Run: ATTACH 'openduck:<db>' AS " + alias + ";");
	}
	return it->second;
}

// ── Storage extension (ATTACH 'openduck:mydb' AS alias) ─────────────────────

class OpenDuckStorageExtension : public StorageExtension {
public:
	OpenDuckStorageExtension() {
		attach = OpenDuckAttach;
		create_transaction_manager = OpenDuckCreateTransactionManager;
	}

	static unique_ptr<Catalog> OpenDuckAttach(optional_ptr<StorageExtensionInfo> info, ClientContext &context,
	                                          AttachedDatabase &db, const string &name,
	                                          AttachInfo &attach_info, AttachOptions &options) {
		auto cfg = ResolveAttachConfig(attach_info.path);
		if (!cfg.valid) {
			throw InvalidInputException("OpenDuck attach error: %s", cfg.error);
		}

		StoreAttachConfig(name, cfg);

		return make_uniq<OpenDuckCatalog>(db, std::move(cfg));
	}

	static unique_ptr<TransactionManager> OpenDuckCreateTransactionManager(optional_ptr<StorageExtensionInfo> info,
	                                                                        AttachedDatabase &db, Catalog &catalog) {
		auto &od_catalog = catalog.Cast<OpenDuckCatalog>();
		return make_uniq<OpenDuckTransactionManager>(db, od_catalog);
	}
};

// ── Shared stream state for table functions ─────────────────────────────────

struct StreamState {
	std::unique_ptr<GrpcClient> client;
	std::unique_ptr<GrpcStream> stream;
	std::deque<std::shared_ptr<arrow::RecordBatch>> pending_batches;
	std::string execution_id;
	bool done = false;
	std::mutex lock;
};

struct OpenDuckTableData : public TableFunctionData {
	string endpoint;
	string token;
	string database;
	string sql;
	mutable std::shared_ptr<StreamState> state;
};

static void InitStreamAndSchema(OpenDuckTableData &data, vector<LogicalType> &return_types,
                                vector<string> &names) {
	data.state = std::make_shared<StreamState>();
	data.state->client = std::make_unique<GrpcClient>(data.endpoint);
	data.state->stream = data.state->client->ExecuteSQL(data.sql, data.database, data.token);
	data.state->execution_id = data.state->stream->ExecutionId();

	auto first_ipc = data.state->stream->Next();
	if (!first_ipc) {
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back("result");
		data.state->done = true;
		return;
	}

	auto schema_info = ExtractSchema(*first_ipc);
	return_types = std::move(schema_info.types);
	names = std::move(schema_info.names);

	ReadAllIpcBatches(*first_ipc, data.state->pending_batches);
}

static void SharedExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<OpenDuckTableData>();
	auto &state = *data.state;

	std::lock_guard<std::mutex> guard(state.lock);
	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	while (state.pending_batches.empty()) {
		auto ipc_bytes = state.stream->Next();
		if (!ipc_bytes) {
			state.done = true;
			output.SetCardinality(0);
			return;
		}
		ReadAllIpcBatches(*ipc_bytes, state.pending_batches);
	}

	auto batch = std::move(state.pending_batches.front());
	state.pending_batches.pop_front();

	auto rows = CopyBatchToDataChunk(batch, output);
	if (rows == 0) {
		state.done = true;
		output.SetCardinality(0);
	}
}

static unique_ptr<FunctionData> RemoteBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<OpenDuckTableData>();
	data->endpoint = input.inputs[0].GetValue<string>();
	data->token = input.inputs[1].GetValue<string>();
	data->sql = input.inputs[2].GetValue<string>();
	data->database = "default";

	InitStreamAndSchema(*data, return_types, names);
	return std::move(data);
}

static unique_ptr<FunctionData> QueryBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto alias = input.inputs[0].GetValue<string>();
	auto cfg = LookupAttachConfig(alias);

	auto data = make_uniq<OpenDuckTableData>();
	data->endpoint = cfg.endpoint;
	data->token = cfg.token;
	data->database = cfg.database;
	data->sql = input.inputs[1].GetValue<string>();

	InitStreamAndSchema(*data, return_types, names);
	return std::move(data);
}

} // namespace openduck

// ═══════════════════════════════════════════════════════════════════════════
// Extension entry point
// ═══════════════════════════════════════════════════════════════════════════

namespace duckdb {

// ── Secret type: openduck_storage ────────────────────────────────────────────
//
// CREATE SECRET my_storage (
//     TYPE openduck_storage,
//     postgres_url 'postgres://localhost/openduck',
//     data_dir '/var/openduck'
// );

static unique_ptr<BaseSecret> CreateOpenDuckStorageSecret(ClientContext &, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope = {"openduck://"};
	}
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
	secret->TrySetValue("postgres_url", input);
	secret->TrySetValue("data_dir", input);
	secret->redact_keys.insert("postgres_url");
	return std::move(secret);
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	auto se1 = make_shared_ptr<openduck::OpenDuckStorageExtension>();
	StorageExtension::Register(config, "openduck", std::move(se1));

	auto se2 = make_shared_ptr<openduck::OpenDuckStorageExtension>();
	StorageExtension::Register(config, "od", std::move(se2));

	auto &fs = FileSystem::GetFileSystem(db);
	fs.RegisterSubSystem(make_uniq<openduck::OpenDuckFileSystem>());

	SecretType storage_secret_type;
	storage_secret_type.name = "openduck_storage";
	storage_secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	storage_secret_type.default_provider = "config";
	storage_secret_type.extension = "openduck";
	loader.RegisterSecretType(storage_secret_type);

	CreateSecretFunction storage_secret_fn = {"openduck_storage", "config", CreateOpenDuckStorageSecret};
	storage_secret_fn.named_parameters["postgres_url"] = LogicalType::VARCHAR;
	storage_secret_fn.named_parameters["data_dir"] = LogicalType::VARCHAR;
	loader.RegisterFunction(storage_secret_fn);

	TableFunction remote_fn("openduck_remote",
	                         {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         openduck::SharedExecute, openduck::RemoteBind);
	loader.RegisterFunction(remote_fn);

	TableFunction query_fn("openduck_query", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                        openduck::SharedExecute, openduck::QueryBind);
	loader.RegisterFunction(query_fn);
}

void OpenduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string OpenduckExtension::Name() {
	return "openduck";
}

std::string OpenduckExtension::Version() const {
#ifdef EXT_VERSION_OPENDUCK
	return EXT_VERSION_OPENDUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(openduck, loader) {
	duckdb::LoadInternal(loader);
}
}
