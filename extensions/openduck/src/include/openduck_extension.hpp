#pragma once

#include "duckdb.hpp"

#include <mutex>
#include <string>
#include <unordered_map>

namespace duckdb {

struct DBConfig;

class OpenduckExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb

namespace openduck {

/// Test-only helper: register the OpenDuck storage extensions (`openduck`
/// and `od` schemes) directly on a `DBConfig`. Production code loads
/// these via `DUCKDB_CPP_EXTENSION_ENTRY` through DuckDB's extension
/// loader; tests that statically link `libopenduck_extension.a` call
/// this instead.
void RegisterStorageExtensionsForTest(duckdb::DBConfig &config);

} // namespace openduck

namespace openduck {

struct AttachConfig {
	std::string scheme;
	std::string database;
	std::string endpoint;
	std::string token;
	std::string error;
	bool valid = false;
	/// Set by the storage extension's attach handler from
	/// `AttachOptions::access_mode`. When true, `EnsureWritable(catalog)`
	/// throws `PermissionException` for every mutation-capable hook.
	bool read_only = false;
};

AttachConfig ResolveAttachConfig(const std::string &uri);
void StoreAttachConfig(const std::string &alias, AttachConfig cfg);
AttachConfig LookupAttachConfig(const std::string &alias);

} // namespace openduck
