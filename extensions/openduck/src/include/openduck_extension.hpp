#pragma once

#include "duckdb.hpp"

#include <mutex>
#include <string>
#include <unordered_map>

namespace duckdb {

class OpenduckExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb

namespace openduck {

struct AttachConfig {
	std::string scheme;
	std::string database;
	std::string endpoint;
	std::string token;
	std::string error;
	bool valid = false;
};

AttachConfig ResolveAttachConfig(const std::string &uri);
void StoreAttachConfig(const std::string &alias, AttachConfig cfg);
AttachConfig LookupAttachConfig(const std::string &alias);

} // namespace openduck
