#include "openduck_sql_emit.hpp"

#include <atomic>
#include <chrono>
#include <random>
#include <sstream>

namespace openduck {

std::string QuoteIdentifier(const std::string &name) {
	std::string out;
	out.reserve(name.size() + 2);
	out.push_back('"');
	for (char c : name) {
		if (c == '"') {
			out.push_back('"');
		}
		out.push_back(c);
	}
	out.push_back('"');
	return out;
}

std::string QuoteLiteral(const std::string &s) {
	std::string out;
	out.reserve(s.size() + 2);
	out.push_back('\'');
	for (char c : s) {
		if (c == '\'') {
			out.push_back('\'');
		}
		out.push_back(c);
	}
	out.push_back('\'');
	return out;
}

std::string QuoteRef(const std::string &catalog, const std::string &schema,
                      const std::string &table) {
	std::string out;
	if (!catalog.empty()) {
		out += QuoteIdentifier(catalog) + ".";
	}
	if (!schema.empty()) {
		out += QuoteIdentifier(schema) + ".";
	}
	out += QuoteIdentifier(table);
	return out;
}

std::string TypeToSQL(const duckdb::LogicalType &type) {
	return type.ToString();
}

std::string GenerateStagingTable() {
	static std::atomic<uint64_t> counter{0};
	auto now = std::chrono::steady_clock::now().time_since_epoch();
	auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
	std::random_device rd;
	std::ostringstream oss;
	oss << "__openduck_ingest_" << std::hex << ns << "_"
	    << counter.fetch_add(1, std::memory_order_relaxed) << "_"
	    << static_cast<uint32_t>(rd());
	return oss.str();
}

} // namespace openduck
