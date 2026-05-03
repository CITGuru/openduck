#include "openduck_errors.hpp"
#include "openduck_catalog.hpp"

#include "openduck/v1/execution.pb.h"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

#include <cctype>
#include <cstring>
#include <string>

namespace openduck {

using namespace duckdb;
using ::openduck::v1::ExecuteFragmentError;

// ── Kind → DuckDB exception ────────────────────────────────────────────────

namespace {

// Pattern-match DuckDB's class prefixes (`"Catalog Error: ..."`,
// `"Binder Error: ..."`, ...) for the untyped fallback path. Mirrors
// `exec-worker`'s `errors::classify` Rust helper — keep them aligned.
ExecuteFragmentError::Kind ClassifyFromMessage(const std::string &message) {
	struct Prefix {
		const char *text;
		ExecuteFragmentError::Kind kind;
	};
	static constexpr Prefix kPrefixes[] = {
	    {"Catalog Error", ExecuteFragmentError::CATALOG},
	    {"Binder Error", ExecuteFragmentError::BINDER},
	    {"Parser Error", ExecuteFragmentError::PARSER},
	    {"Constraint Error", ExecuteFragmentError::CONSTRAINT},
	    {"Conversion Error", ExecuteFragmentError::CONVERSION},
	    {"Invalid Input Error", ExecuteFragmentError::BINDER},
	    {"Invalid Type Error", ExecuteFragmentError::CONVERSION},
	    {"Out of Range Error", ExecuteFragmentError::CONVERSION},
	    {"IO Error", ExecuteFragmentError::IO},
	    {"HTTP Error", ExecuteFragmentError::IO},
	    {"Permission Error", ExecuteFragmentError::PERMISSION},
	    {"Dependency Error", ExecuteFragmentError::CONSTRAINT},
	    {"TransactionContext Error", ExecuteFragmentError::CATALOG},
	    {"Transaction Error", ExecuteFragmentError::CATALOG},
	    {"Not Implemented Error", ExecuteFragmentError::INTERNAL},
	    {"Out of Memory Error", ExecuteFragmentError::INTERNAL},
	    {"Fatal Error", ExecuteFragmentError::INTERNAL},
	    {"Internal Error", ExecuteFragmentError::INTERNAL},
	    {"Serialization Error", ExecuteFragmentError::INTERNAL},
	};
	// Skip leading whitespace so "  Catalog Error: ..." still matches.
	size_t start = 0;
	while (start < message.size() && std::isspace(static_cast<unsigned char>(message[start]))) {
		start++;
	}
	for (const auto &p : kPrefixes) {
		auto len = std::strlen(p.text);
		if (message.size() - start < len) {
			continue;
		}
		if (message.compare(start, len, p.text) != 0) {
			continue;
		}
		auto after = start + len;
		// Require `:` / whitespace / end-of-string immediately after so
		// we don't match `"Catalog Errors are fun"`.
		if (after == message.size() || message[after] == ':' ||
		    std::isspace(static_cast<unsigned char>(message[after]))) {
			return p.kind;
		}
	}
	return ExecuteFragmentError::UNKNOWN;
}

} // namespace

[[noreturn]] void ThrowMappedError(const ExecuteFragmentError &err) {
	const auto &message = err.message();
	switch (err.kind()) {
	case ExecuteFragmentError::CATALOG:
		throw CatalogException(message);
	case ExecuteFragmentError::BINDER:
		throw BinderException(message);
	case ExecuteFragmentError::CONSTRAINT:
		throw ConstraintException(message);
	case ExecuteFragmentError::CONVERSION:
		throw ConversionException(message);
	case ExecuteFragmentError::PARSER:
		throw ParserException(message);
	case ExecuteFragmentError::IO:
		throw IOException(message);
	case ExecuteFragmentError::PERMISSION:
		throw PermissionException(message);
	case ExecuteFragmentError::INTERNAL:
		throw InternalException(message);
	case ExecuteFragmentError::UNKNOWN:
	default:
		// Fall back to string-prefix classification — pre-typed-error
		// workers never populate `typed_error` but still prefix their
		// error strings with DuckDB's class names.
		ThrowMappedErrorFromMessage(message);
	}
}

[[noreturn]] void ThrowMappedErrorFromMessage(const std::string &message) {
	auto kind = ClassifyFromMessage(message);
	switch (kind) {
	case ExecuteFragmentError::CATALOG:
		throw CatalogException(message);
	case ExecuteFragmentError::BINDER:
		throw BinderException(message);
	case ExecuteFragmentError::CONSTRAINT:
		throw ConstraintException(message);
	case ExecuteFragmentError::CONVERSION:
		throw ConversionException(message);
	case ExecuteFragmentError::PARSER:
		throw ParserException(message);
	case ExecuteFragmentError::IO:
		throw IOException(message);
	case ExecuteFragmentError::PERMISSION:
		throw PermissionException(message);
	case ExecuteFragmentError::INTERNAL:
		throw InternalException(message);
	default:
		throw InternalException(message);
	}
}

// ── Read-only guard ─────────────────────────────────────────────────────────

void EnsureWritable(OpenDuckCatalog &catalog) {
	if (catalog.GetConfig().read_only) {
		throw PermissionException(
		    "OpenDuck catalog \"%s\" is attached READ_ONLY; refusing mutation "
		    "(re-ATTACH without READ_ONLY to allow writes)",
		    catalog.GetName());
	}
}

} // namespace openduck
