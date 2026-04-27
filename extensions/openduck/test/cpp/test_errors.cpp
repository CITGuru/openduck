#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "openduck_errors.hpp"
#include "openduck/v1/execution.pb.h"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

using openduck::ThrowMappedError;
using openduck::ThrowMappedErrorFromMessage;
using PbError = ::openduck::v1::ExecuteFragmentError;

static PbError MakeError(PbError::Kind kind, const std::string &message) {
	PbError err;
	err.set_kind(kind);
	err.set_message(message);
	return err;
}

// ── Kind → exception class mapping ──────────────────────────────────────────

TEST_CASE("CATALOG kind throws CatalogException", "[errors][kind_mapping]") {
	auto err = MakeError(PbError::CATALOG, "table not found");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::CatalogException);
}

TEST_CASE("BINDER kind throws BinderException", "[errors][kind_mapping]") {
	auto err = MakeError(PbError::BINDER, "referenced column ambiguous");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::BinderException);
}

TEST_CASE("CONSTRAINT kind throws ConstraintException", "[errors][kind_mapping]") {
	auto err = MakeError(PbError::CONSTRAINT, "UNIQUE violation");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::ConstraintException);
}

TEST_CASE("CONVERSION kind throws ConversionException", "[errors][kind_mapping]") {
	auto err = MakeError(PbError::CONVERSION, "cast failed");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::ConversionException);
}

TEST_CASE("PARSER kind throws ParserException", "[errors][kind_mapping]") {
	auto err = MakeError(PbError::PARSER, "syntax error");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::ParserException);
}

TEST_CASE("IO kind throws IOException", "[errors][kind_mapping]") {
	auto err = MakeError(PbError::IO, "disk full");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::IOException);
}

TEST_CASE("PERMISSION kind throws PermissionException", "[errors][kind_mapping]") {
	auto err = MakeError(PbError::PERMISSION, "forbidden");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::PermissionException);
}

TEST_CASE("INTERNAL kind throws InternalException", "[errors][kind_mapping]") {
	auto err = MakeError(PbError::INTERNAL, "bug");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::InternalException);
}

// ── UNKNOWN fallback exercises the prefix classifier ───────────────────────

TEST_CASE("UNKNOWN with `Catalog Error:` prefix falls through to CatalogException",
          "[errors][classify_prefix]") {
	auto err = MakeError(PbError::UNKNOWN, "Catalog Error: nope");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::CatalogException);
}

TEST_CASE("UNKNOWN with `Binder Error:` prefix falls through to BinderException",
          "[errors][classify_prefix]") {
	auto err = MakeError(PbError::UNKNOWN, "Binder Error: column ambiguous");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::BinderException);
}

TEST_CASE("UNKNOWN with `Parser Error:` prefix falls through to ParserException",
          "[errors][classify_prefix]") {
	auto err = MakeError(PbError::UNKNOWN, "Parser Error: near 'FROM'");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::ParserException);
}

TEST_CASE("UNKNOWN with `Conversion Error:` prefix falls through to ConversionException",
          "[errors][classify_prefix]") {
	auto err = MakeError(PbError::UNKNOWN, "Conversion Error: cast");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::ConversionException);
}

TEST_CASE("UNKNOWN with `Constraint Error:` prefix falls through to ConstraintException",
          "[errors][classify_prefix]") {
	auto err = MakeError(PbError::UNKNOWN, "Constraint Error: FK");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::ConstraintException);
}

TEST_CASE("UNKNOWN with `IO Error:` prefix falls through to IOException",
          "[errors][classify_prefix]") {
	auto err = MakeError(PbError::UNKNOWN, "IO Error: disk");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::IOException);
}

TEST_CASE("UNKNOWN with `Permission Error:` prefix falls through to PermissionException",
          "[errors][classify_prefix]") {
	auto err = MakeError(PbError::UNKNOWN, "Permission Error: denied");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::PermissionException);
}

TEST_CASE("UNKNOWN without any known prefix becomes InternalException",
          "[errors][classify_prefix]") {
	auto err = MakeError(PbError::UNKNOWN, "random worker panic message");
	REQUIRE_THROWS_AS(ThrowMappedError(err), duckdb::InternalException);
}

// ── ThrowMappedErrorFromMessage (legacy path) ──────────────────────────────

TEST_CASE("ThrowMappedErrorFromMessage matches the C++ prefix classifier",
          "[errors][legacy]") {
	REQUIRE_THROWS_AS(
	    ThrowMappedErrorFromMessage("Catalog Error: table foo does not exist"),
	    duckdb::CatalogException);
	REQUIRE_THROWS_AS(
	    ThrowMappedErrorFromMessage("Parser Error: syntax error"),
	    duckdb::ParserException);
	// False-prefix guard — "Catalog Errors are fun" must NOT classify
	// as Catalog; it falls through to Internal.
	REQUIRE_THROWS_AS(ThrowMappedErrorFromMessage("Catalog Errors are fun"),
	                  duckdb::InternalException);
}
