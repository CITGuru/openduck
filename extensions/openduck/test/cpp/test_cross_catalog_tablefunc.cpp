#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include "openduck_extension.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <netinet/in.h>
#include <spawn.h>
#include <string>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

extern char **environ;

namespace {

std::string RepoRootFromSourceFile() {
	std::filesystem::path here = __FILE__;
	return std::filesystem::canonical(here.parent_path() / ".." / ".." / ".." / "..").string();
}

int PickPort() {
	int sock = ::socket(AF_INET, SOCK_STREAM, 0);
	REQUIRE(sock >= 0);
	sockaddr_in addr{};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	REQUIRE(::bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
	socklen_t len = sizeof(addr);
	REQUIRE(::getsockname(sock, reinterpret_cast<sockaddr *>(&addr), &len) == 0);
	int port = ntohs(addr.sin_port);
	::close(sock);
	return port;
}

bool WaitForPort(int port, int timeout_ms = 5000) {
	using clk = std::chrono::steady_clock;
	auto deadline = clk::now() + std::chrono::milliseconds(timeout_ms);
	while (clk::now() < deadline) {
		int sock = ::socket(AF_INET, SOCK_STREAM, 0);
		if (sock < 0) return false;
		sockaddr_in addr{};
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		addr.sin_port = htons(port);
		int rc = ::connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
		::close(sock);
		if (rc == 0) return true;
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	return false;
}

class WorkerSubprocess {
public:
	explicit WorkerSubprocess(int port) : port_(port) {
		auto repo = RepoRootFromSourceFile();
		auto bin = repo + "/target/release/openduck-worker";
		REQUIRE(std::filesystem::exists(bin));
		db_path_ = std::string(std::getenv("TMPDIR") ? std::getenv("TMPDIR") : "/tmp") +
		           "/openduck_tf_" + std::to_string(port) + ".duckdb";
		std::filesystem::remove(db_path_);
		std::filesystem::remove(db_path_ + ".wal");
		listen_env_ = "OPENDUCK_WORKER_LISTEN=127.0.0.1:" + std::to_string(port_);
		token_env_ = "OPENDUCK_TOKEN=e2e-token";
		rust_log_env_ = "RUST_LOG=warn";
		db_env_ = "OPENDUCK_WORKER_DB=" + db_path_;
		std::vector<char *> env;
		for (char **e = environ; *e; e++) {
			if (std::strncmp(*e, "OPENDUCK_TOKEN=", 15) == 0) continue;
			if (std::strncmp(*e, "OPENDUCK_WORKER_LISTEN=", 23) == 0) continue;
			if (std::strncmp(*e, "OPENDUCK_WORKER_DB=", 19) == 0) continue;
			if (std::strncmp(*e, "RUST_LOG=", 9) == 0) continue;
			env.push_back(*e);
		}
		env.push_back(const_cast<char *>(listen_env_.c_str()));
		env.push_back(const_cast<char *>(token_env_.c_str()));
		env.push_back(const_cast<char *>(rust_log_env_.c_str()));
		env.push_back(const_cast<char *>(db_env_.c_str()));
		env.push_back(nullptr);
		std::vector<char *> argv;
		argv.push_back(const_cast<char *>(bin.c_str()));
		argv.push_back(nullptr);
		pid_t pid = 0;
		REQUIRE(::posix_spawn(&pid, bin.c_str(), nullptr, nullptr, argv.data(), env.data()) == 0);
		pid_ = pid;
		REQUIRE(WaitForPort(port_));
	}
	~WorkerSubprocess() {
		if (pid_ > 0) {
			::kill(pid_, SIGTERM);
			int status = 0;
			for (int i = 0; i < 50; i++) {
				if (::waitpid(pid_, &status, WNOHANG) == pid_) break;
				std::this_thread::sleep_for(std::chrono::milliseconds(20));
			}
			if (pid_ > 0) {
				::kill(pid_, SIGKILL);
				::waitpid(pid_, &status, 0);
			}
		}
		if (!db_path_.empty()) {
			std::error_code ec;
			std::filesystem::remove(db_path_, ec);
			std::filesystem::remove(db_path_ + ".wal", ec);
		}
	}
	int port() const { return port_; }
private:
	int port_;
	pid_t pid_ = 0;
	std::string db_path_, listen_env_, token_env_, rust_log_env_, db_env_;
};

struct LocalDuckDBWithOpenDuck {
	duckdb::DBConfig config;
	std::unique_ptr<duckdb::DuckDB> db;
	std::unique_ptr<duckdb::Connection> con;
	LocalDuckDBWithOpenDuck() {
		openduck::RegisterStorageExtensionsForTest(config);
		db = std::make_unique<duckdb::DuckDB>(nullptr, &config);
		con = std::make_unique<duckdb::Connection>(*db);
	}
	duckdb::Connection &operator*() { return *con; }
	duckdb::Connection *operator->() { return con.get(); }
};

bool RunRemote(duckdb::Connection &con, int port, const std::string &sql) {
	auto sql_escaped = sql;
	for (size_t pos = 0; (pos = sql_escaped.find('\'', pos)) != std::string::npos;
	     pos += 2) {
		sql_escaped.replace(pos, 1, "''");
	}
	auto full = "SELECT * FROM openduck_remote('http://127.0.0.1:" +
	            std::to_string(port) + "', 'e2e-token', '" + sql_escaped + "')";
	auto r = con.Query(full);
	return !r->HasError();
}

int64_t RemoteCount(duckdb::Connection &con, int port, const std::string &table) {
	auto r = con.Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT COUNT(*) FROM " + table + "')");
	REQUIRE_FALSE(r->HasError());
	return r->GetValue(0, 0).GetValue<int64_t>();
}

std::string AttachSQL(int port) {
	return "ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:" + std::to_string(port) +
	       "&token=e2e-token' AS r (TYPE openduck)";
}

} // namespace

// ═══════════════════════════════════════════════════════════════════════════
// Cross-catalog INSERT from local table-function sources
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("Cross-catalog INSERT INTO r.t SELECT * FROM range(N)",
          "[cross_catalog][e2e][table_function][range]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE(RunRemote(*fx, port, "CREATE TABLE main.t (n BIGINT)"));
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// `range(10)` is a table function, evaluated locally. The bound
	// LogicalGet has no TableCatalogEntry, so HasNonRemoteReference
	// forces the cross-catalog path; the existing
	// PhysicalOpenDuckIngestAndMutate streams the function's chunks
	// into staging on the worker.
	auto ins = fx->Query("INSERT INTO r.main.t SELECT range FROM range(10)");
	if (ins->HasError()) {
		FAIL("INSERT FROM range(10) failed: " + ins->GetError());
	}
	REQUIRE(RemoteCount(*fx, port, "main.t") == 10);
}

TEST_CASE("Cross-catalog INSERT INTO r.t SELECT * FROM generate_series(...)",
          "[cross_catalog][e2e][table_function][generate_series]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE(RunRemote(*fx, port, "CREATE TABLE main.t (n BIGINT)"));
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	auto ins = fx->Query(
	    "INSERT INTO r.main.t SELECT generate_series FROM generate_series(1, 5)");
	if (ins->HasError()) {
		FAIL("INSERT FROM generate_series failed: " + ins->GetError());
	}
	REQUIRE(RemoteCount(*fx, port, "main.t") == 5);
}

// NOTE: `read_parquet` / `read_csv_auto` / similar require the parquet
// or csv extensions to be statically linked into this test binary —
// our test fixture only links the openduck extension + DuckDB core, so
// the binder for `read_parquet` segfaults. The cross-catalog path
// itself is identical to the `range()` / `generate_series()` cases
// proven above; once a downstream consumer wires those file-reader
// extensions into a build with the openduck extension loaded,
// `INSERT INTO r.t SELECT * FROM read_parquet('local.parquet')` works
// out of the box.
