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

// ═══════════════════════════════════════════════════════════════════════════
// Subprocess + fixture plumbing (mirrors test_cross_catalog_insert.cpp)
// ═══════════════════════════════════════════════════════════════════════════

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
		if (sock < 0) {
			return false;
		}
		sockaddr_in addr{};
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		addr.sin_port = htons(port);
		int rc = ::connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
		::close(sock);
		if (rc == 0) {
			return true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	return false;
}

class WorkerSubprocess {
public:
	explicit WorkerSubprocess(int port) : port_(port) {
		auto repo = RepoRootFromSourceFile();
		auto bin = repo + "/target/release/openduck-worker";
		if (!std::filesystem::exists(bin)) {
			FAIL("Worker binary not found at " + bin);
		}
		db_path_ = std::string(std::getenv("TMPDIR") ? std::getenv("TMPDIR") : "/tmp") +
		           "/openduck_udc_" + std::to_string(port) + ".duckdb";
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
		int rc = ::posix_spawn(&pid, bin.c_str(), nullptr, nullptr, argv.data(), env.data());
		REQUIRE(rc == 0);
		pid_ = pid;
		REQUIRE(WaitForPort(port_));
	}

	~WorkerSubprocess() {
		if (pid_ > 0) {
			::kill(pid_, SIGTERM);
			int status = 0;
			for (int i = 0; i < 50; i++) {
				if (::waitpid(pid_, &status, WNOHANG) == pid_) {
					break;
				}
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
	auto result = con.Query(full);
	if (result->HasError()) {
		INFO("RunRemote failed for SQL: " + sql + " -> " + result->GetError());
		return false;
	}
	return true;
}

int64_t RemoteCount(duckdb::Connection &con, int port, const std::string &table) {
	auto result = con.Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT COUNT(*) FROM " + table + "')");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->RowCount() == 1);
	return result->GetValue(0, 0).GetValue<int64_t>();
}

std::string AttachSQL(int port) {
	return "ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:" +
	       std::to_string(port) + "&token=e2e-token' AS r (TYPE openduck)";
}

} // namespace

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("Cross-catalog DELETE with local IN subquery",
          "[cross_catalog][e2e][delete]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	// Local "bad list" table.
	REQUIRE_FALSE(fx->Query("CREATE TABLE banned (uid INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO banned VALUES (2), (4)")->HasError());

	// Remote target with rows 1..5.
	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.users (id INTEGER, name VARCHAR)"));
	REQUIRE(RunRemote(*fx, port,
	                   "INSERT INTO main.users VALUES "
	                   "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"));

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// Cross-catalog DELETE — the IN subquery references `banned` which
	// lives on the local DuckDB, not the remote worker.
	auto del = fx->Query(
	    "DELETE FROM r.main.users WHERE id IN (SELECT uid FROM banned)");
	if (del->HasError()) {
		FAIL("DELETE failed: " + del->GetError());
	}

	// Remote table should have rows 1, 3, 5 after delete.
	REQUIRE(RemoteCount(*fx, port, "main.users") == 3);
	auto survivors = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT id FROM main.users ORDER BY id')");
	REQUIRE_FALSE(survivors->HasError());
	REQUIRE(survivors->RowCount() == 3);
	REQUIRE(survivors->GetValue(0, 0).GetValue<int32_t>() == 1);
	REQUIRE(survivors->GetValue(0, 1).GetValue<int32_t>() == 3);
	REQUIRE(survivors->GetValue(0, 2).GetValue<int32_t>() == 5);
}

TEST_CASE("Cross-catalog UPDATE with local IN subquery",
          "[cross_catalog][e2e][update]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE_FALSE(fx->Query("CREATE TABLE watchlist (uid INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO watchlist VALUES (1), (3)")->HasError());

	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.users (id INTEGER, flag VARCHAR)"));
	REQUIRE(RunRemote(*fx, port,
	                   "INSERT INTO main.users VALUES "
	                   "(1, 'none'), (2, 'none'), (3, 'none')"));

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	auto upd = fx->Query(
	    "UPDATE r.main.users SET flag = 'watched' "
	    "WHERE id IN (SELECT uid FROM watchlist)");
	if (upd->HasError()) {
		FAIL("UPDATE failed: " + upd->GetError());
	}

	// Verify: rows 1 and 3 are 'watched', row 2 is 'none'.
	auto check = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT id, flag FROM main.users ORDER BY id')");
	REQUIRE_FALSE(check->HasError());
	REQUIRE(check->RowCount() == 3);
	REQUIRE(check->GetValue(1, 0).GetValue<std::string>() == "watched");
	REQUIRE(check->GetValue(1, 1).GetValue<std::string>() == "none");
	REQUIRE(check->GetValue(1, 2).GetValue<std::string>() == "watched");
}

TEST_CASE("Cross-catalog CREATE TABLE AS SELECT from local",
          "[cross_catalog][e2e][ctas]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE_FALSE(fx->Query("CREATE TABLE src (id INTEGER, name VARCHAR)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO src VALUES "
	                         "(1, 'alpha'), (2, 'beta'), (3, 'gamma')")
	                  ->HasError());

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// CTAS into remote from local — the source scan is local, so
	// HasNonRemoteReference routes through the cross-catalog path.
	auto ctas = fx->Query("CREATE TABLE r.main.mirror AS SELECT * FROM src");
	if (ctas->HasError()) {
		FAIL("CTAS failed: " + ctas->GetError());
	}

	// Mirror should exist on the remote with 3 rows.
	REQUIRE(RemoteCount(*fx, port, "main.mirror") == 3);
	auto verify = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT id, name FROM main.mirror ORDER BY id')");
	REQUIRE_FALSE(verify->HasError());
	REQUIRE(verify->RowCount() == 3);
	REQUIRE(verify->GetValue(1, 0).GetValue<std::string>() == "alpha");
	REQUIRE(verify->GetValue(1, 2).GetValue<std::string>() == "gamma");
}

// ── Catalog-qualified column refs in WHERE / ON / SET ─────────────────────
//
// These scenarios were broken until the catalog reference rewriter
// learned to (a) strip 3-part `r.users.id` column chains and (b)
// preserve the local table's user-visible name as an alias on the
// staging temp table. Both interactions are exercised together below.

TEST_CASE("Cross-catalog UPDATE …FROM local WHERE r.t.col = local.col",
          "[cross_catalog][e2e][update][3part_colref]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE_FALSE(fx->Query("CREATE TABLE local_src (id INTEGER, name VARCHAR)")
	                  ->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO local_src VALUES "
	                         "(1, 'Alice-via-local'), (2, 'Bob-via-local')")
	                  ->HasError());

	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.users (id INTEGER, name VARCHAR)"));
	REQUIRE(RunRemote(*fx, port,
	                   "INSERT INTO main.users VALUES "
	                   "(1, 'Alice'), (2, 'Bob'), (3, 'Carol')"));

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// 3-part column ref `r.users.id` in WHERE — the rewriter must
	// strip `r.` AND keep `local_src` as an alias on the staging
	// table so `local_src.id` keeps resolving.
	auto upd = fx->Query(
	    "UPDATE r.users SET name = local_src.name "
	    "FROM local_src WHERE r.users.id = local_src.id");
	if (upd->HasError()) {
		FAIL("UPDATE (3-part colref) failed: " + upd->GetError());
	}

	auto check = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT id, name FROM main.users ORDER BY id')");
	REQUIRE_FALSE(check->HasError());
	REQUIRE(check->RowCount() == 3);
	REQUIRE(check->GetValue(1, 0).GetValue<std::string>() == "Alice-via-local");
	REQUIRE(check->GetValue(1, 1).GetValue<std::string>() == "Bob-via-local");
	REQUIRE(check->GetValue(1, 2).GetValue<std::string>() == "Carol");
}

TEST_CASE("Cross-catalog UPDATE …FROM local WHERE r.s.t.col = local.col (4-part)",
          "[cross_catalog][e2e][update][4part_colref]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE_FALSE(fx->Query("CREATE TABLE local_src (id INTEGER, name VARCHAR)")
	                  ->HasError());
	REQUIRE_FALSE(
	    fx->Query("INSERT INTO local_src VALUES (1, 'four-part-form')")
	        ->HasError());

	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.users (id INTEGER, name VARCHAR)"));
	REQUIRE(RunRemote(*fx, port,
	                   "INSERT INTO main.users VALUES (1, 'Alice')"));

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	auto upd = fx->Query(
	    "UPDATE r.main.users SET name = local_src.name "
	    "FROM local_src WHERE r.main.users.id = local_src.id");
	if (upd->HasError()) {
		FAIL("UPDATE (4-part colref) failed: " + upd->GetError());
	}

	auto check = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT name FROM main.users WHERE id = 1')");
	REQUIRE_FALSE(check->HasError());
	REQUIRE(check->RowCount() == 1);
	REQUIRE(check->GetValue(0, 0).GetValue<std::string>() == "four-part-form");
}

TEST_CASE("Cross-catalog DELETE …USING local WHERE r.t.col = local.col",
          "[cross_catalog][e2e][delete][3part_colref]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE_FALSE(fx->Query("CREATE TABLE targets (id INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO targets VALUES (2), (4)")->HasError());

	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.users (id INTEGER, name VARCHAR)"));
	REQUIRE(RunRemote(*fx, port,
	                   "INSERT INTO main.users VALUES "
	                   "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"));

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// USING-style DELETE with a 3-part column ref in WHERE.
	auto del = fx->Query(
	    "DELETE FROM r.users USING targets WHERE r.users.id = targets.id");
	if (del->HasError()) {
		FAIL("DELETE (USING + 3-part colref) failed: " + del->GetError());
	}

	REQUIRE(RemoteCount(*fx, port, "main.users") == 3);
	auto survivors = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT id FROM main.users ORDER BY id')");
	REQUIRE_FALSE(survivors->HasError());
	REQUIRE(survivors->RowCount() == 3);
	REQUIRE(survivors->GetValue(0, 0).GetValue<int32_t>() == 1);
	REQUIRE(survivors->GetValue(0, 1).GetValue<int32_t>() == 3);
	REQUIRE(survivors->GetValue(0, 2).GetValue<int32_t>() == 5);
}

// ── Pure-remote DML through the simple forwarding path ────────────────────
//
// These don't touch any local table — they MUST take the simple
// `BuildMutate` path (forward the SQL as-is to the worker), NOT the
// cross-catalog ingest path. The bug we fixed in
// `HasNonRemoteReference` would have wrongly routed these through
// the ingest path and surfaced the misleading "no non-remote tables
// were found" Binder error. These tests pin that they go through
// the right path now.

TEST_CASE("Pure-remote DELETE on attached catalog (2-part form)",
          "[cross_catalog][e2e][delete][pure_remote][2part]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.t (id INTEGER, n INTEGER)"));
	REQUIRE(RunRemote(*fx, port,
	                   "INSERT INTO main.t VALUES (1,10),(2,20),(3,30)"));

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// 2-part `r.t` form. WHERE has bare column `n` (no qualifier).
	auto del = fx->Query("DELETE FROM r.t WHERE n >= 20");
	if (del->HasError()) {
		FAIL("Pure-remote DELETE failed: " + del->GetError());
	}
	REQUIRE(RemoteCount(*fx, port, "main.t") == 1);
}

TEST_CASE("Pure-remote UPDATE on attached catalog (2-part form)",
          "[cross_catalog][e2e][update][pure_remote][2part]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.t (id INTEGER, name VARCHAR)"));
	REQUIRE(RunRemote(*fx, port,
	                   "INSERT INTO main.t VALUES (1,'a'),(2,'b')"));

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	auto upd = fx->Query("UPDATE r.t SET name = 'X' WHERE id = 1");
	if (upd->HasError()) {
		FAIL("Pure-remote UPDATE failed: " + upd->GetError());
	}

	auto check = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT id, name FROM main.t ORDER BY id')");
	REQUIRE_FALSE(check->HasError());
	REQUIRE(check->RowCount() == 2);
	REQUIRE(check->GetValue(1, 0).GetValue<std::string>() == "X");
	REQUIRE(check->GetValue(1, 1).GetValue<std::string>() == "b");
}

TEST_CASE("Cross-catalog DELETE: self-join on same local table dedupes staging",
          "[cross_catalog][e2e][delete][dedup]") {
	// Regression: if the same local table appears twice (e.g. in a
	// self-join subquery), the planner should materialize it once and
	// reference the single staging name from multiple BaseTableRefs.
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE_FALSE(fx->Query("CREATE TABLE bad (uid INTEGER, flag INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO bad VALUES (2, 1), (4, 1)")->HasError());

	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.users (id INTEGER, name VARCHAR)"));
	REQUIRE(RunRemote(*fx, port,
	                   "INSERT INTO main.users VALUES "
	                   "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"));

	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// The `bad` table is referenced twice in the subquery.
	auto del = fx->Query(
	    "DELETE FROM r.main.users WHERE id IN ("
	    "    SELECT a.uid FROM bad a JOIN bad b ON a.uid = b.uid)");
	if (del->HasError()) {
		FAIL("DELETE (self-join) failed: " + del->GetError());
	}
	REQUIRE(RemoteCount(*fx, port, "main.users") == 2);
}
