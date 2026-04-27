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
// Worker subprocess lifecycle
// ═══════════════════════════════════════════════════════════════════════════
//
// Spawns `target/release/openduck-worker` on a free port. The binary
// lives at a repo-relative path that we resolve via `__FILE__` walks.
// Kills the subprocess on scope exit.

namespace {

std::string RepoRootFromSourceFile() {
	// test_cross_catalog_insert.cpp lives at:
	//   extensions/openduck/test/cpp/
	// → four `..` gets us to the repo root.
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
			FAIL("Worker binary not found at " + bin +
			      ". Build it first: cargo build --release -p exec-worker");
		}

		// Use a persistent DuckDB file so state survives across
		// per-ExecuteFragment connections. The worker opens a fresh
		// `Connection::open(path)` for every auto-commit RPC and
		// reuses a pinned `Connection` across calls inside a
		// transaction — both variants need the file to exist.
		db_path_ = std::string(std::getenv("TMPDIR") ? std::getenv("TMPDIR") : "/tmp") +
		           "/openduck_e2e_" + std::to_string(port) + ".duckdb";
		std::filesystem::remove(db_path_);
		std::filesystem::remove(db_path_ + ".wal");

		listen_env_ = "OPENDUCK_WORKER_LISTEN=127.0.0.1:" + std::to_string(port_);
		token_env_ = "OPENDUCK_TOKEN=e2e-token";
		rust_log_env_ = "RUST_LOG=warn";
		db_env_ = "OPENDUCK_WORKER_DB=" + db_path_;

		std::vector<char *> env;
		for (char **e = environ; *e; e++) {
			// Strip any pre-existing OPENDUCK_TOKEN / OPENDUCK_WORKER_LISTEN
			// so we don't inherit a conflicting one.
			if (std::strncmp(*e, "OPENDUCK_TOKEN=", 15) == 0) {
				continue;
			}
			if (std::strncmp(*e, "OPENDUCK_WORKER_LISTEN=", 23) == 0) {
				continue;
			}
			if (std::strncmp(*e, "OPENDUCK_WORKER_DB=", 19) == 0) {
				continue;
			}
			if (std::strncmp(*e, "RUST_LOG=", 9) == 0) {
				continue;
			}
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
		int rc = ::posix_spawn(&pid, bin.c_str(), nullptr, nullptr,
		                        argv.data(), env.data());
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

	int port() const {
		return port_;
	}

private:
	int port_;
	pid_t pid_ = 0;
	std::string db_path_;
	std::string listen_env_;
	std::string token_env_;
	std::string rust_log_env_;
	std::string db_env_;
};

// ── DuckDB fixture with OpenDuck extension statically registered ──────────

struct LocalDuckDBWithOpenDuck {
	duckdb::DBConfig config;
	std::unique_ptr<duckdb::DuckDB> db;
	std::unique_ptr<duckdb::Connection> con;

	LocalDuckDBWithOpenDuck() {
		// Register storage extensions directly on the config BEFORE
		// opening the DuckDB instance. Production code does this via
		// `DUCKDB_CPP_EXTENSION_ENTRY`; tests link the extension code
		// statically and skip the loader.
		openduck::RegisterStorageExtensionsForTest(config);
		db = std::make_unique<duckdb::DuckDB>(nullptr, &config);
		con = std::make_unique<duckdb::Connection>(*db);
	}

	duckdb::Connection &operator*() {
		return *con;
	}
	duckdb::Connection *operator->() {
		return con.get();
	}
};

} // namespace

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

// Helper: run openduck_remote for a one-shot auxiliary command against
// the test worker. Returns true on success.
static bool RunRemote(duckdb::Connection &con, int port, const std::string &sql) {
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

static int64_t RemoteCount(duckdb::Connection &con, int port,
                            const std::string &table) {
	auto result = con.Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" +
	    std::to_string(port) +
	    "', 'e2e-token', 'SELECT COUNT(*) FROM " + table + "')");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->RowCount() == 1);
	return result->GetValue(0, 0).GetValue<int64_t>();
}

TEST_CASE("Cross-catalog INSERT happy path: rows land on remote",
          "[cross_catalog][e2e]") {
	int port = PickPort();
	WorkerSubprocess worker(port);

	// Set the token for this test (both directions — DuckDB ATTACH URL
	// and the worker-side env).
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	// 1. Prepare a local source table in the client-side DuckDB.
	auto local_setup = fx->Query("CREATE TABLE local_t (id INTEGER, name VARCHAR)");
	REQUIRE_FALSE(local_setup->HasError());
	auto local_insert = fx->Query(
	    "INSERT INTO local_t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");
	REQUIRE_FALSE(local_insert->HasError());

	// 2. ATTACH the remote worker (OpenDuck catalog).
	auto attach_sql =
	    "ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:" + std::to_string(port) +
	    "&token=e2e-token' AS r (TYPE openduck)";
	auto attach = fx->Query(attach_sql);
	if (attach->HasError()) {
		FAIL("ATTACH failed: " + attach->GetError());
	}

	// 3. Create the target table on the remote worker using the
	//    escape-hatch `openduck_remote` table function (avoids
	//    needing DDL forwarding to also be in scope for this test).
	auto create_remote = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" +
	    std::to_string(port) +
	    "', 'e2e-token', "
	    "'CREATE TABLE main.remote_t (id INTEGER, name VARCHAR)')");
	if (create_remote->HasError()) {
		FAIL("Remote CREATE TABLE failed: " + create_remote->GetError());
	}

	// 4. The actual test: INSERT from local into remote (cross-catalog).
	//    Dispatches to OpenDuckCatalog::PlanInsert → HasNonRemoteReference
	//    returns true → BuildCrossCatalogInsert →
	//    PhysicalOpenDuckIngestAndMutate.
	auto insert = fx->Query("INSERT INTO r.main.remote_t SELECT * FROM local_t");
	if (insert->HasError()) {
		FAIL("Cross-catalog INSERT failed: " + insert->GetError());
	}

	// 5. Verify the rows arrived on the remote side by reading back
	//    through openduck_remote.
	auto count = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" +
	    std::to_string(port) +
	    "', 'e2e-token', 'SELECT COUNT(*) FROM main.remote_t')");
	if (count->HasError()) {
		FAIL("Remote count failed: " + count->GetError());
	}
	REQUIRE(count->RowCount() == 1);
	auto val = count->GetValue(0, 0);
	REQUIRE(val.GetValue<int64_t>() == 3);
}

TEST_CASE("Cross-catalog INSERT rolls back when apply-phase constraint fires",
          "[cross_catalog][e2e][rollback]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	// Local source: two rows with duplicate id=1.
	REQUIRE_FALSE(fx->Query("CREATE TABLE local_t (id INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO local_t VALUES (1), (1)")->HasError());

	// Remote target: PRIMARY KEY on id → the second row violates.
	REQUIRE(RunRemote(*fx, port, "CREATE TABLE main.remote_t (id INTEGER PRIMARY KEY)"));

	// ATTACH.
	auto attach = fx->Query("ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:" +
	                         std::to_string(port) +
	                         "&token=e2e-token' AS r (TYPE openduck)");
	REQUIRE_FALSE(attach->HasError());

	// INSERT — staging step appends both rows, apply step fails on
	// PRIMARY KEY, the implicit txn rolls back.
	auto insert = fx->Query("INSERT INTO r.main.remote_t SELECT * FROM local_t");
	REQUIRE(insert->HasError());
	// The exception surfaces as a ConstraintException from the worker,
	// translated by ThrowMappedError — we just check it's an error
	// and the rollback semantics hold below.

	// Verify rollback: remote table must be empty. If the staging temp
	// table had leaked, this would report >0 or fail; if the apply
	// step had half-applied, there'd be a non-zero count. Implicit
	// transaction rollback cleans both up.
	REQUIRE(RemoteCount(*fx, port, "main.remote_t") == 0);
}

TEST_CASE("Cross-catalog INSERT ... RETURNING projects worker-assigned values",
          "[cross_catalog][e2e][returning]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE_FALSE(fx->Query("CREATE TABLE local_t (v VARCHAR)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO local_t VALUES ('x'), ('y'), ('z')")->HasError());
	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.remote_t (id INTEGER DEFAULT 42, v VARCHAR)"));

	auto attach = fx->Query("ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:" +
	                         std::to_string(port) +
	                         "&token=e2e-token' AS r (TYPE openduck)");
	REQUIRE_FALSE(attach->HasError());

	// INSERT with RETURNING — the worker fills the DEFAULT, and the
	// RETURNING projection surfaces through the apply stream back to
	// the local DuckDB executor.
	auto result = fx->Query(
	    "INSERT INTO r.main.remote_t (v) SELECT v FROM local_t RETURNING id, v");
	if (result->HasError()) {
		FAIL("RETURNING INSERT failed: " + result->GetError());
	}
	REQUIRE(result->RowCount() == 3);
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE(result->GetValue(0, i).GetValue<int32_t>() == 42);
	}
	// Row order from RETURNING is implementation-defined — sort then
	// compare the values column.
	std::vector<std::string> vs;
	for (idx_t i = 0; i < 3; i++) {
		vs.push_back(result->GetValue(1, i).GetValue<std::string>());
	}
	std::sort(vs.begin(), vs.end());
	REQUIRE(vs == std::vector<std::string>{"x", "y", "z"});
}

TEST_CASE("Cross-catalog INSERT ... ON CONFLICT DO NOTHING preserves "
          "pre-existing rows",
          "[cross_catalog][e2e][on_conflict]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query("CREATE TABLE local_t (id INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO local_t VALUES (1), (2), (3)")->HasError());

	// Remote target with PRIMARY KEY on `id`. `LookupEntry` round-trips
	// this constraint (via `duckdb_constraints()`) so the client-side
	// binder can resolve the `ON CONFLICT` target.
	REQUIRE(RunRemote(*fx, port,
	                   "CREATE TABLE main.remote_t (id INTEGER PRIMARY KEY)"));
	REQUIRE(RunRemote(*fx, port, "INSERT INTO main.remote_t VALUES (2)"));

	auto attach = fx->Query("ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:" +
	                         std::to_string(port) +
	                         "&token=e2e-token' AS r (TYPE openduck)");
	REQUIRE_FALSE(attach->HasError());

	// id=2 conflicts with the pre-existing row → DO NOTHING skips it.
	// ids 1 and 3 land. Final count: {1, 2, 3} = 3 rows.
	auto insert = fx->Query(
	    "INSERT INTO r.main.remote_t SELECT * FROM local_t ON CONFLICT DO NOTHING");
	if (insert->HasError()) {
		FAIL("ON CONFLICT DO NOTHING failed: " + insert->GetError());
	}
	REQUIRE(RemoteCount(*fx, port, "main.remote_t") == 3);
}

TEST_CASE("Cross-catalog INSERT inside user transaction reuses caller txn",
          "[cross_catalog][e2e][caller_txn]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;

	REQUIRE_FALSE(fx->Query("CREATE TABLE local_t (id INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO local_t VALUES (1), (2)")->HasError());
	REQUIRE(RunRemote(*fx, port, "CREATE TABLE main.remote_t (id INTEGER)"));

	auto attach = fx->Query("ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:" +
	                         std::to_string(port) +
	                         "&token=e2e-token' AS r (TYPE openduck)");
	REQUIRE_FALSE(attach->HasError());

	// Explicit user transaction: the cross-catalog INSERT must reuse
	// the acquired tx (no implicit txn on the operator itself). On
	// COMMIT, both the ingest + apply + staging drop are visible; on
	// ROLLBACK, nothing persists.

	REQUIRE_FALSE(fx->Query("BEGIN")->HasError());
	auto insert = fx->Query("INSERT INTO r.main.remote_t SELECT * FROM local_t");
	REQUIRE_FALSE(insert->HasError());
	// Rows committed to the target are NOT visible yet to an outside
	// connection — they're inside the txn. openduck_remote opens a
	// separate worker connection so it sees pre-commit state (0 rows).
	REQUIRE(RemoteCount(*fx, port, "main.remote_t") == 0);
	REQUIRE_FALSE(fx->Query("COMMIT")->HasError());

	// After COMMIT, the remote rows are visible.
	REQUIRE(RemoteCount(*fx, port, "main.remote_t") == 2);
}
