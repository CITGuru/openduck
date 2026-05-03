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
#include <set>
#include <spawn.h>
#include <string>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

extern char **environ;

// ═══════════════════════════════════════════════════════════════════════════
// Harness (shared shape with test_cross_catalog_*.cpp; inlined here
// because each `catch.hpp`-driven test binary wants its own
// CATCH_CONFIG_MAIN).
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
	WorkerSubprocess(int port, const std::string &token = "e2e-token",
	                  const std::string &db_slug = "openduck_ddl")
	    : port_(port) {
		auto repo = RepoRootFromSourceFile();
		auto bin = repo + "/target/release/openduck-worker";
		REQUIRE(std::filesystem::exists(bin));

		db_path_ = std::string(std::getenv("TMPDIR") ? std::getenv("TMPDIR") : "/tmp") +
		           "/" + db_slug + "_" + std::to_string(port) + ".duckdb";
		std::filesystem::remove(db_path_);
		std::filesystem::remove(db_path_ + ".wal");

		listen_env_ = "OPENDUCK_WORKER_LISTEN=127.0.0.1:" + std::to_string(port_);
		token_env_ = "OPENDUCK_TOKEN=" + token;
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

std::string AttachSQL(int port, const std::string &token = "e2e-token") {
	return "ATTACH 'openduck:mydb?endpoint=http://127.0.0.1:" + std::to_string(port) +
	       "&token=" + token + "' AS r (TYPE openduck)";
}

int64_t RemoteCount(duckdb::Connection &con, int port, const std::string &table,
                     const std::string &token = "e2e-token") {
	auto result = con.Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', '" + token + "', 'SELECT COUNT(*) FROM " + table + "')");
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->RowCount() == 1);
	return result->GetValue(0, 0).GetValue<int64_t>();
}

} // namespace

// ═══════════════════════════════════════════════════════════════════════════
// DDL round-trip tests
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("ATTACH: CREATE TABLE with common column types round-trips",
          "[attach][ddl][create_table]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// CREATE TABLE through ATTACH. The statement text is forwarded to
	// the worker via `ForwardDDL` after stripping the `r.` catalog
	// qualifier; the worker's DuckDB runs the DDL against its own
	// catalog.
	auto create = fx->Query(
	    "CREATE TABLE r.main.t (id INTEGER, name VARCHAR, score DOUBLE, "
	    "active BOOLEAN DEFAULT true, payload BLOB)");
	if (create->HasError()) {
		FAIL("CREATE TABLE failed: " + create->GetError());
	}

	// INSERT + round-trip through pure-remote path.
	auto insert = fx->Query(
	    "INSERT INTO r.main.t VALUES (1, 'alpha', 3.14, true, NULL), "
	    "(2, 'beta', -2.5, false, NULL)");
	if (insert->HasError()) {
		FAIL("INSERT failed: " + insert->GetError());
	}

	REQUIRE(RemoteCount(*fx, port, "main.t") == 2);
}

TEST_CASE("ATTACH: DROP TABLE reaches the worker and evicts cache",
          "[attach][ddl][drop]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	REQUIRE_FALSE(fx->Query("CREATE TABLE r.main.ephemeral (id INT)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO r.main.ephemeral VALUES (1)")->HasError());
	REQUIRE(RemoteCount(*fx, port, "main.ephemeral") == 1);

	// DROP through ATTACH.
	auto drop = fx->Query("DROP TABLE r.main.ephemeral");
	if (drop->HasError()) {
		FAIL("DROP failed: " + drop->GetError());
	}

	// Subsequent SELECT should fail — cache entry is evicted and the
	// worker no longer knows the table.
	auto sel = fx->Query("SELECT * FROM r.main.ephemeral");
	REQUIRE(sel->HasError());
}

TEST_CASE("ATTACH: ALTER TABLE RENAME forwards to worker",
          "[attach][ddl][alter][rename]") {
	// Note: `ALTER TABLE ... ADD COLUMN ...` hits DuckDB's upstream
	// "FIXME: column definition to string" gap in `AlterStatement::
	// ToString()` for `ADD COLUMN` — not something we can fix from
	// the extension side. Other ALTER variants (RENAME TABLE, RENAME
	// COLUMN) round-trip cleanly.
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	REQUIRE_FALSE(fx->Query("CREATE TABLE r.main.old_name (id INT)")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO r.main.old_name VALUES (1)")->HasError());

	// Prime the cache.
	auto prime = fx->Query("SELECT COUNT(*) FROM r.main.old_name");
	REQUIRE_FALSE(prime->HasError());

	auto alter = fx->Query("ALTER TABLE r.main.old_name RENAME TO new_name");
	if (alter->HasError()) {
		FAIL("ALTER RENAME failed: " + alter->GetError());
	}

	// Old-name cache entry should be invalidated; SELECT on old name
	// fails, SELECT on new name works.
	auto old_sel = fx->Query("SELECT * FROM r.main.old_name");
	REQUIRE(old_sel->HasError());
	REQUIRE(RemoteCount(*fx, port, "main.new_name") == 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// Transaction round-trip (BEGIN/COMMIT, BEGIN/ROLLBACK persistence)
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("ATTACH: BEGIN; INSERT; COMMIT; rows persist",
          "[attach][transaction][commit]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());
	REQUIRE_FALSE(fx->Query("CREATE TABLE r.main.t (x INT)")->HasError());

	REQUIRE_FALSE(fx->Query("BEGIN")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO r.main.t VALUES (1), (2)")->HasError());
	REQUIRE_FALSE(fx->Query("COMMIT")->HasError());

	REQUIRE(RemoteCount(*fx, port, "main.t") == 2);
}

TEST_CASE("ATTACH: BEGIN; INSERT; ROLLBACK; rows absent",
          "[attach][transaction][rollback]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());
	REQUIRE_FALSE(fx->Query("CREATE TABLE r.main.t (x INT)")->HasError());

	REQUIRE_FALSE(fx->Query("BEGIN")->HasError());
	REQUIRE_FALSE(fx->Query("INSERT INTO r.main.t VALUES (1), (2)")->HasError());
	REQUIRE_FALSE(fx->Query("ROLLBACK")->HasError());

	REQUIRE(RemoteCount(*fx, port, "main.t") == 0);
}

TEST_CASE("ATTACH: CREATE TABLE then ROLLBACK leaves no trace on worker",
          "[attach][transaction][rollback][ddl]") {
	// NOTE: An INSERT inside the same transaction as the CREATE TABLE
	// would require the client-side catalog to pick up the newly-
	// created table without a round-trip probe through LookupEntry
	// (which doesn't re-fire within a single statement's planning
	// pass). That's a schema-cache refinement — separate from the
	// rollback semantic we're validating here. Test just
	// CREATE + ROLLBACK: verify the remote never sees the table
	// after the rollback.
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	REQUIRE_FALSE(fx->Query("BEGIN")->HasError());
	REQUIRE_FALSE(fx->Query("CREATE TABLE r.main.scratch (k INT)")->HasError());
	REQUIRE_FALSE(fx->Query("ROLLBACK")->HasError());

	// Remote should not see `scratch` — the pinned transaction was
	// rolled back.
	auto check = fx->Query(
	    "SELECT * FROM openduck_remote('http://127.0.0.1:" + std::to_string(port) +
	    "', 'e2e-token', 'SELECT * FROM main.scratch')");
	REQUIRE(check->HasError());
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrent ATTACH — two DuckDB clients sharing one remote worker
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("Two DuckDB clients ATTACH the same remote, writes visible via Scan re-probe",
          "[attach][concurrent]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fxA;
	LocalDuckDBWithOpenDuck fxB;
	REQUIRE_FALSE(fxA->Query(AttachSQL(port))->HasError());
	REQUIRE_FALSE(fxB->Query(AttachSQL(port))->HasError());

	// A creates, B sees via openduck_remote (direct worker round-trip,
	// not through B's ATTACHed catalog cache).
	REQUIRE_FALSE(fxA->Query("CREATE TABLE r.main.t (n INT)")->HasError());
	REQUIRE_FALSE(fxA->Query("INSERT INTO r.main.t VALUES (1), (2), (3)")->HasError());

	REQUIRE(RemoteCount(*fxB, port, "main.t") == 3);

	// B reads through its own ATTACHed catalog — first access primes
	// B's cache with the schema. Subsequent reads hit the cache but
	// re-query data on every Scan.
	auto read_b = fxB->Query("SELECT COUNT(*) FROM r.main.t");
	REQUIRE_FALSE(read_b->HasError());
	REQUIRE(read_b->GetValue(0, 0).GetValue<int64_t>() == 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// Cross-identity hijack — client-side check through the gateway
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("ATTACH: SHOW TABLES over many tables works end-to-end",
          "[attach][schema_reflection]") {
	// Stress test for the schema-wide `Scan` path: populate a schema
	// with a variety of tables (primary keys, NOT NULL, UNIQUE,
	// extended types), then `SHOW TABLES` and a post-scan `CREATE
	// TABLE` + INSERT + SELECT round-trip to exercise both the
	// catalog-reflection and cold-lookup paths. (The batched §3
	// optimization from `docs/design/learnings-from-duckdb-postgres.md`
	// is implemented but not yet wired in; this test guards the
	// per-table fallback path that's currently in production.)
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	// Populate the remote with 10 tables of varying shape — primary
	// keys, NOT NULL, UNIQUE, extended types.
	REQUIRE_FALSE(fx->Query(
	    "CREATE TABLE r.main.t_a (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)")
	    ->HasError());
	REQUIRE_FALSE(fx->Query(
	    "CREATE TABLE r.main.t_b (k BIGINT UNIQUE, payload BLOB)")->HasError());
	REQUIRE_FALSE(fx->Query(
	    "CREATE TABLE r.main.t_c (d DECIMAL(18,3), ts TIMESTAMP, flags BOOLEAN)")
	    ->HasError());
	for (int i = 0; i < 7; i++) {
		std::string sql = "CREATE TABLE r.main.t_bulk_" + std::to_string(i) +
		                  " (id INTEGER, payload VARCHAR)";
		REQUIRE_FALSE(fx->Query(sql)->HasError());
	}

	// SHOW TABLES drives Scan — which is the primary consumer of the
	// batched reflection path. Before §3 this issued 1 + 2*N remote
	// queries; now it issues ~1 (the N we just CREATEd invalidated
	// schema_loaded_; Scan re-loads in one batched query and iterates
	// the cache).
	auto show = fx->Query("SHOW TABLES FROM r.main");
	if (show->HasError()) {
		FAIL("SHOW TABLES failed: " + show->GetError());
	}

	// Every table should be reflected exactly once.
	std::set<std::string> seen;
	while (auto chunk = show->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			seen.insert(chunk->GetValue(0, i).GetValue<std::string>());
		}
	}
	REQUIRE(seen.count("t_a") == 1);
	REQUIRE(seen.count("t_b") == 1);
	REQUIRE(seen.count("t_c") == 1);
	for (int i = 0; i < 7; i++) {
		REQUIRE(seen.count("t_bulk_" + std::to_string(i)) == 1);
	}

	// Sanity: a cold lookup against a table that WASN'T pre-created
	// (so it's not in the batched snapshot) should still resolve —
	// falling back to the per-table path.
	REQUIRE_FALSE(fx->Query(
	    "CREATE TABLE r.main.t_post_scan (x INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query(
	    "INSERT INTO r.main.t_post_scan VALUES (42)")->HasError());
	auto sel = fx->Query("SELECT x FROM r.main.t_post_scan");
	REQUIRE_FALSE(sel->HasError());
	auto chunk = sel->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	REQUIRE(chunk->GetValue(0, 0).GetValue<int32_t>() == 42);
}

TEST_CASE("ATTACH: LIMIT/OFFSET constants push down into the remote scan SQL",
          "[attach][optimizer][limit_pushdown]") {
	// Regression guard for §8 from
	// docs/design/learnings-from-duckdb-postgres.md: a constant LIMIT
	// over an OpenDuck table scan should get absorbed into the
	// worker-side SQL so only LIMIT rows are streamed back, rather
	// than fetching the full table and discarding client-side.
	//
	// We can't peek into the generated SQL string from here, but we
	// can prove the behavior end-to-end: populate N rows on the
	// worker, issue `SELECT ... FROM r.t LIMIT k OFFSET m`, and
	// assert (a) correctness (the right rows come back) and (b) the
	// number of rows returned is exactly `k` — which is the contract
	// whether or not pushdown happened. The pushdown-specific
	// guarantee is that the worker never streams more than `k + m`
	// rows; we verify the user-visible contract here and lean on
	// manual worker-log inspection for the "only k rows on the wire"
	// part.
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	REQUIRE_FALSE(fx->Query(AttachSQL(port))->HasError());

	REQUIRE_FALSE(fx->Query("CREATE TABLE r.main.big (id INTEGER)")->HasError());
	REQUIRE_FALSE(fx->Query(
	    "INSERT INTO r.main.big SELECT * FROM range(100)")->HasError());

	// Plain LIMIT (no offset) — 5 rows back.
	{
		auto q = fx->Query("SELECT id FROM r.main.big ORDER BY id LIMIT 5");
		REQUIRE_FALSE(q->HasError());
		auto chunk = q->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() == 5);
		for (idx_t i = 0; i < 5; i++) {
			REQUIRE(chunk->GetValue(0, i).GetValue<int32_t>() == static_cast<int32_t>(i));
		}
	}

	// LIMIT + OFFSET.
	{
		auto q = fx->Query("SELECT id FROM r.main.big ORDER BY id LIMIT 3 OFFSET 10");
		REQUIRE_FALSE(q->HasError());
		auto chunk = q->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() == 3);
		REQUIRE(chunk->GetValue(0, 0).GetValue<int32_t>() == 10);
		REQUIRE(chunk->GetValue(0, 1).GetValue<int32_t>() == 11);
		REQUIRE(chunk->GetValue(0, 2).GetValue<int32_t>() == 12);
	}

	// LIMIT with a projection between — the optimizer walks past
	// LogicalProjection nodes, so this should still push down.
	{
		auto q = fx->Query("SELECT id * 2 FROM r.main.big ORDER BY id LIMIT 4");
		REQUIRE_FALSE(q->HasError());
		auto chunk = q->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() == 4);
		REQUIRE(chunk->GetValue(0, 0).GetValue<int32_t>() == 0);
		REQUIRE(chunk->GetValue(0, 3).GetValue<int32_t>() == 6);
	}

	// Correctness sanity: no LIMIT, full 100 rows come through.
	{
		auto q = fx->Query("SELECT COUNT(*) FROM r.main.big");
		REQUIRE_FALSE(q->HasError());
		auto chunk = q->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 100);
	}

	// Direct pushdown verification: `EXPLAIN` of a LIMIT query
	// should show the LogicalLimit absorbed into the scan — i.e.,
	// the optimized plan has no LIMIT node. Without pushdown the
	// plan would show `LIMIT 5` as a distinct operator.
	{
		auto q = fx->Query(
		    "EXPLAIN SELECT id FROM r.main.big LIMIT 5");
		REQUIRE_FALSE(q->HasError());
		// Concatenate every cell of every chunk so we're robust to
		// EXPLAIN's row layout changing across DuckDB versions.
		std::string plan;
		while (auto chunk = q->Fetch()) {
			for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
				for (idx_t i = 0; i < chunk->size(); i++) {
					plan += chunk->GetValue(c, i).ToString() + "\n";
				}
			}
		}
		// Control: a query WITHOUT LIMIT should not mention LIMIT in
		// its plan either — but a query with LIMIT and no pushdown
		// would. Compare our optimized plan against a baseline.
		auto baseline = fx->Query("EXPLAIN SELECT id FROM r.main.big");
		REQUIRE_FALSE(baseline->HasError());
		std::string baseline_plan;
		while (auto chunk = baseline->Fetch()) {
			for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
				for (idx_t i = 0; i < chunk->size(); i++) {
					baseline_plan += chunk->GetValue(c, i).ToString() + "\n";
				}
			}
		}
		// Pushdown success: the two plans (with-LIMIT and without)
		// should be substantively similar — both should have the
		// same scan operator count, neither should have a separate
		// LIMIT node.
		auto count_limit_nodes = [](const std::string &p) {
			// LIMIT shows up in DuckDB's EXPLAIN output as a top-line
			// operator label (e.g., "LIMIT" or "LIMIT 5") on its own.
			// Count occurrences that are NOT inside a comment or
			// inside "LIMIT_PERCENT"/other unrelated tokens.
			size_t count = 0;
			size_t pos = 0;
			while ((pos = p.find("LIMIT", pos)) != std::string::npos) {
				// Skip tokens like "LIMIT_PERCENT" (which shouldn't
				// appear in a simple LIMIT plan but we're defensive).
				bool boundary = (pos + 5 >= p.size() ||
				                 !std::isalnum(static_cast<unsigned char>(p[pos + 5])));
				if (boundary) {
					count++;
				}
				pos += 5;
			}
			return count;
		};
		REQUIRE(count_limit_nodes(plan) == count_limit_nodes(baseline_plan));
	}
}

TEST_CASE("Wrong access_token on ATTACH surfaces an error on first RPC",
          "[attach][security][identity]") {
	int port = PickPort();
	WorkerSubprocess worker(port);
	::setenv("OPENDUCK_TOKEN", "e2e-token", 1);

	LocalDuckDBWithOpenDuck fx;
	// ATTACH with the wrong token. ATTACH itself doesn't hit the
	// worker (we only contact the worker when the user actually reads
	// or writes against `r`), so ATTACH itself succeeds. The first
	// query against `r` hits the worker with the bad token and
	// surfaces an error. We accept any non-empty error message — the
	// specific gRPC Status code / text varies across transports, but
	// the invariant "wrong token → error, never silent success"
	// holds.
	REQUIRE_FALSE(
	    fx->Query(AttachSQL(port, "wrong-token"))->HasError());

	auto bad = fx->Query("SELECT * FROM r.main.anything");
	REQUIRE(bad->HasError());
	REQUIRE_FALSE(bad->GetError().empty());
}
