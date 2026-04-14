#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_open_flags.hpp"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace openduck {

// ── Storage interface ───────────────────────────────────────────────────────

class IStorage {
public:
	virtual ~IStorage() = default;
	virtual void Write(uint64_t logical_offset, const void *data, uint64_t len) = 0;
	virtual void Read(uint64_t logical_offset, void *buffer, uint64_t len) const = 0;
	virtual void Fsync() = 0;
	virtual void Truncate(uint64_t new_size) = 0;
	virtual uint64_t GetSize() const = 0;
};

// ── InMemoryStorage ─────────────────────────────────────────────────────────

struct Extent {
	uint64_t logical_start;
	uint64_t length;
	uint32_t layer_index;
	uint64_t phys_start;
};

static constexpr uint32_t ACTIVE_LAYER = UINT32_MAX;

class InMemoryStorage : public IStorage {
public:
	void Write(uint64_t logical_offset, const void *data, uint64_t len) override;
	void Read(uint64_t logical_offset, void *buffer, uint64_t len) const override;
	void Fsync() override;
	void Truncate(uint64_t new_size) override;
	uint64_t GetSize() const override;

private:
	uint8_t ReadByte(uint64_t offset) const;

	mutable std::mutex mu_;
	std::vector<std::vector<uint8_t>> sealed_;
	std::vector<uint8_t> active_;
	std::vector<Extent> extents_;
	uint64_t logical_size_ = 0;
};

// ── BridgeStorage ───────────────────────────────────────────────────────────

#ifdef OPENDUCK_HAS_BRIDGE

class BridgeStorage : public IStorage {
public:
	BridgeStorage(const std::string &db_name, const std::string &postgres_url,
	              const std::string &data_dir, const std::string &snapshot_id = "");
	~BridgeStorage() override;

	BridgeStorage(const BridgeStorage &) = delete;
	BridgeStorage &operator=(const BridgeStorage &) = delete;

	void Write(uint64_t logical_offset, const void *data, uint64_t len) override;
	void Read(uint64_t logical_offset, void *buffer, uint64_t len) const override;
	void Fsync() override;
	void Truncate(uint64_t new_size) override;
	uint64_t GetSize() const override;

	bool IsReadOnly() const { return read_only_; }

private:
	void *handle_;
	mutable std::mutex mu_;
	uint64_t logical_size_ = 0;
	bool read_only_ = false;
};

#endif // OPENDUCK_HAS_BRIDGE

// ── Resolved storage config ─────────────────────────────────────────────────

struct StorageConfig {
	std::string db_name;
	std::string postgres_url; // empty = use InMemoryStorage
	std::string data_dir;     // empty = use InMemoryStorage
	std::string secret_name;  // which secret was used (for logging)
	std::string snapshot_id;  // empty = read/write tip; set = read-only snapshot
};

/// Parsed query parameters from an openduck:// URI.
struct UriParams {
	std::string db_name;
	std::string secret;      // ?secret=NAME
	std::string data_dir;    // ?data_dir=PATH (overrides secret/env)
	std::string snapshot_id; // ?snapshot=UUID (read-only at snapshot)
};

UriParams ParseUri(const std::string &path);

/// Resolve storage config: ?secret=NAME → default secret → env vars → in-memory.
StorageConfig ResolveStorageConfig(const std::string &path, duckdb::optional_ptr<duckdb::FileOpener> opener);

/// Get or create storage for the given config.
std::shared_ptr<IStorage> GetOrCreateStorage(const StorageConfig &config);

// ── FileHandle ──────────────────────────────────────────────────────────────

class OpenDuckFileHandle : public duckdb::FileHandle {
public:
	OpenDuckFileHandle(duckdb::FileSystem &fs, const std::string &path, duckdb::FileOpenFlags flags,
	                   std::shared_ptr<IStorage> storage);
	~OpenDuckFileHandle() override;
	void Close() override;

	std::string db_name;
	duckdb::idx_t position = 0;
	std::shared_ptr<IStorage> storage;
};

// ── FileSystem ──────────────────────────────────────────────────────────────

class OpenDuckFileSystem : public duckdb::FileSystem {
public:
	duckdb::unique_ptr<duckdb::FileHandle> OpenFile(const std::string &path, duckdb::FileOpenFlags flags,
	                                                 duckdb::optional_ptr<duckdb::FileOpener> opener) override;

	void Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, duckdb::idx_t location) override;
	void Write(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, duckdb::idx_t location) override;
	int64_t Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(duckdb::FileHandle &handle) override;
	void Truncate(duckdb::FileHandle &handle, int64_t new_size) override;
	void FileSync(duckdb::FileHandle &handle) override;

	bool FileExists(const std::string &filename, duckdb::optional_ptr<duckdb::FileOpener> opener) override;
	bool CanHandleFile(const std::string &fpath) override;
	std::string GetName() const override;
};

} // namespace openduck
