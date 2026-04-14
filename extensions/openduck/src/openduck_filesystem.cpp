#include "openduck_filesystem.hpp"

#ifdef OPENDUCK_HAS_BRIDGE
#include "openduck_bridge.h"
#endif

#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <stdexcept>

namespace openduck {

using namespace duckdb;

static const std::string SCHEME = "openduck://";
static const std::string DEFAULT_SECRET_NAME = "openduck_storage";

// ═════════════════════════════════════════════════════════════════════════════
// InMemoryStorage — append-only layers, extent overlay, newest-write-wins.
// ═════════════════════════════════════════════════════════════════════════════

uint8_t InMemoryStorage::ReadByte(uint64_t offset) const {
	for (auto it = extents_.rbegin(); it != extents_.rend(); ++it) {
		if (offset < it->logical_start || offset >= it->logical_start + it->length) {
			continue;
		}
		uint64_t within = offset - it->logical_start;
		uint64_t phys = it->phys_start + within;
		const auto &layer = (it->layer_index == ACTIVE_LAYER) ? active_ : sealed_[it->layer_index];
		if (phys < layer.size()) {
			return layer[static_cast<size_t>(phys)];
		}
		return 0;
	}
	return 0;
}

void InMemoryStorage::Write(uint64_t logical_offset, const void *data, uint64_t len) {
	std::lock_guard<std::mutex> lock(mu_);
	uint64_t phys_start = active_.size();
	auto bytes = static_cast<const uint8_t *>(data);
	active_.insert(active_.end(), bytes, bytes + len);
	extents_.push_back(Extent {logical_offset, len, ACTIVE_LAYER, phys_start});
	uint64_t end = logical_offset + len;
	if (end > logical_size_) {
		logical_size_ = end;
	}
}

void InMemoryStorage::Read(uint64_t logical_offset, void *buffer, uint64_t len) const {
	std::lock_guard<std::mutex> lock(mu_);
	auto out = static_cast<uint8_t *>(buffer);
	for (uint64_t i = 0; i < len; i++) {
		out[i] = ReadByte(logical_offset + i);
	}
}

void InMemoryStorage::Fsync() {
}

void InMemoryStorage::Truncate(uint64_t new_size) {
	std::lock_guard<std::mutex> lock(mu_);
	logical_size_ = new_size;
}

uint64_t InMemoryStorage::GetSize() const {
	std::lock_guard<std::mutex> lock(mu_);
	return logical_size_;
}

// ═════════════════════════════════════════════════════════════════════════════
// BridgeStorage — delegates to Rust PgStorageBackend via C ABI.
// ═════════════════════════════════════════════════════════════════════════════

#ifdef OPENDUCK_HAS_BRIDGE

BridgeStorage::BridgeStorage(const std::string &db_name, const std::string &postgres_url,
                             const std::string &data_dir, const std::string &snapshot_id)
    : handle_(nullptr), read_only_(!snapshot_id.empty()) {
	if (!snapshot_id.empty()) {
		handle_ = openduck_bridge_open_snapshot(db_name.c_str(), postgres_url.c_str(),
		                                       data_dir.c_str(), snapshot_id.c_str());
	} else {
		handle_ = openduck_bridge_open(db_name.c_str(), postgres_url.c_str(), data_dir.c_str());
	}
	if (!handle_) {
		const char *err = openduck_bridge_last_error();
		throw std::runtime_error(std::string("BridgeStorage open failed: ") + (err ? err : "unknown"));
	}
}

BridgeStorage::~BridgeStorage() {
	if (handle_) {
		openduck_bridge_close(static_cast<BridgeHandle *>(handle_));
	}
}

void BridgeStorage::Write(uint64_t logical_offset, const void *data, uint64_t len) {
	if (read_only_) {
		throw std::runtime_error("BridgeStorage: cannot write to a read-only snapshot handle");
	}
	std::lock_guard<std::mutex> lock(mu_);
	int rc = openduck_bridge_write(static_cast<BridgeHandle *>(handle_), logical_offset,
	                               static_cast<const uint8_t *>(data), len);
	if (rc != 0) {
		const char *err = openduck_bridge_last_error();
		throw std::runtime_error(std::string("BridgeStorage write: ") + (err ? err : "unknown"));
	}
	uint64_t end = logical_offset + len;
	if (end > logical_size_) {
		logical_size_ = end;
	}
}

void BridgeStorage::Read(uint64_t logical_offset, void *buffer, uint64_t len) const {
	std::lock_guard<std::mutex> lock(mu_);
	int rc = openduck_bridge_read(static_cast<BridgeHandle *>(handle_), logical_offset,
	                              static_cast<uint8_t *>(buffer), len);
	if (rc != 0) {
		const char *err = openduck_bridge_last_error();
		throw std::runtime_error(std::string("BridgeStorage read: ") + (err ? err : "unknown"));
	}
}

void BridgeStorage::Fsync() {
	std::lock_guard<std::mutex> lock(mu_);
	int rc = openduck_bridge_fsync(static_cast<BridgeHandle *>(handle_));
	if (rc != 0) {
		const char *err = openduck_bridge_last_error();
		throw std::runtime_error(std::string("BridgeStorage fsync: ") + (err ? err : "unknown"));
	}
}

void BridgeStorage::Truncate(uint64_t new_size) {
	std::lock_guard<std::mutex> lock(mu_);
	logical_size_ = new_size;
}

uint64_t BridgeStorage::GetSize() const {
	std::lock_guard<std::mutex> lock(mu_);
	return logical_size_;
}

#endif // OPENDUCK_HAS_BRIDGE

// ═════════════════════════════════════════════════════════════════════════════
// URI parsing
// ═════════════════════════════════════════════════════════════════════════════

static std::unordered_map<std::string, std::string> ParseQueryParams(const std::string &query) {
	std::unordered_map<std::string, std::string> out;
	size_t start = 0;
	while (start < query.size()) {
		size_t amp = query.find('&', start);
		if (amp == std::string::npos) {
			amp = query.size();
		}
		auto part = query.substr(start, amp - start);
		auto eq = part.find('=');
		if (eq != std::string::npos) {
			out[part.substr(0, eq)] = part.substr(eq + 1);
		}
		start = amp + 1;
	}
	return out;
}

UriParams ParseUri(const std::string &path) {
	UriParams uri;
	auto after = path.substr(SCHEME.size());
	auto qmark = after.find('?');
	auto path_part = (qmark == std::string::npos) ? after : after.substr(0, qmark);
	auto slash = path_part.find('/');
	uri.db_name = (slash == std::string::npos) ? path_part : path_part.substr(0, slash);

	if (qmark != std::string::npos) {
		auto params = ParseQueryParams(after.substr(qmark + 1));
		auto it = params.find("secret");
		if (it != params.end()) {
			uri.secret = it->second;
		}
		auto dir_it = params.find("data_dir");
		if (dir_it != params.end()) {
			uri.data_dir = dir_it->second;
		}
		auto snap_it = params.find("snapshot");
		if (snap_it != params.end()) {
			uri.snapshot_id = snap_it->second;
		}
	}
	return uri;
}

// ═════════════════════════════════════════════════════════════════════════════
// Config resolution: ?secret=NAME → default secret → env vars → in-memory
// ═════════════════════════════════════════════════════════════════════════════

static bool TryReadSecret(SecretManager &sm, CatalogTransaction &transaction,
                          const std::string &name, StorageConfig &config) {
	auto entry = sm.GetSecretByName(transaction, name);
	if (!entry || !entry->secret) {
		return false;
	}

	auto *kv = dynamic_cast<const KeyValueSecret *>(entry->secret.get());
	if (!kv) {
		return false;
	}

	Value val;
	if (kv->TryGetValue("postgres_url", val) && !val.IsNull()) {
		config.postgres_url = val.ToString();
	}
	if (kv->TryGetValue("data_dir", val) && !val.IsNull()) {
		config.data_dir = val.ToString();
	}
	config.secret_name = name;
	return !config.postgres_url.empty();
}

StorageConfig ResolveStorageConfig(const std::string &path, optional_ptr<FileOpener> opener) {
	auto uri = ParseUri(path);

	StorageConfig config;
	config.db_name = uri.db_name;

	auto *sm = FileOpener::TryGetSecretManager(opener).get();
	auto catalog_txn = FileOpener::TryGetCatalogTransaction(opener);

	if (sm && catalog_txn) {
		// 1. ?secret=NAME — explicit name from URI
		if (!uri.secret.empty()) {
			if (!TryReadSecret(*sm, *catalog_txn, uri.secret, config)) {
				std::cerr << "warning: secret '" << uri.secret << "' not found or missing postgres_url" << std::endl;
			}
		}

		// 2. Default secret "openduck_storage"
		if (config.postgres_url.empty()) {
			TryReadSecret(*sm, *catalog_txn, DEFAULT_SECRET_NAME, config);
		}
	}

	// 3. Environment variables (if secret didn't provide postgres_url)
	if (config.postgres_url.empty()) {
		const char *pg_env = std::getenv("OPENDUCK_POSTGRES_URL");
		const char *dir_env = std::getenv("OPENDUCK_DATA_DIR");
		if (pg_env && pg_env[0] != '\0') {
			config.postgres_url = pg_env;
			config.data_dir = (dir_env && dir_env[0] != '\0') ? dir_env : "/tmp/openduck";
			config.secret_name = "env";
		}
	}

	// ?data_dir= in URI overrides whatever was resolved above
	if (!uri.data_dir.empty()) {
		config.data_dir = uri.data_dir;
	}

	// ?snapshot= for read-only snapshot access
	if (!uri.snapshot_id.empty()) {
		config.snapshot_id = uri.snapshot_id;
	}

	return config;
}

// ═════════════════════════════════════════════════════════════════════════════
// Global storage registry
// ═════════════════════════════════════════════════════════════════════════════

static std::mutex g_registry_mu;
static std::unordered_map<std::string, std::shared_ptr<IStorage>> g_registry;

static std::string RegistryKey(const StorageConfig &config) {
	std::string key = config.db_name;
	if (!config.postgres_url.empty()) {
		key += "|" + config.postgres_url;
	}
	if (!config.snapshot_id.empty()) {
		key += "@" + config.snapshot_id;
	}
	return key;
}

std::shared_ptr<IStorage> GetOrCreateStorage(const StorageConfig &config) {
	std::lock_guard<std::mutex> lock(g_registry_mu);
	auto key = RegistryKey(config);
	auto it = g_registry.find(key);
	if (it != g_registry.end()) {
		return it->second;
	}

	std::shared_ptr<IStorage> storage;

	if (!config.postgres_url.empty() && !config.data_dir.empty()) {
#ifdef OPENDUCK_HAS_BRIDGE
		storage = std::make_shared<BridgeStorage>(config.db_name, config.postgres_url,
		                                          config.data_dir, config.snapshot_id);
#else
		std::cerr << "warning: storage config has postgres_url but extension was built without bridge support. "
		          << "Using InMemoryStorage. Rebuild with OPENDUCK_BRIDGE_LIB to enable." << std::endl;
		storage = std::make_shared<InMemoryStorage>();
#endif
	} else {
		storage = std::make_shared<InMemoryStorage>();
	}

	g_registry[key] = storage;
	return storage;
}

// ═════════════════════════════════════════════════════════════════════════════
// FileHandle
// ═════════════════════════════════════════════════════════════════════════════

OpenDuckFileHandle::OpenDuckFileHandle(FileSystem &fs, const std::string &path, FileOpenFlags flags,
                                       std::shared_ptr<IStorage> storage)
    : FileHandle(fs, path, flags), db_name(ParseUri(path).db_name), storage(std::move(storage)) {
}

OpenDuckFileHandle::~OpenDuckFileHandle() = default;

void OpenDuckFileHandle::Close() {
}

// ═════════════════════════════════════════════════════════════════════════════
// FileSystem
// ═════════════════════════════════════════════════════════════════════════════

unique_ptr<FileHandle> OpenDuckFileSystem::OpenFile(const std::string &path, FileOpenFlags flags,
                                                     optional_ptr<FileOpener> opener) {
	auto config = ResolveStorageConfig(path, opener);
	auto storage = GetOrCreateStorage(config);
	return make_uniq<OpenDuckFileHandle>(*this, path, flags, std::move(storage));
}

void OpenDuckFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &odh = handle.Cast<OpenDuckFileHandle>();
	odh.storage->Read(static_cast<uint64_t>(location), buffer, static_cast<uint64_t>(nr_bytes));
}

void OpenDuckFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &odh = handle.Cast<OpenDuckFileHandle>();
	odh.storage->Write(static_cast<uint64_t>(location), buffer, static_cast<uint64_t>(nr_bytes));
}

int64_t OpenDuckFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &odh = handle.Cast<OpenDuckFileHandle>();
	odh.storage->Read(static_cast<uint64_t>(odh.position), buffer, static_cast<uint64_t>(nr_bytes));
	odh.position += static_cast<idx_t>(nr_bytes);
	return nr_bytes;
}

int64_t OpenDuckFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &odh = handle.Cast<OpenDuckFileHandle>();
	odh.storage->Write(static_cast<uint64_t>(odh.position), buffer, static_cast<uint64_t>(nr_bytes));
	odh.position += static_cast<idx_t>(nr_bytes);
	return nr_bytes;
}

int64_t OpenDuckFileSystem::GetFileSize(FileHandle &handle) {
	auto &odh = handle.Cast<OpenDuckFileHandle>();
	return static_cast<int64_t>(odh.storage->GetSize());
}

void OpenDuckFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &odh = handle.Cast<OpenDuckFileHandle>();
	odh.storage->Truncate(static_cast<uint64_t>(new_size));
}

void OpenDuckFileSystem::FileSync(FileHandle &handle) {
	auto &odh = handle.Cast<OpenDuckFileHandle>();
	odh.storage->Fsync();
}

bool OpenDuckFileSystem::FileExists(const std::string &filename, optional_ptr<FileOpener> /*opener*/) {
	return filename.rfind(SCHEME, 0) == 0;
}

bool OpenDuckFileSystem::CanHandleFile(const std::string &fpath) {
	return fpath.rfind(SCHEME, 0) == 0;
}

std::string OpenDuckFileSystem::GetName() const {
	return "OpenDuckFileSystem";
}

} // namespace openduck
