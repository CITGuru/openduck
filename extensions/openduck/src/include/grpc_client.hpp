#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

// Forward-declare the generated protobuf message so callers that only
// need the handle types don't pull in grpc/protobuf headers.
namespace openduck {
namespace v1 {
class ExecuteFragmentError;
} // namespace v1
} // namespace openduck

namespace openduck {

/// Thrown when the gateway/worker is unreachable (gRPC UNAVAILABLE).
class GatewayUnavailableError : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

class GrpcStream {
public:
  virtual ~GrpcStream() = default;

  /// Returns the next Arrow IPC stream payload, or nullopt when the stream is
  /// finished. Throws GatewayUnavailableError when the endpoint is unreachable,
  /// or a DuckDB-typed exception (CatalogException / BinderException / ...)
  /// when the worker returned a typed error.
  virtual std::optional<std::string> Next() = 0;

  /// The execution_id assigned by the gateway for this stream.
  virtual const std::string &ExecutionId() const = 0;
};

/// Column definition for an IngestData stream.
struct IngestColumnSpec {
  std::string name;
  /// SQL type string (e.g. "INTEGER", "DECIMAL(18,4)",
  /// "STRUCT(a INT, b VARCHAR)"). Round-trips DuckDB's `LogicalType`
  /// via `LogicalType::ToString()`; the worker parses it as a DDL
  /// fragment when emitting `CREATE TEMP TABLE`.
  std::string sql_type;
};

/// A client-side handle to an open IngestData stream.
class IngestStream {
public:
  virtual ~IngestStream() = default;

  /// Send one Arrow IPC stream payload. Returns false if the stream
  /// has already been closed by the server.
  virtual bool WriteBatch(const std::string &ipc_stream_payload) = 0;

  /// Finish writing and receive the `IngestReply`. On a typed worker
  /// error, throws a DuckDB-typed exception via `ThrowMappedError`.
  /// Returns `rows_ingested` on success.
  virtual uint64_t Finish() = 0;
};

class GrpcClient {
public:
  explicit GrpcClient(const std::string &endpoint);
  ~GrpcClient();

  /// Start executing SQL on the remote gateway. When `transaction_id` is
  /// non-empty the request is routed to the worker that owns that
  /// transaction. Returns a stream that yields Arrow IPC payloads
  /// (each payload is a complete IPC stream).
  std::unique_ptr<GrpcStream> ExecuteSQL(const std::string &sql,
                                         const std::string &database,
                                         const std::string &token,
                                         const std::string &transaction_id);

  /// Legacy overload — kept for read-path call sites that don't
  /// participate in transactions. Equivalent to passing `""` as
  /// `transaction_id`.
  std::unique_ptr<GrpcStream> ExecuteSQL(const std::string &sql,
                                         const std::string &database,
                                         const std::string &token);

  /// Cancel a running execution on the remote gateway.
  bool CancelExecution(const std::string &execution_id,
                       const std::string &token);

  /// Open a remote transaction on the worker chosen by the gateway.
  /// Returns the opaque transaction_id. Throws a DuckDB-typed exception
  /// if the worker failed to open the transaction.
  std::string BeginTransaction(const std::string &database,
                               const std::string &token);

  /// Commit the given transaction. Throws on worker-side error.
  void CommitTransaction(const std::string &transaction_id,
                         const std::string &token);

  /// Rollback the given transaction. Throws on worker-side error.
  void RollbackTransaction(const std::string &transaction_id,
                           const std::string &token);

  /// Open a client-streaming IngestData RPC bound to a staging
  /// TEMP TABLE. The caller then calls `WriteBatch(...)` zero or more
  /// times, followed by exactly one `Finish()`.
  std::unique_ptr<IngestStream> IngestData(
      const std::string &database,
      const std::string &staging_table,
      const std::vector<IngestColumnSpec> &columns,
      const std::string &transaction_id, // "" for implicit-ingest
      const std::string &token);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;};

} // namespace openduck
