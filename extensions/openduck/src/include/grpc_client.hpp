#pragma once

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

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
  /// std::runtime_error for other gRPC or server-side errors.
  virtual std::optional<std::string> Next() = 0;
};

class GrpcClient {
public:
  explicit GrpcClient(const std::string &endpoint);
  ~GrpcClient();

  /// Start executing SQL on the remote gateway. Returns a stream that yields
  /// Arrow IPC payloads (each payload is a complete IPC stream: schema +
  /// record batches + EOS).
  std::unique_ptr<GrpcStream> ExecuteSQL(const std::string &sql,
                                         const std::string &database,
                                         const std::string &token);

  /// Cancel a running execution on the remote gateway.
  /// Returns true if the cancellation was acknowledged.
  bool CancelExecution(const std::string &execution_id);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace openduck
