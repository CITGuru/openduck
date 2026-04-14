#pragma once

#include <memory>
#include <optional>
#include <string>

namespace openduck {

class GrpcStream {
public:
  virtual ~GrpcStream() = default;

  /// Returns the next Arrow IPC stream payload, or nullopt when the stream is
  /// finished. Throws std::runtime_error on gRPC or server-side errors.
  virtual std::optional<std::string> Next() = 0;
};

class GrpcClient {
public:
  explicit GrpcClient(const std::string &endpoint);
  ~GrpcClient();

  /// Start executing SQL on the remote gateway. Returns a stream that yields
  /// Arrow IPC payloads (each payload is a complete IPC stream: schema + one
  /// record batch + EOS).
  std::unique_ptr<GrpcStream> ExecuteSQL(const std::string &sql,
                                         const std::string &database,
                                         const std::string &token);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace openduck
