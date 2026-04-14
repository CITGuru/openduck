#include "grpc_client.hpp"

#include <grpcpp/grpcpp.h>

#include "openduck/v1/execution.grpc.pb.h"

namespace openduck {

// ── Impl (pimpl hides gRPC types from the header) ──────────────────────────

struct GrpcClient::Impl {
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<::openduck::v1::ExecutionService::Stub> stub;
};

GrpcClient::GrpcClient(const std::string &endpoint)
    : impl_(std::make_unique<Impl>()) {
  impl_->channel =
      grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
  impl_->stub = ::openduck::v1::ExecutionService::NewStub(impl_->channel);
}

GrpcClient::~GrpcClient() = default;

// ── GrpcStreamImpl ─────────────────────────────────────────────────────────

class GrpcStreamImpl : public GrpcStream {
public:
  GrpcStreamImpl(
      std::unique_ptr<grpc::ClientContext> ctx,
      std::unique_ptr<
          grpc::ClientReader<::openduck::v1::ExecuteFragmentChunk>> reader)
      : ctx_(std::move(ctx)), reader_(std::move(reader)) {}

  std::optional<std::string> Next() override {
    if (finished_) {
      return std::nullopt;
    }

    ::openduck::v1::ExecuteFragmentChunk chunk;
    if (!reader_->Read(&chunk)) {
      finished_ = true;
      auto status = reader_->Finish();
      if (!status.ok()) {
        throw std::runtime_error("gRPC stream error: " +
                                 status.error_message());
      }
      return std::nullopt;
    }

    switch (chunk.payload_case()) {
    case ::openduck::v1::ExecuteFragmentChunk::kArrowBatch:
      return chunk.arrow_batch().ipc_stream_payload();
    case ::openduck::v1::ExecuteFragmentChunk::kError:
      finished_ = true;
      throw std::runtime_error("Remote execution error: " + chunk.error());
    case ::openduck::v1::ExecuteFragmentChunk::kFinished:
      finished_ = true;
      return std::nullopt;
    default:
      return std::nullopt;
    }
  }

private:
  // ctx_ declared before reader_ so reader_ is destroyed first
  std::unique_ptr<grpc::ClientContext> ctx_;
  std::unique_ptr<grpc::ClientReader<::openduck::v1::ExecuteFragmentChunk>>
      reader_;
  bool finished_ = false;
};

// ── GrpcClient::ExecuteSQL ─────────────────────────────────────────────────

std::unique_ptr<GrpcStream>
GrpcClient::ExecuteSQL(const std::string &sql, const std::string &database,
                       const std::string &token) {
  ::openduck::v1::ExecuteFragmentRequest request;
  request.set_plan(sql);
  request.set_database(database);
  request.set_access_token(token);

  auto ctx = std::make_unique<grpc::ClientContext>();
  auto reader = impl_->stub->ExecuteFragment(ctx.get(), request);
  return std::make_unique<GrpcStreamImpl>(std::move(ctx), std::move(reader));
}

} // namespace openduck
