#include "grpc_client.hpp"

#include <atomic>
#include <chrono>
#include <sstream>

#include <grpcpp/grpcpp.h>

#include "openduck/v1/execution.grpc.pb.h"

namespace openduck {

// ── helpers ─────────────────────────────────────────────────────────────────

static void ThrowForStatus(const grpc::Status &status) {
  if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
    throw GatewayUnavailableError(
        "OpenDuck gateway unreachable: " + status.error_message());
  }
  throw std::runtime_error("gRPC stream error (" +
                           std::to_string(status.error_code()) +
                           "): " + status.error_message());
}

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

static std::string GenerateExecutionId() {
  static std::atomic<uint64_t> counter{0};
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  auto ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
  std::ostringstream oss;
  oss << "exec-" << ns << "-" << counter.fetch_add(1, std::memory_order_relaxed);
  return oss.str();
}

class GrpcStreamImpl : public GrpcStream {
public:
  GrpcStreamImpl(
      std::unique_ptr<grpc::ClientContext> ctx,
      std::unique_ptr<
          grpc::ClientReader<::openduck::v1::ExecuteFragmentChunk>> reader,
      std::string execution_id)
      : ctx_(std::move(ctx)), reader_(std::move(reader)),
        execution_id_(std::move(execution_id)) {}

  const std::string &ExecutionId() const override { return execution_id_; }

  std::optional<std::string> Next() override {
    if (finished_) {
      return std::nullopt;
    }

    ::openduck::v1::ExecuteFragmentChunk chunk;
    if (!reader_->Read(&chunk)) {
      finished_ = true;
      auto status = reader_->Finish();
      if (!status.ok()) {
        ThrowForStatus(status);
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
  std::unique_ptr<grpc::ClientContext> ctx_;
  std::unique_ptr<grpc::ClientReader<::openduck::v1::ExecuteFragmentChunk>>
      reader_;
  std::string execution_id_;
  bool finished_ = false;
};

// ── GrpcClient::ExecuteSQL ─────────────────────────────────────────────────

std::unique_ptr<GrpcStream>
GrpcClient::ExecuteSQL(const std::string &sql, const std::string &database,
                       const std::string &token) {
  auto state = impl_->channel->GetState(true);
  if (state == GRPC_CHANNEL_SHUTDOWN) {
    throw GatewayUnavailableError("OpenDuck gateway channel is shut down");
  }

  auto exec_id = GenerateExecutionId();

  ::openduck::v1::ExecuteFragmentRequest request;
  request.set_plan(sql);
  request.set_database(database);
  request.set_access_token(token);
  request.set_execution_id(exec_id);

  auto ctx = std::make_unique<grpc::ClientContext>();
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(30);
  ctx->set_deadline(deadline);

  auto reader = impl_->stub->ExecuteFragment(ctx.get(), request);
  return std::make_unique<GrpcStreamImpl>(std::move(ctx), std::move(reader),
                                          std::move(exec_id));
}

// ── GrpcClient::CancelExecution ────────────────────────────────────────────

bool GrpcClient::CancelExecution(const std::string &execution_id,
                                 const std::string &token) {
  ::openduck::v1::CancelRequest request;
  request.set_execution_id(execution_id);
  request.set_access_token(token);

  ::openduck::v1::CancelReply reply;
  grpc::ClientContext ctx;
  auto status = impl_->stub->CancelExecution(&ctx, request, &reply);
  if (!status.ok()) {
    return false;
  }
  return reply.acknowledged();
}

} // namespace openduck
