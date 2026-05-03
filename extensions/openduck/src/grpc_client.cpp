#include "grpc_client.hpp"
#include "openduck_errors.hpp"

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

static std::string StripScheme(const std::string &endpoint) {
  if (endpoint.rfind("http://", 0) == 0) {
    return endpoint.substr(7);
  }
  if (endpoint.rfind("https://", 0) == 0) {
    return endpoint.substr(8);
  }
  return endpoint;
}

GrpcClient::GrpcClient(const std::string &endpoint)
    : impl_(std::make_unique<Impl>()) {
  auto target = StripScheme(endpoint);
  impl_->channel =
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
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

    // Typed-error path. The chunk MAY carry a typed_error alongside
    // a legacy string error; prefer the typed form so the caller gets
    // a CatalogException / BinderException / ... instead of a generic
    // runtime_error.
    switch (chunk.payload_case()) {
    case ::openduck::v1::ExecuteFragmentChunk::kArrowBatch:
      return chunk.arrow_batch().ipc_stream_payload();
    case ::openduck::v1::ExecuteFragmentChunk::kError:
      finished_ = true;
      if (chunk.has_typed_error()) {
        ThrowMappedError(chunk.typed_error());
      }
      ThrowMappedErrorFromMessage(chunk.error());
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
                       const std::string &token,
                       const std::string &transaction_id) {
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
  if (!transaction_id.empty()) {
    request.set_transaction_id(transaction_id);
  }

  auto ctx = std::make_unique<grpc::ClientContext>();
  // Transactional fragments get a longer deadline; they can carry DDL or
  // DML that takes meaningfully longer than an auto-commit SELECT.
  auto deadline_sec = transaction_id.empty() ? 30 : 300;
  auto deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(deadline_sec);
  ctx->set_deadline(deadline);

  auto reader = impl_->stub->ExecuteFragment(ctx.get(), request);
  return std::make_unique<GrpcStreamImpl>(std::move(ctx), std::move(reader),
                                          std::move(exec_id));
}

std::unique_ptr<GrpcStream>
GrpcClient::ExecuteSQL(const std::string &sql, const std::string &database,
                       const std::string &token) {
  return ExecuteSQL(sql, database, token, std::string());
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

// ── Transaction RPCs ───────────────────────────────────────────────────────

std::string GrpcClient::BeginTransaction(const std::string &database,
                                         const std::string &token) {
  ::openduck::v1::BeginTransactionRequest request;
  request.set_database(database);
  request.set_access_token(token);

  ::openduck::v1::BeginTransactionReply reply;
  grpc::ClientContext ctx;
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(60);
  ctx.set_deadline(deadline);

  auto status = impl_->stub->BeginTransaction(&ctx, request, &reply);
  if (!status.ok()) {
    ThrowForStatus(status);
  }
  if (reply.has_typed_error()) {
    ThrowMappedError(reply.typed_error());
  }
  return reply.transaction_id();
}

void GrpcClient::CommitTransaction(const std::string &transaction_id,
                                    const std::string &token) {
  ::openduck::v1::CommitTransactionRequest request;
  request.set_transaction_id(transaction_id);
  request.set_access_token(token);

  ::openduck::v1::CommitTransactionReply reply;
  grpc::ClientContext ctx;
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(60);
  ctx.set_deadline(deadline);

  auto status = impl_->stub->CommitTransaction(&ctx, request, &reply);
  if (!status.ok()) {
    ThrowForStatus(status);
  }
  if (reply.has_typed_error()) {
    ThrowMappedError(reply.typed_error());
  }
}

void GrpcClient::RollbackTransaction(const std::string &transaction_id,
                                      const std::string &token) {
  ::openduck::v1::RollbackTransactionRequest request;
  request.set_transaction_id(transaction_id);
  request.set_access_token(token);

  ::openduck::v1::RollbackTransactionReply reply;
  grpc::ClientContext ctx;
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(60);
  ctx.set_deadline(deadline);

  auto status = impl_->stub->RollbackTransaction(&ctx, request, &reply);
  if (!status.ok()) {
    ThrowForStatus(status);
  }
  if (reply.has_typed_error()) {
    ThrowMappedError(reply.typed_error());
  }
}

// ── IngestData ─────────────────────────────────────────────────────────────

class IngestStreamImpl : public IngestStream {
public:
  IngestStreamImpl(
      std::unique_ptr<grpc::ClientContext> ctx,
      std::unique_ptr<grpc::ClientWriter<::openduck::v1::IngestChunk>> writer,
      std::shared_ptr<::openduck::v1::IngestReply> reply)
      : ctx_(std::move(ctx)), writer_(std::move(writer)),
        reply_(std::move(reply)) {}

  bool WriteBatch(const std::string &ipc_stream_payload) override {
    ::openduck::v1::IngestChunk chunk;
    auto *batch = chunk.mutable_arrow_batch();
    batch->set_ipc_stream_payload(ipc_stream_payload);
    return writer_->Write(chunk);
  }

  uint64_t Finish() override {
    writer_->WritesDone();
    // `ClientWriter::Finish()` returns the final Status; the reply
    // message was populated via the pointer passed to
    // `stub->IngestData(ctx, &reply)` at stream-open time.
    auto status = writer_->Finish();
    if (!status.ok()) {
      ThrowForStatus(status);
    }
    if (reply_->has_typed_error()) {
      ThrowMappedError(reply_->typed_error());
    }
    return reply_->rows_ingested();
  }

private:
  std::unique_ptr<grpc::ClientContext> ctx_;
  std::unique_ptr<grpc::ClientWriter<::openduck::v1::IngestChunk>> writer_;
  std::shared_ptr<::openduck::v1::IngestReply> reply_;
};

std::unique_ptr<IngestStream> GrpcClient::IngestData(
    const std::string &database, const std::string &staging_table,
    const std::vector<IngestColumnSpec> &columns,
    const std::string &transaction_id, const std::string &token) {
  auto ctx = std::make_unique<grpc::ClientContext>();
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(300);
  ctx->set_deadline(deadline);

  // Keep the reply alive for the lifetime of the stream: gRPC's
  // `ClientWriter` retains a pointer to it and fills it in `Finish()`.
  auto reply = std::make_shared<::openduck::v1::IngestReply>();
  auto writer = impl_->stub->IngestData(ctx.get(), reply.get());

  // First chunk: metadata.
  ::openduck::v1::IngestChunk first;
  auto *meta = first.mutable_metadata();
  meta->set_database(database);
  meta->set_staging_table(staging_table);
  meta->set_access_token(token);
  if (!transaction_id.empty()) {
    meta->set_transaction_id(transaction_id);
  }
  for (const auto &col : columns) {
    auto *proto_col = meta->add_columns();
    proto_col->set_name(col.name);
    proto_col->set_sql_type(col.sql_type);
  }
  if (!writer->Write(first)) {
    auto status = writer->Finish();
    if (!status.ok()) {
      ThrowForStatus(status);
    }
    throw GatewayUnavailableError(
        "OpenDuck worker closed the IngestData stream before accepting "
        "metadata");
  }
  return std::make_unique<IngestStreamImpl>(std::move(ctx), std::move(writer),
                                            std::move(reply));
}

} // namespace openduck
