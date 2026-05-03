#include "openduck_mutate.hpp"

#include <deque>
#include <memory>
#include <mutex>

#include "openduck_catalog.hpp"
#include "openduck_errors.hpp"
#include "openduck_extension.hpp"

#include "arrow_bridge.hpp"
#include "grpc_client.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_context.hpp"

namespace openduck {

using namespace duckdb;

namespace {

class OpenDuckMutateGlobalState : public GlobalSourceState {
public:
	OpenDuckMutateGlobalState() = default;

	std::mutex lock;
	std::unique_ptr<GrpcClient> client;
	std::unique_ptr<GrpcStream> stream;
	std::deque<std::shared_ptr<arrow::RecordBatch>> pending_batches;
	bool opened = false;
	bool finished = false;
	// When the worker stream closes without emitting any Arrow batch on a
	// non-RETURNING mutation, we synthesize a `[0]` BIGINT count row so
	// downstream formatters never see an empty result (§4.1 "Empty-stream
	// defense").
	bool emitted_any = false;
};

} // namespace

PhysicalOpenDuckMutate::PhysicalOpenDuckMutate(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                               idx_t estimated_cardinality, std::string worker_sql,
                                               OpenDuckCatalog &catalog)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types),
                       estimated_cardinality),
      worker_sql_(std::move(worker_sql)), catalog_(catalog) {
}

unique_ptr<GlobalSourceState>
PhysicalOpenDuckMutate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<OpenDuckMutateGlobalState>();
}

SourceResultType PhysicalOpenDuckMutate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<OpenDuckMutateGlobalState>();
	std::lock_guard<std::mutex> guard(gstate.lock);

	if (gstate.finished) {
		chunk.SetCardinality(0);
		return SourceResultType::FINISHED;
	}

	// Open the stream lazily — catalog resolution / txn acquisition
	// only happens on first pull.
	if (!gstate.opened) {
		auto &owning_catalog = catalog_.get();
		auto &config = owning_catalog.GetConfig();
		auto transaction_id = EnsureAcquired(context.client, owning_catalog);

		gstate.client = std::make_unique<GrpcClient>(config.endpoint);
		gstate.stream =
		    gstate.client->ExecuteSQL(worker_sql_, config.database, config.token, transaction_id);
		gstate.opened = true;
	}

	while (gstate.pending_batches.empty()) {
		auto ipc_bytes = gstate.stream->Next();
		if (!ipc_bytes) {
			// Worker stream closed. If this was a non-RETURNING mutation
			// whose reply somehow arrived empty (shouldn't happen for a
			// healthy worker, but documented defensively), synthesize
			// a `[0]` count row so the formatter never sees zero rows.
			if (!gstate.emitted_any && types.size() == 1 && types[0].id() == LogicalTypeId::BIGINT) {
				chunk.SetCardinality(1);
				chunk.data[0].SetValue(0, Value::BIGINT(0));
				gstate.emitted_any = true;
				gstate.finished = true;
				return SourceResultType::HAVE_MORE_OUTPUT;
			}
			gstate.finished = true;
			chunk.SetCardinality(0);
			return SourceResultType::FINISHED;
		}
		ReadAllIpcBatches(*ipc_bytes, gstate.pending_batches);
	}

	auto batch = std::move(gstate.pending_batches.front());
	gstate.pending_batches.pop_front();

	auto rows = CopyBatchToDataChunk(batch, chunk);
	if (rows == 0) {
		gstate.finished = true;
		chunk.SetCardinality(0);
		return SourceResultType::FINISHED;
	}
	gstate.emitted_any = true;
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace openduck
