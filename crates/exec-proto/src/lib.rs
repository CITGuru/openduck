//! Generated gRPC types for OpenDuck v1 execution.

pub mod auth;

pub mod openduck {
    pub mod v1 {
        tonic::include_proto!("openduck.v1");
    }
}

pub use openduck::v1::execute_fragment_chunk;
pub use openduck::v1::execution_service_client::ExecutionServiceClient;
pub use openduck::v1::execution_service_server::{ExecutionService, ExecutionServiceServer};
pub use openduck::v1::{
    ArrowIpcBatch, CancelReply, CancelRequest, ExecuteFragmentChunk, ExecuteFragmentRequest,
    HeartbeatReply, HeartbeatRequest, RegisterWorkerReply, WorkerRegistration,
};
