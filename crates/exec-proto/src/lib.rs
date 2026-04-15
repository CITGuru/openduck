//! Generated gRPC types for OpenDuck v1 execution.

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

/// Shared authentication utilities used by both the gateway and worker.

/// Validate `request_token` against the expected token from `OPENDUCK_TOKEN`.
///
/// - If `OPENDUCK_TOKEN` is unset or empty: dev mode — any token (including empty) is accepted.
/// - If `OPENDUCK_TOKEN` is set: `request_token` must match (constant-time comparison).
#[allow(clippy::result_large_err)]
pub fn validate_token(request_token: &str) -> Result<(), tonic::Status> {
    let expected = std::env::var("OPENDUCK_TOKEN").unwrap_or_default();
    if expected.is_empty() {
        return Ok(());
    }
    if request_token.is_empty() {
        return Err(tonic::Status::unauthenticated(
            "access_token required when OPENDUCK_TOKEN is set",
        ));
    }
    if constant_time_eq(request_token.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(tonic::Status::unauthenticated("invalid access_token"))
    }
}

/// Constant-time byte comparison to prevent timing side-channel attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    // Use a volatile read to prevent the compiler from short-circuiting.
    unsafe { std::ptr::read_volatile(&diff) == 0 }
}
