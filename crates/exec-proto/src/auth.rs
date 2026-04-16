//! Shared authentication utilities used by both the gateway and worker.

use subtle::ConstantTimeEq;

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

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_slices() {
        assert!(constant_time_eq(b"hello", b"hello"));
    }

    #[test]
    fn different_slices() {
        assert!(!constant_time_eq(b"hello", b"world"));
    }

    #[test]
    fn different_lengths() {
        assert!(!constant_time_eq(b"short", b"longer"));
    }

    #[test]
    fn empty_slices() {
        assert!(constant_time_eq(b"", b""));
    }
}
