//! Caller-identity hashing shared by the worker and gateway.
//!
//! Every OpenDuck transaction is bound to the SHA-256 hash of the
//! caller's `access_token` at `BeginTransaction` time. Both the gateway
//! (defense-in-depth) and the worker (authoritative check) re-derive
//! this hash and compare it against the stored value before routing or
//! executing anything on the pinned transaction.
//!
//! Keeping this in `exec-proto` guarantees the gateway and worker use
//! bit-identical hashing — a divergence would make every
//! cross-caller isolation test meaningless.

use sha2::{Digest, Sha256};

/// A fixed-size identity fingerprint (SHA-256 of the caller's token).
pub type IdentityHash = [u8; 32];

/// Hash an access token into a constant-size identity fingerprint.
pub fn identity_of(access_token: &str) -> IdentityHash {
    let digest = Sha256::digest(access_token.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Hex-format an identity hash for structured logs.
pub fn identity_hex(id: &IdentityHash) -> String {
    let mut s = String::with_capacity(64);
    for b in id {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_and_distinct() {
        let a = identity_of("alpha");
        let b = identity_of("alpha");
        let c = identity_of("beta");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn hex_is_64_chars() {
        let id = identity_of("");
        assert_eq!(identity_hex(&id).len(), 64);
    }
}
