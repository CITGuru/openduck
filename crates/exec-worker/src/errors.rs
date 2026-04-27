//! Typed error classification for responses that funnel through
//! `ExecuteFragmentChunk` / `Begin|Commit|Rollback|IngestReply` messages.
//!
//! DuckDB's C++ layer prefixes every thrown error with a class tag:
//! `"Catalog Error: ..."`, `"Binder Error: ..."`, `"Parser Error: ..."`,
//! `"Constraint Error: ..."`, `"Conversion Error: ..."`, `"IO Error: ..."`,
//! `"Permission Error: ..."`, `"Serialization Error: ..."`,
//! `"TransactionContext Error: ..."`, `"Out of Memory Error: ..."`,
//! `"Fatal Error: ..."`, `"Internal Error: ..."`, etc. We pattern-match
//! on those prefixes to classify into the protobuf `Kind`.
//!
//! The `duckdb-rs` crate's `ErrorCode` enum collapses almost every DuckDB
//! failure into `ErrorCode::Unknown` with the real class embedded in the
//! prose message, so the string-prefix classifier is both the most
//! reliable and the only forward-compatible option.

use exec_proto::execute_fragment_error::Kind;
use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::{ExecuteFragmentChunk, ExecuteFragmentError};

/// Classify a DuckDB / worker error message into a typed `Kind`.
///
/// Returns the same `message` back unchanged (the caller keeps the
/// original human-readable text for the legacy `string error` field).
pub fn classify(message: &str) -> Kind {
    let trimmed = message.trim_start();
    // Match against the class prefixes DuckDB emits on error.
    // Keep this ordering: more specific prefixes first.
    let kinds: &[(&str, Kind)] = &[
        ("Catalog Error", Kind::Catalog),
        ("Binder Error", Kind::Binder),
        ("Parser Error", Kind::Parser),
        ("Constraint Error", Kind::Constraint),
        ("Conversion Error", Kind::Conversion),
        ("Invalid Input Error", Kind::Binder),
        ("Invalid Type Error", Kind::Conversion),
        ("Out of Range Error", Kind::Conversion),
        ("IO Error", Kind::Io),
        ("HTTP Error", Kind::Io),
        ("Permission Error", Kind::Permission),
        ("Dependency Error", Kind::Constraint),
        ("TransactionContext Error", Kind::Catalog),
        ("Transaction Error", Kind::Catalog),
        ("Not Implemented Error", Kind::Internal),
        ("Out of Memory Error", Kind::Internal),
        ("Fatal Error", Kind::Internal),
        ("Internal Error", Kind::Internal),
        ("Serialization Error", Kind::Internal),
    ];
    for (prefix, kind) in kinds {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            // Must be followed by `:` / end-of-string / whitespace to
            // avoid false positives like "Catalog Errors are fun".
            if rest.is_empty() || rest.starts_with(':') || rest.starts_with(char::is_whitespace) {
                return *kind;
            }
        }
    }
    Kind::Unknown
}

/// Build an `ExecuteFragmentError` with the given kind + message.
pub fn typed(kind: Kind, message: impl Into<String>) -> ExecuteFragmentError {
    ExecuteFragmentError {
        kind: kind as i32,
        message: message.into(),
        sql_state: None,
    }
}

/// Classify a worker/DuckDB error string and return a typed error.
pub fn typed_from_message(message: impl Into<String>) -> ExecuteFragmentError {
    let msg = message.into();
    let kind = classify(&msg);
    typed(kind, msg)
}

/// Build an `ExecuteFragmentChunk` carrying both the legacy `string error`
/// field (for pre-typed-error clients) and the new `typed_error` field
/// — populated in lockstep to preserve the proto-additivity invariant.
pub fn error_chunk(err: ExecuteFragmentError) -> ExecuteFragmentChunk {
    ExecuteFragmentChunk {
        payload: Some(Payload::Error(err.message.clone())),
        typed_error: Some(err),
    }
}

/// Convenience: classify `message`, then build an `ExecuteFragmentChunk`
/// with both the legacy and typed fields populated.
pub fn error_chunk_from_message(message: impl Into<String>) -> ExecuteFragmentChunk {
    error_chunk(typed_from_message(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_duckdb_prefixes() {
        assert_eq!(
            classify("Catalog Error: table users does not exist"),
            Kind::Catalog
        );
        assert_eq!(classify("Binder Error: column id not found"), Kind::Binder);
        assert_eq!(
            classify("Parser Error: syntax error at or near"),
            Kind::Parser
        );
        assert_eq!(
            classify("Constraint Error: UNIQUE constraint failed"),
            Kind::Constraint,
        );
        assert_eq!(classify("Conversion Error: cast failure"), Kind::Conversion,);
        assert_eq!(classify("IO Error: disk full"), Kind::Io);
        assert_eq!(classify("Permission Error: denied"), Kind::Permission);
        assert_eq!(classify("Internal Error: bug"), Kind::Internal);
    }

    #[test]
    fn unknown_without_prefix() {
        assert_eq!(classify("random worker failure"), Kind::Unknown);
        assert_eq!(classify(""), Kind::Unknown);
    }

    #[test]
    fn no_false_prefix_match() {
        // Should not match "Catalog Error" because the whole word doesn't end there.
        assert_eq!(classify("Catalog Errors are fun"), Kind::Unknown);
    }
}
