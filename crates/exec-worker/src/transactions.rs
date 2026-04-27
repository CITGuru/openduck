//! Worker-side transaction connection registry.
//!
//! Implements the per-transaction "connection registry" and its
//! companion "idle reaper".
//!
//! Every `BeginTransaction` opens a dedicated DuckDB connection, runs
//! `BEGIN TRANSACTION` on it, and parks it in the registry under a
//! freshly generated `transaction_id`. Subsequent `ExecuteFragment`,
//! `IngestData`, `Commit`, and `Rollback` calls that carry that
//! `transaction_id` grab the same connection — so `CREATE TEMP TABLE`
//! / appender / savepoint semantics work as they would for a local
//! single-connection client.
//!
//! Every registry operation takes the caller's `access_token`, hashes it
//! with SHA-256, and rejects mismatched identities with a typed
//! `PERMISSION` error before any DuckDB work happens. The UUID alone is
//! never enough to control a transaction.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use duckdb::Connection;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::errors::{classify, typed};
use exec_proto::execute_fragment_error::Kind;
use exec_proto::identity::{
    identity_hex as shared_identity_hex, identity_of as shared_identity_of,
};
use exec_proto::ExecuteFragmentError;

// Re-export from the shared module so the rest of the crate imports a
// single canonical path.
pub use exec_proto::identity::IdentityHash;

/// Default per-transaction idle timeout. Entries whose `last_used` is
/// older than this are rolled back and closed by the reaper.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the idle reaper scans the registry.
pub const REAPER_INTERVAL: Duration = Duration::from_secs(15);

/// Hash an access token into a constant-size identity fingerprint.
/// Thin wrapper that re-exports the canonical implementation from
/// `exec_proto::identity` so every OpenDuck component hashes the same
/// way.
pub fn identity_of(access_token: &str) -> IdentityHash {
    shared_identity_of(access_token)
}

/// Hex-format an identity hash for structured logs.
pub fn identity_hex(id: &IdentityHash) -> String {
    shared_identity_hex(id)
}

/// A single pinned worker connection owned by an in-flight transaction.
pub struct ConnectionEntry {
    pub connection: Connection,
    pub last_used: Instant,
    pub identity: IdentityHash,
    pub database: String,
}

impl ConnectionEntry {
    pub fn touch(&mut self) {
        self.last_used = Instant::now();
    }
}

/// Thread-safe registry keyed by `transaction_id`. Each entry holds its
/// own mutex so concurrent transactions run independently; the registry
/// map is only locked for insert / lookup / remove.
#[derive(Default)]
pub struct ConnectionRegistry {
    entries: Mutex<HashMap<String, Arc<Mutex<ConnectionEntry>>>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of active transactions.
    pub fn len(&self) -> usize {
        self.entries.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Register a freshly-opened connection against a new UUID. The
    /// caller MUST have already executed `BEGIN TRANSACTION` on `conn`
    /// — registering only after a successful `BEGIN` prevents a leak
    /// where a transaction_id is handed out for a connection that
    /// never became usable.
    pub fn insert(&self, conn: Connection, identity: IdentityHash, database: String) -> String {
        let tx_id = Uuid::new_v4().to_string();
        let entry = ConnectionEntry {
            connection: conn,
            last_used: Instant::now(),
            identity,
            database,
        };
        if let Ok(mut guard) = self.entries.lock() {
            guard.insert(tx_id.clone(), Arc::new(Mutex::new(entry)));
        }
        tx_id
    }

    /// Look up and return the per-entry mutex, enforcing identity +
    /// database bindings. Returns a typed `PERMISSION` / `BINDER` /
    /// `CATALOG` error on mismatch so the caller can funnel it straight
    /// into the client reply.
    pub fn acquire(
        &self,
        transaction_id: &str,
        identity: &IdentityHash,
        database: &str,
    ) -> Result<Arc<Mutex<ConnectionEntry>>, ExecuteFragmentError> {
        let entry_arc = {
            let guard = self
                .entries
                .lock()
                .map_err(|_| typed(Kind::Internal, "transaction registry lock poisoned"))?;
            guard.get(transaction_id).cloned().ok_or_else(|| {
                typed(
                    Kind::Catalog,
                    format!(
                        "unknown transaction_id `{}` \
                             (the transaction may have been idle-reaped, \
                             rolled back, or never opened on this worker)",
                        transaction_id,
                    ),
                )
            })?
        };
        {
            let entry = entry_arc
                .lock()
                .map_err(|_| typed(Kind::Internal, "transaction entry lock poisoned"))?;
            if entry.identity != *identity {
                return Err(typed(
                    Kind::Permission,
                    format!(
                        "transaction `{}` belongs to a different caller",
                        transaction_id,
                    ),
                ));
            }
            if !database.is_empty() && entry.database != database {
                return Err(typed(
                    Kind::Binder,
                    format!(
                        "transaction `{}` is bound to database `{}`, not `{}`",
                        transaction_id, entry.database, database,
                    ),
                ));
            }
        }
        Ok(entry_arc)
    }

    /// Remove the entry if it exists. Returns the owned entry mutex so
    /// the caller can run `COMMIT` / `ROLLBACK` on the connection and
    /// drop it to close.
    pub fn remove(&self, transaction_id: &str) -> Option<Arc<Mutex<ConnectionEntry>>> {
        self.entries
            .lock()
            .ok()
            .and_then(|mut g| g.remove(transaction_id))
    }

    /// Commit + remove the entry atomically. Identity and database are
    /// re-checked before running `COMMIT`.
    pub fn commit(
        &self,
        transaction_id: &str,
        identity: &IdentityHash,
    ) -> Result<(), ExecuteFragmentError> {
        let entry_arc = self.acquire_for_termination(transaction_id, identity)?;
        let result = {
            let mut entry = entry_arc
                .lock()
                .map_err(|_| typed(Kind::Internal, "transaction entry lock poisoned"))?;
            entry.touch();
            entry.connection.execute_batch("COMMIT").map_err(|e| {
                let msg = e.to_string();
                typed(classify(&msg), msg)
            })
        };
        // Whether the COMMIT succeeded or failed, drop the entry —
        // the pinned connection is no longer useful for further work.
        self.remove(transaction_id);
        result
    }

    /// Rollback + remove. Identity re-checked. Rollback failures are
    /// logged but the entry is always dropped.
    pub fn rollback(
        &self,
        transaction_id: &str,
        identity: &IdentityHash,
    ) -> Result<(), ExecuteFragmentError> {
        let entry_arc = self.acquire_for_termination(transaction_id, identity)?;
        let result = {
            let mut entry = entry_arc
                .lock()
                .map_err(|_| typed(Kind::Internal, "transaction entry lock poisoned"))?;
            entry.touch();
            entry.connection.execute_batch("ROLLBACK").map_err(|e| {
                let msg = e.to_string();
                warn!(
                    transaction_id = %transaction_id,
                    error = %msg,
                    "ROLLBACK on pinned connection failed; dropping entry regardless",
                );
                typed(classify(&msg), msg)
            })
        };
        self.remove(transaction_id);
        result
    }

    /// Like [`acquire`] but skips the `database` check — useful for
    /// commit / rollback / reaper paths where we don't require the
    /// caller to echo the bound database back.
    fn acquire_for_termination(
        &self,
        transaction_id: &str,
        identity: &IdentityHash,
    ) -> Result<Arc<Mutex<ConnectionEntry>>, ExecuteFragmentError> {
        let entry_arc = {
            let guard = self
                .entries
                .lock()
                .map_err(|_| typed(Kind::Internal, "transaction registry lock poisoned"))?;
            guard.get(transaction_id).cloned().ok_or_else(|| {
                typed(
                    Kind::Catalog,
                    format!(
                        "unknown transaction_id `{}` \
                             (the transaction may have been idle-reaped)",
                        transaction_id,
                    ),
                )
            })?
        };
        {
            let entry = entry_arc
                .lock()
                .map_err(|_| typed(Kind::Internal, "transaction entry lock poisoned"))?;
            if entry.identity != *identity {
                return Err(typed(
                    Kind::Permission,
                    format!(
                        "transaction `{}` belongs to a different caller",
                        transaction_id,
                    ),
                ));
            }
        }
        Ok(entry_arc)
    }

    /// Sweep idle entries: ROLLBACK + drop + remove any entry with
    /// `now - last_used > idle_timeout`.
    pub fn reap_idle(&self, idle_timeout: Duration) -> usize {
        let cutoff = Instant::now() - idle_timeout;
        let stale_ids: Vec<String> = {
            let guard = match self.entries.lock() {
                Ok(g) => g,
                Err(_) => return 0,
            };
            guard
                .iter()
                .filter_map(|(id, entry)| {
                    let last = entry.lock().ok().map(|e| e.last_used)?;
                    if last < cutoff {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut reaped = 0usize;
        for tx_id in stale_ids {
            if let Some(entry_arc) = self.remove(&tx_id) {
                let (ident_hex, last_ms, rollback_err) = {
                    let entry = match entry_arc.lock() {
                        Ok(e) => e,
                        Err(_) => continue,
                    };
                    let ident = identity_hex(&entry.identity);
                    let last_ms = Instant::now()
                        .saturating_duration_since(entry.last_used)
                        .as_millis();
                    let rb = entry.connection.execute_batch("ROLLBACK").err();
                    (ident, last_ms, rb)
                };
                match rollback_err {
                    Some(err) => warn!(
                        transaction_id = %tx_id,
                        identity_hash = %ident_hex,
                        idle_ms = last_ms,
                        error = %err,
                        "idle-reap ROLLBACK failed; entry dropped",
                    ),
                    None => info!(
                        transaction_id = %tx_id,
                        identity_hash = %ident_hex,
                        idle_ms = last_ms,
                        "reaped idle transaction",
                    ),
                }
                reaped += 1;
            }
        }
        reaped
    }
}

/// Spawn the idle reaper as a tokio background task. It runs until
/// `shutdown` fires (if provided) or forever.
pub fn spawn_reaper(
    registry: Arc<ConnectionRegistry>,
    idle_timeout: Duration,
    mut shutdown: Option<tokio::sync::watch::Receiver<()>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(REAPER_INTERVAL) => {
                    let reg = registry.clone();
                    let reaped = tokio::task::spawn_blocking(move || {
                        reg.reap_idle(idle_timeout)
                    })
                    .await
                    .unwrap_or(0);
                    if reaped > 0 {
                        debug!(reaped, remaining = registry.len(), "idle reaper swept");
                    }
                }
                _ = async {
                    if let Some(rx) = shutdown.as_mut() {
                        let _ = rx.changed().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => break,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    fn mk_conn_with_begin() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("BEGIN TRANSACTION").unwrap();
        c
    }

    #[test]
    fn identity_of_stable_and_distinct() {
        let a = identity_of("alpha");
        let b = identity_of("alpha");
        let c = identity_of("beta");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(identity_hex(&a).len(), 64);
    }

    #[test]
    fn insert_acquire_commit_roundtrip() {
        let reg = ConnectionRegistry::new();
        let conn = mk_conn_with_begin();
        let ident = identity_of("token-a");
        let tx = reg.insert(conn, ident, "mydb".into());
        assert_eq!(reg.len(), 1);

        let acquired = reg.acquire(&tx, &ident, "mydb").unwrap();
        drop(acquired);

        reg.commit(&tx, &ident).unwrap();
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn identity_mismatch_rejected() {
        let reg = ConnectionRegistry::new();
        let conn = mk_conn_with_begin();
        let ident_a = identity_of("alpha");
        let ident_b = identity_of("beta");
        let tx = reg.insert(conn, ident_a, "mydb".into());

        let err = reg
            .acquire(&tx, &ident_b, "mydb")
            .err()
            .expect("must reject");
        assert_eq!(err.kind, Kind::Permission as i32);

        // Cleanup so reg drops cleanly.
        reg.rollback(&tx, &ident_a).ok();
    }

    #[test]
    fn unknown_tx_id_is_catalog_error() {
        let reg = ConnectionRegistry::new();
        let ident = identity_of("alpha");
        let err = reg
            .acquire("does-not-exist", &ident, "mydb")
            .err()
            .expect("must reject");
        assert_eq!(err.kind, Kind::Catalog as i32);
    }

    #[test]
    fn database_mismatch_is_binder_error() {
        let reg = ConnectionRegistry::new();
        let conn = mk_conn_with_begin();
        let ident = identity_of("alpha");
        let tx = reg.insert(conn, ident, "mydb".into());

        let err = reg
            .acquire(&tx, &ident, "other")
            .err()
            .expect("must reject");
        assert_eq!(err.kind, Kind::Binder as i32);

        reg.rollback(&tx, &ident).ok();
    }

    #[test]
    fn reaper_sweeps_idle_entries() {
        let reg = ConnectionRegistry::new();
        let ident = identity_of("alpha");
        let tx = reg.insert(mk_conn_with_begin(), ident, "mydb".into());
        // Force the entry's last_used backward by touching it.
        {
            let guard = reg.entries.lock().unwrap();
            let entry = guard.get(&tx).cloned().unwrap();
            drop(guard);
            let mut e = entry.lock().unwrap();
            e.last_used = Instant::now() - Duration::from_secs(3600);
        }
        let reaped = reg.reap_idle(Duration::from_secs(60));
        assert_eq!(reaped, 1);
        assert_eq!(reg.len(), 0);
    }
}
