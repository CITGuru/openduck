//! C ABI bridge: exposes [`PgStorageBackend`] to C/C++ callers.
//!
//! The DuckDB extension calls these functions from `OpenDuckFileSystem`
//! when a URI contains `?postgres=...&data_dir=...`.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::path::PathBuf;

use diff_core::{LogicalRange, ReadContext, StorageBackend};
use diff_metadata::PgStorageBackend;

/// Opaque handle wrapping a `PgStorageBackend`.
pub struct BridgeHandle {
    backend: PgStorageBackend,
}

/// Open a Postgres-backed storage backend.
///
/// # Safety
/// `db_name`, `postgres_url`, and `data_dir` must be valid null-terminated UTF-8 strings.
/// Returns null on failure (check `openduck_bridge_last_error`).
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_open(
    db_name: *const c_char,
    postgres_url: *const c_char,
    data_dir: *const c_char,
) -> *mut BridgeHandle {
    let db_name = match CStr::from_ptr(db_name).to_str() {
        Ok(s) => s,
        Err(_) => return set_error_null("db_name is not valid UTF-8"),
    };
    let postgres_url = match CStr::from_ptr(postgres_url).to_str() {
        Ok(s) => s,
        Err(_) => return set_error_null("postgres_url is not valid UTF-8"),
    };
    let data_dir_str = match CStr::from_ptr(data_dir).to_str() {
        Ok(s) => s,
        Err(_) => return set_error_null("data_dir is not valid UTF-8"),
    };
    let data_dir_path = PathBuf::from(data_dir_str);

    if let Err(e) = std::fs::create_dir_all(&data_dir_path) {
        return set_error_null(&format!("create data_dir: {e}"));
    }

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => return set_error_null(&format!("tokio runtime: {e}")),
    };

    let bootstrap_result = rt.block_on(async {
        let pool = sqlx::PgPool::connect(postgres_url).await?;
        diff_metadata::bootstrap_openduck_database(&pool, db_name, &data_dir_path)
            .await
    });
    if let Err(e) = bootstrap_result {
        return set_error_null(&format!("bootstrap: {e}"));
    }

    match PgStorageBackend::connect_rw(postgres_url, db_name, data_dir_path) {
        Ok(backend) => Box::into_raw(Box::new(BridgeHandle { backend })),
        Err(e) => set_error_null(&format!("connect: {e}")),
    }
}

/// Open a Postgres-backed storage backend in read-only mode at a specific snapshot.
///
/// # Safety
/// All string pointers must be valid null-terminated UTF-8.
/// Returns null on failure (check `openduck_bridge_last_error`).
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_open_snapshot(
    db_name: *const c_char,
    postgres_url: *const c_char,
    data_dir: *const c_char,
    snapshot_id: *const c_char,
) -> *mut BridgeHandle {
    let db_name = match CStr::from_ptr(db_name).to_str() {
        Ok(s) => s,
        Err(_) => return set_error_null("db_name is not valid UTF-8"),
    };
    let postgres_url = match CStr::from_ptr(postgres_url).to_str() {
        Ok(s) => s,
        Err(_) => return set_error_null("postgres_url is not valid UTF-8"),
    };
    let data_dir_str = match CStr::from_ptr(data_dir).to_str() {
        Ok(s) => s,
        Err(_) => return set_error_null("data_dir is not valid UTF-8"),
    };
    let snapshot_str = match CStr::from_ptr(snapshot_id).to_str() {
        Ok(s) => s,
        Err(_) => return set_error_null("snapshot_id is not valid UTF-8"),
    };
    let data_dir_path = PathBuf::from(data_dir_str);

    let snap_uuid = match uuid::Uuid::parse_str(snapshot_str) {
        Ok(u) => u,
        Err(e) => return set_error_null(&format!("invalid snapshot UUID: {e}")),
    };
    let snap_id = diff_core::SnapshotId(snap_uuid);

    match PgStorageBackend::connect_ro(postgres_url, db_name, data_dir_path, snap_id) {
        Ok(backend) => Box::into_raw(Box::new(BridgeHandle { backend })),
        Err(e) => set_error_null(&format!("connect_ro: {e}")),
    }
}

/// Read `len` bytes at `offset` into `buf`. Returns 0 on success, -1 on error.
///
/// # Safety
/// `handle` must be a valid pointer from `openduck_bridge_open`.
/// `buf` must point to at least `len` bytes of writable memory.
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_read(
    handle: *mut BridgeHandle,
    offset: u64,
    buf: *mut u8,
    len: u64,
) -> i32 {
    let h = &*handle;
    let range = LogicalRange {
        offset,
        len,
    };
    match h.backend.read(range, ReadContext { snapshot_id: None }) {
        Ok(bytes) => {
            let copy_len = bytes.len().min(len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, copy_len);
            for i in copy_len..(len as usize) {
                *buf.add(i) = 0;
            }
            0
        }
        Err(e) => {
            set_error(&format!("read: {e}"));
            -1
        }
    }
}

/// Write `len` bytes from `buf` at `offset`. Returns 0 on success, -1 on error.
///
/// # Safety
/// `handle` must be a valid pointer from `openduck_bridge_open`.
/// `buf` must point to at least `len` bytes of readable memory.
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_write(
    handle: *mut BridgeHandle,
    offset: u64,
    buf: *const u8,
    len: u64,
) -> i32 {
    let h = &*handle;
    let data = std::slice::from_raw_parts(buf, len as usize);
    match h.backend.write(offset, data) {
        Ok(()) => 0,
        Err(e) => {
            set_error(&format!("write: {e}"));
            -1
        }
    }
}

/// Flush and sync. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_fsync(handle: *mut BridgeHandle) -> i32 {
    let h = &*handle;
    match h.backend.fsync() {
        Ok(()) => 0,
        Err(e) => {
            set_error(&format!("fsync: {e}"));
            -1
        }
    }
}

/// Seal the active layer into a snapshot. Returns the snapshot UUID as a
/// null-terminated string (36 chars + null). The caller must free the returned
/// pointer with `openduck_bridge_free_string`. Returns null on error.
///
/// # Safety
/// `handle` must be a valid pointer from `openduck_bridge_open`.
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_seal(handle: *mut BridgeHandle) -> *mut c_char {
    let h = &*handle;
    match h.backend.seal() {
        Ok(snap_id) => {
            let s = CString::new(snap_id.0.to_string()).unwrap();
            s.into_raw()
        }
        Err(e) => {
            set_error(&format!("seal: {e}"));
            std::ptr::null_mut()
        }
    }
}

/// Free a string returned by `openduck_bridge_seal`.
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}

/// Get the logical file size (highest byte written + length).
/// The bridge tracks this internally since PgStorageBackend doesn't expose file size.
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_get_size(handle: *mut BridgeHandle) -> u64 {
    let _ = &*handle;
    // PgStorageBackend doesn't track logical file size directly.
    // DuckDB manages its own file size via Truncate. Return 0 and let
    // the C++ wrapper track logical_size (same as InMemoryStorage does).
    0
}

/// Close and free the handle.
///
/// # Safety
/// `handle` must be a valid pointer from `openduck_bridge_open`, and must not be used after.
#[no_mangle]
pub unsafe extern "C" fn openduck_bridge_close(handle: *mut BridgeHandle) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}

/// Get the last error message. Returns null if no error.
/// The returned pointer is valid until the next bridge call on the same thread.
#[no_mangle]
pub extern "C" fn openduck_bridge_last_error() -> *const c_char {
    LAST_ERROR.with(|e| {
        let guard = e.borrow();
        match &*guard {
            Some(s) => s.as_ptr(),
            None => std::ptr::null(),
        }
    })
}

// ── Thread-local error storage ──────────────────────────────────────────────

use std::cell::RefCell;
use std::ffi::CString;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_error(msg: &str) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

fn set_error_null<T>(msg: &str) -> *mut T {
    set_error(msg);
    std::ptr::null_mut()
}
