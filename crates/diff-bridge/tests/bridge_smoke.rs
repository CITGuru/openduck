//! Smoke test for the C ABI bridge functions.
//!
//! Requires `DATABASE_URL` pointing at a Postgres instance with migrations applied.
//! Skipped automatically when the env var is not set.
//!
//! ```bash
//! export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
//! cargo test -p diff-bridge -- --ignored
//! ```

use std::ffi::CString;

use diff_bridge::*;

fn skip_unless_db() -> Option<String> {
    std::env::var("DATABASE_URL").ok()
}

#[test]
#[ignore = "requires DATABASE_URL"]
fn bridge_write_read_roundtrip() {
    let pg = skip_unless_db().expect("DATABASE_URL required");
    let dir = tempfile::tempdir().expect("tempdir");
    let db_name = CString::new(format!("bridge_test_{}", uuid::Uuid::new_v4())).unwrap();
    let pg_url = CString::new(pg).unwrap();
    let data_dir = CString::new(dir.path().to_str().unwrap()).unwrap();

    unsafe {
        let handle = openduck_bridge_open(db_name.as_ptr(), pg_url.as_ptr(), data_dir.as_ptr());
        assert!(
            !handle.is_null(),
            "open failed: {:?}",
            bridge_last_error_str()
        );

        let write_data: Vec<u8> = (0..=255u8).collect();
        let rc = openduck_bridge_write(handle, 0, write_data.as_ptr(), write_data.len() as u64);
        assert_eq!(rc, 0, "write failed: {:?}", bridge_last_error_str());

        let rc = openduck_bridge_fsync(handle);
        assert_eq!(rc, 0, "fsync failed: {:?}", bridge_last_error_str());

        let mut read_buf = vec![0u8; 256];
        let rc = openduck_bridge_read(handle, 0, read_buf.as_mut_ptr(), 256);
        assert_eq!(rc, 0, "read failed: {:?}", bridge_last_error_str());
        assert_eq!(read_buf, write_data, "read-after-write mismatch");

        let err = openduck_bridge_last_error();
        assert!(err.is_null(), "expected no error after success");

        openduck_bridge_close(handle);
    }
}

#[test]
#[ignore = "requires DATABASE_URL"]
fn bridge_overwrite_and_read() {
    let pg = skip_unless_db().expect("DATABASE_URL required");
    let dir = tempfile::tempdir().expect("tempdir");
    let db_name = CString::new(format!("bridge_ow_{}", uuid::Uuid::new_v4())).unwrap();
    let pg_url = CString::new(pg).unwrap();
    let data_dir = CString::new(dir.path().to_str().unwrap()).unwrap();

    unsafe {
        let handle = openduck_bridge_open(db_name.as_ptr(), pg_url.as_ptr(), data_dir.as_ptr());
        assert!(!handle.is_null(), "open failed: {:?}", bridge_last_error_str());

        let data_a = [0xAAu8; 128];
        let rc = openduck_bridge_write(handle, 0, data_a.as_ptr(), 128);
        assert_eq!(rc, 0, "write A failed: {:?}", bridge_last_error_str());

        let data_b = [0xBBu8; 64];
        let rc = openduck_bridge_write(handle, 32, data_b.as_ptr(), 64);
        assert_eq!(rc, 0, "write B failed: {:?}", bridge_last_error_str());

        let rc = openduck_bridge_fsync(handle);
        assert_eq!(rc, 0, "fsync failed: {:?}", bridge_last_error_str());

        let mut buf = vec![0u8; 128];
        let rc = openduck_bridge_read(handle, 0, buf.as_mut_ptr(), 128);
        assert_eq!(rc, 0, "read failed: {:?}", bridge_last_error_str());

        assert!(
            buf[0..32].iter().all(|&b| b == 0xAA),
            "first 32 bytes should be 0xAA"
        );
        assert!(
            buf[32..96].iter().all(|&b| b == 0xBB),
            "overwritten range should be 0xBB"
        );
        assert!(
            buf[96..128].iter().all(|&b| b == 0xAA),
            "tail should be 0xAA"
        );

        openduck_bridge_close(handle);
    }
}

unsafe fn bridge_last_error_str() -> Option<String> {
    let ptr = openduck_bridge_last_error();
    if ptr.is_null() {
        None
    } else {
        Some(std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}
