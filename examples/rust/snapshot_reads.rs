//! # Snapshot Reads Example
//!
//! Demonstrates point-in-time reads using the differential storage engine.
//! Writes data, seals a snapshot, writes more data, then reads from both
//! the snapshot and the current tip to show they return different content.
//!
//! Requires Postgres with migrations applied:
//!
//! ```bash
//! export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
//! cargo run --example snapshot_reads
//! ```

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenDuck Snapshot Reads ===\n");

    let pg_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            println!("DATABASE_URL not set — showing conceptual walkthrough.\n");
            println!("Snapshot reads allow point-in-time access to the database:\n");
            println!("  1. Write data to the mutable tip");
            println!("  2. Seal a snapshot → get a UUID");
            println!("  3. Write MORE data to the tip (diverges from snapshot)");
            println!("  4. Open a read-only handle at the snapshot UUID");
            println!("  5. Reading from snapshot returns data as of step 2");
            println!("  6. Reading from tip returns data including step 3\n");
            println!("URI usage:");
            println!("  openduck://mydb/database.duckdb?snapshot=<uuid>\n");
            println!("Set DATABASE_URL to run the live demo.");
            return Ok(());
        }
    };

    let dir = tempfile::tempdir()?;
    let db_name = format!("snap_example_{}", uuid::Uuid::new_v4());
    let c_db = CString::new(db_name.clone())?;
    let c_pg = CString::new(pg_url.clone())?;
    let c_dir = CString::new(dir.path().to_str().unwrap())?;

    unsafe {
        // 1. Open a read-write handle
        println!("1. Opening R/W handle: db={db_name}");
        let handle =
            diff_bridge::openduck_bridge_open(c_db.as_ptr(), c_pg.as_ptr(), c_dir.as_ptr());
        if handle.is_null() {
            return Err(format!("open failed: {:?}", last_error()).into());
        }
        println!("   OK.\n");

        // 2. Write initial data: 1KB of 0xAA
        println!("2. Writing 1KB of 0xAA at offset 0...");
        let data_v1 = vec![0xAAu8; 1024];
        let rc = diff_bridge::openduck_bridge_write(handle, 0, data_v1.as_ptr(), 1024);
        assert_eq!(rc, 0);
        diff_bridge::openduck_bridge_fsync(handle);
        println!("   OK.\n");

        // 3. Seal snapshot
        println!("3. Sealing snapshot...");
        let snap_ptr = diff_bridge::openduck_bridge_seal(handle);
        if snap_ptr.is_null() {
            return Err(format!("seal failed: {:?}", last_error()).into());
        }
        let snapshot_uuid = CStr::from_ptr(snap_ptr).to_str()?.to_string();
        diff_bridge::openduck_bridge_free_string(snap_ptr);
        println!("   Snapshot UUID: {snapshot_uuid}\n");

        // 4. Write more data to tip: overwrite with 0xBB
        println!("4. Overwriting first 512 bytes with 0xBB (tip diverges from snapshot)...");
        let data_v2 = vec![0xBBu8; 512];
        let rc = diff_bridge::openduck_bridge_write(handle, 0, data_v2.as_ptr(), 512);
        assert_eq!(rc, 0);
        diff_bridge::openduck_bridge_fsync(handle);
        println!("   OK.\n");

        // 5. Read from tip → should see 0xBB in first 512, 0xAA in next 512
        println!("5. Reading from current tip...");
        let mut tip_buf = vec![0u8; 1024];
        let rc = diff_bridge::openduck_bridge_read(handle, 0, tip_buf.as_mut_ptr(), 1024);
        assert_eq!(rc, 0, "tip read failed: {:?}", last_error());
        let tip_first_512 = tip_buf[..512].iter().all(|&b| b == 0xBB);
        let tip_last_512 = tip_buf[512..].iter().all(|&b| b == 0xAA);
        println!("   First 512 bytes = 0xBB? {tip_first_512}");
        println!("   Last  512 bytes = 0xAA? {tip_last_512}\n");

        // 6. Open a read-only handle at the snapshot
        println!("6. Opening read-only handle at snapshot {snapshot_uuid}...");
        let c_snap = CString::new(snapshot_uuid.clone())?;
        let snap_handle = diff_bridge::openduck_bridge_open_snapshot(
            c_db.as_ptr(),
            c_pg.as_ptr(),
            c_dir.as_ptr(),
            c_snap.as_ptr(),
        );
        if snap_handle.is_null() {
            return Err(format!("open_snapshot failed: {:?}", last_error()).into());
        }
        println!("   OK.\n");

        // 7. Read from snapshot → should see all 0xAA (pre-overwrite)
        println!("7. Reading from snapshot...");
        let mut snap_buf = vec![0u8; 1024];
        let rc = diff_bridge::openduck_bridge_read(snap_handle, 0, snap_buf.as_mut_ptr(), 1024);
        assert_eq!(rc, 0, "snapshot read failed: {:?}", last_error());
        let snap_all_aa = snap_buf.iter().all(|&b| b == 0xAA);
        println!("   All 1024 bytes = 0xAA? {snap_all_aa}\n");

        // 8. Verify snapshot handle is read-only
        println!("8. Verifying snapshot handle is read-only...");
        let bad_data = vec![0xFFu8; 16];
        let rc = diff_bridge::openduck_bridge_write(snap_handle, 0, bad_data.as_ptr(), 16);
        if rc != 0 {
            println!("   Write correctly rejected (rc={rc}).");
            let err_msg = last_error().unwrap_or_default();
            println!("   Error: {err_msg}\n");
        } else {
            println!("   Warning: write was accepted (backend may not enforce read-only at ABI level).\n");
        }

        // Clean up
        println!("9. Closing handles...");
        diff_bridge::openduck_bridge_close(snap_handle);
        diff_bridge::openduck_bridge_close(handle);
        println!("   Done.\n");
    }

    println!("=== Summary ===");
    println!("  URI: openduck://mydb/database.duckdb?snapshot=<uuid>");
    println!("  Snapshot reads return data as of the seal point.");
    println!("  Tip reads include all subsequent writes.\n");
    println!("=== Snapshot reads example complete ===");
    Ok(())
}

unsafe fn last_error() -> Option<String> {
    let ptr: *const c_char = diff_bridge::openduck_bridge_last_error();
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}
