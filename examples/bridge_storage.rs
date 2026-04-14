//! # Bridge Storage Example
//!
//! Demonstrates the C ABI bridge from Rust into the `PgStorageBackend`,
//! the same API the C++ DuckDB extension uses for persistent differential storage.
//!
//! Requires Postgres with migrations applied:
//!
//! ```bash
//! docker compose -f docker/docker-compose.yml up -d
//! export DATABASE_URL=postgres://openduck:openduck@localhost:5433/openduck_meta
//! for f in crates/diff-metadata/migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done
//! cargo run --example bridge_storage
//! ```

use std::ffi::CString;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenDuck Bridge Storage (C ABI) ===\n");

    let pg_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            println!("DATABASE_URL not set — skipping live demo.\n");
            println!("The bridge exposes these C ABI functions to the C++ extension:\n");
            println!("  openduck_bridge_open(db_name, postgres_url, data_dir)");
            println!("    → Opens a PgStorageBackend connection.\n");
            println!("  openduck_bridge_write(handle, offset, buf, len)");
            println!("    → Appends data to the active layer at a logical offset.\n");
            println!("  openduck_bridge_read(handle, offset, buf, len)");
            println!("    → Reads bytes, resolving through the extent map (newest write wins).\n");
            println!("  openduck_bridge_fsync(handle)");
            println!("    → Flushes and syncs to durable storage.\n");
            println!("  openduck_bridge_close(handle)");
            println!("    → Frees the handle.\n");
            println!("  openduck_bridge_last_error()");
            println!("    → Returns the last error message (thread-local).\n");
            println!("Set DATABASE_URL to run the live demo.");
            return Ok(());
        }
    };

    let dir = tempfile::tempdir()?;
    let db_name = format!("bridge_example_{}", uuid::Uuid::new_v4());
    println!("1. Opening bridge: db={db_name}, data_dir={}", dir.path().display());

    let c_db = CString::new(db_name)?;
    let c_pg = CString::new(pg_url)?;
    let c_dir = CString::new(dir.path().to_str().unwrap())?;

    unsafe {
        let handle = diff_bridge::openduck_bridge_open(c_db.as_ptr(), c_pg.as_ptr(), c_dir.as_ptr());
        if handle.is_null() {
            let err = diff_bridge::openduck_bridge_last_error();
            let msg = if err.is_null() {
                "unknown".to_string()
            } else {
                std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned()
            };
            return Err(format!("bridge_open failed: {msg}").into());
        }
        println!("   Handle opened successfully.\n");

        // Write 4KB page at offset 0 (simulates a DuckDB header page)
        let page = vec![0x42u8; 4096];
        println!("2. Writing 4KB page at offset 0...");
        let rc = diff_bridge::openduck_bridge_write(handle, 0, page.as_ptr(), 4096);
        assert_eq!(rc, 0, "write failed");
        println!("   OK.\n");

        // Write another page at 4KB offset
        let page2 = vec![0x7Fu8; 4096];
        println!("3. Writing 4KB page at offset 4096...");
        let rc = diff_bridge::openduck_bridge_write(handle, 4096, page2.as_ptr(), 4096);
        assert_eq!(rc, 0, "write failed");
        println!("   OK.\n");

        // Fsync
        println!("4. Fsyncing...");
        let rc = diff_bridge::openduck_bridge_fsync(handle);
        assert_eq!(rc, 0, "fsync failed");
        println!("   OK.\n");

        // Read back and verify
        let mut buf = vec![0u8; 4096];
        println!("5. Reading back page at offset 0...");
        let rc = diff_bridge::openduck_bridge_read(handle, 0, buf.as_mut_ptr(), 4096);
        assert_eq!(rc, 0, "read failed");
        assert!(buf.iter().all(|&b| b == 0x42), "data mismatch");
        println!("   Verified: all bytes = 0x42.\n");

        println!("6. Reading back page at offset 4096...");
        let rc = diff_bridge::openduck_bridge_read(handle, 4096, buf.as_mut_ptr(), 4096);
        assert_eq!(rc, 0, "read failed");
        assert!(buf.iter().all(|&b| b == 0x7F), "data mismatch");
        println!("   Verified: all bytes = 0x7F.\n");

        // Overwrite partial range
        let patch = vec![0xFFu8; 512];
        println!("7. Overwriting 512 bytes at offset 256 (within first page)...");
        let rc = diff_bridge::openduck_bridge_write(handle, 256, patch.as_ptr(), 512);
        assert_eq!(rc, 0, "write failed");

        let mut verify = vec![0u8; 4096];
        diff_bridge::openduck_bridge_read(handle, 0, verify.as_mut_ptr(), 4096);
        assert!(verify[..256].iter().all(|&b| b == 0x42), "prefix should be 0x42");
        assert!(verify[256..768].iter().all(|&b| b == 0xFF), "patched range should be 0xFF");
        assert!(verify[768..].iter().all(|&b| b == 0x42), "suffix should be 0x42");
        println!("   Verified: overlay read resolves correctly (newest write wins).\n");

        // Clean up
        println!("8. Closing handle...");
        diff_bridge::openduck_bridge_close(handle);
        println!("   Done.\n");
    }

    println!("=== Bridge storage example complete ===");
    Ok(())
}
