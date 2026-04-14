//! # FUSE Concept — Storage as a File
//!
//! Demonstrates how OpenDuck presents differential storage as a single logical
//! file that DuckDB can read and write like any database file.
//!
//! This example uses the in-memory backend to simulate what the FUSE mount does
//! on Linux — no actual FUSE required.
//!
//! ```bash
//! cargo run --example fuse_concept
//! ```

use diff_core::{InMemoryBackend, LogicalRange, ReadContext, StorageBackend};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== FUSE Concept: Differential Storage as a File ===\n");

    let backend = InMemoryBackend::new();

    // ── Simulate DuckDB's file operations ───────────────────────────────────
    // DuckDB writes pages (typically 4KB) at specific offsets.
    // Through FUSE, these go into OpenDuck's active layer.

    println!("── Simulating DuckDB page writes ──");
    let page_size = 4096;

    // DuckDB writes its file header (page 0)
    let mut header = vec![0u8; page_size];
    header[..6].copy_from_slice(b"DUCKDB");
    header[6] = 1; // version
    backend.write(0, &header)?;
    println!("   Page 0: wrote file header ({page_size} bytes)");

    // DuckDB writes a data page (page 1)
    let mut page1 = vec![0u8; page_size];
    page1[..11].copy_from_slice(b"users_table");
    backend.write(page_size as u64, &page1)?;
    println!("   Page 1: wrote users table data ({page_size} bytes)");

    // DuckDB writes another data page (page 2)
    let mut page2 = vec![0u8; page_size];
    page2[..12].copy_from_slice(b"events_table");
    backend.write(2 * page_size as u64, &page2)?;
    println!("   Page 2: wrote events table data ({page_size} bytes)");

    // DuckDB issues fsync (CHECKPOINT)
    backend.flush()?;
    backend.fsync()?;
    println!("   fsync: DuckDB checkpoint complete\n");

    // ── Read pages back (what DuckDB's read() calls see) ────────────────────
    println!("── Simulating DuckDB page reads ──");

    let h = backend.read(
        LogicalRange { offset: 0, len: 6 },
        ReadContext { snapshot_id: None },
    )?;
    println!("   Page 0 header: {:?}", std::str::from_utf8(&h)?);

    let p1 = backend.read(
        LogicalRange { offset: page_size as u64, len: 11 },
        ReadContext { snapshot_id: None },
    )?;
    println!("   Page 1 data:   {:?}", std::str::from_utf8(&p1)?);

    let p2 = backend.read(
        LogicalRange { offset: 2 * page_size as u64, len: 12 },
        ReadContext { snapshot_id: None },
    )?;
    println!("   Page 2 data:   {:?}\n", std::str::from_utf8(&p2)?);

    // ── Seal snapshot (CHECKPOINT boundary) ─────────────────────────────────
    println!("── Seal: creating snapshot v1 ──");
    let snap_v1 = backend.seal()?;
    println!("   Snapshot v1: {:?}", snap_v1.0);
    println!("   Active layer sealed → immutable");
    println!("   New empty active layer created\n");

    // ── DuckDB continues writing (UPDATE/INSERT) ────────────────────────────
    println!("── DuckDB writes more data (post-snapshot) ──");
    let mut page1_updated = vec![0u8; page_size];
    page1_updated[..18].copy_from_slice(b"users_table_v2_upd");
    backend.write(page_size as u64, &page1_updated)?;
    println!("   Page 1: updated users table data");

    backend.flush()?;
    backend.fsync()?;
    let snap_v2 = backend.seal()?;
    println!("   Sealed snapshot v2: {:?}\n", snap_v2.0);

    // ── Snapshot isolation ──────────────────────────────────────────────────
    println!("── Snapshot isolation ──");

    let v1_page1 = backend.read(
        LogicalRange { offset: page_size as u64, len: 18 },
        ReadContext { snapshot_id: Some(snap_v1) },
    )?;
    let v2_page1 = backend.read(
        LogicalRange { offset: page_size as u64, len: 18 },
        ReadContext { snapshot_id: Some(snap_v2) },
    )?;

    println!("   v1 page 1: {:?}", std::str::from_utf8(&v1_page1)?);
    println!("   v2 page 1: {:?}", std::str::from_utf8(&v2_page1)?);
    println!("   → Two readers see different consistent views\n");

    // ── The full picture ────────────────────────────────────────────────────
    println!("── How this works with FUSE ──");
    println!();
    println!("   On Linux, openduck-fuse mounts a directory containing 'database.duckdb'.");
    println!("   DuckDB opens this file like any other database:");
    println!();
    println!("   $ openduck-fuse --db mydb --postgres $DATABASE_URL \\");
    println!("       --data-dir /var/openduck --mountpoint /mnt/openduck/mydb");
    println!();
    println!("   $ duckdb /mnt/openduck/mydb/database.duckdb");
    println!("   D CREATE TABLE users (id INT, name VARCHAR);");
    println!("   D INSERT INTO users VALUES (1, 'Alice');");
    println!("   D CHECKPOINT;  -- seals a snapshot");
    println!();
    println!("   Every page write goes through FUSE → StorageBackend → append-only layer.");
    println!("   Every page read resolves extents newest-first across sealed layers.");
    println!("   CHECKPOINT triggers seal → immutable snapshot → new active layer.");
    println!();
    println!("   A second process can mount the same database at a specific snapshot:");
    println!();
    println!("   $ openduck-fuse --db mydb --snapshot <uuid> \\");
    println!("       --mountpoint /mnt/openduck/mydb-readonly");
    println!("   $ duckdb /mnt/openduck/mydb-readonly/database.duckdb");
    println!("   -- reads from the frozen snapshot, never sees new writes");

    println!("\n=== Done ===");
    Ok(())
}
