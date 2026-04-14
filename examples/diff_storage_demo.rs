//! # Differential Storage Demo
//!
//! Demonstrates the core concept behind OpenDuck's storage: append-only layers,
//! extent maps, and snapshot isolation — all using the in-memory backend.
//!
//! This is the same algorithm that runs on Postgres + object storage in production.
//!
//! ```bash
//! cargo run --example diff_storage_demo
//! ```

use diff_core::{InMemoryBackend, LogicalRange, ReadContext, StorageBackend};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenDuck Differential Storage Demo ===\n");

    let backend = InMemoryBackend::new();

    // ── 1. Write and read ───────────────────────────────────────────────────
    println!("── 1. Write and read ──");
    backend.write(0, b"Hello, OpenDuck!")?;
    let data = backend.read(
        LogicalRange { offset: 0, len: 16 },
        ReadContext { snapshot_id: None },
    )?;
    println!("   Wrote 16 bytes at offset 0");
    println!("   Read back: {:?}\n", std::str::from_utf8(&data)?);

    // ── 2. Overlapping writes — last write wins ─────────────────────────────
    println!("── 2. Overlapping writes ──");
    backend.write(7, b"World!!!")?;
    let data = backend.read(
        LogicalRange { offset: 0, len: 16 },
        ReadContext { snapshot_id: None },
    )?;
    println!("   Overwrote bytes 7..15 with 'World!!!'");
    println!("   Read back: {:?}\n", std::str::from_utf8(&data)?);

    // ── 3. Seal a snapshot ──────────────────────────────────────────────────
    println!("── 3. Seal a snapshot ──");
    let snap_v1 = backend.seal()?;
    println!("   Sealed snapshot v1: {:?}", snap_v1.0);
    println!("   Active layer is now empty — new writes go to a fresh layer\n");

    // ── 4. Write new data after seal ────────────────────────────────────────
    println!("── 4. Write after seal ──");
    backend.write(0, b"Version 2 data!!")?;

    let tip = backend.read(
        LogicalRange { offset: 0, len: 16 },
        ReadContext { snapshot_id: None },
    )?;
    let frozen = backend.read(
        LogicalRange { offset: 0, len: 16 },
        ReadContext { snapshot_id: Some(snap_v1) },
    )?;

    println!("   Tip (latest):     {:?}", std::str::from_utf8(&tip)?);
    println!("   Snapshot v1:      {:?}", std::str::from_utf8(&frozen)?);
    println!("   → Snapshot is frozen; tip reflects the new write\n");

    // ── 5. Multiple snapshots ───────────────────────────────────────────────
    println!("── 5. Multiple snapshots ──");
    let snap_v2 = backend.seal()?;
    println!("   Sealed snapshot v2: {:?}", snap_v2.0);

    backend.write(0, b"Version 3!!!!!!!")?;
    let snap_v3 = backend.seal()?;
    println!("   Sealed snapshot v3: {:?}", snap_v3.0);

    let v1 = backend.read(
        LogicalRange { offset: 0, len: 16 },
        ReadContext { snapshot_id: Some(snap_v1) },
    )?;
    let v2 = backend.read(
        LogicalRange { offset: 0, len: 16 },
        ReadContext { snapshot_id: Some(snap_v2) },
    )?;
    let v3 = backend.read(
        LogicalRange { offset: 0, len: 16 },
        ReadContext { snapshot_id: Some(snap_v3) },
    )?;

    println!("   v1: {:?}", std::str::from_utf8(&v1)?);
    println!("   v2: {:?}", std::str::from_utf8(&v2)?);
    println!("   v3: {:?}", std::str::from_utf8(&v3)?);
    println!("   → Each snapshot sees its own consistent view\n");

    // ── 6. Sparse writes (holes read as zeroes) ─────────────────────────────
    println!("── 6. Sparse writes ──");
    let sparse = InMemoryBackend::new();
    sparse.write(100, b"data at offset 100")?;

    let hole = sparse.read(
        LogicalRange { offset: 0, len: 4 },
        ReadContext { snapshot_id: None },
    )?;
    let written = sparse.read(
        LogicalRange { offset: 100, len: 18 },
        ReadContext { snapshot_id: None },
    )?;

    println!("   Wrote 'data at offset 100' at offset 100");
    println!("   Hole (offset 0..4): {:?}", &hole[..]);
    println!("   Data (offset 100..118): {:?}\n", std::str::from_utf8(&written)?);

    // ── 7. Large sequential writes ──────────────────────────────────────────
    println!("── 7. Large sequential writes ──");
    let large = InMemoryBackend::new();
    let block = vec![0xABu8; 4096];
    for i in 0..10 {
        large.write(i * 4096, &block)?;
    }
    large.flush()?;
    large.fsync()?;

    let snap = large.seal()?;
    let sample = large.read(
        LogicalRange { offset: 8192, len: 4 },
        ReadContext { snapshot_id: Some(snap) },
    )?;
    println!("   Wrote 10 x 4KB blocks (40KB total)");
    println!("   Sealed and read back 4 bytes at offset 8192: {:?}", &sample[..]);
    println!("   → This is the pattern DuckDB uses when writing pages through FUSE\n");

    println!("=== Done ===");
    Ok(())
}
