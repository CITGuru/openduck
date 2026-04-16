//! # Garbage Collection Walkthrough
//!
//! Demonstrates the GC lifecycle for differential storage layers:
//! - Writing data and sealing snapshots
//! - Layer reference counting
//! - Identifying GC candidates (ref_count = 0)
//! - Extent compaction (superseded extents)
//! - The `openduck gc` CLI subcommand
//!
//! This example runs entirely in-memory using `InMemoryBackend`.
//!
//! ```bash
//! cargo run --example gc_walkthrough
//! ```

use diff_core::{InMemoryBackend, LogicalRange, ReadContext, StorageBackend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OpenDuck Garbage Collection Walkthrough ===\n");

    // ── 1. Differential storage layers ──────────────────────────────────
    println!("── 1. How differential storage layers work ──\n");
    println!("   OpenDuck's differential storage writes data into append-only layers.");
    println!("   When you seal a snapshot, the active layer becomes immutable and a");
    println!("   new empty layer is created for subsequent writes.\n");

    let backend = InMemoryBackend::new();

    let data_v1 = b"initial data for v1";
    backend.write(0, data_v1)?;
    backend.fsync()?;
    let snap_v1 = backend.seal()?;
    println!(
        "   Wrote {} bytes → sealed snapshot v1: {}",
        data_v1.len(),
        snap_v1.0
    );

    let data_v2 = b"updated data for v2";
    backend.write(0, data_v2)?;
    backend.fsync()?;
    let snap_v2 = backend.seal()?;
    println!(
        "   Wrote {} bytes → sealed snapshot v2: {}",
        data_v2.len(),
        snap_v2.0
    );

    let data_v3 = b"final data for v3!!";
    backend.write(0, data_v3)?;
    backend.fsync()?;
    let snap_v3 = backend.seal()?;
    println!(
        "   Wrote {} bytes → sealed snapshot v3: {}",
        data_v3.len(),
        snap_v3.0
    );

    // Verify snapshot isolation
    let v1_data = backend.read(
        LogicalRange { offset: 0, len: 19 },
        ReadContext {
            snapshot_id: Some(snap_v1),
        },
    )?;
    let v3_data = backend.read(
        LogicalRange { offset: 0, len: 19 },
        ReadContext {
            snapshot_id: Some(snap_v3),
        },
    )?;
    println!("\n   v1 reads: \"{}\"", String::from_utf8_lossy(&v1_data));
    println!("   v3 reads: \"{}\"", String::from_utf8_lossy(&v3_data));
    println!("   → Snapshots are isolated: same offset, different data.");

    // ── 2. Layer reference counting ─────────────────────────────────────
    println!("\n── 2. Layer reference counting ──\n");
    println!("   Each snapshot references the layers it depends on.");
    println!("   When a snapshot is dropped, its layers' ref_count decrements.");
    println!("   Layers with ref_count=0 are GC candidates.\n");

    println!("   Snapshot    Layers Referenced");
    println!("   ─────────   ────────────────────");
    println!("   v1          [layer_0]                  ← ref_count: 1");
    println!("   v2          [layer_0, layer_1]         ← layer_0 ref_count: 2");
    println!("   v3          [layer_0, layer_1, layer_2]");
    println!();
    println!("   DROP SNAPSHOT v1 → layer_0 ref_count: 2→1");
    println!("   DROP SNAPSHOT v2 → layer_0 ref_count: 1→0, layer_1 ref_count: 1→0");
    println!("   → layer_0 and layer_1 are now GC candidates");

    // ── 3. Extent compaction ────────────────────────────────────────────
    println!("\n── 3. Extent compaction ──\n");
    println!("   Extents track which logical byte ranges each layer covers.");
    println!("   When a newer layer writes the same range as an older layer,");
    println!("   the older extent is superseded.\n");

    println!("   Layer 0: extent [0..19] = 'initial data for v1'");
    println!("   Layer 1: extent [0..19] = 'updated data for v2'  ← supersedes layer 0");
    println!("   Layer 2: extent [0..19] = 'final data for v3!!'  ← supersedes layer 1\n");

    println!("   compact_extents() marks superseded extents so they're never read.");
    println!("   Reads always resolve newest-first across sealed layers.");

    // ── 4. GC candidates ────────────────────────────────────────────────
    println!("\n── 4. GC candidates ──\n");
    println!("   gc_candidates(pool, db_id) returns layers where:");
    println!("     status = 'sealed' AND ref_count = 0\n");

    println!("   For each candidate, the GC process:");
    println!("   1. Deletes the segment file (blob storage)");
    println!("   2. Deletes extent metadata rows");
    println!("   3. Deletes the layer metadata row");

    // ── 5. The openduck gc CLI ──────────────────────────────────────────
    println!("\n── 5. Using the openduck gc CLI ──\n");

    println!("   # Dry-run: see what would be deleted");
    println!("   openduck gc --postgres $DATABASE_URL --db mydb \\");
    println!("     --data-dir /var/openduck --dry-run\n");

    println!("   Output:");
    println!("   2 GC candidate layer(s) for database 'mydb':");
    println!("     a1b2c3d4-...  layers/mydb/layer_0.segment");
    println!("     e5f6g7h8-...  layers/mydb/layer_1.segment");
    println!("   (dry-run — no layers deleted)\n");

    println!("   # Actual GC: compact extents + delete unreferenced layers");
    println!("   openduck gc --postgres $DATABASE_URL --db mydb \\");
    println!("     --data-dir /var/openduck\n");

    println!("   Output:");
    println!("   2 superseded extents marked");
    println!("   Deleted 2/2 layer(s)");

    // ── 6. Safety and scheduling ────────────────────────────────────────
    println!("\n── 6. Safety guarantees ──\n");
    println!("   - Never deletes active layers (only sealed with ref_count=0)");
    println!("   - Transactional: extent + layer metadata deleted atomically");
    println!("   - Segment file deleted before metadata (if metadata delete fails,");
    println!("     the layer row remains as a tombstone — no data loss)");
    println!("   - Idempotent: running GC twice is safe");

    println!("\n── 7. Scheduled GC ──\n");
    println!("   For production, run GC periodically:");
    println!("   */15 * * * * openduck gc --postgres $DATABASE_URL \\");
    println!("     --db mydb --data-dir /var/openduck");

    println!("\n=== Done ===");
    Ok(())
}
