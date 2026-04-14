//! Layer garbage collection and extent compaction utilities.

use sqlx::PgPool;
use uuid::Uuid;

use crate::MetadataError;

/// Increment `ref_count` for a layer referenced by a new snapshot.
pub async fn increment_layer_ref(
    pool: &PgPool,
    layer_id: Uuid,
) -> Result<(), MetadataError> {
    sqlx::query(r#"UPDATE openduck_layer SET ref_count = ref_count + 1 WHERE id = $1"#)
        .bind(layer_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Decrement `ref_count` for a layer when a snapshot is dropped.
pub async fn decrement_layer_ref(
    pool: &PgPool,
    layer_id: Uuid,
) -> Result<(), MetadataError> {
    sqlx::query(r#"UPDATE openduck_layer SET ref_count = GREATEST(ref_count - 1, 0) WHERE id = $1"#)
        .bind(layer_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Return sealed layers with zero references for the given database (GC candidates).
pub async fn gc_candidates(
    pool: &PgPool,
    db_id: Uuid,
) -> Result<Vec<GcLayerRow>, MetadataError> {
    let rows = sqlx::query_as::<_, GcLayerRow>(
        r#"SELECT id, storage_uri FROM openduck_layer
           WHERE db_id = $1 AND status = 'sealed' AND ref_count = 0
           ORDER BY sealed_at"#,
    )
    .bind(db_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Delete a GC-eligible layer row and its extent metadata.
/// The caller is responsible for deleting the underlying segment file / blob.
pub async fn delete_layer(pool: &PgPool, layer_id: Uuid) -> Result<(), MetadataError> {
    let mut tx = pool.begin().await?;
    sqlx::query(r#"DELETE FROM openduck_extent WHERE layer_id = $1"#)
        .bind(layer_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(r#"DELETE FROM openduck_layer WHERE id = $1"#)
        .bind(layer_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Compact extents for a database: supersede any extents that are fully covered by a
/// later layer's extent at the same logical range. Returns the number of superseded extents.
pub async fn compact_extents(pool: &PgPool, db_id: Uuid) -> Result<u64, MetadataError> {
    let result = sqlx::query(
        r#"UPDATE openduck_extent e
           SET superseded_by = newer.id
           FROM openduck_extent newer
           JOIN openduck_layer nl ON nl.id = newer.layer_id
           JOIN openduck_layer ol ON ol.id = e.layer_id
           WHERE e.db_id = $1
             AND newer.db_id = $1
             AND e.superseded_by IS NULL
             AND newer.superseded_by IS NULL
             AND newer.id <> e.id
             AND nl.seq > ol.seq
             AND newer.logical_start <= e.logical_start
             AND newer.logical_start + newer.length >= e.logical_start + e.length"#,
    )
    .bind(db_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[derive(Debug, sqlx::FromRow)]
pub struct GcLayerRow {
    pub id: Uuid,
    pub storage_uri: String,
}
