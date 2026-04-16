//! PostgreSQL + local segment files implementing [`diff_core::StorageBackend`].

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use diff_core::{DiffCoreError, LogicalRange, ReadContext, SnapshotId, StorageBackend};
use diff_layer_fs::{read_segment_bytes, AppendSegment};
use sqlx::PgPool;
use tokio::runtime::Runtime;
use uuid::Uuid;

use crate::MetadataError;

struct ActiveWriter {
    layer_id: Uuid,
    segment: AppendSegment,
}

struct PreparedRw {
    db_id: Uuid,
    tip: Option<Uuid>,
    writer: ActiveWriter,
}

struct PreparedRo {
    db_id: Uuid,
    tip: Option<Uuid>,
}

/// Optional object store for uploading sealed layers after seal (tiering).
pub type BlobStore = Arc<dyn object_store::ObjectStore>;

/// Differential file backend backed by Postgres metadata and segment files under `data_dir`.
pub struct PgStorageBackend {
    rt: Runtime,
    pool: PgPool,
    db_id: Uuid,
    data_dir: PathBuf,
    active: Mutex<Option<ActiveWriter>>,
    read_only_snapshot: Option<SnapshotId>,
    tip_snapshot: RwLock<Option<Uuid>>,
    blob_store: Option<BlobStore>,
}

#[derive(sqlx::FromRow)]
struct DbRow {
    id: Uuid,
    current_tip_snapshot_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct LayerRow {
    id: Uuid,
    storage_uri: String,
    #[allow(dead_code)]
    byte_len: i64,
}

#[derive(sqlx::FromRow)]
struct LayerIdPath {
    id: Uuid,
    storage_uri: String,
}

#[derive(sqlx::FromRow)]
struct ExtentRow {
    phys_start: i64,
    logical_start: i64,
    #[allow(dead_code)]
    length: i64,
}

fn resolve_path(data_dir: &Path, storage_uri: &str) -> Result<PathBuf, crate::MetadataError> {
    let p = Path::new(storage_uri);
    if p.is_absolute() {
        return Err(crate::MetadataError::Runtime(format!(
            "storage_uri must be relative, got absolute path: {storage_uri}"
        )));
    }
    if p.components().any(|c| c == std::path::Component::ParentDir) {
        return Err(crate::MetadataError::Runtime(format!(
            "storage_uri must not contain '..': {storage_uri}"
        )));
    }
    Ok(data_dir.join(storage_uri))
}

impl PgStorageBackend {
    pub fn connect_rw(
        database_url: &str,
        database_name: &str,
        data_dir: PathBuf,
    ) -> Result<Self, MetadataError> {
        let rt = Runtime::new().map_err(|e| MetadataError::Runtime(e.to_string()))?;
        let pool = rt.block_on(PgPool::connect(database_url))?;
        let prepared = rt.block_on(Self::prepare_rw(&pool, database_name, &data_dir))?;
        Ok(Self {
            rt,
            pool,
            db_id: prepared.db_id,
            data_dir,
            active: Mutex::new(Some(prepared.writer)),
            read_only_snapshot: None,
            tip_snapshot: RwLock::new(prepared.tip),
            blob_store: None,
        })
    }

    pub fn connect_ro(
        database_url: &str,
        database_name: &str,
        data_dir: PathBuf,
        snapshot_id: SnapshotId,
    ) -> Result<Self, MetadataError> {
        let rt = Runtime::new().map_err(|e| MetadataError::Runtime(e.to_string()))?;
        let pool = rt.block_on(PgPool::connect(database_url))?;
        let prepared = rt.block_on(Self::prepare_ro(&pool, database_name, snapshot_id))?;
        Ok(Self {
            rt,
            pool,
            db_id: prepared.db_id,
            data_dir,
            active: Mutex::new(None),
            read_only_snapshot: Some(snapshot_id),
            tip_snapshot: RwLock::new(prepared.tip),
            blob_store: None,
        })
    }

    /// Attach an object store for sealed layer upload after each seal (tiering).
    pub fn set_blob_store(&mut self, store: BlobStore) {
        self.blob_store = Some(store);
    }

    async fn prepare_rw(
        pool: &PgPool,
        database_name: &str,
        data_dir: &Path,
    ) -> Result<PreparedRw, MetadataError> {
        let row = sqlx::query_as::<_, DbRow>(
            r#"SELECT id, current_tip_snapshot_id FROM openduck_db WHERE name = $1"#,
        )
        .bind(database_name)
        .fetch_optional(pool)
        .await?;

        let (db_id, tip) = match row {
            Some(r) => (r.id, r.current_tip_snapshot_id),
            None => return Err(MetadataError::NotFound),
        };

        let active = sqlx::query_as::<_, LayerRow>(
            r#"SELECT id, storage_uri, byte_len FROM openduck_layer
               WHERE db_id = $1 AND status = 'active'"#,
        )
        .bind(db_id)
        .fetch_one(pool)
        .await?;

        let path = resolve_path(data_dir, &active.storage_uri)?;
        let segment = AppendSegment::open_existing(&path)
            .map_err(|e| MetadataError::LayerFs(e.to_string()))?;

        Ok(PreparedRw {
            db_id,
            tip,
            writer: ActiveWriter {
                layer_id: active.id,
                segment,
            },
        })
    }

    async fn prepare_ro(
        pool: &PgPool,
        database_name: &str,
        snapshot_id: SnapshotId,
    ) -> Result<PreparedRo, MetadataError> {
        let row = sqlx::query_as::<_, DbRow>(
            r#"SELECT id, current_tip_snapshot_id FROM openduck_db WHERE name = $1"#,
        )
        .bind(database_name)
        .fetch_optional(pool)
        .await?;

        let (db_id, tip) = match row {
            Some(r) => (r.id, r.current_tip_snapshot_id),
            None => return Err(MetadataError::NotFound),
        };

        let exists = sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS(SELECT 1 FROM openduck_snapshot WHERE id = $1 AND db_id = $2)"#,
        )
        .bind(snapshot_id.0)
        .bind(db_id)
        .fetch_one(pool)
        .await?;
        if !exists {
            return Err(MetadataError::NotFound);
        }

        Ok(PreparedRo { db_id, tip })
    }

    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future,
    {
        self.rt.block_on(fut)
    }

    fn meta_err(e: impl Into<MetadataError>) -> DiffCoreError {
        DiffCoreError::Storage(e.into().to_string())
    }

    fn read_byte_sync(&self, offset: u64, ctx: &ReadContext) -> Result<u8, DiffCoreError> {
        self.block_on(async { self.read_byte_async(offset, ctx).await })
    }

    async fn read_byte_async(&self, offset: u64, ctx: &ReadContext) -> Result<u8, DiffCoreError> {
        let layers = self
            .layer_stack_for_read(ctx)
            .await
            .map_err(Self::meta_err)?;
        for (layer_id, path) in layers {
            let b = self
                .byte_from_layer(layer_id, &path, offset)
                .await
                .map_err(Self::meta_err)?;
            if let Some(bb) = b {
                return Ok(bb);
            }
        }
        Ok(0)
    }

    async fn layer_stack_for_read(
        &self,
        ctx: &ReadContext,
    ) -> Result<Vec<(Uuid, PathBuf)>, MetadataError> {
        if let Some(SnapshotId(sid)) = ctx.snapshot_id {
            let rows = sqlx::query_as::<_, LayerIdPath>(
                r#"SELECT l.id, l.storage_uri
                   FROM openduck_snapshot_layer sl
                   JOIN openduck_layer l ON l.id = sl.layer_id
                   WHERE sl.snapshot_id = $1
                   ORDER BY sl.ordinal DESC"#,
            )
            .bind(sid)
            .fetch_all(&self.pool)
            .await?;
            let mut result = Vec::with_capacity(rows.len());
            for r in rows {
                result.push((r.id, resolve_path(&self.data_dir, &r.storage_uri)?));
            }
            return Ok(result);
        }

        if self.read_only_snapshot.is_some() {
            return Err(MetadataError::InvalidState(
                "read-only mount requires ReadContext::snapshot_id".into(),
            ));
        }

        let tip = *self
            .tip_snapshot
            .read()
            .map_err(|_| MetadataError::InvalidState("lock poisoned".into()))?;

        let active = sqlx::query_as::<_, LayerIdPath>(
            r#"SELECT id, storage_uri FROM openduck_layer
               WHERE db_id = $1 AND status = 'active'"#,
        )
        .bind(self.db_id)
        .fetch_one(&self.pool)
        .await?;

        let mut out = vec![(
            active.id,
            resolve_path(&self.data_dir, &active.storage_uri)?,
        )];

        if let Some(snap_id) = tip {
            let sealed = sqlx::query_as::<_, LayerIdPath>(
                r#"SELECT l.id, l.storage_uri
                   FROM openduck_snapshot_layer sl
                   JOIN openduck_layer l ON l.id = sl.layer_id
                   WHERE sl.snapshot_id = $1
                   ORDER BY sl.ordinal DESC"#,
            )
            .bind(snap_id)
            .fetch_all(&self.pool)
            .await?;
            for r in sealed {
                out.push((r.id, resolve_path(&self.data_dir, &r.storage_uri)?));
            }
        }

        Ok(out)
    }

    async fn byte_from_layer(
        &self,
        layer_id: Uuid,
        path: &Path,
        offset: u64,
    ) -> Result<Option<u8>, MetadataError> {
        let row = sqlx::query_as::<_, ExtentRow>(
            r#"SELECT phys_start, logical_start, length FROM openduck_extent
               WHERE layer_id = $1 AND superseded_by IS NULL
                 AND logical_start <= $2 AND logical_start + length > $2
               ORDER BY created_at DESC
               LIMIT 1"#,
        )
        .bind(layer_id)
        .bind(offset as i64)
        .fetch_optional(&self.pool)
        .await?;

        let Some(e) = row else {
            return Ok(None);
        };
        let within = offset - e.logical_start as u64;
        let phys = e.phys_start as u64 + within;
        let mut buf = [0u8; 1];
        read_segment_bytes(path, phys, &mut buf)
            .map_err(|e| MetadataError::LayerFs(e.to_string()))?;
        Ok(Some(buf[0]))
    }

    async fn write_async(&self, logical_offset: u64, data: &[u8]) -> Result<(), DiffCoreError> {
        let (layer_id, phys_start, new_len) = {
            let mut guard = self.active.lock().map_err(|_| DiffCoreError::Busy)?;
            let w = guard
                .as_mut()
                .ok_or_else(|| DiffCoreError::Storage("no active writer".into()))?;
            let phys_start = w
                .segment
                .append(data)
                .map_err(|e| DiffCoreError::Io(std::io::Error::other(e.to_string())))?;
            let layer_id = w.layer_id;
            let new_len = w.segment.len() as i64;
            (layer_id, phys_start, new_len)
        };

        let ext_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO openduck_extent (id, db_id, layer_id, logical_start, length, phys_start)
               VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind(ext_id)
        .bind(self.db_id)
        .bind(layer_id)
        .bind(logical_offset as i64)
        .bind(data.len() as i64)
        .bind(phys_start as i64)
        .execute(&self.pool)
        .await
        .map_err(Self::meta_err)?;

        sqlx::query(r#"UPDATE openduck_layer SET byte_len = $1 WHERE id = $2"#)
            .bind(new_len)
            .bind(layer_id)
            .execute(&self.pool)
            .await
            .map_err(Self::meta_err)?;
        Ok(())
    }

    async fn seal_async(&self) -> Result<SnapshotId, DiffCoreError> {
        let started = std::time::Instant::now();
        let snap_id = Uuid::new_v4();

        let (sealed_layer_id, byte_len) = {
            let mut guard = self.active.lock().map_err(|_| DiffCoreError::Busy)?;
            let w = guard
                .take()
                .ok_or_else(|| DiffCoreError::Storage("no active writer".into()))?;
            let sealed_layer_id = w.layer_id;
            let byte_len = w.segment.len() as i64;
            drop(w);
            (sealed_layer_id, byte_len)
        };

        let mut tx = self.pool.begin().await.map_err(Self::meta_err)?;

        sqlx::query(
            r#"UPDATE openduck_layer SET byte_len = $1, status = 'sealed', sealed_at = now()
               WHERE id = $2"#,
        )
        .bind(byte_len)
        .bind(sealed_layer_id)
        .execute(&mut *tx)
        .await
        .map_err(Self::meta_err)?;

        let parent_tip: Option<Uuid> = sqlx::query_scalar(
            r#"SELECT current_tip_snapshot_id FROM openduck_db WHERE id = $1 FOR UPDATE"#,
        )
        .bind(self.db_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(Self::meta_err)?;

        sqlx::query(
            r#"INSERT INTO openduck_snapshot (id, db_id, parent_snapshot_id, sealed)
               VALUES ($1, $2, $3, true)"#,
        )
        .bind(snap_id)
        .bind(self.db_id)
        .bind(parent_tip)
        .execute(&mut *tx)
        .await
        .map_err(Self::meta_err)?;

        if let Some(parent) = parent_tip {
            sqlx::query(
                r#"INSERT INTO openduck_snapshot_layer (snapshot_id, layer_id, ordinal)
                   SELECT $1, layer_id, ordinal FROM openduck_snapshot_layer
                   WHERE snapshot_id = $2"#,
            )
            .bind(snap_id)
            .bind(parent)
            .execute(&mut *tx)
            .await
            .map_err(Self::meta_err)?;

            sqlx::query(
                r#"UPDATE openduck_layer SET ref_count = ref_count + 1
                   WHERE id IN (SELECT layer_id FROM openduck_snapshot_layer WHERE snapshot_id = $1)"#,
            )
            .bind(parent)
            .execute(&mut *tx)
            .await
            .map_err(Self::meta_err)?;
        }

        let max_ord: Option<i32> = if parent_tip.is_some() {
            sqlx::query_scalar(
                r#"SELECT MAX(ordinal) FROM openduck_snapshot_layer WHERE snapshot_id = $1"#,
            )
            .bind(snap_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(Self::meta_err)?
        } else {
            None
        };
        let next_ord = max_ord.map(|o| o + 1).unwrap_or(0);

        sqlx::query(
            r#"INSERT INTO openduck_snapshot_layer (snapshot_id, layer_id, ordinal)
               VALUES ($1, $2, $3)"#,
        )
        .bind(snap_id)
        .bind(sealed_layer_id)
        .bind(next_ord)
        .execute(&mut *tx)
        .await
        .map_err(Self::meta_err)?;

        sqlx::query(r#"UPDATE openduck_layer SET ref_count = ref_count + 1 WHERE id = $1"#)
            .bind(sealed_layer_id)
            .execute(&mut *tx)
            .await
            .map_err(Self::meta_err)?;

        sqlx::query(r#"UPDATE openduck_db SET current_tip_snapshot_id = $1 WHERE id = $2"#)
            .bind(snap_id)
            .bind(self.db_id)
            .execute(&mut *tx)
            .await
            .map_err(Self::meta_err)?;

        let new_layer_id = Uuid::new_v4();
        let next_seq: i32 = sqlx::query_scalar(
            r#"SELECT COALESCE(MAX(seq), -1) + 1 FROM openduck_layer WHERE db_id = $1"#,
        )
        .bind(self.db_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(Self::meta_err)?;

        let rel_uri = format!("{}/active-{new_layer_id}.layer", self.db_id);
        let abs = self.data_dir.join(&rel_uri);
        AppendSegment::create_at(&abs)
            .map_err(|e| DiffCoreError::Storage(format!("create segment: {e}")))?;

        sqlx::query(
            r#"INSERT INTO openduck_layer (id, db_id, seq, storage_uri, status)
               VALUES ($1, $2, $3, $4, 'active')"#,
        )
        .bind(new_layer_id)
        .bind(self.db_id)
        .bind(next_seq)
        .bind(&rel_uri)
        .execute(&mut *tx)
        .await
        .map_err(Self::meta_err)?;

        tx.commit().await.map_err(Self::meta_err)?;
        openduck_metrics::record_layer_seal_ms(started.elapsed().as_secs_f64() * 1000.0);

        if let Some(store) = &self.blob_store {
            let sealed_path = resolve_path(
                &self.data_dir,
                &format!("{}/active-{sealed_layer_id}.layer", self.db_id),
            )
            .map_err(|e| DiffCoreError::Storage(e.to_string()))?;
            if sealed_path.exists() {
                if let Ok(data) = std::fs::read(&sealed_path) {
                    let key = format!("{}/{sealed_layer_id}.layer", self.db_id);
                    let uri = diff_blob::put_layer_bytes(store.as_ref(), &key, Bytes::from(data))
                        .await
                        .map_err(|e| DiffCoreError::Storage(format!("blob upload: {e}")))?;
                    sqlx::query(r#"UPDATE openduck_layer SET storage_uri = $1 WHERE id = $2"#)
                        .bind(&uri)
                        .bind(sealed_layer_id)
                        .execute(&self.pool)
                        .await
                        .map_err(Self::meta_err)?;
                }
            }
        }

        let segment = AppendSegment::open_existing(&abs)
            .map_err(|e| DiffCoreError::Storage(e.to_string()))?;
        {
            let mut guard = self.active.lock().map_err(|_| DiffCoreError::Busy)?;
            *guard = Some(ActiveWriter {
                layer_id: new_layer_id,
                segment,
            });
        }

        if let Ok(mut t) = self.tip_snapshot.write() {
            *t = Some(snap_id);
        }

        Ok(SnapshotId(snap_id))
    }
}

/// Create `openduck_db` row + initial active layer + empty segment file when the name is missing.
pub async fn bootstrap_openduck_database(
    pool: &PgPool,
    database_name: &str,
    data_dir: &Path,
) -> Result<(), MetadataError> {
    let exists: bool =
        sqlx::query_scalar(r#"SELECT EXISTS(SELECT 1 FROM openduck_db WHERE name = $1)"#)
            .bind(database_name)
            .fetch_one(pool)
            .await?;
    if exists {
        return Ok(());
    }

    let db_id = Uuid::new_v4();
    let layer_id = Uuid::new_v4();
    let rel_uri = format!("{}/active-{layer_id}.layer", db_id);
    let abs = data_dir.join(&rel_uri);
    if let Some(parent) = abs.parent() {
        std::fs::create_dir_all(parent).map_err(|e| MetadataError::LayerFs(e.to_string()))?;
    }
    AppendSegment::create_at(&abs).map_err(|e| MetadataError::LayerFs(e.to_string()))?;

    let mut tx = pool.begin().await?;
    sqlx::query(r#"INSERT INTO openduck_db (id, name) VALUES ($1, $2)"#)
        .bind(db_id)
        .bind(database_name)
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        r#"INSERT INTO openduck_layer (id, db_id, seq, storage_uri, status)
           VALUES ($1, $2, 0, $3, 'active')"#,
    )
    .bind(layer_id)
    .bind(db_id)
    .bind(&rel_uri)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

impl StorageBackend for PgStorageBackend {
    fn read(&self, logical: LogicalRange, ctx: ReadContext) -> Result<Bytes, DiffCoreError> {
        let mut ctx = ctx;
        if ctx.snapshot_id.is_none() {
            if let Some(s) = self.read_only_snapshot {
                ctx.snapshot_id = Some(s);
            }
        }
        let mut out = vec![0u8; logical.len as usize];
        for (i, b) in out.iter_mut().enumerate() {
            let off = logical
                .offset
                .checked_add(i as u64)
                .ok_or(DiffCoreError::Eof)?;
            *b = self.read_byte_sync(off, &ctx)?;
        }
        Ok(Bytes::from(out))
    }

    fn write(&self, logical_offset: u64, data: &[u8]) -> Result<(), DiffCoreError> {
        if self.read_only_snapshot.is_some() {
            return Err(DiffCoreError::Storage("read-only snapshot mount".into()));
        }
        self.block_on(async { self.write_async(logical_offset, data).await })
    }

    fn flush(&self) -> Result<(), DiffCoreError> {
        if self.read_only_snapshot.is_some() {
            return Ok(());
        }
        let mut guard = self.active.lock().map_err(|_| DiffCoreError::Busy)?;
        let w = guard
            .as_mut()
            .ok_or_else(|| DiffCoreError::Storage("no active writer".into()))?;
        w.segment
            .flush()
            .map_err(|e| DiffCoreError::Io(std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    fn fsync(&self) -> Result<(), DiffCoreError> {
        self.flush()
    }

    fn seal(&self) -> Result<SnapshotId, DiffCoreError> {
        if self.read_only_snapshot.is_some() {
            return Err(DiffCoreError::Storage("cannot seal read-only mount".into()));
        }
        self.block_on(async { self.seal_async().await })
    }
}
