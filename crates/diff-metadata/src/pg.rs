use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{DatabaseRow, MetadataError, MetadataRepository};

#[derive(Clone)]
pub struct PostgresMetadata {
    pool: PgPool,
}

impl PostgresMetadata {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self, MetadataError> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Single-writer lease (see IMPLEMENTATION_PLAN §8.2). Returns `true` if `holder` holds the lease.
pub async fn try_acquire_write_lease(
    pool: &PgPool,
    db_id: Uuid,
    holder: &str,
    ttl_secs: i64,
) -> Result<bool, MetadataError> {
    let mut tx = pool.begin().await?;
    let row = sqlx::query_as::<_, LeaseRow>(
        r#"SELECT holder, expires_at FROM openduck_write_lease WHERE db_id = $1 FOR UPDATE"#,
    )
    .bind(db_id)
    .fetch_optional(&mut *tx)
    .await?;

    let now = Utc::now();
    match row {
        None => {
            sqlx::query(
                r#"INSERT INTO openduck_write_lease (db_id, holder, expires_at)
                   VALUES ($1, $2, now() + ($3 * interval '1 second'))"#,
            )
            .bind(db_id)
            .bind(holder)
            .bind(ttl_secs)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(true)
        }
        Some(l) => {
            if l.expires_at < now || l.holder == holder {
                sqlx::query(
                    r#"UPDATE openduck_write_lease
                       SET holder = $2, expires_at = now() + ($3 * interval '1 second')
                       WHERE db_id = $1"#,
                )
                .bind(db_id)
                .bind(holder)
                .bind(ttl_secs)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok(true)
            } else {
                tx.commit().await?;
                Ok(false)
            }
        }
    }
}

/// Renew an existing lease (heartbeat). Returns `true` if the holder matched and was renewed.
pub async fn renew_write_lease(
    pool: &PgPool,
    db_id: Uuid,
    holder: &str,
    ttl_secs: i64,
) -> Result<bool, MetadataError> {
    let result = sqlx::query(
        r#"UPDATE openduck_write_lease
           SET expires_at = now() + ($3 * interval '1 second')
           WHERE db_id = $1 AND holder = $2"#,
    )
    .bind(db_id)
    .bind(holder)
    .bind(ttl_secs)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Release the write lease if held by `holder`.
pub async fn release_write_lease(
    pool: &PgPool,
    db_id: Uuid,
    holder: &str,
) -> Result<bool, MetadataError> {
    let result = sqlx::query(
        r#"DELETE FROM openduck_write_lease WHERE db_id = $1 AND holder = $2"#,
    )
    .bind(db_id)
    .bind(holder)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

#[derive(sqlx::FromRow)]
struct LeaseRow {
    holder: String,
    expires_at: DateTime<Utc>,
}

#[async_trait]
impl MetadataRepository for PostgresMetadata {
    async fn insert_db(&self, id: Uuid, name: &str) -> Result<(), MetadataError> {
        sqlx::query(
            r#"INSERT INTO openduck_db (id, name) VALUES ($1, $2)
               ON CONFLICT (name) DO NOTHING"#,
        )
        .bind(id)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_db_by_name(&self, name: &str) -> Result<Option<DatabaseRow>, MetadataError> {
        let row = sqlx::query_as::<_, DbRow>(
            r#"SELECT id, name, current_tip_snapshot_id FROM openduck_db WHERE name = $1"#,
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| DatabaseRow {
            id: r.id,
            name: r.name,
            current_tip_snapshot_id: r.current_tip_snapshot_id,
        }))
    }
}

#[derive(sqlx::FromRow)]
struct DbRow {
    id: Uuid,
    name: String,
    current_tip_snapshot_id: Option<Uuid>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip_db_name_requires_postgres() {
        let url = std::env::var("DATABASE_URL").unwrap_or_default();
        if url.is_empty() {
            eprintln!("skip: DATABASE_URL not set");
            return;
        }
        let meta = PostgresMetadata::connect(&url).await.unwrap();
        let name = format!("test_db_{}", Uuid::new_v4());
        let id = Uuid::new_v4();
        meta.insert_db(id, &name).await.unwrap();
        let row = meta.get_db_by_name(&name).await.unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap().id, id);
    }
}
