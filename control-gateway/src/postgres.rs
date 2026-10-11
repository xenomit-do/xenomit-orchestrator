//! Postgres `SnapshotStore` (ADR-22), behind the `postgres` feature.
//!
//! Schema is created by [`PostgresSnapshotStore::migrate`]; queries use
//! `sqlx::query` (not the `query!` macros) so the crate compiles without a
//! `DATABASE_URL`. Epoch fencing is enforced inside the same transaction
//! as the insert, mirroring the in-memory impl exactly.

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use fleet_types::epoch::{Epoch, Fenced};
use fleet_types::snapshot::{
    BotSnapshot, SnapshotId, SnapshotMeta, SnapshotQuality, SnapshotStore, StoreError,
    StoredSnapshot,
};

/// `SnapshotStore` backed by two tables (`bot_epochs`, `snapshots`).
pub struct PostgresSnapshotStore {
    pool: PgPool,
}

impl PostgresSnapshotStore {
    pub fn new(pool: PgPool) -> Self {
        PostgresSnapshotStore { pool }
    }

    pub async fn connect(url: &str, max_connections: u32) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(Self::new(pool))
    }

    /// Idempotent schema setup (kept here, not in a migrations dir, so the
    /// schema lives next to the queries it serves).
    pub async fn migrate(&self) -> Result<(), StoreError> {
        for stmt in [
            "CREATE TABLE IF NOT EXISTS bot_epochs (
                bot_id TEXT PRIMARY KEY,
                epoch  BIGINT NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS snapshots (
                id                TEXT PRIMARY KEY,
                bot_id            TEXT NOT NULL,
                epoch             BIGINT NOT NULL,
                seq               BIGINT NOT NULL,
                quality           TEXT NOT NULL,
                reason            TEXT NOT NULL,
                taken_at_unix_ms  BIGINT NOT NULL,
                size_bytes        BIGINT NOT NULL,
                sha256            TEXT NOT NULL,
                payload           BYTEA NOT NULL,
                deleted_at_unix_ms BIGINT
            )",
            "CREATE INDEX IF NOT EXISTS snapshots_bot
                ON snapshots (bot_id, taken_at_unix_ms DESC)",
        ] {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))?;
        }
        Ok(())
    }

    fn quality_str(q: SnapshotQuality) -> &'static str {
        match q {
            SnapshotQuality::Clean => "clean",
            SnapshotQuality::Degraded => "degraded",
            SnapshotQuality::Checkpoint => "checkpoint",
        }
    }

    fn quality_parse(s: &str) -> SnapshotQuality {
        match s {
            "degraded" => SnapshotQuality::Degraded,
            "checkpoint" => SnapshotQuality::Checkpoint,
            _ => SnapshotQuality::Clean,
        }
    }

    fn reason_str(r: fleet_types::snapshot::SnapshotReason) -> &'static str {
        use fleet_types::snapshot::SnapshotReason as R;
        match r {
            R::Upgrade => "upgrade",
            R::Eviction => "eviction",
            R::ProxyChange => "proxy_change",
            R::Manual => "manual",
            R::Checkpoint => "checkpoint",
            R::CrashRecovered => "crash_recovered",
            R::DesiredState => "desired_state",
        }
    }

    fn reason_parse(s: &str) -> fleet_types::snapshot::SnapshotReason {
        use fleet_types::snapshot::SnapshotReason as R;
        match s {
            "eviction" => R::Eviction,
            "proxy_change" => R::ProxyChange,
            "manual" => R::Manual,
            "checkpoint" => R::Checkpoint,
            "crash_recovered" => R::CrashRecovered,
            "desired_state" => R::DesiredState,
            _ => R::Upgrade,
        }
    }

    fn row_to_stored(row: &sqlx::postgres::PgRow) -> Result<StoredSnapshot, StoreError> {
        let payload: Vec<u8> = row.get("payload");
        let snapshot = BotSnapshot::decode(&payload)?;
        let meta = SnapshotMeta {
            id: SnapshotId(row.get::<String, _>("id")),
            bot_id: row.get("bot_id"),
            epoch: Epoch(row.get::<i64, _>("epoch") as u64),
            seq: row.get::<i64, _>("seq") as u64,
            quality: Self::quality_parse(&row.get::<String, _>("quality")),
            reason: Self::reason_parse(&row.get::<String, _>("reason")),
            taken_at_unix_ms: row.get("taken_at_unix_ms"),
            size_bytes: row.get::<i64, _>("size_bytes") as u64,
            sha256: row.get("sha256"),
            deleted_at_unix_ms: row.get("deleted_at_unix_ms"),
        };
        Ok(StoredSnapshot { meta, snapshot })
    }
}

#[async_trait]
impl SnapshotStore for PostgresSnapshotStore {
    /// Monotonic upsert of the bot's current epoch.
    async fn record_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO bot_epochs (bot_id, epoch) VALUES ($1, $2)
             ON CONFLICT (bot_id) DO UPDATE SET epoch = GREATEST(bot_epochs.epoch, $2)",
        )
        .bind(bot_id)
        .bind(epoch.get() as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn current_epoch(&self, bot_id: &str) -> Result<Option<Epoch>, StoreError> {
        let row = sqlx::query("SELECT epoch FROM bot_epochs WHERE bot_id = $1")
            .bind(bot_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(row.map(|r| Epoch(r.get::<i64, _>(0) as u64)))
    }

    /// Fencing is transactional: the epoch check and the insert commit or
    /// fail together, so a stale writer can never slip a row in between an
    /// operator's epoch bump and the new pod's writes (docs/03 §3.7).
    async fn put(&self, snapshot: &BotSnapshot) -> Result<SnapshotId, StoreError> {
        let payload = snapshot.encode()?;
        let sha256 = hex::encode(Sha256::digest(&payload));
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;

        let current = sqlx::query("SELECT epoch FROM bot_epochs WHERE bot_id = $1 FOR UPDATE")
            .bind(&snapshot.bot_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?
            .map(|r| r.get::<i64, _>(0) as u64)
            .unwrap_or(0);
        if snapshot.epoch.get() != current {
            return Err(StoreError::Fenced(Fenced {
                expected: Epoch(current),
                got: snapshot.epoch,
            }));
        }

        let id = SnapshotId(format!(
            "snap_{:013}",
            chrono::Utc::now().timestamp_millis() as u64 * 1000 + (uuid_short() % 1000)
        ));
        sqlx::query(
            "INSERT INTO snapshots
                (id, bot_id, epoch, seq, quality, reason, taken_at_unix_ms,
                 size_bytes, sha256, payload)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(&id.0)
        .bind(&snapshot.bot_id)
        .bind(snapshot.epoch.get() as i64)
        .bind(snapshot.seq as i64)
        .bind(Self::quality_str(snapshot.quality))
        .bind(Self::reason_str(snapshot.reason))
        .bind(snapshot.taken_at_unix_ms)
        .bind(payload.len() as i64)
        .bind(&sha256)
        .bind(&payload)
        .execute(&mut *tx)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(id)
    }

    async fn get(&self, id: &SnapshotId) -> Result<StoredSnapshot, StoreError> {
        let row = sqlx::query("SELECT * FROM snapshots WHERE id = $1")
            .bind(&id.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?
            .ok_or_else(|| StoreError::NotFound(id.clone()))?;
        Self::row_to_stored(&row)
    }

    async fn latest(
        &self,
        bot_id: &str,
        quality: Option<SnapshotQuality>,
    ) -> Result<Option<StoredSnapshot>, StoreError> {
        let row = match quality {
            Some(q) => {
                sqlx::query(
                    "SELECT * FROM snapshots WHERE bot_id = $1 AND quality = $2
                     AND deleted_at_unix_ms IS NULL
                     ORDER BY taken_at_unix_ms DESC, seq DESC, id DESC LIMIT 1",
                )
                .bind(bot_id)
                .bind(Self::quality_str(q))
                .fetch_optional(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    "SELECT * FROM snapshots WHERE bot_id = $1
                     AND deleted_at_unix_ms IS NULL
                     ORDER BY taken_at_unix_ms DESC, seq DESC, id DESC LIMIT 1",
                )
                .bind(bot_id)
                .fetch_optional(&self.pool)
                .await
            }
        }
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        row.map(|r| Self::row_to_stored(&r)).transpose()
    }

    async fn list(
        &self,
        bot_id: &str,
        include_deleted: bool,
    ) -> Result<Vec<SnapshotMeta>, StoreError> {
        let rows = if include_deleted {
            sqlx::query(
                "SELECT * FROM snapshots WHERE bot_id = $1
                 ORDER BY taken_at_unix_ms DESC, seq DESC, id DESC",
            )
            .bind(bot_id)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT * FROM snapshots WHERE bot_id = $1
                 AND deleted_at_unix_ms IS NULL
                 ORDER BY taken_at_unix_ms DESC, seq DESC, id DESC",
            )
            .bind(bot_id)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        rows.iter()
            .map(|r| Self::row_to_stored(r).map(|s| s.meta))
            .collect()
    }

    async fn soft_delete(&self, bot_id: &str, at_unix_ms: i64) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE snapshots SET deleted_at_unix_ms = $2
             WHERE bot_id = $1 AND deleted_at_unix_ms IS NULL",
        )
        .bind(bot_id)
        .bind(at_unix_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn undelete(&self, bot_id: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE snapshots SET deleted_at_unix_ms = NULL WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn purge(&self, before_unix_ms: i64) -> Result<u64, StoreError> {
        let res = sqlx::query(
            "DELETE FROM snapshots WHERE deleted_at_unix_ms IS NOT NULL
             AND deleted_at_unix_ms <= $1",
        )
        .bind(before_unix_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(res.rows_affected())
    }

    async fn gc(&self, bot_id: &str, keep_clean: usize) -> Result<u64, StoreError> {
        let res = sqlx::query(
            "DELETE FROM snapshots WHERE bot_id = $1 AND id NOT IN (
                (SELECT id FROM snapshots WHERE bot_id = $1 AND quality = 'checkpoint'
                 ORDER BY taken_at_unix_ms DESC LIMIT 1)
                UNION ALL
                (SELECT id FROM snapshots WHERE bot_id = $1 AND quality <> 'checkpoint'
                 ORDER BY taken_at_unix_ms DESC LIMIT $2)
            )",
        )
        .bind(bot_id)
        .bind(keep_clean as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(res.rows_affected())
    }
}

/// Small non-sequential suffix so per-millisecond ids don't collide.
fn uuid_short() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    C.fetch_add(1, Ordering::Relaxed)
}
