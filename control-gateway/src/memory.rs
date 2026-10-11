//! In-memory `SnapshotStore`: the `local` backend's store and the baseline
//! for conformance-testing the Postgres implementation (ADR-22).

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Mutex;

use fleet_types::epoch::{Epoch, Fenced};
use fleet_types::snapshot::{
    BotSnapshot, SnapshotId, SnapshotMeta, SnapshotQuality, SnapshotStore, StoreError,
    StoredSnapshot,
};

#[derive(Default)]
pub struct MemorySnapshotStore {
    epochs: Mutex<HashMap<String, u64>>,
    snapshots: Mutex<HashMap<String, StoredSnapshot>>,
    seq: Mutex<u64>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshots of a bot, oldest first (test/debug helper).
    pub fn stored(&self, bot_id: &str) -> Vec<StoredSnapshot> {
        let mut v: Vec<_> = self
            .snapshots
            .lock()
            .unwrap()
            .values()
            .filter(|s| s.meta.bot_id == bot_id)
            .cloned()
            .collect();
        v.sort_by_key(|s| (s.meta.taken_at_unix_ms, s.meta.seq, s.meta.id.clone()));
        v
    }
}

#[async_trait]
impl SnapshotStore for MemorySnapshotStore {
    async fn record_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), StoreError> {
        let mut epochs = self.epochs.lock().unwrap();
        let cur = epochs.entry(bot_id.into()).or_insert(0);
        // Monotonic: recording an older epoch is a no-op.
        if epoch.get() > *cur {
            *cur = epoch.get();
        }
        Ok(())
    }

    async fn current_epoch(&self, bot_id: &str) -> Result<Option<Epoch>, StoreError> {
        Ok(self.epochs.lock().unwrap().get(bot_id).map(|e| Epoch(*e)))
    }

    async fn put(&self, snapshot: &BotSnapshot) -> Result<SnapshotId, StoreError> {
        let current = self
            .epochs
            .lock()
            .unwrap()
            .get(&snapshot.bot_id)
            .copied()
            .unwrap_or(0);
        if snapshot.epoch.get() != current {
            return Err(StoreError::Fenced(Fenced {
                expected: Epoch(current),
                got: snapshot.epoch,
            }));
        }
        let payload = snapshot.encode()?;
        let sha256 = hex::encode(Sha256::digest(&payload));
        let id = {
            let mut seq = self.seq.lock().unwrap();
            *seq += 1;
            SnapshotId(format!("snap_{:06}", *seq))
        };
        let meta = SnapshotMeta {
            id: id.clone(),
            bot_id: snapshot.bot_id.clone(),
            epoch: snapshot.epoch,
            seq: snapshot.seq,
            quality: snapshot.quality,
            reason: snapshot.reason,
            taken_at_unix_ms: snapshot.taken_at_unix_ms,
            size_bytes: payload.len() as u64,
            sha256,
            deleted_at_unix_ms: None,
        };
        self.snapshots.lock().unwrap().insert(
            id.0.clone(),
            StoredSnapshot {
                meta,
                snapshot: snapshot.clone(),
            },
        );
        Ok(id)
    }

    async fn get(&self, id: &SnapshotId) -> Result<StoredSnapshot, StoreError> {
        self.snapshots
            .lock()
            .unwrap()
            .get(&id.0)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(id.clone()))
    }

    async fn latest(
        &self,
        bot_id: &str,
        quality: Option<SnapshotQuality>,
    ) -> Result<Option<StoredSnapshot>, StoreError> {
        Ok(self.stored(bot_id).into_iter().rfind(|s| {
            s.meta.deleted_at_unix_ms.is_none() && quality.is_none_or(|q| s.meta.quality == q)
        }))
    }

    async fn list(
        &self,
        bot_id: &str,
        include_deleted: bool,
    ) -> Result<Vec<SnapshotMeta>, StoreError> {
        let mut v: Vec<_> = self
            .stored(bot_id)
            .into_iter()
            .map(|s| s.meta)
            .filter(|m| include_deleted || m.deleted_at_unix_ms.is_none())
            .collect();
        v.reverse();
        Ok(v)
    }

    async fn soft_delete(&self, bot_id: &str, at_unix_ms: i64) -> Result<(), StoreError> {
        let mut snaps = self.snapshots.lock().unwrap();
        for s in snaps.values_mut() {
            if s.meta.bot_id == bot_id {
                s.meta.deleted_at_unix_ms = Some(at_unix_ms);
            }
        }
        Ok(())
    }

    async fn undelete(&self, bot_id: &str) -> Result<(), StoreError> {
        let mut snaps = self.snapshots.lock().unwrap();
        for s in snaps.values_mut() {
            if s.meta.bot_id == bot_id {
                s.meta.deleted_at_unix_ms = None;
            }
        }
        Ok(())
    }

    async fn purge(&self, before_unix_ms: i64) -> Result<u64, StoreError> {
        let mut snaps = self.snapshots.lock().unwrap();
        let before = snaps.len();
        snaps.retain(|_, s| s.meta.deleted_at_unix_ms.is_none_or(|t| t > before_unix_ms));
        Ok((before - snaps.len()) as u64)
    }

    /// docs/04 §4.7: keep the latest checkpoint plus the newest
    /// `keep_clean` clean/degraded snapshots per bot.
    async fn gc(&self, bot_id: &str, keep_clean: usize) -> Result<u64, StoreError> {
        let keep: Vec<String> = {
            let all = self.stored(bot_id);
            let mut keep: Vec<String> = all
                .iter()
                .rev()
                .filter(|s| s.meta.quality == SnapshotQuality::Checkpoint)
                .take(1)
                .map(|s| s.meta.id.0.clone())
                .collect();
            keep.extend(
                all.iter()
                    .rev()
                    .filter(|s| s.meta.quality != SnapshotQuality::Checkpoint)
                    .take(keep_clean)
                    .map(|s| s.meta.id.0.clone()),
            );
            keep
        };
        let mut snaps = self.snapshots.lock().unwrap();
        let before = snaps.len();
        snaps.retain(|_, s| s.meta.bot_id != bot_id || keep.contains(&s.meta.id.0));
        Ok((before - snaps.len()) as u64)
    }
}
