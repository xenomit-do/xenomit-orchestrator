//! Wiring from the API's stores to the `plans` crate's job traits
//! (docs/12 §12.4–5). `RepoRetention` exposes `BotRepo`/`SnapshotStore`/
//! `UsageStore` to the daily purge; `RepoSubjects` feeds the meter the
//! per-bot (tenant, version) stamped on `UsageRecord`s.
//!
//! Deliberately additive: the operator's retention *decisions* are reused
//! through `plans::PurgeGate`/`plans::RetentionHooks`; only the store
//! plumbing lives here.

use std::sync::Arc;

use async_trait::async_trait;
use fleet_types::crds::{VersionChannel, XenomitBot};
use fleet_types::snapshot::SnapshotStore;
use plans::{MeterSubject, MeterSubjects, RetentionError, RetentionHooks, UsageError, UsageStore};

use crate::repo::BotRepo;

/// `plans::RetentionHooks` over the fleet's repos. `purge_usage` is a
/// no-op without a usage store; `purge_bot_rows` stays a no-op until
/// stat/config history lands in Postgres (docs/12 §12.4 rows 2–4).
pub struct RepoRetention {
    pub bots: Arc<dyn BotRepo>,
    pub snapshots: Arc<dyn SnapshotStore>,
    pub usage: Option<Arc<dyn UsageStore>>,
}

#[async_trait]
impl RetentionHooks for RepoRetention {
    async fn deleted_bots(&self) -> Result<Vec<XenomitBot>, RetentionError> {
        Ok(self
            .bots
            .list(None)
            .await
            .into_iter()
            .filter(|b| {
                b.status
                    .as_ref()
                    .and_then(|s| s.deleted_at_unix_ms)
                    .is_some()
            })
            .collect())
    }

    async fn delete_bot(&self, bot_id: &str) -> Result<bool, RetentionError> {
        self.bots
            .delete(bot_id)
            .await
            .map_err(|e| RetentionError::Backend(e.to_string()))
    }

    async fn purge_snapshots(&self, before_unix_ms: i64) -> Result<u64, RetentionError> {
        self.snapshots
            .purge(before_unix_ms)
            .await
            .map_err(|e| RetentionError::Backend(e.to_string()))
    }

    async fn purge_usage(&self, before_unix_ms: i64) -> Result<u64, RetentionError> {
        match &self.usage {
            Some(u) => u
                .purge(before_unix_ms)
                .await
                .map_err(|e| RetentionError::Backend(e.to_string())),
            None => Ok(0),
        }
    }
}

/// `plans::MeterSubjects` over `BotRepo`: tenant = `spec.owner`,
/// version = the image the operator last reported (`status.image`),
/// falling back to the channel when the bot never ran. Soft-deleted bots
/// stop metering at delete (their timeline ends); no subject rows for them.
pub struct RepoSubjects {
    pub bots: Arc<dyn BotRepo>,
}

fn channel_str(c: VersionChannel) -> &'static str {
    match c {
        VersionChannel::Stable => "stable",
        VersionChannel::Canary => "canary",
        VersionChannel::Pinned => "pinned",
    }
}

#[async_trait]
impl MeterSubjects for RepoSubjects {
    async fn subjects(&self) -> Result<Vec<MeterSubject>, UsageError> {
        Ok(self
            .bots
            .list(None)
            .await
            .into_iter()
            .filter(|b| {
                b.status
                    .as_ref()
                    .and_then(|s| s.deleted_at_unix_ms)
                    .is_none()
            })
            .map(|b| MeterSubject {
                bot_id: b.metadata.name.clone().unwrap_or_default(),
                tenant: b.spec.owner.clone(),
                version: b
                    .status
                    .as_ref()
                    .and_then(|s| s.image.clone())
                    .unwrap_or_else(|| channel_str(b.spec.version.channel).to_string()),
            })
            .collect())
    }
}
