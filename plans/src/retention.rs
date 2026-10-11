//! Retention (docs/12 §12.4): soft-deleted bots keep their history for
//! `Plan.retentionDaysAfterDelete` (default 30 d), then a daily purge job
//! removes it. Undelete inside the window clears `deletedAt` and lands the
//! bot in `NeedsHuman(credentials)`.
//!
//! The heavy lifting is reused, not duplicated:
//! - the *decision* ("retention elapsed + pods gone") is exactly
//!   `BotController::purge_due` — [`ControllerGate`] delegates to it, and
//!   [`PlanGate`] swaps its flat `retention_days` for the owner's plan value;
//! - the *stores* are traits ([`RetentionHooks`]) so the job runs on the
//!   API's repos today and on Postgres tomorrow.

use async_trait::async_trait;

use fleet_types::crds::XenomitBot;
use fleet_types::snapshot::SnapshotStore;
use operator::BotController;
use operator::policy::retention_expired;
use operator::traits::{Clock, ControlDirectory, PodBackend, ProxyPool, SecretStore};

use crate::directory::PlanDirectory;

/// Usage rows live 13 months for billing disputes / SLO (docs/12 §12.4).
pub const USAGE_RECORD_RETENTION_DAYS: u32 = 395;

const DAY_MS: i64 = 86_400_000;

/// Whether a soft-deleted bot is past retention / free of pods.
///
/// Two gates exist and both are "the operator's rules, applied here":
///
/// - [`ControllerGate`] wraps a `BotController` and calls its `purge_due`
///   verbatim (flat `ControllerConfig::retention_days`).
/// - [`PlanGate`] resolves the bot owner's effective plan through a
///   [`PlanDirectory`] and uses `retentionDaysAfterDelete` — the per-plan
///   knob of docs/12 §12.2.
pub trait PurgeGate: Send + Sync {
    /// `deleted_at + retention <= now`. Pods may still be terminating.
    fn retention_expired(&self, bot: &XenomitBot) -> bool;
    /// The bot's pods are confirmed gone (`status.delete_pods_gone`).
    fn pods_gone(&self, bot: &XenomitBot) -> bool;
    /// Safe to remove the row entirely: retention elapsed AND pods gone.
    fn purge_due(&self, bot: &XenomitBot) -> bool {
        self.retention_expired(bot) && self.pods_gone(bot)
    }
}

/// Plan-aware gate: `retentionDaysAfterDelete` of the owner's effective
/// plan, `DEFAULT_RETENTION_DAYS` when no plan resolves.
pub struct PlanGate<'a> {
    pub plans: &'a dyn PlanDirectory,
    pub clock: &'a dyn Clock,
}

impl PurgeGate for PlanGate<'_> {
    fn retention_expired(&self, bot: &XenomitBot) -> bool {
        let Some(at) = bot.status.as_ref().and_then(|s| s.deleted_at_unix_ms) else {
            return false;
        };
        let days = self.plans.retention_days(&bot.spec.owner);
        retention_expired(at, self.clock.now_unix_ms(), days)
    }

    fn pods_gone(&self, bot: &XenomitBot) -> bool {
        bot.status.as_ref().is_some_and(|s| s.delete_pods_gone)
    }
}

/// Adapter that reuses `BotController::purge_due` (and its
/// `ControllerConfig::retention_days`) exactly — plug this in where a
/// controller is already constructed and no plan-aware retention is wanted.
pub struct ControllerGate<'a, P, V, S, D, C, X>(pub &'a BotController<P, V, S, D, C, X>);

impl<P, V, S, D, C, X> PurgeGate for ControllerGate<'_, P, V, S, D, C, X>
where
    P: PodBackend,
    V: SecretStore,
    S: SnapshotStore,
    D: ControlDirectory,
    C: Clock,
    X: ProxyPool,
{
    fn retention_expired(&self, bot: &XenomitBot) -> bool {
        bot.status
            .as_ref()
            .and_then(|s| s.deleted_at_unix_ms)
            .is_some_and(|at| {
                retention_expired(at, self.0.clock.now_unix_ms(), self.0.cfg.retention_days)
            })
    }

    fn pods_gone(&self, bot: &XenomitBot) -> bool {
        bot.status.as_ref().is_some_and(|s| s.delete_pods_gone)
    }

    /// Delegate verbatim — this *is* the operator's decision.
    fn purge_due(&self, bot: &XenomitBot) -> bool {
        self.0.purge_due(bot)
    }
}

/// The stores a purge touches. Implemented by the orchestrator-api adapter
/// over `BotRepo`/`SnapshotStore`/usage store today; a Postgres impl runs
/// the same statements against `xenomit-platform/db/plans/` tables.
#[async_trait]
pub trait RetentionHooks: Send + Sync {
    /// Soft-deleted bot rows still present (`status.deletedAt` set).
    async fn deleted_bots(&self) -> Result<Vec<XenomitBot>, RetentionError>;

    /// Hard-remove the bot row. Only called once `gate.purge_due` — the
    /// Vault credentials are long destroyed (deletion order, docs/09 §9.2).
    async fn delete_bot(&self, bot_id: &str) -> Result<bool, RetentionError>;

    /// Remove a purged bot's remaining per-bot history — stat events and
    /// instance/config history (docs/12 §12.4 rows 2–4). Default is a
    /// no-op: stat events live panel-side today; a Postgres impl fills
    /// this in with `WHERE bot_id = $1` deletes.
    async fn purge_bot_rows(&self, bot_id: &str) -> Result<u64, RetentionError> {
        let _ = bot_id;
        Ok(0)
    }

    /// Global sweep of soft-deleted snapshot rows at or before `before`
    /// (`SnapshotStore::purge` semantics). The job picks the cutoff via
    /// [`snapshot_cutoff`] so it never reaches inside another bot's window.
    async fn purge_snapshots(&self, before_unix_ms: i64) -> Result<u64, RetentionError>;

    /// Drop usage rows older than 13 months (`hour < before`).
    /// Default no-op for deployments without metering.
    async fn purge_usage(&self, _before_unix_ms: i64) -> Result<u64, RetentionError> {
        Ok(0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RetentionError {
    #[error("retention store: {0}")]
    Backend(String),
}

/// Largest `SnapshotStore::purge` cutoff that only reaches rows of
/// *expired* bots. Snapshot rows share their bot's `deleted_at`, so a
/// global `purge(before)` is safe iff every still-present soft-deleted bot
/// with `deleted_at <= before` is expired — i.e. `before` stays below the
/// earliest unexpired `deleted_at`. Expired bots deleted *after* the
/// earliest unexpired one can't be reached by a prefix cutoff; their rows
/// are left to `purge_bot_rows` or the next run.
pub fn snapshot_cutoff(
    expired_deleted_ats: &[i64],
    earliest_unexpired: Option<i64>,
) -> Option<i64> {
    let cutoff = earliest_unexpired.map_or(i64::MAX, |e| e - 1);
    expired_deleted_ats
        .iter()
        .any(|at| *at <= cutoff)
        .then_some(cutoff)
}

/// One pass of the daily purge job.
#[derive(Debug, Default)]
pub struct PurgeReport {
    /// Bot rows permanently removed.
    pub bots_purged: Vec<String>,
    /// Soft-deleted bots still present: inside their window, or expired
    /// but pods not yet gone.
    pub bots_retained: Vec<String>,
    /// Per-bot history rows removed (stat events / instance history).
    pub history_rows: u64,
    /// Snapshot rows removed by the global sweep.
    pub snapshot_rows: u64,
    /// Usage rows older than 13 months removed.
    pub usage_rows: u64,
}

/// The daily purge job (docs/12 §12.4 "Mechanics").
pub struct RetentionJob<'a> {
    pub hooks: &'a dyn RetentionHooks,
    pub gate: &'a dyn PurgeGate,
    pub clock: &'a dyn Clock,
}

impl RetentionJob<'_> {
    pub async fn run(&self) -> Result<PurgeReport, RetentionError> {
        let now = self.clock.now_unix_ms();
        let mut report = PurgeReport::default();
        let mut earliest_unexpired: Option<i64> = None;
        let mut expired_ats: Vec<i64> = Vec::new();

        for bot in self.hooks.deleted_bots().await? {
            let bot_id = bot.metadata.name.clone().unwrap_or_default();
            if self.gate.retention_expired(&bot) {
                if let Some(at) = bot.status.as_ref().and_then(|s| s.deleted_at_unix_ms) {
                    expired_ats.push(at);
                }
                // Stats/config history is bounded by the same window and
                // doesn't need pods gone — purge it as soon as expired.
                report.history_rows += self.hooks.purge_bot_rows(&bot_id).await?;
                if self.gate.purge_due(&bot) {
                    if self.hooks.delete_bot(&bot_id).await? {
                        report.bots_purged.push(bot_id);
                    }
                } else {
                    report.bots_retained.push(bot_id);
                }
            } else {
                report.bots_retained.push(bot_id);
                if let Some(at) = bot.status.as_ref().and_then(|s| s.deleted_at_unix_ms) {
                    earliest_unexpired = Some(earliest_unexpired.map_or(at, |e| e.min(at)));
                }
            }
        }

        if let Some(cutoff) = snapshot_cutoff(&expired_ats, earliest_unexpired) {
            report.snapshot_rows += self.hooks.purge_snapshots(cutoff).await?;
        }
        report.usage_rows += self
            .hooks
            .purge_usage(now - USAGE_RECORD_RETENTION_DAYS as i64 * DAY_MS)
            .await?;
        Ok(report)
    }
}

/// Undelete eligibility (docs/12 §12.4 "Mechanics"): inside the window the
/// API calls the operator's `undelete` (clears `deletedAt`, lands the bot
/// in `NeedsHuman(credentials)`); past it, the history may already be
/// purged and the request is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UndeleteError {
    #[error("bot is not deleted")]
    NotDeleted,
    /// `expired_at_unix_ms` is the moment the window closed.
    #[error("retention window expired at {expired_at_unix_ms}")]
    WindowExpired { expired_at_unix_ms: i64 },
}

/// May `bot` be undeleted at `now`? `retention_days` is the owner's
/// `Plan.retentionDaysAfterDelete` (or the 30-day default).
pub fn check_undelete(
    bot: &XenomitBot,
    retention_days: u32,
    now_unix_ms: i64,
) -> Result<(), UndeleteError> {
    let at = bot
        .status
        .as_ref()
        .and_then(|s| s.deleted_at_unix_ms)
        .ok_or(UndeleteError::NotDeleted)?;
    if retention_expired(at, now_unix_ms, retention_days) {
        return Err(UndeleteError::WindowExpired {
            expired_at_unix_ms: at + retention_days as i64 * DAY_MS,
        });
    }
    Ok(())
}
