//! Bot snapshots (docs/03 §3.3) and the `SnapshotStore` trait (ADR-22).
//!
//! A snapshot is the serialized hand-off state of a bot: module state,
//! behavior-rule timers, derived stat counters and the outbox cursor — never
//! credentials (docs/09 §9.1). Statistics events are event-sourced at the
//! panel and are *not* inside (docs/03 §3.6).

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

use crate::epoch::{Epoch, Fenced};

/// Size budget enforced by the gateway (docs/03 §3.3, docs/09 §9.2).
pub const SNAPSHOT_SIZE_BUDGET: usize = 256 * 1024;

/// Current snapshot schema version.
pub const SNAPSHOT_SCHEMA: u32 = 1;

/// Checkpoints keep the unacked stat-event tail bounded to this many events
/// (docs/03 §3.6).
pub const CHECKPOINT_OUTBOX_TAIL: usize = 1000;

/// Opaque snapshot identifier (`snap_…`).
#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct SnapshotId(pub String);

impl SnapshotId {
    pub fn new(id: impl Into<String>) -> Self {
        SnapshotId(id.into())
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a snapshot was taken (`reason` field, docs/03 §3.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotReason {
    #[default]
    Upgrade,
    Eviction,
    ProxyChange,
    Manual,
    Checkpoint,
    /// Written by the restore path after a crash so the next crash doesn't
    /// lose what the first restore recovered.
    CrashRecovered,
    /// Desired-state pause/stop drain.
    DesiredState,
}

/// Snapshot fidelity (`quality` field, docs/03 §3.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotQuality {
    /// Taken at a safe point after a cooperative drain.
    #[default]
    Clean,
    /// Taken past `drain_deadline_s` at the next quiescent tick: targets
    /// stopped, in-flight step discarded (docs/03 §3.2 "Deadlines").
    Degraded,
    /// Periodic crash-path snapshot (docs/03 §3.8).
    Checkpoint,
}

/// Versions of the process that wrote the snapshot.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WriterInfo {
    #[serde(rename = "botd")]
    pub botd: String,
    #[serde(default)]
    pub engine: String,
    /// `<VERSION_HASH>` of the packet schema, for the rollback gate.
    #[serde(default)]
    pub packet_schema: String,
}

/// `runtime` section: what the module runner was doing.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSection {
    #[serde(default)]
    pub active_module: Option<String>,
    #[serde(default)]
    pub settings_rev: u64,
    /// "running" | "paused" | "stopped".
    #[serde(default)]
    pub run_state: String,
}

/// `module.sequence` section: a `TaskSequence` resumes by step *name* at a
/// step boundary, never mid-step (docs/03 §3.4, ADR-08).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SequenceState {
    #[serde(default)]
    pub run_key: Option<String>,
    #[serde(default)]
    pub step: Option<String>,
    #[serde(default)]
    pub attempt: u32,
    /// Opaque per-step results carried across the boundary.
    #[serde(default)]
    pub results: serde_json::Value,
    /// Wall-clock time already spent in the current step, rebased on restore.
    #[serde(default)]
    pub elapsed_in_step_ms: u64,
}

/// `module` section: module-defined state plus its sequence cursor.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModuleSection {
    pub id: String,
    #[serde(default = "default_state_version")]
    pub state_version: u32,
    /// Module-defined state blob (`export_state()` output).
    #[serde(default)]
    pub state: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<SequenceState>,
}

fn default_state_version() -> u32 {
    1
}

/// One entry of `behaviors`, keyed by rule id (docs/03 §3.3).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorSection {
    #[serde(default = "default_state_version")]
    pub state_version: u32,
    /// Rule-defined state: durations only (`since_last_ms`), never
    /// `Instant`-relative timestamps (time rebasing, docs/03 §3.4).
    #[serde(default)]
    pub state: serde_json::Value,
}

/// `stats` section: derived counters + outbox cursor only (docs/03 §3.6,
/// ADR-09).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatsSection {
    #[serde(default)]
    pub session_started_unix_ms: i64,
    #[serde(default)]
    pub deaths: u64,
    /// Last known balances.
    #[serde(default)]
    pub balances_last: Vec<f64>,
    /// Last hour, compacted per minute.
    #[serde(default)]
    pub rate_window: Vec<serde_json::Value>,
    /// Last `seq` acked by the panel.
    #[serde(default)]
    pub outbox_cursor: u64,
    /// Unacked tail (bounded to `CHECKPOINT_OUTBOX_TAIL`) so a pod loss
    /// loses at most the checkpoint interval (docs/03 §3.6).
    #[serde(default)]
    pub outbox_tail: Vec<serde_json::Value>,
}

/// `game_hint` section: validation only. Compared against the live world
/// after login; a mismatch re-evaluates the sequence (docs/03 §3.4).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GameHint {
    #[serde(default)]
    pub map_id: Option<i64>,
    #[serde(default)]
    pub hero_x: Option<i64>,
    #[serde(default)]
    pub hero_y: Option<i64>,
}

/// A bot snapshot as exchanged with `botd` (docs/03 §3.3). Field names and
/// serde shapes follow the doc's JSON example exactly.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BotSnapshot {
    pub schema: u32,
    pub bot_id: String,
    pub epoch: Epoch,
    #[serde(default)]
    pub seq: u64,
    pub taken_at_unix_ms: i64,
    pub reason: SnapshotReason,
    #[serde(default)]
    pub quality: SnapshotQuality,
    #[serde(default)]
    pub writer: WriterInfo,
    /// Oldest botd that can read this snapshot (rollback gate, docs/03 §3.5).
    #[serde(default)]
    pub min_reader: String,
    #[serde(default)]
    pub runtime: RuntimeSection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<ModuleSection>,
    #[serde(default)]
    pub behaviors: BTreeMap<String, BehaviorSection>,
    #[serde(default)]
    pub stats: StatsSection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub game_hint: Option<GameHint>,
}

impl BotSnapshot {
    pub fn new(
        bot_id: impl Into<String>,
        epoch: Epoch,
        reason: SnapshotReason,
        quality: SnapshotQuality,
        taken_at_unix_ms: i64,
    ) -> Self {
        BotSnapshot {
            schema: SNAPSHOT_SCHEMA,
            bot_id: bot_id.into(),
            epoch,
            seq: 0,
            taken_at_unix_ms,
            reason,
            quality,
            writer: WriterInfo::default(),
            min_reader: String::new(),
            runtime: RuntimeSection::default(),
            module: None,
            behaviors: BTreeMap::new(),
            stats: StatsSection::default(),
            game_hint: None,
        }
    }

    /// Serialize and check against the 256 KiB budget.
    pub fn encode(&self) -> Result<Vec<u8>, StoreError> {
        let bytes = serde_json::to_vec(self).map_err(|e| StoreError::Codec(e.to_string()))?;
        if bytes.len() > SNAPSHOT_SIZE_BUDGET {
            return Err(StoreError::TooLarge {
                size: bytes.len(),
                budget: SNAPSHOT_SIZE_BUDGET,
            });
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<BotSnapshot, StoreError> {
        if bytes.len() > SNAPSHOT_SIZE_BUDGET {
            return Err(StoreError::TooLarge {
                size: bytes.len(),
                budget: SNAPSHOT_SIZE_BUDGET,
            });
        }
        serde_json::from_slice(bytes).map_err(|e| StoreError::Codec(e.to_string()))
    }
}

/// Per-snapshot metadata kept beside the payload (sha256 for tamper
/// detection, docs/09 §9.3; soft-delete for retention, docs/12 §12.4).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotMeta {
    pub id: SnapshotId,
    pub bot_id: String,
    pub epoch: Epoch,
    #[serde(default)]
    pub seq: u64,
    pub quality: SnapshotQuality,
    pub reason: SnapshotReason,
    pub taken_at_unix_ms: i64,
    #[serde(default)]
    pub size_bytes: u64,
    /// Lowercase hex sha256 of the payload.
    #[serde(default)]
    pub sha256: String,
    /// Set when the owning bot was deleted (30-day retention starts).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix_ms: Option<i64>,
}

/// A stored snapshot: metadata + the document itself.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta,
    pub snapshot: BotSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// The write's epoch is not the bot's current epoch (docs/03 §3.7).
    #[error("write fenced: {0}")]
    Fenced(#[from] Fenced),
    /// Payload exceeds `SNAPSHOT_SIZE_BUDGET`.
    #[error("snapshot too large: {size} bytes (budget {budget})")]
    TooLarge { size: usize, budget: usize },
    #[error("snapshot not found: {0}")]
    NotFound(SnapshotId),
    #[error("unknown bot: {0}")]
    UnknownBot(String),
    #[error("snapshot codec: {0}")]
    Codec(String),
    #[error("store backend: {0}")]
    Backend(String),
}

/// Durable snapshot store (ADR-22). Implementations: in-memory (tests /
/// `local` backend), Postgres (production). All writes are epoch-conditional:
/// `record_epoch` declares the bot's current epoch (mirroring
/// `status.epoch`); `put` refuses writers on any other epoch with
/// `StoreError::Fenced`.
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Declare `bot_id`'s current epoch. Called when the operator bumps
    /// `status.epoch`, and on first-seen bots. Must be monotonic: recording
    /// an older epoch than the stored one is a no-op.
    async fn record_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), StoreError>;

    /// Read the recorded current epoch, if known.
    async fn current_epoch(&self, bot_id: &str) -> Result<Option<Epoch>, StoreError>;

    /// Commit a snapshot. Fenced unless `snapshot.epoch` equals the recorded
    /// epoch (the Postgres impl does `WHERE epoch = current_epoch`).
    async fn put(&self, snapshot: &BotSnapshot) -> Result<SnapshotId, StoreError>;

    async fn get(&self, id: &SnapshotId) -> Result<StoredSnapshot, StoreError>;

    /// Latest stored snapshot for a bot, optionally filtered by quality.
    /// `None` quality returns the newest of any quality (what a crash
    /// restore wants, docs/03 §3.8).
    async fn latest(
        &self,
        bot_id: &str,
        quality: Option<SnapshotQuality>,
    ) -> Result<Option<StoredSnapshot>, StoreError>;

    /// Metadata for all of a bot's snapshots, newest first. Excludes
    /// soft-deleted rows unless `include_deleted`.
    async fn list(
        &self,
        bot_id: &str,
        include_deleted: bool,
    ) -> Result<Vec<SnapshotMeta>, StoreError>;

    /// Mark all of a bot's snapshots soft-deleted at `at_unix_ms`
    /// (30-day retention clock starts, docs/12 §12.4).
    async fn soft_delete(&self, bot_id: &str, at_unix_ms: i64) -> Result<(), StoreError>;

    /// Undo `soft_delete` (undelete within the window).
    async fn undelete(&self, bot_id: &str) -> Result<(), StoreError>;

    /// Permanently remove rows soft-deleted at or before `before_unix_ms`.
    /// Returns rows removed.
    async fn purge(&self, before_unix_ms: i64) -> Result<u64, StoreError>;

    /// Keep the GC invariant (docs/04 §4.7): last `keep_clean` clean
    /// snapshots + latest checkpoint per bot. Returns rows removed.
    async fn gc(&self, bot_id: &str, keep_clean: usize) -> Result<u64, StoreError>;
}

/// Blanket impl so `Arc<T>` works wherever a `SnapshotStore` is expected.
#[async_trait]
impl<T: SnapshotStore + ?Sized> SnapshotStore for std::sync::Arc<T> {
    async fn record_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), StoreError> {
        (**self).record_epoch(bot_id, epoch).await
    }
    async fn current_epoch(&self, bot_id: &str) -> Result<Option<Epoch>, StoreError> {
        (**self).current_epoch(bot_id).await
    }
    async fn put(&self, snapshot: &BotSnapshot) -> Result<SnapshotId, StoreError> {
        (**self).put(snapshot).await
    }
    async fn get(&self, id: &SnapshotId) -> Result<StoredSnapshot, StoreError> {
        (**self).get(id).await
    }
    async fn latest(
        &self,
        bot_id: &str,
        quality: Option<SnapshotQuality>,
    ) -> Result<Option<StoredSnapshot>, StoreError> {
        (**self).latest(bot_id, quality).await
    }
    async fn list(
        &self,
        bot_id: &str,
        include_deleted: bool,
    ) -> Result<Vec<SnapshotMeta>, StoreError> {
        (**self).list(bot_id, include_deleted).await
    }
    async fn soft_delete(&self, bot_id: &str, at_unix_ms: i64) -> Result<(), StoreError> {
        (**self).soft_delete(bot_id, at_unix_ms).await
    }
    async fn undelete(&self, bot_id: &str) -> Result<(), StoreError> {
        (**self).undelete(bot_id).await
    }
    async fn purge(&self, before_unix_ms: i64) -> Result<u64, StoreError> {
        (**self).purge(before_unix_ms).await
    }
    async fn gc(&self, bot_id: &str, keep_clean: usize) -> Result<u64, StoreError> {
        (**self).gc(bot_id, keep_clean).await
    }
}
