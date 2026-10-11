//! `BotRollout` CR (docs/04 §4.1): canary → waves under concurrency caps,
//! domain gates, pause/rollback. Phase 7b (docs/10) added the controller's
//! bookkeeping: per-bot roll entries, pause/resume/abort/rollback flags and
//! the measured offline window.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::bot::VersionChannel;

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "fleet.xenomit.io",
    version = "v1alpha1",
    kind = "BotRollout",
    plural = "botrollouts",
    shortname = "xbr",
    namespaced,
    status = "BotRolloutStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Done","type":"integer","jsonPath":".status.done"}"#,
    printcolumn = r#"{"name":"Total","type":"integer","jsonPath":".status.total"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BotRolloutSpec {
    /// `registry/xenomit-botd@sha256:…`
    pub target_image: String,
    /// Packet-updater game version (`VERSION_HASH` / `upgrade.json` hash)
    /// the target image supports (docs/07 §7.1). Informational today; the
    /// Phase 9 signature/admission machinery binds it to the digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_game_version: Option<String>,
    /// Which bots the rollout applies to.
    #[serde(default)]
    pub selector: RolloutSelector,
    #[serde(default)]
    pub strategy: RolloutStrategy,
    /// What to do when a gate fails (docs/04 §4.1).
    #[serde(default)]
    pub on_gate_failure: GateFailureAction,
    /// User-requested hold: no new bots roll while set; in-flight handoffs
    /// finish (docs/04 §4.1 `pause`).
    #[serde(default)]
    pub paused: bool,
    /// Stop the rollout where it stands: pending bots are skipped,
    /// in-flight handoffs finish, rolled bots stay on the target image.
    #[serde(default)]
    pub aborted: bool,
    /// Hand every successfully rolled bot back to its previous image
    /// (docs/07 §7.5 — same machinery in reverse).
    #[serde(default)]
    pub rollback: bool,
    /// Monotonic resume token bumped by the API's resume action. Lets a
    /// gate-paused rollout resume without the controller writing its own
    /// spec (docs/04 §4.1 `resume`).
    #[serde(default)]
    pub resume_count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RolloutSelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<VersionChannel>,
    /// Explicit bot names (also used by canary.bots).
    #[serde(default)]
    pub bots: Vec<String>,
}

impl RolloutSelector {
    /// Does `bot` match this selector? An empty `bots` list means "every
    /// bot on `channel`"; `channel: None` matches every channel.
    pub fn matches(&self, bot: &super::bot::XenomitBot) -> bool {
        if !self.bots.is_empty() {
            let name = bot.metadata.name.as_deref().unwrap_or_default();
            if !self.bots.iter().any(|b| b == name) {
                return false;
            }
        }
        match self.channel {
            Some(c) => bot.spec.version.channel == c,
            None => true,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RolloutStrategy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canary: Option<CanaryStrategy>,
    #[serde(default)]
    pub waves: WaveStrategy,
    #[serde(default)]
    pub gates: RolloutGates,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CanaryStrategy {
    #[serde(default)]
    pub count: u32,
    /// Post-canary soak before wave 1 starts (docs/04 §4.1 `bakeMinutes`).
    #[serde(default)]
    pub bake_minutes: u32,
    /// Pinned canary bots (docs/04 §4.1 example).
    #[serde(default)]
    pub bots: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WaveStrategy {
    /// Max bots simultaneously draining, percent of fleet ("25%") or an
    /// absolute count ("10").
    #[serde(default = "default_max_draining")]
    pub max_draining: String,
    /// Hard cap on bots offline at once (docs/03 §3.1 constraint 2).
    /// Bots released from `NeedsUpdate` are exempt — they were already
    /// down (ci/docs/needsupdate-failsafe.md).
    #[serde(default = "default_max_offline")]
    pub max_offline: u32,
    /// Soak between waves (docs/04 §4.1 `interWaveDelay`).
    #[serde(default = "default_inter_wave_delay_s")]
    pub inter_wave_delay_s: u32,
    /// Spreads logins inside a wave (login bursts, docs/04 §4.4).
    #[serde(default = "default_login_jitter_s")]
    pub login_jitter_s: u32,
}

impl WaveStrategy {
    /// Resolve `max_draining` against the selected-bot count: a `"N%"`
    /// value is that share of `total` (rounded up, min 1), a bare number
    /// is an absolute count (min 1). Unparsable input falls back to 1 —
    /// conservative beats fast.
    pub fn resolve_max_draining(&self, total: usize) -> u32 {
        let s = self.max_draining.trim();
        if let Some(pct) = s.strip_suffix('%') {
            match pct.trim().parse::<u32>() {
                Ok(p) => ((total as u64 * p as u64).div_ceil(100)).max(1) as u32,
                Err(_) => 1,
            }
        } else {
            s.parse::<u32>().unwrap_or(1).max(1)
        }
    }
}

impl Default for WaveStrategy {
    fn default() -> Self {
        WaveStrategy {
            max_draining: default_max_draining(),
            max_offline: default_max_offline(),
            inter_wave_delay_s: default_inter_wave_delay_s(),
            login_jitter_s: default_login_jitter_s(),
        }
    }
}

fn default_max_draining() -> String {
    "25%".into()
}
fn default_max_offline() -> u32 {
    5
}
fn default_inter_wave_delay_s() -> u32 {
    120
}
fn default_login_jitter_s() -> u32 {
    30
}

/// Domain gates checked per wave (docs/04 §4.1, extended in phase 7b).
/// `None`/absent disables that gate. Rates are permille of the bots that
/// completed a roll attempt (canary + rolled waves so far).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RolloutGates {
    /// Failed restores (`restore.quality = failed` / strict exit 70).
    #[serde(default)]
    pub max_failed_restores: u32,
    /// Permille (2% = 20) of rolled bots that parked in `NeedsHuman`
    /// (login/captcha/credentials) after the roll.
    #[serde(default)]
    pub max_login_failure_rate_permille: u32,
    /// Reward rate vs the pre-rollout window (docs/06 §6.6); checked only
    /// where a metric sample exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_reward_rate_ratio: Option<f64>,
    /// Offline-window budget per bot (docs/03 §3.9 p95 ≤ 45 s): any rolled
    /// bot whose measured drain→Running window exceeds it fails the gate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_offline_window_ms: Option<u64>,
    /// Permille of rolled bots that errored (successor crash, `Failed`,
    /// `Quarantined`, `InvalidConfig`, roll timeout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_error_rate_permille: Option<u32>,
    /// Permille of rolled bots in `NeedsHuman` — same observation as the
    /// login gate, budgeted separately for dashboards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_needs_human_permille: Option<u32>,
    /// Permille of rolled bots that re-parked in `NeedsUpdate` — the new
    /// image still mismatches the game (ci/docs/needsupdate-failsafe.md).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_needs_update_permille: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum GateFailureAction {
    #[default]
    Pause,
    Rollback,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RolloutPhase {
    #[default]
    Pending,
    Canarying,
    /// Canary baked; rolling waves.
    Progressing,
    /// Held (user pause or gate failure); in-flight handoffs finished.
    Paused,
    /// `spec.aborted`: pending bots skipped, in-flight finishing.
    Aborting,
    /// Handing rolled bots back to their previous image (docs/07 §7.5).
    RollingBack,
    Done,
    /// Terminal: signature denied, concurrent rollout, or rolled back
    /// after a gate failure could not complete.
    Failed,
    /// Terminal: `spec.aborted` completed.
    Aborted,
    /// Terminal: every rolled bot was handed back to its previous image.
    RolledBack,
}

impl RolloutPhase {
    /// Phases with no further controller work.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RolloutPhase::Done
                | RolloutPhase::Failed
                | RolloutPhase::Aborted
                | RolloutPhase::RolledBack
        )
    }

    /// Phases that reject a concurrent rollout (any non-terminal).
    pub fn is_active(self) -> bool {
        !self.is_terminal()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RolloutPhase::Pending => "pending",
            RolloutPhase::Canarying => "canarying",
            RolloutPhase::Progressing => "progressing",
            RolloutPhase::Paused => "paused",
            RolloutPhase::Aborting => "aborting",
            RolloutPhase::RollingBack => "rollingBack",
            RolloutPhase::Done => "done",
            RolloutPhase::Failed => "failed",
            RolloutPhase::Aborted => "aborted",
            RolloutPhase::RolledBack => "rolledBack",
        }
    }
}

/// One gate's observed state (docs/04 §4.1 `status.gates`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GateStatus {
    pub name: String,
    pub passed: bool,
    #[serde(default)]
    pub detail: String,
}

/// Why the rollout is in `Paused` (`status.pauseCause`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PauseCause {
    /// `spec.paused`.
    User,
    /// `onGateFailure: pause`.
    Gate,
}

/// Per-bot roll bookkeeping (`status.bots[]`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RolloutBotState {
    /// Selected, not started (waiting for its wave / capacity / login slot).
    #[default]
    Pending,
    /// Handoff or `NeedsUpdate` release in flight.
    Rolling,
    /// Reached the target image Running (or tagged for a settled bot).
    Done,
    /// Roll attempt failed; `outcome` says how.
    Failed,
    /// Deliberately untouched (missing bot, deleted, desired-state change,
    /// pinned `NeedsUpdate`, abort sweep).
    Skipped,
    /// Being handed back to `fromImage`.
    RollingBack,
    /// Back on `fromImage`.
    RolledBack,
}

/// How a roll attempt ended (`status.bots[].outcome`); feeds the gates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RolloutBotOutcome {
    /// On target, restore clean/degraded.
    Success,
    /// Restore reported `failed` (docs/03 §3.5).
    FailedRestore,
    /// Successor died before `Running`; the bot recovered on its old image.
    UpgradeAborted,
    /// Parked `NeedsHuman` after the roll (login/captcha/credentials).
    LoginFailure,
    /// Re-parked `NeedsUpdate` — the target still mismatches the game.
    NeedsUpdateParked,
    /// `Failed` / `Quarantined` / `InvalidConfig` / crash-loop.
    Fault,
    /// In-flight longer than the entry deadline.
    Timeout,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RolloutBotEntry {
    /// `metadata.name` of the `XenomitBot`.
    pub bot: String,
    #[serde(default)]
    pub state: RolloutBotState,
    /// 0 = canary set, 1.. = wave index.
    #[serde(default)]
    pub wave: u32,
    /// Image the bot ran when selected (rollback target).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_image: Option<String>,
    /// The bot was parked `NeedsUpdate` when selected — released, not
    /// drained; exempt from `maxOffline` (ci/docs/needsupdate-failsafe.md).
    #[serde(default)]
    pub from_needs_update: bool,
    /// When the bot provably went offline (predecessor drained / release).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offline_started_at_unix_ms: Option<i64>,
    /// Measured offline window (offline → Running) for the SLO gates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offline_window_ms: Option<u64>,
    /// Earliest time the login may start (token bucket + jitter).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_at_unix_ms: Option<i64>,
    /// Hard deadline for the in-flight roll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<RolloutBotOutcome>,
    /// Restore quality reported by the successor (clean/degraded/failed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_quality: Option<String>,
    #[serde(default)]
    pub detail: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BotRolloutStatus {
    #[serde(default)]
    pub phase: RolloutPhase,
    /// Bots in a terminal per-bot state (Done/Failed/Skipped/RolledBack).
    #[serde(default)]
    pub done: u32,
    #[serde(default)]
    pub total: u32,
    /// Current wave (0 while Canarying).
    #[serde(default)]
    pub wave: u32,
    /// Latest gate evaluation (docs/04 §4.1 `status.gates`).
    #[serde(default)]
    pub gates: Vec<GateStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    /// Per-bot roll bookkeeping.
    #[serde(default)]
    pub bots: Vec<RolloutBotEntry>,
    /// Why `phase = paused`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause_cause: Option<PauseCause>,
    /// Phase to return to on resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_phase: Option<RolloutPhase>,
    /// `spec.resumeCount` last consumed.
    #[serde(default)]
    pub resume_handled: u64,
    /// When the canary set finished (bake timer starts here).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canary_done_at_unix_ms: Option<i64>,
    /// Next wave may not start before this time (`interWaveDelay`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wave_ready_at_unix_ms: Option<i64>,
    /// p95 of `bots[].offlineWindowMs` measured so far (docs/03 §3.9 SLO).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offline_window_p95_ms: Option<u64>,
    /// Human-readable progress / failure detail.
    #[serde(default)]
    pub detail: String,
}
