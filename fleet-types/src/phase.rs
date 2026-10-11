//! Operator-visible lifecycle phases (docs/01 §1.4) and the planned-handoff
//! state machine (docs/03 §3.2).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::epoch::Epoch;

/// What the user asked for (spec.desiredState).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DesiredState {
    #[default]
    Running,
    Paused,
    Stopped,
}

/// Operator-visible phase of a bot (docs/01 §1.4). Stored on
/// `XenomitBot.status.phase` and reported on the control stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum BotPhase {
    /// CR accepted, nothing provisioned yet.
    #[default]
    Pending,
    /// Vault path/policy + pod being created.
    Provisioning,
    /// Warm standby: pod up, config parsed, proxy ok, awaiting Activate.
    Armed,
    /// Activate sent; login/restore in progress.
    Starting,
    Running,
    /// Graceful drain requested; bot still earning while it finds a safe
    /// point (docs/03 §3.2).
    Draining,
    /// Safe point reached; snapshot + outbox flush + logout in progress.
    Snapshotting,
    /// Clean exit acknowledged; next epoch may be activated.
    Terminated,
    /// Desired-state Paused reached (drained, no successor).
    Paused,
    /// Desired-state Stopped reached (drained, no successor).
    Stopped,
    /// Human input needed: captcha / bad credentials / expired session.
    /// First-class phase, not an error (docs/01 §1.4).
    NeedsHuman,
    /// Game version changed; self-drained, waiting for a rollout (ADR-15).
    NeedsUpdate,
    /// Unrecoverable failure (e.g. strict restore policy on a bad snapshot).
    Failed,
    /// Crash budget exceeded; no further restarts until manually cleared.
    Quarantined,
    /// Config failed validation (exit 64); surfaced to the user.
    InvalidConfig,
}

impl BotPhase {
    /// Phases in which a pod may exist for the current epoch.
    pub fn has_live_pod(self) -> bool {
        matches!(
            self,
            BotPhase::Provisioning
                | BotPhase::Armed
                | BotPhase::Starting
                | BotPhase::Running
                | BotPhase::Draining
                | BotPhase::Snapshotting
        )
    }

    /// Terminal-ish phases where reconcile does nothing without a spec or
    /// human change.
    pub fn is_settled(self) -> bool {
        matches!(
            self,
            BotPhase::Paused
                | BotPhase::Stopped
                | BotPhase::NeedsHuman
                | BotPhase::NeedsUpdate
                | BotPhase::Failed
                | BotPhase::Quarantined
                | BotPhase::InvalidConfig
        )
    }

    /// Parse a phase name as reported by `botd` on the control stream
    /// (lowercase snake, e.g. "needs_human").
    pub fn parse(s: &str) -> Option<BotPhase> {
        Some(match s {
            "pending" => BotPhase::Pending,
            "provisioning" => BotPhase::Provisioning,
            "armed" => BotPhase::Armed,
            "starting" => BotPhase::Starting,
            "running" => BotPhase::Running,
            "draining" => BotPhase::Draining,
            "snapshotting" => BotPhase::Snapshotting,
            "terminated" => BotPhase::Terminated,
            "paused" => BotPhase::Paused,
            "stopped" => BotPhase::Stopped,
            "needs_human" => BotPhase::NeedsHuman,
            "needs_update" => BotPhase::NeedsUpdate,
            "failed" => BotPhase::Failed,
            "quarantined" => BotPhase::Quarantined,
            "invalid_config" => BotPhase::InvalidConfig,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            BotPhase::Pending => "pending",
            BotPhase::Provisioning => "provisioning",
            BotPhase::Armed => "armed",
            BotPhase::Starting => "starting",
            BotPhase::Running => "running",
            BotPhase::Draining => "draining",
            BotPhase::Snapshotting => "snapshotting",
            BotPhase::Terminated => "terminated",
            BotPhase::Paused => "paused",
            BotPhase::Stopped => "stopped",
            BotPhase::NeedsHuman => "needs_human",
            BotPhase::NeedsUpdate => "needs_update",
            BotPhase::Failed => "failed",
            BotPhase::Quarantined => "quarantined",
            BotPhase::InvalidConfig => "invalid_config",
        }
    }
}

impl fmt::Display for BotPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a bot is in `NeedsHuman` (docs/02 §2.4, docs/08 §8.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NeedsHumanKind {
    /// Backpage demanded a CAPTCHA; the user supplies a `dosid`.
    Captcha,
    /// Credentials rejected; stop until the user fixes them (lockout risk).
    Credentials,
    /// `auth: "session"` account whose dosid is about to expire.
    SessionExpired,
    /// Undeleted bot needs credentials re-entered (docs/09 §9.2).
    ReenterCredentials,
    /// Bot paused because a plan/quota limit would be exceeded (docs/12 §12.3).
    Quota,
}

impl NeedsHumanKind {
    pub fn as_str(self) -> &'static str {
        match self {
            NeedsHumanKind::Captcha => "captcha",
            NeedsHumanKind::Credentials => "credentials",
            NeedsHumanKind::SessionExpired => "session_expired",
            NeedsHumanKind::ReenterCredentials => "reenter_credentials",
            NeedsHumanKind::Quota => "quota",
        }
    }
}

/// Why a drain/handoff was initiated (docs/03 §3.2 "Triggers").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HandoffReason {
    Upgrade,
    ConfigRequiresRestart,
    ProxyChange,
    /// SIGTERM / node drain / pod eviction.
    Eviction,
    ManualRestart,
    /// desired_state paused|stopped: no successor is created.
    DesiredStateChange,
    /// Restore a specific snapshot by user request.
    RestoreSnapshot,
}

impl HandoffReason {
    /// The `reason` string put on the wire and into snapshots.
    pub fn as_str(self) -> &'static str {
        match self {
            HandoffReason::Upgrade => "upgrade",
            HandoffReason::ConfigRequiresRestart => "config_requires_restart",
            HandoffReason::ProxyChange => "proxy_change",
            HandoffReason::Eviction => "eviction",
            HandoffReason::ManualRestart => "manual_restart",
            HandoffReason::DesiredStateChange => "desired_state",
            HandoffReason::RestoreSnapshot => "restore_snapshot",
        }
    }
}

/// Planned-handoff state machine steps (docs/03 §3.2):
/// arm successor -> drain predecessor -> (snapshot) -> activate successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum HandoffStep {
    /// Successor pod being created and armed.
    Arm,
    /// Drain sent to the predecessor; waiting for `Drained`.
    Drain,
    /// Successor activated; waiting for it to report Running.
    Activate,
}

/// In-flight planned handoff recorded on status. `target_epoch` is always
/// `status.epoch` (the successor); the predecessor is `target_epoch - 1`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HandoffState {
    pub reason: HandoffReason,
    pub step: HandoffStep,
    pub target_epoch: Epoch,
    pub started_at_unix_ms: i64,
    /// Snapshot to hand to Activate; filled when the predecessor reports
    /// `Drained`, or preset for `restore_snapshot` requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    /// Image the successor pod must run when this handoff is a rollout
    /// upgrade (docs/07 §7.3); `None` = the channel/pinned resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_image: Option<String>,
}

/// Condition types used on `XenomitBot.status.conditions` (docs/04 §4.1).
pub mod condition_type {
    pub const READY: &str = "Ready";
    pub const SNAPSHOT_RESTORED: &str = "SnapshotRestored";
    pub const PROXY_HEALTHY: &str = "ProxyHealthy";
    pub const CONFIG_VALID: &str = "ConfigValid";
}

/// A status condition, shaped like metav1.Condition but self-contained so
/// fleet-types doesn't depend on k8s-openapi for schema generation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BotCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: ConditionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Unix milliseconds of the last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_unix_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum ConditionStatus {
    True,
    #[default]
    False,
    Unknown,
}

impl BotCondition {
    pub fn new(type_: &str, status: ConditionStatus, now_unix_ms: i64) -> Self {
        BotCondition {
            type_: type_.to_string(),
            status,
            reason: None,
            message: None,
            last_transition_unix_ms: Some(now_unix_ms),
        }
    }
}
