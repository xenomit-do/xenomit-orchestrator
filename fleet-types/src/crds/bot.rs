//! `XenomitBot` CR (docs/04 §4.1): one per managed bot.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::epoch::Epoch;
use crate::phase::{BotCondition, BotPhase, DesiredState, HandoffState, NeedsHumanKind};
use crate::snapshot::SnapshotId;

/// spec — desired state; lives in etcd, reconciled by `BotController`.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "fleet.xenomit.io",
    version = "v1alpha1",
    kind = "XenomitBot",
    plural = "xenomitbots",
    shortname = "xb",
    namespaced,
    status = "XenomitBotStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Epoch","type":"integer","jsonPath":".status.epoch"}"#,
    printcolumn = r#"{"name":"Desired","type":"string","jsonPath":".spec.desiredState"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct XenomitBotSpec {
    /// Panel user id; quota/ACL enforced in the API.
    pub owner: String,
    #[serde(default)]
    pub desired_state: DesiredState,
    pub account: AccountSpec,
    /// Validated against the botd JSON Schema before it ever reaches a pod.
    pub config: BotConfigSpec,
    /// Bumped on each edit; drives restart-vs-hot-apply.
    #[serde(default)]
    pub config_revision: u64,
    #[serde(default)]
    pub version: VersionSpec,
    #[serde(default)]
    pub proxy: ProxyPolicySpec,
    #[serde(default)]
    pub restore_policy: RestorePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesSpec>,
}

/// Account identity only — credentials live in Vault, never in the CR
/// (docs/04 §4.1, ADR-02).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccountSpec {
    /// e.g. "int1.darkorbit.com".
    pub server: String,
    #[serde(default)]
    pub auth_mode: AuthMode,
    /// Vault path of the account secret (`secret/data/bots/<id>/account`).
    pub credentials_vault_path: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AuthMode {
    /// Backpage username + password.
    #[default]
    Password,
    /// User supplied a `dosid` session cookie.
    Session,
}

/// Bot configuration. `settings` is the same shape as the GUI's
/// `BotSettings` (docs/02 §2.2) and is validated against the botd JSON
/// Schema server-side; here it is intentionally opaque so the CRD schema
/// doesn't fork the engine's schema.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BotConfigSpec {
    #[serde(default)]
    pub client_kind: ClientKind,
    /// `BotSettings` JSON (activeModule, modules, behaviors, plugins).
    #[serde(default)]
    #[schemars(schema_with = "opaque_settings_schema")]
    pub settings: serde_json::Value,
    /// botd handoff knobs (`handoff.*` in the botd config; operator may
    /// override per rollout, docs/02 §2.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<HandoffTuning>,
}

fn opaque_settings_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true
    })
}

/// Pod bots are packet clients only (docs/02 §2.1, ADR-16).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ClientKind {
    #[default]
    Packet,
}

/// Deadlines the operator puts on `Drain` (docs/03 §3.2 "Deadlines").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HandoffTuning {
    #[serde(default = "default_drain_deadline_s")]
    pub drain_deadline_s: u32,
    #[serde(default = "default_hard_deadline_s")]
    pub hard_deadline_s: u32,
    #[serde(default = "default_checkpoint_interval_s")]
    pub checkpoint_interval_s: u32,
}

impl Default for HandoffTuning {
    fn default() -> Self {
        HandoffTuning {
            drain_deadline_s: default_drain_deadline_s(),
            hard_deadline_s: default_hard_deadline_s(),
            checkpoint_interval_s: default_checkpoint_interval_s(),
        }
    }
}

/// docs/03 §3.2 defaults.
pub const fn default_drain_deadline_s() -> u32 {
    600
}
pub const fn default_hard_deadline_s() -> u32 {
    1800
}
pub const fn default_checkpoint_interval_s() -> u32 {
    15
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VersionSpec {
    #[serde(default)]
    pub channel: VersionChannel,
    /// Image digest when `channel = pinned`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_image: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum VersionChannel {
    #[default]
    Stable,
    Canary,
    Pinned,
}

/// How the bot reaches the game network (docs/04 §4.1, docs/05 §5.4).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProxyPolicySpec {
    #[serde(default)]
    pub mode: ProxyMode,
    /// Pool selector; ignored when `mode = fixed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<ProxySelector>,
    /// Vault path of a user-supplied proxy when `mode = fixed`; the pool
    /// never touches it (docs/05 §5.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_vault_path: Option<String>,
    /// `host:port` of a user-supplied proxy when `mode = fixed` — address
    /// only, never credentials. The API copies it out of the submitted
    /// proxy credentials so the operator can render the egress policy and
    /// probe health without reading secret values back (docs/05 §5.4,
    /// docs/09 §9.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_addr: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ProxyMode {
    /// Auto-assigned, sticky (docs/05 §5.4).
    #[default]
    Pool,
    /// User-supplied proxy in Vault.
    Fixed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProxySelector {
    #[serde(default)]
    pub country: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// `best_effort` never strands an account on a bad snapshot (docs/03 §3.5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RestorePolicy {
    #[default]
    BestEffort,
    Strict,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<ResourceList>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<ResourceList>,
}

/// Per-bot requests are a placeholder to benchmark (docs/04 §4.3): "50m",
/// "64Mi" style quantities.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceList {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

/// Recorded pod exit (docs/04 §4.1 `status.lastExit`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LastExit {
    /// `None` = killed by signal / no code reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    pub at_unix_ms: i64,
    /// e.g. "upgrade", "crash", "fenced".
    pub reason: String,
    #[serde(default)]
    pub epoch: Epoch,
}

/// Crash-loop bookkeeping for the backoff+jitter+quarantine policy
/// (docs/04 §4.4).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CrashStatus {
    /// Unix-ms timestamps of recent crash exits (inside the budget window).
    #[serde(default)]
    pub recent_unix_ms: Vec<i64>,
    /// Don't start the successor before this time (backoff + jitter).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at_unix_ms: Option<i64>,
    /// Restart attempt count driving the exponential backoff.
    #[serde(default)]
    pub attempts: u32,
}

/// A one-shot request recorded on status by the API for the controller to
/// consume (restart, restore, re-arm after NeedsHuman).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PendingRequest {
    pub kind: PendingRequestKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<SnapshotId>,
    pub requested_at_unix_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PendingRequestKind {
    /// Manual restart via drain+activate handoff.
    Restart,
    /// Restore the given snapshot (or latest when absent).
    RestoreSnapshot,
    /// New credentials/dosid written to Vault; leave NeedsHuman and re-arm.
    Rearm,
}

/// status — observed state, written only by the operator.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct XenomitBotStatus {
    #[serde(default)]
    pub phase: BotPhase,
    /// Current pod generation; incremented for every new pod (docs/03 §3.7).
    #[serde(default)]
    pub epoch: Epoch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_ref: Option<String>,
    /// Observed exit IP of the bot's proxy: the per-bot egress identity
    /// (docs/05 §5.4). Written by the ProxyController from probe results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_exit_ip: Option<String>,
    /// Last resolved proxy IP; the egress NetworkPolicy is rendered from it
    /// and re-rendered when it changes (ops/k8s/20-bot-egress.template.yaml).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_resolved_ip: Option<String>,
    /// Consecutive failed proxy probes for `mode = fixed` bots (pool proxies
    /// count on `Proxy.status.consecutiveFailures` instead).
    #[serde(default)]
    pub proxy_probe_failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_snapshot_id: Option<SnapshotId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit: Option<LastExit>,
    #[serde(default)]
    pub uptime_seconds: u64,
    #[serde(default)]
    pub conditions: Vec<BotCondition>,
    /// Set while `phase = needsHuman`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needs_human: Option<NeedsHumanKind>,
    /// In-flight planned handoff (docs/03 §3.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<HandoffState>,
    /// Crash bookkeeping for backoff/quarantine.
    #[serde(default)]
    pub crash: CrashStatus,
    /// Last configRevision the running pod applied; spec drift vs this
    /// drives hot-apply vs handoff (docs/04 §4.5).
    #[serde(default)]
    pub applied_config_revision: u64,
    /// Spec fingerprint of the running pod: image + client kind + proxy +
    /// account. Compared to spec to detect restart-requiring edits.
    #[serde(default)]
    pub applied_fingerprint: String,
    /// Snapshot to restore on the next activation; set by restore requests
    /// and cleared once used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activate_snapshot_id: Option<SnapshotId>,
    /// One-shot API request pending controller pickup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_request: Option<PendingRequest>,
    /// Set when the bot is soft-deleted (docs/12 §12.4). Credentials are
    /// already destroyed; snapshots/stats purge after 30 days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix_ms: Option<i64>,
    /// Set once the drain-on-delete has completed (pods gone).
    #[serde(default)]
    pub delete_pods_gone: bool,
    /// Restore quality/downtime reported by the successor on activation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_restore: Option<RestoreReport>,
}

/// What the successor reported after `Activate` (docs/08 §8.7 `restored`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreReport {
    pub snapshot_id: Option<SnapshotId>,
    /// clean | degraded | checkpoint | crash_recovered | fresh.
    pub quality: String,
    pub downtime_ms: u64,
}

impl XenomitBot {
    /// The tenant label (docs/04 §4.1 example).
    pub const TENANT_LABEL: &'static str = "xenomit.io/tenant";
    /// Pod labels (docs/04 §4.2 pod template).
    pub const BOT_LABEL: &'static str = "xenomit.io/bot";
    pub const EPOCH_LABEL: &'static str = "xenomit.io/epoch";

    /// Deterministic pod name for an epoch: `bot-…-e8`.
    pub fn pod_name(&self) -> String {
        let epoch = self.status.as_ref().map(|s| s.epoch).unwrap_or_default();
        pod_name(self.metadata.name.as_deref().unwrap_or_default(), epoch)
    }
}

/// Deterministic per-epoch pod name (`<bot>-e<epoch>`).
pub fn pod_name(bot_id: &str, epoch: Epoch) -> String {
    format!("{bot_id}-e{}", epoch.get())
}

/// Classify a spec edit (docs/04 §4.5): changes needing a restart trigger a
/// `config_requires_restart` handoff; everything else is hot-applicable via
/// `ApplyConfig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfigEditKind {
    /// Only hot-applicable settings changed.
    HotApply,
    /// clientKind / account / proxy / plugins changed — needs handoff.
    RequiresRestart,
}

/// Fingerprint of the restart-relevant spec fields. If this changes between
/// `applied_fingerprint` and spec, the edit `RequiresRestart`; if it doesn't
/// but `config_revision` moved, the edit is `HotApply`.
pub fn restart_fingerprint(spec: &XenomitBotSpec) -> String {
    let plugins = spec
        .config
        .settings
        .get("plugins")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    serde_json::to_string(&serde_json::json!({
        "account": spec.account,
        "clientKind": spec.config.client_kind,
        "proxy": spec.proxy,
        "version": spec.version,
        "plugins": plugins,
    }))
    .unwrap_or_default()
}

/// Decide how a spec edit reaches the bot (docs/04 §4.5).
pub fn classify_edit(spec: &XenomitBotSpec, applied_fingerprint: &str) -> ConfigEditKind {
    if restart_fingerprint(spec) != applied_fingerprint {
        ConfigEditKind::RequiresRestart
    } else {
        ConfigEditKind::HotApply
    }
}
