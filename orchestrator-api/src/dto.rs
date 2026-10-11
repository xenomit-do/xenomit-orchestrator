//! Request/response bodies for the REST API. Credential fields are
//! `ZeroizeOnDrop` and never appear in responses, logs or the repo
//! (docs/09 §9.1).

use fleet_types::crds::{PendingRequestKind, XenomitBot, XenomitBotSpec};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

/// `POST /api/v1/bots` body: the spec (credentials path is filled in by the
/// server) plus the account credentials to vault.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateBotRequest {
    /// Bot name (metadata.name); DNS-1123 validated server-side.
    pub name: String,
    pub spec: XenomitBotSpec,
    /// Account credentials → `secret/data/bots/<id>/account`.
    pub credentials: Credentials,
    /// Optional user-supplied proxy → `secret/data/bots/<id>/proxy`
    /// (only valid with `spec.proxy.mode = fixed`).
    #[serde(default)]
    pub proxy: Option<ProxyCredentials>,
    /// Admin-only quota override (docs/12 §12.3): when set and the caller
    /// is `admin`, a failing plan check is logged (audited via tracing)
    /// and the create proceeds. Ignored for non-admins.
    #[serde(default)]
    pub admin_override_reason: Option<String>,
}

/// `POST /api/v1/bots/{name}/credentials` body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsRequest {
    pub credentials: Credentials,
    /// Also rotate the proxy secret.
    #[serde(default)]
    pub proxy: Option<ProxyCredentials>,
    /// Re-arm the bot after writing (used to leave NeedsHuman).
    #[serde(default)]
    pub rearm: bool,
}

/// Credential material as accepted by the API. Serialized to Vault as the
/// same JSON shape; never echoed back.
#[derive(Debug, Deserialize, Zeroize, JsonSchema)]
#[zeroize(drop)]
#[serde(rename_all = "camelCase", untagged)]
pub enum Credentials {
    Password { username: String, password: String },
    Session { dosid: String },
}

impl Credentials {
    /// Encode for Vault KV storage (docs/09 §9.2a `account` secret shape).
    /// Callers [`Zeroize::zeroize`] the value after use.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Credentials::Password { username, password } => serde_json::json!({
                "username": username,
                "password": password,
            })
            .to_string()
            .into_bytes(),
            Credentials::Session { dosid } => serde_json::json!({
                "dosid": dosid,
            })
            .to_string()
            .into_bytes(),
        }
    }
}

/// Proxy credential material (`spec.proxy.mode = fixed`, docs/05 §5.4).
#[derive(Debug, Deserialize, Zeroize, JsonSchema)]
#[zeroize(drop)]
#[serde(rename_all = "camelCase")]
pub struct ProxyCredentials {
    /// host:port
    pub addr: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

impl ProxyCredentials {
    /// Callers [`Zeroize::zeroize`] the value after use.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::json!({
            "addr": self.addr,
            "username": self.username,
            "password": self.password,
        })
        .to_string()
        .into_bytes()
    }
}

/// `POST /api/v1/bots/{name}/desired` / action endpoints share this body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DesiredStateRequest {
    pub desired_state: fleet_types::phase::DesiredState,
}

/// `POST /api/v1/bots/{name}/restore` body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreRequest {
    /// Snapshot to restore; omit for the latest.
    #[serde(default)]
    pub snapshot_id: Option<fleet_types::snapshot::SnapshotId>,
}

/// `POST /api/v1/bots/{name}/request` body (restart / rearm).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingRequestBody {
    pub kind: PendingRequestKind,
    #[serde(default)]
    pub snapshot_id: Option<fleet_types::snapshot::SnapshotId>,
}

/// `PATCH /api/v1/bots/{name}` body — spec edit.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchBotRequest {
    #[serde(default)]
    pub config: Option<fleet_types::crds::BotConfigSpec>,
    #[serde(default)]
    pub version: Option<fleet_types::crds::VersionSpec>,
    #[serde(default)]
    pub proxy: Option<fleet_types::crds::ProxyPolicySpec>,
    #[serde(default)]
    pub restore_policy: Option<fleet_types::crds::RestorePolicy>,
    /// Bump to signal the edit; the controller classifies hot-apply vs
    /// handoff (docs/04 §4.5).
    pub config_revision: u64,
    /// Admin-only quota override (docs/12 §12.3) — same semantics as on
    /// `CreateBotRequest`.
    #[serde(default)]
    pub admin_override_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Proxy pool admin (docs/05 §5.4, §5.6)
// ---------------------------------------------------------------------------

/// `POST /api/v1/proxies` + `PUT /api/v1/proxies/{name}` body: the `Proxy`
/// spec (Vault path is rewritten server-side) plus optional credentials.
/// Credential values go straight to Vault — never onto the CR, a response,
/// or a log line (docs/05 §5.6, docs/09 §9.1).
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpsertProxyRequest {
    /// `metadata.name` on POST; ignored on PUT (path wins).
    #[serde(default)]
    pub name: String,
    pub spec: fleet_types::crds::ProxySpec,
    /// Credentials → `secret/data/proxies/<name>`; omitted keeps the
    /// existing entry (rotation is a deliberate act).
    #[serde(default)]
    pub credentials: Option<ProxySecret>,
}

/// Pool proxy credentials (docs/05 §5.6: `(scheme, host, port, user, pass)`
/// is what botd reads at `Arm`; the network tuple comes from the spec).
#[derive(Debug, Deserialize, Zeroize, JsonSchema)]
#[zeroize(drop)]
#[serde(rename_all = "camelCase")]
pub struct ProxySecret {
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

impl ProxySecret {
    /// Encode for Vault KV storage; callers [`Zeroize::zeroize`] the
    /// request after use. The network fields are copied from the spec so
    /// the stored entry is self-contained.
    pub fn encode(&self, spec: &fleet_types::crds::ProxySpec) -> Vec<u8> {
        serde_json::json!({
            "scheme": spec.scheme,
            "host": spec.host,
            "port": spec.port,
            "username": self.username,
            "password": self.password,
        })
        .to_string()
        .into_bytes()
    }
}

/// `GET /api/v1/proxies/pool` response: fleet-wide pool health/capacity.
#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolStatus {
    pub total: usize,
    pub healthy: usize,
    /// Sum of `spec.capacity`.
    pub capacity: u64,
    /// Sum of `status.assigned` lengths.
    pub assigned: usize,
    pub proxies: Vec<PoolProxyEntry>,
}

/// One proxy's pool view — everything on the CR, no credentials
/// (the CR never carries them; docs/05 §5.6).
#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolProxyEntry {
    pub name: String,
    pub scheme: fleet_types::crds::ProxyScheme,
    pub host: String,
    pub port: u16,
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    pub capacity: u32,
    pub assigned: Vec<String>,
    pub healthy: bool,
    pub consecutive_failures: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_probe_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_cooldown_until_unix_ms: Option<i64>,
    pub resolved_ips: Vec<String>,
}

impl PoolProxyEntry {
    pub fn of(p: &fleet_types::crds::Proxy) -> Self {
        let st = p.status.clone().unwrap_or_default();
        PoolProxyEntry {
            name: p.metadata.name.clone().unwrap_or_default(),
            scheme: p.spec.scheme,
            host: p.spec.host.clone(),
            port: p.spec.port,
            tags: p.spec.tags.clone(),
            country: p.spec.country.clone(),
            capacity: p.spec.capacity,
            assigned: st.assigned,
            healthy: st.healthy,
            consecutive_failures: st.consecutive_failures,
            exit_ip: st.exit_ip,
            latency_ms: st.latency_ms,
            last_probe_at_unix_ms: st.last_probe_at_unix_ms,
            last_tenant: st.last_tenant,
            tenant_cooldown_until_unix_ms: st.tenant_cooldown_until_unix_ms,
            resolved_ips: st.resolved_ips,
        }
    }
}

/// `POST /api/v1/rollouts` body (docs/07 §7.3): name + the `BotRollout`
/// spec. Server-side validation mirrors the CRD schema plus the fleet
/// rules (digest-shaped image, parseable `maxDraining`, no active
/// rollout).
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateRolloutRequest {
    pub name: String,
    pub spec: fleet_types::crds::BotRolloutSpec,
}

/// `POST /api/v1/rollouts/{name}/{action}` — pause / resume / abort /
/// rollback have no body; the action is in the path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolloutAction {
    Pause,
    Resume,
    Abort,
    Rollback,
}

impl RolloutAction {
    pub fn as_str(self) -> &'static str {
        match self {
            RolloutAction::Pause => "pause",
            RolloutAction::Resume => "resume",
            RolloutAction::Abort => "abort",
            RolloutAction::Rollback => "rollback",
        }
    }
}

/// The bot JSON the API returns (spec + status, never credentials).
pub fn bot_view(bot: &XenomitBot) -> serde_json::Value {
    serde_json::to_value(bot).unwrap_or_default()
}

/// `GET /api/v1/fleet` summary counts.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetSummary {
    pub bots_total: usize,
    pub running: usize,
    pub draining: usize,
    pub needs_human: usize,
    pub quarantined: usize,
    pub stopped_or_paused: usize,
    pub soft_deleted: usize,
}
