//! The panel contract, mounted at `/v1` (docs/04 §4.6, docs/08 §8.6).
//!
//! `xenomit-web-panel/shared/orchestrator.ts` is the typed public surface
//! the panel — and any third-party client — speaks: flat DTOs, `bot-*` ids,
//! PascalCase phases, `{bots|snapshots|proxies|rollouts}` envelopes and an
//! admin SSE stream with numeric ids + `Last-Event-ID` replay. The
//! k8s-shaped `/api/v1` surface stays for operators; this module translates
//! between the two on top of the same [`Fleet`] service — no second truth.
//!
//! Panel pairing (docs/08 §8.6): on `POST /v1/bots` the orchestrator calls
//! `POST {panel}/api/orchestrator/instances` with the service bearer and
//! writes the returned agent token into the bot's Vault `instance` path —
//! in that order, so the token exists before any reconcile can arm botd.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use zeroize::Zeroize;

use fleet_types::crds::{PendingRequestKind, ProxyMode, XenomitBot};
use fleet_types::phase::{BotPhase, DesiredState};
use fleet_types::snapshot::{SnapshotId, SnapshotQuality};

use crate::auth::{ApiError, Claims};
use crate::dto;
use crate::events::FleetEvent;
use crate::repo::BotRepo;
use crate::routes::AppState;

/// How the orchestrator reaches the panel's service endpoint (docs/08 §8.6).
/// `None` = standalone API: bots still run, they just never bind a panel
/// instance (botd's `panel.base_url` stays unset too).
pub struct PanelLink {
    /// e.g. `http://localhost:3000` — the pairing call appends
    /// `/api/orchestrator/instances`.
    pub url: String,
    /// Shared bearer the panel's `verifyServiceAuth` checks
    /// (`ORCHESTRATOR_SERVICE_TOKEN`).
    pub service_token: String,
    pub http: reqwest::Client,
}

impl std::fmt::Debug for PanelLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PanelLink")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

/// Response shape of `POST /api/orchestrator/instances` (shared/orchestrator.ts).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ProvisionInstanceResponse {
    pub instance_id: String,
    pub token: String,
    pub agent_ws_url: String,
}

/// Ask the panel to mint `Instance(kind=managed)` + agent token for a bot.
/// The panel endpoint is idempotent on `bot_id` (re-bind rotates the token).
pub async fn provision_panel_instance(
    link: &PanelLink,
    bot_id: &str,
    owner: &str,
    server: &str,
    username: &str,
    desired_state: DesiredState,
) -> Result<ProvisionInstanceResponse, String> {
    let resp = link
        .http
        .post(format!("{}/api/orchestrator/instances", link.url))
        .bearer_auth(&link.service_token)
        .json(&json!({
            "bot_id": bot_id,
            "owner": owner,
            "server": server,
            "username": username,
            "desired_state": phase::desired_str(desired_state),
        }))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| format!("panel unreachable: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "panel provision {status}: {}",
            &body[..body.len().min(200)]
        ));
    }
    resp.json::<ProvisionInstanceResponse>()
        .await
        .map_err(|e| format!("panel provision malformed: {e}"))
}

// ---------------------------------------------------------------------------
// /v1 SSE event log — numeric ids + bounded replay (docs/04 §4.6).
// ---------------------------------------------------------------------------

/// One translated, panel-shaped lifecycle event.
#[derive(Clone, Debug, serde::Serialize)]
pub struct CompatEvent {
    pub id: u64,
    pub at: String,
    #[serde(rename = "botId")]
    pub bot_id: String,
    pub owner: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "desiredState")]
    pub desired_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Replay backlog + live fan-out. `REPLAY_CAP` bounds memory; clients that
/// lag past it resync via `GET /v1/bots` — the panel's reconnect path does
/// exactly that (src/server/orchestrator-events.ts).
const REPLAY_CAP: usize = 512;

pub struct CompatEvents {
    seq: AtomicU64,
    backlog: Mutex<VecDeque<CompatEvent>>,
    tx: tokio::sync::broadcast::Sender<CompatEvent>,
}

impl Default for CompatEvents {
    fn default() -> Self {
        Self::new()
    }
}

impl CompatEvents {
    pub fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(256);
        CompatEvents {
            seq: AtomicU64::new(0),
            backlog: Mutex::new(VecDeque::with_capacity(REPLAY_CAP)),
            tx,
        }
    }

    fn push(&self, mut ev: CompatEvent) {
        ev.id = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let mut backlog = self.backlog.lock().unwrap();
            if backlog.len() == REPLAY_CAP {
                backlog.pop_front();
            }
            backlog.push_back(ev.clone());
        }
        let _ = self.tx.send(ev);
    }

    fn replay_after(&self, last_id: u64) -> Vec<CompatEvent> {
        self.backlog
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.id > last_id)
            .cloned()
            .collect()
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<CompatEvent> {
        self.tx.subscribe()
    }
}

/// Translate a [`FleetEvent`] to a panel `BotEvent`. `owner`/`desiredState`
/// are looked up on the repo so the SSE payload is self-contained.
fn translate(ev: &FleetEvent, bot: Option<&XenomitBot>) -> Option<CompatEvent> {
    let bot_id = ev.bot_id()?.to_string();
    let (kind, phase, detail) = match ev {
        FleetEvent::PhaseChanged { phase, detail, .. } => (
            "phase",
            Some(phase_str(*phase).to_string()),
            Some(json!({ "detail": detail })),
        ),
        FleetEvent::SpecChanged { .. } => ("desired_state", None, None),
        FleetEvent::Deleted { .. } => ("deleted", None, None),
        FleetEvent::Undeleted { .. } => ("created", None, None),
        FleetEvent::Drained {
            snapshot_id, clean, ..
        } => (
            "phase",
            Some("Draining".into()),
            Some(json!({ "drained": true, "clean": clean, "snapshot_id": snapshot_id })),
        ),
        FleetEvent::SnapshotCommitted { snapshot_id, .. } => (
            "snapshot",
            None,
            Some(json!({ "snapshot_id": snapshot_id })),
        ),
        FleetEvent::SessionOpened { .. } => ("phase", None, Some(json!({ "session": "opened" }))),
        FleetEvent::SessionClosed { reason, .. } => {
            ("exit", None, Some(json!({ "reason": reason })))
        }
        FleetEvent::Fenced { expected, got, .. } => (
            "exit",
            None,
            Some(json!({ "fenced": true, "expected": expected.get(), "got": got.get() })),
        ),
        FleetEvent::MetricAlert {
            name,
            severity,
            message,
            ..
        } => (
            "phase",
            None,
            Some(json!({ "metric": name, "severity": severity, "message": message })),
        ),
        FleetEvent::RolloutBot {
            rollout,
            ok,
            detail,
            ..
        } => (
            "phase",
            None,
            Some(json!({ "rollout": rollout, "ok": ok, "detail": detail })),
        ),
        FleetEvent::RolloutSpecChanged { .. }
        | FleetEvent::RolloutPhaseChanged { .. }
        | FleetEvent::RolloutGate { .. }
        | FleetEvent::RolloutWave { .. } => return None,
    };
    let epoch = match ev {
        FleetEvent::SessionOpened { epoch, .. }
        | FleetEvent::SessionClosed { epoch, .. }
        | FleetEvent::PhaseChanged { epoch, .. }
        | FleetEvent::Drained { epoch, .. } => Some(epoch.get()),
        _ => bot.and_then(|b| b.status.as_ref().map(|s| s.epoch.get())),
    };
    Some(CompatEvent {
        id: 0, // stamped by `push`
        at: chrono::Utc::now().to_rfc3339(),
        bot_id,
        owner: bot.map(|b| b.spec.owner.clone()).unwrap_or_default(),
        kind,
        phase,
        desired_state: bot.map(|b| phase::desired_str(b.spec.desired_state).to_string()),
        detail,
        epoch,
        version: bot.and_then(version_of),
    })
}

/// Fold the `Fleet` broadcast into the `/v1` event log. Spawn once at
/// startup alongside `events::bridge_gateway_events`:
///
/// ```ignore
/// tokio::spawn(orchestrator_api::compat::bridge_compat_events(
///     fleet.events.subscribe(), bots, compat_events));
/// ```
pub async fn bridge_compat_events(
    mut rx: tokio::sync::broadcast::Receiver<FleetEvent>,
    bots: Arc<dyn BotRepo>,
    sink: Arc<CompatEvents>,
) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match rx.recv().await {
            Ok(ev) => {
                let bot = match ev.bot_id() {
                    Some(id) => bots.get(id).await,
                    None => None,
                };
                if let Some(out) = translate(&ev, bot.as_ref()) {
                    sink.push(out);
                }
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => return,
        }
    }
}

// ---------------------------------------------------------------------------
// Phase/version naming — the API serializes camelCase; the panel contract is
// PascalCase ("Running", "NeedsHuman", …) with forward-compat passthrough.
// ---------------------------------------------------------------------------

pub mod phase {
    use super::DesiredState;

    pub fn desired_str(d: DesiredState) -> &'static str {
        match d {
            DesiredState::Running => "Running",
            DesiredState::Paused => "Paused",
            DesiredState::Stopped => "Stopped",
        }
    }

    pub fn parse_desired(s: &str) -> Option<DesiredState> {
        match s.to_ascii_lowercase().as_str() {
            "running" => Some(DesiredState::Running),
            "paused" => Some(DesiredState::Paused),
            "stopped" => Some(DesiredState::Stopped),
            _ => None,
        }
    }
}

fn phase_str(p: BotPhase) -> &'static str {
    match p {
        BotPhase::Pending => "Pending",
        BotPhase::Provisioning => "Provisioning",
        BotPhase::Armed => "Armed",
        BotPhase::Starting => "Starting",
        BotPhase::Running => "Running",
        BotPhase::Draining => "Draining",
        BotPhase::Snapshotting => "Snapshotting",
        BotPhase::Terminated => "Terminated",
        BotPhase::Paused => "Paused",
        BotPhase::Stopped => "Stopped",
        BotPhase::NeedsHuman => "NeedsHuman",
        BotPhase::NeedsUpdate => "NeedsUpdate",
        BotPhase::Failed => "Failed",
        BotPhase::Quarantined => "Quarantined",
        BotPhase::InvalidConfig => "InvalidConfig",
    }
}

fn version_of(bot: &XenomitBot) -> Option<String> {
    match bot.spec.version.channel {
        fleet_types::crds::VersionChannel::Pinned => bot.spec.version.pinned_image.clone(),
        other => Some(
            match other {
                fleet_types::crds::VersionChannel::Stable => "stable",
                fleet_types::crds::VersionChannel::Canary => "canary",
                fleet_types::crds::VersionChannel::Pinned => unreachable!(),
            }
            .to_string(),
        ),
    }
}

fn rfc3339(unix_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(unix_ms)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}

fn created_at(bot: &XenomitBot) -> String {
    bot.metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_string())
        .unwrap_or_default()
}

fn last_exit(st: &fleet_types::crds::XenomitBotStatus) -> Option<Value> {
    st.last_exit.as_ref().map(|e| {
        json!({
            "code": e.code,
            "at": rfc3339(e.at_unix_ms),
            "reason": e.reason,
            "epoch": e.epoch.get(),
        })
    })
}

/// `GET /v1/bots/{id}` payload (`BotStatus`).
fn bot_status(bot: &XenomitBot) -> Value {
    let st = bot.status.clone().unwrap_or_default();
    json!({
        "id": bot.metadata.name,
        "owner": bot.spec.owner,
        "name": bot.metadata.name,
        "desiredState": phase::desired_str(bot.spec.desired_state),
        "phase": phase_str(st.phase),
        "phaseDetail": st.needs_human.map(|k| json!({ "kind": k.as_str() })),
        "epoch": st.epoch.get(),
        "podName": st.pod_name,
        "node": null,
        "image": st.image,
        "version": version_of(bot),
        "proxyRef": st.proxy_ref,
        "proxyExitIp": st.proxy_exit_ip,
        "account": {
            "server": bot.spec.account.server,
            "authMode": match bot.spec.account.auth_mode {
                fleet_types::crds::AuthMode::Password => "password",
                fleet_types::crds::AuthMode::Session => "session",
            },
        },
        "configRevision": bot.spec.config_revision,
        "uptimeSeconds": st.uptime_seconds,
        "lastExit": last_exit(&st),
        "lastSnapshotId": st.last_snapshot_id.map(|s| s.0),
        "conditions": st.conditions.iter().map(|c| json!({
            "type": c.type_,
            "status": matches!(c.status, fleet_types::phase::ConditionStatus::True),
            "detail": c.reason,
            "since": c.last_transition_unix_ms.map(rfc3339),
        })).collect::<Vec<_>>(),
        "createdAt": created_at(bot),
    })
}

/// `GET /v1/bots` list entry (`BotSummary`).
fn bot_summary(bot: &XenomitBot) -> Value {
    let st = bot.status.clone().unwrap_or_default();
    json!({
        "id": bot.metadata.name,
        "owner": bot.spec.owner,
        "name": bot.metadata.name,
        "desiredState": phase::desired_str(bot.spec.desired_state),
        "phase": phase_str(st.phase),
        "version": version_of(bot),
        "node": null,
        "proxyRef": st.proxy_ref,
        "uptimeSeconds": st.uptime_seconds,
        "createdAt": created_at(bot),
    })
}

fn proxy_view(v: &Value) -> Value {
    let name = v.pointer("/metadata/name").cloned().unwrap_or(Value::Null);
    let spec = &v["spec"];
    let st = &v["status"];
    let assigned = st
        .get("assigned")
        .and_then(|a| a.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    json!({
        "id": name,
        "scheme": spec.get("scheme").cloned().unwrap_or(json!("http")),
        "host": spec.get("host").cloned().unwrap_or(Value::Null),
        "port": spec.get("port").cloned().unwrap_or(Value::Null),
        "country": spec.get("country").cloned().unwrap_or(Value::Null),
        "tags": spec.get("tags").cloned().unwrap_or(json!([])),
        "capacity": spec.get("capacity").cloned().unwrap_or(json!(1)),
        "assigned": assigned,
        "healthy": st.get("healthy").cloned().unwrap_or(json!(false)),
        "exitIp": st.get("exitIp").cloned().unwrap_or(Value::Null),
        "createdAt": v
            .pointer("/metadata/creationTimestamp")
            .cloned()
            .unwrap_or(Value::Null),
    })
}

fn rollout_view(v: &Value) -> Value {
    let spec = &v["spec"];
    let st = &v["status"];
    let phase = st
        .get("phase")
        .and_then(|p| p.as_str())
        .unwrap_or("pending");
    let status = match phase {
        "done" => "done",
        "aborted" => "aborted",
        "rolledBack" | "rolled_back" => "rolled_back",
        "paused" => "paused",
        _ => "running",
    };
    let failed = st
        .get("bots")
        .and_then(|b| b.as_array())
        .map(|bots| {
            bots.iter()
                .filter(|b| {
                    matches!(
                        b.get("state").and_then(|s| s.as_str()),
                        Some("failed") | Some("rolled_back") | Some("rolledBack")
                    )
                })
                .count()
        })
        .unwrap_or(0);
    json!({
        "id": v.pointer("/metadata/name").cloned().unwrap_or(Value::Null),
        "image": spec.get("targetImage").cloned().unwrap_or(Value::Null),
        "selector": {
            "versions": spec.pointer("/selector/channel").cloned().map(|c| json!([c])),
            "phases": Value::Null,
        },
        "canary": spec.pointer("/strategy/canary/bots").cloned().unwrap_or(json!([])),
        "wave": {
            "size": spec.pointer("/strategy/waves/maxDraining").cloned().unwrap_or(Value::Null),
            "waitSeconds": spec.pointer("/strategy/waves/interWaveDelayS").cloned().unwrap_or(Value::Null),
        },
        "status": status,
        "progress": {
            "total": st.get("total").cloned().unwrap_or(json!(0)),
            "done": st.get("done").cloned().unwrap_or(json!(0)),
            "failed": failed,
            "currentWave": st.get("wave").cloned().unwrap_or(json!(0)),
        },
        "createdBy": v
            .pointer("/metadata/annotations/xenomit.io~1created-by")
            .cloned()
            .unwrap_or(Value::Null),
        "createdAt": v
            .pointer("/metadata/creationTimestamp")
            .cloned()
            .unwrap_or(Value::Null),
    })
}

// ---------------------------------------------------------------------------
// Request bodies (panel shapes)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateBot {
    owner: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    desired_state: Option<String>,
    account: CreateAccount,
    credentials: CreateCredentials,
    config: CreateConfig,
    #[serde(default)]
    version: Option<CreateVersion>,
    #[serde(default)]
    proxy: Option<CreateProxy>,
    #[serde(default)]
    restore_policy: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAccount {
    server: String,
    #[serde(default = "password_auth")]
    auth_mode: String,
}
fn password_auth() -> String {
    "password".into()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
enum CreateCredentials {
    Password { username: String, password: String },
    Session { dosid: String },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateConfig {
    // `clientKind` accepted but ignored — the fleet runs packet clients only.
    #[serde(default)]
    settings: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateVersion {
    #[serde(default = "stable_channel")]
    channel: String,
    #[serde(default)]
    pinned_image: Option<String>,
}
fn stable_channel() -> String {
    "stable".into()
}

/// `proxy` in `CreateBotRequest`: `{mode:"pool", selector?}` or
/// `{mode:"fixed", fixed:{scheme,host,port,username?,password?}}`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateProxy {
    mode: String,
    #[serde(default)]
    selector: Option<CreateProxySelector>,
    #[serde(default)]
    fixed: Option<CreateFixedProxy>,
}

#[derive(Debug, Deserialize)]
struct CreateProxySelector {
    #[serde(default)]
    country: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Deserialize, Zeroize)]
#[zeroize(drop)]
struct CreateFixedProxy {
    scheme: Option<String>,
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchBot {
    #[serde(default)]
    desired_state: Option<String>,
    #[serde(default)]
    config: Option<CreateConfig>,
    #[serde(default)]
    version: Option<CreateVersion>,
    #[serde(default)]
    proxy: Option<PatchProxy>,
}

/// `PATCH` proxy: pool/fixed policy fields (credentials are not patchable
/// here — they rotate via `/credentials`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchProxy {
    mode: String,
    #[serde(default)]
    selector: Option<CreateProxySelector>,
    #[serde(default)]
    fixed_addr: Option<String>,
    #[serde(default)]
    fixed_vault_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DosidBody {
    dosid: String,
}

#[derive(Debug, Deserialize)]
struct CredentialsBody {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct RestoreBody {
    snapshot_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateProxyBody {
    scheme: String,
    host: String,
    port: u16,
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    capacity: Option<u32>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateRolloutBody {
    image: String,
    #[serde(default)]
    selector: Option<RolloutSelectorBody>,
    #[serde(default)]
    canary: Option<Vec<String>>,
    #[serde(default)]
    wave: Option<WaveBody>,
}

#[derive(Debug, Deserialize)]
struct RolloutSelectorBody {
    #[serde(default)]
    phases: Vec<String>,
    #[serde(default)]
    versions: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WaveBody {
    #[serde(default)]
    size: Option<Value>,
    #[serde(default)]
    wait_seconds: Option<u32>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// `bot-<rand>` — the panel contract generates ids server-side when the
/// caller doesn't pin `name`.
fn gen_bot_name() -> String {
    format!("bot-{:08x}", rand::random::<u32>())
}

fn version_spec(v: &CreateVersion) -> fleet_types::crds::VersionSpec {
    fleet_types::crds::VersionSpec {
        channel: match v.channel.as_str() {
            "canary" => fleet_types::crds::VersionChannel::Canary,
            "pinned" => fleet_types::crds::VersionChannel::Pinned,
            _ => fleet_types::crds::VersionChannel::Stable,
        },
        pinned_image: v.pinned_image.clone(),
    }
}

fn proxy_policy(
    mode: &str,
    selector: Option<&CreateProxySelector>,
) -> Result<fleet_types::crds::ProxyPolicySpec, ApiError> {
    Ok(fleet_types::crds::ProxyPolicySpec {
        mode: match mode {
            "pool" => ProxyMode::Pool,
            "fixed" => ProxyMode::Fixed,
            other => {
                return Err(ApiError::bad_request(format!(
                    "proxy.mode must be pool|fixed (got {other})"
                )));
            }
        },
        selector: selector.map(|s| fleet_types::crds::ProxySelector {
            country: s.country.clone(),
            tags: s.tags.clone(),
        }),
        fixed_vault_path: None,
        fixed_addr: None,
    })
}

fn require_admin(claims: &Claims, what: &str) -> Result<(), ApiError> {
    if claims.is_admin() {
        Ok(())
    } else {
        Err(ApiError::forbidden(format!("{what} is admin-only")))
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// `/v1` surface: same JWT middleware as `/api/v1`; only the DTOs differ.
/// Returned as `Router<Arc<AppState>>` so the outer router can `.merge` it
/// before the single `with_state` call.
pub fn router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route("/bots", get(list_bots).post(create_bot))
        .route(
            "/bots/{id}",
            get(get_bot).patch(patch_bot).delete(delete_bot),
        )
        .route("/bots/{id}/restart", post(restart_bot))
        .route("/bots/{id}/drain", post(drain_bot))
        .route("/bots/{id}/dosid", post(submit_dosid))
        .route("/bots/{id}/credentials", post(submit_credentials))
        .route("/bots/{id}/snapshots", get(list_snapshots))
        .route("/bots/{id}/restore", post(restore_bot))
        .route("/events", get(events))
        .route("/fleet/summary", get(fleet_summary))
        .route("/proxies", get(list_proxies).post(create_proxy))
        .route("/proxies/{id}", get(get_proxy).delete(delete_proxy))
        .route("/rollouts", get(list_rollouts).post(create_rollout))
        .route("/rollouts/{id}", get(get_rollout))
        .route("/rollouts/{id}/{action}", post(rollout_action))
        .layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            crate::auth::require_jwt,
        ))
}

// -- bots --------------------------------------------------------------------

async fn list_bots(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let owner = q.get("owner").map(|s| s.as_str());
    let bots = st
        .fleet
        .list(&claims)
        .await
        .into_iter()
        .filter(|b| owner.is_none_or(|o| b.spec.owner == o))
        .collect::<Vec<_>>();
    Json(json!({ "bots": bots.iter().map(bot_summary).collect::<Vec<_>>() }))
}

async fn create_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(mut req): Json<CreateBot>,
) -> Result<impl IntoResponse, ApiError> {
    if req.account.server.trim().is_empty() {
        return Err(ApiError::bad_request("account.server required"));
    }
    if !claims.is_admin() && req.owner != claims.sub {
        return Err(ApiError::forbidden("owner must equal sub"));
    }
    let name = req
        .name
        .take()
        .filter(|n| !n.is_empty())
        .unwrap_or_else(gen_bot_name);

    let mut proxy_policy = proxy_policy(
        &req.proxy
            .as_ref()
            .map(|p| p.mode.clone())
            .unwrap_or_else(|| "pool".into()),
        req.proxy.as_ref().and_then(|p| p.selector.as_ref()),
    )?;
    let mut proxy_creds: Option<dto::ProxyCredentials> = None;
    if let Some(p) = req.proxy.as_mut() {
        if let Some(mut fixed) = p.fixed.take() {
            proxy_policy.mode = ProxyMode::Fixed;
            proxy_policy.fixed_addr = Some(format!("{}:{}", fixed.host, fixed.port));
            proxy_creds = Some(dto::ProxyCredentials {
                addr: format!("{}:{}", fixed.host, fixed.port),
                username: fixed.username.take(),
                password: fixed.password.take(),
            });
            fixed.zeroize();
        }
    }

    let credentials = match req.credentials {
        CreateCredentials::Password { username, password } => {
            dto::Credentials::Password { username, password }
        }
        CreateCredentials::Session { dosid } => dto::Credentials::Session { dosid },
    };

    // Panel pairing first (docs/08 §8.6): the instance token must sit in
    // the bot's Vault `instance` path *before* `create` can reconcile →
    // spawn → arm botd. Failure is non-fatal: the bot runs panel-less.
    let desired = req
        .desired_state
        .as_deref()
        .and_then(phase::parse_desired)
        .unwrap_or_default();
    let mut panel_token: Option<String> = None;
    if let Some(link) = st.panel.as_ref() {
        match provision_panel_instance(link, &name, &req.owner, &req.account.server, &name, desired)
            .await
        {
            Ok(resp) => {
                panel_token = Some(resp.token);
            }
            Err(e) => {
                tracing::warn!(bot = %name, error = %e, "panel provision failed; bot continues without a panel link");
            }
        }
    }
    if let Some(token) = panel_token {
        let paths = operator::traits::bot_secret_paths(&name);
        let payload = json!({ "token": token }).to_string();
        if let Err(e) = st
            .fleet
            .secrets
            .write(
                &paths.instance_token_path,
                operator::traits::SecretValue::new(payload),
            )
            .await
        {
            tracing::warn!(bot = %name, error = %e, "panel instance token write failed");
        }
    }

    let bot = st
        .fleet
        .create(
            &claims,
            dto::CreateBotRequest {
                name: name.clone(),
                spec: fleet_types::crds::XenomitBotSpec {
                    owner: req.owner.clone(),
                    desired_state: desired,
                    account: fleet_types::crds::AccountSpec {
                        server: req.account.server.clone(),
                        auth_mode: match req.account.auth_mode.as_str() {
                            "session" => fleet_types::crds::AuthMode::Session,
                            _ => fleet_types::crds::AuthMode::Password,
                        },
                        // Filled in by the API (provisioned Vault path).
                        credentials_vault_path: String::new(),
                    },
                    config: fleet_types::crds::BotConfigSpec {
                        client_kind: fleet_types::crds::ClientKind::Packet,
                        settings: req.config.settings.clone(),
                        handoff: None,
                    },
                    config_revision: 0,
                    version: req.version.as_ref().map(version_spec).unwrap_or_default(),
                    proxy: proxy_policy,
                    restore_policy: match req.restore_policy.as_deref() {
                        Some("strict") => fleet_types::crds::RestorePolicy::Strict,
                        _ => fleet_types::crds::RestorePolicy::BestEffort,
                    },
                    resources: None,
                },
                credentials,
                proxy: proxy_creds,
                admin_override_reason: None,
            },
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": bot.metadata.name })),
    ))
}

async fn get_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bot = st.fleet.get(&claims, &id).await?;
    Ok(Json(bot_status(&bot)))
}

async fn patch_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(req): Json<PatchBot>,
) -> Result<impl IntoResponse, ApiError> {
    if let Some(d) = req.desired_state.as_deref() {
        let desired = phase::parse_desired(d)
            .ok_or_else(|| ApiError::bad_request("desiredState must be Running|Paused|Stopped"))?;
        st.fleet.set_desired(&claims, &id, desired).await?;
    }
    if req.config.is_some() || req.version.is_some() || req.proxy.is_some() {
        let proxy = match &req.proxy {
            Some(p) => Some(proxy_policy(&p.mode, p.selector.as_ref()).map(|mut pp| {
                pp.fixed_addr = p.fixed_addr.clone();
                pp.fixed_vault_path = p.fixed_vault_path.clone();
                pp
            })?),
            None => None,
        };
        let current = st.fleet.get(&claims, &id).await?;
        st.fleet
            .patch(
                &claims,
                &id,
                dto::PatchBotRequest {
                    config: req.config.map(|c| fleet_types::crds::BotConfigSpec {
                        client_kind: current.spec.config.client_kind,
                        settings: c.settings,
                        handoff: current.spec.config.handoff,
                    }),
                    version: req.version.as_ref().map(version_spec),
                    proxy,
                    restore_policy: None,
                    config_revision: current.spec.config_revision + 1,
                    admin_override_reason: None,
                },
            )
            .await?;
    }
    let bot = st.fleet.get(&claims, &id).await?;
    Ok(Json(bot_status(&bot)))
}

async fn delete_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    st.fleet.delete(&claims, &id).await?;
    Ok(Json(json!({ "ok": true })))
}

async fn restart_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bot = st.fleet.restart(&claims, &id).await?;
    Ok(Json(bot_status(&bot)))
}

/// Graceful drain + re-arm: the panel's "drain" is the planned-handoff
/// (`Restart` pending request) — drain at a safe point, snapshot, successor
/// restores (docs/03 §3.2).
async fn drain_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bot = st
        .fleet
        .request(&claims, &id, PendingRequestKind::Restart, None)
        .await?;
    Ok(Json(bot_status(&bot)))
}

/// Fresh dosid for `NeedsHuman` — credential write + re-arm in one call,
/// matching the mock orchestrator's accept-or-409 contract: rearm only
/// makes sense while parked; elsewhere the write still lands.
async fn submit_dosid(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(req): Json<DosidBody>,
) -> Result<impl IntoResponse, ApiError> {
    if req.dosid.trim().is_empty() {
        return Err(ApiError::bad_request("dosid required"));
    }
    let bot = st.fleet.get(&claims, &id).await?;
    let needs_human = matches!(
        bot.status.as_ref().map(|s| s.phase),
        Some(BotPhase::NeedsHuman)
    );
    if !needs_human {
        return Err(ApiError::conflict("dosid only accepted while NeedsHuman"));
    }
    let bot = st
        .fleet
        .write_credentials(
            &claims,
            &id,
            dto::CredentialsRequest {
                credentials: dto::Credentials::Session { dosid: req.dosid },
                proxy: None,
                rearm: true,
            },
        )
        .await?;
    Ok(Json(bot_status(&bot)))
}

async fn submit_credentials(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(req): Json<CredentialsBody>,
) -> Result<impl IntoResponse, ApiError> {
    let bot = st.fleet.get(&claims, &id).await?;
    let needs_human = matches!(
        bot.status.as_ref().map(|s| s.phase),
        Some(BotPhase::NeedsHuman)
    );
    if !needs_human {
        return Err(ApiError::conflict(
            "credentials only accepted while NeedsHuman",
        ));
    }
    let bot = st
        .fleet
        .write_credentials(
            &claims,
            &id,
            dto::CredentialsRequest {
                credentials: dto::Credentials::Password {
                    username: req.username,
                    password: req.password,
                },
                proxy: None,
                rearm: true,
            },
        )
        .await?;
    Ok(Json(bot_status(&bot)))
}

async fn list_snapshots(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let snaps = st.fleet.snapshots(&claims, &id).await?;
    Ok(Json(json!({
        "snapshots": snaps.iter().map(|s| json!({
            "id": s.id.0,
            "at": rfc3339(s.taken_at_unix_ms),
            "kind": match s.quality {
                SnapshotQuality::Clean => "clean",
                SnapshotQuality::Degraded => "degraded",
                SnapshotQuality::Checkpoint => "checkpoint",
            },
            "sizeBytes": s.size_bytes,
        })).collect::<Vec<_>>()
    })))
}

async fn restore_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(req): Json<RestoreBody>,
) -> Result<impl IntoResponse, ApiError> {
    let bot = st
        .fleet
        .restore(&claims, &id, req.snapshot_id.map(SnapshotId))
        .await?;
    Ok(Json(bot_status(&bot)))
}

// -- events (SSE, admin) ------------------------------------------------------

/// `GET /v1/events` — admin-only fleet stream. `Last-Event-ID` replays the
/// bounded backlog, then the live tail follows.
async fn events(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    headers: axum::http::HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    require_admin(&claims, "events")?;
    let last_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let sink = st.compat_events.clone();
    let replay = sink.replay_after(last_id);
    let rx = sink.subscribe();
    let live = BroadcastStream::new(rx).filter_map(|res| match res {
        Ok(ev) => Some(ev),
        Err(_) => None,
    });
    let stream = tokio_stream::iter(replay).chain(live).map(|ev| {
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Ok(Event::default().id(ev.id.to_string()).data(data))
    });
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

// -- fleet / proxies / rollouts (admin) --------------------------------------

async fn fleet_summary(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "fleet summary")?;
    let bots = st.fleet.bots.list(None).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let mut by_phase: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut by_version: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut total = 0usize;
    let mut needs_human = 0usize;
    let mut offline_seconds = 0u64;
    for b in &bots {
        let Some(s) = &b.status else { continue };
        if s.deleted_at_unix_ms.is_some() {
            continue;
        }
        total += 1;
        let phase = phase_str(s.phase);
        *by_phase.entry(phase.to_string()).or_insert(json!(0)) =
            json!(by_phase.get(phase).and_then(|v| v.as_u64()).unwrap_or(0) + 1);
        let ver = version_of(b).unwrap_or_else(|| "unknown".into());
        *by_version.entry(ver.clone()).or_insert(json!(0)) =
            json!(by_version.get(&ver).and_then(|v| v.as_u64()).unwrap_or(0) + 1);
        if s.phase == BotPhase::NeedsHuman {
            needs_human += 1;
        }
        if s.phase != BotPhase::Running
            && let Some(e) = &s.last_exit
        {
            offline_seconds += (now - e.at_unix_ms).max(0) as u64 / 1000;
        }
    }
    Ok(Json(json!({
        "total": total,
        "byPhase": Value::Object(by_phase),
        "byVersion": Value::Object(by_version),
        "offlineSeconds": offline_seconds,
        "needsHuman": needs_human,
        "needsUpdate": 0,
    })))
}

async fn list_proxies(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "proxies")?;
    let items = st.fleet.proxies.list().await;
    Ok(Json(json!({
        "proxies": items.iter().map(proxy_view).collect::<Vec<_>>()
    })))
}

async fn get_proxy(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "proxies")?;
    let v = st
        .fleet
        .proxies
        .get(&id)
        .await
        .ok_or_else(|| ApiError::not_found("proxy not found"))?;
    Ok(Json(proxy_view(&v)))
}

async fn create_proxy(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<CreateProxyBody>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "proxies")?;
    let scheme = match req.scheme.as_str() {
        "http" => fleet_types::crds::ProxyScheme::Http,
        "https" => fleet_types::crds::ProxyScheme::Https,
        "socks5" => fleet_types::crds::ProxyScheme::Socks5,
        other => {
            return Err(ApiError::bad_request(format!(
                "scheme must be http|https|socks5 (got {other})"
            )));
        }
    };
    let name = format!(
        "px-{}-{}-{}",
        req.host.replace(['.', ':', '/'], "-"),
        req.port,
        &format!("{:06x}", rand::random::<u32>())[..6]
    );
    let out = st
        .fleet
        .upsert_proxy(
            &name,
            dto::UpsertProxyRequest {
                name: name.clone(),
                spec: fleet_types::crds::ProxySpec {
                    scheme,
                    host: req.host.clone(),
                    port: req.port,
                    tags: req.tags.clone(),
                    country: req.country.clone(),
                    isp: None,
                    capacity: req.capacity.unwrap_or(50),
                    credentials_vault_path: crate::fleet::Fleet::proxy_secret_path(&name),
                },
                credentials: Some(dto::ProxySecret {
                    username: req.username.clone(),
                    password: req.password.clone(),
                }),
            },
        )
        .await?;
    Ok((StatusCode::CREATED, Json(proxy_view(&out))))
}

async fn delete_proxy(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "proxies")?;
    st.fleet.delete_proxy(&id).await?;
    Ok(Json(json!({ "ok": true })))
}

async fn list_rollouts(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "rollouts")?;
    let items = st.fleet.rollouts.list().await;
    Ok(Json(json!({
        "rollouts": items.iter().map(rollout_view).collect::<Vec<_>>()
    })))
}

async fn get_rollout(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "rollouts")?;
    let v = st
        .fleet
        .rollouts
        .get(&id)
        .await
        .ok_or_else(|| ApiError::not_found("rollout not found"))?;
    Ok(Json(rollout_view(&v)))
}

async fn create_rollout(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<CreateRolloutBody>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "rollouts")?;
    let name = format!("rollout-{:08x}", rand::random::<u32>());
    let max_draining = match req.wave.as_ref().and_then(|w| w.size.clone()) {
        Some(Value::String(s)) => s,
        Some(Value::Number(n)) => n.to_string(),
        _ => "1".into(),
    };
    // Panel `phases` selects bots by current phase — resolve to names.
    let mut bots = vec![];
    if let Some(phases) = req.selector.as_ref().map(|s| &s.phases) {
        if !phases.is_empty() {
            bots = st
                .fleet
                .bots
                .list(None)
                .await
                .iter()
                .filter(|b| {
                    b.status
                        .as_ref()
                        .is_some_and(|s| phases.iter().any(|p| p == phase_str(s.phase)))
                })
                .filter_map(|b| b.metadata.name.clone())
                .collect();
        }
    }
    let out = st
        .fleet
        .create_rollout(dto::CreateRolloutRequest {
            name: name.clone(),
            spec: fleet_types::crds::BotRolloutSpec {
                target_image: req.image.clone(),
                target_game_version: None,
                selector: fleet_types::crds::RolloutSelector {
                    channel: req.selector.as_ref().and_then(|s| s.versions.first()).map(
                        |v| match v.as_str() {
                            "canary" => fleet_types::crds::VersionChannel::Canary,
                            "pinned" => fleet_types::crds::VersionChannel::Pinned,
                            _ => fleet_types::crds::VersionChannel::Stable,
                        },
                    ),
                    bots,
                },
                strategy: fleet_types::crds::RolloutStrategy {
                    canary: req
                        .canary
                        .as_ref()
                        .map(|bots| fleet_types::crds::CanaryStrategy {
                            count: bots.len() as u32,
                            bake_minutes: 0,
                            bots: bots.clone(),
                        }),
                    waves: fleet_types::crds::WaveStrategy {
                        max_draining,
                        max_offline: 1,
                        inter_wave_delay_s: req
                            .wave
                            .as_ref()
                            .and_then(|w| w.wait_seconds)
                            .unwrap_or(60),
                        login_jitter_s: 5,
                    },
                    gates: fleet_types::crds::RolloutGates::default(),
                },
                on_gate_failure: Default::default(),
                paused: false,
                aborted: false,
                rollback: false,
                resume_count: 0,
            },
        })
        .await?;
    // Record the caller for the admin view (`createdBy` on the contract).
    let mut stamped = out.clone();
    stamped["metadata"]["annotations"] = json!({ "xenomit.io/created-by": claims.sub });
    stamped["metadata"]["creationTimestamp"] = json!(chrono::Utc::now().to_rfc3339());
    let _ = st.fleet.rollouts.apply(stamped.clone()).await;
    Ok((StatusCode::CREATED, Json(rollout_view(&stamped))))
}

async fn rollout_action(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((id, action)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&claims, "rollouts")?;
    let action = match action.as_str() {
        "pause" => dto::RolloutAction::Pause,
        "resume" => dto::RolloutAction::Resume,
        "abort" => dto::RolloutAction::Abort,
        "rollback" => dto::RolloutAction::Rollback,
        other => {
            return Err(ApiError::bad_request(format!(
                "rollout action must be pause|resume|abort|rollback (got {other})"
            )));
        }
    };
    let v = st.fleet.rollout_action(&id, action).await?;
    Ok(Json(rollout_view(&v)))
}
