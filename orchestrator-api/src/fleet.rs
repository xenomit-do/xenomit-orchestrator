//! `Fleet`: the service layer the REST handlers call. It owns the repos and
//! talks to the secrets/snapshot/control seams only through traits, so the
//! whole API is exercisable offline with the operator's fakes.
//!
//! Credential flow (docs/09 §9.1, §9.2): the API *writes* credentials into
//! the `SecretStore` (Vault) and records only the Vault *path* on the CR.
//! Values are never persisted elsewhere, logged, or returned.

use async_trait::async_trait;
use axum::http::StatusCode;
use std::sync::Arc;
use tokio::sync::broadcast;

use fleet_types::crds::{
    BotRollout, PendingRequest, PendingRequestKind, Proxy, ProxyMode, XenomitBot, pod_name,
};
use fleet_types::epoch::Epoch;
use fleet_types::phase::{BotPhase, DesiredState, HandoffReason};
use fleet_types::snapshot::{SnapshotId, SnapshotMeta, SnapshotStore};
use zeroize::Zeroize;

use operator::BotController;
use operator::locks::BotLocks;
use operator::proxy::parse_host_port;
use operator::traits::{
    Clock, ControlDirectory, PodBackend, ProxyPool, SecretStore, SecretValue, bot_secret_paths,
};
use plans::{Consumption, CreateIntent, PlanDirectory, QuotaError, UndeleteError, UsageStore};

use crate::auth::{ApiError, Claims};
use crate::dto::{
    self, CreateBotRequest, CreateRolloutRequest, CredentialsRequest, FleetSummary, PoolProxyEntry,
    PoolStatus, RolloutAction, UpsertProxyRequest,
};
use crate::events::FleetEvent;
use crate::repo::{BotRepo, CrRepo};

/// Object-safe subset of `BotController` the API and the daemon's driver
/// task share (reconcile / mark_deleted / undelete / handoff / gateway
/// event callbacks), so both stay concrete while the controller keeps its
/// generics.
#[async_trait]
pub trait BotOps: Send + Sync {
    async fn reconcile(&self, bot: &mut XenomitBot) -> Result<(), String>;
    async fn mark_deleted(&self, bot: &mut XenomitBot) -> Result<(), String>;
    async fn undelete(&self, bot: &mut XenomitBot) -> Result<(), String>;
    async fn request_handoff(
        &self,
        bot: &mut XenomitBot,
        reason: HandoffReason,
        snapshot_id: Option<SnapshotId>,
    ) -> Result<(), String>;
    /// Gateway `PhaseChanged` report (docs/03 §3.7 stale-epoch fencing
    /// is the controller's job).
    async fn on_phase_changed(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
        phase: BotPhase,
        detail: &str,
    ) -> Result<(), String>;
    /// Gateway `Drained` report (docs/03 §3.2).
    async fn on_drained(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
        snapshot_id: Option<SnapshotId>,
        clean: bool,
    ) -> Result<(), String>;
}

#[async_trait]
impl<P, V, T, D, C, X> BotOps for BotController<P, V, T, D, C, X>
where
    P: PodBackend,
    V: SecretStore,
    T: SnapshotStore,
    D: ControlDirectory,
    C: Clock,
    X: ProxyPool,
{
    async fn reconcile(&self, bot: &mut XenomitBot) -> Result<(), String> {
        BotController::reconcile(self, bot)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn on_phase_changed(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
        phase: BotPhase,
        detail: &str,
    ) -> Result<(), String> {
        BotController::on_phase_changed(self, bot, epoch, phase, detail)
            .await
            .map_err(|e| e.to_string())
    }
    async fn on_drained(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
        snapshot_id: Option<SnapshotId>,
        clean: bool,
    ) -> Result<(), String> {
        BotController::on_drained(self, bot, epoch, snapshot_id, clean)
            .await
            .map_err(|e| e.to_string())
    }
    async fn mark_deleted(&self, bot: &mut XenomitBot) -> Result<(), String> {
        BotController::mark_deleted(self, bot)
            .await
            .map_err(|e| e.to_string())
    }
    async fn undelete(&self, bot: &mut XenomitBot) -> Result<(), String> {
        BotController::undelete(self, bot)
            .await
            .map_err(|e| e.to_string())
    }
    async fn request_handoff(
        &self,
        bot: &mut XenomitBot,
        reason: HandoffReason,
        snapshot_id: Option<SnapshotId>,
    ) -> Result<(), String> {
        BotController::request_handoff(self, bot, reason, snapshot_id)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Shared application state.
pub struct Fleet {
    pub bots: Arc<dyn BotRepo>,
    pub rollouts: Arc<dyn CrRepo>,
    pub proxies: Arc<dyn CrRepo>,
    pub secrets: Arc<dyn SecretStore>,
    pub snapshots: Arc<dyn SnapshotStore>,
    pub ops: Arc<dyn BotOps>,
    /// Per-bot serialization for controller passes (docs/03 §3.1). API
    /// requests reconcile inline, so every status-mutating path holds the
    /// bot's gate; the operator driver shares the same `BotLocks` so an
    /// HTTP-triggered reconcile can never overlap a loop pass for the
    /// same bot.
    pub locks: Arc<BotLocks>,
    pub events: broadcast::Sender<FleetEvent>,
    pub clock: Arc<dyn Clock>,
    /// The plan/quota directory (docs/12 §12.3). `None` = no plan model
    /// configured — every quota check is skipped (dev mode). The same
    /// `PlanDirectory` is the seam the operator later calls
    /// `plans::quota::check_running` against on `desiredState = Running`.
    pub plans: Option<Arc<dyn PlanDirectory>>,
    /// Append-only usage store for hourly metering + showback
    /// (docs/12 §12.5.1). `None` = metering not wired (`/usage` 404s).
    pub usage: Option<Arc<dyn UsageStore>>,
}

/// Empty directory for `plan_dir()` when `self.plans` is unset.
static NO_PLANS: plans::NoPlans = plans::NoPlans;

impl Fleet {
    fn now(&self) -> i64 {
        self.clock.now_unix_ms()
    }

    fn emit(&self, ev: FleetEvent) {
        let _ = self.events.send(ev);
    }

    /// Fetch a bot the caller may see; owner scoping enforced here.
    pub async fn get(&self, claims: &Claims, name: &str) -> Result<XenomitBot, ApiError> {
        let bot = self
            .bots
            .get(name)
            .await
            .ok_or_else(|| ApiError::not_found(format!("bot {name} not found")))?;
        self.authorize(claims, &bot)?;
        Ok(bot)
    }

    fn authorize(&self, claims: &Claims, bot: &XenomitBot) -> Result<(), ApiError> {
        if claims.is_admin() || bot.spec.owner == claims.sub {
            Ok(())
        } else {
            // Same 404 as absence: don't leak existence across tenants.
            Err(ApiError::not_found("bot not found"))
        }
    }

    pub async fn list(&self, claims: &Claims) -> Vec<XenomitBot> {
        let owner = if claims.is_admin() {
            None
        } else {
            Some(claims.sub.as_str())
        };
        self.bots.list(owner).await
    }

    // -- plans / quota / retention (docs/12) --------------------------------

    /// The configured directory, or an empty one (lookups all `None`).
    fn plan_dir(&self) -> &(dyn PlanDirectory + 'static) {
        self.plans.as_deref().unwrap_or(&NO_PLANS)
    }

    /// Live consumption for `owner`: soft-deleted bots don't hold a slot;
    /// `running_bots` counts `desiredState = running` (the intent the
    /// operator's `check_running` enforces, docs/12 §12.3).
    async fn consumption(&self, owner: &str) -> Consumption {
        let mut c = Consumption::default();
        for b in self.bots.list(Some(owner)).await {
            if b.status
                .as_ref()
                .and_then(|s| s.deleted_at_unix_ms)
                .is_some()
            {
                continue;
            }
            c.bots_total += 1;
            if b.spec.desired_state == DesiredState::Running {
                c.running_bots += 1;
            }
        }
        c
    }

    /// Map a quota check onto an API error — or drop it when the caller is
    /// an admin carrying an override reason (docs/12 §12.3: "admin may
    /// override with an audited reason"; the audit trail is the warn!).
    fn quota_gate(
        &self,
        claims: &Claims,
        res: Result<(), QuotaError>,
        override_reason: Option<&str>,
    ) -> Result<(), ApiError> {
        match res {
            Ok(()) => Ok(()),
            Err(e) if claims.is_admin() && override_reason.is_some_and(|r| !r.is_empty()) => {
                tracing::warn!(
                    sub = %claims.sub,
                    reason = %override_reason.unwrap_or_default(),
                    error = %e,
                    "admin quota override"
                );
                Ok(())
            }
            Err(e) => Err(ApiError::quota(e)),
        }
    }

    /// Create a bot: validate, vault the credentials, apply the CR.
    /// `credentials_vault_path` on the spec is rewritten to the provisioned
    /// path — callers may not point it at another bot's secret.
    pub async fn create(
        &self,
        claims: &Claims,
        mut req: CreateBotRequest,
    ) -> Result<XenomitBot, ApiError> {
        if req.name.is_empty()
            || req.name.len() > 63
            || !req
                .name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(ApiError::bad_request(
                "name must be 1-63 chars of [a-z0-9-]",
            ));
        }
        if self.bots.get(&req.name).await.is_some() {
            return Err(ApiError::conflict(format!("bot {} exists", req.name)));
        }
        // Tenants may only create bots they own.
        if !claims.is_admin() {
            req.spec.owner = claims.sub.clone();
        } else if req.spec.owner.is_empty() {
            return Err(ApiError::bad_request("spec.owner required"));
        }
        // Plan/quota enforcement (docs/12 §12.3): the API is authoritative
        // — the panel runs the same `check_create` for UX only. Skipped
        // entirely when no `PlanDirectory` is configured (dev mode).
        if let Some(dir) = self.plans.as_deref() {
            let usage = self.consumption(&req.spec.owner).await;
            self.quota_gate(
                claims,
                plans::check_create(
                    dir,
                    &req.spec.owner,
                    &usage,
                    &CreateIntent::from_spec(&req.spec),
                ),
                req.admin_override_reason.as_deref(),
            )?;
        }

        // Encode + scrub the credentials so the plaintext is gone before
        // the CR is assembled (fields also zeroize on drop).
        let account = req.credentials.encode();
        req.credentials.zeroize();
        // A fixed proxy needs its credentials here (or a pre-provisioned
        // Vault path) — the controller fails closed without them.
        let fixed_addr = req.proxy.as_ref().map(|p| p.addr.clone());
        let proxy = req.proxy.as_ref().map(|p| p.encode());
        if let Some(p) = req.proxy.as_mut() {
            p.zeroize();
        }
        let mut spec = req.spec;
        match spec.proxy.mode {
            ProxyMode::Fixed => {
                if fixed_addr.is_none()
                    && spec.proxy.fixed_vault_path.is_none()
                    && spec.proxy.fixed_addr.is_none()
                {
                    return Err(ApiError::bad_request(
                        "proxy.mode=fixed needs `proxy` credentials or a fixed vault path",
                    ));
                }
                // An unparsable fixedAddr can never resolve at spawn —
                // catch it here rather than leaving the bot parked on the
                // require_proxy gate.
                let addr = fixed_addr.as_deref().or(spec.proxy.fixed_addr.as_deref());
                if addr.is_some_and(|a| parse_host_port(a).is_none()) {
                    return Err(ApiError::bad_request("proxy.fixedAddr must be host:port"));
                }
            }
            ProxyMode::Pool if proxy.is_some() => {
                return Err(ApiError::bad_request(
                    "`proxy` credentials are only valid with proxy.mode=fixed",
                ));
            }
            _ => {}
        }

        // Provision the secret prefix, then write the credentials there —
        // never through the CR, the repo, or logs (docs/09 §9.2a).
        let paths = self
            .secrets
            .provision_bot(&req.name)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        self.secrets
            .write(&paths.account_path, SecretValue::new(account))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        if let Some(proxy) = proxy {
            self.secrets
                .write(&paths.proxy_path, SecretValue::new(proxy))
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
        }
        spec.account.credentials_vault_path = paths.account_path;
        // Record the address-only copy on the CR: the operator renders the
        // egress policy + probes health without reading the secret
        // (docs/05 §5.6, docs/09 §9.1).
        if spec.proxy.mode == ProxyMode::Fixed {
            if let Some(addr) = fixed_addr {
                spec.proxy.fixed_addr = Some(addr);
            }
            if spec.proxy.fixed_vault_path.is_none() {
                spec.proxy.fixed_vault_path = Some(paths.proxy_path.clone());
            }
        }

        let bot = crate::repo::new_bot(&req.name, spec);
        self.bots
            .apply(bot)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        self.emit(FleetEvent::SpecChanged {
            bot_id: req.name.clone(),
        });
        self.reconcile_one(&req.name).await
    }

    /// desired_state transitions go through the spec — the controller
    /// drives the actual drain (docs/01 §1.4).
    pub async fn set_desired(
        &self,
        claims: &Claims,
        name: &str,
        desired: DesiredState,
    ) -> Result<XenomitBot, ApiError> {
        self.get(claims, name).await?;
        let found = self
            .bots
            .update(name, &mut |b: &mut XenomitBot| {
                b.spec.desired_state = desired;
            })
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        if !found {
            return Err(ApiError::not_found("bot not found"));
        }
        self.emit(FleetEvent::SpecChanged {
            bot_id: name.into(),
        });
        self.reconcile_one(name).await
    }

    /// `PATCH` spec fields; configRevision classifies hot-apply vs handoff.
    pub async fn patch(
        &self,
        claims: &Claims,
        name: &str,
        req: dto::PatchBotRequest,
    ) -> Result<XenomitBot, ApiError> {
        let bot = self.get(claims, name).await?;
        // Spec edits that touch plan-gated fields re-run the plan check
        // (docs/12 §12.3): proxy mode/pool access and version channel.
        if self.plans.is_some() && (req.proxy.is_some() || req.version.is_some()) {
            let mut intent = CreateIntent::from_spec(&bot.spec);
            if let Some(p) = &req.proxy {
                intent.proxy_mode = p.mode;
            }
            if let Some(v) = &req.version {
                intent.channel = v.channel;
            }
            self.quota_gate(
                claims,
                plans::check_spec_change(self.plan_dir(), &bot.spec.owner, &intent),
                req.admin_override_reason.as_deref(),
            )?;
        }
        let found = self
            .bots
            .update(name, &mut |b: &mut XenomitBot| {
                if let Some(c) = req.config.clone() {
                    b.spec.config = c;
                }
                if let Some(v) = req.version.clone() {
                    b.spec.version = v;
                }
                if let Some(p) = req.proxy.clone() {
                    b.spec.proxy = p;
                }
                if let Some(rp) = req.restore_policy {
                    b.spec.restore_policy = rp;
                }
                b.spec.config_revision = req.config_revision.max(b.spec.config_revision);
            })
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        if !found {
            return Err(ApiError::not_found("bot not found"));
        }
        self.emit(FleetEvent::SpecChanged {
            bot_id: name.into(),
        });
        self.reconcile_one(name).await
    }

    /// One-shot requests land on `status.pending_request` (docs/04 §4.1);
    /// the controller picks them up next pass.
    pub async fn request(
        &self,
        claims: &Claims,
        name: &str,
        kind: PendingRequestKind,
        snapshot_id: Option<SnapshotId>,
    ) -> Result<XenomitBot, ApiError> {
        self.get(claims, name).await?;
        let now = self.now();
        let snap = snapshot_id.clone();
        let found = self
            .bots
            .update(name, &mut move |b: &mut XenomitBot| {
                let st = b.status.get_or_insert_with(Default::default);
                st.pending_request = Some(PendingRequest {
                    kind,
                    snapshot_id: snap.clone(),
                    requested_at_unix_ms: now,
                });
            })
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        if !found {
            return Err(ApiError::not_found("bot not found"));
        }
        self.reconcile_one(name).await
    }

    /// Credential rotation: write new values, then optionally re-arm.
    /// The old value is overwritten in Vault (KV), never read back.
    pub async fn write_credentials(
        &self,
        claims: &Claims,
        name: &str,
        req: CredentialsRequest,
    ) -> Result<XenomitBot, ApiError> {
        let bot = self.get(claims, name).await?;
        let paths = bot_secret_paths(name);
        let rearm = req.rearm;
        let mut req = req;
        self.secrets
            .write(
                &paths.account_path,
                SecretValue::new(req.credentials.encode()),
            )
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        req.credentials.zeroize();
        if let Some(p) = req.proxy.as_mut() {
            let addr = p.addr.clone();
            self.secrets
                .write(&paths.proxy_path, SecretValue::new(p.encode()))
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
            p.zeroize();
            // A rotated fixed proxy may also be a *new* address: copy it to
            // the CR so the controller re-renders egress + re-fingerprints
            // the restart (docs/04 §4.5, docs/05 §5.6).
            let pp = paths.proxy_path.clone();
            self.bots
                .update(name, &mut move |b: &mut XenomitBot| {
                    b.spec.proxy.fixed_addr = Some(addr.clone());
                    b.spec.proxy.fixed_vault_path = Some(pp.clone());
                })
                .await
                .ok();
        }
        if bot.spec.account.credentials_vault_path != paths.account_path {
            // Repair a drifted path so botd reads the rotated secret.
            let p = paths.account_path.clone();
            self.bots
                .update(name, &mut move |b: &mut XenomitBot| {
                    b.spec.account.credentials_vault_path = p.clone();
                })
                .await
                .ok();
        }
        if rearm
            || matches!(
                bot.status.as_ref().map(|s| s.phase),
                Some(BotPhase::NeedsHuman)
            )
        {
            self.request(claims, name, PendingRequestKind::Rearm, None)
                .await
        } else {
            self.reconcile_one(name).await
        }
    }

    /// Soft delete (docs/09 §9.2 order): destroy Vault credentials
    /// *first*, then mark the CR. History/snapshots purge after 30 days.
    pub async fn delete(&self, claims: &Claims, name: &str) -> Result<StatusCode, ApiError> {
        {
            // Load → `mark_deleted` → write-back all run under the bot's
            // gate: `mark_deleted` mutates spec *and* status, and a racing
            // reconcile could otherwise overwrite `deleted_at` (or lose a
            // spec write) with its stale copy.
            let _gate = self.locks.gate(name).await;
            let mut bot = self.get(claims, name).await?;
            if bot
                .status
                .as_ref()
                .is_some_and(|s| s.deleted_at_unix_ms.is_some())
            {
                return Ok(StatusCode::ACCEPTED); // idempotent
            }
            self.secrets
                .destroy_bot(name)
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
            self.ops
                .mark_deleted(&mut bot)
                .await
                .map_err(ApiError::internal)?;
            self.bots
                .update(name, &mut move |b: &mut XenomitBot| {
                    b.spec = bot.spec.clone();
                    b.status = bot.status.clone();
                })
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
        }
        self.emit(FleetEvent::Deleted {
            bot_id: name.into(),
        });
        // Drive the drain so the deletion is observable in the local backend.
        let _ = self.reconcile_one(name).await;
        Ok(StatusCode::ACCEPTED)
    }

    pub async fn undelete(&self, claims: &Claims, name: &str) -> Result<XenomitBot, ApiError> {
        {
            // Same gate as `delete`/reconcile: the undelete stamp must not
            // be lost to a racing reconcile's write-back.
            let _gate = self.locks.gate(name).await;
            let mut bot = self.get(claims, name).await?;
            if bot
                .status
                .as_ref()
                .is_none_or(|s| s.deleted_at_unix_ms.is_none())
            {
                return Err(ApiError::bad_request("bot is not deleted"));
            }
            // Undelete window (docs/12 §12.4): within
            // `retentionDaysAfterDelete` (default 30d) the bot comes back
            // as NeedsHuman(credentials); past it the row is purge-fodder
            // and undelete is refused.
            let days = self
                .plans
                .as_deref()
                .map(|d| d.retention_days(&bot.spec.owner))
                .unwrap_or(plans::DEFAULT_RETENTION_DAYS);
            plans::check_undelete(&bot, days, self.now()).map_err(|e| match e {
                UndeleteError::NotDeleted => ApiError::bad_request("bot is not deleted"),
                UndeleteError::WindowExpired { expired_at_unix_ms } => {
                    ApiError::gone(format!("retention window expired at {expired_at_unix_ms}"))
                }
            })?;
            self.ops
                .undelete(&mut bot)
                .await
                .map_err(ApiError::internal)?;
            let found = self
                .bots
                .update(name, &mut move |b: &mut XenomitBot| {
                    b.status = bot.status.clone()
                })
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
            if !found {
                return Err(ApiError::not_found("bot not found"));
            }
        }
        self.emit(FleetEvent::Undeleted {
            bot_id: name.into(),
        });
        self.reconcile_one(name).await
    }

    /// Restart via a planned handoff (docs/03 §3.2).
    pub async fn restart(&self, claims: &Claims, name: &str) -> Result<XenomitBot, ApiError> {
        self.request(claims, name, PendingRequestKind::Restart, None)
            .await
    }

    /// Restore a snapshot: running bots hand off; settled bots record the
    /// source for the next activation (docs/03 §3.8).
    pub async fn restore(
        &self,
        claims: &Claims,
        name: &str,
        snapshot_id: Option<SnapshotId>,
    ) -> Result<XenomitBot, ApiError> {
        if let Some(id) = &snapshot_id {
            let snap = self.snapshots.get(id).await?;
            if snap.meta.bot_id != name {
                return Err(ApiError::bad_request("snapshot belongs to another bot"));
            }
        }
        self.request(
            claims,
            name,
            PendingRequestKind::RestoreSnapshot,
            snapshot_id,
        )
        .await
    }

    pub async fn snapshots(
        &self,
        claims: &Claims,
        name: &str,
    ) -> Result<Vec<SnapshotMeta>, ApiError> {
        self.get(claims, name).await?;
        Ok(self.snapshots.list(name, true).await?)
    }

    pub async fn summary(&self, claims: &Claims) -> FleetSummary {
        let bots = self.list(claims).await;
        let mut s = FleetSummary {
            bots_total: bots.len(),
            running: 0,
            draining: 0,
            needs_human: 0,
            quarantined: 0,
            stopped_or_paused: 0,
            soft_deleted: 0,
        };
        for b in &bots {
            let Some(st) = b.status.as_ref() else {
                continue;
            };
            if st.deleted_at_unix_ms.is_some() {
                s.soft_deleted += 1;
                continue;
            }
            match st.phase {
                BotPhase::Running | BotPhase::Starting => s.running += 1,
                BotPhase::Draining | BotPhase::Snapshotting => s.draining += 1,
                BotPhase::NeedsHuman => s.needs_human += 1,
                BotPhase::Quarantined => s.quarantined += 1,
                BotPhase::Paused | BotPhase::Stopped => s.stopped_or_paused += 1,
                _ => {}
            }
        }
        s
    }

    // -- proxy pool admin (docs/05 §5.4, §5.6; admin-only at the routes) ------

    /// Vault path for a pool proxy's credentials (docs/05 §5.6). Written
    /// by `upsert_proxy`, removed by `delete_proxy`; the CR only names it.
    pub fn proxy_secret_path(name: &str) -> String {
        format!("secret/data/proxies/{name}")
    }

    fn parse_proxy(v: serde_json::Value) -> Result<Proxy, ApiError> {
        serde_json::from_value::<Proxy>(v)
            .map_err(|e| ApiError::internal(format!("proxy CR decode: {e}")))
    }

    /// Create-or-update a `Proxy` CR (apply semantics). `spec`'s Vault path
    /// is rewritten to `secret/data/proxies/<name>` — callers may not point
    /// a proxy at another secret. Existing `status` survives the apply
    /// (the operator owns the subresource).
    pub async fn upsert_proxy(
        &self,
        name: &str,
        mut req: UpsertProxyRequest,
    ) -> Result<serde_json::Value, ApiError> {
        if name.is_empty()
            || name.len() > 63
            || !name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(ApiError::bad_request(
                "name must be 1-63 chars of [a-z0-9-]",
            ));
        }
        if req.spec.host.is_empty() || req.spec.port == 0 {
            return Err(ApiError::bad_request("spec.host/spec.port required"));
        }
        if req.spec.capacity == 0 {
            return Err(ApiError::bad_request("spec.capacity must be >= 1"));
        }
        req.spec.credentials_vault_path = Self::proxy_secret_path(name);

        // Credentials first (a CR without its secret breaks every assigned
        // bot), then the spec. Status is preserved across updates.
        if let Some(creds) = req.credentials.as_mut() {
            let enc = creds.encode(&req.spec);
            self.secrets
                .write(&req.spec.credentials_vault_path, SecretValue::new(enc))
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
            creds.zeroize();
        }
        let status = self
            .proxies
            .get(name)
            .await
            .and_then(|v| Self::parse_proxy(v).ok())
            .and_then(|p| p.status);
        let proxy = Proxy {
            metadata: kube::core::ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec: req.spec,
            status,
        };
        let v = serde_json::to_value(&proxy).map_err(|e| ApiError::internal(e.to_string()))?;
        self.proxies
            .apply(v.clone())
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        Ok(v)
    }

    /// `POST /proxies`: create only — existing names are a conflict.
    pub async fn create_proxy(
        &self,
        req: UpsertProxyRequest,
    ) -> Result<serde_json::Value, ApiError> {
        let name = req.name.clone();
        if self.proxies.get(&name).await.is_some() {
            return Err(ApiError::conflict(format!("proxy {name} exists")));
        }
        self.upsert_proxy(&name, req).await
    }

    /// Delete a `Proxy` CR and its Vault credentials. Refused while bots
    /// still hold an assignment (`status.assigned`) — fail closed beats
    /// stranding running bots.
    pub async fn delete_proxy(&self, name: &str) -> Result<StatusCode, ApiError> {
        let Some(v) = self.proxies.get(name).await else {
            return Err(ApiError::not_found("proxy not found"));
        };
        let p = Self::parse_proxy(v)?;
        if !p.status.unwrap_or_default().assigned.is_empty() {
            return Err(ApiError::conflict(
                "proxy has assigned bots; drain them first",
            ));
        }
        // The secret goes regardless of whether it was ever written.
        self.secrets
            .delete_path(&Self::proxy_secret_path(name))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        self.proxies
            .delete(name)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        Ok(StatusCode::NO_CONTENT)
    }

    /// Pool overview for `GET /proxies/pool` (admin): capacity, health and
    /// assignments summed across the fleet.
    pub async fn pool_status(&self) -> PoolStatus {
        let mut out = PoolStatus {
            total: 0,
            healthy: 0,
            capacity: 0,
            assigned: 0,
            proxies: Vec::new(),
        };
        for v in self.proxies.list().await {
            let Ok(p) = serde_json::from_value::<Proxy>(v) else {
                continue;
            };
            let e = PoolProxyEntry::of(&p);
            out.total += 1;
            out.capacity += e.capacity as u64;
            out.assigned += e.assigned.len();
            if e.healthy {
                out.healthy += 1;
            }
            out.proxies.push(e);
        }
        out
    }

    // -- rollouts (docs/07 §7.3; admin-only at the routes) --------------------

    fn parse_rollout(v: serde_json::Value) -> Result<BotRollout, ApiError> {
        serde_json::from_value::<BotRollout>(v)
            .map_err(|e| ApiError::internal(format!("rollout CR decode: {e}")))
    }

    /// Validate + apply a `BotRollout` (docs/07 §7.3). Creation is
    /// rejected while another rollout is active — the controller
    /// enforces the same rule; this is the friendly 409. The CR lands
    /// `status: pending` and the controller takes it from there.
    pub async fn create_rollout(
        &self,
        req: CreateRolloutRequest,
    ) -> Result<serde_json::Value, ApiError> {
        if req.name.is_empty()
            || req.name.len() > 63
            || !req
                .name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(ApiError::bad_request(
                "name must be 1-63 chars of [a-z0-9-]",
            ));
        }
        let spec = &req.spec;
        // Only digests deploy (docs/07 §7.2): the tag → digest pin happens
        // in CI; a bare tag here is almost certainly a mistake.
        if spec.target_image.is_empty() || !spec.target_image.contains('@') {
            return Err(ApiError::bad_request(
                "spec.targetImage must be a digest reference (repo@sha256:…)",
            ));
        }
        // `maxDraining` is "N%" or an absolute count — it must parse
        // (the controller falls back to 1 on garbage, but a typo'd wave
        // limit shouldn't silently pin the fleet).
        let md = spec.strategy.waves.max_draining.trim();
        let md_ok = match md.strip_suffix('%') {
            Some(p) => p.trim().parse::<u32>().is_ok(),
            None => md.parse::<u32>().is_ok(),
        };
        if !md_ok {
            return Err(ApiError::bad_request(
                "spec.strategy.waves.maxDraining must be a count or 'N%'",
            ));
        }
        if spec.strategy.waves.max_offline == 0 {
            return Err(ApiError::bad_request(
                "spec.strategy.waves.maxOffline must be >= 1",
            ));
        }
        if let Some(r) = spec.strategy.gates.min_reward_rate_ratio
            && r <= 0.0
        {
            return Err(ApiError::bad_request(
                "spec.strategy.gates.minRewardRateRatio must be > 0",
            ));
        }
        for v in self.rollouts.list().await {
            let Ok(other) = serde_json::from_value::<BotRollout>(v) else {
                continue;
            };
            let other_name = other.metadata.name.clone().unwrap_or_default();
            if other_name == req.name {
                return Err(ApiError::conflict(format!("rollout {} exists", req.name)));
            }
            let phase = other.status.as_ref().map(|s| s.phase).unwrap_or_default();
            if phase.is_active() {
                return Err(ApiError::conflict(format!(
                    "rollout {other_name} is already active ({})",
                    phase.as_str()
                )));
            }
        }
        let rollout = BotRollout {
            metadata: kube::core::ObjectMeta {
                name: Some(req.name.clone()),
                ..Default::default()
            },
            spec: req.spec,
            status: None,
        };
        let v = serde_json::to_value(&rollout).map_err(|e| ApiError::internal(e.to_string()))?;
        self.rollouts
            .apply(v.clone())
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        self.emit(FleetEvent::RolloutSpecChanged {
            rollout: req.name,
            action: "create".into(),
        });
        Ok(v)
    }

    /// Pause / resume / abort / rollback (docs/04 §4.1 lifecycle): the
    /// API writes spec flags, the controller turns them into phase
    /// transitions on its next pass — same contract as `desiredState`.
    /// `resume` both clears `paused` and bumps `resumeCount`, covering
    /// user- and gate-pause in one call.
    pub async fn rollout_action(
        &self,
        name: &str,
        action: RolloutAction,
    ) -> Result<serde_json::Value, ApiError> {
        let Some(v) = self.rollouts.get(name).await else {
            return Err(ApiError::not_found("rollout not found"));
        };
        let mut r = Self::parse_rollout(v)?;
        let phase = r.status.as_ref().map(|s| s.phase).unwrap_or_default();
        if phase.is_terminal() {
            return Err(ApiError::conflict(format!(
                "rollout {name} already {}",
                phase.as_str()
            )));
        }
        match action {
            RolloutAction::Pause => r.spec.paused = true,
            RolloutAction::Resume => {
                r.spec.paused = false;
                r.spec.resume_count += 1;
            }
            RolloutAction::Abort => r.spec.aborted = true,
            RolloutAction::Rollback => r.spec.rollback = true,
        }
        let v = serde_json::to_value(&r).map_err(|e| ApiError::internal(e.to_string()))?;
        self.rollouts
            .apply(v.clone())
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        self.emit(FleetEvent::RolloutSpecChanged {
            rollout: name.to_string(),
            action: action.as_str().into(),
        });
        Ok(v)
    }

    /// The daily purge (docs/12 §12.4): plan-aware
    /// `retentionDaysAfterDelete` when `plans` is configured, the 30-day
    /// default otherwise; usage rows purge at 13 months.
    pub async fn purge_expired(&self) -> Result<plans::PurgeReport, plans::RetentionError> {
        let hooks = crate::plan_hooks::RepoRetention {
            bots: self.bots.clone(),
            snapshots: self.snapshots.clone(),
            usage: self.usage.clone(),
        };
        plans::RetentionJob {
            hooks: &hooks,
            gate: &plans::PlanGate {
                plans: self.plan_dir(),
                clock: self.clock.as_ref(),
            },
            clock: self.clock.as_ref(),
        }
        .run()
        .await
    }

    /// One metering sweep over `timelines` into `self.usage`
    /// (docs/12 §12.5.1). No-op when no usage store is configured.
    pub async fn meter_usage(&self, timelines: &dyn plans::PhaseTimelines) -> plans::MeterReport {
        let Some(usage) = &self.usage else {
            return plans::MeterReport::default();
        };
        let subjects = crate::plan_hooks::RepoSubjects {
            bots: self.bots.clone(),
        };
        plans::MeterJob {
            subjects: &subjects,
            timelines,
            usage: usage.as_ref(),
            plans: self.plan_dir(),
            now_unix_ms: self.now(),
        }
        .run()
        .await
    }

    /// `GET /api/v1/usage`: the showback report (docs/12 §12.5.1) —
    /// per-tenant running/offline/needs-human hours over `[from, to)`.
    pub async fn usage_report(
        &self,
        from_unix_ms: i64,
        to_unix_ms: i64,
    ) -> Result<plans::ShowbackReport, ApiError> {
        let Some(usage) = &self.usage else {
            return Err(ApiError::not_found("usage metering not configured"));
        };
        let records = usage
            .all(from_unix_ms, to_unix_ms)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        Ok(plans::showback::report(
            &records,
            from_unix_ms,
            to_unix_ms,
            self.now(),
        ))
    }

    /// Persisted reconcile: load → controller passes → store. The controller
    /// is level-triggered and advances one step per pass, so this drives
    /// passes until a status fixpoint (or a safety cap) — always safe to
    /// call after a mutation.
    ///
    /// The whole sequence runs under the bot's [`BotLocks`] gate: two
    /// concurrent requests (or a request racing the operator loop) could
    /// otherwise each work from a stale copy, double-run side effects such
    /// as `ensure_pod`/`Activate`, and clobber each other's status.
    /// Write-back is status-only via `BotRepo::update` — spec/metadata are
    /// owned by atomic `update` calls, and a status merge can never
    /// resurrect a hard-deleted row the way `apply` would.
    async fn reconcile_one(&self, name: &str) -> Result<XenomitBot, ApiError> {
        let _gate = self.locks.gate(name).await;
        let mut bot = self
            .bots
            .get(name)
            .await
            .ok_or_else(|| ApiError::not_found("bot not found"))?;
        for _ in 0..32 {
            let before = serde_json::to_string(&bot.status).unwrap_or_default();
            self.ops
                .reconcile(&mut bot)
                .await
                .map_err(ApiError::internal)?;
            if serde_json::to_string(&bot.status).unwrap_or_default() == before {
                break;
            }
        }
        let found = self
            .bots
            .update(name, &mut move |b: &mut XenomitBot| {
                b.status = bot.status.clone()
            })
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        if !found {
            return Err(ApiError::not_found("bot not found"));
        }
        self.bots
            .get(name)
            .await
            .ok_or_else(|| ApiError::not_found("bot not found"))
    }
}

/// Deterministic pod name helper re-exported for handlers/tests.
pub fn expected_pod_name(bot_id: &str, epoch: Epoch) -> String {
    pod_name(bot_id, epoch)
}
