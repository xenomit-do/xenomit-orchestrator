//! `BotController`: the reconcile loop for `XenomitBot` (docs/04 §4.2),
//! written entirely against [`crate::traits`] so every path is unit-testable
//! with fakes.
//!
//! Implements the lifecycle of docs/01 §1.4: provision secrets → pod →
//! `Armed` → `Activate` → `Running`; planned handoffs as Arm → Drain →
//! (snapshot) → Activate (docs/03 §3.2); the docs/02 §2.5 exit-code table;
//! backoff+jitter+quarantine (docs/04 §4.4); epoch fencing (docs/03 §3.7);
//! and 30-day soft-delete retention (docs/12 §12.4).

use std::time::Duration;

use fleet_types::condition_type;
use fleet_types::crds::{
    LastExit, PendingRequest, PendingRequestKind, ProxyMode, RestoreReport, XenomitBot,
    XenomitBotSpec, pod_name,
};
use fleet_types::epoch::Epoch;
use fleet_types::exit_code::{ExitAction, action, classify};
use fleet_types::phase::{
    BotCondition, BotPhase, ConditionStatus, DesiredState, HandoffReason, HandoffState,
    HandoffStep, NeedsHumanKind,
};
use fleet_types::proto::GatewayCommand;
use fleet_types::snapshot::{SnapshotId, SnapshotStore};

use crate::policy::{
    CrashPolicy, CrashVerdict, TERMINATED_POD_RETAIN_MS, bot_seed, retention_expired,
};
use crate::proxy::parse_host_port;
use crate::traits::{
    BotPodSpec, BotSecretPaths, Clock, ControlDirectory, ControlError, HandoffTrigger, PodBackend,
    PodObservation, PodState, ProxyAssignment, ProxyError, ProxyPool, SecretStore,
};

/// Tuning + images for the controller.
#[derive(Clone, Debug)]
pub struct ControllerConfig {
    /// botd image for channel=stable.
    pub stable_image: String,
    /// botd image for channel=canary.
    pub canary_image: String,
    pub crash: CrashPolicy,
    /// Defaults for Drain when the bot doesn't tune them (docs/03 §3.2).
    pub drain_deadline_s: u32,
    pub hard_deadline_s: u32,
    /// Extra wait past a predecessor's lease expiry before arming a
    /// replacement (docs/03 §3.7 "lease_ttl + grace").
    pub lease_grace: Duration,
    /// `Drained` precedes the pod's exit 0 (docs/03 §3.2): a pod that
    /// reported drained keeps its pod object for this long so the clean
    /// shutdown finishes; past the grace the pod is deleted like any
    /// stale/hung pod. Bounds the wait — `Drained` is never a promise of
    /// exit.
    pub drained_exit_grace: Duration,
    /// Soft-delete retention (docs/12 §12.4).
    pub retention_days: u32,
    /// `panel.base_url` rendered into every botd config (Agent Protocol
    /// endpoint, docs/01 §1.2). `None` = the `panel` link block is rendered
    /// without a URL and the data plane stays disabled.
    pub panel_base_url: Option<String>,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        ControllerConfig {
            stable_image: "registry.local/xenomit-botd:stable".into(),
            canary_image: "registry.local/xenomit-botd:canary".into(),
            crash: CrashPolicy::default(),
            drain_deadline_s: fleet_types::crds::default_drain_deadline_s(),
            hard_deadline_s: fleet_types::crds::default_hard_deadline_s(),
            lease_grace: Duration::from_secs(30),
            drained_exit_grace: Duration::from_secs(15),
            retention_days: crate::policy::DEFAULT_RETENTION_DAYS,
            panel_base_url: None,
        }
    }
}

impl ControllerConfig {
    pub fn image_for(&self, spec: &XenomitBotSpec) -> String {
        match spec.version.channel {
            fleet_types::crds::VersionChannel::Pinned => spec
                .version
                .pinned_image
                .clone()
                .unwrap_or_else(|| self.stable_image.clone()),
            fleet_types::crds::VersionChannel::Canary => self.canary_image.clone(),
            fleet_types::crds::VersionChannel::Stable => self.stable_image.clone(),
        }
    }
}

/// What reconcile wants the caller to do with time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    /// Ask for another pass after this delay (backoff, lease waits).
    pub requeue_after: Option<Duration>,
}

impl ReconcileOutcome {
    pub fn requeue(d: Duration) -> Self {
        ReconcileOutcome {
            requeue_after: Some(d),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Backend(#[from] crate::traits::BackendError),
    #[error(transparent)]
    Secret(#[from] crate::traits::SecretError),
    #[error(transparent)]
    Control(#[from] crate::traits::ControlError),
    #[error(transparent)]
    Store(#[from] fleet_types::snapshot::StoreError),
    #[error(transparent)]
    Proxy(#[from] ProxyError),
}

/// What [`BotController::spawn`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpawnOutcome {
    /// Pod created for the epoch.
    Spawned,
    /// `require_proxy` gate: no proxy resolvable right now — no pod was
    /// created (docs/05 §5.3 fail closed).
    AwaitingProxy,
}

/// The controller. Generic over the six seams so tests plug in fakes.
pub struct BotController<P, V, T, D, C, X> {
    pub pods: P,
    pub secrets: V,
    pub snapshots: T,
    pub control: D,
    pub clock: C,
    /// Pool/fixed proxy resolution (docs/05 §5.4); a pod is never spawned
    /// without a resolved proxy (`require_proxy` stays on).
    pub proxy: X,
    pub cfg: ControllerConfig,
}

impl<P: PodBackend, V: SecretStore, T: SnapshotStore, D: ControlDirectory, C: Clock, X: ProxyPool>
    BotController<P, V, T, D, C, X>
{
    pub fn new(pods: P, secrets: V, snapshots: T, control: D, clock: C, proxy: X) -> Self {
        BotController {
            pods,
            secrets,
            snapshots,
            control,
            clock,
            proxy,
            cfg: ControllerConfig::default(),
        }
    }

    fn status<'a>(&self, bot: &'a mut XenomitBot) -> &'a mut fleet_types::crds::XenomitBotStatus {
        bot.status.get_or_insert_with(Default::default)
    }

    fn bot_id(bot: &XenomitBot) -> String {
        bot.metadata.name.clone().unwrap_or_default()
    }

    fn set_phase(&self, bot: &mut XenomitBot, phase: BotPhase, now: i64) {
        let st = self.status(bot);
        if st.phase != phase {
            st.phase = phase;
            if phase != BotPhase::NeedsHuman {
                st.needs_human = None;
            }
            upsert_condition(
                &mut st.conditions,
                BotCondition::new(condition_type::READY, ConditionStatus::Unknown, now),
            );
        }
    }

    fn epoch(&self, bot: &XenomitBot) -> Epoch {
        bot.status.as_ref().map(|s| s.epoch).unwrap_or_default()
    }

    /// `Some(at)` when `on_drained` recorded the drain marker for `epoch`
    /// (reason `drained`/`drained_degraded`): the pod reported its terminal
    /// drain at `at` but its exit 0 may still be in flight (docs/03 §3.2 —
    /// `Drained` precedes process exit, it is not the exit itself).
    fn drained_since(&self, bot: &XenomitBot, epoch: Epoch) -> Option<i64> {
        let e = bot.status.as_ref()?.last_exit.as_ref()?;
        (e.epoch == epoch && e.reason.starts_with("drained")).then_some(e.at_unix_ms)
    }

    /// One reconcile pass over a bot (idempotent, level-triggered — docs/04
    /// §4.2). Mutates `bot.status`; the caller persists it.
    pub async fn reconcile(
        &self,
        bot: &mut XenomitBot,
    ) -> Result<ReconcileOutcome, ReconcileError> {
        let now = self.clock.now_unix_ms();
        let bot_id = Self::bot_id(bot);
        let mut out = ReconcileOutcome::default();
        let mut pods = self.pods.list(&bot_id).await?;
        let epoch = self.epoch(bot);

        // --- soft-delete path (docs/12 §12.4) -----------------------------
        if let Some(deleted_at) = self.status(bot).deleted_at_unix_ms {
            // The drain sent by mark_deleted gets its deadline; after
            // hard_deadline + buffer the pods are deleted anyway.
            let force_after = deleted_at + (self.cfg.hard_deadline_s as i64 + 30) * 1000;
            let draining = self.status(bot).phase == BotPhase::Draining && now < force_after;
            // A pod that reported `Drained` gets `drained_exit_grace` to
            // exit 0 before it is deleted — the report precedes the
            // process exit (docs/03 §2), so deleting on the report would
            // kill a clean shutdown mid-flight.
            let in_grace = pods.iter().any(|p| {
                p.is_alive()
                    && self
                        .drained_since(bot, p.epoch)
                        .is_some_and(|at| now - at < self.cfg.drained_exit_grace.as_millis() as i64)
            });
            if pods.iter().any(|p| p.is_alive()) && (draining || in_grace) {
                out.requeue_after = Some(Duration::from_secs(1));
                return Ok(out);
            }
            for p in &pods {
                self.pods.delete_pod(&bot_id, p.epoch).await?;
            }
            // Confirm via the backend (level-triggered): deletes are async
            // in real backends, so alive pods may linger until next pass.
            let remaining = self.pods.list(&bot_id).await?;
            if !remaining.iter().any(|p| p.is_alive()) {
                self.status(bot).delete_pods_gone = true;
                self.set_phase(bot, BotPhase::Stopped, now);
            } else {
                out.requeue_after = Some(Duration::from_secs(1));
            }
            return Ok(out);
        }

        // --- stale pods: anything older than the handoff predecessor has no
        // right to exist (fencing hygiene, docs/03 §3.7) -------------------
        let keep_older = self
            .status(bot)
            .handoff
            .as_ref()
            .filter(|h| matches!(h.step, HandoffStep::Arm | HandoffStep::Drain))
            .map(|h| h.target_epoch.prev());
        for p in pods.iter() {
            // Once the handoff reaches `Activate` the predecessor loses its
            // `keep_older` cover — but its `Drained` report precedes exit 0
            // (docs/03 §3.2), so hold the sweep for `drained_exit_grace`
            // rather than killing the clean shutdown mid-flight.
            let drained_grace = p.is_alive()
                && self
                    .drained_since(bot, p.epoch)
                    .is_some_and(|at| now - at < self.cfg.drained_exit_grace.as_millis() as i64);
            let stale = p.epoch < epoch && Some(p.epoch) != keep_older && !drained_grace;
            let dead_long = !p.is_alive()
                && self
                    .status(bot)
                    .last_exit
                    .as_ref()
                    .is_some_and(|e| now - e.at_unix_ms > TERMINATED_POD_RETAIN_MS);
            if stale || dead_long {
                self.pods.delete_pod(&bot_id, p.epoch).await?;
            }
        }
        pods = self.pods.list(&bot_id).await?;

        // --- one-shot API requests ----------------------------------------
        if let Some(req) = self.status(bot).pending_request.take() {
            self.apply_request(bot, req, now).await?;
        }

        // --- spec drift while running -------------------------------------
        if self.status(bot).phase == BotPhase::Running && self.status(bot).handoff.is_none() {
            self.check_drift(bot, now).await?;
        }

        match self.status(bot).phase {
            BotPhase::Pending => {
                // A recreated bot resumes its epoch line: the fence stores
                // still carry the previous lifetime's watermark (the
                // `bot_epochs` row; in local mode the shared
                // `botd-state/epochs` file that pod-side `probe_fence`
                // reads) and epochs are never reused (docs/03 §3.7) —
                // restarting at 1 would get the fresh pod fenced out of
                // every commit and probe.
                if let Some(cur) = self.snapshots.current_epoch(&bot_id).await? {
                    let floor = cur.next();
                    if self.status(bot).epoch < floor {
                        self.status(bot).epoch = floor;
                    }
                }
                self.set_phase(bot, BotPhase::Provisioning, now);
            }
            BotPhase::Provisioning => self.step_provisioning(bot, &pods, now, &mut out).await?,
            BotPhase::Armed => self.step_armed(bot, &pods, now, &mut out).await?,
            BotPhase::Starting => self.step_starting(bot, &pods, now).await?,
            BotPhase::Running => self.step_running(bot, &pods, now, &mut out).await?,
            BotPhase::Draining | BotPhase::Snapshotting => {
                self.step_draining(bot, &pods, now, &mut out).await?
            }
            BotPhase::Terminated => self.step_terminated(bot, now).await?,
            BotPhase::Paused | BotPhase::Stopped => {
                if bot.spec.desired_state == DesiredState::Running {
                    self.set_phase(bot, BotPhase::Provisioning, now);
                }
            }
            // Settled phases: wait for a pending_request or spec change.
            BotPhase::NeedsHuman
            | BotPhase::NeedsUpdate
            | BotPhase::Failed
            | BotPhase::Quarantined
            | BotPhase::InvalidConfig => {}
        }
        Ok(out)
    }

    // -- phases ------------------------------------------------------------

    /// Provision secrets, then create/observe the pod for the current epoch.
    async fn step_provisioning(
        &self,
        bot: &mut XenomitBot,
        pods: &[PodObservation],
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), ReconcileError> {
        let bot_id = Self::bot_id(bot);

        // Crash backoff gate.
        if let Some(retry_at) = self.status(bot).crash.next_retry_at_unix_ms {
            if now < retry_at {
                out.requeue_after = Some(Duration::from_millis((retry_at - now) as u64));
                return Ok(());
            }
            self.status(bot).crash.next_retry_at_unix_ms = None;
        }

        self.secrets.provision_bot(&bot_id).await?;
        let epoch = self.epoch(bot);

        let cur = pods.iter().find(|p| p.epoch == epoch);
        match cur.map(|p| &p.state) {
            Some(PodState::Running) => {
                self.status(bot).pod_name = Some(pod_name(&bot_id, epoch));
                self.set_phase(bot, BotPhase::Armed, now);
            }
            Some(PodState::Pending) | Some(PodState::Unknown) => {
                out.requeue_after = Some(Duration::from_millis(500));
            }
            Some(PodState::Exited { code }) => {
                let obs = cur.unwrap().clone();
                let code = *code;
                if self
                    .status(bot)
                    .last_exit
                    .as_ref()
                    .is_some_and(|e| e.epoch == epoch)
                {
                    // Exit already classified: spawn the successor epoch.
                    out.requeue_after = Some(match self.spawn(bot, epoch.next()).await? {
                        SpawnOutcome::Spawned => Duration::from_millis(500),
                        SpawnOutcome::AwaitingProxy => Self::PROXY_WAIT,
                    });
                } else {
                    self.handle_exit(bot, pods, &obs, code, now).await?;
                }
            }
            None => {
                // No pod for the current epoch. Create one — at
                // `status.epoch` if it never existed, else the next epoch.
                let last_exit_epoch = self.status(bot).last_exit.as_ref().map(|e| e.epoch);
                let target = if last_exit_epoch == Some(epoch) || epoch == Epoch::ZERO {
                    epoch.next()
                } else {
                    epoch
                };
                out.requeue_after = Some(match self.spawn(bot, target).await? {
                    SpawnOutcome::Spawned => Duration::from_millis(500),
                    SpawnOutcome::AwaitingProxy => Self::PROXY_WAIT,
                });
            }
        }
        Ok(())
    }

    /// Pod is up; wait for its control session, then Activate.
    async fn step_armed(
        &self,
        bot: &mut XenomitBot,
        pods: &[PodObservation],
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), ReconcileError> {
        let epoch = self.epoch(bot);
        // The pod may have failed while arming (e.g. exit 69 proxy check).
        if let Some(obs) = pods.iter().find(|p| p.epoch == epoch)
            && let PodState::Exited { code } = obs.state
        {
            let obs = obs.clone();
            self.handle_exit(bot, pods, &obs, code, now).await?;
            return Ok(());
        }
        self.activate(bot, epoch, now, out).await
    }

    async fn step_starting(
        &self,
        bot: &mut XenomitBot,
        pods: &[PodObservation],
        now: i64,
    ) -> Result<(), ReconcileError> {
        let epoch = self.epoch(bot);
        if let Some(obs) = pods.iter().find(|p| p.epoch == epoch)
            && let PodState::Exited { code } = obs.state
        {
            let obs = obs.clone();
            self.handle_exit(bot, pods, &obs, code, now).await?;
            return Ok(());
        }
        // Waiting for the bot to report Running (or NeedsHuman …) via
        // `on_phase_changed`.
        Ok(())
    }

    async fn step_running(
        &self,
        bot: &mut XenomitBot,
        pods: &[PodObservation],
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), ReconcileError> {
        let bot_id = Self::bot_id(bot);
        let epoch = self.epoch(bot);

        // Pod died under us.
        if let Some(obs) = pods.iter().find(|p| p.epoch == epoch)
            && let PodState::Exited { code } = obs.state
        {
            let obs = obs.clone();
            self.handle_exit(bot, pods, &obs, code, now).await?;
            return Ok(());
        }

        // Desired-state drain (no successor).
        if bot.spec.desired_state != DesiredState::Running {
            self.send_drain(bot, epoch, HandoffReason::DesiredStateChange)
                .await?;
            self.set_phase(bot, BotPhase::Draining, now);
            return Ok(());
        }

        // Planned handoff in flight.
        if self
            .status(bot)
            .handoff
            .as_ref()
            .is_some_and(|h| h.step == HandoffStep::Arm)
        {
            let target = self.status(bot).handoff.as_ref().unwrap().target_epoch;
            let succ = pods.iter().find(|p| p.epoch == target);
            let ready = matches!(succ.map(|p| &p.state), Some(PodState::Running))
                || self.control.session(&bot_id, target).await?.is_some();
            if ready {
                let pred = target.prev();
                let reason = self.status(bot).handoff.as_ref().unwrap().reason;
                self.send_drain(bot, pred, reason).await?;
                self.status(bot).handoff.as_mut().unwrap().step = HandoffStep::Drain;
                self.set_phase(bot, BotPhase::Draining, now);
            } else {
                out.requeue_after = Some(Duration::from_millis(500));
            }
        }
        Ok(())
    }

    async fn step_draining(
        &self,
        bot: &mut XenomitBot,
        pods: &[PodObservation],
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), ReconcileError> {
        // A successor that died while the handoff was in Drain/Activate
        // ends it — `handle_exit` recovers the bot on the old image.
        // (Arm-step deaths are caught by `step_running`'s pod check.)
        if let Some(h) = self.status(bot).handoff.clone()
            && let Some(obs) = pods.iter().find(|p| p.epoch == h.target_epoch)
            && let PodState::Exited { code } = obs.state
        {
            let obs = obs.clone();
            self.handle_exit(bot, pods, &obs, code, now).await?;
            return Ok(());
        }

        let handoff = self.status(bot).handoff.clone();

        match handoff.map(|h| h.step) {
            // Planned handoff: wait for Drained (→ step Activate set by
            // `on_drained`) or the old pod's exit, then Activate successor.
            Some(HandoffStep::Drain) => {
                let h = self.status(bot).handoff.clone().unwrap();
                let pred = h.target_epoch.prev();
                let pred_gone = pods
                    .iter()
                    .find(|p| p.epoch == pred)
                    .is_none_or(|p| !p.is_alive());
                if pred_gone {
                    // Old pod exited (maybe mid-drain crash): proceed to
                    // Activate with whatever snapshot we have.
                    self.status(bot).handoff.as_mut().unwrap().step = HandoffStep::Activate;
                } else {
                    out.requeue_after = Some(Duration::from_secs(1));
                }
            }
            Some(HandoffStep::Activate) => {
                let h = self.status(bot).handoff.clone().unwrap();
                if let Some(id) = h.snapshot_id.clone() {
                    self.status(bot).last_snapshot_id = Some(SnapshotId(id));
                }
                self.activate(bot, h.target_epoch, now, out).await?;
            }
            _ => {
                // Drain without successor (desired-state / delete).
                let epoch = self.epoch(bot);
                let cur = pods.iter().find(|p| p.epoch == epoch);
                if cur.is_none_or(|p| !p.is_alive()) {
                    self.set_phase(bot, BotPhase::Terminated, now);
                } else {
                    out.requeue_after = Some(Duration::from_secs(1));
                }
            }
        }
        Ok(())
    }

    async fn step_terminated(&self, bot: &mut XenomitBot, now: i64) -> Result<(), ReconcileError> {
        match bot.spec.desired_state {
            DesiredState::Running => self.set_phase(bot, BotPhase::Provisioning, now),
            DesiredState::Paused => self.set_phase(bot, BotPhase::Paused, now),
            DesiredState::Stopped => self.set_phase(bot, BotPhase::Stopped, now),
        }
        Ok(())
    }

    // -- commands to bots ---------------------------------------------------

    /// How long to wait before retrying a spawn with no eligible proxy.
    const PROXY_WAIT: Duration = Duration::from_secs(30);

    /// The proxy the next pod must run under — the `require_proxy` gate:
    /// a pod is never created without one (docs/05 §5.3). Pool mode asks
    /// the pool (sticky `status.proxyRef` first); fixed mode parses the
    /// user-supplied `host:port`.
    async fn resolve_pod_proxy(
        &self,
        bot: &mut XenomitBot,
        paths: &BotSecretPaths,
    ) -> Result<Option<ProxyAssignment>, ReconcileError> {
        let now = self.clock.now_unix_ms();
        match bot.spec.proxy.mode {
            ProxyMode::Pool => {
                let a = self.proxy.assign(bot, now).await?;
                let st = self.status(bot);
                st.proxy_ref = a.as_ref().and_then(|a| a.name.clone());
                if let Some(ip) = a.as_ref().and_then(|a| a.resolved_ip.clone()) {
                    st.proxy_resolved_ip = Some(ip);
                }
                Ok(a)
            }
            ProxyMode::Fixed => {
                // The pool never touches a user-supplied proxy (docs/05
                // §5.4): `fixed_addr` is the address-only copy the API
                // recorded, `fixed_vault_path` its credentials — or the
                // default per-bot override path (docs/05 §5.6).
                let Some((host, port)) = bot
                    .spec
                    .proxy
                    .fixed_addr
                    .as_deref()
                    .and_then(parse_host_port)
                else {
                    return Ok(None);
                };
                let credentials_vault_path = bot
                    .spec
                    .proxy
                    .fixed_vault_path
                    .clone()
                    .unwrap_or_else(|| paths.proxy_path.clone());
                let st = self.status(bot);
                Ok(Some(ProxyAssignment {
                    name: None,
                    scheme: None,
                    host,
                    port,
                    credentials_vault_path,
                    resolved_ip: st.proxy_resolved_ip.clone(),
                    exit_ip: st.proxy_exit_ip.clone(),
                }))
            }
        }
    }

    /// Which image the next pod runs. A rollout handoff's `target_image`
    /// wins; `channel = pinned` always resolves the spec; otherwise the
    /// last image the bot actually ran (`status.image`) sticks across
    /// crash restarts, so a bot rolled to a new digest doesn't silently
    /// revert on an unrelated crash (docs/07 §7.3).
    fn desired_image(&self, bot: &XenomitBot) -> String {
        let st = bot.status.as_ref();
        if let Some(img) = st
            .and_then(|s| s.handoff.as_ref())
            .and_then(|h| h.target_image.clone())
        {
            return img;
        }
        if bot.spec.version.channel == fleet_types::crds::VersionChannel::Pinned {
            return self.cfg.image_for(&bot.spec);
        }
        st.and_then(|s| s.image.clone())
            .unwrap_or_else(|| self.cfg.image_for(&bot.spec))
    }

    async fn spawn(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
    ) -> Result<SpawnOutcome, ReconcileError> {
        let bot_id = Self::bot_id(bot);
        let image = self.desired_image(bot);
        let paths = self.secrets.provision_bot(&bot_id).await?;
        let Some(mut proxy) = self.resolve_pod_proxy(bot, &paths).await? else {
            // Fail closed: a proxy-less pod would just exit 69 at Arm —
            // report the wait instead of burning an epoch (docs/05 §5.3).
            let mut cond = BotCondition::new(
                condition_type::PROXY_HEALTHY,
                ConditionStatus::False,
                self.clock.now_unix_ms(),
            );
            cond.reason = Some("awaiting_proxy".into());
            let st = self.status(bot);
            upsert_condition(&mut st.conditions, cond);
            return Ok(SpawnOutcome::AwaitingProxy);
        };
        // Pool proxies live outside the bot's Vault policy subtree —
        // mirror the credentials into `bots/<id>/proxy` so botd's
        // read-only policy can resolve them (docs/09 §9.2a, bot.hcl.tpl).
        // A proxy without credentials (no auth) has nothing at the pool
        // path; the rendered reference stays where it was.
        if proxy.name.is_some()
            && !proxy.credentials_vault_path.starts_with(&paths.prefix)
            && self
                .secrets
                .exists(&proxy.credentials_vault_path)
                .await
                .unwrap_or(false)
        {
            self.secrets
                .copy(&proxy.credentials_vault_path, &paths.proxy_path)
                .await?;
            proxy.credentials_vault_path = paths.proxy_path.clone();
        }
        // The `panel` block is emitted only when pairing actually wrote an
        // instance token — an unpaired bot runs panel-less (botd would
        // resolve the empty placeholder as InvalidConfig otherwise).
        let has_instance_token = self
            .secrets
            .exists(&paths.instance_token_path)
            .await
            .unwrap_or(false);
        let panel_base_url = if has_instance_token {
            self.cfg.panel_base_url.as_deref()
        } else {
            None
        };
        let config_json = render_botd_config(bot, &paths, &image, &proxy, panel_base_url);
        self.pods
            .ensure_pod(&BotPodSpec {
                bot_id: bot_id.clone(),
                tenant: bot.spec.owner.clone(),
                epoch,
                image,
                config_json,
                config_revision: bot.spec.config_revision,
                labels: Default::default(),
                drain_deadline_s: self.cfg.drain_deadline_s,
                hard_deadline_s: self.cfg.hard_deadline_s,
                proxy: Some(proxy),
                resources: bot.spec.resources.as_ref().map(|r| {
                    (
                        r.requests.as_ref().and_then(|l| l.cpu.clone()),
                        r.requests.as_ref().and_then(|l| l.memory.clone()),
                    )
                }),
                owner_uid: bot.metadata.uid.clone(),
            })
            .await?;
        let st = self.status(bot);
        st.epoch = epoch;
        st.pod_name = Some(pod_name(&bot_id, epoch));
        self.control.note_pending_epoch(&bot_id, epoch).await?;
        Ok(SpawnOutcome::Spawned)
    }

    /// Send Activate for `epoch` once the predecessor is provably gone and
    /// the session is up; promotes epoch authority on success.
    async fn activate(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
        _now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), ReconcileError> {
        let bot_id = Self::bot_id(bot);

        if !self.control.predecessor_clear(&bot_id, epoch).await? {
            out.requeue_after = Some(Duration::from_secs(1));
            return Ok(());
        }
        if self.control.session(&bot_id, epoch).await?.is_none() {
            out.requeue_after = Some(Duration::from_millis(500));
            return Ok(());
        }

        let snapshot_id = self.restore_source(bot).await?;
        self.control
            .send(
                &bot_id,
                epoch,
                GatewayCommand::activate(epoch.get(), snapshot_id.clone()),
            )
            .await?;
        self.control.promote(&bot_id, epoch).await?;
        // Snapshot write authority transfers with Activate: the store's
        // recorded epoch follows the *serving* epoch so a draining
        // predecessor can still commit its terminal snapshot while the
        // successor is armed (docs/03 §3.2, docs/03 §3.7).
        self.snapshots.record_epoch(&bot_id, epoch).await?;
        {
            let st = self.status(bot);
            st.activate_snapshot_id = None;
            if let Some(id) = snapshot_id {
                st.last_snapshot_id = Some(SnapshotId(id));
            }
        }
        self.set_phase(bot, BotPhase::Starting, _now);
        Ok(())
    }

    /// Which snapshot the next activation restores: explicit request >
    /// in-flight handoff's > latest stored (crash/cold-start path).
    async fn restore_source(&self, bot: &mut XenomitBot) -> Result<Option<String>, ReconcileError> {
        let bot_id = Self::bot_id(bot);
        if let Some(id) = self.status(bot).activate_snapshot_id.clone() {
            return Ok(Some(id.0));
        }
        if let Some(h) = self.status(bot).handoff.clone()
            && let Some(id) = h.snapshot_id
        {
            return Ok(Some(id));
        }
        Ok(self
            .snapshots
            .latest(&bot_id, None)
            .await?
            .map(|s| s.meta.id.0))
    }

    async fn send_drain(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
        reason: HandoffReason,
    ) -> Result<(), ReconcileError> {
        let bot_id = Self::bot_id(bot);
        // Per-bot tuning overrides the controller defaults (docs/02 §2.2).
        let tuning = bot
            .spec
            .config
            .handoff
            .unwrap_or(fleet_types::crds::HandoffTuning {
                drain_deadline_s: self.cfg.drain_deadline_s,
                hard_deadline_s: self.cfg.hard_deadline_s,
                checkpoint_interval_s: fleet_types::crds::default_checkpoint_interval_s(),
            });
        self.control
            .send(
                &bot_id,
                epoch,
                GatewayCommand::drain(
                    reason.as_str(),
                    tuning.drain_deadline_s,
                    tuning.hard_deadline_s,
                ),
            )
            .await?;
        Ok(())
    }

    /// Begin a planned handoff: record it and arm the successor pod. The old
    /// pod keeps running until the successor is ready (docs/03 §3.9, ADR-11).
    pub async fn request_handoff(
        &self,
        bot: &mut XenomitBot,
        reason: HandoffReason,
        snapshot_id: Option<SnapshotId>,
    ) -> Result<(), ReconcileError> {
        self.begin_handoff(bot, reason, snapshot_id, None).await
    }

    /// Rollout-driven upgrade handoff: same Arm → Drain → Activate flow
    /// but the successor pod runs `target_image` (docs/07 §7.3). The
    /// digest is carried on the handoff — `spec.version` (and the drift
    /// fingerprint) is untouched.
    pub async fn request_upgrade_handoff(
        &self,
        bot: &mut XenomitBot,
        target_image: &str,
    ) -> Result<(), ReconcileError> {
        self.begin_handoff(
            bot,
            HandoffReason::Upgrade,
            None,
            Some(target_image.to_string()),
        )
        .await
    }

    async fn begin_handoff(
        &self,
        bot: &mut XenomitBot,
        reason: HandoffReason,
        snapshot_id: Option<SnapshotId>,
        target_image: Option<String>,
    ) -> Result<(), ReconcileError> {
        let now = self.clock.now_unix_ms();
        if self.status(bot).handoff.is_some() || self.status(bot).phase != BotPhase::Running {
            return Ok(());
        }
        let target = self.epoch(bot).next();
        self.status(bot).handoff = Some(HandoffState {
            reason,
            step: HandoffStep::Arm,
            target_epoch: target,
            started_at_unix_ms: now,
            snapshot_id: snapshot_id.map(|s| s.0),
            target_image,
        });
        if self.spawn(bot, target).await? == SpawnOutcome::AwaitingProxy {
            // Can't arm the successor without a proxy; un-record so the
            // running bot isn't left holding a dead handoff.
            self.status(bot).handoff = None;
        }
        Ok(())
    }

    /// Release a bot parked in `NeedsUpdate` (exit 68) onto
    /// `target_image`: already down, so there is nothing to drain — the
    /// next pod picks the digest via `status.image` and re-enters
    /// `Provisioning` (ci/docs/needsupdate-failsafe.md).
    pub async fn release_for_rollout(
        &self,
        bot: &mut XenomitBot,
        target_image: &str,
    ) -> Result<(), ReconcileError> {
        let now = self.clock.now_unix_ms();
        if self.status(bot).phase != BotPhase::NeedsUpdate {
            return Ok(());
        }
        {
            let st = self.status(bot);
            st.image = Some(target_image.to_string());
        }
        self.set_phase(bot, BotPhase::Provisioning, now);
        Ok(())
    }

    /// Consume a one-shot request recorded on status by the API.
    async fn apply_request(
        &self,
        bot: &mut XenomitBot,
        req: PendingRequest,
        now: i64,
    ) -> Result<(), ReconcileError> {
        match req.kind {
            PendingRequestKind::Restart => {
                if self.status(bot).phase == BotPhase::Running {
                    self.request_handoff(bot, HandoffReason::ManualRestart, None)
                        .await?;
                } else if self.status(bot).phase.is_settled() {
                    self.set_phase(bot, BotPhase::Provisioning, now);
                }
            }
            PendingRequestKind::RestoreSnapshot => {
                if let Some(id) = req.snapshot_id.clone() {
                    self.status(bot).activate_snapshot_id = Some(id.clone());
                }
                if self.status(bot).phase == BotPhase::Running {
                    self.request_handoff(bot, HandoffReason::RestoreSnapshot, req.snapshot_id)
                        .await?;
                } else {
                    self.set_phase(bot, BotPhase::Provisioning, now);
                }
            }
            PendingRequestKind::Rearm => {
                // New credentials/dosid in Vault: leave NeedsHuman, re-arm
                // (docs/02 §2.4).
                self.status(bot).needs_human = None;
                self.status(bot).crash = Default::default();
                self.set_phase(bot, BotPhase::Provisioning, now);
            }
        }
        Ok(())
    }

    /// Detect spec edits that must reach the running bot (docs/04 §4.5).
    async fn check_drift(&self, bot: &mut XenomitBot, _now: i64) -> Result<(), ReconcileError> {
        let bot_id = Self::bot_id(bot);
        let epoch = self.epoch(bot);
        let fingerprint = fleet_types::crds::restart_fingerprint(&bot.spec);
        let applied = self.status(bot).applied_fingerprint.clone();
        let applied_rev = self.status(bot).applied_config_revision;

        // applied_fingerprint is empty until the first pod reports Running;
        // nothing to drift-check before then.
        if applied.is_empty() || fingerprint == applied && bot.spec.config_revision == applied_rev {
            return Ok(());
        }

        if fingerprint != applied {
            // Restart-relevant edit; pick the handoff reason by what changed.
            let reason = restart_reason(&applied, &fingerprint);
            self.request_handoff(bot, reason, None).await?;
        } else {
            // fingerprint equal, revision moved: hot-applicable settings.
            self.control
                .send(
                    &bot_id,
                    epoch,
                    GatewayCommand::apply_config(
                        bot.spec.config_revision,
                        bot.spec.config.settings.to_string(),
                    ),
                )
                .await?;
            self.status(bot).applied_config_revision = bot.spec.config_revision;
        }
        Ok(())
    }

    // -- reports from the control plane --------------------------------------

    /// A bot reported a phase on its control stream. Stale-epoch reports
    /// are dropped (fencing, docs/03 §3.7).
    pub async fn on_phase_changed(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
        phase: BotPhase,
        detail: &str,
    ) -> Result<(), ReconcileError> {
        let now = self.clock.now_unix_ms();
        if epoch != self.epoch(bot) {
            return Ok(()); // fenced: stale writer
        }
        match phase {
            BotPhase::Armed => self.set_phase(bot, BotPhase::Armed, now),
            BotPhase::Running => {
                let fingerprint = fleet_types::crds::restart_fingerprint(&bot.spec);
                let revision = bot.spec.config_revision;
                let image = self.desired_image(bot);
                {
                    let st = self.status(bot);
                    st.applied_fingerprint = fingerprint;
                    st.applied_config_revision = revision;
                    st.image = Some(image);
                    if let Some(h) = st.handoff.clone()
                        && epoch == h.target_epoch
                    {
                        st.handoff = None;
                        st.last_restore = Some(RestoreReport {
                            snapshot_id: st.last_snapshot_id.clone(),
                            quality: if detail.is_empty() {
                                "unknown".into()
                            } else {
                                detail.to_string()
                            },
                            downtime_ms: 0,
                        });
                    }
                }
                self.set_phase(bot, BotPhase::Running, now);
                upsert_condition(
                    &mut self.status(bot).conditions,
                    BotCondition::new(condition_type::READY, ConditionStatus::True, now),
                );
            }
            BotPhase::NeedsHuman => {
                let kind = match detail {
                    "credentials" => NeedsHumanKind::Credentials,
                    "session_expired" => NeedsHumanKind::SessionExpired,
                    _ => NeedsHumanKind::Captcha,
                };
                self.status(bot).needs_human = Some(kind);
                self.set_phase(bot, BotPhase::NeedsHuman, now);
            }
            BotPhase::NeedsUpdate => self.set_phase(bot, BotPhase::NeedsUpdate, now),
            BotPhase::Snapshotting => self.set_phase(bot, BotPhase::Snapshotting, now),
            other => self.set_phase(bot, other, now),
        }
        Ok(())
    }

    /// The predecessor finished draining (docs/03 §3.2 `Drained`).
    pub async fn on_drained(
        &self,
        bot: &mut XenomitBot,
        epoch: Epoch,
        snapshot_id: Option<SnapshotId>,
        clean: bool,
    ) -> Result<(), ReconcileError> {
        let now = self.clock.now_unix_ms();
        if let Some(id) = snapshot_id.clone() {
            self.status(bot).last_snapshot_id = Some(id);
        }
        // Drained precedes a clean exit 0 (docs/03 §3.2); record it so the
        // provisioning path treats the pod as already-classified.
        {
            let st = self.status(bot);
            st.last_exit = Some(LastExit {
                code: Some(fleet_types::exit_code::codes::CLEAN),
                at_unix_ms: now,
                reason: if clean { "drained" } else { "drained_degraded" }.into(),
                epoch,
            });
        }
        let handoff = self.status(bot).handoff.clone();
        match handoff {
            Some(h) if epoch == h.target_epoch.prev() => {
                let st = self.status(bot);
                let h = st.handoff.as_mut().unwrap();
                h.step = HandoffStep::Activate;
                if h.snapshot_id.is_none() {
                    h.snapshot_id = snapshot_id.map(|s| s.0);
                }
            }
            Some(_) => return Ok(()), // stale report
            None => {
                // Desired-state drain finished.
                if epoch == self.epoch(bot) {
                    self.set_phase(bot, BotPhase::Terminated, now);
                }
            }
        }
        Ok(())
    }

    // -- exit handling -------------------------------------------------------

    /// Apply the docs/02 §2.5 table to a pod exit.
    async fn handle_exit(
        &self,
        bot: &mut XenomitBot,
        pods: &[PodObservation],
        obs: &PodObservation,
        code: Option<i32>,
        now: i64,
    ) -> Result<(), ReconcileError> {
        let meaning = classify(code);

        // A predecessor exiting mid-handoff is just the drain completing.
        if let Some(h) = self.status(bot).handoff.clone()
            && obs.epoch == h.target_epoch.prev()
        {
            self.status(bot).handoff.as_mut().unwrap().step = HandoffStep::Activate;
            return Ok(());
        }
        // Exits from stale pods carry no authority.
        if obs.epoch < self.epoch(bot) {
            return Ok(());
        }

        // A successor dying mid-handoff ends the handoff. When the
        // predecessor was never drained (still `Arm`) and is alive, revert
        // to it — the bot keeps running the old image with zero downtime.
        // Otherwise the normal table below recovers the bot on the old
        // image (`status.image` only moves once the successor reports
        // Running), which is also the rollout's "upgrade aborted" signal.
        if let Some(h) = self.status(bot).handoff.clone()
            && obs.epoch == h.target_epoch
        {
            let pred = h.target_epoch.prev();
            self.status(bot).handoff = None;
            if h.step == HandoffStep::Arm && pods.iter().any(|p| p.epoch == pred && p.is_alive()) {
                let bot_id = Self::bot_id(bot);
                let st = self.status(bot);
                st.epoch = pred;
                st.pod_name = Some(pod_name(&bot_id, pred));
                st.last_exit = Some(LastExit {
                    code,
                    at_unix_ms: now,
                    reason: format!("handoff_aborted:{meaning}"),
                    epoch: obs.epoch,
                });
                return Ok(());
            }
        }

        {
            let st = self.status(bot);
            st.last_exit = Some(LastExit {
                code,
                at_unix_ms: now,
                reason: meaning.to_string(),
                epoch: obs.epoch,
            });
        }

        match action(meaning) {
            ExitAction::StartSuccessor => {
                // Clean exit outside a drain: restart if desired running.
                if bot.spec.desired_state == DesiredState::Running {
                    self.set_phase(bot, BotPhase::Provisioning, now);
                } else {
                    self.set_phase(bot, BotPhase::Terminated, now);
                }
            }
            ExitAction::MarkInvalidConfig => {
                upsert_condition(
                    &mut self.status(bot).conditions,
                    BotCondition::new(condition_type::CONFIG_VALID, ConditionStatus::False, now),
                );
                self.set_phase(bot, BotPhase::InvalidConfig, now);
            }
            ExitAction::NeedsHumanCredentials => {
                self.status(bot).needs_human = Some(NeedsHumanKind::Credentials);
                self.set_phase(bot, BotPhase::NeedsHuman, now);
            }
            ExitAction::NeedsHumanCaptcha => {
                self.status(bot).needs_human = Some(NeedsHumanKind::Captcha);
                self.set_phase(bot, BotPhase::NeedsHuman, now);
            }
            ExitAction::FencedDoNothing => {
                // Reachable only for the *current* epoch: stale pods and
                // mid-handoff exits are settled by the early returns above.
                // "A newer pod owns the bot" (docs/02 §2.5) is therefore
                // false here — nothing supersedes this pod, and it is
                // already gone. Sitting still would wedge the phase
                // forever (Running reports a dead pod; a pending pause can
                // never land), so take the crash path: restore when the
                // spec wants the bot running, terminate otherwise.
                if bot.spec.desired_state == DesiredState::Running {
                    let bot_id = Self::bot_id(bot);
                    match self.cfg.crash.record_crash(
                        &mut self.status(bot).crash,
                        now,
                        bot_seed(&bot_id),
                    ) {
                        CrashVerdict::Retry { .. } => {
                            self.set_phase(bot, BotPhase::Provisioning, now)
                        }
                        CrashVerdict::Quarantine => {
                            self.set_phase(bot, BotPhase::Quarantined, now)
                        }
                    }
                } else {
                    self.set_phase(bot, BotPhase::Terminated, now);
                }
            }
            ExitAction::MarkNeedsUpdate => self.set_phase(bot, BotPhase::NeedsUpdate, now),
            ExitAction::ReassignProxyAndRestart => {
                // Exit 69: botd found its proxy unusable. The failure counts
                // toward the proxy's health budget and frees the bot's slot
                // so the next `assign` avoids it (docs/05 §5.4).
                let bot_id = Self::bot_id(bot);
                if let Some(name) = self.status(bot).proxy_ref.clone() {
                    self.proxy.report_failure(bot, &name, now).await?;
                }
                match bot.spec.proxy.mode {
                    ProxyMode::Pool => {
                        // Clearing proxyRef makes the next assign pick a
                        // fresh proxy (sticky affinity broken deliberately).
                        self.status(bot).proxy_ref = None;
                    }
                    ProxyMode::Fixed => {
                        // A user-supplied proxy is never replaced; a dead one
                        // can't hot-loop pods forever, so it draws on the
                        // crash budget like any restart storm.
                        let verdict = self.cfg.crash.record_crash(
                            &mut self.status(bot).crash,
                            now,
                            bot_seed(&bot_id),
                        );
                        if verdict == CrashVerdict::Quarantine {
                            self.set_phase(bot, BotPhase::Quarantined, now);
                            return Ok(());
                        }
                    }
                }
                upsert_condition(
                    &mut self.status(bot).conditions,
                    BotCondition::new(condition_type::PROXY_HEALTHY, ConditionStatus::False, now),
                );
                self.set_phase(bot, BotPhase::Provisioning, now);
            }
            ExitAction::MarkFailed => self.set_phase(bot, BotPhase::Failed, now),
            ExitAction::CrashRestore => {
                let bot_id = Self::bot_id(bot);
                match self.cfg.crash.record_crash(
                    &mut self.status(bot).crash,
                    now,
                    bot_seed(&bot_id),
                ) {
                    CrashVerdict::Retry { .. } => {
                        self.set_phase(bot, BotPhase::Provisioning, now);
                    }
                    CrashVerdict::Quarantine => {
                        self.set_phase(bot, BotPhase::Quarantined, now);
                    }
                }
            }
        }
        Ok(())
    }

    // -- deletion / retention (docs/09 §9.2, docs/12 §12.4) -------------------

    /// Start a soft delete: the API calls this after destroying Vault
    /// credentials. The bot drains to `Stopped`; snapshots/stat rows get
    /// their 30-day clock via `SnapshotStore::soft_delete`.
    pub async fn mark_deleted(&self, bot: &mut XenomitBot) -> Result<(), ReconcileError> {
        let now = self.clock.now_unix_ms();
        let bot_id = Self::bot_id(bot);
        {
            let st = self.status(bot);
            st.deleted_at_unix_ms = Some(now);
        }
        // Free the proxy slot + start the tenant cooldown while the drain
        // is still in flight (docs/05 §5.4).
        self.proxy.release(bot, now).await?;
        self.status(bot).proxy_ref = None;
        bot.spec.desired_state = DesiredState::Stopped;
        self.snapshots.soft_delete(&bot_id, now).await?;
        // A running bot still drains cleanly before its pods are removed.
        // A dead/absent stream doesn't block deletion — the reconcile path
        // force-removes the pods at the deadline (docs/09 §9.2).
        if self.status(bot).phase == BotPhase::Running {
            let epoch = self.epoch(bot);
            match self
                .send_drain(bot, epoch, HandoffReason::DesiredStateChange)
                .await
            {
                Ok(()) | Err(ReconcileError::Control(ControlError::NoSession { .. })) => {}
                Err(e) => return Err(e),
            }
            self.set_phase(bot, BotPhase::Draining, now);
        }
        Ok(())
    }

    /// Undelete inside the retention window (docs/09 §9.2): history is
    /// restored but credentials must be re-entered.
    pub async fn undelete(&self, bot: &mut XenomitBot) -> Result<(), ReconcileError> {
        let now = self.clock.now_unix_ms();
        let bot_id = Self::bot_id(bot);
        {
            let st = self.status(bot);
            st.deleted_at_unix_ms = None;
            st.delete_pods_gone = false;
            st.needs_human = Some(NeedsHumanKind::ReenterCredentials);
        }
        self.snapshots.undelete(&bot_id).await?;
        self.set_phase(bot, BotPhase::NeedsHuman, now);
        Ok(())
    }

    /// May this bot's rows be purged? (retention clock elapsed + pods gone)
    pub fn purge_due(&self, bot: &XenomitBot) -> bool {
        let Some(st) = bot.status.as_ref() else {
            return false;
        };
        match st.deleted_at_unix_ms {
            Some(at) => {
                st.delete_pods_gone
                    && retention_expired(at, self.clock.now_unix_ms(), self.cfg.retention_days)
            }
            None => false,
        }
    }
}

/// How the `ProxyController` triggers a `proxy_change` handoff on failover
/// (docs/05 §5.4). Same machinery as a manual restart.
#[async_trait::async_trait]
impl<P: PodBackend, V: SecretStore, T: SnapshotStore, D: ControlDirectory, C: Clock, X: ProxyPool>
    HandoffTrigger for BotController<P, V, T, D, C, X>
{
    async fn proxy_change_handoff(&self, bot: &mut XenomitBot) -> Result<(), ProxyError> {
        self.request_handoff(bot, HandoffReason::ProxyChange, None)
            .await
            .map_err(|e| ProxyError::Handoff(e.to_string()))
    }
}

/// The rollout controller drives bots through this seam (docs/07 §7.3):
/// handoffs onto a target digest, `NeedsUpdate` releases, and the per-bot
/// reconcile loop it shares with every other caller.
#[async_trait::async_trait]
impl<P: PodBackend, V: SecretStore, T: SnapshotStore, D: ControlDirectory, C: Clock, X: ProxyPool>
    crate::traits::RolloutBotOps for BotController<P, V, T, D, C, X>
{
    async fn reconcile(&self, bot: &mut XenomitBot) -> Result<(), ReconcileError> {
        BotController::reconcile(self, bot).await.map(|_| ())
    }

    async fn request_upgrade_handoff(
        &self,
        bot: &mut XenomitBot,
        target_image: &str,
    ) -> Result<(), ReconcileError> {
        BotController::request_upgrade_handoff(self, bot, target_image).await
    }

    async fn release_for_rollout(
        &self,
        bot: &mut XenomitBot,
        target_image: &str,
    ) -> Result<(), ReconcileError> {
        BotController::release_for_rollout(self, bot, target_image).await
    }
}

/// Pick the handoff reason by diffing the applied vs desired restart
/// fingerprints (docs/04 §4.5).
fn restart_reason(applied: &str, desired: &str) -> HandoffReason {
    let a: serde_json::Value = serde_json::from_str(applied).unwrap_or_default();
    let d: serde_json::Value = serde_json::from_str(desired).unwrap_or_default();
    if a.get("version") != d.get("version") {
        HandoffReason::Upgrade
    } else if a.get("proxy") != d.get("proxy") {
        HandoffReason::ProxyChange
    } else {
        HandoffReason::ConfigRequiresRestart
    }
}

/// Insert or update a condition keyed by type, preserving the original
/// transition time while the status is unchanged.
pub fn upsert_condition(conditions: &mut Vec<BotCondition>, cond: BotCondition) {
    if let Some(existing) = conditions.iter_mut().find(|c| c.type_ == cond.type_) {
        let keep_time = existing.status == cond.status;
        let t = existing.last_transition_unix_ms;
        *existing = cond;
        if keep_time {
            existing.last_transition_unix_ms = t;
        }
    } else {
        conditions.push(cond);
    }
}

/// Render the botd config JSON (docs/02 §2.2): identity + secret
/// *references*, never values. The proxy block names the Vault path holding
/// (scheme, host, port, user, pass) — `secret/data/proxies/<id>` for pool
/// assignments, the per-bot override path for fixed mode (docs/05 §5.6);
/// the bot's Vault policy must grant read on both prefixes.
fn render_botd_config(
    bot: &XenomitBot,
    paths: &crate::traits::BotSecretPaths,
    _image: &str,
    proxy: &ProxyAssignment,
    panel_base_url: Option<&str>,
) -> String {
    let spec = &bot.spec;
    let name = bot.metadata.name.clone().unwrap_or_default();
    let handoff = spec.config.handoff.unwrap_or_default();
    serde_json::json!({
        "schema": 1,
        "bot": { "id": name, "tenant": spec.owner },
        "account": {
            "server": spec.account.server,
            "auth": match spec.account.auth_mode {
                fleet_types::crds::AuthMode::Password => "password",
                fleet_types::crds::AuthMode::Session => "session",
            },
            "credentials": { "vault": { "path": spec.account.credentials_vault_path } },
        },
        "client": { "kind": "packet" },
        "settings": spec.config.settings,
        "network": {
            "proxy": {
                "mode": if proxy.name.is_some() { "pool" } else { "fixed" },
                "vault": { "path": proxy.credentials_vault_path },
            },
            // Never run game traffic direct — botd exits 69 rather than
            // leak the node IP (docs/05 §5.2).
            "require_proxy": true,
        },
        "control": {
            "token": { "vault": { "path": paths.control_token_path } },
        },
        // `panel` is present only when a base URL is configured *and* a real
        // instance token was written — the caller decides (`None` → the key
        // is omitted entirely, so botd never resolves the token ref).
        "panel": panel_base_url.map(|base| serde_json::json!({
            "base_url": base,
            "instance_token": { "vault": { "path": paths.instance_token_path } },
        })),
        // botd config uses snake_case keys (docs/02 §2.2).
        "handoff": {
            "drain_deadline_s": handoff.drain_deadline_s,
            "hard_deadline_s": handoff.hard_deadline_s,
            "checkpoint_interval_s": handoff.checkpoint_interval_s,
        },
    })
    .to_string()
}
