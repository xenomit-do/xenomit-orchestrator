//! `BotRolloutController` (docs/04 §4.2 `RolloutController`): executes a
//! `BotRollout` — verify digest → canary → soak → waves under
//! `maxDraining`/`maxOffline` with login token-bucket + jitter, domain
//! gates, pause/abort/resume, automatic or manual rollback — by driving
//! per-bot `BotController` handoffs onto the target digest (docs/07
//! §7.3, docs/03 §3.2).
//!
//! Offline-window accounting: a bot's window starts when its predecessor
//! is provably gone (`handoff.step = Activate`) — or at release for
//! `NeedsUpdate` bots — and ends when the successor reports `Running`.
//! Each measurement lands on `status.bots[].offlineWindowMs` and the
//! rollout's p95 on `status.offlineWindowP95Ms` (docs/03 §3.9 SLO).
//!
//! `NeedsUpdate` (exit 68) fail-safe integration: parked bots are
//! *released* onto the target image, not drained — there is nothing left
//! to drain — and they are exempt from `maxOffline` since they were
//! already down (ci/docs/needsupdate-failsafe.md).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use fleet_types::crds::{
    BotRollout, BotRolloutStatus, GateFailureAction, GateStatus, PauseCause, RolloutBotEntry,
    RolloutBotOutcome, RolloutBotState, RolloutPhase, VersionChannel, XenomitBot,
};
use fleet_types::phase::BotPhase;

use crate::controller::ReconcileOutcome;
use crate::policy::{bot_seed, hash64};
use crate::traits::{
    BotDirectory, Clock, ImageVerifier, MetricsFeed, RolloutBotOps, RolloutError, RolloutStore,
};

/// Tuning for the rollout controller.
#[derive(Clone, Debug)]
pub struct RolloutConfig {
    /// Sustained new logins per second fleet-wide (docs/04 §4.7 shared
    /// token bucket: "logins per minute per proxy group, per node, and
    /// global"). This is the global bucket.
    pub login_rate_per_sec: f64,
    /// Burst capacity of the login bucket.
    pub login_burst: u32,
    /// How long one bot's roll may take before it fails (`drain +
    /// hard_deadline + login + slack`).
    pub entry_timeout: Duration,
    /// Poll interval while rolls are in flight and no timer lands sooner.
    pub poll: Duration,
}

impl Default for RolloutConfig {
    fn default() -> Self {
        RolloutConfig {
            login_rate_per_sec: 1.0,
            login_burst: 4,
            entry_timeout: Duration::from_secs(3600),
            poll: Duration::from_millis(500),
        }
    }
}

/// Shared login token bucket (docs/04 §4.7): `rate_per_sec` sustained
/// plus `burst` capacity. Deterministic — identical clock inputs give
/// identical slots — and the jitter lands on top of the grant.
#[derive(Debug)]
pub struct LoginBucket {
    tokens: f64,
    capacity: f64,
    rate_per_sec: f64,
    last_unix_ms: i64,
}

impl LoginBucket {
    pub fn new(rate_per_sec: f64, burst: u32) -> Self {
        LoginBucket {
            tokens: burst as f64,
            capacity: burst as f64,
            rate_per_sec,
            last_unix_ms: 0,
        }
    }

    /// Claim a login slot at `now`: returns the earliest unix-ms the
    /// login may start (grant + deterministic jitter for `seed`), or
    /// `None` when the bucket is empty — the caller requeues.
    pub fn try_acquire(&mut self, now_unix_ms: i64, jitter: Duration, seed: u64) -> Option<i64> {
        let elapsed_s = (now_unix_ms - self.last_unix_ms).max(0) as f64 / 1000.0;
        self.tokens = (self.tokens + elapsed_s * self.rate_per_sec).min(self.capacity);
        self.last_unix_ms = now_unix_ms;
        if self.tokens < 1.0 {
            return None;
        }
        self.tokens -= 1.0;
        Some(now_unix_ms + jitter_ms(jitter, seed))
    }
}

/// Deterministic jitter in `[0, jitter]` for `seed`.
fn jitter_ms(jitter: Duration, seed: u64) -> i64 {
    let max = jitter.as_millis() as u64;
    if max == 0 {
        return 0;
    }
    (hash64(seed) % (max + 1)) as i64
}

/// What the controller reports to its event sink (bridged to SSE by the
/// API, collected by a vec in tests).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RolloutEvent {
    PhaseChanged {
        rollout: String,
        phase: RolloutPhase,
    },
    BotRolled {
        rollout: String,
        bot_id: String,
        ok: bool,
        detail: String,
    },
    Gate {
        rollout: String,
        gate: String,
        passed: bool,
        detail: String,
    },
    WaveStarted {
        rollout: String,
        wave: u32,
    },
}

/// Event sink seam (sync — a broadcast send or a vec push).
pub trait RolloutEventSink: Send + Sync {
    fn emit(&self, ev: RolloutEvent);
}

/// Drops every event.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopSink;

impl RolloutEventSink for NoopSink {
    fn emit(&self, _ev: RolloutEvent) {}
}

impl<T: RolloutEventSink + ?Sized> RolloutEventSink for std::sync::Arc<T> {
    fn emit(&self, ev: RolloutEvent) {
        (**self).emit(ev)
    }
}

/// The controller (docs/04 §4.2 `RolloutController`). Generic over the
/// seams so the whole canary → waves → gates → rollback flow is
/// unit-testable with fakes.
pub struct BotRolloutController<R, B, O, C, V, M, E> {
    /// `BotRollout` CRs.
    pub rollouts: R,
    /// Fleet view (`XenomitBot` list/save).
    pub bots: B,
    /// Per-bot handoff/reconcile driving (`BotController`).
    pub ops: O,
    pub clock: C,
    /// Digest admission hook (Phase 9 does real cosign verification).
    pub verifier: V,
    /// Gate inputs from observability (docs/06 §6.6).
    pub metrics: M,
    pub events: E,
    pub cfg: RolloutConfig,
    /// The shared login token bucket (docs/04 §4.7); interior-mutable so
    /// `&self` reconcile passes can claim slots.
    bucket: Mutex<LoginBucket>,
}

impl<R, B, O, C, V, M, E> BotRolloutController<R, B, O, C, V, M, E>
where
    R: RolloutStore,
    B: BotDirectory,
    O: RolloutBotOps,
    C: Clock,
    V: ImageVerifier,
    M: MetricsFeed,
    E: RolloutEventSink,
{
    pub fn new(rollouts: R, bots: B, ops: O, clock: C, verifier: V, metrics: M, events: E) -> Self {
        Self::with(
            rollouts,
            bots,
            ops,
            clock,
            verifier,
            metrics,
            events,
            RolloutConfig::default(),
        )
    }

    /// Construct with an explicit config (tests tune the login bucket and
    /// roll deadlines).
    #[allow(clippy::too_many_arguments)]
    pub fn with(
        rollouts: R,
        bots: B,
        ops: O,
        clock: C,
        verifier: V,
        metrics: M,
        events: E,
        cfg: RolloutConfig,
    ) -> Self {
        BotRolloutController {
            rollouts,
            bots,
            ops,
            clock,
            verifier,
            metrics,
            events,
            bucket: Mutex::new(LoginBucket::new(cfg.login_rate_per_sec, cfg.login_burst)),
            cfg,
        }
    }

    fn status<'a>(&self, r: &'a mut BotRollout) -> &'a mut BotRolloutStatus {
        r.status.get_or_insert_with(Default::default)
    }

    fn name(r: &BotRollout) -> String {
        r.metadata.name.clone().unwrap_or_default()
    }

    fn set_phase(&self, r: &mut BotRollout, phase: RolloutPhase, detail: &str) {
        let st = self.status(r);
        if st.phase != phase {
            st.phase = phase;
            st.detail = detail.to_string();
            self.events.emit(RolloutEvent::PhaseChanged {
                rollout: Self::name(r),
                phase,
            });
        }
    }

    /// One level-triggered pass over a rollout (docs/04 §4.2). Mutates
    /// `rollout.status` and the fleet's bots (saved via `BotDirectory`);
    /// the caller persists the rollout through `RolloutStore`.
    pub async fn reconcile(&self, r: &mut BotRollout) -> Result<ReconcileOutcome, RolloutError> {
        let now = self.clock.now_unix_ms();
        let mut out = ReconcileOutcome::default();
        match self.status(r).phase {
            RolloutPhase::Pending => self.step_pending(r, now).await?,
            RolloutPhase::Canarying => self.step_canary(r, now, &mut out).await?,
            RolloutPhase::Progressing => self.step_progressing(r, now, &mut out).await?,
            RolloutPhase::Paused => self.step_paused(r, now, &mut out).await?,
            RolloutPhase::Aborting => self.step_aborting(r, now, &mut out).await?,
            RolloutPhase::RollingBack => self.step_rollback(r, now, &mut out).await?,
            _ => {}
        }
        self.refresh_counters(r);
        Ok(out)
    }

    // -- phases ------------------------------------------------------------

    /// Verify the digest, reject concurrency, resolve the selector into
    /// per-bot entries, then enter `Canarying` (or `Progressing` when no
    /// canary is configured).
    async fn step_pending(&self, r: &mut BotRollout, now: i64) -> Result<(), RolloutError> {
        let name = Self::name(r);

        // Cancelled before it ever started.
        if r.spec.aborted {
            self.status(r).completed_at_unix_ms = Some(now);
            self.set_phase(r, RolloutPhase::Aborted, "aborted before start");
            return Ok(());
        }
        if r.spec.rollback {
            self.status(r).completed_at_unix_ms = Some(now);
            self.set_phase(r, RolloutPhase::RolledBack, "rolled back before start");
            return Ok(());
        }

        // Concurrent rollouts are rejected (docs/07 §7.3): only one
        // rollout may be active fleet-wide. Among two `Pending` rollouts
        // the lexicographically smaller name wins — deterministic.
        for other in self.rollouts.list().await? {
            let other_name = other.metadata.name.clone().unwrap_or_default();
            if other_name == name {
                continue;
            }
            let other_phase = other.status.as_ref().map(|s| s.phase).unwrap_or_default();
            let blocks = other_phase.is_active()
                && (other_phase != RolloutPhase::Pending || other_name < name);
            if blocks {
                self.fail(
                    r,
                    &format!("concurrent rollout {other_name} is already active"),
                );
                return Ok(());
            }
        }

        // Digest admission (docs/07 §7.2, docs/09 §9.2 — real cosign
        // verification is Phase 9; the hook fails closed).
        if let Err(e) = self.verifier.verify(&r.spec.target_image).await {
            self.fail(r, &format!("image verification: {e}"));
            return Ok(());
        }

        // Resolve the selector. Only selected bots are ever touched —
        // a `canary.bots` name outside the selector becomes a Skipped
        // entry for audit, never a roll.
        let all = self.bots.list().await?;
        let mut selected: Vec<&XenomitBot> = all
            .iter()
            .filter(|b| {
                r.spec.selector.matches(b)
                    && b.status
                        .as_ref()
                        .and_then(|s| s.deleted_at_unix_ms)
                        .is_none()
            })
            .collect();
        selected.sort_by_key(|b| b.metadata.name.clone().unwrap_or_default());
        let selected_names: HashSet<String> = selected
            .iter()
            .map(|b| b.metadata.name.clone().unwrap_or_default())
            .collect();

        let mut entries: Vec<RolloutBotEntry> = Vec::new();
        let mut canary_names: HashSet<String> = HashSet::new();
        let canary = r.spec.strategy.canary.clone().unwrap_or_default();

        // Pinned canary bots must also match the selector (docs/04 §4.1
        // `strategy.canary.bots` + selector safety).
        for pinned in &canary.bots {
            if selected_names.contains(pinned) {
                canary_names.insert(pinned.clone());
            } else {
                entries.push(RolloutBotEntry {
                    bot: pinned.clone(),
                    state: RolloutBotState::Skipped,
                    wave: 0,
                    detail: "not selected".into(),
                    ..Default::default()
                });
            }
        }
        // Fill the canary count from the selection (alphabetical).
        for b in &selected {
            let n = b.metadata.name.clone().unwrap_or_default();
            if canary_names.len() >= canary.count as usize || canary_names.contains(&n) {
                continue;
            }
            canary_names.insert(n);
        }
        // Wave assignment: canary → wave 0, the rest chunked by the
        // resolved maxDraining into waves 1..=N.
        let wave_size = r
            .spec
            .strategy
            .waves
            .resolve_max_draining(selected.len())
            .max(1) as usize;
        let mut wave = 0u32;
        let mut in_wave = 0usize;
        for b in &selected {
            let n = b.metadata.name.clone().unwrap_or_default();
            let w = if canary_names.contains(&n) {
                0
            } else {
                if in_wave == 0 {
                    wave += 1;
                }
                in_wave += 1;
                if in_wave >= wave_size {
                    in_wave = 0;
                }
                wave
            };
            entries.push(Self::entry_for(&n, &all, w));
        }

        let spec_resume = r.spec.resume_count;
        {
            let st = self.status(r);
            st.bots = entries;
            st.total = st.bots.len() as u32;
            st.started_at_unix_ms = Some(now);
            st.resume_handled = spec_resume;
        }

        if self.status(r).bots.is_empty() {
            self.set_phase(r, RolloutPhase::Done, "no bots matched the selector");
            self.status(r).completed_at_unix_ms = Some(now);
            return Ok(());
        }
        if canary_names.is_empty() {
            self.status(r).wave = 1;
            self.status(r).wave_ready_at_unix_ms = Some(now);
            self.set_phase(r, RolloutPhase::Progressing, "rolling (no canary)");
            self.events.emit(RolloutEvent::WaveStarted {
                rollout: name,
                wave: 1,
            });
        } else {
            self.set_phase(r, RolloutPhase::Canarying, "canary started");
            self.events.emit(RolloutEvent::WaveStarted {
                rollout: name,
                wave: 0,
            });
        }
        Ok(())
    }

    /// Canary roll + bake: drive wave-0 entries, soak for
    /// `canary.bakeMinutes` after the last canary finishes, evaluate
    /// gates, then enter `Progressing`.
    async fn step_canary(
        &self,
        r: &mut BotRollout,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), RolloutError> {
        if self.spec_interrupts(r) {
            return Ok(());
        }
        let mut bots = self.bot_map().await?;
        self.drive_scope(r, &mut bots, &[0u32], now, out, true)
            .await?;

        let done = self
            .status(r)
            .bots
            .iter()
            .filter(|e| e.wave == 0)
            .all(|e| Self::terminal(e.state));
        if !done {
            out.requeue_after = Some(
                out.requeue_after
                    .unwrap_or(self.cfg.poll)
                    .min(self.cfg.poll),
            );
            return Ok(());
        }

        let bake_ms = r
            .spec
            .strategy
            .canary
            .as_ref()
            .map(|c| c.bake_minutes as i64 * 60_000)
            .unwrap_or(0);
        let st = self.status(r);
        if st.canary_done_at_unix_ms.is_none() {
            st.canary_done_at_unix_ms = Some(now);
        }
        let bake_until = st.canary_done_at_unix_ms.unwrap_or(now) + bake_ms;
        if now < bake_until {
            out.requeue_after = Some(Duration::from_millis((bake_until - now) as u64));
            return Ok(());
        }

        let gates = self.eval_gates(r).await;
        let st = self.status(r);
        st.gates = gates.clone();
        if let Some(g) = gates.iter().find(|g| !g.passed) {
            self.gate_failed(r, g);
            return Ok(());
        }
        self.status(r).wave = 1;
        self.status(r).wave_ready_at_unix_ms = Some(now);
        self.set_phase(r, RolloutPhase::Progressing, "canary passed; wave 1");
        self.events.emit(RolloutEvent::WaveStarted {
            rollout: Self::name(r),
            wave: 1,
        });
        Ok(())
    }

    /// Wave rolling: fire the current wave's pending entries under the
    /// caps + login bucket, drive in-flight ones, gate at each wave end.
    async fn step_progressing(
        &self,
        r: &mut BotRollout,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), RolloutError> {
        if self.spec_interrupts(r) {
            return Ok(());
        }
        let wave = self.status(r).wave;
        let ready_at = self.status(r).wave_ready_at_unix_ms.unwrap_or(now);
        if now < ready_at {
            out.requeue_after = Some(Duration::from_millis((ready_at - now) as u64));
            return Ok(());
        }

        let mut bots = self.bot_map().await?;
        self.drive_scope(r, &mut bots, &[wave], now, out, false)
            .await?;

        let last_wave = self
            .status(r)
            .bots
            .iter()
            .map(|e| e.wave)
            .max()
            .unwrap_or(0);
        let wave_done = self
            .status(r)
            .bots
            .iter()
            .filter(|e| e.wave == wave)
            .all(|e| Self::terminal(e.state));
        if !wave_done {
            return Ok(());
        }

        let gates = self.eval_gates(r).await;
        let st = self.status(r);
        st.gates = gates.clone();
        if let Some(g) = gates.iter().find(|g| !g.passed) {
            self.gate_failed(r, g);
            return Ok(());
        }
        if wave >= last_wave {
            self.status(r).completed_at_unix_ms = Some(now);
            self.set_phase(r, RolloutPhase::Done, "rollout complete");
            return Ok(());
        }
        let delay_ms = r.spec.strategy.waves.inter_wave_delay_s as i64 * 1000;
        let st = self.status(r);
        st.wave = wave + 1;
        st.wave_ready_at_unix_ms = Some(now + delay_ms);
        self.events.emit(RolloutEvent::WaveStarted {
            rollout: Self::name(r),
            wave: wave + 1,
        });
        // Soak: don't requeue earlier than the inter-wave delay unless
        // something else needs a sooner look.
        let next = Duration::from_secs(r.spec.strategy.waves.inter_wave_delay_s as u64);
        out.requeue_after = Some(out.requeue_after.map_or(next, |d| d.min(next)));
        Ok(())
    }

    /// Held: finish in-flight rolls, start nothing. Resumes on
    /// `!spec.paused` (user pause) or a `resumeCount` bump (gate pause).
    async fn step_paused(
        &self,
        r: &mut BotRollout,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), RolloutError> {
        if r.spec.aborted {
            self.set_phase(r, RolloutPhase::Aborting, "aborted while paused");
            return Ok(());
        }
        if r.spec.rollback {
            self.set_phase(r, RolloutPhase::RollingBack, "rollback while paused");
            return Ok(());
        }
        // In-flight rolls still finish — a bot left half-handed-off is
        // worse than a completed one.
        let mut bots = self.bot_map().await?;
        self.drive_in_flight(r, &mut bots, now, out).await?;

        let spec_resume = r.spec.resume_count;
        let (cause, resume_phase) = {
            let st = self.status(r);
            (
                st.pause_cause.unwrap_or(PauseCause::User),
                st.resume_phase.unwrap_or(RolloutPhase::Progressing),
            )
        };
        let resume = !r.spec.paused
            && match cause {
                PauseCause::User => true,
                PauseCause::Gate => spec_resume > self.status(r).resume_handled,
            };
        if resume {
            let st = self.status(r);
            st.resume_handled = spec_resume;
            st.pause_cause = None;
            st.resume_phase = None;
            self.set_phase(r, resume_phase, "resumed");
        } else {
            out.requeue_after = Some(Duration::from_secs(5));
        }
        Ok(())
    }

    /// `spec.aborted`: pending entries become skipped, in-flight rolls
    /// finish, then `Aborted`. Rolled bots keep the target image — abort
    /// is not a rollback (docs/04 §4.1).
    async fn step_aborting(
        &self,
        r: &mut BotRollout,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), RolloutError> {
        {
            let st = self.status(r);
            for e in st.bots.iter_mut() {
                if e.state == RolloutBotState::Pending {
                    e.state = RolloutBotState::Skipped;
                    e.detail = "aborted".into();
                }
            }
        }
        let mut bots = self.bot_map().await?;
        self.drive_in_flight(r, &mut bots, now, out).await?;
        if self.status(r).bots.iter().all(|e| Self::terminal(e.state)) {
            self.status(r).completed_at_unix_ms = Some(now);
            self.set_phase(r, RolloutPhase::Aborted, "aborted");
        }
        Ok(())
    }

    /// Hand every successfully rolled bot back to `fromImage` through the
    /// same handoff machinery (docs/07 §7.5). Bots released from
    /// `NeedsUpdate` are skipped — there is nothing to roll back *to*
    /// (ci/docs/needsupdate-failsafe.md). Ends `RolledBack`.
    async fn step_rollback(
        &self,
        r: &mut BotRollout,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), RolloutError> {
        if r.spec.aborted {
            self.set_phase(r, RolloutPhase::Aborting, "aborted during rollback");
            return Ok(());
        }
        if r.spec.paused {
            self.enter_pause(r, PauseCause::User);
            return Ok(());
        }
        {
            // Waves that never fired stay on their old image — record
            // them as skipped so the rollback can terminate.
            let st = self.status(r);
            for e in st.bots.iter_mut() {
                if e.state == RolloutBotState::Pending {
                    e.state = RolloutBotState::Skipped;
                    e.detail = "rollback before roll".into();
                }
            }
        }
        let mut bots = self.bot_map().await?;
        // Fire rollbacks for entries sitting on the target.
        for i in 0..self.status(r).bots.len() {
            if self.status(r).bots[i].state != RolloutBotState::Done {
                continue;
            }
            let (bot_name, from_image, from_nu, tagged_only) = {
                let e = &self.status(r).bots[i];
                (
                    e.bot.clone(),
                    e.from_image.clone(),
                    e.from_needs_update,
                    e.outcome.is_none(),
                )
            };
            if from_nu {
                continue; // can't roll back a game-mismatch park
            }
            let Some(bot) = bots.get_mut(&bot_name) else {
                self.status(r).bots[i].state = RolloutBotState::Skipped;
                continue;
            };
            if tagged_only {
                // A settled bot was only re-tagged: tag it back.
                if let Some(st) = bot.status.as_mut() {
                    st.image = from_image.clone();
                }
                self.bots.save(bot).await?;
                self.status(r).bots[i].state = RolloutBotState::RolledBack;
                continue;
            }
            let Some(from) = from_image else {
                self.status(r).bots[i].state = RolloutBotState::RolledBack;
                continue;
            };
            if bot
                .status
                .as_ref()
                .and_then(|s| s.image.as_ref())
                .is_some_and(|img| *img == from)
            {
                self.status(r).bots[i].state = RolloutBotState::RolledBack;
                continue;
            }
            // Capacity + login slot, same as forward rolls.
            if !self.caps_allow(r) {
                out.requeue_after = Some(self.cfg.poll);
                continue;
            }
            if self.status(r).bots[i].ready_at_unix_ms.is_none() {
                let t = self.acquire_login(r, &bot_name, now);
                self.status(r).bots[i].ready_at_unix_ms = t;
            }
            let ready_at = self.status(r).bots[i].ready_at_unix_ms;
            match ready_at {
                Some(t) if t <= now => {
                    self.ops.request_upgrade_handoff(bot, &from).await?;
                    self.bots.save(bot).await?;
                    let e = &mut self.status(r).bots[i];
                    e.state = RolloutBotState::RollingBack;
                    e.expires_at_unix_ms = Some(now + self.cfg.entry_timeout.as_millis() as i64);
                    e.detail = format!("rollback → {from}");
                }
                Some(t) => {
                    out.requeue_after = Some(
                        out.requeue_after
                            .map_or(Duration::from_millis((t - now) as u64), |d| {
                                d.min(Duration::from_millis((t - now) as u64))
                            }),
                    );
                }
                None => {
                    out.requeue_after = Some(self.cfg.poll);
                }
            }
        }
        // Drive in-flight rollbacks.
        self.drive_in_flight(r, &mut bots, now, out).await?;

        if self.status(r).bots.iter().all(|e| Self::terminal(e.state)) {
            self.status(r).completed_at_unix_ms = Some(now);
            self.set_phase(
                r,
                RolloutPhase::RolledBack,
                "rolled back to previous images",
            );
        }
        Ok(())
    }

    // -- driving ------------------------------------------------------------

    /// Drive every entry scoped to `waves` (Pending → fire, Rolling →
    /// advance). `watch_done` additionally re-checks `Done` entries — the
    /// canary bake keeps watching canaries for late failures.
    async fn drive_scope(
        &self,
        r: &mut BotRollout,
        bots: &mut HashMap<String, XenomitBot>,
        waves: &[u32],
        now: i64,
        out: &mut ReconcileOutcome,
        watch_done: bool,
    ) -> Result<(), RolloutError> {
        for i in 0..self.status(r).bots.len() {
            let (state, wave) = {
                let e = &self.status(r).bots[i];
                (e.state, e.wave)
            };
            if !waves.contains(&wave) {
                continue;
            }
            match state {
                RolloutBotState::Pending => self.fire_entry(r, bots, i, now, out).await?,
                RolloutBotState::Rolling | RolloutBotState::RollingBack => {
                    self.drive_entry(r, bots, i, now, out).await?
                }
                RolloutBotState::Done if watch_done => {
                    self.watch_done_entry(r, bots, i, now).await?
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Drive every in-flight (Rolling/RollingBack) entry regardless of
    /// wave — used by Paused/Aborting/RollingBack where no new fires
    /// happen but running handoffs still need reconcile passes.
    async fn drive_in_flight(
        &self,
        r: &mut BotRollout,
        bots: &mut HashMap<String, XenomitBot>,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), RolloutError> {
        for i in 0..self.status(r).bots.len() {
            let state = self.status(r).bots[i].state;
            if matches!(
                state,
                RolloutBotState::Rolling | RolloutBotState::RollingBack
            ) {
                self.drive_entry(r, bots, i, now, out).await?;
            }
        }
        Ok(())
    }

    /// Classify a `Pending` entry and maybe start its roll: the
    /// `NeedsUpdate` release path, the image retag for settled bots, or
    /// the normal upgrade handoff under caps + login throttle.
    async fn fire_entry(
        &self,
        r: &mut BotRollout,
        bots: &mut HashMap<String, XenomitBot>,
        i: usize,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), RolloutError> {
        let target = r.spec.target_image.clone();
        let bot_name = self.status(r).bots[i].bot.clone();
        let Some(bot) = bots.get_mut(&bot_name) else {
            let e = &mut self.status(r).bots[i];
            e.state = RolloutBotState::Skipped;
            e.detail = "bot not found".into();
            return Ok(());
        };
        if bot
            .status
            .as_ref()
            .and_then(|s| s.deleted_at_unix_ms)
            .is_some()
        {
            let e = &mut self.status(r).bots[i];
            e.state = RolloutBotState::Skipped;
            e.detail = "deleted".into();
            return Ok(());
        }
        let (phase, image, handoff) = {
            let st = bot.status.as_ref();
            (
                st.map(|s| s.phase).unwrap_or_default(),
                st.and_then(|s| s.image.clone()),
                st.and_then(|s| s.handoff.clone()),
            )
        };
        // Already running the target: nothing to do.
        if phase == BotPhase::Running
            && handoff.is_none()
            && image.as_deref() == Some(target.as_str())
        {
            self.finish_entry(r, i, RolloutBotState::Done, None, "already on target", now);
            return Ok(());
        }
        // A handoff someone else started: wait it out rather than stack —
        // but not forever.
        if handoff.is_some() {
            let e = &mut self.status(r).bots[i];
            if e.expires_at_unix_ms.is_none() {
                e.expires_at_unix_ms = Some(now + self.cfg.entry_timeout.as_millis() as i64);
            }
            if now > e.expires_at_unix_ms.unwrap_or(i64::MAX) {
                e.state = RolloutBotState::Failed;
                e.outcome = Some(RolloutBotOutcome::Timeout);
                e.detail = "another handoff never finished".into();
                self.emit_bot(r, i, false);
                return Ok(());
            }
            out.requeue_after = Some(self.cfg.poll);
            return Ok(());
        }
        match phase {
            BotPhase::Running => {
                if bot.spec.desired_state != fleet_types::phase::DesiredState::Running {
                    // Will drain itself; retag so the next pod is new.
                    self.retag(bot, &target).await?;
                    self.finish_entry(r, i, RolloutBotState::Done, None, "tagged (draining)", now);
                    return Ok(());
                }
                if !self.caps_allow(r) {
                    out.requeue_after = Some(self.cfg.poll);
                    return Ok(());
                }
                if !self.claim_ready(r, i, now, out) {
                    return Ok(());
                }
                self.ops.request_upgrade_handoff(bot, &target).await?;
                self.bots.save(bot).await?;
                let e = &mut self.status(r).bots[i];
                e.state = RolloutBotState::Rolling;
                e.expires_at_unix_ms = Some(now + self.cfg.entry_timeout.as_millis() as i64);
                e.detail = format!("handoff → {target}");
                Ok(())
            }
            BotPhase::NeedsUpdate => {
                if bot.spec.version.channel == VersionChannel::Pinned {
                    let e = &mut self.status(r).bots[i];
                    e.state = RolloutBotState::Skipped;
                    e.detail = "pinned".into();
                    return Ok(());
                }
                if !self.claim_ready(r, i, now, out) {
                    return Ok(());
                }
                self.ops.release_for_rollout(bot, &target).await?;
                self.bots.save(bot).await?;
                let e = &mut self.status(r).bots[i];
                e.state = RolloutBotState::Rolling;
                e.offline_started_at_unix_ms = Some(now);
                e.expires_at_unix_ms = Some(now + self.cfg.entry_timeout.as_millis() as i64);
                e.detail = format!("release needs_update → {target}");
                Ok(())
            }
            // Mid-lifecycle: another transition owns the bot this pass.
            BotPhase::Pending
            | BotPhase::Provisioning
            | BotPhase::Armed
            | BotPhase::Starting
            | BotPhase::Draining
            | BotPhase::Snapshotting
            | BotPhase::Terminated => {
                out.requeue_after = Some(self.cfg.poll);
                Ok(())
            }
            // Settled phases (Paused/Stopped/NeedsHuman/Failed/
            // Quarantined/InvalidConfig): no session to hand off — retag
            // so the next pod runs the target.
            _ => {
                self.retag(bot, &target).await?;
                self.finish_entry(r, i, RolloutBotState::Done, None, "tagged (settled)", now);
                Ok(())
            }
        }
    }

    /// Advance one `Rolling`/`RollingBack` entry: reconcile the bot once,
    /// record offline-window start/end, classify terminal outcomes.
    async fn drive_entry(
        &self,
        r: &mut BotRollout,
        bots: &mut HashMap<String, XenomitBot>,
        i: usize,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> Result<(), RolloutError> {
        let (bot_name, rolling_back) = {
            let e = &self.status(r).bots[i];
            (e.bot.clone(), e.state == RolloutBotState::RollingBack)
        };
        let Some(bot) = bots.get_mut(&bot_name) else {
            self.finish_entry(
                r,
                i,
                if rolling_back {
                    RolloutBotState::Failed
                } else {
                    RolloutBotState::Skipped
                },
                None,
                "bot gone",
                now,
            );
            return Ok(());
        };
        // The bot's own reconcile advances the handoff (Arm → Drain →
        // Activate) — in a cluster this is the BotController's watch loop.
        self.ops.reconcile(bot).await?;
        let want = if rolling_back {
            self.status(r).bots[i].from_image.clone()
        } else {
            Some(r.spec.target_image.clone())
        };
        let (phase, image, handoff, deleted) = {
            let st = bot.status.as_ref();
            (
                st.map(|s| s.phase).unwrap_or_default(),
                st.and_then(|s| s.image.clone()),
                st.and_then(|s| s.handoff.clone()),
                st.and_then(|s| s.deleted_at_unix_ms).is_some(),
            )
        };

        // Offline window starts when the bot provably went down:
        // predecessor drained (handoff step Activate) or phase left
        // Running entirely.
        {
            let e = &mut self.status(r).bots[i];
            if e.offline_started_at_unix_ms.is_none()
                && (e.from_needs_update
                    || handoff
                        .as_ref()
                        .is_some_and(|h| h.step == fleet_types::phase::HandoffStep::Activate)
                    || matches!(
                        phase,
                        BotPhase::Starting | BotPhase::Provisioning | BotPhase::Armed
                    ))
            {
                e.offline_started_at_unix_ms = Some(now);
            }
            if e.expires_at_unix_ms.is_some_and(|deadline| now > deadline) {
                e.state = RolloutBotState::Failed;
                e.outcome = Some(RolloutBotOutcome::Timeout);
                e.detail = "roll deadline exceeded".into();
                self.emit_bot(r, i, false);
                self.bots.save(bot).await?;
                return Ok(());
            }
        }

        if deleted {
            self.finish_entry(r, i, RolloutBotState::Skipped, None, "deleted", now);
            self.bots.save(bot).await?;
            return Ok(());
        }

        let on_want = image.is_some() && image == want;
        match (phase, handoff.is_none()) {
            (BotPhase::Running, true) if on_want => {
                let (state, outcome) = if rolling_back {
                    (RolloutBotState::RolledBack, None)
                } else {
                    let quality = bot
                        .status
                        .as_ref()
                        .and_then(|s| s.last_restore.as_ref())
                        .map(|lr| lr.quality.clone());
                    let failed = quality.as_deref() == Some("failed");
                    let e = &mut self.status(r).bots[i];
                    e.restore_quality = quality;
                    (
                        RolloutBotState::Done,
                        Some(if failed {
                            RolloutBotOutcome::FailedRestore
                        } else {
                            RolloutBotOutcome::Success
                        }),
                    )
                };
                {
                    let e = &mut self.status(r).bots[i];
                    e.offline_window_ms = e
                        .offline_started_at_unix_ms
                        .map(|t| (now - t).max(0) as u64);
                }
                self.finish_entry(r, i, state, outcome, "on target", now);
            }
            (BotPhase::Running, true) => self.finish_entry(
                r,
                i,
                RolloutBotState::Failed,
                Some(RolloutBotOutcome::UpgradeAborted),
                "handoff died; bot stayed on old image",
                now,
            ),
            (BotPhase::NeedsHuman, _) => self.finish_entry(
                r,
                i,
                RolloutBotState::Failed,
                Some(RolloutBotOutcome::LoginFailure),
                "needs_human after roll",
                now,
            ),
            (BotPhase::NeedsUpdate, true) => self.finish_entry(
                r,
                i,
                RolloutBotState::Failed,
                Some(RolloutBotOutcome::NeedsUpdateParked),
                "re-parked needs_update",
                now,
            ),
            (BotPhase::Failed | BotPhase::Quarantined | BotPhase::InvalidConfig, _) => self
                .finish_entry(
                    r,
                    i,
                    RolloutBotState::Failed,
                    Some(RolloutBotOutcome::Fault),
                    &format!("phase {phase}"),
                    now,
                ),
            (BotPhase::Paused | BotPhase::Stopped, true) => self.finish_entry(
                r,
                i,
                RolloutBotState::Skipped,
                None,
                "desired state changed",
                now,
            ),
            _ => {
                out.requeue_after = Some(
                    out.requeue_after
                        .unwrap_or(self.cfg.poll)
                        .min(self.cfg.poll),
                );
            }
        }
        self.bots.save(bot).await?;
        Ok(())
    }

    /// Canary bake watch: a `Done` canary that leaves `Running` badly
    /// demotes to `Failed` so the gate catches it at bake end.
    async fn watch_done_entry(
        &self,
        r: &mut BotRollout,
        bots: &mut HashMap<String, XenomitBot>,
        i: usize,
        _now: i64,
    ) -> Result<(), RolloutError> {
        let bot_name = self.status(r).bots[i].bot.clone();
        let Some(bot) = bots.get_mut(&bot_name) else {
            return Ok(());
        };
        self.ops.reconcile(bot).await?;
        let (phase, image) = {
            let st = bot.status.as_ref();
            (
                st.map(|s| s.phase).unwrap_or_default(),
                st.and_then(|s| s.image.clone()),
            )
        };
        if phase == BotPhase::Running && image.as_deref() == Some(r.spec.target_image.as_str()) {
            self.bots.save(bot).await?;
            return Ok(()); // still good
        }
        // Transient recovery on the target (crash → Provisioning → same
        // image) doesn't demote; only clear bad states do.
        let demote = match phase {
            BotPhase::NeedsHuman => Some((RolloutBotOutcome::LoginFailure, "needs_human in bake")),
            BotPhase::NeedsUpdate => Some((RolloutBotOutcome::NeedsUpdateParked, "re-parked")),
            BotPhase::Failed | BotPhase::Quarantined | BotPhase::InvalidConfig => {
                Some((RolloutBotOutcome::Fault, "fault in bake"))
            }
            BotPhase::Paused | BotPhase::Stopped => {
                Some((RolloutBotOutcome::Fault, "parked in bake"))
            }
            BotPhase::Running if image.as_deref() != Some(r.spec.target_image.as_str()) => {
                Some((RolloutBotOutcome::UpgradeAborted, "left target image"))
            }
            _ => None,
        };
        if let Some((outcome, detail)) = demote {
            let e = &mut self.status(r).bots[i];
            e.state = RolloutBotState::Failed;
            e.outcome = Some(outcome);
            e.detail = detail.into();
            self.emit_bot(r, i, false);
        }
        self.bots.save(bot).await?;
        Ok(())
    }

    // -- helpers -------------------------------------------------------------

    fn terminal(state: RolloutBotState) -> bool {
        matches!(
            state,
            RolloutBotState::Done
                | RolloutBotState::Failed
                | RolloutBotState::Skipped
                | RolloutBotState::RolledBack
        )
    }

    /// In-flight concurrency accounting: entries mid-roll that started
    /// from `Running` count against `maxDraining`; those already offline
    /// count against `maxOffline`. `NeedsUpdate` releases are exempt —
    /// they were already down (ci/docs/needsupdate-failsafe.md).
    fn caps(&self, r: &BotRollout) -> (u32, u32) {
        let mut in_flight = 0u32;
        let mut offline = 0u32;
        for e in &self.status_ref(r).bots {
            if e.state != RolloutBotState::Rolling || e.from_needs_update {
                continue;
            }
            in_flight += 1;
            if e.offline_started_at_unix_ms.is_some() {
                offline += 1;
            }
        }
        (in_flight, offline)
    }

    fn caps_allow(&self, r: &BotRollout) -> bool {
        let (in_flight, offline) = self.caps(r);
        let total = self.status_ref(r).total.max(1) as usize;
        let max_draining = r.spec.strategy.waves.resolve_max_draining(total);
        in_flight < max_draining && offline < r.spec.strategy.waves.max_offline
    }

    /// Login token bucket + jitter (docs/04 §4.7): returns `true` when
    /// `entry.ready_at` has passed (fires now), `false` otherwise (a
    /// requeue is recorded). The grant itself is stored on the entry so a
    /// scheduled slot survives passes — and a controller restart keeps
    /// the same scheduled login time.
    fn claim_ready(
        &self,
        r: &mut BotRollout,
        i: usize,
        now: i64,
        out: &mut ReconcileOutcome,
    ) -> bool {
        let ready = match self.status(r).bots[i].ready_at_unix_ms {
            Some(t) => Some(t),
            None => {
                let bot_name = self.status(r).bots[i].bot.clone();
                let grant = self.acquire_login(r, &bot_name, now);
                self.status(r).bots[i].ready_at_unix_ms = grant;
                grant
            }
        };
        match ready {
            Some(t) if t <= now => true,
            Some(t) => {
                let wait = Duration::from_millis((t - now) as u64);
                out.requeue_after = Some(out.requeue_after.map_or(wait, |d| d.min(wait)));
                false
            }
            None => {
                out.requeue_after = Some(self.cfg.poll);
                false
            }
        }
    }

    fn acquire_login(&self, r: &BotRollout, bot: &str, now: i64) -> Option<i64> {
        let jitter = Duration::from_secs(r.spec.strategy.waves.login_jitter_s as u64);
        let seed = bot_seed(bot) ^ bot_seed(&Self::name(r));
        self.bucket.lock().unwrap().try_acquire(now, jitter, seed)
    }

    /// Point a settled bot's next pod at `image` (docs/07 §7.3: the
    /// digest travels on `status.image`, spec stays untouched).
    async fn retag(&self, bot: &mut XenomitBot, image: &str) -> Result<(), RolloutError> {
        if let Some(st) = bot.status.as_mut() {
            st.image = Some(image.to_string());
        }
        self.bots.save(bot).await?;
        Ok(())
    }

    fn entry_for(name: &str, all: &[XenomitBot], wave: u32) -> RolloutBotEntry {
        let bot = all
            .iter()
            .find(|b| b.metadata.name.as_deref() == Some(name));
        match bot {
            Some(b) => {
                let st = b.status.as_ref();
                RolloutBotEntry {
                    bot: name.to_string(),
                    state: RolloutBotState::Pending,
                    wave,
                    from_image: st.and_then(|s| s.image.clone()),
                    from_needs_update: st.map(|s| s.phase) == Some(BotPhase::NeedsUpdate),
                    ..Default::default()
                }
            }
            None => RolloutBotEntry {
                bot: name.to_string(),
                state: RolloutBotState::Skipped,
                wave,
                detail: "not selected".into(),
                ..Default::default()
            },
        }
    }

    fn finish_entry(
        &self,
        r: &mut BotRollout,
        i: usize,
        state: RolloutBotState,
        outcome: Option<RolloutBotOutcome>,
        detail: &str,
        _now: i64,
    ) {
        {
            let e = &mut self.status(r).bots[i];
            e.state = state;
            if outcome.is_some() {
                e.outcome = outcome;
            }
            e.detail = detail.to_string();
            // The login grant was consumed — a later rollback acquires a
            // fresh token + jitter instead of reusing a stale slot.
            e.ready_at_unix_ms = None;
        }
        let ok = matches!(state, RolloutBotState::Done | RolloutBotState::RolledBack);
        self.emit_bot(r, i, ok);
    }

    fn emit_bot(&self, r: &BotRollout, i: usize, ok: bool) {
        let e = &self.status_ref(r).bots[i];
        self.events.emit(RolloutEvent::BotRolled {
            rollout: Self::name(r),
            bot_id: e.bot.clone(),
            ok,
            detail: e.detail.clone(),
        });
    }

    /// Spec-level interrupts shared by Canarying/Progressing: abort >
    /// rollback > pause. Returns `true` when the phase changed.
    fn spec_interrupts(&self, r: &mut BotRollout) -> bool {
        if r.spec.aborted {
            self.set_phase(r, RolloutPhase::Aborting, "aborted");
            return true;
        }
        if r.spec.rollback {
            self.set_phase(r, RolloutPhase::RollingBack, "rollback requested");
            return true;
        }
        if r.spec.paused {
            self.enter_pause(r, PauseCause::User);
            return true;
        }
        false
    }

    fn enter_pause(&self, r: &mut BotRollout, cause: PauseCause) {
        let resume_phase = self.status(r).phase;
        let st = self.status(r);
        if st.phase != RolloutPhase::Paused {
            st.pause_cause = Some(cause);
            st.resume_phase = Some(resume_phase);
        }
        self.set_phase(r, RolloutPhase::Paused, "paused");
    }

    fn fail(&self, r: &mut BotRollout, detail: &str) {
        self.status(r).completed_at_unix_ms = Some(self.clock.now_unix_ms());
        self.set_phase(r, RolloutPhase::Failed, detail);
    }

    /// Gate evaluation result; on failure applies `onGateFailure`.
    fn gate_failed(&self, r: &mut BotRollout, gate: &GateStatus) {
        self.events.emit(RolloutEvent::Gate {
            rollout: Self::name(r),
            gate: gate.name.clone(),
            passed: false,
            detail: gate.detail.clone(),
        });
        match r.spec.on_gate_failure {
            GateFailureAction::Pause => self.enter_pause(r, PauseCause::Gate),
            GateFailureAction::Rollback => {
                self.set_phase(r, RolloutPhase::RollingBack, "gate failed; rolling back")
            }
        }
    }

    /// Evaluate the gates over all completed roll attempts (docs/04 §4.1
    /// `status.gates`). Rates are permille of completed entries.
    async fn eval_gates(&self, r: &BotRollout) -> Vec<GateStatus> {
        let g = &r.spec.strategy.gates;
        let entries = &self.status_ref(r).bots;
        let completed: Vec<&RolloutBotEntry> = entries
            .iter()
            .filter(|e| e.outcome.is_some() && e.state != RolloutBotState::Skipped)
            .collect();
        let n = completed.len() as u64;
        let denom = n.max(1);
        let cnt = |pred: &dyn Fn(&RolloutBotEntry) -> bool| {
            completed.iter().filter(|e| pred(e)).count() as u64
        };
        let permille = |c: u64| c * 1000 / denom;

        let mut gates = Vec::new();

        let fr = cnt(&|e| e.outcome == Some(RolloutBotOutcome::FailedRestore));
        gates.push(GateStatus {
            name: "failed_restores".into(),
            passed: fr <= g.max_failed_restores as u64,
            detail: format!("{fr} failed restores (max {})", g.max_failed_restores),
        });

        let lf = cnt(&|e| e.outcome == Some(RolloutBotOutcome::LoginFailure));
        let lf_rate = permille(lf);
        gates.push(GateStatus {
            name: "login_failure_rate".into(),
            passed: lf_rate <= g.max_login_failure_rate_permille as u64,
            detail: format!(
                "{lf_rate}‰ over {n} (max {}‰)",
                g.max_login_failure_rate_permille
            ),
        });

        if let Some(max) = g.max_error_rate_permille {
            let er = cnt(&|e| {
                matches!(
                    e.outcome,
                    Some(RolloutBotOutcome::UpgradeAborted)
                        | Some(RolloutBotOutcome::Fault)
                        | Some(RolloutBotOutcome::Timeout)
                )
            });
            let rate = permille(er);
            gates.push(GateStatus {
                name: "error_rate".into(),
                passed: rate <= max as u64,
                detail: format!("{rate}‰ over {n} (max {max}‰)"),
            });
        }

        if let Some(max) = g.max_needs_human_permille {
            let rate = permille(lf);
            gates.push(GateStatus {
                name: "needs_human_rate".into(),
                passed: rate <= max as u64,
                detail: format!("{rate}‰ over {n} (max {max}‰)"),
            });
        }

        if let Some(max) = g.max_needs_update_permille {
            let nu = cnt(&|e| e.outcome == Some(RolloutBotOutcome::NeedsUpdateParked));
            let rate = permille(nu);
            gates.push(GateStatus {
                name: "needs_update_rate".into(),
                passed: rate <= max as u64,
                detail: format!("{rate}‰ over {n} (max {max}‰)"),
            });
        }

        if let Some(budget) = g.max_offline_window_ms {
            let over = cnt(&|e| e.offline_window_ms.is_some_and(|w| w > budget));
            gates.push(GateStatus {
                name: "offline_window".into(),
                passed: over == 0,
                detail: format!("{over} bots over {budget}ms budget"),
            });
        }

        if let Some(min) = g.min_reward_rate_ratio {
            let mut measured = 0u64;
            let mut under = 0u64;
            for e in completed
                .iter()
                .filter(|e| matches!(e.outcome, Some(RolloutBotOutcome::Success)))
            {
                if let Some(ratio) = self.metrics.reward_rate_ratio(&e.bot).await {
                    measured += 1;
                    if ratio < min {
                        under += 1;
                    }
                }
            }
            gates.push(GateStatus {
                name: "reward_rate".into(),
                passed: under == 0,
                detail: format!("{under}/{measured} under ratio {min}"),
            });
        }

        gates
    }

    /// done/total/p95 bookkeeping after every pass.
    fn refresh_counters(&self, r: &mut BotRollout) {
        let st = self.status(r);
        st.done = st.bots.iter().filter(|e| Self::terminal(e.state)).count() as u32;
        st.total = st.bots.len() as u32;
        let mut windows: Vec<u64> = st.bots.iter().filter_map(|e| e.offline_window_ms).collect();
        windows.sort_unstable();
        st.offline_window_p95_ms = if windows.is_empty() {
            None
        } else {
            let idx = (windows.len() * 95).div_ceil(100) - 1;
            Some(windows[idx.min(windows.len() - 1)])
        };
    }

    /// Read-only status access; an untouched rollout reads as a shared
    /// empty status.
    fn status_ref<'a>(&self, r: &'a BotRollout) -> &'a BotRolloutStatus {
        static EMPTY: std::sync::OnceLock<BotRolloutStatus> = std::sync::OnceLock::new();
        r.status
            .as_ref()
            .unwrap_or_else(|| EMPTY.get_or_init(BotRolloutStatus::default))
    }

    async fn bot_map(&self) -> Result<HashMap<String, XenomitBot>, RolloutError> {
        Ok(self
            .bots
            .list()
            .await?
            .into_iter()
            .map(|b| (b.metadata.name.clone().unwrap_or_default(), b))
            .collect())
    }
}
