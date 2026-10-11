//! Fakes for unit-testing the controller (and reused by orchestrator-api
//! tests). Each fake implements its seam trait with scriptable state.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

use fleet_types::crds::{BotRollout, XenomitBot};
use fleet_types::epoch::Epoch;
use fleet_types::proto::GatewayCommand;
use fleet_types::snapshot::{
    BotSnapshot, SnapshotId, SnapshotMeta, SnapshotStore, StoreError, StoredSnapshot,
};

use crate::rollout::{RolloutEvent, RolloutEventSink};
use crate::traits::{
    BackendError, BotDirectory, BotPodSpec, ControlDirectory, ControlError, ImageVerifier,
    MetricsFeed, PodBackend, PodObservation, PodState, ProbeReport, ProxyEndpoint, ProxyError,
    ProxyProber, RolloutError, RolloutStore, SessionInfo, VerifyError,
};

// ---------------------------------------------------------------------------
// Pods
// ---------------------------------------------------------------------------

/// Scriptable `PodBackend`. Pods are created by `ensure_pod` in `Pending`
/// state; tests move them with `set_running`/`exit`.
#[derive(Default)]
pub struct FakePods {
    pods: Mutex<HashMap<(String, u64), PodObservation>>,
    /// ensure_pod calls recorded for assertions.
    pub created: Mutex<Vec<BotPodSpec>>,
}

impl FakePods {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(bot_id: &str, epoch: Epoch) -> (String, u64) {
        (bot_id.to_string(), epoch.get())
    }

    pub fn set_running(&self, bot_id: &str, epoch: Epoch) {
        let mut pods = self.pods.lock().unwrap();
        if let Some(p) = pods.get_mut(&Self::key(bot_id, epoch)) {
            p.state = PodState::Running;
        }
    }

    /// Simulate a pod exit with a code (or `None` = signal/crash).
    pub fn exit(&self, bot_id: &str, epoch: Epoch, code: Option<i32>) {
        let mut pods = self.pods.lock().unwrap();
        if let Some(p) = pods.get_mut(&Self::key(bot_id, epoch)) {
            p.state = PodState::Exited { code };
        }
    }

    pub fn pod(&self, bot_id: &str, epoch: Epoch) -> Option<PodObservation> {
        self.pods
            .lock()
            .unwrap()
            .get(&Self::key(bot_id, epoch))
            .cloned()
    }

    pub fn count(&self, bot_id: &str) -> usize {
        self.pods
            .lock()
            .unwrap()
            .keys()
            .filter(|(id, _)| id == bot_id)
            .count()
    }
}

#[async_trait]
impl PodBackend for FakePods {
    async fn ensure_pod(&self, spec: &BotPodSpec) -> Result<(), BackendError> {
        self.created.lock().unwrap().push(spec.clone());
        let mut pods = self.pods.lock().unwrap();
        pods.entry(Self::key(&spec.bot_id, spec.epoch))
            .or_insert_with(|| PodObservation {
                name: fleet_types::crds::pod_name(&spec.bot_id, spec.epoch),
                epoch: spec.epoch,
                state: PodState::Pending,
            });
        Ok(())
    }

    async fn delete_pod(&self, bot_id: &str, epoch: Epoch) -> Result<(), BackendError> {
        self.pods.lock().unwrap().remove(&Self::key(bot_id, epoch));
        Ok(())
    }

    async fn list(&self, bot_id: &str) -> Result<Vec<PodObservation>, BackendError> {
        let prefix = format!("{bot_id}-e");
        Ok(self
            .pods
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.name.starts_with(&prefix))
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Control directory
// ---------------------------------------------------------------------------

/// Fake `ControlDirectory`: sessions are opened explicitly by tests
/// (`connect`), commands are recorded in `sent`.
#[derive(Default)]
pub struct FakeControl {
    sessions: Mutex<HashMap<(String, u64), SessionInfo>>,
    /// bot_id -> pending (armed, not yet promoted) epoch.
    pending: Mutex<HashMap<String, u64>>,
    /// bot_id -> serving (write-authority) epoch.
    serving: Mutex<HashMap<String, u64>>,
    /// bot_id -> lease expiry overrides for `predecessor_clear` waits.
    expired: Mutex<Vec<(String, u64)>>,
    pub sent: Mutex<Vec<(String, Epoch, GatewayCommand)>>,
}

impl FakeControl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulate a bot opening its control stream at `epoch`.
    pub fn connect(&self, bot_id: &str, epoch: Epoch) {
        self.sessions.lock().unwrap().insert(
            Self::key(bot_id, epoch),
            SessionInfo {
                bot_id: bot_id.into(),
                epoch,
                reported_phase: None,
                lease_expires_unix_ms: i64::MAX,
            },
        );
    }

    /// Simulate stream close (pod exited / drained).
    pub fn disconnect(&self, bot_id: &str, epoch: Epoch) {
        self.sessions
            .lock()
            .unwrap()
            .remove(&Self::key(bot_id, epoch));
    }

    /// Mark a session's lease expired (node-loss simulation).
    pub fn expire_lease(&self, bot_id: &str, epoch: Epoch) {
        self.expired
            .lock()
            .unwrap()
            .push((bot_id.into(), epoch.get()));
        self.disconnect(bot_id, epoch);
    }

    fn key(bot_id: &str, epoch: Epoch) -> (String, u64) {
        (bot_id.to_string(), epoch.get())
    }

    /// Commands sent to `(bot_id, epoch)` in order.
    pub fn commands(&self, bot_id: &str, epoch: Epoch) -> Vec<GatewayCommand> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(b, e, _)| b == bot_id && *e == epoch)
            .map(|(_, _, c)| c.clone())
            .collect()
    }

    pub fn serving(&self, bot_id: &str) -> Option<Epoch> {
        self.serving.lock().unwrap().get(bot_id).map(|e| Epoch(*e))
    }
}

#[async_trait]
impl ControlDirectory for FakeControl {
    async fn note_pending_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), ControlError> {
        self.pending
            .lock()
            .unwrap()
            .insert(bot_id.into(), epoch.get());
        Ok(())
    }

    async fn promote(&self, bot_id: &str, epoch: Epoch) -> Result<(), ControlError> {
        self.pending.lock().unwrap().remove(bot_id);
        self.serving
            .lock()
            .unwrap()
            .insert(bot_id.into(), epoch.get());
        Ok(())
    }

    async fn session(
        &self,
        bot_id: &str,
        epoch: Epoch,
    ) -> Result<Option<SessionInfo>, ControlError> {
        Ok(self
            .sessions
            .lock()
            .unwrap()
            .get(&Self::key(bot_id, epoch))
            .cloned())
    }

    async fn send(
        &self,
        bot_id: &str,
        epoch: Epoch,
        cmd: GatewayCommand,
    ) -> Result<(), ControlError> {
        if !self
            .sessions
            .lock()
            .unwrap()
            .contains_key(&Self::key(bot_id, epoch))
        {
            return Err(ControlError::NoSession {
                bot_id: bot_id.into(),
                epoch,
            });
        }
        self.sent.lock().unwrap().push((bot_id.into(), epoch, cmd));
        Ok(())
    }

    async fn predecessor_clear(&self, bot_id: &str, epoch: Epoch) -> Result<bool, ControlError> {
        let sessions = self.sessions.lock().unwrap();
        let expired = self.expired.lock().unwrap();
        Ok(!sessions.values().any(|s| {
            s.bot_id == bot_id
                && s.epoch < epoch
                && !expired.contains(&(bot_id.into(), s.epoch.get()))
        }))
    }
}

// ---------------------------------------------------------------------------
// Proxy probing + bot directory
// ---------------------------------------------------------------------------

/// Scriptable `ProxyProber`: reports are keyed `"host:port"`; unknown
/// endpoints answer with `default` (a clean probe).
pub struct FakeProber {
    reports: Mutex<HashMap<String, ProbeReport>>,
    pub default_report: Mutex<ProbeReport>,
}

impl Default for FakeProber {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeProber {
    /// Unconfigured endpoints answer a clean probe; `set` overrides.
    pub fn new() -> Self {
        FakeProber {
            reports: Mutex::new(HashMap::new()),
            default_report: Mutex::new(ProbeReport {
                ok: true,
                latency_ms: Some(50),
                exit_ip: None,
                resolved_ips: vec![],
                error: None,
            }),
        }
    }

    fn key(ep: &ProxyEndpoint) -> String {
        format!("{}:{}", ep.host, ep.port)
    }

    /// What `probe` should return for this endpoint until changed.
    pub fn set(&self, ep: &ProxyEndpoint, report: ProbeReport) {
        self.reports.lock().unwrap().insert(Self::key(ep), report);
    }
}

#[async_trait]
impl ProxyProber for FakeProber {
    async fn probe(&self, endpoint: &ProxyEndpoint) -> ProbeReport {
        self.reports
            .lock()
            .unwrap()
            .get(&Self::key(endpoint))
            .cloned()
            .unwrap_or_else(|| self.default_report.lock().unwrap().clone())
    }
}

/// In-memory `BotDirectory` for `ProxyController` tests.
#[derive(Default)]
pub struct FakeBotDirectory {
    bots: Mutex<HashMap<String, XenomitBot>>,
}

impl FakeBotDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed(&self, bot: &XenomitBot) {
        let name = bot.metadata.name.clone().unwrap_or_default();
        self.bots.lock().unwrap().insert(name, bot.clone());
    }

    pub fn get_sync(&self, name: &str) -> Option<XenomitBot> {
        self.bots.lock().unwrap().get(name).cloned()
    }
}

#[async_trait]
impl BotDirectory for FakeBotDirectory {
    async fn list(&self) -> Result<Vec<XenomitBot>, ProxyError> {
        let mut v: Vec<_> = self.bots.lock().unwrap().values().cloned().collect();
        v.sort_by_key(|b| b.metadata.name.clone().unwrap_or_default());
        Ok(v)
    }

    async fn save(&self, bot: &XenomitBot) -> Result<(), ProxyError> {
        let name = bot.metadata.name.clone().unwrap_or_default();
        self.bots.lock().unwrap().insert(name, bot.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

/// Minimal in-memory `SnapshotStore` for controller tests. The full
/// in-memory implementation lives in `control-gateway`; this one exists so
/// `operator` unit tests don't depend on it.
#[derive(Default)]
pub struct FakeSnapshots {
    epochs: Mutex<HashMap<String, u64>>,
    snapshots: Mutex<HashMap<String, StoredSnapshot>>,
    seq: Mutex<u64>,
    pub soft_deleted: Mutex<Vec<String>>,
}

impl FakeSnapshots {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stored(&self, bot_id: &str) -> Vec<StoredSnapshot> {
        let mut v: Vec<_> = self
            .snapshots
            .lock()
            .unwrap()
            .values()
            .filter(|s| s.meta.bot_id == bot_id)
            .cloned()
            .collect();
        v.sort_by_key(|s| (s.meta.taken_at_unix_ms, s.meta.seq));
        v
    }
}

#[async_trait]
impl SnapshotStore for FakeSnapshots {
    async fn record_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), StoreError> {
        let mut epochs = self.epochs.lock().unwrap();
        let cur = epochs.entry(bot_id.into()).or_insert(0);
        if epoch.get() > *cur {
            *cur = epoch.get();
        }
        Ok(())
    }

    async fn current_epoch(&self, bot_id: &str) -> Result<Option<Epoch>, StoreError> {
        Ok(self.epochs.lock().unwrap().get(bot_id).map(|e| Epoch(*e)))
    }

    async fn put(&self, snapshot: &BotSnapshot) -> Result<SnapshotId, StoreError> {
        let cur = self
            .epochs
            .lock()
            .unwrap()
            .get(&snapshot.bot_id)
            .copied()
            .unwrap_or(0);
        if snapshot.epoch.get() != cur {
            return Err(StoreError::Fenced(fleet_types::epoch::Fenced {
                expected: Epoch(cur),
                got: snapshot.epoch,
            }));
        }
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        let id = SnapshotId(format!("snap_{seq:06}"));
        let payload = snapshot.encode()?;
        let meta = SnapshotMeta {
            id: id.clone(),
            bot_id: snapshot.bot_id.clone(),
            epoch: snapshot.epoch,
            seq: snapshot.seq,
            quality: snapshot.quality,
            reason: snapshot.reason,
            taken_at_unix_ms: snapshot.taken_at_unix_ms,
            size_bytes: payload.len() as u64,
            sha256: String::new(),
            deleted_at_unix_ms: None,
        };
        self.snapshots.lock().unwrap().insert(
            id.0.clone(),
            StoredSnapshot {
                meta,
                snapshot: snapshot.clone(),
            },
        );
        Ok(id)
    }

    async fn get(&self, id: &SnapshotId) -> Result<StoredSnapshot, StoreError> {
        self.snapshots
            .lock()
            .unwrap()
            .get(&id.0)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(id.clone()))
    }

    async fn latest(
        &self,
        bot_id: &str,
        quality: Option<fleet_types::SnapshotQuality>,
    ) -> Result<Option<StoredSnapshot>, StoreError> {
        Ok(self
            .stored(bot_id)
            .into_iter()
            .rfind(|s| quality.is_none_or(|q| s.meta.quality == q)))
    }

    async fn list(
        &self,
        bot_id: &str,
        include_deleted: bool,
    ) -> Result<Vec<SnapshotMeta>, StoreError> {
        let mut v: Vec<_> = self
            .stored(bot_id)
            .into_iter()
            .map(|s| s.meta)
            .filter(|m| include_deleted || m.deleted_at_unix_ms.is_none())
            .collect();
        v.reverse();
        Ok(v)
    }

    async fn soft_delete(&self, bot_id: &str, at_unix_ms: i64) -> Result<(), StoreError> {
        self.soft_deleted.lock().unwrap().push(bot_id.into());
        let mut snaps = self.snapshots.lock().unwrap();
        for s in snaps.values_mut() {
            if s.meta.bot_id == bot_id {
                s.meta.deleted_at_unix_ms = Some(at_unix_ms);
            }
        }
        Ok(())
    }

    async fn undelete(&self, bot_id: &str) -> Result<(), StoreError> {
        self.soft_deleted.lock().unwrap().retain(|b| b != bot_id);
        let mut snaps = self.snapshots.lock().unwrap();
        for s in snaps.values_mut() {
            if s.meta.bot_id == bot_id {
                s.meta.deleted_at_unix_ms = None;
            }
        }
        Ok(())
    }

    async fn purge(&self, before_unix_ms: i64) -> Result<u64, StoreError> {
        let mut snaps = self.snapshots.lock().unwrap();
        let before = snaps.len();
        snaps.retain(|_, s| s.meta.deleted_at_unix_ms.is_none_or(|t| t > before_unix_ms));
        Ok((before - snaps.len()) as u64)
    }

    async fn gc(&self, bot_id: &str, keep_clean: usize) -> Result<u64, StoreError> {
        let kept: Vec<String> = {
            let all = self.stored(bot_id);
            let mut keep: Vec<String> = all
                .iter()
                .filter(|s| s.meta.quality == fleet_types::SnapshotQuality::Checkpoint)
                .map(|s| s.meta.id.0.clone())
                .take(1)
                .collect();
            keep.extend(
                all.iter()
                    .filter(|s| s.meta.quality != fleet_types::SnapshotQuality::Checkpoint)
                    .map(|s| s.meta.id.0.clone())
                    .rev()
                    .take(keep_clean),
            );
            keep
        };
        let mut snaps = self.snapshots.lock().unwrap();
        let before = snaps.len();
        snaps.retain(|_, s| s.meta.bot_id != bot_id || kept.contains(&s.meta.id.0));
        Ok((before - snaps.len()) as u64)
    }
}

// ---------------------------------------------------------------------------
// Rollout seams (docs/04 §4.2 RolloutController, docs/07 §7.3)
// ---------------------------------------------------------------------------

/// In-memory `RolloutStore`.
#[derive(Default)]
pub struct FakeRollouts {
    rollouts: Mutex<HashMap<String, BotRollout>>,
}

impl FakeRollouts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed(&self, rollout: &BotRollout) {
        let name = rollout.metadata.name.clone().unwrap_or_default();
        self.rollouts.lock().unwrap().insert(name, rollout.clone());
    }

    pub fn get_sync(&self, name: &str) -> Option<BotRollout> {
        self.rollouts.lock().unwrap().get(name).cloned()
    }
}

#[async_trait]
impl RolloutStore for FakeRollouts {
    async fn list(&self) -> Result<Vec<BotRollout>, RolloutError> {
        let mut v: Vec<_> = self.rollouts.lock().unwrap().values().cloned().collect();
        v.sort_by_key(|r| r.metadata.name.clone().unwrap_or_default());
        Ok(v)
    }

    async fn get(&self, name: &str) -> Result<Option<BotRollout>, RolloutError> {
        Ok(self.rollouts.lock().unwrap().get(name).cloned())
    }

    async fn save(&self, rollout: &BotRollout) -> Result<(), RolloutError> {
        let name = rollout.metadata.name.clone().unwrap_or_default();
        self.rollouts.lock().unwrap().insert(name, rollout.clone());
        Ok(())
    }
}

/// Scriptable `ImageVerifier`: images in `denied` fail closed with the
/// stored reason; everything else is admitted. `calls` records what was
/// asked.
#[derive(Default)]
pub struct FakeVerifier {
    denied: Mutex<HashMap<String, String>>,
    pub calls: Mutex<Vec<String>>,
}

impl FakeVerifier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reject `image` with `reason` until `allow` clears it.
    pub fn deny(&self, image: &str, reason: &str) {
        self.denied
            .lock()
            .unwrap()
            .insert(image.into(), reason.into());
    }

    pub fn allow(&self, image: &str) {
        self.denied.lock().unwrap().remove(image);
    }
}

#[async_trait]
impl ImageVerifier for FakeVerifier {
    async fn verify(&self, image: &str) -> Result<(), VerifyError> {
        self.calls.lock().unwrap().push(image.into());
        if let Some(reason) = self.denied.lock().unwrap().get(image) {
            return Err(VerifyError::Denied {
                image: image.into(),
                reason: reason.clone(),
            });
        }
        Ok(())
    }
}

/// Scriptable `MetricsFeed`: `set` fixes a bot's reward-rate ratio;
/// unset bots report no sample (the gate skips them, never fails).
#[derive(Default)]
pub struct FakeMetrics {
    ratios: Mutex<HashMap<String, f64>>,
}

impl FakeMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, bot_id: &str, ratio: f64) {
        self.ratios.lock().unwrap().insert(bot_id.into(), ratio);
    }
}

#[async_trait]
impl MetricsFeed for FakeMetrics {
    async fn reward_rate_ratio(&self, bot_id: &str) -> Option<f64> {
        self.ratios.lock().unwrap().get(bot_id).copied()
    }
}

/// Collects `RolloutEvent`s in order for assertions.
#[derive(Default)]
pub struct FakeEvents {
    pub events: Mutex<Vec<RolloutEvent>>,
}

impl FakeEvents {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn all(&self) -> Vec<RolloutEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl RolloutEventSink for FakeEvents {
    fn emit(&self, ev: RolloutEvent) {
        self.events.lock().unwrap().push(ev);
    }
}
