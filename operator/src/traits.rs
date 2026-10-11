//! Backend traits `BotController` is written against (docs/04 §4.2). Every
//! reconciler decision is testable with fakes; real implementations live in
//! [`crate::backends`] (kube / local child processes) and the control
//! gateway crate (`ControlDirectory` over the session registry).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use fleet_types::crds::{Proxy, ProxyScheme, XenomitBot};
use fleet_types::epoch::Epoch;
use fleet_types::proto::GatewayCommand;

use crate::controller::ReconcileError;

/// Wall clock, abstracted for deterministic tests.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
    fn now_unix_ms(&self) -> i64 {
        self.now().timestamp_millis()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Manual clock for tests.
#[derive(Debug)]
pub struct ManualClock {
    unix_ms: AtomicI64,
}

impl ManualClock {
    pub fn new(unix_ms: i64) -> Self {
        ManualClock {
            unix_ms: AtomicI64::new(unix_ms),
        }
    }
    pub fn advance(&self, d: Duration) {
        self.unix_ms
            .fetch_add(d.as_millis() as i64, Ordering::SeqCst);
    }
    pub fn set(&self, unix_ms: i64) {
        self.unix_ms.store(unix_ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.unix_ms.load(Ordering::SeqCst))
            .expect("manual clock inside DateTime range")
    }
}

impl<T: Clock + ?Sized> Clock for std::sync::Arc<T> {
    fn now(&self) -> DateTime<Utc> {
        (**self).now()
    }
}

// ---------------------------------------------------------------------------
// Pod backend
// ---------------------------------------------------------------------------

/// What the controller asks a backend to run: one `botd` instance for a
/// specific epoch (docs/04 §4.2 pod template).
#[derive(Clone, Debug)]
pub struct BotPodSpec {
    pub bot_id: String,
    pub tenant: String,
    pub epoch: Epoch,
    pub image: String,
    /// Rendered botd config JSON (docs/02 §2.2); the backend decides how it
    /// reaches the process (ConfigMap volume / file).
    pub config_json: String,
    pub config_revision: u64,
    /// Extra labels merged onto the pod (`xenomit.io/bot|epoch|tenant` are
    /// always added).
    pub labels: std::collections::BTreeMap<String, String>,
    /// Deadlines the pod needs for terminationGracePeriodSeconds and the
    /// preStop drain (docs/03 §3.2).
    pub drain_deadline_s: u32,
    pub hard_deadline_s: u32,
    /// Resolved proxy for this pod once assigned (docs/05 §5.3); backends
    /// that do per-bot NetworkPolicy render it from `resolved_ip`/`port`.
    pub proxy: Option<ProxyAssignment>,
    /// Resource requests/limits as (cpu, memory) strings.
    pub resources: Option<(Option<String>, Option<String>)>,
    /// UID of the owning `XenomitBot` CR. Kube backends put it on the pod /
    /// ConfigMap / NetworkPolicy `ownerReferences` (Argo nests children
    /// under the CR; GC cascades on bot delete). `None` in dev/in-memory
    /// runs where the CR was never assigned a UID.
    pub owner_uid: Option<String>,
}

/// Observed lifecycle of a single pod/process, backend-agnostic.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PodObservation {
    /// Pod/process name (`<bot>-e<epoch>`).
    pub name: String,
    pub epoch: Epoch,
    pub state: PodState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum PodState {
    /// Created, not yet running.
    #[default]
    Pending,
    Running,
    /// Exited; `code` is `None` for a signal or when no code was reported.
    Exited {
        code: Option<i32>,
    },
    /// Present but state unreadable (API error, corrupt record).
    Unknown,
}

impl PodObservation {
    pub fn is_alive(&self) -> bool {
        matches!(self.state, PodState::Pending | PodState::Running)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend: {0}")]
    Other(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("kube: {0}")]
    Kube(String),
}

/// Runs botd instances: bare Pods on Kubernetes (docs/04 §4.2, ADR-04) or
/// child processes for the `local` dev backend (docs/02 §2.6).
#[async_trait]
pub trait PodBackend: Send + Sync {
    /// Idempotent: create the pod/process for `(bot_id, epoch)` if absent.
    async fn ensure_pod(&self, spec: &BotPodSpec) -> Result<(), BackendError>;
    /// Remove the pod/process for `(bot_id, epoch)`; unknown = Ok.
    async fn delete_pod(&self, bot_id: &str, epoch: Epoch) -> Result<(), BackendError>;
    /// All pods/processes currently known for a bot, every epoch.
    async fn list(&self, bot_id: &str) -> Result<Vec<PodObservation>, BackendError>;
}

#[async_trait]
impl<T: PodBackend + ?Sized> PodBackend for std::sync::Arc<T> {
    async fn ensure_pod(&self, spec: &BotPodSpec) -> Result<(), BackendError> {
        (**self).ensure_pod(spec).await
    }
    async fn delete_pod(&self, bot_id: &str, epoch: Epoch) -> Result<(), BackendError> {
        (**self).delete_pod(bot_id, epoch).await
    }
    async fn list(&self, bot_id: &str) -> Result<Vec<PodObservation>, BackendError> {
        (**self).list(bot_id).await
    }
}

// ---------------------------------------------------------------------------
// Secret store (Vault abstraction; docs/09 §9.2a, ADR-23)
// ---------------------------------------------------------------------------

/// A secret value. Zeroized on drop; `Debug` never prints the value
/// (docs/09 §9.1: never logged).
#[derive(Clone, zeroize::ZeroizeOnDrop)]
pub struct SecretValue(Vec<u8>);

impl SecretValue {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        SecretValue(bytes.into())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue(<redacted>)")
    }
}

/// Per-bot Vault artifacts the orchestrator provisions (docs/09 §9.2a,
/// ADR-23): one path + policy + role + ServiceAccount per bot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BotSecretPaths {
    /// `secret/data/bots/<id>/` prefix.
    pub prefix: String,
    pub account_path: String,
    pub proxy_path: String,
    pub control_token_path: String,
    pub instance_token_path: String,
    pub dosid_path: String,
    /// Vault policy + JWT role + k8s ServiceAccount names.
    pub policy_name: String,
    pub role_name: String,
    pub service_account: String,
}

/// Default path layout (docs/02 §2.2, docs/05 §5.6).
pub fn bot_secret_paths(bot_id: &str) -> BotSecretPaths {
    let prefix = format!("secret/data/bots/{bot_id}");
    BotSecretPaths {
        account_path: format!("{prefix}/account"),
        proxy_path: format!("{prefix}/proxy"),
        control_token_path: format!("{prefix}/control"),
        instance_token_path: format!("{prefix}/instance"),
        dosid_path: format!("{prefix}/dosid"),
        policy_name: format!("bot-{bot_id}"),
        role_name: format!("bot-{bot_id}"),
        service_account: format!("bot-{bot_id}"),
        prefix,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("secret store: {0}")]
    Other(String),
    /// Vault sealed / unreachable; the bot stays Pending and retries
    /// (docs/02 §2.2).
    #[error("secret store unavailable: {0}")]
    Unavailable(String),
}

/// Credential and token storage. The orchestrator may create/delete
/// `bots/*` paths, policies and roles, and write — but **not** read —
/// credential values after creation (docs/09 §9.1).
#[async_trait]
pub trait SecretStore: Send + Sync {
    /// Idempotent: ensure the bot's path prefix, policy, role and
    /// ServiceAccount exist. Returns the paths the botd config references.
    async fn provision_bot(&self, bot_id: &str) -> Result<BotSecretPaths, SecretError>;

    /// Write a value at a Vault path (KV v2 semantics).
    async fn write(&self, path: &str, value: SecretValue) -> Result<(), SecretError>;

    /// Does a path currently hold a value?
    async fn exists(&self, path: &str) -> Result<bool, SecretError>;

    /// Delete a single value at `path` (e.g. a removed proxy's credentials
    /// under `secret/data/proxies/<id>`). `false` = nothing was there.
    async fn delete_path(&self, path: &str) -> Result<bool, SecretError>;

    /// Copy the value at `from` to `to` (both KV v2 `secret/data/…` paths).
    /// The operator mirrors an assigned pool proxy's credentials into the
    /// bot's own subtree at spawn — bot policies may read only
    /// `secret/data/bots/<id>/*`, so the reference alone is not enough
    /// (docs/09 §9.2a, bot.hcl.tpl).
    async fn copy(&self, from: &str, to: &str) -> Result<(), SecretError>;

    /// Destroy the bot's credentials, role and policy **immediately**
    /// (docs/09 §9.2 deletion ordering). Snapshots/stats are not here.
    async fn destroy_bot(&self, bot_id: &str) -> Result<(), SecretError>;
}

#[async_trait]
impl<T: SecretStore + ?Sized> SecretStore for std::sync::Arc<T> {
    async fn provision_bot(&self, bot_id: &str) -> Result<BotSecretPaths, SecretError> {
        (**self).provision_bot(bot_id).await
    }
    async fn write(&self, path: &str, value: SecretValue) -> Result<(), SecretError> {
        (**self).write(path, value).await
    }
    async fn exists(&self, path: &str) -> Result<bool, SecretError> {
        (**self).exists(path).await
    }
    async fn delete_path(&self, path: &str) -> Result<bool, SecretError> {
        (**self).delete_path(path).await
    }
    async fn copy(&self, from: &str, to: &str) -> Result<(), SecretError> {
        (**self).copy(from, to).await
    }
    async fn destroy_bot(&self, bot_id: &str) -> Result<(), SecretError> {
        (**self).destroy_bot(bot_id).await
    }
}

// ---------------------------------------------------------------------------
// Control directory: gateway sessions, epochs, leases, command channel
// ---------------------------------------------------------------------------

/// What the controller learns about one live control stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInfo {
    pub bot_id: String,
    pub epoch: Epoch,
    /// Phase last reported on the stream, if any.
    pub reported_phase: Option<String>,
    /// Lease expiry (unix ms). `0` = session present, lease not yet renewed.
    pub lease_expires_unix_ms: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// No session for `(bot_id, epoch)` right now.
    #[error("no session for {bot_id} epoch {epoch}")]
    NoSession { bot_id: String, epoch: Epoch },
    /// The session's epoch is stale relative to the recorded epoch.
    #[error("session fenced")]
    Fenced,
    #[error("control channel: {0}")]
    Other(String),
}

/// The controller's view of the control gateway: which sessions exist,
/// which epoch may write, and a way to push `GatewayCommand`s down a bot's
/// stream (docs/03 §3.7, docs/04 §4.5).
///
/// Epoch authority model: `note_pending_epoch` mirrors the operator bumping
/// `status.epoch` when a successor pod is *armed*; `promote` mirrors the
/// authority transfer when `Activate` is sent. The gateway accepts a stream
/// for the serving epoch or the pending one, but only the serving epoch may
/// commit snapshots — so the draining old pod can still write its terminal
/// snapshot after the successor's epoch was recorded (docs/03 §3.2).
#[async_trait]
pub trait ControlDirectory: Send + Sync {
    /// Record that a pod for `epoch` has been created (armed successor or
    /// restart). Registers `epoch` as the pending/successor epoch.
    async fn note_pending_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), ControlError>;

    /// Transfer write/command authority to `epoch` (called when Activate is
    /// sent). Any session on an older epoch becomes fenced.
    async fn promote(&self, bot_id: &str, epoch: Epoch) -> Result<(), ControlError>;

    /// Session for an exact epoch, if connected.
    async fn session(
        &self,
        bot_id: &str,
        epoch: Epoch,
    ) -> Result<Option<SessionInfo>, ControlError>;

    /// Push a command to the session bound to `epoch`.
    async fn send(
        &self,
        bot_id: &str,
        epoch: Epoch,
        cmd: GatewayCommand,
    ) -> Result<(), ControlError>;

    /// `true` when no session older than `epoch` is still alive: either it
    /// closed (Drained / exited) or its lease expired past
    /// `lease_ttl + grace` (docs/03 §3.7). The operator waits for this
    /// before sending `Activate`.
    async fn predecessor_clear(&self, bot_id: &str, epoch: Epoch) -> Result<bool, ControlError>;
}

#[async_trait]
impl<T: ControlDirectory + ?Sized> ControlDirectory for std::sync::Arc<T> {
    async fn note_pending_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), ControlError> {
        (**self).note_pending_epoch(bot_id, epoch).await
    }
    async fn promote(&self, bot_id: &str, epoch: Epoch) -> Result<(), ControlError> {
        (**self).promote(bot_id, epoch).await
    }
    async fn session(
        &self,
        bot_id: &str,
        epoch: Epoch,
    ) -> Result<Option<SessionInfo>, ControlError> {
        (**self).session(bot_id, epoch).await
    }
    async fn send(
        &self,
        bot_id: &str,
        epoch: Epoch,
        cmd: GatewayCommand,
    ) -> Result<(), ControlError> {
        (**self).send(bot_id, epoch, cmd).await
    }
    async fn predecessor_clear(&self, bot_id: &str, epoch: Epoch) -> Result<bool, ControlError> {
        (**self).predecessor_clear(bot_id, epoch).await
    }
}

// ---------------------------------------------------------------------------
// Proxy pool, health probing, failover (docs/05 §5.4)
// ---------------------------------------------------------------------------

/// Errors from the proxy seams.
#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("proxy store: {0}")]
    Store(String),
    #[error("proxy prober: {0}")]
    Probe(String),
    #[error("bot directory: {0}")]
    Bots(String),
    /// Triggering the `proxy_change` handoff failed.
    #[error("handoff: {0}")]
    Handoff(String),
}

/// A `Proxy` CR endpoint as it is probed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyEndpoint {
    /// `None` when the scheme is only known to the secret (fixed mode).
    pub scheme: Option<ProxyScheme>,
    pub host: String,
    pub port: u16,
}

/// A resolved proxy for one bot — everything the pod spec, the botd config
/// render and the egress NetworkPolicy need. `name` is `None` for
/// `proxy.mode = fixed` (no `Proxy` CR; docs/05 §5.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyAssignment {
    /// `Proxy` CR name (`status.proxyRef`); `None` for fixed mode.
    pub name: Option<String>,
    pub scheme: Option<ProxyScheme>,
    pub host: String,
    pub port: u16,
    /// Vault path of the proxy credentials — a reference, never values
    /// (docs/05 §5.6).
    pub credentials_vault_path: String,
    /// Last known resolution of `host`; the egress policy's ipBlock source.
    /// Literal-IP hosts resolve to themselves.
    pub resolved_ip: Option<String>,
    /// Last observed exit IP (per-bot egress identity, docs/05 §5.4).
    pub exit_ip: Option<String>,
}

impl ProxyAssignment {
    /// The endpoint a [`ProxyProber`] checks.
    pub fn endpoint(&self) -> ProxyEndpoint {
        ProxyEndpoint {
            scheme: self.scheme,
            host: self.host.clone(),
            port: self.port,
        }
    }

    /// IP usable in an egress ipBlock: resolved address, or the host itself
    /// when it is a literal IP.
    pub fn egress_ip(&self) -> Option<String> {
        self.resolved_ip.clone().or_else(|| {
            self.host
                .parse::<std::net::IpAddr>()
                .ok()
                .map(|_| self.host.clone())
        })
    }
}

/// Result of one health probe (docs/05 §5.4: TCP connect → HTTPS HEAD →
/// latency + exit-IP lookup).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProbeReport {
    pub ok: bool,
    pub latency_ms: Option<u32>,
    /// Observed exit IP — detects silent rotation or a leaked non-proxy exit.
    pub exit_ip: Option<String>,
    /// The proxy host's resolved addresses.
    pub resolved_ips: Vec<String>,
    /// Failure detail for status conditions.
    pub error: Option<String>,
}

/// `Proxy` CR access (the pool's source of truth; docs/04 §4.1).
/// `save` persists spec+status — offline stores don't split the status
/// subresource.
#[async_trait]
pub trait ProxyStore: Send + Sync {
    async fn list(&self) -> Result<Vec<Proxy>, ProxyError>;
    async fn get(&self, name: &str) -> Result<Option<Proxy>, ProxyError>;
    async fn save(&self, proxy: &Proxy) -> Result<(), ProxyError>;
}

#[async_trait]
impl<T: ProxyStore + ?Sized> ProxyStore for std::sync::Arc<T> {
    async fn list(&self) -> Result<Vec<Proxy>, ProxyError> {
        (**self).list().await
    }
    async fn get(&self, name: &str) -> Result<Option<Proxy>, ProxyError> {
        (**self).get(name).await
    }
    async fn save(&self, proxy: &Proxy) -> Result<(), ProxyError> {
        (**self).save(proxy).await
    }
}

/// The pool's view of the fleet, for failover and capacity accounting.
#[async_trait]
pub trait BotDirectory: Send + Sync {
    async fn list(&self) -> Result<Vec<XenomitBot>, ProxyError>;
    async fn save(&self, bot: &XenomitBot) -> Result<(), ProxyError>;
}

#[async_trait]
impl<T: BotDirectory + ?Sized> BotDirectory for std::sync::Arc<T> {
    async fn list(&self) -> Result<Vec<XenomitBot>, ProxyError> {
        (**self).list().await
    }
    async fn save(&self, bot: &XenomitBot) -> Result<(), ProxyError> {
        (**self).save(bot).await
    }
}

/// Health probing, abstracted so tests drive every branch offline. The real
/// implementation dials through the proxy itself (docs/05 §5.4).
#[async_trait]
pub trait ProxyProber: Send + Sync {
    async fn probe(&self, endpoint: &ProxyEndpoint) -> ProbeReport;
}

#[async_trait]
impl<T: ProxyProber + ?Sized> ProxyProber for std::sync::Arc<T> {
    async fn probe(&self, endpoint: &ProxyEndpoint) -> ProbeReport {
        (**self).probe(endpoint).await
    }
}

/// What `BotController` asks the pool at spawn time (docs/05 §5.4).
#[async_trait]
pub trait ProxyPool: Send + Sync {
    /// Ensure `bot` has a pool assignment: keep the current `proxyRef` if it
    /// is still eligible (sticky), else pick a replacement honoring the
    /// selector, capacity and the per-tenant cooldown. `None` = nothing
    /// eligible right now; the caller retries.
    async fn assign(
        &self,
        bot: &XenomitBot,
        now_unix_ms: i64,
    ) -> Result<Option<ProxyAssignment>, ProxyError>;

    /// Resolve a `proxyRef` to its assignment without changing anything.
    async fn resolve(&self, proxy_name: &str) -> Result<Option<ProxyAssignment>, ProxyError>;

    /// Drop `bot`'s claim on its current `proxyRef`: frees the slot and
    /// starts the tenant cooldown (docs/05 §5.4).
    async fn release(&self, bot: &XenomitBot, now_unix_ms: i64) -> Result<(), ProxyError>;

    /// A bot reported its proxy unusable (exit 69). Counted like a probe
    /// failure and frees the bot's slot so the next `assign` avoids it.
    async fn report_failure(
        &self,
        bot: &XenomitBot,
        proxy_name: &str,
        now_unix_ms: i64,
    ) -> Result<(), ProxyError>;
}

#[async_trait]
impl<T: ProxyPool + ?Sized> ProxyPool for std::sync::Arc<T> {
    async fn assign(
        &self,
        bot: &XenomitBot,
        now_unix_ms: i64,
    ) -> Result<Option<ProxyAssignment>, ProxyError> {
        (**self).assign(bot, now_unix_ms).await
    }
    async fn resolve(&self, proxy_name: &str) -> Result<Option<ProxyAssignment>, ProxyError> {
        (**self).resolve(proxy_name).await
    }
    async fn release(&self, bot: &XenomitBot, now_unix_ms: i64) -> Result<(), ProxyError> {
        (**self).release(bot, now_unix_ms).await
    }
    async fn report_failure(
        &self,
        bot: &XenomitBot,
        proxy_name: &str,
        now_unix_ms: i64,
    ) -> Result<(), ProxyError> {
        (**self).report_failure(bot, proxy_name, now_unix_ms).await
    }
}

/// How the pool asks for a `proxy_change` handoff on failover (docs/05
/// §5.4). Implemented by [`crate::controller::BotController`].
#[async_trait]
pub trait HandoffTrigger: Send + Sync {
    /// Start a `proxy_change` handoff for `bot` (no-op unless it is Running
    /// without a handoff already in flight).
    async fn proxy_change_handoff(&self, bot: &mut XenomitBot) -> Result<(), ProxyError>;
}

#[async_trait]
impl<T: HandoffTrigger + ?Sized> HandoffTrigger for std::sync::Arc<T> {
    async fn proxy_change_handoff(&self, bot: &mut XenomitBot) -> Result<(), ProxyError> {
        (**self).proxy_change_handoff(bot).await
    }
}

// ---------------------------------------------------------------------------
// Rollout controller seams (docs/04 §4.2 RolloutController, docs/07 §7.3)
// ---------------------------------------------------------------------------

use fleet_types::crds::BotRollout;

/// `BotRollout` CR access (the rollout controller's source of truth).
/// Same shape as [`ProxyStore`]: `save` persists spec+status — offline
/// stores don't split the status subresource.
#[async_trait]
pub trait RolloutStore: Send + Sync {
    async fn list(&self) -> Result<Vec<BotRollout>, RolloutError>;
    async fn get(&self, name: &str) -> Result<Option<BotRollout>, RolloutError>;
    async fn save(&self, rollout: &BotRollout) -> Result<(), RolloutError>;
}

#[async_trait]
impl<T: RolloutStore + ?Sized> RolloutStore for std::sync::Arc<T> {
    async fn list(&self) -> Result<Vec<BotRollout>, RolloutError> {
        (**self).list().await
    }
    async fn get(&self, name: &str) -> Result<Option<BotRollout>, RolloutError> {
        (**self).get(name).await
    }
    async fn save(&self, rollout: &BotRollout) -> Result<(), RolloutError> {
        (**self).save(rollout).await
    }
}

/// Image-digest admission hook (docs/07 §7.2 sign-by-digest, docs/09 §9.2
/// Kyverno/cosign). The rollout controller asks it once before a rollout
/// leaves `Pending`; real signature verification is Phase 9 — today the
/// production impl admits digests the pipeline produced.
#[async_trait]
pub trait ImageVerifier: Send + Sync {
    async fn verify(&self, image: &str) -> Result<(), VerifyError>;
}

#[async_trait]
impl<T: ImageVerifier + ?Sized> ImageVerifier for std::sync::Arc<T> {
    async fn verify(&self, image: &str) -> Result<(), VerifyError> {
        (**self).verify(image).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// The digest fails admission (unsigned, wrong signer, wrong game
    /// version); the rollout fails closed.
    #[error("image {image} rejected: {reason}")]
    Denied { image: String, reason: String },
    /// The verifier itself could not answer (missing keys, unreachable).
    #[error("verifier unavailable: {0}")]
    Unavailable(String),
}

/// Admits every digest (docs/09: signature enforcement is Phase 9).
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllVerifier;

#[async_trait]
impl ImageVerifier for AllowAllVerifier {
    async fn verify(&self, _image: &str) -> Result<(), VerifyError> {
        Ok(())
    }
}

/// Fleet metrics the rollout gates read (docs/06 §6.6): the same
/// `xenomit.bot.rewards_per_hour` data the dashboards show. Implemented
/// against SigNoz in production; `None` means "no sample", which skips —
/// never fails — a gate.
#[async_trait]
pub trait MetricsFeed: Send + Sync {
    /// Post-roll reward rate as a fraction of the bot's pre-rollout
    /// baseline window (`minRewardRateRatio` gate input).
    async fn reward_rate_ratio(&self, bot_id: &str) -> Option<f64>;
}

#[async_trait]
impl<T: MetricsFeed + ?Sized> MetricsFeed for std::sync::Arc<T> {
    async fn reward_rate_ratio(&self, bot_id: &str) -> Option<f64> {
        (**self).reward_rate_ratio(bot_id).await
    }
}

/// No telemetry wired (offline/dev): every sample is absent.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoMetrics;

#[async_trait]
impl MetricsFeed for NoMetrics {
    async fn reward_rate_ratio(&self, _bot_id: &str) -> Option<f64> {
        None
    }
}

/// What the rollout controller asks of `BotController`: drive one bot's
/// reconcile loop and start the two rollout entry points (docs/07 §7.3).
/// Implemented by [`crate::controller::BotController`].
#[async_trait]
pub trait RolloutBotOps: Send + Sync {
    /// One reconcile pass over `bot` (level-triggered, idempotent).
    async fn reconcile(&self, bot: &mut XenomitBot) -> Result<(), ReconcileError>;

    /// Begin a planned `upgrade` handoff whose successor runs
    /// `target_image` instead of the channel image (Arm → Drain →
    /// Activate, docs/03 §3.2). No-op unless the bot is Running without a
    /// handoff already in flight.
    async fn request_upgrade_handoff(
        &self,
        bot: &mut XenomitBot,
        target_image: &str,
    ) -> Result<(), ReconcileError>;

    /// Release a bot parked in `NeedsUpdate` (exit 68) onto
    /// `target_image`: the bot is already down, so this just points the
    /// next pod at the target and re-enters `Provisioning`
    /// (ci/docs/needsupdate-failsafe.md).
    async fn release_for_rollout(
        &self,
        bot: &mut XenomitBot,
        target_image: &str,
    ) -> Result<(), ReconcileError>;
}

#[async_trait]
impl<T: RolloutBotOps + ?Sized> RolloutBotOps for std::sync::Arc<T> {
    async fn reconcile(&self, bot: &mut XenomitBot) -> Result<(), ReconcileError> {
        (**self).reconcile(bot).await
    }
    async fn request_upgrade_handoff(
        &self,
        bot: &mut XenomitBot,
        target_image: &str,
    ) -> Result<(), ReconcileError> {
        (**self).request_upgrade_handoff(bot, target_image).await
    }
    async fn release_for_rollout(
        &self,
        bot: &mut XenomitBot,
        target_image: &str,
    ) -> Result<(), ReconcileError> {
        (**self).release_for_rollout(bot, target_image).await
    }
}

/// Errors the rollout controller surfaces.
#[derive(Debug, thiserror::Error)]
pub enum RolloutError {
    #[error("rollout store: {0}")]
    Store(String),
    #[error(transparent)]
    Proxy(#[from] ProxyError),
    #[error(transparent)]
    Reconcile(#[from] ReconcileError),
    #[error(transparent)]
    Verify(#[from] VerifyError),
}
