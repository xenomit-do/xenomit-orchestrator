//! `BotController` and its backend seams for the Xenomit fleet (docs/04
//! §4.2). Everything is written against traits so the whole lifecycle —
//! create, pause, delete, crash restore, planned handoff, fencing,
//! soft-delete — is unit-testable with fakes and without a cluster.

pub mod argocd;
pub mod backends;
pub mod controller;
pub mod fakes;
pub mod locks;
pub mod netpol;
pub mod policy;
pub mod proxy;
pub mod rollout;
pub mod secrets;
pub mod traits;

pub use argocd::ArgoTracking;
pub use controller::{BotController, ControllerConfig, ReconcileError, ReconcileOutcome};
pub use locks::{BotGate, BotLocks};
pub use netpol::{BotEgressSpec, InfraEgress, NetpolError, bot_egress_policy, bot_egress_yaml};
pub use policy::{CrashPolicy, CrashVerdict, bot_seed, retention_expired};
pub use proxy::{MemoryProxyStore, Pool, ProxyConfig, ProxyController, TcpProber};
pub use rollout::{
    BotRolloutController, LoginBucket, NoopSink, RolloutConfig, RolloutEvent, RolloutEventSink,
};
pub use secrets::MemorySecretStore;
pub use traits::{
    AllowAllVerifier, BotDirectory, BotPodSpec, BotSecretPaths, Clock, ControlDirectory,
    ControlError, HandoffTrigger, ImageVerifier, ManualClock, MetricsFeed, NoMetrics, PodBackend,
    PodObservation, PodState, ProbeReport, ProxyAssignment, ProxyEndpoint, ProxyError, ProxyPool,
    ProxyProber, ProxyStore, RolloutBotOps, RolloutError, RolloutStore, SecretError, SecretStore,
    SecretValue, SessionInfo, SystemClock, VerifyError, bot_secret_paths,
};
