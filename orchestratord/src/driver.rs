//! The operator loop: gateway events → controller callbacks, a
//! level-triggered reconcile tick, plus the rollout and proxy-pool
//! periodic passes. Same shape as the e2e rig's `Driver`
//! (e2e/tests/harness/rig.rs) — the daemon is the rig, minus assertions.
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use control_gateway::GatewayEvent;
use fleet_types::crds::XenomitBot;
use operator::locks::BotLocks;
use operator::proxy::ProxyController;
use operator::rollout::BotRolloutController;
use operator::traits::{
    BotDirectory, ProbeReport, ProxyEndpoint, ProxyError, ProxyProber, ProxyStore, RolloutStore,
};
use orchestrator_api::BotOps;
use orchestrator_api::repo::BotRepo;

/// Reconcile cadence for the level-triggered pass (matches the e2e rig).
const DRIVER_TICK: Duration = Duration::from_millis(75);
/// Rollout-controller pass interval — slower than the bot tick; rollouts
/// are minutes-scale work.
const ROLLOUT_TICK: Duration = Duration::from_secs(5);
/// Proxy-pool probe interval (docs/05 §5.4 ~60 s; dev keeps it snappier).
const PROXY_TICK: Duration = Duration::from_secs(30);

/// `BotDirectory` over the repo seam — the rollout and proxy controllers'
/// fleet view (`list`/`save` of whole CRs).
pub struct RepoBotDirectory(pub Arc<dyn BotRepo>);

#[async_trait]
impl BotDirectory for RepoBotDirectory {
    async fn list(&self) -> Result<Vec<XenomitBot>, ProxyError> {
        Ok(self.0.list(None).await)
    }

    async fn save(&self, bot: &XenomitBot) -> Result<(), ProxyError> {
        self.0
            .apply(bot.clone())
            .await
            .map_err(|e| ProxyError::Store(e.to_string()))
    }
}

/// A `ProxyProber` that answers "does a TCP connect to host:port
/// succeed" — enough for the mock proxy and honest for the k3d path
/// (pool proxies are reachable from the operator pod). A full CONNECT
/// handshake + exit-IP check is the production follow-up.
pub struct TcpProber;

#[async_trait]
impl ProxyProber for TcpProber {
    async fn probe(&self, endpoint: &ProxyEndpoint) -> ProbeReport {
        let target = format!("{}:{}", endpoint.host, endpoint.port);
        let started = std::time::Instant::now();
        match tokio::time::timeout(
            Duration::from_secs(3),
            tokio::net::TcpStream::connect(&target),
        )
        .await
        {
            Ok(Ok(_)) => ProbeReport {
                ok: true,
                latency_ms: Some(started.elapsed().as_millis() as u32),
                exit_ip: None,
                resolved_ips: vec![endpoint.host.clone()],
                error: None,
            },
            Ok(Err(e)) => ProbeReport {
                ok: false,
                latency_ms: None,
                exit_ip: None,
                resolved_ips: vec![],
                error: Some(e.to_string()),
            },
            Err(_) => ProbeReport {
                ok: false,
                latency_ms: None,
                exit_ip: None,
                resolved_ips: vec![],
                error: Some("probe timed out".into()),
            },
        }
    }
}

/// The per-bot driver: gateway events feed `on_phase_changed`/`on_drained`,
/// a fast level-triggered tick settles every bot, and `BotLocks` shared
/// with `Fleet` serializes all controller work per bot (docs/03 §3.1).
pub struct Driver {
    bots: Arc<dyn BotRepo>,
    ops: Arc<dyn BotOps>,
    locks: Arc<BotLocks>,
    rx: broadcast::Receiver<GatewayEvent>,
}

impl Driver {
    pub fn new(
        bots: Arc<dyn BotRepo>,
        ops: Arc<dyn BotOps>,
        locks: Arc<BotLocks>,
        rx: broadcast::Receiver<GatewayEvent>,
    ) -> Self {
        Driver {
            bots,
            ops,
            locks,
            rx,
        }
    }

    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(DRIVER_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                ev = self.rx.recv() => match ev {
                    Ok(ev) => self.apply(&ev).await,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                },
                _ = tick.tick() => {
                    for bot in self.bots.list(None).await {
                        if let Some(name) = bot.metadata.name.clone() {
                            self.pump(&name).await;
                        }
                    }
                }
            }
        }
    }

    /// Feed a gateway event into the controller, then settle the bot.
    async fn apply(&self, ev: &GatewayEvent) {
        match ev {
            GatewayEvent::PhaseChanged {
                bot_id,
                epoch,
                phase,
                detail,
            } => {
                {
                    let _gate = self.locks.gate(bot_id).await;
                    if let Some(mut bot) = self.bots.get(bot_id).await {
                        let _ = self
                            .ops
                            .on_phase_changed(&mut bot, *epoch, *phase, detail)
                            .await;
                        self.write_back(bot_id, bot).await;
                    }
                }
                self.pump(bot_id).await;
            }
            GatewayEvent::Drained {
                bot_id,
                epoch,
                snapshot_id,
                clean,
            } => {
                {
                    let _gate = self.locks.gate(bot_id).await;
                    if let Some(mut bot) = self.bots.get(bot_id).await {
                        let _ = self
                            .ops
                            .on_drained(&mut bot, *epoch, snapshot_id.clone(), *clean)
                            .await;
                        self.write_back(bot_id, bot).await;
                    }
                }
                self.pump(bot_id).await;
            }
            GatewayEvent::SessionClosed { bot_id, .. }
            | GatewayEvent::SessionOpened { bot_id, .. }
            | GatewayEvent::SnapshotCommitted { bot_id, .. }
            | GatewayEvent::Fenced { bot_id, .. }
            | GatewayEvent::MetricAlert { bot_id, .. } => {
                self.pump(bot_id).await;
            }
        }
    }

    /// Status-only write-back (same rule as `Fleet::reconcile_one`): spec
    /// and metadata belong to atomic `BotRepo::update` callers — merging a
    /// stale full copy would clobber them.
    async fn write_back(&self, name: &str, bot: XenomitBot) {
        let _ = self
            .bots
            .update(name, &mut move |b: &mut XenomitBot| {
                b.status = bot.status.clone()
            })
            .await;
    }

    /// Reconcile a bot until the CR stops changing (level-triggered — a
    /// no-change pass still leaves the next tick to retry requeues).
    /// The per-bot gate is held across load→passes→write-back so an
    /// API-driven reconcile can never interleave.
    async fn pump(&self, name: &str) {
        let _gate = self.locks.gate(name).await;
        for _ in 0..60 {
            let Some(mut bot) = self.bots.get(name).await else {
                return;
            };
            let before = serde_json::to_value(&bot).unwrap_or(Value::Null);
            if self.ops.reconcile(&mut bot).await.is_err() {
                return; // transient — next tick retries
            }
            if serde_json::to_value(&bot).unwrap_or(Value::Null) == before {
                return;
            }
            self.write_back(name, bot).await;
        }
    }
}

/// Periodic `BotRolloutController` passes over every non-terminal
/// `BotRollout` CR (docs/07 §7.3).
pub async fn rollout_driver<R, B, O, C, V, M, E>(ctl: BotRolloutController<R, B, O, C, V, M, E>)
where
    R: RolloutStore,
    B: BotDirectory,
    O: operator::traits::RolloutBotOps,
    C: operator::traits::Clock,
    V: operator::traits::ImageVerifier,
    M: operator::traits::MetricsFeed,
    E: operator::rollout::RolloutEventSink,
{
    use fleet_types::crds::RolloutPhase;
    let mut tick = tokio::time::interval(ROLLOUT_TICK);
    loop {
        tick.tick().await;
        let mut rollouts = ctl.rollouts.list().await.unwrap_or_default();
        for r in rollouts.iter_mut() {
            let terminal = matches!(
                r.status.as_ref().map(|s| s.phase),
                Some(RolloutPhase::Done)
                    | Some(RolloutPhase::Failed)
                    | Some(RolloutPhase::Aborted)
                    | Some(RolloutPhase::RolledBack)
            );
            if terminal {
                continue;
            }
            if ctl.reconcile(r).await.is_ok() {
                let _ = ctl.rollouts.save(r).await;
            }
        }
    }
}

/// Periodic `ProxyController` pass: probe every pool proxy, sync egress
/// identity, fail over dead proxies (docs/05 §5.4).
pub async fn proxy_driver<S, B, C, H>(ctl: ProxyController<S, TcpProber, B, C, H>)
where
    S: ProxyStore,
    B: BotDirectory,
    C: operator::traits::Clock,
    H: operator::traits::HandoffTrigger,
{
    let mut tick = tokio::time::interval(PROXY_TICK);
    loop {
        tick.tick().await;
        let _ = ctl.reconcile().await;
    }
}
