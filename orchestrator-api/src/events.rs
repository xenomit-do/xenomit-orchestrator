//! Lifecycle events streamed to clients over SSE (`GET /api/v1/events`,
//! docs/06 §6.3). `FleetEvent` is the API-side union of gateway lifecycle
//! events and control-plane writes this API performs itself.

use control_gateway::GatewayEvent;
use fleet_types::epoch::Epoch;
use fleet_types::phase::BotPhase;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FleetEvent {
    SessionOpened {
        bot_id: String,
        epoch: Epoch,
    },
    SessionClosed {
        bot_id: String,
        epoch: Epoch,
        reason: String,
    },
    PhaseChanged {
        bot_id: String,
        epoch: Epoch,
        phase: BotPhase,
        detail: String,
    },
    Drained {
        bot_id: String,
        epoch: Epoch,
        snapshot_id: Option<String>,
        clean: bool,
    },
    SnapshotCommitted {
        bot_id: String,
        snapshot_id: String,
    },
    Fenced {
        bot_id: String,
        expected: Epoch,
        got: Epoch,
    },
    MetricAlert {
        bot_id: String,
        name: String,
        severity: String,
        message: String,
    },
    /// The API wrote a spec change (create/pause/resume/edit).
    SpecChanged {
        bot_id: String,
    },
    /// Soft-delete recorded; retention clock started.
    Deleted {
        bot_id: String,
    },
    /// Undelete inside the retention window.
    Undeleted {
        bot_id: String,
    },
    /// The API wrote a rollout spec change (create/pause/resume/abort/
    /// rollback). Admin-visible only.
    RolloutSpecChanged {
        rollout: String,
        action: String,
    },
    /// Controller progress, bridged from the rollout controller's sink
    /// (docs/07 §7.3). Admin-visible except `RolloutBot`, which carries a
    /// bot_id so the bot's owner sees their own roll.
    RolloutPhaseChanged {
        rollout: String,
        phase: String,
    },
    RolloutBot {
        rollout: String,
        bot_id: String,
        ok: bool,
        detail: String,
    },
    RolloutGate {
        rollout: String,
        gate: String,
        passed: bool,
        detail: String,
    },
    RolloutWave {
        rollout: String,
        wave: u32,
    },
}

impl FleetEvent {
    /// The bot this event concerns; rollout-level events have none.
    pub fn bot_id(&self) -> Option<&str> {
        match self {
            FleetEvent::SessionOpened { bot_id, .. }
            | FleetEvent::SessionClosed { bot_id, .. }
            | FleetEvent::PhaseChanged { bot_id, .. }
            | FleetEvent::Drained { bot_id, .. }
            | FleetEvent::SnapshotCommitted { bot_id, .. }
            | FleetEvent::Fenced { bot_id, .. }
            | FleetEvent::MetricAlert { bot_id, .. }
            | FleetEvent::SpecChanged { bot_id }
            | FleetEvent::Deleted { bot_id }
            | FleetEvent::Undeleted { bot_id }
            | FleetEvent::RolloutBot { bot_id, .. } => Some(bot_id),
            FleetEvent::RolloutSpecChanged { .. }
            | FleetEvent::RolloutPhaseChanged { .. }
            | FleetEvent::RolloutGate { .. }
            | FleetEvent::RolloutWave { .. } => None,
        }
    }
}

impl From<&GatewayEvent> for FleetEvent {
    fn from(e: &GatewayEvent) -> Self {
        match e {
            GatewayEvent::SessionOpened { bot_id, epoch } => FleetEvent::SessionOpened {
                bot_id: bot_id.clone(),
                epoch: *epoch,
            },
            GatewayEvent::SessionClosed {
                bot_id,
                epoch,
                reason,
            } => FleetEvent::SessionClosed {
                bot_id: bot_id.clone(),
                epoch: *epoch,
                reason: (*reason).into(),
            },
            GatewayEvent::PhaseChanged {
                bot_id,
                epoch,
                phase,
                detail,
            } => FleetEvent::PhaseChanged {
                bot_id: bot_id.clone(),
                epoch: *epoch,
                phase: *phase,
                detail: detail.clone(),
            },
            GatewayEvent::Drained {
                bot_id,
                epoch,
                snapshot_id,
                clean,
            } => FleetEvent::Drained {
                bot_id: bot_id.clone(),
                epoch: *epoch,
                snapshot_id: snapshot_id.as_ref().map(|s| s.0.clone()),
                clean: *clean,
            },
            GatewayEvent::SnapshotCommitted {
                bot_id,
                snapshot_id,
                ..
            } => FleetEvent::SnapshotCommitted {
                bot_id: bot_id.clone(),
                snapshot_id: snapshot_id.0.clone(),
            },
            GatewayEvent::Fenced {
                bot_id,
                expected,
                got,
            } => FleetEvent::Fenced {
                bot_id: bot_id.clone(),
                expected: *expected,
                got: *got,
            },
            GatewayEvent::MetricAlert {
                bot_id,
                name,
                severity,
                message,
                ..
            } => FleetEvent::MetricAlert {
                bot_id: bot_id.clone(),
                name: name.clone(),
                severity: severity.clone(),
                message: message.clone(),
            },
        }
    }
}

/// Forward gateway events onto the API's broadcast channel. Spawn once at
/// startup when a `Gateway` is wired in:
///
/// ```ignore
/// tokio::spawn(orchestrator_api::events::bridge_gateway_events(gw.subscribe(), tx));
/// ```
pub async fn bridge_gateway_events(
    mut rx: tokio::sync::broadcast::Receiver<GatewayEvent>,
    tx: tokio::sync::broadcast::Sender<FleetEvent>,
) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match rx.recv().await {
            Ok(ev) => {
                let _ = tx.send(FleetEvent::from(&ev));
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => return,
        }
    }
}

/// Feed `PhaseChanged` gateway events into a `plans::PhaseTimelines`
/// store — the metering input (docs/12 §12.5.1). Spawn alongside
/// `bridge_gateway_events` when usage metering is wired; `clock` stamps
/// each transition at observe-time so `ManualClock` tests meter
/// deterministically.
pub async fn record_gateway_timeline(
    mut rx: tokio::sync::broadcast::Receiver<GatewayEvent>,
    timelines: std::sync::Arc<dyn plans::PhaseTimelines>,
    clock: std::sync::Arc<dyn operator::traits::Clock>,
) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match rx.recv().await {
            Ok(GatewayEvent::PhaseChanged {
                bot_id,
                phase,
                detail,
                ..
            }) => {
                let ev = plans::PhaseEvent {
                    at_unix_ms: clock.now_unix_ms(),
                    phase,
                    detail: needs_human_detail(&detail),
                };
                if let Err(e) = timelines.record(&bot_id, ev).await {
                    tracing::warn!(%bot_id, error = %e, "phase timeline record failed");
                }
            }
            Ok(_) => {}
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => return,
        }
    }
}

/// `PhaseChanged.detail` is the NeedsHuman kind for parked bots — `quota`
/// distinguishes plan-driven pauses from user ones (docs/12 §12.5.1).
fn needs_human_detail(s: &str) -> Option<fleet_types::phase::NeedsHumanKind> {
    use fleet_types::phase::NeedsHumanKind as K;
    match s {
        "captcha" => Some(K::Captcha),
        "credentials" => Some(K::Credentials),
        "session_expired" => Some(K::SessionExpired),
        "reenter_credentials" => Some(K::ReenterCredentials),
        "quota" => Some(K::Quota),
        _ => None,
    }
}

/// `RolloutEventSink` that forwards controller progress onto the API's
/// SSE broadcast (docs/06 §6.3). Wire it as the rollout controller's
/// `events` field:
///
/// ```ignore
/// let sink = orchestrator_api::events::FleetEventSink::new(fleet.events.clone());
/// ```
pub struct FleetEventSink {
    tx: tokio::sync::broadcast::Sender<FleetEvent>,
}

impl FleetEventSink {
    pub fn new(tx: tokio::sync::broadcast::Sender<FleetEvent>) -> Self {
        FleetEventSink { tx }
    }
}

impl From<operator::rollout::RolloutEvent> for FleetEvent {
    fn from(e: operator::rollout::RolloutEvent) -> Self {
        use operator::rollout::RolloutEvent as R;
        match e {
            R::PhaseChanged { rollout, phase } => FleetEvent::RolloutPhaseChanged {
                rollout,
                phase: phase.as_str().to_string(),
            },
            R::BotRolled {
                rollout,
                bot_id,
                ok,
                detail,
            } => FleetEvent::RolloutBot {
                rollout,
                bot_id,
                ok,
                detail,
            },
            R::Gate {
                rollout,
                gate,
                passed,
                detail,
            } => FleetEvent::RolloutGate {
                rollout,
                gate,
                passed,
                detail,
            },
            R::WaveStarted { rollout, wave } => FleetEvent::RolloutWave { rollout, wave },
        }
    }
}

impl operator::rollout::RolloutEventSink for FleetEventSink {
    fn emit(&self, ev: operator::rollout::RolloutEvent) {
        let _ = self.tx.send(FleetEvent::from(ev));
    }
}
