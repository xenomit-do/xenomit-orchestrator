//! Lifecycle events emitted by the gateway on a broadcast channel
//! (docs/04 §4.5, docs/06 §6.3). The operator's status bridge and the REST
//! API's SSE stream subscribe here; nothing in this enum carries
//! credentials (docs/09 §9.1).

use fleet_types::epoch::Epoch;
use fleet_types::phase::BotPhase;
use fleet_types::snapshot::SnapshotId;

#[derive(Clone, Debug, PartialEq)]
pub enum GatewayEvent {
    /// A bot opened its control stream (`Hello` accepted).
    SessionOpened { bot_id: String, epoch: Epoch },
    /// A stream ended (clean exit, drop, or fenced).
    SessionClosed {
        bot_id: String,
        epoch: Epoch,
        reason: &'static str,
    },
    /// `PhaseChanged` from the bot.
    PhaseChanged {
        bot_id: String,
        epoch: Epoch,
        phase: BotPhase,
        detail: String,
    },
    /// Terminal drain finished; the operator may activate the successor.
    Drained {
        bot_id: String,
        epoch: Epoch,
        snapshot_id: Option<SnapshotId>,
        clean: bool,
    },
    /// A checkpoint or terminal snapshot was committed to the store.
    SnapshotCommitted {
        bot_id: String,
        epoch: Epoch,
        snapshot_id: SnapshotId,
    },
    /// A stream or write was epoch-fenced (docs/03 §3.7).
    Fenced {
        bot_id: String,
        expected: Epoch,
        got: Epoch,
    },
    /// `MetricAlert` passthrough (opaque to the gateway).
    MetricAlert {
        bot_id: String,
        epoch: Epoch,
        name: String,
        severity: String,
        message: String,
    },
}
