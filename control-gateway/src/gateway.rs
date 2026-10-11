//! The control-plane gateway (docs/04 §4.5): one bidirectional gRPC stream
//! per bot, epoch-fenced writes into [`SnapshotStore`], per-bot leases and
//! the [`ControlDirectory`] implementation the operator drives.
//!
//! Session acceptance (docs/03 §3.7): a `Hello` is accepted when its epoch
//! is the *serving* epoch or the *pending* (armed successor) epoch recorded
//! by [`ControlDirectory::note_pending_epoch`]. Anything else gets `Fenced`
//! and the stream closes — the bot exits 67 without touching the game.
//!
//! Write authority: snapshot commits go through the epoch-conditional
//! `SnapshotStore::put`. Because `record_epoch` follows *promotion* (the
//! operator bumps the recorded epoch when `Activate` is sent), the draining
//! predecessor keeps write authority until the successor is activated.

use async_trait::async_trait;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use fleet_types::epoch::Epoch;
use fleet_types::phase::BotPhase;
use fleet_types::proto::fleet_control_server::{FleetControl, FleetControlServer};
use fleet_types::proto::{ClientEvent, GatewayCommand, Hello, client_event, gateway_command};
use fleet_types::snapshot::{BotSnapshot, SnapshotId, SnapshotStore, StoreError};

use operator::traits::{Clock, ControlDirectory, ControlError, SessionInfo, SystemClock};

use crate::events::GatewayEvent;

/// Tuning for [`Gateway`].
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Lease TTL (docs/03 §3.7: 30 s).
    pub lease_ttl: Duration,
    /// Extra wait past lease expiry before a predecessor counts as clear
    /// ("lease_ttl + grace").
    pub lease_grace: Duration,
    /// Outbound command buffer per session.
    pub outbound_buffer: usize,
    /// Broadcast capacity for [`GatewayEvent`]s.
    pub event_buffer: usize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        GatewayConfig {
            lease_ttl: Duration::from_millis(fleet_types::epoch::DEFAULT_LEASE_TTL_MS),
            lease_grace: Duration::from_secs(30),
            outbound_buffer: 64,
            event_buffer: 512,
        }
    }
}

#[derive(Clone, Default)]
struct Authority {
    /// Armed successor epoch; accepts sessions but may not commit snapshots.
    pending: Option<Epoch>,
    /// Epoch with write/command authority.
    serving: Option<Epoch>,
}

struct SessionState {
    info: SessionInfo,
    tx: mpsc::Sender<GatewayCommand>,
    last_lease_seq: u64,
    /// `Activate` is idempotent per `(bot, epoch)` session: once pushed,
    /// later duplicates — a reconcile retry, a racing second pass — are
    /// absorbed here instead of re-arming the pod's one-shot activation.
    /// A replacement session (flap/reconnect) starts false again so the
    /// retry reaches the new stream.
    activate_sent: bool,
}

#[derive(Default)]
struct Inner {
    sessions: HashMap<(String, u64), SessionState>,
    authority: HashMap<String, Authority>,
}

/// Cheap to clone; all state is shared.
pub struct Gateway {
    inner: Arc<Mutex<Inner>>,
    store: Arc<dyn SnapshotStore>,
    clock: Arc<dyn Clock>,
    cfg: GatewayConfig,
    events: broadcast::Sender<GatewayEvent>,
}

impl Clone for Gateway {
    fn clone(&self) -> Self {
        Gateway {
            inner: self.inner.clone(),
            store: self.store.clone(),
            clock: self.clock.clone(),
            cfg: self.cfg.clone(),
            events: self.events.clone(),
        }
    }
}

impl Gateway {
    pub fn new(store: Arc<dyn SnapshotStore>) -> Self {
        Self::with_clock(store, Arc::new(SystemClock), GatewayConfig::default())
    }

    pub fn with_clock(
        store: Arc<dyn SnapshotStore>,
        clock: Arc<dyn Clock>,
        cfg: GatewayConfig,
    ) -> Self {
        let (events, _) = broadcast::channel(cfg.event_buffer);
        Gateway {
            inner: Arc::new(Mutex::new(Inner::default())),
            store,
            clock,
            cfg,
            events,
        }
    }

    /// Subscribe to lifecycle events (status bridge, SSE).
    pub fn subscribe(&self) -> broadcast::Receiver<GatewayEvent> {
        self.events.subscribe()
    }

    /// The tonic service for `tonic::transport::Server::builder().add_service(..)`.
    pub fn into_service(self) -> FleetControlServer<Gateway> {
        FleetControlServer::new(self)
    }

    fn now(&self) -> i64 {
        self.clock.now_unix_ms()
    }

    fn authority(&self, bot_id: &str) -> Authority {
        self.inner
            .lock()
            .unwrap()
            .authority
            .get(bot_id)
            .cloned()
            .unwrap_or_default()
    }

    fn emit(&self, ev: GatewayEvent) {
        // No subscribers is fine.
        let _ = self.events.send(ev);
    }

    /// Push a command into a live session, ignoring a missing one (used for
    /// replies; the operator's `send` surfaces `NoSession`). `Activate` is
    /// delivered at most once per session — duplicates return `Ok` because
    /// the pod already has everything it needs (docs/03 §3.7).
    fn push(&self, bot_id: &str, epoch: Epoch, cmd: GatewayCommand) -> Result<(), ControlError> {
        let is_activate = matches!(cmd.command, Some(gateway_command::Command::Activate(_)));
        let mut inner = self.inner.lock().unwrap();
        let Some(session) = inner.sessions.get_mut(&(bot_id.to_string(), epoch.get())) else {
            return Err(ControlError::NoSession {
                bot_id: bot_id.into(),
                epoch,
            });
        };
        if is_activate && session.activate_sent {
            return Ok(());
        }
        session
            .tx
            .try_send(cmd)
            .map_err(|_| ControlError::Other("session buffer full".into()))?;
        if is_activate {
            session.activate_sent = true;
        }
        Ok(())
    }

    /// Register an accepted session. A pre-existing session for the same
    /// `(bot, epoch)` is replaced (its sender dropped → old stream ends).
    fn open_session(&self, bot_id: &str, epoch: Epoch, tx: mpsc::Sender<GatewayCommand>) {
        let mut inner = self.inner.lock().unwrap();
        inner.sessions.insert(
            (bot_id.to_string(), epoch.get()),
            SessionState {
                info: SessionInfo {
                    bot_id: bot_id.into(),
                    epoch,
                    reported_phase: None,
                    lease_expires_unix_ms: self.now() + self.cfg.lease_ttl.as_millis() as i64,
                },
                tx,
                last_lease_seq: 0,
                activate_sent: false,
            },
        );
        self.emit(GatewayEvent::SessionOpened {
            bot_id: bot_id.into(),
            epoch,
        });
    }

    /// Remove a session on stream end/drop and notify.
    pub fn close_session(&self, bot_id: &str, epoch: Epoch, reason: &'static str) {
        let removed = self
            .inner
            .lock()
            .unwrap()
            .sessions
            .remove(&(bot_id.to_string(), epoch.get()))
            .is_some();
        if removed {
            self.emit(GatewayEvent::SessionClosed {
                bot_id: bot_id.into(),
                epoch,
                reason,
            });
        }
    }

    /// Process one inbound `ClientEvent` for an open session. Returns
    /// `false` when the stream should close (fencing, protocol errors).
    /// Errors surface as `Status` only for decode failures.
    pub async fn handle_client_event(
        &self,
        bot_id: &str,
        session_epoch: Epoch,
        ev: ClientEvent,
    ) -> Result<bool, Status> {
        let now = self.now();
        let Some(event) = ev.event else {
            return Ok(true);
        };
        use client_event::Event as E;
        match event {
            E::Hello(_) => {
                // Second Hello is a protocol violation.
                Ok(false)
            }
            E::LeaseRenew(m) => {
                if !self.epoch_matches(bot_id, session_epoch, Epoch(m.epoch)) {
                    return Ok(false);
                }
                let mut inner = self.inner.lock().unwrap();
                if let Some(s) = inner.sessions.get_mut(&(bot_id.to_string(), m.epoch)) {
                    // Ignore reordered heartbeats (docs/03 §3.7).
                    if m.lease_seq > s.last_lease_seq {
                        s.last_lease_seq = m.lease_seq;
                        s.info.lease_expires_unix_ms = now + self.cfg.lease_ttl.as_millis() as i64;
                    }
                }
                Ok(true)
            }
            E::Checkpoint(m) => {
                if !self.epoch_matches(bot_id, session_epoch, Epoch(m.epoch)) {
                    return Ok(false);
                }
                self.commit_snapshot(bot_id, session_epoch, &m.snapshot, None)
                    .await
            }
            E::PutSnapshot(m) => {
                if !self.epoch_matches(bot_id, session_epoch, Epoch(m.epoch)) {
                    return Ok(false);
                }
                let quality = match m.quality.as_str() {
                    "degraded" => fleet_types::SnapshotQuality::Degraded,
                    _ => fleet_types::SnapshotQuality::Clean,
                };
                self.commit_snapshot(bot_id, session_epoch, &m.snapshot, Some(quality))
                    .await
            }
            E::PhaseChanged(m) => {
                if !self.epoch_matches(bot_id, session_epoch, Epoch(m.epoch)) {
                    return Ok(false);
                }
                let phase = BotPhase::parse(&m.phase).unwrap_or_default();
                {
                    let mut inner = self.inner.lock().unwrap();
                    if let Some(s) = inner.sessions.get_mut(&(bot_id.to_string(), m.epoch)) {
                        s.info.reported_phase = Some(m.phase.clone());
                    }
                }
                self.emit(GatewayEvent::PhaseChanged {
                    bot_id: bot_id.into(),
                    epoch: session_epoch,
                    phase,
                    detail: m.detail,
                });
                Ok(true)
            }
            E::MetricAlert(m) => {
                if !self.epoch_matches(bot_id, session_epoch, Epoch(m.epoch)) {
                    return Ok(false);
                }
                self.emit(GatewayEvent::MetricAlert {
                    bot_id: bot_id.into(),
                    epoch: session_epoch,
                    name: m.name,
                    severity: m.severity,
                    message: m.message,
                });
                Ok(true)
            }
            E::Drained(m) => {
                if !self.epoch_matches(bot_id, session_epoch, Epoch(m.epoch)) {
                    return Ok(false);
                }
                self.emit(GatewayEvent::Drained {
                    bot_id: bot_id.into(),
                    epoch: session_epoch,
                    snapshot_id: if m.snapshot_id.is_empty() {
                        None
                    } else {
                        Some(SnapshotId(m.snapshot_id))
                    },
                    clean: m.clean,
                });
                Ok(true)
            }
        }
    }

    /// The event's epoch must be the session's own epoch *and* the session
    /// must not be fenced relative to the serving epoch. On violation the
    /// bot is told `Fenced` (→ exit 67) and the stream closes.
    fn epoch_matches(&self, bot_id: &str, session_epoch: Epoch, event_epoch: Epoch) -> bool {
        let auth = self.authority(bot_id);
        let serving = auth.serving.unwrap_or(Epoch::ZERO);
        let ok = event_epoch == session_epoch
            && (session_epoch >= serving || Some(session_epoch) == auth.pending);
        if !ok {
            let _ = self.push(
                bot_id,
                session_epoch,
                GatewayCommand::fenced(serving.get(), event_epoch.get()),
            );
            self.emit(GatewayEvent::Fenced {
                bot_id: bot_id.into(),
                expected: serving,
                got: event_epoch,
            });
        }
        ok
    }

    /// Decode + commit a snapshot through the epoch-conditional store; the
    /// `Ack` (or `Fenced`) goes back down the bot's stream.
    async fn commit_snapshot(
        &self,
        bot_id: &str,
        session_epoch: Epoch,
        bytes: &[u8],
        quality: Option<fleet_types::SnapshotQuality>,
    ) -> Result<bool, Status> {
        let mut snap = BotSnapshot::decode(bytes)
            .map_err(|e| Status::invalid_argument(format!("snapshot decode: {e}")))?;
        if snap.bot_id != bot_id || snap.epoch != session_epoch {
            self.push_fenced(bot_id, session_epoch);
            return Ok(false);
        }
        if let Some(q) = quality {
            snap.quality = q;
        }
        match self.store.put(&snap).await {
            Ok(id) => {
                self.emit(GatewayEvent::SnapshotCommitted {
                    bot_id: bot_id.into(),
                    epoch: session_epoch,
                    snapshot_id: id.clone(),
                });
                let _ = self.push(
                    bot_id,
                    session_epoch,
                    GatewayCommand::ack(session_epoch.get(), Some(id.0)),
                );
                Ok(true)
            }
            Err(StoreError::Fenced(_)) => {
                self.push_fenced(bot_id, session_epoch);
                Ok(false)
            }
            Err(e) => {
                tracing::warn!(bot_id, error = %e, "snapshot commit failed");
                let _ = self.push(
                    bot_id,
                    session_epoch,
                    GatewayCommand::ack(session_epoch.get(), None),
                );
                Ok(true)
            }
        }
    }

    fn push_fenced(&self, bot_id: &str, session_epoch: Epoch) {
        let serving = self.authority(bot_id).serving.unwrap_or(Epoch::ZERO);
        let _ = self.push(
            bot_id,
            session_epoch,
            GatewayCommand::fenced(serving.get(), session_epoch.get()),
        );
        self.emit(GatewayEvent::Fenced {
            bot_id: bot_id.into(),
            expected: serving,
            got: session_epoch,
        });
    }
}

#[async_trait]
impl ControlDirectory for Gateway {
    async fn note_pending_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), ControlError> {
        let mut inner = self.inner.lock().unwrap();
        let auth = inner.authority.entry(bot_id.into()).or_default();
        if auth.serving.is_none_or(|s| epoch > s) {
            auth.pending = Some(epoch);
        }
        Ok(())
    }

    async fn promote(&self, bot_id: &str, epoch: Epoch) -> Result<(), ControlError> {
        let stale: Vec<(String, u64)> = {
            let mut inner = self.inner.lock().unwrap();
            let auth = inner.authority.entry(bot_id.into()).or_default();
            auth.serving = Some(epoch);
            if auth.pending == Some(epoch) {
                auth.pending = None;
            }
            inner
                .sessions
                .keys()
                .filter(|(b, e)| b == bot_id && Epoch(*e) < epoch)
                .cloned()
                .collect()
        };
        // Fence every older stream (docs/03 §3.7); the queued `Fenced`
        // delivers before the sender is dropped.
        for (b, e) in stale {
            let epoch_e = Epoch(e);
            let _ = self.push(&b, epoch_e, GatewayCommand::fenced(epoch.get(), e));
            self.emit(GatewayEvent::Fenced {
                bot_id: b.clone(),
                expected: epoch,
                got: epoch_e,
            });
            self.close_session(&b, epoch_e, "fenced");
        }
        Ok(())
    }

    async fn session(
        &self,
        bot_id: &str,
        epoch: Epoch,
    ) -> Result<Option<SessionInfo>, ControlError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .sessions
            .get(&(bot_id.to_string(), epoch.get()))
            .map(|s| s.info.clone()))
    }

    async fn send(
        &self,
        bot_id: &str,
        epoch: Epoch,
        cmd: GatewayCommand,
    ) -> Result<(), ControlError> {
        self.push(bot_id, epoch, cmd)
    }

    async fn predecessor_clear(&self, bot_id: &str, epoch: Epoch) -> Result<bool, ControlError> {
        let now = self.now();
        let grace = self.cfg.lease_grace.as_millis() as i64;
        let inner = self.inner.lock().unwrap();
        Ok(!inner.sessions.values().any(|s| {
            s.info.bot_id == bot_id
                && s.info.epoch < epoch
                && now < s.info.lease_expires_unix_ms + grace
        }))
    }
}

/// The one RPC. A bot opens the stream with `Hello`, keeps it for its
/// lifetime, and receives `GatewayCommand`s pushed by the operator.
#[tonic::async_trait]
impl FleetControl for Gateway {
    type SessionStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<GatewayCommand, Status>> + Send>>;

    async fn session(
        &self,
        request: Request<Streaming<ClientEvent>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let mut inbound = request.into_inner();
        // Response headers go out immediately; Hello is validated inside
        // the spawned task so a fenced bot learns via the stream (not by
        // the RPC never resolving).
        let (tx, rx) = mpsc::channel(self.cfg.outbound_buffer);
        let gw = self.clone();
        tokio::spawn(async move {
            // --- Hello handshake -----------------------------------------
            let (bot_id, epoch) = match inbound.message().await {
                Ok(Some(ClientEvent {
                    event:
                        Some(client_event::Event::Hello(Hello {
                            bot_id,
                            epoch,
                            botd_version,
                            phase,
                            ..
                        })),
                })) => {
                    let epoch = Epoch(epoch);
                    let auth = gw.authority(&bot_id);
                    if Some(epoch) == auth.serving || Some(epoch) == auth.pending {
                        tracing::info!(
                            bot_id,
                            epoch = epoch.get(),
                            botd_version,
                            phase,
                            "session opened"
                        );
                        (bot_id, epoch)
                    } else {
                        let expected = auth.pending.or(auth.serving).unwrap_or(Epoch::ZERO);
                        gw.emit(GatewayEvent::Fenced {
                            bot_id,
                            expected,
                            got: epoch,
                        });
                        let _ = tx
                            .send(GatewayCommand::fenced(expected.get(), epoch.get()))
                            .await;
                        return; // dropping tx ends the stream
                    }
                }
                Ok(_) => {
                    let _ = tx.send(GatewayCommand::fenced(0, 0)).await;
                    return;
                }
                Err(status) => {
                    tracing::warn!(%status, "stream error before Hello");
                    return;
                }
            };

            gw.open_session(&bot_id, epoch, tx.clone());

            // --- steady-state event loop ---------------------------------
            loop {
                match inbound.message().await {
                    Ok(Some(ev)) => match gw.handle_client_event(&bot_id, epoch, ev).await {
                        Ok(true) => {}
                        Ok(false) => {
                            gw.close_session(&bot_id, epoch, "fenced");
                            return;
                        }
                        Err(status) => {
                            tracing::warn!(bot_id = %bot_id, %status, "client event rejected");
                            gw.close_session(&bot_id, epoch, "protocol_error");
                            return;
                        }
                    },
                    Ok(None) => {
                        gw.close_session(&bot_id, epoch, "closed");
                        return;
                    }
                    Err(status) => {
                        tracing::warn!(bot_id = %bot_id, %status, "stream error");
                        gw.close_session(&bot_id, epoch, "error");
                        return;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx).map(Ok)) as Self::SessionStream
        ))
    }
}
