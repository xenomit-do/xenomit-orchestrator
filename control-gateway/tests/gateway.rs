//! Offline tests for the control gateway. The tonic service is exercised
//! end-to-end over `127.0.0.1` (no external services); store semantics are
//! covered through `MemorySnapshotStore` and, when `DATABASE_URL` is set,
//! the Postgres impl.

use std::sync::Arc;
use std::time::Duration;

use control_gateway::{Gateway, GatewayConfig, GatewayEvent, MemorySnapshotStore};
use fleet_types::epoch::Epoch;
use fleet_types::proto::fleet_control_client::FleetControlClient;
use fleet_types::proto::gateway_command::Command;
use fleet_types::proto::{
    Checkpoint, ClientEvent, Drained, GatewayCommand, Hello, LeaseRenew, client_event,
};
use fleet_types::snapshot::{BotSnapshot, SnapshotQuality, SnapshotReason, SnapshotStore};
use operator::traits::{ControlDirectory, ManualClock};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

fn ev(e: client_event::Event) -> ClientEvent {
    ClientEvent { event: Some(e) }
}

fn hello(bot_id: &str, epoch: u64) -> ClientEvent {
    ev(client_event::Event::Hello(Hello {
        bot_id: bot_id.into(),
        epoch,
        botd_version: "0.7.2".into(),
        phase: "armed".into(),
        traceparent: String::new(),
    }))
}

fn lease(epoch: u64, seq: u64) -> ClientEvent {
    ev(client_event::Event::LeaseRenew(LeaseRenew {
        epoch,
        lease_seq: seq,
    }))
}

fn checkpoint(epoch: u64, snapshot: Vec<u8>) -> ClientEvent {
    ev(client_event::Event::Checkpoint(Checkpoint {
        epoch,
        snapshot,
    }))
}

fn drained(epoch: u64, snapshot_id: &str, clean: bool) -> ClientEvent {
    ev(client_event::Event::Drained(Drained {
        epoch,
        snapshot_id: snapshot_id.into(),
        clean,
    }))
}

/// Spin up the service on an ephemeral localhost port.
async fn serve(gw: Gateway) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(gw.into_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

async fn connect(url: &str) -> (mpsc::Sender<ClientEvent>, tonic::Streaming<GatewayCommand>) {
    let mut client = FleetControlClient::connect(url.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(32);
    let outbound = client
        .session(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    (tx, outbound)
}

#[tokio::test]
async fn hello_wrong_epoch_is_fenced() {
    let store = Arc::new(MemorySnapshotStore::new());
    let gw = Gateway::new(store);
    gw.note_pending_epoch("bot-a", Epoch(2)).await.unwrap();
    let url = serve(gw.clone()).await;
    let (tx, mut outbound) = connect(&url).await;

    tx.send(hello("bot-a", 1)).await.unwrap();
    let cmd = outbound.message().await.unwrap().unwrap();
    match cmd.command {
        Some(Command::Fenced(f)) => {
            assert_eq!(f.expected_epoch, 2);
            assert_eq!(f.got_epoch, 1);
        }
        other => panic!("expected Fenced, got {other:?}"),
    }
    assert!(outbound.message().await.unwrap().is_none());
}

#[tokio::test]
async fn session_send_receive_and_lease() {
    let store = Arc::new(MemorySnapshotStore::new());
    let gw = Gateway::new(store.clone());
    store.record_epoch("bot-b", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-b", Epoch(1)).await.unwrap();
    gw.promote("bot-b", Epoch(1)).await.unwrap();

    let url = serve(gw.clone()).await;
    let (tx, mut outbound) = connect(&url).await;
    tx.send(hello("bot-b", 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The operator sees the session and can push commands.
    let info = gw.session("bot-b", Epoch(1)).await.unwrap().unwrap();
    assert_eq!(info.epoch, Epoch(1));
    ControlDirectory::send(&gw, "bot-b", Epoch(1), GatewayCommand::activate(1, None))
        .await
        .unwrap();
    let cmd = outbound.message().await.unwrap().unwrap();
    assert!(matches!(cmd.command, Some(Command::Activate(_))));

    // Lease renewal extends the lease.
    let before = gw.session("bot-b", Epoch(1)).await.unwrap().unwrap();
    tx.send(lease(1, 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = gw.session("bot-b", Epoch(1)).await.unwrap().unwrap();
    assert!(after.lease_expires_unix_ms >= before.lease_expires_unix_ms);

    drop(tx);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(gw.session("bot-b", Epoch(1)).await.unwrap().is_none());
}

#[tokio::test]
async fn checkpoint_commits_and_acks() {
    let store = Arc::new(MemorySnapshotStore::new());
    let gw = Gateway::new(store.clone());
    store.record_epoch("bot-c", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-c", Epoch(1)).await.unwrap();
    gw.promote("bot-c", Epoch(1)).await.unwrap();

    let mut events = gw.subscribe();
    let url = serve(gw.clone()).await;
    let (tx, mut outbound) = connect(&url).await;
    tx.send(hello("bot-c", 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut snap = BotSnapshot::new(
        "bot-c",
        Epoch(1),
        SnapshotReason::Checkpoint,
        SnapshotQuality::Checkpoint,
        1000,
    );
    snap.seq = 7;
    tx.send(checkpoint(1, snap.encode().unwrap()))
        .await
        .unwrap();

    // Ack comes back down the stream.
    let cmd = tokio::time::timeout(Duration::from_secs(2), outbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Some(Command::Ack(ack)) = cmd.command else {
        panic!("expected Ack, got {cmd:?}")
    };
    assert!(!ack.snapshot_id.is_empty());

    // Stored with sha256 + quality.
    let stored = store.latest("bot-c", None).await.unwrap().unwrap();
    assert_eq!(stored.meta.seq, 7);
    assert!(!stored.meta.sha256.is_empty());

    let mut saw_commit = false;
    while let Ok(e) = events.try_recv() {
        if matches!(e, GatewayEvent::SnapshotCommitted { .. }) {
            saw_commit = true;
        }
    }
    assert!(saw_commit);
}

#[tokio::test]
async fn promote_fences_predecessor_stream() {
    let store = Arc::new(MemorySnapshotStore::new());
    let gw = Gateway::new(store.clone());
    store.record_epoch("bot-d", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-d", Epoch(1)).await.unwrap();
    gw.promote("bot-d", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-d", Epoch(2)).await.unwrap();

    let url = serve(gw.clone()).await;
    let (tx, mut outbound) = connect(&url).await;
    tx.send(hello("bot-d", 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(!gw.predecessor_clear("bot-d", Epoch(2)).await.unwrap());

    // Activate the successor: e1 is fenced and its stream receives Fenced.
    gw.promote("bot-d", Epoch(2)).await.unwrap();
    let cmd = tokio::time::timeout(Duration::from_secs(2), outbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(cmd.command, Some(Command::Fenced(_))));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(gw.predecessor_clear("bot-d", Epoch(2)).await.unwrap());
    assert!(gw.session("bot-d", Epoch(1)).await.unwrap().is_none());
}

#[tokio::test]
async fn pending_epoch_write_is_fenced_by_store() {
    let store = Arc::new(MemorySnapshotStore::new());
    let gw = Gateway::new(store.clone());
    store.record_epoch("bot-e", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-e", Epoch(1)).await.unwrap();
    gw.promote("bot-e", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-e", Epoch(2)).await.unwrap();

    let url = serve(gw.clone()).await;
    let (tx, mut outbound) = connect(&url).await;

    // Armed successor connects at the pending epoch: session accepted.
    tx.send(hello("bot-e", 2)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(gw.session("bot-e", Epoch(2)).await.unwrap().is_some());

    // But it may not commit snapshots until promoted (docs/03 §3.7).
    let snap = BotSnapshot::new(
        "bot-e",
        Epoch(2),
        SnapshotReason::Checkpoint,
        SnapshotQuality::Checkpoint,
        1000,
    );
    tx.send(checkpoint(2, snap.encode().unwrap()))
        .await
        .unwrap();
    let cmd = tokio::time::timeout(Duration::from_secs(2), outbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(cmd.command, Some(Command::Fenced(_))));
}

#[tokio::test]
async fn drained_event_flows_to_subscribers() {
    let store = Arc::new(MemorySnapshotStore::new());
    let gw = Gateway::new(store.clone());
    store.record_epoch("bot-f", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-f", Epoch(1)).await.unwrap();
    gw.promote("bot-f", Epoch(1)).await.unwrap();

    let mut events = gw.subscribe();
    let url = serve(gw.clone()).await;
    let (tx, _outbound) = connect(&url).await;
    tx.send(hello("bot-f", 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    tx.send(drained(1, "snap_terminal", true)).await.unwrap();

    let mut saw_drained = false;
    for _ in 0..20 {
        while let Ok(e) = events.try_recv() {
            if let GatewayEvent::Drained { snapshot_id, .. } = e {
                assert_eq!(snapshot_id.unwrap().0, "snap_terminal");
                saw_drained = true;
            }
        }
        if saw_drained {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(saw_drained);
}

#[tokio::test]
async fn expired_lease_lets_successor_activate() {
    let store = Arc::new(MemorySnapshotStore::new());
    let clock = Arc::new(ManualClock::new(1_000_000));
    let gw = Gateway::with_clock(
        store,
        clock.clone(),
        GatewayConfig {
            lease_ttl: Duration::from_millis(1_000),
            lease_grace: Duration::from_millis(500),
            ..Default::default()
        },
    );
    gw.note_pending_epoch("bot-g", Epoch(1)).await.unwrap();
    gw.promote("bot-g", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-g", Epoch(2)).await.unwrap();

    let url = serve(gw.clone()).await;
    let (tx, _outbound) = connect(&url).await;
    tx.send(hello("bot-g", 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!gw.predecessor_clear("bot-g", Epoch(2)).await.unwrap());

    // Node-loss: stream still open but lease goes stale; ttl + grace later
    // the successor may activate anyway (docs/03 §3.7).
    clock.advance(Duration::from_millis(1_600));
    assert!(gw.predecessor_clear("bot-g", Epoch(2)).await.unwrap());
}

#[tokio::test]
async fn memory_store_fencing_and_gc() {
    use fleet_types::snapshot::StoreError;
    let store = MemorySnapshotStore::new();
    store.record_epoch("b1", Epoch(1)).await.unwrap();
    let snap = BotSnapshot::new(
        "b1",
        Epoch(1),
        SnapshotReason::Manual,
        SnapshotQuality::Clean,
        1,
    );
    let id = store.put(&snap).await.unwrap();
    assert!(store.get(&id).await.is_ok());

    // Wrong-epoch write fenced.
    let mut stale = snap.clone();
    stale.epoch = Epoch(2);
    assert!(matches!(
        store.put(&stale).await,
        Err(StoreError::Fenced(_))
    ));

    // Soft-delete hides from latest/list; undelete restores.
    store.soft_delete("b1", 5_000).await.unwrap();
    assert!(store.latest("b1", None).await.unwrap().is_none());
    assert!(store.list("b1", false).await.unwrap().is_empty());
    assert_eq!(store.list("b1", true).await.unwrap().len(), 1);
    store.undelete("b1").await.unwrap();
    assert_eq!(store.list("b1", false).await.unwrap().len(), 1);

    // Purge removes soft-deleted rows past the retention point.
    assert_eq!(store.purge(6_000).await.unwrap(), 0); // undeleted again
    store.soft_delete("b1", 5_000).await.unwrap();
    assert_eq!(store.purge(6_000).await.unwrap(), 1);
}

#[tokio::test]
async fn activate_is_delivered_once_per_session() {
    let store = Arc::new(MemorySnapshotStore::new());
    let gw = Gateway::new(store.clone());
    store.record_epoch("bot-h", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-h", Epoch(1)).await.unwrap();
    gw.promote("bot-h", Epoch(1)).await.unwrap();

    let url = serve(gw.clone()).await;
    let (tx, mut outbound) = connect(&url).await;
    tx.send(hello("bot-h", 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // A racing/retrying reconciler may push Activate twice; the pod's
    // activation slot is one-shot, so only the first may reach the wire.
    for _ in 0..2 {
        ControlDirectory::send(&gw, "bot-h", Epoch(1), GatewayCommand::activate(1, None))
            .await
            .unwrap();
    }
    let cmd = tokio::time::timeout(Duration::from_secs(2), outbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(cmd.command, Some(Command::Activate(_))));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), outbound.message())
            .await
            .is_err(),
        "duplicate Activate reached the wire"
    );
}

#[tokio::test]
async fn activate_again_after_session_replacement() {
    let store = Arc::new(MemorySnapshotStore::new());
    let gw = Gateway::new(store.clone());
    store.record_epoch("bot-i", Epoch(1)).await.unwrap();
    gw.note_pending_epoch("bot-i", Epoch(1)).await.unwrap();
    gw.promote("bot-i", Epoch(1)).await.unwrap();

    let url = serve(gw.clone()).await;
    let (tx, mut outbound) = connect(&url).await;
    tx.send(hello("bot-i", 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    ControlDirectory::send(&gw, "bot-i", Epoch(1), GatewayCommand::activate(1, None))
        .await
        .unwrap();
    let cmd = tokio::time::timeout(Duration::from_secs(2), outbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(cmd.command, Some(Command::Activate(_))));

    // Stream flap: the replacement session must get its own Activate —
    // dedup is per-session, not per-pod-lifetime (that part is botd's job).
    drop(tx);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (tx2, mut outbound2) = connect(&url).await;
    tx2.send(hello("bot-i", 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    ControlDirectory::send(&gw, "bot-i", Epoch(1), GatewayCommand::activate(1, None))
        .await
        .unwrap();
    let cmd = tokio::time::timeout(Duration::from_secs(2), outbound2.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(cmd.command, Some(Command::Activate(_))),
        "reconnect must receive its own Activate"
    );
}

// ---------------------------------------------------------------------------
// Postgres conformance: only runs when DATABASE_URL is set.
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_store_conformance() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL unset; skipping postgres conformance");
        return;
    };
    use fleet_types::snapshot::StoreError;

    let store = control_gateway::PostgresSnapshotStore::connect(&url, 2)
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let bot = format!(
        "pg-test-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    store.record_epoch(&bot, Epoch(1)).await.unwrap();
    let snap = BotSnapshot::new(
        &bot,
        Epoch(1),
        SnapshotReason::Checkpoint,
        SnapshotQuality::Checkpoint,
        1000,
    );
    let id = store.put(&snap).await.unwrap();
    assert_eq!(store.get(&id).await.unwrap().meta.seq, snap.seq);
    // Stale epoch fenced inside the transaction.
    let mut stale = snap.clone();
    stale.epoch = Epoch(9);
    assert!(matches!(
        store.put(&stale).await,
        Err(StoreError::Fenced(_))
    ));
}
