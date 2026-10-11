//! Lifecycle tests for `BotController`, all offline with fakes.
//!
//! Covered: create → armed → activate → running; pause/stop drains; planned
//! handoff (Arm→Drain→Activate); crash restore with backoff+jitter and
//! quarantine; the docs/02 §2.5 exit-code table; stale-epoch fencing;
//! hot-apply vs restart config edits; 30-day soft-delete retention.

use std::sync::Arc;
use std::time::Duration;

use fleet_types::condition_type;
use fleet_types::crds::{
    AccountSpec, BotConfigSpec, PendingRequest, PendingRequestKind, Proxy, ProxyMode, ProxyScheme,
    ProxySelector, ProxySpec, ProxyStatus, XenomitBot, XenomitBotSpec,
};
use fleet_types::epoch::Epoch;
use fleet_types::phase::{BotPhase, ConditionStatus, DesiredState, NeedsHumanKind};
use fleet_types::proto::gateway_command;
use fleet_types::snapshot::{
    BotSnapshot, SnapshotId, SnapshotQuality, SnapshotReason, SnapshotStore,
};

use operator::fakes::{FakeControl, FakePods, FakeSnapshots};
use operator::traits::ManualClock;
use operator::{
    BotController, Clock, HandoffTrigger, MemoryProxyStore, MemorySecretStore, Pool, ProxyStore,
    SecretStore,
};

type Ctl = BotController<
    Arc<FakePods>,
    Arc<MemorySecretStore>,
    Arc<FakeSnapshots>,
    Arc<FakeControl>,
    Arc<ManualClock>,
    Pool<Arc<MemoryProxyStore>>,
>;

struct Rig {
    ctl: Ctl,
    pods: Arc<FakePods>,
    secrets: Arc<MemorySecretStore>,
    snapshots: Arc<FakeSnapshots>,
    control: Arc<FakeControl>,
    clock: Arc<ManualClock>,
    proxies: Arc<MemoryProxyStore>,
}

fn mk_proxy(name: &str, ip: &str, capacity: u32) -> Proxy {
    let mut p = Proxy::new(
        name,
        ProxySpec {
            scheme: ProxyScheme::Socks5,
            host: ip.into(),
            port: 1080,
            tags: vec![],
            country: Some("DE".into()),
            isp: None,
            capacity,
            credentials_vault_path: format!("secret/data/proxies/{name}"),
        },
    );
    // Seeded healthy + probed so the pool admits them immediately.
    p.status = Some(ProxyStatus {
        healthy: true,
        last_probe_at_unix_ms: Some(1),
        resolved_ips: vec![ip.into()],
        exit_ip: Some("198.51.100.7".into()),
        ..Default::default()
    });
    p
}

fn rig_with(seeds: &[Proxy]) -> Rig {
    let pods = Arc::new(FakePods::new());
    let secrets = Arc::new(MemorySecretStore::new());
    let snapshots = Arc::new(FakeSnapshots::new());
    let control = Arc::new(FakeControl::new());
    let clock = Arc::new(ManualClock::new(1_800_000_000_000));
    let proxies = Arc::new(MemoryProxyStore::new());
    for p in seeds {
        proxies.seed(p);
    }
    let pool = Pool::new(proxies.clone());
    let ctl = BotController::new(
        pods.clone(),
        secrets.clone(),
        snapshots.clone(),
        control.clone(),
        clock.clone(),
        pool,
    );
    Rig {
        ctl,
        pods,
        secrets,
        control,
        clock,
        snapshots,
        proxies,
    }
}

fn rig() -> Rig {
    rig_with(&[
        mk_proxy("px-a", "203.0.113.10", 5),
        mk_proxy("px-b", "203.0.113.11", 5),
    ])
}

fn mk_bot(name: &str) -> XenomitBot {
    XenomitBot::new(
        name,
        XenomitBotSpec {
            owner: "user_abc".into(),
            desired_state: DesiredState::Running,
            account: AccountSpec {
                server: "int1.darkorbit.com".into(),
                credentials_vault_path: format!("secret/data/bots/{name}/account"),
                ..Default::default()
            },
            config: BotConfigSpec {
                settings: serde_json::json!({"activeModule": "kill_collect"}),
                ..Default::default()
            },
            config_revision: 1,
            version: Default::default(),
            proxy: Default::default(),
            restore_policy: Default::default(),
            resources: None,
        },
    )
}

fn phase(bot: &XenomitBot) -> BotPhase {
    bot.status.as_ref().unwrap().phase
}

fn epoch(bot: &XenomitBot) -> Epoch {
    bot.status.as_ref().unwrap().epoch
}

/// Drive a fresh bot to Running: Pending → Provisioning → pod → Armed →
/// connect session → Activate → report Running.
async fn drive_to_running(rig: &Rig, bot: &mut XenomitBot) {
    let name = bot.metadata.name.clone().unwrap();
    rig.ctl.reconcile(bot).await.unwrap(); // Pending -> Provisioning
    rig.ctl.reconcile(bot).await.unwrap(); // create pod
    assert_eq!(phase(bot), BotPhase::Provisioning);
    rig.pods.set_running(&name, epoch(bot));
    rig.ctl.reconcile(bot).await.unwrap(); // pod running -> Armed
    assert_eq!(phase(bot), BotPhase::Armed);

    let ep = epoch(bot);
    rig.control.connect(&name, ep);
    rig.ctl.reconcile(bot).await.unwrap(); // Activate sent -> Starting
    assert_eq!(phase(bot), BotPhase::Starting);

    rig.ctl
        .on_phase_changed(bot, ep, BotPhase::Running, "")
        .await
        .unwrap();
    assert_eq!(phase(bot), BotPhase::Running);
    assert!(rig.secrets.is_provisioned(&name));
}

#[tokio::test]
async fn create_bot_happy_path() {
    let rig = rig();
    let mut bot = mk_bot("bot-01");
    drive_to_running(&rig, &mut bot).await;

    assert_eq!(epoch(&bot), Epoch(1));
    let cmds = rig.control.commands("bot-01", Epoch(1));
    assert!(matches!(
        cmds.last().unwrap().command,
        Some(gateway_command::Command::Activate(_))
    ));
    assert_eq!(rig.control.serving("bot-01"), Some(Epoch(1)));
}

#[tokio::test]
async fn activate_waits_for_session() {
    let rig = rig();
    let mut bot = mk_bot("bot-02");
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.pods.set_running("bot-02", Epoch(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Armed);

    // No session yet: Activate is deferred.
    let out = rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Armed);
    assert!(out.requeue_after.is_some());
}

#[tokio::test]
async fn pause_drains_without_successor() {
    let rig = rig();
    let mut bot = mk_bot("bot-03");
    drive_to_running(&rig, &mut bot).await;

    bot.spec.desired_state = DesiredState::Paused;
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Draining);
    let drains = rig
        .control
        .commands("bot-03", Epoch(1))
        .into_iter()
        .filter(|c| matches!(c.command, Some(gateway_command::Command::Drain(_))))
        .count();
    assert_eq!(drains, 1);

    rig.ctl
        .on_drained(&mut bot, Epoch(1), Some(SnapshotId("snap_1".into())), true)
        .await
        .unwrap();
    assert_eq!(phase(&bot), BotPhase::Terminated);
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Paused);
    assert_eq!(
        bot.status.unwrap().last_snapshot_id.unwrap().0,
        "snap_1".to_string()
    );
}

#[tokio::test]
async fn resume_from_pause_starts_new_epoch() {
    let rig = rig();
    let mut bot = mk_bot("bot-04");
    drive_to_running(&rig, &mut bot).await;
    bot.spec.desired_state = DesiredState::Stopped;
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.ctl
        .on_drained(&mut bot, Epoch(1), None, true)
        .await
        .unwrap();
    rig.pods.exit("bot-04", Epoch(1), Some(0)); // Drained -> exit 0
    rig.control.disconnect("bot-04", Epoch(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Stopped);

    bot.spec.desired_state = DesiredState::Running;
    rig.ctl.reconcile(&mut bot).await.unwrap(); // -> Provisioning
    rig.ctl.reconcile(&mut bot).await.unwrap(); // create epoch 2
    assert_eq!(epoch(&bot), Epoch(2));
}

#[tokio::test]
async fn planned_handoff_arm_drain_activate() {
    let rig = rig();
    let mut bot = mk_bot("bot-05");
    drive_to_running(&rig, &mut bot).await;

    rig.ctl
        .request_handoff(&mut bot, fleet_types::HandoffReason::Upgrade, None)
        .await
        .unwrap();
    // Successor pod for epoch 2 is armed; old pod still running.
    assert_eq!(epoch(&bot), Epoch(2));
    assert!(rig.pods.pod("bot-05", Epoch(2)).is_some());
    assert!(rig.pods.pod("bot-05", Epoch(1)).is_some());
    assert_eq!(phase(&bot), BotPhase::Running);

    // Successor ready -> Drain predecessor.
    rig.pods.set_running("bot-05", Epoch(2));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Draining);
    let drain_to_old = rig
        .control
        .commands("bot-05", Epoch(1))
        .into_iter()
        .any(|c| matches!(c.command, Some(gateway_command::Command::Drain(_))));
    assert!(drain_to_old, "predecessor must get Drain");

    // Old reports Drained -> Activate successor once its session is up.
    rig.control.connect("bot-05", Epoch(2));
    rig.control.disconnect("bot-05", Epoch(1));
    rig.ctl
        .on_drained(&mut bot, Epoch(1), Some(SnapshotId("snap_h".into())), true)
        .await
        .unwrap();
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Starting);
    let acts = rig
        .control
        .commands("bot-05", Epoch(2))
        .into_iter()
        .filter_map(|c| match c.command {
            Some(gateway_command::Command::Activate(a)) => Some(a),
            _ => None,
        })
        .count();
    assert_eq!(acts, 1);
    assert_eq!(rig.control.serving("bot-05"), Some(Epoch(2)));

    rig.ctl
        .on_phase_changed(&mut bot, Epoch(2), BotPhase::Running, "clean")
        .await
        .unwrap();
    let st = bot.status.as_ref().unwrap();
    assert_eq!(st.phase, BotPhase::Running);
    assert!(
        st.handoff.is_none(),
        "handoff completes when successor runs"
    );
}

#[tokio::test]
async fn crash_restore_backoff_then_new_epoch() {
    let rig = rig();
    let mut bot = mk_bot("bot-06");
    drive_to_running(&rig, &mut bot).await;

    // Pod crashes (exit code != mapped codes).
    rig.pods.exit("bot-06", Epoch(1), Some(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Provisioning);
    let st = bot.status.as_ref().unwrap();
    assert!(st.crash.next_retry_at_unix_ms.is_some());
    assert_eq!(st.last_exit.as_ref().unwrap().code, Some(1));

    // Still in backoff: no new pod yet.
    let out = rig.ctl.reconcile(&mut bot).await.unwrap();
    assert!(rig.pods.pod("bot-06", Epoch(2)).is_none());
    assert!(out.requeue_after.is_some());

    // Past the retry point: successor spawns on a new epoch.
    let retry_at = bot
        .status
        .as_ref()
        .unwrap()
        .crash
        .next_retry_at_unix_ms
        .unwrap();
    rig.clock.set(retry_at + 1);
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(epoch(&bot), Epoch(2));
    assert!(rig.pods.pod("bot-06", Epoch(2)).is_some());
}

#[tokio::test]
async fn crash_restore_uses_latest_checkpoint() {
    let rig = rig();
    let mut bot = mk_bot("bot-07");
    drive_to_running(&rig, &mut bot).await;

    // Store a checkpoint as the bot's current epoch (docs/03 §3.8).
    let snap = BotSnapshot::new(
        "bot-07",
        Epoch(1),
        SnapshotReason::Checkpoint,
        SnapshotQuality::Checkpoint,
        1,
    );
    let id = rig.snapshots.put(&snap).await.unwrap();

    rig.pods.exit("bot-07", Epoch(1), None); // signal kill
    rig.control.disconnect("bot-07", Epoch(1)); // dead process drops its stream
    rig.ctl.reconcile(&mut bot).await.unwrap();
    let retry_at = bot
        .status
        .as_ref()
        .unwrap()
        .crash
        .next_retry_at_unix_ms
        .unwrap();
    rig.clock.set(retry_at + 1);
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.pods.set_running("bot-07", Epoch(2));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.control.connect("bot-07", Epoch(2));
    rig.ctl.reconcile(&mut bot).await.unwrap();

    let activates: Vec<_> = rig
        .control
        .commands("bot-07", Epoch(2))
        .into_iter()
        .filter_map(|c| match c.command {
            Some(gateway_command::Command::Activate(a)) => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(activates.len(), 1);
    assert_eq!(activates[0].snapshot_id, id.0, "restores latest snapshot");
}

#[tokio::test]
async fn quarantine_after_budget() {
    let rig = rig();
    let mut bot = mk_bot("bot-08");
    drive_to_running(&rig, &mut bot).await;

    // 5 crashes inside the window -> 6th quarantines (default policy N=5).
    let mut ep = Epoch(1);
    for _ in 0..5 {
        rig.pods.exit("bot-08", ep, Some(1));
        rig.ctl.reconcile(&mut bot).await.unwrap();
        let retry_at = bot
            .status
            .as_ref()
            .unwrap()
            .crash
            .next_retry_at_unix_ms
            .unwrap();
        rig.clock.set(retry_at + 1);
        rig.ctl.reconcile(&mut bot).await.unwrap();
        ep = epoch(&bot);
        if phase(&bot) == BotPhase::Quarantined {
            break;
        }
        rig.pods.set_running("bot-08", ep);
        rig.ctl.reconcile(&mut bot).await.unwrap();
        rig.control.connect("bot-08", ep);
        rig.ctl.reconcile(&mut bot).await.unwrap();
        rig.ctl
            .on_phase_changed(&mut bot, ep, BotPhase::Running, "")
            .await
            .unwrap();
    }
    rig.pods.exit("bot-08", ep, Some(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Quarantined);
}

#[tokio::test]
async fn exit_code_table() {
    // Each independent case: run bot, kill pod with code, assert phase.
    for (code, want) in [
        (64, BotPhase::InvalidConfig),
        (65, BotPhase::NeedsHuman),
        (66, BotPhase::NeedsHuman),
        (68, BotPhase::NeedsUpdate),
        (70, BotPhase::Failed),
    ] {
        let rig = rig();
        let mut bot = mk_bot(&format!("bot-x{code}"));
        drive_to_running(&rig, &mut bot).await;
        rig.pods.exit(&format!("bot-x{code}"), Epoch(1), Some(code));
        rig.ctl.reconcile(&mut bot).await.unwrap();
        assert_eq!(phase(&bot), want, "exit {code}");
    }
}

#[tokio::test]
async fn exit_66_flags_captcha() {
    let rig = rig();
    let mut bot = mk_bot("bot-cap");
    drive_to_running(&rig, &mut bot).await;
    rig.pods.exit("bot-cap", Epoch(1), Some(66));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(
        bot.status.unwrap().needs_human,
        Some(NeedsHumanKind::Captcha)
    );
}

#[tokio::test]
async fn exit_65_flags_credentials() {
    let rig = rig();
    let mut bot = mk_bot("bot-cred");
    drive_to_running(&rig, &mut bot).await;
    rig.pods.exit("bot-cred", Epoch(1), Some(65));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(
        bot.status.unwrap().needs_human,
        Some(NeedsHumanKind::Credentials)
    );
}

#[tokio::test]
async fn fenced_stale_pod_is_ignored() {
    let rig = rig();
    let mut bot = mk_bot("bot-09");
    drive_to_running(&rig, &mut bot).await;

    // Hand off to epoch 2 first so a successor owns the bot — the docs/02
    // §2.5 "do nothing" row. The stale epoch-1 pod then exits 67: it
    // carries no authority, so phase and epoch must not move.
    rig.ctl
        .request_handoff(&mut bot, fleet_types::HandoffReason::Upgrade, None)
        .await
        .unwrap();
    rig.pods.set_running("bot-09", Epoch(2));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.control.connect("bot-09", Epoch(2));
    rig.control.disconnect("bot-09", Epoch(1));
    rig.ctl
        .on_drained(&mut bot, Epoch(1), None, true)
        .await
        .unwrap();
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.ctl
        .on_phase_changed(&mut bot, Epoch(2), BotPhase::Running, "")
        .await
        .unwrap();
    assert_eq!(epoch(&bot), Epoch(2));

    rig.pods.exit("bot-09", Epoch(1), Some(67));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Running);
    assert_eq!(epoch(&bot), Epoch(2));
    assert!(rig.pods.pod("bot-09", Epoch(3)).is_none());
}

/// §2.5's "do nothing" only holds while a newer pod owns the bot. When the
/// *current* pod exits 67 — e.g. fenced by stale local epoch state —
/// nothing supersedes it, so the controller must restore like any crash.
#[tokio::test]
async fn fenced_current_pod_crash_restores() {
    let rig = rig();
    let mut bot = mk_bot("bot-fc");
    drive_to_running(&rig, &mut bot).await;

    rig.pods.exit("bot-fc", Epoch(1), Some(67));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    let st = bot.status.as_ref().unwrap();
    assert_eq!(phase(&bot), BotPhase::Provisioning);
    assert_eq!(st.last_exit.as_ref().unwrap().code, Some(67));
    assert!(st.crash.next_retry_at_unix_ms.is_some());

    // Past the backoff point the successor spawns on the next epoch.
    let retry_at = st.crash.next_retry_at_unix_ms.unwrap();
    rig.clock.set(retry_at + 1);
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(epoch(&bot), Epoch(2));
    assert!(rig.pods.pod("bot-fc", Epoch(2)).is_some());
}

/// The reported wedge: the pod dies fenced while desired_state flips to
/// Paused — a dead pod must not pin the bot in `Running` forever.
#[tokio::test]
async fn fenced_dead_pod_pause_terminates() {
    let rig = rig();
    let mut bot = mk_bot("bot-fp");
    drive_to_running(&rig, &mut bot).await;

    rig.pods.exit("bot-fp", Epoch(1), Some(67));
    bot.spec.desired_state = DesiredState::Paused;
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Terminated);
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Paused);
    assert!(rig.pods.pod("bot-fp", Epoch(2)).is_none());
}

#[tokio::test]
async fn stale_epoch_report_is_dropped() {
    let rig = rig();
    let mut bot = mk_bot("bot-10");
    drive_to_running(&rig, &mut bot).await;

    // A ghost session on epoch 0 reporting Running is ignored.
    rig.ctl
        .on_phase_changed(&mut bot, Epoch(0), BotPhase::NeedsHuman, "captcha")
        .await
        .unwrap();
    assert_eq!(phase(&bot), BotPhase::Running);
}

#[tokio::test]
async fn hot_config_edit_sends_apply_config() {
    let rig = rig();
    let mut bot = mk_bot("bot-11");
    drive_to_running(&rig, &mut bot).await;

    // Change only settings + bump revision -> hot apply, no handoff.
    bot.spec.config.settings = serde_json::json!({"activeModule": "palladium"});
    bot.spec.config_revision = 2;
    rig.ctl.reconcile(&mut bot).await.unwrap();

    let applies = rig
        .control
        .commands("bot-11", Epoch(1))
        .into_iter()
        .filter(|c| matches!(c.command, Some(gateway_command::Command::ApplyConfig(_))))
        .count();
    assert_eq!(applies, 1);
    assert!(bot.status.as_ref().unwrap().handoff.is_none());
    assert_eq!(phase(&bot), BotPhase::Running);
}

#[tokio::test]
async fn version_edit_triggers_handoff() {
    let rig = rig();
    let mut bot = mk_bot("bot-12");
    drive_to_running(&rig, &mut bot).await;

    bot.spec.version.channel = fleet_types::crds::VersionChannel::Canary;
    rig.ctl.reconcile(&mut bot).await.unwrap();
    let st = bot.status.as_ref().unwrap();
    assert_eq!(
        st.handoff.as_ref().unwrap().reason,
        fleet_types::HandoffReason::Upgrade
    );
}

#[tokio::test]
async fn needs_human_rearm() {
    let rig = rig();
    let mut bot = mk_bot("bot-13");
    drive_to_running(&rig, &mut bot).await;
    rig.pods.exit("bot-13", Epoch(1), Some(66));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::NeedsHuman);

    // User supplies a dosid: API writes it to Vault + sets Rearm.
    rig.secrets
        .write(
            "secret/data/bots/bot-13/dosid",
            operator::SecretValue::new(b"dosid-value".to_vec()),
        )
        .await
        .unwrap();
    bot.status.as_mut().unwrap().pending_request = Some(PendingRequest {
        kind: PendingRequestKind::Rearm,
        snapshot_id: None,
        requested_at_unix_ms: rig.clock.now_unix_ms(),
    });
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Provisioning);
    assert!(bot.status.as_ref().unwrap().needs_human.is_none());
}

#[tokio::test]
async fn soft_delete_drains_then_removes_pods() {
    let rig = rig();
    let mut bot = mk_bot("bot-14");
    drive_to_running(&rig, &mut bot).await;

    // API destroys creds, then marks deleted (docs/09 §9.2 order).
    rig.secrets.destroy_bot("bot-14").await.unwrap();
    assert!(rig.secrets.is_destroyed("bot-14"));
    rig.ctl.mark_deleted(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Draining);
    assert_eq!(bot.spec.desired_state, DesiredState::Stopped);

    // `Drained` precedes the process exit 0 (docs/03 §3.2): the pod is
    // still alive, so the controller must keep it — deleting on the
    // report alone would kill a clean shutdown mid-flight.
    rig.ctl
        .on_drained(&mut bot, Epoch(1), Some(SnapshotId("s".into())), true)
        .await
        .unwrap();
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert!(
        rig.pods.pod("bot-14", Epoch(1)).is_some(),
        "Drained-reported pod deleted before its exit was observed"
    );

    // Once the exit lands, the pod is reaped and the flag is set.
    rig.pods.exit("bot-14", Epoch(1), Some(0));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert!(rig.pods.pod("bot-14", Epoch(1)).is_none());
    assert!(bot.status.as_ref().unwrap().delete_pods_gone);
    assert_eq!(phase(&bot), BotPhase::Stopped);
    assert!(!rig.ctl.purge_due(&bot));

    // Retention clock: 30 days.
    rig.clock.advance(Duration::from_secs(31 * 86_400));
    assert!(rig.ctl.purge_due(&bot));
}

/// The post-`Drained` keep is bounded by `drained_exit_grace`: a pod that
/// never exits (hung shutdown) is deleted once the grace elapses — the
/// report is a hint, never a promise of exit.
#[tokio::test]
async fn drained_pod_survives_only_within_exit_grace() {
    let rig = rig();
    let mut bot = mk_bot("bot-grace");
    drive_to_running(&rig, &mut bot).await;
    rig.secrets.destroy_bot("bot-grace").await.unwrap();
    rig.ctl.mark_deleted(&mut bot).await.unwrap();
    rig.ctl
        .on_drained(&mut bot, Epoch(1), Some(SnapshotId("s".into())), true)
        .await
        .unwrap();

    // Within the grace window the drained pod is left alone even though
    // the drain already concluded.
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert!(rig.pods.pod("bot-grace", Epoch(1)).is_some());

    // Past the grace the pod is deleted anyway — `Drained` is not a
    // promise the process exited.
    rig.clock
        .advance(rig.ctl.cfg.drained_exit_grace + Duration::from_secs(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert!(
        rig.pods.pod("bot-grace", Epoch(1)).is_none(),
        "grace-expired pod must be deleted"
    );
    assert_eq!(phase(&bot), BotPhase::Stopped);
}

/// Same contract inside a planned handoff: after the predecessor reports
/// `Drained` (step Activate) the stale-pod sweep must not delete it while
/// its exit 0 is still in flight.
#[tokio::test]
async fn handoff_predecessor_kept_until_exit_observed() {
    let rig = rig();
    let mut bot = mk_bot("bot-hand");
    drive_to_running(&rig, &mut bot).await;

    rig.ctl
        .request_handoff(&mut bot, fleet_types::HandoffReason::Upgrade, None)
        .await
        .unwrap();
    rig.pods.set_running("bot-hand", Epoch(2));
    rig.ctl.reconcile(&mut bot).await.unwrap(); // -> Draining, Drain sent to e1

    // The predecessor reports Drained; its process is still shutting down
    // (pod object stays alive). The stale sweep must hold off.
    rig.control.connect("bot-hand", Epoch(2));
    rig.control.disconnect("bot-hand", Epoch(1));
    rig.ctl
        .on_drained(&mut bot, Epoch(1), Some(SnapshotId("snap_h".into())), true)
        .await
        .unwrap();
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Starting, "successor activated");
    assert!(
        rig.pods.pod("bot-hand", Epoch(1)).is_some(),
        "predecessor deleted between Drained and exit 0"
    );

    // Observed exit reaps the pod on the next pass.
    rig.pods.exit("bot-hand", Epoch(1), Some(0));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert!(rig.pods.pod("bot-hand", Epoch(1)).is_none());
}

#[tokio::test]
async fn undelete_requires_credentials() {
    let rig = rig();
    let mut bot = mk_bot("bot-15");
    drive_to_running(&rig, &mut bot).await;
    rig.ctl.mark_deleted(&mut bot).await.unwrap();
    rig.ctl
        .on_drained(&mut bot, Epoch(1), None, true)
        .await
        .unwrap();
    rig.ctl.reconcile(&mut bot).await.unwrap();

    rig.ctl.undelete(&mut bot).await.unwrap();
    let st = bot.status.as_ref().unwrap();
    assert_eq!(st.phase, BotPhase::NeedsHuman);
    assert_eq!(st.needs_human, Some(NeedsHumanKind::ReenterCredentials));
    assert!(st.deleted_at_unix_ms.is_none());
}

#[tokio::test]
async fn predecessor_lease_gates_activate() {
    let rig = rig();
    let mut bot = mk_bot("bot-16");
    drive_to_running(&rig, &mut bot).await;

    // Pod dies but its session/lease lingers (node loss): a replacement
    // must not activate until the lease clears (docs/03 §3.7).
    rig.pods.exit("bot-16", Epoch(1), Some(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    let retry_at = bot
        .status
        .as_ref()
        .unwrap()
        .crash
        .next_retry_at_unix_ms
        .unwrap();
    rig.clock.set(retry_at + 1);
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(epoch(&bot), Epoch(2));
    rig.pods.set_running("bot-16", Epoch(2));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Armed);

    // Old session still connected (lease not expired): no Activate.
    rig.control.connect("bot-16", Epoch(2));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    let no_activate = !rig
        .control
        .commands("bot-16", Epoch(2))
        .iter()
        .any(|c| matches!(c.command, Some(gateway_command::Command::Activate(_))));
    assert!(no_activate, "predecessor still holds the lease");

    // Lease expires: Activate proceeds.
    rig.control.expire_lease("bot-16", Epoch(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    let activated = rig
        .control
        .commands("bot-16", Epoch(2))
        .iter()
        .any(|c| matches!(c.command, Some(gateway_command::Command::Activate(_))));
    assert!(activated);
}

#[tokio::test]
async fn sealed_vault_keeps_bot_provisioning() {
    let rig = rig();
    let mut bot = mk_bot("bot-17");
    rig.secrets.set_unavailable(true);
    rig.ctl.reconcile(&mut bot).await.unwrap();
    let err = rig.ctl.reconcile(&mut bot).await;
    assert!(err.is_err(), "sealed vault surfaces as error");
    assert_eq!(phase(&bot), BotPhase::Provisioning);
    assert!(rig.pods.pod("bot-17", Epoch(1)).is_none());
}

// ---------------------------------------------------------------------------
// Proxy pool integration (docs/05 §5.3–5.4)
// ---------------------------------------------------------------------------

fn proxy_status(rig: &Rig, name: &str) -> ProxyStatus {
    rig.proxies.get_sync(name).unwrap().status.unwrap()
}

fn created_spec(rig: &Rig, bot_id: &str, ep: Epoch) -> operator::BotPodSpec {
    rig.pods
        .created
        .lock()
        .unwrap()
        .iter()
        .find(|s| s.bot_id == bot_id && s.epoch == ep)
        .cloned()
        .expect("pod spec recorded")
}

#[tokio::test]
async fn pod_carries_resolved_pool_proxy() {
    let rig = rig();
    let mut bot = mk_bot("bot-px1");
    drive_to_running(&rig, &mut bot).await;

    let st = bot.status.as_ref().unwrap();
    assert_eq!(st.proxy_ref.as_deref(), Some("px-a"));
    assert_eq!(st.proxy_resolved_ip.as_deref(), Some("203.0.113.10"));

    // The pod spec carries the resolved assignment, and the rendered
    // config names the Vault path + require_proxy (docs/05 §5.6).
    let spec = created_spec(&rig, "bot-px1", Epoch(1));
    let pa = spec.proxy.expect("pod spec has proxy assignment");
    assert_eq!(pa.name.as_deref(), Some("px-a"));
    assert_eq!(pa.endpoint().host, "203.0.113.10");
    assert_eq!(pa.endpoint().port, 1080);
    assert_eq!(
        pa.credentials_vault_path,
        "secret/data/proxies/px-a".to_string()
    );
    assert!(spec.config_json.contains("\"require_proxy\":true"));
    assert!(spec.config_json.contains("secret/data/proxies/px-a"));

    // The proxy CR accounts the claim.
    assert!(
        proxy_status(&rig, "px-a")
            .assigned
            .contains(&"bot-px1".to_string())
    );
}

#[tokio::test]
async fn pod_spec_carries_bot_uid_for_owner_reference() {
    let rig = rig();
    let mut bot = mk_bot("bot-own1");
    bot.metadata.uid = Some("uid-42".into());
    drive_to_running(&rig, &mut bot).await;
    // The kube backend turns this into the pod/ConfigMap/NetworkPolicy
    // ownerReference so Argo nests children under the CR.
    let spec = created_spec(&rig, "bot-own1", Epoch(1));
    assert_eq!(spec.owner_uid.as_deref(), Some("uid-42"));
}

#[tokio::test]
async fn require_proxy_gate_holds_spawn_until_eligible() {
    let rig = rig_with(&[]); // empty pool: fail closed
    let mut bot = mk_bot("bot-nopx");
    rig.ctl.reconcile(&mut bot).await.unwrap(); // Pending -> Provisioning
    let out = rig.ctl.reconcile(&mut bot).await.unwrap(); // spawn blocked
    assert_eq!(phase(&bot), BotPhase::Provisioning);
    assert_eq!(out.requeue_after, Some(std::time::Duration::from_secs(30)));
    assert!(rig.pods.pod("bot-nopx", Epoch(1)).is_none());
    let st = bot.status.as_ref().unwrap();
    let cond = st
        .conditions
        .iter()
        .find(|c| c.type_ == condition_type::PROXY_HEALTHY)
        .expect("ProxyHealthy condition");
    assert_eq!(cond.status, ConditionStatus::False);
    assert_eq!(cond.reason.as_deref(), Some("awaiting_proxy"));

    // A proxy appearing unblocks the next pass.
    rig.proxies.seed(&mk_proxy("px-late", "203.0.113.99", 5));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert!(rig.pods.pod("bot-nopx", Epoch(1)).is_some());
    assert_eq!(
        bot.status.as_ref().unwrap().proxy_ref.as_deref(),
        Some("px-late")
    );
}

#[tokio::test]
async fn exit_69_reassigns_pool_proxy() {
    let rig = rig();
    let mut bot = mk_bot("bot-69");
    drive_to_running(&rig, &mut bot).await;
    assert_eq!(
        bot.status.as_ref().unwrap().proxy_ref.as_deref(),
        Some("px-a")
    );

    // botd exits 69: the proxy is reported, the claim released, the
    // tenant cooldown starts on px-a (docs/05 §5.4).
    rig.pods.exit("bot-69", Epoch(1), Some(69));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Provisioning);
    assert!(bot.status.as_ref().unwrap().proxy_ref.is_none());
    let pa = proxy_status(&rig, "px-a");
    assert_eq!(pa.consecutive_failures, 1);
    assert!(!pa.assigned.contains(&"bot-69".to_string()));
    assert_eq!(pa.last_tenant.as_deref(), Some("user_abc"));
    assert!(pa.tenant_cooldown_until_unix_ms.is_some());

    // The next spawn must land on a different proxy: px-a has a recorded
    // failure so it is skipped until a clean probe clears it.
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(epoch(&bot), Epoch(2));
    assert_eq!(
        bot.status.as_ref().unwrap().proxy_ref.as_deref(),
        Some("px-b")
    );
    let spec = created_spec(&rig, "bot-69", Epoch(2));
    assert_eq!(spec.proxy.unwrap().name.as_deref(), Some("px-b"));
}

#[tokio::test]
async fn fixed_mode_uses_user_proxy_not_pool() {
    let rig = rig();
    let mut bot = mk_bot("bot-fix");
    bot.spec.proxy = fleet_types::crds::ProxyPolicySpec {
        mode: ProxyMode::Fixed,
        selector: None,
        fixed_vault_path: None, // falls back to the per-bot override path
        fixed_addr: Some("192.0.2.44:3128".into()),
    };
    drive_to_running(&rig, &mut bot).await;

    // No pool claim, no proxyRef.
    assert!(bot.status.as_ref().unwrap().proxy_ref.is_none());
    assert!(proxy_status(&rig, "px-a").assigned.is_empty());
    let spec = created_spec(&rig, "bot-fix", Epoch(1));
    let pa = spec.proxy.expect("fixed proxy assignment");
    assert!(pa.name.is_none());
    assert_eq!(pa.endpoint().host, "192.0.2.44");
    assert_eq!(pa.endpoint().port, 3128);
    assert!(
        pa.credentials_vault_path.contains("bot-fix"),
        "default per-bot proxy path, got {}",
        pa.credentials_vault_path
    );
}

#[tokio::test]
async fn fixed_mode_exit_69_draws_crash_budget_not_reassignment() {
    let rig = rig();
    let mut bot = mk_bot("bot-f69");
    bot.spec.proxy = fleet_types::crds::ProxyPolicySpec {
        mode: ProxyMode::Fixed,
        selector: None,
        fixed_vault_path: None,
        fixed_addr: Some("192.0.2.44:3128".into()),
    };
    drive_to_running(&rig, &mut bot).await;

    // Exit 69 on a fixed proxy: crash accounting, not reassignment —
    // the same address is rendered again on the retry (docs/05 §5.4).
    rig.pods.exit("bot-f69", Epoch(1), Some(69));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    assert_eq!(phase(&bot), BotPhase::Provisioning);
    assert!(
        bot.status
            .as_ref()
            .unwrap()
            .crash
            .next_retry_at_unix_ms
            .is_some()
    );
    let retry_at = bot
        .status
        .as_ref()
        .unwrap()
        .crash
        .next_retry_at_unix_ms
        .unwrap();
    rig.clock.set(retry_at + 1);
    rig.ctl.reconcile(&mut bot).await.unwrap();
    let spec = created_spec(&rig, "bot-f69", Epoch(2));
    assert_eq!(spec.proxy.unwrap().endpoint().host, "192.0.2.44");
}

#[tokio::test]
async fn delete_releases_pool_claim_and_starts_cooldown() {
    let rig = rig();
    let mut bot = mk_bot("bot-del");
    drive_to_running(&rig, &mut bot).await;
    assert!(
        proxy_status(&rig, "px-a")
            .assigned
            .contains(&"bot-del".to_string())
    );

    rig.secrets.destroy_bot("bot-del").await.unwrap();
    rig.ctl.mark_deleted(&mut bot).await.unwrap();
    let pa = proxy_status(&rig, "px-a");
    assert!(!pa.assigned.contains(&"bot-del".to_string()));
    assert_eq!(pa.last_tenant.as_deref(), Some("user_abc"));
    assert!(pa.tenant_cooldown_until_unix_ms.is_some());
}

#[tokio::test]
async fn proxy_change_handoff_arms_successor() {
    // The ProxyController drives failover through this trait (docs/05 §5.4).
    let rig = rig();
    let mut bot = mk_bot("bot-ho");
    drive_to_running(&rig, &mut bot).await;

    rig.ctl.proxy_change_handoff(&mut bot).await.unwrap();
    let st = bot.status.as_ref().unwrap();
    assert_eq!(
        st.handoff.as_ref().unwrap().reason,
        fleet_types::HandoffReason::ProxyChange
    );
    assert!(rig.pods.pod("bot-ho", Epoch(2)).is_some());
    assert!(
        rig.pods.pod("bot-ho", Epoch(1)).is_some(),
        "predecessor kept"
    );
}

#[tokio::test]
async fn selector_restricts_assignment() {
    let rig = rig_with(&[
        mk_proxy("px-de", "203.0.113.10", 5),
        mk_proxy("px-us", "198.51.100.20", 5),
    ]);
    // px-us has a different country tag.
    let mut us = rig.proxies.get_sync("px-us").unwrap();
    us.spec.country = Some("US".into());
    rig.proxies.save(&us).await.unwrap();

    let mut bot = mk_bot("bot-sel");
    bot.spec.proxy.selector = Some(ProxySelector {
        country: vec!["US".into()],
        tags: vec![],
    });
    drive_to_running(&rig, &mut bot).await;
    assert_eq!(
        bot.status.as_ref().unwrap().proxy_ref.as_deref(),
        Some("px-us")
    );
}

#[tokio::test]
async fn capacity_exhaustion_skips_full_proxy() {
    let rig = rig_with(&[mk_proxy("px-solo", "203.0.113.10", 1)]);
    let mut b1 = mk_bot("bot-c1");
    drive_to_running(&rig, &mut b1).await;
    assert_eq!(
        b1.status.as_ref().unwrap().proxy_ref.as_deref(),
        Some("px-solo")
    );

    // capacity=1 and b1 holds the slot: b2 waits behind the gate.
    let mut b2 = mk_bot("bot-c2");
    rig.ctl.reconcile(&mut b2).await.unwrap();
    let out = rig.ctl.reconcile(&mut b2).await.unwrap();
    assert_eq!(phase(&b2), BotPhase::Provisioning);
    assert!(rig.pods.pod("bot-c2", Epoch(1)).is_none());
    assert_eq!(out.requeue_after, Some(std::time::Duration::from_secs(30)));
}
