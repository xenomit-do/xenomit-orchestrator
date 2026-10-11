//! `BotRolloutController` tests — the whole canary → waves → gates →
//! rollback flow offline with fakes (docs/07 §7.3).
//!
//! Covered: canary pass then gated waves, canary failure → pause /
//! automatic rollback, wave + `maxDraining`/`maxOffline` gating, the
//! shared login token bucket + jitter, successor crashes mid-handoff,
//! controller restart mid-rollout (status-driven recovery), concurrent
//! rollout rejection, the `NeedsUpdate` (exit 68) detect → release flow,
//! pause/resume/abort, and the image-admission fail-closed path.

use std::sync::Arc;
use std::time::Duration;

use fleet_types::crds::{
    AccountSpec, BotConfigSpec, BotRollout, BotRolloutSpec, CanaryStrategy, GateFailureAction,
    PauseCause, Proxy, ProxyScheme, ProxySpec, ProxyStatus, RolloutBotOutcome, RolloutBotState,
    RolloutPhase, RolloutSelector, RolloutStrategy, VersionChannel, VersionSpec, WaveStrategy,
    XenomitBot, XenomitBotSpec,
};
use fleet_types::epoch::Epoch;
use fleet_types::phase::{BotPhase, DesiredState};
use fleet_types::snapshot::SnapshotId;

use operator::fakes::{
    FakeBotDirectory, FakeControl, FakeEvents, FakeMetrics, FakePods, FakeRollouts, FakeSnapshots,
    FakeVerifier,
};
use operator::traits::{BotDirectory, ManualClock, RolloutStore};
use operator::{
    BotController, BotRolloutController, LoginBucket, MemoryProxyStore, MemorySecretStore, Pool,
    RolloutConfig, RolloutEvent,
};

const STABLE: &str = "registry.local/xenomit-botd:stable";
const TARGET: &str = "registry.local/xenomit-botd@sha256:aaaaaa";

type Ctl = BotController<
    Arc<FakePods>,
    Arc<MemorySecretStore>,
    Arc<FakeSnapshots>,
    Arc<FakeControl>,
    Arc<ManualClock>,
    Pool<Arc<MemoryProxyStore>>,
>;

type RC = BotRolloutController<
    Arc<FakeRollouts>,
    Arc<FakeBotDirectory>,
    Arc<Ctl>,
    Arc<ManualClock>,
    Arc<FakeVerifier>,
    Arc<FakeMetrics>,
    Arc<FakeEvents>,
>;

struct Rig {
    rc: RC,
    rollouts: Arc<FakeRollouts>,
    bots: Arc<FakeBotDirectory>,
    ctl: Arc<Ctl>,
    pods: Arc<FakePods>,
    control: Arc<FakeControl>,
    clock: Arc<ManualClock>,
    verifier: Arc<FakeVerifier>,
    metrics: Arc<FakeMetrics>,
    events: Arc<FakeEvents>,
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
    p.status = Some(ProxyStatus {
        healthy: true,
        last_probe_at_unix_ms: Some(1),
        resolved_ips: vec![ip.into()],
        exit_ip: Some("198.51.100.7".into()),
        ..Default::default()
    });
    p
}

fn rig_with_cfg(cfg: RolloutConfig) -> Rig {
    let pods = Arc::new(FakePods::new());
    let secrets = Arc::new(MemorySecretStore::new());
    let snapshots = Arc::new(FakeSnapshots::new());
    let control = Arc::new(FakeControl::new());
    let clock = Arc::new(ManualClock::new(1_800_000_000_000));
    let proxies = Arc::new(MemoryProxyStore::new());
    for p in [
        mk_proxy("px-a", "203.0.113.10", 50),
        mk_proxy("px-b", "203.0.113.11", 50),
    ] {
        proxies.seed(&p);
    }
    let pool = Pool::new(proxies.clone());
    let ctl = Arc::new(BotController::new(
        pods.clone(),
        secrets,
        snapshots,
        control.clone(),
        clock.clone(),
        pool,
    ));
    let rollouts = Arc::new(FakeRollouts::new());
    let bots = Arc::new(FakeBotDirectory::new());
    let verifier = Arc::new(FakeVerifier::new());
    let metrics = Arc::new(FakeMetrics::new());
    let events = Arc::new(FakeEvents::new());
    let rc = BotRolloutController::with(
        rollouts.clone(),
        bots.clone(),
        ctl.clone(),
        clock.clone(),
        verifier.clone(),
        metrics.clone(),
        events.clone(),
        cfg,
    );
    Rig {
        rc,
        rollouts,
        bots,
        ctl,
        pods,
        control,
        clock,
        verifier,
        metrics,
        events,
    }
}

fn rig() -> Rig {
    rig_with_cfg(RolloutConfig::default())
}

fn mk_bot(name: &str) -> XenomitBot {
    mk_bot_version(name, VersionSpec::default())
}

fn mk_bot_version(name: &str, version: VersionSpec) -> XenomitBot {
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
            version,
            proxy: Default::default(),
            restore_policy: Default::default(),
            resources: None,
        },
    )
}

/// One rollout reconcile pass + persist — the real loop saves the CR
/// after each pass; `status` is the crash-recovery source of truth.
async fn pass(rig: &Rig, r: &mut BotRollout) {
    rig.rc.reconcile(r).await.unwrap();
    rig.rollouts.save(r).await.unwrap();
}

async fn passes(rig: &Rig, r: &mut BotRollout, n: usize) {
    for _ in 0..n {
        pass(rig, r).await;
    }
}

/// Drive a fresh bot to Running (epoch 1) and save it into the fleet dir.
async fn drive_to_running(rig: &Rig, name: &str) {
    drive_bot(rig, mk_bot(name)).await;
}

async fn drive_bot(rig: &Rig, mut bot: XenomitBot) {
    let name = bot.metadata.name.clone().unwrap();
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.pods.set_running(&name, Epoch(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.control.connect(&name, Epoch(1));
    rig.ctl.reconcile(&mut bot).await.unwrap();
    rig.ctl
        .on_phase_changed(&mut bot, Epoch(1), BotPhase::Running, "clean")
        .await
        .unwrap();
    assert_eq!(bot.status.as_ref().unwrap().phase, BotPhase::Running);
    rig.bots.save(&bot).await.unwrap();
}

/// Simulate botd for a bot whose upgrade/rollback handoff is armed:
/// successor pod runs → (pass: Drain) → predecessor Drained + exits +
/// disconnects, successor connects → (pass: Activate) → successor
/// reports Running → (pass: entry terminal). Advances 15 s during the
/// offline window so `offlineWindowMs` is measurable.
async fn finish_roll(rig: &Rig, r: &mut BotRollout, name: &str) {
    let (succ, pred) = {
        let b = rig.bots.get_sync(name).unwrap();
        let e = b.status.as_ref().unwrap().epoch;
        (e, e.prev())
    };
    rig.pods.set_running(name, succ);
    pass(rig, r).await; // Drain sent to predecessor
    let mut b = rig.bots.get_sync(name).unwrap();
    rig.ctl
        .on_drained(&mut b, pred, Some(SnapshotId("snap_ho".into())), true)
        .await
        .unwrap();
    rig.bots.save(&b).await.unwrap();
    rig.pods.exit(name, pred, Some(0));
    rig.control.disconnect(name, pred);
    rig.control.connect(name, succ);
    pass(rig, r).await; // Activate sent to successor — bot is now offline
    rig.clock.advance(Duration::from_secs(15)); // successor takes 15 s to come up
    let mut b = rig.bots.get_sync(name).unwrap();
    rig.ctl
        .on_phase_changed(&mut b, succ, BotPhase::Running, "clean")
        .await
        .unwrap();
    rig.bots.save(&b).await.unwrap();
    pass(rig, r).await; // entry terminal
}

fn mk_rollout(name: &str, target: &str) -> BotRollout {
    BotRollout::new(
        name,
        BotRolloutSpec {
            target_image: target.into(),
            target_game_version: Some("game-2026.03".into()),
            selector: RolloutSelector::default(),
            strategy: RolloutStrategy {
                canary: Some(CanaryStrategy {
                    count: 1,
                    bake_minutes: 2,
                    bots: vec![],
                }),
                waves: WaveStrategy {
                    max_draining: "2".into(),
                    max_offline: 5,
                    inter_wave_delay_s: 60,
                    login_jitter_s: 0,
                },
                gates: Default::default(),
            },
            on_gate_failure: GateFailureAction::Pause,
            paused: false,
            aborted: false,
            rollback: false,
            resume_count: 0,
        },
    )
}

fn status(r: &BotRollout) -> &fleet_types::crds::BotRolloutStatus {
    r.status.as_ref().unwrap()
}

fn entry<'a>(r: &'a BotRollout, name: &str) -> &'a fleet_types::crds::RolloutBotEntry {
    status(r).bots.iter().find(|e| e.bot == name).unwrap()
}

fn bot(rig: &Rig, name: &str) -> XenomitBot {
    rig.bots.get_sync(name).unwrap()
}

fn phase(r: &BotRollout) -> RolloutPhase {
    status(r).phase
}

// -- tests --------------------------------------------------------------------

/// Canary → bake → waves 1..N under `maxDraining`, gates passing;
/// Arm-before-Drain ordering and `status.image` moves verified end to end.
#[tokio::test]
async fn canary_pass_then_waves() {
    let rig = rig();
    for n in ["b1", "b2", "b3", "b4", "b5"] {
        drive_to_running(&rig, n).await;
    }
    let mut r = mk_rollout("roll-1", TARGET);

    pass(&rig, &mut r).await; // Pending -> Canarying
    assert_eq!(phase(&r), RolloutPhase::Canarying);
    assert_eq!(status(&r).total, 5);
    // Canary = alphabetically first; waves chunked by maxDraining=2.
    assert_eq!(entry(&r, "b1").wave, 0);
    assert_eq!(entry(&r, "b2").wave, 1);
    assert_eq!(entry(&r, "b3").wave, 1);
    assert_eq!(entry(&r, "b4").wave, 2);
    assert_eq!(entry(&r, "b5").wave, 2);

    pass(&rig, &mut r).await; // fire canary
    assert_eq!(entry(&r, "b1").state, RolloutBotState::Rolling);
    // The successor pod was armed on the TARGET image before any drain
    // (docs/03 §3.2: arm → drain → activate).
    let arm = rig.pods.created.lock().unwrap().last().unwrap().clone();
    assert_eq!(arm.bot_id, "b1");
    assert_eq!(arm.epoch, Epoch(2));
    assert_eq!(arm.image, TARGET);
    // Old pod still alive — no overlap loss.
    assert!(rig.pods.pod("b1", Epoch(1)).unwrap().is_alive());

    finish_roll(&rig, &mut r, "b1").await;
    assert_eq!(entry(&r, "b1").state, RolloutBotState::Done);
    assert_eq!(entry(&r, "b1").outcome, Some(RolloutBotOutcome::Success));
    assert!(entry(&r, "b1").offline_window_ms.unwrap() >= 15_000);
    // Only the canary rolled.
    assert_eq!(
        status(&r)
            .bots
            .iter()
            .filter(|e| e.state != RolloutBotState::Pending)
            .count(),
        1
    );

    // Bake 2 min, then gates eval -> wave 1.
    rig.clock.advance(Duration::from_secs(130));
    pass(&rig, &mut r).await;
    assert_eq!(phase(&r), RolloutPhase::Progressing);
    assert_eq!(status(&r).wave, 1);

    pass(&rig, &mut r).await; // wave 1 fires under maxDraining=2
    assert_eq!(entry(&r, "b2").state, RolloutBotState::Rolling);
    assert_eq!(entry(&r, "b3").state, RolloutBotState::Rolling);
    assert_eq!(entry(&r, "b4").state, RolloutBotState::Pending); // wave 2 untouched

    finish_roll(&rig, &mut r, "b2").await;
    finish_roll(&rig, &mut r, "b3").await;
    pass(&rig, &mut r).await; // wave 1 gates -> wave 2 ready at now+60s
    assert_eq!(phase(&r), RolloutPhase::Progressing);
    assert_eq!(entry(&r, "b4").state, RolloutBotState::Pending);
    pass(&rig, &mut r).await; // still inside inter-wave delay
    assert_eq!(entry(&r, "b4").state, RolloutBotState::Pending);

    rig.clock.advance(Duration::from_secs(61));
    pass(&rig, &mut r).await; // wave 2 fires
    assert_eq!(entry(&r, "b4").state, RolloutBotState::Rolling);
    assert_eq!(entry(&r, "b5").state, RolloutBotState::Rolling);
    finish_roll(&rig, &mut r, "b4").await;
    finish_roll(&rig, &mut r, "b5").await;
    pass(&rig, &mut r).await; // last wave gates -> Done
    assert_eq!(phase(&r), RolloutPhase::Done);

    for e in &status(&r).bots {
        assert_eq!(e.state, RolloutBotState::Done, "{}", e.bot);
        assert_eq!(e.outcome, Some(RolloutBotOutcome::Success), "{}", e.bot);
        assert_eq!(e.restore_quality.as_deref(), Some("clean"), "{}", e.bot);
    }
    assert!(status(&r).offline_window_p95_ms.unwrap() >= 15_000);
    for n in ["b1", "b2", "b3", "b4", "b5"] {
        assert_eq!(
            bot(&rig, n).status.as_ref().unwrap().image.as_deref(),
            Some(TARGET)
        );
    }
    // Events: phase transitions + a wave per stage + a roll per bot.
    let evs = rig.events.all();
    assert!(evs.contains(&RolloutEvent::PhaseChanged {
        rollout: "roll-1".into(),
        phase: RolloutPhase::Canarying
    }));
    for w in [0, 1, 2] {
        assert!(evs.contains(&RolloutEvent::WaveStarted {
            rollout: "roll-1".into(),
            wave: w
        }));
    }
    assert!(evs.contains(&RolloutEvent::PhaseChanged {
        rollout: "roll-1".into(),
        phase: RolloutPhase::Done
    }));
    assert_eq!(
        evs.iter()
            .filter(|e| matches!(e, RolloutEvent::BotRolled { ok: true, .. }))
            .count(),
        5
    );
}

/// `onGateFailure: pause` — a bad reward-rate sample pauses after the
/// bake; the operator fixes the cause and resumes via `resumeCount`.
#[tokio::test]
async fn gate_pause_resume() {
    let rig = rig();
    for n in ["b1", "b2"] {
        drive_to_running(&rig, n).await;
    }
    let mut r = mk_rollout("roll-gate", TARGET);
    r.spec.strategy.gates.min_reward_rate_ratio = Some(0.8);
    r.spec.strategy.canary.as_mut().unwrap().bake_minutes = 0;

    passes(&rig, &mut r, 1).await;
    pass(&rig, &mut r).await; // fire canary
    // The canary's reward ratio craters while it rolls; the gate eval on
    // the completing pass must catch it.
    rig.metrics.set("b1", 0.3);
    finish_roll(&rig, &mut r, "b1").await;
    assert_eq!(phase(&r), RolloutPhase::Paused);
    assert_eq!(status(&r).pause_cause, Some(PauseCause::Gate));
    assert_eq!(entry(&r, "b2").state, RolloutBotState::Pending);

    // Plain unpause does NOT clear a gate pause — the spec flag was
    // never set; only resumeCount acknowledges the gate.
    pass(&rig, &mut r).await;
    assert_eq!(phase(&r), RolloutPhase::Paused);

    // Operator fixes the cause and resumes.
    rig.metrics.set("b1", 0.95);
    r.spec.resume_count += 1;
    pass(&rig, &mut r).await; // resume -> back to canary gate eval
    assert_eq!(phase(&r), RolloutPhase::Canarying);
    pass(&rig, &mut r).await; // gates pass now -> Progressing
    assert_eq!(phase(&r), RolloutPhase::Progressing);

    pass(&rig, &mut r).await; // wave 1 fires
    finish_roll(&rig, &mut r, "b2").await;
    pass(&rig, &mut r).await;
    assert_eq!(phase(&r), RolloutPhase::Done);
}

/// `onGateFailure: rollback` — offline-window gate breach hands every
/// rolled bot back to its previous image through the same machinery.
#[tokio::test]
async fn gate_failure_rolls_back() {
    let rig = rig();
    for n in ["b1", "b2"] {
        drive_to_running(&rig, n).await;
    }
    let mut r = mk_rollout("roll-rb", TARGET);
    r.spec.on_gate_failure = GateFailureAction::Rollback;
    r.spec.strategy.canary.as_mut().unwrap().bake_minutes = 0;
    r.spec.strategy.gates.max_offline_window_ms = Some(10_000);

    pass(&rig, &mut r).await;
    pass(&rig, &mut r).await; // fire canary
    // The roll exceeds the 10 s offline budget (finish_roll waits 15 s).
    finish_roll(&rig, &mut r, "b1").await;

    pass(&rig, &mut r).await; // canary gates: offline_window over -> rollback
    assert_eq!(phase(&r), RolloutPhase::RollingBack);

    // b1 is handed back to the stable image through a real handoff.
    pass(&rig, &mut r).await; // rollback handoff armed (epoch 3)
    assert_eq!(entry(&r, "b1").state, RolloutBotState::RollingBack);
    let arm = rig.pods.created.lock().unwrap().last().unwrap().clone();
    assert_eq!(arm.bot_id, "b1");
    assert_eq!(arm.epoch, Epoch(3));
    assert_eq!(arm.image, STABLE, "rollback rearms the previous image");

    finish_roll(&rig, &mut r, "b1").await;
    pass(&rig, &mut r).await;
    assert_eq!(phase(&r), RolloutPhase::RolledBack);
    assert_eq!(entry(&r, "b1").state, RolloutBotState::RolledBack);
    assert_eq!(
        bot(&rig, "b1").status.as_ref().unwrap().image.as_deref(),
        Some(STABLE)
    );
    // Untouched entries are recorded as skipped, never rolled.
    assert_eq!(entry(&r, "b2").state, RolloutBotState::Skipped);
}

/// The shared login token bucket paces starts; `loginJitterS` spreads
/// grants deterministically inside `[now, now + jitter]`.
#[tokio::test]
async fn login_storm_limited() {
    let cfg = RolloutConfig {
        login_rate_per_sec: 1.0,
        login_burst: 2,
        ..Default::default()
    };
    let rig = rig_with_cfg(cfg);
    for n in ["b1", "b2", "b3", "b4"] {
        drive_to_running(&rig, n).await;
    }
    let mut r = mk_rollout("roll-storm", TARGET);
    r.spec.strategy.canary = None; // straight to waves
    r.spec.strategy.waves.max_draining = "4".into();

    pass(&rig, &mut r).await; // -> Progressing (single wave of 4)
    pass(&rig, &mut r).await; // burst = 2 tokens
    let rolling = status(&r)
        .bots
        .iter()
        .filter(|e| e.state == RolloutBotState::Rolling)
        .count();
    assert_eq!(rolling, 2, "login burst caps simultaneous starts");

    rig.clock.advance(Duration::from_secs(1));
    pass(&rig, &mut r).await; // +1 token per second
    assert_eq!(
        status(&r)
            .bots
            .iter()
            .filter(|e| e.state == RolloutBotState::Rolling)
            .count(),
        3
    );
    rig.clock.advance(Duration::from_secs(1));
    pass(&rig, &mut r).await;
    assert_eq!(
        status(&r)
            .bots
            .iter()
            .filter(|e| e.state == RolloutBotState::Rolling)
            .count(),
        4
    );
}

/// Bucket semantics: deterministic grants, refill at `rate_per_sec`,
/// jitter inside `[now, now + jitter]`.
#[test]
fn login_bucket_semantics() {
    let mut b = LoginBucket::new(1.0, 2);
    // Burst of two, then empty.
    assert!(b.try_acquire(1_000, Duration::ZERO, 7).is_some());
    assert!(b.try_acquire(1_000, Duration::ZERO, 8).is_some());
    assert!(b.try_acquire(1_000, Duration::ZERO, 9).is_none());
    // One second refills exactly one token.
    let t = b.try_acquire(2_000, Duration::ZERO, 9).unwrap();
    assert_eq!(t, 2_000);
    assert!(b.try_acquire(2_000, Duration::ZERO, 10).is_none());
    // Jitter is deterministic per seed and inside (now, now + max].
    let j1 = b.try_acquire(10_000, Duration::from_secs(30), 42);
    let mut b2 = LoginBucket::new(1.0, 2);
    assert!(b2.try_acquire(1, Duration::ZERO, 1).is_some());
    let j2 = b2.try_acquire(10_000, Duration::from_secs(30), 42);
    assert_eq!(j1, j2);
    assert!((10_000..=40_000).contains(&j1.unwrap()));
}

/// `spec.paused` holds new starts while in-flight handoffs finish;
/// resume picks the waves back up. `spec.aborted` skips everything not
/// yet rolling.
#[tokio::test]
async fn pause_resume_abort() {
    let rig = rig();
    for n in ["b1", "b2", "b3", "b4"] {
        drive_to_running(&rig, n).await;
    }
    let mut r = mk_rollout("roll-pa", TARGET);
    r.spec.strategy.canary = None; // waves 1,2 of two

    pass(&rig, &mut r).await;
    pass(&rig, &mut r).await; // wave 1 fires b1,b2
    assert_eq!(phase(&r), RolloutPhase::Progressing);

    r.spec.paused = true;
    pass(&rig, &mut r).await;
    assert_eq!(phase(&r), RolloutPhase::Paused);
    assert_eq!(entry(&r, "b3").state, RolloutBotState::Pending);

    // In-flight rolls still finish under the pause.
    finish_roll(&rig, &mut r, "b1").await;
    finish_roll(&rig, &mut r, "b2").await;
    assert_eq!(phase(&r), RolloutPhase::Paused);

    r.spec.paused = false;
    pass(&rig, &mut r).await; // resume -> wave 1 gate eval -> wave 2 timer
    assert_eq!(phase(&r), RolloutPhase::Progressing);

    // Abort during the inter-wave soak: pending entries are skipped,
    // nothing new fires.
    r.spec.aborted = true;
    pass(&rig, &mut r).await; // -> Aborting
    pass(&rig, &mut r).await; // pending skipped -> Aborted
    assert_eq!(phase(&r), RolloutPhase::Aborted);
    assert_eq!(entry(&r, "b3").state, RolloutBotState::Skipped);
    assert_eq!(entry(&r, "b4").state, RolloutBotState::Skipped);
    assert!(bot(&rig, "b3").status.as_ref().unwrap().handoff.is_none());
}

/// A successor that dies before draining ends the handoff — the bot
/// stays Running on the old image (zero downtime), and the entry is a
/// gate-visible `UpgradeAborted` failure.
#[tokio::test]
async fn successor_crash_reverts_to_old_image() {
    let rig = rig();
    drive_to_running(&rig, "b1").await;
    let mut r = mk_rollout("roll-crash", TARGET);
    r.spec.strategy.gates.max_error_rate_permille = Some(0);

    pass(&rig, &mut r).await;
    pass(&rig, &mut r).await; // canary fires; successor e2 armed
    assert_eq!(entry(&r, "b1").state, RolloutBotState::Rolling);

    // The new image crashes before it ever serves.
    rig.pods.exit("b1", Epoch(2), Some(1));
    pass(&rig, &mut r).await; // reconcile sees the dead successor
    pass(&rig, &mut r).await; // entry classified

    let b = bot(&rig, "b1");
    let st = b.status.as_ref().unwrap();
    assert_eq!(
        st.phase,
        BotPhase::Running,
        "bot stayed up on the old image"
    );
    assert_eq!(st.epoch, Epoch(1), "reverted to the predecessor epoch");
    assert_eq!(st.image.as_deref(), Some(STABLE));
    assert!(st.handoff.is_none());
    assert_eq!(entry(&r, "b1").state, RolloutBotState::Failed);
    assert_eq!(
        entry(&r, "b1").outcome,
        Some(RolloutBotOutcome::UpgradeAborted)
    );
    pass(&rig, &mut r).await; // bake=2min -> advance past it, gate eval
    rig.clock.advance(Duration::from_secs(130));
    pass(&rig, &mut r).await;
    // Default gates: 1/1 error rate fails -> onGateFailure pause.
    assert_eq!(phase(&r), RolloutPhase::Paused);
}

/// A restart mid-rollout loses only the in-memory token bucket: status
/// on the CR and the bots resumes the roll where it left off.
#[tokio::test]
async fn controller_restart_resumes_from_status() {
    let rig = rig();
    for n in ["b1", "b2"] {
        drive_to_running(&rig, n).await;
    }
    let mut r = mk_rollout("roll-restart", TARGET);

    pass(&rig, &mut r).await;
    pass(&rig, &mut r).await; // canary b1 fired, handoff armed

    // "Crash": a brand-new controller over the same stores (fresh login
    // bucket, no memory of the pass so far).
    let rc2 = BotRolloutController::new(
        rig.rollouts.clone(),
        rig.bots.clone(),
        rig.ctl.clone(),
        rig.clock.clone(),
        rig.verifier.clone(),
        rig.metrics.clone(),
        rig.events.clone(),
    );
    rig.rollouts.save(&r).await.unwrap();
    let mut r2 = rig.rollouts.get_sync("roll-restart").unwrap();
    rc2.reconcile(&mut r2).await.unwrap(); // still Canarying, entry Rolling
    assert_eq!(phase(&r2), RolloutPhase::Canarying);
    assert_eq!(entry(&r2, "b1").state, RolloutBotState::Rolling);
    rig.rollouts.save(&r2).await.unwrap();
    r = r2;

    // Finish under the new controller.
    finish_roll(&rig, &mut r, "b1").await;
    rig.clock.advance(Duration::from_secs(130));
    pass(&rig, &mut r).await; // -> Progressing
    pass(&rig, &mut r).await; // wave 1 fires b2
    finish_roll(&rig, &mut r, "b2").await;
    pass(&rig, &mut r).await;
    assert_eq!(phase(&r), RolloutPhase::Done);
}

/// Only one rollout may be active: a second `BotRollout` fails closed,
/// and between two Pending rollouts the smaller name wins —
/// deterministically.
#[tokio::test]
async fn concurrent_rollout_rejected() {
    let rig = rig();
    drive_to_running(&rig, "b1").await;
    let mut a = mk_rollout("a-roll", TARGET);
    let mut b = mk_rollout("b-roll", TARGET);
    rig.rollouts.save(&a).await.unwrap();
    rig.rollouts.save(&b).await.unwrap();

    // Reconcile the loser first: `a-roll` is also Pending and sorts
    // first, so `b-roll` is the one rejected.
    rig.rc.reconcile(&mut b).await.unwrap();
    rig.rollouts.save(&b).await.unwrap();
    assert_eq!(phase(&b), RolloutPhase::Failed);
    assert!(status(&b).detail.contains("a-roll"));

    pass(&rig, &mut a).await; // a proceeds
    assert_eq!(phase(&a), RolloutPhase::Canarying);
    // Its canary fired but `b-roll` never touched a bot.
    assert!(status(&b).bots.is_empty());
}

/// Game update detect → rollout (ci/docs/needsupdate-failsafe.md): a
/// bot that exits 68 parks in `NeedsUpdate` and holds there until a
/// rollout targeting the new version *releases* it — never retried on
/// the old image. Pinned-channel bots are skipped, not forced.
#[tokio::test]
async fn needs_update_release() {
    let rig = rig();
    drive_to_running(&rig, "b1").await;
    drive_bot(
        &rig,
        mk_bot_version(
            "b2",
            VersionSpec {
                channel: VersionChannel::Pinned,
                pinned_image: Some("registry.local/xenomit-botd:pinned".into()),
            },
        ),
    )
    .await;

    // Game update lands: both bots exit 68 (VERSION_MISMATCH) and park.
    for n in ["b1", "b2"] {
        rig.pods.exit(n, Epoch(1), Some(68));
        rig.control.disconnect(n, Epoch(1));
        let mut b = bot(&rig, n);
        rig.ctl.reconcile(&mut b).await.unwrap();
        rig.bots.save(&b).await.unwrap();
        assert_eq!(b.status.as_ref().unwrap().phase, BotPhase::NeedsUpdate);
    }
    // The failsafe: no replacement pod was spawned on the old image.
    assert!(rig.pods.pod("b1", Epoch(2)).is_none());

    let mut r = mk_rollout("roll-update", TARGET);
    r.spec.strategy.canary = None;
    pass(&rig, &mut r).await;
    assert_eq!(entry(&r, "b1").state, RolloutBotState::Pending);
    assert!(entry(&r, "b1").from_needs_update);
    assert!(entry(&r, "b2").from_needs_update);

    pass(&rig, &mut r).await; // release b1; b2 is pinned -> skipped
    assert_eq!(entry(&r, "b1").state, RolloutBotState::Rolling);
    assert_eq!(entry(&r, "b2").state, RolloutBotState::Skipped);
    let b = bot(&rig, "b1");
    assert_eq!(b.status.as_ref().unwrap().phase, BotPhase::Provisioning);
    assert_eq!(b.status.as_ref().unwrap().image.as_deref(), Some(TARGET));

    pass(&rig, &mut r).await; // spawn epoch 2 on the target
    let spawn = rig.pods.created.lock().unwrap().last().unwrap().clone();
    assert_eq!(spawn.bot_id, "b1");
    assert_eq!(spawn.image, TARGET);
    rig.pods.set_running("b1", Epoch(2));
    pass(&rig, &mut r).await; // Armed
    rig.control.connect("b1", Epoch(2));
    pass(&rig, &mut r).await; // Activate -> Starting
    let mut b = bot(&rig, "b1");
    rig.ctl
        .on_phase_changed(&mut b, Epoch(2), BotPhase::Running, "clean")
        .await
        .unwrap();
    rig.bots.save(&b).await.unwrap();
    pass(&rig, &mut r).await; // entry Done

    assert_eq!(entry(&r, "b1").state, RolloutBotState::Done);
    assert_eq!(entry(&r, "b1").outcome, Some(RolloutBotOutcome::Success));
    assert!(entry(&r, "b1").offline_window_ms.is_some());
    assert_eq!(
        bot(&rig, "b1").status.as_ref().unwrap().image.as_deref(),
        Some(TARGET)
    );
}

/// Released `NeedsUpdate` bots were already down — they don't spend
/// `maxOffline` (ci/docs/needsupdate-failsafe.md).
#[tokio::test]
async fn needs_update_release_exempt_from_offline_cap() {
    let rig = rig();
    for n in ["b1", "b2"] {
        drive_to_running(&rig, n).await;
        rig.pods.exit(n, Epoch(1), Some(68));
        rig.control.disconnect(n, Epoch(1));
        let mut b = bot(&rig, n);
        rig.ctl.reconcile(&mut b).await.unwrap();
        rig.bots.save(&b).await.unwrap();
    }
    let mut r = mk_rollout("roll-nu-cap", TARGET);
    r.spec.strategy.canary = None;
    r.spec.strategy.waves.max_offline = 1; // still admits both releases

    pass(&rig, &mut r).await;
    pass(&rig, &mut r).await;
    assert_eq!(entry(&r, "b1").state, RolloutBotState::Rolling);
    assert_eq!(entry(&r, "b2").state, RolloutBotState::Rolling);
}

/// `spec.rollback` mid-rollout: rolled bots hand back, in-flight ones
/// finish, pending ones stay.
#[tokio::test]
async fn manual_rollback() {
    let rig = rig();
    for n in ["b1", "b2"] {
        drive_to_running(&rig, n).await;
    }
    let mut r = mk_rollout("roll-manual-rb", TARGET);
    r.spec.strategy.canary = None;

    pass(&rig, &mut r).await;
    pass(&rig, &mut r).await; // wave fires b1,b2
    finish_roll(&rig, &mut r, "b1").await;
    r.spec.rollback = true;
    finish_roll(&rig, &mut r, "b2").await; // in-flight completes to Done
    pass(&rig, &mut r).await; // -> RollingBack
    assert_eq!(phase(&r), RolloutPhase::RollingBack);

    pass(&rig, &mut r).await; // rollback handoffs arm (epoch 3)
    assert_eq!(entry(&r, "b1").state, RolloutBotState::RollingBack);
    assert_eq!(entry(&r, "b2").state, RolloutBotState::RollingBack);
    finish_roll(&rig, &mut r, "b1").await;
    finish_roll(&rig, &mut r, "b2").await;
    pass(&rig, &mut r).await;

    assert_eq!(phase(&r), RolloutPhase::RolledBack);
    for n in ["b1", "b2"] {
        assert_eq!(
            bot(&rig, n).status.as_ref().unwrap().image.as_deref(),
            Some(STABLE)
        );
    }
}

/// The digest-admission hook fails closed: a rejected image kills the
/// rollout in Pending and never touches a bot.
#[tokio::test]
async fn verifier_denial_fails_closed() {
    let rig = rig();
    drive_to_running(&rig, "b1").await;
    rig.verifier.deny(TARGET, "unsigned digest");
    let mut r = mk_rollout("roll-deny", TARGET);

    pass(&rig, &mut r).await;
    assert_eq!(phase(&r), RolloutPhase::Failed);
    assert!(status(&r).detail.contains("unsigned digest"));
    assert!(status(&r).bots.is_empty());
    assert!(rig.pods.pod("b1", Epoch(2)).is_none());
}

/// Selector scoping: only matching bots are touched; a `canary.bots`
/// name outside the selector is recorded as skipped, never rolled.
#[tokio::test]
async fn selector_scopes_the_rollout() {
    let rig = rig();
    drive_to_running(&rig, "b1").await;
    drive_bot(
        &rig,
        mk_bot_version(
            "b2",
            VersionSpec {
                channel: VersionChannel::Canary,
                pinned_image: None,
            },
        ),
    )
    .await;

    let mut r = mk_rollout("roll-sel", TARGET);
    r.spec.selector.channel = Some(VersionChannel::Stable);
    r.spec.strategy.canary = Some(CanaryStrategy {
        count: 0,
        bake_minutes: 0,
        bots: vec!["b2".into()], // pinned but not selected
    });
    r.spec.strategy.waves.max_draining = "1".into();

    pass(&rig, &mut r).await;
    assert_eq!(entry(&r, "b1").state, RolloutBotState::Pending);
    assert_eq!(entry(&r, "b1").wave, 1);
    assert_eq!(entry(&r, "b2").state, RolloutBotState::Skipped);
    assert_eq!(phase(&r), RolloutPhase::Progressing);

    pass(&rig, &mut r).await;
    finish_roll(&rig, &mut r, "b1").await;
    pass(&rig, &mut r).await;
    assert_eq!(phase(&r), RolloutPhase::Done);
    // b2 never saw a handoff or a second pod.
    assert_eq!(rig.pods.count("b2"), 1);
    assert!(bot(&rig, "b2").status.as_ref().unwrap().handoff.is_none());
}
