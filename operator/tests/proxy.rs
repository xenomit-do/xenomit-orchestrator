//! `ProxyController` integration tests: probe → health → failover →
//! graceful `proxy_change` handoff through the real `BotController`
//! (docs/05 §5.4), plus fixed-mode probe reporting.

use std::sync::Arc;

use fleet_types::condition_type;
use fleet_types::crds::{
    AccountSpec, BotConfigSpec, Proxy, ProxyMode, ProxyScheme, ProxySpec, ProxyStatus, XenomitBot,
    XenomitBotSpec,
};
use fleet_types::epoch::Epoch;
use fleet_types::phase::{BotPhase, ConditionStatus, DesiredState};

use operator::fakes::{FakeBotDirectory, FakeControl, FakePods, FakeProber, FakeSnapshots};
use operator::traits::ManualClock;
use operator::{
    BotController, MemoryProxyStore, MemorySecretStore, Pool, ProbeReport, ProxyController,
    ProxyEndpoint, ProxyStore,
};

type Ctl = BotController<
    Arc<FakePods>,
    Arc<MemorySecretStore>,
    Arc<FakeSnapshots>,
    Arc<FakeControl>,
    Arc<ManualClock>,
    Pool<Arc<MemoryProxyStore>>,
>;

type PxCtl = ProxyController<
    Arc<MemoryProxyStore>,
    Arc<FakeProber>,
    Arc<FakeBotDirectory>,
    Arc<ManualClock>,
    Arc<Ctl>,
>;

struct Rig {
    px: PxCtl,
    ctl: Arc<Ctl>,
    pods: Arc<FakePods>,
    control: Arc<FakeControl>,
    proxies: Arc<MemoryProxyStore>,
    prober: Arc<FakeProber>,
    bots: Arc<FakeBotDirectory>,
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
    let prober = Arc::new(FakeProber::new());
    let bots = Arc::new(FakeBotDirectory::new());
    let pool = Pool::new(proxies.clone());
    let ctl = Arc::new(BotController::new(
        pods.clone(),
        secrets.clone(),
        snapshots.clone(),
        control.clone(),
        clock.clone(),
        pool,
    ));
    let px = ProxyController::new(
        proxies.clone(),
        prober.clone(),
        bots.clone(),
        clock.clone(),
        ctl.clone(),
    );
    Rig {
        px,
        ctl,
        pods,
        control,
        proxies,
        prober,
        bots,
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

fn endpoint(p: &Proxy) -> ProxyEndpoint {
    ProxyEndpoint {
        scheme: Some(p.spec.scheme),
        host: p.spec.host.clone(),
        port: p.spec.port,
    }
}

fn fail_report(msg: &str) -> ProbeReport {
    ProbeReport {
        ok: false,
        latency_ms: None,
        exit_ip: None,
        resolved_ips: vec![],
        error: Some(msg.into()),
    }
}

fn ok_report() -> ProbeReport {
    ProbeReport {
        ok: true,
        latency_ms: Some(120),
        exit_ip: Some("198.51.100.7".into()),
        resolved_ips: vec!["203.0.113.10".into()],
        error: None,
    }
}

/// Drive a fresh bot to Running, then publish it to the bot directory.
async fn drive_and_publish(rig: &Rig, bot: &mut XenomitBot) {
    let name = bot.metadata.name.clone().unwrap();
    rig.ctl.reconcile(bot).await.unwrap();
    rig.ctl.reconcile(bot).await.unwrap();
    rig.pods.set_running(&name, Epoch(1));
    rig.ctl.reconcile(bot).await.unwrap();
    rig.control.connect(&name, Epoch(1));
    rig.ctl.reconcile(bot).await.unwrap();
    rig.ctl
        .on_phase_changed(bot, Epoch(1), BotPhase::Running, "")
        .await
        .unwrap();
    assert_eq!(bot.status.as_ref().unwrap().phase, BotPhase::Running);
    rig.bots.seed(bot);
}

fn proxy_status(rig: &Rig, name: &str) -> ProxyStatus {
    rig.proxies.get_sync(name).unwrap().status.unwrap()
}

#[tokio::test]
async fn three_failed_probes_unhealthy_and_failover() {
    let rig = rig();
    let mut bot = mk_bot("bot-f1");
    drive_and_publish(&rig, &mut bot).await;
    assert_eq!(
        bot.status.as_ref().unwrap().proxy_ref.as_deref(),
        Some("px-a")
    );

    let ep = endpoint(&rig.proxies.get_sync("px-a").unwrap());
    rig.prober.set(&ep, fail_report("connect timeout"));

    // Two failures: still sticky (below threshold).
    rig.px.reconcile().await.unwrap();
    rig.px.reconcile().await.unwrap();
    assert!(proxy_status(&rig, "px-a").healthy);
    let b = rig.bots.get_sync("bot-f1").unwrap();
    assert_eq!(
        b.status.as_ref().unwrap().proxy_ref.as_deref(),
        Some("px-a")
    );

    // Third failure: unhealthy → failover to px-b with a proxy_change
    // handoff arming epoch 2 (docs/05 §5.4).
    let out = rig.px.reconcile().await.unwrap();
    assert!(out.requeue_after.is_some());
    let pa = proxy_status(&rig, "px-a");
    assert!(!pa.healthy);
    assert_eq!(pa.consecutive_failures, 3);
    assert!(!pa.assigned.contains(&"bot-f1".to_string()));
    assert_eq!(pa.last_tenant.as_deref(), Some("user_abc"));

    let b = rig.bots.get_sync("bot-f1").unwrap();
    let bst = b.status.as_ref().unwrap();
    assert_eq!(bst.proxy_ref.as_deref(), Some("px-b"));
    assert_eq!(
        bst.handoff.as_ref().unwrap().reason,
        fleet_types::HandoffReason::ProxyChange
    );
    let cond = bst
        .conditions
        .iter()
        .find(|c| c.type_ == condition_type::PROXY_HEALTHY)
        .unwrap();
    assert_eq!(cond.status, ConditionStatus::False);
    assert_eq!(cond.reason.as_deref(), Some("failover"));

    // The handoff armed the successor under the replacement proxy.
    let spec = rig
        .pods
        .created
        .lock()
        .unwrap()
        .iter()
        .find(|s| s.bot_id == "bot-f1" && s.epoch == Epoch(2))
        .cloned()
        .expect("epoch-2 pod spec");
    assert_eq!(spec.proxy.unwrap().name.as_deref(), Some("px-b"));
    assert!(
        proxy_status(&rig, "px-b")
            .assigned
            .contains(&"bot-f1".to_string())
    );
}

#[tokio::test]
async fn no_replacement_keeps_dead_proxy_ref() {
    let rig = rig_with(&[mk_proxy("px-solo", "203.0.113.10", 5)]);
    let mut bot = mk_bot("bot-nr");
    drive_and_publish(&rig, &mut bot).await;

    let ep = endpoint(&rig.proxies.get_sync("px-solo").unwrap());
    rig.prober.set(&ep, fail_report("refused"));
    for _ in 0..3 {
        rig.px.reconcile().await.unwrap();
    }
    assert!(!proxy_status(&rig, "px-solo").healthy);
    let b = rig.bots.get_sync("bot-nr").unwrap();
    let bst = b.status.as_ref().unwrap();
    // proxyRef stays: botd's own require_proxy exit-69 path handles it
    // once the pool recovers (docs/05 §5.4).
    assert_eq!(bst.proxy_ref.as_deref(), Some("px-solo"));
    assert!(bst.handoff.is_none());
    let cond = bst
        .conditions
        .iter()
        .find(|c| c.type_ == condition_type::PROXY_HEALTHY)
        .unwrap();
    assert_eq!(cond.reason.as_deref(), Some("no_replacement"));
}

#[tokio::test]
async fn probe_recovery_restores_health() {
    let rig = rig();
    let ep = endpoint(&rig.proxies.get_sync("px-a").unwrap());
    rig.prober.set(&ep, fail_report("timeout"));
    for _ in 0..3 {
        rig.px.reconcile().await.unwrap();
    }
    assert!(!proxy_status(&rig, "px-a").healthy);

    rig.prober.set(&ep, ok_report());
    rig.px.reconcile().await.unwrap();
    let pa = proxy_status(&rig, "px-a");
    assert!(pa.healthy);
    assert_eq!(pa.consecutive_failures, 0);
    assert_eq!(pa.latency_ms, Some(120));
    assert_eq!(pa.resolved_ips, vec!["203.0.113.10".to_string()]);
    let cond = pa
        .conditions
        .iter()
        .find(|c| c.type_ == condition_type::PROXY_HEALTHY)
        .unwrap();
    assert_eq!(cond.status, ConditionStatus::True);
    assert_eq!(cond.reason.as_deref(), Some("recovered"));
}

#[tokio::test]
async fn fixed_mode_bot_probed_but_never_reassigned() {
    let rig = rig();
    let mut bot = mk_bot("bot-fx");
    bot.spec.proxy = fleet_types::crds::ProxyPolicySpec {
        mode: ProxyMode::Fixed,
        selector: None,
        fixed_vault_path: None,
        fixed_addr: Some("192.0.2.44:3128".into()),
    };
    drive_and_publish(&rig, &mut bot).await;

    rig.prober.set(
        &ProxyEndpoint {
            scheme: None,
            host: "192.0.2.44".into(),
            port: 3128,
        },
        fail_report("auth rejected"),
    );
    for _ in 0..3 {
        rig.px.reconcile().await.unwrap();
    }
    let b = rig.bots.get_sync("bot-fx").unwrap();
    let bst = b.status.as_ref().unwrap();
    assert_eq!(bst.proxy_probe_failures, 3);
    assert!(bst.proxy_ref.is_none(), "fixed mode has no pool proxyRef");
    assert!(bst.handoff.is_none(), "no failover for user proxy");
    let cond = bst
        .conditions
        .iter()
        .find(|c| c.type_ == condition_type::PROXY_HEALTHY)
        .unwrap();
    assert_eq!(cond.status, ConditionStatus::False);
    assert_eq!(cond.message.as_deref(), Some("auth rejected"));
}

#[tokio::test]
async fn fixed_mode_probe_records_exit_ip() {
    let rig = rig();
    let mut bot = mk_bot("bot-fe");
    bot.spec.proxy = fleet_types::crds::ProxyPolicySpec {
        mode: ProxyMode::Fixed,
        selector: None,
        fixed_vault_path: None,
        fixed_addr: Some("192.0.2.44:3128".into()),
    };
    drive_and_publish(&rig, &mut bot).await;

    rig.prober.set(
        &ProxyEndpoint {
            scheme: None,
            host: "192.0.2.44".into(),
            port: 3128,
        },
        ProbeReport {
            ok: true,
            latency_ms: Some(80),
            exit_ip: Some("203.0.113.200".into()),
            resolved_ips: vec!["192.0.2.44".into()],
            error: None,
        },
    );
    rig.px.reconcile().await.unwrap();
    let b = rig.bots.get_sync("bot-fe").unwrap();
    let bst = b.status.as_ref().unwrap();
    assert_eq!(bst.proxy_exit_ip.as_deref(), Some("203.0.113.200"));
    assert_eq!(bst.proxy_resolved_ip.as_deref(), Some("192.0.2.44"));
    assert_eq!(bst.proxy_probe_failures, 0);
}

#[tokio::test]
async fn stale_capacity_claim_pruned() {
    let rig = rig();
    // Forge a claim for a bot that doesn't exist: the reconcile must
    // prune it and start the cooldown bookkeeping (docs/05 §5.4).
    let mut px_a = rig.proxies.get_sync("px-a").unwrap();
    px_a.status.as_mut().unwrap().assigned = vec!["ghost-bot".into()];
    rig.proxies.save(&px_a).await.unwrap();

    let mut bot = mk_bot("bot-real");
    drive_and_publish(&rig, &mut bot).await;
    // px-a carries the phantom claim so px-b wins the ranking.
    assert_eq!(
        bot.status.as_ref().unwrap().proxy_ref.as_deref(),
        Some("px-b")
    );
    rig.px.reconcile().await.unwrap();

    let pa = proxy_status(&rig, "px-a");
    assert!(!pa.assigned.contains(&"ghost-bot".to_string()));
    let pb = proxy_status(&rig, "px-b");
    assert!(pb.assigned.contains(&"bot-real".to_string()));
}
