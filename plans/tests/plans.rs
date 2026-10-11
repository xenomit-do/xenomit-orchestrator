//! Offline tests for the `plans` crate: pure aggregation against crafted
//! timelines (restarts, pauses, hour splits), the quota engine, the
//! retention purge job, showback, dunning and invites. No services.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use fleet_types::crds::XenomitBot;
use fleet_types::phase::{BotPhase, NeedsHumanKind};
use operator::traits::ManualClock;
use plans::*;

const H: i64 = HOUR_MS;
const H0: i64 = H * 470_000; // an aligned hour

fn subject() -> UsageSubject {
    UsageSubject {
        bot_id: "b1".into(),
        tenant: "alice".into(),
        version: "stable".into(),
        plan_id: "friends".into(),
    }
}

fn totals(recs: &[UsageRecord]) -> [u64; 4] {
    let mut t = [0u64; 4];
    for r in recs {
        t[0] += r.running_s;
        t[1] += r.offline_s;
        t[2] += r.needs_human_s;
        t[3] += r.user_paused_s;
    }
    t
}

// -- metering ---------------------------------------------------------------

#[test]
fn meter_full_hour_running() {
    let tl = [PhaseEvent::at(H0, BotPhase::Running)];
    let recs = meter_window(&subject(), &tl, H0, H0 + H, tl.first().copied());
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].hour_unix_ms, H0);
    assert_eq!(recs[0].running_s, 3600);
    assert_eq!(recs[0].offline_s, 0);
}

#[test]
fn meter_restart_splits_running_and_offline() {
    // Running, crash at +20m, back Running at +30m → 20m running, 10m
    // offline (provisioning gap), 30m running.
    let tl = [
        PhaseEvent::at(H0, BotPhase::Running),
        PhaseEvent::at(H0 + 20 * 60_000, BotPhase::Provisioning),
        PhaseEvent::at(H0 + 30 * 60_000, BotPhase::Running),
    ];
    let recs = meter_window(&subject(), &tl, H0, H0 + H, tl.first().copied());
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].running_s, 20 * 60 + 30 * 60);
    assert_eq!(recs[0].offline_s, 10 * 60);
    assert_eq!(recs[0].total_s(), 3600);
}

#[test]
fn meter_pause_is_user_caused_not_offline() {
    // Paused by the user lands in `user_paused_s`; it must not pollute
    // `offline_s` (docs/12 §12.5.4 SLO denominator).
    let tl = [
        PhaseEvent::at(H0, BotPhase::Running),
        PhaseEvent::at(H0 + 15 * 60_000, BotPhase::Paused),
        PhaseEvent::at(H0 + 45 * 60_000, BotPhase::Running),
    ];
    let recs = meter_window(&subject(), &tl, H0, H0 + H, tl.first().copied());
    assert_eq!(recs[0].running_s, 30 * 60);
    assert_eq!(recs[0].user_paused_s, 30 * 60);
    assert_eq!(recs[0].offline_s, 0);
}

#[test]
fn meter_quota_parked_counts_as_offline() {
    // Quota loss parks the bot — policy-paused, not user-caused, so it is
    // infrastructure downtime (docs/12 §12.3).
    let tl = [
        PhaseEvent::at(H0, BotPhase::Running),
        PhaseEvent {
            at_unix_ms: H0 + 10 * 60_000,
            phase: BotPhase::Paused,
            detail: Some(NeedsHumanKind::Quota),
        },
    ];
    let recs = meter_window(&subject(), &tl, H0, H0 + H, tl.first().copied());
    assert_eq!(recs[0].running_s, 10 * 60);
    assert_eq!(recs[0].offline_s, 50 * 60);
    assert_eq!(recs[0].user_paused_s, 0);
}

#[test]
fn meter_needs_human_gets_own_bucket() {
    let tl = [
        PhaseEvent::at(H0, BotPhase::Running),
        PhaseEvent::at(H0 + 10 * 60_000, BotPhase::NeedsHuman),
        PhaseEvent::at(H0 + 20 * 60_000, BotPhase::Running),
    ];
    let recs = meter_window(&subject(), &tl, H0, H0 + H, tl.first().copied());
    assert_eq!(recs[0].needs_human_s, 10 * 60);
    assert_eq!(recs[0].offline_s, 0);
}

#[test]
fn meter_spans_hour_boundaries() {
    // First transition at H0+30m (no earlier history → unknown head meters
    // as Offline), Running through H0+2H.
    let tl = [PhaseEvent::at(H0 + 30 * 60_000, BotPhase::Running)];
    let recs = meter_window(&subject(), &tl, H0, H0 + 2 * H, None);
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].hour_unix_ms, H0);
    assert_eq!(recs[0].offline_s, 1800);
    assert_eq!(recs[0].running_s, 1800);
    assert_eq!(recs[1].hour_unix_ms, H0 + H);
    assert_eq!(recs[1].running_s, 3600);
}

#[test]
fn meter_unknown_initial_is_offline_and_gaps_fill() {
    // No initial → the window head meters as Offline until the first event.
    let tl = [PhaseEvent::at(H0 + 30 * 60_000, BotPhase::Running)];
    let recs = meter_window(&subject(), &tl, H0, H0 + H, None);
    assert_eq!(recs[0].offline_s, 30 * 60);
    assert_eq!(recs[0].running_s, 30 * 60);
}

#[test]
fn meter_out_of_order_events_are_sorted() {
    let tl = [
        PhaseEvent::at(H0 + 30 * 60_000, BotPhase::Paused),
        PhaseEvent::at(H0, BotPhase::Running),
    ];
    let recs = meter_window(&subject(), &tl, H0, H0 + H, tl.first().copied());
    assert_eq!(recs[0].running_s, 30 * 60);
    assert_eq!(recs[0].user_paused_s, 30 * 60);
}

#[test]
fn meter_subsecond_flaps_dont_leak() {
    // Restart flap mid-second: truncated per-bucket so a 1500ms flap
    // can't emit a phantom second.
    let tl = [
        PhaseEvent::at(H0, BotPhase::Running),
        PhaseEvent::at(H0 + 1_500, BotPhase::Provisioning),
        PhaseEvent::at(H0 + 1_999, BotPhase::Running),
    ];
    let recs = meter_window(&subject(), &tl, H0, H0 + 60_000, tl.first().copied());
    assert_eq!(totals(&recs), [59, 0, 0, 0]);
}

#[tokio::test]
async fn meter_job_is_idempotent_and_advances() {
    let timelines = InMemoryTimelines::new();
    let usage = InMemoryUsage::new();
    let subjects = StaticSubjects {
        bots: vec![MeterSubject {
            bot_id: "b1".into(),
            tenant: "alice".into(),
            version: "stable".into(),
        }],
    };
    let dir = InMemoryPlans::seeded();
    alice_on(&dir, "friends");
    timelines
        .record("b1", PhaseEvent::at(H0, BotPhase::Running))
        .await
        .unwrap();

    let job = MeterJob {
        subjects: &subjects,
        timelines: &timelines,
        usage: &usage,
        plans: &dir,
        now_unix_ms: H0 + 2 * H + 1234,
    };
    let rep = job.run().await;
    assert_eq!(rep.records.len(), 2); // hours H0, H0+H fully metered
    assert!(rep.errors.is_empty());

    // Re-run: same rows upserted, not duplicated.
    let rep2 = job.run().await;
    let rows = usage.for_bot("b1", 0, i64::MAX).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rep2.records.iter().all(|r| r.plan_id == "friends"));

    // A restart inside the in-flight hour is metered next run: Running
    // for 100ms (truncates to 0s), then Failed for the rest of the hour.
    timelines
        .record("b1", PhaseEvent::at(H0 + 2 * H + 100, BotPhase::Failed))
        .await
        .unwrap();
    let job3 = MeterJob {
        now_unix_ms: H0 + 3 * H + 5,
        ..job
    };
    let rep3 = job3.run().await;
    let last = rep3
        .records
        .iter()
        .find(|r| r.hour_unix_ms == H0 + 2 * H)
        .unwrap();
    assert_eq!(last.running_s, 0);
    assert_eq!(last.offline_s, 3599);
}

struct StaticSubjects {
    bots: Vec<MeterSubject>,
}

#[async_trait]
impl MeterSubjects for StaticSubjects {
    async fn subjects(&self) -> Result<Vec<MeterSubject>, UsageError> {
        Ok(self.bots.clone())
    }
}

// -- quota ------------------------------------------------------------------

fn alice_on(dir: &InMemoryPlans, plan: &str) {
    dir.set_user(UserAccess {
        user_id: "alice".into(),
        plan_id: Some(plan.into()),
        status: UserStatus::Active,
    });
}

#[test]
fn quota_no_plan_and_suspended() {
    let dir = InMemoryPlans::seeded();
    let u = Consumption::default();
    let intent = CreateIntent {
        proxy_mode: fleet_types::crds::ProxyMode::Pool,
        channel: fleet_types::crds::VersionChannel::Stable,
    };
    assert!(matches!(
        check_create(&dir, "ghost", &u, &intent),
        Err(QuotaError::NoPlan { .. })
    ));

    alice_on(&dir, "friends");
    dir.set_user(UserAccess {
        user_id: "alice".into(),
        plan_id: Some("friends".into()),
        status: UserStatus::Suspended,
    });
    assert!(matches!(
        check_create(&dir, "alice", &u, &intent),
        Err(QuotaError::UserNotActive {
            status: UserStatus::Suspended,
            ..
        })
    ));
}

#[test]
fn quota_max_bots_and_running() {
    let dir = InMemoryPlans::seeded();
    alice_on(&dir, "friends"); // maxBots = maxRunningBots = 5
    let intent = CreateIntent {
        proxy_mode: fleet_types::crds::ProxyMode::Pool,
        channel: fleet_types::crds::VersionChannel::Stable,
    };
    let at_cap = Consumption {
        bots_total: 5,
        running_bots: 5,
    };
    assert!(matches!(
        check_create(&dir, "alice", &at_cap, &intent),
        Err(QuotaError::MaxBotsExceeded { limit: 5, .. })
    ));
    assert!(matches!(
        check_running(&dir, "alice", &at_cap),
        Err(QuotaError::MaxRunningBotsExceeded { limit: 5, .. })
    ));
    let under = Consumption {
        bots_total: 4,
        running_bots: 4,
    };
    check_create(&dir, "alice", &under, &intent).unwrap();
    check_running(&dir, "alice", &under).unwrap();
}

#[test]
fn quota_features_proxy_pool_channel() {
    let dir = InMemoryPlans::new();
    dir.upsert_plan(Plan {
        id: "bare".into(),
        name: "Bare".into(),
        active: true,
        max_bots: Some(1),
        max_running_bots: Some(1),
        proxy_modes: vec![fleet_types::crds::ProxyMode::Fixed],
        pool_access: false,
        channels: vec![fleet_types::crds::VersionChannel::Stable],
        rollout_priority: 0,
        handoff_priority: 0,
        retention_days_after_delete: 30,
        price_cents: None,
        interval: None,
    });
    alice_on(&dir, "bare");
    let u = Consumption::default();

    // Pool mode not even in proxyModes → proxy_mode_denied.
    let pool_intent = CreateIntent {
        proxy_mode: fleet_types::crds::ProxyMode::Pool,
        channel: fleet_types::crds::VersionChannel::Stable,
    };
    assert!(matches!(
        check_create(&dir, "alice", &u, &pool_intent),
        Err(QuotaError::ProxyModeDenied { .. })
    ));

    // Canary channel denied.
    let canary = CreateIntent {
        proxy_mode: fleet_types::crds::ProxyMode::Fixed,
        channel: fleet_types::crds::VersionChannel::Canary,
    };
    assert!(matches!(
        check_create(&dir, "alice", &u, &canary),
        Err(QuotaError::ChannelDenied { .. })
    ));

    // poolAccess: list pool mode but no access → pool_access_denied.
    dir.upsert_plan(Plan {
        id: "nopool".into(),
        name: "nopool".into(),
        active: true,
        max_bots: Some(1),
        max_running_bots: Some(1),
        proxy_modes: vec![
            fleet_types::crds::ProxyMode::Pool,
            fleet_types::crds::ProxyMode::Fixed,
        ],
        pool_access: false,
        channels: vec![fleet_types::crds::VersionChannel::Stable],
        rollout_priority: 0,
        handoff_priority: 0,
        retention_days_after_delete: 30,
        price_cents: None,
        interval: None,
    });
    alice_on(&dir, "nopool");
    assert!(matches!(
        check_create(&dir, "alice", &u, &pool_intent),
        Err(QuotaError::PoolAccessDenied { .. })
    ));
}

#[test]
fn effective_plan_subscription_overrides_user_row() {
    let dir = InMemoryPlans::seeded();
    alice_on(&dir, "friends");
    assert_eq!(dir.effective_plan("alice").unwrap().id, "friends");

    // Live subscription wins.
    dir.set_subscription(Subscription {
        user_id: "alice".into(),
        plan_id: "owner".into(),
        status: SubscriptionStatus::Active,
        provider: None,
        provider_customer_id: None,
        provider_subscription_id: None,
        current_period_end_unix_ms: None,
    });
    assert_eq!(dir.effective_plan("alice").unwrap().id, "owner");

    // Canceled falls back to the direct assignment.
    dir.set_subscription(Subscription {
        user_id: "alice".into(),
        plan_id: "owner".into(),
        status: SubscriptionStatus::Canceled,
        provider: None,
        provider_customer_id: None,
        provider_subscription_id: None,
        current_period_end_unix_ms: None,
    });
    assert_eq!(dir.effective_plan("alice").unwrap().id, "friends");
}

// -- retention --------------------------------------------------------------

fn deleted_bot(name: &str, deleted_at: i64, pods_gone: bool) -> XenomitBot {
    let spec = fleet_types::crds::XenomitBotSpec {
        owner: "alice".into(),
        desired_state: Default::default(),
        account: fleet_types::crds::AccountSpec {
            server: "int1.darkorbit.com".into(),
            auth_mode: Default::default(),
            credentials_vault_path: "secret/data/bots/x/account".into(),
        },
        config: Default::default(),
        config_revision: 0,
        version: Default::default(),
        proxy: Default::default(),
        restore_policy: Default::default(),
        resources: None,
    };
    XenomitBot {
        metadata: kube::core::ObjectMeta {
            name: Some(name.into()),
            ..Default::default()
        },
        spec,
        status: Some(fleet_types::crds::XenomitBotStatus {
            deleted_at_unix_ms: Some(deleted_at),
            delete_pods_gone: pods_gone,
            phase: BotPhase::Stopped,
            ..Default::default()
        }),
    }
}

#[test]
fn undelete_window_check() {
    let now = H0;
    let b = deleted_bot("u", now - 10 * 86_400_000, true);
    check_undelete(&b, 30, now).unwrap();
    let old = deleted_bot("u", now - 31 * 86_400_000, true);
    assert!(matches!(
        check_undelete(&old, 30, now),
        Err(UndeleteError::WindowExpired { .. })
    ));
    let live = {
        let mut b = deleted_bot("u", now, true);
        b.status.as_mut().unwrap().deleted_at_unix_ms = None;
        b
    };
    assert_eq!(
        check_undelete(&live, 30, now),
        Err(UndeleteError::NotDeleted)
    );
}

#[test]
fn snapshot_cutoff_respects_unexpired_bots() {
    // Expired bot deleted at t=10, unexpired at t=5: the only safe prefix
    // cutoff is <5, which can't reach the expired bot's rows → no sweep.
    assert_eq!(snapshot_cutoff(&[10], Some(5)), None);
    // Expired at t=4 *before* the earliest unexpired (t=10) → cutoff 9
    // purges its rows without touching the t=10 bot's.
    assert_eq!(snapshot_cutoff(&[4], Some(10)), Some(9));
    // No unexpired bots → sweep everything.
    assert_eq!(snapshot_cutoff(&[10], None), Some(i64::MAX));
    // No expired bots → no sweep at all.
    assert_eq!(snapshot_cutoff(&[], Some(5)), None);
}

struct FakeRetention {
    bots: Mutex<Vec<XenomitBot>>,
    purged_snapshots: Mutex<Vec<i64>>,
    usage_cutoff: Mutex<Option<i64>>,
    history: Mutex<Vec<String>>,
}

#[async_trait]
impl RetentionHooks for FakeRetention {
    async fn deleted_bots(&self) -> Result<Vec<XenomitBot>, RetentionError> {
        Ok(self.bots.lock().unwrap().clone())
    }
    async fn delete_bot(&self, bot_id: &str) -> Result<bool, RetentionError> {
        let mut v = self.bots.lock().unwrap();
        let n = v.len();
        v.retain(|b| b.metadata.name.as_deref() != Some(bot_id));
        Ok(v.len() != n)
    }
    async fn purge_bot_rows(&self, bot_id: &str) -> Result<u64, RetentionError> {
        self.history.lock().unwrap().push(bot_id.into());
        Ok(1)
    }
    async fn purge_snapshots(&self, before_unix_ms: i64) -> Result<u64, RetentionError> {
        self.purged_snapshots.lock().unwrap().push(before_unix_ms);
        Ok(3)
    }
    async fn purge_usage(&self, before_unix_ms: i64) -> Result<u64, RetentionError> {
        *self.usage_cutoff.lock().unwrap() = Some(before_unix_ms);
        Ok(7)
    }
}

#[tokio::test]
async fn retention_job_purges_expired_and_keeps_recent() {
    let now = H0;
    let day = 86_400_000;
    let hooks = FakeRetention {
        bots: Mutex::new(vec![
            deleted_bot("old", now - 40 * day, true), // expired, pods gone → purged
            deleted_bot("hanging", now - 40 * day, false), // expired, pods remain → retained
            deleted_bot("fresh", now - 5 * day, true), // in window → retained
        ]),
        purged_snapshots: Mutex::new(vec![]),
        usage_cutoff: Mutex::new(None),
        history: Mutex::new(vec![]),
    };
    let dir = InMemoryPlans::seeded();
    let clock = ManualClock::new(now);
    let job = RetentionJob {
        hooks: &hooks,
        gate: &PlanGate {
            plans: &dir,
            clock: &clock,
        },
        clock: &clock,
    };
    let rep = job.run().await.unwrap();

    assert_eq!(rep.bots_purged, ["old"]);
    assert!(rep.bots_retained.contains(&"hanging".to_string()));
    assert!(rep.bots_retained.contains(&"fresh".to_string()));
    // History rows purged for every expired bot, purge_due or not.
    assert_eq!(rep.history_rows, 2);
    // Snapshot cutoff stays below `fresh`'s deleted_at (now-5d).
    assert_eq!(hooks.purged_snapshots.lock().unwrap()[0], now - 5 * day - 1);
    assert_eq!(rep.snapshot_rows, 3);
    // Usage purge at the 13-month horizon.
    let expect = now - USAGE_RECORD_RETENTION_DAYS as i64 * day;
    assert_eq!(*hooks.usage_cutoff.lock().unwrap(), Some(expect));
    assert_eq!(rep.usage_rows, 7);
}

#[tokio::test]
async fn per_plan_retention_days_drive_the_gate() {
    let now = H0;
    let day = 86_400_000;
    let dir = InMemoryPlans::new();
    dir.upsert_plan(Plan {
        id: "long".into(),
        name: "Long".into(),
        active: true,
        max_bots: None,
        max_running_bots: None,
        proxy_modes: vec![],
        pool_access: false,
        channels: vec![],
        rollout_priority: 0,
        handoff_priority: 0,
        retention_days_after_delete: 90, // paid tier keeps history longer
        price_cents: None,
        interval: None,
    });
    dir.set_user(UserAccess {
        user_id: "alice".into(),
        plan_id: Some("long".into()),
        status: UserStatus::Active,
    });
    let hooks = FakeRetention {
        bots: Mutex::new(vec![deleted_bot("b", now - 40 * day, true)]),
        purged_snapshots: Mutex::new(vec![]),
        usage_cutoff: Mutex::new(None),
        history: Mutex::new(vec![]),
    };
    let clock = ManualClock::new(now);
    let rep = RetentionJob {
        hooks: &hooks,
        gate: &PlanGate {
            plans: &dir,
            clock: &clock,
        },
        clock: &clock,
    }
    .run()
    .await
    .unwrap();
    // 40 days < 90-day plan retention → nothing purged.
    assert!(rep.bots_purged.is_empty());
    assert_eq!(rep.bots_retained, ["b"]);
}

// -- showback ---------------------------------------------------------------

#[test]
fn showback_aggregates_per_tenant_and_availability() {
    let recs = vec![
        UsageRecord {
            bot_id: "b1".into(),
            tenant: "alice".into(),
            hour_unix_ms: H0,
            running_s: 3000,
            offline_s: 600,
            needs_human_s: 0,
            user_paused_s: 0,
            version: "stable".into(),
            plan_id: "friends".into(),
        },
        UsageRecord {
            bot_id: "b1".into(),
            tenant: "alice".into(),
            hour_unix_ms: H0 + H,
            running_s: 3600,
            ..Default::default()
        },
        UsageRecord {
            bot_id: "b2".into(),
            tenant: "bob".into(),
            hour_unix_ms: H0,
            user_paused_s: 3600,
            ..Default::default()
        },
        // Outside the window — ignored.
        UsageRecord {
            bot_id: "b9".into(),
            tenant: "alice".into(),
            hour_unix_ms: H0 - H,
            running_s: 9999,
            ..Default::default()
        },
    ];
    let rep = showback_report(&recs, H0, H0 + 2 * H, H0 + 2 * H);
    assert_eq!(rep.tenants.len(), 2);

    let a = rep.tenants.iter().find(|t| t.tenant == "alice").unwrap();
    assert_eq!(a.bots.len(), 1);
    assert_eq!(a.bots[0].running_hours(), (3000.0 + 3600.0) / 3600.0);
    assert_eq!(a.totals.running_s, 6600);
    // availability = running / (running + offline) — user/needs-human
    // buckets stay out of the SLO denominator.
    let avail = a.availability.unwrap();
    assert!((avail - 6600.0 / 7200.0).abs() < 1e-9);

    let b = rep.tenants.iter().find(|t| t.tenant == "bob").unwrap();
    assert_eq!(b.totals.user_paused_s, 3600);
    assert!(b.availability.is_none());
    assert_eq!(rep.totals.running_s, 6600);
}

// -- billing (interface only) ------------------------------------------------

#[test]
fn dunning_ladder() {
    let end = H0;
    let grace = 7 * 86_400_000;
    let now = H0 + 3 * 86_400_000;
    assert_eq!(
        dunning_action(SubscriptionStatus::Active, Some(end), now, grace),
        DunningAction::None
    );
    assert_eq!(
        dunning_action(SubscriptionStatus::PastDue, Some(end), now, grace),
        DunningAction::Warn
    );
    assert_eq!(
        dunning_action(
            SubscriptionStatus::PastDue,
            Some(end),
            end + grace + 1,
            grace
        ),
        DunningAction::PauseQuota
    );
    assert_eq!(
        dunning_action(SubscriptionStatus::Canceled, Some(end), now, grace),
        DunningAction::Stop
    );
}

/// The trait is object-safe and implementable — prove it with a stub so a
/// future Stripe impl knows the shape compiles (no provider here).
struct StubBilling;

#[async_trait]
impl BillingProvider for StubBilling {
    fn provider(&self) -> &'static str {
        "stub"
    }
    async fn create_customer(&self, _u: &UserAccess) -> Result<String, BillingError> {
        Ok("cus_stub".into())
    }
    async fn create_subscription(
        &self,
        _c: &str,
        _p: &Plan,
    ) -> Result<ProviderSubscription, BillingError> {
        Ok(ProviderSubscription {
            provider_subscription_id: "sub_stub".into(),
            current_period_end_unix_ms: Some(H0 + 30 * 86_400_000),
        })
    }
    async fn cancel_subscription(&self, _id: &str) -> Result<(), BillingError> {
        Ok(())
    }
    async fn report_usage(&self, _id: &str, _u: &[UsageRecord]) -> Result<(), BillingError> {
        Ok(())
    }
    fn parse_webhook(
        &self,
        _sig: Option<&str>,
        _body: &[u8],
    ) -> Result<BillingEvent, BillingError> {
        Ok(BillingEvent::Ignored {
            kind: "ping".into(),
        })
    }
}

#[tokio::test]
async fn billing_provider_trait_is_object_safe() {
    let bp: Box<dyn BillingProvider> = Box::new(StubBilling);
    assert_eq!(bp.provider(), "stub");
    let user = UserAccess {
        user_id: "alice".into(),
        plan_id: None,
        status: UserStatus::Active,
    };
    assert_eq!(bp.create_customer(&user).await.unwrap(), "cus_stub");
}

// -- invites ------------------------------------------------------------------

#[tokio::test]
async fn invite_lifecycle_single_use_and_expiry() {
    let dir = InMemoryPlans::seeded();
    let (inv, code) = Invite::issue("friends", "root", H0, Some(Duration::from_secs(3600)));
    assert_eq!(inv.status(H0), InviteStatus::Open);
    assert!(inv.matches(&code));
    dir.put_invite(inv);

    // Redeem → used; second redeem is refused.
    let inv = dir.redeem_invite(&code, "alice", H0 + 1).unwrap();
    assert_eq!(inv.used_by.as_deref(), Some("alice"));
    assert_eq!(
        dir.redeem_invite(&code, "bob", H0 + 2),
        Err(InviteError::Used)
    );
    // Unknown code and expiry both refuse cleanly.
    assert_eq!(
        dir.redeem_invite("nope", "bob", H0 + 2),
        Err(InviteError::UnknownCode)
    );
    let (exp_inv, exp_code) = Invite::issue("friends", "root", H0, Some(Duration::from_secs(10)));
    dir.put_invite(exp_inv);
    assert_eq!(
        dir.redeem_invite(&exp_code, "bob", H0 + 11_000),
        Err(InviteError::Expired)
    );
}
