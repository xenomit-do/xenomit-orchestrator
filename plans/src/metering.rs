//! Hourly metering (docs/12 §12.5.1): the orchestrator writes
//! `UsageRecord` rows from its own phase timeline.
//!
//! - [`PhaseEvent`] is one transition on a bot's timeline;
//!   [`PhaseTimelines`] is the store trait for that log (an in-memory impl
//!   is included; the operator feeds it on every phase transition).
//! - [`meter_window`] is the pure aggregator: timeline + window → hourly
//!   records. It is total — every elapsed millisecond lands in exactly one
//!   of the four disjoint buckets of [`Bucket`].
//! - [`UsageStore`] is the append-only record store (upsert on
//!   `(bot_id, hour)` so hourly recompute is idempotent);
//!   [`MeterJob`] sweeps all known bots once per hour.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use fleet_types::phase::{BotPhase, NeedsHumanKind};

use crate::directory::PlanDirectory;
use crate::model::UsageRecord;

/// One hour in milliseconds — the [`UsageRecord::hour_unix_ms`] bucket.
pub const HOUR_MS: i64 = 3_600_000;

/// One entry on a bot's phase timeline: `phase` became current at
/// `at_unix_ms`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PhaseEvent {
    pub at_unix_ms: i64,
    pub phase: BotPhase,
    /// Operator detail that refines bucketing — currently `quota` on a
    /// `Paused`/`Stopped` phase (docs/12 §12.3: a quota-parked bot is
    /// policy-paused, not user-caused).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<NeedsHumanKind>,
}

impl PhaseEvent {
    pub fn at(at_unix_ms: i64, phase: BotPhase) -> Self {
        PhaseEvent {
            at_unix_ms,
            phase,
            detail: None,
        }
    }

    pub fn with_detail(mut self, detail: NeedsHumanKind) -> Self {
        self.detail = Some(detail);
        self
    }
}

/// The four disjoint second buckets of a [`UsageRecord`]. `Offline` is
/// deliberately *not* "not running": it is orchestrator/infrastructure
/// downtime only — the SLO input of docs/12 §12.5.4 — because `NeedsHuman`
/// and `UserPaused` get their own buckets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bucket {
    /// Holds a live game session (`Running`, plus `Draining`/
    /// `Snapshotting` — a draining bot is still in game earning).
    Running = 0,
    /// Provision/start/login gaps, crashes, quarantine, version waits —
    /// downtime nobody asked for.
    Offline = 1,
    /// `NeedsHuman`: captcha / credentials / session — waiting on a human.
    NeedsHuman = 2,
    /// `Paused`/`Stopped` requested by the user (desired-state drains land
    /// in `Running` until the bot logs out; the settled phases land here).
    UserPaused = 3,
}

/// Phase → bucket (docs/12 §12.5.1 "running/offline/needs-human seconds",
/// plus the §12.5.4 user-caused carve-out).
pub fn bucket_of(phase: BotPhase, detail: Option<NeedsHumanKind>) -> Bucket {
    match phase {
        BotPhase::NeedsHuman => Bucket::NeedsHuman,
        BotPhase::Running | BotPhase::Draining | BotPhase::Snapshotting => Bucket::Running,
        // Quota-parked bots are policy-paused (§12.3): neither user-caused
        // nor infrastructure downtime — count them as offline.
        BotPhase::Paused | BotPhase::Stopped if detail != Some(NeedsHumanKind::Quota) => {
            Bucket::UserPaused
        }
        _ => Bucket::Offline,
    }
}

/// Identity fields stamped onto each record the aggregator emits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageSubject {
    pub bot_id: String,
    pub tenant: String,
    /// Bot image/channel at metering time.
    pub version: String,
    /// Tenant's effective plan at metering time.
    pub plan_id: String,
}

/// Aggregate a phase timeline into hourly [`UsageRecord`]s over
/// `[from_unix_ms, to_unix_ms)`.
///
/// `timeline` carries the transitions (any order; only those inside the
/// window count). `initial` is the phase in effect *at* `from_unix_ms` —
/// typically the latest event at-or-before the window start. `None` means
/// "unknown" and never inflates running time: it meters as `Offline`.
///
/// Sub-second remainders are truncated per (hour, bucket) after summing,
/// so a restart flipping phases mid-second can't leak a phantom second.
pub fn meter_window(
    subject: &UsageSubject,
    timeline: &[PhaseEvent],
    from_unix_ms: i64,
    to_unix_ms: i64,
    initial: Option<PhaseEvent>,
) -> Vec<UsageRecord> {
    if to_unix_ms <= from_unix_ms {
        return Vec::new();
    }
    let mut events: Vec<PhaseEvent> = timeline.to_vec();
    events.sort_by_key(|e| e.at_unix_ms); // stable: equal stamps keep input order

    // ms per (hour, bucket), converted to seconds once at emit.
    let mut acc: BTreeMap<i64, [u64; 4]> = BTreeMap::new();
    let mut add = |mut s: i64, e: i64, b: Bucket| {
        while s < e {
            let cell = s - s.rem_euclid(HOUR_MS);
            let take = e.min(cell + HOUR_MS) - s;
            acc.entry(cell).or_default()[b as usize] += take as u64;
            s += take;
        }
    };

    let bucket = |cur: Option<PhaseEvent>| {
        cur.map(|c| bucket_of(c.phase, c.detail))
            .unwrap_or(Bucket::Offline) // unknown history never inflates running
    };
    let mut t = from_unix_ms;
    let mut cur = initial;
    for ev in events {
        if ev.at_unix_ms <= t {
            // Transition at/before the cursor supersedes the current phase
            // (window-start events replace `initial`; same-stamp events
            // resolve last-one-wins).
            cur = Some(ev);
            continue;
        }
        if ev.at_unix_ms >= to_unix_ms {
            break;
        }
        add(t, ev.at_unix_ms, bucket(cur));
        t = ev.at_unix_ms;
        cur = Some(ev);
    }
    add(t, to_unix_ms, bucket(cur));

    acc.into_iter()
        .map(|(hour, ms)| UsageRecord {
            bot_id: subject.bot_id.clone(),
            tenant: subject.tenant.clone(),
            hour_unix_ms: hour,
            running_s: ms[Bucket::Running as usize] / 1000,
            offline_s: ms[Bucket::Offline as usize] / 1000,
            needs_human_s: ms[Bucket::NeedsHuman as usize] / 1000,
            user_paused_s: ms[Bucket::UserPaused as usize] / 1000,
            version: subject.version.clone(),
            plan_id: subject.plan_id.clone(),
        })
        .collect()
}

/// What the meter stamps on a record besides the timeline (per-bot tenant
/// + running version). In production this is the bot repo (`spec.owner`,
///   `status.image`); tests plug in a map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeterSubject {
    pub bot_id: String,
    pub tenant: String,
    pub version: String,
}

/// Read seam for the subject fields of [`MeterJob`].
#[async_trait]
pub trait MeterSubjects: Send + Sync {
    async fn subjects(&self) -> Result<Vec<MeterSubject>, UsageError>;
}

/// The orchestrator's recorded phase timeline (docs/12 §12.5.1). One
/// transition per observed `status.phase` change — appended by the
/// operator (or the API's gateway-event bridge), read back by the meter.
#[async_trait]
pub trait PhaseTimelines: Send + Sync {
    /// Record a transition. Out-of-order writes are stored sorted by
    /// `at_unix_ms` (stable for equal stamps).
    async fn record(&self, bot_id: &str, ev: PhaseEvent) -> Result<(), UsageError>;

    /// Transitions with `from_unix_ms <= at < to_unix_ms`, sorted.
    async fn events(
        &self,
        bot_id: &str,
        from_unix_ms: i64,
        to_unix_ms: i64,
    ) -> Result<Vec<PhaseEvent>, UsageError>;

    /// The latest transition at-or-before `at` — the phase in effect then
    /// (`meter_window`'s `initial`). Default walks `events(i64::MIN, at+1)`.
    async fn phase_at(
        &self,
        bot_id: &str,
        at_unix_ms: i64,
    ) -> Result<Option<PhaseEvent>, UsageError> {
        Ok(self
            .events(bot_id, i64::MIN, at_unix_ms.saturating_add(1))
            .await?
            .into_iter()
            .last())
    }

    /// Bots with any recorded transition (the meter's sweep set).
    async fn bots(&self) -> Result<Vec<String>, UsageError>;
}

/// Append-only usage store (docs/12 §12.2). `put` upserts on
/// `(bot_id, hour)` so re-metering the cursor hour is idempotent.
#[async_trait]
pub trait UsageStore: Send + Sync {
    async fn put(&self, records: &[UsageRecord]) -> Result<(), UsageError>;
    /// Highest hour bucket stored for a bot — the metering cursor.
    async fn latest_hour(&self, bot_id: &str) -> Result<Option<i64>, UsageError>;
    async fn for_bot(
        &self,
        bot_id: &str,
        from_unix_ms: i64,
        to_unix_ms: i64,
    ) -> Result<Vec<UsageRecord>, UsageError>;
    async fn for_tenant(
        &self,
        tenant: &str,
        from_unix_ms: i64,
        to_unix_ms: i64,
    ) -> Result<Vec<UsageRecord>, UsageError>;
    /// Every record in `[from, to)` — the showback query (docs/12 §12.5.1).
    async fn all(&self, from_unix_ms: i64, to_unix_ms: i64)
    -> Result<Vec<UsageRecord>, UsageError>;
    /// Drop rows with `hour < before_unix_ms` (13-month retention,
    /// docs/12 §12.4). Wired into [`crate::retention::RetentionJob`].
    async fn purge(&self, before_unix_ms: i64) -> Result<u64, UsageError>;
}

#[derive(Debug, thiserror::Error)]
pub enum UsageError {
    #[error("usage store: {0}")]
    Backend(String),
}

/// In-memory `UsageStore` (local backend + tests).
#[derive(Default)]
pub struct InMemoryUsage {
    rows: Mutex<BTreeMap<(String, i64), UsageRecord>>,
}

impl InMemoryUsage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl UsageStore for InMemoryUsage {
    async fn put(&self, records: &[UsageRecord]) -> Result<(), UsageError> {
        let mut rows = self.rows.lock().unwrap();
        for r in records {
            rows.insert((r.bot_id.clone(), r.hour_unix_ms), r.clone());
        }
        Ok(())
    }

    async fn latest_hour(&self, bot_id: &str) -> Result<Option<i64>, UsageError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .range((bot_id.to_string(), i64::MIN)..=(bot_id.to_string(), i64::MAX))
            .last()
            .map(|((_, h), _)| *h))
    }

    async fn for_bot(
        &self,
        bot_id: &str,
        from_unix_ms: i64,
        to_unix_ms: i64,
    ) -> Result<Vec<UsageRecord>, UsageError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .range((bot_id.to_string(), from_unix_ms)..(bot_id.to_string(), to_unix_ms))
            .map(|(_, r)| r.clone())
            .collect())
    }

    async fn for_tenant(
        &self,
        tenant: &str,
        from_unix_ms: i64,
        to_unix_ms: i64,
    ) -> Result<Vec<UsageRecord>, UsageError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| {
                r.tenant == tenant && r.hour_unix_ms >= from_unix_ms && r.hour_unix_ms < to_unix_ms
            })
            .cloned()
            .collect())
    }

    async fn all(
        &self,
        from_unix_ms: i64,
        to_unix_ms: i64,
    ) -> Result<Vec<UsageRecord>, UsageError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.hour_unix_ms >= from_unix_ms && r.hour_unix_ms < to_unix_ms)
            .cloned()
            .collect())
    }

    async fn purge(&self, before_unix_ms: i64) -> Result<u64, UsageError> {
        let mut rows = self.rows.lock().unwrap();
        let before = rows.len();
        rows.retain(|(_, hour), _| *hour >= before_unix_ms);
        Ok((before - rows.len()) as u64)
    }
}

/// In-memory `PhaseTimelines` (local backend + tests).
#[derive(Default)]
pub struct InMemoryTimelines {
    events: Mutex<HashMap<String, Vec<PhaseEvent>>>,
}

impl InMemoryTimelines {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PhaseTimelines for InMemoryTimelines {
    async fn record(&self, bot_id: &str, ev: PhaseEvent) -> Result<(), UsageError> {
        let mut events = self.events.lock().unwrap();
        let v = events.entry(bot_id.to_string()).or_default();
        // Keep sorted; equal timestamps keep insertion order (a later
        // same-stamp event supersedes, matching `meter_window`).
        let pos = v.partition_point(|e| e.at_unix_ms <= ev.at_unix_ms);
        v.insert(pos, ev);
        Ok(())
    }

    async fn events(
        &self,
        bot_id: &str,
        from_unix_ms: i64,
        to_unix_ms: i64,
    ) -> Result<Vec<PhaseEvent>, UsageError> {
        Ok(self
            .events
            .lock()
            .unwrap()
            .get(bot_id)
            .map(|v| {
                v.iter()
                    .copied()
                    .filter(|e| e.at_unix_ms >= from_unix_ms && e.at_unix_ms < to_unix_ms)
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn phase_at(
        &self,
        bot_id: &str,
        at_unix_ms: i64,
    ) -> Result<Option<PhaseEvent>, UsageError> {
        Ok(self
            .events
            .lock()
            .unwrap()
            .get(bot_id)
            .and_then(|v| v.iter().copied().rfind(|e| e.at_unix_ms <= at_unix_ms)))
    }

    async fn bots(&self) -> Result<Vec<String>, UsageError> {
        let mut v: Vec<_> = self.events.lock().unwrap().keys().cloned().collect();
        v.sort();
        Ok(v)
    }
}

/// The hourly sweep (docs/12 §12.5.1). One pass meters every known bot up
/// to the start of the *current* hour — the in-flight hour is recomputed
/// next run, and the `(bot_id, hour)` upsert makes that idempotent.
/// Per-bot failures are collected in the report, never fatal.
pub struct MeterJob<'a> {
    pub subjects: &'a dyn MeterSubjects,
    pub timelines: &'a dyn PhaseTimelines,
    pub usage: &'a dyn UsageStore,
    pub plans: &'a dyn PlanDirectory,
    pub now_unix_ms: i64,
}

#[derive(Debug, Default)]
pub struct MeterReport {
    /// One entry per (bot, hour) row written this run.
    pub records: Vec<UsageRecord>,
    /// Bots skipped because no timeline data exists yet.
    pub bots_skipped: Vec<String>,
    /// `(bot_id, error)` for per-bot failures.
    pub errors: Vec<(String, String)>,
}

impl MeterJob<'_> {
    pub async fn run(&self) -> MeterReport {
        let mut report = MeterReport::default();
        let end = self.now_unix_ms - self.now_unix_ms.rem_euclid(HOUR_MS);
        let subjects = match self.subjects.subjects().await {
            Ok(s) => s,
            Err(e) => {
                report.errors.push(("*".into(), e.to_string()));
                return report;
            }
        };
        for subj in subjects {
            if let Err(e) = self.meter_one(&subj, end, &mut report).await {
                report.errors.push((subj.bot_id.clone(), e.to_string()));
            }
        }
        report
    }

    async fn meter_one(
        &self,
        subj: &MeterSubject,
        end: i64,
        report: &mut MeterReport,
    ) -> Result<(), UsageError> {
        // Cursor: recompute the last written hour (it may have been
        // partial); first run starts at the bot's earliest event.
        let start = match self.usage.latest_hour(&subj.bot_id).await? {
            Some(h) => h,
            None => {
                let first = self
                    .timelines
                    .events(&subj.bot_id, i64::MIN, end)
                    .await?
                    .first()
                    .map(|e| e.at_unix_ms - e.at_unix_ms.rem_euclid(HOUR_MS));
                match first {
                    Some(h) => h,
                    None => {
                        report.bots_skipped.push(subj.bot_id.clone());
                        return Ok(());
                    }
                }
            }
        };
        if start >= end {
            return Ok(()); // cursor hour is the in-flight one — nothing to do
        }
        let timeline = self.timelines.events(&subj.bot_id, start, end).await?;
        let initial = self.timelines.phase_at(&subj.bot_id, start).await?;
        let plan_id = self
            .plans
            .effective_plan(&subj.tenant)
            .map(|p| p.id)
            .unwrap_or_default();
        let subject = UsageSubject {
            bot_id: subj.bot_id.clone(),
            tenant: subj.tenant.clone(),
            version: subj.version.clone(),
            plan_id,
        };
        let records = meter_window(&subject, &timeline, start, end, initial);
        self.usage.put(&records).await?;
        report.records.extend(records);
        Ok(())
    }
}
