//! The orchestrator's read-only copy of the panel's plan data
//! (docs/12 §12.2): plans, user access rows, subscriptions and invites.
//!
//! The trait is **synchronous on purpose**: quota checks run on the request
//! hot path (bot create) and on the reconcile hot path (`desiredState =
//! Running`), so the copy is materialized in memory and refreshed from
//! Postgres by the deploy — a Postgres-backed implementation fetches into
//! this same surface ahead of reads rather than querying per call.
//!
//! The operator is a future consumer: on `desiredState = Running` it calls
//! [`crate::quota::check_running`] against a `PlanDirectory` and parks
//! excess bots at `Paused(quota)` (docs/12 §12.3).

use std::collections::HashMap;
use std::sync::Mutex;

use crate::model::{Invite, Plan, Subscription, UserAccess};

/// Read-only plan/subscription directory (docs/12 §12.2 "the orchestrator
/// enforces a read-only copy").
pub trait PlanDirectory: Send + Sync {
    fn plan(&self, id: &str) -> Option<Plan>;
    fn plans(&self) -> Vec<Plan>;
    fn user(&self, user_id: &str) -> Option<UserAccess>;
    fn subscription(&self, user_id: &str) -> Option<Subscription>;
    /// Invite row by code hash (`Invite::hash_code`); lookup by the
    /// *hash*, never the code itself.
    fn invite(&self, code_hash: &str) -> Option<Invite>;

    /// Effective plan for a user: a live subscription's plan wins over the
    /// user's direct `planId` (docs/12 §12.5.2 — the webhook updates
    /// `Subscription.status`, which maps to `Plan` and therefore quotas).
    /// `canceled` (or absent) falls back to the direct assignment.
    fn effective_plan(&self, user_id: &str) -> Option<Plan> {
        if let Some(sub) = self.subscription(user_id)
            && sub.status.keeps_access()
            && let Some(plan) = self.plan(&sub.plan_id)
        {
            return Some(plan);
        }
        self.user(user_id)
            .and_then(|u| u.plan_id.as_deref().map(str::to_string))
            .and_then(|id| self.plan(&id))
    }

    /// Post-delete retention for a user's history (docs/12 §12.4):
    /// the effective plan's `retentionDaysAfterDelete`, defaulting to 30 d.
    fn retention_days(&self, user_id: &str) -> u32 {
        self.effective_plan(user_id)
            .map(|p| p.retention_days_after_delete)
            .unwrap_or(operator::policy::DEFAULT_RETENTION_DAYS)
    }
}

/// In-memory directory: the `local` backend's copy and the test fixture.
/// Mutations are the *refresh* API — a deploy keeps this in sync with the
/// panel DB (or seeds it directly for dev).
#[derive(Default)]
pub struct InMemoryPlans {
    plans: Mutex<HashMap<String, Plan>>,
    users: Mutex<HashMap<String, UserAccess>>,
    subscriptions: Mutex<HashMap<String, Subscription>>,
    invites: Mutex<HashMap<String, Invite>>,
}

impl InMemoryPlans {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed plans from docs/12 §12.2: `owner` (unlimited) and `friends`
    /// (5 bots, pool + fixed, all channels). Mirrors `xenomit-platform/db/plans/0006_seed_plans.sql`.
    pub fn seeded() -> Self {
        let dir = Self::new();
        dir.upsert_plan(Plan {
            id: "owner".into(),
            name: "Owner".into(),
            active: true,
            max_bots: None,
            max_running_bots: None,
            proxy_modes: vec![
                fleet_types::crds::ProxyMode::Pool,
                fleet_types::crds::ProxyMode::Fixed,
            ],
            pool_access: true,
            channels: vec![
                fleet_types::crds::VersionChannel::Stable,
                fleet_types::crds::VersionChannel::Canary,
                fleet_types::crds::VersionChannel::Pinned,
            ],
            rollout_priority: 100,
            handoff_priority: 100,
            retention_days_after_delete: operator::policy::DEFAULT_RETENTION_DAYS,
            price_cents: None,
            interval: None,
        });
        dir.upsert_plan(Plan {
            id: "friends".into(),
            name: "Friends".into(),
            active: true,
            max_bots: Some(5),
            max_running_bots: Some(5),
            proxy_modes: vec![
                fleet_types::crds::ProxyMode::Pool,
                fleet_types::crds::ProxyMode::Fixed,
            ],
            pool_access: true,
            channels: vec![
                fleet_types::crds::VersionChannel::Stable,
                fleet_types::crds::VersionChannel::Canary,
                fleet_types::crds::VersionChannel::Pinned,
            ],
            rollout_priority: 0,
            handoff_priority: 0,
            retention_days_after_delete: operator::policy::DEFAULT_RETENTION_DAYS,
            price_cents: None,
            interval: None,
        });
        dir
    }

    pub fn upsert_plan(&self, plan: Plan) {
        self.plans.lock().unwrap().insert(plan.id.clone(), plan);
    }

    pub fn remove_plan(&self, id: &str) -> Option<Plan> {
        self.plans.lock().unwrap().remove(id)
    }

    pub fn set_user(&self, user: UserAccess) {
        self.users
            .lock()
            .unwrap()
            .insert(user.user_id.clone(), user);
    }

    pub fn set_subscription(&self, sub: Subscription) {
        self.subscriptions
            .lock()
            .unwrap()
            .insert(sub.user_id.clone(), sub);
    }

    pub fn put_invite(&self, invite: Invite) {
        self.invites
            .lock()
            .unwrap()
            .insert(invite.code_hash.clone(), invite);
    }

    /// Redeem an invite for `user_id` at `now`: validates single-use +
    /// expiry, stamps `used_by`. The panel performs the equivalent inside
    /// its login transaction.
    pub fn redeem_invite(
        &self,
        code: &str,
        user_id: &str,
        now_unix_ms: i64,
    ) -> Result<Invite, crate::model::InviteError> {
        let hash = Invite::hash_code(code);
        let mut invites = self.invites.lock().unwrap();
        let Some(inv) = invites.get_mut(&hash) else {
            return Err(crate::model::InviteError::UnknownCode);
        };
        inv.check_redeemable(now_unix_ms)?;
        inv.used_by = Some(user_id.into());
        Ok(inv.clone())
    }
}

impl PlanDirectory for InMemoryPlans {
    fn plan(&self, id: &str) -> Option<Plan> {
        self.plans.lock().unwrap().get(id).cloned()
    }

    fn plans(&self) -> Vec<Plan> {
        let mut v: Vec<_> = self.plans.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    fn user(&self, user_id: &str) -> Option<UserAccess> {
        self.users.lock().unwrap().get(user_id).cloned()
    }

    fn subscription(&self, user_id: &str) -> Option<Subscription> {
        self.subscriptions.lock().unwrap().get(user_id).cloned()
    }

    fn invite(&self, code_hash: &str) -> Option<Invite> {
        self.invites.lock().unwrap().get(code_hash).cloned()
    }
}

/// Empty directory: every lookup `None`, retention default. Used where a
/// `PlanDirectory` is required but no plan data is configured (dev mode).
pub struct NoPlans;

impl PlanDirectory for NoPlans {
    fn plan(&self, _id: &str) -> Option<Plan> {
        None
    }
    fn plans(&self) -> Vec<Plan> {
        Vec::new()
    }
    fn user(&self, _user_id: &str) -> Option<UserAccess> {
        None
    }
    fn subscription(&self, _user_id: &str) -> Option<Subscription> {
        None
    }
    fn invite(&self, _code_hash: &str) -> Option<Invite> {
        None
    }
}
