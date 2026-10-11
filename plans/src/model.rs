//! Data model of docs/12 §12.2. These rows live in Postgres (panel DB);
//! the orchestrator enforces a *read-only copy* via [`crate::directory`].
//! They are deliberately **not** CRDs — ADR-05 keeps desired state in etcd
//! and bulky/account data in Postgres, and none of these types is
//! reconciled by the operator.
//!
//! Field names serialize camelCase like every other type in the workspace;
//! the SQL columns in `xenomit-platform/db/plans/` are the snake_case equivalents.

use fleet_types::crds::{ProxyMode, VersionChannel};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

fn default_true() -> bool {
    true
}
fn default_retention_days() -> u32 {
    operator::policy::DEFAULT_RETENTION_DAYS
}

/// `Plan` (docs/12 §12.2). `None` on a cap means "unlimited" (`owner`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub id: String,
    pub name: String,
    /// Assignable to new subscriptions. Existing holders keep their quota —
    /// `active = false` retires the plan, it does not punish its users.
    #[serde(default = "default_true")]
    pub active: bool,
    /// Max live (non-soft-deleted) bots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bots: Option<u32>,
    /// Max bots with `desiredState = running` (docs/12 §12.3 operator row).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_running_bots: Option<u32>,
    /// Allowed proxy modes (`pool` | `fixed` | both).
    #[serde(default)]
    pub proxy_modes: Vec<ProxyMode>,
    /// May draw from the shared proxy pool (docs/12 §12.5.5 — pool proxies
    /// cost money, so this is plan-gated).
    #[serde(default)]
    pub pool_access: bool,
    /// Allowed release channels (`stable` | `canary` | `pinned`).
    #[serde(default)]
    pub channels: Vec<VersionChannel>,
    /// Order in rollout waves; paying users first or last is a policy knob
    /// (docs/12 §12.2). Higher runs earlier.
    #[serde(default)]
    pub rollout_priority: i32,
    /// Login rate-limit bucket weight for handoffs (docs/12 §12.3).
    #[serde(default)]
    pub handoff_priority: i32,
    /// Per-plan override of the 30-day post-delete retention (docs/12 §12.4).
    #[serde(default = "default_retention_days")]
    pub retention_days_after_delete: u32,
    /// Price; `None` until billing exists (docs/12 §12.2, ADR-24).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_cents: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<BillingInterval>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BillingInterval {
    Month,
    Year,
}

/// `User` access row — the fields of the panel `User` model the
/// orchestrator needs (docs/12 §12.1–2). Panel-owned; read-only here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserAccess {
    pub user_id: String,
    /// Direct plan assignment; a live subscription wins over it
    /// ([`crate::directory::PlanDirectory::effective_plan`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    #[serde(default)]
    pub status: UserStatus,
}

/// `User.status`: `invited → active ↔ suspended` (docs/12 §12.1).
/// Suspending drains the user's bots to a clean snapshot and stops them;
/// nothing is deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    Invited,
    #[default]
    Active,
    Suspended,
}

impl UserStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            UserStatus::Invited => "invited",
            UserStatus::Active => "active",
            UserStatus::Suspended => "suspended",
        }
    }
}

impl std::fmt::Display for UserStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `Subscription` (docs/12 §12.2). Provider fields stay `None` while
/// billing is off; the webhook flow of §12.5.2 fills them later.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    pub user_id: String,
    pub plan_id: String,
    pub status: SubscriptionStatus,
    /// Billing provider name (`"stripe"`, …); `None` while billing is off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_customer_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_subscription_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_period_end_unix_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStatus {
    Trialing,
    #[default]
    Active,
    PastDue,
    Canceled,
}

impl SubscriptionStatus {
    /// Does this status still entitle the user to the subscription's plan?
    /// `past_due` keeps access while dunning runs (docs/12 §12.5.3);
    /// `canceled` falls back to the user's direct `planId`.
    pub fn keeps_access(self) -> bool {
        matches!(
            self,
            SubscriptionStatus::Trialing | SubscriptionStatus::Active | SubscriptionStatus::PastDue
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SubscriptionStatus::Trialing => "trialing",
            SubscriptionStatus::Active => "active",
            SubscriptionStatus::PastDue => "past_due",
            SubscriptionStatus::Canceled => "canceled",
        }
    }
}

impl std::fmt::Display for SubscriptionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `Invite` (docs/12 §12.1, docs/08 §8.3a): single-use code or a
/// pre-approved Discord id. Only the sha256 of the code is stored — the
/// code itself is shown once at creation and never persisted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Invite {
    /// Lowercase hex sha256 of the invite code.
    pub code_hash: String,
    /// Admin user id that created the invite.
    pub created_by: String,
    /// Plan granted on redemption.
    pub plan_id: String,
    /// User that redeemed it (`usedBy`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
    pub created_at_unix_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InviteStatus {
    /// Usable.
    Open,
    Used,
    Expired,
}

impl Invite {
    /// sha256 hex of an invite code — what gets stored and looked up.
    pub fn hash_code(code: &str) -> String {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(code.as_bytes()))
    }

    /// Issue a single-use invite. Returns `(row, code)`; `code` is the
    /// only copy of the secret and is meant to be shown once.
    pub fn issue(
        plan_id: impl Into<String>,
        created_by: impl Into<String>,
        created_at_unix_ms: i64,
        ttl: Option<Duration>,
    ) -> (Invite, String) {
        let code = hex::encode(rand::random::<[u8; 16]>());
        let invite = Invite {
            code_hash: Self::hash_code(&code),
            created_by: created_by.into(),
            plan_id: plan_id.into(),
            used_by: None,
            expires_at_unix_ms: ttl.map(|d| created_at_unix_ms + d.as_millis() as i64),
            created_at_unix_ms,
        };
        (invite, code)
    }

    /// Does `code` open this invite?
    pub fn matches(&self, code: &str) -> bool {
        self.code_hash == Self::hash_code(code)
    }

    pub fn status(&self, now_unix_ms: i64) -> InviteStatus {
        if self.used_by.is_some() {
            InviteStatus::Used
        } else if self.expires_at_unix_ms.is_some_and(|e| now_unix_ms >= e) {
            InviteStatus::Expired
        } else {
            InviteStatus::Open
        }
    }

    /// Can this invite be redeemed at `now`? Pure check; the caller
    /// (panel) performs the `usedBy` write + `User` row creation
    /// transactionally.
    pub fn check_redeemable(&self, now_unix_ms: i64) -> Result<(), InviteError> {
        match self.status(now_unix_ms) {
            InviteStatus::Open => Ok(()),
            InviteStatus::Used => Err(InviteError::Used),
            InviteStatus::Expired => Err(InviteError::Expired),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InviteError {
    #[error("invite code already used")]
    Used,
    #[error("invite code expired")]
    Expired,
    #[error("unknown invite code")]
    UnknownCode,
}

/// `UsageRecord` (docs/12 §12.2): one append-only row per bot per hour,
/// written by the orchestrator's meter ([`crate::metering`]).
///
/// The seconds buckets are disjoint and sum to the elapsed wall time:
/// `running_s` holds a live game session; `offline_s` is orchestrator- or
/// infrastructure-caused downtime (the SLO input of §12.5.4);
/// `needs_human_s` waits on the user; `user_paused_s` is the user-caused
/// carve-out §12.5.4 subtracts from the SLO denominator.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsageRecord {
    pub bot_id: String,
    pub tenant: String,
    /// Hour bucket start, unix ms (aligned to [`crate::metering::HOUR_MS`]).
    pub hour_unix_ms: i64,
    #[serde(default)]
    pub running_s: u64,
    #[serde(default)]
    pub offline_s: u64,
    #[serde(default)]
    pub needs_human_s: u64,
    /// User-caused pause/stop seconds (`Paused`/`Stopped`, excluding
    /// quota-parked). Additive to the doc's field list so the SLO credit
    /// formula of docs/12 §12.5.4 is computable.
    #[serde(default)]
    pub user_paused_s: u64,
    /// Bot image/channel at metering time.
    #[serde(default)]
    pub version: String,
    /// Tenant's effective plan at metering time.
    #[serde(default)]
    pub plan_id: String,
}

impl UsageRecord {
    pub fn total_s(&self) -> u64 {
        self.running_s + self.offline_s + self.needs_human_s + self.user_paused_s
    }
}
