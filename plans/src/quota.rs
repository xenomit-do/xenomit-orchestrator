//! Quota engine (docs/12 §12.3). Pure checks over a [`PlanDirectory`] plus
//! a usage snapshot — no I/O, so the same code serves every enforcement
//! point in the table:
//!
//! | Where | Calls |
//! |---|---|
//! | Orchestrator API `POST /bots` (authoritative) | [`check_create`] |
//! | Orchestrator API `PATCH /bots` (proxy/version edits) | [`check_spec_change`] |
//! | Operator on `desiredState = Running` | [`check_running`] — excess bots stay `Paused(quota)` |
//! | Proxy assignment | [`Plan::pool_access`] via [`check_create`]/[`check_spec_change`] |
//!
//! Errors are typed ([`QuotaError`]) so the API maps them to a stable
//! `code` and the operator can react without parsing strings.
//! Quota loss (downgrade, past-due) never deletes: bots are drained and
//! parked `Paused(quota)` — a controller decision, not part of this API.

use fleet_types::crds::{ProxyMode, VersionChannel, XenomitBotSpec};

use crate::directory::PlanDirectory;
use crate::model::{Plan, UserStatus};

/// Live consumption the checks are evaluated against. Counting rules:
/// `bots_total` excludes soft-deleted rows (a deleted bot's slot frees
/// immediately; its history is still inside the retention window), and
/// `running_bots` counts `desiredState = running` — the intent the operator
/// enforces, not the transient phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Consumption {
    pub bots_total: usize,
    pub running_bots: usize,
}

/// The spec features a create or patch wants to use. Built from
/// `XenomitBotSpec` (or the merge of an existing spec and a patch).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreateIntent {
    pub proxy_mode: ProxyMode,
    pub channel: VersionChannel,
}

impl CreateIntent {
    pub fn from_spec(spec: &XenomitBotSpec) -> Self {
        CreateIntent {
            proxy_mode: spec.proxy.mode,
            channel: spec.version.channel,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum QuotaError {
    /// Invite-only fleet (docs/12 §12.1): a user with no resolvable plan
    /// gets nothing.
    #[error("user {user} has no plan")]
    NoPlan { user: String },
    /// Only `active` users create bots; `invited` users must redeem first,
    /// `suspended` users are drained-and-stopped (docs/12 §12.1).
    #[error("user {user} is {status}")]
    UserNotActive { user: String, status: UserStatus },
    /// The plan exists but is retired (`active = false`) — relevant when
    /// *assigning* a plan, not when an existing holder uses it.
    #[error("plan {plan} is not active")]
    PlanInactive { plan: String },
    #[error("bot limit reached ({current}/{limit})")]
    MaxBotsExceeded { limit: u32, current: usize },
    #[error("running-bot limit reached ({current}/{limit})")]
    MaxRunningBotsExceeded { limit: u32, current: usize },
    #[error("proxy mode {} not allowed by plan {plan}", mode_str(*.mode))]
    ProxyModeDenied { plan: String, mode: ProxyMode },
    /// `mode = pool` requested on a plan without `poolAccess`
    /// (docs/12 §12.5.5 — pool proxies cost money).
    #[error("plan {plan} has no shared-pool access")]
    PoolAccessDenied { plan: String },
    #[error("channel {} not allowed by plan {plan}", channel_str(*.channel))]
    ChannelDenied {
        plan: String,
        channel: VersionChannel,
    },
}

impl QuotaError {
    /// Stable machine-readable kind for the API's typed error body.
    pub fn kind(&self) -> &'static str {
        match self {
            QuotaError::NoPlan { .. } => "no_plan",
            QuotaError::UserNotActive { .. } => "user_not_active",
            QuotaError::PlanInactive { .. } => "plan_inactive",
            QuotaError::MaxBotsExceeded { .. } => "max_bots_exceeded",
            QuotaError::MaxRunningBotsExceeded { .. } => "max_running_bots_exceeded",
            QuotaError::ProxyModeDenied { .. } => "proxy_mode_denied",
            QuotaError::PoolAccessDenied { .. } => "pool_access_denied",
            QuotaError::ChannelDenied { .. } => "channel_denied",
        }
    }
}

fn mode_str(m: ProxyMode) -> &'static str {
    match m {
        ProxyMode::Pool => "pool",
        ProxyMode::Fixed => "fixed",
    }
}

fn channel_str(c: VersionChannel) -> &'static str {
    match c {
        VersionChannel::Stable => "stable",
        VersionChannel::Canary => "canary",
        VersionChannel::Pinned => "pinned",
    }
}

/// Resolve the user's effective plan, enforcing `User.status`
/// (docs/12 §12.1). Missing user rows fall through to `effective_plan`
/// (a sub may carry a plan even if the read-only user row hasn't synced).
fn plan_for(dir: &dyn PlanDirectory, user: &str) -> Result<Plan, QuotaError> {
    if let Some(u) = dir.user(user)
        && u.status != UserStatus::Active
    {
        return Err(QuotaError::UserNotActive {
            user: user.into(),
            status: u.status,
        });
    }
    dir.effective_plan(user)
        .ok_or_else(|| QuotaError::NoPlan { user: user.into() })
}

fn check_features(plan: &Plan, intent: &CreateIntent) -> Result<(), QuotaError> {
    if !plan.proxy_modes.contains(&intent.proxy_mode) {
        return Err(QuotaError::ProxyModeDenied {
            plan: plan.id.clone(),
            mode: intent.proxy_mode,
        });
    }
    if intent.proxy_mode == ProxyMode::Pool && !plan.pool_access {
        return Err(QuotaError::PoolAccessDenied {
            plan: plan.id.clone(),
        });
    }
    if !plan.channels.contains(&intent.channel) {
        return Err(QuotaError::ChannelDenied {
            plan: plan.id.clone(),
            channel: intent.channel,
        });
    }
    Ok(())
}

/// `POST /bots`: user status, plan presence, `maxBots`, proxy mode,
/// pool access, channel (docs/12 §12.3 "Orchestrator API" row).
/// `maxRunningBots` is *not* checked here — create admits the bot and the
/// operator parks excess desired-running bots at `Paused(quota)`.
pub fn check_create(
    dir: &dyn PlanDirectory,
    user: &str,
    usage: &Consumption,
    intent: &CreateIntent,
) -> Result<(), QuotaError> {
    let plan = plan_for(dir, user)?;
    if let Some(max) = plan.max_bots
        && usage.bots_total >= max as usize
    {
        return Err(QuotaError::MaxBotsExceeded {
            limit: max,
            current: usage.bots_total,
        });
    }
    check_features(&plan, intent)
}

/// `PATCH /bots`: re-check the feature gates when `proxy` or `version`
/// change (docs/04 §4.7 "enforced at create and on config/proxy changes").
/// Bot-count caps are not re-evaluated — the bot already holds its slot.
pub fn check_spec_change(
    dir: &dyn PlanDirectory,
    user: &str,
    intent: &CreateIntent,
) -> Result<(), QuotaError> {
    let plan = plan_for(dir, user)?;
    check_features(&plan, intent)
}

/// `desiredState = Running` admission — **for the operator** (docs/12
/// §12.3 third row): when `usage.running_bots` is already at
/// `maxRunningBots`, this bot is excess and must stay `Paused(quota)`.
/// Quota loss never deletes; the controller drains, parks, and re-admits
/// when budget frees up.
pub fn check_running(
    dir: &dyn PlanDirectory,
    user: &str,
    usage: &Consumption,
) -> Result<(), QuotaError> {
    let plan = plan_for(dir, user)?;
    if let Some(max) = plan.max_running_bots
        && usage.running_bots >= max as usize
    {
        return Err(QuotaError::MaxRunningBotsExceeded {
            limit: max,
            current: usage.running_bots,
        });
    }
    Ok(())
}

/// Issuing or redeeming an invite: the granted plan must be active
/// (don't hand out seats on a retired plan).
pub fn check_invitable_plan(plan: &Plan) -> Result<(), QuotaError> {
    if !plan.active {
        return Err(QuotaError::PlanInactive {
            plan: plan.id.clone(),
        });
    }
    Ok(())
}
