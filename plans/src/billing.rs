//! The `BillingProvider` seam (docs/12 §12.5.2, ADR-24). **Interface only
//! — no implementation.** Stripe is the likely first impl (roadmap phase
//! 10, a separate decision); the trait keeps that choice open and keeps
//! taxes/VAT/invoices/refunds out of this codebase until then.
//!
//! Data flow once a provider exists:
//! `create_customer`/`create_subscription` fill the `provider*` fields of
//! [`Subscription`]; `parse_webhook` events update `Subscription.status`,
//! which maps to `Plan` (and therefore quotas) through
//! [`crate::directory::PlanDirectory::effective_plan`].

use async_trait::async_trait;

use crate::model::{Plan, SubscriptionStatus, UsageRecord, UserAccess};

/// A billing provider integration (Stripe, …). Every method is
/// provider-side state; the local `Subscription` row is updated by the
/// caller from the returned values and webhook events.
#[async_trait]
pub trait BillingProvider: Send + Sync {
    /// Provider name stored on `Subscription.provider` (`"stripe"`, …).
    fn provider(&self) -> &'static str;

    /// Create the provider-side customer for a user; returns
    /// `providerCustomerId`.
    async fn create_customer(&self, user: &UserAccess) -> Result<String, BillingError>;

    /// Subscribe `providerCustomerId` to `plan`. Returns the
    /// provider-side ids and current period end. The caller stores them
    /// on `Subscription` and sets `status` per the first webhook.
    async fn create_subscription(
        &self,
        provider_customer_id: &str,
        plan: &Plan,
    ) -> Result<ProviderSubscription, BillingError>;

    /// Cancel at the current period end (docs/12 §12.5.3: `canceled` →
    /// bots stopped; the retention clock starts at deletion, not here).
    async fn cancel_subscription(&self, provider_subscription_id: &str)
    -> Result<(), BillingError>;

    /// Push metered usage to the provider (metered plans). `usage` is the
    /// hour rows since the last report; providers that don't meter may
    /// accept-and-ignore.
    async fn report_usage(
        &self,
        provider_subscription_id: &str,
        usage: &[UsageRecord],
    ) -> Result<(), BillingError>;

    /// Verify the signature and decode a webhook payload into a
    /// [`BillingEvent`]. Verification failures are `BillingError::Webhook`
    /// and must not be retried against the provider.
    fn parse_webhook(
        &self,
        signature: Option<&str>,
        body: &[u8],
    ) -> Result<BillingEvent, BillingError>;
}

/// Provider-side ids returned by `create_subscription`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderSubscription {
    pub provider_subscription_id: String,
    /// `None` when the provider doesn't report a period end (e.g. metered).
    pub current_period_end_unix_ms: Option<i64>,
}

/// What a webhook carries back. The caller applies it to the
/// `Subscription` row (`providerSubscriptionId` → `status` update).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BillingEvent {
    /// `invoice.paid` / `customer.subscription.updated` style events:
    /// the new status + period end.
    SubscriptionUpdated {
        provider_subscription_id: String,
        status: SubscriptionStatus,
        current_period_end_unix_ms: Option<i64>,
    },
    /// `customer.subscription.deleted`: canceled immediately.
    SubscriptionCanceled { provider_subscription_id: String },
    /// Provider events that don't move `Subscription.status`
    /// (payment_succeeded receipts, etc.) — safe to log-and-drop.
    Ignored { kind: String },
}

#[derive(Debug, thiserror::Error)]
pub enum BillingError {
    /// Provider unreachable / rate-limited — retryable.
    #[error("billing provider unavailable: {0}")]
    Unavailable(String),
    /// Provider refused the call (bad card, unknown customer, …).
    #[error("billing provider rejected the call: {0}")]
    Rejected(String),
    /// Webhook signature/decoding failure — not retryable, must not be
    /// surfaced verbatim to the caller (docs/09-style "no oracle").
    #[error("webhook rejected")]
    Webhook(String),
    #[error("billing: {0}")]
    Other(String),
}

/// Dunning policy (docs/12 §12.5.3): `past_due` → warn, grace period →
/// `Paused(quota)` (drain, no deletion); `canceled` → stopped. Pure: the
/// caller decides how to apply the action to the user's bots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DunningAction {
    /// Subscription healthy — nothing to do.
    None,
    /// `past_due` but inside the grace period: warn the user.
    Warn,
    /// Grace expired: drain the bots and park them `Paused(quota)`.
    PauseQuota,
    /// `canceled`: stop the bots (retention starts at deletion, not here).
    Stop,
}

/// Map `Subscription.status` + `currentPeriodEnd` to a dunning action.
/// `grace_ms` is the past-due grace window (e.g. 7 days).
pub fn dunning_action(
    status: SubscriptionStatus,
    current_period_end_unix_ms: Option<i64>,
    now_unix_ms: i64,
    grace_ms: i64,
) -> DunningAction {
    match status {
        SubscriptionStatus::Trialing | SubscriptionStatus::Active => DunningAction::None,
        SubscriptionStatus::PastDue => match current_period_end_unix_ms {
            Some(end) if now_unix_ms >= end + grace_ms => DunningAction::PauseQuota,
            _ => DunningAction::Warn,
        },
        SubscriptionStatus::Canceled => DunningAction::Stop,
    }
}
