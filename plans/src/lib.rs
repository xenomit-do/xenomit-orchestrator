//! `plans`: Phase 8 — plans, usage & retention, orchestrator side
//! (docs/12, ADR-24).
//!
//! The plan data model lives in Postgres (panel DB); this crate is the
//! **read-only copy the orchestrator enforces** plus the machinery that is
//! genuinely orchestrator-owned:
//!
//! - [`model`]: `Plan` / `Subscription` / `Invite` / `UserAccess` /
//!   `UsageRecord` (docs/12 §12.2). No CRDs — docs/12 §12.2 puts the model
//!   in the panel DB, and ADR-05 keeps etcd for desired state only.
//! - [`directory`]: [`PlanDirectory`], the synchronous read-only-copy
//!   trait, with `InMemoryPlans` (local/dev/tests) and `NoPlans`.
//! - [`quota`]: the typed quota engine of docs/12 §12.3 — consumed by
//!   `orchestrator-api` on create/patch and intended for the operator on
//!   `desiredState = Running` (`check_running` → `Paused(quota)`).
//! - [`metering`]: hourly `UsageRecord` aggregation off the phase
//!   timeline — a pure function ([`metering::meter_window`]) plus the
//!   `PhaseTimelines`/`UsageStore` traits, in-memory impls, and the
//!   hourly `MeterJob` sweep.
//! - [`retention`]: the daily purge job (soft-delete +
//!   `retentionDaysAfterDelete` + undelete window), reusing the
//!   operator's `purge_due` decision and `SnapshotStore::purge` through
//!   traits.
//! - [`showback`]: per-tenant bot-hours report generator (§12.5.1).
//! - [`billing`]: `BillingProvider` trait — **interface only, no impl**
//!   (roadmap phase 10).
//!
//! SQL for the tables lives in `xenomit-platform/db/plans/` as plain numbered
//! `.sql` files (the workspace has no sqlx-migrations convention;
//! control-gateway migrates inline).

pub mod billing;
pub mod directory;
pub mod metering;
pub mod model;
pub mod quota;
pub mod retention;
pub mod showback;

pub use billing::{
    BillingError, BillingEvent, BillingProvider, DunningAction, ProviderSubscription,
    dunning_action,
};
pub use directory::{InMemoryPlans, NoPlans, PlanDirectory};
pub use metering::{
    Bucket, HOUR_MS, InMemoryTimelines, InMemoryUsage, MeterJob, MeterReport, MeterSubject,
    MeterSubjects, PhaseEvent, PhaseTimelines, UsageError, UsageStore, UsageSubject, bucket_of,
    meter_window,
};
pub use model::{
    BillingInterval, Invite, InviteError, InviteStatus, Plan, Subscription, SubscriptionStatus,
    UsageRecord, UserAccess, UserStatus,
};
pub use quota::{
    Consumption, CreateIntent, QuotaError, check_create, check_invitable_plan, check_running,
    check_spec_change,
};
pub use retention::{
    ControllerGate, PlanGate, PurgeGate, PurgeReport, RetentionError, RetentionHooks, RetentionJob,
    USAGE_RECORD_RETENTION_DAYS, UndeleteError, check_undelete, snapshot_cutoff,
};
pub use showback::{
    BotUsageTotals, ShowbackReport, TenantUsage, UsageTotals, report as showback_report,
};

pub use operator::policy::DEFAULT_RETENTION_DAYS;
