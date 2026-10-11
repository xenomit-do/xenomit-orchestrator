//! Showback report (docs/12 §12.5.1): "the admin page shows each user's
//! bot-hours and proxy usage, which also tells you what hosting actually
//! costs." Built from stored [`UsageRecord`]s — pure function, so the
//! admin endpoint is a query + this.
//!
//! Proxy usage isn't a `UsageRecord` field today; when per-bot proxy
//! accounting lands, it joins the report the same way (one more counter
//! per hour bucket).

use schemars::JsonSchema;
use serde::Serialize;
use std::collections::BTreeMap;

use crate::model::UsageRecord;

/// Per-bot column of the report.
#[derive(Clone, Debug, Default, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BotUsageTotals {
    pub bot_id: String,
    pub running_s: u64,
    pub offline_s: u64,
    pub needs_human_s: u64,
    pub user_paused_s: u64,
    /// Hour buckets observed (a bot billed from hour 0 has `hours` cells).
    pub hours: u64,
    /// Version(s)/plan(s) seen on this bot's rows, for auditing.
    pub versions: Vec<String>,
    pub plan_ids: Vec<String>,
}

impl BotUsageTotals {
    /// Bot-hours actually in game — the number the admin page shows.
    pub fn running_hours(&self) -> f64 {
        self.running_s as f64 / 3600.0
    }
}

/// Sum over a tenant's bots (and the report's grand total).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsageTotals {
    pub running_s: u64,
    pub offline_s: u64,
    pub needs_human_s: u64,
    pub user_paused_s: u64,
}

impl UsageTotals {
    /// SLO availability per docs/12 §12.5.4: `running / (running +
    /// offline)` — `offline_s` already excludes `needs_human_s` and the
    /// user-caused bucket by construction, so this is exactly "downtime
    /// nobody asked for". `None` when there is nothing to measure.
    pub fn availability(&self) -> Option<f64> {
        let denom = self.running_s + self.offline_s;
        (denom > 0).then(|| self.running_s as f64 / denom as f64)
    }
}

/// One tenant's usage block.
#[derive(Clone, Debug, Default, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TenantUsage {
    pub tenant: String,
    /// Plans seen on the tenant's rows in the window.
    pub plan_ids: Vec<String>,
    pub bots: Vec<BotUsageTotals>,
    pub totals: UsageTotals,
    pub availability: Option<f64>,
}

/// `GET …/usage` response body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ShowbackReport {
    pub from_unix_ms: i64,
    pub to_unix_ms: i64,
    pub generated_at_unix_ms: i64,
    pub tenants: Vec<TenantUsage>,
    pub totals: UsageTotals,
}

/// Build the report from stored rows; `from`/`to` bound the window.
pub fn report(
    records: &[UsageRecord],
    from_unix_ms: i64,
    to_unix_ms: i64,
    generated_at_unix_ms: i64,
) -> ShowbackReport {
    let mut tenants: BTreeMap<String, TenantAcc> = BTreeMap::new();
    for r in records {
        if r.hour_unix_ms < from_unix_ms || r.hour_unix_ms >= to_unix_ms {
            continue;
        }
        let t = tenants.entry(r.tenant.clone()).or_default();
        if !r.plan_id.is_empty() && !t.plan_ids.contains(&r.plan_id) {
            t.plan_ids.push(r.plan_id.clone());
        }
        let b = t
            .bots
            .entry(r.bot_id.clone())
            .or_insert_with(|| BotUsageTotals {
                bot_id: r.bot_id.clone(),
                ..Default::default()
            });
        b.running_s += r.running_s;
        b.offline_s += r.offline_s;
        b.needs_human_s += r.needs_human_s;
        b.user_paused_s += r.user_paused_s;
        b.hours += 1;
        if !r.version.is_empty() && !b.versions.contains(&r.version) {
            b.versions.push(r.version.clone());
        }
        if !r.plan_id.is_empty() && !b.plan_ids.contains(&r.plan_id) {
            b.plan_ids.push(r.plan_id.clone());
        }
    }

    let mut total = UsageTotals::default();
    let tenants = tenants
        .into_iter()
        .map(|(tenant, acc)| {
            let bots: Vec<BotUsageTotals> = acc.bots.into_values().collect();
            let mut totals = UsageTotals::default();
            for b in &bots {
                totals.running_s += b.running_s;
                totals.offline_s += b.offline_s;
                totals.needs_human_s += b.needs_human_s;
                totals.user_paused_s += b.user_paused_s;
            }
            total.running_s += totals.running_s;
            total.offline_s += totals.offline_s;
            total.needs_human_s += totals.needs_human_s;
            total.user_paused_s += totals.user_paused_s;
            let availability = totals.availability();
            TenantUsage {
                tenant,
                plan_ids: acc.plan_ids,
                bots,
                totals,
                availability,
            }
        })
        .collect();

    ShowbackReport {
        from_unix_ms,
        to_unix_ms,
        generated_at_unix_ms,
        tenants,
        totals: total,
    }
}

#[derive(Default)]
struct TenantAcc {
    plan_ids: Vec<String>,
    bots: BTreeMap<String, BotUsageTotals>,
}
