//! The proxy pool and the `ProxyController` (docs/05 §5.4, docs/04 §4.2):
//!
//! * [`Pool`] — the [`ProxyPool`] seam the `BotController` consults at
//!   spawn: sticky auto-assignment honoring the bot's selector, per-proxy
//!   capacity and the cross-tenant cooldown.
//! * [`ProxyController`] — probes every `Proxy` on a jittered interval,
//!   maintains `status.healthy`/`assigned`/`exit_ip`, and on failover moves
//!   each affected bot to a replacement and triggers a `proxy_change`
//!   handoff (graceful when the old proxy still answers, crash-style
//!   restore otherwise).
//! * [`MemoryProxyStore`] — reference `ProxyStore` for tests and the
//!   `local` backend.
//!
//! Everything is level-triggered and safe to re-run; probe failures are
//! cumulative across passes (docs/05 §5.4 "three consecutive failures").

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use fleet_types::condition_type;
use fleet_types::crds::{Proxy, ProxyMode, ProxySelector, XenomitBot};
use fleet_types::phase::{BotCondition, ConditionStatus};

use crate::controller::ReconcileOutcome;
use crate::policy::bot_seed;
use crate::traits::{
    BotDirectory, Clock, HandoffTrigger, ProbeReport, ProxyAssignment, ProxyEndpoint, ProxyError,
    ProxyPool, ProxyProber, ProxyStore,
};

/// Knobs shared by [`Pool`] and [`ProxyController`] (docs/05 §5.4).
#[derive(Clone, Debug)]
pub struct ProxyConfig {
    /// Consecutive failed probes before a proxy is marked unhealthy
    /// (doc: 3).
    pub failure_threshold: u32,
    /// Nominal probe interval; jitter ±25 % is added per proxy (doc: 60 s).
    pub probe_interval: Duration,
    /// How long a proxy stays reserved for its last tenant after the last
    /// bot leaves (IP-reputation cooldown; doc: 24 h).
    pub tenant_cooldown: Duration,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            failure_threshold: 3,
            probe_interval: Duration::from_secs(60),
            tenant_cooldown: Duration::from_secs(24 * 3600),
        }
    }
}

fn proxy_name(p: &Proxy) -> String {
    p.metadata.name.clone().unwrap_or_default()
}

fn status_mut(p: &mut Proxy) -> &mut fleet_types::crds::ProxyStatus {
    p.status.get_or_insert_with(Default::default)
}

fn status(p: &Proxy) -> fleet_types::crds::ProxyStatus {
    p.status.clone().unwrap_or_default()
}

fn bot_id(bot: &XenomitBot) -> String {
    bot.metadata.name.clone().unwrap_or_default()
}

/// The endpoint a prober checks for one `Proxy` CR.
pub fn endpoint_of(p: &Proxy) -> ProxyEndpoint {
    ProxyEndpoint {
        scheme: Some(p.spec.scheme),
        host: p.spec.host.clone(),
        port: p.spec.port,
    }
}

fn assignment_of(p: &Proxy) -> ProxyAssignment {
    let st = status(p);
    ProxyAssignment {
        name: Some(proxy_name(p)),
        scheme: Some(p.spec.scheme),
        host: p.spec.host.clone(),
        port: p.spec.port,
        credentials_vault_path: p.spec.credentials_vault_path.clone(),
        resolved_ip: st.resolved_ips.first().cloned(),
        exit_ip: st.exit_ip.clone(),
    }
}

/// Does the bot's pool selector admit this proxy? (docs/04 §4.1:
/// `country` matches the proxy's `spec.country`; every `tags` entry must
/// be present on the proxy.)
fn selector_matches(p: &Proxy, sel: Option<&ProxySelector>) -> bool {
    let Some(sel) = sel else { return true };
    if !sel.country.is_empty() {
        match &p.spec.country {
            Some(c) if sel.country.contains(c) => {}
            _ => return false,
        }
    }
    sel.tags.iter().all(|t| p.spec.tags.contains(t))
}

/// May this proxy take *new* bots? Unprobed proxies are admitted
/// optimistically — a dead one is evicted by its first probe or by bot
/// exit-69 reports — but a proxy with any recorded failure is skipped
/// until a clean probe clears it (an exit-69 bot must land somewhere else).
fn fresh_ok(st: &fleet_types::crds::ProxyStatus) -> bool {
    st.consecutive_failures == 0 && (st.healthy || st.last_probe_at_unix_ms.is_none())
}

/// May a bot *keep* its current proxy? Transient probe failures under the
/// threshold don't break sticky affinity (docs/05 §5.4).
fn sticky_ok(st: &fleet_types::crds::ProxyStatus, cfg: &ProxyConfig) -> bool {
    st.healthy || st.consecutive_failures < cfg.failure_threshold
}

/// Per-tenant cooldown (docs/05 §5.4): a proxy last used by a *different*
/// tenant is off-limits until `tenant_cooldown_until_unix_ms`.
fn tenant_ok(st: &fleet_types::crds::ProxyStatus, tenant: &str, now: i64) -> bool {
    match (&st.last_tenant, st.tenant_cooldown_until_unix_ms) {
        (Some(t), Some(until)) if t != tenant => now >= until,
        (Some(t), _) if t == tenant => true,
        (None, Some(until)) => now >= until,
        _ => true,
    }
}

/// Candidate ranking for assignment: fewest assigned bots, then name —
/// deterministic so tests and replays agree.
fn candidate_rank(p: &Proxy) -> (usize, String) {
    (status(p).assigned.len(), proxy_name(p))
}

/// Pick a replacement for `bot` among `candidates`, honoring health,
/// selector, capacity and cooldown. `assigned.contains(bot_id)` counts as
/// the bot already holding a slot.
pub fn select_proxy<'a>(
    bot: &XenomitBot,
    candidates: impl Iterator<Item = &'a Proxy>,
    now: i64,
) -> Option<&'a Proxy> {
    let id = bot_id(bot);
    let tenant = &bot.spec.owner;
    let mut pick: Option<&Proxy> = None;
    for p in candidates {
        let st = status(p);
        if !fresh_ok(&st)
            || !selector_matches(p, bot.spec.proxy.selector.as_ref())
            || !tenant_ok(&st, tenant, now)
        {
            continue;
        }
        let has_room = st.assigned.contains(&id) || st.assigned.len() < p.spec.capacity as usize;
        if !has_room {
            continue;
        }
        let better = pick.is_none_or(|q| candidate_rank(p) < candidate_rank(q));
        if better {
            pick = Some(p);
        }
    }
    pick
}

/// The [`ProxyPool`] over a [`ProxyStore`]: assignment writes the
/// proxy's `status.assigned` so capacity is accounted between reconciles.
pub struct Pool<S> {
    pub store: S,
    pub cfg: ProxyConfig,
}

impl<S> Pool<S> {
    pub fn new(store: S) -> Self {
        Pool {
            store,
            cfg: ProxyConfig::default(),
        }
    }

    /// Mark a proxy's tenant cooldown from a departing bot.
    fn depart(st: &mut fleet_types::crds::ProxyStatus, tenant: &str, now: i64, cfg: &ProxyConfig) {
        st.last_tenant = Some(tenant.to_string());
        st.tenant_cooldown_until_unix_ms = Some(now + cfg.tenant_cooldown.as_millis() as i64);
    }
}

#[async_trait]
impl<S: ProxyStore> ProxyPool for Pool<S> {
    async fn assign(
        &self,
        bot: &XenomitBot,
        now_unix_ms: i64,
    ) -> Result<Option<ProxyAssignment>, ProxyError> {
        let id = bot_id(bot);
        let proxies = self.store.list().await?;

        // Sticky affinity: keep the current proxyRef while it is still a
        // legal home for this bot.
        if let Some(cur) = bot
            .status
            .as_ref()
            .and_then(|s| s.proxy_ref.clone())
            .filter(|cur| {
                proxies.iter().any(|p| {
                    proxy_name(p) == *cur
                        && sticky_ok(&status(p), &self.cfg)
                        && selector_matches(p, bot.spec.proxy.selector.as_ref())
                        && tenant_ok(&status(p), &bot.spec.owner, now_unix_ms)
                })
            })
        {
            return self.resolve(&cur).await;
        }

        // The old proxyRef (if any) is unusable — free the slot.
        self.release(bot, now_unix_ms).await?;

        let pick = select_proxy(bot, proxies.iter(), now_unix_ms).cloned();
        let Some(mut pick) = pick else {
            return Ok(None);
        };
        {
            let st = status_mut(&mut pick);
            if !st.assigned.contains(&id) {
                st.assigned.push(id.clone());
                st.assigned.sort();
            }
        }
        self.store.save(&pick).await?;
        Ok(Some(assignment_of(&pick)))
    }

    async fn resolve(&self, proxy_name: &str) -> Result<Option<ProxyAssignment>, ProxyError> {
        Ok(self.store.get(proxy_name).await?.map(|p| assignment_of(&p)))
    }

    async fn release(&self, bot: &XenomitBot, now_unix_ms: i64) -> Result<(), ProxyError> {
        let Some(name) = bot.status.as_ref().and_then(|s| s.proxy_ref.clone()) else {
            return Ok(());
        };
        if let Some(mut p) = self.store.get(&name).await? {
            let st = status_mut(&mut p);
            if st.assigned.iter().any(|b| b == &bot_id(bot)) {
                st.assigned.retain(|b| b != &bot_id(bot));
                Self::depart(st, &bot.spec.owner, now_unix_ms, &self.cfg);
                self.store.save(&p).await?;
            }
        }
        Ok(())
    }

    async fn report_failure(
        &self,
        bot: &XenomitBot,
        proxy_name: &str,
        now_unix_ms: i64,
    ) -> Result<(), ProxyError> {
        let Some(mut p) = self.store.get(proxy_name).await? else {
            return Ok(());
        };
        {
            let st = status_mut(&mut p);
            st.consecutive_failures += 1;
            if st.consecutive_failures >= self.cfg.failure_threshold {
                st.healthy = false;
            }
            let id = bot_id(bot);
            if st.assigned.iter().any(|b| b == &id) {
                st.assigned.retain(|b| b != &id);
                Self::depart(st, &bot.spec.owner, now_unix_ms, &self.cfg);
            }
            crate::controller::upsert_condition(
                &mut st.conditions,
                BotCondition::new(
                    condition_type::PROXY_HEALTHY,
                    ConditionStatus::False,
                    now_unix_ms,
                ),
            );
        }
        self.store.save(&p).await?;
        Ok(())
    }
}

/// The `Proxy` reconciler (docs/04 §4.2): probes every `Proxy` CR, keeps
/// `status` truthful, migrates bots off failed proxies via `proxy_change`
/// handoffs, and probes fixed-mode bots' own proxies for reporting only.
pub struct ProxyController<S, R, B, C, H> {
    pub store: S,
    pub prober: R,
    pub bots: B,
    pub clock: C,
    pub handoffs: H,
    pub cfg: ProxyConfig,
}

impl<S, R, B, C, H> ProxyController<S, R, B, C, H>
where
    S: ProxyStore,
    R: ProxyProber,
    B: BotDirectory,
    C: Clock,
    H: HandoffTrigger,
{
    pub fn new(store: S, prober: R, bots: B, clock: C, handoffs: H) -> Self {
        ProxyController {
            store,
            prober,
            bots,
            clock,
            handoffs,
            cfg: ProxyConfig::default(),
        }
    }

    /// The next probe time for `name`: `probe_interval` ±25 % jitter seeded
    /// by the proxy name so the fleet doesn't probe in lockstep.
    pub fn next_probe_delay(&self, proxy_name: &str) -> Duration {
        let seed = bot_seed(proxy_name);
        let base_ms = self.cfg.probe_interval.as_millis() as u64;
        let spread = base_ms / 4;
        let jitter = if spread == 0 {
            0
        } else {
            (seed % (2 * spread + 1)) as i64 - spread as i64
        };
        Duration::from_millis((base_ms as i64 + jitter).max(1) as u64)
    }

    /// One level-triggered pass over the pool: probe every `Proxy`, sync
    /// per-bot egress identity, fail over off dead proxies, prune stale
    /// capacity claims, and probe fixed-mode bot proxies for reporting.
    pub async fn reconcile(&self) -> Result<ReconcileOutcome, ProxyError> {
        let now = self.clock.now_unix_ms();
        let threshold = self.cfg.failure_threshold;

        // --- probe every pool proxy ------------------------------------
        let mut proxies = self.store.list().await?;
        for p in proxies.iter_mut() {
            let rep = self.prober.probe(&endpoint_of(p)).await;
            {
                let st = status_mut(p);
                st.last_probe_at_unix_ms = Some(now);
                if rep.ok {
                    let was_unhealthy = !st.healthy;
                    st.healthy = true;
                    st.consecutive_failures = 0;
                    st.latency_ms = rep.latency_ms;
                    st.exit_ip = rep.exit_ip.clone().or(st.exit_ip.clone());
                    // An empty list means "prober didn't resolve", not
                    // "no IPs" — keep the last known egress addresses.
                    if !rep.resolved_ips.is_empty() {
                        st.resolved_ips = rep.resolved_ips.clone();
                    }
                    let mut cond = BotCondition::new(
                        condition_type::PROXY_HEALTHY,
                        ConditionStatus::True,
                        now,
                    );
                    if was_unhealthy {
                        cond.reason = Some("recovered".into());
                    }
                    crate::controller::upsert_condition(&mut st.conditions, cond);
                } else {
                    st.consecutive_failures += 1;
                    if st.consecutive_failures >= threshold {
                        st.healthy = false;
                    }
                    let mut cond = BotCondition::new(
                        condition_type::PROXY_HEALTHY,
                        ConditionStatus::False,
                        now,
                    );
                    cond.message = rep.error.clone();
                    crate::controller::upsert_condition(&mut st.conditions, cond);
                }
            }
            self.store.save(p).await?;
        }

        // --- bots: failover, egress-identity sync, fixed-mode probes -----
        let mut bots = self.bots.list().await?;
        for bot in bots.iter_mut() {
            let before = bot.status.clone();
            let deleted = bot
                .status
                .as_ref()
                .is_some_and(|s| s.deleted_at_unix_ms.is_some());
            if deleted {
                continue;
            }
            match bot.spec.proxy.mode {
                ProxyMode::Pool => self.reconcile_pool_bot(bot, &mut proxies, now).await?,
                ProxyMode::Fixed => self.reconcile_fixed_bot(bot, now).await?,
            }
            if bot.status != before {
                self.bots.save(bot).await?;
            }
        }

        // --- capacity accounting: `status.assigned` mirrors bot proxyRefs
        // and departed bots start the tenant cooldown -------------------
        let claims: HashMap<String, Vec<(String, String)>> = bots
            .iter()
            .filter(|b| {
                b.spec.proxy.mode == ProxyMode::Pool
                    && !b
                        .status
                        .as_ref()
                        .is_some_and(|s| s.deleted_at_unix_ms.is_some())
            })
            .filter_map(|b| {
                b.status
                    .as_ref()
                    .and_then(|s| s.proxy_ref.clone())
                    .map(|r| (r, (bot_id(b), b.spec.owner.clone())))
            })
            .fold(HashMap::new(), |mut m, (p, b)| {
                m.entry(p).or_default().push(b);
                m
            });
        for p in proxies.iter_mut() {
            let name = proxy_name(p);
            let referenced: Vec<String> = claims
                .get(&name)
                .map(|v| v.iter().map(|(b, _)| b.clone()).collect())
                .unwrap_or_default();
            let st = status_mut(p);
            let mut changed = false;
            // Add references the bots hold but the CR doesn't list.
            for b in &referenced {
                if !st.assigned.contains(b) {
                    st.assigned.push(b.clone());
                    changed = true;
                }
            }
            // Drop stale entries; the departure starts the cooldown.
            let departed: Vec<String> = st
                .assigned
                .iter()
                .filter(|b| !referenced.contains(b))
                .cloned()
                .collect();
            if !departed.is_empty() {
                st.assigned.retain(|b| referenced.contains(b));
                if let Some(owner) = bots
                    .iter()
                    .find(|b| departed.contains(&bot_id(b)))
                    .map(|b| b.spec.owner.clone())
                {
                    Pool::<S>::depart(st, &owner, now, &self.cfg);
                }
                changed = true;
            }
            if changed {
                st.assigned.sort();
                self.store.save(p).await?;
            }
        }

        // Wait until the soonest due probe.
        let requeue = proxies
            .iter()
            .map(|p| self.next_probe_delay(&proxy_name(p)))
            .min()
            .unwrap_or(self.cfg.probe_interval);
        Ok(ReconcileOutcome::requeue(requeue))
    }

    /// A pool-mode bot: keep its `proxyRef` healthy or migrate it.
    async fn reconcile_pool_bot(
        &self,
        bot: &mut XenomitBot,
        proxies: &mut [Proxy],
        now: i64,
    ) -> Result<(), ProxyError> {
        let id = bot_id(bot);
        let current = bot.status.as_ref().and_then(|s| s.proxy_ref.clone());
        match current {
            Some(cur) => {
                let idx = proxies.iter().position(|p| proxy_name(p) == cur);
                let usable = idx.is_some_and(|i| {
                    let p = &proxies[i];
                    sticky_ok(&status(p), &self.cfg)
                        && selector_matches(p, bot.spec.proxy.selector.as_ref())
                        && tenant_ok(&status(p), &bot.spec.owner, now)
                });
                if usable {
                    let p = &proxies[idx.unwrap()];
                    let st = status(p);
                    {
                        let bst = bot.status.get_or_insert_with(Default::default);
                        bst.proxy_exit_ip = st.exit_ip.clone().or(bst.proxy_exit_ip.clone());
                        bst.proxy_resolved_ip = st
                            .resolved_ips
                            .first()
                            .cloned()
                            .or(bst.proxy_resolved_ip.clone());
                    }
                    return Ok(());
                }

                // Failover: pick a replacement, move the claim, then drive
                // the handoff (docs/05 §5.4).
                let repl = select_proxy(bot, proxies.iter().filter(|p| proxy_name(p) != cur), now)
                    .cloned();
                match repl {
                    Some(mut newp) => {
                        {
                            let nst = status_mut(&mut newp);
                            if !nst.assigned.contains(&id) {
                                nst.assigned.push(id.clone());
                                nst.assigned.sort();
                            }
                        }
                        self.store.save(&newp).await?;
                        let assignment = assignment_of(&newp);
                        {
                            let bst = bot.status.get_or_insert_with(Default::default);
                            bst.proxy_ref = assignment.name.clone();
                            bst.proxy_resolved_ip = assignment
                                .resolved_ip
                                .clone()
                                .or(bst.proxy_resolved_ip.clone());
                            bst.proxy_exit_ip =
                                assignment.exit_ip.clone().or(bst.proxy_exit_ip.clone());
                            let mut cond = BotCondition::new(
                                condition_type::PROXY_HEALTHY,
                                ConditionStatus::False,
                                now,
                            );
                            cond.reason = Some("failover".into());
                            crate::controller::upsert_condition(&mut bst.conditions, cond);
                        }
                        self.handoffs.proxy_change_handoff(bot).await?;
                    }
                    None => {
                        // No replacement: leave proxyRef on the dead proxy —
                        // botd's own require_proxy check exits 69 and the
                        // exit path reassigns once the pool recovers.
                        let bst = bot.status.get_or_insert_with(Default::default);
                        let mut cond = BotCondition::new(
                            condition_type::PROXY_HEALTHY,
                            ConditionStatus::False,
                            now,
                        );
                        cond.reason = Some("no_replacement".into());
                        crate::controller::upsert_condition(&mut bst.conditions, cond);
                    }
                }
            }
            None => {
                // Opportunistic pre-assignment; the BotController also
                // assigns at spawn, so this only warms `status.proxyRef`.
                let pick = select_proxy(bot, proxies.iter(), now).cloned();
                if let Some(mut newp) = pick {
                    {
                        let st = status_mut(&mut newp);
                        if !st.assigned.contains(&id) {
                            st.assigned.push(id.clone());
                            st.assigned.sort();
                        }
                    }
                    self.store.save(&newp).await?;
                    let a = assignment_of(&newp);
                    let bst = bot.status.get_or_insert_with(Default::default);
                    bst.proxy_ref = a.name;
                    bst.proxy_resolved_ip = a.resolved_ip.or(bst.proxy_resolved_ip.clone());
                    bst.proxy_exit_ip = a.exit_ip.or(bst.proxy_exit_ip.clone());
                }
            }
        }
        Ok(())
    }

    /// A fixed-mode bot: the pool never reassigns it, but the user-supplied
    /// proxy is still probed and reported (docs/05 §5.4).
    async fn reconcile_fixed_bot(&self, bot: &mut XenomitBot, now: i64) -> Result<(), ProxyError> {
        let Some((host, port)) = bot
            .spec
            .proxy
            .fixed_addr
            .as_deref()
            .and_then(parse_host_port)
        else {
            return Ok(());
        };
        let ep = ProxyEndpoint {
            scheme: None,
            host,
            port,
        };
        let rep = self.prober.probe(&ep).await;
        let bst = bot.status.get_or_insert_with(Default::default);
        if rep.ok {
            bst.proxy_probe_failures = 0;
            bst.proxy_exit_ip = rep.exit_ip.or(bst.proxy_exit_ip.clone());
            bst.proxy_resolved_ip = rep
                .resolved_ips
                .first()
                .cloned()
                .or(bst.proxy_resolved_ip.clone());
            crate::controller::upsert_condition(
                &mut bst.conditions,
                BotCondition::new(condition_type::PROXY_HEALTHY, ConditionStatus::True, now),
            );
        } else {
            bst.proxy_probe_failures += 1;
            if bst.proxy_probe_failures >= self.cfg.failure_threshold {
                let mut cond =
                    BotCondition::new(condition_type::PROXY_HEALTHY, ConditionStatus::False, now);
                cond.message = rep.error;
                crate::controller::upsert_condition(&mut bst.conditions, cond);
            }
        }
        Ok(())
    }
}

/// The production prober (docs/05 §5.4): TCP-connect the endpoint inside a
/// timeout, measure connect latency, record the resolved IPs. The deeper
/// checks (HTTPS `HEAD` through the proxy, exit-IP lookup) are a follow-up
/// behind the same [`ProxyProber`] seam — a TCP-level "the proxy answers"
/// is already enough to drive the failure/failover budget.
#[derive(Clone, Debug)]
pub struct TcpProber {
    /// Connect deadline per probe (default 5 s).
    pub timeout: Duration,
}

impl Default for TcpProber {
    fn default() -> Self {
        TcpProber {
            timeout: Duration::from_secs(5),
        }
    }
}

#[async_trait]
impl ProxyProber for TcpProber {
    async fn probe(&self, endpoint: &ProxyEndpoint) -> ProbeReport {
        let addr = format!("{}:{}", endpoint.host, endpoint.port);
        let resolved_ips: Vec<String> = match tokio::net::lookup_host(&addr).await {
            Ok(addrs) => addrs.map(|a| a.ip().to_string()).collect(),
            Err(_) => vec![],
        };
        let started = std::time::Instant::now();
        match tokio::time::timeout(self.timeout, tokio::net::TcpStream::connect(&addr)).await {
            Ok(Ok(_)) => ProbeReport {
                ok: true,
                latency_ms: Some(started.elapsed().as_millis() as u32),
                exit_ip: None,
                resolved_ips,
                error: None,
            },
            Ok(Err(e)) => ProbeReport {
                ok: false,
                latency_ms: None,
                exit_ip: None,
                resolved_ips,
                error: Some(e.to_string()),
            },
            Err(_) => ProbeReport {
                ok: false,
                latency_ms: None,
                exit_ip: None,
                resolved_ips,
                error: Some(format!("connect timeout after {:?}", self.timeout)),
            },
        }
    }
}

/// Parse `"host:port"` (IPv6 in brackets) for `spec.proxy.fixedAddr`.
pub fn parse_host_port(addr: &str) -> Option<(String, u16)> {
    let addr = addr.trim();
    if addr.starts_with('[') {
        let end = addr.find(']')?;
        let host = &addr[1..end];
        let port = addr[end + 1..].strip_prefix(':')?.parse().ok()?;
        return Some((host.to_string(), port));
    }
    let (host, port) = addr.rsplit_once(':')?;
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port.parse().ok()?))
}

/// In-memory `ProxyStore` — reference impl for tests and `local` runs,
/// like [`crate::secrets::MemorySecretStore`].
#[derive(Default)]
pub struct MemoryProxyStore {
    items: Mutex<HashMap<String, Proxy>>,
}

impl MemoryProxyStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a proxy (test helper).
    pub fn seed(&self, p: &Proxy) {
        self.items.lock().unwrap().insert(proxy_name(p), p.clone());
    }

    /// Read one back (test helper).
    pub fn get_sync(&self, name: &str) -> Option<Proxy> {
        self.items.lock().unwrap().get(name).cloned()
    }
}

#[async_trait]
impl ProxyStore for MemoryProxyStore {
    async fn list(&self) -> Result<Vec<Proxy>, ProxyError> {
        let mut v: Vec<_> = self.items.lock().unwrap().values().cloned().collect();
        v.sort_by_key(proxy_name);
        Ok(v)
    }

    async fn get(&self, name: &str) -> Result<Option<Proxy>, ProxyError> {
        Ok(self.items.lock().unwrap().get(name).cloned())
    }

    async fn save(&self, proxy: &Proxy) -> Result<(), ProxyError> {
        self.items
            .lock()
            .unwrap()
            .insert(proxy_name(proxy), proxy.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fleet_types::crds::{AccountSpec, BotConfigSpec, ProxySpec, XenomitBotSpec};
    use fleet_types::phase::DesiredState;
    use kube::core::ObjectMeta;

    pub fn mk_proxy(name: &str) -> Proxy {
        Proxy::new(
            name,
            ProxySpec {
                scheme: fleet_types::crds::ProxyScheme::Socks5,
                host: "10.0.0.1".into(),
                port: 1080,
                tags: vec!["residential".into()],
                country: Some("DE".into()),
                isp: None,
                capacity: 1,
                credentials_vault_path: format!("secret/data/proxies/{name}"),
            },
        )
    }

    fn mk_bot(name: &str) -> XenomitBot {
        XenomitBot {
            metadata: ObjectMeta {
                name: Some(name.into()),
                ..Default::default()
            },
            spec: XenomitBotSpec {
                owner: "user_abc".into(),
                desired_state: DesiredState::Running,
                account: AccountSpec {
                    server: "int1.darkorbit.com".into(),
                    credentials_vault_path: "secret/data/bots/b/account".into(),
                    ..Default::default()
                },
                config: BotConfigSpec::default(),
                config_revision: 1,
                version: Default::default(),
                proxy: Default::default(),
                restore_policy: Default::default(),
                resources: None,
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn assign_is_sticky_and_capacity_bounded() {
        let store = MemoryProxyStore::new();
        store.seed(&mk_proxy("px-a"));
        store.seed(&mk_proxy("px-b"));
        let pool = Pool::new(store);

        let mut b1 = mk_bot("b1");
        let mut b2 = mk_bot("b2");

        let a1 = pool.assign(&b1, 0).await.unwrap().unwrap();
        b1.status.get_or_insert_with(Default::default).proxy_ref = a1.name.clone();
        assert_eq!(a1.name.as_deref(), Some("px-a"));

        // Second bot can't share a capacity-1 proxy.
        let a2 = pool.assign(&b2, 0).await.unwrap().unwrap();
        assert_eq!(a2.name.as_deref(), Some("px-b"));
        b2.status.get_or_insert_with(Default::default).proxy_ref = a2.name.clone();

        // Sticky: b1 keeps px-a.
        let a1b = pool.assign(&b1, 0).await.unwrap().unwrap();
        assert_eq!(a1b.name.as_deref(), Some("px-a"));
    }

    #[tokio::test]
    async fn selector_and_cooldown_filter_candidates() {
        let store = MemoryProxyStore::new();
        let mut de = mk_proxy("px-de");
        let mut us = mk_proxy("px-us");
        us.spec.country = Some("US".into());
        // px-de was last used by another tenant 1 h ago (cooldown 24 h).
        {
            let st = status_mut(&mut de);
            st.last_tenant = Some("other_tenant".into());
            st.tenant_cooldown_until_unix_ms = Some(3_600_000 * 24);
            st.healthy = true;
        }
        status_mut(&mut us).healthy = true;
        store.seed(&de);
        store.seed(&us);
        let pool = Pool::new(store);

        let mut bot = mk_bot("b1");
        bot.spec.proxy.selector = Some(ProxySelector {
            country: vec!["DE".into(), "US".into()],
            tags: vec![],
        });
        // DE is under another tenant's cooldown -> US wins.
        let a = pool.assign(&bot, 3_600_000).await.unwrap().unwrap();
        assert_eq!(a.name.as_deref(), Some("px-us"));

        // Same-tenant reuse is allowed inside the cooldown.
        let mut other = mk_bot("b2");
        other.spec.owner = "other_tenant".into();
        let st_de = pool.store.get("px-de").await.unwrap().unwrap();
        let candidates = [st_de];
        let pick = select_proxy(&other, candidates.iter(), 3_600_000);
        assert_eq!(proxy_name(pick.unwrap()), "px-de");
    }

    #[tokio::test]
    async fn report_failure_counts_and_releases() {
        let store = MemoryProxyStore::new();
        let mut px = mk_proxy("px-a");
        status_mut(&mut px).healthy = true;
        store.seed(&px);
        let pool = Pool::new(store);
        let mut bot = mk_bot("b1");
        pool.assign(&bot, 0).await.unwrap();
        bot.status.get_or_insert_with(Default::default).proxy_ref = Some("px-a".into());

        for i in 1..=3 {
            pool.report_failure(&bot, "px-a", i).await.unwrap();
        }
        let p = pool.store.get("px-a").await.unwrap().unwrap();
        let st = status(&p);
        assert!(!st.healthy);
        assert_eq!(st.consecutive_failures, 3);
        assert!(st.assigned.is_empty());
        assert_eq!(st.last_tenant.as_deref(), Some("user_abc"));
    }

    #[test]
    fn parse_host_port_variants() {
        assert_eq!(
            parse_host_port("proxy.example.com:1080"),
            Some(("proxy.example.com".into(), 1080))
        );
        assert_eq!(
            parse_host_port("203.0.113.7:3128"),
            Some(("203.0.113.7".into(), 3128))
        );
        assert_eq!(
            parse_host_port("[2001:db8::1]:1080"),
            Some(("2001:db8::1".into(), 1080))
        );
        assert_eq!(parse_host_port("no-port"), None);
        assert_eq!(parse_host_port("h:abc"), None);
    }
}
