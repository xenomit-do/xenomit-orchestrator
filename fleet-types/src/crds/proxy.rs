//! `Proxy` CR (docs/04 §4.1, docs/05 §5.4): one CR per proxy. Credentials
//! stay in Vault; the CR carries only the path (ADR-02, ADR-10).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::phase::BotCondition;

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "fleet.xenomit.io",
    version = "v1alpha1",
    kind = "Proxy",
    plural = "proxies",
    shortname = "xpx",
    namespaced,
    status = "ProxyStatus",
    printcolumn = r#"{"name":"Scheme","type":"string","jsonPath":".spec.scheme"}"#,
    printcolumn = r#"{"name":"Host","type":"string","jsonPath":".spec.host"}"#,
    printcolumn = r#"{"name":"Healthy","type":"string","jsonPath":".status.healthy"}"#,
    printcolumn = r#"{"name":"Bots","type":"integer","jsonPath":".status.assigned"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ProxySpec {
    pub scheme: ProxyScheme,
    pub host: String,
    pub port: u16,
    /// e.g. ["residential"], ["datacenter"]. `country` is separate because
    /// selectors match on it directly (docs/04 §4.1).
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isp: Option<String>,
    /// Max concurrent bots on this proxy; residential defaults to 1
    /// (docs/05 §5.4).
    #[serde(default = "default_capacity")]
    pub capacity: u32,
    /// Vault path of the proxy credentials (`secret/data/proxies/<id>`);
    /// never a CR field (docs/05 §5.6).
    pub credentials_vault_path: String,
}

fn default_capacity() -> u32 {
    1
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ProxyScheme {
    Http,
    Https,
    Socks5,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProxyStatus {
    /// Three consecutive failed probes mark it unhealthy (docs/05 §5.4).
    #[serde(default)]
    pub healthy: bool,
    #[serde(default)]
    pub consecutive_failures: u32,
    /// Bot names currently assigned (capacity accounting).
    #[serde(default)]
    pub assigned: Vec<String>,
    /// Observed exit IP, recorded per assignment (docs/05 §5.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_probe_at_unix_ms: Option<i64>,
    /// Don't reassign to a different tenant before this time (IP-reputation
    /// cooldown, docs/05 §5.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_cooldown_until_unix_ms: Option<i64>,
    /// Tenant that last used this proxy; the cooldown above applies to
    /// *other* tenants only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tenant: Option<String>,
    /// IPs `spec.host` last resolved to (health prober records them); the
    /// per-bot egress NetworkPolicy is rendered from the first entry
    /// (ops/k8s/20-bot-egress.template.yaml).
    #[serde(default)]
    pub resolved_ips: Vec<String>,
    #[serde(default)]
    pub conditions: Vec<BotCondition>,
}
