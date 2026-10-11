//! Per-bot egress `NetworkPolicy` rendering (docs/05 §5.3).
//!
//! These are pure functions over k8s-openapi types — no client, no
//! cluster — so the golden-YAML tests run offline and the kube backend
//! can apply the same objects.
//!
//! The generated policy layers on top of the namespace-wide default-deny
//! (`ops/k8s/10-network-default-deny.yaml`). It selects pods by the
//! `xenomit.io/bot` label — not a pod name — so it follows the bot across
//! pod-for-pod handoffs. Egress is admitted to exactly:
//!
//! * the assigned proxy (ipBlock + port) — the only game-traffic egress,
//! * cluster DNS (`kube-system`, UDP+TCP 53),
//! * the control-plane gateway, the panel, the OTel collector
//!   (`observability`), each `namespaceSelector` + `podSelector`,
//! * Vault via `host.k3d.internal`'s node IP when known,
//! * node-local service ports via the node CIDR when known.
//!
//! A `requireProxy` bot whose proxy cannot be resolved must not get a
//! policy at all (fail closed): [`bot_egress_policy`] returns
//! [`NetpolError::NoProxyIp`]. Re-render whenever `resolved_ip` changes.

use std::collections::BTreeMap;
use std::net::IpAddr;

use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyPeer, NetworkPolicyPort,
    NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use serde::Serialize;

/// Canonical ports, matching `ops/k8s/11-bots-allow-infra.yaml`.
pub const GATEWAY_PORT: i32 = 8443;
pub const PANEL_PORT: i32 = 3000;
pub const OTEL_GRPC_PORT: i32 = 4317;
pub const VAULT_PORT: i32 = 8200;
pub const DNS_PORT: i32 = 53;

/// Labels every bot pod carries (see `kube_backend::labels_for`).
pub const LABEL_BOT: &str = "xenomit.io/bot";
pub const LABEL_TENANT: &str = "xenomit.io/tenant";

#[derive(Debug, thiserror::Error)]
pub enum NetpolError {
    /// The proxy host resolved to nothing usable — fail closed.
    #[error("no resolved proxy IP: a require-proxy bot cannot be given egress")]
    NoProxyIp,
    /// A caller-supplied CIDR couldn't be parsed.
    #[error("invalid CIDR {0:?}")]
    InvalidCidr(String),
    #[error("yaml: {0}")]
    Yaml(String),
}

/// Infra destinations only known at bootstrap (ops/k8s/12-*.template.yaml
/// and the Vault host entry). `None` entries are omitted rather than
/// broadened — a missing gateway IP must never widen egress.
#[derive(Clone, Debug, Default)]
pub struct InfraEgress {
    /// Vault's reachable address: typically `host.k3d.internal`'s node IP
    /// (docs/09 §9.2a) plus its port.
    pub vault: Option<(String, u16)>,
    /// Node CIDR covering node-local service ports (OTLP hostPort and
    /// friends, ops/k8s/12-node-cidr-policies.template.yaml).
    pub node_cidr: Option<String>,
}

/// Everything needed to render one bot's egress policy.
#[derive(Clone, Debug)]
pub struct BotEgressSpec {
    /// CR name / pod label `xenomit.io/bot` value.
    pub bot_id: String,
    /// `xenomit.io/tenant` label value; also the generated object name.
    pub tenant: String,
    /// The assigned proxy's resolved IP (or literal-IP host).
    pub proxy_ip: String,
    pub proxy_port: u16,
    pub infra: InfraEgress,
}

fn ip_block(cidr: &str) -> Result<IPBlock, NetpolError> {
    // k8s wants "<ip>/<prefix>"; a bare IP gets the host prefix.
    let cidr = match cidr.split_once('/') {
        Some((ip, p)) => {
            let addr: IpAddr = ip
                .parse()
                .map_err(|_| NetpolError::InvalidCidr(cidr.to_string()))?;
            let bits: u32 = p
                .parse()
                .map_err(|_| NetpolError::InvalidCidr(cidr.to_string()))?;
            let max = if addr.is_ipv4() { 32 } else { 128 };
            if bits > max {
                return Err(NetpolError::InvalidCidr(cidr.to_string()));
            }
            cidr.to_string()
        }
        None => {
            let addr: IpAddr = cidr
                .parse()
                .map_err(|_| NetpolError::InvalidCidr(cidr.to_string()))?;
            let bits = if addr.is_ipv4() { 32 } else { 128 };
            format!("{cidr}/{bits}")
        }
    };
    Ok(IPBlock { cidr, except: None })
}

fn labels(pairs: &[(&str, &str)]) -> Option<BTreeMap<String, String>> {
    Some(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

fn selector(pairs: &[(&str, &str)]) -> Option<LabelSelector> {
    Some(LabelSelector {
        match_labels: labels(pairs),
        ..Default::default()
    })
}

fn peer(ipblock: &str) -> Result<NetworkPolicyPeer, NetpolError> {
    Ok(NetworkPolicyPeer {
        ip_block: Some(ip_block(ipblock)?),
        ..Default::default()
    })
}

fn ns_peer(ns: &str, pods: &[(&str, &str)]) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        namespace_selector: selector(&[("kubernetes.io/metadata.name", ns)]),
        pod_selector: selector(pods),
        ..Default::default()
    }
}

fn ns_wide_peer(ns: &str) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        namespace_selector: selector(&[("kubernetes.io/metadata.name", ns)]),
        ..Default::default()
    }
}

fn tcp(port: i32) -> Vec<NetworkPolicyPort> {
    vec![NetworkPolicyPort {
        port: Some(IntOrString::Int(port)),
        protocol: Some("TCP".into()),
        ..Default::default()
    }]
}

fn udp_and_tcp(port: i32) -> Vec<NetworkPolicyPort> {
    vec![
        NetworkPolicyPort {
            port: Some(IntOrString::Int(port)),
            protocol: Some("UDP".into()),
            ..Default::default()
        },
        NetworkPolicyPort {
            port: Some(IntOrString::Int(port)),
            protocol: Some("TCP".into()),
            ..Default::default()
        },
    ]
}

/// Render the per-bot egress `NetworkPolicy` (unsorted egress order is
/// deliberate: proxy first so diffs read top-down).
///
/// `proxy_ip` must already be a literal IP (resolved upstream); a missing
/// or non-IP value is a hard error, never a wildcard — that is the
/// "deny all outbound except assigned proxy" invariant.
pub fn bot_egress_policy(spec: &BotEgressSpec) -> Result<NetworkPolicy, NetpolError> {
    if spec.proxy_ip.is_empty() {
        return Err(NetpolError::NoProxyIp);
    }
    let mut egress = vec![
        // The one and only non-infra destination: the assigned proxy.
        NetworkPolicyEgressRule {
            to: Some(vec![peer(&spec.proxy_ip)?]),
            ports: Some(tcp(spec.proxy_port as i32)),
        },
        // Cluster DNS (docs/05 §5.3).
        NetworkPolicyEgressRule {
            to: Some(vec![ns_wide_peer("kube-system")]),
            ports: Some(udp_and_tcp(DNS_PORT)),
        },
        // Control-plane gateway. Label contract per
        // ops/k8s/11-bots-allow-infra.yaml: xenomit.io/component.
        NetworkPolicyEgressRule {
            to: Some(vec![ns_peer(
                "xenomit-system",
                &[("xenomit.io/component", "gateway")],
            )]),
            ports: Some(tcp(GATEWAY_PORT)),
        },
        // Panel (Agent Protocol data plane).
        NetworkPolicyEgressRule {
            to: Some(vec![ns_peer(
                "xenomit-system",
                &[("xenomit.io/component", "panel")],
            )]),
            ports: Some(tcp(PANEL_PORT)),
        },
        // OTel collector (observability is its own trust domain).
        NetworkPolicyEgressRule {
            to: Some(vec![ns_peer(
                "observability",
                &[("app.kubernetes.io/name", "opentelemetry-collector")],
            )]),
            ports: Some(tcp(OTEL_GRPC_PORT)),
        },
    ];
    if let Some((ip, port)) = &spec.infra.vault {
        egress.push(NetworkPolicyEgressRule {
            to: Some(vec![peer(ip)?]),
            ports: Some(tcp(*port as i32)),
        });
    }
    if let Some(cidr) = &spec.infra.node_cidr {
        egress.push(NetworkPolicyEgressRule {
            to: Some(vec![peer(cidr)?]),
            // Node-local service ports only — not a broad node grant.
            ports: Some(tcp(OTEL_GRPC_PORT)),
        });
    }

    Ok(NetworkPolicy {
        metadata: ObjectMeta {
            name: Some(format!("bot-{}-egress", spec.bot_id)),
            namespace: Some("xenomit-bots".into()),
            labels: labels(&[
                ("app.kubernetes.io/managed-by", "xenomit-operator"),
                (LABEL_BOT, &spec.bot_id),
                (LABEL_TENANT, &spec.tenant),
            ]),
            ..Default::default()
        },
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: labels(&[(LABEL_BOT, &spec.bot_id)]),
                ..Default::default()
            }),
            policy_types: Some(vec!["Egress".into()]),
            egress: Some(egress),
            // No `ingress` key + not in policyTypes => ingress untouched
            // by this object; the default-deny owns it.
            ..Default::default()
        }),
    })
}

/// YAML-serializable view for `kubectl apply` / golden tests.
/// k8s-openapi types don't emit `apiVersion`/`kind`, so we wrap them.
#[derive(Serialize)]
struct Manifest<'a> {
    #[serde(rename = "apiVersion")]
    api_version: &'static str,
    kind: &'static str,
    metadata: &'a ObjectMeta,
    spec: &'a NetworkPolicySpec,
}

/// Render to a `kubectl apply -f`-able YAML document.
pub fn bot_egress_yaml(spec: &BotEgressSpec) -> Result<String, NetpolError> {
    let policy = bot_egress_policy(spec)?;
    let manifest = Manifest {
        api_version: "networking.k8s.io/v1",
        kind: "NetworkPolicy",
        metadata: &policy.metadata,
        spec: policy.spec.as_ref().expect("bot_egress_policy sets spec"),
    };
    serde_saphyr::to_string(&manifest).map_err(|e| NetpolError::Yaml(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> BotEgressSpec {
        BotEgressSpec {
            bot_id: "bot-7".into(),
            tenant: "user_abc".into(),
            proxy_ip: "203.0.113.9".into(),
            proxy_port: 1080,
            infra: InfraEgress {
                vault: Some(("192.168.65.2".into(), 8200)),
                node_cidr: Some("10.42.0.0/16".into()),
            },
        }
    }

    #[test]
    fn egress_is_closed_by_default() {
        let p = bot_egress_policy(&spec()).unwrap();
        let s = p.spec.unwrap();
        // Pod selector follows the bot label, not a pod name, so the
        // policy survives handoff.
        assert_eq!(
            s.pod_selector.unwrap().match_labels.unwrap()[LABEL_BOT],
            "bot-7"
        );
        assert_eq!(s.policy_types.unwrap(), vec!["Egress"]);
        assert!(s.ingress.is_none());
        let egress = s.egress.unwrap();
        // Proxy rule first, and the ipBlock is exactly /32.
        let proxy_rule = &egress[0];
        let peers = proxy_rule.to.as_ref().unwrap();
        assert_eq!(peers[0].ip_block.as_ref().unwrap().cidr, "203.0.113.9/32");
        assert_eq!(
            proxy_rule.ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(1080))
        );
        // Nothing else gets a wildcard: every peer is ipBlock or a
        // namespaceSelector+podSelector pair.
        for rule in &egress {
            for peer in rule.to.as_ref().unwrap() {
                assert!(
                    peer.ip_block.is_some() || peer.namespace_selector.is_some(),
                    "wild peer"
                );
            }
        }
    }

    #[test]
    fn fails_closed_without_proxy_ip() {
        let mut s = spec();
        s.proxy_ip = String::new();
        assert!(matches!(bot_egress_policy(&s), Err(NetpolError::NoProxyIp)));
    }

    /// Golden YAML lives in `tests/golden/bot-egress.yaml`; regenerate
    /// with `UPDATE_GOLDEN=1 cargo test -p operator`.
    #[test]
    fn yaml_matches_golden() {
        let yaml = bot_egress_yaml(&spec()).unwrap();
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/bot-egress.yaml");
        if std::env::var("UPDATE_GOLDEN").is_ok() {
            std::fs::write(path, &yaml).unwrap();
            return;
        }
        let golden = std::fs::read_to_string(path).expect("golden file");
        assert_eq!(yaml, golden, "run UPDATE_GOLDEN=1 to regen");
    }
}
