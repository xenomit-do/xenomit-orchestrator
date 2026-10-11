//! `PodBackend` on Kubernetes: bare Pods per docs/04 §4.2 (ADR-04), config
//! via a per-revision ConfigMap, projected ServiceAccount token for the
//! gateway, hardened pod spec per docs/09 §9.2.
//!
//! Every object it applies also carries the [`crate::argocd`] tracking
//! stamps and an `ownerReference` to the bot CR so Argo renders the
//! `xenomit-bots` tree (CR → pod → ConfigMap/NetworkPolicy) while being
//! structurally unable to touch any of it.
//!
//! Compiles and is structured for a real cluster; the object builders are
//! pure functions exercised offline, only the API calls need a cluster.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, Pod,
    PodSecurityContext, PodSpec, ProjectedVolumeSource, ResourceRequirements, SeccompProfile,
    ServiceAccountTokenProjection, Volume, VolumeMount, VolumeProjection,
};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Client;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use std::collections::BTreeMap;

use fleet_types::crds::XenomitBot;
use fleet_types::epoch::Epoch;

use crate::argocd::ArgoTracking;
use crate::netpol::{BotEgressSpec, InfraEgress, bot_egress_policy};
use crate::traits::{BackendError, BotPodSpec, PodBackend, PodObservation, PodState};

/// Audience of the projected SA token the bot presents to the gateway
/// (docs/04 §4.5).
pub const GATEWAY_TOKEN_AUDIENCE: &str = "xenomit-gateway";

/// Audience of the projected SA token the bot presents to Vault's jwt auth
/// mount (ops/vault/vault-configure-jwt.ps1: `bound_audiences=vault`).
pub const VAULT_TOKEN_AUDIENCE: &str = "vault";

/// Where the `vault-token` volume lands — matches botd's
/// `default_vault_token_file`.
pub const VAULT_TOKEN_MOUNT: &str = "/var/run/secrets/vault";

/// Endpoints the pod needs that the ConfigMap intentionally does not carry
/// — botd's `apply_env_overrides` synthesizes its `control.endpoint` and
/// `vault` block from these envs (docs/02 §2.2).
#[derive(Clone, Debug)]
pub struct DeployEnv {
    /// `XENOMIT_CONTROL_ENDPOINT`, e.g.
    /// `http://control-gateway.xenomit-system.svc.cluster.local:8443`.
    pub control_endpoint: String,
    /// `XENOMIT_VAULT_ADDR`, e.g.
    /// `https://vault.xenomit-system.svc.cluster.local:8200`.
    pub vault_addr: String,
    /// `XENOMIT_VAULT_MOUNT` — the Vault jwt auth mount (`jwt` per
    /// ops/vault/vault-configure-jwt.ps1).
    pub vault_mount: String,
}

impl Default for DeployEnv {
    fn default() -> Self {
        DeployEnv {
            control_endpoint: "http://control-gateway.xenomit-system:8443".into(),
            vault_addr: "https://vault.xenomit-system:8200".into(),
            vault_mount: "jwt".into(),
        }
    }
}

/// Kubernetes `PodBackend`. All calls hit the API server for `namespace`.
pub struct KubePodBackend {
    client: Client,
    namespace: String,
    /// Bootstrap-known infra CIDRs for the per-bot egress policies
    /// (ops/k8s/12-*.template.yaml). Empty = emit selector rules only.
    pub infra: InfraEgress,
    /// Argo CD read-only adoption (default on — [`ArgoTracking`]).
    pub argo: ArgoTracking,
    /// Runtime env endpoints injected into every bot pod.
    pub env: DeployEnv,
}

/// `ownerReferences` pointing at the bot CR, or `None` without a UID.
fn owner_refs(spec: &BotPodSpec, argo: &ArgoTracking) -> Option<Vec<OwnerReference>> {
    argo.owner_reference_for(&spec.bot_id, spec.owner_uid.as_deref())
        .map(|r| vec![r])
}

/// The rendered botd-config ConfigMap (`<bot>-r<revision>`), stamped and
/// owned — pure for offline tests.
pub fn render_configmap(namespace: &str, spec: &BotPodSpec, argo: &ArgoTracking) -> ConfigMap {
    let mut data = BTreeMap::new();
    data.insert("botd.json".into(), spec.config_json.clone());
    let mut meta = ObjectMeta {
        name: Some(KubePodBackend::configmap_name(spec)),
        namespace: Some(namespace.into()),
        owner_references: owner_refs(spec, argo),
        ..Default::default()
    };
    argo.stamp(&mut meta, "", "ConfigMap");
    ConfigMap {
        metadata: meta,
        data: Some(data),
        ..Default::default()
    }
}

/// The operator-owned Pod manifest (docs/04 §4.2 pod template), stamped
/// and owned — pure for offline tests.
pub fn render_pod(namespace: &str, spec: &BotPodSpec, argo: &ArgoTracking, env: &DeployEnv) -> Pod {
    let name = pod_name_for(spec);
    let cm = KubePodBackend::configmap_name(spec);
    let mut labels = spec.labels.clone();
    labels.insert(XenomitBot::BOT_LABEL.into(), spec.bot_id.clone());
    labels.insert(XenomitBot::EPOCH_LABEL.into(), spec.epoch.get().to_string());
    labels.insert(XenomitBot::TENANT_LABEL.into(), spec.tenant.clone());

    let mut meta = ObjectMeta {
        name: Some(name),
        namespace: Some(namespace.into()),
        labels: Some(labels),
        owner_references: owner_refs(spec, argo),
        ..Default::default()
    };
    argo.stamp(&mut meta, "", "Pod");

    Pod {
        metadata: meta,
        spec: Some(PodSpec {
            // The operator interprets exit codes, not kubelet
            // (docs/02 §2.5, ADR-14).
            restart_policy: Some("Never".into()),
            // hard deadline + 30 s buffer (docs/03 §3.2).
            termination_grace_period_seconds: Some(spec.hard_deadline_s as i64 + 30),
            // Per-bot SA; the token itself comes from a projected
            // volume below (docs/04 §4.2).
            service_account_name: Some(format!("bot-{}", spec.bot_id)),
            automount_service_account_token: Some(false),
            security_context: Some(PodSecurityContext {
                run_as_non_root: Some(true),
                run_as_user: Some(65534),
                run_as_group: Some(65534),
                fs_group: Some(65534),
                seccomp_profile: Some(SeccompProfile {
                    type_: "RuntimeDefault".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            containers: vec![Container {
                name: "botd".into(),
                image: Some(spec.image.clone()),
                image_pull_policy: Some("IfNotPresent".into()),
                args: Some(vec!["--config".into(), "/etc/xenomit/botd.json".into()]),
                env: Some(vec![
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "XENOMIT_BOT_ID".into(),
                        value: Some(spec.bot_id.clone()),
                        ..Default::default()
                    },
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "XENOMIT_EPOCH".into(),
                        value: Some(spec.epoch.get().to_string()),
                        ..Default::default()
                    },
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "XENOMIT_CONTROL_ENDPOINT".into(),
                        value: Some(env.control_endpoint.clone()),
                        ..Default::default()
                    },
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "XENOMIT_VAULT_ADDR".into(),
                        value: Some(env.vault_addr.clone()),
                        ..Default::default()
                    },
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "XENOMIT_VAULT_MOUNT".into(),
                        value: Some(env.vault_mount.clone()),
                        ..Default::default()
                    },
                    // readOnlyRootFilesystem: scratch + outbox land on an
                    // emptyDir (docs/09 §9.2 pod hardening).
                    k8s_openapi::api::core::v1::EnvVar {
                        name: "XENOMIT_STATE_DIR".into(),
                        value: Some("/botd-state".into()),
                        ..Default::default()
                    },
                ]),
                resources: spec.resources.as_ref().map(|(cpu, mem)| {
                    let mut requests = BTreeMap::new();
                    if let Some(cpu) = cpu {
                        requests.insert("cpu".into(), Quantity(cpu.clone()));
                    }
                    if let Some(mem) = mem {
                        requests.insert("memory".into(), Quantity(mem.clone()));
                    }
                    ResourceRequirements {
                        requests: Some(requests),
                        ..Default::default()
                    }
                }),
                security_context: Some(k8s_openapi::api::core::v1::SecurityContext {
                    allow_privilege_escalation: Some(false),
                    read_only_root_filesystem: Some(true),
                    run_as_non_root: Some(true),
                    capabilities: Some(Capabilities {
                        drop: Some(vec!["ALL".into()]),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                volume_mounts: Some(vec![
                    VolumeMount {
                        name: "config".into(),
                        mount_path: "/etc/xenomit".into(),
                        read_only: Some(true),
                        ..Default::default()
                    },
                    VolumeMount {
                        name: "outbox".into(),
                        mount_path: "/var/lib/xenomit/outbox".into(),
                        ..Default::default()
                    },
                    VolumeMount {
                        name: "gateway-token".into(),
                        mount_path: "/var/run/secrets/xenomit".into(),
                        read_only: Some(true),
                        ..Default::default()
                    },
                    VolumeMount {
                        name: "vault-token".into(),
                        mount_path: VAULT_TOKEN_MOUNT.into(),
                        read_only: Some(true),
                        ..Default::default()
                    },
                    VolumeMount {
                        name: "botd-state".into(),
                        mount_path: "/botd-state".into(),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }],
            volumes: Some(vec![
                Volume {
                    name: "config".into(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: cm,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                // Outbox of unacked stat events (docs/03 §3.6).
                Volume {
                    name: "outbox".into(),
                    empty_dir: Some(EmptyDirVolumeSource::default()),
                    ..Default::default()
                },
                Volume {
                    name: "gateway-token".into(),
                    projected: Some(ProjectedVolumeSource {
                        sources: Some(vec![VolumeProjection {
                            service_account_token: Some(ServiceAccountTokenProjection {
                                audience: Some(GATEWAY_TOKEN_AUDIENCE.into()),
                                expiration_seconds: Some(3600),
                                path: "token".into(),
                            }),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                // Vault JWT login: the bot's own SA token projected with
                // audience "vault" (docs/09 §9.2a, ADR-23). botd's default
                // token_file points at /var/run/secrets/vault/token.
                Volume {
                    name: "vault-token".into(),
                    projected: Some(ProjectedVolumeSource {
                        sources: Some(vec![VolumeProjection {
                            service_account_token: Some(ServiceAccountTokenProjection {
                                audience: Some(VAULT_TOKEN_AUDIENCE.into()),
                                expiration_seconds: Some(3600),
                                path: "token".into(),
                            }),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                // Writable scratch — readOnlyRootFilesystem means botd's
                // state_dir and tmp files need a volume (docs/09 §9.2).
                Volume {
                    name: "botd-state".into(),
                    empty_dir: Some(EmptyDirVolumeSource::default()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

impl KubePodBackend {
    pub fn new(client: Client, namespace: impl Into<String>) -> Self {
        KubePodBackend {
            client,
            namespace: namespace.into(),
            infra: InfraEgress::default(),
            argo: ArgoTracking::default(),
            env: DeployEnv::default(),
        }
    }

    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    fn configmaps(&self) -> Api<ConfigMap> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    fn netpols(&self) -> Api<NetworkPolicy> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    /// ConfigMap name holding the rendered botd config for a revision
    /// (docs/04 §4.2: "config via a generated ConfigMap named by
    /// configRevision").
    pub fn configmap_name(spec: &BotPodSpec) -> String {
        format!("{}-r{}", spec.bot_id, spec.config_revision)
    }

    /// Build the operator-owned Pod manifest (docs/04 §4.2 pod template).
    /// Stamped + owned per [`crate::argocd`]; rendering lives in
    /// [`render_pod`] so the manifest is testable without a client.
    pub fn pod_manifest(&self, spec: &BotPodSpec) -> Pod {
        render_pod(&self.namespace, spec, &self.argo, &self.env)
    }

    fn to_observation(pod: Pod) -> PodObservation {
        let meta = pod.metadata;
        let name = meta.name.unwrap_or_default();
        let epoch = meta
            .labels
            .as_ref()
            .and_then(|l| l.get(XenomitBot::EPOCH_LABEL))
            .and_then(|v| v.parse::<u64>().ok())
            .map(Epoch)
            .unwrap_or_default();
        let state = match pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .unwrap_or("Pending")
        {
            "Running" => PodState::Running,
            "Succeeded" | "Failed" => {
                let code = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.container_statuses.as_ref())
                    .and_then(|cs| cs.first())
                    .and_then(|c| c.state.as_ref())
                    .and_then(|st| st.terminated.as_ref())
                    .map(|t| t.exit_code);
                PodState::Exited { code }
            }
            "Pending" => PodState::Pending,
            _ => PodState::Unknown,
        };
        PodObservation { name, epoch, state }
    }
}

fn pod_name_for(spec: &BotPodSpec) -> String {
    fleet_types::crds::pod_name(&spec.bot_id, spec.epoch)
}

/// `bot-<id>-egress` for a pod spec carrying a proxy, stamped + owned.
/// A spec without a resolved proxy IP renders nothing — a wildcard rule
/// is worse than none (botd's `require_proxy` is the backstop). Pure:
/// only [`KubePodBackend::apply_egress_policy`] touches the cluster.
pub fn render_egress_policy(
    spec: &BotPodSpec,
    infra: &InfraEgress,
    argo: &ArgoTracking,
) -> Result<Option<NetworkPolicy>, BackendError> {
    let Some(proxy) = &spec.proxy else {
        return Ok(None);
    };
    let Some(ip) = proxy.egress_ip() else {
        tracing::warn!(
            bot = %spec.bot_id,
            "proxy host unresolved; skipping per-bot egress policy"
        );
        return Ok(None);
    };
    let mut policy = bot_egress_policy(&BotEgressSpec {
        bot_id: spec.bot_id.clone(),
        tenant: spec.tenant.clone(),
        proxy_ip: ip,
        proxy_port: proxy.port,
        infra: infra.clone(),
    })
    .map_err(|e| BackendError::Other(e.to_string()))?;
    argo.stamp(&mut policy.metadata, "networking.k8s.io", "NetworkPolicy");
    policy.metadata.owner_references = owner_refs(spec, argo);
    Ok(Some(policy))
}

impl KubePodBackend {
    /// Apply `bot-<id>-egress` when [`render_egress_policy`] renders one.
    /// The object is left behind on pod delete so a successor epoch keeps
    /// its rules; GC removes it with the owning bot CR (the ownerReference
    /// also makes purge clean when the CR is hard-deleted).
    async fn apply_egress_policy(&self, spec: &BotPodSpec) -> Result<(), BackendError> {
        let Some(policy) = render_egress_policy(spec, &self.infra, &self.argo)? else {
            return Ok(());
        };
        let name = policy.metadata.name.clone().unwrap_or_default();
        self.netpols()
            .patch(
                &name,
                &PatchParams::apply("xenomit-operator").force(),
                &Patch::Apply(&policy),
            )
            .await
            .map_err(|e| BackendError::Kube(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl PodBackend for KubePodBackend {
    async fn ensure_pod(&self, spec: &BotPodSpec) -> Result<(), BackendError> {
        // Config first (a pod must never see a missing/stale config), then
        // the pod itself. Server-side apply keeps ensure_pod idempotent.
        let cm = render_configmap(&self.namespace, spec, &self.argo);
        let pp = PatchParams::apply("xenomit-operator").force();
        self.configmaps()
            .patch(&Self::configmap_name(spec), &pp, &Patch::Apply(&cm))
            .await
            .map_err(|e| BackendError::Kube(e.to_string()))?;

        let pod = self.pod_manifest(spec);
        match self.pods().create(&PostParams::default(), &pod).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 409 => Ok(()), // already exists
            Err(e) => Err(BackendError::Kube(e.to_string())),
        }
        .map(|_| ())?;

        // Per-bot egress policy: same object every epoch, so the apply is
        // idempotent and picks up a changed resolved_ip on re-render
        // (docs/05 §5.3). It selects pods by `xenomit.io/bot`, not by pod
        // name, so it follows the bot across handoffs.
        self.apply_egress_policy(spec).await
    }

    async fn delete_pod(&self, bot_id: &str, epoch: Epoch) -> Result<(), BackendError> {
        let name = fleet_types::crds::pod_name(bot_id, epoch);
        match self.pods().delete(&name, &DeleteParams::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(()),
            Err(e) => Err(BackendError::Kube(e.to_string())),
        }
        .map(|_| ())
    }

    async fn list(&self, bot_id: &str) -> Result<Vec<PodObservation>, BackendError> {
        let lp = ListParams::default().labels(&format!("{}={}", XenomitBot::BOT_LABEL, bot_id));
        let pods = self
            .pods()
            .list(&lp)
            .await
            .map_err(|e| BackendError::Kube(e.to_string()))?;
        Ok(pods.items.into_iter().map(Self::to_observation).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::argocd::{COMPARE_OPTIONS, SYNC_OPTIONS, TRACKING_ID};
    use crate::traits::ProxyAssignment;

    fn spec(uid: Option<&str>) -> BotPodSpec {
        BotPodSpec {
            bot_id: "bot-7".into(),
            tenant: "user_abc".into(),
            epoch: Epoch(3),
            image: "img:test".into(),
            config_json: "{}".into(),
            config_revision: 2,
            labels: Default::default(),
            drain_deadline_s: 600,
            hard_deadline_s: 1800,
            proxy: Some(ProxyAssignment {
                name: Some("px-a".into()),
                scheme: None,
                host: "203.0.113.9".into(),
                port: 1080,
                credentials_vault_path: "secret/data/proxies/px-a".into(),
                resolved_ip: Some("203.0.113.9".into()),
                exit_ip: None,
            }),
            resources: None,
            owner_uid: uid.map(String::from),
        }
    }

    fn ann(meta: &ObjectMeta) -> &BTreeMap<String, String> {
        meta.annotations.as_ref().expect("argo annotations stamped")
    }

    #[test]
    fn pod_manifest_is_stamped_and_owned() {
        let pod = render_pod(
            "xenomit-bots",
            &spec(Some("uid-1")),
            &ArgoTracking::default(),
            &DeployEnv::default(),
        );
        let a = ann(&pod.metadata);
        // Core-group kind renders `/Pod` in the tracking-id.
        assert_eq!(a[TRACKING_ID], "xenomit-bots:/Pod:xenomit-bots/bot-7-e3");
        assert_eq!(a[COMPARE_OPTIONS], "IgnoreExtraneous");
        assert_eq!(a[SYNC_OPTIONS], "Prune=false,Delete=false");
        let owner = &pod.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(owner.kind, "XenomitBot");
        assert_eq!(owner.name, "bot-7");
        assert_eq!(owner.uid, "uid-1");
    }

    #[test]
    fn pod_without_uid_is_stamped_but_not_owned() {
        let pod = render_pod(
            "xenomit-bots",
            &spec(None),
            &ArgoTracking::default(),
            &DeployEnv::default(),
        );
        assert!(pod.metadata.owner_references.is_none());
        assert!(ann(&pod.metadata).contains_key(TRACKING_ID));
    }

    #[test]
    fn configmap_is_stamped_and_owned() {
        let cm = render_configmap(
            "xenomit-bots",
            &spec(Some("uid-1")),
            &ArgoTracking::default(),
        );
        assert_eq!(cm.metadata.name.as_deref(), Some("bot-7-r2"));
        assert_eq!(
            ann(&cm.metadata)[TRACKING_ID],
            "xenomit-bots:/ConfigMap:xenomit-bots/bot-7-r2"
        );
        assert!(cm.metadata.owner_references.is_some());
    }

    #[test]
    fn egress_policy_is_stamped_and_owned() {
        let p = render_egress_policy(
            &spec(Some("uid-1")),
            &InfraEgress::default(),
            &ArgoTracking::default(),
        )
        .unwrap()
        .expect("renders with a resolved proxy");
        assert_eq!(
            ann(&p.metadata)[TRACKING_ID],
            "xenomit-bots:networking.k8s.io/NetworkPolicy:xenomit-bots/bot-bot-7-egress"
        );
        assert_eq!(ann(&p.metadata)[SYNC_OPTIONS], "Prune=false,Delete=false");
        let owner = &p.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(owner.name, "bot-7");
    }

    #[test]
    fn disabled_stamps_nothing_anywhere() {
        let argo = ArgoTracking::disabled();
        let s = spec(Some("uid-1"));
        let pod = render_pod("xenomit-bots", &s, &argo, &DeployEnv::default());
        assert!(pod.metadata.annotations.is_none());
        // ownerReferences are GC semantics, not Argo's — they stay even
        // when tracking is off (they don't leak a name Argo could claim).
        assert!(pod.metadata.owner_references.is_some());
        let cm = render_configmap("xenomit-bots", &s, &argo);
        assert!(cm.metadata.annotations.is_none());
    }
}
