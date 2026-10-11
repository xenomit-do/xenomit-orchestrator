//! Repository seams behind the REST handlers. Production resolves these
//! against the Kubernetes API (the `XenomitBot`/`BotRollout`/`Proxy` CRs);
//! the in-memory implementation backs the `local` backend and all tests.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

use fleet_types::crds::{BotRollout, Proxy, XenomitBot};
use kube::core::ObjectMeta;
use operator::argocd::ArgoTracking;
use operator::traits::{ProxyError, ProxyStore, RolloutError, RolloutStore};

/// Object-safe bot store. `update` takes a callback rather than being
/// generic so `dyn BotRepo` works.
#[async_trait]
pub trait BotRepo: Send + Sync {
    /// All bots, optionally filtered by `spec.owner`.
    async fn list(&self, owner: Option<&str>) -> Vec<XenomitBot>;
    async fn get(&self, name: &str) -> Option<XenomitBot>;
    /// Create-or-update (kubectl apply semantics).
    async fn apply(&self, bot: XenomitBot) -> Result<(), RepoError>;
    /// Mutate a stored bot under lock; `false` if absent.
    async fn update(
        &self,
        name: &str,
        f: &mut (dyn for<'a> FnMut(&'a mut XenomitBot) + Send),
    ) -> Result<bool, RepoError>;
    /// Hard remove (only after purge_due; soft-delete is a status flag).
    async fn delete(&self, name: &str) -> Result<bool, RepoError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("repo: {0}")]
    Other(String),
}

/// Generic object-safe store for the smaller CRs (`BotRollout`, `Proxy`).
#[async_trait]
pub trait CrRepo: Send + Sync {
    async fn list(&self) -> Vec<serde_json::Value>;
    async fn get(&self, name: &str) -> Option<serde_json::Value>;
    async fn apply(&self, obj: serde_json::Value) -> Result<(), RepoError>;
    async fn delete(&self, name: &str) -> Result<bool, RepoError>;
}

fn name_of(v: &serde_json::Value) -> String {
    v.pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .unwrap_or_default()
        .to_string()
}

/// In-memory repositories (`local` backend + tests). `InMemoryBots` keeps
/// the `XenomitBot` CRs; `InMemoryCrs` stores any CR as JSON keyed by
/// `metadata.name`.
///
/// `argo` carries the Argo CD read-only adoption config
/// ([`ArgoTracking`], default on): every `XenomitBot` write re-asserts the
/// tracking/sync annotations so they can never drift or be stripped by a
/// spec edit. Any future kube-backed `BotRepo` must stamp the same way —
/// `ArgoTracking::stamp_bot` before its API call.
#[derive(Default)]
pub struct InMemoryBots {
    bots: Mutex<HashMap<String, XenomitBot>>,
    pub argo: ArgoTracking,
}

impl InMemoryBots {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl BotRepo for InMemoryBots {
    async fn list(&self, owner: Option<&str>) -> Vec<XenomitBot> {
        let mut v: Vec<_> = self
            .bots
            .lock()
            .unwrap()
            .values()
            .filter(|b| owner.is_none_or(|o| b.spec.owner == o))
            .cloned()
            .collect();
        v.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        v
    }

    async fn get(&self, name: &str) -> Option<XenomitBot> {
        self.bots.lock().unwrap().get(name).cloned()
    }

    async fn apply(&self, mut bot: XenomitBot) -> Result<(), RepoError> {
        if bot.metadata.name.is_none() {
            return Err(RepoError::Other("metadata.name required".into()));
        }
        self.argo.stamp_bot(&mut bot);
        // First write defaults status; later writes keep it (status
        // subresource is owned by the operator).
        let mut bots = self.bots.lock().unwrap();
        if let Some(existing) = bots.get(bot.metadata.name.as_ref().unwrap())
            && bot.status.is_none()
        {
            bot.status = existing.status.clone();
        }
        bots.insert(bot.metadata.name.clone().unwrap(), bot);
        Ok(())
    }

    async fn update(
        &self,
        name: &str,
        f: &mut (dyn for<'a> FnMut(&'a mut XenomitBot) + Send),
    ) -> Result<bool, RepoError> {
        let mut bots = self.bots.lock().unwrap();
        match bots.get_mut(name) {
            Some(b) => {
                f(b);
                self.argo.stamp_bot(b);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn delete(&self, name: &str) -> Result<bool, RepoError> {
        Ok(self.bots.lock().unwrap().remove(name).is_some())
    }
}

pub struct InMemoryCrs {
    kind: &'static str,
    items: Mutex<HashMap<String, serde_json::Value>>,
}

impl InMemoryCrs {
    pub fn new(kind: &'static str) -> Self {
        InMemoryCrs {
            kind,
            items: Mutex::new(HashMap::new()),
        }
    }

    /// Seed helpers for tests/local.
    pub fn seed_rollout(&self, r: &BotRollout) {
        self.items.lock().unwrap().insert(
            name_of(&serde_json::to_value(r).unwrap()),
            serde_json::to_value(r).unwrap(),
        );
    }

    pub fn seed_proxy(&self, p: &Proxy) {
        self.items.lock().unwrap().insert(
            name_of(&serde_json::to_value(p).unwrap()),
            serde_json::to_value(p).unwrap(),
        );
    }
}

#[async_trait]
impl CrRepo for InMemoryCrs {
    async fn list(&self) -> Vec<serde_json::Value> {
        let mut v: Vec<_> = self.items.lock().unwrap().values().cloned().collect();
        v.sort_by_key(name_of);
        v
    }

    async fn get(&self, name: &str) -> Option<serde_json::Value> {
        self.items.lock().unwrap().get(name).cloned()
    }

    async fn apply(&self, obj: serde_json::Value) -> Result<(), RepoError> {
        let name = name_of(&obj);
        if name.is_empty() {
            return Err(RepoError::Other(format!(
                "{} object needs metadata.name",
                self.kind
            )));
        }
        self.items.lock().unwrap().insert(name, obj);
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<bool, RepoError> {
        Ok(self.items.lock().unwrap().remove(name).is_some())
    }
}

/// `ProxyStore` over the CR repo so one `InMemoryCrs` (or its k8s-backed
/// twin) feeds both the API routes and the controller's `Pool`
/// (docs/05 §5.4).
#[async_trait]
impl ProxyStore for InMemoryCrs {
    async fn list(&self) -> Result<Vec<Proxy>, ProxyError> {
        Ok(CrRepo::list(self)
            .await
            .into_iter()
            .filter_map(|v| serde_json::from_value::<Proxy>(v).ok())
            .collect())
    }

    async fn get(&self, name: &str) -> Result<Option<Proxy>, ProxyError> {
        Ok(CrRepo::get(self, name)
            .await
            .and_then(|v| serde_json::from_value::<Proxy>(v).ok()))
    }

    async fn save(&self, proxy: &Proxy) -> Result<(), ProxyError> {
        let v = serde_json::to_value(proxy).map_err(|e| ProxyError::Store(e.to_string()))?;
        CrRepo::apply(self, v)
            .await
            .map_err(|e| ProxyError::Store(e.to_string()))
    }
}

/// `RolloutStore` over the CR repo so one `InMemoryCrs` (or its k8s-backed
/// twin) feeds both the API routes and the `BotRolloutController`
/// (docs/04 §4.2, docs/07 §7.3).
#[async_trait]
impl RolloutStore for InMemoryCrs {
    async fn list(&self) -> Result<Vec<BotRollout>, RolloutError> {
        Ok(CrRepo::list(self)
            .await
            .into_iter()
            .filter_map(|v| serde_json::from_value::<BotRollout>(v).ok())
            .collect())
    }

    async fn get(&self, name: &str) -> Result<Option<BotRollout>, RolloutError> {
        Ok(CrRepo::get(self, name)
            .await
            .and_then(|v| serde_json::from_value::<BotRollout>(v).ok()))
    }

    async fn save(&self, rollout: &BotRollout) -> Result<(), RolloutError> {
        let v = serde_json::to_value(rollout).map_err(|e| RolloutError::Store(e.to_string()))?;
        CrRepo::apply(self, v)
            .await
            .map_err(|e| RolloutError::Store(e.to_string()))
    }
}

/// Build a `XenomitBot` CR for create requests.
pub fn new_bot(name: &str, spec: fleet_types::crds::XenomitBotSpec) -> XenomitBot {
    XenomitBot {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        },
        spec,
        status: Some(Default::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fleet_types::crds::XenomitBotSpec;
    use operator::argocd::{COMPARE_OPTIONS, SYNC_OPTIONS, TRACKING_ID};

    fn spec() -> XenomitBotSpec {
        XenomitBotSpec {
            owner: "user_abc".into(),
            desired_state: Default::default(),
            account: Default::default(),
            config: Default::default(),
            config_revision: 0,
            version: Default::default(),
            proxy: Default::default(),
            restore_policy: Default::default(),
            resources: None,
        }
    }

    fn ann(b: &XenomitBot) -> &std::collections::BTreeMap<String, String> {
        b.metadata.annotations.as_ref().expect("argo stamped")
    }

    #[tokio::test]
    async fn apply_stamps_the_bot_for_the_tracking_app() {
        let repo = InMemoryBots::new();
        repo.apply(new_bot("bot-1", spec())).await.unwrap();
        let b = repo.get("bot-1").await.unwrap();
        let a = ann(&b);
        // Namespace falls back to the bots namespace — the API server
        // normally fills metadata.namespace on a real apply.
        assert_eq!(
            a[TRACKING_ID],
            "xenomit-bots:fleet.xenomit.io/XenomitBot:xenomit-bots/bot-1"
        );
        assert_eq!(a[COMPARE_OPTIONS], "IgnoreExtraneous");
        assert_eq!(a[SYNC_OPTIONS], "Prune=false,Delete=false");
    }

    #[tokio::test]
    async fn update_reasserts_stripped_annotations() {
        let repo = InMemoryBots::new();
        repo.apply(new_bot("bot-1", spec())).await.unwrap();
        // A write stripping our annotations gets repaired at write time.
        repo.update("bot-1", &mut |b| {
            b.metadata.annotations = None;
        })
        .await
        .unwrap();
        assert!(ann(&repo.get("bot-1").await.unwrap()).contains_key(TRACKING_ID));
    }

    #[tokio::test]
    async fn disabled_tracking_writes_nothing() {
        let mut repo = InMemoryBots::new();
        repo.argo = ArgoTracking::disabled();
        repo.apply(new_bot("bot-1", spec())).await.unwrap();
        assert!(
            repo.get("bot-1")
                .await
                .unwrap()
                .metadata
                .annotations
                .is_none()
        );
    }
}
