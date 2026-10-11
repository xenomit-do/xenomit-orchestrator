//! Kubernetes-backed `BotRepo`/`CrRepo`/`ProxyStore`/`RolloutStore`/
//! `BotDirectory` — the production twin of the API crate's `InMemory*`
//! pair (docs/04 §4.1: CRs are the source of truth).
//!
//! The CRDs declare a `status` subresource, so the API server ignores
//! `status` on `replace`/`create` of the main object: `update` splits its
//! mutation into a spec write (`replace`) plus a status merge
//! (`patch_status`), mirroring what an informer-driven operator would
//! write anyway.
use async_trait::async_trait;
use k8s_openapi::NamespaceResourceScope;
use kube::Client;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::core::Resource;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use fleet_types::crds::{BotRollout, Proxy, XenomitBot};
use operator::argocd::ArgoTracking;
use operator::traits::{BotDirectory, ProxyError, ProxyStore, RolloutError, RolloutStore};
use orchestrator_api::repo::{BotRepo, CrRepo, RepoError};

fn repo_err(e: impl std::fmt::Display) -> RepoError {
    RepoError::Other(e.to_string())
}

fn status_of<T: Serialize>(obj: &T) -> Option<Value> {
    serde_json::to_value(obj)
        .ok()
        .and_then(|v| v.get("status").cloned())
        .filter(|s| !s.is_null())
}

/// `XenomitBot` CRs in `namespace`.
pub struct KubeBots {
    api: Api<XenomitBot>,
    pub argo: ArgoTracking,
    /// Serializes `update`'s read-modify-write per name — kube has no
    /// CAS on status merges, and two drivers share this repo.
    update_lock: tokio::sync::Mutex<()>,
}

impl KubeBots {
    pub fn new(client: Client, namespace: &str) -> Self {
        KubeBots {
            api: Api::namespaced(client, namespace),
            argo: ArgoTracking::default(),
            update_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Write `bot.status` through the subresource (`patch_status` merge).
    async fn write_status(&self, bot: &XenomitBot) -> Result<(), RepoError> {
        let Some(name) = bot.metadata.name.clone() else {
            return Ok(());
        };
        let Some(status) = status_of(bot) else {
            return Ok(());
        };
        self.api
            .patch_status(
                &name,
                &PatchParams::default(),
                &Patch::Merge(json!({ "status": status })),
            )
            .await
            .map_err(repo_err)?;
        Ok(())
    }
}

#[async_trait]
impl BotRepo for KubeBots {
    async fn list(&self, owner: Option<&str>) -> Vec<XenomitBot> {
        let mut items = self
            .api
            .list(&ListParams::default())
            .await
            .map(|l| l.items)
            .unwrap_or_default();
        items.retain(|b| owner.is_none_or(|o| b.spec.owner == o));
        items
    }

    async fn get(&self, name: &str) -> Option<XenomitBot> {
        self.api.get_opt(name).await.ok().flatten()
    }

    /// kubectl-apply semantics: create or spec-replace, then status when
    /// the caller carried one (subresource write).
    async fn apply(&self, mut bot: XenomitBot) -> Result<(), RepoError> {
        let name = bot
            .metadata
            .name
            .clone()
            .ok_or_else(|| RepoError::Other("metadata.name required".into()))?;
        self.argo.stamp_bot(&mut bot);
        let existing = self.get(&name).await;
        match existing {
            Some(cur) => {
                if cur.metadata.labels.as_ref().is_some_and(|labels| {
                    labels
                        .get("xenomit.io/spec-owner")
                        .is_some_and(|owner| owner == "argocd")
                }) {
                    if bot.status.is_some() {
                        self.write_status(&bot).await?;
                    }
                    return Ok(());
                }
                let mut put = bot.clone();
                put.metadata.resource_version = cur.metadata.resource_version;
                // Keep the stored status on the spec write — the
                // subresource strips it anyway, but write_status decides
                // from `bot.status`, not from the PUT result.
                let status = bot.status.clone();
                put.status = cur.status.clone();
                self.api
                    .replace(&name, &PostParams::default(), &put)
                    .await
                    .map_err(repo_err)?;
                if status.is_some() {
                    let mut sb = put;
                    sb.status = status;
                    self.write_status(&sb).await?;
                }
            }
            None => {
                if bot.metadata.labels.as_ref().is_some_and(|labels| {
                    labels
                        .get("xenomit.io/spec-owner")
                        .is_some_and(|owner| owner == "argocd")
                }) {
                    return Err(RepoError::Other(
                        "Git-owned bot was removed; refusing to recreate it".into(),
                    ));
                }
                let status = bot.status.clone();
                let mut no_status = bot;
                no_status.status = None;
                let created = self
                    .api
                    .create(&PostParams::default(), &no_status)
                    .await
                    .map_err(repo_err)?;
                if status.is_some() {
                    let mut sb = created;
                    sb.status = status;
                    self.write_status(&sb).await?;
                }
            }
        }
        Ok(())
    }

    /// Read-modify-write under the local lock; spec/meta changes go
    /// through `replace`, status through the subresource. 409-conflicts
    /// retry once with a fresh read.
    async fn update(
        &self,
        name: &str,
        f: &mut (dyn for<'a> FnMut(&'a mut XenomitBot) + Send),
    ) -> Result<bool, RepoError> {
        let _lock = self.update_lock.lock().await;
        let Some(cur) = self.get(name).await else {
            return Ok(false);
        };
        let mut bot = cur.clone();
        f(&mut bot);
        if cur.metadata.labels.as_ref().is_some_and(|labels| {
            labels
                .get("xenomit.io/spec-owner")
                .is_some_and(|owner| owner == "argocd")
        }) {
            if bot.status != cur.status {
                self.write_status(&bot).await?;
            }
            return Ok(true);
        }
        self.argo.stamp_bot(&mut bot);
        let spec_changed = serde_json::to_value(&bot.spec).ok()
            != serde_json::to_value(&cur.spec).ok()
            || bot.metadata.labels != cur.metadata.labels
            || bot.metadata.annotations != cur.metadata.annotations;
        let status_changed = bot.status != cur.status;
        if spec_changed {
            let mut put = bot.clone();
            put.status = None;
            put.metadata.resource_version = cur.metadata.resource_version.clone();
            match self.api.replace(name, &PostParams::default(), &put).await {
                Ok(_) => {}
                Err(kube::Error::Api(ae)) if ae.code == 409 => {
                    // One retry on a fresh read — the caller's mutation is
                    // still valid (it only touched spec/status).
                    let Some(fresh) = self.get(name).await else {
                        return Ok(false);
                    };
                    let mut bot = fresh.clone();
                    f(&mut bot);
                    self.argo.stamp_bot(&mut bot);
                    let mut put = bot.clone();
                    put.status = None;
                    put.metadata.resource_version = fresh.metadata.resource_version;
                    self.api
                        .replace(name, &PostParams::default(), &put)
                        .await
                        .map_err(repo_err)?;
                    if bot.status != fresh.status {
                        self.write_status(&bot).await?;
                    }
                    return Ok(true);
                }
                Err(e) => return Err(repo_err(e)),
            }
        }
        if status_changed {
            self.write_status(&bot).await?;
        }
        Ok(true)
    }

    async fn delete(&self, name: &str) -> Result<bool, RepoError> {
        if let Some(current) = self.api.get_opt(name).await.map_err(repo_err)? {
            if current.metadata.labels.as_ref().is_some_and(|labels| {
                labels
                    .get("xenomit.io/spec-owner")
                    .is_some_and(|owner| owner == "argocd")
            }) {
                return Err(RepoError::Other(
                    "Git-owned desired resources must be deleted through a deployment PR".into(),
                ));
            }
        }
        match self.api.delete(name, &DeleteParams::default()).await {
            Ok(_) => Ok(true),
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(false),
            Err(e) => Err(repo_err(e)),
        }
    }
}

#[async_trait]
impl BotDirectory for KubeBots {
    async fn list(&self) -> Result<Vec<XenomitBot>, ProxyError> {
        Ok(<Self as BotRepo>::list(self, None).await)
    }

    async fn save(&self, bot: &XenomitBot) -> Result<(), ProxyError> {
        <Self as BotRepo>::apply(self, bot.clone())
            .await
            .map_err(|e| ProxyError::Store(e.to_string()))
    }
}

/// Generic CR repo over `Api<K>` (rollouts, proxies). Same
/// spec/status split as [`KubeBots`].
pub struct KubeCrRepo<K> {
    api: Api<K>,
    kind: &'static str,
}

impl<K> KubeCrRepo<K>
where
    K: Clone
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Serialize
        + DeserializeOwned
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    pub fn new(client: Client, namespace: &str, kind: &'static str) -> Self {
        KubeCrRepo {
            api: Api::namespaced(client, namespace),
            kind,
        }
    }

    fn name_of(v: &Value) -> String {
        v.pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }
}

#[async_trait]
impl<K> CrRepo for KubeCrRepo<K>
where
    K: Clone
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Serialize
        + DeserializeOwned
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    async fn list(&self) -> Vec<Value> {
        self.api
            .list(&ListParams::default())
            .await
            .map(|l| {
                l.items
                    .iter()
                    .filter_map(|o| serde_json::to_value(o).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn get(&self, name: &str) -> Option<Value> {
        self.api
            .get_opt(name)
            .await
            .ok()
            .flatten()
            .and_then(|o| serde_json::to_value(o).ok())
    }

    async fn apply(&self, obj: Value) -> Result<(), RepoError> {
        let name = Self::name_of(&obj);
        if name.is_empty() {
            return Err(RepoError::Other(format!(
                "{} object needs metadata.name",
                self.kind
            )));
        }
        let mut parsed: K = serde_json::from_value(obj).map_err(repo_err)?;
        let status = status_of(&parsed);
        // Strip status for the spec write — the subresource owns it.
        let existing = self.api.get_opt(&name).await.map_err(repo_err)?;
        {
            let meta = parsed.meta_mut();
            if let Some(cur) = &existing {
                meta.resource_version = cur.meta().resource_version.clone();
            }
        }
        let git_owned = existing.as_ref().is_some_and(|cur| {
            cur.meta().labels.as_ref().is_some_and(|labels| {
                labels
                    .get("xenomit.io/spec-owner")
                    .is_some_and(|owner| owner == "argocd")
            })
        });
        if existing.is_none()
            && parsed.meta().labels.as_ref().is_some_and(|labels| {
                labels
                    .get("xenomit.io/spec-owner")
                    .is_some_and(|owner| owner == "argocd")
            })
        {
            return Err(RepoError::Other(
                "Git-owned resource was removed; refusing to recreate it".into(),
            ));
        }
        if git_owned {
            // Skip spec replacement; the status subresource write below is operator-owned.
        } else if existing.is_some() {
            self.api
                .replace(&name, &PostParams::default(), &parsed)
                .await
                .map_err(repo_err)?;
        } else {
            self.api
                .create(&PostParams::default(), &parsed)
                .await
                .map_err(repo_err)?;
        }
        if let Some(status) = status {
            self.api
                .patch_status(
                    &name,
                    &PatchParams::default(),
                    &Patch::Merge(json!({ "status": status })),
                )
                .await
                .map_err(repo_err)?;
        }
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<bool, RepoError> {
        if let Some(current) = self.api.get_opt(name).await.map_err(repo_err)? {
            if current.meta().labels.as_ref().is_some_and(|labels| {
                labels
                    .get("xenomit.io/spec-owner")
                    .is_some_and(|owner| owner == "argocd")
            }) {
                return Err(RepoError::Other(
                    "Git-owned desired resources must be deleted through a deployment PR".into(),
                ));
            }
        }
        match self.api.delete(name, &DeleteParams::default()).await {
            Ok(_) => Ok(true),
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(false),
            Err(e) => Err(repo_err(e)),
        }
    }
}

#[async_trait]
impl ProxyStore for KubeCrRepo<Proxy> {
    async fn list(&self) -> Result<Vec<Proxy>, ProxyError> {
        self.api
            .list(&ListParams::default())
            .await
            .map(|l| l.items)
            .map_err(|e| ProxyError::Store(e.to_string()))
    }

    async fn get(&self, name: &str) -> Result<Option<Proxy>, ProxyError> {
        self.api
            .get_opt(name)
            .await
            .map_err(|e| ProxyError::Store(e.to_string()))
    }

    async fn save(&self, proxy: &Proxy) -> Result<(), ProxyError> {
        let v = serde_json::to_value(proxy).map_err(|e| ProxyError::Store(e.to_string()))?;
        <Self as CrRepo>::apply(self, v)
            .await
            .map_err(|e| ProxyError::Store(e.to_string()))
    }
}

#[async_trait]
impl RolloutStore for KubeCrRepo<BotRollout> {
    async fn list(&self) -> Result<Vec<BotRollout>, RolloutError> {
        self.api
            .list(&ListParams::default())
            .await
            .map(|l| l.items)
            .map_err(|e| RolloutError::Store(e.to_string()))
    }

    async fn get(&self, name: &str) -> Result<Option<BotRollout>, RolloutError> {
        self.api
            .get_opt(name)
            .await
            .map_err(|e| RolloutError::Store(e.to_string()))
    }

    async fn save(&self, rollout: &BotRollout) -> Result<(), RolloutError> {
        let v = serde_json::to_value(rollout).map_err(|e| RolloutError::Store(e.to_string()))?;
        <Self as CrRepo>::apply(self, v)
            .await
            .map_err(|e| RolloutError::Store(e.to_string()))
    }
}
