//! Argo owns Git-declared fleet roots; the operator owns workload children and status.
//! Roots labeled `xenomit.io/spec-owner=argocd` retain Argo's tracking metadata.
//! Generated children carry ownerReferences and no-prune tracking annotations so
//! they remain visible in the fleet Application without entering its desired state.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

use fleet_types::crds::XenomitBot;

/// Argo CD annotation keys (argocd.argoproj.io/*).
pub const TRACKING_ID: &str = "argocd.argoproj.io/tracking-id";
pub const COMPARE_OPTIONS: &str = "argocd.argoproj.io/compare-options";
pub const SYNC_OPTIONS: &str = "argocd.argoproj.io/sync-options";
/// Compare option value: tracked-but-external objects don't count toward
/// OutOfSync (their health still propagates to the app — Argo docs).
pub const COMPARE_IGNORE_EXTRANEOUS: &str = "IgnoreExtraneous";
/// Sync option value: never pruned by a sync, never deleted with the app.
pub const SYNC_NO_PRUNE_NO_DELETE: &str = "Prune=false,Delete=false";

/// The tracking parent Application (ops/argocd/apps/14-bots.yaml).
pub const DEFAULT_APP_NAME: &str = "xenomit-bots";
/// Namespace every bot object lives in.
pub const DEFAULT_BOTS_NAMESPACE: &str = "xenomit-bots";

/// API identity of the `XenomitBot` CRD — kept in lockstep with the
/// `#[kube(...)]` derive in fleet-types/src/crds/bot.rs by a unit test.
pub const BOT_GROUP: &str = "fleet.xenomit.io";
pub const BOT_VERSION: &str = "v1alpha1";
pub const BOT_KIND: &str = "XenomitBot";

/// Feature flag + identity for the tracking stamps. `Default` is ON: the
/// annotations are inert without the matching Application, and the whole
/// point of the feature is that it needs no live Argo to be safe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArgoTracking {
    /// `false` disables stamping entirely (objects get no Argo metadata).
    pub enabled: bool,
    /// Application name the tracking-ids point at.
    pub app: String,
    /// Namespace stamped when `metadata.namespace` is unset (the API server
    /// fills the real one on write; in-memory stores don't).
    pub namespace: String,
}

impl Default for ArgoTracking {
    fn default() -> Self {
        ArgoTracking {
            enabled: true,
            app: DEFAULT_APP_NAME.into(),
            namespace: DEFAULT_BOTS_NAMESPACE.into(),
        }
    }
}

impl ArgoTracking {
    /// Tracking off — everything still works, bots are just invisible to
    /// the `xenomit-bots` tree (they remain in the project's orphaned list).
    pub fn disabled() -> Self {
        ArgoTracking {
            enabled: false,
            ..Default::default()
        }
    }

    /// `<app>:<group>/<kind>:<ns>/<name>` — core-group kinds render `/Kind`.
    pub fn tracking_id(&self, group: &str, kind: &str, namespace: &str, name: &str) -> String {
        format!("{}:{}/{}:{}/{}", self.app, group, kind, namespace, name)
    }

    /// The three-annotation set for one object.
    pub fn annotations(
        &self,
        group: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                TRACKING_ID.to_string(),
                self.tracking_id(group, kind, namespace, name),
            ),
            (
                COMPARE_OPTIONS.to_string(),
                COMPARE_IGNORE_EXTRANEOUS.to_string(),
            ),
            (
                SYNC_OPTIONS.to_string(),
                SYNC_NO_PRUNE_NO_DELETE.to_string(),
            ),
        ])
    }

    /// Merge the set into `meta.annotations` for the object's own GVK.
    /// Idempotent: our three keys are rewritten, foreign annotations kept.
    /// No-op when disabled or the object has no name yet.
    pub fn stamp(&self, meta: &mut ObjectMeta, group: &str, kind: &str) {
        if !self.enabled {
            return;
        }
        let Some(name) = meta.name.clone() else {
            return;
        };
        let ns = meta
            .namespace
            .clone()
            .unwrap_or_else(|| self.namespace.clone());
        let ann = meta.annotations.get_or_insert_with(BTreeMap::new);
        for (k, v) in self.annotations(group, kind, &ns, &name) {
            ann.insert(k, v);
        }
    }

    /// Stamp the `XenomitBot` CR itself (group `fleet.xenomit.io`).
    pub fn stamp_bot(&self, bot: &mut XenomitBot) {
        // Never overwrite Argo's tracking/prune metadata on a declaratively owned root.
        if bot.metadata.labels.as_ref().is_some_and(|labels| {
            labels
                .get("xenomit.io/spec-owner")
                .is_some_and(|owner| owner == "argocd")
        }) {
            return;
        }
        self.stamp(&mut bot.metadata, BOT_GROUP, BOT_KIND);
    }

    /// `ownerReference` from a child object to its bot. `controller` and
    /// `blockOwnerDeletion` are both true: the CR owns its children, k8s GC
    /// removes them when the bot is deleted, and Argo nests them under the
    /// CR in the app tree. (`blockOwnerDeletion` requires the operator SA
    /// to have `delete` on `xenomitbots` — it already must, to purge.)
    pub fn bot_owner_reference(&self, bot: &XenomitBot) -> Option<OwnerReference> {
        let uid = bot.metadata.uid.as_deref()?;
        Some(bot_owner_reference(
            bot.metadata.name.as_deref().unwrap_or_default(),
            uid,
        ))
    }

    /// `ownerReference` from the parts a `BotPodSpec` carries — `None`
    /// without a UID (in-memory repos never assign one; children are then
    /// stamped but not linked).
    pub fn owner_reference_for(&self, bot_id: &str, uid: Option<&str>) -> Option<OwnerReference> {
        uid.map(|uid| bot_owner_reference(bot_id, uid))
    }
}

/// Build the `XenomitBot` `ownerReference` — see
/// [`ArgoTracking::bot_owner_reference`].
pub fn bot_owner_reference(bot_id: &str, uid: &str) -> OwnerReference {
    OwnerReference {
        api_version: format!("{BOT_GROUP}/{BOT_VERSION}"),
        kind: BOT_KIND.into(),
        name: bot_id.into(),
        uid: uid.into(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fleet_types::crds::XenomitBotSpec;
    use kube::Resource;

    fn bot(name: &str, ns: Option<&str>, uid: Option<&str>) -> XenomitBot {
        let mut b = XenomitBot::new(
            name,
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
            },
        );
        b.metadata.namespace = ns.map(String::from);
        b.metadata.uid = uid.map(String::from);
        b
    }

    #[test]
    fn constants_match_the_crd_derive() {
        assert_eq!(<XenomitBot as Resource>::group(&()), BOT_GROUP);
        assert_eq!(<XenomitBot as Resource>::kind(&()), BOT_KIND);
        assert_eq!(<XenomitBot as Resource>::version(&()), BOT_VERSION);
    }

    #[test]
    fn tracking_id_matches_argo_format() {
        let t = ArgoTracking::default();
        // <app>:<group>/<kind>:<namespace>/<name>
        assert_eq!(
            t.tracking_id("fleet.xenomit.io", "XenomitBot", "xenomit-bots", "bot-1"),
            "xenomit-bots:fleet.xenomit.io/XenomitBot:xenomit-bots/bot-1"
        );
        // Core group renders as /Kind (docs example: "my-app:/Pod:…" style).
        assert_eq!(
            t.tracking_id("", "Pod", "xenomit-bots", "bot-1-e3"),
            "xenomit-bots:/Pod:xenomit-bots/bot-1-e3"
        );
    }

    #[test]
    fn gitops_root_keeps_argo_metadata() {
        let t = ArgoTracking::default();
        let mut b = bot("bot-1", None, None);
        b.metadata.labels = Some(BTreeMap::from([(
            "xenomit.io/spec-owner".into(),
            "argocd".into(),
        )]));
        b.metadata.annotations = Some(BTreeMap::from([(TRACKING_ID.into(), "git-owned".into())]));
        t.stamp_bot(&mut b);
        assert_eq!(
            b.metadata.annotations.as_ref().unwrap()[TRACKING_ID],
            "git-owned"
        );
        assert!(
            !b.metadata
                .annotations
                .as_ref()
                .unwrap()
                .contains_key(SYNC_OPTIONS)
        );
    }

    #[test]
    fn stamp_bot_writes_the_full_set() {
        let t = ArgoTracking::default();
        let mut b = bot("bot-1", None, None);
        t.stamp_bot(&mut b);
        let ann = b.metadata.annotations.unwrap();
        // Namespace falls back to the configured bots namespace when unset.
        assert_eq!(
            ann[TRACKING_ID],
            "xenomit-bots:fleet.xenomit.io/XenomitBot:xenomit-bots/bot-1"
        );
        assert_eq!(ann[COMPARE_OPTIONS], "IgnoreExtraneous");
        assert_eq!(ann[SYNC_OPTIONS], "Prune=false,Delete=false");
    }

    #[test]
    fn stamp_uses_real_namespace_when_present() {
        let t = ArgoTracking::default();
        let mut b = bot("bot-1", Some("other-ns"), None);
        t.stamp_bot(&mut b);
        let ann = b.metadata.annotations.unwrap();
        assert_eq!(
            ann[TRACKING_ID],
            "xenomit-bots:fleet.xenomit.io/XenomitBot:other-ns/bot-1"
        );
    }

    #[test]
    fn stamp_is_idempotent_and_preserves_foreign_annotations() {
        let t = ArgoTracking::default();
        let mut b = bot("bot-1", None, None);
        b.metadata.annotations = Some(BTreeMap::from([(
            "xenomit.io/note".to_string(),
            "keep-me".to_string(),
        )]));
        t.stamp_bot(&mut b);
        t.stamp_bot(&mut b); // second pass must not duplicate or corrupt
        let ann = b.metadata.annotations.unwrap();
        assert_eq!(ann.len(), 4);
        assert_eq!(ann["xenomit.io/note"], "keep-me");
        assert_eq!(
            ann[TRACKING_ID],
            t.tracking_id(BOT_GROUP, BOT_KIND, "xenomit-bots", "bot-1")
        );
    }

    #[test]
    fn disabled_writes_nothing() {
        let t = ArgoTracking::disabled();
        let mut b = bot("bot-1", None, None);
        t.stamp_bot(&mut b);
        assert!(b.metadata.annotations.is_none());
    }

    #[test]
    fn custom_app_and_namespace_are_honored() {
        let t = ArgoTracking {
            enabled: true,
            app: "fleet-view".into(),
            namespace: "bots-2".into(),
        };
        let mut b = bot("bot-1", None, None);
        t.stamp_bot(&mut b);
        let ann = b.metadata.annotations.unwrap();
        assert_eq!(
            ann[TRACKING_ID],
            "fleet-view:fleet.xenomit.io/XenomitBot:bots-2/bot-1"
        );
    }

    #[test]
    fn owner_reference_needs_a_uid() {
        let t = ArgoTracking::default();
        let without = bot("bot-1", None, None);
        assert!(t.bot_owner_reference(&without).is_none());
        assert!(t.owner_reference_for("bot-1", None).is_none());

        let with = bot("bot-1", None, Some("uid-123"));
        let r = t.bot_owner_reference(&with).unwrap();
        assert_eq!(r.api_version, "fleet.xenomit.io/v1alpha1");
        assert_eq!(r.kind, "XenomitBot");
        assert_eq!(r.name, "bot-1");
        assert_eq!(r.uid, "uid-123");
        assert_eq!(r.controller, Some(true));
        assert_eq!(r.block_owner_deletion, Some(true));
        assert_eq!(r, t.owner_reference_for("bot-1", Some("uid-123")).unwrap());
    }
}
