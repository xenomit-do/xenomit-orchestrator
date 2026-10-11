//! `SecretStore` over real Vault (kube mode): JWT auth-mount login with
//! the orchestrator's own projected SA token, KV v2 writes/metadata
//! reads/deletes, and the per-bot policy + jwt role + ServiceAccount
//! lifecycle from docs/09 §9.2a.
//!
//! Mirrors `ops/vault/vault-configure-jwt.ps1` + `ops/vault/policies/*.hcl`:
//! - mount `jwt`, `bound_audiences=vault`, `user_claim=sub`
//! - orchestrator policy may write `secret/data/bots/*` and read only
//!   `secret/metadata/bots/*` — `exists` therefore queries metadata
//! - bot policies/roles are named `bot-<id>`; the SA lives in the bots
//!   namespace and is projected with audience `vault`
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::ServiceAccount;
use kube::Client;
use kube::api::{Api, DeleteParams, PostParams};
use serde_json::{Value, json};
use zeroize::Zeroizing;

use operator::traits::{BotSecretPaths, SecretError, SecretStore, SecretValue, bot_secret_paths};

/// Per-bot Vault policy — `ops/vault/policies/bot.hcl.tpl` with `{{BOT_ID}}`
/// rendered. Kept in-tree so provisioning needs no file mounts.
const BOT_POLICY_TPL: &str = r#"path "secret/data/bots/{{BOT_ID}}/*" {
  capabilities = ["read"]
}

path "secret/metadata/bots/{{BOT_ID}}/*" {
  capabilities = ["read", "list"]
}
"#;

/// Cached client token + when to re-login (a slice of the granted lease).
struct CachedToken {
    value: Zeroizing<String>,
    refresh_at: Instant,
}

/// Real-Vault `SecretStore`. The kube client is used only for the
/// ServiceAccount half of `provision_bot`/`destroy_bot`.
pub struct VaultSecretStore {
    http: reqwest::Client,
    addr: String,
    mount: String,
    role: String,
    token_file: PathBuf,
    token: Mutex<Option<CachedToken>>,
    kube: Option<Client>,
    /// Namespace bot ServiceAccounts live in (`xenomit-bots`).
    bots_ns: String,
}

impl VaultSecretStore {
    pub fn new(
        addr: impl Into<String>,
        mount: impl Into<String>,
        role: impl Into<String>,
        token_file: PathBuf,
        kube: Option<Client>,
        bots_ns: impl Into<String>,
    ) -> Self {
        VaultSecretStore {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            addr: addr.into().trim_end_matches('/').to_string(),
            mount: mount.into(),
            role: role.into(),
            token_file,
            token: Mutex::new(None),
            kube,
            bots_ns: bots_ns.into(),
        }
    }

    /// Login with the projected SA token; cached until ~80 % of the lease.
    async fn client_token(&self) -> Result<Zeroizing<String>, SecretError> {
        if let Some(t) = self.token.lock().unwrap().as_ref()
            && Instant::now() < t.refresh_at
        {
            return Ok(t.value.clone());
        }
        let jwt = std::fs::read_to_string(&self.token_file).map_err(|e| {
            SecretError::Unavailable(format!(
                "vault token file {} unreadable: {e}",
                self.token_file.display()
            ))
        })?;
        let url = format!("{}/v1/auth/{}/login", self.addr, self.mount);
        let resp = self
            .http
            .post(&url)
            .json(&json!({"jwt": jwt.trim(), "role": self.role}))
            .send()
            .await
            .map_err(|e| SecretError::Unavailable(format!("vault auth unreachable: {e}")))?;
        if !resp.status().is_success() {
            return Err(self.classify(resp.status(), "vault login failed"));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| SecretError::Other(format!("vault login malformed: {e}")))?;
        let token = body
            .pointer("/auth/client_token")
            .and_then(Value::as_str)
            .ok_or_else(|| SecretError::Other("vault login returned no client_token".into()))?;
        let lease = body
            .pointer("/auth/lease_duration")
            .and_then(Value::as_u64)
            .unwrap_or(300);
        *self.token.lock().unwrap() = Some(CachedToken {
            value: Zeroizing::new(token.to_string()),
            refresh_at: Instant::now() + Duration::from_secs((lease * 4 / 5).max(1)),
        });
        Ok(Zeroizing::new(token.to_string()))
    }

    /// 5xx/transport → `Unavailable` (the controller keeps the bot Pending
    /// and retries); the rest → `Other`.
    fn classify(&self, status: reqwest::StatusCode, what: &str) -> SecretError {
        if status.is_server_error() {
            SecretError::Unavailable(format!("{what} ({status})"))
        } else {
            SecretError::Other(format!("{what} ({status})"))
        }
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<reqwest::Response, SecretError> {
        let token = self.client_token().await?;
        let url = format!("{}/v1/{}", self.addr, path.trim_start_matches('/'));
        let mut req = self
            .http
            .request(method, &url)
            .header("X-Vault-Token", token.as_str());
        if let Some(b) = body {
            req = req.json(&b);
        }
        req.send()
            .await
            .map_err(|e| SecretError::Unavailable(format!("vault unreachable: {e}")))
    }

    /// `secret/data/bots/x` → `secret/metadata/bots/x` (orchestrator policy
    /// reads metadata, never data — docs/09 §9.1).
    fn metadata_path(path: &str) -> String {
        path.replacen("secret/data/", "secret/metadata/", 1)
    }

    async fn kv_put(&self, path: &str, data: Value) -> Result<(), SecretError> {
        let resp = self
            .request(reqwest::Method::POST, path, Some(json!({"data": data})))
            .await?;
        if !resp.status().is_success() {
            return Err(self.classify(resp.status(), &format!("vault write {path}")));
        }
        Ok(())
    }

    /// KV v2 read helper (`copy` needs the source value; the policy grants
    /// `secret/data/proxies/*` reads for exactly this).
    async fn kv_get(&self, path: &str) -> Result<Value, SecretError> {
        let resp = self.request(reqwest::Method::GET, path, None).await?;
        match resp.status() {
            s if s.is_success() => {
                let body: Value = resp
                    .json()
                    .await
                    .map_err(|e| SecretError::Other(format!("vault read {path}: {e}")))?;
                body.pointer("/data/data")
                    .cloned()
                    .ok_or_else(|| SecretError::Other(format!("vault {path} has no data")))
            }
            reqwest::StatusCode::NOT_FOUND => {
                Err(SecretError::Other(format!("no secret at {path}")))
            }
            s => Err(self.classify(s, &format!("vault read {path}"))),
        }
    }

    async fn ensure_service_account(&self, name: &str) -> Result<(), SecretError> {
        let Some(client) = &self.kube else {
            return Ok(());
        };
        let sas: Api<ServiceAccount> = Api::namespaced(client.clone(), &self.bots_ns);
        let sa = ServiceAccount {
            metadata: kube::core::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(self.bots_ns.clone()),
                ..Default::default()
            },
            ..Default::default()
        };
        match sas.create(&PostParams::default(), &sa).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 409 => Ok(()),
            Err(e) => Err(SecretError::Other(format!("serviceaccount {name}: {e}"))),
        }
    }
}

#[async_trait]
impl SecretStore for VaultSecretStore {
    async fn provision_bot(&self, bot_id: &str) -> Result<BotSecretPaths, SecretError> {
        let paths = bot_secret_paths(bot_id);

        // 1. The ServiceAccount the pod authenticates as (audience `vault`).
        self.ensure_service_account(&paths.service_account).await?;

        // 2. Vault policy `bot-<id>` (read own subtree only).
        let policy = BOT_POLICY_TPL.replace("{{BOT_ID}}", bot_id);
        let resp = self
            .request(
                reqwest::Method::PUT,
                &format!("sys/policies/acl/{}", paths.policy_name),
                Some(json!({"policy": policy})),
            )
            .await?;
        if !resp.status().is_success() {
            return Err(self.classify(resp.status(), "vault policy write"));
        }

        // 3. JWT role `bot-<id>` bound to that SA (audience `vault`).
        let bound_sub = format!(
            "system:serviceaccount:{}:{}",
            self.bots_ns, paths.service_account
        );
        let resp = self
            .request(
                reqwest::Method::POST,
                &format!("auth/{}/role/{}", self.mount, paths.role_name),
                Some(json!({
                    "role_type": "jwt",
                    "bound_subject": bound_sub,
                    "bound_audiences": "vault",
                    "user_claim": "sub",
                    "token_policies": paths.policy_name,
                    "token_ttl": "1h",
                })),
            )
            .await?;
        if !resp.status().is_success() {
            return Err(self.classify(resp.status(), "vault role write"));
        }

        // 4. Seed the control bearer botd resolves at Arm. `instance` is
        //    deliberately not seeded — an absent path is the operator's
        //    signal that no panel pairing happened (the compat create
        //    writes the real token itself).
        if !self.exists(&paths.control_token_path).await? {
            let token = format!("ctrl-{:016x}", rand::random::<u64>());
            self.write(
                &paths.control_token_path,
                SecretValue::new(json!({"token": token}).to_string()),
            )
            .await?;
        }
        Ok(paths)
    }

    async fn write(&self, path: &str, value: SecretValue) -> Result<(), SecretError> {
        let bytes = value.as_bytes();
        let data: Value = serde_json::from_slice(bytes)
            .unwrap_or_else(|_| json!({"value": String::from_utf8_lossy(bytes)}));
        self.kv_put(path, data).await
    }

    async fn exists(&self, path: &str) -> Result<bool, SecretError> {
        let resp = self
            .request(reqwest::Method::GET, &Self::metadata_path(path), None)
            .await?;
        match resp.status() {
            s if s.is_success() => Ok(true),
            reqwest::StatusCode::NOT_FOUND => Ok(false),
            s => Err(self.classify(s, &format!("vault metadata {path}"))),
        }
    }

    async fn delete_path(&self, path: &str) -> Result<bool, SecretError> {
        // Metadata delete destroys all versions — the write-then-delete
        // pattern (rotate, undelete) needs the value truly gone.
        let resp = self
            .request(reqwest::Method::DELETE, &Self::metadata_path(path), None)
            .await?;
        match resp.status() {
            s if s.is_success() => Ok(true),
            reqwest::StatusCode::NOT_FOUND => Ok(false),
            s => Err(self.classify(s, &format!("vault delete {path}"))),
        }
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), SecretError> {
        let data = self.kv_get(from).await?;
        self.kv_put(to, data).await
    }

    async fn destroy_bot(&self, bot_id: &str) -> Result<(), SecretError> {
        let paths = bot_secret_paths(bot_id);
        // KV subtree: LIST the metadata prefix, delete each key.
        let meta_prefix = Self::metadata_path(&paths.prefix);
        let resp = self
            .request(
                reqwest::Method::GET,
                &format!("{meta_prefix}?list=true"),
                None,
            )
            .await?;
        if resp.status().is_success() {
            let body: Value = resp.json().await.unwrap_or_default();
            if let Some(keys) = body.pointer("/data/keys").and_then(Value::as_array) {
                for key in keys.iter().filter_map(Value::as_str) {
                    let _ = self.delete_path(&format!("{}/{key}", paths.prefix)).await;
                }
            }
        }
        // JWT role + ACL policy (docs/09 §9.2 deletion ordering — the bot
        // loses its credentials before the CR is marked).
        for path in [
            format!("auth/{}/role/{}", self.mount, paths.role_name),
            format!("sys/policies/acl/{}", paths.policy_name),
        ] {
            let resp = self.request(reqwest::Method::DELETE, &path, None).await?;
            if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
                return Err(self.classify(resp.status(), &format!("vault destroy {path}")));
            }
        }
        // ServiceAccount — the pod can't re-authenticate after teardown.
        if let Some(client) = &self.kube {
            let sas: Api<ServiceAccount> = Api::namespaced(client.clone(), &self.bots_ns);
            match sas
                .delete(&paths.service_account, &DeleteParams::default())
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(ae)) if ae.code == 404 => {}
                Err(e) => {
                    return Err(SecretError::Other(format!(
                        "serviceaccount {}: {e}",
                        paths.service_account
                    )));
                }
            }
        }
        Ok(())
    }
}
