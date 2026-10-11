//! File-backed dev Vault: the exact HTTP surface
//! `xenomit_botd::secrets::VaultSecretResolver` speaks (JWT/k8s login,
//! `renew-self`, KV v2 reads) plus writes/deletes for seeding, backed by a
//! JSON file so a restarted daemon keeps its secrets.
//!
//! **This is a development stub, not an auth boundary.** Any non-empty
//! service-account JWT gets a client token and every client token reads
//! every path — there is no per-bot policy enforcement. Production runs
//! `VaultSecretStore` against real Vault, where the jwt mount validates SA
//! tokens and the `bot-<id>` policy scopes reads (docs/09 §9.2a).
//!
//! The exit-IP echo (`GET /ip`) rides the same listener so botd's proxy
//! check has a target without a second process.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use operator::traits::{BotSecretPaths, SecretError, SecretStore, SecretValue, bot_secret_paths};

/// The client token this dev Vault issues and accepts.
pub const CLIENT_TOKEN: &str = "dev-vault-client-token";
/// The service-account "JWT" contents dev botd children present. Any
/// non-empty value would be accepted — the constant exists so generated
/// token files are greppable and clearly fake.
pub const DEV_SA_JWT: &str = "dev-sa-jwt";

struct Kv {
    map: HashMap<String, Value>,
    file: PathBuf,
}

impl Kv {
    fn load(file: PathBuf) -> Self {
        let map = std::fs::read_to_string(&file)
            .ok()
            .and_then(|t| serde_json::from_str::<HashMap<String, Value>>(&t).ok())
            .unwrap_or_default();
        Kv { map, file }
    }

    fn persist(&self) {
        let tmp = self.file.with_extension("json.tmp");
        if let Ok(text) = serde_json::to_string(&self.map) {
            if std::fs::write(&tmp, &text).is_ok() {
                let _ = std::fs::rename(&tmp, &self.file);
            }
        }
    }
}

pub struct DevVault {
    kv: Arc<Mutex<Kv>>,
    addr: SocketAddr,
    serve: JoinHandle<()>,
}

impl DevVault {
    /// Bind `bind` (`127.0.0.1:8200` in dev), load `kv_file` if it exists.
    pub async fn start(bind: SocketAddr, kv_file: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = kv_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let kv = Arc::new(Mutex::new(Kv::load(kv_file)));
        let app = Router::new()
            .route("/v1/auth/{mount}/login", post(login))
            .route("/v1/auth/token/renew-self", post(renew))
            .route("/v1/{*path}", get(read).post(write).delete(remove))
            .route("/ip", get(exit_ip))
            .with_state(kv.clone());
        let listener = TcpListener::bind(bind).await?;
        let addr = listener.local_addr()?;
        let serve = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(DevVault { kv, addr, serve })
    }

    /// `http://{addr}` — `XENOMIT_VAULT_ADDR` for botd children.
    pub fn addr(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// `XENOMIT_EXIT_IP_URL` — botd fetches it *through* its proxy.
    pub fn echo_url(&self) -> String {
        format!("http://{}/ip", self.addr)
    }

    pub fn abort(&self) {
        self.serve.abort();
    }
}

#[async_trait]
impl SecretStore for DevVault {
    async fn provision_bot(&self, bot_id: &str) -> Result<BotSecretPaths, SecretError> {
        let paths = bot_secret_paths(bot_id);
        // botd resolves the control token at Arm — it must exist before
        // any `write` lands. `instance` is deliberately NOT seeded: an
        // absent path is the operator's signal that no panel pairing
        // happened (the compat create writes the real token itself).
        let mut kv = self.kv.lock().unwrap();
        let missing = !kv.map.contains_key(&paths.control_token_path);
        kv.map
            .entry(paths.control_token_path.clone())
            .or_insert_with(|| json!({"token": format!("ctrl-{bot_id}")}));
        if missing {
            kv.persist();
        }
        Ok(paths)
    }

    async fn write(&self, path: &str, value: SecretValue) -> Result<(), SecretError> {
        let bytes = value.as_bytes();
        let data: Value = serde_json::from_slice(bytes)
            .unwrap_or_else(|_| json!({"value": String::from_utf8_lossy(bytes)}));
        let mut kv = self.kv.lock().unwrap();
        kv.map.insert(path.into(), data);
        kv.persist();
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool, SecretError> {
        Ok(self.kv.lock().unwrap().map.contains_key(path))
    }

    async fn delete_path(&self, path: &str) -> Result<bool, SecretError> {
        let mut kv = self.kv.lock().unwrap();
        let had = kv.map.remove(path).is_some();
        if had {
            kv.persist();
        }
        Ok(had)
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), SecretError> {
        let mut kv = self.kv.lock().unwrap();
        let Some(value) = kv.map.get(from).cloned() else {
            return Err(SecretError::Other(format!("no secret at {from}")));
        };
        kv.map.insert(to.to_string(), value);
        kv.persist();
        Ok(())
    }

    async fn destroy_bot(&self, bot_id: &str) -> Result<(), SecretError> {
        let prefix = format!("secret/data/bots/{bot_id}");
        let mut kv = self.kv.lock().unwrap();
        kv.map.retain(|k, _| !k.starts_with(&prefix));
        kv.persist();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HTTP handlers — Vault wire shapes only; no real auth (dev stub).
// ---------------------------------------------------------------------------

fn authed(headers: &HeaderMap) -> bool {
    headers.get("x-vault-token").and_then(|v| v.to_str().ok()) == Some(CLIENT_TOKEN)
}

fn denied(msg: &str) -> (StatusCode, axum::Json<Value>) {
    (StatusCode::FORBIDDEN, axum::Json(json!({"errors": [msg]})))
}

async fn login(axum::Json(body): axum::Json<Value>) -> impl IntoResponse {
    // Dev stub: any non-empty jwt is a valid SA token (see module docs).
    let ok = body
        .get("jwt")
        .and_then(Value::as_str)
        .is_some_and(|j| !j.trim().is_empty());
    if !ok {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(json!({"errors": ["missing service-account jwt"]})),
        );
    }
    (
        StatusCode::OK,
        axum::Json(json!({
            "auth": {
                "client_token": CLIENT_TOKEN,
                "lease_duration": 3600,
                "renewable": true,
            }
        })),
    )
}

async fn renew(headers: HeaderMap) -> impl IntoResponse {
    if !authed(&headers) {
        return denied("bad client token");
    }
    (
        StatusCode::OK,
        axum::Json(json!({"auth": {"lease_duration": 3600, "renewable": true}})),
    )
}

async fn read(
    State(kv): State<Arc<Mutex<Kv>>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !authed(&headers) {
        return denied("bad client token");
    }
    match kv.lock().unwrap().map.get(&path) {
        Some(data) => (
            StatusCode::OK,
            axum::Json(json!({
                "request_id": "dev",
                "data": {
                    "data": data,
                    "metadata": {"version": 1},
                },
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"errors": ["no secret at this path"]})),
        ),
    }
}

/// KV v2 write (`POST /v1/secret/data/<rel>` with `{"data": {…}}`) — lets
/// dev tooling and ops scripts seed proxy/bot secrets over HTTP.
async fn write(
    State(kv): State<Arc<Mutex<Kv>>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<Value>,
) -> impl IntoResponse {
    if !authed(&headers) {
        return denied("bad client token");
    }
    let data = body.get("data").cloned().unwrap_or(body);
    let mut kv = kv.lock().unwrap();
    kv.map.insert(path, data);
    kv.persist();
    (StatusCode::OK, axum::Json(json!({"data": {"version": 1}})))
}

async fn remove(
    State(kv): State<Arc<Mutex<Kv>>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !authed(&headers) {
        return denied("bad client token");
    }
    let mut kv = kv.lock().unwrap();
    let had = kv.map.remove(&path).is_some();
    if had {
        kv.persist();
    }
    (
        if had {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::NOT_FOUND
        },
        axum::Json(json!({})),
    )
}

/// Exit-IP echo — plain text, like api.ipify.org. Reached via the proxy.
async fn exit_ip() -> &'static str {
    "127.0.0.1"
}
