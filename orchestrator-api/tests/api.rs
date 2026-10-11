//! Offline API tests: the axum router over tower::oneshot with in-memory
//! repos, fakes and a real Ed25519-signed JWT. No sockets, no services.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{Value, json};
use tower::ServiceExt;

use control_gateway::MemorySnapshotStore;
use fleet_types::crds::{Proxy, ProxyScheme, ProxySpec, ProxyStatus, XenomitBot};
use operator::fakes::{FakeControl, FakePods};
use operator::traits::{Clock, ManualClock};
use operator::{BotController, BotLocks, MemorySecretStore, Pool};
use orchestrator_api::{AppState, AuthConfig, Claims, Fleet, InMemoryBots, InMemoryCrs};
use plans::PhaseTimelines;

struct Rig {
    app: axum::Router,
    enc: EncodingKey,
    pods: Arc<FakePods>,
    secrets: Arc<MemorySecretStore>,
    fleet: Arc<AppState>,
    /// The one store behind both `Fleet.proxies` and the controller's
    /// `Pool` — POST /proxies is immediately visible to assignment.
    proxies: Arc<InMemoryCrs>,
    control: Arc<FakeControl>,
    clock: Arc<ManualClock>,
}

fn mk_proxy(name: &str, ip: &str, capacity: u32) -> Proxy {
    let mut p = Proxy::new(
        name,
        ProxySpec {
            scheme: ProxyScheme::Socks5,
            host: ip.into(),
            port: 1080,
            tags: vec![],
            country: Some("DE".into()),
            isp: None,
            capacity,
            credentials_vault_path: format!("secret/data/proxies/{name}"),
        },
    );
    p.status = Some(ProxyStatus {
        healthy: true,
        last_probe_at_unix_ms: Some(1),
        resolved_ips: vec![ip.into()],
        exit_ip: Some("198.51.100.7".into()),
        ..Default::default()
    });
    p
}

fn rig() -> Rig {
    rig_with(None, None)
}

fn rig_with(
    plans: Option<Arc<plans::InMemoryPlans>>,
    usage: Option<Arc<plans::InMemoryUsage>>,
) -> Rig {
    let sk = SigningKey::from_bytes(&[42u8; 32]);
    let private_der = sk.to_pkcs8_der().unwrap();
    let public_raw = sk.verifying_key().to_bytes();

    let pods = Arc::new(FakePods::new());
    let secrets = Arc::new(MemorySecretStore::new());
    let snapshots = Arc::new(MemorySnapshotStore::new());
    let control = Arc::new(FakeControl::new());
    let clock = Arc::new(ManualClock::new(1_700_000_000_000));
    let proxies = Arc::new(InMemoryCrs::new("proxy"));
    // Healthy pool proxies so created bots get an assignment — without
    // one the require_proxy gate parks every bot in Provisioning.
    proxies.seed_proxy(&mk_proxy("px-a", "203.0.113.10", 50));
    proxies.seed_proxy(&mk_proxy("px-b", "203.0.113.11", 50));
    let pool = Pool::new(proxies.clone());
    let ctl = BotController::new(
        pods.clone(),
        secrets.clone(),
        snapshots.clone(),
        control.clone(),
        clock.clone(),
        pool,
    );
    let (events, _) = tokio::sync::broadcast::channel(64);
    let fleet = Fleet {
        bots: Arc::new(InMemoryBots::new()),
        rollouts: Arc::new(InMemoryCrs::new("botrollout")),
        proxies: proxies.clone(),
        secrets: secrets.clone(),
        snapshots,
        ops: Arc::new(ctl),
        locks: Arc::new(BotLocks::new()),
        events,
        clock: clock.clone(),
        plans: plans.map(|p| p as Arc<dyn plans::PlanDirectory>),
        usage: usage.map(|u| u as Arc<dyn plans::UsageStore>),
    };
    let auth = AuthConfig::from_ed_der(&public_raw).unwrap();
    let state = Arc::new(AppState::new(fleet, auth));
    Rig {
        app: orchestrator_api::router(state.clone()),
        enc: EncodingKey::from_ed_der(private_der.as_bytes()),
        pods,
        secrets,
        fleet: state,
        proxies,
        control,
        clock,
    }
}

fn token(enc: &EncodingKey, sub: &str, roles: &[&str]) -> String {
    let claims = Claims {
        sub: sub.into(),
        exp: 4_000_000_000,
        iat: None,
        roles: roles.iter().map(|s| s.to_string()).collect(),
        role: None,
        sid: None,
    };
    jsonwebtoken::encode(&Header::new(Algorithm::EdDSA), &claims, enc).unwrap()
}

fn req(method: &str, uri: &str, token: Option<&str>, body: Option<Value>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    match body {
        Some(v) => b
            .header("content-type", "application/json")
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => b.body(Body::empty()).unwrap(),
    }
}

async fn json_body(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn create_body(name: &str, owner: &str) -> Value {
    json!({
        "name": name,
        "spec": {
            "owner": owner,
            "desiredState": "running",
            "account": {
                "server": "int1.darkorbit.com",
                "authMode": "password",
                "credentialsVaultPath": "placeholder"
            },
            "config": { "settings": {"activeModule": "kill_collect"} }
        },
        "credentials": { "username": "u1", "password": "p1" }
    })
}

async fn bot_at(rig: &Rig, name: &str) -> XenomitBot {
    rig.fleet.fleet.bots.get(name).await.unwrap()
}

#[tokio::test]
async fn healthz_is_public() {
    let rig = rig();
    let resp = rig
        .app
        .clone()
        .oneshot(req("GET", "/healthz", None, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn api_requires_jwt() {
    let rig = rig();
    let resp = rig
        .app
        .clone()
        .oneshot(req("GET", "/api/v1/bots", None, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Garbage token rejected too.
    let resp = rig
        .app
        .clone()
        .oneshot(req("GET", "/api/v1/bots", Some("not.a.jwt"), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn create_vaults_credentials_and_provisions() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("b1", "mallory")),
        ))
        .await
        .unwrap();
    let (status, body) = json_body(resp).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Owner is forced to the JWT subject for non-admins.
    assert_eq!(body["spec"]["owner"], "alice");
    // Credentials went to Vault, not the CR.
    assert_eq!(
        body["spec"]["account"]["credentialsVaultPath"],
        "secret/data/bots/b1/account"
    );
    assert!(body.to_string().find("p1").is_none());
    assert!(rig.secrets.has_value("secret/data/bots/b1/account"));

    // One reconcile was driven: secrets provisioned, pod created.
    let bot = bot_at(&rig, "b1").await;
    assert_eq!(bot.status.unwrap().epoch.get(), 1);
    assert_eq!(rig.pods.count("b1"), 1);
}

#[tokio::test]
async fn create_conflict_and_bad_name() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("b1", "alice")),
        ))
        .await
        .unwrap();
    // Duplicate → 409.
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("b1", "alice")),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    // Bad name → 400.
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("BAD_NAME", "alice")),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn tenant_isolation_and_admin() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    let bob = token(&rig.enc, "bob", &[]);
    let admin = token(&rig.enc, "root", &["admin"]);

    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("a1", "alice")),
        ))
        .await
        .unwrap();

    // Bob cannot see or touch alice's bot (404, not 403 — no existence leak).
    for (m, u) in [
        ("GET", "/api/v1/bots/a1"),
        ("POST", "/api/v1/bots/a1/pause"),
        ("DELETE", "/api/v1/bots/a1"),
    ] {
        let resp = rig
            .app
            .clone()
            .oneshot(req(m, u, Some(&bob), None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{m} {u}");
    }
    // Bob's list is empty; admin sees everything.
    let (_, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("GET", "/api/v1/bots", Some(&bob), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["items"].as_array().unwrap().len(), 0);
    let (_, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("GET", "/api/v1/bots", Some(&admin), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn pause_resume_and_stop_flow() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("p1", "alice")),
        ))
        .await
        .unwrap();

    let (_, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("POST", "/api/v1/bots/p1/pause", Some(&alice), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["spec"]["desiredState"], "paused");

    let (_, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("POST", "/api/v1/bots/p1/resume", Some(&alice), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["spec"]["desiredState"], "running");

    let (_, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("POST", "/api/v1/bots/p1/stop", Some(&alice), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["spec"]["desiredState"], "stopped");
}

#[tokio::test]
async fn delete_destroys_vault_then_marks() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("d1", "alice")),
        ))
        .await
        .unwrap();
    assert!(rig.secrets.has_value("secret/data/bots/d1/account"));

    let resp = rig
        .app
        .clone()
        .oneshot(req("DELETE", "/api/v1/bots/d1", Some(&alice), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    assert!(rig.secrets.is_destroyed("d1"));
    assert!(!rig.secrets.has_value("secret/data/bots/d1/account"));
    let bot = bot_at(&rig, "d1").await;
    assert!(bot.status.unwrap().deleted_at_unix_ms.is_some());

    // Undelete brings the history back but demands re-entered credentials.
    let (_, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("POST", "/api/v1/bots/d1/undelete", Some(&alice), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["status"]["phase"], "needsHuman");
    assert_eq!(body["status"]["needsHuman"], "reenterCredentials");
}

#[tokio::test]
async fn credential_rotation_rearms_needs_human() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("c1", "alice")),
        ))
        .await
        .unwrap();

    // Drive to a NeedsHuman state by simulating a credential rejection.
    {
        let mut bot = bot_at(&rig, "c1").await;
        bot.status.as_mut().unwrap().phase = fleet_types::phase::BotPhase::NeedsHuman;
        rig.fleet.fleet.bots.apply(bot).await.unwrap();
    }

    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots/c1/credentials",
            Some(&alice),
            Some(json!({
                "credentials": {"username": "u2", "password": "p2"},
                "rearm": true
            })),
        ))
        .await
        .unwrap();
    let (status, body) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Rearm consumed: no pending request, phase moved off NeedsHuman.
    assert_ne!(body["status"]["phase"], "needsHuman");
    assert!(body.to_string().find("p2").is_none());
}

#[tokio::test]
async fn openapi_document_is_served() {
    let rig = rig();
    let resp = rig
        .app
        .clone()
        .oneshot(req("GET", "/api/v1/openapi.json", None, None))
        .await
        .unwrap();
    let (status, body) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["openapi"], "3.1.0");
    for p in ["/bots", "/bots/{name}", "/events", "/fleet", "/proxies"] {
        assert!(body["paths"].get(p).is_some(), "missing path {p}");
    }
}

// ---------------------------------------------------------------------------
// Proxy pool endpoints (docs/05 §5.4, §5.6)
// ---------------------------------------------------------------------------

fn proxy_body(name: &str, host: &str) -> Value {
    json!({
        "name": name,
        "spec": {
            "scheme": "socks5",
            "host": host,
            "port": 1080,
            "capacity": 5,
            "country": "DE",
            // Callers may not point a proxy at another secret — the
            // server rewrites this to secret/data/proxies/<name>.
            "credentialsVaultPath": "secret/data/bots/b1/account"
        },
        "credentials": {"username": "pxuser", "password": "pxpass"}
    })
}

#[tokio::test]
async fn proxy_crud_admin_only_and_credentials_vaulted() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    let admin = token(&rig.enc, "root", &["admin"]);

    // Mutations are admin-only.
    for (m, u) in [
        ("POST", "/api/v1/proxies"),
        ("PUT", "/api/v1/proxies/px-x"),
        ("DELETE", "/api/v1/proxies/px-x"),
        ("GET", "/api/v1/proxies/pool"),
    ] {
        let body = if m == "GET" {
            None
        } else {
            Some(proxy_body("px-x", "203.0.113.55"))
        };
        let resp = rig
            .app
            .clone()
            .oneshot(req(m, u, Some(&alice), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{m} {u}");
    }

    // Admin create: 201, vault path rewritten, credentials vaulted.
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/proxies",
            Some(&admin),
            Some(proxy_body("px-x", "203.0.113.55")),
        ))
        .await
        .unwrap();
    let (status, body) = json_body(resp).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["spec"]["credentialsVaultPath"],
        "secret/data/proxies/px-x"
    );
    assert!(body.to_string().find("pxpass").is_none());
    assert!(rig.secrets.has_value("secret/data/proxies/px-x"));

    // Read back.
    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("GET", "/api/v1/proxies/px-x", Some(&admin), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["spec"]["host"], "203.0.113.55");

    // Update preserves status and rewrites the path again.
    let mut upd = proxy_body("px-x", "203.0.113.66");
    upd["credentials"] = Value::Null;
    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("PUT", "/api/v1/proxies/px-x", Some(&admin), Some(upd)))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["host"], "203.0.113.66");
    assert_eq!(
        body["spec"]["credentialsVaultPath"],
        "secret/data/proxies/px-x"
    );

    // Delete: CR + Vault entry go together.
    let resp = rig
        .app
        .clone()
        .oneshot(req("DELETE", "/api/v1/proxies/px-x", Some(&admin), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(!rig.secrets.has_value("secret/data/proxies/px-x"));
    let resp = rig
        .app
        .clone()
        .oneshot(req("DELETE", "/api/v1/proxies/px-x", Some(&admin), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn proxy_create_conflict() {
    let rig = rig();
    let admin = token(&rig.enc, "root", &["admin"]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/proxies",
            Some(&admin),
            Some(proxy_body("px-dup", "203.0.113.56")),
        ))
        .await
        .unwrap();
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/proxies",
            Some(&admin),
            Some(proxy_body("px-dup", "203.0.113.57")),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn proxy_delete_refused_while_assigned() {
    let rig = rig();
    let admin = token(&rig.enc, "root", &["admin"]);
    // Seed a proxy with a live claim: deletion must be refused (docs/05
    // §5.4 — never strand a running bot).
    let mut busy = mk_proxy("px-busy", "203.0.113.60", 5);
    busy.status.as_mut().unwrap().assigned = vec!["bot-x".into()];
    rig.proxies.seed_proxy(&busy);

    let resp = rig
        .app
        .clone()
        .oneshot(req("DELETE", "/api/v1/proxies/px-busy", Some(&admin), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn pool_status_aggregates() {
    let rig = rig();
    let admin = token(&rig.enc, "root", &["admin"]);
    let mut p = orchestrator_api::CrRepo::get(rig.proxies.as_ref(), "px-a")
        .await
        .unwrap();
    p["status"]["assigned"] = json!(["bot-1", "bot-2"]);
    orchestrator_api::CrRepo::apply(rig.proxies.as_ref(), p)
        .await
        .unwrap();

    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("GET", "/api/v1/proxies/pool", Some(&admin), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 2);
    assert_eq!(body["healthy"], 2);
    assert_eq!(body["capacity"], 100);
    assert_eq!(body["assigned"], 2);
    let names: Vec<&str> = body["proxies"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["px-a", "px-b"]);
    // Per-proxy details — no credential fields anywhere.
    assert!(body.to_string().find("password").is_none());
}

#[tokio::test]
async fn fixed_proxy_bot_wires_vault_path() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    let mut body = create_body("fx1", "alice");
    body["spec"]["proxy"] = json!({"mode": "fixed"});
    body["proxy"] = json!({"addr": "192.0.2.44:3128", "username": "fu", "password": "fp"});

    let resp = rig
        .app
        .clone()
        .oneshot(req("POST", "/api/v1/bots", Some(&alice), Some(body)))
        .await
        .unwrap();
    let (status, body) = json_body(resp).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    // Address-only copy on the CR; credentials in Vault only.
    assert_eq!(body["spec"]["proxy"]["fixedAddr"], "192.0.2.44:3128");
    assert_eq!(
        body["spec"]["proxy"]["fixedVaultPath"],
        "secret/data/bots/fx1/proxy"
    );
    assert!(body.to_string().find("fp").is_none());
    assert!(rig.secrets.has_value("secret/data/bots/fx1/proxy"));
}

#[tokio::test]
async fn fixed_proxy_validation_errors() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);

    // mode=fixed with no credentials and no vault path → 400.
    let mut b = create_body("fx2", "alice");
    b["spec"]["proxy"] = json!({"mode": "fixed"});
    let resp = rig
        .app
        .clone()
        .oneshot(req("POST", "/api/v1/bots", Some(&alice), Some(b)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Unparsable fixedAddr → 400.
    let mut b = create_body("fx3", "alice");
    b["spec"]["proxy"] = json!({"mode": "fixed", "fixedAddr": "not-an-addr"});
    let resp = rig
        .app
        .clone()
        .oneshot(req("POST", "/api/v1/bots", Some(&alice), Some(b)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Proxy credentials on a pool-mode bot → 400.
    let mut b = create_body("fx4", "alice");
    b["proxy"] = json!({"addr": "192.0.2.44:3128"});
    let resp = rig
        .app
        .clone()
        .oneshot(req("POST", "/api/v1/bots", Some(&alice), Some(b)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn fleet_summary_counts() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("s1", "alice")),
        ))
        .await
        .unwrap();
    {
        let mut bot = bot_at(&rig, "s1").await;
        bot.status.as_mut().unwrap().phase = fleet_types::phase::BotPhase::Running;
        rig.fleet.fleet.bots.apply(bot).await.unwrap();
    }
    let (_, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("GET", "/api/v1/fleet", Some(&alice), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["botsTotal"], 1);
    assert_eq!(body["running"], 1);
}

// ---------------------------------------------------------------------------
// Plans / quotas / retention / usage (docs/12)
// ---------------------------------------------------------------------------

fn mk_plan(
    id: &str,
    max_bots: Option<u32>,
    channels: &[fleet_types::crds::VersionChannel],
) -> plans::Plan {
    plans::Plan {
        id: id.into(),
        name: id.into(),
        active: true,
        max_bots,
        max_running_bots: max_bots,
        proxy_modes: vec![
            fleet_types::crds::ProxyMode::Pool,
            fleet_types::crds::ProxyMode::Fixed,
        ],
        pool_access: true,
        channels: channels.to_vec(),
        rollout_priority: 0,
        handoff_priority: 0,
        retention_days_after_delete: 30,
        price_cents: None,
        interval: None,
    }
}

fn plans_for(user: &str, plan: &str) -> Arc<plans::InMemoryPlans> {
    let dir = Arc::new(plans::InMemoryPlans::seeded());
    dir.upsert_plan(mk_plan(
        "tiny",
        Some(1),
        &[fleet_types::crds::VersionChannel::Stable],
    ));
    dir.set_user(plans::UserAccess {
        user_id: user.into(),
        plan_id: Some(plan.into()),
        status: plans::UserStatus::Active,
    });
    dir
}

#[tokio::test]
async fn quota_max_bots_and_no_plan_and_suspended() {
    let dir = plans_for("alice", "tiny");
    let rig = rig_with(Some(dir.clone()), None);
    let alice = token(&rig.enc, "alice", &[]);
    let bob = token(&rig.enc, "bob", &[]);

    // First bot fits the plan.
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("q1", "alice")),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Second exceeds maxBots=1 → typed 403.
    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req(
                "POST",
                "/api/v1/bots",
                Some(&alice),
                Some(create_body("q2", "alice")),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "max_bots_exceeded");

    // A user with no plan at all → no_plan.
    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req(
                "POST",
                "/api/v1/bots",
                Some(&bob),
                Some(create_body("q3", "bob")),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "no_plan");

    // Suspension (docs/12 §12.1) blocks creates without deleting anything.
    dir.set_user(plans::UserAccess {
        user_id: "alice".into(),
        plan_id: Some("owner".into()),
        status: plans::UserStatus::Suspended,
    });
    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req(
                "POST",
                "/api/v1/bots",
                Some(&alice),
                Some(create_body("q4", "alice")),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "user_not_active");
}

#[tokio::test]
async fn quota_admin_override_with_reason() {
    let rig = rig_with(Some(plans_for("alice", "tiny")), None);
    let alice = token(&rig.enc, "alice", &[]);
    let admin = token(&rig.enc, "root", &["admin"]);

    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("q1", "alice")),
        ))
        .await
        .unwrap();

    // Admin without a reason still hits the quota.
    let (status, _) = json_body(
        rig.app
            .clone()
            .oneshot(req(
                "POST",
                "/api/v1/bots",
                Some(&admin),
                Some(create_body("q2", "alice")),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Admin + audited reason (docs/12 §12.3) bypasses.
    let mut body = create_body("q2", "alice");
    body["adminOverrideReason"] = json!("support ticket 4815162342");
    let resp = rig
        .app
        .clone()
        .oneshot(req("POST", "/api/v1/bots", Some(&admin), Some(body)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn quota_soft_deleted_bot_frees_slot() {
    let rig = rig_with(Some(plans_for("alice", "tiny")), None);
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("q1", "alice")),
        ))
        .await
        .unwrap();
    rig.app
        .clone()
        .oneshot(req("DELETE", "/api/v1/bots/q1", Some(&alice), None))
        .await
        .unwrap();
    // The deleted bot is inside its retention window but no longer holds
    // a plan slot (docs/12 §12.4).
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("q2", "alice")),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn patch_version_rechecks_channels() {
    let rig = rig_with(Some(plans_for("alice", "tiny")), None);
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("p1", "alice")),
        ))
        .await
        .unwrap();

    // The `tiny` plan allows `stable` only.
    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req(
                "PATCH",
                "/api/v1/bots/p1",
                Some(&alice),
                Some(json!({
                    "version": {"channel": "canary"},
                    "configRevision": 2,
                })),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "channel_denied");

    // A stable patch (allowed) goes through.
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "PATCH",
            "/api/v1/bots/p1",
            Some(&alice),
            Some(json!({
                "version": {"channel": "stable"},
                "configRevision": 2,
            })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn undelete_after_retention_window_is_gone() {
    let rig = rig(); // no plans → 30-day default
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("u1", "alice")),
        ))
        .await
        .unwrap();
    rig.app
        .clone()
        .oneshot(req("DELETE", "/api/v1/bots/u1", Some(&alice), None))
        .await
        .unwrap();

    // Inside the window it works…
    rig.clock
        .advance(std::time::Duration::from_secs(29 * 86_400));
    let resp = rig
        .app
        .clone()
        .oneshot(req("POST", "/api/v1/bots/u1/undelete", Some(&alice), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // …delete again and pass the window: 410.
    rig.app
        .clone()
        .oneshot(req("DELETE", "/api/v1/bots/u1", Some(&alice), None))
        .await
        .unwrap();
    rig.clock
        .advance(std::time::Duration::from_secs(31 * 86_400));
    let resp = rig
        .app
        .clone()
        .oneshot(req("POST", "/api/v1/bots/u1/undelete", Some(&alice), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::GONE);
}

#[tokio::test]
async fn purge_expired_removes_rows() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &[]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("pd1", "alice")),
        ))
        .await
        .unwrap();
    rig.app
        .clone()
        .oneshot(req("DELETE", "/api/v1/bots/pd1", Some(&alice), None))
        .await
        .unwrap();

    // Pods are confirmed gone post-drain, but the row must survive inside
    // the 30-day window.
    let rep = rig.fleet.fleet.purge_expired().await.unwrap();
    assert!(rep.bots_purged.is_empty());
    assert_eq!(rep.bots_retained, ["pd1"]);

    rig.clock
        .advance(std::time::Duration::from_secs(31 * 86_400));
    let rep = rig.fleet.fleet.purge_expired().await.unwrap();
    assert_eq!(rep.bots_purged, ["pd1"]);
    assert!(rig.fleet.fleet.bots.get("pd1").await.is_none());
}

#[tokio::test]
async fn metering_and_usage_endpoint() {
    let usage = Arc::new(plans::InMemoryUsage::new());
    let rig = rig_with(Some(plans_for("alice", "friends")), Some(usage.clone()));
    let alice = token(&rig.enc, "alice", &[]);
    let admin = token(&rig.enc, "root", &["admin"]);
    rig.app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots",
            Some(&alice),
            Some(create_body("m1", "alice")),
        ))
        .await
        .unwrap();

    // Timeline: the bot ran for the entire previous hour.
    let now = rig.clock.now_unix_ms();
    let hour_start = now - now.rem_euclid(plans::HOUR_MS);
    let timelines = plans::InMemoryTimelines::new();
    timelines
        .record(
            "m1",
            plans::PhaseEvent::at(
                hour_start - plans::HOUR_MS,
                fleet_types::phase::BotPhase::Running,
            ),
        )
        .await
        .unwrap();
    let rep = rig.fleet.fleet.meter_usage(&timelines).await;
    assert_eq!(rep.records.len(), 1, "{rep:?}");
    assert_eq!(rep.records[0].running_s, 3600);
    assert_eq!(rep.records[0].tenant, "alice");
    assert_eq!(rep.records[0].plan_id, "friends");

    // /usage is admin-only…
    let resp = rig
        .app
        .clone()
        .oneshot(req("GET", "/api/v1/usage", Some(&alice), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // …and returns the showback rows for admins.
    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("GET", "/api/v1/usage", Some(&admin), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["totals"]["runningS"], 3600);
    assert_eq!(body["tenants"][0]["tenant"], "alice");
}

#[tokio::test]
async fn usage_endpoint_404_when_unconfigured() {
    let rig = rig();
    let admin = token(&rig.enc, "root", &["admin"]);
    let resp = rig
        .app
        .clone()
        .oneshot(req("GET", "/api/v1/usage", Some(&admin), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// -- rollouts (docs/07 §7.3) --------------------------------------------------

fn rollout_body(name: &str) -> Value {
    json!({
        "name": name,
        "spec": {
            "targetImage": "registry.local/xenomit-botd@sha256:abcd1234",
            "targetGameVersion": "game-2026.03",
            "selector": {},
            "strategy": {
                "canary": { "count": 1, "bakeMinutes": 5 },
                "waves": {
                    "maxDraining": "25%",
                    "maxOffline": 2,
                    "interWaveDelayS": 60,
                    "loginJitterS": 5
                },
                "gates": { "maxFailedRestores": 0 }
            }
        }
    })
}

async fn create_rollout(rig: &Rig, name: &str) -> (StatusCode, Value) {
    let admin = token(&rig.enc, "ops", &["admin"]);
    json_body(
        rig.app
            .clone()
            .oneshot(req(
                "POST",
                "/api/v1/rollouts",
                Some(&admin),
                Some(rollout_body(name)),
            ))
            .await
            .unwrap(),
    )
    .await
}

#[tokio::test]
async fn rollout_create_get_list() {
    let rig = rig();
    let admin = token(&rig.enc, "ops", &["admin"]);

    let (status, body) = create_rollout(&rig, "roll-1").await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["spec"]["targetImage"],
        "registry.local/xenomit-botd@sha256:abcd1234"
    );

    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("GET", "/api/v1/rollouts", Some(&admin), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().unwrap().len(), 1);

    let (status, body) = json_body(
        rig.app
            .clone()
            .oneshot(req("GET", "/api/v1/rollouts/roll-1", Some(&admin), None))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["metadata"]["name"], "roll-1");

    let resp = rig
        .app
        .clone()
        .oneshot(req("GET", "/api/v1/rollouts/nope", Some(&admin), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rollout_create_validation() {
    let rig = rig();
    let admin = token(&rig.enc, "ops", &["admin"]);
    let post = |body: Value| {
        let app = rig.app.clone();
        let admin = admin.clone();
        async move {
            json_body(
                app.oneshot(req("POST", "/api/v1/rollouts", Some(&admin), Some(body)))
                    .await
                    .unwrap(),
            )
            .await
        }
    };

    // Bad name.
    let (status, _) = post(rollout_body("BAD_NAME")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // A bare tag, not a digest — only digests deploy (docs/07 §7.2).
    let mut b = rollout_body("roll-bad-img");
    b["spec"]["targetImage"] = json!("registry.local/xenomit-botd:latest");
    let (status, _) = post(b).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // maxDraining that is neither a count nor a percent.
    let mut b = rollout_body("roll-bad-md");
    b["spec"]["strategy"]["waves"]["maxDraining"] = json!("lots");
    let (status, _) = post(b).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // maxOffline 0 would deadlock the rollout.
    let mut b = rollout_body("roll-bad-mo");
    b["spec"]["strategy"]["waves"]["maxOffline"] = json!(0);
    let (status, _) = post(b).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Non-positive reward-rate gate.
    let mut b = rollout_body("roll-bad-rr");
    b["spec"]["strategy"]["gates"]["minRewardRateRatio"] = json!(-0.5);
    let (status, _) = post(b).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rollout_admin_only() {
    let rig = rig();
    let alice = token(&rig.enc, "alice", &["tenant"]);

    let (status, _) = create_rollout(&rig, "roll-admin").await;
    assert_eq!(status, StatusCode::CREATED);

    for (m, u) in [
        ("POST", "/api/v1/rollouts"),
        ("DELETE", "/api/v1/rollouts/roll-admin"),
        ("POST", "/api/v1/rollouts/roll-admin/pause"),
        ("POST", "/api/v1/rollouts/roll-admin/resume"),
        ("POST", "/api/v1/rollouts/roll-admin/abort"),
        ("POST", "/api/v1/rollouts/roll-admin/rollback"),
    ] {
        let body = if m == "POST" && u == "/api/v1/rollouts" {
            Some(rollout_body("x"))
        } else {
            None
        };
        let resp = rig
            .app
            .clone()
            .oneshot(req(m, u, Some(&alice), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{m} {u}");
    }
    // Read endpoints are fine for tenants.
    let resp = rig
        .app
        .clone()
        .oneshot(req("GET", "/api/v1/rollouts", Some(&alice), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn rollout_concurrent_conflict() {
    let rig = rig();
    let admin = token(&rig.enc, "ops", &["admin"]);

    let (status, _) = create_rollout(&rig, "roll-a").await;
    assert_eq!(status, StatusCode::CREATED);
    // A second rollout while the first is active (no status yet = pending).
    let (status, body) = create_rollout(&rig, "roll-b").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    // Same name again is also a conflict.
    let (status, _) = create_rollout(&rig, "roll-a").await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Delete frees the slot.
    let resp = rig
        .app
        .clone()
        .oneshot(req("DELETE", "/api/v1/rollouts/roll-a", Some(&admin), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let (status, _) = create_rollout(&rig, "roll-b").await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn rollout_lifecycle_actions() {
    let rig = rig();
    let admin = token(&rig.enc, "ops", &["admin"]);
    let (status, _) = create_rollout(&rig, "roll-life").await;
    assert_eq!(status, StatusCode::CREATED);

    let act = |action: &str| {
        let app = rig.app.clone();
        let admin = admin.clone();
        let uri = format!("/api/v1/rollouts/roll-life/{action}");
        async move {
            json_body(
                app.oneshot(req("POST", &uri, Some(&admin), None))
                    .await
                    .unwrap(),
            )
            .await
        }
    };

    let (status, body) = act("pause").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["paused"], true);

    let (status, body) = act("resume").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["paused"], false);
    assert_eq!(body["spec"]["resumeCount"], 1);

    let (status, body) = act("rollback").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["rollback"], true);

    let (status, body) = act("abort").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["aborted"], true);

    // Actions on a missing rollout 404.
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/rollouts/nope/pause",
            Some(&admin),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// A terminal rollout rejects every action — the spec flags of a Done
/// CR must not flip.
#[tokio::test]
async fn rollout_action_on_terminal_409() {
    let rig = rig();
    let admin = token(&rig.enc, "ops", &["admin"]);

    let mut r = fleet_types::crds::BotRollout::new(
        "roll-done",
        serde_json::from_value(rollout_body("roll-done")["spec"].clone()).unwrap(),
    );
    r.status = Some(fleet_types::crds::BotRolloutStatus {
        phase: fleet_types::crds::RolloutPhase::Done,
        ..Default::default()
    });
    rig.fleet
        .fleet
        .rollouts
        .apply(serde_json::to_value(&r).unwrap())
        .await
        .unwrap();

    for action in ["pause", "resume", "abort", "rollback"] {
        let resp = rig
            .app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/api/v1/rollouts/roll-done/{action}"),
                Some(&admin),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT, "{action}");
    }
}

/// Rollout writes broadcast `FleetEvent`s (the SSE stream the control
/// plane UI subscribes to).
#[tokio::test]
async fn rollout_events_broadcast() {
    let rig = rig();
    let admin = token(&rig.enc, "ops", &["admin"]);
    let mut rx = rig.fleet.fleet.events.subscribe();

    let (status, _) = create_rollout(&rig, "roll-ev").await;
    assert_eq!(status, StatusCode::CREATED);
    let ev = rx.try_recv().unwrap();
    assert!(
        matches!(
            ev,
            orchestrator_api::FleetEvent::RolloutSpecChanged { ref rollout, ref action }
                if rollout == "roll-ev" && action == "create"
        ),
        "{ev:?}"
    );

    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/rollouts/roll-ev/pause",
            Some(&admin),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ev = rx.try_recv().unwrap();
    assert!(
        matches!(
            ev,
            orchestrator_api::FleetEvent::RolloutSpecChanged { ref rollout, ref action }
                if rollout == "roll-ev" && action == "pause"
        ),
        "{ev:?}"
    );
}

// ---------------------------------------------------------------------------
// Reconcile race (fix1): concurrent API requests for one bot run under the
// shared per-bot gate — overlapping passes would each work from a stale
// copy and double the handoff pod + Activate.
// ---------------------------------------------------------------------------

/// 16 concurrent `POST /restart` calls must arm exactly one successor pod
/// and send exactly one `Activate`. Without `Fleet.locks` each request
/// reconciles on its own stale copy: two passes both see `handoff == None`
/// and both call `ensure_pod` — the regression this guards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_serialize_per_bot() {
    use fleet_types::epoch::Epoch;
    use fleet_types::phase::BotPhase;
    use fleet_types::proto::gateway_command;

    let rig = rig();
    let owner = token(&rig.enc, "owner", &[]);
    let (status, _) = {
        let resp = rig
            .app
            .clone()
            .oneshot(req(
                "POST",
                "/api/v1/bots",
                Some(&owner),
                Some(create_body("bot-conc", "owner")),
            ))
            .await
            .unwrap();
        json_body(resp).await
    };
    assert_eq!(status, StatusCode::CREATED);

    // Simulate the pod reporting in: Running pod + live session, then the
    // phase flip `on_phase_changed` would drive (no route feeds it offline,
    // so the status write stands in for the gateway event).
    rig.pods.set_running("bot-conc", Epoch(1));
    rig.control.connect("bot-conc", Epoch(1));
    rig.fleet
        .fleet
        .bots
        .update("bot-conc", &mut |b: &mut XenomitBot| {
            if let Some(st) = b.status.as_mut() {
                st.phase = BotPhase::Running;
            }
        })
        .await
        .unwrap();

    // Fire N concurrent restarts. Every request writes `pending_request`
    // then reconciles — serialized by the shared per-bot gate.
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let app = rig.app.clone();
        let owner = owner.clone();
        set.spawn(async move {
            app.oneshot(req(
                "POST",
                "/api/v1/bots/bot-conc/restart",
                Some(&owner),
                None,
            ))
            .await
            .unwrap()
            .status()
        });
    }
    while let Some(status) = set.join_next().await {
        assert_eq!(status.unwrap(), StatusCode::OK);
    }

    // Exactly one successor pod was armed for epoch 2.
    let e2_spawns = rig
        .pods
        .created
        .lock()
        .unwrap()
        .iter()
        .filter(|s| s.bot_id == "bot-conc" && s.epoch == Epoch(2))
        .count();
    assert_eq!(e2_spawns, 1, "concurrent reconciles double-spawned epoch 2");

    // Drive the handoff through drain → activation. The Drain must reach
    // the predecessor's live session first, so connect the successor and
    // mark it Running *before* the predecessor exits.
    rig.pods.set_running("bot-conc", Epoch(2));
    rig.control.connect("bot-conc", Epoch(2));
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots/bot-conc/restart",
            Some(&owner),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "drain send failed");

    // Predecessor drained: pod exited, session closed. One more pass runs
    // Drain → Activate → Starting.
    rig.pods.exit("bot-conc", Epoch(1), Some(0));
    rig.control.disconnect("bot-conc", Epoch(1));
    let resp = rig
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/bots/bot-conc/restart",
            Some(&owner),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "activate pass failed");

    let activates = rig
        .control
        .commands("bot-conc", Epoch(2))
        .into_iter()
        .filter(|c| matches!(c.command, Some(gateway_command::Command::Activate(_))))
        .count();
    assert_eq!(activates, 1, "Activate must be delivered exactly once");
}
