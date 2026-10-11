//! axum routes. Every mutating call goes through [`Fleet`]; the only
//! unauthenticated paths are `/healthz` and the OpenAPI document.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use fleet_types::phase::DesiredState;

use crate::auth::{ApiError, AuthConfig, Claims, require_jwt};
use crate::dto;
use crate::events::FleetEvent;
use crate::fleet::Fleet;

/// Application state handed to every handler.
pub struct AppState {
    pub fleet: Fleet,
    pub auth: AuthConfig,
    /// Panel pairing endpoint (docs/08 §8.6) used by the `/v1` compat
    /// surface; `None` = bots run without a panel link.
    pub panel: Option<Arc<crate::compat::PanelLink>>,
    /// `/v1/events` replay log + fan-out.
    pub compat_events: Arc<crate::compat::CompatEvents>,
    /// Cancelled on shutdown; keeps the compat bridge task.
    pub tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl AppState {
    pub fn new(fleet: Fleet, auth: AuthConfig) -> Self {
        let compat_events = Arc::new(crate::compat::CompatEvents::new());
        let mut tasks = Vec::new();
        // Fold the fleet broadcast into the /v1 SSE log here so every
        // composition (binary, tests) gets the same stream; callers that
        // construct AppState outside a runtime must spawn
        // `compat::bridge_compat_events` themselves.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tasks.push(handle.spawn(crate::compat::bridge_compat_events(
                fleet.events.subscribe(),
                fleet.bots.clone(),
                compat_events.clone(),
            )));
        }
        AppState {
            fleet,
            auth,
            panel: None,
            compat_events,
            tasks: std::sync::Mutex::new(tasks),
        }
    }

    pub fn with_panel(mut self, link: crate::compat::PanelLink) -> Self {
        self.panel = Some(Arc::new(link));
        self
    }
}

/// The mounted router: `/api/v1/**` behind the JWT middleware, `/healthz`
/// and `/api/v1/openapi.json` public.
pub fn router(state: Arc<AppState>) -> Router {
    let api = Router::new()
        .route("/bots", get(list_bots).post(create_bot))
        .route(
            "/bots/{name}",
            get(get_bot).patch(patch_bot).delete(delete_bot),
        )
        .route("/bots/{name}/pause", post(pause_bot))
        .route("/bots/{name}/resume", post(resume_bot))
        .route("/bots/{name}/stop", post(stop_bot))
        .route("/bots/{name}/restart", post(restart_bot))
        .route("/bots/{name}/restore", post(restore_bot))
        .route("/bots/{name}/credentials", post(write_credentials))
        .route("/bots/{name}/request", post(send_request))
        .route("/bots/{name}/undelete", post(undelete_bot))
        .route("/bots/{name}/snapshots", get(list_snapshots))
        .route("/events", get(events))
        .route("/rollouts", get(list_rollouts).post(create_rollout))
        .route("/rollouts/{name}", get(get_rollout).delete(delete_rollout))
        .route("/rollouts/{name}/pause", post(pause_rollout))
        .route("/rollouts/{name}/resume", post(resume_rollout))
        .route("/rollouts/{name}/abort", post(abort_rollout))
        .route("/rollouts/{name}/rollback", post(rollback_rollout))
        .route("/proxies", get(list_proxies).post(create_proxy))
        .route("/proxies/pool", get(pool_status))
        .route(
            "/proxies/{name}",
            get(get_proxy).put(upsert_proxy).delete(delete_proxy),
        )
        .route("/fleet", get(fleet_summary))
        .route("/usage", get(usage_report))
        .layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            require_jwt,
        ));

    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        // k8s probes (services/gitops/base/api.yaml): liveness is the process
        // being up; readiness requires nothing external in dev mode.
        .route("/livez", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ok" }))
        .route("/api/v1/openapi.json", get(openapi))
        .nest("/api/v1", api)
        // The panel speaks `/v1/*` (xenomit-web-panel/src/lib/orchestrator.ts
        // + mock-orchestrator); the compat router carries its own DTOs.
        .nest("/v1", crate::compat::router(state.clone()))
        .with_state(state)
        .layer(axum::middleware::from_fn(gitops_boundary))
}

/// Deployed GitOps mode reserves fleet specifications for Argo CD. Reads and
/// credential submission remain available; they do not rewrite desired state.
async fn gitops_boundary(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if std::env::var("XENOMIT_FLEET_GITOPS").as_deref() == Ok("true")
        && !gitops_request_allowed(request.method(), request.uri().path())
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "Fleet specifications are managed in xenomit-deployments; open a GitHub PR."})),
        ).into_response();
    }
    next.run(request).await
}

fn gitops_request_allowed(method: &axum::http::Method, path: &str) -> bool {
    use axum::http::Method;
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return true;
    }
    if *method != Method::POST {
        return false;
    }
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    matches!(
        parts.as_slice(),
        ["api", "v1", "bots", _, "credentials"]
            | ["v1", "bots", _, "credentials"]
            | ["v1", "bots", _, "dosid"]
    )
}

#[cfg(test)]
mod gitops_boundary_tests {
    use super::gitops_request_allowed;
    use axum::http::Method;

    #[test]
    fn both_api_surfaces_reject_spec_mutations() {
        for path in [
            "/api/v1/bots",
            "/api/v1/bots/a/pause",
            "/api/v1/bots/a/request",
            "/v1/bots",
            "/v1/bots/a/restart",
            "/v1/rollouts/a/resume",
            "/v1/proxies",
        ] {
            assert!(!gitops_request_allowed(&Method::POST, path), "{path}");
        }
        assert!(!gitops_request_allowed(&Method::PATCH, "/api/v1/bots/a"));
        assert!(!gitops_request_allowed(&Method::DELETE, "/v1/bots/a"));
    }

    #[test]
    fn reads_and_credentials_remain_available() {
        assert!(gitops_request_allowed(&Method::GET, "/v1/bots/a"));
        assert!(gitops_request_allowed(&Method::POST, "/v1/bots/a/dosid"));
        assert!(gitops_request_allowed(
            &Method::POST,
            "/api/v1/bots/a/credentials"
        ));
        assert!(!gitops_request_allowed(
            &Method::POST,
            "/v1/bots/a/credentials/extra"
        ));
    }
}

// -- bots -------------------------------------------------------------------

async fn list_bots(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> impl IntoResponse {
    let bots = st.fleet.list(&claims).await;
    Json(serde_json::json!({
        "items": bots.iter().map(dto::bot_view).collect::<Vec<_>>()
    }))
}

async fn create_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<dto::CreateBotRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let bot = st.fleet.create(&claims, req).await?;
    Ok((StatusCode::CREATED, Json(dto::bot_view(&bot))))
}

async fn get_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(&st.fleet.get(&claims, &name).await?)))
}

async fn patch_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
    Json(req): Json<dto::PatchBotRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet.patch(&claims, &name, req).await?,
    )))
}

async fn pause_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet
            .set_desired(&claims, &name, DesiredState::Paused)
            .await?,
    )))
}

async fn resume_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet
            .set_desired(&claims, &name, DesiredState::Running)
            .await?,
    )))
}

async fn stop_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet
            .set_desired(&claims, &name, DesiredState::Stopped)
            .await?,
    )))
}

async fn restart_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet.restart(&claims, &name).await?,
    )))
}

async fn restore_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
    Json(req): Json<dto::RestoreRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet.restore(&claims, &name, req.snapshot_id).await?,
    )))
}

async fn write_credentials(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
    Json(req): Json<dto::CredentialsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet.write_credentials(&claims, &name, req).await?,
    )))
}

async fn send_request(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
    Json(req): Json<dto::PendingRequestBody>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet
            .request(&claims, &name, req.kind, req.snapshot_id)
            .await?,
    )))
}

async fn delete_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    st.fleet.delete(&claims, &name).await
}

async fn undelete_bot(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(dto::bot_view(
        &st.fleet.undelete(&claims, &name).await?,
    )))
}

async fn list_snapshots(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let snaps = st.fleet.snapshots(&claims, &name).await?;
    Ok(Json(serde_json::json!({ "items": snaps })))
}

// -- rollouts / proxies -----------------------------------------------------

async fn list_rollouts(
    State(st): State<Arc<AppState>>,
    Extension(_claims): Extension<Claims>,
) -> impl IntoResponse {
    Json(serde_json::json!({ "items": st.fleet.rollouts.list().await }))
}

/// `POST /rollouts`: validated create (docs/07 §7.3) — digest-shaped
/// image, parseable wave limits, no other active rollout (409).
async fn create_rollout(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<dto::CreateRolloutRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if !claims.is_admin() {
        return Err(ApiError::forbidden("rollouts are admin-only"));
    }
    let v = st.fleet.create_rollout(req).await?;
    Ok((StatusCode::CREATED, Json(v)))
}

async fn get_rollout(
    State(st): State<Arc<AppState>>,
    Extension(_claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    st.fleet
        .rollouts
        .get(&name)
        .await
        .map(Json)
        .ok_or_else(|| ApiError::not_found("rollout not found"))
}

async fn delete_rollout(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    if !claims.is_admin() {
        return Err(ApiError::forbidden("rollouts are admin-only"));
    }
    match st.fleet.rollouts.delete(&name).await {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) => Err(ApiError::not_found("rollout not found")),
        Err(e) => Err(ApiError::internal(e.to_string())),
    }
}

/// Lifecycle actions (docs/04 §4.1): the handlers share one shape — the
/// spec flag the controller reads next pass.
async fn rollout_action(
    st: &Arc<AppState>,
    claims: &Claims,
    name: &str,
    action: dto::RolloutAction,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !claims.is_admin() {
        return Err(ApiError::forbidden("rollouts are admin-only"));
    }
    st.fleet.rollout_action(name, action).await.map(Json)
}

async fn pause_rollout(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    rollout_action(&st, &claims, &name, dto::RolloutAction::Pause).await
}

async fn resume_rollout(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    rollout_action(&st, &claims, &name, dto::RolloutAction::Resume).await
}

async fn abort_rollout(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    rollout_action(&st, &claims, &name, dto::RolloutAction::Abort).await
}

async fn rollback_rollout(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    rollout_action(&st, &claims, &name, dto::RolloutAction::Rollback).await
}

async fn list_proxies(
    State(st): State<Arc<AppState>>,
    Extension(_claims): Extension<Claims>,
) -> impl IntoResponse {
    Json(serde_json::json!({ "items": st.fleet.proxies.list().await }))
}

/// Admin-only pool management (docs/05 §5.4): CRUD on `Proxy` CRs. Their
/// credentials never leave Vault (docs/05 §5.6).
async fn create_proxy(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<dto::UpsertProxyRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if !claims.is_admin() {
        return Err(ApiError::forbidden("proxies are admin-only"));
    }
    let v = st.fleet.create_proxy(req).await?;
    Ok((StatusCode::CREATED, Json(v)))
}

async fn get_proxy(
    State(st): State<Arc<AppState>>,
    Extension(_claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    st.fleet
        .proxies
        .get(&name)
        .await
        .map(Json)
        .ok_or_else(|| ApiError::not_found("proxy not found"))
}

async fn upsert_proxy(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
    Json(req): Json<dto::UpsertProxyRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if !claims.is_admin() {
        return Err(ApiError::forbidden("proxies are admin-only"));
    }
    Ok(Json(st.fleet.upsert_proxy(&name, req).await?))
}

async fn delete_proxy(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    if !claims.is_admin() {
        return Err(ApiError::forbidden("proxies are admin-only"));
    }
    st.fleet.delete_proxy(&name).await
}

async fn pool_status(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<impl IntoResponse, ApiError> {
    if !claims.is_admin() {
        return Err(ApiError::forbidden("pool status is admin-only"));
    }
    Ok(Json(st.fleet.pool_status().await))
}

// -- fleet admin ------------------------------------------------------------

async fn fleet_summary(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> impl IntoResponse {
    Json(st.fleet.summary(&claims).await)
}

/// `GET /api/v1/usage` query: `[fromUnixMs, toUnixMs)` — both optional,
/// defaulting to the store's full range.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageQuery {
    #[serde(default)]
    from_unix_ms: Option<i64>,
    #[serde(default)]
    to_unix_ms: Option<i64>,
}

/// Admin-only showback report (docs/12 §12.5.1): per-tenant
/// running/offline/needs-human hours. 404 when metering isn't wired.
async fn usage_report(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Query(q): axum::extract::Query<UsageQuery>,
) -> Result<impl IntoResponse, ApiError> {
    if !claims.is_admin() {
        return Err(ApiError::forbidden("usage is admin-only"));
    }
    let rep = st
        .fleet
        .usage_report(
            q.from_unix_ms.unwrap_or(0),
            q.to_unix_ms.unwrap_or(i64::MAX),
        )
        .await?;
    Ok(Json(serde_json::to_value(rep).unwrap_or_default()))
}

// -- events (SSE) -----------------------------------------------------------

/// `GET /api/v1/events`: server-sent events for every gateway/API
/// lifecycle event, filtered to the caller's tenant unless admin.
async fn events(
    State(st): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = st.fleet.events.subscribe();
    let st = st.clone();
    let stream = BroadcastStream::new(rx)
        .then(move |res| {
            let st = st.clone();
            let claims = claims.clone();
            async move {
                match res {
                    Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                        Some(Ok(Event::default().comment(format!("lagged {n} events"))))
                    }
                    Ok(ev) => {
                        if visible(&st, &claims, &ev).await {
                            serde_json::to_string(&ev)
                                .ok()
                                .map(|data| Ok(Event::default().data(data)))
                        } else {
                            None
                        }
                    }
                }
            }
        })
        .filter_map(|item| item);
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn visible(st: &Arc<AppState>, claims: &Claims, ev: &FleetEvent) -> bool {
    if claims.is_admin() {
        return true;
    }
    let Some(bot_id) = ev.bot_id() else {
        return false;
    };
    st.fleet
        .bots
        .get(bot_id)
        .await
        .is_some_and(|b| b.spec.owner == claims.sub)
}

// -- misc -------------------------------------------------------------------

async fn openapi() -> impl IntoResponse {
    Json(crate::openapi::spec())
}
