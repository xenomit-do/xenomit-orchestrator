//! EdDSA (Ed25519) JWT authentication (docs/08 §8.2). The panel mints
//! short-lived tokens; this API only verifies — never issues — them.
//!
//! `sub` is the tenant id used for owner scoping; `roles` may contain
//! `"admin"` which skips owner filtering.

use axum::Json;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// JWT claims the API honors.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    /// Tenant / account id; bots are scoped to `spec.owner == sub`.
    pub sub: String,
    pub exp: usize,
    #[serde(default)]
    pub iat: Option<usize>,
    #[serde(default)]
    pub roles: Vec<String>,
    /// Panel session id (opaque).
    #[serde(default)]
    pub sid: Option<String>,
    /// Panel contract's singular role (`shared/orchestrator.ts` mints
    /// `{sub, role, act, …}`); equivalent to `roles: [role]`.
    #[serde(default)]
    pub role: Option<String>,
}

impl Claims {
    pub fn is_admin(&self) -> bool {
        self.roles.iter().any(|r| r == "admin") || self.role.as_deref() == Some("admin")
    }

    /// Owner a request may act as: its own `sub` unless admin.
    pub fn owner(&self) -> &str {
        &self.sub
    }
}

/// Verification setup for the auth middleware.
#[derive(Clone)]
pub struct AuthConfig {
    /// Ed25519 public key (PEM or raw components) the panel's signer pairs with.
    key: Arc<DecodingKey>,
    /// Expected `aud`, if enforced.
    pub audience: Option<String>,
    /// Expected `iss`, if enforced.
    pub issuer: Option<String>,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("audience", &self.audience)
            .field("issuer", &self.issuer)
            .finish_non_exhaustive()
    }
}

impl AuthConfig {
    /// PEM-encoded Ed25519 public key ("-----BEGIN PUBLIC KEY-----").
    pub fn from_ed_pem(pem: &[u8]) -> Result<Self, jsonwebtoken::errors::Error> {
        Ok(AuthConfig {
            key: Arc::new(DecodingKey::from_ed_pem(pem)?),
            audience: None,
            issuer: None,
        })
    }

    /// Ed25519 public key bytes (`jsonwebtoken` expects the raw 32-byte key here,
    /// despite the `from_ed_der` name — pass SPKI DER through `from_ed_pem` instead
    /// if you have a PEM/SPKI document).
    pub fn from_ed_der(der: &[u8]) -> Result<Self, jsonwebtoken::errors::Error> {
        Ok(AuthConfig {
            key: Arc::new(DecodingKey::from_ed_der(der)),
            audience: None,
            issuer: None,
        })
    }

    /// Raw 32-byte Ed25519 public key, base64url-encoded.
    pub fn from_ed_components(key: &str) -> Result<Self, jsonwebtoken::errors::Error> {
        Ok(AuthConfig {
            key: Arc::new(DecodingKey::from_ed_components(key)?),
            audience: None,
            issuer: None,
        })
    }

    pub fn with_audience(mut self, aud: impl Into<String>) -> Self {
        self.audience = Some(aud.into());
        self
    }

    pub fn with_issuer(mut self, iss: impl Into<String>) -> Self {
        self.issuer = Some(iss.into());
        self
    }

    /// Validate a bearer token. Errors are deliberately vague (no oracle).
    pub fn validate(&self, token: &str) -> Result<Claims, StatusCode> {
        let mut v = Validation::new(Algorithm::EdDSA);
        v.required_spec_claims = ["exp", "sub"].into_iter().map(String::from).collect();
        if let Some(aud) = &self.audience {
            v.set_audience(std::slice::from_ref(aud));
        } else {
            v.validate_aud = false;
        }
        if let Some(iss) = &self.issuer {
            v.set_issuer(std::slice::from_ref(iss));
        }
        jsonwebtoken::decode::<Claims>(token, &self.key, &v)
            .map(|d| d.claims)
            .map_err(|_| StatusCode::UNAUTHORIZED)
    }
}

/// Bearer-token middleware: validates the JWT and inserts [`Claims`] into
/// request extensions. Handlers read `Extension<Claims>`; rejections are
/// bare 401s (docs/09 §9.1: no detail that helps an attacker).
pub async fn require_jwt(State(cfg): State<AuthConfig>, mut req: Request, next: Next) -> Response {
    let token = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let Some(token) = token else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    match cfg.validate(token) {
        Ok(claims) => {
            req.extensions_mut().insert(claims);
            next.run(req).await
        }
        Err(code) => code.into_response(),
    }
}

/// Uniform error body for handlers.
#[derive(Debug, Serialize)]
pub struct ErrorBody<'a> {
    pub error: &'a str,
}

/// Handler error type: status + short message (never secrets) + an
/// optional machine-readable `code` for typed rejections (quota, …).
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    /// Stable code clients can switch on (e.g. `max_bots_exceeded`).
    pub code: Option<&'static str>,
}

impl ApiError {
    pub fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        ApiError {
            status,
            message: msg.into(),
            code: None,
        }
    }

    pub fn with_code(mut self, code: &'static str) -> Self {
        self.code = Some(code);
        self
    }

    /// Quota rejections (docs/12 §12.3): 403 + the typed kind so the panel
    /// renders "upgrade to add bots" rather than a generic failure.
    pub fn quota(e: plans::QuotaError) -> Self {
        Self::new(StatusCode::FORBIDDEN, e.to_string()).with_code(e.kind())
    }

    /// Resource existed but is gone for good (e.g. an expired retention
    /// window on undelete).
    pub fn gone(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::GONE, msg)
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, msg)
    }
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, msg)
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, msg)
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, msg)
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, msg)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = serde_json::json!({ "error": self.message });
        if let Some(code) = self.code {
            body["code"] = code.into();
        }
        (self.status, Json(body)).into_response()
    }
}

impl From<fleet_types::snapshot::StoreError> for ApiError {
    fn from(e: fleet_types::snapshot::StoreError) -> Self {
        match e {
            fleet_types::snapshot::StoreError::NotFound(_) => ApiError::not_found(e.to_string()),
            _ => ApiError::internal(e.to_string()),
        }
    }
}
