//! `orchestrator-api`: the REST control surface of the Xenomit fleet
//! (docs/08), deliberately fully drivable offline.
//!
//! - EdDSA (Ed25519) JWT verification ([`auth`]); tenants see only their
//!   own bots (`spec.owner == claims.sub`), `roles: ["admin"]` sees all.
//! - Bot lifecycle: create / pause / resume / stop / restart / restore /
//!   credentials rotation / soft-delete + undelete / snapshots.
//! - `GET /api/v1/events` streams [`FleetEvent`]s over SSE; gateway
//!   lifecycle events are folded in via [`events::bridge_gateway_events`].
//! - Credentials flow *into* the `SecretStore` only (docs/09 §9.1): never
//!   into the repo, responses, or logs.
//! - `GET /api/v1/openapi.json` serves the OpenAPI 3.1 document.

pub mod auth;
pub mod compat;
pub mod dto;
pub mod events;
pub mod fleet;
pub mod openapi;
pub mod plan_hooks;
pub mod repo;
pub mod routes;

pub use auth::{ApiError, AuthConfig, Claims};
pub use events::FleetEvent;
pub use fleet::{BotOps, Fleet};
pub use repo::{BotRepo, CrRepo, InMemoryBots, InMemoryCrs, RepoError};
pub use routes::{AppState, router};
