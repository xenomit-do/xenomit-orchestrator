//! OpenAPI 3.1 document for the REST API, built programmatically (no
//! runtime codegen) and served at `GET /api/v1/openapi.json`. Schemas for
//! the CRs come from schemars so the doc cannot drift from the types.

pub fn spec() -> serde_json::Value {
    let bot_schema = schemars::schema_for!(fleet_types::crds::XenomitBot);
    let spec_schema = schemars::schema_for!(fleet_types::crds::XenomitBotSpec);
    let rollout_schema = schemars::schema_for!(fleet_types::crds::BotRollout);
    let create_rollout_schema = schemars::schema_for!(crate::dto::CreateRolloutRequest);
    let proxy_schema = schemars::schema_for!(fleet_types::crds::Proxy);
    let create_schema = schemars::schema_for!(crate::dto::CreateBotRequest);
    let upsert_proxy_schema = schemars::schema_for!(crate::dto::UpsertProxyRequest);
    let pool_status_schema = schemars::schema_for!(crate::dto::PoolStatus);
    let showback_schema = schemars::schema_for!(plans::ShowbackReport);

    serde_json::json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Xenomit Fleet Orchestrator API",
            "version": "0.1.0",
            "description": "REST admin API for the managed bot fleet (docs/08). \
                All mutating endpoints require a Bearer JWT (EdDSA/Ed25519). \
                Credentials are accepted on create/credentials endpoints and \
                written straight to Vault — they are never returned."
        },
        "servers": [{ "url": "/api/v1" }],
        "security": [{ "bearerAuth": [] }],
        "paths": {
            "/bots": {
                "get": {
                    "operationId": "listBots",
                    "summary": "List the caller's bots (all bots for admins)",
                    "responses": { "200": { "description": "Bot list" } }
                },
                "post": {
                    "operationId": "createBot",
                    "summary": "Create a bot; credentials go to Vault",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": {
                            "schema": create_schema
                        }}
                    },
                    "responses": {
                        "201": { "description": "Created bot (spec + status)" },
                        "409": { "description": "Name already exists" }
                    }
                }
            },
            "/bots/{name}": {
                "get": {
                    "operationId": "getBot",
                    "summary": "Bot spec + status",
                    "responses": {
                        "200": { "description": "The bot" },
                        "404": { "description": "Unknown or foreign bot" }
                    }
                },
                "patch": {
                    "operationId": "patchBot",
                    "summary": "Spec edit; configRevision classifies hot-apply vs handoff",
                    "responses": { "200": { "description": "Updated bot" } }
                },
                "delete": {
                    "operationId": "deleteBot",
                    "summary": "Soft delete: destroy Vault credentials, 30-day retention",
                    "responses": { "202": { "description": "Deletion accepted" } }
                }
            },
            "/bots/{name}/pause": {
                "post": { "operationId": "pauseBot", "summary": "desiredState=paused (drain)", "responses": { "200": { "description": "Updated bot" } } }
            },
            "/bots/{name}/resume": {
                "post": { "operationId": "resumeBot", "summary": "desiredState=running", "responses": { "200": { "description": "Updated bot" } } }
            },
            "/bots/{name}/stop": {
                "post": { "operationId": "stopBot", "summary": "desiredState=stopped (drain)", "responses": { "200": { "description": "Updated bot" } } }
            },
            "/bots/{name}/restart": {
                "post": { "operationId": "restartBot", "summary": "Planned handoff restart (docs/03 §3.2)", "responses": { "200": { "description": "Updated bot" } } }
            },
            "/bots/{name}/restore": {
                "post": { "operationId": "restoreBot", "summary": "Restore a snapshot (latest when omitted)", "responses": { "200": { "description": "Updated bot" } } }
            },
            "/bots/{name}/credentials": {
                "post": { "operationId": "writeCredentials", "summary": "Rotate Vault credentials; optional re-arm", "responses": { "200": { "description": "Updated bot" } } }
            },
            "/bots/{name}/request": {
                "post": { "operationId": "sendRequest", "summary": "One-shot request (restart|restore_snapshot|rearm)", "responses": { "200": { "description": "Updated bot" } } }
            },
            "/bots/{name}/undelete": {
                "post": { "operationId": "undeleteBot", "summary": "Undelete inside the retention window", "responses": { "200": { "description": "Updated bot" } } }
            },
            "/bots/{name}/snapshots": {
                "get": { "operationId": "listSnapshots", "summary": "Snapshot metadata, newest first", "responses": { "200": { "description": "Snapshot list" } } }
            },
            "/events": {
                "get": { "operationId": "events", "summary": "SSE lifecycle events (tenant-filtered)", "responses": { "200": { "description": "text/event-stream" } } }
            },
            "/rollouts": {
                "get": { "operationId": "listRollouts", "summary": "BotRollout CRs", "responses": { "200": { "description": "Rollout list" } } },
                "post": {
                    "operationId": "createRollout",
                    "summary": "Create a BotRollout (admin, docs/07 §7.3); 409 while another rollout is active",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": create_rollout_schema } }
                    },
                    "responses": {
                        "201": { "description": "Created rollout" },
                        "400": { "description": "Invalid spec" },
                        "409": { "description": "Name exists or another rollout is active" }
                    }
                }
            },
            "/rollouts/{name}": {
                "get": { "operationId": "getRollout", "responses": { "200": { "description": "The rollout" } } },
                "delete": { "operationId": "deleteRollout", "summary": "Admin only", "responses": { "204": { "description": "Deleted" } } }
            },
            "/rollouts/{name}/pause": {
                "post": { "operationId": "pauseRollout", "summary": "Hold new rolls; in-flight handoffs finish (admin)", "responses": { "200": { "description": "Updated rollout" }, "409": { "description": "Rollout already terminal" } } }
            },
            "/rollouts/{name}/resume": {
                "post": { "operationId": "resumeRollout", "summary": "Resume a paused rollout (admin)", "responses": { "200": { "description": "Updated rollout" }, "409": { "description": "Rollout already terminal" } } }
            },
            "/rollouts/{name}/abort": {
                "post": { "operationId": "abortRollout", "summary": "Stop where it stands; rolled bots keep the target (admin)", "responses": { "200": { "description": "Updated rollout" }, "409": { "description": "Rollout already terminal" } } }
            },
            "/rollouts/{name}/rollback": {
                "post": { "operationId": "rollbackRollout", "summary": "Hand rolled bots back to their previous image (admin)", "responses": { "200": { "description": "Updated rollout" }, "409": { "description": "Rollout already terminal" } } }
            },
            "/proxies": {
                "get": { "operationId": "listProxies", "summary": "Proxy CRs", "responses": { "200": { "description": "Proxy list" } } },
                "post": {
                    "operationId": "createProxy",
                    "summary": "Create a pool proxy (admin); credentials go to Vault",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": upsert_proxy_schema } }
                    },
                    "responses": {
                        "201": { "description": "Created proxy" },
                        "409": { "description": "Name already exists" }
                    }
                }
            },
            "/proxies/pool": {
                "get": {
                    "operationId": "poolStatus",
                    "summary": "Pool health/capacity summary (admin)",
                    "responses": { "200": { "description": "PoolStatus", "content": { "application/json": { "schema": pool_status_schema } } } }
                }
            },
            "/proxies/{name}": {
                "get": { "operationId": "getProxy", "responses": { "200": { "description": "The proxy" }, "404": { "description": "Unknown proxy" } } },
                "put": {
                    "operationId": "upsertProxy",
                    "summary": "Create-or-update a pool proxy (admin); optional credential rotation via Vault",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": upsert_proxy_schema } }
                    },
                    "responses": { "200": { "description": "Applied proxy" } }
                },
                "delete": {
                    "operationId": "deleteProxy",
                    "summary": "Delete a pool proxy + its Vault credentials (admin); refused while assigned",
                    "responses": {
                        "204": { "description": "Deleted" },
                        "409": { "description": "Proxy has assigned bots" }
                    }
                }
            },
            "/fleet": {
                "get": { "operationId": "fleetSummary", "summary": "Counts by phase for the caller's scope", "responses": { "200": { "description": "Summary" } } }
            },
            "/usage": {
                "get": {
                    "operationId": "usageReport",
                    "summary": "Showback report (admin): per-tenant running/offline/needs-human hours over [fromUnixMs, toUnixMs) (docs/12 §12.5.1)",
                    "parameters": [
                        { "name": "fromUnixMs", "in": "query", "required": false, "schema": { "type": "integer" } },
                        { "name": "toUnixMs", "in": "query", "required": false, "schema": { "type": "integer" } }
                    ],
                    "responses": {
                        "200": { "description": "ShowbackReport", "content": { "application/json": { "schema": showback_schema } } },
                        "403": { "description": "Admin only" },
                        "404": { "description": "Usage metering not configured" }
                    }
                }
            }
        },
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT",
                    "description": "EdDSA (Ed25519) JWT minted by the panel"
                }
            },
            "schemas": {
                "XenomitBot": bot_schema,
                "XenomitBotSpec": spec_schema,
                "BotRollout": rollout_schema,
                "CreateRolloutRequest": create_rollout_schema,
                "Proxy": proxy_schema,
                "UpsertProxyRequest": upsert_proxy_schema,
                "PoolStatus": pool_status_schema
            }
        }
    })
}
