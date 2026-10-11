# Xenomit Orchestrator

Rust backend that orchestrates Xenomit bot instances on Kubernetes: the
operator, control gateway, REST API, and the `xenomit-orchestratord` binary
that runs them together. Cluster bootstrap, database schema, and the
Deployment that runs this binary live in `xenomit-platform`. Argo CD
application `xenomit-control-plane` syncs `services/gitops/overlays/local`
from that repository.

Design documentation for the fleet: one Kubernetes pod per bot, an operator
that keeps them alive, rolling upgrades that **hand a bot's live state from
the old version to the new one** without losing statistics or the module's
position, per-bot proxy routing, OpenTelemetry everywhere, and an admin/user
surface in `xenomit-web-panel`.

> Status: **implemented through Phase 7b** — a Rust workspace that compiles
> and is fully unit-testable offline (no cluster, Vault, or Postgres
> needed): the bot controller, proxy pool, control gateway, REST/SSE API,
> plans/quotas/metering, the `BotRollout` canary→waves→gates→rollback
> controller, and `xenomit-orchestratord`, the single binary that composes
> all of it (local dev or Kubernetes). See
> [Implementation status](#implementation-status). **To run it:**
> [GETTING-STARTED.md](GETTING-STARTED.md). Design docs are unchanged;
> every claim about existing code was checked against the repos at the
> time of writing (2026-10-04); each doc says where it is an assumption.

## Goal

Give users a simple way to create a bot that runs in the background, and
maximize its uptime, including across bot upgrades and game-client updates.

## Decisions made (kickoff Q&A + follow-up answers)

| Topic | Decision |
|---|---|
| Orchestrator language | **Rust**, as a Kubernetes operator (`kube-rs`); shares types with the bot |
| Credentials | Users submit Backpage user/password **or a session id (`dosid`)**; held in **HashiCorp Vault** (Docker), read by the bot at start; nothing in etcd or the panel DB |
| Proxies | Admin-managed **pool** (auto-assigned, sticky) **and** per-bot override; modelled as CRs (scale is ~50 bots) |
| Observability | `xenomit-core` OTel pipeline in every process; backend **SigNoz** (confirmed) |
| Scale target | **~50 bots** for now |
| Where it runs | **Your current Windows machine, locally**, on Kubernetes (k3d on Docker Desktop), built so the same charts deploy elsewhere later → [11](docs/11-local-deployment.md) |
| CI/CD | GitHub as the trigger, **self-hosted runners on your machine** (free, no inbound exposure); Jenkins is a supported alternative, not the default → [07](docs/07-cicd-and-updates.md) |
| Game session | A second login **kicks the first** → handoff downtime = logout + login, never overlap |
| CAPTCHA | Human-in-the-loop only; user supplies a session id; expected to be rare if sessions are kept alive |
| Users / money | You and friends first (invite-only); plans, quotas and billing are designed in and built later → [12](docs/12-plans-billing-retention.md) |
| Retention | Deleted bots: credentials destroyed immediately, stats/snapshots kept **30 days** in Postgres, then purged |
| Custom modules | Not in v1 (extension point kept) |
| Config edits needing restart | Handled by a handoff (accepted) |
| Dir name | `xenomit-orchestrator` (kebab-case like every sibling repo) |

## The shape of it

```
 Browser ──► xenomit-web-panel ──REST/JWT──► orchestrator-api ─┐
               ▲  ▲                                            │ reconcile
               │  └── SSE: lifecycle events ◄───────────────── │
               │                                  ┌─────────────▼──────────────┐
   data plane  │  (frames, status, stats,         │ operator (leader-elected)  │
   Agent v1 WS │   logs — unchanged protocol)     │ Bot / Rollout / Proxy CRs  │
               │                                  └─────┬───────────────┬──────┘
               │                                        │ pods          │ snapshots
         ┌─────┴──────────┐   control plane (gRPC)      ▼               ▼
         │  botd pod      │◄───────────────────────► control-gateway   Postgres
         │  (headless bot)│   lease · checkpoint · drain · activate
         └─────┬──────────┘
               │ egress: only via the bot's assigned proxy
               ▼
        DarkOrbit (Backpage HTTPS + game TCP)

   every process ──OTLP gRPC──► otel-collector ──► SigNoz
```

## Implementation status

Offline-first Rust workspace (`edition = "2024"`), implemented through Phase
7b. Everything below runs and is tested without a Kubernetes cluster, Vault,
Postgres, or network access to real services.

| Crate | What it is | Status |
|---|---|---|
| `fleet-types` | `XenomitBot`/`BotRollout`/`Proxy` CRDs, phases & desired states, handoff state, epochs/fencing, exit-code map, snapshot types + `SnapshotStore`, tonic `FleetControl` protocol (prost), CRD YAML generator | Implemented, tested |
| `operator` | `BotController` state machine on traits (`PodBackend`/`SecretStore`/`SnapshotStore`/`ControlDirectory`/`Clock`): provision → arm → drain → snapshot → activate handoffs, epoch fencing, crash backoff+jitter+quarantine, exit-code handling, pause/stop, soft-delete + retention, hot config, stale-pod GC, rollout target-image handoffs + `NeedsUpdate` release. `BotRolloutController` (Phase 7b): digest admission, canary → waves under `maxDraining`/`maxOffline`, health gates, login token bucket + jitter, pause/resume/abort/rollback, measured offline windows, `NeedsUpdate` release exempt from `maxOffline` — all on seams (`RolloutStore`/`ImageVerifier`/`MetricsFeed`/`RolloutBotOps`). `KubePods` + `LocalPods` backends, in-memory fakes | Implemented, tested (74 tests) |
| `control-gateway` | tonic `FleetControl` bidirectional session server: hello/lease/epoch fencing, pending-vs-serving authority, command delivery, checkpoint & terminal snapshot ingestion, gateway events. `MemoryStore` offline; `PostgresStore` behind `postgres` feature (dynamic sqlx — no `DATABASE_URL` at compile time) | Implemented, tested over real gRPC on localhost (9 tests) |
| `orchestrator-api` | axum REST API: Ed25519/EdDSA JWT, tenant isolation on `sub` (+admin), bot CRUD & lifecycle (pause/resume/stop/restart/restore), credential rotation (values → `SecretStore` only, DTOs zeroized), soft-delete/undelete + retention-window check, snapshots, validated rollout create + lifecycle actions (pause/resume/abort/rollback as spec flags), proxies, fleet summary, plan-quota enforcement on create/patch (typed `code` on 403s, admin-override), `GET /usage` showback, SSE events incl. rollout spec/phase events, OpenAPI JSON | Implemented, tested (34 tests) |
| `plans` | Phase 8a (docs/12): `Plan`/`Subscription`/`Invite`/`UserAccess`/`UsageRecord` model + `PlanDirectory` read-copy seam, quota engine (`check_create`/`check_spec_change`/`check_running`, typed `QuotaError`), hourly metering (`meter_window` pure fn + `PhaseTimelines`/`UsageStore` traits + `MeterJob`), retention purge job reusing the operator's `purge_due` via `PurgeGate`, showback report, `BillingProvider` trait (interface only — no impl, phase 10). SQL in `xenomit-platform/db/plans/` | Implemented, tested (22 tests) |
| `orchestratord` | Single compositor binary (`--mode local|kube`): Fleet + controllers, `FleetControl` gateway, REST API, dev or real Vault, `LocalPods`/`KubePods`, rollout + proxy drivers, panel pairing, graceful shutdown | Implemented — local mode verified end-to-end against the mock game (scripts/dev-smoke.ps1, 9/9); kube mode compiles, untested on a cluster |
| `orchestrator-api` compat | Panel-shaped `/v1` router alongside `/api/v1`: panel DTOs/SSE, EdDSA JWT roles, panel provisioning callback (docs/08 §8.6) | Implemented, verified against the real panel |

Generated CRDs live in `deploy/crds/` (`cargo run -p fleet-types --bin gen_crds`).

**Works today without external systems:** the full reconcile loop, handoffs,
crash recovery, fencing semantics, the gRPC session protocol, and the REST
API — against in-memory/local backends. The local dev stack
(`scripts\dev-up.ps1` → `dev-smoke.ps1`) runs real `xenomit-botd` children
through it all — see [GETTING-STARTED.md](GETTING-STARTED.md).

**Cluster-dependent (implemented, not exercised against real infra):**
`KubePods` (pod CRUD via `kube`), the Vault `SecretStore`, Postgres snapshot
persistence, SSE bridge to live gateway events. No live Kubernetes, Vault, or
Postgres was contacted during development or testing.

**Not yet implemented:** CI/CD pipelines, OTel wiring — see
[10](docs/10-roadmap.md).

Verification: `cargo test --workspace --all-features` (all green) and
`cargo clippy --workspace --all-targets --all-features -- -D warnings` (clean).

## Documents

| # | Document | Read it for |
|---|---|---|
| 1 | [Architecture](docs/01-architecture.md) | Components, planes, state ownership, bot lifecycle |
| 2 | [Headless bot (`botd`)](docs/02-headless-bot.md) | Extracting the runtime from the GPUI app, config file, startup, exit codes |
| 3 | [Graceful handoff & snapshots](docs/03-graceful-handoff.md) | The drain → snapshot → logout → restore protocol; fencing; failure modes |
| 4 | [Orchestrator](docs/04-orchestrator.md) | CRDs, controllers, placement, uptime machinery, API |
| 5 | [Networking & proxies](docs/05-networking-and-proxies.md) | Per-bot egress, proxy pool, NetworkPolicy |
| 6 | [Observability](docs/06-observability.md) | `xenomit-core` gaps, trace/metric conventions, SigNoz |
| 7 | [CI/CD & game updates](docs/07-cicd-and-updates.md) | Packet-updater → image → canary → fleet rollout |
| 8 | [Panel integration](docs/08-panel-integration.md) | Admin role, user "create bot" flow, schema and API changes |
| 9 | [Security](docs/09-security.md) | Secrets, threat model, isolation |
| 10 | [Roadmap](docs/10-roadmap.md) | Phased delivery with exit criteria |
| 11 | [Local deployment](docs/11-local-deployment.md) | k3d on your machine, Vault in Docker, resource budget, path to a real deployment |
| 12 | [Plans, billing & retention](docs/12-plans-billing-retention.md) | Invite-only access, plans/quotas, metering, billing later, 30-day retention |
| – | [Decision log (ADRs)](docs/adr.md) | Every non-obvious choice and its rejected alternatives |

Remaining unknowns are listed in [10-roadmap.md](docs/10-roadmap.md#assumptions-to-validate-early); none block phases 0–3.

## Findings from the existing code that shaped the design

1. **`xenomit-bot/src/bot/behavior.rs` (2.8k lines) has zero GPUI references**;
   only `view.rs`/`live_map.rs`/`module_settings.rs`/`mod.rs` do. Extracting a
   headless runtime is a refactor, not a rewrite.
2. **A cooperative interruption path already exists.** `run_loop` turns a
   `HostAction::RequestRefresh` into a permission ladder (active takeover →
   rule operations → pickup → attack → `ModuleRunner::interruption_status`) and
   only acts on `Ready`. Handoff reuses this ladder with a new
   `InterruptionReason::Handoff`; custom modules already default to `Deferred`.
3. **Module state is not serializable today.** `BehaviorModule` has no
   export/restore, and `TaskSequence` holds closures. Handoff needs a
   checkpoint/restore API; sequences resume **by step name at a step boundary**.
4. **Time is monotonic-relative** (`now_ms = started.elapsed()`); snapshots must
   store durations, not timestamps, and rebase on restore.
5. **Statistics are a local JSONL file under `%APPDATA%`** with a 100k-event
   in-memory window. Pods are ephemeral, so stats become an event-sourced
   stream (durable at the panel) and the snapshot only carries derived
   counters plus a cursor.
6. **The panel's agent protocol and token model already fit.** Headless bots
   can speak Agent Protocol v1 unchanged; only the pairing step is replaced by
   orchestrator-issued tokens.
7. **`xenomit-core` has gaps for this plan**: traces and logs only (no
   metrics), no custom resource attributes, no inject/extract helpers, and
   **secret redaction only covers the console layer, not the OTLP export.** See
   [06](docs/06-observability.md).
8. **Flash/OCX is Windows-only and Ruffle needs a window**, so pod bots use
   `client = "packet"` (already the "VPS-friendly mode" in the panel spec).
