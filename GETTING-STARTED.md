# Getting Started — running the Xenomit fleet for real

Three ways to run the system on a Windows 11 machine, cheapest first.
Pick the one you need; each section is self-contained.

| Path | What it proves | Needs | Verified |
|------|----------------|-------|----------|
| **A — Local dev stack** | Real `xenomit-botd` child processes driven by the real operator + API + panel, no Kubernetes | Rust, Node.js | ✅ end-to-end |
| **B — Standalone botd** | One bot, one process, config file + secrets. No orchestrator at all | Rust | ✅ vs mock game |
| **C — k3d platform** | The production shape: operator + Vault + Postgres + GitOps on a local cluster | Docker Desktop, kubectl, k3d, helm | ⚠️ manifests + image builds verified; cluster bring-up **not** run |

Verification legend: ✅ = executed and observed on this machine ·
⚠️ UNVERIFIED = written from code review / dry-run only — the step is
believed correct but was not executed, for the stated reason.

---

## Prerequisites

Everything below assumes the sibling checkouts sit next to each other:

```
repos\
  xenomit-orchestrator\      this repo
  xenomit-botd\              the headless bot
  xenomit-mock-game\         fake Backpage + game + proxy for dev
  xenomit-web-panel\         Next.js admin/user UI
```

Tools used (install once, with winget — none of the scripts install system
software themselves):

```powershell
winget install Rustlang.Rustup        # rustc/cargo (1.98 used here)
winget install OpenJS.NodeJS.LTS      # node + npm for the panel
winget install Git.Git
# Path C only:
# Docker Desktop — manual installer, https://docs.docker.com/desktop (WSL2 backend)
winget install Kubernetes.kubectl
winget install k3d
winget install Helm.Helm
```

---

## Repo map — where things live

```
xenomit-orchestrator\
  fleet-types\        CRD types (XenomitBot/BotRollout/Proxy), phases,
                      handoff/epoch/snapshot types, gRPC proto, CRD generator
  operator\           BotController state machine + BotRolloutController
                      on traits; PodBackend: KubePods / LocalPods;
                      SecretStore: Vault client / in-memory / dev-file
  control-gateway\    tonic FleetControl server (hello/lease/fencing,
                      commands, checkpoints, terminal snapshots);
                      MemoryStore + PostgresStore(feature "postgres")
  orchestrator-api\   axum REST API. /api/v1 (CRD-shaped) and /v1
                      (panel-compatible: compat.rs), EdDSA JWT auth,
                      SSE /v1/events, plans/quotas, probes /livez /readyz
  orchestratord\      the single binary that composes everything:
                      --mode local (LocalPods + dev Vault) or
                      --mode kube  (kube client + real Vault)
  plans\              plans/quotas/metering/retention (Phase 8a). SQL is in xenomit-platform\db\plans
  e2e\                lifecycle tests over a real compiled botd
  deploy\crds\        generated CRD YAML (cargo run -p fleet-types --bin gen_crds)
  scripts\            dev-up.ps1 / dev-down.ps1 / dev-smoke.ps1  (path A)
  ci\                 orchestrator Dockerfile and the Rust workflow
  docs\               the design corpus (01–12 + ADRs + rpo-rto)
  .devstack\          created by dev-up: state, logs, pids (gitignored)

xenomit-botd\         the bot itself (config schema in schema/botd.schema.json)
xenomit-mock-game\    fake Backpage HTTP + game TCP + egress proxy +
                      control API, seeded accounts
xenomit-web-panel\    Next.js app; orchestrator client in src/lib/orchestrator.ts
```

---

## Path A — local dev stack (no Kubernetes)

What it is: `xenomit-orchestratord --mode local` running the **real**
operator/controller, control gateway, REST API and a file-backed dev Vault;
`LocalPods` spawns real `xenomit-botd` child processes; `xenomit-mock-game`
plays Backpage/game/proxy; the panel talks to the real API.

```
 browser ──► panel :3000 ──REST+EdDSA JWT──► orchestratord :8080 ─┐
                ▲  ▲ SSE /v1/events                              │ reconcile
   data-plane   │  └─────────────────────────── pairing callback  │
   agent ws     │                              ┌─────────────────┴───┐
                └────────── instance token ────│  BotController      │
                                               │  (operator traits)  │
 mock-game      ┌── botd child #N ◄─gRPC───────│ + FleetControl :8443│
  :5610-:5613   │   (real exe, per-bot cfg)     │ + dev Vault :8200   │
               └── logs → .devstack\logs\*.log └─────────────────────┘
```

### Commands

```powershell
cd xenomit-orchestrator
scripts\dev-up.ps1        # build all three binaries, start everything
scripts\dev-smoke.ps1     # full lifecycle smoke (see below)
scripts\dev-down.ps1      # kill everything it started
scripts\dev-down.ps1 -Clean   # also delete .devstack state
```

`dev-up` builds `xenomit-mock-game`, `xenomit-botd`, `orchestratord`
(`-NoBuild` skips), refuses to start if a port is taken, then starts in
order: mock game → panel (`npm run dev` with `ORCHESTRATOR_URL`) →
orchestratord. It writes `.devstack\env.json` (ports/accounts) and
`.devstack\pids\*.pid`.

`dev-smoke` mints an admin EdDSA JWT from the panel's
`.orchestrator-dev.json` (the same Ed25519 signature path the panel uses,
via node `crypto.sign`) and walks the whole lifecycle:

```
PASS readyz                         orchestratord healthy
PASS proxies: seed pool proxy       POST /v1/proxies (mock egress proxy)
PASS bots: create                   POST /v1/bots
PASS bot reaches Running            arm → activate → login → attach → run
PASS pause -> Paused                clean drain + snapshot
PASS resume -> Running              new epoch
PASS planned handoff (restart)      drain → new botd → restore → Running
PASS force-kill botd (kill -9)      crash detected → auto restore → Running
PASS teardown (DELETE)              soft-delete
```

✅ **Verified on this machine:** `9 passed, 0 failed`. botd logs showed
`arm → proxy verified → activated epoch=N → login → attach → run`, the
planned handoff advanced the epoch, and a force-killed child was restored
onto the next epoch automatically.

### Ports (path A defaults — all overridable with `-…Port` flags)

| Port | Process | Purpose |
|-----:|---------|---------|
| 3000 | panel (node) | UI |
| 8080 | orchestratord | REST API (`/api/v1` + `/v1`, probes `/livez` `/readyz` `/startupz`) |
| 8443 | orchestratord | FleetControl gRPC (botd control streams) |
| 8200 | orchestratord | dev Vault (file-backed — **dev only, not real Vault**) |
| 5610 | mock game | fake Backpage HTTP |
| 5611 | mock game | fake game TCP |
| 5612 | mock game | fake egress proxy |
| 5613 | mock game | control API (`/status`, `/accounts`, faults) |

Mock accounts seeded by dev-up: `bot`/`bot-pass` (uid 1001) and
`bot2`/`bot-pass2` (uid 1002). **Use a different account per bot** — like
the real game, the mock kicks the earlier session when the same account
logs in twice; two bots on one account fight forever
(`lost readiness → login → kicked`).

### Using the panel

1. `http://localhost:3000` → **Dev Login** (the panel's `.env` enables the
   dev provider — no Discord needed).
2. Fleet page → create a managed instance. The panel calls
   `POST /api/instances {kind:"managed"}` which talks to the real
   orchestrator end-to-end, then the orchestrator calls back
   `POST /api/orchestrator/instances` to mint the bot's instance token.
3. The bot then opens the Agent-Protocol v1 websocket to the panel and
   streams live status — verified earlier with real balances/uptime.

Panel logs are `.devstack\logs\panel.out.log`; the
`[orch-events] connected to orchestrator event stream` line is the SSE
bridge into `/v1/events` working.

> **Pairing quirk:** creating a bot through the raw `/v1` API with an
> `owner` that isn't an existing panel *user id* still creates and runs
> the bot, but the pairing callback 404s (`owner not found`) and the bot
> runs panel-less — by design. Pair via the panel UI/API instead, or set
> `owner` to the panel user's `id` (a `cuid`, see `db.user`).

### Raw API access (no panel)

`dev-smoke.ps1` is the reference — it shows the exact JWT mint + request
bodies. Summary: `POST /v1/bots` `{owner, name, account:{server,authMode},
credentials:{username,password|dosid}, config:{settings:{}}, proxy:{mode}}`,
`PATCH /v1/bots/{id}` `{desiredState:"paused"|"running"|"stopped"}`,
`POST /v1/bots/{id}/restart`, `DELETE /v1/bots/{id}`,
`GET /v1/events` (SSE), `GET /v1/fleet/summary`, `/v1/proxies`,
`/v1/rollouts`. The same handlers also exist CRD-shaped under `/api/v1`.

### Real game instead of the mock

Point `dev-up` at real endpoints by dropping the two `--botd-env
XENOMIT_TESTING_*` flags (edit the script or run orchestratord by hand with
the same flags minus them). Bots then need real credentials and, for the
game side, `account.server` = your real Backpage host. Real-game traffic
was ⚠️ UNVERIFIED in this session — the mechanics are identical (the
testing overrides only swap URLs).

---

## Path B — standalone `xenomit-botd` (no orchestrator)

One bot, one process. `botd` reads a config file, resolves its secrets,
logs into the game and runs its module. ✅ Verified against the mock game
(arm → login → attach → run; the process then fought over the shared
account exactly as documented — with its own account it stays attached).

### 1. Build

```powershell
cd ..\xenomit-botd
cargo build --release        # target\release\xenomit-botd.exe
```

`xenomit-botd --print-schema` prints the JSON Schema (also committed at
`xenomit-botd\schema\botd.schema.json`); `--help` lists the CLI
(`--config <path>` → `$XENOMIT_BOTD_CONFIG` → `/etc/xenomit/botd.json`).

### 2. Credentials file — never in the config, never committed

`secrets\account.json` (password auth):

```json
{"username": "<game-user>", "password": "<game-password>"}
```

or session auth:

```json
{"dosid": "<session id>"}
```

`user:pass`, `KEY=value` and a bare raw dosid are also accepted
(`xenomit-botd\src\secrets.rs`). Git-ignore this file; botd re-reads it on
each auth attempt so rotation doesn't need a restart.

Optional proxy file `secrets\proxy.json`:
`{"scheme":"http","host":"…","port":8080,"username":"…","password":"…"}`
or `{"url":"socks5://user:pass@host:1080"}`.

### 3. Config `botd.json`

Minimal production-shape config (verified fields):

```json
{
  "schema": 1,
  "bot":    { "id": "bot-01", "tenant": "me", "label": "my bot" },
  "account": {
    "server": "int1.darkorbit.com",
    "auth": "password",
    "credentials": { "file": "C:/path/to/secrets/account.json" }
  },
  "client": { "kind": "packet" },
  "settings": {},
  "autostart": true,
  "network": { "proxy": { "file": "C:/path/to/secrets/proxy.json" },
               "require_proxy": false },
  "state_dir": "C:/path/to/botd-state",
  "health": { "bind": "127.0.0.1:8787" }
}
```

- `account.auth`: `"password"` or `"session"`.
- `settings` is the exact `BotSettings`/`modules.json` shape the GUI
  persists; `{}` = defaults.
- **Do not add `control`** — that block is managed mode (orchestrator
  control stream). Its absence is what makes this standalone.
- Optional: `panel` (base_url + instance-token secret ref), `vault`
  (real Vault only; resolver exists for `vault` secret refs),
  `handoff` (drain/hard/checkpoint budgets), `telemetry.otlp_endpoint`.
- `testing` (`backpage_base_url`, `front_base_url`, `game_endpoint`)
  exists **only** for the mock game — never a production key.

### 4. Getting a `dosid` (session auth)

Log into the game normally in a browser → DevTools → Application →
Cookies → copy the `dosid` value for your Backpage host → put it in the
credentials file with `"auth": "session"`. A dosid can't be self-refreshed
past expiry; when it dies, mint a new one the same way (or switch to
password auth, which botd refreshes itself).

### 5. Run

```powershell
.\target\release\xenomit-botd.exe --config botd.json
```

Health endpoints (localhost only): `/startupz` (armed), `/readyz`
(attached + data plane live), `/livez` (supervisor heartbeats).
Meaningful exit codes: `0` clean drain · `64` bad config/secrets ·
`65` credentials rejected · `66` CAPTCHA (human) · `67` fenced by another
login · `69` proxy required but unreachable · `70` strict-restore failed.
Full list: `xenomit-botd\README.md`.

Against the **real game**: set `account.server` to your Backpage host
(e.g. `int1.darkorbit.com`), keep `auth`/`credentials` real, delete
`testing`. Not exercised in this session (no real account used —
⚠️ UNVERIFIED against live game servers; the code path is identical, only
the URLs differ).

---

## Path C — the k3d platform (production shape)

The cluster that runs this binary is `xenomit-platform`: one
`xenomit-orchestratord` pod in `xenomit-system` (operator + API :8080 +
control gateway :8443), bot pods in `xenomit-bots` spawned by the
operator, Vault in Docker beside the cluster, Postgres and the
OpenTelemetry collector in-cluster, and OpenObserve for observability.
Argo CD syncs the orchestrator from `services/gitops` in that repository.

```
                          ┌──────────── Windows host ────────────┐
 browser ──► port-forward 3000 ──► web-panel (xenomit-system)
                                       │ REST + EdDSA JWT
   botd pods (xenomit-bots) ◄─gRPC:8443─► xenomit-orchestratord (xenomit-system)
        │ k8s api                        │ kube client │ REST :8080
        │ projected SA token             │             │ Vault JWT (audience=vault)
        ▼                                ▼             ▼
   game/egress proxy              Vault (docker, :8200)  Postgres (xenomit-data)
                                  └─ k3d cluster: 1 server, network-policy ON ─┘
```

### C.0 — honest status

| Step | Status |
|------|--------|
| Manifests/gitops reviewed + defects fixed | ✅ (see below) |
| `Dockerfile.orchestrator` / `Dockerfile.botd` / `.dockerignore` | ✅ both images build (`xenomit-orchestrator:local` 77 MB, `xenomit-botd` 62 MB) |
| k3d cluster, Vault, Postgres, OpenObserve, Argo CD | follow `xenomit-platform\ops\BOOTSTRAP-IAC.md` |
| Bot pods from the operator in-cluster | ⚠️ UNVERIFIED — `KubePods` is implemented and unit-tested with fakes; no real cluster run |

Everything below is the intended order; unverified steps are marked.

### C.1 — Prerequisites

From a clean Windows 11 machine:

```powershell
winget install Kubernetes.kubectl
winget install k3d
winget install Helm.Helm
winget install Git.Git
# Docker Desktop: manual install (WSL2 backend) — https://docs.docker.com/desktop
# optional: openssl comes with Git for Windows
```

Then `%UserProfile%\.wslconfig` (applies to the Docker/WSL2 VM — restart
with `wsl --shutdown` after editing). See `xenomit-platform\ops\wslconfig.md`; for a 32 GB
host:

```ini
[wsl2]
memory=24GB
processors=10
swap=8GB
localhostForwarding=true
```

### C.2 — Bootstrap

Cluster bootstrap, Vault, Gitea, Argo CD, Atlantis, and OpenTofu live in
`xenomit-platform`. Follow `xenomit-platform\ops\BOOTSTRAP-IAC.md`.

```powershell
cd ..\xenomit-platform\ops
.\bootstrap-iac.ps1 -DryRun
.\bootstrap-iac.ps1
```

That script publishes this platform repository and applies it. Argo CD
application `xenomit-control-plane` then syncs
`services\gitops\overlays\local`, which is the orchestrator Deployment.
`xenomit-panel` syncs `services\gitops\panel`.

Vault unseal keys are `xenomit-platform\ops\vault\cluster-keys.json`.

### C.3 — Build and push the images

Build contexts are the **repos root** (path deps cross repos):

```powershell
cd ..            # repos root — the parent of all xenomit-* checkouts
docker build -f xenomit-orchestrator\ci\docker\Dockerfile.orchestrator -t xenomit-orchestrator:local .
docker build -f xenomit-botd\Dockerfile                             -t xenomit-botd:local .
docker build -f xenomit-platform\services\docker\Dockerfile.web-panel -t xenomit-web-panel:local .
```

For pods to see them, import into k3d or push to `localhost:5000` and pin
**digests** in `xenomit-platform\services\gitops\overlays\local\kustomization.yaml`
(`name → newName → digest`). botd is absent from that overlay: bots roll via
`BotRollout`, not Deployments. The example rollout is
`xenomit-platform\services\gitops\rollouts\botd-rollout.example.yaml`.

### C.4 — Ops-managed secrets the Deployments need

`xenomit-api-jwt` (namespace `xenomit-system`) is referenced by both the
orchestratord and panel pods and is created by `bootstrap-iac.ps1`, not by
the GitOps manifests. It has three keys: `jwt-private.pem` /
`jwt-public.pem` and `service-token`.

`postgres-auth` in `xenomit-system` (`orchestrator-url`, `panel-url`) is
created by the same script.

### C.5 — Day-2 operations

Local URLs, port-forwards, and credential commands are in
`xenomit-platform\ops\LOCAL-ACCESS.md`. Teardown is
`xenomit-platform\ops\teardown.ps1`. Backup drills are
`xenomit-platform\ops\drills\`.

### Runbook references

| Where | What |
|-------|------|
| `xenomit-platform\ops\BOOTSTRAP-IAC.md` | current cluster bootstrap |
| `xenomit-platform\ops\LOCAL-ACCESS.md` | URLs and credentials |
| `docs\11-local-deployment.md` | the platform design + resource budget |
| `docs\09-security.md` | secrets model, threat model |
| `xenomit-platform\ops\README.md` | ops scripts overview |
| `xenomit-platform\ops\drills\README.md` | drill mechanics + report format |
| `xenomit-platform\ops\wslconfig.md` | memory sizing |

---

## Troubleshooting (issues actually hit this session)

| Symptom | Cause | Fix |
|---------|-------|-----|
| Bots flap `lost readiness → login → attach` every few seconds, fleet phase stays Running | Two bots on the **same game account** — second login kicks the first (real-game rule, enforced by the mock) | Give each bot its own account (`bot`, `bot2`, …) |
| botd exits 64 "invalid secret" at arm | `panel` block present but its `instance` secret is empty | fixed — the operator now only renders `panel` when a real token exists; on old builds remove the empty `instance` Vault key |
| `POST /v1/*` → 404 | compat router was mounted at root instead of `/v1` | fixed — nested under `/v1` |
| Panel shows an empty fleet though bots exist | `owner` ≠ panel user id — tenant isolation is real | log in as the owner, or create via the panel |
| `POST /api/orchestrator/instances` → 404 | `owner` is not an existing panel user `id` | expected: bot runs panel-less; pair via panel-side create |
| `docker build` fails "kube requires rustc 1.89 / sqlx 1.94" | `rust:1.88-bookworm` too old for the lockfile | fixed — `rust:1.98-bookworm` in both Dockerfiles |
| `secretKeyRef: postgres-auth` not found in `xenomit-system` | secrets can't cross namespaces (`xenomit-data`) | fixed — bootstrap creates the consumer copy with `orchestrator-url`/`panel-url` |
| `cargo build` "file in use (os error 32)" on Windows | antivirus/indexer or a stale test binary locking `target\` | retry; `Get-Process` for stray `xenomit-*.exe` |
| PowerShell dies on `cargo build` with `NativeCommandError` | cargo logs to stderr + `ErrorActionPreference=Stop` | scripts redirect builds to `.devstack\logs\build-*.log` via `cmd /c` |
| `Start-Process -ArgumentList` "null value" | `$Args` collides with PowerShell's automatic `$args` | use any other name (scripts already do) |
| orchestratord won't Postgres-connect at boot | `DATABASE_URL` injected empty | fixed — empty DSN falls back to MemorySnapshotStore |
| "port … already in use" from dev-up | stale process from an earlier run | `scripts\dev-down.ps1` (kills recorded pids + dev-owned botd children) |

## Known limitations

- **Single replica orchestratord**, `Recreate` strategy — there is no
  leader election; never run two reconcilers at once.
- The **dev Vault** (`--mode local`) is a file-backed KV under
  `.devstack` — same API, not a secrets engine; never use it for real
  credentials.
- **Panel pairing needs a panel user id** as bot `owner` — raw-API bots
  without one run fine but never appear paired (see quirk above).
- Mock game = happy path: no real anti-bot pressure, version-hash checks
  are stubbed, single-map world.
- In-cluster bot pods (`KubePods`), Vault JWT auth, Postgres snapshot
  store and Argo reconciliation are implemented + unit-tested but
  ⚠️ UNVERIFIED against live infrastructure.
- The panel container image was reviewed but not built here; Next.js runs
  in dev mode on path A (`npm run dev`, tsx watch).
- `xenomit-api-jwt` is provisioned once by `ops/provision-api-jwt.ps1`,
  called by bootstrap when `-WithArgoCD` is selected.
- `docs/` describe a *future* three-Deployment split
  (operator/gateway/API); the code today is intentionally one
  `xenomit-orchestratord` process. GitOps manifests match the code.
