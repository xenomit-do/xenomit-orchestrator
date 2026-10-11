//! `xenomit-orchestratord` — the single-process Xenomit control plane.
//!
//! One binary carries every control-plane concern because `Gateway` is
//! both the bot-facing tonic service *and* the operator's in-process
//! `ControlDirectory` — there is no remote directory seam yet (docs/04
//! §4.7's split Deployment needs one). What runs:
//!
//! ```text
//!   axum API (:8080) ──> Fleet ──> BotController
//!      /api/v1 + /v1      │            │ ensure_pod      ┌── LocalBackend  (local)
//!   tonic :8443 ◄─────────┼─ Gateway   │      ──spawn──► ├── KubePodBackend(kube)
//!   driver task ◄─────────┘  (also     │                 │      │
//!                             the      ▼                 ▼      ▼
//!                        ControlDir) snapshots        botd ═════╝ control stream
//!   dev Vault (:8200, local mode only) ◄── botd resolves secret refs
//! ```
//!
//! Modes:
//! - `local` — LocalBackend (real botd child processes), DevVault,
//!   in-memory repos + snapshots. `scripts/dev-up.ps1` runs this.
//! - `kube` — KubePodBackend, real Vault, CR-backed repos. The
//!   control-plane Deployment's `command`.
mod devvault;
mod driver;
mod kuberepo;
mod vaultstore;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::broadcast;

use control_gateway::{Gateway, GatewayConfig, MemorySnapshotStore};
use operator::backends::kube_backend::DeployEnv;
use operator::backends::local::{LocalBackend, SpawnSpec};
use operator::proxy::ProxyController;
use operator::rollout::BotRolloutController;
use operator::traits::{AllowAllVerifier, NoMetrics, PodBackend, SystemClock};
use operator::{BotController, BotLocks, Pool};
use orchestrator_api::compat::PanelLink;
use orchestrator_api::events::FleetEventSink;
use orchestrator_api::{AppState, AuthConfig, BotOps, BotRepo, Fleet, InMemoryBots, InMemoryCrs};

const USAGE: &str = "\
xenomit-orchestratord — single-process Xenomit control plane.

USAGE:
  xenomit-orchestratord [options]

MODES:
  --mode local|kube               backend + stores (default local)

COMMON:
  --data-dir <dir>                state dir (default ./orchestratord-data)
  --api-bind <host:port>          REST API (default 127.0.0.1:8080)
  --gateway-bind <host:port>      bot control stream (default 127.0.0.1:8443)
  --public-key <file.pem>         Ed25519 JWT public key (else --panel-dev-file)
  --panel-dev-file <file>         panel's .orchestrator-dev.json: pubkey +
                                  serviceToken (default: probe
                                  ../xenomit-web-panel/.orchestrator-dev.json)
  --panel-url <url>               e.g. http://127.0.0.1:3000 — enables
                                  managed-instance pairing + botd panel link
  --service-token <tok>           orchestrator→panel bearer (else env
                                  ORCHESTRATOR_SERVICE_TOKEN, else dev file)
  --stable-image <img>            channel image (default xenomit-botd:local)
  --canary-image <img>            canary image (default = stable)
  --lease-grace <secs>            post-lease grace for handoffs (default 5
                                  local / 30 kube)

LOCAL:
  --vault-bind <host:port>        dev Vault (default 127.0.0.1:8200)
  --botd <path>                   xenomit-botd binary (env XENOMIT_BOTD_EXE;
                                  default: sibling repo debug build)
  --botd-env KEY=VALUE            extra env for botd children, repeatable
                                  (XENOMIT_TESTING_*, XENOMIT_EXIT_IP_URL…)

KUBE:
  --bots-namespace <ns>           bot pods/CRs namespace (default xenomit-bots)
  --control-endpoint <url>        injected into bot pods (default
                                  http://control-gateway.xenomit-system:8443)
  --vault-addr <url>              real Vault (default
                                  https://vault.xenomit-system:8200)
  --vault-mount <name>            jwt auth mount (default jwt)
  --vault-role <role>             orchestrator's jwt role (default orchestrator)
  --vault-token-file <path>       orchestrator SA token (default
                                  /var/run/secrets/vault/token)
  --database-url <dsn>            PostgresSnapshotStore (feature postgres)
";

struct Args {
    mode: Mode,
    data_dir: PathBuf,
    api_bind: SocketAddr,
    gateway_bind: SocketAddr,
    vault_bind: SocketAddr,
    botd: Option<PathBuf>,
    botd_env: Vec<(String, String)>,
    public_key: Option<PathBuf>,
    panel_dev_file: Option<PathBuf>,
    panel_url: Option<String>,
    service_token: Option<String>,
    stable_image: String,
    canary_image: Option<String>,
    lease_grace: Option<u64>,
    bots_namespace: String,
    control_endpoint: Option<String>,
    vault_addr: Option<String>,
    vault_mount: String,
    vault_role: String,
    vault_token_file: Option<PathBuf>,
    database_url: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Local,
    Kube,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut a = Args {
            mode: Mode::Local,
            data_dir: PathBuf::from("orchestratord-data"),
            api_bind: "127.0.0.1:8080".parse().unwrap(),
            gateway_bind: "127.0.0.1:8443".parse().unwrap(),
            vault_bind: "127.0.0.1:8200".parse().unwrap(),
            botd: None,
            botd_env: vec![],
            public_key: None,
            panel_dev_file: None,
            panel_url: None,
            service_token: None,
            stable_image: "xenomit-botd:local".into(),
            canary_image: None,
            lease_grace: None,
            bots_namespace: "xenomit-bots".into(),
            control_endpoint: None,
            vault_addr: None,
            vault_mount: "jwt".into(),
            vault_role: "orchestrator".into(),
            vault_token_file: None,
            database_url: None,
        };
        let mut it = std::env::args().skip(1);
        macro_rules! val {
            () => {
                it.next().ok_or_else(|| "missing flag value".to_string())?
            };
        }
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--mode" => {
                    a.mode = match val!().as_str() {
                        "local" => Mode::Local,
                        "kube" => Mode::Kube,
                        m => return Err(format!("--mode must be local|kube (got {m})")),
                    }
                }
                "--data-dir" => a.data_dir = PathBuf::from(val!()),
                "--api-bind" => {
                    a.api_bind = val!().parse().map_err(|e| format!("--api-bind: {e}"))?
                }
                "--gateway-bind" => {
                    a.gateway_bind = val!().parse().map_err(|e| format!("--gateway-bind: {e}"))?
                }
                "--vault-bind" => {
                    a.vault_bind = val!().parse().map_err(|e| format!("--vault-bind: {e}"))?
                }
                "--botd" => a.botd = Some(PathBuf::from(val!())),
                "--botd-env" => {
                    let kv = val!();
                    let Some((k, v)) = kv.split_once('=') else {
                        return Err(format!("--botd-env needs KEY=VALUE (got {kv})"));
                    };
                    a.botd_env.push((k.to_string(), v.to_string()));
                }
                "--public-key" => a.public_key = Some(PathBuf::from(val!())),
                "--panel-dev-file" => a.panel_dev_file = Some(PathBuf::from(val!())),
                "--panel-url" => a.panel_url = Some(val!()),
                "--service-token" => a.service_token = Some(val!()),
                "--stable-image" => a.stable_image = val!(),
                "--canary-image" => a.canary_image = Some(val!()),
                "--lease-grace" => {
                    a.lease_grace = Some(val!().parse().map_err(|e| format!("--lease-grace: {e}"))?)
                }
                "--bots-namespace" => a.bots_namespace = val!(),
                "--control-endpoint" => a.control_endpoint = Some(val!()),
                "--vault-addr" => a.vault_addr = Some(val!()),
                "--vault-mount" => a.vault_mount = val!(),
                "--vault-role" => a.vault_role = val!(),
                "--vault-token-file" => a.vault_token_file = Some(PathBuf::from(val!())),
                "--database-url" => a.database_url = Some(val!()),
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                other => return Err(format!("unknown flag {other}\n\n{USAGE}")),
            }
        }
        Ok(a)
    }
}

/// The panel's `.orchestrator-dev.json` (src/lib/orchestrator.ts).
#[derive(serde::Deserialize)]
struct PanelDevFile {
    #[serde(rename = "publicKey")]
    public_key: String,
    #[serde(rename = "serviceToken")]
    service_token: String,
}

/// Resolve JWT verification: explicit PEM > panel dev file > error.
/// A dev file also supplies the service token for `PanelLink`.
fn resolve_auth(args: &Args) -> Result<(AuthConfig, Option<String>), String> {
    if let Some(path) = &args.public_key {
        let pem = std::fs::read(path).map_err(|e| format!("{path:?}: {e}"))?;
        let auth = AuthConfig::from_ed_pem(&pem)
            .map_err(|e| format!("invalid public key {}: {e}", path.display()))?;
        return Ok((auth, None));
    }
    let dev_path = args.panel_dev_file.clone().or_else(|| {
        let probe = PathBuf::from("../xenomit-web-panel/.orchestrator-dev.json");
        probe.exists().then_some(probe)
    });
    let Some(path) = dev_path else {
        return Err(
            "no JWT public key: pass --public-key <pem> or --panel-dev-file \
             <panel>/.orchestrator-dev.json (run the panel once to generate it)"
                .into(),
        );
    };
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let dev: PanelDevFile =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let auth = AuthConfig::from_ed_pem(dev.public_key.as_bytes())
        .map_err(|e| format!("invalid public key in {}: {e}", path.display()))?;
    Ok((auth, Some(dev.service_token)))
}

/// Where the compiled `xenomit-botd` lives: `--botd` → `XENOMIT_BOTD_EXE`
/// → the sibling repo's `target-4b/debug` build (the e2e convention).
fn botd_exe(args: &Args) -> Result<PathBuf, String> {
    if let Some(p) = &args.botd {
        return Ok(p.clone());
    }
    if let Some(p) = std::env::var_os("XENOMIT_BOTD_EXE") {
        return Ok(PathBuf::from(p));
    }
    let exe_name = if cfg!(windows) {
        "xenomit-botd.exe"
    } else {
        "xenomit-botd"
    };
    let exe = std::env::current_dir()
        .map_err(|e| e.to_string())?
        .join("../xenomit-botd/target-4b/debug")
        .join(exe_name);
    if exe.exists() {
        return Ok(exe);
    }
    Err(format!(
        "xenomit-botd not found: pass --botd <path> or set XENOMIT_BOTD_EXE \
         (looked in {})",
        exe.display()
    ))
}

/// The six-seam controller type is fixed per mode at wiring time; the
/// driver/Fleet share it through `Arc<dyn BotOps>`.
async fn serve(
    api_bind: SocketAddr,
    gateway_bind: SocketAddr,
    gateway: Gateway,
    fleet: Fleet,
    auth: AuthConfig,
    panel: Option<PanelLink>,
    locks: Arc<BotLocks>,
    bots: Arc<dyn BotRepo>,
    ops: Arc<dyn BotOps>,
) -> Result<(), String> {
    // -- tonic control stream ------------------------------------------
    let gw_listener = TcpListener::bind(gateway_bind)
        .await
        .map_err(|e| format!("gateway bind {gateway_bind}: {e}"))?;
    let gw_service = gateway.clone();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(gw_service.into_service())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(gw_listener))
            .await;
    });

    // -- REST API -------------------------------------------------------
    let mut state = AppState::new(fleet, auth);
    if let Some(link) = panel {
        state = state.with_panel(link);
    }
    let api_listener = TcpListener::bind(api_bind)
        .await
        .map_err(|e| format!("api bind {api_bind}: {e}"))?;
    let state = Arc::new(state);
    tokio::spawn(async move {
        let _ = axum::serve(api_listener, orchestrator_api::router(state)).await;
    });

    // -- driver ---------------------------------------------------------
    let drv = driver::Driver::new(bots, ops, locks, gateway.subscribe());
    tokio::spawn(drv.run());
    Ok(())
}

/// The panel pairing seam: `--panel-url` + service token (flag > env >
/// dev file) → `PanelLink` for `/v1` create + `panel.base_url` in botd.
fn panel_link(args: &Args, dev_token: Option<String>) -> Option<PanelLink> {
    let url = args.panel_url.clone()?;
    let token = args
        .service_token
        .clone()
        .filter(|t| !t.is_empty())
        .or_else(|| std::env::var("ORCHESTRATOR_SERVICE_TOKEN").ok())
        .or(dev_token)?;
    Some(PanelLink {
        url,
        service_token: token,
        http: reqwest::Client::new(),
    })
}

fn gateway_cfg(args: &Args) -> GatewayConfig {
    GatewayConfig {
        lease_grace: Duration::from_secs(args.lease_grace.unwrap_or(match args.mode {
            Mode::Local => 5,
            Mode::Kube => 30,
        })),
        ..GatewayConfig::default()
    }
}

/// Drop pod-local state that must not outlive the control plane. The local
/// epoch authority is `status.epoch` on the in-memory bot CRs — a restarted
/// daemon re-issues epochs from 1 — while botd's fencing files
/// (`botd-state/epochs/<bot>.txt`) are monotone by design and still carry
/// the previous run's watermark. Every fresh epoch-1 pod would then read
/// the stale file on its first `probe_fence` tick and self-exit 67 with no
/// log line, wedging the bot. Rendered pod configs (`pods/`) die with the
/// pods they describe. The snapshot/statistics mirrors and the dev Vault
/// are records, not authority — they survive.
fn reset_local_pod_state(dir: &Path) -> Result<(), String> {
    for sub in ["pods", "botd-state/epochs"] {
        let p = dir.join(sub);
        if p.exists() {
            std::fs::remove_dir_all(&p).map_err(|e| format!("{}: {e}", p.display()))?;
        }
    }
    Ok(())
}

async fn run_local(args: &Args) -> Result<(), String> {
    let dir = args.data_dir.clone();
    std::fs::create_dir_all(&dir).map_err(|e| format!("data dir: {e}"))?;
    reset_local_pod_state(&dir)?;

    // --- dev Vault -----------------------------------------------------
    let kv_file = dir.join("vault-kv.json");
    let vault = devvault::DevVault::start(args.vault_bind, kv_file)
        .await
        .map_err(|e| format!("dev vault bind {}: {e}", args.vault_bind))?;
    let vault = Arc::new(vault);
    let sa_jwt = dir.join("bot-sa.jwt");
    std::fs::write(&sa_jwt, devvault::DEV_SA_JWT).map_err(|e| format!("sa jwt: {e}"))?;

    // --- gateway + snapshots -------------------------------------------
    let snapshots = Arc::new(MemorySnapshotStore::new());
    let gateway = Gateway::with_clock(snapshots.clone(), Arc::new(SystemClock), gateway_cfg(args));
    let control_endpoint = format!("http://{}", args.gateway_bind);

    // --- pod backend -----------------------------------------------------
    let exe = botd_exe(args)?;
    let state_dir = dir.join("botd-state");
    std::fs::create_dir_all(&state_dir).map_err(|e| e.to_string())?;
    let mut spawn = SpawnSpec::botd(&exe);
    spawn.env = vec![
        ("XENOMIT_CONTROL_ENDPOINT".into(), control_endpoint.clone()),
        ("XENOMIT_VAULT_ADDR".into(), vault.addr()),
        ("XENOMIT_VAULT_ROLE".into(), "dev-bot".into()),
        (
            "XENOMIT_VAULT_TOKEN_FILE".into(),
            sa_jwt.to_string_lossy().into_owned(),
        ),
        ("XENOMIT_VAULT_MOUNT".into(), "jwt".into()),
        (
            "XENOMIT_STATE_DIR".into(),
            state_dir.to_string_lossy().into_owned(),
        ),
        ("XENOMIT_HEALTH_BIND".into(), "127.0.0.1:0".into()),
        ("XENOMIT_EXIT_IP_URL".into(), vault.echo_url()),
    ];
    spawn.env.extend(args.botd_env.iter().cloned());
    let backend = Arc::new(
        LocalBackend::new(dir.join("pods"), spawn)
            .map_err(|e| format!("local backend: {e}"))?
            .with_log_dir(dir.join("logs"))
            .map_err(|e| format!("bot log dir: {e}"))?,
    );

    // --- controller + fleet ----------------------------------------------
    let bots: Arc<InMemoryBots> = Arc::new(InMemoryBots::new());
    let proxies = Arc::new(InMemoryCrs::new("proxy"));
    let rollouts = Arc::new(InMemoryCrs::new("botrollout"));
    let pool = Pool::new(proxies.clone());
    let mut ctl = BotController::new(
        backend.clone(),
        vault.clone(),
        snapshots.clone(),
        Arc::new(gateway.clone()),
        Arc::new(SystemClock),
        pool,
    );
    ctl.cfg.stable_image = args.stable_image.clone();
    ctl.cfg.canary_image = args
        .canary_image
        .clone()
        .unwrap_or_else(|| args.stable_image.clone());
    ctl.cfg.panel_base_url = args.panel_url.clone();
    let ctl = Arc::new(ctl);

    let locks = Arc::new(BotLocks::new());
    let (fleet_events, _) = broadcast::channel(256);
    let bots_repo: Arc<dyn BotRepo> = bots.clone();
    let fleet = Fleet {
        bots: bots_repo.clone(),
        rollouts: rollouts.clone(),
        proxies: proxies.clone(),
        secrets: vault.clone(),
        snapshots: snapshots.clone(),
        ops: ctl.clone(),
        locks: locks.clone(),
        events: fleet_events.clone(),
        clock: Arc::new(SystemClock),
        plans: None,
        usage: None,
    };

    let (auth, dev_token) = resolve_auth(args)?;
    let panel = panel_link(args, dev_token);
    serve(
        args.api_bind,
        args.gateway_bind,
        gateway,
        fleet,
        auth,
        panel,
        locks,
        bots_repo.clone(),
        ctl.clone(),
    )
    .await?;

    tracing::info!(
        api = %args.api_bind,
        gateway = %args.gateway_bind,
        vault = %vault.addr(),
        botd = %exe.display(),
        "local mode up — dev Vault + LocalBackend + in-memory repos"
    );

    // --- rollout + proxy loops, then ctrl-c -----------------------------
    let rollout_ctl = BotRolloutController::new(
        rollouts.clone(),
        driver::RepoBotDirectory(bots_repo.clone()),
        ctl.clone(),
        Arc::new(SystemClock),
        AllowAllVerifier,
        NoMetrics,
        Arc::new(FleetEventSink::new(fleet_events)),
    );
    tokio::spawn(driver::rollout_driver(rollout_ctl));
    let proxy_ctl = ProxyController::new(
        proxies.clone(),
        driver::TcpProber,
        driver::RepoBotDirectory(bots_repo.clone()),
        Arc::new(SystemClock),
        ctl.clone(),
    );
    tokio::spawn(driver::proxy_driver(proxy_ctl));

    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down — stopping botd children");

    // Teardown: reap every live botd child before exit, then the dev
    // Vault listener.
    for bot in bots_repo.list(None).await {
        let Some(name) = bot.metadata.name.clone() else {
            continue;
        };
        if let Ok(pods) = backend.list(&name).await {
            for pod in pods {
                if pod.is_alive() {
                    let _ = backend.delete_pod(&name, pod.epoch).await;
                }
            }
        }
    }
    vault.abort();
    Ok(())
}

#[cfg(feature = "postgres")]
async fn snapshots_for(
    args: &Args,
) -> Result<Arc<dyn fleet_types::snapshot::SnapshotStore>, String> {
    if let Some(dsn) = args.database_url.as_deref().filter(|d| !d.is_empty()) {
        let store = control_gateway::postgres::PostgresSnapshotStore::connect(dsn, 4)
            .await
            .map_err(|e| format!("postgres: {e}"))?;
        return Ok(Arc::new(store));
    }
    tracing::warn!("--database-url unset: snapshots live in memory only");
    Ok(Arc::new(MemorySnapshotStore::new()))
}

#[cfg(not(feature = "postgres"))]
async fn snapshots_for(
    _args: &Args,
) -> Result<Arc<dyn fleet_types::snapshot::SnapshotStore>, String> {
    Ok(Arc::new(MemorySnapshotStore::new()))
}

async fn run_kube(args: &Args) -> Result<(), String> {
    let client = kube::Client::try_default()
        .await
        .map_err(|e| format!("kubeconfig: {e}"))?;

    // --- real Vault ------------------------------------------------------
    let vault_addr = args
        .vault_addr
        .clone()
        .unwrap_or_else(|| "https://vault.xenomit-system:8200".into());
    let secrets = Arc::new(vaultstore::VaultSecretStore::new(
        vault_addr.clone(),
        args.vault_mount.clone(),
        args.vault_role.clone(),
        args.vault_token_file
            .clone()
            .unwrap_or_else(|| PathBuf::from("/var/run/secrets/vault/token")),
        Some(client.clone()),
        args.bots_namespace.clone(),
    ));

    // --- backend + stores --------------------------------------------------
    let mut backend = operator::backends::kube_backend::KubePodBackend::new(
        client.clone(),
        args.bots_namespace.clone(),
    );
    backend.env = DeployEnv {
        control_endpoint: args
            .control_endpoint
            .clone()
            .unwrap_or_else(|| "http://control-gateway.xenomit-system:8443".into()),
        vault_addr,
        vault_mount: args.vault_mount.clone(),
    };
    let backend = Arc::new(backend);

    let snapshots = snapshots_for(args).await?;
    let gateway = Gateway::with_clock(snapshots.clone(), Arc::new(SystemClock), gateway_cfg(args));

    // --- CR-backed repos ----------------------------------------------------
    let bots: Arc<kuberepo::KubeBots> = Arc::new(kuberepo::KubeBots::new(
        client.clone(),
        &args.bots_namespace,
    ));
    let proxies = Arc::new(kuberepo::KubeCrRepo::<fleet_types::crds::Proxy>::new(
        client.clone(),
        &args.bots_namespace,
        "proxy",
    ));
    let rollouts = Arc::new(kuberepo::KubeCrRepo::<fleet_types::crds::BotRollout>::new(
        client.clone(),
        &args.bots_namespace,
        "botrollout",
    ));
    let pool = Pool::new(proxies.clone());
    let mut ctl = BotController::new(
        backend.clone(),
        secrets.clone(),
        snapshots.clone(),
        Arc::new(gateway.clone()),
        Arc::new(SystemClock),
        pool,
    );
    ctl.cfg.stable_image = args.stable_image.clone();
    ctl.cfg.canary_image = args
        .canary_image
        .clone()
        .unwrap_or_else(|| args.stable_image.clone());
    ctl.cfg.panel_base_url = args.panel_url.clone();
    let ctl = Arc::new(ctl);

    let locks = Arc::new(BotLocks::new());
    let (fleet_events, _) = broadcast::channel(256);
    let bots_repo: Arc<dyn BotRepo> = bots.clone();
    let fleet = Fleet {
        bots: bots_repo.clone(),
        rollouts: rollouts.clone(),
        proxies: proxies.clone(),
        secrets: secrets.clone(),
        snapshots: snapshots.clone(),
        ops: ctl.clone(),
        locks: locks.clone(),
        events: fleet_events.clone(),
        clock: Arc::new(SystemClock),
        plans: None,
        usage: None,
    };

    let (auth, dev_token) = resolve_auth(args)?;
    let panel = panel_link(args, dev_token);
    serve(
        args.api_bind,
        args.gateway_bind,
        gateway,
        fleet,
        auth,
        panel,
        locks,
        bots_repo.clone(),
        ctl.clone(),
    )
    .await?;

    tracing::info!(
        api = %args.api_bind,
        gateway = %args.gateway_bind,
        namespace = %args.bots_namespace,
        "kube mode up — KubePodBackend + real Vault + CR repos"
    );

    let rollout_ctl = BotRolloutController::new(
        rollouts.clone(),
        driver::RepoBotDirectory(bots_repo.clone()),
        ctl.clone(),
        Arc::new(SystemClock),
        AllowAllVerifier,
        NoMetrics,
        Arc::new(FleetEventSink::new(fleet_events)),
    );
    tokio::spawn(driver::rollout_driver(rollout_ctl));
    let proxy_ctl = ProxyController::new(
        proxies.clone(),
        driver::TcpProber,
        driver::RepoBotDirectory(bots_repo.clone()),
        Arc::new(SystemClock),
        ctl.clone(),
    );
    tokio::spawn(driver::proxy_driver(proxy_ctl));

    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let result = match args.mode {
        Mode::Local => run_local(&args).await,
        Mode::Kube => run_kube(&args).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The epoch authority restarts at 1 on every local-mode launch while
    /// `botd-state/epochs/<bot>.txt` is monotone — a leftover file fences
    /// every fresh pod (silent exit 67). `reset_local_pod_state` must drop
    /// the per-generation pod state but keep the archives and dev Vault.
    #[test]
    fn reset_local_pod_state_wipes_epochs_keeps_archives() {
        let dir = tempfile::tempdir().unwrap();
        for sub in [
            "pods/bot-smoke-e3.json",
            "botd-state/epochs/bot-smoke.txt",
            "botd-state/snapshots/snap_000001.json",
            "botd-state/statistics/127.0.0.1-1001.jsonl",
            "vault-kv.json",
        ] {
            let p = dir.path().join(sub);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, b"x").unwrap();
        }

        reset_local_pod_state(dir.path()).unwrap();

        assert!(!dir.path().join("pods").exists());
        assert!(!dir.path().join("botd-state/epochs").exists());
        assert!(
            dir.path()
                .join("botd-state/snapshots/snap_000001.json")
                .exists()
        );
        assert!(
            dir.path()
                .join("botd-state/statistics/127.0.0.1-1001.jsonl")
                .exists()
        );
        assert!(dir.path().join("vault-kv.json").exists());
    }
}
