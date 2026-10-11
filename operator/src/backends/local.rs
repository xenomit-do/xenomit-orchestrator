//! `local` PodBackend: spawns `botd` child processes (docs/02 §2.6). For
//! development and tests only — it bypasses NetworkPolicy and every pod
//! guarantee; the production target is k3d (docs/05 §5.5).
//!
//! Testable with a fake child script: the backend renders the botd config
//! to `<dir>/<bot>-e<epoch>.json` and runs `program --config <file>`.

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use tokio::process::{Child, Command};

use fleet_types::epoch::Epoch;

use crate::traits::{BackendError, BotPodSpec, PodBackend, PodObservation, PodState};

/// How to launch one botd process. `program` with `{config}` in `args` is
/// substituted with the rendered config path.
#[derive(Clone, Debug)]
pub struct SpawnSpec {
    pub program: PathBuf,
    /// `["--config", "{config}"]` by convention; `{config}` is replaced.
    pub args: Vec<String>,
    /// Extra env for the child (bot never inherits secrets through env).
    pub env: Vec<(String, String)>,
}

impl SpawnSpec {
    /// The default: `botd --config <file>`.
    pub fn botd(program: impl Into<PathBuf>) -> Self {
        SpawnSpec {
            program: program.into(),
            args: vec!["--config".into(), "{config}".into()],
            env: vec![],
        }
    }
}

/// One tracked pod slot. `Spawning` reserves the `(bot, epoch)` key while
/// the config write + `spawn` are in flight so a concurrent `ensure_pod`
/// observes the reservation instead of double-spawning (docs/02 §2.6).
enum ChildEntry {
    Spawning,
    Running(Box<Child>),
}

impl ChildEntry {
    /// `Ok(None)` = still live — a `Spawning` slot counts as live because a
    /// child is about to appear (a caller may not treat it as gone).
    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        match self {
            ChildEntry::Spawning => Ok(None),
            ChildEntry::Running(child) => child.try_wait(),
        }
    }
}

/// Child-process backend. State lives in memory plus the config files under
/// `dir`; a restarted backend only knows pods whose process is still a
/// tracked child (dev-only simplification, documented in docs/02 §2.6).
pub struct LocalBackend {
    dir: PathBuf,
    spawn: SpawnSpec,
    /// When set, each child's stdout+stderr append to
    /// `<log_dir>/<bot>-e<epoch>.log` instead of being discarded — the dev
    /// daemon's per-bot log surface.
    log_dir: Option<PathBuf>,
    children: Mutex<HashMap<(String, u64), ChildEntry>>,
    /// Exit codes of processes that already reaped — including epochs a
    /// mid-flight `delete_pod` burned while their spawn was in flight.
    exits: Mutex<HashMap<(String, u64), Option<i32>>>,
    /// Every pid ever spawned per `(bot, epoch)` — test audit surface for
    /// "exactly one process per epoch" (a correct run records exactly one).
    spawned: Mutex<HashMap<(String, u64), Vec<u32>>>,
}

impl LocalBackend {
    pub fn new(dir: impl Into<PathBuf>, spawn: SpawnSpec) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(LocalBackend {
            dir,
            spawn,
            log_dir: None,
            children: Mutex::new(HashMap::new()),
            exits: Mutex::new(HashMap::new()),
            spawned: Mutex::new(HashMap::new()),
        })
    }

    /// Route child stdout+stderr into `<dir>/<bot>-e<epoch>.log` files.
    pub fn with_log_dir(mut self, dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        self.log_dir = Some(dir);
        Ok(self)
    }

    fn config_path(&self, bot_id: &str, epoch: Epoch) -> PathBuf {
        self.dir.join(format!("{bot_id}-e{}.json", epoch.get()))
    }

    fn key(bot_id: &str, epoch: Epoch) -> (String, u64) {
        (bot_id.to_string(), epoch.get())
    }

    /// Build the Command. `.bat`/`.cmd` programs go through `cmd /C`;
    /// everything else is spawned directly.
    fn build_command(&self, program: &Path, args: &[String]) -> Command {
        let prog_lower = program.to_string_lossy().to_lowercase();
        if prog_lower.ends_with(".bat") || prog_lower.ends_with(".cmd") {
            let mut c = Command::new("cmd");
            c.arg("/C").arg(program).args(args);
            c
        } else {
            let mut c = Command::new(program);
            c.args(args);
            c
        }
    }

    /// Pids this backend spawned for `(bot_id, epoch)` — tests assert this
    /// is exactly one per epoch even under concurrent `ensure_pod` calls.
    pub fn spawned_pids(&self, bot_id: &str, epoch: Epoch) -> Vec<u32> {
        self.spawned
            .lock()
            .unwrap()
            .get(&Self::key(bot_id, epoch))
            .cloned()
            .unwrap_or_default()
    }

    /// Pids of children currently tracked as running (still-alive not
    /// checked — the bookkeeping is what leaks must escape).
    pub fn tracked_pids(&self) -> Vec<u32> {
        self.children
            .lock()
            .unwrap()
            .values()
            .filter_map(|e| match e {
                ChildEntry::Running(c) => c.id(),
                ChildEntry::Spawning => None,
            })
            .collect()
    }
}

#[async_trait]
impl PodBackend for LocalBackend {
    async fn ensure_pod(&self, spec: &BotPodSpec) -> Result<(), BackendError> {
        let key = Self::key(&spec.bot_id, spec.epoch);
        {
            // The check *and* the reservation happen under one lock hold:
            // a concurrent `ensure_pod` for the same epoch either sees a
            // live child or the `Spawning` slot — never an empty key.
            let mut children = self.children.lock().unwrap();
            if let Some(entry) = children.get_mut(&key) {
                match entry.try_wait() {
                    Ok(Some(status)) => {
                        // The child already ran and exited: record it and
                        // refuse — epochs are never reused (docs/03 §3.7).
                        // Falling through to `Spawning` would resurrect the
                        // dead epoch under a fresh pid.
                        self.exits
                            .lock()
                            .unwrap()
                            .insert(key.clone(), status.code());
                        children.remove(&key);
                        return Ok(());
                    }
                    Ok(None) => return Ok(()),
                    Err(_) => {
                        // An unobservable child can't be supervised — burn
                        // the epoch so the controller sees `Exited` on the
                        // next `list` instead of erroring every pass.
                        self.exits.lock().unwrap().insert(key.clone(), None);
                        children.remove(&key);
                        return Ok(());
                    }
                }
            } else if self.exits.lock().unwrap().contains_key(&key) {
                // A process for this epoch already ran and exited; epochs
                // are never reused (docs/03 §3.7).
                return Ok(());
            }
            children.insert(key.clone(), ChildEntry::Spawning);
        }

        let config_path = self.config_path(&spec.bot_id, spec.epoch);
        let spawned = async {
            tokio::fs::write(&config_path, &spec.config_json).await?;
            let args: Vec<String> = self
                .spawn
                .args
                .iter()
                .map(|a| a.replace("{config}", &config_path.to_string_lossy()))
                .collect();
            let mut cmd = self.build_command(&self.spawn.program, &args);
            cmd.envs(self.spawn.env.iter().cloned())
                .env("XENOMIT_BOT_ID", &spec.bot_id)
                .env("XENOMIT_EPOCH", spec.epoch.get().to_string())
                .stdin(Stdio::null());
            match &self.log_dir {
                Some(dir) => {
                    let log_path = dir.join(format!("{}-e{}.log", spec.bot_id, spec.epoch.get()));
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log_path)?;
                    cmd.stdout(Stdio::from(file.try_clone()?))
                        .stderr(Stdio::from(file));
                }
                None => {
                    cmd.stdout(Stdio::null()).stderr(Stdio::null());
                }
            }
            cmd.spawn().map_err(BackendError::from)
        }
        .await;

        // Commit under the lock: the slot must still be our `Spawning`
        // reservation — a `delete_pod` that landed mid-spawn removed it and
        // burned the epoch, in which case the fresh child is killed below
        // (after the guard is dropped) instead of resurrecting the pod.
        enum Commit {
            Done,
            Orphan(Box<Child>),
            Fail(BackendError),
        }
        let commit = {
            let mut children = self.children.lock().unwrap();
            match spawned {
                Ok(child) => {
                    let child = Box::new(child);
                    if matches!(children.get(&key), Some(ChildEntry::Spawning)) {
                        if let Some(pid) = child.id() {
                            self.spawned
                                .lock()
                                .unwrap()
                                .entry(key.clone())
                                .or_default()
                                .push(pid);
                        }
                        children.insert(key, ChildEntry::Running(child));
                        Commit::Done
                    } else {
                        Commit::Orphan(child)
                    }
                }
                Err(e) => {
                    // Release the reservation so a later pass can retry.
                    if matches!(children.get(&key), Some(ChildEntry::Spawning)) {
                        children.remove(&key);
                    }
                    Commit::Fail(e)
                }
            }
        };
        match commit {
            Commit::Done => Ok(()),
            Commit::Orphan(mut child) => {
                let _ = child.kill().await;
                Ok(())
            }
            Commit::Fail(e) => Err(e),
        }
    }

    async fn delete_pod(&self, bot_id: &str, epoch: Epoch) -> Result<(), BackendError> {
        let key = Self::key(bot_id, epoch);
        let entry = self.children.lock().unwrap().remove(&key);
        match entry {
            Some(ChildEntry::Running(mut child)) => {
                child.kill().await?;
                let code = child.try_wait().ok().flatten().and_then(|s| s.code());
                self.exits.lock().unwrap().insert(key, code);
            }
            // A spawn is still in flight: burn the epoch now; the committing
            // branch of `ensure_pod` kills the fresh child instead of
            // resurrecting a deleted pod.
            Some(ChildEntry::Spawning) => {
                self.exits.lock().unwrap().insert(key, None);
            }
            None => {}
        }
        let _ = tokio::fs::remove_file(self.config_path(bot_id, epoch)).await;
        Ok(())
    }

    async fn list(&self, bot_id: &str) -> Result<Vec<PodObservation>, BackendError> {
        let mut out = Vec::new();
        let mut children = self.children.lock().unwrap();
        let mut exits = self.exits.lock().unwrap();
        for ((id, ep), entry) in children.iter_mut() {
            if id != bot_id {
                continue;
            }
            let epoch = Epoch(*ep);
            let state = match entry {
                // A reserved slot is a pod on its way to Running.
                ChildEntry::Spawning => PodState::Pending,
                ChildEntry::Running(child) => match child.try_wait() {
                    Ok(Some(status)) => {
                        exits.insert((id.clone(), *ep), status.code());
                        PodState::Exited {
                            code: status.code(),
                        }
                    }
                    Ok(None) => PodState::Running,
                    Err(_) => {
                        // `retain` drops the entry below — record the exit
                        // first or the pod silently vanishes from `list`:
                        // no `Exited` observation means no crash restore.
                        exits.insert((id.clone(), *ep), None);
                        PodState::Exited { code: None }
                    }
                },
            };
            out.push(PodObservation {
                name: fleet_types::crds::pod_name(bot_id, epoch),
                epoch,
                state,
            });
        }
        children.retain(|(id, _), c| id != bot_id || matches!(c.try_wait(), Ok(None)));
        for ((id, ep), code) in exits.iter() {
            if id == bot_id {
                let epoch = Epoch(*ep);
                out.push(PodObservation {
                    name: fleet_types::crds::pod_name(bot_id, epoch),
                    epoch,
                    state: PodState::Exited { code: *code },
                });
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod_spec(bot_id: &str, epoch: u64) -> BotPodSpec {
        BotPodSpec {
            bot_id: bot_id.into(),
            tenant: "t".into(),
            epoch: Epoch(epoch),
            image: "img".into(),
            config_json: "{}".into(),
            config_revision: 1,
            labels: Default::default(),
            drain_deadline_s: 30,
            hard_deadline_s: 60,
            proxy: None,
            resources: None,
            owner_uid: None,
        }
    }

    /// A child that exits promptly with `code`.
    fn quick_exit_spawn(code: i32) -> SpawnSpec {
        if cfg!(windows) {
            SpawnSpec {
                program: "cmd".into(),
                args: vec!["/C".into(), format!("exit {code}")],
                env: vec![],
            }
        } else {
            SpawnSpec {
                program: "sh".into(),
                args: vec!["-c".into(), format!("exit {code}")],
                env: vec![],
            }
        }
    }

    /// Poll `list` until the `(bot, 1)` pod reports `Exited` (or panic).
    async fn wait_exited(backend: &LocalBackend, bot_id: &str) -> Option<i32> {
        for _ in 0..200 {
            if let Some(PodState::Exited { code }) = backend
                .list(bot_id)
                .await
                .unwrap()
                .iter()
                .find(|p| p.epoch == Epoch(1))
                .map(|p| p.state.clone())
            {
                return code;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("pod never reported Exited");
    }

    /// A child that dies shortly after spawn must surface as
    /// `PodState::Exited` — it is the controller's only dead-pod signal;
    /// missing it wedges the bot in `Running` forever.
    #[tokio::test]
    async fn dead_child_is_observed_as_exited() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path(), quick_exit_spawn(7)).unwrap();
        backend.ensure_pod(&pod_spec("bot-x", 1)).await.unwrap();

        assert_eq!(wait_exited(&backend, "bot-x").await, Some(7));
    }

    /// A burned epoch is never respawned: a second `ensure_pod` for the
    /// same `(bot, epoch)` after the child died must not resurrect it
    /// under a fresh pid (docs/03 §3.7 — the controller moves to the next
    /// epoch; the backend must not resurrect the old one).
    #[tokio::test]
    async fn dead_epoch_is_never_respawned() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path(), quick_exit_spawn(7)).unwrap();
        backend.ensure_pod(&pod_spec("bot-x", 1)).await.unwrap();
        wait_exited(&backend, "bot-x").await;

        backend.ensure_pod(&pod_spec("bot-x", 1)).await.unwrap();
        // Give a spurious respawn a beat to land before counting pids.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(backend.spawned_pids("bot-x", Epoch(1)).len(), 1);
        assert!(backend
            .list("bot-x")
            .await
            .unwrap()
            .iter()
            .all(|p| matches!(p.state, PodState::Exited { .. })));
    }
}
