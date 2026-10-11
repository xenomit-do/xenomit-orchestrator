//! Test-side driver mirroring `orchestratord/src/driver.rs` — the same
//! level-triggered tick cadence, minus production wiring. Scaffold for
//! the lifecycle rig lost in migration; assertions are re-authored per
//! test in `tests/`.

use std::path::PathBuf;
use std::time::Duration;

/// Reconcile cadence for the level-triggered pass (matches the daemon).
pub const DRIVER_TICK: Duration = Duration::from_millis(75);
/// Rollout-controller pass interval — rollouts are minutes-scale work.
pub const ROLLOUT_TICK: Duration = Duration::from_secs(5);
/// Proxy-pool probe interval (docs/05 §5.4 ~60 s; dev keeps it snappier).
pub const PROXY_TICK: Duration = Duration::from_secs(30);

/// Locate a compiled `xenomit-botd`: `XENOMIT_BOTD_EXE` → the sibling
/// repo's `target-4b/debug` build (the e2e convention shared with
/// `orchestratord --botd`, see `orchestratord/src/main.rs`).
pub fn botd_exe() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("XENOMIT_BOTD_EXE") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    let exe_name = if cfg!(windows) {
        "xenomit-botd.exe"
    } else {
        "xenomit-botd"
    };
    let sibling = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../xenomit-botd/target-4b/debug")
        .join(exe_name);
    sibling.is_file().then_some(sibling)
}
