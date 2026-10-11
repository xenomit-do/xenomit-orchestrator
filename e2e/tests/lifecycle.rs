//! Lifecycle tests over a real compiled `xenomit-botd` driven against
//! `xenomit-mock-game` (Backpage HTTP + game TCP + egress proxy).
//!
//! Scaffold: the original rig was lost in the 2026-10 migration. Tests
//! skip until a `xenomit-botd` binary resolves via `XENOMIT_BOTD_EXE`
//! or the sibling `target-4b/debug` build — the same convention as
//! `orchestratord --botd`.

mod harness;

#[tokio::test]
async fn lifecycle_smoke() {
    // Rig cadence invariant (orchestratord/src/driver.rs): the level-triggered
    // tick is the fastest pass; rollout and proxy passes are slower.
    assert!(
        harness::rig::DRIVER_TICK < harness::rig::ROLLOUT_TICK
            && harness::rig::ROLLOUT_TICK < harness::rig::PROXY_TICK,
        "tick cadence ordering drifted from the daemon driver"
    );

    let Some(_botd) = harness::rig::botd_exe() else {
        eprintln!(
            "skipping lifecycle rig: no xenomit-botd binary \
             (set XENOMIT_BOTD_EXE or build ../xenomit-botd with target-dir target-4b)"
        );
        return;
    };
    // TODO(rebuild rig): boot mock-game, spawn botd, drive rig ticks at
    // DRIVER_TICK/ROLLOUT_TICK/PROXY_TICK cadence, assert connect → pair →
    // session lifecycle, then clean teardown.
}
