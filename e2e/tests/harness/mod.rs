//! Shared harness for the lifecycle rig: spawns a real compiled
//! `xenomit-botd` against `xenomit-mock-game` and drives operator
//! ticks in-process.
//!
//! Rebuilt scaffold — the original rig (`e2e/tests/harness/rig.rs`)
//! was lost in the 2026-10 Gitea→GitHub migration and is being
//! re-authored; see `GETTING-STARTED.md` and
//! `ci/docker/Dockerfile.orchestrator`.

pub mod rig;
