//! Shared types for the Xenomit fleet (docs/01 §1.1): the orchestrator and
//! `botd` can never disagree about a snapshot or a control-plane message
//! because both consume this crate.
//!
//! Contents:
//! - [`crds`]: `XenomitBot` / `BotRollout` / `Proxy` specs + statuses
//!   (docs/04 §4.1).
//! - [`phase`]: operator-visible lifecycle + the planned-handoff state
//!   machine (docs/01 §1.4, docs/03 §3.2).
//! - [`exit_code`]: the docs/02 §2.5 exit-code → action table.
//! - [`snapshot`]: `BotSnapshot` (docs/03 §3.3) and the `SnapshotStore`
//!   trait (ADR-22).
//! - [`epoch`]: fencing primitives (docs/03 §3.7, ADR-12).
//! - [`proto`]: control-plane messages generated from `proto/fleet.proto`
//!   (tonic/prost, docs/04 §4.5).

pub mod crds;
pub mod epoch;
pub mod exit_code;
pub mod phase;
pub mod proto;
pub mod snapshot;

pub use crds::*;
pub use epoch::*;
pub use exit_code::*;
pub use phase::*;
pub use snapshot::*;
