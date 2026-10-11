//! Epoch & fencing types (docs/03 §3.7, ADR-12).
//!
//! The operator increments `status.epoch` for every new pod of a bot. Every
//! control-channel message and every snapshot/checkpoint write carries the
//! epoch; writes are conditional on it matching the recorded current epoch,
//! so a stale writer can never drive an account a newer pod owns.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Monotonically increasing per-bot generation counter.
///
/// Epoch 0 means "no pod has ever existed"; the first pod is epoch 1.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
pub struct Epoch(pub u64);

impl Epoch {
    pub const ZERO: Epoch = Epoch(0);

    pub fn next(self) -> Epoch {
        Epoch(self.0 + 1)
    }

    /// Previous epoch, saturating at 0.
    pub fn prev(self) -> Epoch {
        Epoch(self.0.saturating_sub(1))
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn is_stale(self, current: Epoch) -> bool {
        self < current
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}", self.0)
    }
}

/// Returned when a writer presents an epoch that is not the current one.
/// The bot maps this to exit code 67 (docs/02 §2.5).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("fenced: bot has epoch {expected}, writer presented {got}")]
pub struct Fenced {
    pub expected: Epoch,
    pub got: Epoch,
}

/// Ensure `writer` is the current epoch for this bot.
pub fn check_epoch(current: Epoch, writer: Epoch) -> Result<(), Fenced> {
    if writer == current {
        Ok(())
    } else {
        Err(Fenced {
            expected: current,
            got: writer,
        })
    }
}

/// Per-bot liveness proof (docs/03 §3.7): a Kubernetes Lease in production,
/// a timestamp here for trait-based tests. A replacement pod is only armed
/// once `renewed_at + ttl` has elapsed (plus the configured grace).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LeaseInfo {
    pub epoch: Epoch,
    pub renewed_at_unix_ms: i64,
    pub ttl_ms: u64,
    /// Number of renewals seen on this lease (monotonic per epoch).
    pub renewals: u64,
}

impl LeaseInfo {
    pub fn is_expired(&self, now_unix_ms: i64) -> bool {
        now_unix_ms >= self.renewed_at_unix_ms + self.ttl_ms as i64
    }

    pub fn expires_at_unix_ms(&self) -> i64 {
        self.renewed_at_unix_ms + self.ttl_ms as i64
    }
}

/// Default per-bot lease TTL (docs/03 §3.7).
pub const DEFAULT_LEASE_TTL_MS: u64 = 30_000;
