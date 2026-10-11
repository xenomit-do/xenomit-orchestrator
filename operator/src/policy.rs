//! Uptime policy: backoff + jitter and the crash budget (docs/04 §4.4,
//! docs/02 §2.5). Jitter is deterministic (seeded by bot id + attempt) so
//! tests and replays behave identically; it still decorrelates login bursts
//! across the fleet.

use fleet_types::crds::CrashStatus;
use std::time::Duration;

/// Restart policy for genuine crashes (docs/04 §4.4).
#[derive(Clone, Debug)]
pub struct CrashPolicy {
    /// First backoff (docs/04 §4.4: 5 s).
    pub base_backoff: Duration,
    /// Backoff cap (docs/04 §4.4: 5 min).
    pub max_backoff: Duration,
    /// Quarantine window M (docs/04 §4.4: "N crashes in M minutes").
    pub budget_window: Duration,
    /// Quarantine threshold N.
    pub budget_count: usize,
    /// ±fraction of jitter applied to each backoff (0.25 = ±25 %).
    pub jitter_frac: f64,
}

impl Default for CrashPolicy {
    fn default() -> Self {
        CrashPolicy {
            base_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(300),
            budget_window: Duration::from_secs(600),
            budget_count: 5,
            jitter_frac: 0.25,
        }
    }
}

/// Result of recording one crash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CrashVerdict {
    /// Restart allowed after `retry_at_unix_ms`.
    Retry {
        retry_at_unix_ms: i64,
        attempts: u32,
    },
    /// Crash budget exceeded → `Quarantined`, no further restarts.
    Quarantine,
}

impl CrashPolicy {
    /// Record a crash at `now_unix_ms` on `crash` and decide what happens
    /// next. `seed` should be stable per bot (e.g. hash of the bot id).
    pub fn record_crash(
        &self,
        crash: &mut CrashStatus,
        now_unix_ms: i64,
        seed: u64,
    ) -> CrashVerdict {
        let window_start = now_unix_ms - self.budget_window.as_millis() as i64;
        crash.recent_unix_ms.retain(|t| *t >= window_start);
        crash.recent_unix_ms.push(now_unix_ms);
        if crash.recent_unix_ms.len() > self.budget_count {
            crash.next_retry_at_unix_ms = None;
            return CrashVerdict::Quarantine;
        }
        crash.attempts += 1;
        let backoff = self.jittered_backoff(crash.attempts, seed);
        let retry_at = now_unix_ms + backoff.as_millis() as i64;
        crash.next_retry_at_unix_ms = Some(retry_at);
        CrashVerdict::Retry {
            retry_at_unix_ms: retry_at,
            attempts: crash.attempts,
        }
    }

    /// Exponential backoff `base * 2^(attempt-1)` capped at `max`, with
    /// deterministic ±`jitter_frac` jitter from `seed`.
    pub fn jittered_backoff(&self, attempt: u32, seed: u64) -> Duration {
        let exp = attempt.saturating_sub(1).min(20);
        let base_ms = self.base_backoff.as_millis() as u64;
        let max_ms = self.max_backoff.as_millis() as u64;
        let unjittered = base_ms.saturating_mul(1u64 << exp).min(max_ms);
        if self.jitter_frac <= 0.0 || unjittered == 0 {
            return Duration::from_millis(unjittered);
        }
        // hash → [-j, +j]
        let h = hash64(seed ^ (attempt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let jitter_range = (unjittered as f64 * self.jitter_frac) as i64;
        let unit = (h % 2_000_000) as i64 - 1_000_000; // [-1e6, 1e6)
        let jitter = jitter_range * unit / 1_000_000;
        // Jitter must not push the delay above the documented cap.
        Duration::from_millis(unjittered.saturating_add_signed(jitter).min(max_ms))
    }
}

/// SplitMix-style mix; stable across platforms.
pub(crate) fn hash64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Stable per-bot seed for jitter.
pub fn bot_seed(bot_id: &str) -> u64 {
    bot_id.bytes().fold(0xcbf2_9ce4_8422_2325u64, |acc, b| {
        (acc ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Retention for soft-deleted bots (docs/12 §12.4): snapshots/stats/
/// config-history purge 30 days after `deleted_at`.
pub const DEFAULT_RETENTION_DAYS: u32 = 30;

/// Terminated pods are kept for debugging (docs/04 §4.7): 1 h.
pub const TERMINATED_POD_RETAIN_MS: i64 = 3_600_000;

/// Whether a soft-deleted bot's retention window has elapsed.
pub fn retention_expired(deleted_at_unix_ms: i64, now_unix_ms: i64, retention_days: u32) -> bool {
    let retention_ms = retention_days as i64 * 86_400_000;
    now_unix_ms >= deleted_at_unix_ms + retention_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let p = CrashPolicy {
            jitter_frac: 0.0,
            ..Default::default()
        };
        assert_eq!(p.jittered_backoff(1, 42), Duration::from_secs(5));
        assert_eq!(p.jittered_backoff(2, 42), Duration::from_secs(10));
        assert_eq!(p.jittered_backoff(10, 42), Duration::from_secs(300));
        // Jittered variants never exceed the cap.
        let p = CrashPolicy::default();
        for attempt in 1..40 {
            assert!(p.jittered_backoff(attempt, 42) <= p.max_backoff);
        }
    }

    #[test]
    fn jitter_is_deterministic_and_bounded() {
        let p = CrashPolicy::default();
        let x = p.jittered_backoff(3, 7);
        assert_eq!(x, p.jittered_backoff(3, 7));
        let nominal = 20_000f64; // 5s * 2^2
        let lo = (nominal * (1.0 - p.jitter_frac)) as u64;
        let hi = (nominal * (1.0 + p.jitter_frac)) as u64 + 1;
        assert!((lo..=hi).contains(&(x.as_millis() as u64)), "{x:?}");
    }

    #[test]
    fn budget_quarantines() {
        let p = CrashPolicy {
            budget_count: 3,
            ..Default::default()
        };
        let mut c = CrashStatus::default();
        for i in 0..3 {
            match p.record_crash(&mut c, i * 1000, 1) {
                CrashVerdict::Retry { .. } => {}
                CrashVerdict::Quarantine => panic!("quarantined too early"),
            }
        }
        assert_eq!(p.record_crash(&mut c, 4000, 1), CrashVerdict::Quarantine);
    }

    #[test]
    fn old_crashes_expire_from_window() {
        let p = CrashPolicy {
            budget_count: 1,
            budget_window: Duration::from_secs(10),
            ..Default::default()
        };
        let mut c = CrashStatus::default();
        assert!(matches!(
            p.record_crash(&mut c, 0, 1),
            CrashVerdict::Retry { .. }
        ));
        // Next crash far outside the window: budget restarts.
        assert!(matches!(
            p.record_crash(&mut c, 60_000, 1),
            CrashVerdict::Retry { .. }
        ));
        assert_eq!(c.recent_unix_ms, vec![60_000]);
    }
}
