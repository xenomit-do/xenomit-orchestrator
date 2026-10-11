//! Per-bot keyed async serialization for controller passes.
//!
//! Any reconciler — the REST API, the operator driver, rollouts — must not
//! run overlapping [`crate::BotOps`] passes for the same bot: controller
//! state machines are read-modify-write over `status`, and backends such as
//! [`crate::LocalBackend`] own real process side effects that must be
//! observed exactly once (docs/03 §3.1). [`BotLocks`] hands out a guard per
//! bot name; holding it serializes every `reconcile`/`apply`-driven pass
//! for that bot while leaving different bots fully parallel.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Keyed gate set: `gate(name)` serializes work per bot. Entries are weak so
/// the map does not leak a slot per deleted bot (docs/03 §3.1 note: locks
/// are re-created on demand; a queued guard keeps its slot alive).
#[derive(Debug, Default)]
pub struct BotLocks {
    slots: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
}

impl BotLocks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take the exclusive pass for `bot`; returns when the previous holder
    /// dropped its guard. Cheap when uncontended.
    pub async fn gate(&self, bot: &str) -> BotGate {
        let lock = {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            match slots.get(bot) {
                Some(weak) => match weak.upgrade() {
                    Some(lock) => lock,
                    None => {
                        let lock = Arc::new(AsyncMutex::new(()));
                        slots.insert(bot.to_string(), Arc::downgrade(&lock));
                        lock
                    }
                },
                None => {
                    let lock = Arc::new(AsyncMutex::new(()));
                    slots.insert(bot.to_string(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        BotGate {
            _guard: lock.lock_owned().await,
        }
    }
}

/// Held while one reconciler runs controller passes for a bot.
pub struct BotGate {
    _guard: OwnedMutexGuard<()>,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// N tasks gating on the same bot must never overlap inside the guard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_bot_passes_are_serialized() {
        let locks = Arc::new(BotLocks::new());
        let inside = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let locks = locks.clone();
            let inside = inside.clone();
            let max = max.clone();
            set.spawn(async move {
                let _gate = locks.gate("bot").await;
                let n = inside.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(n, Ordering::SeqCst);
                tokio::task::yield_now().await;
                inside.fetch_sub(1, Ordering::SeqCst);
            });
        }
        while set.join_next().await.is_some() {}
        assert_eq!(max.load(Ordering::SeqCst), 1);
    }

    /// Different bots are independent gates.
    #[tokio::test]
    async fn different_bots_run_in_parallel() {
        let locks = Arc::new(BotLocks::new());
        // If gate("b") waited on gate("a"), the three-party barrier would
        // never complete: tasks enter the barrier while holding the gate.
        let bar = Arc::new(tokio::sync::Barrier::new(3));
        let mut set = tokio::task::JoinSet::new();
        for name in ["a", "b"] {
            let locks = locks.clone();
            let bar = bar.clone();
            set.spawn(async move {
                let _gate = locks.gate(name).await;
                bar.wait().await;
            });
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), bar.wait())
            .await
            .expect("both bots held their gates concurrently");
        while set.join_next().await.is_some() {}
    }
}
