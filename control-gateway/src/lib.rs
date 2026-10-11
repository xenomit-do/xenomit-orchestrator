//! Control gateway (docs/04 §4.5): the bot-facing tonic endpoint of the
//! Xenomit control plane.
//!
//! - [`Gateway`] serves `xenomit.fleet.v1alpha1.FleetControl` — one
//!   bidirectional stream per bot — and implements the operator's
//!   `ControlDirectory` seam (`note_pending_epoch` / `promote` / `send` /
//!   `predecessor_clear`), so the same object is both the wire endpoint and
//!   the controller's view of sessions.
//! - [`MemorySnapshotStore`] is the offline `SnapshotStore`.
//! - `PostgresSnapshotStore` (feature `postgres`) is the production store;
//!   its tests run only when `DATABASE_URL` is set.
//!
//! Epoch fencing (docs/03 §3.7): sessions are accepted for the serving or
//! pending epoch only; writes are fenced by `SnapshotStore::put`, and every
//! violation is answered with the `Fenced` command so the bot exits 67
//! without logging in.

pub mod events;
pub mod gateway;
pub mod memory;
#[cfg(feature = "postgres")]
pub mod postgres;

pub use events::GatewayEvent;
pub use gateway::{Gateway, GatewayConfig};
pub use memory::MemorySnapshotStore;
#[cfg(feature = "postgres")]
pub use postgres::PostgresSnapshotStore;
