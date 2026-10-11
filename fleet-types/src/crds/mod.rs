//! Custom resource specs + statuses, group `fleet.xenomit.io/v1alpha1`
//! (docs/04 §4.1). `deploy/crds/*.yaml` is generated from these types by
//! `cargo run -p fleet-types --bin gen_crds`.

pub mod bot;
pub mod proxy;
pub mod rollout;

pub use bot::*;
pub use proxy::*;
pub use rollout::*;
