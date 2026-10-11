//! Control-plane messages generated from `proto/fleet.proto` (docs/04 §4.5)
//! plus small helpers that keep the wire types ergonomic.

#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod v1alpha1 {
    #![allow(clippy::enum_variant_names)]
    tonic::include_proto!("xenomit.fleet.v1alpha1");
}

pub use v1alpha1::*;

impl ClientEvent {
    /// The epoch carried by this event, for fencing checks (0 for `Hello`,
    /// which is checked on its own field).
    pub fn epoch(&self) -> u64 {
        use client_event::Event as E;
        match self.event.as_ref() {
            Some(E::Hello(_)) => 0,
            Some(E::LeaseRenew(m)) => m.epoch,
            Some(E::Checkpoint(m)) => m.epoch,
            Some(E::PutSnapshot(m)) => m.epoch,
            Some(E::PhaseChanged(m)) => m.epoch,
            Some(E::MetricAlert(m)) => m.epoch,
            Some(E::Drained(m)) => m.epoch,
            None => 0,
        }
    }
}

impl GatewayCommand {
    pub fn activate(epoch: u64, snapshot_id: Option<String>) -> Self {
        GatewayCommand {
            command: Some(gateway_command::Command::Activate(Activate {
                epoch,
                snapshot_id: snapshot_id.unwrap_or_default(),
            })),
        }
    }

    pub fn drain(reason: &str, drain_deadline_s: u32, hard_deadline_s: u32) -> Self {
        GatewayCommand {
            command: Some(gateway_command::Command::Drain(Drain {
                reason: reason.to_string(),
                drain_deadline_s,
                hard_deadline_s,
                traceparent: String::new(),
            })),
        }
    }

    pub fn fenced(expected: u64, got: u64) -> Self {
        GatewayCommand {
            command: Some(gateway_command::Command::Fenced(Fenced {
                expected_epoch: expected,
                got_epoch: got,
            })),
        }
    }

    pub fn ack(epoch: u64, snapshot_id: Option<String>) -> Self {
        GatewayCommand {
            command: Some(gateway_command::Command::Ack(Ack {
                epoch,
                snapshot_id: snapshot_id.unwrap_or_default(),
            })),
        }
    }

    pub fn apply_config(revision: u64, settings_json: String) -> Self {
        GatewayCommand {
            command: Some(gateway_command::Command::ApplyConfig(ApplyConfig {
                revision,
                settings_json,
            })),
        }
    }

    pub fn pause(reason: &str) -> Self {
        GatewayCommand {
            command: Some(gateway_command::Command::Pause(Pause {
                reason: reason.to_string(),
            })),
        }
    }

    pub fn resume(reason: &str) -> Self {
        GatewayCommand {
            command: Some(gateway_command::Command::Resume(Resume {
                reason: reason.to_string(),
            })),
        }
    }

    pub fn shutdown(reason: &str) -> Self {
        GatewayCommand {
            command: Some(gateway_command::Command::Shutdown(Shutdown {
                reason: reason.to_string(),
            })),
        }
    }
}
