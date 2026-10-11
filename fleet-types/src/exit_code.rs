//! Exit-code-driven restart policy (docs/02 §2.5, ADR-14).
//!
//! Pods run `restartPolicy: Never`; the operator interprets the exit code so
//! "needs a human", "fenced" and "version mismatch" are never crash-looped
//! and genuine crashes don't hammer the login endpoint.

use serde::{Deserialize, Serialize};
use std::fmt;

/// botd exit codes (docs/02 §2.5).
pub mod codes {
    /// Clean drain, snapshot committed, logged out.
    pub const CLEAN: i32 = 0;
    /// Config invalid.
    pub const CONFIG_INVALID: i32 = 64;
    /// Credentials rejected.
    pub const CREDENTIALS_REJECTED: i32 = 65;
    /// CAPTCHA required.
    pub const CAPTCHA: i32 = 66;
    /// Fenced (stale epoch).
    pub const FENCED: i32 = 67;
    /// Game version mismatch.
    pub const VERSION_MISMATCH: i32 = 68;
    /// Proxy failure.
    pub const PROXY_FAILURE: i32 = 69;
    /// Snapshot unreadable with restore policy = strict.
    pub const SNAPSHOT_UNREADABLE_STRICT: i32 = 70;
}

/// What a pod exit means, independent of how the pod was run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitMeaning {
    /// 0 — clean drain, snapshot committed, logged out.
    CleanDrain,
    /// 64 — config invalid.
    ConfigInvalid,
    /// 65 — credentials rejected.
    CredentialsRejected,
    /// 66 — CAPTCHA required.
    Captcha,
    /// 67 — fenced (stale epoch).
    Fenced,
    /// 68 — game version mismatch.
    VersionMismatch,
    /// 69 — proxy failure.
    ProxyFailure,
    /// 70 — snapshot unreadable and policy = strict.
    SnapshotUnreadableStrict,
    /// Any other code or a signal — a genuine crash.
    Crash,
}

/// The operator's reaction to an `ExitMeaning` (docs/02 §2.5 "Operator
/// action" column).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExitAction {
    /// Start a successor if desired state says running.
    StartSuccessor,
    /// `InvalidConfig`; no restart; surface to user/admin.
    MarkInvalidConfig,
    /// `NeedsHuman(credentials)`; stop (account-lockout risk).
    NeedsHumanCredentials,
    /// `NeedsHuman(captcha)`.
    NeedsHumanCaptcha,
    /// Do nothing; a newer pod owns the bot.
    FencedDoNothing,
    /// `NeedsUpdate`; wait for a rollout.
    MarkNeedsUpdate,
    /// Reassign proxy, then restart.
    ReassignProxyAndRestart,
    /// `Failed`; page admin (strict restore policy only).
    MarkFailed,
    /// Restore from last checkpoint; backoff with jitter; quarantine after
    /// N crashes in the budget window.
    CrashRestore,
}

/// Classify a pod exit. `exit_code` is `None` when the pod was killed by a
/// signal or the runtime reported no code — always a crash.
pub fn classify(exit_code: Option<i32>) -> ExitMeaning {
    match exit_code {
        Some(codes::CLEAN) => ExitMeaning::CleanDrain,
        Some(codes::CONFIG_INVALID) => ExitMeaning::ConfigInvalid,
        Some(codes::CREDENTIALS_REJECTED) => ExitMeaning::CredentialsRejected,
        Some(codes::CAPTCHA) => ExitMeaning::Captcha,
        Some(codes::FENCED) => ExitMeaning::Fenced,
        Some(codes::VERSION_MISMATCH) => ExitMeaning::VersionMismatch,
        Some(codes::PROXY_FAILURE) => ExitMeaning::ProxyFailure,
        Some(codes::SNAPSHOT_UNREADABLE_STRICT) => ExitMeaning::SnapshotUnreadableStrict,
        _ => ExitMeaning::Crash,
    }
}

/// The docs/02 §2.5 table, verbatim.
pub fn action(meaning: ExitMeaning) -> ExitAction {
    match meaning {
        ExitMeaning::CleanDrain => ExitAction::StartSuccessor,
        ExitMeaning::ConfigInvalid => ExitAction::MarkInvalidConfig,
        ExitMeaning::CredentialsRejected => ExitAction::NeedsHumanCredentials,
        ExitMeaning::Captcha => ExitAction::NeedsHumanCaptcha,
        ExitMeaning::Fenced => ExitAction::FencedDoNothing,
        ExitMeaning::VersionMismatch => ExitAction::MarkNeedsUpdate,
        ExitMeaning::ProxyFailure => ExitAction::ReassignProxyAndRestart,
        ExitMeaning::SnapshotUnreadableStrict => ExitAction::MarkFailed,
        ExitMeaning::Crash => ExitAction::CrashRestore,
    }
}

/// Convenience: exit code (or signal) -> operator action.
pub fn exit_action(exit_code: Option<i32>) -> ExitAction {
    action(classify(exit_code))
}

impl fmt::Display for ExitMeaning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ExitMeaning::CleanDrain => "clean_drain",
            ExitMeaning::ConfigInvalid => "config_invalid",
            ExitMeaning::CredentialsRejected => "credentials_rejected",
            ExitMeaning::Captcha => "captcha",
            ExitMeaning::Fenced => "fenced",
            ExitMeaning::VersionMismatch => "version_mismatch",
            ExitMeaning::ProxyFailure => "proxy_failure",
            ExitMeaning::SnapshotUnreadableStrict => "snapshot_unreadable_strict",
            ExitMeaning::Crash => "crash",
        };
        f.write_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_matches_doc() {
        assert_eq!(exit_action(Some(0)), ExitAction::StartSuccessor);
        assert_eq!(exit_action(Some(64)), ExitAction::MarkInvalidConfig);
        assert_eq!(exit_action(Some(65)), ExitAction::NeedsHumanCredentials);
        assert_eq!(exit_action(Some(66)), ExitAction::NeedsHumanCaptcha);
        assert_eq!(exit_action(Some(67)), ExitAction::FencedDoNothing);
        assert_eq!(exit_action(Some(68)), ExitAction::MarkNeedsUpdate);
        assert_eq!(exit_action(Some(69)), ExitAction::ReassignProxyAndRestart);
        assert_eq!(exit_action(Some(70)), ExitAction::MarkFailed);
        assert_eq!(exit_action(Some(1)), ExitAction::CrashRestore);
        assert_eq!(exit_action(Some(137)), ExitAction::CrashRestore);
        assert_eq!(exit_action(None), ExitAction::CrashRestore);
    }
}
