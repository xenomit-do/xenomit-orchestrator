//! `SecretStore` implementations. `MemorySecretStore` is the reference impl
//! for tests/dev; a real Vault client (KV v2 + policy/role management,
//! docs/09 §9.2a) implements the same trait and is out of scope for the
//! offline build.

use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use zeroize::Zeroizing;

use crate::traits::{BotSecretPaths, SecretError, SecretStore, SecretValue, bot_secret_paths};

/// In-memory `SecretStore` for tests and the `local` backend.
///
/// Behaves like the Vault contract: paths are provisioned per bot, values
/// are write-only after creation (reads go through `exists`), and
/// `destroy_bot` removes the whole `bots/<id>` subtree plus policy/role.
#[derive(Default)]
pub struct MemorySecretStore {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// path -> value. Values are held zeroized in the map.
    values: BTreeMap<String, Zeroizing<Vec<u8>>>,
    /// bot_id -> provisioned artifacts.
    provisioned: BTreeMap<String, BotSecretPaths>,
    /// destroyed bot_ids (for assertions).
    destroyed: BTreeSet<String>,
    /// Simulated unavailability (Vault sealed; docs/02 §2.2).
    unavailable: bool,
}

impl MemorySecretStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulate a sealed Vault (bots stay Pending; docs/09 §9.2a).
    pub fn set_unavailable(&self, unavailable: bool) {
        self.inner.lock().unwrap().unavailable = unavailable;
    }

    fn check(&self) -> Result<(), SecretError> {
        if self.inner.lock().unwrap().unavailable {
            return Err(SecretError::Unavailable("sealed".into()));
        }
        Ok(())
    }

    /// Test hook: is a bot fully provisioned?
    pub fn is_provisioned(&self, bot_id: &str) -> bool {
        self.inner.lock().unwrap().provisioned.contains_key(bot_id)
    }

    /// Test hook: was `destroy_bot` called?
    pub fn is_destroyed(&self, bot_id: &str) -> bool {
        self.inner.lock().unwrap().destroyed.contains(bot_id)
    }

    /// Test hook: does a path hold a value?
    pub fn has_value(&self, path: &str) -> bool {
        self.inner.lock().unwrap().values.contains_key(path)
    }
}

#[async_trait]
impl SecretStore for MemorySecretStore {
    async fn provision_bot(&self, bot_id: &str) -> Result<BotSecretPaths, SecretError> {
        self.check()?;
        let mut inner = self.inner.lock().unwrap();
        if let Some(paths) = inner.provisioned.get(bot_id) {
            return Ok(paths.clone());
        }
        let paths = bot_secret_paths(bot_id);
        inner.provisioned.insert(bot_id.to_string(), paths.clone());
        Ok(paths)
    }

    async fn write(&self, path: &str, value: SecretValue) -> Result<(), SecretError> {
        self.check()?;
        self.inner
            .lock()
            .unwrap()
            .values
            .insert(path.to_string(), Zeroizing::new(value.as_bytes().to_vec()));
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool, SecretError> {
        self.check()?;
        Ok(self.inner.lock().unwrap().values.contains_key(path))
    }

    async fn delete_path(&self, path: &str) -> Result<bool, SecretError> {
        self.check()?;
        Ok(self.inner.lock().unwrap().values.remove(path).is_some())
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), SecretError> {
        self.check()?;
        let mut inner = self.inner.lock().unwrap();
        let Some(value) = inner.values.get(from).cloned() else {
            return Err(SecretError::Other(format!("no secret at {from}")));
        };
        inner.values.insert(to.to_string(), value);
        Ok(())
    }

    async fn destroy_bot(&self, bot_id: &str) -> Result<(), SecretError> {
        self.check()?;
        let mut inner = self.inner.lock().unwrap();
        let prefix = format!("secret/data/bots/{bot_id}");
        inner.values.retain(|k, _| !k.starts_with(&prefix));
        inner.provisioned.remove(bot_id);
        inner.destroyed.insert(bot_id.to_string());
        Ok(())
    }
}
