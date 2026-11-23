// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Durable persistence for the database-level Locy rule registry.
//!
//! Registered Locy rules live in memory in [`LocyRuleRegistry`], but the
//! *source* of each registration is persisted to `catalog/locy_rules.json`
//! through the same [`ObjectStore`] catalog mechanism that backs
//! `catalog/schema.json`. Only the source text is stored — never compiled
//! artifacts, which carry no binary-stability guarantee — so rules are
//! recompiled on open. A missing catalog file yields an empty registry,
//! keeping databases written by older builds openable.
//!
//! Persistence is scoped to the database-level registry only. Session,
//! transaction, and fork registries are deep-cloned in memory and stay
//! ephemeral, so their handles carry no [`LocyRulePersister`] and never
//! write the catalog.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectStorePath;
use uni_common::{Result, UniError};

use super::impl_locy::LocyRuleRegistry;

/// Current on-disk schema version for `catalog/locy_rules.json`.
pub(crate) const CATALOG_VERSION: u32 = 1;

/// One registered Locy program plus the rule names it defines.
///
/// `rule_names` is derived from compilation and recomputed on every rebuild,
/// so a stale or empty value loaded from disk is self-correcting.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RegisteredSource {
    /// Verbatim source text of one `db.rules().register()` call.
    pub source: String,
    /// Names of the rules this source defines, sorted for stable output.
    pub rule_names: Vec<String>,
}

/// Serialized form of the persisted Locy rule catalog.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct LocyRuleCatalogFile {
    /// On-disk format version; see [`CATALOG_VERSION`].
    pub version: u32,
    /// Registered sources, in registration order.
    pub rules: Vec<RegisteredSource>,
}

/// Writes the database-level Locy rule registry to the metadata catalog.
///
/// Holds the object store handle and the canonical catalog path. A single
/// async mutex serializes concurrent saves so a later-completing save always
/// writes the most recent registry snapshot.
#[derive(Debug)]
pub struct LocyRulePersister {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
    save_gate: tokio::sync::Mutex<()>,
    /// Whether the handle may still write.
    fence: Option<Arc<uni_store::lease::WriteFence>>,
}

impl LocyRulePersister {
    /// Creates a persister for the given store and catalog path.
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: ObjectStorePath) -> Self {
        Self {
            store,
            path,
            save_gate: tokio::sync::Mutex::new(()),
            fence: None,
        }
    }

    /// Gates saves on `fence`: a read-only or closed handle persists no rule.
    pub(crate) fn with_fence(mut self, fence: Arc<uni_store::lease::WriteFence>) -> Self {
        self.fence = Some(fence);
        self
    }

    /// Persists the registry's current sources to `catalog/locy_rules.json`.
    ///
    /// The full source list is written as one atomic `put`, mirroring
    /// [`SchemaManager::save`](uni_common::core::schema). The save gate is
    /// acquired *before* snapshotting the sources so that, under concurrent
    /// callers, the put reflecting the latest state always lands last.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::Internal`] if serialization or the object-store
    /// write fails.
    pub(crate) async fn save(&self, registry: &std::sync::RwLock<LocyRuleRegistry>) -> Result<()> {
        if let Some(fence) = &self.fence {
            fence.check().map_err(|e| match e.downcast::<UniError>() {
                Ok(typed) => typed,
                Err(other) => UniError::Internal(other),
            })?;
        }
        let _gate = self.save_gate.lock().await;
        let file = {
            let reg = registry
                .read()
                .map_err(|_| UniError::Internal(anyhow::anyhow!("locy rule registry poisoned")))?;
            LocyRuleCatalogFile {
                version: CATALOG_VERSION,
                rules: reg.sources.clone(),
            }
        };
        let json = serde_json::to_string_pretty(&file)
            .map_err(|e| UniError::Internal(anyhow::anyhow!("serialize locy rule catalog: {e}")))?;
        self.store
            .put(&self.path, json.into())
            .await
            .map_err(|e| UniError::Internal(e.into()))?;
        Ok(())
    }

    /// Loads persisted rule sources from `catalog/locy_rules.json`.
    ///
    /// A missing file yields an empty list so databases without the catalog
    /// open cleanly.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::Internal`] if the file exists but cannot be read,
    /// is not valid UTF-8/JSON, or carries an unsupported `version`.
    pub(crate) async fn load(
        store: Arc<dyn ObjectStore>,
        path: &ObjectStorePath,
    ) -> Result<Vec<RegisteredSource>> {
        match store.get(path).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| UniError::Internal(e.into()))?;
                let content =
                    String::from_utf8(bytes.to_vec()).map_err(|e| UniError::Internal(e.into()))?;
                let file: LocyRuleCatalogFile = serde_json::from_str(&content).map_err(|e| {
                    UniError::Internal(anyhow::anyhow!("parse catalog/locy_rules.json: {e}"))
                })?;
                if file.version != CATALOG_VERSION {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "catalog/locy_rules.json has unsupported version {} (expected {CATALOG_VERSION})",
                        file.version
                    )));
                }
                Ok(file.rules)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Vec::new()),
            Err(e) => Err(UniError::Internal(e.into())),
        }
    }
}
