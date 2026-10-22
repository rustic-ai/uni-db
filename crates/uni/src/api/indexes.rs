// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Indexes facade for managing and rebuilding indexes.

use uni_common::{Result, UniError};

use super::UniInner;

/// Facade for index management operations.
///
/// Obtained via `db.indexes()`.
pub struct Indexes<'a> {
    pub(crate) inner: &'a UniInner,
}

impl Indexes<'_> {
    /// List index definitions, optionally filtered by label.
    pub fn list(&self, label: Option<&str>) -> Vec<uni_common::core::schema::IndexDefinition> {
        let schema = self.inner.schema.schema();
        match label {
            Some(l) => schema
                .indexes
                .iter()
                .filter(|i| i.label() == l)
                .cloned()
                .collect(),
            None => schema.indexes.clone(),
        }
    }

    /// Rebuild indexes for a label.
    ///
    /// When `background` is true, rebuilds asynchronously and returns a task ID.
    /// When `background` is false, blocks until complete and returns `None`.
    pub async fn rebuild(&self, label: &str, background: bool) -> Result<Option<String>> {
        if background {
            // Queued on the database's one manager, whose worker picks it up.
            let task_ids = self
                .inner
                .index_rebuild_manager()
                .await?
                .schedule(vec![label.to_string()])
                .await
                .map_err(UniError::Internal)?;

            Ok(task_ids.into_iter().next())
        } else {
            // Like a background build, it must not straddle a snapshot
            // restore, which waits for it and then rewinds.
            let _permit = self.inner.storage.index_build_permit().await;
            let idx_mgr = uni_store::storage::IndexManager::new(
                self.inner.storage.base_path(),
                self.inner.schema.clone(),
            )
            .with_backend(self.inner.storage.backend_arc());
            idx_mgr
                .rebuild_indexes_for_label(label)
                .await
                .map_err(UniError::Internal)?;
            Ok(None)
        }
    }

    /// Get status of all rebuild tasks.
    pub async fn rebuild_status(&self) -> Result<Vec<uni_store::storage::IndexRebuildTask>> {
        // The live manager's view, when there is one; otherwise the persisted
        // state, read without starting a worker just to answer a query.
        if let Some(manager) = self.inner.index_rebuild.get() {
            return Ok(manager.status());
        }
        let manager = uni_store::storage::IndexRebuildManager::new(
            self.inner.storage.clone(),
            self.inner.schema.clone(),
            self.inner.config.index_rebuild.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        Ok(manager.status())
    }

    /// Retry failed rebuild tasks. Returns retried task IDs.
    pub async fn retry_failed(&self) -> Result<Vec<String>> {
        self.inner
            .index_rebuild_manager()
            .await?
            .retry_failed()
            .await
            .map_err(UniError::Internal)
    }
}
