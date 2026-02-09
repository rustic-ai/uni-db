// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Background index rebuild manager for async index building during bulk loading.
//!
//! This module provides `IndexRebuildManager` which handles background index
//! rebuilding with status tracking, retry logic, and persistence for restart recovery.

use crate::storage::index_manager::{IndexManager, IndexRebuildStatus, IndexRebuildTask};
use crate::storage::manager::StorageManager;
use anyhow::{Result, anyhow};
use chrono::Utc;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};
use uni_common::config::IndexRebuildConfig;
use uni_common::core::schema::SchemaManager;
use uuid::Uuid;

/// Persisted state for index rebuild tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRebuildState {
    /// All tracked tasks.
    pub tasks: Vec<IndexRebuildTask>,
    /// When this state was last updated.
    pub last_updated: chrono::DateTime<Utc>,
}

impl Default for IndexRebuildState {
    fn default() -> Self {
        Self {
            tasks: Vec::new(),
            last_updated: Utc::now(),
        }
    }
}

/// Manages background index rebuilding with status tracking and retry logic.
///
/// The manager maintains a queue of index rebuild tasks and processes them
/// in the background. Tasks can be monitored via `status()` and retried
/// via `retry_failed()`.
pub struct IndexRebuildManager {
    storage: Arc<StorageManager>,
    schema_manager: Arc<SchemaManager>,
    tasks: Arc<RwLock<HashMap<String, IndexRebuildTask>>>,
    config: IndexRebuildConfig,
    store: Arc<dyn ObjectStore>,
    state_path: ObjectPath,
}

impl IndexRebuildManager {
    /// Create a new IndexRebuildManager.
    pub async fn new(
        storage: Arc<StorageManager>,
        schema_manager: Arc<SchemaManager>,
        config: IndexRebuildConfig,
    ) -> Result<Self> {
        let store = storage.store();
        let state_path = ObjectPath::from("index_rebuild_state.json");

        let manager = Self {
            storage,
            schema_manager,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            config,
            store,
            state_path,
        };

        // Load persisted state if it exists
        manager.load_state().await?;

        Ok(manager)
    }

    /// Load persisted state from storage.
    async fn load_state(&self) -> Result<()> {
        match self.store.get(&self.state_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let state: IndexRebuildState = serde_json::from_slice(&bytes)?;

                let mut tasks = self.tasks.write();
                for task in state.tasks {
                    // Only restore non-completed tasks
                    if task.status != IndexRebuildStatus::Completed {
                        // Reset in-progress tasks to pending for retry
                        let mut task = task;
                        if task.status == IndexRebuildStatus::InProgress {
                            task.status = IndexRebuildStatus::Pending;
                            task.started_at = None;
                        }
                        tasks.insert(task.id.clone(), task);
                    }
                }
                info!(
                    "Loaded {} pending index rebuild tasks from state",
                    tasks.len()
                );
            }
            Err(object_store::Error::NotFound { .. }) => {
                // No persisted state, start fresh
            }
            Err(e) => {
                warn!("Failed to load index rebuild state: {}", e);
            }
        }
        Ok(())
    }

    /// Save current state to storage.
    async fn save_state(&self) -> Result<()> {
        let tasks: Vec<IndexRebuildTask> = self.tasks.read().values().cloned().collect();
        let state = IndexRebuildState {
            tasks,
            last_updated: Utc::now(),
        };
        let bytes = serde_json::to_vec_pretty(&state)?;
        self.store
            .put(&self.state_path, bytes.into())
            .await
            .map_err(|e| anyhow!("Failed to save index rebuild state: {}", e))?;
        Ok(())
    }

    /// Schedule labels for background index rebuild.
    ///
    /// Returns the task IDs for the scheduled rebuilds.
    pub async fn schedule(&self, labels: Vec<String>) -> Result<Vec<String>> {
        let mut task_ids = Vec::with_capacity(labels.len());
        let now = Utc::now();

        {
            let mut tasks = self.tasks.write();
            for label in labels {
                // Check if there's already a pending/in-progress task for this label
                let existing = tasks
                    .values()
                    .find(|t| {
                        t.label == label
                            && (t.status == IndexRebuildStatus::Pending
                                || t.status == IndexRebuildStatus::InProgress)
                    })
                    .map(|t| t.id.clone());

                if let Some(existing_id) = existing {
                    info!(
                        "Index rebuild for label '{}' already scheduled (task {})",
                        label, existing_id
                    );
                    task_ids.push(existing_id);
                    continue;
                }

                let task_id = Uuid::new_v4().to_string();
                let task = IndexRebuildTask {
                    id: task_id.clone(),
                    label: label.clone(),
                    status: IndexRebuildStatus::Pending,
                    created_at: now,
                    started_at: None,
                    completed_at: None,
                    error: None,
                    retry_count: 0,
                };
                tasks.insert(task_id.clone(), task);
                task_ids.push(task_id);
                info!("Scheduled index rebuild for label '{}'", label);
            }
        }

        // Persist state
        self.save_state().await?;

        Ok(task_ids)
    }

    /// Get status of all tasks.
    pub fn status(&self) -> Vec<IndexRebuildTask> {
        self.tasks.read().values().cloned().collect()
    }

    /// Get status of a specific task by ID.
    pub fn task_status(&self, task_id: &str) -> Option<IndexRebuildTask> {
        self.tasks.read().get(task_id).cloned()
    }

    /// Check if a label has a pending or in-progress index rebuild.
    pub fn is_index_building(&self, label: &str) -> bool {
        self.tasks.read().values().any(|t| {
            t.label == label
                && (t.status == IndexRebuildStatus::Pending
                    || t.status == IndexRebuildStatus::InProgress)
        })
    }

    /// Retry all failed tasks.
    pub async fn retry_failed(&self) -> Result<Vec<String>> {
        let mut retried = Vec::new();

        {
            let mut tasks = self.tasks.write();
            for task in tasks.values_mut() {
                if task.status == IndexRebuildStatus::Failed
                    && task.retry_count < self.config.max_retries
                {
                    task.status = IndexRebuildStatus::Pending;
                    task.error = None;
                    task.started_at = None;
                    task.completed_at = None;
                    retried.push(task.id.clone());
                    info!(
                        "Task {} for label '{}' scheduled for retry (attempt {})",
                        task.id,
                        task.label,
                        task.retry_count + 1
                    );
                }
            }
        }

        if !retried.is_empty() {
            self.save_state().await?;
        }

        Ok(retried)
    }

    /// Cancel a pending task.
    pub async fn cancel(&self, task_id: &str) -> Result<()> {
        {
            let mut tasks = self.tasks.write();
            if let Some(task) = tasks.get_mut(task_id) {
                if task.status == IndexRebuildStatus::Pending {
                    tasks.remove(task_id);
                    info!("Cancelled index rebuild task {}", task_id);
                } else if task.status == IndexRebuildStatus::InProgress {
                    return Err(anyhow!(
                        "Cannot cancel in-progress task. Wait for completion or restart."
                    ));
                } else {
                    return Err(anyhow!("Task {} is already completed or failed", task_id));
                }
            } else {
                return Err(anyhow!("Task {} not found", task_id));
            }
        }

        self.save_state().await?;
        Ok(())
    }

    /// Remove completed/failed tasks from tracking.
    pub async fn cleanup_completed(&self) -> Result<usize> {
        let removed;
        {
            let mut tasks = self.tasks.write();
            let before = tasks.len();
            tasks.retain(|_, t| {
                t.status == IndexRebuildStatus::Pending
                    || t.status == IndexRebuildStatus::InProgress
            });
            removed = before - tasks.len();
        }

        if removed > 0 {
            self.save_state().await?;
        }

        Ok(removed)
    }

    /// Start background worker that processes pending tasks.
    ///
    /// This spawns a tokio task that periodically checks for pending
    /// tasks and processes them.
    pub fn start_background_worker(
        self: Arc<Self>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.config.worker_check_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {

                // Find a pending task
                let task_to_process = {
                    let mut tasks = self.tasks.write();
                    let pending = tasks
                        .values_mut()
                        .find(|t| t.status == IndexRebuildStatus::Pending);

                    if let Some(task) = pending {
                        task.status = IndexRebuildStatus::InProgress;
                        task.started_at = Some(Utc::now());
                        Some((task.id.clone(), task.label.clone()))
                    } else {
                        None
                    }
                };

                if let Some((task_id, label)) = task_to_process {
                    // Save state before processing
                    if let Err(e) = self.save_state().await {
                        error!("Failed to save state before processing: {}", e);
                    }

                    info!("Starting index rebuild for label '{}'", label);

                    // Execute the index rebuild
                    let result = self.execute_rebuild(&label).await;

                    // Update task status
                    {
                        let mut tasks = self.tasks.write();
                        if let Some(task) = tasks.get_mut(&task_id) {
                            match result {
                                Ok(()) => {
                                    task.status = IndexRebuildStatus::Completed;
                                    task.completed_at = Some(Utc::now());
                                    task.error = None;
                                    info!("Index rebuild completed for label '{}'", label);
                                }
                                Err(e) => {
                                    task.status = IndexRebuildStatus::Failed;
                                    task.completed_at = Some(Utc::now());
                                    task.error = Some(e.to_string());
                                    task.retry_count += 1;
                                    error!("Index rebuild failed for label '{}': {}", label, e);

                                    // Auto-retry if within limits
                                    if task.retry_count < self.config.max_retries {
                                        info!(
                                            "Will retry index rebuild for '{}' after delay (attempt {}/{})",
                                            label, task.retry_count, self.config.max_retries
                                        );
                                        // Schedule retry by setting status back to pending after delay
                                        let manager = self.clone();
                                        let task_id_clone = task_id.clone();
                                        let delay = self.config.retry_delay;
                                        tokio::spawn(async move {
                                            tokio::time::sleep(delay).await;
                                            let mut tasks = manager.tasks.write();
                                            if let Some(task) = tasks.get_mut(&task_id_clone)
                                                && task.status == IndexRebuildStatus::Failed
                                            {
                                                task.status = IndexRebuildStatus::Pending;
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }

                    // Save state after processing
                    if let Err(e) = self.save_state().await {
                        error!("Failed to save state after processing: {}", e);
                    }
                }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Index rebuild worker shutting down");
                        let _ = self.save_state().await;
                        break;
                    }
                }
            }
        })
    }

    /// Execute the actual index rebuild for a label.
    async fn execute_rebuild(&self, label: &str) -> Result<()> {
        let idx_mgr = IndexManager::new(
            self.storage.base_path(),
            self.schema_manager.clone(),
            self.storage.lancedb_store_arc(),
        );
        idx_mgr.rebuild_indexes_for_label(label).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_rebuild_status_serialize() {
        let status = IndexRebuildStatus::Pending;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"Pending\"");

        let parsed: IndexRebuildStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, IndexRebuildStatus::Pending);
    }

    #[test]
    fn test_index_rebuild_task_serialize() {
        let task = IndexRebuildTask {
            id: "test-id".to_string(),
            label: "Person".to_string(),
            status: IndexRebuildStatus::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            error: None,
            retry_count: 0,
        };

        let json = serde_json::to_string(&task).unwrap();
        let parsed: IndexRebuildTask = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, task.id);
        assert_eq!(parsed.label, task.label);
        assert_eq!(parsed.status, task.status);
    }
}
