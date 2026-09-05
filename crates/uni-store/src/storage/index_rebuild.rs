// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Background index rebuild manager for async index building during bulk loading.
//!
//! This module provides `IndexRebuildManager` which handles background index
//! rebuilding with status tracking, retry logic, and persistence for restart recovery.

#[cfg(feature = "lance-backend")]
use crate::storage::index_manager::IndexManager;
use crate::storage::index_manager::{IndexRebuildStatus, IndexRebuildTask};
use crate::storage::manager::StorageManager;
use anyhow::{Result, anyhow};
use chrono::Utc;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{error, info, warn};
use uni_common::config::IndexRebuildConfig;
use uni_common::core::schema::{IndexDefinition, IndexStatus, SchemaManager};
use uni_common::core::snapshot::SnapshotManifest;
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

/// Checks whether indexes need rebuilding based on growth and time thresholds.
pub struct RebuildTriggerChecker {
    config: IndexRebuildConfig,
}

impl RebuildTriggerChecker {
    pub fn new(config: IndexRebuildConfig) -> Self {
        Self { config }
    }

    /// Returns the list of labels whose indexes need rebuilding based on
    /// configured growth and time thresholds.
    ///
    /// Skips indexes with `Building` or `Failed` status.
    pub fn labels_needing_rebuild(
        &self,
        manifest: &SnapshotManifest,
        indexes: &[IndexDefinition],
    ) -> Vec<String> {
        let mut labels: HashSet<String> = HashSet::new();
        let now = Utc::now();

        for idx in indexes {
            let meta = idx.metadata();

            // Skip indexes that are already being rebuilt or have permanently failed
            if meta.status == IndexStatus::Building || meta.status == IndexStatus::Failed {
                continue;
            }

            let label = idx.label();

            // `Stale` means "needs building", and nothing acted on it: only the
            // growth and time triggers below could pick an index up, so a stale
            // index was rebuilt just in case one of them happened to fire later.
            // Every writer of `Stale` means the same thing — a retryable rebuild
            // failure (`handle_rebuild_failure`), a failed Lance build
            // (`BuildOutcome::Failed`), drift detected at flush, and bulk load —
            // so the status is a trigger in its own right (#233 Tier 3).
            //
            // This is also what makes a dropped rebuild schedule recoverable:
            // the index stays `Stale` and the next trigger pass picks it up,
            // where before it waited on an unrelated trigger.
            if meta.status == IndexStatus::Stale {
                labels.insert(label.to_string());
                continue;
            }

            // Growth trigger: current count exceeds row_count_at_build * (1 + ratio)
            if self.config.growth_trigger_ratio > 0.0
                && let Some(built_count) = meta.row_count_at_build
            {
                let current_count = manifest.vertices.get(label).map(|ls| ls.count).unwrap_or(0);
                let threshold =
                    (built_count as f64 * (1.0 + self.config.growth_trigger_ratio)) as u64;
                if current_count > threshold {
                    labels.insert(label.to_string());
                    continue;
                }
            }

            // Time trigger: index age exceeds max_index_age
            if let Some(max_age) = self.config.max_index_age
                && let Some(built_at) = meta.last_built_at
            {
                let age = now.signed_duration_since(built_at);
                if age.to_std().unwrap_or_default() > max_age {
                    labels.insert(label.to_string());
                }
            }
        }

        labels.into_iter().collect()
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
                        self.process_next_pending_task().await;
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Index rebuild worker shutting down");
                        let _ = self.save_state().await;
                        break;
                    }
                }
            }
        })
    }

    /// Claim and process the next pending index rebuild task, if any.
    ///
    /// Marks the task as in-progress, executes the rebuild, updates task
    /// status and index metadata, and persists state.
    async fn process_next_pending_task(self: &Arc<Self>) {
        // Find and claim a pending task
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

        let Some((task_id, label)) = task_to_process else {
            return;
        };

        // Save state before processing
        if let Err(e) = self.save_state().await {
            error!("Failed to save state before processing: {}", e);
        }

        info!("Starting index rebuild for label '{}'", label);
        self.set_index_status_for_label(&label, IndexStatus::Building);

        // Execute the index rebuild
        let result = self.execute_rebuild(&label).await;

        match result {
            Ok(()) => self.handle_rebuild_success(&task_id, &label).await,
            Err(e) => self.handle_rebuild_failure(&task_id, &label, e),
        }

        // Save state and schema after processing
        if let Err(e) = self.save_state().await {
            error!("Failed to save state after processing: {}", e);
        }
        if let Err(e) = self.schema_manager.save().await {
            error!("Failed to save schema after index rebuild: {}", e);
        }
    }

    /// Handle a successful index rebuild: mark task completed and update index metadata.
    async fn handle_rebuild_success(&self, task_id: &str, label: &str) {
        let now = Utc::now();
        let row_count = self.get_label_row_count(label).await;

        {
            let mut tasks = self.tasks.write();
            if let Some(task) = tasks.get_mut(task_id) {
                task.status = IndexRebuildStatus::Completed;
                task.completed_at = Some(now);
                task.error = None;
            }
        }
        info!("Index rebuild completed for label '{}'", label);

        self.update_index_metadata_for_label(label, IndexStatus::Online, Some(now), row_count);
    }

    /// Handle a failed index rebuild: mark task failed and schedule retry if within limits.
    fn handle_rebuild_failure(self: &Arc<Self>, task_id: &str, label: &str, err: anyhow::Error) {
        let (retry_count, exhausted) = {
            let mut tasks = self.tasks.write();
            if let Some(task) = tasks.get_mut(task_id) {
                task.status = IndexRebuildStatus::Failed;
                task.completed_at = Some(Utc::now());
                task.error = Some(err.to_string());
                task.retry_count += 1;
                (
                    task.retry_count,
                    task.retry_count >= self.config.max_retries,
                )
            } else {
                (0, true)
            }
        };
        error!("Index rebuild failed for label '{}': {}", label, err);

        if exhausted {
            self.set_index_status_for_label(label, IndexStatus::Failed);
        } else {
            self.set_index_status_for_label(label, IndexStatus::Stale);
            info!(
                "Will retry index rebuild for '{}' after delay (attempt {}/{})",
                label, retry_count, self.config.max_retries
            );
            let manager = self.clone();
            let task_id_owned = task_id.to_string();
            let delay = self.config.retry_delay;
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let mut tasks = manager.tasks.write();
                if let Some(task) = tasks.get_mut(&task_id_owned)
                    && task.status == IndexRebuildStatus::Failed
                {
                    task.status = IndexRebuildStatus::Pending;
                }
            });
        }
    }

    /// Set the lifecycle status for all indexes on a given label.
    fn set_index_status_for_label(&self, label: &str, status: IndexStatus) {
        let schema = self.schema_manager.schema();
        for idx in &schema.indexes {
            if idx.label() == label {
                let name = idx.name().to_string();
                if let Err(e) = self.schema_manager.update_index_metadata(&name, |m| {
                    m.status = status.clone();
                }) {
                    // A discarded status write leaves the index reporting its
                    // *previous* status, so an index that just failed to build
                    // still reads `Online` and every gate that consults the
                    // status is misled — the shape the index-status-honesty work
                    // already fixed once elsewhere. There is no caller to
                    // propagate to on these background paths, so it is recorded
                    // loudly and counted instead (#233 Tier 3).
                    metrics::counter!("uni_index_status_write_failures_total").increment(1);
                    tracing::error!(
                        index = %name,
                        target_status = ?status,
                        error = %e,
                        "failed to record index status; the index still reports its previous status"
                    );
                }
            }
        }
    }

    /// Update index metadata (status, build time, row count) for all indexes on a label.
    fn update_index_metadata_for_label(
        &self,
        label: &str,
        status: IndexStatus,
        last_built_at: Option<chrono::DateTime<Utc>>,
        row_count: Option<u64>,
    ) {
        let schema = self.schema_manager.schema();
        for idx in &schema.indexes {
            if idx.label() == label {
                let name = idx.name().to_string();
                if let Err(e) = self.schema_manager.update_index_metadata(&name, |m| {
                    m.status = status.clone();
                    if let Some(ts) = last_built_at {
                        m.last_built_at = Some(ts);
                    }
                    if let Some(count) = row_count {
                        m.row_count_at_build = Some(count);
                    }
                }) {
                    metrics::counter!("uni_index_status_write_failures_total").increment(1);
                    tracing::error!(
                        index = %name,
                        target_status = ?status,
                        error = %e,
                        "failed to record index build metadata; the index still \
                         reports its previous status and build time"
                    );
                }
            }
        }
    }

    /// Get the current row count for a label from the latest snapshot.
    async fn get_label_row_count(&self, label: &str) -> Option<u64> {
        let manifest = self
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .ok()
            .flatten()?;
        manifest.vertices.get(label).map(|ls| ls.count)
    }

    /// Execute the actual index rebuild for a label.
    #[cfg(feature = "lance-backend")]
    async fn execute_rebuild(&self, label: &str) -> Result<()> {
        let idx_mgr = IndexManager::new(self.storage.base_path(), self.schema_manager.clone())
            .with_backend(self.storage.backend_arc());
        idx_mgr.rebuild_indexes_for_label(label).await
    }

    /// Execute the actual index rebuild for a label (stub when lance-backend is disabled).
    #[cfg(not(feature = "lance-backend"))]
    async fn execute_rebuild(&self, _label: &str) -> Result<()> {
        Err(anyhow!("Index rebuild requires the lance-backend feature"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_common::core::schema::IndexMetadata;

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

    fn make_test_manifest(label: &str, count: u64) -> SnapshotManifest {
        use uni_common::core::snapshot::LabelSnapshot;

        let mut manifest = SnapshotManifest::new("test".into(), 1);
        manifest.vertices.insert(
            label.to_string(),
            LabelSnapshot {
                version: 1,
                count,
                lance_version: 0,
            },
        );
        manifest
    }

    fn make_scalar_index(label: &str, status: IndexStatus, meta: IndexMetadata) -> IndexDefinition {
        use uni_common::core::schema::{ScalarIndexConfig, ScalarIndexType};
        IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}", label),
            label: label.to_string(),
            properties: vec!["prop".to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: IndexMetadata { status, ..meta },
        })
    }

    #[test]
    fn test_trigger_growth_fires() {
        let config = IndexRebuildConfig {
            growth_trigger_ratio: 0.5,
            ..Default::default()
        };
        let checker = RebuildTriggerChecker::new(config);

        // Built at 100 rows, now 151 (> 100 * 1.5 = 150)
        let manifest = make_test_manifest("Person", 151);
        let indexes = vec![make_scalar_index(
            "Person",
            IndexStatus::Online,
            IndexMetadata {
                row_count_at_build: Some(100),
                ..Default::default()
            },
        )];

        let labels = checker.labels_needing_rebuild(&manifest, &indexes);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "Person");
    }

    #[test]
    fn test_trigger_growth_below_threshold() {
        let config = IndexRebuildConfig {
            growth_trigger_ratio: 0.5,
            ..Default::default()
        };
        let checker = RebuildTriggerChecker::new(config);

        // Built at 100 rows, now 120 (< 100 * 1.5 = 150)
        let manifest = make_test_manifest("Person", 120);
        let indexes = vec![make_scalar_index(
            "Person",
            IndexStatus::Online,
            IndexMetadata {
                row_count_at_build: Some(100),
                ..Default::default()
            },
        )];

        let labels = checker.labels_needing_rebuild(&manifest, &indexes);
        assert!(labels.is_empty());
    }

    #[test]
    fn test_trigger_time_based() {
        let config = IndexRebuildConfig {
            growth_trigger_ratio: 0.0, // disable growth trigger
            max_index_age: Some(std::time::Duration::from_secs(3600)), // 1 hour
            ..Default::default()
        };
        let checker = RebuildTriggerChecker::new(config);

        // Built 2 hours ago
        let two_hours_ago = Utc::now() - chrono::Duration::hours(2);
        let manifest = make_test_manifest("Person", 100);
        let indexes = vec![make_scalar_index(
            "Person",
            IndexStatus::Online,
            IndexMetadata {
                last_built_at: Some(two_hours_ago),
                row_count_at_build: Some(100),
                ..Default::default()
            },
        )];

        let labels = checker.labels_needing_rebuild(&manifest, &indexes);
        assert_eq!(labels.len(), 1);
    }

    /// A `Stale` index is rebuilt because it is stale, not by coincidence.
    ///
    /// `Stale` is written by every path that means "this needs building" — a
    /// retryable rebuild failure, a failed Lance build (`BuildOutcome::Failed`),
    /// drift detected at flush, and bulk load — and nothing acted on it: only the
    /// growth and time triggers could pick an index up, so a stale index waited
    /// on an unrelated trigger happening to fire (#233 Tier 3).
    ///
    /// Both triggers are disabled here, so the only thing that can select this
    /// index is its status.
    #[test]
    fn test_trigger_fires_on_stale_status_alone() {
        let config = IndexRebuildConfig {
            growth_trigger_ratio: 0.0,
            max_index_age: None,
            ..Default::default()
        };
        let checker = RebuildTriggerChecker::new(config);

        let manifest = make_test_manifest("Person", 100);
        let stale = make_scalar_index(
            "Person",
            IndexStatus::Stale,
            IndexMetadata {
                // Matching the manifest, so growth could not fire even if enabled.
                row_count_at_build: Some(100),
                last_built_at: Some(Utc::now()),
                ..Default::default()
            },
        );
        assert_eq!(
            checker.labels_needing_rebuild(&manifest, &[stale]),
            vec!["Person".to_string()]
        );

        // The control: identical metadata, `Online`, must not be selected — or
        // the assertion above would pass for any index at all.
        let online = make_scalar_index(
            "Person",
            IndexStatus::Online,
            IndexMetadata {
                row_count_at_build: Some(100),
                last_built_at: Some(Utc::now()),
                ..Default::default()
            },
        );
        assert!(
            checker
                .labels_needing_rebuild(&manifest, &[online])
                .is_empty()
        );
    }

    #[test]
    fn test_trigger_skips_building_and_failed() {
        let config = IndexRebuildConfig {
            growth_trigger_ratio: 0.5,
            ..Default::default()
        };
        let checker = RebuildTriggerChecker::new(config);

        // Would trigger (151 > 150), but status is Building
        let manifest = make_test_manifest("Person", 151);
        let building = vec![make_scalar_index(
            "Person",
            IndexStatus::Building,
            IndexMetadata {
                row_count_at_build: Some(100),
                ..Default::default()
            },
        )];
        assert!(
            checker
                .labels_needing_rebuild(&manifest, &building)
                .is_empty()
        );

        // Same with Failed status
        let failed = vec![make_scalar_index(
            "Person",
            IndexStatus::Failed,
            IndexMetadata {
                row_count_at_build: Some(100),
                ..Default::default()
            },
        )];
        assert!(
            checker
                .labels_needing_rebuild(&manifest, &failed)
                .is_empty()
        );
    }
}
