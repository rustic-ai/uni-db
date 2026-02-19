// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::embedding::{EmbeddingService, create_embedding_service, embedding_service_key};
use crate::runtime::context::QueryContext;
use crate::runtime::id_allocator::IdAllocator;
use crate::runtime::l0::L0Buffer;
use crate::runtime::l0_manager::L0Manager;
use crate::runtime::property_manager::PropertyManager;
use crate::runtime::wal::WriteAheadLog;
use crate::storage::adjacency_manager::AdjacencyManager;
use crate::storage::delta::{L1Entry, Op};
use crate::storage::main_edge::MainEdgeDataset;
use crate::storage::main_vertex::MainVertexDataset;
use crate::storage::manager::StorageManager;
use anyhow::{Result, anyhow};
use chrono::Utc;
use futures::TryStreamExt;
use metrics;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, instrument};
use uni_common::Properties;
use uni_common::Value;
use uni_common::config::UniConfig;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{ConstraintTarget, ConstraintType, EmbeddingModel, IndexDefinition};
use uni_common::core::snapshot::{EdgeSnapshot, LabelSnapshot, SnapshotManifest};
use uni_common::sync::acquire_mutex;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct WriterConfig {
    pub max_mutations: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            max_mutations: 10_000,
        }
    }
}

pub struct Writer {
    pub l0_manager: Arc<L0Manager>,
    pub storage: Arc<StorageManager>,
    pub schema_manager: Arc<uni_common::core::schema::SchemaManager>,
    pub allocator: Arc<IdAllocator>,
    pub config: UniConfig,
    pub embedding_services: Arc<Mutex<HashMap<String, Arc<dyn EmbeddingService>>>>,
    pub transaction_l0: Option<Arc<RwLock<L0Buffer>>>,
    /// Property manager for cache invalidation after flush
    pub property_manager: Option<Arc<PropertyManager>>,
    /// Adjacency manager for dual-write (edges survive flush).
    adjacency_manager: Arc<AdjacencyManager>,
    /// Timestamp of last flush or creation
    last_flush_time: std::time::Instant,
}

impl Writer {
    pub async fn new(
        storage: Arc<StorageManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        start_version: u64,
    ) -> Result<Self> {
        Self::new_with_config(
            storage,
            schema_manager,
            start_version,
            UniConfig::default(),
            None,
            None,
        )
        .await
    }

    pub async fn new_with_config(
        storage: Arc<StorageManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        start_version: u64,
        config: UniConfig,
        wal: Option<Arc<WriteAheadLog>>,
        allocator: Option<Arc<IdAllocator>>,
    ) -> Result<Self> {
        let allocator = if let Some(a) = allocator {
            a
        } else {
            let store = storage.store();
            let path = object_store::path::Path::from("id_allocator.json");
            Arc::new(IdAllocator::new(store, path, 1000).await?)
        };

        let l0_manager = Arc::new(L0Manager::new(start_version, wal));

        let property_manager = Some(Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            1000,
        )));

        let adjacency_manager = storage.adjacency_manager();

        Ok(Self {
            l0_manager,
            storage,
            schema_manager,
            allocator,
            config,
            embedding_services: Arc::new(Mutex::new(HashMap::new())),
            transaction_l0: None,
            property_manager,
            adjacency_manager,
            last_flush_time: std::time::Instant::now(),
        })
    }

    /// Replay WAL mutations into the current L0 buffer.
    pub async fn replay_wal(&self, wal_high_water_mark: u64) -> Result<usize> {
        let l0 = self.l0_manager.get_current();
        let wal = l0.read().wal.clone();

        if let Some(wal) = wal {
            wal.initialize().await?;
            let mutations = wal.replay_since(wal_high_water_mark).await?;
            let count = mutations.len();

            if count > 0 {
                log::info!(
                    "Replaying {} mutations from WAL (LSN > {})",
                    count,
                    wal_high_water_mark
                );
                let mut l0_guard = l0.write();
                l0_guard.replay_mutations(mutations)?;
            }

            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// Allocates the next VID (pure auto-increment).
    pub async fn next_vid(&self) -> Result<Vid> {
        self.allocator.allocate_vid().await
    }

    /// Allocates multiple VIDs at once for bulk operations.
    /// This is more efficient than calling next_vid() in a loop.
    pub async fn allocate_vids(&self, count: usize) -> Result<Vec<Vid>> {
        self.allocator.allocate_vids(count).await
    }

    /// Allocates the next EID (pure auto-increment).
    pub async fn next_eid(&self, _type_id: u32) -> Result<Eid> {
        self.allocator.allocate_eid().await
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.transaction_l0.is_some() {
            return Err(anyhow!("Transaction already active"));
        }
        let current_version = self.l0_manager.get_current().read().current_version;
        // Transaction mutations are logged to WAL at COMMIT time, not during the transaction.
        self.transaction_l0 = Some(Arc::new(RwLock::new(L0Buffer::new(current_version, None))));
        Ok(())
    }

    /// Returns the active L0 buffer: the transaction L0 if a transaction is open,
    /// otherwise the current L0 from the manager.
    fn active_l0(&self) -> Arc<RwLock<L0Buffer>> {
        self.transaction_l0
            .clone()
            .unwrap_or_else(|| self.l0_manager.get_current())
    }

    fn update_metrics(&self) {
        let l0 = self.l0_manager.get_current();
        let size = l0.read().size_bytes();
        metrics::gauge!("l0_buffer_size_bytes").set(size as f64);

        if let Some(tx_l0) = &self.transaction_l0 {
            metrics::gauge!("active_transactions").set(1.0);
            let tx_size = tx_l0.read().size_bytes();
            metrics::gauge!("transaction_l0_size_bytes").set(tx_size as f64);
        } else {
            metrics::gauge!("active_transactions").set(0.0);
            metrics::gauge!("transaction_l0_size_bytes").set(0.0);
        }
    }

    pub async fn commit_transaction(&mut self) -> Result<()> {
        let tx_l0_arc = self
            .transaction_l0
            .take()
            .ok_or_else(|| anyhow!("No active transaction"))?;

        {
            let tx_l0 = tx_l0_arc.read();
            let main_l0_arc = self.l0_manager.get_current();
            let mut main_l0 = main_l0_arc.write();
            main_l0.merge(&tx_l0)?;

            // Replay transaction edges into the AdjacencyManager overlay so they
            // become visible to queries that read from the AM (post-migration path).
            let version = main_l0.current_version;
            for (eid, (src, dst, etype)) in &tx_l0.edge_endpoints {
                if tx_l0.tombstones.contains_key(eid) {
                    self.adjacency_manager
                        .add_tombstone(*eid, *src, *dst, *etype, version);
                } else {
                    self.adjacency_manager
                        .insert_edge(*src, *dst, *eid, *etype, version);
                }
            }
        }

        self.update_metrics();

        // Flush WAL to ensure transaction is durable before returning
        self.flush_wal().await?;

        self.check_flush().await
    }

    /// Flush the WAL buffer to durable storage.
    pub async fn flush_wal(&self) -> Result<()> {
        let l0 = self.l0_manager.get_current();
        let wal = l0.read().wal.clone();

        if let Some(wal) = wal {
            wal.flush().await?;
        }
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<()> {
        if self.transaction_l0.is_none() {
            return Err(anyhow!("No active transaction"));
        }
        self.transaction_l0 = None;
        Ok(())
    }

    /// Validates vertex constraints for the given properties.
    /// In the new design, label is passed as a parameter since VID no longer embeds label.
    async fn validate_vertex_constraints_for_label(
        &self,
        vid: Vid,
        properties: &Properties,
        label: &str,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();

        {
            // 1. Check NOT NULL constraints (from Property definitions)
            if let Some(props_meta) = schema.properties.get(label) {
                for (prop_name, meta) in props_meta {
                    if !meta.nullable && properties.get(prop_name).is_none_or(|v| v.is_null()) {
                        log::warn!(
                            "Constraint violation: Property '{}' cannot be null for label '{}'",
                            prop_name,
                            label
                        );
                        return Err(anyhow!(
                            "Constraint violation: Property '{}' cannot be null",
                            prop_name
                        ));
                    }
                }
            }

            // 2. Check Explicit Constraints (Unique, Check, etc.)
            for constraint in &schema.constraints {
                if !constraint.enabled {
                    continue;
                }
                match &constraint.target {
                    ConstraintTarget::Label(l) if l == label => {}
                    _ => continue,
                }

                match &constraint.constraint_type {
                    ConstraintType::Unique {
                        properties: unique_props,
                    } => {
                        // Support single and multi-property unique constraints
                        if !unique_props.is_empty() {
                            let mut key_values = Vec::new();
                            let mut missing = false;
                            for prop in unique_props {
                                if let Some(val) = properties.get(prop) {
                                    key_values.push((prop.clone(), val.clone()));
                                } else {
                                    missing = true; // Can't enforce if property missing (partial update?)
                                    // For INSERT, missing means null?
                                    // If property is nullable, unique constraint typically allows multiple nulls or ignores?
                                    // For now, only check if ALL keys are present
                                }
                            }

                            if !missing {
                                self.check_unique_constraint_multi(label, &key_values, vid)
                                    .await?;
                            }
                        }
                    }
                    ConstraintType::Exists { property } => {
                        if properties.get(property).is_none_or(|v| v.is_null()) {
                            log::warn!(
                                "Constraint violation: Property '{}' must exist for label '{}'",
                                property,
                                label
                            );
                            return Err(anyhow!(
                                "Constraint violation: Property '{}' must exist",
                                property
                            ));
                        }
                    }
                    ConstraintType::Check { expression } => {
                        if !self.evaluate_check_constraint(expression, properties)? {
                            return Err(anyhow!(
                                "CHECK constraint '{}' violated: expression '{}' evaluated to false",
                                constraint.name,
                                expression
                            ));
                        }
                    }
                    _ => {
                        return Err(anyhow!("Unsupported constraint type"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates vertex constraints for a vertex with the given labels.
    /// Labels must be passed explicitly since the vertex may not yet be in L0.
    /// Unknown labels (not in schema) are skipped.
    async fn validate_vertex_constraints(
        &self,
        vid: Vid,
        properties: &Properties,
        labels: &[String],
    ) -> Result<()> {
        let schema = self.schema_manager.schema();

        // Validate constraints only for known labels
        for label in labels {
            // Skip unknown labels (schemaless support)
            if schema.get_label_case_insensitive(label).is_none() {
                continue;
            }
            self.validate_vertex_constraints_for_label(vid, properties, label)
                .await?;
        }

        // Check global ext_id uniqueness if ext_id is provided
        if let Some(ext_id) = properties.get("ext_id").and_then(|v| v.as_str()) {
            self.check_extid_globally_unique(ext_id, vid).await?;
        }

        Ok(())
    }

    /// Collect ext_ids and unique constraint keys from an iterator of vertex properties.
    ///
    /// Used to build a constraint key index from L0 buffers for batch validation.
    fn collect_constraint_keys_from_properties<'a>(
        properties_iter: impl Iterator<Item = &'a Properties>,
        label: &str,
        constraints: &[uni_common::core::schema::Constraint],
        existing_keys: &mut HashMap<String, HashSet<String>>,
        existing_extids: &mut HashSet<String>,
    ) {
        for props in properties_iter {
            if let Some(ext_id) = props.get("ext_id").and_then(|v| v.as_str()) {
                existing_extids.insert(ext_id.to_string());
            }

            for constraint in constraints {
                if !constraint.enabled {
                    continue;
                }
                if let ConstraintTarget::Label(l) = &constraint.target {
                    if l != label {
                        continue;
                    }
                } else {
                    continue;
                }

                if let ConstraintType::Unique {
                    properties: unique_props,
                } = &constraint.constraint_type
                {
                    let mut key_parts = Vec::new();
                    let mut all_present = true;
                    for prop in unique_props {
                        if let Some(val) = props.get(prop) {
                            key_parts.push(format!("{}:{}", prop, val));
                        } else {
                            all_present = false;
                            break;
                        }
                    }
                    if all_present {
                        let key = key_parts.join("|");
                        existing_keys
                            .entry(constraint.name.clone())
                            .or_default()
                            .insert(key);
                    }
                }
            }
        }
    }

    /// Validates constraints for a batch of vertices efficiently.
    ///
    /// This method builds an in-memory index from L0 buffers ONCE instead of scanning
    /// per vertex, reducing complexity from O(n²) to O(n) for bulk inserts.
    ///
    /// # Arguments
    /// * `vids` - VIDs of vertices being inserted
    /// * `properties_batch` - Properties for each vertex
    /// * `label` - Label for all vertices (assumes single label for now)
    ///
    /// # Performance
    /// For N vertices with unique constraints:
    /// - Old approach: O(N²) - scan L0 buffer N times
    /// - New approach: O(N) - scan L0 buffer once, build HashSet, check each vertex in O(1)
    async fn validate_vertex_batch_constraints(
        &self,
        vids: &[Vid],
        properties_batch: &[Properties],
        label: &str,
    ) -> Result<()> {
        if vids.len() != properties_batch.len() {
            return Err(anyhow!("VID/properties length mismatch"));
        }

        let schema = self.schema_manager.schema();

        // 1. Validate NOT NULL constraints for each vertex
        if let Some(props_meta) = schema.properties.get(label) {
            for (idx, properties) in properties_batch.iter().enumerate() {
                for (prop_name, meta) in props_meta {
                    if !meta.nullable && properties.get(prop_name).is_none_or(|v| v.is_null()) {
                        return Err(anyhow!(
                            "Constraint violation at index {}: Property '{}' cannot be null",
                            idx,
                            prop_name
                        ));
                    }
                }
            }
        }

        // 2. Build constraint key index from L0 buffers (ONCE for entire batch)
        let mut existing_keys: HashMap<String, HashSet<String>> = HashMap::new();
        let mut existing_extids: HashSet<String> = HashSet::new();

        // Scan current L0 buffer
        {
            let l0 = self.l0_manager.get_current();
            let l0_guard = l0.read();
            Self::collect_constraint_keys_from_properties(
                l0_guard.vertex_properties.values(),
                label,
                &schema.constraints,
                &mut existing_keys,
                &mut existing_extids,
            );
        }

        // Scan transaction L0 if present
        if let Some(tx_l0) = &self.transaction_l0 {
            let tx_l0_guard = tx_l0.read();
            Self::collect_constraint_keys_from_properties(
                tx_l0_guard.vertex_properties.values(),
                label,
                &schema.constraints,
                &mut existing_keys,
                &mut existing_extids,
            );
        }

        // 3. Check batch vertices against index AND check for duplicates within batch
        let mut batch_keys: HashMap<String, HashMap<String, usize>> = HashMap::new();
        let mut batch_extids: HashMap<String, usize> = HashMap::new();

        for (idx, (_vid, properties)) in vids.iter().zip(properties_batch.iter()).enumerate() {
            // Check ext_id uniqueness
            if let Some(ext_id) = properties.get("ext_id").and_then(|v| v.as_str()) {
                if existing_extids.contains(ext_id) {
                    return Err(anyhow!(
                        "Constraint violation at index {}: ext_id '{}' already exists",
                        idx,
                        ext_id
                    ));
                }
                if let Some(first_idx) = batch_extids.get(ext_id) {
                    return Err(anyhow!(
                        "Constraint violation: ext_id '{}' duplicated in batch at indices {} and {}",
                        ext_id,
                        first_idx,
                        idx
                    ));
                }
                batch_extids.insert(ext_id.to_string(), idx);
            }

            // Check unique constraints
            for constraint in &schema.constraints {
                if !constraint.enabled {
                    continue;
                }
                if let ConstraintTarget::Label(l) = &constraint.target {
                    if l != label {
                        continue;
                    }
                } else {
                    continue;
                }

                match &constraint.constraint_type {
                    ConstraintType::Unique {
                        properties: unique_props,
                    } => {
                        let mut key_parts = Vec::new();
                        let mut all_present = true;
                        for prop in unique_props {
                            if let Some(val) = properties.get(prop) {
                                key_parts.push(format!("{}:{}", prop, val));
                            } else {
                                all_present = false;
                                break;
                            }
                        }

                        if all_present {
                            let key = key_parts.join("|");

                            // Check against existing L0 keys
                            if let Some(keys) = existing_keys.get(&constraint.name)
                                && keys.contains(&key)
                            {
                                return Err(anyhow!(
                                    "Constraint violation at index {}: Duplicate composite key for label '{}' (constraint '{}')",
                                    idx,
                                    label,
                                    constraint.name
                                ));
                            }

                            // Check for duplicates within batch
                            let batch_constraint_keys =
                                batch_keys.entry(constraint.name.clone()).or_default();
                            if let Some(first_idx) = batch_constraint_keys.get(&key) {
                                return Err(anyhow!(
                                    "Constraint violation: Duplicate key '{}' in batch at indices {} and {}",
                                    key,
                                    first_idx,
                                    idx
                                ));
                            }
                            batch_constraint_keys.insert(key, idx);
                        }
                    }
                    ConstraintType::Exists { property } => {
                        if properties.get(property).is_none_or(|v| v.is_null()) {
                            return Err(anyhow!(
                                "Constraint violation at index {}: Property '{}' must exist",
                                idx,
                                property
                            ));
                        }
                    }
                    ConstraintType::Check { expression } => {
                        if !self.evaluate_check_constraint(expression, properties)? {
                            return Err(anyhow!(
                                "Constraint violation at index {}: CHECK constraint '{}' violated",
                                idx,
                                constraint.name
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }

        // 4. Check storage for unique constraints (can batch this into a single query)
        for constraint in &schema.constraints {
            if !constraint.enabled {
                continue;
            }
            if let ConstraintTarget::Label(l) = &constraint.target {
                if l != label {
                    continue;
                }
            } else {
                continue;
            }

            if let ConstraintType::Unique {
                properties: unique_props,
            } = &constraint.constraint_type
            {
                // Build compound OR filter for all batch vertices
                let mut or_filters = Vec::new();
                for properties in properties_batch.iter() {
                    let mut and_parts = Vec::new();
                    let mut all_present = true;
                    for prop in unique_props {
                        if let Some(val) = properties.get(prop) {
                            let val_str = match val {
                                Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                Value::Int(n) => n.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::Bool(b) => b.to_string(),
                                _ => {
                                    all_present = false;
                                    break;
                                }
                            };
                            and_parts.push(format!("{} = {}", prop, val_str));
                        } else {
                            all_present = false;
                            break;
                        }
                    }
                    if all_present {
                        or_filters.push(format!("({})", and_parts.join(" AND ")));
                    }
                }

                if !or_filters.is_empty() {
                    let vid_list: Vec<String> =
                        vids.iter().map(|v| v.as_u64().to_string()).collect();
                    let filter = format!(
                        "({}) AND _deleted = false AND _vid NOT IN ({})",
                        or_filters.join(" OR "),
                        vid_list.join(", ")
                    );

                    if let Ok(ds) = self.storage.vertex_dataset(label)
                        && let Ok(lance_ds) = ds.open_raw().await
                    {
                        let count = lance_ds.count_rows(Some(filter.clone())).await?;
                        if count > 0 {
                            return Err(anyhow!(
                                "Constraint violation: Duplicate composite key for label '{}' in storage (constraint '{}')",
                                label,
                                constraint.name
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Checks that ext_id is globally unique across all vertices.
    ///
    /// Searches L0 buffers (current, transaction, pending) and the main vertices table
    /// to ensure no other vertex uses this ext_id.
    ///
    /// # Errors
    ///
    /// Returns error if another vertex with the same ext_id exists.
    async fn check_extid_globally_unique(&self, ext_id: &str, current_vid: Vid) -> Result<()> {
        // Check L0 buffers: current, transaction, and pending flush
        let l0_buffers_to_check: Vec<Arc<RwLock<L0Buffer>>> = {
            let mut buffers = vec![self.l0_manager.get_current()];
            if let Some(tx_l0) = &self.transaction_l0 {
                buffers.push(tx_l0.clone());
            }
            buffers.extend(self.l0_manager.get_pending_flush());
            buffers
        };

        for l0 in &l0_buffers_to_check {
            if let Some(vid) =
                Self::find_extid_in_properties(&l0.read().vertex_properties, ext_id, current_vid)
            {
                return Err(anyhow!(
                    "Constraint violation: ext_id '{}' already exists (vertex {:?})",
                    ext_id,
                    vid
                ));
            }
        }

        // Check main vertices table (if it exists)
        let lancedb = self.storage.lancedb_store();
        if let Ok(Some(found_vid)) = MainVertexDataset::find_by_ext_id(lancedb, ext_id).await
            && found_vid != current_vid
        {
            return Err(anyhow!(
                "Constraint violation: ext_id '{}' already exists (vertex {:?})",
                ext_id,
                found_vid
            ));
        }

        Ok(())
    }

    /// Search vertex properties for a duplicate ext_id, excluding `current_vid`.
    fn find_extid_in_properties(
        vertex_properties: &HashMap<Vid, Properties>,
        ext_id: &str,
        current_vid: Vid,
    ) -> Option<Vid> {
        vertex_properties.iter().find_map(|(&vid, props)| {
            if vid != current_vid && props.get("ext_id").and_then(|v| v.as_str()) == Some(ext_id) {
                Some(vid)
            } else {
                None
            }
        })
    }

    /// Helper to get vertex labels from L0 buffer.
    fn get_vertex_labels_from_l0(&self, vid: Vid) -> Option<Vec<String>> {
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        // Check if vertex is tombstoned (deleted) - if so, return None
        if l0_guard.vertex_tombstones.contains(&vid) {
            return None;
        }
        l0_guard.get_vertex_labels(vid).map(|l| l.to_vec())
    }

    /// Get vertex labels from all sources: current L0, pending L0s, and storage.
    /// This is the proper way to read vertex labels after a flush, as it checks both
    /// in-memory buffers and persisted storage.
    pub async fn get_vertex_labels(&self, vid: Vid) -> Option<Vec<String>> {
        // 1. Check current L0
        if let Some(labels) = self.get_vertex_labels_from_l0(vid) {
            return Some(labels);
        }

        // 2. Check transaction L0 if present
        if let Some(tx_l0) = &self.transaction_l0 {
            let guard = tx_l0.read();
            if guard.vertex_tombstones.contains(&vid) {
                return None;
            }
            if let Some(labels) = guard.get_vertex_labels(vid) {
                return Some(labels.to_vec());
            }
        }

        // 3. Check pending flush L0s
        for pending_l0 in self.l0_manager.get_pending_flush() {
            let guard = pending_l0.read();
            if guard.vertex_tombstones.contains(&vid) {
                return None;
            }
            if let Some(labels) = guard.get_vertex_labels(vid) {
                return Some(labels.to_vec());
            }
        }

        // 4. Check storage
        self.find_vertex_labels_in_storage(vid).await.ok().flatten()
    }

    /// Helper to get edge type from L0 buffer.
    fn get_edge_type_from_l0(&self, eid: Eid) -> Option<String> {
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        l0_guard.get_edge_type(eid).map(|s| s.to_string())
    }

    /// Look up the edge type ID (u32) for an EID from the L0 buffer's edge endpoints.
    /// Falls back to the transaction L0 if available.
    pub fn get_edge_type_id_from_l0(&self, eid: Eid) -> Option<u32> {
        // Check transaction L0 first
        if let Some(tx_l0) = &self.transaction_l0 {
            let guard = tx_l0.read();
            if let Some((_, _, etype)) = guard.get_edge_endpoint_full(eid) {
                return Some(etype);
            }
        }
        // Fall back to main L0
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        l0_guard.get_edge_endpoint_full(eid).map(|(_, _, etype)| etype)
    }

    /// Set the type name for an edge (used for schemaless edge types).
    /// This is called during CREATE for edge types not found in the schema.
    pub fn set_edge_type(&self, eid: Eid, type_name: String) {
        self.active_l0().write().set_edge_type(eid, type_name);
    }

    /// Evaluate a simple CHECK constraint expression.
    /// Supports: "property op value" (e.g., "age > 18", "status = 'active'")
    fn evaluate_check_constraint(&self, expression: &str, properties: &Properties) -> Result<bool> {
        let parts: Vec<&str> = expression.split_whitespace().collect();
        if parts.len() != 3 {
            // For now, only support "prop op val"
            // Fallback to true if too complex to avoid breaking, but warn
            log::warn!(
                "Complex CHECK constraint expression '{}' not fully supported yet; allowing write.",
                expression
            );
            return Ok(true);
        }

        let prop_part = parts[0].trim_start_matches('(');
        // Handle "variable.property" format - take the part after the dot
        let prop_name = if let Some(idx) = prop_part.find('.') {
            &prop_part[idx + 1..]
        } else {
            prop_part
        };

        let op = parts[1];
        let val_str = parts[2].trim_end_matches(')');

        let prop_val = match properties.get(prop_name) {
            Some(v) => v,
            None => return Ok(true), // If property missing, CHECK usually passes (unless NOT NULL)
        };

        // Parse value string (handle quotes for strings)
        let target_val = if (val_str.starts_with('\'') && val_str.ends_with('\''))
            || (val_str.starts_with('"') && val_str.ends_with('"'))
        {
            Value::String(val_str[1..val_str.len() - 1].to_string())
        } else if let Ok(n) = val_str.parse::<i64>() {
            Value::Int(n)
        } else if let Ok(n) = val_str.parse::<f64>() {
            Value::Float(n)
        } else if let Ok(b) = val_str.parse::<bool>() {
            Value::Bool(b)
        } else {
            // Check for internal format wrappers if they somehow leaked through
            if val_str.starts_with("Number(") && val_str.ends_with(')') {
                let n_str = &val_str[7..val_str.len() - 1];
                if let Ok(n) = n_str.parse::<i64>() {
                    Value::Int(n)
                } else if let Ok(n) = n_str.parse::<f64>() {
                    Value::Float(n)
                } else {
                    Value::String(val_str.to_string())
                }
            } else {
                Value::String(val_str.to_string())
            }
        };

        match op {
            "=" | "==" => Ok(prop_val == &target_val),
            "!=" | "<>" => Ok(prop_val != &target_val),
            ">" => self
                .compare_values(prop_val, &target_val)
                .map(|o| o.is_gt()),
            "<" => self
                .compare_values(prop_val, &target_val)
                .map(|o| o.is_lt()),
            ">=" => self
                .compare_values(prop_val, &target_val)
                .map(|o| o.is_ge()),
            "<=" => self
                .compare_values(prop_val, &target_val)
                .map(|o| o.is_le()),
            _ => {
                log::warn!("Unsupported operator '{}' in CHECK constraint", op);
                Ok(true)
            }
        }
    }

    fn compare_values(&self, a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
        use std::cmp::Ordering;

        fn cmp_f64(x: f64, y: f64) -> Ordering {
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }

        match (a, b) {
            (Value::Int(n1), Value::Int(n2)) => Ok(n1.cmp(n2)),
            (Value::Float(f1), Value::Float(f2)) => Ok(cmp_f64(*f1, *f2)),
            (Value::Int(n), Value::Float(f)) => Ok(cmp_f64(*n as f64, *f)),
            (Value::Float(f), Value::Int(n)) => Ok(cmp_f64(*f, *n as f64)),
            (Value::String(s1), Value::String(s2)) => Ok(s1.cmp(s2)),
            _ => Err(anyhow!(
                "Cannot compare incompatible types: {:?} vs {:?}",
                a,
                b
            )),
        }
    }

    /// Check whether any vertex in the given property map matches all key-value pairs,
    /// excluding `current_vid`.
    fn l0_has_duplicate_key(
        vertex_properties: &HashMap<Vid, Properties>,
        key_values: &[(String, Value)],
        current_vid: Vid,
    ) -> bool {
        vertex_properties.iter().any(|(&vid, props)| {
            vid != current_vid
                && key_values
                    .iter()
                    .all(|(prop, val)| props.get(prop).is_some_and(|v| v == val))
        })
    }

    async fn check_unique_constraint_multi(
        &self,
        label: &str,
        key_values: &[(String, Value)],
        current_vid: Vid,
    ) -> Result<()> {
        // 1. Check L0 (in-memory)
        {
            let l0 = self.l0_manager.get_current();
            let l0_guard = l0.read();
            if Self::l0_has_duplicate_key(&l0_guard.vertex_properties, key_values, current_vid) {
                return Err(anyhow!(
                    "Constraint violation: Duplicate composite key for label '{}'",
                    label
                ));
            }
        }

        // Check Transaction L0
        if let Some(tx_l0) = &self.transaction_l0 {
            let tx_l0_guard = tx_l0.read();
            if Self::l0_has_duplicate_key(&tx_l0_guard.vertex_properties, key_values, current_vid) {
                return Err(anyhow!(
                    "Constraint violation: Duplicate composite key for label '{}' (in tx)",
                    label
                ));
            }
        }

        // 2. Check Storage (L1/L2)
        let filters: Vec<String> = key_values
            .iter()
            .map(|(prop, val)| {
                let val_str = match val {
                    Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    Value::Int(n) => n.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => "NULL".to_string(),
                };
                format!("{} = {}", prop, val_str)
            })
            .collect();

        let mut filter = filters.join(" AND ");
        filter.push_str(&format!(
            " AND _deleted = false AND _vid != {}",
            current_vid.as_u64()
        ));

        if let Ok(ds) = self.storage.vertex_dataset(label)
            && let Ok(lance_ds) = ds.open_raw().await
        {
            let count = lance_ds.count_rows(Some(filter.clone())).await?;
            if count > 0 {
                return Err(anyhow!(
                    "Constraint violation: Duplicate composite key for label '{}' (in storage). Filter: {}",
                    label,
                    filter
                ));
            }
        }

        Ok(())
    }

    async fn check_write_pressure(&self) -> Result<()> {
        let status = self.storage.compaction_status();
        let l1_runs = status.l1_runs;
        let throttle = &self.config.throttle;

        if l1_runs >= throttle.hard_limit {
            log::warn!("Write stalled: L1 runs ({}) at hard limit", l1_runs);
            // Simple polling for now
            while self.storage.compaction_status().l1_runs >= throttle.hard_limit {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        } else if l1_runs >= throttle.soft_limit {
            let excess = l1_runs - throttle.soft_limit;
            // Cap multiplier to avoid overflow
            let excess = std::cmp::min(excess, 31);
            let multiplier = 2_u32.pow(excess as u32);
            let delay = throttle.base_delay * multiplier;
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    async fn get_query_context(&self) -> Option<QueryContext> {
        Some(QueryContext::new_with_pending(
            self.l0_manager.get_current(),
            self.transaction_l0.clone(),
            self.l0_manager.get_pending_flush(),
        ))
    }

    /// Prepare a vertex for upsert by merging CRDT properties with existing values.
    ///
    /// When `label` is provided, uses it directly to look up property metadata.
    /// Otherwise falls back to discovering the label from L0 buffers and storage.
    ///
    /// # Errors
    ///
    /// Returns an error if CRDT property merging fails.
    async fn prepare_vertex_upsert(
        &self,
        vid: Vid,
        properties: &mut Properties,
        label: Option<&str>,
    ) -> Result<()> {
        let Some(pm) = &self.property_manager else {
            return Ok(());
        };

        let schema = self.schema_manager.schema();

        // Resolve label: use provided label or discover from L0/storage
        let discovered_labels;
        let label_name = if let Some(l) = label {
            Some(l)
        } else {
            discovered_labels = self.get_vertex_labels(vid).await;
            discovered_labels
                .as_ref()
                .and_then(|l| l.first().map(|s| s.as_str()))
        };

        let Some(label_str) = label_name else {
            return Ok(());
        };
        let Some(props_meta) = schema.properties.get(label_str) else {
            return Ok(());
        };

        // Identify CRDT properties in the insert data
        let crdt_keys: Vec<String> = properties
            .keys()
            .filter(|key| {
                props_meta.get(*key).is_some_and(|meta| {
                    matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                })
            })
            .cloned()
            .collect();

        if crdt_keys.is_empty() {
            return Ok(());
        }

        let ctx = self.get_query_context().await;
        for key in crdt_keys {
            let existing = pm.get_vertex_prop_with_ctx(vid, &key, ctx.as_ref()).await?;
            if !existing.is_null()
                && let Some(val) = properties.get_mut(&key)
            {
                *val = pm.merge_crdt_values(&existing, val)?;
            }
        }

        Ok(())
    }

    async fn prepare_edge_upsert(&self, eid: Eid, properties: &mut Properties) -> Result<()> {
        if let Some(pm) = &self.property_manager {
            let schema = self.schema_manager.schema();
            // Get edge type from L0 buffer instead of from EID
            let type_name = self.get_edge_type_from_l0(eid);

            if let Some(ref t_name) = type_name
                && let Some(props_meta) = schema.properties.get(t_name)
            {
                let mut crdt_keys = Vec::new();
                for (key, _) in properties.iter() {
                    if let Some(meta) = props_meta.get(key)
                        && matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                    {
                        crdt_keys.push(key.clone());
                    }
                }

                if !crdt_keys.is_empty() {
                    let ctx = self.get_query_context().await;
                    for key in crdt_keys {
                        let existing = pm.get_edge_prop(eid, &key, ctx.as_ref()).await?;

                        if !existing.is_null()
                            && let Some(val) = properties.get_mut(&key)
                        {
                            *val = pm.merge_crdt_values(&existing, val)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self, properties), level = "trace")]
    pub async fn insert_vertex(&mut self, vid: Vid, properties: Properties) -> Result<()> {
        self.insert_vertex_with_labels(vid, properties, vec![])
            .await?;
        Ok(())
    }

    #[instrument(skip(self, properties, labels), level = "trace")]
    pub async fn insert_vertex_with_labels(
        &mut self,
        vid: Vid,
        mut properties: Properties,
        labels: Vec<String>,
    ) -> Result<Properties> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        self.process_embeddings_for_labels(&labels, &mut properties)
            .await?;
        self.validate_vertex_constraints(vid, &properties, &labels)
            .await?;
        self.prepare_vertex_upsert(vid, &mut properties, labels.first().map(|s| s.as_str()))
            .await?;

        // Clone properties before moving into L0 to return them (includes auto-generated embeddings)
        let properties_copy = properties.clone();
        self.active_l0()
            .write()
            .insert_vertex_with_labels(vid, properties, labels);
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if self.transaction_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow insert_vertex: {}ms", start.elapsed().as_millis());
        }
        Ok(properties_copy)
    }

    /// Insert multiple vertices with batched operations.
    ///
    /// This method uses batched operations to achieve O(N) complexity instead of O(N²)
    /// for bulk inserts with unique constraints.
    ///
    /// # Performance Improvements
    /// - Batch VID allocation: 1 call instead of N calls
    /// - Batch constraint validation: O(N) instead of O(N²)
    /// - Batch embedding generation: 1 API call per config instead of N calls
    /// - Transaction wrapping: Automatic flush deferral, atomicity
    ///
    /// # Arguments
    /// * `vids` - Pre-allocated VIDs for the vertices
    /// * `properties_batch` - Properties for each vertex
    /// * `labels` - Labels for all vertices (assumes single label for simplicity)
    ///
    /// # Errors
    /// Returns error if:
    /// - VID/properties length mismatch
    /// - Constraint violation detected
    /// - Embedding generation fails
    /// - Transaction commit fails
    ///
    /// # Atomicity
    /// If this method fails, all changes are rolled back (if transaction was started here).
    pub async fn insert_vertices_batch(
        &mut self,
        vids: Vec<Vid>,
        mut properties_batch: Vec<Properties>,
        labels: Vec<String>,
    ) -> Result<Vec<Properties>> {
        let start = std::time::Instant::now();

        // Validate inputs
        if vids.len() != properties_batch.len() {
            return Err(anyhow!(
                "VID/properties size mismatch: {} vids, {} properties",
                vids.len(),
                properties_batch.len()
            ));
        }

        if vids.is_empty() {
            return Ok(Vec::new());
        }

        // Start transaction if not already in one
        let is_nested = self.transaction_l0.is_some();
        if !is_nested {
            self.begin_transaction()?;
        }

        // Batch operations
        let result = async {
            self.check_write_pressure().await?;

            // Batch embedding generation (1 API call per config)
            self.process_embeddings_for_batch(&labels, &mut properties_batch)
                .await?;

            // Batch constraint validation (O(N) instead of O(N²))
            let label = labels
                .first()
                .ok_or_else(|| anyhow!("No labels provided"))?;
            self.validate_vertex_batch_constraints(&vids, &properties_batch, label)
                .await?;

            // Batch prepare (CRDT merging if needed)
            // Check schema once: skip entirely if no CRDT properties for this label.
            // For new vertices (freshly allocated VIDs), there are no existing CRDT
            // values to merge, so the per-vertex lookup is unnecessary in that case.
            let has_crdt_fields = {
                let schema = self.schema_manager.schema();
                schema
                    .properties
                    .get(label.as_str())
                    .is_some_and(|props_meta| {
                        props_meta.values().any(|meta| {
                            matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                        })
                    })
            };

            if has_crdt_fields {
                // Batch fetch existing CRDT values: collect VIDs that need merging,
                // then query once via PropertyManager instead of per-vertex lookups.
                let schema = self.schema_manager.schema();
                let crdt_keys: Vec<String> = schema
                    .properties
                    .get(label.as_str())
                    .map(|props_meta| {
                        props_meta
                            .iter()
                            .filter(|(_, meta)| {
                                matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                            })
                            .map(|(key, _)| key.clone())
                            .collect()
                    })
                    .unwrap_or_default();

                if let Some(pm) = &self.property_manager {
                    let ctx = self.get_query_context().await;
                    for (vid, props) in vids.iter().zip(&mut properties_batch) {
                        for key in &crdt_keys {
                            if props.contains_key(key) {
                                let existing =
                                    pm.get_vertex_prop_with_ctx(*vid, key, ctx.as_ref()).await?;
                                if !existing.is_null()
                                    && let Some(val) = props.get_mut(key)
                                {
                                    *val = pm.merge_crdt_values(&existing, val)?;
                                }
                            }
                        }
                    }
                }
            }

            // Batch L0 writes (WAL batched automatically via transaction)
            let tx_l0 = self
                .transaction_l0
                .as_ref()
                .ok_or_else(|| anyhow!("Transaction L0 missing"))?;

            let properties_result = properties_batch.clone();
            {
                let mut l0_guard = tx_l0.write();
                for (vid, props) in vids.iter().zip(properties_batch) {
                    l0_guard.insert_vertex_with_labels(*vid, props, labels.clone());
                }
            }

            // Update metrics (batch increment)
            metrics::counter!("uni_l0_buffer_mutations_total").increment(vids.len() as u64);
            self.update_metrics();

            Ok::<Vec<Properties>, anyhow::Error>(properties_result)
        }
        .await;

        // Handle transaction commit/rollback
        match result {
            Ok(props) => {
                // Commit if we started the transaction
                if !is_nested {
                    self.commit_transaction().await?;
                }

                if start.elapsed().as_millis() > 100 {
                    log::warn!(
                        "Slow insert_vertices_batch ({} vertices): {}ms",
                        vids.len(),
                        start.elapsed().as_millis()
                    );
                }

                Ok(props)
            }
            Err(e) => {
                // Rollback if we started the transaction
                if !is_nested {
                    self.rollback_transaction()?;
                }
                Err(e)
            }
        }
    }

    /// Delete a vertex by VID.
    ///
    /// When `labels` is provided, uses them directly to populate L0 for
    /// correct tombstone flushing. Otherwise discovers labels from L0
    /// buffers and storage (which can be slow for many vertices).
    ///
    /// # Errors
    ///
    /// Returns an error if write pressure stalls, label lookup fails, or
    /// the L0 delete operation fails.
    #[instrument(skip(self, labels), level = "trace")]
    pub async fn delete_vertex(&mut self, vid: Vid, labels: Option<Vec<String>>) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        let l0 = self.active_l0();

        // Before deleting, ensure we have the vertex's labels stored in L0
        // so the tombstone can be properly flushed to the correct label datasets.
        let has_labels = {
            let l0_guard = l0.read();
            l0_guard.vertex_labels.contains_key(&vid)
        };

        if !has_labels {
            let resolved_labels = if let Some(provided) = labels {
                // Caller provided labels — skip the lookup entirely
                Some(provided)
            } else {
                // Discover labels from pending flush L0s, then storage
                let mut found = None;
                for pending_l0 in self.l0_manager.get_pending_flush() {
                    let pending_guard = pending_l0.read();
                    if let Some(l) = pending_guard.get_vertex_labels(vid) {
                        found = Some(l.to_vec());
                        break;
                    }
                }
                if found.is_none() {
                    found = self.find_vertex_labels_in_storage(vid).await?;
                }
                found
            };

            if let Some(found_labels) = resolved_labels {
                let mut l0_guard = l0.write();
                l0_guard.vertex_labels.insert(vid, found_labels);
            }
        }

        l0.write().delete_vertex(vid)?;
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if self.transaction_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow delete_vertex: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    /// Find vertex labels from storage by querying the main vertices table.
    /// Returns the labels from the latest non-deleted version of the vertex.
    async fn find_vertex_labels_in_storage(&self, vid: Vid) -> Result<Option<Vec<String>>> {
        use arrow_array::Array;
        use arrow_array::cast::AsArray;
        use lancedb::query::{ExecutableQuery, QueryBase, Select};

        let lancedb_store = self.storage.lancedb_store();
        let table_name = MainVertexDataset::table_name();

        // Check if table exists first; if not, vertex hasn't been flushed to storage yet
        if !lancedb_store.table_exists(table_name).await? {
            return Ok(None);
        }

        let table = lancedb_store.open_table(table_name).await?;

        // Query for this specific vid (don't filter by _deleted yet - we need to find the latest version first)
        let filter = format!("_vid = {}", vid.as_u64());
        let query = table.query().only_if(filter).select(Select::Columns(vec![
            "_vid".to_string(),
            "labels".to_string(),
            "_version".to_string(),
            "_deleted".to_string(),
        ]));

        let stream = query.execute().await?;
        let batches: Vec<arrow_array::RecordBatch> = stream.try_collect().await.unwrap_or_default();

        // Find the row with the highest version number
        let mut max_version: Option<u64> = None;
        let mut labels: Option<Vec<String>> = None;
        let mut is_deleted = false;

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let version_array = batch
                .column_by_name("_version")
                .unwrap()
                .as_primitive::<arrow_array::types::UInt64Type>();

            let deleted_array = batch.column_by_name("_deleted").unwrap().as_boolean();

            let labels_array = batch.column_by_name("labels").unwrap().as_list::<i32>();

            for row_idx in 0..batch.num_rows() {
                let version = version_array.value(row_idx);

                if max_version.is_none_or(|mv| version > mv) {
                    is_deleted = deleted_array.value(row_idx);

                    let labels_list = labels_array.value(row_idx);
                    let string_array = labels_list.as_string::<i32>();
                    let vertex_labels: Vec<String> = (0..string_array.len())
                        .filter(|&i| !string_array.is_null(i))
                        .map(|i| string_array.value(i).to_string())
                        .collect();

                    max_version = Some(version);
                    labels = Some(vertex_labels);
                }
            }
        }

        // If the latest version is deleted, return None
        if is_deleted { Ok(None) } else { Ok(labels) }
    }

    #[instrument(skip(self, properties), level = "trace")]
    pub async fn insert_edge(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        mut properties: Properties,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        self.prepare_edge_upsert(eid, &mut properties).await?;

        let l0 = self.active_l0();
        l0.write()
            .insert_edge(src_vid, dst_vid, edge_type, eid, properties)?;

        // Dual-write to AdjacencyManager overlay (survives flush).
        // Skip for transaction-local L0 -- transaction edges are overlaid separately.
        if self.transaction_l0.is_none() {
            let version = l0.read().current_version;
            self.adjacency_manager
                .insert_edge(src_vid, dst_vid, eid, edge_type, version);
        }

        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if self.transaction_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow insert_edge: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    pub async fn delete_edge(
        &mut self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        let l0 = self.active_l0();

        l0.write().delete_edge(eid, src_vid, dst_vid, edge_type)?;

        // Dual-write tombstone to AdjacencyManager overlay.
        if self.transaction_l0.is_none() {
            let version = l0.read().current_version;
            self.adjacency_manager
                .add_tombstone(eid, src_vid, dst_vid, edge_type, version);
        }
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if self.transaction_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow delete_edge: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    /// Check if flush should be triggered based on mutation count or time elapsed.
    /// This method is called after each write operation and can also be called
    /// by a background task for time-based flushing.
    pub async fn check_flush(&mut self) -> Result<()> {
        let count = self.l0_manager.get_current().read().mutation_count;

        // Skip if no mutations
        if count == 0 {
            return Ok(());
        }

        // Flush on mutation count threshold (10,000 default)
        if count >= self.config.auto_flush_threshold {
            self.flush_to_l1(None).await?;
            return Ok(());
        }

        // Flush on time interval IF minimum mutations met
        if let Some(interval) = self.config.auto_flush_interval
            && self.last_flush_time.elapsed() >= interval
            && count >= self.config.auto_flush_min_mutations
        {
            self.flush_to_l1(None).await?;
        }

        Ok(())
    }

    /// Process embeddings for a vertex using labels passed directly.
    /// Use this when labels haven't been stored to L0 yet.
    async fn process_embeddings_for_labels(
        &self,
        labels: &[String],
        properties: &mut Properties,
    ) -> Result<()> {
        let label_name = labels.first().map(|s| s.as_str());
        self.process_embeddings_impl(label_name, properties).await
    }

    /// Process embeddings for a batch of vertices efficiently.
    ///
    /// Groups vertices by embedding config and makes batched API calls to the
    /// embedding service instead of calling once per vertex.
    ///
    /// # Performance
    /// For N vertices with embedding config:
    /// - Old approach: N API calls to embedding service
    /// - New approach: 1 API call per embedding config (usually 1 total)
    async fn process_embeddings_for_batch(
        &self,
        labels: &[String],
        properties_batch: &mut [Properties],
    ) -> Result<()> {
        let label_name = labels.first().map(|s| s.as_str());
        let schema = self.schema_manager.schema();

        if let Some(label) = label_name {
            // Find vector indexes with embedding config for this label
            let mut configs = Vec::new();
            for idx in &schema.indexes {
                if let IndexDefinition::Vector(v_config) = idx
                    && v_config.label == label
                    && let Some(emb_config) = &v_config.embedding_config
                {
                    configs.push((v_config.property.clone(), emb_config.clone()));
                }
            }

            if configs.is_empty() {
                return Ok(());
            }

            for (target_prop, emb_config) in configs {
                // Collect input texts from all vertices that need embeddings
                let mut input_texts: Vec<String> = Vec::new();
                let mut needs_embedding: Vec<usize> = Vec::new();

                for (idx, properties) in properties_batch.iter().enumerate() {
                    // Skip if target property already exists
                    if properties.contains_key(&target_prop) {
                        continue;
                    }

                    // Check if source properties exist
                    let mut inputs = Vec::new();
                    for src_prop in &emb_config.source_properties {
                        if let Some(val) = properties.get(src_prop)
                            && let Some(s) = val.as_str()
                        {
                            inputs.push(s.to_string());
                        }
                    }

                    if !inputs.is_empty() {
                        let input_text = inputs.join(" ");
                        input_texts.push(input_text);
                        needs_embedding.push(idx);
                    }
                }

                if input_texts.is_empty() {
                    continue;
                }

                // Get embedding service
                let service = self.get_embedding_service(&emb_config.model).await?;

                // Batch generate embeddings (single API call)
                let input_refs: Vec<&str> = input_texts.iter().map(|s| s.as_str()).collect();
                let embeddings = service.embed(&input_refs).await?;

                // Distribute results back to properties
                for (embedding_idx, &prop_idx) in needs_embedding.iter().enumerate() {
                    if let Some(vec) = embeddings.get(embedding_idx) {
                        let vals: Vec<Value> =
                            vec.iter().map(|f| Value::Float(*f as f64)).collect();
                        properties_batch[prop_idx].insert(target_prop.clone(), Value::List(vals));
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_embeddings_impl(
        &self,
        label_name: Option<&str>,
        properties: &mut Properties,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();

        if let Some(label) = label_name {
            // Find vector indexes with embedding config for this label
            let mut configs = Vec::new();
            for idx in &schema.indexes {
                if let IndexDefinition::Vector(v_config) = idx
                    && v_config.label == label
                    && let Some(emb_config) = &v_config.embedding_config
                {
                    configs.push((v_config.property.clone(), emb_config.clone()));
                }
            }

            if configs.is_empty() {
                log::info!("No embedding config found for label {}", label);
            }

            for (target_prop, emb_config) in configs {
                // If target property already exists, skip (assume user provided it)
                if properties.contains_key(&target_prop) {
                    continue;
                }

                // Check if source properties exist
                let mut inputs = Vec::new();
                for src_prop in &emb_config.source_properties {
                    if let Some(val) = properties.get(src_prop)
                        && let Some(s) = val.as_str()
                    {
                        inputs.push(s.to_string());
                    }
                }

                if inputs.is_empty() {
                    continue;
                }

                let input_text = inputs.join(" "); // Simple concatenation

                // Get or create service
                let service = self.get_embedding_service(&emb_config.model).await?;

                // Generate
                let embeddings = service.embed(&[&input_text]).await?;
                if let Some(vec) = embeddings.first() {
                    // Store as array of floats
                    let vals: Vec<Value> = vec.iter().map(|f| Value::Float(*f as f64)).collect();
                    properties.insert(target_prop.clone(), Value::List(vals));
                }
            }
        }
        Ok(())
    }

    /// Gets or creates a cached embedding service for the given model.
    async fn get_embedding_service(
        &self,
        model: &EmbeddingModel,
    ) -> Result<Arc<dyn EmbeddingService>> {
        let key = embedding_service_key(model)?;

        // Check cache first
        {
            let services = acquire_mutex(&self.embedding_services, "embedding_services")?;
            if let Some(service) = services.get(&key) {
                return Ok(service.clone());
            }
        }

        // Create and cache new service
        let service = create_embedding_service(model)?;
        let mut services = acquire_mutex(&self.embedding_services, "embedding_services")?;
        services.insert(key, service.clone());
        Ok(service)
    }

    /// Flushes the current in-memory L0 buffer to L1 storage.
    ///
    /// # Lock Ordering
    ///
    /// To prevent deadlocks, locks must be acquired in the following order:
    /// 1. `Writer` lock (held by caller)
    /// 2. `L0Manager` lock (via `begin_flush` / `get_current`)
    /// 3. `L0Buffer` lock (individual buffer RWLocks)
    /// 4. `Index` / `Storage` locks (during actual flush)
    #[instrument(
        skip(self),
        fields(snapshot_id, mutations_count, size_bytes),
        level = "info"
    )]
    pub async fn flush_to_l1(&mut self, name: Option<String>) -> Result<String> {
        let start = std::time::Instant::now();
        let schema = self.schema_manager.schema();

        let (initial_size, initial_count) = {
            let l0_arc = self.l0_manager.get_current();
            let l0 = l0_arc.read();
            (l0.size_bytes(), l0.mutation_count)
        };
        tracing::Span::current().record("size_bytes", initial_size);
        tracing::Span::current().record("mutations_count", initial_count);

        debug!("Starting L0 flush to L1");

        // 1. Flush WAL BEFORE rotating L0
        // This ensures that if WAL flush fails, the current L0 is still active
        // and mutations are retained in memory until restart/retry.
        // Capture the LSN of the flushed segment for the snapshot's wal_high_water_mark.
        let wal_for_truncate = {
            let current_l0 = self.l0_manager.get_current();
            let l0_guard = current_l0.read();
            l0_guard.wal.clone()
        };

        let wal_lsn = if let Some(ref w) = wal_for_truncate {
            w.flush().await?
        } else {
            0
        };

        // 2. Begin flush: rotate L0 and keep old L0 visible to reads
        // The old L0 stays in pending_flush list until complete_flush is called,
        // ensuring data remains visible even if L1 writes fail.
        let old_l0_arc = self.l0_manager.begin_flush(0, None);
        metrics::counter!("uni_l0_buffer_rotations_total").increment(1);

        let current_version;
        {
            // Acquire Write lock to take WAL and version
            let mut old_l0_guard = old_l0_arc.write();
            current_version = old_l0_guard.current_version;

            // Record the WAL LSN for this L0 so we don't truncate past it
            // if this flush fails and a subsequent flush succeeds.
            old_l0_guard.wal_lsn_at_flush = wal_lsn;

            let wal = old_l0_guard.wal.take();

            // Give WAL to new L0
            let new_l0_arc = self.l0_manager.get_current();
            let mut new_l0_guard = new_l0_arc.write();
            new_l0_guard.wal = wal;
            new_l0_guard.current_version = current_version;
        } // Drop locks

        // 2. Acquire Read lock on Old L0 for flushing
        let mut entries_by_type: HashMap<u32, Vec<L1Entry>> = HashMap::new();
        // (Vid, labels, properties, deleted, version)
        type VertexEntry = (Vid, Vec<String>, Properties, bool, u64);
        let mut vertices_by_label: HashMap<u16, Vec<VertexEntry>> = HashMap::new();
        // Collect vertex timestamps from L0 for flushing to storage
        let mut vertex_created_at: HashMap<Vid, i64> = HashMap::new();
        let mut vertex_updated_at: HashMap<Vid, i64> = HashMap::new();

        {
            let old_l0 = old_l0_arc.read();

            // 1. Collect all edges and tombstones from L0
            for edge in old_l0.graph.edges() {
                let properties = old_l0
                    .edge_properties
                    .get(&edge.eid)
                    .cloned()
                    .unwrap_or_default();
                let version = old_l0.edge_versions.get(&edge.eid).copied().unwrap_or(0);

                // Get timestamps from L0 buffer (populated during insert)
                let created_at = old_l0.edge_created_at.get(&edge.eid).copied();
                let updated_at = old_l0.edge_updated_at.get(&edge.eid).copied();

                entries_by_type
                    .entry(edge.edge_type)
                    .or_default()
                    .push(L1Entry {
                        src_vid: edge.src_vid,
                        dst_vid: edge.dst_vid,
                        eid: edge.eid,
                        op: Op::Insert,
                        version,
                        properties,
                        created_at,
                        updated_at,
                    });
            }

            // From tombstones
            for tombstone in old_l0.tombstones.values() {
                let version = old_l0
                    .edge_versions
                    .get(&tombstone.eid)
                    .copied()
                    .unwrap_or(0);
                // Get timestamps - for deletes, updated_at reflects deletion time
                let created_at = old_l0.edge_created_at.get(&tombstone.eid).copied();
                let updated_at = old_l0.edge_updated_at.get(&tombstone.eid).copied();

                entries_by_type
                    .entry(tombstone.edge_type)
                    .or_default()
                    .push(L1Entry {
                        src_vid: tombstone.src_vid,
                        dst_vid: tombstone.dst_vid,
                        eid: tombstone.eid,
                        op: Op::Delete,
                        version,
                        properties: HashMap::new(),
                        created_at,
                        updated_at,
                    });
            }

            // 2.5 Flush Vertices - Collect by label (using vertex_labels from L0)
            //
            // Helper: fan-out a single vertex entry into per-label buckets.
            // Each per-label table row carries the full label set so multi-label
            // info is preserved after flush.
            let push_vertex_to_labels =
                |vid: Vid,
                 all_labels: &[String],
                 props: Properties,
                 deleted: bool,
                 version: u64,
                 out: &mut HashMap<u16, Vec<VertexEntry>>| {
                    for label in all_labels {
                        if let Some(label_id) = schema.label_id_by_name(label) {
                            out.entry(label_id).or_default().push((
                                vid,
                                all_labels.to_vec(),
                                props.clone(),
                                deleted,
                                version,
                            ));
                        }
                    }
                };

            for (vid, props) in &old_l0.vertex_properties {
                let version = old_l0.vertex_versions.get(vid).copied().unwrap_or(0);
                // Collect timestamps for this vertex
                if let Some(&ts) = old_l0.vertex_created_at.get(vid) {
                    vertex_created_at.insert(*vid, ts);
                }
                if let Some(&ts) = old_l0.vertex_updated_at.get(vid) {
                    vertex_updated_at.insert(*vid, ts);
                }
                if let Some(labels) = old_l0.vertex_labels.get(vid) {
                    push_vertex_to_labels(
                        *vid,
                        labels,
                        props.clone(),
                        false,
                        version,
                        &mut vertices_by_label,
                    );
                }
            }
            for &vid in &old_l0.vertex_tombstones {
                let version = old_l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                if let Some(labels) = old_l0.vertex_labels.get(&vid) {
                    push_vertex_to_labels(
                        vid,
                        labels,
                        HashMap::new(),
                        true,
                        version,
                        &mut vertices_by_label,
                    );
                }
            }
        } // Drop read lock

        // 0. Load previous snapshot or create new
        let mut manifest = self
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?
            .unwrap_or_else(|| {
                SnapshotManifest::new(Uuid::new_v4().to_string(), schema.schema_version)
            });

        // Update snapshot metadata
        // Save parent snapshot ID before generating new one (for lineage tracking)
        let parent_id = manifest.snapshot_id.clone();
        manifest.parent_snapshot = Some(parent_id);
        manifest.snapshot_id = Uuid::new_v4().to_string();
        manifest.name = name;
        manifest.created_at = Utc::now();
        manifest.version_high_water_mark = current_version;
        manifest.wal_high_water_mark = wal_lsn;
        let snapshot_id = manifest.snapshot_id.clone();

        tracing::Span::current().record("snapshot_id", &snapshot_id);

        // 2. For each edge type, write FWD and BWD runs
        let lancedb_store = self.storage.lancedb_store();

        for (&edge_type_id, entries) in entries_by_type.iter() {
            // Get edge type name from unified lookup (handles both schema'd and schemaless)
            let edge_type_name = self
                .storage
                .schema_manager()
                .edge_type_name_by_id_unified(edge_type_id)
                .ok_or_else(|| anyhow!("Edge type ID {} not found", edge_type_id))?;

            // FWD Run (sorted by src_vid)
            let mut fwd_entries = entries.clone();
            fwd_entries.sort_by_key(|e| e.src_vid);
            let fwd_ds = self.storage.delta_dataset(&edge_type_name, "fwd")?;
            let fwd_batch = fwd_ds.build_record_batch(&fwd_entries, &schema)?;

            // Write using LanceDB
            let table = fwd_ds.write_run_lancedb(lancedb_store, fwd_batch).await?;
            fwd_ds.ensure_eid_index_lancedb(&table).await?;

            // BWD Run (sorted by dst_vid)
            let mut bwd_entries = entries.clone();
            bwd_entries.sort_by_key(|e| e.dst_vid);
            let bwd_ds = self.storage.delta_dataset(&edge_type_name, "bwd")?;
            let bwd_batch = bwd_ds.build_record_batch(&bwd_entries, &schema)?;

            let bwd_table = bwd_ds.write_run_lancedb(lancedb_store, bwd_batch).await?;
            bwd_ds.ensure_eid_index_lancedb(&bwd_table).await?;

            // Update Manifest
            let current_snap =
                manifest
                    .edges
                    .entry(edge_type_name.to_string())
                    .or_insert(EdgeSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.version += 1;
            current_snap.count += entries.len() as u64;
            // LanceDB tables don't expose Lance version directly
            current_snap.lance_version = 0;

            // Note: No CSR invalidation needed. AdjacencyManager's overlay
            // already has these edges via dual-write in insert_edge/delete_edge.
        }

        // 2.5 Flush Vertices
        for (label_id, vertices) in vertices_by_label {
            let label_name = schema
                .label_name_by_id(label_id)
                .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

            let ds = self.storage.vertex_dataset(label_name)?;

            // Collect inverted index updates before consuming vertices
            // Maps: cfg.property -> (added, removed)
            type InvertedUpdateMap = HashMap<String, (HashMap<Vid, Vec<String>>, HashSet<Vid>)>;
            let mut inverted_updates: InvertedUpdateMap = HashMap::new();

            for idx in &schema.indexes {
                if let IndexDefinition::Inverted(cfg) = idx
                    && cfg.label == label_name
                {
                    let mut added: HashMap<Vid, Vec<String>> = HashMap::new();
                    let mut removed: HashSet<Vid> = HashSet::new();

                    for (vid, _labels, props, deleted, _version) in &vertices {
                        if *deleted {
                            removed.insert(*vid);
                        } else if let Some(prop_value) = props.get(&cfg.property) {
                            // Extract terms from the property value (List<String>)
                            if let Some(arr) = prop_value.as_array() {
                                let terms: Vec<String> = arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(ToString::to_string))
                                    .collect();
                                if !terms.is_empty() {
                                    added.insert(*vid, terms);
                                }
                            }
                        }
                    }

                    if !added.is_empty() || !removed.is_empty() {
                        inverted_updates.insert(cfg.property.clone(), (added, removed));
                    }
                }
            }

            let mut v_data = Vec::new();
            let mut d_data = Vec::new();
            let mut ver_data = Vec::new();
            for (vid, labels, props, deleted, version) in vertices {
                v_data.push((vid, labels, props));
                d_data.push(deleted);
                ver_data.push(version);
            }

            let batch = ds.build_record_batch_with_timestamps(
                &v_data,
                &d_data,
                &ver_data,
                &schema,
                Some(&vertex_created_at),
                Some(&vertex_updated_at),
            )?;

            // Write using LanceDB
            let table = ds
                .write_batch_lancedb(lancedb_store, batch, &schema)
                .await?;
            ds.ensure_default_indexes_lancedb(&table).await?;

            // Update Manifest
            let current_snap =
                manifest
                    .vertices
                    .entry(label_name.to_string())
                    .or_insert(LabelSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.version += 1;
            current_snap.count += v_data.len() as u64;
            // LanceDB tables don't expose Lance version directly
            current_snap.lance_version = 0;

            // Invalidate table cache to ensure next read picks up new version
            self.storage.invalidate_table_cache(label_name);

            // Apply inverted index updates incrementally
            for idx in &schema.indexes {
                if let IndexDefinition::Inverted(cfg) = idx
                    && cfg.label == label_name
                    && let Some((added, removed)) = inverted_updates.get(&cfg.property)
                {
                    self.storage
                        .index_manager()
                        .update_inverted_index_incremental(cfg, added, removed)
                        .await?;
                }
            }
        }

        // 3. Write to main unified tables (dual-write for fast ID-based lookups)
        // 3.1 Write to main edges table
        // Collect data while holding the lock, then release before async operations
        let (main_edges, edge_created_at_map, edge_updated_at_map) = {
            let _old_l0 = old_l0_arc.read();
            let mut main_edges: Vec<(
                uni_common::core::id::Eid,
                Vid,
                Vid,
                String,
                Properties,
                bool,
                u64,
            )> = Vec::new();
            let mut edge_created_at_map: HashMap<uni_common::core::id::Eid, i64> = HashMap::new();
            let mut edge_updated_at_map: HashMap<uni_common::core::id::Eid, i64> = HashMap::new();

            for (&edge_type_id, entries) in entries_by_type.iter() {
                for entry in entries {
                    // Get edge type name from unified lookup (handles both schema'd and schemaless)
                    let edge_type_name = self
                        .storage
                        .schema_manager()
                        .edge_type_name_by_id_unified(edge_type_id)
                        .unwrap_or_else(|| "unknown".to_string());

                    let deleted = matches!(entry.op, Op::Delete);
                    main_edges.push((
                        entry.eid,
                        entry.src_vid,
                        entry.dst_vid,
                        edge_type_name,
                        entry.properties.clone(),
                        deleted,
                        entry.version,
                    ));

                    if let Some(ts) = entry.created_at {
                        edge_created_at_map.insert(entry.eid, ts);
                    }
                    if let Some(ts) = entry.updated_at {
                        edge_updated_at_map.insert(entry.eid, ts);
                    }
                }
            }

            (main_edges, edge_created_at_map, edge_updated_at_map)
        }; // Lock released here

        if !main_edges.is_empty() {
            let main_edge_batch = MainEdgeDataset::build_record_batch(
                &main_edges,
                Some(&edge_created_at_map),
                Some(&edge_updated_at_map),
            )?;
            let main_edge_table =
                MainEdgeDataset::write_batch_lancedb(lancedb_store, main_edge_batch).await?;
            MainEdgeDataset::ensure_default_indexes_lancedb(&main_edge_table).await?;
        }

        // 3.2 Write to main vertices table
        // Collect data while holding the lock, then release before async operations
        let main_vertices: Vec<(Vid, Vec<String>, Properties, bool, u64)> = {
            let old_l0 = old_l0_arc.read();
            let mut vertices = Vec::new();

            // Collect all vertices from vertex_properties
            for (vid, props) in &old_l0.vertex_properties {
                let version = old_l0.vertex_versions.get(vid).copied().unwrap_or(0);
                let labels = old_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                vertices.push((*vid, labels, props.clone(), false, version));
            }

            // Collect tombstones
            for &vid in &old_l0.vertex_tombstones {
                let version = old_l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                let labels = old_l0.vertex_labels.get(&vid).cloned().unwrap_or_default();
                vertices.push((vid, labels, HashMap::new(), true, version));
            }

            vertices
        }; // Lock released here

        if !main_vertices.is_empty() {
            let main_vertex_batch = MainVertexDataset::build_record_batch(
                &main_vertices,
                Some(&vertex_created_at),
                Some(&vertex_updated_at),
            )?;
            let main_vertex_table =
                MainVertexDataset::write_batch_lancedb(lancedb_store, main_vertex_batch).await?;
            MainVertexDataset::ensure_default_indexes_lancedb(&main_vertex_table).await?;
        }

        // Save Snapshot
        self.storage
            .snapshot_manager()
            .save_snapshot(&manifest)
            .await?;
        self.storage
            .snapshot_manager()
            .set_latest_snapshot(&manifest.snapshot_id)
            .await?;

        // Complete flush: remove old L0 from pending list now that L1 writes succeeded.
        // This must happen BEFORE WAL truncation so min_pending_wal_lsn is accurate.
        self.l0_manager.complete_flush(&old_l0_arc);

        // Truncate WAL segments, but only up to the minimum LSN of any remaining pending L0s.
        // This prevents data loss if earlier flushes failed and left L0s in pending_flush.
        if let Some(w) = wal_for_truncate {
            // Determine safe truncation point: the minimum of our LSN and any pending L0s
            let safe_lsn = self
                .l0_manager
                .min_pending_wal_lsn()
                .map(|min_pending| min_pending.min(wal_lsn))
                .unwrap_or(wal_lsn);
            w.truncate_before(safe_lsn).await?;
        }

        // Invalidate property cache after flush to prevent stale reads.
        // Once L0 data moves to storage, cached values from storage may be outdated.
        if let Some(ref pm) = self.property_manager {
            pm.clear_cache().await;
        }

        // Reset last flush time for time-based auto-flush
        self.last_flush_time = std::time::Instant::now();

        info!(
            snapshot_id,
            mutations_count = initial_count,
            size_bytes = initial_size,
            "L0 flush to L1 completed successfully"
        );
        metrics::histogram!("uni_flush_duration_seconds").record(start.elapsed().as_secs_f64());
        metrics::counter!("uni_flush_bytes_total").increment(initial_size as u64);
        metrics::counter!("uni_flush_rows_total").increment(initial_count as u64);

        // Trigger CSR compaction if enough frozen segments have accumulated.
        // After flush, the old L0 data is now in L1; the overlay segments can be merged
        // into the Main CSR to reduce lookup overhead.
        let am = self.adjacency_manager.clone();
        if am.should_compact(4) {
            tokio::spawn(async move {
                am.compact();
            });
        }

        Ok(snapshot_id)
    }

    /// Set the property manager for cache invalidation.
    pub fn set_property_manager(&mut self, pm: Arc<PropertyManager>) {
        self.property_manager = Some(pm);
    }
}
