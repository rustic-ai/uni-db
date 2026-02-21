// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::WINDOW_FUNCTIONS;
use crate::query::datetime::{classify_temporal, eval_datetime_function, parse_datetime_utc};
use crate::query::expr_eval::{
    eval_binary_op, eval_in_op, eval_scalar_function, eval_vector_similarity,
};
use crate::query::planner::{LogicalPlan, QueryPlanner};
use crate::query::pushdown::LanceFilterGenerator;
use crate::types::Value;
use anyhow::{Result, anyhow};

/// Convert a `Value` to `chrono::DateTime<Utc>`, handling both `Value::Temporal` and `Value::String`.
fn value_to_datetime_utc(val: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
    match val {
        Value::Temporal(tv) => {
            use uni_common::TemporalValue;
            match tv {
                TemporalValue::DateTime {
                    nanos_since_epoch, ..
                }
                | TemporalValue::LocalDateTime {
                    nanos_since_epoch, ..
                } => Some(chrono::DateTime::from_timestamp_nanos(*nanos_since_epoch)),
                TemporalValue::Date { days_since_epoch } => {
                    chrono::DateTime::from_timestamp(*days_since_epoch as i64 * 86400, 0)
                }
                _ => None,
            }
        }
        Value::String(s) => parse_datetime_utc(s).ok(),
        _ => None,
    }
}
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use metrics;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{ConstraintTarget, ConstraintType, DataType, SchemaManager};
use uni_cypher::ast::{
    BinaryOp, ConstraintTarget as AstConstraintTarget, Expr, MapProjectionItem, Quantifier,
    ShowConstraints, UnaryOp,
};
use uni_store::QueryContext;
use uni_store::cloud::{build_store_from_url, copy_store_prefix, is_cloud_url};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::arrow_convert;
use uni_store::storage::index_manager::IndexManager;

// DataFusion engine imports
use crate::query::df_graph::L0Context;
use crate::query::df_planner::HybridPhysicalPlanner;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::SessionContext;
use parking_lot::RwLock as SyncRwLock;

use arrow_array::{Array, RecordBatch};
use csv;
use parquet;

use super::core::*;

/// Number of system fields on an edge map: `_eid`, `_src`, `_dst`, `_type`, `_type_name`.
const EDGE_SYSTEM_FIELD_COUNT: usize = 5;
/// Number of system fields on a vertex map: `_vid`, `_label`, `_uid`.
const VERTEX_SYSTEM_FIELD_COUNT: usize = 3;

/// Collect VIDs from all L0 buffers visible to a query context.
///
/// Applies `extractor` to each L0 buffer (main, transaction, pending flush) and
/// collects the results. Returns an empty vec when no query context is present.
fn collect_l0_vids(
    ctx: Option<&QueryContext>,
    extractor: impl Fn(&uni_store::runtime::l0::L0Buffer) -> Vec<Vid>,
) -> Vec<Vid> {
    let mut vids = Vec::new();
    if let Some(ctx) = ctx {
        vids.extend(extractor(&ctx.l0.read()));
        if let Some(tx_l0_arc) = &ctx.transaction_l0 {
            vids.extend(extractor(&tx_l0_arc.read()));
        }
        for pending_l0_arc in &ctx.pending_flush_l0s {
            vids.extend(extractor(&pending_l0_arc.read()));
        }
    }
    vids
}

/// Hydrate an entity map (vertex or edge) with properties if not already loaded.
///
/// This is the fallback for pushdown hydration - if the entity only has system fields
/// (indicating pushdown didn't load properties), we load all properties here.
///
/// System field counts:
/// - Edge: 5 fields (_eid, _src, _dst, _type, _type_name)
/// - Vertex: 3 fields (_vid, _label, _uid)
async fn hydrate_entity_if_needed(
    map: &mut HashMap<String, Value>,
    prop_manager: &PropertyManager,
    ctx: Option<&QueryContext>,
) {
    // Check for edge entity
    if let Some(eid_u64) = map.get("_eid").and_then(|v| v.as_u64()) {
        if map.len() <= EDGE_SYSTEM_FIELD_COUNT {
            tracing::debug!(
                "Pushdown fallback: hydrating edge {} at execution time",
                eid_u64
            );
            if let Ok(Some(props)) = prop_manager
                .get_all_edge_props_with_ctx(Eid::from(eid_u64), ctx)
                .await
            {
                for (key, value) in props {
                    map.entry(key).or_insert(value);
                }
            }
        } else {
            tracing::trace!(
                "Pushdown success: edge {} already has {} properties",
                eid_u64,
                map.len() - EDGE_SYSTEM_FIELD_COUNT
            );
        }
        return;
    }

    // Check for vertex entity
    if let Some(vid_u64) = map.get("_vid").and_then(|v| v.as_u64()) {
        if map.len() <= VERTEX_SYSTEM_FIELD_COUNT {
            tracing::debug!(
                "Pushdown fallback: hydrating vertex {} at execution time",
                vid_u64
            );
            if let Ok(Some(props)) = prop_manager
                .get_all_vertex_props_with_ctx(Vid::from(vid_u64), ctx)
                .await
            {
                for (key, value) in props {
                    map.entry(key).or_insert(value);
                }
            }
        } else {
            tracing::trace!(
                "Pushdown success: vertex {} already has {} properties",
                vid_u64,
                map.len() - VERTEX_SYSTEM_FIELD_COUNT
            );
        }
    }
}

impl Executor {
    /// Helper to verify and filter candidates against an optional predicate.
    ///
    /// Deduplicates candidates, loads properties, and evaluates the filter expression.
    /// Returns only VIDs that pass the filter (or are not deleted).
    async fn verify_and_filter_candidates(
        &self,
        mut candidates: Vec<Vid>,
        variable: &str,
        filter: Option<&Expr>,
        ctx: Option<&QueryContext>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Vid>> {
        candidates.sort_unstable();
        candidates.dedup();

        let mut verified_vids = Vec::new();
        for vid in candidates {
            let Some(props) = prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await? else {
                continue; // Deleted
            };

            if let Some(expr) = filter {
                let mut props_map: HashMap<String, Value> = props;
                props_map.insert("_vid".to_string(), Value::Int(vid.as_u64() as i64));

                let mut row = HashMap::new();
                row.insert(variable.to_string(), Value::Map(props_map));

                let res = self
                    .evaluate_expr(expr, &row, prop_manager, params, ctx)
                    .await?;
                if res.as_bool().unwrap_or(false) {
                    verified_vids.push(vid);
                }
            } else {
                verified_vids.push(vid);
            }
        }

        Ok(verified_vids)
    }

    pub(crate) async fn scan_storage_candidates(
        &self,
        label_id: u16,
        variable: &str,
        filter: Option<&Expr>,
    ) -> Result<Vec<Vid>> {
        let schema = self.storage.schema_manager().schema();
        let label_name = schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

        let ds = self.storage.vertex_dataset(label_name)?;
        let lancedb_store = self.storage.lancedb_store();

        // Try LanceDB first (canonical storage)
        match ds.open_lancedb(lancedb_store).await {
            Ok(table) => {
                use arrow_array::UInt64Array;
                use futures::TryStreamExt;
                use lancedb::query::{ExecutableQuery, QueryBase, Select};

                let mut query = table.query();

                // Apply filter if provided, with schema awareness
                // to skip overflow properties that aren't physical Lance columns.
                // For labels with no registered properties (schemaless), use an empty
                // map so all non-system properties are recognized as overflow.
                let empty_props = std::collections::HashMap::new();
                let label_props = schema.properties.get(label_name).unwrap_or(&empty_props);
                if let Some(expr) = filter
                    && let Some(sql) = LanceFilterGenerator::generate(
                        std::slice::from_ref(expr),
                        variable,
                        Some(label_props),
                    )
                {
                    query = query.only_if(format!("_deleted = false AND ({})", sql));
                } else {
                    query = query.only_if("_deleted = false");
                }

                // Project to only _vid
                let query = query.select(Select::columns(&["_vid"]));
                let stream = query.execute().await?;
                let batches: Vec<arrow_array::RecordBatch> = stream.try_collect().await?;

                let mut vids = Vec::new();
                for batch in batches {
                    let vid_col = batch
                        .column_by_name("_vid")
                        .ok_or(anyhow!("Missing _vid"))?
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or(anyhow!("Invalid _vid"))?;
                    for i in 0..batch.num_rows() {
                        vids.push(Vid::from(vid_col.value(i)));
                    }
                }
                Ok(vids)
            }
            Err(e) => {
                // Only treat "not found" / "does not exist" errors as empty results.
                // Propagate all other errors (network, auth, corruption, etc.)
                let err_msg = e.to_string().to_lowercase();
                if err_msg.contains("not found")
                    || err_msg.contains("does not exist")
                    || err_msg.contains("no such file")
                    || err_msg.contains("object not found")
                {
                    Ok(Vec::new())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub(crate) async fn scan_label_with_filter(
        &self,
        label_id: u16,
        variable: &str,
        filter: Option<&Expr>,
        ctx: Option<&QueryContext>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Vid>> {
        let mut candidates = self
            .scan_storage_candidates(label_id, variable, filter)
            .await?;

        // Convert label_id to label_name for L0 lookup
        let schema = self.storage.schema_manager().schema();
        if let Some(label_name) = schema.label_name_by_id(label_id) {
            candidates.extend(collect_l0_vids(ctx, |l0| l0.vids_for_label(label_name)));
        }

        self.verify_and_filter_candidates(candidates, variable, filter, ctx, prop_manager, params)
            .await
    }

    pub(crate) fn vid_from_value(val: &Value) -> Result<Vid> {
        // Handle Value::Node directly (has vid field)
        if let Value::Node(node) = val {
            return Ok(node.vid);
        }
        // Handle Object (node) containing _vid field
        if let Value::Map(map) = val
            && let Some(vid_val) = map.get("_vid")
            && let Some(v) = vid_val.as_u64()
        {
            return Ok(Vid::from(v));
        }
        // Handle string format
        if let Some(s) = val.as_str()
            && let Ok(id) = s.parse::<u64>()
        {
            return Ok(Vid::new(id));
        }
        // Handle raw u64
        if let Some(v) = val.as_u64() {
            return Ok(Vid::from(v));
        }
        Err(anyhow!("Invalid Vid format: {:?}", val))
    }

    /// Find a node value in the row by VID.
    ///
    /// Scans all values in the row, looking for a node (Map or Node) whose VID
    /// matches the target. Returns the full node value if found, or a minimal
    /// Map with just `_vid` as fallback.
    fn find_node_by_vid(row: &HashMap<String, Value>, target_vid: Vid) -> Value {
        for val in row.values() {
            if let Ok(vid) = Self::vid_from_value(val)
                && vid == target_vid
            {
                return val.clone();
            }
        }
        // Fallback: return minimal node map
        Value::Map(HashMap::from([(
            "_vid".to_string(),
            Value::Int(target_vid.as_u64() as i64),
        )]))
    }

    /// Executes a query using the DataFusion-based engine.
    ///
    /// Uses `HybridPhysicalPlanner` which produces DataFusion `ExecutionPlan`
    /// trees with custom graph operators for graph-specific operations.
    pub async fn execute_datafusion(
        &self,
        plan: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<RecordBatch>> {
        use futures::TryStreamExt;

        let ctx = self.get_context().await;

        // Build L0 context for MVCC visibility
        let l0_context = if let Some(ref query_ctx) = ctx {
            L0Context::from_query_context(query_ctx)
        } else {
            L0Context::empty()
        };

        // Create property manager Arc
        let prop_manager_arc = Arc::new(PropertyManager::new(
            self.storage.clone(),
            self.storage.schema_manager_arc(),
            prop_manager.cache_size(),
        ));

        // Create DataFusion session context
        let ctx = SessionContext::new();

        // Register Cypher UDFs (including JSON functions for overflow properties)
        crate::query::df_udfs::register_cypher_udfs(&ctx)?;

        let session_ctx = Arc::new(SyncRwLock::new(ctx));

        // Create hybrid planner
        let mut planner = HybridPhysicalPlanner::with_l0_context(
            session_ctx.clone(),
            self.storage.clone(),
            l0_context,
            prop_manager_arc.clone(),
            Arc::new(self.storage.schema_manager().schema()),
            params.clone(),
        );

        // Thread algorithm registry for uni.algo.* procedure dispatch
        planner = planner.with_algo_registry(self.algo_registry.clone());

        // Build MutationContext when the plan contains write operations
        if Self::contains_write_operations(&plan) {
            let writer = self
                .writer
                .as_ref()
                .ok_or_else(|| anyhow!("Write operations require a Writer"))?
                .clone();
            let query_ctx = self.get_context().await;

            debug_assert!(
                query_ctx.is_some(),
                "BUG: query_ctx is None for write operation"
            );

            let mutation_ctx = Arc::new(crate::query::df_graph::MutationContext {
                executor: self.clone(),
                writer,
                prop_manager: prop_manager_arc,
                params: params.clone(),
                query_ctx,
            });
            planner = planner.with_mutation_context(mutation_ctx);
            tracing::debug!(
                plan_type = Self::get_plan_type(&plan),
                "Mutation routed to DataFusion engine"
            );
        }

        // Plan the query
        let execution_plan = planner.plan(&plan)?;

        // Execute and collect results from all partitions
        let task_ctx = session_ctx.read().task_ctx();
        let partition_count = execution_plan.output_partitioning().partition_count();

        let mut all_batches = Vec::new();
        for partition in 0..partition_count {
            let stream = execution_plan.execute(partition, task_ctx.clone())?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    /// Execute a MERGE read sub-plan through the DataFusion engine.
    ///
    /// Plans and executes the MERGE pattern match using flat columnar output
    /// (no structural projections), then groups dotted columns (`a._vid`,
    /// `a._labels`, etc.) into per-variable Maps for downstream MERGE logic.
    pub(crate) async fn execute_merge_read_plan(
        &self,
        plan: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        merge_variables: Vec<String>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        use futures::TryStreamExt;

        let ctx = self.get_context().await;
        let l0_context = if let Some(ref query_ctx) = ctx {
            L0Context::from_query_context(query_ctx)
        } else {
            L0Context::empty()
        };

        let prop_manager_arc = Arc::new(PropertyManager::new(
            self.storage.clone(),
            self.storage.schema_manager_arc(),
            prop_manager.cache_size(),
        ));

        let session = SessionContext::new();
        crate::query::df_udfs::register_cypher_udfs(&session)?;
        let session_ctx = Arc::new(SyncRwLock::new(session));

        let planner = HybridPhysicalPlanner::with_l0_context(
            session_ctx.clone(),
            self.storage.clone(),
            l0_context,
            prop_manager_arc,
            Arc::new(self.storage.schema_manager().schema()),
            params.clone(),
        );

        // Plan with full property access ("*") for all merge variables so that
        // ON MATCH SET / ON CREATE SET have complete entity Maps to work with.
        let extra: HashMap<String, HashSet<String>> = merge_variables
            .iter()
            .map(|v| (v.clone(), ["*".to_string()].into_iter().collect()))
            .collect();
        let execution_plan = planner.plan_with_properties(&plan, extra)?;

        let task_ctx = session_ctx.read().task_ctx();
        let partition_count = execution_plan.output_partitioning().partition_count();
        let mut all_batches = Vec::new();
        for partition in 0..partition_count {
            let stream = execution_plan.execute(partition, task_ctx.clone())?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            all_batches.extend(batches);
        }

        // Convert to flat rows (dotted column names like "a._vid", "b._labels")
        let flat_rows = self.record_batches_to_rows(all_batches)?;

        // Group dotted columns into per-variable Maps for MERGE's match logic.
        // E.g., {"a._vid": 0, "a._labels": ["A"]} → {"a": Map({"_vid": 0, "_labels": ["A"]})}
        let rows = flat_rows
            .into_iter()
            .map(|mut row| {
                for var in &merge_variables {
                    // Skip if already materialized (e.g., by record_batches_to_rows)
                    if row.contains_key(var) {
                        continue;
                    }
                    let prefix = format!("{}.", var);
                    let dotted_keys: Vec<String> = row
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .cloned()
                        .collect();
                    if !dotted_keys.is_empty() {
                        let mut map = HashMap::new();
                        for key in dotted_keys {
                            let prop_name = key[prefix.len()..].to_string();
                            if let Some(val) = row.remove(&key) {
                                map.insert(prop_name, val);
                            }
                        }
                        row.insert(var.clone(), Value::Map(map));
                    }
                }
                row
            })
            .collect();

        Ok(rows)
    }

    /// Converts DataFusion RecordBatches to row-based HashMap format.
    ///
    /// Handles special metadata on fields:
    /// - `cv_encoded=true`: Parse the string value as JSON to restore original type
    ///
    /// Also normalizes path structures to user-facing format (converts _vid to _id).
    fn record_batches_to_rows(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut rows = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();
            let schema = batch.schema();

            for row_idx in 0..num_rows {
                let mut row = HashMap::new();

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_idx);
                    // Infer Uni DataType from Arrow type for DateTime/Time struct decoding
                    let data_type =
                        if uni_common::core::schema::is_datetime_struct(field.data_type()) {
                            Some(&uni_common::DataType::DateTime)
                        } else if uni_common::core::schema::is_time_struct(field.data_type()) {
                            Some(&uni_common::DataType::Time)
                        } else {
                            None
                        };
                    let mut value =
                        arrow_convert::arrow_to_value(column.as_ref(), row_idx, data_type);

                    // Check if this field contains JSON-encoded values (e.g., from UNWIND)
                    // Parse JSON string to restore the original type
                    if field
                        .metadata()
                        .get("cv_encoded")
                        .is_some_and(|v| v == "true")
                        && let Value::String(s) = &value
                        && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s)
                    {
                        value = Value::from(parsed);
                    }

                    // Normalize path structures to user-facing format
                    value = Self::normalize_path_if_needed(value);

                    row.insert(field.name().clone(), value);
                }

                // Merge system fields into bare variable maps.
                // The projection step emits helper columns like "n._vid" and "n._labels"
                // alongside the materialized "n" column (a Map of user properties).
                // Here we merge those system fields into the map and remove the helpers.
                //
                // For search procedures (vector/FTS), the bare variable may be a VID
                // string placeholder rather than a Map. In that case, promote it to a
                // Map so we can merge system fields and properties into it.
                let bare_vars: Vec<String> = row
                    .keys()
                    .filter(|k| !k.contains('.') && matches!(row.get(*k), Some(Value::Map(_))))
                    .cloned()
                    .collect();

                // Detect VID-placeholder variables that have system columns
                // (e.g., "node" is String("1") but "node._vid" exists).
                // Promote these to Maps so system fields can be merged in.
                let vid_placeholder_vars: Vec<String> = row
                    .keys()
                    .filter(|k| {
                        !k.contains('.')
                            && matches!(row.get(*k), Some(Value::String(_)))
                            && row.contains_key(&format!("{}._vid", k))
                    })
                    .cloned()
                    .collect();

                for var in &vid_placeholder_vars {
                    // Build a Map from system and property columns
                    let prefix = format!("{}.", var);
                    let mut map = HashMap::new();

                    let dotted_keys: Vec<String> = row
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .cloned()
                        .collect();

                    for key in &dotted_keys {
                        let prop_name = &key[prefix.len()..];
                        if let Some(val) = row.remove(key) {
                            map.insert(prop_name.to_string(), val);
                        }
                    }

                    // Replace the VID-string placeholder with the constructed Map
                    row.insert(var.clone(), Value::Map(map));
                }

                for var in &bare_vars {
                    // Merge node system fields (_vid, _labels)
                    let vid_key = format!("{}._vid", var);
                    let labels_key = format!("{}._labels", var);

                    let vid_val = row.remove(&vid_key);
                    let labels_val = row.remove(&labels_key);

                    if let Some(Value::Map(map)) = row.get_mut(var) {
                        if let Some(v) = vid_val {
                            map.insert("_vid".to_string(), v);
                        }
                        if let Some(v) = labels_val {
                            map.insert("_labels".to_string(), v);
                        }
                    }

                    // Merge edge system fields (_eid, _type, _src_vid, _dst_vid).
                    // These are emitted as helper columns by the traverse exec.
                    // The structural projection already includes them in the struct,
                    // but we still need to remove the dotted helper columns.
                    let eid_key = format!("{}._eid", var);
                    let type_key = format!("{}._type", var);

                    let eid_val = row.remove(&eid_key);
                    let type_val = row.remove(&type_key);

                    if (eid_val.is_some() || type_val.is_some())
                        && let Some(Value::Map(map)) = row.get_mut(var)
                    {
                        if let Some(v) = eid_val {
                            map.entry("_eid".to_string()).or_insert(v);
                        }
                        if let Some(v) = type_val {
                            map.entry("_type".to_string()).or_insert(v);
                        }
                    }

                    // Remove remaining dotted helper columns (e.g. _all_props, _src_vid, _dst_vid)
                    let prefix = format!("{}.", var);
                    let helper_keys: Vec<String> = row
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .cloned()
                        .collect();
                    for key in helper_keys {
                        row.remove(&key);
                    }
                }

                rows.push(row);
            }
        }

        Ok(rows)
    }

    /// Normalize a value if it's a path structure, converting internal format to user-facing format.
    ///
    /// This only normalizes path structures (objects with "nodes" and "relationships" arrays).
    /// Other values are returned unchanged to avoid interfering with query execution.
    fn normalize_path_if_needed(value: Value) -> Value {
        match value {
            Value::Map(map)
                if map.contains_key("nodes")
                    && (map.contains_key("relationships") || map.contains_key("edges")) =>
            {
                Self::normalize_path_map(map)
            }
            other => other,
        }
    }

    /// Normalize a path map object.
    fn normalize_path_map(mut map: HashMap<String, Value>) -> Value {
        // Normalize nodes array
        if let Some(Value::List(nodes)) = map.remove("nodes") {
            let normalized_nodes: Vec<Value> = nodes
                .into_iter()
                .map(|n| {
                    if let Value::Map(node_map) = n {
                        Self::normalize_path_node_map(node_map)
                    } else {
                        n
                    }
                })
                .collect();
            map.insert("nodes".to_string(), Value::List(normalized_nodes));
        }

        // Normalize relationships array (may be called "relationships" or "edges")
        let rels_key = if map.contains_key("relationships") {
            "relationships"
        } else {
            "edges"
        };
        if let Some(Value::List(rels)) = map.remove(rels_key) {
            let normalized_rels: Vec<Value> = rels
                .into_iter()
                .map(|r| {
                    if let Value::Map(rel_map) = r {
                        Self::normalize_path_edge_map(rel_map)
                    } else {
                        r
                    }
                })
                .collect();
            map.insert("relationships".to_string(), Value::List(normalized_rels));
        }

        Value::Map(map)
    }

    /// Convert a Value to its string representation for path normalization.
    fn value_to_id_string(val: Value) -> String {
        match val {
            Value::Int(n) => n.to_string(),
            Value::Float(n) => n.to_string(),
            Value::String(s) => s,
            other => other.to_string(),
        }
    }

    /// Move a map entry from `src_key` to `dst_key`, converting the value to a string.
    /// When `src_key == dst_key`, this simply stringifies the value in place.
    fn stringify_map_field(map: &mut HashMap<String, Value>, src_key: &str, dst_key: &str) {
        if let Some(val) = map.remove(src_key) {
            map.insert(
                dst_key.to_string(),
                Value::String(Self::value_to_id_string(val)),
            );
        }
    }

    /// Ensure the "properties" field is a non-null map.
    fn ensure_properties_map(map: &mut HashMap<String, Value>) {
        match map.get("properties") {
            Some(props) if !props.is_null() => {}
            _ => {
                map.insert("properties".to_string(), Value::Map(HashMap::new()));
            }
        }
    }

    /// Normalize a node within a path to user-facing format.
    fn normalize_path_node_map(mut map: HashMap<String, Value>) -> Value {
        Self::stringify_map_field(&mut map, "_vid", "_id");
        Self::ensure_properties_map(&mut map);
        Value::Map(map)
    }

    /// Normalize an edge within a path to user-facing format.
    fn normalize_path_edge_map(mut map: HashMap<String, Value>) -> Value {
        Self::stringify_map_field(&mut map, "_eid", "_id");
        Self::stringify_map_field(&mut map, "_src", "_src");
        Self::stringify_map_field(&mut map, "_dst", "_dst");

        if let Some(type_name) = map.remove("_type_name") {
            map.insert("_type".to_string(), type_name);
        }

        Self::ensure_properties_map(&mut map);
        Value::Map(map)
    }

    #[instrument(
        skip(self, prop_manager, params),
        fields(rows_returned, duration_ms),
        level = "info"
    )]
    pub fn execute<'a>(
        &'a self,
        plan: LogicalPlan,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
    ) -> BoxFuture<'a, Result<Vec<HashMap<String, Value>>>> {
        Box::pin(async move {
            let query_type = Self::get_plan_type(&plan);
            let ctx = self.get_context().await;
            let start = Instant::now();

            // Route DDL/Admin queries to the fallback executor.
            // All other queries (reads, mutations, window functions) flow through DataFusion.
            let res = if Self::is_ddl_or_admin(&plan) {
                self.execute_subplan(plan, prop_manager, params, ctx.as_ref())
                    .await
            } else {
                let batches = self
                    .execute_datafusion(plan.clone(), prop_manager, params)
                    .await?;
                self.record_batches_to_rows(batches)
            };

            let duration = start.elapsed();
            metrics::histogram!("uni_query_duration_seconds", "query_type" => query_type)
                .record(duration.as_secs_f64());

            tracing::Span::current().record("duration_ms", duration.as_millis());
            match &res {
                Ok(rows) => {
                    tracing::Span::current().record("rows_returned", rows.len());
                    metrics::counter!("uni_query_rows_returned_total", "query_type" => query_type)
                        .increment(rows.len() as u64);
                }
                Err(e) => {
                    let error_type = if e.to_string().contains("timed out") {
                        "timeout"
                    } else if e.to_string().contains("syntax") {
                        "syntax"
                    } else {
                        "execution"
                    };
                    metrics::counter!("uni_query_errors_total", "query_type" => query_type, "error_type" => error_type).increment(1);
                }
            }

            res
        })
    }

    fn get_plan_type(plan: &LogicalPlan) -> &'static str {
        match plan {
            LogicalPlan::Scan { .. } => "read_scan",
            LogicalPlan::ExtIdLookup { .. } => "read_extid_lookup",
            LogicalPlan::Traverse { .. } => "read_traverse",
            LogicalPlan::TraverseMainByType { .. } => "read_traverse_main",
            LogicalPlan::ScanAll { .. } => "read_scan_all",
            LogicalPlan::ScanMainByLabels { .. } => "read_scan_main",
            LogicalPlan::VectorKnn { .. } => "read_vector",
            LogicalPlan::Create { .. } | LogicalPlan::CreateBatch { .. } => "write_create",
            LogicalPlan::Merge { .. } => "write_merge",
            LogicalPlan::Delete { .. } => "write_delete",
            LogicalPlan::Set { .. } => "write_set",
            LogicalPlan::Remove { .. } => "write_remove",
            LogicalPlan::ProcedureCall { .. } => "call",
            LogicalPlan::Copy { .. } => "copy",
            LogicalPlan::Backup { .. } => "backup",
            _ => "other",
        }
    }

    /// Return all direct child plan references from a `LogicalPlan`.
    ///
    /// This centralizes the variant→children mapping so that recursive walkers
    /// (e.g., `contains_write_operations`) can delegate the
    /// "recurse into children" logic instead of duplicating the match arms.
    ///
    /// Note: `Foreach` returns only its `input`; the `body: Vec<LogicalPlan>`
    /// is not included because it requires special iteration. Callers that
    /// need to inspect the body should handle `Foreach` before falling through.
    fn plan_children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
        match plan {
            // Single-input wrappers
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Unwind { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Create { input, .. }
            | LogicalPlan::CreateBatch { input, .. }
            | LogicalPlan::Set { input, .. }
            | LogicalPlan::Remove { input, .. }
            | LogicalPlan::Delete { input, .. }
            | LogicalPlan::Merge { input, .. }
            | LogicalPlan::Foreach { input, .. }
            | LogicalPlan::Traverse { input, .. }
            | LogicalPlan::TraverseMainByType { input, .. }
            | LogicalPlan::BindZeroLengthPath { input, .. }
            | LogicalPlan::BindPath { input, .. }
            | LogicalPlan::ShortestPath { input, .. }
            | LogicalPlan::AllShortestPaths { input, .. }
            | LogicalPlan::Explain { plan: input, .. } => vec![input.as_ref()],

            // Two-input wrappers
            LogicalPlan::Apply {
                input, subquery, ..
            }
            | LogicalPlan::SubqueryCall { input, subquery } => {
                vec![input.as_ref(), subquery.as_ref()]
            }
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                vec![left.as_ref(), right.as_ref()]
            }
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => vec![initial.as_ref(), recursive.as_ref()],
            LogicalPlan::QuantifiedPattern {
                input,
                pattern_plan,
                ..
            } => vec![input.as_ref(), pattern_plan.as_ref()],

            // Leaf nodes (scans, DDL, admin, etc.)
            _ => vec![],
        }
    }

    /// Check if a plan is a DDL or admin operation that should skip DataFusion.
    ///
    /// These operations don't produce data streams and aren't supported by the
    /// DataFusion planner. Recurses through wrapper nodes (`Project`, `Sort`,
    /// `Limit`, etc.) to detect DDL/admin operations nested inside read
    /// wrappers (e.g. `CALL procedure(...) YIELD x RETURN x`).
    fn is_ddl_or_admin(plan: &LogicalPlan) -> bool {
        match plan {
            // DDL / schema operations
            LogicalPlan::CreateLabel(_)
            | LogicalPlan::CreateEdgeType(_)
            | LogicalPlan::AlterLabel(_)
            | LogicalPlan::AlterEdgeType(_)
            | LogicalPlan::DropLabel(_)
            | LogicalPlan::DropEdgeType(_)
            | LogicalPlan::CreateConstraint(_)
            | LogicalPlan::DropConstraint(_)
            | LogicalPlan::ShowConstraints(_) => true,

            // Index operations
            LogicalPlan::CreateVectorIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateScalarIndex { .. }
            | LogicalPlan::CreateJsonFtsIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::ShowIndexes { .. } => true,

            // Admin / utility operations
            LogicalPlan::ShowDatabase
            | LogicalPlan::ShowConfig
            | LogicalPlan::ShowStatistics
            | LogicalPlan::Vacuum
            | LogicalPlan::Checkpoint
            | LogicalPlan::Begin
            | LogicalPlan::Commit
            | LogicalPlan::Rollback
            | LogicalPlan::Copy { .. }
            | LogicalPlan::CopyTo { .. }
            | LogicalPlan::CopyFrom { .. }
            | LogicalPlan::Backup { .. }
            | LogicalPlan::Explain { .. } => true,

            // Procedure calls: DF-eligible procedures go through DataFusion,
            // everything else (DDL, admin, unknown) stays on fallback.
            LogicalPlan::ProcedureCall { procedure_name, .. } => {
                !Self::is_df_eligible_procedure(procedure_name)
            }

            // Recurse through single-input read wrapper nodes
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Unwind { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::BindZeroLengthPath { input, .. }
            | LogicalPlan::BindPath { input, .. }
            | LogicalPlan::ShortestPath { input, .. }
            | LogicalPlan::AllShortestPaths { input, .. } => Self::is_ddl_or_admin(input),

            // Recurse through two-input wrapper nodes
            LogicalPlan::Apply {
                input, subquery, ..
            }
            | LogicalPlan::SubqueryCall { input, subquery } => {
                Self::is_ddl_or_admin(input) || Self::is_ddl_or_admin(subquery)
            }
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                Self::is_ddl_or_admin(left) || Self::is_ddl_or_admin(right)
            }
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => Self::is_ddl_or_admin(initial) || Self::is_ddl_or_admin(recursive),
            LogicalPlan::QuantifiedPattern {
                input,
                pattern_plan,
                ..
            } => Self::is_ddl_or_admin(input) || Self::is_ddl_or_admin(pattern_plan),

            _ => false,
        }
    }

    /// Returns `true` if the procedure is a read-only, data-producing procedure
    /// that can be executed through the DataFusion engine.
    ///
    /// This is a **positive allowlist** — unknown procedures default to the
    /// fallback executor (safe for TCK test procedures, future DDL, and admin).
    fn is_df_eligible_procedure(name: &str) -> bool {
        matches!(
            name,
            "uni.schema.labels"
                | "uni.schema.edgeTypes"
                | "uni.schema.relationshipTypes"
                | "uni.schema.indexes"
                | "uni.schema.constraints"
                | "uni.schema.labelInfo"
                | "uni.vector.query"
                | "uni.fts.query"
                | "uni.search"
        ) || name.starts_with("uni.algo.")
    }

    /// Check if a plan contains write/mutation operations anywhere in the tree.
    ///
    /// Write operations (`CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`, `FOREACH`)
    /// are used to determine when a MutationContext needs to be built for DataFusion.
    /// This recurses through read-only wrapper nodes to detect writes nested inside
    /// projections (e.g. `CREATE (n:Person) RETURN n` produces `Project { Create { ... } }`).
    fn contains_write_operations(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Create { .. }
            | LogicalPlan::CreateBatch { .. }
            | LogicalPlan::Merge { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Set { .. }
            | LogicalPlan::Remove { .. }
            | LogicalPlan::Foreach { .. } => true,
            _ => Self::plan_children(plan)
                .iter()
                .any(|child| Self::contains_write_operations(child)),
        }
    }

    /// Executes a query as a stream of result batches.
    ///
    /// Routes DDL/Admin through the fallback executor and everything else
    /// through DataFusion.
    pub fn execute_stream(
        self,
        plan: LogicalPlan,
        prop_manager: Arc<PropertyManager>,
        params: HashMap<String, Value>,
    ) -> BoxStream<'static, Result<Vec<HashMap<String, Value>>>> {
        let this = self;
        let this_for_ctx = this.clone();

        let ctx_stream = stream::once(async move { this_for_ctx.get_context().await });

        ctx_stream
            .flat_map(move |ctx| {
                let plan = plan.clone();
                let this = this.clone();
                let prop_manager = prop_manager.clone();
                let params = params.clone();

                let fut = async move {
                    if Self::is_ddl_or_admin(&plan) {
                        this.execute_subplan(plan, &prop_manager, &params, ctx.as_ref())
                            .await
                    } else {
                        let batches = this
                            .execute_datafusion(plan, &prop_manager, &params)
                            .await?;
                        this.record_batches_to_rows(batches)
                    }
                };
                stream::once(fut).boxed()
            })
            .boxed()
    }

    /// Converts an Arrow array element at a given row index to a Value.
    /// Delegates to the shared implementation in arrow_convert module.
    pub(crate) fn arrow_to_value(col: &dyn Array, row: usize) -> Value {
        arrow_convert::arrow_to_value(col, row, None)
    }

    pub(crate) fn evaluate_expr<'a>(
        &'a self,
        expr: &'a Expr,
        row: &'a HashMap<String, Value>,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> BoxFuture<'a, Result<Value>> {
        let this = self;
        Box::pin(async move {
            // First check if the expression itself is already pre-computed in the row
            let repr = expr.to_string_repr();
            if let Some(val) = row.get(&repr) {
                return Ok(val.clone());
            }

            match expr {
                Expr::PatternComprehension { .. } => {
                    // Handled by DataFusion path via PatternComprehensionExecExpr
                    Err(anyhow::anyhow!(
                        "Pattern comprehensions are handled by DataFusion executor"
                    ))
                }
                Expr::CollectSubquery(_) => Err(anyhow::anyhow!(
                    "COLLECT subqueries not yet supported in executor"
                )),
                Expr::Variable(name) => {
                    if let Some(val) = row.get(name) {
                        Ok(val.clone())
                    } else {
                        Ok(params.get(name).cloned().unwrap_or(Value::Null))
                    }
                }
                Expr::Parameter(name) => Ok(params.get(name).cloned().unwrap_or(Value::Null)),
                Expr::Property(var_expr, prop_name) => {
                    let base_val = this
                        .evaluate_expr(var_expr, row, prop_manager, params, ctx)
                        .await?;

                    // Handle system properties _vid and _id directly
                    if (prop_name == "_vid" || prop_name == "_id")
                        && let Ok(vid) = Self::vid_from_value(&base_val)
                    {
                        return Ok(Value::Int(vid.as_u64() as i64));
                    }

                    // Handle Value::Node - access properties directly or via prop manager
                    if let Value::Node(node) = &base_val {
                        // Handle system properties
                        if prop_name == "_vid" || prop_name == "_id" {
                            return Ok(Value::Int(node.vid.as_u64() as i64));
                        }
                        if prop_name == "_labels" {
                            return Ok(Value::List(
                                node.labels
                                    .iter()
                                    .map(|l| Value::String(l.clone()))
                                    .collect(),
                            ));
                        }
                        // Check in-memory properties first
                        if let Some(val) = node.properties.get(prop_name.as_str()) {
                            return Ok(val.clone());
                        }
                        // Fallback to storage lookup
                        if let Ok(val) = prop_manager
                            .get_vertex_prop_with_ctx(node.vid, prop_name, ctx)
                            .await
                        {
                            return Ok(val);
                        }
                        return Ok(Value::Null);
                    }

                    // Handle Value::Edge - access properties directly or via prop manager
                    if let Value::Edge(edge) = &base_val {
                        // Handle system properties
                        if prop_name == "_eid" || prop_name == "_id" {
                            return Ok(Value::Int(edge.eid.as_u64() as i64));
                        }
                        if prop_name == "_type" {
                            return Ok(Value::String(edge.edge_type.clone()));
                        }
                        if prop_name == "_src" {
                            return Ok(Value::Int(edge.src.as_u64() as i64));
                        }
                        if prop_name == "_dst" {
                            return Ok(Value::Int(edge.dst.as_u64() as i64));
                        }
                        // Check in-memory properties first
                        if let Some(val) = edge.properties.get(prop_name.as_str()) {
                            return Ok(val.clone());
                        }
                        // Fallback to storage lookup
                        if let Ok(val) = prop_manager.get_edge_prop(edge.eid, prop_name, ctx).await
                        {
                            return Ok(val);
                        }
                        return Ok(Value::Null);
                    }

                    // If base_val is an object (node/edge), check its properties first
                    // This handles properties from CREATE/MERGE that may not be persisted yet
                    if let Value::Map(map) = &base_val {
                        // First check top-level (for system properties like _id, _label, etc.)
                        if let Some(val) = map.get(prop_name.as_str()) {
                            return Ok(val.clone());
                        }
                        // Then check inside "properties" object (for user properties)
                        if let Some(Value::Map(props)) = map.get("properties")
                            && let Some(val) = props.get(prop_name.as_str())
                        {
                            return Ok(val.clone());
                        }
                        // Fallback to storage lookup using _vid or _id
                        let vid_opt = map.get("_vid").and_then(|v| v.as_u64()).or_else(|| {
                            map.get("_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<u64>().ok())
                        });
                        if let Some(id) = vid_opt {
                            let vid = Vid::from(id);
                            if let Ok(val) = prop_manager
                                .get_vertex_prop_with_ctx(vid, prop_name, ctx)
                                .await
                            {
                                return Ok(val);
                            }
                        } else if let Some(id) = map.get("_eid").and_then(|v| v.as_u64()) {
                            let eid = uni_common::core::id::Eid::from(id);
                            if let Ok(val) = prop_manager.get_edge_prop(eid, prop_name, ctx).await {
                                return Ok(val);
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // If base_val is just a VID, fetch from property manager
                    if let Ok(vid) = Self::vid_from_value(&base_val) {
                        return prop_manager
                            .get_vertex_prop_with_ctx(vid, prop_name, ctx)
                            .await;
                    }

                    if base_val.is_null() {
                        return Ok(Value::Null);
                    }

                    // Check if base_val is a temporal value and prop_name is a temporal accessor
                    {
                        use crate::query::datetime::{
                            eval_duration_accessor, eval_temporal_accessor, is_duration_accessor,
                            is_duration_string, is_temporal_accessor, is_temporal_string,
                        };

                        // Handle Value::Temporal directly (no string parsing needed)
                        if let Value::Temporal(tv) = &base_val {
                            if matches!(tv, uni_common::TemporalValue::Duration { .. }) {
                                if is_duration_accessor(prop_name) {
                                    // Convert to string for the existing accessor logic
                                    return eval_duration_accessor(
                                        &base_val.to_string(),
                                        prop_name,
                                    );
                                }
                            } else if is_temporal_accessor(prop_name) {
                                return eval_temporal_accessor(&base_val.to_string(), prop_name);
                            }
                        }

                        // Handle Value::String temporal (backward compat)
                        if let Value::String(s) = &base_val {
                            if is_temporal_string(s) && is_temporal_accessor(prop_name) {
                                return eval_temporal_accessor(s, prop_name);
                            }
                            if is_duration_string(s) && is_duration_accessor(prop_name) {
                                return eval_duration_accessor(s, prop_name);
                            }
                        }
                    }

                    Err(anyhow!(
                        "Cannot access property '{}' on {:?}",
                        prop_name,
                        base_val
                    ))
                }
                Expr::ArrayIndex {
                    array: arr_expr,
                    index: idx_expr,
                } => {
                    let arr_val = this
                        .evaluate_expr(arr_expr, row, prop_manager, params, ctx)
                        .await?;
                    let idx_val = this
                        .evaluate_expr(idx_expr, row, prop_manager, params, ctx)
                        .await?;

                    if let Value::List(arr) = &arr_val {
                        // Handle signed indices (allow negative)
                        if let Some(i) = idx_val.as_i64() {
                            let idx = if i < 0 {
                                // Negative index: -1 = last element, -2 = second to last, etc.
                                let positive_idx = arr.len() as i64 + i;
                                if positive_idx < 0 {
                                    return Ok(Value::Null); // Out of bounds
                                }
                                positive_idx as usize
                            } else {
                                i as usize
                            };
                            if idx < arr.len() {
                                return Ok(arr[idx].clone());
                            }
                            return Ok(Value::Null);
                        } else if idx_val.is_null() {
                            return Ok(Value::Null);
                        } else {
                            return Err(anyhow::anyhow!(
                                "TypeError: InvalidArgumentType - list index must be an integer, got: {:?}",
                                idx_val
                            ));
                        }
                    }
                    if let Value::Map(map) = &arr_val {
                        if let Some(key) = idx_val.as_str() {
                            return Ok(map.get(key).cloned().unwrap_or(Value::Null));
                        } else if !idx_val.is_null() {
                            return Err(anyhow::anyhow!(
                                "TypeError: InvalidArgumentValue - Map index must be a string, got: {:?}",
                                idx_val
                            ));
                        }
                    }
                    // Handle bracket access on Node: n['name'] returns property
                    if let Value::Node(node) = &arr_val {
                        if let Some(key) = idx_val.as_str() {
                            // Check in-memory properties first
                            if let Some(val) = node.properties.get(key) {
                                return Ok(val.clone());
                            }
                            // Fallback to property manager
                            if let Ok(val) = prop_manager
                                .get_vertex_prop_with_ctx(node.vid, key, ctx)
                                .await
                            {
                                return Ok(val);
                            }
                            return Ok(Value::Null);
                        } else if !idx_val.is_null() {
                            return Err(anyhow::anyhow!(
                                "TypeError: Node index must be a string, got: {:?}",
                                idx_val
                            ));
                        }
                    }
                    // Handle bracket access on Edge: e['property'] returns property
                    if let Value::Edge(edge) = &arr_val {
                        if let Some(key) = idx_val.as_str() {
                            // Check in-memory properties first
                            if let Some(val) = edge.properties.get(key) {
                                return Ok(val.clone());
                            }
                            // Fallback to property manager
                            if let Ok(val) = prop_manager.get_edge_prop(edge.eid, key, ctx).await {
                                return Ok(val);
                            }
                            return Ok(Value::Null);
                        } else if !idx_val.is_null() {
                            return Err(anyhow::anyhow!(
                                "TypeError: Edge index must be a string, got: {:?}",
                                idx_val
                            ));
                        }
                    }
                    // Handle bracket access on VID (integer): n['name'] where n is a VID
                    if let Ok(vid) = Self::vid_from_value(&arr_val)
                        && let Some(key) = idx_val.as_str()
                    {
                        if let Ok(val) = prop_manager.get_vertex_prop_with_ctx(vid, key, ctx).await
                        {
                            return Ok(val);
                        }
                        return Ok(Value::Null);
                    }
                    if arr_val.is_null() {
                        return Ok(Value::Null);
                    }
                    Err(anyhow!(
                        "TypeError: InvalidArgumentType - cannot index into {:?}",
                        arr_val
                    ))
                }
                Expr::ArraySlice { array, start, end } => {
                    let arr_val = this
                        .evaluate_expr(array, row, prop_manager, params, ctx)
                        .await?;

                    if let Value::List(arr) = &arr_val {
                        let len = arr.len();

                        // Evaluate start index (default to 0), null → null result
                        let start_idx = if let Some(s) = start {
                            let v = this
                                .evaluate_expr(s, row, prop_manager, params, ctx)
                                .await?;
                            if v.is_null() {
                                return Ok(Value::Null);
                            }
                            let raw = v.as_i64().unwrap_or(0);
                            if raw < 0 {
                                (len as i64 + raw).max(0) as usize
                            } else {
                                (raw as usize).min(len)
                            }
                        } else {
                            0
                        };

                        // Evaluate end index (default to length), null → null result
                        let end_idx = if let Some(e) = end {
                            let v = this
                                .evaluate_expr(e, row, prop_manager, params, ctx)
                                .await?;
                            if v.is_null() {
                                return Ok(Value::Null);
                            }
                            let raw = v.as_i64().unwrap_or(len as i64);
                            if raw < 0 {
                                (len as i64 + raw).max(0) as usize
                            } else {
                                (raw as usize).min(len)
                            }
                        } else {
                            len
                        };

                        // Return sliced array
                        if start_idx >= end_idx {
                            return Ok(Value::List(vec![]));
                        }
                        let end_idx = end_idx.min(len);
                        return Ok(Value::List(arr[start_idx..end_idx].to_vec()));
                    }

                    if arr_val.is_null() {
                        return Ok(Value::Null);
                    }
                    Err(anyhow!("Cannot slice {:?}", arr_val))
                }
                Expr::Literal(lit) => Ok(lit.to_value()),
                Expr::List(items) => {
                    let mut vals = Vec::new();
                    for item in items {
                        vals.push(
                            this.evaluate_expr(item, row, prop_manager, params, ctx)
                                .await?,
                        );
                    }
                    Ok(Value::List(vals))
                }
                Expr::Map(items) => {
                    let mut map = HashMap::new();
                    for (key, value_expr) in items {
                        let val = this
                            .evaluate_expr(value_expr, row, prop_manager, params, ctx)
                            .await?;
                        map.insert(key.clone(), val);
                    }
                    Ok(Value::Map(map))
                }
                Expr::Exists { query, .. } => {
                    // Plan and execute subquery; failures return false (pattern doesn't match)
                    let planner =
                        QueryPlanner::new(Arc::new(this.storage.schema_manager().schema()));
                    let vars_in_scope: Vec<String> = row.keys().cloned().collect();

                    match planner.plan_with_scope(*query.clone(), vars_in_scope) {
                        Ok(plan) => {
                            let mut sub_params = params.clone();
                            sub_params.extend(row.clone());

                            match this.execute(plan, prop_manager, &sub_params).await {
                                Ok(results) => Ok(Value::Bool(!results.is_empty())),
                                Err(e) => {
                                    log::debug!("EXISTS subquery execution failed: {}", e);
                                    Ok(Value::Bool(false))
                                }
                            }
                        }
                        Err(e) => {
                            log::debug!("EXISTS subquery planning failed: {}", e);
                            Ok(Value::Bool(false))
                        }
                    }
                }
                Expr::CountSubquery(query) => {
                    // Similar to Exists but returns count
                    let planner =
                        QueryPlanner::new(Arc::new(this.storage.schema_manager().schema()));

                    let vars_in_scope: Vec<String> = row.keys().cloned().collect();

                    match planner.plan_with_scope(*query.clone(), vars_in_scope) {
                        Ok(plan) => {
                            let mut sub_params = params.clone();
                            sub_params.extend(row.clone());

                            match this.execute(plan, prop_manager, &sub_params).await {
                                Ok(results) => Ok(Value::from(results.len() as i64)),
                                Err(e) => Err(anyhow!("Subquery execution failed: {}", e)),
                            }
                        }
                        Err(e) => Err(anyhow!("Subquery planning failed: {}", e)),
                    }
                }
                Expr::Quantifier {
                    quantifier,
                    variable,
                    list,
                    predicate,
                } => {
                    // Quantifier expression evaluation (ALL/ANY/SINGLE/NONE)
                    //
                    // This is the primary execution path for quantifiers because DataFusion
                    // does not support lambda functions yet. Queries with quantifiers attempt
                    // DataFusion translation first, fail (see df_expr.rs:289), then fall back
                    // to this fallback executor path.
                    //
                    // This is intentional design - we get correct semantics with row-by-row
                    // evaluation until DataFusion adds lambda support.
                    //
                    // See: https://github.com/apache/datafusion/issues/14205

                    // Evaluate the list expression
                    let list_val = this
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;

                    // Handle null propagation
                    if list_val.is_null() {
                        return Ok(Value::Null);
                    }

                    // Convert to array
                    let items = match list_val {
                        Value::List(arr) => arr,
                        _ => return Err(anyhow!("Quantifier expects a list, got: {:?}", list_val)),
                    };

                    // Evaluate predicate for each item
                    let mut satisfied_count = 0;
                    for item in &items {
                        // Create new row with bound variable
                        let mut item_row = row.clone();
                        item_row.insert(variable.clone(), item.clone());

                        // Evaluate predicate with bound variable
                        let pred_result = this
                            .evaluate_expr(predicate, &item_row, prop_manager, params, ctx)
                            .await?;

                        // Check if predicate is satisfied
                        if let Value::Bool(true) = pred_result {
                            satisfied_count += 1;
                        }
                    }

                    // Return based on quantifier type
                    let result = match quantifier {
                        Quantifier::All => satisfied_count == items.len(),
                        Quantifier::Any => satisfied_count > 0,
                        Quantifier::Single => satisfied_count == 1,
                        Quantifier::None => satisfied_count == 0,
                    };

                    Ok(Value::Bool(result))
                }
                Expr::ListComprehension {
                    variable,
                    list,
                    where_clause,
                    map_expr,
                } => {
                    // List comprehension evaluation: [x IN list WHERE pred | expr]
                    //
                    // Similar to quantifiers, this requires lambda-like evaluation
                    // which DataFusion doesn't support yet. This is the primary execution path.

                    // Evaluate the list expression
                    let list_val = this
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;

                    // Handle null propagation
                    if list_val.is_null() {
                        return Ok(Value::Null);
                    }

                    // Convert to array
                    let items = match list_val {
                        Value::List(arr) => arr,
                        _ => {
                            return Err(anyhow!(
                                "List comprehension expects a list, got: {:?}",
                                list_val
                            ));
                        }
                    };

                    // Collect mapped values
                    let mut results = Vec::new();
                    for item in &items {
                        // Create new row with bound variable
                        let mut item_row = row.clone();
                        item_row.insert(variable.clone(), item.clone());

                        // Apply WHERE filter if present
                        if let Some(predicate) = where_clause {
                            let pred_result = this
                                .evaluate_expr(predicate, &item_row, prop_manager, params, ctx)
                                .await?;

                            // Skip items that don't match the filter
                            if !matches!(pred_result, Value::Bool(true)) {
                                continue;
                            }
                        }

                        // Apply map expression
                        let mapped_val = this
                            .evaluate_expr(map_expr, &item_row, prop_manager, params, ctx)
                            .await?;
                        results.push(mapped_val);
                    }

                    Ok(Value::List(results))
                }
                Expr::BinaryOp { left, op, right } => {
                    // Short-circuit evaluation for AND/OR
                    match op {
                        BinaryOp::And => {
                            let l_val = this
                                .evaluate_expr(left, row, prop_manager, params, ctx)
                                .await?;
                            // Short-circuit: if left is false, don't evaluate right
                            if let Some(false) = l_val.as_bool() {
                                return Ok(Value::Bool(false));
                            }
                            let r_val = this
                                .evaluate_expr(right, row, prop_manager, params, ctx)
                                .await?;
                            eval_binary_op(&l_val, op, &r_val)
                        }
                        BinaryOp::Or => {
                            let l_val = this
                                .evaluate_expr(left, row, prop_manager, params, ctx)
                                .await?;
                            // Short-circuit: if left is true, don't evaluate right
                            if let Some(true) = l_val.as_bool() {
                                return Ok(Value::Bool(true));
                            }
                            let r_val = this
                                .evaluate_expr(right, row, prop_manager, params, ctx)
                                .await?;
                            eval_binary_op(&l_val, op, &r_val)
                        }
                        _ => {
                            // For all other operators, evaluate both sides
                            let l_val = this
                                .evaluate_expr(left, row, prop_manager, params, ctx)
                                .await?;
                            let r_val = this
                                .evaluate_expr(right, row, prop_manager, params, ctx)
                                .await?;
                            eval_binary_op(&l_val, op, &r_val)
                        }
                    }
                }
                Expr::In { expr, list } => {
                    let l_val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    let r_val = this
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;
                    eval_in_op(&l_val, &r_val)
                }
                Expr::UnaryOp { op, expr } => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    match op {
                        UnaryOp::Not => {
                            // Three-valued logic: NOT null = null
                            match val.as_bool() {
                                Some(b) => Ok(Value::Bool(!b)),
                                None if val.is_null() => Ok(Value::Null),
                                None => Err(anyhow!(
                                    "InvalidArgumentType: NOT requires a boolean argument"
                                )),
                            }
                        }
                        UnaryOp::Neg => {
                            if let Some(i) = val.as_i64() {
                                Ok(Value::Int(-i))
                            } else if let Some(f) = val.as_f64() {
                                Ok(Value::Float(-f))
                            } else {
                                Err(anyhow!("Cannot negate non-numeric value: {:?}", val))
                            }
                        }
                    }
                }
                Expr::IsNull(expr) => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    Ok(Value::Bool(val.is_null()))
                }
                Expr::IsNotNull(expr) => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    Ok(Value::Bool(!val.is_null()))
                }
                Expr::IsUnique(_) => {
                    // IS UNIQUE is only valid in constraint definitions, not in query expressions
                    Err(anyhow!(
                        "IS UNIQUE can only be used in constraint definitions"
                    ))
                }
                Expr::Case {
                    expr,
                    when_then,
                    else_expr,
                } => {
                    if let Some(base_expr) = expr {
                        let base_val = this
                            .evaluate_expr(base_expr, row, prop_manager, params, ctx)
                            .await?;
                        for (w, t) in when_then {
                            let w_val = this
                                .evaluate_expr(w, row, prop_manager, params, ctx)
                                .await?;
                            if base_val == w_val {
                                return this.evaluate_expr(t, row, prop_manager, params, ctx).await;
                            }
                        }
                    } else {
                        for (w, t) in when_then {
                            let w_val = this
                                .evaluate_expr(w, row, prop_manager, params, ctx)
                                .await?;
                            if w_val.as_bool() == Some(true) {
                                return this.evaluate_expr(t, row, prop_manager, params, ctx).await;
                            }
                        }
                    }
                    if let Some(e) = else_expr {
                        return this.evaluate_expr(e, row, prop_manager, params, ctx).await;
                    }
                    Ok(Value::Null)
                }
                Expr::Wildcard => Ok(Value::Null),
                Expr::FunctionCall { name, args, .. } => {
                    // Special case: id() returns VID for nodes and EID for relationships
                    if name.eq_ignore_ascii_case("ID") {
                        if args.len() != 1 {
                            return Err(anyhow!("id() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Map(map) = &val {
                            // Check for _vid (vertex) first
                            if let Some(vid_val) = map.get("_vid") {
                                return Ok(vid_val.clone());
                            }
                            // Check for _eid (edge/relationship)
                            if let Some(eid_val) = map.get("_eid") {
                                return Ok(eid_val.clone());
                            }
                            // Check for _id (fallback)
                            if let Some(id_val) = map.get("_id") {
                                return Ok(id_val.clone());
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: elementId() returns string format "label_id:local_offset"
                    if name.eq_ignore_ascii_case("ELEMENTID") {
                        if args.len() != 1 {
                            return Err(anyhow!("elementId() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Map(map) = &val {
                            // Check for _vid (vertex) first
                            // In new storage model, VIDs are pure auto-increment - return as simple ID string
                            if let Some(vid_val) = map.get("_vid").and_then(|v| v.as_u64()) {
                                return Ok(Value::String(vid_val.to_string()));
                            }
                            // Check for _eid (edge/relationship)
                            // In new storage model, EIDs are pure auto-increment - return as simple ID string
                            if let Some(eid_val) = map.get("_eid").and_then(|v| v.as_u64()) {
                                return Ok(Value::String(eid_val.to_string()));
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: type() returns the relationship type name
                    if name.eq_ignore_ascii_case("TYPE") {
                        if args.len() != 1 {
                            return Err(anyhow!("type() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Map(map) = &val
                            && let Some(type_val) = map.get("_type")
                        {
                            // Numeric _type is an edge type ID; string _type is already a name
                            if let Some(type_id) =
                                type_val.as_u64().and_then(|v| u32::try_from(v).ok())
                            {
                                if let Some(name) = this
                                    .storage
                                    .schema_manager()
                                    .edge_type_name_by_id_unified(type_id)
                                {
                                    return Ok(Value::String(name));
                                }
                            } else if let Some(name) = type_val.as_str() {
                                return Ok(Value::String(name.to_string()));
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: labels() returns the labels of a node
                    if name.eq_ignore_ascii_case("LABELS") {
                        if args.len() != 1 {
                            return Err(anyhow!("labels() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Map(map) = &val
                            && let Some(labels_val) = map.get("_labels")
                        {
                            return Ok(labels_val.clone());
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: properties() returns the properties map of a node/edge
                    if name.eq_ignore_ascii_case("PROPERTIES") {
                        if args.len() != 1 {
                            return Err(anyhow!("properties() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Map(map) = &val {
                            // Filter out internal properties (those starting with _)
                            let mut props = HashMap::new();
                            for (k, v) in map.iter() {
                                if !k.starts_with('_') {
                                    props.insert(k.clone(), v.clone());
                                }
                            }
                            return Ok(Value::Map(props));
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: startNode() returns the start node of a relationship
                    if name.eq_ignore_ascii_case("STARTNODE") {
                        if args.len() != 1 {
                            return Err(anyhow!("startNode() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Edge(edge) = &val {
                            return Ok(Self::find_node_by_vid(row, edge.src));
                        }
                        if let Value::Map(map) = &val {
                            if let Some(start_node) = map.get("_startNode") {
                                return Ok(start_node.clone());
                            }
                            if let Some(src_vid) = map.get("_src_vid") {
                                return Ok(Value::Map(HashMap::from([(
                                    "_vid".to_string(),
                                    src_vid.clone(),
                                )])));
                            }
                            // Resolve _src VID by looking up node in row
                            if let Some(src_id) = map.get("_src")
                                && let Some(u) = src_id.as_u64()
                            {
                                return Ok(Self::find_node_by_vid(row, Vid::new(u)));
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: endNode() returns the end node of a relationship
                    if name.eq_ignore_ascii_case("ENDNODE") {
                        if args.len() != 1 {
                            return Err(anyhow!("endNode() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Edge(edge) = &val {
                            return Ok(Self::find_node_by_vid(row, edge.dst));
                        }
                        if let Value::Map(map) = &val {
                            if let Some(end_node) = map.get("_endNode") {
                                return Ok(end_node.clone());
                            }
                            if let Some(dst_vid) = map.get("_dst_vid") {
                                return Ok(Value::Map(HashMap::from([(
                                    "_vid".to_string(),
                                    dst_vid.clone(),
                                )])));
                            }
                            // Resolve _dst VID by looking up node in row
                            if let Some(dst_id) = map.get("_dst")
                                && let Some(u) = dst_id.as_u64()
                            {
                                return Ok(Self::find_node_by_vid(row, Vid::new(u)));
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: hasLabel() checks if a node has a specific label
                    // Used for WHERE n:Label predicates
                    if name.eq_ignore_ascii_case("HASLABEL") {
                        if args.len() != 2 {
                            return Err(anyhow!("hasLabel() requires exactly 2 arguments"));
                        }
                        let node_val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        let label_val = this
                            .evaluate_expr(&args[1], row, prop_manager, params, ctx)
                            .await?;

                        let label_to_check = label_val.as_str().ok_or_else(|| {
                            anyhow!("Second argument to hasLabel must be a string")
                        })?;

                        let has_label = match &node_val {
                            // Handle proper Value::Node type (from result normalization)
                            Value::Map(map) if map.contains_key("_vid") => {
                                if let Some(Value::List(labels_arr)) = map.get("_labels") {
                                    labels_arr
                                        .iter()
                                        .any(|l| l.as_str() == Some(label_to_check))
                                } else {
                                    false
                                }
                            }
                            // Also handle legacy Object format
                            Value::Map(map) => {
                                if let Some(Value::List(labels_arr)) = map.get("_labels") {
                                    labels_arr
                                        .iter()
                                        .any(|l| l.as_str() == Some(label_to_check))
                                } else {
                                    false
                                }
                            }
                            _ => false,
                        };
                        return Ok(Value::Bool(has_label));
                    }

                    // Quantifier functions (ANY/ALL/NONE/SINGLE) as function calls are not supported.
                    // These should be parsed as Expr::Quantifier instead.
                    if matches!(
                        name.to_uppercase().as_str(),
                        "ANY" | "ALL" | "NONE" | "SINGLE"
                    ) {
                        return Err(anyhow!(
                            "{}() with list comprehensions is not yet supported. Use MATCH with WHERE instead.",
                            name.to_lowercase()
                        ));
                    }

                    // Special case: COALESCE needs short-circuit evaluation
                    if name.eq_ignore_ascii_case("COALESCE") {
                        for arg in args {
                            let val = this
                                .evaluate_expr(arg, row, prop_manager, params, ctx)
                                .await?;
                            if !val.is_null() {
                                return Ok(val);
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: vector_similarity has dedicated implementation
                    if name.eq_ignore_ascii_case("vector_similarity") {
                        if args.len() != 2 {
                            return Err(anyhow!("vector_similarity takes 2 arguments"));
                        }
                        let v1 = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        let v2 = this
                            .evaluate_expr(&args[1], row, prop_manager, params, ctx)
                            .await?;
                        return eval_vector_similarity(&v1, &v2);
                    }

                    // Special case: uni.validAt handles node fetching
                    if name.eq_ignore_ascii_case("uni.temporal.validAt")
                        || name.eq_ignore_ascii_case("uni.validAt")
                        || name.eq_ignore_ascii_case("validAt")
                    {
                        if args.len() != 4 {
                            return Err(anyhow!("validAt requires 4 arguments"));
                        }
                        let node_val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        let start_prop = this
                            .evaluate_expr(&args[1], row, prop_manager, params, ctx)
                            .await?
                            .as_str()
                            .ok_or(anyhow!("start_prop must be string"))?
                            .to_string();
                        let end_prop = this
                            .evaluate_expr(&args[2], row, prop_manager, params, ctx)
                            .await?
                            .as_str()
                            .ok_or(anyhow!("end_prop must be string"))?
                            .to_string();
                        let time_val = this
                            .evaluate_expr(&args[3], row, prop_manager, params, ctx)
                            .await?;

                        let query_time = value_to_datetime_utc(&time_val).ok_or_else(|| {
                            anyhow!("time argument must be a datetime value or string")
                        })?;

                        // Fetch temporal property values - supports both vertices and edges
                        let valid_from_val: Option<Value> = if let Ok(vid) =
                            Self::vid_from_value(&node_val)
                        {
                            // Vertex case - VID string format
                            prop_manager
                                .get_vertex_prop_with_ctx(vid, &start_prop, ctx)
                                .await
                                .ok()
                        } else if let Value::Map(map) = &node_val {
                            // Check for embedded _vid or _eid in object
                            if let Some(vid_val) = map.get("_vid").and_then(|v| v.as_u64()) {
                                let vid = Vid::from(vid_val);
                                prop_manager
                                    .get_vertex_prop_with_ctx(vid, &start_prop, ctx)
                                    .await
                                    .ok()
                            } else if let Some(eid_val) = map.get("_eid").and_then(|v| v.as_u64()) {
                                // Edge case
                                let eid = uni_common::core::id::Eid::from(eid_val);
                                prop_manager.get_edge_prop(eid, &start_prop, ctx).await.ok()
                            } else {
                                // Inline object - property embedded directly
                                map.get(&start_prop).cloned()
                            }
                        } else {
                            return Ok(Value::Bool(false));
                        };

                        let valid_from = match valid_from_val {
                            Some(ref v) => match value_to_datetime_utc(v) {
                                Some(dt) => dt,
                                None if v.is_null() => return Ok(Value::Bool(false)),
                                None => {
                                    return Err(anyhow!(
                                        "Property {} must be a datetime value or string",
                                        start_prop
                                    ));
                                }
                            },
                            None => return Ok(Value::Bool(false)),
                        };

                        let valid_to_val: Option<Value> = if let Ok(vid) =
                            Self::vid_from_value(&node_val)
                        {
                            // Vertex case - VID string format
                            prop_manager
                                .get_vertex_prop_with_ctx(vid, &end_prop, ctx)
                                .await
                                .ok()
                        } else if let Value::Map(map) = &node_val {
                            // Check for embedded _vid or _eid in object
                            if let Some(vid_val) = map.get("_vid").and_then(|v| v.as_u64()) {
                                let vid = Vid::from(vid_val);
                                prop_manager
                                    .get_vertex_prop_with_ctx(vid, &end_prop, ctx)
                                    .await
                                    .ok()
                            } else if let Some(eid_val) = map.get("_eid").and_then(|v| v.as_u64()) {
                                // Edge case
                                let eid = uni_common::core::id::Eid::from(eid_val);
                                prop_manager.get_edge_prop(eid, &end_prop, ctx).await.ok()
                            } else {
                                // Inline object - property embedded directly
                                map.get(&end_prop).cloned()
                            }
                        } else {
                            return Ok(Value::Bool(false));
                        };

                        let valid_to = match valid_to_val {
                            Some(ref v) => match value_to_datetime_utc(v) {
                                Some(dt) => Some(dt),
                                None if v.is_null() => None,
                                None => {
                                    return Err(anyhow!(
                                        "Property {} must be a datetime value or null",
                                        end_prop
                                    ));
                                }
                            },
                            None => None,
                        };

                        let is_valid = valid_from <= query_time
                            && valid_to.map(|vt| query_time < vt).unwrap_or(true);
                        return Ok(Value::Bool(is_valid));
                    }

                    // For all other functions, evaluate arguments then call helper
                    let mut evaluated_args = Vec::with_capacity(args.len());
                    for arg in args {
                        let mut val = this
                            .evaluate_expr(arg, row, prop_manager, params, ctx)
                            .await?;

                        // Eagerly hydrate edge/vertex maps if pushdown hydration didn't load properties.
                        // Functions like validAt() need access to properties like valid_from/valid_to.
                        if let Value::Map(ref mut map) = val {
                            hydrate_entity_if_needed(map, prop_manager, ctx).await;
                        }

                        evaluated_args.push(val);
                    }
                    eval_scalar_function(name, &evaluated_args)
                }
                Expr::Reduce {
                    accumulator,
                    init,
                    variable,
                    list,
                    expr,
                } => {
                    let mut acc = self
                        .evaluate_expr(init, row, prop_manager, params, ctx)
                        .await?;
                    let list_val = self
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;

                    if let Value::List(items) = list_val {
                        for item in items {
                            // Create a temporary scope/row with accumulator and variable
                            // For simplicity in fallback executor, we can construct a new row map
                            // merging current row + new variables.
                            let mut scope = row.clone();
                            scope.insert(accumulator.clone(), acc.clone());
                            scope.insert(variable.clone(), item);

                            acc = self
                                .evaluate_expr(expr, &scope, prop_manager, params, ctx)
                                .await?;
                        }
                    } else {
                        return Err(anyhow!("REDUCE list argument must evaluate to a list"));
                    }
                    Ok(acc)
                }
                Expr::ValidAt { .. } => {
                    // VALID_AT should have been transformed to a function call in the planner
                    Err(anyhow!(
                        "VALID_AT expression should have been transformed to function call in planner"
                    ))
                }

                Expr::LabelCheck { expr, labels } => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    match &val {
                        Value::Null => Ok(Value::Null),
                        Value::Map(map) => {
                            // Check if this is an edge (has _eid) or node (has _vid)
                            let is_edge = map.contains_key("_eid")
                                || map.contains_key("_type_name")
                                || (map.contains_key("_type") && !map.contains_key("_vid"));

                            if is_edge {
                                // Edges have a single type
                                if labels.len() > 1 {
                                    return Ok(Value::Bool(false));
                                }
                                let label_to_check = &labels[0];
                                let has_type = if let Some(Value::String(t)) = map.get("_type_name")
                                {
                                    t == label_to_check
                                } else if let Some(Value::String(t)) = map.get("_type") {
                                    t == label_to_check
                                } else {
                                    false
                                };
                                Ok(Value::Bool(has_type))
                            } else {
                                // Node: check all labels
                                let has_all = labels.iter().all(|label_to_check| {
                                    if let Some(Value::List(labels_arr)) = map.get("_labels") {
                                        labels_arr
                                            .iter()
                                            .any(|l| l.as_str() == Some(label_to_check.as_str()))
                                    } else {
                                        false
                                    }
                                });
                                Ok(Value::Bool(has_all))
                            }
                        }
                        _ => Ok(Value::Bool(false)),
                    }
                }

                Expr::MapProjection { base, items } => {
                    let base_value = this
                        .evaluate_expr(base, row, prop_manager, params, ctx)
                        .await?;

                    // Extract properties from the base object
                    let properties = match &base_value {
                        Value::Map(map) => map,
                        _ => {
                            return Err(anyhow!(
                                "Map projection requires object, got {:?}",
                                base_value
                            ));
                        }
                    };

                    let mut result_map = HashMap::new();

                    for item in items {
                        match item {
                            MapProjectionItem::Property(prop) => {
                                if let Some(value) = properties.get(prop.as_str()) {
                                    result_map.insert(prop.clone(), value.clone());
                                }
                            }
                            MapProjectionItem::AllProperties => {
                                // Include all properties except internal fields (those starting with _)
                                for (key, value) in properties.iter() {
                                    if !key.starts_with('_') {
                                        result_map.insert(key.clone(), value.clone());
                                    }
                                }
                            }
                            MapProjectionItem::LiteralEntry(key, expr) => {
                                let value = this
                                    .evaluate_expr(expr, row, prop_manager, params, ctx)
                                    .await?;
                                result_map.insert(key.clone(), value);
                            }
                            MapProjectionItem::Variable(var_name) => {
                                // Variable selector: include the value of the variable in the result
                                // e.g., person{.name, friend} includes the value of 'friend' variable
                                if let Some(value) = row.get(var_name.as_str()) {
                                    result_map.insert(var_name.clone(), value.clone());
                                }
                            }
                        }
                    }

                    Ok(Value::Map(result_map))
                }
            }
        })
    }

    pub(crate) fn execute_subplan<'a>(
        &'a self,
        plan: LogicalPlan,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> BoxFuture<'a, Result<Vec<HashMap<String, Value>>>> {
        Box::pin(async move {
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }
            match plan {
                LogicalPlan::Union { left, right, all } => {
                    self.execute_union(left, right, all, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::CreateVectorIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                        self.storage.lancedb_store_arc(),
                    );
                    idx_mgr.create_vector_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateFullTextIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                        self.storage.lancedb_store_arc(),
                    );
                    idx_mgr.create_fts_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateScalarIndex {
                    mut config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }

                    // Check for expression indexes - create generated columns
                    let mut modified_properties = Vec::new();

                    for prop in &config.properties {
                        // Heuristic: if contains '(' and ')', it's an expression
                        if prop.contains('(') && prop.contains(')') {
                            let gen_col = SchemaManager::generated_column_name(prop);

                            // Add generated property to schema
                            let sm = self.storage.schema_manager_arc();
                            if let Err(e) = sm.add_generated_property(
                                &config.label,
                                &gen_col,
                                DataType::String, // Default type for expressions
                                prop.clone(),
                            ) {
                                log::warn!("Failed to add generated property (might exist): {}", e);
                            }

                            modified_properties.push(gen_col);
                        } else {
                            // Simple property - use as-is
                            modified_properties.push(prop.clone());
                        }
                    }

                    config.properties = modified_properties;

                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                        self.storage.lancedb_store_arc(),
                    );
                    idx_mgr.create_scalar_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateJsonFtsIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                        self.storage.lancedb_store_arc(),
                    );
                    idx_mgr.create_json_fts_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::ShowDatabase => Ok(self.execute_show_database()),
                LogicalPlan::ShowConfig => Ok(self.execute_show_config()),
                LogicalPlan::ShowStatistics => self.execute_show_statistics().await,
                LogicalPlan::Vacuum => {
                    self.execute_vacuum().await?;
                    Ok(vec![])
                }
                LogicalPlan::Checkpoint => {
                    self.execute_checkpoint().await?;
                    Ok(vec![])
                }
                LogicalPlan::CopyTo {
                    label,
                    path,
                    format,
                    options,
                } => {
                    let count = self
                        .execute_copy_to(&label, &path, &format, &options)
                        .await?;
                    let mut result = HashMap::new();
                    result.insert("count".to_string(), Value::Int(count as i64));
                    Ok(vec![result])
                }
                LogicalPlan::CopyFrom {
                    label,
                    path,
                    format,
                    options,
                } => {
                    let count = self
                        .execute_copy_from(&label, &path, &format, &options)
                        .await?;
                    let mut result = HashMap::new();
                    result.insert("count".to_string(), Value::Int(count as i64));
                    Ok(vec![result])
                }
                LogicalPlan::CreateLabel(clause) => {
                    self.execute_create_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateEdgeType(clause) => {
                    self.execute_create_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::AlterLabel(clause) => {
                    self.execute_alter_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::AlterEdgeType(clause) => {
                    self.execute_alter_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropLabel(clause) => {
                    self.execute_drop_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropEdgeType(clause) => {
                    self.execute_drop_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateConstraint(clause) => {
                    self.execute_create_constraint(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropConstraint(clause) => {
                    self.execute_drop_constraint(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::ShowConstraints(clause) => Ok(self.execute_show_constraints(clause)),
                LogicalPlan::DropIndex { name, if_exists } => {
                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                        self.storage.lancedb_store_arc(),
                    );
                    match idx_mgr.drop_index(&name).await {
                        Ok(_) => Ok(vec![]),
                        Err(e) => {
                            if if_exists && e.to_string().contains("not found") {
                                Ok(vec![])
                            } else {
                                Err(e)
                            }
                        }
                    }
                }
                LogicalPlan::ShowIndexes { filter } => {
                    Ok(self.execute_show_indexes(filter.as_deref()))
                }
                LogicalPlan::Scan { .. } => {
                    unreachable!("Scan is handled by DataFusion engine")
                }
                LogicalPlan::ExtIdLookup { .. } => {
                    unreachable!("ExtIdLookup is handled by DataFusion engine")
                }
                LogicalPlan::ScanAll { .. } => {
                    unreachable!("ScanAll is handled by DataFusion engine")
                }
                LogicalPlan::ScanMainByLabels { .. } => {
                    unreachable!("ScanMainByLabels is handled by DataFusion engine")
                }
                LogicalPlan::Traverse { .. } => {
                    unreachable!("Traverse is handled by DataFusion engine")
                }
                LogicalPlan::TraverseMainByType { .. } => {
                    unreachable!("TraverseMainByType is handled by DataFusion engine")
                }
                LogicalPlan::Filter {
                    input,
                    predicate,
                    optional_variables,
                } => {
                    let input_matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;

                    tracing::debug!(
                        "Filter: Evaluating predicate {:?} on {} input rows, optional_vars={:?}",
                        predicate,
                        input_matches.len(),
                        optional_variables
                    );

                    // For OPTIONAL MATCH with WHERE: we need LEFT OUTER JOIN semantics.
                    // Group rows by non-optional variables, apply filter, and ensure
                    // at least one row per group (with NULLs if filter removes all).
                    if !optional_variables.is_empty() {
                        // Helper to check if a key belongs to an optional variable.
                        // Keys can be "var" or "var.field" (e.g., "m" or "m._vid").
                        let is_optional_key = |k: &str| -> bool {
                            optional_variables.contains(k)
                                || optional_variables
                                    .iter()
                                    .any(|var| k.starts_with(&format!("{}.", var)))
                        };

                        // Helper to check if a key is internal (should not affect grouping)
                        let is_internal_key =
                            |k: &str| -> bool { k.starts_with("__") || k.starts_with("_") };

                        // Compute the key (non-optional, non-internal variables) for grouping
                        let non_optional_vars: Vec<String> = input_matches
                            .first()
                            .map(|row| {
                                row.keys()
                                    .filter(|k| !is_optional_key(k) && !is_internal_key(k))
                                    .cloned()
                                    .collect()
                            })
                            .unwrap_or_default();

                        // Group rows by their non-optional variable values
                        let mut groups: std::collections::HashMap<
                            Vec<u8>,
                            Vec<HashMap<String, Value>>,
                        > = std::collections::HashMap::new();

                        for row in &input_matches {
                            // Create a key from non-optional variable values
                            let key: Vec<u8> = non_optional_vars
                                .iter()
                                .map(|var| {
                                    row.get(var).map(|v| format!("{:?}", v)).unwrap_or_default()
                                })
                                .collect::<Vec<_>>()
                                .join("|")
                                .into_bytes();

                            groups.entry(key).or_default().push(row.clone());
                        }

                        let mut filtered = Vec::new();
                        for (_key, group_rows) in groups {
                            let mut group_passed = Vec::new();

                            for row in &group_rows {
                                // If optional variables are already NULL, preserve the row
                                let has_null_optional = optional_variables.iter().any(|var| {
                                    // Check both "var" and "var._vid" style keys
                                    let direct_null =
                                        matches!(row.get(var), Some(Value::Null) | None);
                                    let prefixed_null = row
                                        .keys()
                                        .filter(|k| k.starts_with(&format!("{}.", var)))
                                        .any(|k| matches!(row.get(k), Some(Value::Null)));
                                    direct_null || prefixed_null
                                });

                                if has_null_optional {
                                    group_passed.push(row.clone());
                                    continue;
                                }

                                let res = self
                                    .evaluate_expr(&predicate, row, prop_manager, params, ctx)
                                    .await?;

                                if res.as_bool().unwrap_or(false) {
                                    group_passed.push(row.clone());
                                }
                            }

                            if group_passed.is_empty() {
                                // No rows passed - emit one row with NULLs for optional variables
                                // Use the first row's non-optional values as a template
                                if let Some(template) = group_rows.first() {
                                    let mut null_row = HashMap::new();
                                    for (k, v) in template {
                                        if is_optional_key(k) {
                                            null_row.insert(k.clone(), Value::Null);
                                        } else {
                                            null_row.insert(k.clone(), v.clone());
                                        }
                                    }
                                    filtered.push(null_row);
                                }
                            } else {
                                filtered.extend(group_passed);
                            }
                        }

                        tracing::debug!(
                            "Filter (OPTIONAL): {} input rows -> {} output rows",
                            input_matches.len(),
                            filtered.len()
                        );

                        return Ok(filtered);
                    }

                    // Standard filter for non-OPTIONAL MATCH
                    let mut filtered = Vec::new();
                    for row in input_matches.iter() {
                        let res = self
                            .evaluate_expr(&predicate, row, prop_manager, params, ctx)
                            .await?;

                        let passes = res.as_bool().unwrap_or(false);

                        if passes {
                            filtered.push(row.clone());
                        }
                    }

                    tracing::debug!(
                        "Filter: {} input rows -> {} output rows",
                        input_matches.len(),
                        filtered.len()
                    );

                    Ok(filtered)
                }
                LogicalPlan::ProcedureCall {
                    procedure_name,
                    arguments,
                    yield_items,
                } => {
                    let yield_names: Vec<String> =
                        yield_items.iter().map(|(n, _)| n.clone()).collect();
                    let results = self
                        .execute_procedure(
                            &procedure_name,
                            &arguments,
                            &yield_names,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await?;

                    // Handle aliasing
                    let mut aliased_results = Vec::with_capacity(results.len());
                    for mut row in results {
                        let mut new_row = row.clone();
                        for (name, alias) in &yield_items {
                            if let Some(a) = alias
                                && let Some(val) = row.remove(name)
                            {
                                new_row.remove(name);
                                new_row.insert(a.clone(), val);
                            }
                        }
                        aliased_results.push(new_row);
                    }
                    Ok(aliased_results)
                }
                LogicalPlan::VectorKnn { .. } => {
                    unreachable!("VectorKnn is handled by DataFusion engine")
                }
                LogicalPlan::InvertedIndexLookup { .. } => {
                    unreachable!("InvertedIndexLookup is handled by DataFusion engine")
                }
                LogicalPlan::Sort { input, order_by } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_sort(rows, &order_by, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Limit { input, skip, fetch } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let skip = skip.unwrap_or(0);
                    let take = fetch.unwrap_or(usize::MAX);
                    Ok(rows.into_iter().skip(skip).take(take).collect())
                }
                LogicalPlan::Aggregate {
                    input,
                    group_by,
                    aggregates,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_aggregate(rows, &group_by, &aggregates, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Window {
                    input,
                    window_exprs,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_window(rows, &window_exprs, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Project { input, projections } => {
                    let matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_project(matches, &projections, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Distinct { input } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let mut seen = std::collections::HashSet::new();
                    let mut result = Vec::new();
                    for row in rows {
                        let key = Self::canonical_row_key(&row);
                        if seen.insert(key) {
                            result.push(row);
                        }
                    }
                    Ok(result)
                }
                LogicalPlan::Unwind {
                    input,
                    expr,
                    variable,
                } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_unwind(input_rows, &expr, &variable, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Apply {
                    input,
                    subquery,
                    input_filter,
                } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_apply(
                        input_rows,
                        &subquery,
                        input_filter.as_ref(),
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::SubqueryCall { input, subquery } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    // Execute subquery for each input row (correlated)
                    // No input_filter for CALL { }
                    self.execute_apply(input_rows, &subquery, None, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::RecursiveCTE {
                    cte_name,
                    initial,
                    recursive,
                } => {
                    self.execute_recursive_cte(
                        &cte_name,
                        *initial,
                        *recursive,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::CrossJoin { left, right } => {
                    self.execute_cross_join(left, right, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Set { .. }
                | LogicalPlan::Remove { .. }
                | LogicalPlan::Merge { .. }
                | LogicalPlan::Create { .. }
                | LogicalPlan::CreateBatch { .. } => {
                    unreachable!("mutations are handled by DataFusion engine")
                }
                LogicalPlan::Delete { .. } => {
                    unreachable!("mutations are handled by DataFusion engine")
                }
                LogicalPlan::Begin => {
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        writer.begin_transaction()?;
                    } else {
                        return Err(anyhow!("Transaction requires a Writer"));
                    }
                    Ok(vec![HashMap::new()])
                }
                LogicalPlan::Commit => {
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        writer.commit_transaction().await?;
                    } else {
                        return Err(anyhow!("Transaction requires a Writer"));
                    }
                    Ok(vec![HashMap::new()])
                }
                LogicalPlan::Rollback => {
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        writer.rollback_transaction()?;
                    } else {
                        return Err(anyhow!("Transaction requires a Writer"));
                    }
                    Ok(vec![HashMap::new()])
                }
                LogicalPlan::Copy {
                    target,
                    source,
                    is_export,
                    options,
                } => {
                    if is_export {
                        self.execute_export(&target, &source, &options, prop_manager, ctx)
                            .await
                    } else {
                        self.execute_copy(&target, &source, &options, prop_manager)
                            .await
                    }
                }
                LogicalPlan::Backup {
                    destination,
                    options,
                } => self.execute_backup(&destination, &options).await,
                LogicalPlan::Explain { plan } => {
                    let plan_str = format!("{:#?}", plan);
                    let mut row = HashMap::new();
                    row.insert("plan".to_string(), Value::String(plan_str));
                    Ok(vec![row])
                }
                LogicalPlan::ShortestPath { .. } => {
                    unreachable!("ShortestPath is handled by DataFusion engine")
                }
                LogicalPlan::AllShortestPaths { .. } => {
                    unreachable!("AllShortestPaths is handled by DataFusion engine")
                }
                LogicalPlan::Foreach { .. } => {
                    unreachable!("mutations are handled by DataFusion engine")
                }
                LogicalPlan::Empty => Ok(vec![HashMap::new()]),
                LogicalPlan::BindZeroLengthPath { .. } => {
                    unreachable!("BindZeroLengthPath is handled by DataFusion engine")
                }
                LogicalPlan::BindPath { .. } => {
                    unreachable!("BindPath is handled by DataFusion engine")
                }
                LogicalPlan::QuantifiedPattern { .. } => {
                    unreachable!("QuantifiedPattern is handled by DataFusion engine")
                }
            }
        })
    }

    /// Execute a single plan from a FOREACH body with the given scope.
    ///
    /// Used by the DataFusion ForeachExec operator to delegate body clause
    /// execution back to the executor.
    pub(crate) async fn execute_foreach_body_plan(
        &self,
        plan: LogicalPlan,
        scope: &mut HashMap<String, Value>,
        writer: &mut uni_store::runtime::writer::Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        match plan {
            LogicalPlan::Set { items, .. } => {
                self.execute_set_items_locked(&items, scope, writer, prop_manager, params, ctx)
                    .await?;
            }
            LogicalPlan::Remove { items, .. } => {
                self.execute_remove_items_locked(&items, scope, writer, prop_manager, ctx)
                    .await?;
            }
            LogicalPlan::Delete { items, detach, .. } => {
                for expr in &items {
                    let val = self
                        .evaluate_expr(expr, scope, prop_manager, params, ctx)
                        .await?;
                    self.execute_delete_item_locked(&val, detach, writer)
                        .await?;
                }
            }
            LogicalPlan::Create { pattern, .. } => {
                self.execute_create_pattern(&pattern, scope, writer, prop_manager, params, ctx)
                    .await?;
            }
            LogicalPlan::CreateBatch { patterns, .. } => {
                for pattern in &patterns {
                    self.execute_create_pattern(pattern, scope, writer, prop_manager, params, ctx)
                        .await?;
                }
            }
            LogicalPlan::Merge {
                pattern,
                on_match: _,
                on_create,
                ..
            } => {
                self.execute_create_pattern(&pattern, scope, writer, prop_manager, params, ctx)
                    .await?;
                if let Some(on_create_clause) = on_create {
                    self.execute_set_items_locked(
                        &on_create_clause.items,
                        scope,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await?;
                }
            }
            LogicalPlan::Foreach {
                variable,
                list,
                body,
                ..
            } => {
                let list_val = self
                    .evaluate_expr(&list, scope, prop_manager, params, ctx)
                    .await?;
                let items = match list_val {
                    Value::List(arr) => arr,
                    Value::Null => return Ok(()),
                    _ => return Err(anyhow!("FOREACH requires a list")),
                };
                for item in items {
                    let mut nested_scope = scope.clone();
                    nested_scope.insert(variable.clone(), item);
                    for nested_plan in &body {
                        Box::pin(self.execute_foreach_body_plan(
                            nested_plan.clone(),
                            &mut nested_scope,
                            writer,
                            prop_manager,
                            params,
                            ctx,
                        ))
                        .await?;
                    }
                }
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported operation in FOREACH body: only SET, REMOVE, DELETE, CREATE, MERGE, and nested FOREACH are allowed"
                ));
            }
        }
        Ok(())
    }

    fn canonical_row_key(row: &HashMap<String, Value>) -> String {
        let mut pairs: Vec<_> = row.iter().collect();
        pairs.sort_by(|(lk, _), (rk, _)| lk.cmp(rk));

        pairs
            .into_iter()
            .map(|(k, v)| format!("{k}={}", Self::canonical_value_key(v)))
            .collect::<Vec<_>>()
            .join("|")
    }

    fn canonical_value_key(v: &Value) -> String {
        match v {
            Value::Null => "null".to_string(),
            Value::Bool(b) => format!("b:{b}"),
            Value::Int(i) => format!("n:{i}"),
            Value::Float(f) => {
                if f.is_nan() {
                    "nan".to_string()
                } else if f.is_infinite() {
                    if f.is_sign_positive() {
                        "inf:+".to_string()
                    } else {
                        "inf:-".to_string()
                    }
                } else if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                    format!("n:{}", *f as i64)
                } else {
                    format!("f:{f}")
                }
            }
            Value::String(s) => {
                if let Some(k) = Self::temporal_string_key(s) {
                    format!("temporal:{k}")
                } else {
                    format!("s:{s}")
                }
            }
            Value::Bytes(b) => format!("bytes:{:?}", b),
            Value::List(items) => format!(
                "list:[{}]",
                items
                    .iter()
                    .map(Self::canonical_value_key)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Value::Map(map) => {
                let mut pairs: Vec<_> = map.iter().collect();
                pairs.sort_by(|(lk, _), (rk, _)| lk.cmp(rk));
                format!(
                    "map:{{{}}}",
                    pairs
                        .into_iter()
                        .map(|(k, v)| format!("{k}:{}", Self::canonical_value_key(v)))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Value::Node(n) => {
                let mut labels = n.labels.clone();
                labels.sort();
                format!(
                    "node:{}:{}:{}",
                    n.vid.as_u64(),
                    labels.join(":"),
                    Self::canonical_value_key(&Value::Map(n.properties.clone()))
                )
            }
            Value::Edge(e) => format!(
                "edge:{}:{}:{}:{}:{}",
                e.eid.as_u64(),
                e.edge_type,
                e.src.as_u64(),
                e.dst.as_u64(),
                Self::canonical_value_key(&Value::Map(e.properties.clone()))
            ),
            Value::Path(p) => format!(
                "path:nodes=[{}];edges=[{}]",
                p.nodes
                    .iter()
                    .map(|n| Self::canonical_value_key(&Value::Node(n.clone())))
                    .collect::<Vec<_>>()
                    .join(","),
                p.edges
                    .iter()
                    .map(|e| Self::canonical_value_key(&Value::Edge(e.clone())))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Value::Vector(vs) => format!("vec:{:?}", vs),
            Value::Temporal(t) => format!("temporal:{}", Self::canonical_temporal_key(t)),
            _ => format!("{v:?}"),
        }
    }

    fn canonical_temporal_key(t: &uni_common::TemporalValue) -> String {
        match t {
            uni_common::TemporalValue::Date { days_since_epoch } => {
                format!("date:{days_since_epoch}")
            }
            uni_common::TemporalValue::LocalTime {
                nanos_since_midnight,
            } => format!("localtime:{nanos_since_midnight}"),
            uni_common::TemporalValue::Time {
                nanos_since_midnight,
                offset_seconds,
            } => {
                let utc_nanos = *nanos_since_midnight - (*offset_seconds as i64 * 1_000_000_000);
                format!("time:{utc_nanos}")
            }
            uni_common::TemporalValue::LocalDateTime { nanos_since_epoch } => {
                format!("localdatetime:{nanos_since_epoch}")
            }
            uni_common::TemporalValue::DateTime {
                nanos_since_epoch, ..
            } => format!("datetime:{nanos_since_epoch}"),
            uni_common::TemporalValue::Duration {
                months,
                days,
                nanos,
            } => format!("duration:{months}:{days}:{nanos}"),
        }
    }

    fn temporal_string_key(s: &str) -> Option<String> {
        let fn_name = match classify_temporal(s)? {
            uni_common::TemporalType::Date => "DATE",
            uni_common::TemporalType::LocalTime => "LOCALTIME",
            uni_common::TemporalType::Time => "TIME",
            uni_common::TemporalType::LocalDateTime => "LOCALDATETIME",
            uni_common::TemporalType::DateTime => "DATETIME",
            uni_common::TemporalType::Duration => "DURATION",
        };
        match eval_datetime_function(fn_name, &[Value::String(s.to_string())]).ok()? {
            Value::Temporal(tv) => Some(Self::canonical_temporal_key(&tv)),
            _ => None,
        }
    }

    /// Execute aggregate operation: GROUP BY + aggregate functions.
    /// Interval for timeout checks in aggregate loops.
    pub(crate) const AGGREGATE_TIMEOUT_CHECK_INTERVAL: usize = 1000;

    pub(crate) async fn execute_aggregate(
        &self,
        rows: Vec<HashMap<String, Value>>,
        group_by: &[Expr],
        aggregates: &[Expr],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before aggregation
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        let mut groups: HashMap<String, (Vec<Value>, Vec<Accumulator>)> = HashMap::new();

        // Cypher semantics: aggregation without grouping keys returns one row even
        // on empty input (e.g. `RETURN count(*)`, `RETURN avg(x)`).
        if rows.is_empty() {
            if group_by.is_empty() {
                let accs = Self::create_accumulators(aggregates);
                let row = Self::build_aggregate_result(group_by, aggregates, &[], &accs);
                return Ok(vec![row]);
            }
            return Ok(vec![]);
        }

        for (idx, row) in rows.into_iter().enumerate() {
            // Periodic timeout check during aggregation
            if idx.is_multiple_of(Self::AGGREGATE_TIMEOUT_CHECK_INTERVAL)
                && let Some(ctx) = ctx
            {
                ctx.check_timeout()?;
            }

            let key_vals = self
                .evaluate_group_keys(group_by, &row, prop_manager, params, ctx)
                .await?;
            // Build a canonical key so grouping follows Cypher value semantics
            // (e.g. temporal equality by instant, numeric normalization where applicable).
            let key_str = format!(
                "[{}]",
                key_vals
                    .iter()
                    .map(Self::canonical_value_key)
                    .collect::<Vec<_>>()
                    .join(",")
            );

            let entry = groups
                .entry(key_str)
                .or_insert_with(|| (key_vals, Self::create_accumulators(aggregates)));

            self.update_accumulators(&mut entry.1, aggregates, &row, prop_manager, params, ctx)
                .await?;
        }

        let results = groups
            .values()
            .map(|(k_vals, accs)| Self::build_aggregate_result(group_by, aggregates, k_vals, accs))
            .collect();

        Ok(results)
    }

    pub(crate) async fn execute_window(
        &self,
        mut rows: Vec<HashMap<String, Value>>,
        window_exprs: &[Expr],
        _prop_manager: &PropertyManager,
        _params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before window computation
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        // If no rows or no window expressions, return as-is
        if rows.is_empty() || window_exprs.is_empty() {
            return Ok(rows);
        }

        // Process each window function expression
        for window_expr in window_exprs {
            // Extract window function details
            let Expr::FunctionCall {
                name,
                args,
                window_spec: Some(window_spec),
                ..
            } = window_expr
            else {
                return Err(anyhow!(
                    "Window expression must be a FunctionCall with OVER clause: {:?}",
                    window_expr
                ));
            };

            let name_upper = name.to_uppercase();

            // Validate it's a supported window function
            if !WINDOW_FUNCTIONS.contains(&name_upper.as_str()) {
                return Err(anyhow!(
                    "Unsupported window function: {}. Supported functions: {}",
                    name,
                    WINDOW_FUNCTIONS.join(", ")
                ));
            }

            // Build partition groups based on PARTITION BY clause
            let mut partition_map: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();

            for (row_idx, row) in rows.iter().enumerate() {
                // Evaluate partition key
                let partition_key: Vec<Value> = if window_spec.partition_by.is_empty() {
                    // No partitioning - all rows in one partition
                    vec![]
                } else {
                    window_spec
                        .partition_by
                        .iter()
                        .map(|expr| self.evaluate_simple_expr(expr, row))
                        .collect::<Result<Vec<_>>>()?
                };

                partition_map
                    .entry(partition_key)
                    .or_default()
                    .push(row_idx);
            }

            // Process each partition
            for (_partition_key, row_indices) in partition_map.iter_mut() {
                // Sort rows within partition by ORDER BY clause
                if !window_spec.order_by.is_empty() {
                    row_indices.sort_by(|&a, &b| {
                        for sort_item in &window_spec.order_by {
                            let val_a = self.evaluate_simple_expr(&sort_item.expr, &rows[a]);
                            let val_b = self.evaluate_simple_expr(&sort_item.expr, &rows[b]);

                            if let (Ok(va), Ok(vb)) = (val_a, val_b) {
                                let cmp = Executor::compare_values(&va, &vb);
                                let cmp = if sort_item.ascending {
                                    cmp
                                } else {
                                    cmp.reverse()
                                };
                                if cmp != std::cmp::Ordering::Equal {
                                    return cmp;
                                }
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }

                // Compute window function values for this partition
                for (position, &row_idx) in row_indices.iter().enumerate() {
                    let window_value = match name_upper.as_str() {
                        "ROW_NUMBER" => Value::from((position + 1) as i64),
                        "RANK" => {
                            // RANK: position (1-indexed) of first row in group of tied rows
                            let rank = if position == 0 {
                                1i64
                            } else {
                                let prev_row_idx = row_indices[position - 1];
                                let same_as_prev = self.rows_have_same_sort_keys(
                                    &window_spec.order_by,
                                    &rows,
                                    row_idx,
                                    prev_row_idx,
                                );

                                if same_as_prev {
                                    // Walk backwards to find where this group started
                                    let mut group_start = position - 1;
                                    while group_start > 0 {
                                        let curr_idx = row_indices[group_start];
                                        let prev_idx = row_indices[group_start - 1];
                                        if !self.rows_have_same_sort_keys(
                                            &window_spec.order_by,
                                            &rows,
                                            curr_idx,
                                            prev_idx,
                                        ) {
                                            break;
                                        }
                                        group_start -= 1;
                                    }
                                    (group_start + 1) as i64
                                } else {
                                    (position + 1) as i64
                                }
                            };
                            Value::from(rank)
                        }
                        "DENSE_RANK" => {
                            // Dense rank: continuous ranking without gaps
                            let mut dense_rank = 1i64;
                            for i in 0..position {
                                let curr_idx = row_indices[i + 1];
                                let prev_idx = row_indices[i];
                                if !self.rows_have_same_sort_keys(
                                    &window_spec.order_by,
                                    &rows,
                                    curr_idx,
                                    prev_idx,
                                ) {
                                    dense_rank += 1;
                                }
                            }
                            Value::from(dense_rank)
                        }
                        "LAG" => {
                            let (value_expr, offset, default_value) =
                                self.extract_lag_lead_params("LAG", args, &rows[row_idx])?;

                            if position >= offset {
                                let target_idx = row_indices[position - offset];
                                self.evaluate_simple_expr(value_expr, &rows[target_idx])?
                            } else {
                                default_value
                            }
                        }
                        "LEAD" => {
                            let (value_expr, offset, default_value) =
                                self.extract_lag_lead_params("LEAD", args, &rows[row_idx])?;

                            if position + offset < row_indices.len() {
                                let target_idx = row_indices[position + offset];
                                self.evaluate_simple_expr(value_expr, &rows[target_idx])?
                            } else {
                                default_value
                            }
                        }
                        "NTILE" => {
                            // Extract num_buckets argument: NTILE(num_buckets)
                            let num_buckets_expr = args.first().ok_or_else(|| {
                                anyhow!("NTILE requires 1 argument: NTILE(num_buckets)")
                            })?;
                            let num_buckets_val =
                                self.evaluate_simple_expr(num_buckets_expr, &rows[row_idx])?;
                            let num_buckets = num_buckets_val.as_i64().ok_or_else(|| {
                                anyhow!(
                                    "NTILE argument must be an integer, got: {:?}",
                                    num_buckets_val
                                )
                            })?;

                            if num_buckets <= 0 {
                                return Err(anyhow!(
                                    "NTILE bucket count must be positive, got: {}",
                                    num_buckets
                                ));
                            }

                            let num_buckets = num_buckets as usize;
                            let partition_size = row_indices.len();

                            // Calculate bucket assignment using standard algorithm
                            // For N rows and B buckets:
                            // - Base size: N / B
                            // - Extra rows: N % B (go to first buckets)
                            let base_size = partition_size / num_buckets;
                            let extra_rows = partition_size % num_buckets;

                            // Determine bucket for current row
                            let bucket = if position < extra_rows * (base_size + 1) {
                                // Row is in one of the larger buckets (first 'extra_rows' buckets)
                                position / (base_size + 1) + 1
                            } else {
                                // Row is in one of the normal-sized buckets
                                let adjusted_position = position - extra_rows * (base_size + 1);
                                extra_rows + (adjusted_position / base_size) + 1
                            };

                            Value::from(bucket as i64)
                        }
                        "FIRST_VALUE" => {
                            // FIRST_VALUE returns the value of the expression from the first row in the window frame
                            let value_expr = args.first().ok_or_else(|| {
                                anyhow!("FIRST_VALUE requires 1 argument: FIRST_VALUE(expr)")
                            })?;

                            // Get the first row in the partition (after ordering)
                            if row_indices.is_empty() {
                                Value::Null
                            } else {
                                let first_idx = row_indices[0];
                                self.evaluate_simple_expr(value_expr, &rows[first_idx])?
                            }
                        }
                        "LAST_VALUE" => {
                            // LAST_VALUE returns the value of the expression from the last row in the window frame
                            let value_expr = args.first().ok_or_else(|| {
                                anyhow!("LAST_VALUE requires 1 argument: LAST_VALUE(expr)")
                            })?;

                            // Get the last row in the partition (after ordering)
                            if row_indices.is_empty() {
                                Value::Null
                            } else {
                                let last_idx = row_indices[row_indices.len() - 1];
                                self.evaluate_simple_expr(value_expr, &rows[last_idx])?
                            }
                        }
                        "NTH_VALUE" => {
                            // NTH_VALUE returns the value of the expression from the nth row in the window frame
                            if args.len() != 2 {
                                return Err(anyhow!(
                                    "NTH_VALUE requires 2 arguments: NTH_VALUE(expr, n)"
                                ));
                            }

                            let value_expr = &args[0];
                            let n_expr = &args[1];

                            let n_val = self.evaluate_simple_expr(n_expr, &rows[row_idx])?;
                            let n = n_val.as_i64().ok_or_else(|| {
                                anyhow!(
                                    "NTH_VALUE second argument must be an integer, got: {:?}",
                                    n_val
                                )
                            })?;

                            if n <= 0 {
                                return Err(anyhow!(
                                    "NTH_VALUE position must be positive, got: {}",
                                    n
                                ));
                            }

                            let nth_index = (n - 1) as usize; // Convert 1-based to 0-based
                            if nth_index < row_indices.len() {
                                let nth_idx = row_indices[nth_index];
                                self.evaluate_simple_expr(value_expr, &rows[nth_idx])?
                            } else {
                                Value::Null
                            }
                        }
                        _ => unreachable!("Window function {} already validated", name),
                    };

                    // Add window function result to row
                    // Use the window expression's string representation as the column name
                    let col_name = window_expr.to_string_repr();
                    rows[row_idx].insert(col_name, window_value);
                }
            }
        }

        Ok(rows)
    }

    /// Helper to evaluate simple expressions for window function sorting/partitioning.
    ///
    /// Uses `&self` for consistency with other evaluation methods, though it only
    /// recurses for property access.
    #[expect(clippy::only_used_in_recursion)]
    fn evaluate_simple_expr(&self, expr: &Expr, row: &HashMap<String, Value>) -> Result<Value> {
        match expr {
            Expr::Variable(name) => row
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow!("Variable not found: {}", name)),
            Expr::Property(base, prop) => {
                let base_val = self.evaluate_simple_expr(base, row)?;
                if let Value::Map(map) = base_val {
                    map.get(prop)
                        .cloned()
                        .ok_or_else(|| anyhow!("Property not found: {}", prop))
                } else {
                    Err(anyhow!("Cannot access property on non-object"))
                }
            }
            Expr::Literal(lit) => Ok(lit.to_value()),
            _ => Err(anyhow!(
                "Unsupported expression in window function: {:?}",
                expr
            )),
        }
    }

    /// Check if two rows have matching sort keys for ranking functions.
    fn rows_have_same_sort_keys(
        &self,
        order_by: &[uni_cypher::ast::SortItem],
        rows: &[HashMap<String, Value>],
        idx_a: usize,
        idx_b: usize,
    ) -> bool {
        order_by.iter().all(|sort_item| {
            let val_a = self.evaluate_simple_expr(&sort_item.expr, &rows[idx_a]);
            let val_b = self.evaluate_simple_expr(&sort_item.expr, &rows[idx_b]);
            matches!((val_a, val_b), (Ok(a), Ok(b)) if a == b)
        })
    }

    /// Extract offset and default value for LAG/LEAD window functions.
    fn extract_lag_lead_params<'a>(
        &self,
        func_name: &str,
        args: &'a [Expr],
        row: &HashMap<String, Value>,
    ) -> Result<(&'a Expr, usize, Value)> {
        let value_expr = args.first().ok_or_else(|| {
            anyhow!(
                "{} requires at least 1 argument: {}(expr [, offset [, default]])",
                func_name,
                func_name
            )
        })?;

        let offset = if let Some(offset_expr) = args.get(1) {
            let offset_val = self.evaluate_simple_expr(offset_expr, row)?;
            offset_val.as_i64().ok_or_else(|| {
                anyhow!(
                    "{} offset must be an integer, got: {:?}",
                    func_name,
                    offset_val
                )
            })? as usize
        } else {
            1
        };

        let default_value = if let Some(default_expr) = args.get(2) {
            self.evaluate_simple_expr(default_expr, row)?
        } else {
            Value::Null
        };

        Ok((value_expr, offset, default_value))
    }

    /// Evaluate group-by key expressions for a row.
    pub(crate) async fn evaluate_group_keys(
        &self,
        group_by: &[Expr],
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<Value>> {
        let mut key_vals = Vec::new();
        for expr in group_by {
            key_vals.push(
                self.evaluate_expr(expr, row, prop_manager, params, ctx)
                    .await?,
            );
        }
        Ok(key_vals)
    }

    /// Update accumulators with values from the current row.
    pub(crate) async fn update_accumulators(
        &self,
        accs: &mut [Accumulator],
        aggregates: &[Expr],
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        for (i, agg_expr) in aggregates.iter().enumerate() {
            if let Expr::FunctionCall { args, .. } = agg_expr {
                let is_wildcard = args.is_empty() || matches!(args[0], Expr::Wildcard);
                let val = if is_wildcard {
                    Value::Null
                } else {
                    self.evaluate_expr(&args[0], row, prop_manager, params, ctx)
                        .await?
                };
                accs[i].update(&val, is_wildcard);
            }
        }
        Ok(())
    }

    /// Execute sort operation with ORDER BY clauses.
    pub(crate) async fn execute_recursive_cte(
        &self,
        cte_name: &str,
        initial: LogicalPlan,
        recursive: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        use std::collections::HashSet;

        // Helper to create a stable key for cycle detection.
        // Uses sorted keys to ensure consistent ordering.
        pub(crate) fn row_key(row: &HashMap<String, Value>) -> String {
            let mut pairs: Vec<_> = row.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            format!("{:?}", pairs)
        }

        // 1. Execute Anchor
        let mut working_table = self
            .execute_subplan(initial, prop_manager, params, ctx)
            .await?;
        let mut result_table = working_table.clone();

        // Track seen rows for cycle detection
        let mut seen: HashSet<String> = working_table.iter().map(row_key).collect();

        // 2. Loop
        // Safety: Max iterations to prevent infinite loop
        // TODO: expose this via UniConfig for user control
        let max_iterations = 1000;
        for _iteration in 0..max_iterations {
            // CWE-400: Check timeout at each iteration to prevent resource exhaustion
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }

            if working_table.is_empty() {
                break;
            }

            // Bind working table to CTE name in params
            let working_val = Value::List(
                working_table
                    .iter()
                    .map(|row| {
                        if row.len() == 1 {
                            row.values().next().unwrap().clone()
                        } else {
                            Value::Map(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        }
                    })
                    .collect(),
            );

            let mut next_params = params.clone();
            next_params.insert(cte_name.to_string(), working_val);

            // Execute recursive part
            let next_result = self
                .execute_subplan(recursive.clone(), prop_manager, &next_params, ctx)
                .await?;

            if next_result.is_empty() {
                break;
            }

            // Filter out already-seen rows (cycle detection)
            let new_rows: Vec<_> = next_result
                .into_iter()
                .filter(|row| {
                    let key = row_key(row);
                    seen.insert(key) // Returns false if already present
                })
                .collect();

            if new_rows.is_empty() {
                // All results were cycles - terminate
                break;
            }

            result_table.extend(new_rows.clone());
            working_table = new_rows;
        }

        // Output accumulated results as a variable
        let final_list = Value::List(
            result_table
                .into_iter()
                .map(|row| {
                    // If the CTE returns a single column and we want to treat it as a list of values?
                    // E.g. WITH RECURSIVE r AS (RETURN 1 UNION RETURN 2) -> [1, 2] or [{expr:1}, {expr:2}]?
                    // Cypher LISTs usually contain values.
                    // If the row has 1 column, maybe unwrap?
                    // But SQL CTEs are tables.
                    // Let's stick to List<Map> for consistency with how we pass it in.
                    // UNLESS the user extracts it.
                    // My parser test `MATCH (n) WHERE n IN hierarchy` implies `hierarchy` contains Nodes.
                    // If `row` contains `root` (Node), then `hierarchy` should be `[Node, Node]`.
                    // If row has multiple cols, `[ {a:1, b:2}, ... ]`.
                    // If row has 1 col, users expect `[val, val]`.
                    if row.len() == 1 {
                        row.values().next().unwrap().clone()
                    } else {
                        Value::Map(row.into_iter().collect())
                    }
                })
                .collect(),
        );

        let mut final_row = HashMap::new();
        final_row.insert(cte_name.to_string(), final_list);
        Ok(vec![final_row])
    }

    /// Interval for timeout checks in sort loops.
    const SORT_TIMEOUT_CHECK_INTERVAL: usize = 1000;

    pub(crate) async fn execute_sort(
        &self,
        rows: Vec<HashMap<String, Value>>,
        order_by: &[uni_cypher::ast::SortItem],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before potentially expensive sort
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        let mut rows_with_keys = Vec::with_capacity(rows.len());
        for (idx, row) in rows.into_iter().enumerate() {
            // Periodic timeout check during key extraction
            if idx.is_multiple_of(Self::SORT_TIMEOUT_CHECK_INTERVAL)
                && let Some(ctx) = ctx
            {
                ctx.check_timeout()?;
            }

            let mut keys = Vec::new();
            for item in order_by {
                let val = row
                    .get(&item.expr.to_string_repr())
                    .cloned()
                    .unwrap_or(Value::Null);
                let val = if val.is_null() {
                    self.evaluate_expr(&item.expr, &row, prop_manager, params, ctx)
                        .await
                        .unwrap_or(Value::Null)
                } else {
                    val
                };
                keys.push(val);
            }
            rows_with_keys.push((row, keys));
        }

        // Check timeout again before synchronous sort (can't be interrupted)
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        rows_with_keys.sort_by(|a, b| Self::compare_sort_keys(&a.1, &b.1, order_by));

        Ok(rows_with_keys.into_iter().map(|(r, _)| r).collect())
    }

    /// Create accumulators for aggregate expressions.
    pub(crate) fn create_accumulators(aggregates: &[Expr]) -> Vec<Accumulator> {
        aggregates
            .iter()
            .map(|expr| {
                if let Expr::FunctionCall { name, distinct, .. } = expr {
                    Accumulator::new(name, *distinct)
                } else {
                    Accumulator::new("COUNT", false)
                }
            })
            .collect()
    }

    /// Build result row from group-by keys and accumulators.
    pub(crate) fn build_aggregate_result(
        group_by: &[Expr],
        aggregates: &[Expr],
        key_vals: &[Value],
        accs: &[Accumulator],
    ) -> HashMap<String, Value> {
        let mut res_row = HashMap::new();
        for (i, expr) in group_by.iter().enumerate() {
            res_row.insert(expr.to_string_repr(), key_vals[i].clone());
        }
        for (i, expr) in aggregates.iter().enumerate() {
            // Use aggregate_column_name to ensure consistency with planner
            let col_name = crate::query::planner::aggregate_column_name(expr);
            res_row.insert(col_name, accs[i].finish());
        }
        res_row
    }

    /// Compare and return ordering for sort operation.
    pub(crate) fn compare_sort_keys(
        a_keys: &[Value],
        b_keys: &[Value],
        order_by: &[uni_cypher::ast::SortItem],
    ) -> std::cmp::Ordering {
        for (i, item) in order_by.iter().enumerate() {
            let order = Self::compare_values(&a_keys[i], &b_keys[i]);
            if order != std::cmp::Ordering::Equal {
                return if item.ascending {
                    order
                } else {
                    order.reverse()
                };
            }
        }
        std::cmp::Ordering::Equal
    }

    /// Executes BACKUP command to local or cloud storage.
    ///
    /// Supports both local filesystem paths and cloud URLs (s3://, gs://, az://).
    pub(crate) async fn execute_backup(
        &self,
        destination: &str,
        _options: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // 1. Flush L0
        if let Some(writer_arc) = &self.writer {
            let mut writer = writer_arc.write().await;
            writer.flush_to_l1(None).await?;
        }

        // 2. Snapshot
        let snapshot_manager = self.storage.snapshot_manager();
        let snapshot = snapshot_manager
            .load_latest_snapshot()
            .await?
            .ok_or_else(|| anyhow!("No snapshot found"))?;

        // 3. Copy files - cloud or local path
        if is_cloud_url(destination) {
            self.backup_to_cloud(destination, &snapshot.snapshot_id)
                .await?;
        } else {
            // Validate local destination path against sandbox
            let validated_dest = self.validate_path(destination)?;
            self.backup_to_local(&validated_dest, &snapshot.snapshot_id)
                .await?;
        }

        let mut res = HashMap::new();
        res.insert(
            "status".to_string(),
            Value::String("Backup completed".to_string()),
        );
        res.insert(
            "snapshot_id".to_string(),
            Value::String(snapshot.snapshot_id),
        );
        Ok(vec![res])
    }

    /// Backs up database to a local filesystem destination.
    async fn backup_to_local(&self, dest_path: &std::path::Path, _snapshot_id: &str) -> Result<()> {
        let source_path = std::path::Path::new(self.storage.base_path());

        if !dest_path.exists() {
            std::fs::create_dir_all(dest_path)?;
        }

        // Recursive copy (local to local)
        if source_path.exists() {
            Self::copy_dir_all(source_path, dest_path)?;
        }

        // Copy schema to destination/catalog/schema.json
        let schema_manager = self.storage.schema_manager();
        let dest_catalog = dest_path.join("catalog");
        if !dest_catalog.exists() {
            std::fs::create_dir_all(&dest_catalog)?;
        }

        let schema_content = serde_json::to_string_pretty(&schema_manager.schema())?;
        std::fs::write(dest_catalog.join("schema.json"), schema_content)?;

        Ok(())
    }

    /// Backs up database to a cloud storage destination.
    ///
    /// Streams data from source to destination, supporting cross-cloud backups.
    async fn backup_to_cloud(&self, dest_url: &str, _snapshot_id: &str) -> Result<()> {
        use object_store::ObjectStore;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjPath;

        let (dest_store, dest_prefix) = build_store_from_url(dest_url)?;
        let source_path = std::path::Path::new(self.storage.base_path());

        // Create local store for source, coerced to dyn ObjectStore
        let src_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(source_path)?);

        // Copy catalog/ directory
        let catalog_src = ObjPath::from("catalog");
        let catalog_dst = if dest_prefix.as_ref().is_empty() {
            ObjPath::from("catalog")
        } else {
            ObjPath::from(format!("{}/catalog", dest_prefix.as_ref()))
        };
        copy_store_prefix(&src_store, &dest_store, &catalog_src, &catalog_dst).await?;

        // Copy storage/ directory
        let storage_src = ObjPath::from("storage");
        let storage_dst = if dest_prefix.as_ref().is_empty() {
            ObjPath::from("storage")
        } else {
            ObjPath::from(format!("{}/storage", dest_prefix.as_ref()))
        };
        copy_store_prefix(&src_store, &dest_store, &storage_src, &storage_dst).await?;

        // Copy schema.json
        let schema_manager = self.storage.schema_manager();
        let schema_content = serde_json::to_string_pretty(&schema_manager.schema())?;
        let schema_path = if dest_prefix.as_ref().is_empty() {
            ObjPath::from("schema.json")
        } else {
            ObjPath::from(format!("{}/schema.json", dest_prefix.as_ref()))
        };
        dest_store
            .put(&schema_path, bytes::Bytes::from(schema_content).into())
            .await?;

        Ok(())
    }

    /// Maximum directory depth for backup operations.
    ///
    /// **CWE-674 (Uncontrolled Recursion)**: Prevents stack overflow from
    /// excessively deep directory structures.
    const MAX_BACKUP_DEPTH: usize = 100;

    /// Maximum file count for backup operations.
    ///
    /// **CWE-400 (Resource Consumption)**: Prevents disk exhaustion and
    /// long-running operations from malicious or unexpectedly large directories.
    const MAX_BACKUP_FILES: usize = 100_000;

    /// Recursively copies a directory with security limits.
    ///
    /// # Security
    ///
    /// - **CWE-674**: Depth limit prevents stack overflow
    /// - **CWE-400**: File count limit prevents resource exhaustion
    /// - **Symlink handling**: Symlinks are skipped to prevent loop attacks
    pub(crate) fn copy_dir_all(
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> std::io::Result<()> {
        let mut file_count = 0usize;
        Self::copy_dir_all_impl(src, dst, 0, &mut file_count)
    }

    /// Internal implementation with depth and file count tracking.
    pub(crate) fn copy_dir_all_impl(
        src: &std::path::Path,
        dst: &std::path::Path,
        depth: usize,
        file_count: &mut usize,
    ) -> std::io::Result<()> {
        if depth >= Self::MAX_BACKUP_DEPTH {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Maximum backup depth {} exceeded at {:?}",
                    Self::MAX_BACKUP_DEPTH,
                    src
                ),
            ));
        }

        std::fs::create_dir_all(dst)?;

        for entry in std::fs::read_dir(src)? {
            if *file_count >= Self::MAX_BACKUP_FILES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Maximum backup file count {} exceeded",
                        Self::MAX_BACKUP_FILES
                    ),
                ));
            }
            *file_count += 1;

            let entry = entry?;
            let metadata = entry.metadata()?;

            // Skip symlinks to prevent loops and traversal attacks
            if metadata.file_type().is_symlink() {
                // Silently skip - logging would require tracing dependency
                continue;
            }

            let dst_path = dst.join(entry.file_name());
            if metadata.is_dir() {
                Self::copy_dir_all_impl(&entry.path(), &dst_path, depth + 1, file_count)?;
            } else {
                std::fs::copy(entry.path(), dst_path)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_copy(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let format = options
            .get("format")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                if source.ends_with(".parquet") {
                    "parquet"
                } else {
                    "csv"
                }
            });

        match format.to_lowercase().as_str() {
            "csv" => self.execute_csv_import(target, source, options).await,
            "parquet" => {
                self.execute_parquet_import(target, source, options, prop_manager)
                    .await
            }
            _ => Err(anyhow!("Unsupported format: {}", format)),
        }
    }

    pub(crate) async fn execute_csv_import(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Validate source path against sandbox
        let validated_source = self.validate_path(source)?;

        let writer_lock = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("COPY requires a Writer"))?;

        let schema = self.storage.schema_manager().schema();

        // 1. Determine if target is Label or EdgeType
        let label_meta = schema.labels.get(target);
        let edge_meta = schema.edge_types.get(target);

        if label_meta.is_none() && edge_meta.is_none() {
            return Err(anyhow!("Target '{}' not found in schema", target));
        }

        // 2. Open CSV
        let delimiter_str = options
            .get("delimiter")
            .and_then(|v| v.as_str())
            .unwrap_or(",");
        let delimiter = if delimiter_str.is_empty() {
            b','
        } else {
            delimiter_str.as_bytes()[0]
        };
        let has_header = options
            .get("header")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(has_header)
            .from_path(&validated_source)?;

        let headers = rdr.headers()?.clone();
        let mut count = 0;

        let mut writer = writer_lock.write().await;

        if label_meta.is_some() {
            let target_props = schema
                .properties
                .get(target)
                .ok_or_else(|| anyhow!("Properties for label '{}' not found", target))?;

            for result in rdr.records() {
                let record = result?;
                let mut props = HashMap::new();

                for (i, header) in headers.iter().enumerate() {
                    if let Some(val_str) = record.get(i)
                        && let Some(prop_meta) = target_props.get(header)
                    {
                        let val = self.parse_csv_value(val_str, &prop_meta.r#type, header)?;
                        props.insert(header.to_string(), val);
                    }
                }

                let vid = writer.next_vid().await?;
                writer
                    .insert_vertex_with_labels(vid, props, vec![target.to_string()])
                    .await?;
                count += 1;
            }
        } else if let Some(meta) = edge_meta {
            let type_id = meta.id;
            let target_props = schema
                .properties
                .get(target)
                .ok_or_else(|| anyhow!("Properties for edge type '{}' not found", target))?;

            // For edges, we need src and dst VIDs.
            // Expecting columns '_src' and '_dst' or as specified in options.
            let src_col = options
                .get("src_col")
                .and_then(|v| v.as_str())
                .unwrap_or("_src");
            let dst_col = options
                .get("dst_col")
                .and_then(|v| v.as_str())
                .unwrap_or("_dst");

            for result in rdr.records() {
                let record = result?;
                let mut props = HashMap::new();
                let mut src_vid = None;
                let mut dst_vid = None;

                for (i, header) in headers.iter().enumerate() {
                    if let Some(val_str) = record.get(i) {
                        if header == src_col {
                            src_vid =
                                Some(Self::vid_from_value(&Value::String(val_str.to_string()))?);
                        } else if header == dst_col {
                            dst_vid =
                                Some(Self::vid_from_value(&Value::String(val_str.to_string()))?);
                        } else if let Some(prop_meta) = target_props.get(header) {
                            let val = self.parse_csv_value(val_str, &prop_meta.r#type, header)?;
                            props.insert(header.to_string(), val);
                        }
                    }
                }

                let src =
                    src_vid.ok_or_else(|| anyhow!("Missing source VID in column '{}'", src_col))?;
                let dst = dst_vid
                    .ok_or_else(|| anyhow!("Missing destination VID in column '{}'", dst_col))?;

                let eid = writer.next_eid(type_id).await?;
                writer.insert_edge(src, dst, type_id, eid, props).await?;
                count += 1;
            }
        }

        let mut res = HashMap::new();
        res.insert("count".to_string(), Value::Int(count as i64));
        Ok(vec![res])
    }

    /// Imports data from Parquet file to a label or edge type.
    ///
    /// Supports local filesystem and cloud URLs (s3://, gs://, az://).
    pub(crate) async fn execute_parquet_import(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
        _prop_manager: &PropertyManager,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let writer_lock = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("COPY requires a Writer"))?;

        let schema = self.storage.schema_manager().schema();

        // 1. Determine if target is Label or EdgeType
        let label_meta = schema.labels.get(target);
        let edge_meta = schema.edge_types.get(target);

        if label_meta.is_none() && edge_meta.is_none() {
            return Err(anyhow!("Target '{}' not found in schema", target));
        }

        // 2. Open Parquet - support both local and cloud URLs
        let reader = if is_cloud_url(source) {
            self.open_parquet_from_cloud(source).await?
        } else {
            // Validate local source path against sandbox
            let validated_source = self.validate_path(source)?;
            let file = std::fs::File::open(&validated_source)?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
            builder.build()?
        };
        let mut reader = reader;

        let mut count = 0;
        let mut writer = writer_lock.write().await;

        if label_meta.is_some() {
            let target_props = schema
                .properties
                .get(target)
                .ok_or_else(|| anyhow!("Properties for label '{}' not found", target))?;

            for batch in reader.by_ref() {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    let mut props = HashMap::new();
                    for field in batch.schema().fields() {
                        let name = field.name();
                        if target_props.contains_key(name) {
                            let col = batch.column_by_name(name).unwrap();
                            if !col.is_null(row) {
                                // Look up Uni DataType from schema for proper DateTime/Time decoding
                                let data_type = target_props.get(name).map(|pm| &pm.r#type);
                                let val =
                                    arrow_convert::arrow_to_value(col.as_ref(), row, data_type);
                                props.insert(name.clone(), val);
                            }
                        }
                    }
                    let vid = writer.next_vid().await?;
                    writer
                        .insert_vertex_with_labels(vid, props, vec![target.to_string()])
                        .await?;
                    count += 1;
                }
            }
        } else if let Some(meta) = edge_meta {
            let type_id = meta.id;
            let target_props = schema
                .properties
                .get(target)
                .ok_or_else(|| anyhow!("Properties for edge type '{}' not found", target))?;

            let src_col = options
                .get("src_col")
                .and_then(|v| v.as_str())
                .unwrap_or("_src");
            let dst_col = options
                .get("dst_col")
                .and_then(|v| v.as_str())
                .unwrap_or("_dst");

            for batch in reader {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    let mut props = HashMap::new();
                    let mut src_vid = None;
                    let mut dst_vid = None;

                    for field in batch.schema().fields() {
                        let name = field.name();
                        let col = batch.column_by_name(name).unwrap();
                        if col.is_null(row) {
                            continue;
                        }

                        if name == src_col {
                            let val = Self::arrow_to_value(col.as_ref(), row);
                            src_vid = Some(Self::vid_from_value(&val)?);
                        } else if name == dst_col {
                            let val = Self::arrow_to_value(col.as_ref(), row);
                            dst_vid = Some(Self::vid_from_value(&val)?);
                        } else if let Some(pm) = target_props.get(name) {
                            // Look up Uni DataType from schema for proper DateTime/Time decoding
                            let val =
                                arrow_convert::arrow_to_value(col.as_ref(), row, Some(&pm.r#type));
                            props.insert(name.clone(), val);
                        }
                    }

                    let src = src_vid
                        .ok_or_else(|| anyhow!("Missing source VID in column '{}'", src_col))?;
                    let dst = dst_vid.ok_or_else(|| {
                        anyhow!("Missing destination VID in column '{}'", dst_col)
                    })?;

                    let eid = writer.next_eid(type_id).await?;
                    writer.insert_edge(src, dst, type_id, eid, props).await?;
                    count += 1;
                }
            }
        }

        let mut res = HashMap::new();
        res.insert("count".to_string(), Value::Int(count as i64));
        Ok(vec![res])
    }

    /// Opens a Parquet file from a cloud URL.
    ///
    /// Downloads the file to memory and creates a Parquet reader.
    async fn open_parquet_from_cloud(
        &self,
        source_url: &str,
    ) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader> {
        use object_store::ObjectStore;

        let (store, path) = build_store_from_url(source_url)?;

        // Download file contents
        let bytes = store.get(&path).await?.bytes().await?;

        // Create a Parquet reader from the bytes
        let reader = bytes::Bytes::from(bytes.to_vec());
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(reader)?;
        Ok(builder.build()?)
    }

    pub(crate) async fn scan_edge_type(
        &self,
        edge_type: &str,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<(uni_common::core::id::Eid, Vid, Vid)>> {
        let mut edges: HashMap<uni_common::core::id::Eid, (Vid, Vid)> = HashMap::new();

        // 1. Scan L2 (Base)
        self.scan_edge_type_l2(edge_type, &mut edges).await?;

        // 2. Scan L1 (Delta)
        self.scan_edge_type_l1(edge_type, &mut edges).await?;

        // 3. Scan L0 (Memory) and filter tombstoned vertices
        if let Some(ctx) = ctx {
            self.scan_edge_type_l0(edge_type, ctx, &mut edges);
            self.filter_tombstoned_vertex_edges(ctx, &mut edges);
        }

        Ok(edges
            .into_iter()
            .map(|(eid, (src, dst))| (eid, src, dst))
            .collect())
    }

    /// Scan L2 (base) storage for edges of a given type.
    ///
    /// Note: Edges are now stored exclusively in delta datasets (L1) via LanceDB.
    /// This L2 scan will typically find no data.
    pub(crate) async fn scan_edge_type_l2(
        &self,
        _edge_type: &str,
        _edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) -> Result<()> {
        // Edges are now stored in delta datasets (L1) via LanceDB.
        // Legacy L2 base edge storage is no longer used.
        Ok(())
    }

    /// Scan L1 (delta) storage for edges of a given type.
    pub(crate) async fn scan_edge_type_l1(
        &self,
        edge_type: &str,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) -> Result<()> {
        use futures::TryStreamExt;
        use lancedb::query::{ExecutableQuery, QueryBase, Select};

        if let Ok(ds) = self.storage.delta_dataset(edge_type, "fwd") {
            let lancedb_store = self.storage.lancedb_store();
            if let Ok(table) = ds.open_lancedb(lancedb_store).await {
                let query = table.query().select(Select::Columns(vec![
                    "eid".into(),
                    "src_vid".into(),
                    "dst_vid".into(),
                    "op".into(),
                    "_version".into(),
                ]));

                if let Ok(stream) = query.execute().await {
                    let batches: Vec<arrow_array::RecordBatch> =
                        stream.try_collect().await.unwrap_or_default();

                    // Collect ops with versions: eid -> (version, op, src, dst)
                    let mut versioned_ops: HashMap<uni_common::core::id::Eid, (u64, u8, Vid, Vid)> =
                        HashMap::new();

                    for batch in batches {
                        self.process_delta_batch(&batch, &mut versioned_ops)?;
                    }

                    // Apply the winning ops
                    for (eid, (_, op, src, dst)) in versioned_ops {
                        if op == 0 {
                            edges.insert(eid, (src, dst));
                        } else if op == 1 {
                            edges.remove(&eid);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Process a delta batch, tracking versioned operations.
    pub(crate) fn process_delta_batch(
        &self,
        batch: &arrow_array::RecordBatch,
        versioned_ops: &mut HashMap<uni_common::core::id::Eid, (u64, u8, Vid, Vid)>,
    ) -> Result<()> {
        use arrow_array::UInt64Array;
        let eid_col = batch
            .column_by_name("eid")
            .ok_or(anyhow!("Missing eid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid eid"))?;
        let src_col = batch
            .column_by_name("src_vid")
            .ok_or(anyhow!("Missing src_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid src_vid"))?;
        let dst_col = batch
            .column_by_name("dst_vid")
            .ok_or(anyhow!("Missing dst_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid dst_vid"))?;
        let op_col = batch
            .column_by_name("op")
            .ok_or(anyhow!("Missing op"))?
            .as_any()
            .downcast_ref::<arrow_array::UInt8Array>()
            .ok_or(anyhow!("Invalid op"))?;
        let version_col = batch
            .column_by_name("_version")
            .ok_or(anyhow!("Missing _version"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid _version"))?;

        for i in 0..batch.num_rows() {
            let eid = uni_common::core::id::Eid::from(eid_col.value(i));
            let version = version_col.value(i);
            let op = op_col.value(i);
            let src = Vid::from(src_col.value(i));
            let dst = Vid::from(dst_col.value(i));

            match versioned_ops.entry(eid) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert((version, op, src, dst));
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if version > e.get().0 {
                        e.insert((version, op, src, dst));
                    }
                }
            }
        }
        Ok(())
    }

    /// Scan L0 (memory) buffers for edges of a given type.
    pub(crate) fn scan_edge_type_l0(
        &self,
        edge_type: &str,
        ctx: &QueryContext,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) {
        let schema = self.storage.schema_manager().schema();
        let type_id = schema.edge_types.get(edge_type).map(|m| m.id);

        if let Some(type_id) = type_id {
            // Main L0
            self.scan_single_l0(&ctx.l0.read(), type_id, edges);

            // Transaction L0
            if let Some(tx_l0_arc) = &ctx.transaction_l0 {
                self.scan_single_l0(&tx_l0_arc.read(), type_id, edges);
            }

            // Pending flush L0s
            for pending_l0_arc in &ctx.pending_flush_l0s {
                self.scan_single_l0(&pending_l0_arc.read(), type_id, edges);
            }
        }
    }

    /// Scan a single L0 buffer for edges and apply tombstones.
    pub(crate) fn scan_single_l0(
        &self,
        l0: &uni_store::runtime::L0Buffer,
        type_id: u32,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) {
        for edge_entry in l0.graph.edges() {
            if edge_entry.edge_type == type_id {
                edges.insert(edge_entry.eid, (edge_entry.src_vid, edge_entry.dst_vid));
            }
        }
        // Process Tombstones
        let eids_to_check: Vec<_> = edges.keys().cloned().collect();
        for eid in eids_to_check {
            if l0.is_tombstoned(eid) {
                edges.remove(&eid);
            }
        }
    }

    /// Filter out edges connected to tombstoned vertices.
    pub(crate) fn filter_tombstoned_vertex_edges(
        &self,
        ctx: &QueryContext,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) {
        let l0 = ctx.l0.read();
        let mut all_vertex_tombstones = l0.vertex_tombstones.clone();

        // Include tx_l0 vertex tombstones if present
        if let Some(tx_l0_arc) = &ctx.transaction_l0 {
            let tx_l0 = tx_l0_arc.read();
            all_vertex_tombstones.extend(tx_l0.vertex_tombstones.iter().cloned());
        }

        // Include pending flush L0 vertex tombstones
        for pending_l0_arc in &ctx.pending_flush_l0s {
            let pending_l0 = pending_l0_arc.read();
            all_vertex_tombstones.extend(pending_l0.vertex_tombstones.iter().cloned());
        }

        edges.retain(|_, (src, dst)| {
            !all_vertex_tombstones.contains(src) && !all_vertex_tombstones.contains(dst)
        });
    }

    /// Execute a projection operation.
    pub(crate) async fn execute_project(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        projections: &[(Expr, Option<String>)],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut results = Vec::new();
        for m in input_rows {
            let mut row = HashMap::new();
            for (expr, alias) in projections {
                let val = self
                    .evaluate_expr(expr, &m, prop_manager, params, ctx)
                    .await?;
                let name = alias.clone().unwrap_or_else(|| expr.to_string_repr());
                row.insert(name, val);
            }
            results.push(row);
        }
        Ok(results)
    }

    /// Execute an UNWIND operation.
    pub(crate) async fn execute_unwind(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        expr: &Expr,
        variable: &str,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut results = Vec::new();
        for row in input_rows {
            let val = self
                .evaluate_expr(expr, &row, prop_manager, params, ctx)
                .await?;
            if let Value::List(items) = val {
                for item in items {
                    let mut new_row = row.clone();
                    new_row.insert(variable.to_string(), item);
                    results.push(new_row);
                }
            }
        }
        Ok(results)
    }

    /// Execute an APPLY (correlated subquery) operation.
    pub(crate) async fn execute_apply(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        subquery: &LogicalPlan,
        input_filter: Option<&Expr>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut filtered_rows = input_rows;

        if let Some(filter) = input_filter {
            let mut filtered = Vec::new();
            for row in filtered_rows {
                let res = self
                    .evaluate_expr(filter, &row, prop_manager, params, ctx)
                    .await?;
                if res.as_bool().unwrap_or(false) {
                    filtered.push(row);
                }
            }
            filtered_rows = filtered;
        }

        // Handle empty input: execute subquery once with empty context
        // This is critical for standalone CALL statements at the beginning of a query
        if filtered_rows.is_empty() {
            let sub_rows = self
                .execute_subplan(subquery.clone(), prop_manager, params, ctx)
                .await?;
            return Ok(sub_rows);
        }

        let mut results = Vec::new();
        for row in filtered_rows {
            let mut sub_params = params.clone();
            sub_params.extend(row.clone());

            let sub_rows = self
                .execute_subplan(subquery.clone(), prop_manager, &sub_params, ctx)
                .await?;

            for sub_row in sub_rows {
                let mut new_row = row.clone();
                new_row.extend(sub_row);
                results.push(new_row);
            }
        }
        Ok(results)
    }

    /// Execute SHOW INDEXES command.
    pub(crate) fn execute_show_indexes(&self, filter: Option<&str>) -> Vec<HashMap<String, Value>> {
        let schema = self.storage.schema_manager().schema();
        let mut rows = Vec::new();
        for idx in schema.indexes {
            let (name, type_str, details) = match idx {
                uni_common::core::schema::IndexDefinition::Vector(c) => (
                    c.name,
                    "VECTOR",
                    format!("{:?} on {}.{}", c.index_type, c.label, c.property),
                ),
                uni_common::core::schema::IndexDefinition::FullText(c) => (
                    c.name,
                    "FULLTEXT",
                    format!("on {}:{:?}", c.label, c.properties),
                ),
                uni_common::core::schema::IndexDefinition::Scalar(cfg) => (
                    cfg.name.clone(),
                    "SCALAR",
                    format!(":{}({:?})", cfg.label, cfg.properties),
                ),
                _ => ("UNKNOWN".to_string(), "UNKNOWN", "".to_string()),
            };

            if let Some(f) = filter
                && f != type_str
            {
                continue;
            }

            let mut row = HashMap::new();
            row.insert("name".to_string(), Value::String(name));
            row.insert("type".to_string(), Value::String(type_str.to_string()));
            row.insert("details".to_string(), Value::String(details));
            rows.push(row);
        }
        rows
    }

    pub(crate) fn execute_show_database(&self) -> Vec<HashMap<String, Value>> {
        let mut row = HashMap::new();
        row.insert("name".to_string(), Value::String("uni".to_string()));
        // Could add storage path, etc.
        vec![row]
    }

    pub(crate) fn execute_show_config(&self) -> Vec<HashMap<String, Value>> {
        // Placeholder as we don't easy access to config struct from here
        vec![]
    }

    pub(crate) async fn execute_show_statistics(&self) -> Result<Vec<HashMap<String, Value>>> {
        let snapshot = self
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?;
        let mut results = Vec::new();

        if let Some(snap) = snapshot {
            for (label, s) in &snap.vertices {
                let mut row = HashMap::new();
                row.insert("type".to_string(), Value::String("Label".to_string()));
                row.insert("name".to_string(), Value::String(label.clone()));
                row.insert("count".to_string(), Value::Int(s.count as i64));
                results.push(row);
            }
            for (edge, s) in &snap.edges {
                let mut row = HashMap::new();
                row.insert("type".to_string(), Value::String("Edge".to_string()));
                row.insert("name".to_string(), Value::String(edge.clone()));
                row.insert("count".to_string(), Value::Int(s.count as i64));
                results.push(row);
            }
        }

        Ok(results)
    }

    pub(crate) fn execute_show_constraints(
        &self,
        clause: ShowConstraints,
    ) -> Vec<HashMap<String, Value>> {
        let schema = self.storage.schema_manager().schema();
        let mut rows = Vec::new();
        for c in &schema.constraints {
            if let Some(target) = &clause.target {
                match (target, &c.target) {
                    (AstConstraintTarget::Label(l1), ConstraintTarget::Label(l2)) if l1 == l2 => {}
                    (AstConstraintTarget::EdgeType(e1), ConstraintTarget::EdgeType(e2))
                        if e1 == e2 => {}
                    _ => continue,
                }
            }

            let mut row = HashMap::new();
            row.insert("name".to_string(), Value::String(c.name.clone()));
            let type_str = match c.constraint_type {
                ConstraintType::Unique { .. } => "UNIQUE",
                ConstraintType::Exists { .. } => "EXISTS",
                ConstraintType::Check { .. } => "CHECK",
                _ => "UNKNOWN",
            };
            row.insert("type".to_string(), Value::String(type_str.to_string()));

            let target_str = match &c.target {
                ConstraintTarget::Label(l) => format!("(:{})", l),
                ConstraintTarget::EdgeType(e) => format!("[:{}]", e),
                _ => "UNKNOWN".to_string(),
            };
            row.insert("target".to_string(), Value::String(target_str));

            rows.push(row);
        }
        rows
    }

    /// Execute a MERGE operation.
    pub(crate) async fn execute_cross_join(
        &self,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let left_rows = self
            .execute_subplan(*left, prop_manager, params, ctx)
            .await?;
        let right_rows = self
            .execute_subplan(*right, prop_manager, params, ctx)
            .await?;

        let mut results = Vec::new();
        for l in &left_rows {
            for r in &right_rows {
                let mut combined = l.clone();
                combined.extend(r.clone());
                results.push(combined);
            }
        }
        Ok(results)
    }

    /// Execute a UNION operation with optional deduplication.
    pub(crate) async fn execute_union(
        &self,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut left_rows = self
            .execute_subplan(*left, prop_manager, params, ctx)
            .await?;
        let mut right_rows = self
            .execute_subplan(*right, prop_manager, params, ctx)
            .await?;

        left_rows.append(&mut right_rows);

        if !all {
            let mut seen = HashSet::new();
            left_rows.retain(|row| {
                let sorted_row: std::collections::BTreeMap<_, _> = row.iter().collect();
                let key = format!("{:?}", sorted_row);
                seen.insert(key)
            });
        }
        Ok(left_rows)
    }

    /// Check if an index with the given name exists.
    pub(crate) fn index_exists_by_name(&self, name: &str) -> bool {
        let schema = self.storage.schema_manager().schema();
        schema.indexes.iter().any(|idx| match idx {
            uni_common::core::schema::IndexDefinition::Vector(c) => c.name == name,
            uni_common::core::schema::IndexDefinition::FullText(c) => c.name == name,
            uni_common::core::schema::IndexDefinition::Scalar(c) => c.name == name,
            _ => false,
        })
    }

    pub(crate) async fn execute_export(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let format = options
            .get("format")
            .and_then(|v| v.as_str())
            .unwrap_or("csv")
            .to_lowercase();

        match format.as_str() {
            "csv" => {
                self.execute_csv_export(target, source, options, prop_manager, ctx)
                    .await
            }
            "parquet" => {
                self.execute_parquet_export(target, source, options, prop_manager, ctx)
                    .await
            }
            _ => Err(anyhow!("Unsupported export format: {}", format)),
        }
    }

    pub(crate) async fn execute_csv_export(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Validate destination path against sandbox
        let validated_dest = self.validate_path(source)?;

        let schema = self.storage.schema_manager().schema();
        let label_meta = schema.labels.get(target);
        let edge_meta = schema.edge_types.get(target);

        if label_meta.is_none() && edge_meta.is_none() {
            return Err(anyhow!("Target '{}' not found in schema", target));
        }

        let delimiter_str = options
            .get("delimiter")
            .and_then(|v| v.as_str())
            .unwrap_or(",");
        let delimiter = if delimiter_str.is_empty() {
            b','
        } else {
            delimiter_str.as_bytes()[0]
        };
        let has_header = options
            .get("header")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let mut wtr = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .from_path(&validated_dest)?;

        let mut count = 0;
        // Empty properties map for labels/edge types without registered properties
        let empty_props = HashMap::new();

        if let Some(meta) = label_meta {
            let label_id = meta.id;
            let props_meta = schema.properties.get(target).unwrap_or(&empty_props);
            let mut prop_names: Vec<_> = props_meta.keys().cloned().collect();
            prop_names.sort();

            let mut headers = vec!["_vid".to_string()];
            headers.extend(prop_names.clone());

            if has_header {
                wtr.write_record(&headers)?;
            }

            let vids = self
                .scan_label_with_filter(label_id, "n", None, ctx, prop_manager, &HashMap::new())
                .await?;

            for vid in vids {
                let props = prop_manager
                    .get_all_vertex_props_with_ctx(vid, ctx)
                    .await?
                    .unwrap_or_default();

                let mut row = Vec::with_capacity(headers.len());
                row.push(vid.to_string());
                for p_name in &prop_names {
                    let val = props.get(p_name).cloned().unwrap_or(Value::Null);
                    row.push(self.format_csv_value(val));
                }
                wtr.write_record(&row)?;
                count += 1;
            }
        } else if let Some(meta) = edge_meta {
            let props_meta = schema.properties.get(target).unwrap_or(&empty_props);
            let mut prop_names: Vec<_> = props_meta.keys().cloned().collect();
            prop_names.sort();

            // Headers for Edge: _eid, _src, _dst, _type, ...props
            let mut headers = vec![
                "_eid".to_string(),
                "_src".to_string(),
                "_dst".to_string(),
                "_type".to_string(),
            ];
            headers.extend(prop_names.clone());

            if has_header {
                wtr.write_record(&headers)?;
            }

            let edges = self.scan_edge_type(target, ctx).await?;

            for (eid, src, dst) in edges {
                let props = prop_manager
                    .get_all_edge_props_with_ctx(eid, ctx)
                    .await?
                    .unwrap_or_default();

                let mut row = Vec::with_capacity(headers.len());
                row.push(eid.to_string());
                row.push(src.to_string());
                row.push(dst.to_string());
                row.push(meta.id.to_string());

                for p_name in &prop_names {
                    let val = props.get(p_name).cloned().unwrap_or(Value::Null);
                    row.push(self.format_csv_value(val));
                }
                wtr.write_record(&row)?;
                count += 1;
            }
        }

        wtr.flush()?;
        let mut res = HashMap::new();
        res.insert("count".to_string(), Value::Int(count as i64));
        Ok(vec![res])
    }

    /// Exports data to Parquet format.
    ///
    /// Supports local filesystem and cloud URLs (s3://, gs://, az://).
    pub(crate) async fn execute_parquet_export(
        &self,
        target: &str,
        destination: &str,
        _options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let schema_manager = self.storage.schema_manager();
        let schema = schema_manager.schema();
        let label_meta = schema.labels.get(target);
        let edge_meta = schema.edge_types.get(target);

        if label_meta.is_none() && edge_meta.is_none() {
            return Err(anyhow!("Target '{}' not found in schema", target));
        }

        let arrow_schema = if label_meta.is_some() {
            let dataset = self.storage.vertex_dataset(target)?;
            dataset.get_arrow_schema(&schema)?
        } else {
            // Edge Schema
            let dataset = self.storage.edge_dataset(target, "", "")?;
            dataset.get_arrow_schema(&schema)?
        };

        let mut rows: Vec<HashMap<String, uni_common::Value>> = Vec::new();

        if let Some(meta) = label_meta {
            let label_id = meta.id;
            let vids = self
                .scan_label_with_filter(label_id, "n", None, ctx, prop_manager, &HashMap::new())
                .await?;

            for vid in vids {
                let mut props = prop_manager
                    .get_all_vertex_props_with_ctx(vid, ctx)
                    .await?
                    .unwrap_or_default();

                props.insert(
                    "_vid".to_string(),
                    uni_common::Value::Int(vid.as_u64() as i64),
                );
                if !props.contains_key("_uid") {
                    props.insert(
                        "_uid".to_string(),
                        uni_common::Value::List(vec![uni_common::Value::Int(0); 32]),
                    );
                }
                props.insert("_deleted".to_string(), uni_common::Value::Bool(false));
                props.insert("_version".to_string(), uni_common::Value::Int(1));
                rows.push(props);
            }
        } else if edge_meta.is_some() {
            let edges = self.scan_edge_type(target, ctx).await?;
            for (eid, src, dst) in edges {
                let mut props = prop_manager
                    .get_all_edge_props_with_ctx(eid, ctx)
                    .await?
                    .unwrap_or_default();

                props.insert(
                    "eid".to_string(),
                    uni_common::Value::Int(eid.as_u64() as i64),
                );
                props.insert(
                    "src_vid".to_string(),
                    uni_common::Value::Int(src.as_u64() as i64),
                );
                props.insert(
                    "dst_vid".to_string(),
                    uni_common::Value::Int(dst.as_u64() as i64),
                );
                props.insert("_deleted".to_string(), uni_common::Value::Bool(false));
                props.insert("_version".to_string(), uni_common::Value::Int(1));
                rows.push(props);
            }
        }

        // Write to cloud or local file
        if is_cloud_url(destination) {
            self.write_parquet_to_cloud(destination, &rows, &arrow_schema)
                .await?;
        } else {
            // Validate local destination path against sandbox
            let validated_dest = self.validate_path(destination)?;
            let file = std::fs::File::create(&validated_dest)?;
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(file, arrow_schema.clone(), None)?;

            // Write all in one batch for now (simplification)
            if !rows.is_empty() {
                let batch = self.rows_to_batch(&rows, &arrow_schema)?;
                writer.write(&batch)?;
            }

            writer.close()?;
        }

        let mut res = HashMap::new();
        res.insert("count".to_string(), Value::Int(rows.len() as i64));
        Ok(vec![res])
    }

    /// Writes Parquet data to a cloud storage destination.
    async fn write_parquet_to_cloud(
        &self,
        dest_url: &str,
        rows: &[HashMap<String, uni_common::Value>],
        arrow_schema: &arrow_schema::Schema,
    ) -> Result<()> {
        use object_store::ObjectStore;

        let (store, path) = build_store_from_url(dest_url)?;

        // Write to an in-memory buffer
        let mut buffer = Vec::new();
        {
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                &mut buffer,
                Arc::new(arrow_schema.clone()),
                None,
            )?;

            if !rows.is_empty() {
                let batch = self.rows_to_batch(rows, arrow_schema)?;
                writer.write(&batch)?;
            }

            writer.close()?;
        }

        // Upload to cloud storage
        store.put(&path, bytes::Bytes::from(buffer).into()).await?;

        Ok(())
    }

    pub(crate) fn rows_to_batch(
        &self,
        rows: &[HashMap<String, uni_common::Value>],
        schema: &arrow_schema::Schema,
    ) -> Result<RecordBatch> {
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();

        for field in schema.fields() {
            let name = field.name();
            let dt = field.data_type();

            let values: Vec<uni_common::Value> = rows
                .iter()
                .map(|row| row.get(name).cloned().unwrap_or(uni_common::Value::Null))
                .collect();
            let array = self.values_to_array(&values, dt)?;
            columns.push(array);
        }

        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }

    /// Convert a slice of Values to an Arrow array.
    /// Delegates to the shared implementation in arrow_convert module.
    pub(crate) fn values_to_array(
        &self,
        values: &[uni_common::Value],
        dt: &arrow_schema::DataType,
    ) -> Result<Arc<dyn Array>> {
        arrow_convert::values_to_array(values, dt)
    }

    pub(crate) fn format_csv_value(&self, val: Value) -> String {
        match val {
            Value::Null => "".to_string(),
            Value::String(s) => s,
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => format!("{}", val),
        }
    }

    pub(crate) fn parse_csv_value(
        &self,
        s: &str,
        data_type: &uni_common::core::schema::DataType,
        prop_name: &str,
    ) -> Result<Value> {
        if s.is_empty() || s.to_lowercase() == "null" {
            return Ok(Value::Null);
        }

        use uni_common::core::schema::DataType;
        match data_type {
            DataType::String => Ok(Value::String(s.to_string())),
            DataType::Int32 | DataType::Int64 => {
                let i = s.parse::<i64>().map_err(|_| {
                    anyhow!(
                        "Failed to parse integer for property '{}': {}",
                        prop_name,
                        s
                    )
                })?;
                Ok(Value::Int(i))
            }
            DataType::Float32 | DataType::Float64 => {
                let f = s.parse::<f64>().map_err(|_| {
                    anyhow!("Failed to parse float for property '{}': {}", prop_name, s)
                })?;
                Ok(Value::Float(f))
            }
            DataType::Bool => {
                let b = s.to_lowercase().parse::<bool>().map_err(|_| {
                    anyhow!(
                        "Failed to parse boolean for property '{}': {}",
                        prop_name,
                        s
                    )
                })?;
                Ok(Value::Bool(b))
            }
            DataType::CypherValue => {
                let json_val: serde_json::Value = serde_json::from_str(s).map_err(|_| {
                    anyhow!("Failed to parse JSON for property '{}': {}", prop_name, s)
                })?;
                Ok(Value::from(json_val))
            }
            DataType::Vector { .. } => {
                let v: Vec<f32> = serde_json::from_str(s).map_err(|_| {
                    anyhow!("Failed to parse Vector for property '{}': {}", prop_name, s)
                })?;
                Ok(Value::Vector(v))
            }
            _ => Ok(Value::String(s.to_string())),
        }
    }

    pub(crate) async fn detach_delete_vertex(&self, vid: Vid, writer: &mut Writer) -> Result<()> {
        let schema = self.storage.schema_manager().schema();
        let edge_type_ids: Vec<u32> = schema.all_edge_type_ids();

        // 1. Find and delete all outgoing edges
        let out_graph = self
            .storage
            .load_subgraph_cached(
                &[vid],
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Outgoing,
                Some(writer.l0_manager.get_current()),
            )
            .await?;

        for edge in out_graph.edges() {
            writer
                .delete_edge(edge.eid, edge.src_vid, edge.dst_vid, edge.edge_type)
                .await?;
        }

        // 2. Find and delete all incoming edges
        let in_graph = self
            .storage
            .load_subgraph_cached(
                &[vid],
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Incoming,
                Some(writer.l0_manager.get_current()),
            )
            .await?;

        for edge in in_graph.edges() {
            writer
                .delete_edge(edge.eid, edge.src_vid, edge.dst_vid, edge.edge_type)
                .await?;
        }

        Ok(())
    }

    /// Batch detach-delete: load subgraphs for all VIDs at once, then delete edges and vertices.
    pub(crate) async fn batch_detach_delete_vertices(
        &self,
        vids: &[Vid],
        labels_per_vid: Vec<Option<Vec<String>>>,
        writer: &mut Writer,
    ) -> Result<()> {
        let schema = self.storage.schema_manager().schema();
        let edge_type_ids: Vec<u32> = schema.all_edge_type_ids();

        // Load outgoing subgraph for all VIDs in one call.
        let out_graph = self
            .storage
            .load_subgraph_cached(
                vids,
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Outgoing,
                Some(writer.l0_manager.get_current()),
            )
            .await?;

        for edge in out_graph.edges() {
            writer
                .delete_edge(edge.eid, edge.src_vid, edge.dst_vid, edge.edge_type)
                .await?;
        }

        // Load incoming subgraph for all VIDs in one call.
        let in_graph = self
            .storage
            .load_subgraph_cached(
                vids,
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Incoming,
                Some(writer.l0_manager.get_current()),
            )
            .await?;

        for edge in in_graph.edges() {
            writer
                .delete_edge(edge.eid, edge.src_vid, edge.dst_vid, edge.edge_type)
                .await?;
        }

        // Delete all vertices.
        for (vid, labels) in vids.iter().zip(labels_per_vid) {
            writer.delete_vertex(*vid, labels).await?;
        }

        Ok(())
    }
}
