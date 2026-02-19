// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::WINDOW_FUNCTIONS;
use crate::query::datetime::{classify_temporal, eval_datetime_function, parse_datetime_utc};
use crate::query::expr_eval::{
    eval_binary_op, eval_in_op, eval_scalar_function, eval_vector_similarity,
};
use crate::query::planner::{LogicalPlan, QueryPlanner, classify_window_expressions};
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
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{ConstraintTarget, ConstraintType, DataType, SchemaManager};
use uni_cypher::ast::{
    BinaryOp, ConstraintTarget as AstConstraintTarget, Direction, Expr, MapProjectionItem,
    Quantifier, ShowConstraints, UnaryOp,
};
use uni_store::QueryContext;
use uni_store::cloud::{build_store_from_url, copy_store_prefix, is_cloud_url};
use uni_store::runtime::l0_visibility;
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

    /// Scan all vertices from main table (schemaless MATCH (n)).
    ///
    /// Returns VIDs from the main vertices table combined with L0 buffers.
    async fn scan_all_vertices(
        &self,
        variable: &str,
        filter: Option<&Expr>,
        ctx: Option<&QueryContext>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Vid>> {
        use uni_store::storage::main_vertex::MainVertexDataset;

        // Get VIDs from main table
        let lancedb = self.storage.lancedb_store();
        let mut candidates = MainVertexDataset::find_all_vids(lancedb).await?;

        // Add VIDs from L0 buffers
        candidates.extend(collect_l0_vids(ctx, |l0| l0.all_vertex_vids()));

        self.verify_and_filter_candidates(candidates, variable, filter, ctx, prop_manager, params)
            .await
    }

    /// Scan main table for vertices with a specific label (schemaless unknown label).
    ///
    /// Returns VIDs where the labels array contains the given label name.
    async fn scan_main_by_label(
        &self,
        label_name: &str,
        variable: &str,
        filter: Option<&Expr>,
        ctx: Option<&QueryContext>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Vid>> {
        use uni_store::storage::main_vertex::MainVertexDataset;

        // Get VIDs from main table filtered by label
        let lancedb = self.storage.lancedb_store();
        let mut candidates =
            MainVertexDataset::find_vids_by_label_name(lancedb, label_name).await?;

        // Add VIDs from L0 buffers that have this label
        candidates.extend(collect_l0_vids(ctx, |l0| l0.vids_for_label(label_name)));

        self.verify_and_filter_candidates(candidates, variable, filter, ctx, prop_manager, params)
            .await
    }

    /// Scan vertices that have ALL the specified labels (intersection semantics).
    pub(crate) async fn scan_multi_labels_with_filter(
        &self,
        labels: &[String],
        variable: &str,
        filter: Option<&Expr>,
        ctx: Option<&QueryContext>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Vid>> {
        if labels.is_empty() {
            return Ok(Vec::new());
        }

        // Build candidate set from storage (main vertices table) with label intersection
        let mut candidates = Vec::new();
        let lancedb_store = self.storage.lancedb_store();

        // Scan the main vertices table with label filter
        if let Ok(table) =
            uni_store::storage::main_vertex::MainVertexDataset::open_table(lancedb_store).await
        {
            use arrow_array::UInt64Array;
            use futures::TryStreamExt;
            use lancedb::query::{ExecutableQuery, QueryBase, Select};

            let mut query = table.query();

            // Build SQL filter for multi-label intersection:
            // array_contains(labels, 'Person') AND array_contains(labels, 'Employee')
            let mut label_filters = Vec::new();
            for label in labels {
                label_filters.push(format!(
                    "array_contains(labels, '{}')",
                    label.replace('\'', "''")
                ));
            }
            let sql_filter = label_filters.join(" AND ");
            query = query.only_if(sql_filter);

            // Project to only _vid
            let query = query.select(Select::columns(&["_vid"]));
            match query.execute().await {
                Ok(stream) => {
                    let batches: Vec<arrow_array::RecordBatch> = stream.try_collect().await?;
                    for batch in batches {
                        if let Some(vid_col) = batch.column_by_name("_vid")
                            && let Some(vid_arr) = vid_col.as_any().downcast_ref::<UInt64Array>()
                        {
                            for i in 0..batch.num_rows() {
                                candidates.push(Vid::from(vid_arr.value(i)));
                            }
                        }
                    }
                }
                Err(e) => {
                    // Only treat "not found" / "does not exist" errors as empty results
                    let err_msg = e.to_string().to_lowercase();
                    if !err_msg.contains("not found")
                        && !err_msg.contains("does not exist")
                        && !err_msg.contains("no such file")
                        && !err_msg.contains("object not found")
                    {
                        return Err(anyhow::anyhow!(
                            "Failed to query main vertices table: {}",
                            e
                        ));
                    }
                }
            }
        }

        // Overlay L0 buffer intersection
        {
            let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
            candidates.extend(collect_l0_vids(ctx, |l0| {
                l0.vids_with_all_labels(&label_refs)
            }));
        }

        self.verify_and_filter_candidates(candidates, variable, filter, ctx, prop_manager, params)
            .await
    }

    pub(crate) fn vid_from_value(val: &Value) -> Result<Vid> {
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
            Arc::new(self.storage.schema_manager().schema().clone()),
            params.clone(),
        );

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
                    if field.metadata().get("cv_encoded") == Some(&"true".to_string())
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
                let bare_vars: Vec<String> = row
                    .keys()
                    .filter(|k| !k.contains('.') && matches!(row.get(*k), Some(Value::Map(_))))
                    .cloned()
                    .collect();

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

            // Route DDL/Admin, MERGE, FOREACH, and complex mutation queries to the
            // fallback executor. Simple "terminal" mutations (single mutation clause at
            // the outermost level, no RETURN/WITH) flow through DataFusion via MutationExec
            // operators. Complex mutations (multi-clause, or with downstream RETURN/WITH)
            // still use the fallback path for correct variable flow and output semantics.
            let mutation_config = ctx
                .as_ref()
                .map(|c| &c.mutation_path)
                .unwrap_or(&self.config.mutation_path);
            let res = if Self::is_ddl_or_admin(&plan)
                || Self::contains_merge_or_foreach(&plan)
                || Self::needs_mutation_fallback(&plan)
                || Self::mutation_clause_disabled_by_config(&plan, mutation_config)
            {
                if Self::contains_write_operations(&plan) {
                    tracing::debug!(
                        plan_type = query_type,
                        needs_fallback = Self::needs_mutation_fallback(&plan),
                        clause_disabled =
                            Self::mutation_clause_disabled_by_config(&plan, mutation_config),
                        "Mutation routed to fallback executor"
                    );
                }
                self.execute_subplan(plan, prop_manager, params, ctx.as_ref())
                    .await
            } else if Self::contains_window_functions(&plan) {
                // Extract window expressions from the plan (may be wrapped in Sort/Limit/etc)
                let window_exprs = Self::extract_window_expressions(&plan);

                if !window_exprs.is_empty() {
                    // Classify window functions before routing
                    let (manual_exprs, df_exprs) = classify_window_expressions(&window_exprs);

                    if !df_exprs.is_empty() && !manual_exprs.is_empty() {
                        // Mixed: not yet supported
                        return Err(anyhow!(
                            "Queries with both aggregate and manual window functions not yet supported"
                        ));
                    } else if !df_exprs.is_empty() {
                        // Aggregate windows: MUST use DataFusion
                        let batches = self
                            .execute_datafusion(plan.clone(), prop_manager, params)
                            .await?;
                        self.record_batches_to_rows(batches)
                    } else {
                        // Manual windows only: use fallback executor
                        self.execute_subplan(plan, prop_manager, params, ctx.as_ref())
                            .await
                    }
                } else {
                    // No window functions found - use DataFusion
                    let batches = self
                        .execute_datafusion(plan.clone(), prop_manager, params)
                        .await?;
                    self.record_batches_to_rows(batches)
                }
            } else if Self::contains_sort(&plan)
                && !Self::contains_pattern_comprehension(&plan)
                && !Self::needs_datafusion_for_step_indexing(&plan)
            {
                // Route non-window ORDER BY through fallback executor for Cypher
                // ordering semantics (mixed-type precedence, list lexicographic,
                // NaN/null handling) until DataFusion parity is complete.
                self.execute_subplan(plan, prop_manager, params, ctx.as_ref())
                    .await
            } else {
                // Execute using DataFusion engine
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
    /// (e.g., `contains_sort`, `contains_write_operations`) can delegate the
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
            | LogicalPlan::Explain { .. }
            | LogicalPlan::LoadCsv { .. }
            | LogicalPlan::ProcedureCall { .. } => true,

            // Recurse through single-input wrapper nodes
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

    /// Check if a plan contains MERGE or FOREACH operations.
    ///
    /// Only these two write operations stay on the fallback path. All other
    /// mutations (CREATE, SET, REMOVE, DELETE) now flow through DataFusion
    /// via MutationExec operators.
    fn contains_merge_or_foreach(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Merge { .. } | LogicalPlan::Foreach { .. } => true,
            _ => Self::plan_children(plan)
                .iter()
                .any(|child| Self::contains_merge_or_foreach(child)),
        }
    }

    /// Check if a mutation query needs the fallback executor path.
    ///
    /// Returns true when mutations are "complex" — i.e., the DF mutation operators
    /// can't produce correct output because:
    /// 1. The mutation is wrapped by RETURN/WITH (needs updated values or created variables
    ///    in the output batch, which requires complex row→batch Struct reconstruction)
    /// 2. The mutation has nested mutations (multi-clause CREATE needs variable flow
    ///    between clauses, which requires created variables in intermediate output)
    /// 3. The plan contains LOAD CSV (not yet supported in DF engine)
    ///
    /// Simple "terminal" mutations (outermost plan is a single mutation clause,
    /// input is a read plan) are safe for DF: the mutation is a storage-level side
    /// effect and the output batches (passed through from the read operators) aren't
    /// consumed for their mutation-affected values.
    fn needs_mutation_fallback(plan: &LogicalPlan) -> bool {
        if !Self::contains_write_operations(plan) {
            return false;
        }

        // Check for LOAD CSV anywhere in the plan
        if Self::contains_load_csv(plan) {
            return true;
        }

        // If the outermost plan is NOT a mutation (e.g., it's RETURN, WITH, Sort wrapping
        // a mutation), the downstream consumer needs correct output from the mutation.
        // The DF path passes through original input batches unchanged, so RETURN/WITH
        // would see stale values. Route to fallback.
        if !Self::is_mutation_plan(plan) {
            return true;
        }

        // If the mutation's input contains other mutations (multi-clause CREATE, SET+SET, etc.),
        // variable flow between clauses requires proper output schemas. Route to fallback.
        Self::has_nested_mutations(plan)
    }

    /// Check if the outermost plan node is a mutation clause.
    fn is_mutation_plan(plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Create { .. }
                | LogicalPlan::CreateBatch { .. }
                | LogicalPlan::Set { .. }
                | LogicalPlan::Remove { .. }
                | LogicalPlan::Delete { .. }
        )
    }

    /// Check if a mutation plan has nested mutations in its input.
    ///
    /// This detects multi-clause mutation patterns like:
    /// `CREATE (a) CREATE (a)-[:R]->(:B)` → Create { input: Create { ... } }
    fn has_nested_mutations(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Create { input, .. }
            | LogicalPlan::CreateBatch { input, .. }
            | LogicalPlan::Set { input, .. }
            | LogicalPlan::Remove { input, .. }
            | LogicalPlan::Delete { input, .. } => Self::contains_write_operations(input),
            _ => false,
        }
    }

    /// Check if a plan contains LOAD CSV anywhere in the tree.
    fn contains_load_csv(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::LoadCsv { .. } => true,
            // Foreach has a body of plans — check body members too
            LogicalPlan::Foreach { body, .. } => {
                Self::plan_children(plan)
                    .iter()
                    .any(|child| Self::contains_load_csv(child))
                    || body.iter().any(Self::contains_load_csv)
            }
            _ => Self::plan_children(plan)
                .iter()
                .any(|child| Self::contains_load_csv(child)),
        }
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

    /// Check if the outermost mutation clause is disabled via `MutationPathConfig`.
    ///
    /// Returns `true` when the plan's mutation clause has been gated off in the config,
    /// forcing the query to the fallback executor. Non-mutation plans always return `false`.
    fn mutation_clause_disabled_by_config(
        plan: &LogicalPlan,
        config: &uni_common::config::MutationPathConfig,
    ) -> bool {
        use uni_common::config::MutationClause;
        match plan {
            LogicalPlan::Create { .. } | LogicalPlan::CreateBatch { .. } => {
                !config.is_clause_enabled(MutationClause::Create)
            }
            LogicalPlan::Set { .. } => !config.is_clause_enabled(MutationClause::Set),
            LogicalPlan::Remove { .. } => !config.is_clause_enabled(MutationClause::Remove),
            LogicalPlan::Delete { .. } => !config.is_clause_enabled(MutationClause::Delete),
            LogicalPlan::Merge { .. } => !config.is_clause_enabled(MutationClause::Merge),
            _ => false,
        }
    }

    /// Check if a logical plan contains window functions (may be nested in Sort/Limit/etc).
    fn contains_window_functions(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Window { .. } => true,
            LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Project { input, .. } => Self::contains_window_functions(input),
            _ => false,
        }
    }

    /// Check if a logical plan contains ORDER BY (Sort) anywhere in the tree.
    fn contains_sort(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Sort { .. } => true,
            _ => Self::plan_children(plan)
                .iter()
                .any(|child| Self::contains_sort(child)),
        }
    }

    fn expr_contains_pattern_comprehension(expr: &Expr) -> bool {
        match expr {
            Expr::PatternComprehension { .. } => true,
            Expr::Property(base, _) | Expr::UnaryOp { expr: base, .. } => {
                Self::expr_contains_pattern_comprehension(base)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_pattern_comprehension(left)
                    || Self::expr_contains_pattern_comprehension(right)
            }
            Expr::FunctionCall { args, .. } | Expr::List(args) => {
                args.iter().any(Self::expr_contains_pattern_comprehension)
            }
            Expr::Map(entries) => entries
                .iter()
                .any(|(_, v)| Self::expr_contains_pattern_comprehension(v)),
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                expr.as_ref()
                    .is_some_and(|e| Self::expr_contains_pattern_comprehension(e.as_ref()))
                    || when_then.iter().any(|(w, t)| {
                        Self::expr_contains_pattern_comprehension(w)
                            || Self::expr_contains_pattern_comprehension(t)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| Self::expr_contains_pattern_comprehension(e.as_ref()))
            }
            _ => false,
        }
    }

    /// Detect pattern comprehensions, which are DataFusion-only in the current fallback engine.
    fn contains_pattern_comprehension(plan: &LogicalPlan) -> bool {
        // Check expressions in nodes that carry them
        let has_expr = match plan {
            LogicalPlan::Filter { predicate, .. } => {
                Self::expr_contains_pattern_comprehension(predicate)
            }
            LogicalPlan::Project { projections, .. } => projections
                .iter()
                .any(|(e, _)| Self::expr_contains_pattern_comprehension(e)),
            LogicalPlan::Sort { order_by, .. } => order_by
                .iter()
                .any(|s| Self::expr_contains_pattern_comprehension(&s.expr)),
            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                group_by
                    .iter()
                    .any(Self::expr_contains_pattern_comprehension)
                    || aggregates
                        .iter()
                        .any(Self::expr_contains_pattern_comprehension)
            }
            LogicalPlan::Window { window_exprs, .. } => window_exprs
                .iter()
                .any(Self::expr_contains_pattern_comprehension),
            _ => false,
        };
        if has_expr {
            return true;
        }
        // Recurse into child plans
        Self::plan_children(plan)
            .iter()
            .any(|child| Self::contains_pattern_comprehension(child))
    }

    /// Detect traversals with step variables; DataFusion currently handles these
    /// more robustly for indexed list access semantics (e.g. r[0].prop).
    fn contains_step_variable_traversal(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Traverse {
                step_variable,
                input,
                ..
            }
            | LogicalPlan::TraverseMainByType {
                step_variable,
                input,
                ..
            } => step_variable.is_some() || Self::contains_step_variable_traversal(input),
            _ => Self::plan_children(plan)
                .iter()
                .any(|child| Self::contains_step_variable_traversal(child)),
        }
    }

    fn expr_contains_array_index(expr: &Expr) -> bool {
        match expr {
            Expr::ArrayIndex { .. } | Expr::ArraySlice { .. } => true,
            Expr::Property(base, _)
            | Expr::UnaryOp { expr: base, .. }
            | Expr::IsNull(base)
            | Expr::IsNotNull(base)
            | Expr::IsUnique(base) => Self::expr_contains_array_index(base),
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_array_index(left) || Self::expr_contains_array_index(right)
            }
            Expr::FunctionCall { args, .. } | Expr::List(args) => {
                args.iter().any(Self::expr_contains_array_index)
            }
            Expr::Map(entries) => entries
                .iter()
                .any(|(_, v)| Self::expr_contains_array_index(v)),
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                expr.as_ref()
                    .is_some_and(|e| Self::expr_contains_array_index(e.as_ref()))
                    || when_then.iter().any(|(w, t)| {
                        Self::expr_contains_array_index(w) || Self::expr_contains_array_index(t)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| Self::expr_contains_array_index(e.as_ref()))
            }
            Expr::In { expr, list } => {
                Self::expr_contains_array_index(expr) || Self::expr_contains_array_index(list)
            }
            Expr::Quantifier {
                list, predicate, ..
            } => {
                Self::expr_contains_array_index(list) || Self::expr_contains_array_index(predicate)
            }
            Expr::Reduce {
                init, list, expr, ..
            } => {
                Self::expr_contains_array_index(init)
                    || Self::expr_contains_array_index(list)
                    || Self::expr_contains_array_index(expr)
            }
            Expr::ListComprehension {
                list,
                where_clause,
                map_expr,
                ..
            } => {
                Self::expr_contains_array_index(list)
                    || where_clause
                        .as_ref()
                        .is_some_and(|e| Self::expr_contains_array_index(e.as_ref()))
                    || Self::expr_contains_array_index(map_expr)
            }
            Expr::PatternComprehension {
                where_clause,
                map_expr,
                ..
            } => {
                where_clause
                    .as_ref()
                    .is_some_and(|e| Self::expr_contains_array_index(e.as_ref()))
                    || Self::expr_contains_array_index(map_expr)
            }
            Expr::MapProjection { base, items } => {
                Self::expr_contains_array_index(base)
                    || items.iter().any(|item| match item {
                        MapProjectionItem::Property(_) | MapProjectionItem::Variable(_) => false,
                        MapProjectionItem::LiteralEntry(_, expr) => {
                            Self::expr_contains_array_index(expr)
                        }
                        MapProjectionItem::AllProperties => false,
                    })
            }
            Expr::ValidAt {
                entity, timestamp, ..
            } => {
                Self::expr_contains_array_index(entity)
                    || Self::expr_contains_array_index(timestamp)
            }
            Expr::LabelCheck { expr, .. } => Self::expr_contains_array_index(expr),
            _ => false,
        }
    }

    fn plan_contains_array_index(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Filter {
                input, predicate, ..
            } => {
                Self::expr_contains_array_index(predicate) || Self::plan_contains_array_index(input)
            }
            LogicalPlan::Project { input, projections } => {
                projections
                    .iter()
                    .any(|(e, _)| Self::expr_contains_array_index(e))
                    || Self::plan_contains_array_index(input)
            }
            LogicalPlan::Sort { input, order_by } => {
                order_by
                    .iter()
                    .any(|s| Self::expr_contains_array_index(&s.expr))
                    || Self::plan_contains_array_index(input)
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                group_by.iter().any(Self::expr_contains_array_index)
                    || aggregates.iter().any(Self::expr_contains_array_index)
                    || Self::plan_contains_array_index(input)
            }
            LogicalPlan::Window {
                input,
                window_exprs,
            } => {
                window_exprs.iter().any(Self::expr_contains_array_index)
                    || Self::plan_contains_array_index(input)
            }
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                Self::plan_contains_array_index(left) || Self::plan_contains_array_index(right)
            }
            LogicalPlan::Traverse { input, .. }
            | LogicalPlan::TraverseMainByType { input, .. }
            | LogicalPlan::Create { input, .. }
            | LogicalPlan::CreateBatch { input, .. }
            | LogicalPlan::Merge { input, .. }
            | LogicalPlan::Set { input, .. }
            | LogicalPlan::Remove { input, .. }
            | LogicalPlan::Delete { input, .. }
            | LogicalPlan::Foreach { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::SubqueryCall { input, .. }
            | LogicalPlan::ShortestPath { input, .. }
            | LogicalPlan::AllShortestPaths { input, .. }
            | LogicalPlan::QuantifiedPattern { input, .. }
            | LogicalPlan::BindZeroLengthPath { input, .. }
            | LogicalPlan::BindPath { input, .. }
            | LogicalPlan::Unwind { input, .. } => Self::plan_contains_array_index(input),
            LogicalPlan::Apply {
                input, subquery, ..
            } => {
                Self::plan_contains_array_index(input) || Self::plan_contains_array_index(subquery)
            }
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => {
                Self::plan_contains_array_index(initial)
                    || Self::plan_contains_array_index(recursive)
            }
            LogicalPlan::Explain { plan } => Self::plan_contains_array_index(plan),
            _ => false,
        }
    }

    fn needs_datafusion_for_step_indexing(plan: &LogicalPlan) -> bool {
        Self::contains_step_variable_traversal(plan) && Self::plan_contains_array_index(plan)
    }

    /// Extract window expressions from a logical plan (recursively unwrap Sort/Limit/etc).
    fn extract_window_expressions(plan: &LogicalPlan) -> Vec<Expr> {
        match plan {
            LogicalPlan::Window { window_exprs, .. } => window_exprs.clone(),
            LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Project { input, .. } => Self::extract_window_expressions(input),
            _ => Vec::new(),
        }
    }

    /// Executes a query as a stream of result batches.
    ///
    /// Uses execute_subplan for streaming results. DataFusion streaming
    /// execution will be added in a future release.
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

                // Use execute_subplan for streaming
                let fut = async move {
                    this.execute_subplan(plan, &prop_manager, &params, ctx.as_ref())
                        .await
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
                        QueryPlanner::new(Arc::new(this.storage.schema_manager().schema().clone()));
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
                        QueryPlanner::new(Arc::new(this.storage.schema_manager().schema().clone()));

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
                        if let Value::Map(map) = &val {
                            if let Some(start_node) = map.get("_startNode") {
                                return Ok(start_node.clone());
                            }
                            // Try _src_vid for raw edge data
                            if let Some(src_vid) = map.get("_src_vid") {
                                return Ok(Value::Map(HashMap::from([(
                                    "_vid".to_string(),
                                    src_vid.clone(),
                                )])));
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
                        if let Value::Map(map) = &val {
                            if let Some(end_node) = map.get("_endNode") {
                                return Ok(end_node.clone());
                            }
                            // Try _dst_vid for raw edge data
                            if let Some(dst_vid) = map.get("_dst_vid") {
                                return Ok(Value::Map(HashMap::from([(
                                    "_vid".to_string(),
                                    dst_vid.clone(),
                                )])));
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
                LogicalPlan::Scan {
                    label_id,
                    labels,
                    variable,
                    filter,
                    optional,
                } => {
                    // Branch on multi-label vs single-label scanning
                    let vids = if labels.len() > 1 {
                        // Multi-label path: intersection semantics
                        self.scan_multi_labels_with_filter(
                            &labels,
                            &variable,
                            filter.as_ref(),
                            ctx,
                            prop_manager,
                            params,
                        )
                        .await?
                    } else {
                        // Single-label path: existing logic (no regression)
                        self.scan_label_with_filter(
                            label_id,
                            &variable,
                            filter.as_ref(),
                            ctx,
                            prop_manager,
                            params,
                        )
                        .await?
                    };

                    if vids.is_empty() && optional {
                        let mut map = HashMap::new();
                        map.insert(variable.clone(), Value::Null);
                        return Ok(vec![map]);
                    }

                    // For multi-label, use first label name (or all labels joined)
                    let label_name = if labels.len() > 1 {
                        labels.join(":")
                    } else {
                        self.storage
                            .schema_manager()
                            .schema()
                            .label_name_by_id(label_id)
                            .unwrap_or("Unknown")
                            .to_string()
                    };

                    // Batch-fetch properties for all VIDs in one LanceDB query.
                    let batch_props = prop_manager
                        .get_batch_vertex_props_for_label(&vids, &label_name, ctx)
                        .await?;

                    // Fetch all labels for multi-label support in labels() function
                    let labels_map = prop_manager.get_batch_labels(&vids, ctx).await?;

                    let mut matches = Vec::new();
                    for vid in vids {
                        if let Some(props) = batch_props.get(&vid) {
                            let mut props_json: HashMap<String, Value> =
                                props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            props_json.insert("_vid".to_string(), Value::Int(vid.as_u64() as i64));
                            // Add full list of labels
                            if let Some(labels) = labels_map.get(&vid) {
                                props_json.insert(
                                    "_labels".to_string(),
                                    Value::List(
                                        labels.iter().map(|l| Value::String(l.clone())).collect(),
                                    ),
                                );
                            } else {
                                // Fallback to the scan label
                                props_json.insert(
                                    "_labels".to_string(),
                                    Value::List(vec![Value::String(label_name.clone())]),
                                );
                            }

                            let mut map = HashMap::new();
                            map.insert(variable.clone(), Value::Map(props_json));
                            matches.push(map);
                        }
                    }
                    Ok(matches)
                }
                LogicalPlan::ExtIdLookup {
                    variable,
                    ext_id,
                    filter: _filter, // Filter already applied via ext_id match
                    optional,
                } => {
                    use uni_store::storage::main_vertex::MainVertexDataset;

                    // Look up vertex by ext_id in main vertices table
                    let lancedb = self.storage.lancedb_store();
                    let found_vid = MainVertexDataset::find_by_ext_id(lancedb, &ext_id).await?;

                    if let Some(vid) = found_vid {
                        // Load properties
                        let props_opt =
                            prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await?;

                        if let Some(props) = props_opt {
                            // Get labels for the vertex from main table
                            let labels = MainVertexDataset::find_labels_by_vid(lancedb, vid)
                                .await?
                                .unwrap_or_default();

                            let mut props_json: HashMap<String, Value> = props;
                            props_json.insert("_vid".to_string(), Value::Int(vid.as_u64() as i64));
                            props_json.insert("ext_id".to_string(), Value::String(ext_id.clone()));
                            // Add first label or "Unknown" if no labels
                            let label_name = labels
                                .first()
                                .cloned()
                                .unwrap_or_else(|| "Unknown".to_string());
                            props_json.insert(
                                "_labels".to_string(),
                                Value::List(vec![Value::String(label_name.to_string())]),
                            );

                            let mut map = HashMap::new();
                            map.insert(variable.clone(), Value::Map(props_json));
                            return Ok(vec![map]);
                        }
                    }

                    // No match found
                    if optional {
                        let mut map = HashMap::new();
                        map.insert(variable.clone(), Value::Null);
                        Ok(vec![map])
                    } else {
                        Ok(vec![])
                    }
                }
                LogicalPlan::ScanAll {
                    variable,
                    filter,
                    optional,
                } => {
                    // Scan all vertices from main table (schemaless MATCH (n))
                    let vids = self
                        .scan_all_vertices(&variable, filter.as_ref(), ctx, prop_manager, params)
                        .await?;

                    if vids.is_empty() && optional {
                        let mut map = HashMap::new();
                        map.insert(variable.clone(), Value::Null);
                        return Ok(vec![map]);
                    }

                    // Build result rows with properties from main table
                    let lancedb = self.storage.lancedb_store();
                    let mut matches = Vec::new();
                    for vid in vids {
                        // Get properties from property manager (will fallback to main table)
                        let props_opt =
                            prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await?;

                        // Get labels: check L0 first, then main table
                        let labels = if let Some(ctx) = ctx
                            && let Some(l0_labels) = ctx.l0.read().get_vertex_labels(vid)
                        {
                            l0_labels.to_vec()
                        } else {
                            uni_store::storage::main_vertex::MainVertexDataset::find_labels_by_vid(
                                lancedb, vid,
                            )
                            .await?
                            .unwrap_or_default()
                        };

                        let mut props_json: HashMap<String, Value> = props_opt.unwrap_or_default();

                        props_json.insert("_vid".to_string(), Value::Int(vid.as_u64() as i64));
                        props_json.insert(
                            "_labels".to_string(),
                            Value::List(labels.iter().map(|l| Value::String(l.clone())).collect()),
                        );

                        let mut map = HashMap::new();
                        map.insert(variable.clone(), Value::Map(props_json));
                        matches.push(map);
                    }
                    Ok(matches)
                }
                LogicalPlan::ScanMainByLabels {
                    labels,
                    variable,
                    filter,
                    optional,
                } => {
                    // Scan main table for vertices with schemaless labels
                    // For multi-label, use intersection semantics (must have ALL labels)
                    let vids = if labels.len() > 1 {
                        self.scan_multi_labels_with_filter(
                            &labels,
                            &variable,
                            filter.as_ref(),
                            ctx,
                            prop_manager,
                            params,
                        )
                        .await?
                    } else if let Some(label_name) = labels.first() {
                        self.scan_main_by_label(
                            label_name,
                            &variable,
                            filter.as_ref(),
                            ctx,
                            prop_manager,
                            params,
                        )
                        .await?
                    } else {
                        // Empty labels - scan all vertices
                        self.scan_all_vertices(
                            &variable,
                            filter.as_ref(),
                            ctx,
                            prop_manager,
                            params,
                        )
                        .await?
                    };

                    if vids.is_empty() && optional {
                        let mut map = HashMap::new();
                        map.insert(variable.clone(), Value::Null);
                        return Ok(vec![map]);
                    }

                    // Build result rows with properties from main table
                    let lancedb = self.storage.lancedb_store();
                    let mut matches = Vec::new();
                    for vid in vids {
                        // Get properties from property manager (will fallback to main table)
                        let props_opt =
                            prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await?;

                        // Get actual labels: check L0 first, then main table
                        let actual_labels = if let Some(ctx) = ctx
                            && let Some(l0_labels) = ctx.l0.read().get_vertex_labels(vid)
                        {
                            l0_labels.to_vec()
                        } else {
                            uni_store::storage::main_vertex::MainVertexDataset::find_labels_by_vid(
                                lancedb, vid,
                            )
                            .await?
                            .unwrap_or_default()
                        };

                        let mut props_json: HashMap<String, Value> = props_opt.unwrap_or_default();

                        props_json.insert("_vid".to_string(), Value::Int(vid.as_u64() as i64));
                        props_json.insert(
                            "_labels".to_string(),
                            Value::List(
                                actual_labels
                                    .iter()
                                    .map(|l| Value::String(l.clone()))
                                    .collect(),
                            ),
                        );

                        let mut map = HashMap::new();
                        map.insert(variable.clone(), Value::Map(props_json));
                        matches.push(map);
                    }
                    Ok(matches)
                }
                LogicalPlan::Traverse {
                    input,
                    edge_type_ids,
                    direction,
                    source_variable,
                    target_variable,
                    target_label_id,
                    step_variable,
                    min_hops,
                    max_hops,
                    optional,
                    target_filter,
                    path_variable,
                    optional_pattern_vars,
                    ..
                } => {
                    let input_matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let traverse_results = self
                        .execute_traverse(
                            input_matches,
                            edge_type_ids,
                            &direction,
                            &source_variable,
                            &target_variable,
                            target_label_id,
                            &step_variable,
                            min_hops,
                            max_hops,
                            optional,
                            &path_variable,
                            &optional_pattern_vars,
                            prop_manager,
                            ctx,
                        )
                        .await?;

                    // Apply target_filter if present
                    // For OPTIONAL MATCH, preserve rows where target is NULL (no match found)
                    if let Some(filter) = target_filter {
                        let mut filtered = Vec::new();
                        for row in traverse_results {
                            // If this is optional and target is NULL, preserve the row
                            // (indicates OPTIONAL MATCH didn't find any edges)
                            let target_is_null = row
                                .get(&target_variable)
                                .map(|v| matches!(v, Value::Null))
                                .unwrap_or(false);

                            if optional && target_is_null {
                                filtered.push(row);
                            } else {
                                let res = self
                                    .evaluate_expr(&filter, &row, prop_manager, params, ctx)
                                    .await?;
                                if res.as_bool().unwrap_or(false) {
                                    filtered.push(row);
                                }
                            }
                        }
                        Ok(filtered)
                    } else {
                        Ok(traverse_results)
                    }
                }
                LogicalPlan::TraverseMainByType {
                    type_names,
                    input,
                    direction,
                    source_variable,
                    target_variable,
                    step_variable,
                    min_hops,
                    max_hops,
                    optional,
                    target_filter,
                    path_variable,
                    is_variable_length,
                    optional_pattern_vars,
                } => {
                    let input_matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let traverse_results = if is_variable_length {
                        self.execute_vlp(
                            input_matches,
                            &type_names,
                            &direction,
                            &source_variable,
                            &target_variable,
                            &step_variable,
                            min_hops,
                            max_hops,
                            optional,
                            &path_variable,
                            &optional_pattern_vars,
                            prop_manager,
                            ctx,
                        )
                        .await?
                    } else {
                        self.execute_traverse_main_by_type(
                            input_matches,
                            &type_names,
                            &direction,
                            &source_variable,
                            &target_variable,
                            &step_variable,
                            min_hops,
                            max_hops,
                            optional,
                            &path_variable,
                            &optional_pattern_vars,
                            prop_manager,
                            ctx,
                        )
                        .await?
                    };

                    // Apply target_filter if present
                    // For OPTIONAL MATCH, preserve rows where target is NULL (no match found)
                    if let Some(filter) = target_filter {
                        let mut filtered = Vec::new();
                        for row in traverse_results {
                            // If this is optional and target is NULL, preserve the row
                            // (indicates OPTIONAL MATCH didn't find any edges)
                            let target_is_null = row
                                .get(&target_variable)
                                .map(|v| matches!(v, Value::Null))
                                .unwrap_or(false);

                            if optional && target_is_null {
                                filtered.push(row);
                            } else {
                                let res = self
                                    .evaluate_expr(&filter, &row, prop_manager, params, ctx)
                                    .await?;
                                if res.as_bool().unwrap_or(false) {
                                    filtered.push(row);
                                }
                            }
                        }
                        Ok(filtered)
                    } else {
                        Ok(traverse_results)
                    }
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
                    for (idx, row) in input_matches.iter().enumerate() {
                        let res = self
                            .evaluate_expr(&predicate, row, prop_manager, params, ctx)
                            .await?;

                        let passes = res.as_bool().unwrap_or(false);

                        // Debug first few rows
                        if idx < 3 {
                            tracing::debug!(
                                "Filter row {}: predicate result={:?} passes={}",
                                idx,
                                res,
                                passes
                            );
                        }

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
                LogicalPlan::VectorKnn {
                    label_id,
                    variable,
                    property,
                    query,
                    k,
                    threshold,
                } => {
                    self.execute_vector_knn(
                        label_id,
                        &variable,
                        &property,
                        &query,
                        k,
                        threshold,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::InvertedIndexLookup {
                    label_id,
                    variable,
                    property,
                    terms,
                } => {
                    self.execute_inverted_index_lookup(
                        label_id,
                        &variable,
                        &property,
                        &terms,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
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
                LogicalPlan::Set { input, items } => {
                    let mut rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        for row in &mut rows {
                            self.execute_set_items_locked(
                                &items,
                                row,
                                &mut writer,
                                prop_manager,
                                params,
                                ctx,
                            )
                            .await?;
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    Ok(rows)
                }
                LogicalPlan::Remove { input, items } => {
                    let mut rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        for row in &mut rows {
                            self.execute_remove_items_locked(
                                &items,
                                row,
                                &mut writer,
                                prop_manager,
                                ctx,
                            )
                            .await?;
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    Ok(rows)
                }
                LogicalPlan::Merge {
                    input,
                    pattern,
                    on_match,
                    on_create,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_merge(
                        rows,
                        &pattern,
                        on_match.as_ref(),
                        on_create.as_ref(),
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::Create { input, pattern } => {
                    let mut rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        for row in &mut rows {
                            self.execute_create_pattern(
                                &pattern,
                                row,
                                &mut writer,
                                prop_manager,
                                params,
                                ctx,
                            )
                            .await?;
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    // Return rows with created entities for RETURN clause projection
                    Ok(rows)
                }
                LogicalPlan::CreateBatch { input, patterns } => {
                    // Execute input plan once (no recursion per pattern)
                    let mut rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;

                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        // For each row, execute all patterns sequentially.
                        // Later patterns can reference variables from earlier patterns.
                        for row in &mut rows {
                            for pattern in &patterns {
                                self.execute_create_pattern(
                                    pattern,
                                    row,
                                    &mut writer,
                                    prop_manager,
                                    params,
                                    ctx,
                                )
                                .await?;
                            }
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    // Return rows with created entities for RETURN clause projection
                    Ok(rows)
                }
                LogicalPlan::Delete {
                    input,
                    items,
                    detach,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;

                        if detach {
                            // Batch detach-delete: collect all vertex VIDs and labels,
                            // then load subgraphs once for all vertices.
                            let mut vertex_vids = Vec::new();
                            let mut vertex_labels = Vec::new();
                            let mut edge_vals = Vec::new();

                            for row in &rows {
                                for expr in &items {
                                    let val = self
                                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                                        .await?;
                                    if let Ok(vid) = Self::vid_from_value(&val) {
                                        let labels = Self::extract_labels_from_node(&val);
                                        vertex_vids.push(vid);
                                        vertex_labels.push(labels);
                                    } else if let Value::Map(_) = &val {
                                        edge_vals.push(val);
                                    }
                                }
                            }

                            // Batch detach-delete all vertices at once.
                            if !vertex_vids.is_empty() {
                                self.batch_detach_delete_vertices(
                                    &vertex_vids,
                                    vertex_labels,
                                    &mut writer,
                                )
                                .await?;
                            }

                            // Delete edges individually (typically few).
                            for val in &edge_vals {
                                if let Value::Map(map) = val {
                                    self.execute_delete_edge_from_map(map, &mut writer).await?;
                                }
                            }
                        } else {
                            // Non-detach delete: per-item (checks for dangling edges).
                            for row in &rows {
                                for expr in &items {
                                    let val = self
                                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                                        .await?;
                                    self.execute_delete_item_locked(&val, false, &mut writer)
                                        .await?;
                                }
                            }
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    // DELETE passes through input rows to support RETURN clauses.
                    // The planner wraps terminal DELETE operations in Limit(0) to produce
                    // empty results when no RETURN clause is present (OpenCypher spec).
                    Ok(rows)
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
                LogicalPlan::ShortestPath {
                    input,
                    edge_type_ids,
                    direction,
                    source_variable,
                    target_variable,
                    target_label_id: _,
                    path_variable,
                    min_hops: _,
                    max_hops,
                } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;

                    log::debug!(
                        "ShortestPath: got {} input rows, source_var={}, target_var={}",
                        input_rows.len(),
                        source_variable,
                        target_variable
                    );

                    let mut results = Vec::new();

                    for row in &input_rows {
                        // Extract source and target VIDs
                        // The VID can be either at "var._vid" (flat) or nested in "var" object
                        let source_vid = Self::extract_vid_from_row(row, &source_variable);
                        let target_vid = Self::extract_vid_from_row(row, &target_variable);

                        let (source_vid, target_vid) = match (source_vid, target_vid) {
                            (Some(s), Some(t)) => (s, t),
                            _ => {
                                log::debug!(
                                    "ShortestPath: could not extract VIDs from row keys {:?}",
                                    row.keys().collect::<Vec<_>>()
                                );
                                continue;
                            }
                        };

                        log::debug!(
                            "ShortestPath: searching path from {:?} to {:?}",
                            source_vid,
                            target_vid
                        );

                        // BFS to find shortest path
                        let path = self
                            .find_shortest_path(
                                source_vid,
                                target_vid,
                                &edge_type_ids,
                                &direction,
                                max_hops,
                                ctx,
                            )
                            .await;

                        log::debug!("ShortestPath: found path = {:?}", path);

                        if let Some(path_vids) = path {
                            let mut result_row = row.clone();

                            // Create node objects for the path
                            let path_nodes: Vec<crate::types::Node> = path_vids
                                .iter()
                                .map(|v| crate::types::Node {
                                    vid: *v,
                                    labels: vec![],
                                    properties: HashMap::new(),
                                })
                                .collect();

                            // Create empty relationships (edges between nodes)
                            // For a path of N nodes, there are N-1 edges
                            let path_edges: Vec<crate::types::Edge> =
                                (0..path_vids.len().saturating_sub(1))
                                    .map(|i| crate::types::Edge {
                                        eid: Eid::new(i as u64),
                                        edge_type: String::new(),
                                        src: Vid::new(0),
                                        dst: Vid::new(0),
                                        properties: HashMap::new(),
                                    })
                                    .collect();

                            // Create a proper path object that length() can understand
                            let path_obj = Value::Path(crate::types::Path {
                                nodes: path_nodes,
                                edges: path_edges,
                            });
                            result_row.insert(path_variable.clone(), path_obj);

                            results.push(result_row);
                        }
                    }

                    Ok(results)
                }
                LogicalPlan::AllShortestPaths { .. } => {
                    // AllShortestPaths is handled at the vectorized execution layer
                    // If we reach here, return empty result
                    Ok(vec![])
                }
                LogicalPlan::Foreach {
                    input,
                    variable,
                    list,
                    body,
                } => {
                    // Execute the input first
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;

                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;

                        for row in &rows {
                            // Evaluate the list expression
                            let list_val = self
                                .evaluate_expr(&list, row, prop_manager, params, ctx)
                                .await?;

                            let items = match list_val {
                                Value::List(arr) => arr,
                                Value::Null => continue,
                                _ => return Err(anyhow!("FOREACH requires a list")),
                            };

                            // Execute body for each item
                            for item in items {
                                // Create scope with the iteration variable
                                let mut scope = row.clone();
                                scope.insert(variable.clone(), item);

                                // Execute each update clause in the body
                                for plan in &body {
                                    self.execute_foreach_body_plan(
                                        plan.clone(),
                                        &mut scope,
                                        &mut writer,
                                        prop_manager,
                                        params,
                                        ctx,
                                    )
                                    .await?;
                                }
                            }
                        }
                    } else {
                        return Err(anyhow!("FOREACH requires a Writer"));
                    }

                    Ok(rows)
                }
                LogicalPlan::Empty => Ok(vec![HashMap::new()]),
                LogicalPlan::BindZeroLengthPath {
                    input,
                    node_variable,
                    path_variable,
                } => {
                    // Execute input first
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;

                    // For each row, create a zero-length path
                    let mut result = Vec::with_capacity(rows.len());
                    for mut row in rows {
                        // Get node VID
                        let vid_key = format!("{}._vid", node_variable);
                        let label_key = format!("{}._label", node_variable);

                        let vid = row.get(&vid_key).cloned().unwrap_or(Value::Null);
                        let label = row.get(&label_key).cloned().unwrap_or(Value::Null);

                        // Build node for path
                        let node_vid = vid.as_u64().map(Vid::new).unwrap_or_else(|| Vid::new(0));
                        let node_label = label.as_str().unwrap_or("").to_string();
                        let path_node = crate::types::Node {
                            vid: node_vid,
                            labels: if node_label.is_empty() {
                                vec![]
                            } else {
                                vec![node_label]
                            },
                            properties: HashMap::new(),
                        };

                        // Create path with one node and zero edges
                        let path = Value::Path(crate::types::Path {
                            nodes: vec![path_node],
                            edges: vec![],
                        });

                        row.insert(path_variable.clone(), path);
                        result.push(row);
                    }
                    Ok(result)
                }
                LogicalPlan::BindPath {
                    input,
                    node_variables,
                    edge_variables,
                    path_variable,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;

                    let mut result = Vec::with_capacity(rows.len());
                    for mut row in rows {
                        let nodes = node_variables
                            .iter()
                            .map(|var| Self::coerce_row_node(&row, var))
                            .collect();
                        let edges = edge_variables
                            .iter()
                            .map(|var| Self::coerce_row_edge(&row, var))
                            .collect();

                        row.insert(
                            path_variable.clone(),
                            Value::Path(crate::types::Path { nodes, edges }),
                        );
                        result.push(row);
                    }
                    Ok(result)
                }
                LogicalPlan::QuantifiedPattern { .. } => Err(anyhow!(
                    "Quantified patterns are not supported in the fallback executor"
                )),
                LogicalPlan::LoadCsv {
                    url,
                    variable,
                    with_headers,
                    field_terminator,
                } => {
                    self.execute_load_csv(&url, &variable, with_headers, field_terminator)
                        .await
                }
            }
        })
    }

    pub async fn execute_load_csv(
        &self,
        url: &str,
        variable: &str,
        with_headers: bool,
        field_terminator: Option<char>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let delimiter = field_terminator.unwrap_or(',') as u8;

        // Load data
        let content = if let Some(path) = url.strip_prefix("file://") {
            tokio::fs::read_to_string(path).await?
        } else if url.starts_with("http://") || url.starts_with("https://") {
            reqwest::get(url).await?.text().await?
        } else {
            // Assume local file path
            tokio::fs::read_to_string(url).await?
        };

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(with_headers)
            .delimiter(delimiter)
            .from_reader(content.as_bytes());

        let headers: Option<Vec<String>> = if with_headers {
            Some(reader.headers()?.iter().map(|s| s.to_string()).collect())
        } else {
            None
        };

        let mut rows = Vec::new();
        for result in reader.records() {
            let record = result?;
            let row_val: Value = if let Some(ref hdrs) = headers {
                // Return as map
                let mut map = HashMap::new();
                for (h, v) in hdrs.iter().zip(record.iter()) {
                    map.insert(h.clone(), Value::String(v.to_string()));
                }
                Value::Map(map)
            } else {
                // Return as array
                let arr: Vec<Value> = record
                    .iter()
                    .map(|v| Value::String(v.to_string()))
                    .collect();
                Value::List(arr)
            };

            let mut map = HashMap::new();
            map.insert(variable.to_string(), row_val);
            rows.push(map);
        }

        Ok(rows)
    }

    /// Execute a single plan from a FOREACH body with the given scope.
    async fn execute_foreach_body_plan(
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
                // Execute all patterns sequentially; later patterns can reference
                // variables from earlier ones.
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
                // For MERGE inside FOREACH, we do a simplified create-if-not-exists
                // Full MERGE semantics would require checking for existence first
                self.execute_create_pattern(&pattern, scope, writer, prop_manager, params, ctx)
                    .await?;

                // Apply ON CREATE if present
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
                // Nested FOREACH
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
                        // Use Box::pin for recursive async call
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
                    "Unsupported operation in FOREACH: only SET, REMOVE, DELETE, CREATE, MERGE, and nested FOREACH are allowed"
                ));
            }
        }
        Ok(())
    }

    /// Executes a graph traversal operation using BFS.
    ///
    /// # Errors
    ///
    /// Returns an error if the traversal times out or encounters a storage error.
    #[expect(
        clippy::too_many_arguments,
        reason = "Graph traversal requires many parameters"
    )]
    pub(crate) async fn execute_traverse(
        &self,
        input_matches: Vec<HashMap<String, Value>>,
        edge_type_ids: Vec<u32>,
        direction: &Direction,
        source_variable: &str,
        target_variable: &str,
        target_label_id: u16,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        path_variable: &Option<String>,
        optional_pattern_vars: &std::collections::HashSet<String>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut new_matches = Vec::new();
        for m in input_matches {
            // Check timeout between rows to prevent long-running traversals
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }

            let found = self
                .traverse_from_row(
                    &m,
                    &edge_type_ids,
                    direction,
                    source_variable,
                    target_variable,
                    target_label_id,
                    step_variable,
                    min_hops,
                    max_hops,
                    path_variable,
                    &mut new_matches,
                    prop_manager,
                    ctx,
                )
                .await?;

            if !found && optional {
                let mut new_m = m.clone();
                // For multi-hop OPTIONAL MATCH, set ALL pattern variables to NULL
                // when any hop fails to match. This ensures proper semantics where
                // the entire pattern either matches completely or returns NULL for all vars.
                if optional_pattern_vars.is_empty() {
                    // Fallback for single-hop patterns without optional_pattern_vars
                    new_m.insert(target_variable.to_string(), Value::Null);
                    if let Some(sv) = step_variable {
                        new_m.insert(sv.clone(), Value::Null);
                    }
                    if let Some(pv) = path_variable {
                        new_m.insert(pv.clone(), Value::Null);
                    }
                } else {
                    // Multi-hop: set ALL optional pattern variables to NULL
                    for var in optional_pattern_vars {
                        new_m.insert(var.clone(), Value::Null);
                    }
                }
                new_matches.push(new_m);
            }
        }
        Ok(new_matches)
    }

    /// Executes a schemaless graph traversal by type name(s) using the main edges table.
    ///
    /// This is used for edge types not defined in the schema (e.g., MATCH (a)-[:UnknownType]->(b)).
    /// Supports OR relationship types like `[:KNOWS|HATES]` via multiple type_names.
    /// It scans the main edges table for edges with any of the given type names.
    ///
    /// # Errors
    ///
    /// Returns an error if the traversal times out or encounters a storage error.
    #[expect(
        clippy::too_many_arguments,
        reason = "Graph traversal requires many parameters"
    )]
    pub(crate) async fn execute_traverse_main_by_type(
        &self,
        input_matches: Vec<HashMap<String, Value>>,
        type_names: &[String],
        direction: &Direction,
        source_variable: &str,
        target_variable: &str,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        path_variable: &Option<String>,
        optional_pattern_vars: &std::collections::HashSet<String>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        use uni_store::storage::main_edge::MainEdgeDataset;

        // For now, only support single-hop traversal for schemaless edges
        if min_hops != 1 || max_hops != 1 {
            return Err(anyhow!(
                "Variable-length paths not yet supported for schemaless edge types"
            ));
        }

        // Get edges from main table for all type names
        let lancedb = self.storage.lancedb_store();
        let type_refs: Vec<&str> = type_names.iter().map(|s| s.as_str()).collect();
        let mut edges_by_type =
            MainEdgeDataset::find_edges_by_type_names(lancedb, &type_refs).await?;

        // Helper to collect edges from an L0 buffer for multiple types
        fn collect_l0_edges(
            l0: &uni_store::runtime::l0::L0Buffer,
            type_names: &[String],
            edges: &mut Vec<(Eid, Vid, Vid, String, uni_common::Properties)>,
        ) {
            for type_name in type_names {
                for eid in l0.eids_for_type(type_name) {
                    if let Some((src, dst)) = l0.get_edge_endpoints(eid) {
                        let props = l0.edge_properties.get(&eid).cloned().unwrap_or_default();
                        edges.push((eid, src, dst, type_name.clone(), props));
                    }
                }
            }
        }

        // Add edges from L0 buffers
        if let Some(ctx) = ctx {
            collect_l0_edges(&ctx.l0.read(), type_names, &mut edges_by_type);

            if let Some(tx_l0_arc) = &ctx.transaction_l0 {
                collect_l0_edges(&tx_l0_arc.read(), type_names, &mut edges_by_type);
            }

            for pending_l0_arc in &ctx.pending_flush_l0s {
                collect_l0_edges(&pending_l0_arc.read(), type_names, &mut edges_by_type);
            }
        }

        // Deduplicate by eid (in case edge appears in both storage and L0)
        let mut seen_eids = std::collections::HashSet::new();
        edges_by_type.retain(|(eid, _, _, _, _)| seen_eids.insert(*eid));

        let mut new_matches = Vec::new();

        for input_row in &input_matches {
            // Check timeout between rows
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }

            let source_vid = match input_row.get(source_variable).and_then(|v| {
                let result = Self::vid_from_value(v);
                if result.is_err() {
                    tracing::debug!("  vid_from_value failed: {:?}", result);
                }
                result.ok()
            }) {
                Some(v) => v,
                None => {
                    if optional {
                        let mut new_m = input_row.clone();
                        // For multi-hop OPTIONAL MATCH, set ALL pattern variables to NULL
                        if optional_pattern_vars.is_empty() {
                            new_m.insert(target_variable.to_string(), Value::Null);
                            if let Some(sv) = step_variable {
                                new_m.insert(sv.clone(), Value::Null);
                            }
                            if let Some(pv) = path_variable {
                                new_m.insert(pv.clone(), Value::Null);
                            }
                        } else {
                            for var in optional_pattern_vars {
                                new_m.insert(var.clone(), Value::Null);
                            }
                        }
                        new_matches.push(new_m);
                    }
                    continue;
                }
            };

            let mut found = false;

            // Find edges matching source and direction
            for (eid, src_vid, dst_vid, edge_type, edge_props) in &edges_by_type {
                let (matches, target_vid) = match direction {
                    Direction::Outgoing => (*src_vid == source_vid, *dst_vid),
                    Direction::Incoming => (*dst_vid == source_vid, *src_vid),
                    Direction::Both => {
                        if *src_vid == source_vid {
                            (true, *dst_vid)
                        } else if *dst_vid == source_vid {
                            (true, *src_vid)
                        } else {
                            (false, Vid::new(0))
                        }
                    }
                };

                if !matches {
                    continue;
                }

                found = true;
                let mut new_row = input_row.clone();

                // Build target node value with label
                let mut target_json = prop_manager
                    .get_all_vertex_props_with_ctx(target_vid, ctx)
                    .await?
                    .unwrap_or_default();
                target_json.insert("_vid".to_string(), Value::Int(target_vid.as_u64() as i64));

                // Look up target label - first from L0 (for recently created nodes),
                // Check L0 first, then fall back to storage
                let target_labels = if let Some(ctx) = ctx {
                    let l0_labels =
                        uni_store::runtime::l0_visibility::get_vertex_labels(target_vid, ctx);
                    if !l0_labels.is_empty() {
                        l0_labels
                    } else {
                        uni_store::storage::main_vertex::MainVertexDataset::find_labels_by_vid(
                            self.storage.lancedb_store(),
                            target_vid,
                        )
                        .await?
                        .unwrap_or_default()
                    }
                } else {
                    uni_store::storage::main_vertex::MainVertexDataset::find_labels_by_vid(
                        self.storage.lancedb_store(),
                        target_vid,
                    )
                    .await?
                    .unwrap_or_default()
                };
                target_json.insert(
                    "_labels".to_string(),
                    Value::List(
                        target_labels
                            .iter()
                            .map(|l| Value::String(l.clone()))
                            .collect(),
                    ),
                );

                new_row.insert(target_variable.to_string(), Value::Map(target_json));

                // Build step (relationship) variable if present
                if let Some(sv) = step_variable {
                    let mut edge_json = edge_props.clone();
                    edge_json.insert("_eid".to_string(), Value::Int(eid.as_u64() as i64));
                    edge_json.insert("_type".to_string(), Value::String(edge_type.to_string()));
                    edge_json.insert("_src".to_string(), Value::Int(src_vid.as_u64() as i64));
                    edge_json.insert("_dst".to_string(), Value::Int(dst_vid.as_u64() as i64));
                    new_row.insert(sv.clone(), Value::Map(edge_json));
                }

                // Path variable not fully supported for schemaless yet
                if let Some(pv) = path_variable {
                    // Build a minimal path representation
                    let path_obj = Value::Path(crate::types::Path {
                        nodes: vec![
                            crate::types::Node {
                                vid: source_vid,
                                labels: vec![],
                                properties: HashMap::new(),
                            },
                            crate::types::Node {
                                vid: target_vid,
                                labels: vec![],
                                properties: HashMap::new(),
                            },
                        ],
                        edges: vec![crate::types::Edge {
                            eid: *eid,
                            edge_type: String::new(),
                            src: source_vid,
                            dst: target_vid,
                            properties: HashMap::new(),
                        }],
                    });
                    new_row.insert(pv.clone(), path_obj);
                }

                new_matches.push(new_row);
            }

            if !found && optional {
                let mut new_m = input_row.clone();
                // For multi-hop OPTIONAL MATCH, set ALL pattern variables to NULL
                if optional_pattern_vars.is_empty() {
                    new_m.insert(target_variable.to_string(), Value::Null);
                    if let Some(sv) = step_variable {
                        new_m.insert(sv.clone(), Value::Null);
                    }
                    if let Some(pv) = path_variable {
                        new_m.insert(pv.clone(), Value::Null);
                    }
                } else {
                    for var in optional_pattern_vars {
                        new_m.insert(var.clone(), Value::Null);
                    }
                }
                new_matches.push(new_m);
            }
        }

        Ok(new_matches)
    }

    /// Execute a variable-length path (VLP) traversal using BFS.
    ///
    /// This function handles patterns like `(a)-[r*1..3]->(b)` by performing a
    /// breadth-first search from each source node, accumulating edges along the way.
    ///
    /// Key semantics:
    /// - Relationship uniqueness: Each edge can only appear once per path
    /// - Zero-length paths (min_hops=0): Source equals target with empty edge list
    /// - step_variable holds `Value::List` of edge maps (even for single-hop VLP)
    /// - path_variable holds `Value::Path` with nodes and edges
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_vlp(
        &self,
        input_matches: Vec<HashMap<String, Value>>,
        type_names: &[String],
        direction: &Direction,
        source_variable: &str,
        target_variable: &str,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        path_variable: &Option<String>,
        optional_pattern_vars: &std::collections::HashSet<String>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        tracing::debug!(
            "execute_vlp: step_variable={:?}, path_variable={:?}, min_hops={}, max_hops={}",
            step_variable,
            path_variable,
            min_hops,
            max_hops
        );
        self.execute_vlp_inner(
            input_matches,
            type_names,
            direction,
            source_variable,
            target_variable,
            step_variable,
            min_hops,
            max_hops,
            optional,
            path_variable,
            optional_pattern_vars,
            prop_manager,
            ctx,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_vlp_inner(
        &self,
        input_matches: Vec<HashMap<String, Value>>,
        type_names: &[String],
        direction: &Direction,
        source_variable: &str,
        target_variable: &str,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        path_variable: &Option<String>,
        optional_pattern_vars: &std::collections::HashSet<String>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        use std::collections::{HashSet, VecDeque};
        use uni_store::storage::main_edge::MainEdgeDataset;

        // Get edges from main table for all type names
        let lancedb = self.storage.lancedb_store();
        let type_refs: Vec<&str> = type_names.iter().map(|s| s.as_str()).collect();
        let mut edges_by_type =
            MainEdgeDataset::find_edges_by_type_names(lancedb, &type_refs).await?;

        // Helper to collect edges from an L0 buffer for multiple types
        fn collect_l0_edges(
            l0: &uni_store::runtime::l0::L0Buffer,
            type_names: &[String],
            edges: &mut Vec<(Eid, Vid, Vid, String, uni_common::Properties)>,
        ) {
            for type_name in type_names {
                for eid in l0.eids_for_type(type_name) {
                    if let Some((src, dst)) = l0.get_edge_endpoints(eid) {
                        let props = l0.edge_properties.get(&eid).cloned().unwrap_or_default();
                        edges.push((eid, src, dst, type_name.clone(), props));
                    }
                }
            }
        }

        // Add edges from L0 buffers
        if let Some(ctx) = ctx {
            collect_l0_edges(&ctx.l0.read(), type_names, &mut edges_by_type);

            if let Some(tx_l0_arc) = &ctx.transaction_l0 {
                collect_l0_edges(&tx_l0_arc.read(), type_names, &mut edges_by_type);
            }

            for pending_l0_arc in &ctx.pending_flush_l0s {
                collect_l0_edges(&pending_l0_arc.read(), type_names, &mut edges_by_type);
            }
        }

        // Deduplicate by eid (in case edge appears in both storage and L0)
        let mut seen_eids = HashSet::new();
        edges_by_type.retain(|(eid, _, _, _, _)| seen_eids.insert(*eid));

        // Build adjacency index for efficient neighbor lookup
        // Key: vid, Value: list of (eid, neighbor_vid, edge_type, edge_props)
        let mut outgoing: HashMap<Vid, Vec<(Eid, Vid, String, uni_common::Properties)>> =
            HashMap::new();
        let mut incoming: HashMap<Vid, Vec<(Eid, Vid, String, uni_common::Properties)>> =
            HashMap::new();

        for (eid, src_vid, dst_vid, edge_type, edge_props) in &edges_by_type {
            outgoing.entry(*src_vid).or_default().push((
                *eid,
                *dst_vid,
                edge_type.clone(),
                edge_props.clone(),
            ));
            incoming.entry(*dst_vid).or_default().push((
                *eid,
                *src_vid,
                edge_type.clone(),
                edge_props.clone(),
            ));
        }

        let mut new_matches = Vec::new();

        for input_row in &input_matches {
            // Check timeout between rows
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }

            let source_vid = match input_row
                .get(source_variable)
                .and_then(|v| Self::vid_from_value(v).ok())
            {
                Some(v) => v,
                None => {
                    if optional {
                        let mut new_m = input_row.clone();
                        if optional_pattern_vars.is_empty() {
                            new_m.insert(target_variable.to_string(), Value::Null);
                            if let Some(sv) = step_variable {
                                new_m.insert(sv.clone(), Value::Null);
                            }
                            if let Some(pv) = path_variable {
                                new_m.insert(pv.clone(), Value::Null);
                            }
                        } else {
                            for var in optional_pattern_vars {
                                new_m.insert(var.clone(), Value::Null);
                            }
                        }
                        new_matches.push(new_m);
                    }
                    continue;
                }
            };

            // Get source node properties for path building
            let source_props = prop_manager
                .get_all_vertex_props_with_ctx(source_vid, ctx)
                .await?
                .unwrap_or_default();
            let source_labels = if let Some(ctx) = ctx {
                let l0_labels =
                    uni_store::runtime::l0_visibility::get_vertex_labels(source_vid, ctx);
                if !l0_labels.is_empty() {
                    l0_labels
                } else {
                    uni_store::storage::main_vertex::MainVertexDataset::find_labels_by_vid(
                        self.storage.lancedb_store(),
                        source_vid,
                    )
                    .await?
                    .unwrap_or_default()
                }
            } else {
                uni_store::storage::main_vertex::MainVertexDataset::find_labels_by_vid(
                    self.storage.lancedb_store(),
                    source_vid,
                )
                .await?
                .unwrap_or_default()
            };

            // BFS state: (current_vid, depth, path_edges, used_edge_ids, path_nodes)
            // path_edges: Vec<(Eid, edge_type, src, dst, props)>
            // path_nodes: Vec<(Vid, label, props)>
            type BfsState = (
                Vid,
                usize,
                Vec<(Eid, String, Vid, Vid, HashMap<String, Value>)>,
                HashSet<u64>,
                Vec<(Vid, String, HashMap<String, Value>)>,
            );

            let mut queue: VecDeque<BfsState> = VecDeque::new();
            let mut found = false;

            // Initialize BFS with source node
            let initial_nodes = vec![(source_vid, source_labels.join(":"), source_props)];
            queue.push_back((source_vid, 0, Vec::new(), HashSet::new(), initial_nodes));

            // Zero-length path: emit result if min_hops == 0
            if min_hops == 0 {
                found = true;
                let mut new_row = input_row.clone();

                // Target is same as source
                let mut target_json = input_row
                    .get(source_variable)
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                target_json.insert("_vid".to_string(), Value::Int(source_vid.as_u64() as i64));
                new_row.insert(target_variable.to_string(), Value::Map(target_json.clone()));

                // Empty edge list for step_variable
                if let Some(sv) = step_variable {
                    new_row.insert(sv.clone(), Value::List(Vec::new()));
                }

                // Path with just the source node
                if let Some(pv) = path_variable {
                    let path = crate::types::Path {
                        nodes: vec![crate::types::Node {
                            vid: source_vid,
                            labels: source_labels.clone(),
                            properties: target_json
                                .into_iter()
                                .filter(|(k, _)| !k.starts_with('_'))
                                .collect(),
                        }],
                        edges: vec![],
                    };
                    new_row.insert(pv.clone(), Value::Path(path));
                }

                new_matches.push(new_row);
            }

            // BFS traversal
            while let Some((current_vid, depth, path_edges, used_edges, path_nodes)) =
                queue.pop_front()
            {
                if depth >= max_hops {
                    continue;
                }

                // Get neighbors based on direction
                let neighbors: Vec<_> = match direction {
                    Direction::Outgoing => outgoing
                        .get(&current_vid)
                        .map(|v| v.as_slice())
                        .unwrap_or(&[])
                        .to_vec(),
                    Direction::Incoming => incoming
                        .get(&current_vid)
                        .map(|v| v.as_slice())
                        .unwrap_or(&[])
                        .to_vec(),
                    Direction::Both => {
                        let mut combined = Vec::new();
                        if let Some(out) = outgoing.get(&current_vid) {
                            combined.extend(out.iter().cloned());
                        }
                        if let Some(inc) = incoming.get(&current_vid) {
                            combined.extend(inc.iter().cloned());
                        }
                        combined
                    }
                };

                for (eid, neighbor_vid, edge_type, edge_props) in neighbors {
                    // Relationship uniqueness: skip if edge already used in this path
                    if used_edges.contains(&eid.as_u64()) {
                        continue;
                    }

                    let new_depth = depth + 1;

                    // Build edge info for path
                    // Determine actual src/dst based on direction
                    let (actual_src, actual_dst) = match direction {
                        Direction::Outgoing => (current_vid, neighbor_vid),
                        Direction::Incoming => (neighbor_vid, current_vid),
                        Direction::Both => {
                            // For Both direction, check which way this edge goes
                            if outgoing
                                .get(&current_vid)
                                .map(|v| v.iter().any(|(e, _, _, _)| *e == eid))
                                .unwrap_or(false)
                            {
                                (current_vid, neighbor_vid)
                            } else {
                                (neighbor_vid, current_vid)
                            }
                        }
                    };

                    let mut new_path_edges = path_edges.clone();
                    new_path_edges.push((
                        eid,
                        edge_type.clone(),
                        actual_src,
                        actual_dst,
                        edge_props,
                    ));

                    let mut new_used_edges = used_edges.clone();
                    new_used_edges.insert(eid.as_u64());

                    // Get neighbor node info
                    let neighbor_props = prop_manager
                        .get_all_vertex_props_with_ctx(neighbor_vid, ctx)
                        .await?
                        .unwrap_or_default();
                    let neighbor_labels = if let Some(ctx) = ctx {
                        let l0_labels =
                            uni_store::runtime::l0_visibility::get_vertex_labels(neighbor_vid, ctx);
                        if !l0_labels.is_empty() {
                            l0_labels
                        } else {
                            uni_store::storage::main_vertex::MainVertexDataset::find_labels_by_vid(
                                self.storage.lancedb_store(),
                                neighbor_vid,
                            )
                            .await?
                            .unwrap_or_default()
                        }
                    } else {
                        uni_store::storage::main_vertex::MainVertexDataset::find_labels_by_vid(
                            self.storage.lancedb_store(),
                            neighbor_vid,
                        )
                        .await?
                        .unwrap_or_default()
                    };

                    let mut new_path_nodes = path_nodes.clone();
                    new_path_nodes.push((
                        neighbor_vid,
                        neighbor_labels.join(":"),
                        neighbor_props.clone(),
                    ));

                    // Emit result if within hop bounds
                    if new_depth >= min_hops && new_depth <= max_hops {
                        found = true;
                        let mut new_row = input_row.clone();

                        // Build target node value
                        let mut target_json = neighbor_props.clone();
                        target_json
                            .insert("_vid".to_string(), Value::Int(neighbor_vid.as_u64() as i64));
                        target_json.insert(
                            "_labels".to_string(),
                            Value::List(
                                neighbor_labels
                                    .iter()
                                    .map(|l| Value::String(l.clone()))
                                    .collect(),
                            ),
                        );
                        new_row.insert(target_variable.to_string(), Value::Map(target_json));

                        // Build step_variable as list of Edge objects
                        if let Some(sv) = step_variable {
                            let edge_list: Vec<Value> = new_path_edges
                                .iter()
                                .map(|(eid, etype, src, dst, props)| {
                                    Value::Edge(crate::types::Edge {
                                        eid: *eid,
                                        edge_type: etype.clone(),
                                        src: *src,
                                        dst: *dst,
                                        properties: props
                                            .iter()
                                            .filter(|(k, _)| !k.starts_with('_'))
                                            .map(|(k, v)| (k.clone(), v.clone()))
                                            .collect(),
                                    })
                                })
                                .collect();
                            new_row.insert(sv.clone(), Value::List(edge_list));
                        }

                        // Build path_variable
                        if let Some(pv) = path_variable {
                            let nodes: Vec<crate::types::Node> = new_path_nodes
                                .iter()
                                .map(|(vid, label, props)| crate::types::Node {
                                    vid: *vid,
                                    labels: label
                                        .split(':')
                                        .filter(|s| !s.is_empty())
                                        .map(String::from)
                                        .collect(),
                                    properties: props
                                        .iter()
                                        .filter(|(k, _)| !k.starts_with('_'))
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect(),
                                })
                                .collect();
                            let edges: Vec<crate::types::Edge> = new_path_edges
                                .iter()
                                .map(|(eid, etype, src, dst, props)| crate::types::Edge {
                                    eid: *eid,
                                    edge_type: etype.clone(),
                                    src: *src,
                                    dst: *dst,
                                    properties: props
                                        .iter()
                                        .filter(|(k, _)| !k.starts_with('_'))
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect(),
                                })
                                .collect();
                            new_row.insert(
                                pv.clone(),
                                Value::Path(crate::types::Path { nodes, edges }),
                            );
                        }

                        new_matches.push(new_row);
                    }

                    // Continue BFS if we can still expand
                    if new_depth < max_hops {
                        queue.push_back((
                            neighbor_vid,
                            new_depth,
                            new_path_edges,
                            new_used_edges,
                            new_path_nodes,
                        ));
                    }
                }
            }

            // Handle OPTIONAL MATCH with no results
            if !found && optional {
                let mut new_m = input_row.clone();
                if optional_pattern_vars.is_empty() {
                    new_m.insert(target_variable.to_string(), Value::Null);
                    if let Some(sv) = step_variable {
                        new_m.insert(sv.clone(), Value::Null);
                    }
                    if let Some(pv) = path_variable {
                        new_m.insert(pv.clone(), Value::Null);
                    }
                } else {
                    for var in optional_pattern_vars {
                        new_m.insert(var.clone(), Value::Null);
                    }
                }
                new_matches.push(new_m);
            }
        }

        Ok(new_matches)
    }

    /// Finds the shortest path between two vertices using BFS.
    ///
    /// Returns the path as a vector of VIDs from source to target (inclusive),
    /// or None if no path exists.
    async fn find_shortest_path(
        &self,
        source: Vid,
        target: Vid,
        edge_type_ids: &[u32],
        direction: &Direction,
        max_hops: u32,
        ctx: Option<&QueryContext>,
    ) -> Option<Vec<Vid>> {
        if source == target {
            return Some(vec![source]);
        }

        let graph_dir = Self::map_to_store_direction(direction);
        let l0_arc_opt = self.get_l0_arc().await;

        // Load subgraph from storage (includes L1/delta data)
        log::debug!(
            "ShortestPath: loading subgraph from source={:?}, edge_types={:?}, max_hops={}, dir={:?}",
            source,
            edge_type_ids,
            max_hops,
            graph_dir
        );
        let graph = match self
            .storage
            .load_subgraph_cached(
                &[source],
                edge_type_ids,
                max_hops as usize,
                graph_dir,
                l0_arc_opt,
            )
            .await
        {
            Ok(g) => {
                log::debug!(
                    "ShortestPath: loaded graph with {} vertices, {} outgoing edges",
                    g.vertex_count(),
                    g.edge_count()
                );
                g
            }
            Err(e) => {
                log::debug!("ShortestPath: failed to load subgraph: {}", e);
                return None;
            }
        };

        let mut visited: HashSet<Vid> = HashSet::new();
        let mut queue: VecDeque<(Vid, Vec<Vid>)> = VecDeque::new();

        visited.insert(source);
        queue.push_back((source, vec![source]));

        while let Some((current, path)) = queue.pop_front() {
            // Check timeout
            if let Some(ctx) = ctx
                && ctx.check_timeout().is_err()
            {
                log::debug!("ShortestPath: timeout hit");
                return None;
            }

            // Enforce max hops (path includes source, so edges = path.len() - 1)
            if path.len() > max_hops as usize {
                continue;
            }

            // Get neighbors from the loaded subgraph
            // We need to check outgoing, incoming, or both based on direction
            let directions_to_check: Vec<uni_store::runtime::Direction> = match direction {
                Direction::Outgoing => vec![uni_store::runtime::Direction::Outgoing],
                Direction::Incoming => vec![uni_store::runtime::Direction::Incoming],
                Direction::Both => vec![
                    uni_store::runtime::Direction::Outgoing,
                    uni_store::runtime::Direction::Incoming,
                ],
            };

            // For Direction::Both, deduplicate edges by eid.
            // This prevents the same edge being counted twice (once outgoing, once incoming).
            let mut seen_edges: HashSet<Eid> = HashSet::new();
            let is_undirected = matches!(direction, Direction::Both);

            for dir in &directions_to_check {
                let edges = graph.neighbors(current, *dir);

                for edge in edges {
                    // Filter by edge type if specified
                    if !edge_type_ids.is_empty() && !edge_type_ids.contains(&edge.edge_type) {
                        continue;
                    }

                    // Deduplicate edges for undirected patterns
                    if is_undirected && !seen_edges.insert(edge.eid) {
                        continue;
                    }

                    // Get the neighbor VID based on direction
                    let neighbor = match dir {
                        uni_store::runtime::Direction::Outgoing => edge.dst_vid,
                        uni_store::runtime::Direction::Incoming => edge.src_vid,
                    };

                    if neighbor == target {
                        // Found the target
                        let mut result = path.clone();
                        result.push(target);
                        return Some(result);
                    }

                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        let mut new_path = path.clone();
                        new_path.push(neighbor);
                        queue.push_back((neighbor, new_path));
                    }
                }
            }
        }

        None // No path found
    }

    /// Extracts a VID from a row for a given variable name.
    ///
    /// Tries multiple strategies:
    /// 1. Look for "variable._vid" key (flat structure)
    /// 2. Look for "variable" key containing an object with "_vid" field
    fn extract_vid_from_row(row: &HashMap<String, Value>, variable: &str) -> Option<Vid> {
        // Strategy 1: Direct "variable._vid" key
        let vid_key = format!("{}._vid", variable);
        if let Some(val) = row.get(&vid_key)
            && let Some(v) = val.as_u64()
        {
            return Some(Vid::from(v));
        }

        // Strategy 2: "variable" is an object with "_vid" inside
        if let Some(val) = row.get(variable)
            && let Ok(vid) = Self::vid_from_value(val)
        {
            return Some(vid);
        }

        None
    }

    fn id_from_value(value: &Value) -> Option<u64> {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|v| (v >= 0).then_some(v as u64)))
            .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok()))
    }

    fn coerce_row_node(row: &HashMap<String, Value>, variable: &str) -> crate::types::Node {
        if let Some(Value::Node(node)) = row.get(variable) {
            return node.clone();
        }

        if let Some(Value::Map(map)) = row.get(variable) {
            let vid = map
                .get("_vid")
                .or_else(|| map.get("vid"))
                .or_else(|| map.get("_id"))
                .and_then(Self::id_from_value)
                .map(Vid::from)
                .unwrap_or_else(|| Vid::from(0u64));

            let mut labels = if let Some(Value::List(items)) = map.get("_labels") {
                items
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            } else if let Some(label) = map.get("_label").and_then(Value::as_str) {
                if label.is_empty() {
                    Vec::new()
                } else {
                    vec![label.to_string()]
                }
            } else if let Some(label) = map.get("label").and_then(Value::as_str) {
                if label.is_empty() {
                    Vec::new()
                } else {
                    vec![label.to_string()]
                }
            } else {
                Vec::new()
            };
            labels.sort();

            let properties = if let Some(Value::Map(props)) = map.get("properties") {
                props.clone()
            } else {
                map.iter()
                    .filter_map(|(k, v)| (!k.starts_with('_')).then_some((k.clone(), v.clone())))
                    .collect::<HashMap<_, _>>()
            };

            return crate::types::Node {
                vid,
                labels,
                properties,
            };
        }

        let vid = row
            .get(&format!("{}._vid", variable))
            .or_else(|| row.get(&format!("{}._id", variable)))
            .and_then(Self::id_from_value)
            .map(Vid::from)
            .unwrap_or_else(|| Vid::from(0u64));

        let labels = if let Some(Value::List(items)) = row.get(&format!("{}._labels", variable)) {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        } else if let Some(label) = row
            .get(&format!("{}._label", variable))
            .and_then(Value::as_str)
        {
            if label.is_empty() {
                Vec::new()
            } else {
                vec![label.to_string()]
            }
        } else {
            Vec::new()
        };

        let prefix = format!("{variable}.");
        let properties = row
            .iter()
            .filter_map(|(k, v)| {
                if !k.starts_with(&prefix) {
                    return None;
                }
                let prop = &k[prefix.len()..];
                if prop.starts_with('_') {
                    return None;
                }
                Some((prop.to_string(), v.clone()))
            })
            .collect::<HashMap<_, _>>();

        crate::types::Node {
            vid,
            labels,
            properties,
        }
    }

    fn coerce_row_edge(row: &HashMap<String, Value>, variable: &str) -> crate::types::Edge {
        if let Some(Value::Edge(edge)) = row.get(variable) {
            return edge.clone();
        }

        if let Some(Value::Map(map)) = row.get(variable) {
            let eid = map
                .get("_eid")
                .or_else(|| map.get("eid"))
                .or_else(|| map.get("_id"))
                .and_then(Self::id_from_value)
                .map(Eid::from)
                .unwrap_or_else(|| Eid::from(0u64));
            let src = map
                .get("_src")
                .or_else(|| map.get("src"))
                .and_then(Self::id_from_value)
                .map(Vid::from)
                .unwrap_or_else(|| Vid::from(0u64));
            let dst = map
                .get("_dst")
                .or_else(|| map.get("dst"))
                .and_then(Self::id_from_value)
                .map(Vid::from)
                .unwrap_or_else(|| Vid::from(0u64));
            let edge_type = map
                .get("_type")
                .or_else(|| map.get("_type_name"))
                .or_else(|| map.get("edge_type"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();

            let properties = if let Some(Value::Map(props)) = map.get("properties") {
                props.clone()
            } else {
                map.iter()
                    .filter_map(|(k, v)| (!k.starts_with('_')).then_some((k.clone(), v.clone())))
                    .collect::<HashMap<_, _>>()
            };

            return crate::types::Edge {
                eid,
                edge_type,
                src,
                dst,
                properties,
            };
        }

        let prefix = format!("{variable}.");
        let eid = row
            .get(&format!("{}._eid", variable))
            .or_else(|| row.get(&format!("{}._id", variable)))
            .and_then(Self::id_from_value)
            .map(Eid::from)
            .unwrap_or_else(|| Eid::from(0u64));
        let src = row
            .get(&format!("{}._src", variable))
            .and_then(Self::id_from_value)
            .map(Vid::from)
            .unwrap_or_else(|| Vid::from(0u64));
        let dst = row
            .get(&format!("{}._dst", variable))
            .and_then(Self::id_from_value)
            .map(Vid::from)
            .unwrap_or_else(|| Vid::from(0u64));
        let edge_type = row
            .get(&format!("{}._type", variable))
            .or_else(|| row.get(&format!("{}._type_name", variable)))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();

        let properties = row
            .iter()
            .filter_map(|(k, v)| {
                if !k.starts_with(&prefix) {
                    return None;
                }
                let prop = &k[prefix.len()..];
                if prop.starts_with('_') {
                    return None;
                }
                Some((prop.to_string(), v.clone()))
            })
            .collect::<HashMap<_, _>>();

        crate::types::Edge {
            eid,
            edge_type,
            src,
            dst,
            properties,
        }
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

    /// Performs BFS traversal from a single row, collecting matching results.
    ///
    /// # Errors
    ///
    /// Returns an error if the traversal times out or encounters a storage error.
    #[expect(
        clippy::too_many_arguments,
        reason = "Graph traversal requires many parameters"
    )]
    pub(crate) async fn traverse_from_row(
        &self,
        row: &HashMap<String, Value>,
        edge_type_ids: &[u32],
        direction: &Direction,
        source_variable: &str,
        target_variable: &str,
        target_label_id: u16,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        path_variable: &Option<String>,
        new_matches: &mut Vec<HashMap<String, Value>>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<bool> {
        let source_vid = match row
            .get(source_variable)
            .and_then(|v| Self::vid_from_value(v).ok())
        {
            Some(v) => v,
            None => return Ok(false),
        };

        let l0_arc_opt = self.get_l0_arc().await;

        // For Direction::Both, we need to load both outgoing and incoming subgraphs
        // and merge them, since load_subgraph_cached only supports one direction at a time
        let graph = if matches!(direction, Direction::Both) {
            // Load outgoing edges
            let outgoing_graph = self
                .storage
                .load_subgraph_cached(
                    &[source_vid],
                    edge_type_ids,
                    max_hops,
                    uni_store::runtime::Direction::Outgoing,
                    l0_arc_opt.clone(),
                )
                .await?;

            // Load incoming edges
            let incoming_graph = self
                .storage
                .load_subgraph_cached(
                    &[source_vid],
                    edge_type_ids,
                    max_hops,
                    uni_store::runtime::Direction::Incoming,
                    l0_arc_opt,
                )
                .await?;

            // Merge the two graphs by copying vertices and edges from incoming to outgoing
            let mut merged = outgoing_graph;

            // Track edges already in the outgoing graph to avoid duplicates from bidirectional patterns
            let mut seen_edges = std::collections::HashSet::new();
            for edge in merged.edges() {
                seen_edges.insert(edge.eid);
            }

            for vid in incoming_graph.vertices() {
                merged.add_vertex(vid);
            }
            for edge in incoming_graph.edges() {
                // Only add edge if not already present in merged graph
                if seen_edges.insert(edge.eid) {
                    merged.add_edge(edge.src_vid, edge.dst_vid, edge.eid, edge.edge_type);
                }
            }
            merged
        } else {
            let graph_dir = Self::map_to_store_direction(direction);
            self.storage
                .load_subgraph_cached(
                    &[source_vid],
                    edge_type_ids,
                    max_hops,
                    graph_dir,
                    l0_arc_opt,
                )
                .await?
        };

        if !graph.contains_vertex(source_vid) {
            return Ok(false);
        }

        let found = self.bfs_traverse(
            row,
            &graph,
            source_vid,
            edge_type_ids,
            direction,
            target_variable,
            target_label_id,
            step_variable,
            min_hops,
            max_hops,
            path_variable,
            new_matches,
            ctx,
        )?;

        // Hydrate target node and edge properties for single-hop traversals
        // (Variable-length paths return path objects instead, handled below)
        if max_hops == 1 && !new_matches.is_empty() {
            for m in new_matches.iter_mut() {
                // Hydrate target node properties
                if let Some(Value::Map(target_obj)) = m.get_mut(target_variable) {
                    hydrate_entity_if_needed(target_obj, prop_manager, ctx).await;
                }
                // Hydrate edge properties if step variable is present
                if let Some(sv) = step_variable
                    && let Some(Value::Map(edge_obj)) = m.get_mut(sv)
                {
                    hydrate_entity_if_needed(edge_obj, prop_manager, ctx).await;
                }
            }
        }

        // Populate path properties if path variable is present and we found matches
        if let Some(pv) = path_variable
            && !new_matches.is_empty()
        {
            self.populate_path_properties(new_matches, pv, prop_manager, ctx)
                .await?;
        }

        Ok(found)
    }

    /// Interval for timeout checks in BFS loop to avoid excessive overhead.
    const BFS_TIMEOUT_CHECK_INTERVAL: usize = 100;

    /// BFS traversal core logic with timeout enforcement.
    ///
    /// # Errors
    ///
    /// Returns an error if the query times out during traversal.
    ///
    /// # Security
    ///
    /// **CWE-400 (Resource Consumption)**: Periodic timeout checks prevent
    /// unbounded traversal on large graphs with high fan-out.
    #[expect(
        clippy::too_many_arguments,
        reason = "Graph traversal requires many parameters"
    )]
    pub(crate) fn bfs_traverse(
        &self,
        row: &HashMap<String, Value>,
        graph: &uni_store::runtime::WorkingGraph,
        source_vid: Vid,
        edge_type_ids: &[u32],
        direction: &Direction,
        target_variable: &str,
        target_label_id: u16,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        path_variable: &Option<String>,
        new_matches: &mut Vec<HashMap<String, Value>>,
        ctx: Option<&QueryContext>,
    ) -> Result<bool> {
        let bound_target_vid = row
            .get(target_variable)
            .and_then(|v| Self::vid_from_value(v).ok());

        // Get target label name from schema for label filtering in variable-length traversals
        let schema = self.storage.schema_manager().schema();
        let target_label_name = schema.label_name_by_id(target_label_id);

        // Get previously used edges from the row for relationship uniqueness across hops
        let used_edges_from_previous_hops: std::collections::HashSet<u64> =
            if let Some(Value::List(arr)) = row.get("__used_edges") {
                arr.iter().filter_map(|v| v.as_u64()).collect()
            } else {
                std::collections::HashSet::new()
            };

        let mut found_neighbor = false;
        let mut visited = HashMap::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((source_vid, 0, Vec::new()));
        visited.insert(source_vid, 0);

        let mut iteration_count = 0usize;

        while let Some((curr, depth, path)) = queue.pop_front() {
            // Periodic timeout check to prevent unbounded traversal
            iteration_count += 1;
            if iteration_count.is_multiple_of(Self::BFS_TIMEOUT_CHECK_INTERVAL)
                && let Some(ctx) = ctx
            {
                ctx.check_timeout()?;
            }

            if depth >= max_hops {
                continue;
            }

            for (next, edge_entry) in
                self.collect_incident_edges(graph, curr, edge_type_ids, direction)
            {
                // Skip edges already used in previous hops (relationship uniqueness)
                if used_edges_from_previous_hops.contains(&edge_entry.eid.as_u64()) {
                    continue;
                }

                if path.contains(&edge_entry.eid) {
                    continue;
                }

                let mut new_path = path.clone();
                new_path.push(edge_entry.eid);

                if Self::should_visit_vertex(&visited, next, depth, step_variable) {
                    visited.insert(next, depth + 1);
                    queue.push_back((next, depth + 1, new_path.clone()));

                    if Self::is_valid_target(
                        next,
                        depth + 1,
                        min_hops,
                        target_label_id,
                        bound_target_vid,
                        target_label_name,
                        ctx,
                    ) {
                        found_neighbor = true;
                        // Look up edge type name from schema
                        let edge_type_name =
                            schema.edge_type_name_by_id_unified(edge_entry.edge_type);

                        // For schemaless labels (target_label_id == 0), look up actual label from L0
                        // since schema.label_name_by_id(0) returns None
                        let actual_target_label: Option<String> = if target_label_id == 0 {
                            if let Some(query_ctx) = ctx {
                                let vertex_labels =
                                    l0_visibility::get_vertex_labels(next, query_ctx);
                                if !vertex_labels.is_empty() {
                                    Some(vertex_labels.join(":"))
                                } else {
                                    None // Will be looked up from storage if needed
                                }
                            } else {
                                None
                            }
                        } else {
                            target_label_name.map(|s| s.to_string())
                        };

                        let new_m = Self::build_traverse_match(
                            row,
                            target_variable,
                            next,
                            step_variable,
                            &new_path,
                            &edge_entry,
                            curr,
                            max_hops,
                            path_variable,
                            source_vid,
                            graph,
                            actual_target_label.as_deref(),
                            edge_type_name.as_deref(),
                        );
                        new_matches.push(new_m);
                    }
                }
            }
        }

        Ok(found_neighbor)
    }

    /// Check if a vertex should be visited during BFS.
    pub(crate) fn should_visit_vertex(
        visited: &HashMap<Vid, usize>,
        next: Vid,
        depth: usize,
        step_variable: &Option<String>,
    ) -> bool {
        step_variable.is_some() || !visited.contains_key(&next) || visited[&next] > depth + 1
    }

    /// Check if a vertex is a valid target for the traversal.
    ///
    /// Validates both the depth requirement and label filtering. For variable-length
    /// traversals, we need to check that target vertices have the expected label
    /// since VIDs no longer embed label information.
    pub(crate) fn is_valid_target(
        vid: Vid,
        current_depth: usize,
        min_hops: usize,
        target_label_id: u16,
        bound_target_vid: Option<Vid>,
        target_label_name: Option<&str>,
        ctx: Option<&QueryContext>,
    ) -> bool {
        if current_depth < min_hops {
            return false;
        }

        // Check bound target VID if specified
        if let Some(bound_vid) = bound_target_vid {
            return vid == bound_vid;
        }

        // Check label filtering for variable-length traversals.
        // Label ID 0 means no label constraint (any label is valid).
        if target_label_id != 0
            && let (Some(label_name), Some(query_ctx)) = (target_label_name, ctx)
        {
            let vertex_labels = l0_visibility::get_vertex_labels(vid, query_ctx);
            // If we get labels from L0, check they contain the target label.
            // If L0 returns empty, the vertex is in storage (not in L0), so we trust
            // it was already filtered correctly by the dataset scan.
            if !vertex_labels.is_empty() && !vertex_labels.contains(&label_name.to_string()) {
                return false;
            }
        }

        true
    }

    /// Collect incident edges from a graph node based on direction.
    pub(crate) fn collect_incident_edges(
        &self,
        graph: &uni_store::runtime::WorkingGraph,
        curr: Vid,
        edge_type_ids: &[u32],
        direction: &Direction,
    ) -> Vec<(Vid, uni_common::graph::simple_graph::EdgeEntry)> {
        let directions = match direction {
            Direction::Outgoing => vec![uni_store::runtime::Direction::Outgoing],
            Direction::Incoming => vec![uni_store::runtime::Direction::Incoming],
            Direction::Both => vec![
                uni_store::runtime::Direction::Outgoing,
                uni_store::runtime::Direction::Incoming,
            ],
        };

        let mut incident_edges = Vec::new();
        // For Direction::Both, deduplicate edges by eid.
        // This prevents the same edge being counted twice (once outgoing, once incoming).
        let mut seen_edges = std::collections::HashSet::new();
        let is_undirected = matches!(direction, Direction::Both);

        for dir in directions {
            for edge in graph.neighbors(curr, dir) {
                if !edge_type_ids.contains(&edge.edge_type) {
                    continue;
                }
                // Deduplicate edges for undirected patterns
                if is_undirected && !seen_edges.insert(edge.eid) {
                    continue;
                }
                let next = match dir {
                    uni_store::runtime::Direction::Outgoing => edge.dst_vid,
                    uni_store::runtime::Direction::Incoming => edge.src_vid,
                };
                incident_edges.push((next, *edge));
            }
        }
        incident_edges
    }

    /// Map query direction to storage direction.
    /// Note: Direction::Both is handled specially in traverse_from_row by loading both
    /// directions separately, so here we map it to Outgoing as a fallback.
    pub(crate) fn map_to_store_direction(direction: &Direction) -> uni_store::runtime::Direction {
        match direction {
            Direction::Outgoing => uni_store::runtime::Direction::Outgoing,
            Direction::Incoming => uni_store::runtime::Direction::Incoming,
            Direction::Both => uni_store::runtime::Direction::Outgoing,
        }
    }

    /// Get L0 arc from writer or l0_manager.
    pub(crate) async fn get_l0_arc(
        &self,
    ) -> Option<std::sync::Arc<parking_lot::RwLock<uni_store::runtime::L0Buffer>>> {
        if let Some(writer_lock) = &self.writer {
            let writer = writer_lock.read().await;
            Some(writer.l0_manager.get_current())
        } else {
            self.l0_manager.as_ref().map(|l0_mgr| l0_mgr.get_current())
        }
    }

    /// Build a match result for BFS traversal with optional step variable.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_traverse_match(
        row: &HashMap<String, Value>,
        target_variable: &str,
        target_vid: Vid,
        step_variable: &Option<String>,
        path: &[uni_common::core::id::Eid],
        edge_entry: &uni_common::graph::simple_graph::EdgeEntry,
        curr_vid: Vid,
        max_hops: usize,
        path_variable: &Option<String>,
        source_vid: Vid,
        graph: &uni_store::runtime::WorkingGraph,
        target_label_name: Option<&str>,
        edge_type_name: Option<&str>,
    ) -> HashMap<String, Value> {
        let mut new_m = row.clone();

        // Build proper node object for target variable (instead of just VID string)
        let target_obj = Value::Map(HashMap::from([
            ("_vid".to_string(), Value::Int(target_vid.as_u64() as i64)),
            (
                "_labels".to_string(),
                if let Some(lbl) = target_label_name {
                    Value::List(vec![Value::String(lbl.to_string())])
                } else {
                    Value::List(vec![])
                },
            ),
        ]));
        new_m.insert(target_variable.to_string(), target_obj);

        // Track used edges for relationship uniqueness across hops.
        // Add current edge to __used_edges internal tracking.
        let mut used_edges: Vec<u64> = if let Some(Value::List(arr)) = row.get("__used_edges") {
            arr.iter().filter_map(|v| v.as_u64()).collect()
        } else {
            Vec::new()
        };
        used_edges.push(edge_entry.eid.as_u64());
        new_m.insert(
            "__used_edges".to_string(),
            Value::List(
                used_edges
                    .into_iter()
                    .map(|e| Value::Int(e as i64))
                    .collect(),
            ),
        );

        if let Some(sv) = step_variable {
            if max_hops > 1 {
                let eids: Vec<Value> = path.iter().map(|e| Value::Int(e.as_u64() as i64)).collect();
                new_m.insert(sv.clone(), Value::List(eids));
            } else {
                // Build edge object with numeric _type (for internal operations)
                // and _type_name (for user-facing output after normalization)
                let edge_obj = Value::Map(HashMap::from([
                    (
                        "_eid".to_string(),
                        Value::Int(edge_entry.eid.as_u64() as i64),
                    ),
                    ("_src".to_string(), Value::Int(curr_vid.as_u64() as i64)),
                    ("_dst".to_string(), Value::Int(target_vid.as_u64() as i64)),
                    ("_type".to_string(), Value::Int(edge_entry.edge_type as i64)),
                    (
                        "_type_name".to_string(),
                        Value::String(edge_type_name.unwrap_or("").to_string()),
                    ),
                ]));
                new_m.insert(sv.clone(), edge_obj);
            }
        }

        if let Some(pv) = path_variable {
            // Check if there's an existing path in the row (from previous hop in multi-hop pattern)
            // Paths are stored as Value::Path or Value::Map with "nodes" and "relationships" arrays
            let (mut path_nodes, mut path_edges, mut current) =
                if let Some(Value::Path(existing_path)) = row.get(pv) {
                    // Already a proper Path - extract directly
                    let current = existing_path
                        .nodes
                        .last()
                        .map(|n| n.vid)
                        .unwrap_or(source_vid);
                    (
                        existing_path.nodes.clone(),
                        existing_path.edges.clone(),
                        current,
                    )
                } else if let Some(Value::Map(existing_obj)) = row.get(pv) {
                    // Try to parse existing path from Map structure
                    // Path object uses "nodes" and "relationships" (or "edges") keys
                    let edges_key = if existing_obj.contains_key("relationships") {
                        "relationships"
                    } else {
                        "edges"
                    };
                    if let (Some(Value::List(nodes_arr)), Some(Value::List(edges_arr))) =
                        (existing_obj.get("nodes"), existing_obj.get(edges_key))
                    {
                        // Convert Map nodes back to types::Node
                        let mut nodes: Vec<crate::types::Node> = Vec::new();
                        for node_val in nodes_arr {
                            if let Value::Node(n) = node_val {
                                nodes.push(n.clone());
                            } else if let Value::Map(node_obj) = node_val {
                                let vid = node_obj
                                    .get("vid")
                                    .or_else(|| node_obj.get("_id"))
                                    .and_then(|v| {
                                        v.as_u64()
                                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                    })
                                    .map(Vid::from)
                                    .unwrap_or_else(|| Vid::from(0u64));
                                let labels = if let Some(Value::List(labels_arr)) =
                                    node_obj.get("_labels")
                                {
                                    labels_arr
                                        .iter()
                                        .filter_map(|v| v.as_str().map(String::from))
                                        .collect()
                                } else if let Some(label_val) = node_obj.get("label") {
                                    match label_val.as_str() {
                                        Some(s) if !s.is_empty() => vec![s.to_string()],
                                        _ => vec![],
                                    }
                                } else {
                                    vec![]
                                };
                                // Properties are already in the node object
                                let mut properties = HashMap::new();
                                if let Some(Value::Map(props)) = node_obj.get("properties") {
                                    for (k, v) in props {
                                        properties.insert(k.clone(), v.clone());
                                    }
                                }
                                nodes.push(crate::types::Node {
                                    vid,
                                    labels,
                                    properties,
                                });
                            }
                        }
                        // Convert Map edges back to types::Edge
                        let mut edges: Vec<crate::types::Edge> = Vec::new();
                        for edge_val in edges_arr {
                            if let Value::Edge(e) = edge_val {
                                edges.push(e.clone());
                            } else if let Value::Map(edge_obj) = edge_val {
                                let eid = edge_obj
                                    .get("eid")
                                    .or_else(|| edge_obj.get("_id"))
                                    .and_then(|v| {
                                        v.as_u64()
                                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                    })
                                    .map(Eid::from)
                                    .unwrap_or_else(|| Eid::from(0u64));
                                let edge_type = edge_obj
                                    .get("edge_type")
                                    .or_else(|| edge_obj.get("_type"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let src = edge_obj
                                    .get("src")
                                    .or_else(|| edge_obj.get("_src"))
                                    .and_then(|v| {
                                        v.as_u64()
                                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                    })
                                    .map(Vid::from)
                                    .unwrap_or_else(|| Vid::from(0u64));
                                let dst = edge_obj
                                    .get("dst")
                                    .or_else(|| edge_obj.get("_dst"))
                                    .and_then(|v| {
                                        v.as_u64()
                                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                    })
                                    .map(Vid::from)
                                    .unwrap_or_else(|| Vid::from(0u64));
                                edges.push(crate::types::Edge {
                                    eid,
                                    edge_type,
                                    src,
                                    dst,
                                    properties: HashMap::new(),
                                });
                            }
                        }
                        // Current position is the last node in the existing path
                        let current = nodes.last().map(|n| n.vid).unwrap_or(source_vid);
                        (nodes, edges, current)
                    } else {
                        // Existing value is not a valid path structure, start fresh
                        let path_nodes = vec![crate::types::Node {
                            vid: source_vid,
                            labels: vec![],
                            properties: HashMap::new(),
                        }];
                        (path_nodes, Vec::new(), source_vid)
                    }
                } else {
                    // No existing path - start from source_vid
                    let path_nodes = vec![crate::types::Node {
                        vid: source_vid,
                        labels: vec![],
                        properties: HashMap::new(),
                    }];
                    (path_nodes, Vec::new(), source_vid)
                };
            for eid in path {
                // Find edge in graph to get dst
                // WorkingGraph is optimized for out/in edges.
                // We assume path is followed in valid direction.
                // We need to find the edge with 'eid' starting from 'current'.
                // This is slow if we iterate. But 'path' came from traversal.

                // Optimized: We know the sequence of edges.
                // But we don't know the exact target node for each edge without lookup if we only have Eids.
                // However, bfs_traverse built the path.

                // Actually, bfs_traverse `queue` stores `(Vid, depth, Vec<Eid>)`.
                // It doesn't store the intermediate vertices!
                // To reconstruct the path nodes, we need to traverse the edges again.
                // Or change queue to store `Vec<(Eid, Vid)>`?
                // Or since we have the graph, we can lookup edge endpoints.

                // WorkingGraph doesn't index by Eid.
                // So we can't look up edge by Eid efficiently to get next node.
                // We MUST traverse from current.

                // Let's assume we can find the edge in outgoing edges of current.
                // (Or incoming if direction is incoming).

                // For simplicity/correctness, bfs_traverse should probably track the path of vertices too?
                // Or we iterate neighbors of current and find the one with eid.

                let mut next_vid = None;
                // Try both directions? The graph is loaded with direction.
                // We assume consistent direction for the whole path traversal.
                // But we don't have direction passed here easily (it's in caller).
                // Actually, we do pass `edge_entry` which has `edge_type` but not direction used.

                // We can iterate all edges of `current` in graph.

                // Iterate both directions manually as SimpleGraph neighbors doesn't support Both
                let out_edges = graph.neighbors(
                    current,
                    uni_common::graph::simple_graph::Direction::Outgoing,
                );
                let inc_edges = graph.neighbors(
                    current,
                    uni_common::graph::simple_graph::Direction::Incoming,
                );

                for edge in out_edges.iter().chain(inc_edges.iter()) {
                    if edge.eid == *eid {
                        let neighbor = if edge.src_vid == current {
                            edge.dst_vid
                        } else {
                            edge.src_vid
                        };
                        next_vid = Some(neighbor);

                        path_edges.push(crate::types::Edge {
                            eid: *eid,
                            edge_type: String::new(), // Unknown name
                            src: edge.src_vid,
                            dst: edge.dst_vid,
                            properties: HashMap::new(),
                        });

                        path_nodes.push(crate::types::Node {
                            vid: neighbor,
                            labels: vec![],
                            properties: HashMap::new(),
                        });

                        break;
                    }
                }

                // Remove unused get_edges block if present (it was not in the replace range, but I should ensure it's clean)
                // The previous code had `if let Some(edges) = graph.get_edges(current)` block.
                // I am replacing the block that contained `for (neighbor, edge) in graph.neighbors(...)`.

                if let Some(next) = next_vid {
                    current = next;
                } else {
                    // Should not happen if graph is consistent
                    log::warn!("Could not find next node for edge {} from {}", eid, current);
                }
            }

            let path_obj = crate::types::Path {
                nodes: path_nodes,
                edges: path_edges,
            };

            new_m.insert(pv.clone(), Value::Path(path_obj));
        }

        new_m
    }

    /// Populate labels, edge types, and properties for all paths in matches.
    ///
    /// This function post-processes path results from BFS traversal to enrich
    /// Node and Edge objects with their actual properties, labels, and edge types.
    ///
    /// # Errors
    ///
    /// Returns an error if property loading fails.
    /// Populates node and relationship properties for path objects in match results.
    ///
    /// This function enriches path data with labels, types, and properties by:
    /// 1. Collecting all unique VIDs and EIDs from paths
    /// 2. Batch loading properties from the property manager
    /// 3. Looking up labels (for nodes) and types (for relationships)
    /// 4. Updating each path object with the loaded data
    async fn populate_path_properties(
        &self,
        matches: &mut [HashMap<String, Value>],
        path_variable: &str,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        // Collect all unique VIDs and EIDs from all paths
        let (all_vids, all_eids) = Self::collect_path_ids(matches, path_variable);

        if all_vids.is_empty() && all_eids.is_empty() {
            return Ok(());
        }

        let vids_vec: Vec<Vid> = all_vids.into_iter().collect();
        let eids_vec: Vec<Eid> = all_eids.into_iter().collect();

        // Load properties in batch
        let schema = self.storage.schema_manager().schema();
        let vertex_props = self
            .batch_load_vertex_props(&vids_vec, &schema, prop_manager, ctx)
            .await?;
        let edge_props = self
            .batch_load_edge_props(&eids_vec, &schema, prop_manager, ctx)
            .await?;

        // Build lookup maps for labels and types
        let vid_labels = self.build_vertex_label_map(&vids_vec, ctx).await;
        let eid_types = self.build_edge_type_map(&eids_vec, ctx).await;

        // Update all path objects with loaded data
        for m in matches.iter_mut() {
            match m.get_mut(path_variable) {
                Some(Value::Path(path)) => {
                    // Update nodes in Value::Path directly
                    for node in &mut path.nodes {
                        if let Some(label) = vid_labels.get(&node.vid) {
                            node.labels = label
                                .split(':')
                                .filter(|s| !s.is_empty())
                                .map(String::from)
                                .collect();
                        }
                        if let Some(props) = vertex_props.get(&node.vid) {
                            node.properties = props.clone();
                        }
                    }
                    // Update edges in Value::Path directly
                    for edge in &mut path.edges {
                        if let Some(type_name) = eid_types.get(&edge.eid) {
                            edge.edge_type = type_name.clone();
                        }
                        let vid_key = Vid::from(edge.eid.as_u64());
                        if let Some(props) = edge_props.get(&vid_key) {
                            edge.properties = props.clone();
                        }
                    }
                }
                Some(Value::Map(path_map)) => {
                    // Handle Value::Map (legacy format)
                    Self::update_path_nodes(path_map, &vid_labels, &vertex_props);
                    Self::update_path_relationships(path_map, &eid_types, &edge_props);
                }
                _ => continue,
            }
        }

        Ok(())
    }

    /// Collects all unique VIDs and EIDs from path objects in match results.
    fn collect_path_ids(
        matches: &[HashMap<String, Value>],
        path_variable: &str,
    ) -> (HashSet<Vid>, HashSet<Eid>) {
        let mut all_vids = HashSet::new();
        let mut all_eids = HashSet::new();

        for m in matches {
            match m.get(path_variable) {
                Some(Value::Path(path)) => {
                    // Handle Value::Path directly
                    for node in &path.nodes {
                        all_vids.insert(node.vid);
                    }
                    for edge in &path.edges {
                        all_eids.insert(edge.eid);
                    }
                }
                Some(Value::Map(path_map)) => {
                    // Handle Value::Map (legacy format)
                    if let Some(Value::List(nodes)) = path_map.get("nodes") {
                        for node in nodes {
                            if let Some(vid_val) = node.get("_id")
                                && let Ok(vid) = Self::vid_from_value(vid_val)
                            {
                                all_vids.insert(vid);
                            }
                        }
                    }

                    if let Some(Value::List(relationships)) = path_map.get("relationships") {
                        for rel in relationships {
                            if let Some(eid_val) = rel.get("_id")
                                && let Ok(vid) = Self::vid_from_value(eid_val)
                            {
                                all_eids.insert(Eid::from(vid.as_u64()));
                            }
                        }
                    }
                }
                _ => continue,
            }
        }

        (all_vids, all_eids)
    }

    /// Batch loads vertex properties for a set of VIDs.
    async fn batch_load_vertex_props(
        &self,
        vids: &[Vid],
        schema: &uni_common::core::schema::Schema,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Vid, HashMap<String, Value>>> {
        if vids.is_empty() {
            return Ok(HashMap::new());
        }

        // Collect unique property names across all vertex labels
        let all_props: HashSet<&str> = schema
            .labels
            .keys()
            .filter_map(|label| schema.properties.get(label))
            .flat_map(|props| props.keys().map(String::as_str))
            .collect();

        let prop_refs: Vec<&str> = all_props.into_iter().collect();
        let result = prop_manager
            .get_batch_vertex_props(vids, &prop_refs, ctx)
            .await?;
        Ok(result)
    }

    /// Batch loads edge properties for a set of EIDs.
    async fn batch_load_edge_props(
        &self,
        eids: &[Eid],
        schema: &uni_common::core::schema::Schema,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Vid, HashMap<String, Value>>> {
        if eids.is_empty() {
            return Ok(HashMap::new());
        }

        // Collect unique property names across all edge types
        let all_props: HashSet<&str> = schema
            .edge_types
            .keys()
            .filter_map(|type_name| schema.properties.get(type_name))
            .flat_map(|props| props.keys().map(String::as_str))
            .collect();

        let prop_refs: Vec<&str> = all_props.into_iter().collect();
        prop_manager
            .get_batch_edge_props(eids, &prop_refs, ctx)
            .await
    }

    /// Builds a mapping from VID to label string.
    ///
    /// First checks L0 buffer, then falls back to storage scan.
    async fn build_vertex_label_map(
        &self,
        vids: &[Vid],
        ctx: Option<&QueryContext>,
    ) -> HashMap<Vid, String> {
        let mut vid_labels = HashMap::new();
        let schema = self.storage.schema_manager().schema();

        for &vid in vids {
            // Check L0 buffer first
            if let Some(ctx) = ctx {
                let labels = l0_visibility::get_vertex_labels(vid, ctx);
                if !labels.is_empty() {
                    vid_labels.insert(vid, labels.join(":"));
                    continue;
                }
            }

            // Fall back to storage scan
            if let Some(label) = self.find_vertex_label_in_storage(vid, &schema).await {
                vid_labels.insert(vid, label);
            }
        }

        vid_labels
    }

    /// Searches storage to find which label dataset contains a VID.
    async fn find_vertex_label_in_storage(
        &self,
        vid: Vid,
        schema: &uni_common::core::schema::Schema,
    ) -> Option<String> {
        use futures::TryStreamExt;
        use lancedb::query::{ExecutableQuery, QueryBase, Select};

        for label_name in schema.labels.keys() {
            let ds = self.storage.vertex_dataset(label_name).ok()?;
            let lancedb_store = self.storage.lancedb_store();
            let table = ds.open_lancedb(lancedb_store).await.ok()?;

            let filter = format!("_vid = {}", vid.as_u64());
            let query_result = table
                .query()
                .only_if(filter)
                .select(Select::Columns(vec!["_vid".to_string()]))
                .execute()
                .await
                .ok()?;

            let batches: Vec<_> = query_result.try_collect().await.ok()?;
            if !batches.is_empty() && batches[0].num_rows() > 0 {
                return Some(label_name.clone());
            }
        }

        None
    }

    /// Builds a mapping from EID to edge type string.
    ///
    /// First checks L0 buffer, then falls back to storage scan.
    async fn build_edge_type_map(
        &self,
        eids: &[Eid],
        ctx: Option<&QueryContext>,
    ) -> HashMap<Eid, String> {
        let mut eid_types = HashMap::new();
        let schema = self.storage.schema_manager().schema();

        for &eid in eids {
            // Check L0 buffer first
            if let Some(ctx) = ctx
                && let Some(edge_type) = l0_visibility::get_edge_type(eid, ctx)
            {
                eid_types.insert(eid, edge_type);
                continue;
            }

            // Fall back to storage scan
            if let Some(edge_type) = self.find_edge_type_in_storage(eid, &schema).await {
                eid_types.insert(eid, edge_type);
            }
        }

        eid_types
    }

    /// Searches storage to find which edge type dataset contains an EID.
    async fn find_edge_type_in_storage(
        &self,
        eid: Eid,
        schema: &uni_common::core::schema::Schema,
    ) -> Option<String> {
        use futures::TryStreamExt;
        use lancedb::query::{ExecutableQuery, QueryBase, Select};

        for type_name in schema.edge_types.keys() {
            let delta_ds = self.storage.delta_dataset(type_name, "fwd").ok()?;
            let lancedb_store = self.storage.lancedb_store();
            let table = delta_ds.open_lancedb(lancedb_store).await.ok()?;

            let filter = format!("eid = {}", eid.as_u64());
            let query_result = table
                .query()
                .only_if(filter)
                .select(Select::Columns(vec!["eid".to_string()]))
                .execute()
                .await
                .ok()?;

            let batches: Vec<_> = query_result.try_collect().await.ok()?;
            if !batches.is_empty() && batches[0].num_rows() > 0 {
                return Some(type_name.clone());
            }
        }

        None
    }

    /// Updates node objects in a path with labels and properties.
    fn update_path_nodes(
        path_map: &mut HashMap<String, Value>,
        vid_labels: &HashMap<Vid, String>,
        vertex_props: &HashMap<Vid, HashMap<String, Value>>,
    ) {
        let Some(Value::List(nodes)) = path_map.get_mut("nodes") else {
            return;
        };

        for node in nodes {
            let Value::Map(node_obj) = node else {
                continue;
            };
            let Some(vid_val) = node_obj.get("_id") else {
                continue;
            };
            let Ok(vid) = Self::vid_from_value(vid_val) else {
                continue;
            };

            if let Some(label) = vid_labels.get(&vid) {
                node_obj.insert(
                    "_labels".to_string(),
                    Value::List(
                        label
                            .split(':')
                            .filter(|s| !s.is_empty())
                            .map(|s| Value::String(s.to_string()))
                            .collect(),
                    ),
                );
            }

            if let Some(props) = vertex_props.get(&vid) {
                node_obj.insert("properties".to_string(), Self::props_to_value(props));
            }
        }
    }

    /// Updates relationship objects in a path with types and properties.
    fn update_path_relationships(
        path_map: &mut HashMap<String, Value>,
        eid_types: &HashMap<Eid, String>,
        edge_props: &HashMap<Vid, HashMap<String, Value>>,
    ) {
        let Some(Value::List(relationships)) = path_map.get_mut("relationships") else {
            return;
        };

        for rel in relationships {
            let Value::Map(rel_obj) = rel else {
                continue;
            };
            let Some(eid_val) = rel_obj.get("_id") else {
                continue;
            };
            let Ok(eid_vid) = Self::vid_from_value(eid_val) else {
                continue;
            };
            let eid = Eid::from(eid_vid.as_u64());

            if let Some(edge_type) = eid_types.get(&eid) {
                rel_obj.insert("_type".to_string(), Value::String(edge_type.clone()));
            }

            // Edge props use Vid as key (converted from Eid)
            let vid_key = Vid::from(eid.as_u64());
            if let Some(props) = edge_props.get(&vid_key) {
                rel_obj.insert("properties".to_string(), Self::props_to_value(props));
            }
        }
    }

    /// Converts a properties HashMap to a Value::Map.
    fn props_to_value(props: &HashMap<String, Value>) -> Value {
        Value::Map(props.clone())
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
    #[allow(clippy::only_used_in_recursion)]
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

    /// Execute a vector KNN search.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_vector_knn(
        &self,
        label_id: u16,
        variable: &str,
        property: &str,
        query: &Expr,
        k: usize,
        threshold: Option<f32>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let empty_row = HashMap::new();
        let query_val = self
            .evaluate_expr(query, &empty_row, prop_manager, params, ctx)
            .await?;

        let query_vector: Vec<f32> = match query_val {
            Value::Vector(v) => v,
            Value::List(arr) => {
                let mut vec = Vec::with_capacity(arr.len());
                for v in arr {
                    if let Some(f) = v.as_f64() {
                        vec.push(f as f32);
                    } else {
                        return Err(anyhow!("Query vector must contain numbers"));
                    }
                }
                vec
            }
            _ => return Err(anyhow!("Query vector must be an array")),
        };

        let schema = self.storage.schema_manager().schema();
        let label_name = schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

        let results = self
            .storage
            .vector_search(label_name, property, &query_vector, k, None, ctx)
            .await?;

        let mut matches = Vec::new();
        for (vid, dist) in results {
            if let Some(thresh) = threshold {
                // Convert distance to similarity (assuming Cosine/Dot)
                // TODO: Check index metric from schema for precise conversion
                let sim = 1.0 - dist;
                if sim < thresh {
                    continue;
                }
            }
            let mut m = HashMap::new();
            m.insert(variable.to_string(), Value::String(vid.to_string()));
            matches.push(m);
        }
        Ok(matches)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_inverted_index_lookup(
        &self,
        label_id: u16,
        variable: &str,
        property: &str,
        terms_expr: &Expr,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let empty_row = HashMap::new();
        let terms_val = self
            .evaluate_expr(terms_expr, &empty_row, prop_manager, params, ctx)
            .await?;

        let terms: Vec<String> = match terms_val {
            Value::List(arr) => arr
                .iter()
                .map(|v| v.as_str().map(|s| s.to_string()).unwrap_or_default())
                .collect(),
            _ => return Err(anyhow!("Terms must be a list")),
        };

        let schema = self.storage.schema_manager().schema();
        let label_name = schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

        let index = self.storage.inverted_index(label_name, property).await?;
        let vids = index.query_any(&terms).await?;

        let mut matches = Vec::with_capacity(vids.len());
        for vid in vids {
            // Check visibility/deletion and fetch properties
            // We use get_all_vertex_props_with_ctx to respect transaction isolation
            let props_opt = prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await?;
            if let Some(mut props_json) = props_opt {
                props_json.insert("_vid".to_string(), Value::Int(vid.as_u64() as i64));
                props_json.insert(
                    "_labels".to_string(),
                    Value::List(vec![Value::String(label_name.to_string())]),
                );

                let mut row = HashMap::new();
                row.insert(variable.to_string(), Value::Map(props_json));
                matches.push(row);
            }
        }
        Ok(matches)
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
    #[allow(clippy::too_many_arguments)]
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
        let edge_type_ids: Vec<u32> = schema.edge_types.values().map(|m| m.id).collect();

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
        let edge_type_ids: Vec<u32> = schema.edge_types.values().map(|m| m.id).collect();

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
