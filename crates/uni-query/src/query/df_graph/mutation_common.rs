// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Common infrastructure for DataFusion mutation operators (CREATE, SET, REMOVE, DELETE).
//!
//! Provides:
//! - [`MutationContext`]: Shared context for mutation operators containing executor, writer, etc.
//! - [`batches_to_rows`]: Convert RecordBatches to row-based HashMaps (batch→row direction).
//! - [`rows_to_batches`]: Convert row-based HashMaps back to RecordBatches (row→batch direction).
//! - [`MutationStream`]: Eager-barrier RecordBatchStream that collects all input, applies
//!   mutations via Writer, and yields output batches.

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;
use uni_common::core::id::Vid;
use uni_common::{Path, Value};
use uni_cypher::ast::{Expr, Pattern, PatternElement, RemoveItem, SetClause, SetItem};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::arrow_convert;

use super::common::compute_plan_properties;
use crate::query::executor::core::Executor;

/// Shared context for mutation operators.
///
/// Contains all resources needed to execute write operations from within
/// DataFusion ExecutionPlan operators. The Executor is `Clone` with all
/// Arc-wrapped fields, so cloning it is cheap.
#[derive(Clone)]
pub struct MutationContext {
    /// The query executor (cheap clone, all Arc fields).
    pub executor: Executor,

    /// Writer for graph mutations (vertices, edges, properties).
    pub writer: Arc<RwLock<Writer>>,

    /// Property manager for lazy-loading vertex/edge properties.
    pub prop_manager: Arc<PropertyManager>,

    /// Query parameters (e.g., `$param` references in Cypher).
    pub params: HashMap<String, Value>,

    /// Query context for L0 buffer visibility.
    pub query_ctx: Option<uni_store::QueryContext>,
}

impl std::fmt::Debug for MutationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutationContext")
            .field("has_writer", &true)
            .field("has_prop_manager", &true)
            .field("params_count", &self.params.len())
            .field("has_query_ctx", &self.query_ctx.is_some())
            .finish()
    }
}

/// The kind of mutation to apply per row.
#[derive(Debug, Clone)]
pub enum MutationKind {
    /// CREATE clause: create nodes/edges per the pattern.
    Create { pattern: Pattern },

    /// CREATE with multiple patterns (batched CREATE).
    CreateBatch { patterns: Vec<Pattern> },

    /// SET clause: update properties/labels.
    Set { items: Vec<SetItem> },

    /// REMOVE clause: remove properties/labels.
    Remove { items: Vec<RemoveItem> },

    /// DELETE clause: delete nodes/edges.
    Delete { items: Vec<Expr>, detach: bool },

    /// MERGE clause: match-or-create with optional ON MATCH/ON CREATE actions.
    Merge {
        pattern: Pattern,
        on_match: Option<SetClause>,
        on_create: Option<SetClause>,
    },
}

/// Convert RecordBatches to row-based HashMaps for mutation processing.
///
/// Handles special metadata on fields:
/// - `cv_encoded=true`: Parse string value as JSON to restore original type
/// - DateTime/Time struct types: Decode to temporal values
///
/// NOTE: This does NOT merge system fields (like `n._vid`) into bare variable
/// maps. The raw column names are preserved so that `rows_to_batches` can
/// reconstruct the RecordBatch with the same schema. System field merging
/// happens later in `Executor::record_batches_to_rows()` for user-facing output.
pub fn batches_to_rows(batches: &[RecordBatch]) -> Result<Vec<HashMap<String, Value>>> {
    let mut rows = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        let schema = batch.schema();

        for row_idx in 0..num_rows {
            let mut row = HashMap::new();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                // Infer Uni DataType from Arrow type for DateTime/Time struct decoding
                let data_type = if uni_common::core::schema::is_datetime_struct(field.data_type()) {
                    Some(&uni_common::DataType::DateTime)
                } else if uni_common::core::schema::is_time_struct(field.data_type()) {
                    Some(&uni_common::DataType::Time)
                } else {
                    None
                };
                let mut value = arrow_convert::arrow_to_value(column.as_ref(), row_idx, data_type);

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

                row.insert(field.name().clone(), value);
            }

            // Also merge system fields into bare variable maps for the write helpers.
            // The write helpers (execute_set_items_locked, etc.) expect variables
            // as bare Maps with _vid/_labels inside. We do this AFTER preserving
            // the raw keys so rows_to_batches can reconstruct the schema.
            merge_system_fields_for_write(&mut row);

            rows.push(row);
        }
    }

    Ok(rows)
}

/// After mutations, sync `_all_props` within bare variable Maps from their direct property keys.
///
/// SET/REMOVE modify direct property keys in the bare Map (e.g., `row["n"]["name"] = "Bob"`)
/// but the `_all_props` sub-map retains its stale pre-mutation values. The result normalizer
/// and property UDFs (keys(), properties()) read from `_all_props`, so it must be kept in sync.
///
/// This must be called BEFORE `sync_dotted_columns` so that the dotted `n._all_props` column
/// also gets the updated value.
fn sync_all_props_in_maps(rows: &mut [HashMap<String, Value>]) {
    for row in rows {
        let map_keys: Vec<String> = row
            .keys()
            .filter(|k| !k.contains('.') && matches!(row.get(*k), Some(Value::Map(_))))
            .cloned()
            .collect();

        for key in map_keys {
            if let Some(Value::Map(map)) = row.get_mut(&key)
                && map.contains_key("_all_props")
            {
                // Collect non-internal property keys and their values
                let updates: Vec<(String, Value)> = map
                    .iter()
                    .filter(|(k, _)| !k.starts_with('_') && k.as_str() != "ext_id")
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if !updates.is_empty()
                    && let Some(Value::Map(all_props)) = map.get_mut("_all_props")
                {
                    for (k, v) in updates {
                        all_props.insert(k, v);
                    }
                }
            }
        }
    }
}

/// After mutations, sync dotted property columns from bare variable Maps.
///
/// SET/REMOVE modify the bare Map (e.g., `row["n"]["name"] = "Bob"`) but the
/// dotted column (`row["n.name"]`) retains its stale pre-mutation value.
/// This step overwrites dotted columns from the Map so `rows_to_batches()`
/// produces correct output. Also handles newly created variables from CREATE/MERGE
/// by inserting dotted columns that didn't exist in the input.
fn sync_dotted_columns(rows: &mut [HashMap<String, Value>], schema: &SchemaRef) {
    for row in rows {
        for field in schema.fields() {
            let name = field.name();
            if let Some(dot_pos) = name.find('.') {
                let var_name = &name[..dot_pos];
                let prop_name = &name[dot_pos + 1..];
                if let Some(Value::Map(map)) = row.get(var_name) {
                    let val = map.get(prop_name).cloned().unwrap_or(Value::Null);
                    row.insert(name.clone(), val);
                }
            }
        }
    }
}

/// Normalize edge system field names in a map: `_src_vid` -> `_src`, `_dst_vid` -> `_dst`.
///
/// The write executor expects `_src`/`_dst` but DataFusion traverse emits `_src_vid`/`_dst_vid`.
fn normalize_edge_field_names(map: &mut HashMap<String, Value>) {
    if let Some(val) = map.remove("_src_vid") {
        map.entry("_src".to_string()).or_insert(val);
    }
    if let Some(val) = map.remove("_dst_vid") {
        map.entry("_dst".to_string()).or_insert(val);
    }
}

/// Merge system fields into bare variable maps for write helper consumption.
///
/// The write helpers expect variables like `n` to be a Map containing `_vid`, `_labels`, etc.
/// This merges dotted columns (like `n._vid`, `n._labels`) into the variable Map,
/// while KEEPING the dotted columns in the row so `rows_to_batches` still works.
fn merge_system_fields_for_write(row: &mut HashMap<String, Value>) {
    // Vertex system fields (overwrite into the bare map) and edge system fields
    // (insert only if absent) that should be copied from dotted columns.
    const VERTEX_FIELDS: &[&str] = &["_vid", "_labels"];
    const EDGE_FIELDS: &[&str] = &["_eid", "_type", "_src_vid", "_dst_vid"];

    // Collect all variable names that have dotted columns (var.field).
    let dotted_vars: HashSet<String> = row
        .keys()
        .filter_map(|key| key.find('.').map(|pos| key[..pos].to_string()))
        .collect();

    // For each variable with dotted columns, ensure a bare Map exists.
    // If the variable is only represented via dotted columns (e.g., edge from
    // TraverseMainByType), assemble a Map from those columns.
    for var in &dotted_vars {
        if !row.contains_key(var) {
            let prefix = format!("{var}.");
            let mut map: HashMap<String, Value> = row
                .iter()
                .filter_map(|(k, v)| {
                    k.strip_prefix(prefix.as_str())
                        .map(|field| (field.to_string(), v.clone()))
                })
                .collect();
            normalize_edge_field_names(&mut map);
            if !map.is_empty() {
                row.insert(var.clone(), Value::Map(map));
            }
        }
    }

    // Merge system fields from dotted columns into bare Maps and normalize edge names.
    // Single pass: vertex fields overwrite, edge fields insert-if-absent, then normalize.
    let bare_vars: Vec<String> = row
        .keys()
        .filter(|k| !k.contains('.') && matches!(row.get(*k), Some(Value::Map(_))))
        .cloned()
        .collect();

    for var in &bare_vars {
        // Collect dotted values to merge (avoids borrowing row mutably while reading)
        let vertex_vals: Vec<(&str, Value)> = VERTEX_FIELDS
            .iter()
            .filter_map(|&field| {
                row.get(&format!("{var}.{field}"))
                    .cloned()
                    .map(|v| (field, v))
            })
            .collect();
        let edge_vals: Vec<(&str, Value)> = EDGE_FIELDS
            .iter()
            .filter_map(|&field| {
                row.get(&format!("{var}.{field}"))
                    .cloned()
                    .map(|v| (field, v))
            })
            .collect();

        if let Some(Value::Map(map)) = row.get_mut(var) {
            for (field, v) in vertex_vals {
                map.insert(field.to_string(), v);
            }
            for (field, v) in edge_vals {
                map.entry(field.to_string()).or_insert(v);
            }
            normalize_edge_field_names(map);
        }
    }
}

/// Convert row-based HashMaps back to RecordBatches.
///
/// This is the inverse of `batches_to_rows`. Schema-driven: iterates over the
/// output schema fields and extracts named values from each row HashMap.
///
/// - Entity columns (LargeBinary with `cv_encoded=true`): serialize Map/Node/Edge values
///   to CypherValue binary encoding.
/// - Scalar columns: use `arrow_convert::values_to_array()` for type-appropriate conversion.
pub fn rows_to_batches(
    rows: &[HashMap<String, Value>],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        let batch = RecordBatch::new_empty(schema.clone());
        return Ok(vec![batch]);
    }

    if schema.fields().is_empty() {
        // Schema has no fields but there ARE rows. Preserve the row count so that
        // downstream operators (chained mutations, aggregations) see the correct
        // number of rows. A RecordBatch with 0 columns can still carry a row count.
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len()));
        let batch = RecordBatch::try_new_with_options(schema.clone(), vec![], &options)?;
        return Ok(vec![batch]);
    }

    // Build columns from rows using schema
    let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let name = field.name();
        let values: Vec<Value> = rows
            .iter()
            .map(|row| row.get(name).cloned().unwrap_or(Value::Null))
            .collect();

        let array = value_column_to_arrow(&values, field.data_type(), field)?;
        columns.push(array);
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok(vec![batch])
}

/// Convert a column of Values to an Arrow array, handling entity-encoded columns.
fn value_column_to_arrow(
    values: &[Value],
    arrow_type: &DataType,
    field: &arrow_schema::Field,
) -> Result<arrow_array::ArrayRef> {
    let is_cv_encoded = field
        .metadata()
        .get("cv_encoded")
        .is_some_and(|v| v == "true");

    if *arrow_type == DataType::LargeBinary || is_cv_encoded {
        Ok(encode_as_large_binary(values))
    } else if *arrow_type == DataType::Binary {
        // Binary columns (e.g., CRDT payloads): encode as Binary, not LargeBinary
        Ok(encode_as_binary(values))
    } else {
        // Use arrow_convert for scalar types, falling back to CypherValue encoding
        arrow_convert::values_to_array(values, arrow_type)
            .or_else(|_| Ok(encode_as_large_binary(values)))
    }
}

/// Encode values as CypherValue blobs using the given builder type.
macro_rules! encode_as_cv {
    ($builder_ty:ty, $values:expr) => {{
        let values = $values;
        let mut builder = <$builder_ty>::with_capacity(values.len(), values.len() * 64);
        for v in values {
            if v.is_null() {
                builder.append_null();
            } else {
                let bytes = uni_common::cypher_value_codec::encode(v);
                builder.append_value(&bytes);
            }
        }
        Arc::new(builder.finish()) as arrow_array::ArrayRef
    }};
}

/// Encode values as CypherValue Binary blobs.
fn encode_as_binary(values: &[Value]) -> arrow_array::ArrayRef {
    encode_as_cv!(arrow_array::builder::BinaryBuilder, values)
}

/// Encode values as CypherValue LargeBinary blobs.
fn encode_as_large_binary(values: &[Value]) -> arrow_array::ArrayRef {
    encode_as_cv!(arrow_array::builder::LargeBinaryBuilder, values)
}

/// Execute a mutation stream: collect all input batches, apply mutations, yield output.
///
/// This is the core logic shared by all mutation operators. It implements the
/// "eager barrier" pattern:
/// 1. Pull ALL input batches to completion
/// 2. Convert to rows
/// 3. Acquire writer lock once for the entire clause
/// 4. Apply mutations per row
/// 5. Convert back to batches
/// 6. Yield output
pub fn execute_mutation_stream(
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    mutation_ctx: Arc<MutationContext>,
    mutation_kind: MutationKind,
    partition: usize,
    task_ctx: Arc<datafusion::execution::TaskContext>,
) -> DFResult<SendableRecordBatchStream> {
    if mutation_ctx.query_ctx.is_none() {
        tracing::warn!(
            "MutationContext.query_ctx is None — mutations may not see latest L0 buffer state"
        );
    }

    let stream = futures::stream::once(execute_mutation_inner(
        input,
        output_schema.clone(),
        mutation_ctx,
        mutation_kind,
        partition,
        task_ctx,
    ))
    .try_flatten();

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

/// Inner async function for mutation execution.
///
/// Separated from the stream combinator to provide explicit return type
/// annotation, avoiding type inference issues with multiple From<DataFusionError> impls.
///
/// Mutations are applied as storage-level side effects via Writer/L0 buffer.
/// After mutations, output batches are reconstructed from the modified rows
/// so downstream operators (RETURN, WITH, subsequent mutations) see the
/// created/updated variables and properties.
async fn execute_mutation_inner(
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    mutation_ctx: Arc<MutationContext>,
    mutation_kind: MutationKind,
    partition: usize,
    task_ctx: Arc<datafusion::execution::TaskContext>,
) -> DFResult<futures::stream::Iter<std::vec::IntoIter<DFResult<RecordBatch>>>> {
    let mutation_label = mutation_kind_label(&mutation_kind);

    // 1. Collect all input batches (eager barrier)
    let input_stream = input.execute(partition, task_ctx)?;
    let input_batches: Vec<RecordBatch> = input_stream.try_collect().await?;

    let input_row_count: usize = input_batches.iter().map(|b| b.num_rows()).sum();
    tracing::debug!(
        mutation = mutation_label,
        batches = input_batches.len(),
        rows = input_row_count,
        "Executing mutation"
    );

    // 2. Convert to rows for mutation helpers (they operate on HashMap rows)
    let mut rows = batches_to_rows(&input_batches).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to convert batches to rows: {e}"
        ))
    })?;

    // 3. Apply mutations.
    // MERGE manages its own writer lock internally (acquires/releases per-row because
    // execute_merge_match needs to run a read subplan between lock acquisitions).
    // All other mutations acquire the writer lock once for the entire clause.
    if let MutationKind::Merge {
        ref pattern,
        ref on_match,
        ref on_create,
    } = mutation_kind
    {
        let exec = &mutation_ctx.executor;
        let pm = &mutation_ctx.prop_manager;
        let params = &mutation_ctx.params;
        let ctx = mutation_ctx.query_ctx.as_ref();

        let mut result_rows = exec
            .execute_merge(
                rows,
                pattern,
                on_match.as_ref(),
                on_create.as_ref(),
                pm,
                params,
                ctx,
            )
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("MERGE failed: {e}"))
            })?;

        tracing::debug!(
            mutation = mutation_label,
            input_rows = input_row_count,
            output_rows = result_rows.len(),
            "MERGE mutation complete"
        );

        // Reconstruct output batches from modified rows so downstream operators
        // (RETURN, WITH, subsequent mutations) see the merged/created variables.
        sync_all_props_in_maps(&mut result_rows);
        sync_dotted_columns(&mut result_rows, &output_schema);
        let result_batches = rows_to_batches(&result_rows, &output_schema).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to reconstruct MERGE batches: {e}"
            ))
        })?;
        let results: Vec<DFResult<RecordBatch>> = result_batches.into_iter().map(Ok).collect();
        return Ok(futures::stream::iter(results));
    }

    let mut writer = mutation_ctx.writer.write().await;
    apply_mutations(&mutation_ctx, &mutation_kind, &mut rows, &mut writer).await?;
    drop(writer);

    tracing::debug!(
        mutation = mutation_label,
        rows = input_row_count,
        "Mutation complete"
    );

    // 4. Reconstruct output batches from modified rows.
    // Mutations modify the row HashMaps in place (CREATE adds new variable keys,
    // SET updates property values). Reconstruct batches so downstream operators
    // (RETURN, WITH, subsequent mutations) see these modifications.
    sync_all_props_in_maps(&mut rows);
    sync_dotted_columns(&mut rows, &output_schema);
    let result_batches = rows_to_batches(&rows, &output_schema).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Failed to reconstruct batches: {e}"))
    })?;
    let results: Vec<DFResult<RecordBatch>> = result_batches.into_iter().map(Ok).collect();
    Ok(futures::stream::iter(results))
}

/// Collects and classifies DELETE targets into nodes and edges.
///
/// Handles `Value::Path`, `Value::Node`, `Value::Edge`, map-encoded paths
/// (from Arrow round-trip), and raw VID values. When `dedup` is true,
/// uses HashSets to skip duplicates (needed for non-DETACH DELETE to
/// handle shared nodes across paths).
struct DeleteCollector {
    /// Collected node entries: (vid, labels) pairs.
    node_entries: Vec<(Vid, Option<Vec<String>>)>,
    /// Collected edge values to delete.
    edge_vals: Vec<Value>,
    /// Deduplication sets (only used when dedup=true).
    seen_vids: HashSet<u64>,
    seen_eids: HashSet<u64>,
    dedup: bool,
}

impl DeleteCollector {
    fn new(dedup: bool) -> Self {
        Self {
            node_entries: Vec::new(),
            edge_vals: Vec::new(),
            seen_vids: HashSet::new(),
            seen_eids: HashSet::new(),
            dedup,
        }
    }

    fn add(&mut self, val: Value) {
        if val.is_null() {
            return;
        }

        // Try to resolve value as a Path (native or map-encoded).
        let path = match &val {
            Value::Path(p) => Some(p.clone()),
            _ => Path::try_from(&val).ok(),
        };

        if let Some(path) = path {
            for edge in &path.edges {
                if !self.dedup || self.seen_eids.insert(edge.eid.as_u64()) {
                    self.edge_vals.push(Value::Edge(edge.clone()));
                }
            }
            for node in &path.nodes {
                self.add_node(node.vid, Some(node.labels.clone()));
            }
            return;
        }

        // Not a path -- try as a node (by VID).
        if let Ok(vid) = Executor::vid_from_value(&val) {
            let labels = Executor::extract_labels_from_node(&val);
            self.add_node(vid, labels);
            return;
        }

        // Otherwise treat as an edge value.
        if matches!(&val, Value::Map(_) | Value::Edge(_)) {
            self.edge_vals.push(val);
        }
    }

    fn add_node(&mut self, vid: Vid, labels: Option<Vec<String>>) {
        if self.dedup && !self.seen_vids.insert(vid.as_u64()) {
            return;
        }
        self.node_entries.push((vid, labels));
    }
}

/// Apply mutations to rows using the appropriate executor helper.
async fn apply_mutations(
    mutation_ctx: &MutationContext,
    mutation_kind: &MutationKind,
    rows: &mut [HashMap<String, Value>],
    writer: &mut Writer,
) -> DFResult<()> {
    tracing::trace!(
        mutation = mutation_kind_label(mutation_kind),
        rows = rows.len(),
        "Applying mutations"
    );

    let exec = &mutation_ctx.executor;
    let pm = &mutation_ctx.prop_manager;
    let params = &mutation_ctx.params;
    let ctx = mutation_ctx.query_ctx.as_ref();

    let df_err = |msg: &str, e: anyhow::Error| {
        datafusion::error::DataFusionError::Execution(format!("{msg}: {e}"))
    };

    match mutation_kind {
        MutationKind::Create { pattern } => {
            for row in rows.iter_mut() {
                exec.execute_create_pattern(pattern, row, writer, pm, params, ctx)
                    .await
                    .map_err(|e| df_err("CREATE failed", e))?;
            }
        }
        MutationKind::CreateBatch { patterns } => {
            for row in rows.iter_mut() {
                for pattern in patterns {
                    exec.execute_create_pattern(pattern, row, writer, pm, params, ctx)
                        .await
                        .map_err(|e| df_err("CREATE failed", e))?;
                }
            }
        }
        MutationKind::Set { items } => {
            for row in rows.iter_mut() {
                exec.execute_set_items_locked(items, row, writer, pm, params, ctx)
                    .await
                    .map_err(|e| df_err("SET failed", e))?;
            }
        }
        MutationKind::Remove { items } => {
            for row in rows.iter_mut() {
                exec.execute_remove_items_locked(items, row, writer, pm, ctx)
                    .await
                    .map_err(|e| df_err("REMOVE failed", e))?;
            }
        }
        MutationKind::Delete { items, detach } => {
            // Evaluate all DELETE targets and classify into nodes vs edges.
            let mut collector = DeleteCollector::new(!*detach);
            for row in rows.iter() {
                for expr in items {
                    let val = exec
                        .evaluate_expr(expr, row, pm, params, ctx)
                        .await
                        .map_err(|e| df_err("DELETE eval failed", e))?;
                    collector.add(val);
                }
            }

            // Delete edges before nodes so non-detach DELETE satisfies constraints.
            for val in &collector.edge_vals {
                exec.execute_delete_item_locked(val, false, writer)
                    .await
                    .map_err(|e| df_err("DELETE edge failed", e))?;
            }

            if *detach {
                let (vids, labels): (Vec<Vid>, Vec<Option<Vec<String>>>) =
                    collector.node_entries.into_iter().unzip();
                exec.batch_detach_delete_vertices(&vids, labels, writer)
                    .await
                    .map_err(|e| df_err("DETACH DELETE failed", e))?;
            } else {
                for (vid, labels) in &collector.node_entries {
                    exec.execute_delete_vertex(*vid, false, labels.clone(), writer)
                        .await
                        .map_err(|e| df_err("DELETE node failed", e))?;
                }
            }
        }
        MutationKind::Merge { .. } => {
            // MERGE is handled before the writer lock in execute_mutation_inner.
            // This branch is unreachable but required for exhaustive matching.
            unreachable!("MERGE mutations are handled before apply_mutations is called");
        }
    }

    Ok(())
}

/// Extract variable names introduced by a CREATE/MERGE pattern.
///
/// Walks the pattern tree and collects all node and relationship variable names.
/// Used to compute extended output schemas for CREATE/MERGE operators.
pub fn pattern_variable_names(pattern: &Pattern) -> Vec<String> {
    let mut vars = Vec::new();
    for path in &pattern.paths {
        if let Some(ref v) = path.variable {
            vars.push(v.clone());
        }
        for element in &path.elements {
            match element {
                PatternElement::Node(n) => {
                    if let Some(ref v) = n.variable {
                        vars.push(v.clone());
                    }
                }
                PatternElement::Relationship(r) => {
                    if let Some(ref v) = r.variable {
                        vars.push(v.clone());
                    }
                }
                PatternElement::Parenthesized { pattern, .. } => {
                    // Recurse into parenthesized sub-patterns
                    let sub = Pattern {
                        paths: vec![pattern.as_ref().clone()],
                    };
                    vars.extend(pattern_variable_names(&sub));
                }
            }
        }
    }
    vars
}

/// Normalize a schema for mutation output.
///
/// After mutation processing, entity values (nodes/edges) are stored as
/// `Value::Map` in row HashMaps. The input schema may have Struct columns
/// for these entities, but `rows_to_batches()` encodes Map values as
/// cv_encoded LargeBinary. This function converts Struct and Binary entity
/// columns to cv_encoded LargeBinary to match the actual output format.
fn normalize_mutation_schema(schema: &SchemaRef) -> SchemaRef {
    use arrow_schema::{Field, Schema};

    let needs_normalization = schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Struct(_)));

    if !needs_normalization {
        return schema.clone();
    }

    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|field| {
            if matches!(field.data_type(), DataType::Struct(_)) {
                let mut metadata = field.metadata().clone();
                metadata.insert("cv_encoded".to_string(), "true".to_string());
                Arc::new(
                    Field::new(field.name(), DataType::LargeBinary, true).with_metadata(metadata),
                )
            } else {
                field.clone()
            }
        })
        .collect();

    Arc::new(Schema::new(fields))
}

/// Compute an extended output schema that includes columns for newly created variables.
///
/// Extracts variables from CREATE/MERGE patterns and adds:
/// - Bare cv_encoded LargeBinary column for each variable
/// - System dotted columns based on element type:
///   - Node → `{var}._vid` (UInt64), `{var}._labels` (LargeBinary cv_encoded)
///   - Edge → `{var}._eid` (UInt64), `{var}._type` (LargeBinary cv_encoded)
///   - Path → bare column only (no system columns)
///
/// Property access on mutation variables uses dynamic `index()` UDF extraction,
/// so property columns are NOT added here.
///
/// Also normalizes existing Struct entity columns to cv_encoded LargeBinary,
/// since after mutation processing, entities are stored as Maps in row HashMaps.
pub fn extended_schema_for_new_vars(input_schema: &SchemaRef, patterns: &[Pattern]) -> SchemaRef {
    use arrow_schema::{Field, Schema};

    // First normalize existing columns
    let normalized = normalize_mutation_schema(input_schema);

    let existing_names: HashSet<&str> = normalized
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let mut fields: Vec<Arc<arrow_schema::Field>> = normalized.fields().to_vec();
    let mut added: HashSet<String> = HashSet::new();

    fn cv_metadata() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("cv_encoded".to_string(), "true".to_string());
        m
    }

    fn add_bare_column(
        var: &str,
        fields: &mut Vec<Arc<arrow_schema::Field>>,
        existing: &HashSet<&str>,
        added: &mut HashSet<String>,
    ) -> bool {
        if existing.contains(var) || added.contains(var) {
            return false;
        }
        added.insert(var.to_string());
        fields.push(Arc::new(
            Field::new(var, DataType::LargeBinary, true).with_metadata(cv_metadata()),
        ));
        true
    }

    for pattern in patterns {
        for path in &pattern.paths {
            // Path variable (e.g., `p` in `MERGE p = (a)-[r]->(b)`)
            if let Some(ref var) = path.variable {
                add_bare_column(var, &mut fields, &existing_names, &mut added);
            }
            for element in &path.elements {
                match element {
                    PatternElement::Node(n) => {
                        if let Some(ref var) = n.variable
                            && add_bare_column(var, &mut fields, &existing_names, &mut added)
                        {
                            // Node system columns for id()/labels()
                            fields.push(Arc::new(Field::new(
                                format!("{var}._vid"),
                                DataType::UInt64,
                                true,
                            )));
                            fields.push(Arc::new(
                                Field::new(format!("{var}._labels"), DataType::LargeBinary, true)
                                    .with_metadata(cv_metadata()),
                            ));
                        }
                    }
                    PatternElement::Relationship(r) => {
                        if let Some(ref var) = r.variable
                            && add_bare_column(var, &mut fields, &existing_names, &mut added)
                        {
                            // Edge system columns for id()/type()
                            fields.push(Arc::new(Field::new(
                                format!("{var}._eid"),
                                DataType::UInt64,
                                true,
                            )));
                            fields.push(Arc::new(
                                Field::new(format!("{var}._type"), DataType::LargeBinary, true)
                                    .with_metadata(cv_metadata()),
                            ));
                        }
                    }
                    PatternElement::Parenthesized { pattern, .. } => {
                        // Recurse into sub-patterns. Pass current fields as
                        // input so the recursive call's `existing_names` check
                        // prevents duplicates for variables already added.
                        let sub = Pattern {
                            paths: vec![pattern.as_ref().clone()],
                        };
                        let sub_schema = extended_schema_for_new_vars(
                            &Arc::new(Schema::new(fields.clone())),
                            &[sub],
                        );
                        // Sync `added` from new fields to prevent duplicates
                        // if a later pattern element reuses a variable.
                        for field in sub_schema.fields() {
                            added.insert(field.name().clone());
                        }
                        fields = sub_schema.fields().to_vec();
                    }
                }
            }
        }
    }

    Arc::new(Schema::new(fields))
}

/// Human-readable label for a MutationKind (used in tracing spans).
fn mutation_kind_label(kind: &MutationKind) -> &'static str {
    match kind {
        MutationKind::Create { .. } => "CREATE",
        MutationKind::CreateBatch { .. } => "CREATE_BATCH",
        MutationKind::Set { .. } => "SET",
        MutationKind::Remove { .. } => "REMOVE",
        MutationKind::Delete { .. } => "DELETE",
        MutationKind::Merge { .. } => "MERGE",
    }
}

// ============================================================================
// Unified MutationExec: single ExecutionPlan for all mutation kinds
// ============================================================================

/// Unified DataFusion `ExecutionPlan` for all Cypher mutation clauses
/// (CREATE, SET, REMOVE, DELETE).
///
/// Instead of four near-identical ExecutionPlan structs, this single struct
/// holds a [`MutationKind`] discriminant and delegates to the shared
/// [`execute_mutation_stream`] implementation. Typed constructors in
/// `mutation_create`, `mutation_set`, `mutation_remove`, and `mutation_delete`
/// provide ergonomic construction with the correct kind.
#[derive(Debug)]
pub struct MutationExec {
    /// Child plan producing input rows.
    input: Arc<dyn ExecutionPlan>,

    /// The kind of mutation to apply.
    kind: MutationKind,

    /// Display name for EXPLAIN output.
    display_name: &'static str,

    /// Shared mutation context with executor and writer.
    mutation_ctx: Arc<MutationContext>,

    /// Output schema (input schema, mutations are side effects).
    schema: SchemaRef,

    /// Plan properties for DataFusion optimizer.
    properties: PlanProperties,

    /// Metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl MutationExec {
    /// Create a new `MutationExec` with the given kind.
    ///
    /// The output schema is derived from the input schema with Struct entity
    /// columns normalized to cv_encoded LargeBinary. For mutations that
    /// introduce new variables (CREATE, MERGE), use [`new_with_schema`] instead.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        kind: MutationKind,
        display_name: &'static str,
        mutation_ctx: Arc<MutationContext>,
    ) -> Self {
        let schema = normalize_mutation_schema(&input.schema());
        let properties = compute_plan_properties(schema.clone());
        Self {
            input,
            kind,
            display_name,
            mutation_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Create a new `MutationExec` with an explicit output schema.
    ///
    /// Used by CREATE and MERGE operators whose output includes newly created
    /// variables not present in the input schema.
    pub fn new_with_schema(
        input: Arc<dyn ExecutionPlan>,
        kind: MutationKind,
        display_name: &'static str,
        mutation_ctx: Arc<MutationContext>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = compute_plan_properties(output_schema.clone());
        Self {
            input,
            kind,
            display_name,
            mutation_ctx,
            schema: output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for MutationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        if matches!(&self.kind, MutationKind::Delete { detach: true, .. }) {
            write!(f, "{} [DETACH]", self.display_name)
        } else {
            write!(f, "{}", self.display_name)
        }
    }
}

impl ExecutionPlan for MutationExec {
    fn name(&self) -> &str {
        self.display_name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "{} requires exactly one child",
                self.display_name,
            )));
        }
        Ok(Arc::new(MutationExec::new_with_schema(
            children[0].clone(),
            self.kind.clone(),
            self.display_name,
            self.mutation_ctx.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        execute_mutation_stream(
            self.input.clone(),
            self.schema.clone(),
            self.mutation_ctx.clone(),
            self.kind.clone(),
            partition,
            context,
        )
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Create a new `MutationExec` configured for a CREATE clause.
///
/// Computes an extended output schema that includes LargeBinary cv_encoded
/// columns for any variables introduced by the pattern that are not already
/// in the input schema.
pub fn new_create_exec(
    input: Arc<dyn ExecutionPlan>,
    pattern: Pattern,
    mutation_ctx: Arc<MutationContext>,
) -> MutationExec {
    let output_schema =
        extended_schema_for_new_vars(&input.schema(), std::slice::from_ref(&pattern));
    MutationExec::new_with_schema(
        input,
        MutationKind::Create { pattern },
        "MutationCreateExec",
        mutation_ctx,
        output_schema,
    )
}

/// Create a new `MutationExec` configured for a MERGE clause.
///
/// Computes an extended output schema that includes LargeBinary cv_encoded
/// columns for any variables introduced by the pattern that are not already
/// in the input schema.
pub fn new_merge_exec(
    input: Arc<dyn ExecutionPlan>,
    pattern: Pattern,
    on_match: Option<SetClause>,
    on_create: Option<SetClause>,
    mutation_ctx: Arc<MutationContext>,
) -> MutationExec {
    let output_schema =
        extended_schema_for_new_vars(&input.schema(), std::slice::from_ref(&pattern));
    MutationExec::new_with_schema(
        input,
        MutationKind::Merge {
            pattern,
            on_match,
            on_create,
        },
        "MutationMergeExec",
        mutation_ctx,
        output_schema,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    #[test]
    fn test_batches_to_rows_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
                Arc::new(Int64Array::from(vec![Some(30), Some(25)])),
            ],
        )
        .unwrap();

        let rows = batches_to_rows(&[batch]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(rows[0].get("age"), Some(&Value::Int(30)));
        assert_eq!(rows[1].get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(rows[1].get("age"), Some(&Value::Int(25)));
    }

    #[test]
    fn test_rows_to_batches_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));

        let rows = vec![
            {
                let mut m = HashMap::new();
                m.insert("name".to_string(), Value::String("Alice".into()));
                m.insert("age".to_string(), Value::Int(30));
                m
            },
            {
                let mut m = HashMap::new();
                m.insert("name".to_string(), Value::String("Bob".into()));
                m.insert("age".to_string(), Value::Int(25));
                m
            },
        ];

        let batches = rows_to_batches(&rows, &schema).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_roundtrip_scalar_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("i", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("hello")])),
                Arc::new(Int64Array::from(vec![Some(42)])),
                Arc::new(arrow_array::Float64Array::from(vec![Some(3.125)])),
                Arc::new(arrow_array::BooleanArray::from(vec![Some(true)])),
            ],
        )
        .unwrap();

        // Roundtrip: batches → rows → batches
        let rows = batches_to_rows(&[batch]).unwrap();
        let output_batches = rows_to_batches(&rows, &schema).unwrap();

        assert_eq!(output_batches.len(), 1);
        assert_eq!(output_batches[0].num_rows(), 1);

        // Verify roundtrip fidelity
        let roundtrip_rows = batches_to_rows(&output_batches).unwrap();
        assert_eq!(roundtrip_rows.len(), 1);
        assert_eq!(
            roundtrip_rows[0].get("s"),
            Some(&Value::String("hello".into()))
        );
        assert_eq!(roundtrip_rows[0].get("i"), Some(&Value::Int(42)));
        assert_eq!(roundtrip_rows[0].get("b"), Some(&Value::Bool(true)));
        // Float comparison
        if let Some(Value::Float(f)) = roundtrip_rows[0].get("f") {
            assert!((*f - 3.125).abs() < 1e-10);
        } else {
            panic!("Expected float value");
        }
    }

    #[test]
    fn test_roundtrip_cypher_value_encoded() {
        use std::collections::HashMap as StdHashMap;

        // Create a schema with a cv_encoded LargeBinary column (entity column)
        let mut metadata = StdHashMap::new();
        metadata.insert("cv_encoded".to_string(), "true".to_string());
        let field = Field::new("n", DataType::LargeBinary, true).with_metadata(metadata);
        let schema = Arc::new(Schema::new(vec![field]));

        // Create a node-like Map value
        let mut node_map = HashMap::new();
        node_map.insert("name".to_string(), Value::String("Alice".into()));
        node_map.insert("_vid".to_string(), Value::Int(1));
        let map_val = Value::Map(node_map);

        // Encode to CypherValue bytes
        let encoded = uni_common::cypher_value_codec::encode(&map_val);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::LargeBinaryArray::from(vec![Some(
                encoded.as_slice(),
            )]))],
        )
        .unwrap();

        // Roundtrip
        let rows = batches_to_rows(&[batch]).unwrap();
        assert_eq!(rows.len(), 1);

        // The decoded value should be a Map
        let val = rows[0].get("n").unwrap();
        assert!(matches!(val, Value::Map(_)));

        let output_batches = rows_to_batches(&rows, &schema).unwrap();
        assert_eq!(output_batches[0].num_rows(), 1);

        // Verify we can decode it back
        let roundtrip_rows = batches_to_rows(&output_batches).unwrap();
        assert_eq!(roundtrip_rows.len(), 1);
    }

    #[test]
    fn test_empty_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));

        let batches = rows_to_batches(&[], &schema).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }
}
