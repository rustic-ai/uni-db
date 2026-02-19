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
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;
use uni_common::Value;
use uni_cypher::ast::{Expr, Pattern, RemoveItem, SetItem};
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
                if field.metadata().get("cv_encoded") == Some(&"true".to_string())
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

/// Merge system fields into bare variable maps for write helper consumption.
///
/// The write helpers expect variables like `n` to be a Map containing `_vid`, `_labels`, etc.
/// This merges dotted columns (like `n._vid`, `n._labels`) into the variable Map,
/// while KEEPING the dotted columns in the row so `rows_to_batches` still works.
fn merge_system_fields_for_write(row: &mut HashMap<String, Value>) {
    let bare_vars: Vec<String> = row
        .keys()
        .filter(|k| !k.contains('.') && matches!(row.get(*k), Some(Value::Map(_))))
        .cloned()
        .collect();

    // Vertex system fields (overwrite into the bare map) and edge system fields
    // (insert only if absent) that should be copied from dotted columns.
    const VERTEX_FIELDS: &[&str] = &["_vid", "_labels"];
    const EDGE_FIELDS: &[&str] = &["_eid", "_type"];

    for var in &bare_vars {
        for &field in VERTEX_FIELDS {
            if let Some(v) = row.get(&format!("{var}.{field}")).cloned()
                && let Some(Value::Map(map)) = row.get_mut(var)
            {
                map.insert(field.to_string(), v);
            }
        }
        for &field in EDGE_FIELDS {
            if let Some(v) = row.get(&format!("{var}.{field}")).cloned()
                && let Some(Value::Map(map)) = row.get_mut(var)
            {
                map.entry(field.to_string()).or_insert(v);
            }
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
    if rows.is_empty() || schema.fields().is_empty() {
        // Return an empty batch with the correct schema.
        // When schema has no fields (e.g., standalone CREATE with no RETURN),
        // the side effects have already been applied; just return empty.
        let batch = RecordBatch::new_empty(schema.clone());
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
    let is_cv_encoded = field.metadata().get("cv_encoded") == Some(&"true".to_string());

    if *arrow_type == DataType::LargeBinary || is_cv_encoded {
        Ok(encode_as_large_binary(values))
    } else {
        // Use arrow_convert for scalar types, falling back to CypherValue encoding
        arrow_convert::values_to_array(values, arrow_type)
            .or_else(|_| Ok(encode_as_large_binary(values)))
    }
}

/// Encode values as CypherValue LargeBinary blobs.
fn encode_as_large_binary(values: &[Value]) -> arrow_array::ArrayRef {
    let mut builder =
        arrow_array::builder::LargeBinaryBuilder::with_capacity(values.len(), values.len() * 64);
    for v in values {
        if v.is_null() {
            builder.append_null();
        } else {
            let bytes = uni_common::cypher_value_codec::encode(v);
            builder.append_value(&bytes);
        }
    }
    Arc::new(builder.finish())
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
/// The original input batches are passed through unchanged to downstream operators.
/// This avoids the complex row→batch reconstruction for Struct/entity columns.
async fn execute_mutation_inner(
    input: Arc<dyn ExecutionPlan>,
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

    // 3. Acquire writer lock and apply mutations
    let mut writer = mutation_ctx.writer.write().await;

    apply_mutations(&mutation_ctx, &mutation_kind, &mut rows, &mut writer).await?;
    drop(writer);

    tracing::debug!(
        mutation = mutation_label,
        rows = input_row_count,
        "Mutation complete"
    );

    // 4. Pass through original input batches unchanged.
    // Mutations are storage-level side effects (writes to Writer/L0 buffer).
    // The original batch schema (including Struct columns for entities) is
    // preserved without complex reconstruction.
    let results: Vec<DFResult<RecordBatch>> = input_batches.into_iter().map(Ok).collect();
    Ok(futures::stream::iter(results))
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
            if *detach {
                let mut vertex_vids = Vec::new();
                let mut vertex_labels = Vec::new();
                let mut edge_vals = Vec::new();

                for row in rows.iter() {
                    for expr in items {
                        let val = exec
                            .evaluate_expr(expr, row, pm, params, ctx)
                            .await
                            .map_err(|e| df_err("DELETE eval failed", e))?;
                        if let Ok(vid) = Executor::vid_from_value(&val) {
                            vertex_labels.push(Executor::extract_labels_from_node(&val));
                            vertex_vids.push(vid);
                        } else if matches!(&val, Value::Map(_) | Value::Edge(_)) {
                            edge_vals.push(val);
                        }
                    }
                }

                exec.batch_detach_delete_vertices(&vertex_vids, vertex_labels, writer)
                    .await
                    .map_err(|e| df_err("DETACH DELETE failed", e))?;

                for val in &edge_vals {
                    exec.execute_delete_item_locked(val, false, writer)
                        .await
                        .map_err(|e| df_err("DELETE edge failed", e))?;
                }
            } else {
                for row in rows.iter() {
                    for expr in items {
                        let val = exec
                            .evaluate_expr(expr, row, pm, params, ctx)
                            .await
                            .map_err(|e| df_err("DELETE eval failed", e))?;
                        exec.execute_delete_item_locked(&val, false, writer)
                            .await
                            .map_err(|e| df_err("DELETE failed", e))?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Human-readable label for a MutationKind (used in tracing spans).
fn mutation_kind_label(kind: &MutationKind) -> &'static str {
    match kind {
        MutationKind::Create { .. } => "CREATE",
        MutationKind::CreateBatch { .. } => "CREATE_BATCH",
        MutationKind::Set { .. } => "SET",
        MutationKind::Remove { .. } => "REMOVE",
        MutationKind::Delete { .. } => "DELETE",
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
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        kind: MutationKind,
        display_name: &'static str,
        mutation_ctx: Arc<MutationContext>,
    ) -> Self {
        let schema = input.schema();
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

    /// Whether this is a DETACH DELETE mutation.
    fn is_detach_delete(&self) -> bool {
        matches!(&self.kind, MutationKind::Delete { detach: true, .. })
    }
}

impl DisplayAs for MutationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_detach_delete() {
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
        Ok(Arc::new(MutationExec::new(
            children[0].clone(),
            self.kind.clone(),
            self.display_name,
            self.mutation_ctx.clone(),
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
                Arc::new(arrow_array::Float64Array::from(vec![Some(3.14)])),
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
            assert!((*f - 3.14).abs() < 1e-10);
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
