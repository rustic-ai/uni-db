// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Procedure call execution plan for DataFusion.
//!
//! This module provides [`GraphProcedureCallExec`], a DataFusion [`ExecutionPlan`] that
//! executes Cypher `CALL` procedures natively within the DataFusion engine.
//!
//! Used for composite queries where a `CALL` is followed by `MATCH`, e.g.:
//! ```text
//! CALL uni.schema.labels() YIELD label
//! MATCH (n:Person) WHERE label = 'Person'
//! RETURN n.name, label
//! ```

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    compute_plan_properties, evaluate_simple_expr, labels_data_type,
};
use crate::query::df_graph::scan::resolve_property_type;
use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_cypher::ast::Expr;

/// Maps a user-provided yield name to a canonical name.
///
/// - "vid", "_vid" → "vid"
/// - "distance", "dist", "_distance" → "distance"
/// - "score", "_score" → "score"
/// - anything else → "node" (treated as node variable)
pub(crate) fn map_yield_to_canonical(yield_name: &str) -> String {
    match yield_name.to_lowercase().as_str() {
        "vid" | "_vid" => "vid",
        "distance" | "dist" | "_distance" => "distance",
        "score" | "_score" => "score",
        "vector_score" => "vector_score",
        "fts_score" => "fts_score",
        "raw_score" => "raw_score",
        _ => "node",
    }
    .to_string()
}

/// Procedure call execution plan for DataFusion.
///
/// Executes Cypher CALL procedures (schema introspection, vector search, FTS, etc.)
/// and emits results as Arrow RecordBatches.
pub struct GraphProcedureCallExec {
    /// Graph execution context for storage access.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Fully qualified procedure name (e.g. "uni.schema.labels").
    procedure_name: String,

    /// Argument expressions from the CALL clause.
    arguments: Vec<Expr>,

    /// Yield items: (original_name, optional_alias).
    yield_items: Vec<(String, Option<String>)>,

    /// Query parameters for expression evaluation.
    params: HashMap<String, Value>,

    /// Target properties per variable (for node-like yields).
    target_properties: HashMap<String, Vec<String>>,

    /// Output schema.
    schema: SchemaRef,

    /// Plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphProcedureCallExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphProcedureCallExec")
            .field("procedure_name", &self.procedure_name)
            .field("yield_items", &self.yield_items)
            .finish()
    }
}

impl GraphProcedureCallExec {
    /// Create a new procedure call execution plan.
    pub fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        procedure_name: String,
        arguments: Vec<Expr>,
        yield_items: Vec<(String, Option<String>)>,
        params: HashMap<String, Value>,
        target_properties: HashMap<String, Vec<String>>,
    ) -> Self {
        let schema = Self::build_schema(
            &procedure_name,
            &yield_items,
            &target_properties,
            &graph_ctx,
        );
        let properties = compute_plan_properties(schema.clone());

        Self {
            graph_ctx,
            procedure_name,
            arguments,
            yield_items,
            params,
            target_properties,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build the output schema based on the procedure name and yield items.
    fn build_schema(
        procedure_name: &str,
        yield_items: &[(String, Option<String>)],
        target_properties: &HashMap<String, Vec<String>>,
        graph_ctx: &GraphExecutionContext,
    ) -> SchemaRef {
        let mut fields = Vec::new();

        match procedure_name {
            "uni.schema.labels" => {
                // Schema procedure yields scalar columns
                for (name, alias) in yield_items {
                    let col_name = alias.as_ref().unwrap_or(name);
                    let data_type = match name.as_str() {
                        "label" => DataType::Utf8,
                        "propertyCount" | "nodeCount" | "indexCount" => DataType::Int64,
                        _ => DataType::Utf8,
                    };
                    fields.push(Field::new(col_name, data_type, true));
                }
            }
            "uni.schema.edgeTypes" | "uni.schema.relationshipTypes" => {
                for (name, alias) in yield_items {
                    let col_name = alias.as_ref().unwrap_or(name);
                    let data_type = match name.as_str() {
                        "type" | "relationshipType" => DataType::Utf8,
                        "propertyCount" => DataType::Int64,
                        "sourceLabels" | "targetLabels" => DataType::Utf8, // JSON string
                        _ => DataType::Utf8,
                    };
                    fields.push(Field::new(col_name, data_type, true));
                }
            }
            "uni.schema.indexes" => {
                for (name, alias) in yield_items {
                    let col_name = alias.as_ref().unwrap_or(name);
                    let data_type = match name.as_str() {
                        "name" | "type" | "label" | "state" | "properties" => DataType::Utf8,
                        _ => DataType::Utf8,
                    };
                    fields.push(Field::new(col_name, data_type, true));
                }
            }
            "uni.schema.constraints" => {
                for (name, alias) in yield_items {
                    let col_name = alias.as_ref().unwrap_or(name);
                    let data_type = match name.as_str() {
                        "enabled" => DataType::Boolean,
                        _ => DataType::Utf8,
                    };
                    fields.push(Field::new(col_name, data_type, true));
                }
            }
            "uni.schema.labelInfo" => {
                for (name, alias) in yield_items {
                    let col_name = alias.as_ref().unwrap_or(name);
                    let data_type = match name.as_str() {
                        "property" | "dataType" => DataType::Utf8,
                        "nullable" | "indexed" | "unique" => DataType::Boolean,
                        _ => DataType::Utf8,
                    };
                    fields.push(Field::new(col_name, data_type, true));
                }
            }
            "uni.vector.query" | "uni.fts.query" | "uni.search" => {
                // Search procedures yield node-like and scalar columns
                for (name, alias) in yield_items {
                    let output_name = alias.as_ref().unwrap_or(name);
                    let canonical = map_yield_to_canonical(name);

                    match canonical.as_str() {
                        "node" => {
                            // Node-like yield: emit _vid, variable, _label, and properties
                            fields.push(Field::new(
                                format!("{}._vid", output_name),
                                DataType::UInt64,
                                false,
                            ));
                            fields.push(Field::new(output_name, DataType::Utf8, false));
                            fields.push(Field::new(
                                format!("{}._labels", output_name),
                                labels_data_type(),
                                true,
                            ));

                            // Add property columns
                            if let Some(props) = target_properties.get(output_name.as_str()) {
                                let uni_schema = graph_ctx.storage().schema_manager().schema();
                                // We don't know the exact label yet at planning time,
                                // but we can try to resolve property types from any label
                                for prop_name in props {
                                    let col_name = format!("{}.{}", output_name, prop_name);
                                    let arrow_type = resolve_property_type(prop_name, None);
                                    // Try to resolve from all labels in the schema
                                    let resolved_type = uni_schema
                                        .properties
                                        .values()
                                        .find_map(|label_props| {
                                            label_props.get(prop_name.as_str()).map(|_| {
                                                resolve_property_type(prop_name, Some(label_props))
                                            })
                                        })
                                        .unwrap_or(arrow_type);
                                    fields.push(Field::new(&col_name, resolved_type, true));
                                }
                            }
                        }
                        "distance" => {
                            fields.push(Field::new(output_name, DataType::Float64, true));
                        }
                        "score" | "vector_score" | "fts_score" | "raw_score" => {
                            fields.push(Field::new(output_name, DataType::Float32, true));
                        }
                        "vid" => {
                            fields.push(Field::new(output_name, DataType::Int64, true));
                        }
                        _ => {
                            fields.push(Field::new(output_name, DataType::Utf8, true));
                        }
                    }
                }
            }
            name if name.starts_with("uni.algo.") => {
                if let Some(registry) = graph_ctx.algo_registry()
                    && let Some(procedure) = registry.get(name)
                {
                    let sig = procedure.signature();
                    for (yield_name, alias) in yield_items {
                        let col_name = alias.as_ref().unwrap_or(yield_name);
                        let yield_vt = sig.yields.iter().find(|(n, _)| *n == yield_name.as_str());
                        let data_type = yield_vt
                            .map(|(_, vt)| value_type_to_arrow(vt))
                            .unwrap_or(DataType::Utf8);
                        let mut field = Field::new(col_name, data_type, true);
                        // Tag complex types (List, Map, etc.) so record_batches_to_rows
                        // can parse the JSON string back to the original type.
                        if yield_vt.is_some_and(|(_, vt)| is_complex_value_type(vt)) {
                            let mut metadata = std::collections::HashMap::new();
                            metadata.insert("cv_encoded".to_string(), "true".to_string());
                            field = field.with_metadata(metadata);
                        }
                        fields.push(field);
                    }
                } else {
                    // Unknown algo or no registry: fallback to Utf8
                    for (name, alias) in yield_items {
                        let col_name = alias.as_ref().unwrap_or(name);
                        fields.push(Field::new(col_name, DataType::Utf8, true));
                    }
                }
            }
            _ => {
                // Check external procedure registry for type information
                if let Some(registry) = graph_ctx.procedure_registry()
                    && let Some(proc_def) = registry.get(procedure_name)
                {
                    for (name, alias) in yield_items {
                        let col_name = alias.as_ref().unwrap_or(name);
                        // Find the output type from the procedure definition
                        let data_type = proc_def
                            .outputs
                            .iter()
                            .find(|o| o.name == *name)
                            .map(|o| procedure_value_type_to_arrow(&o.output_type))
                            .unwrap_or(DataType::Utf8);
                        fields.push(Field::new(col_name, data_type, true));
                    }
                } else if yield_items.is_empty() {
                    // Void procedure (no YIELD) — no output columns
                } else {
                    // Unknown procedure without registry: fallback to Utf8
                    for (name, alias) in yield_items {
                        let col_name = alias.as_ref().unwrap_or(name);
                        fields.push(Field::new(col_name, DataType::Utf8, true));
                    }
                }
            }
        }

        Arc::new(Schema::new(fields))
    }
}

/// Convert an algorithm `ValueType` to an Arrow `DataType`.
fn value_type_to_arrow(vt: &uni_algo::algo::procedures::ValueType) -> DataType {
    use uni_algo::algo::procedures::ValueType;
    match vt {
        ValueType::Int => DataType::Int64,
        ValueType::Float => DataType::Float64,
        ValueType::String => DataType::Utf8,
        ValueType::Bool => DataType::Boolean,
        ValueType::List
        | ValueType::Map
        | ValueType::Node
        | ValueType::Relationship
        | ValueType::Path
        | ValueType::Any => DataType::Utf8,
    }
}

/// Returns true if the ValueType is a complex type that should be JSON-encoded as Utf8
/// and tagged with `cv_encoded=true` metadata for downstream parsing.
fn is_complex_value_type(vt: &uni_algo::algo::procedures::ValueType) -> bool {
    use uni_algo::algo::procedures::ValueType;
    matches!(
        vt,
        ValueType::List
            | ValueType::Map
            | ValueType::Node
            | ValueType::Relationship
            | ValueType::Path
    )
}

/// Convert a `ProcedureValueType` to an Arrow `DataType`.
fn procedure_value_type_to_arrow(
    vt: &crate::query::executor::procedure::ProcedureValueType,
) -> DataType {
    use crate::query::executor::procedure::ProcedureValueType;
    match vt {
        ProcedureValueType::Integer => DataType::Int64,
        ProcedureValueType::Float | ProcedureValueType::Number => DataType::Float64,
        ProcedureValueType::Boolean => DataType::Boolean,
        ProcedureValueType::String | ProcedureValueType::Any => DataType::Utf8,
    }
}

impl DisplayAs for GraphProcedureCallExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphProcedureCallExec: procedure={}",
            self.procedure_name
        )
    }
}

impl ExecutionPlan for GraphProcedureCallExec {
    fn name(&self) -> &str {
        "GraphProcedureCallExec"
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "GraphProcedureCallExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Evaluate arguments upfront
        let mut evaluated_args = Vec::with_capacity(self.arguments.len());
        for arg in &self.arguments {
            evaluated_args.push(evaluate_simple_expr(arg, &self.params)?);
        }

        Ok(Box::pin(ProcedureCallStream::new(
            self.graph_ctx.clone(),
            self.procedure_name.clone(),
            evaluated_args,
            self.yield_items.clone(),
            self.target_properties.clone(),
            self.schema.clone(),
            metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

/// State machine for procedure call stream.
enum ProcedureCallState {
    /// Initial state, ready to start execution.
    Init,
    /// Executing the async procedure.
    Executing(Pin<Box<dyn std::future::Future<Output = DFResult<Option<RecordBatch>>> + Send>>),
    /// Stream is done.
    Done,
}

/// Stream that executes a procedure call.
struct ProcedureCallStream {
    graph_ctx: Arc<GraphExecutionContext>,
    procedure_name: String,
    evaluated_args: Vec<Value>,
    yield_items: Vec<(String, Option<String>)>,
    target_properties: HashMap<String, Vec<String>>,
    schema: SchemaRef,
    state: ProcedureCallState,
    metrics: BaselineMetrics,
}

impl ProcedureCallStream {
    fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        procedure_name: String,
        evaluated_args: Vec<Value>,
        yield_items: Vec<(String, Option<String>)>,
        target_properties: HashMap<String, Vec<String>>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            graph_ctx,
            procedure_name,
            evaluated_args,
            yield_items,
            target_properties,
            schema,
            state: ProcedureCallState::Init,
            metrics,
        }
    }
}

impl Stream for ProcedureCallStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, ProcedureCallState::Done);

            match state {
                ProcedureCallState::Init => {
                    let graph_ctx = self.graph_ctx.clone();
                    let procedure_name = self.procedure_name.clone();
                    let evaluated_args = self.evaluated_args.clone();
                    let yield_items = self.yield_items.clone();
                    let target_properties = self.target_properties.clone();
                    let schema = self.schema.clone();

                    let fut = async move {
                        graph_ctx.check_timeout().map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;

                        execute_procedure(
                            &graph_ctx,
                            &procedure_name,
                            &evaluated_args,
                            &yield_items,
                            &target_properties,
                            &schema,
                        )
                        .await
                    };

                    self.state = ProcedureCallState::Executing(Box::pin(fut));
                }
                ProcedureCallState::Executing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.state = ProcedureCallState::Done;
                        self.metrics
                            .record_output(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
                        return Poll::Ready(batch.map(Ok));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = ProcedureCallState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = ProcedureCallState::Executing(fut);
                        return Poll::Pending;
                    }
                },
                ProcedureCallState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for ProcedureCallStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// Procedure execution dispatch
// ---------------------------------------------------------------------------

/// Execute a procedure and build a RecordBatch result.
async fn execute_procedure(
    graph_ctx: &GraphExecutionContext,
    procedure_name: &str,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    match procedure_name {
        "uni.schema.labels" => execute_schema_labels(graph_ctx, yield_items, schema).await,
        "uni.schema.edgeTypes" | "uni.schema.relationshipTypes" => {
            execute_schema_edge_types(graph_ctx, yield_items, schema).await
        }
        "uni.schema.indexes" => execute_schema_indexes(graph_ctx, yield_items, schema).await,
        "uni.schema.constraints" => {
            execute_schema_constraints(graph_ctx, yield_items, schema).await
        }
        "uni.schema.labelInfo" => {
            execute_schema_label_info(graph_ctx, args, yield_items, schema).await
        }
        "uni.vector.query" => {
            execute_vector_query(graph_ctx, args, yield_items, target_properties, schema).await
        }
        "uni.fts.query" => {
            execute_fts_query(graph_ctx, args, yield_items, target_properties, schema).await
        }
        "uni.search" => {
            execute_hybrid_search(graph_ctx, args, yield_items, target_properties, schema).await
        }
        name if name.starts_with("uni.algo.") => {
            execute_algo_procedure(graph_ctx, name, args, yield_items, schema).await
        }
        _ => {
            execute_registered_procedure(graph_ctx, procedure_name, args, yield_items, schema).await
        }
    }
}

// ---------------------------------------------------------------------------
// Schema procedures
// ---------------------------------------------------------------------------

async fn execute_schema_labels(
    graph_ctx: &GraphExecutionContext,
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let uni_schema = graph_ctx.storage().schema_manager().schema();
    let storage = graph_ctx.storage();

    // Collect rows: one per label
    let mut rows: Vec<HashMap<String, Value>> = Vec::new();
    for label_name in uni_schema.labels.keys() {
        let mut row = HashMap::new();
        row.insert("label".to_string(), Value::String(label_name.clone()));

        let prop_count = uni_schema
            .properties
            .get(label_name)
            .map(|p| p.len())
            .unwrap_or(0);
        row.insert("propertyCount".to_string(), Value::Int(prop_count as i64));

        let node_count = if let Ok(ds) = storage.vertex_dataset(label_name) {
            if let Ok(raw) = ds.open_raw().await {
                raw.count_rows(None).await.unwrap_or(0)
            } else {
                0
            }
        } else {
            0
        };
        row.insert("nodeCount".to_string(), Value::Int(node_count as i64));

        let idx_count = uni_schema
            .indexes
            .iter()
            .filter(|i| i.label() == label_name)
            .count();
        row.insert("indexCount".to_string(), Value::Int(idx_count as i64));

        rows.push(row);
    }

    build_scalar_batch(&rows, yield_items, schema)
}

async fn execute_schema_edge_types(
    graph_ctx: &GraphExecutionContext,
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let uni_schema = graph_ctx.storage().schema_manager().schema();

    let mut rows: Vec<HashMap<String, Value>> = Vec::new();
    for (type_name, meta) in &uni_schema.edge_types {
        let mut row = HashMap::new();
        row.insert("type".to_string(), Value::String(type_name.clone()));
        row.insert(
            "relationshipType".to_string(),
            Value::String(type_name.clone()),
        );
        row.insert(
            "sourceLabels".to_string(),
            Value::String(format!("{:?}", meta.src_labels)),
        );
        row.insert(
            "targetLabels".to_string(),
            Value::String(format!("{:?}", meta.dst_labels)),
        );

        let prop_count = uni_schema
            .properties
            .get(type_name)
            .map(|p| p.len())
            .unwrap_or(0);
        row.insert("propertyCount".to_string(), Value::Int(prop_count as i64));

        rows.push(row);
    }

    build_scalar_batch(&rows, yield_items, schema)
}

async fn execute_schema_indexes(
    graph_ctx: &GraphExecutionContext,
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let uni_schema = graph_ctx.storage().schema_manager().schema();

    let mut rows: Vec<HashMap<String, Value>> = Vec::new();
    for idx in uni_schema.indexes {
        use uni_common::core::schema::IndexDefinition;

        // Extract type name and properties JSON per variant
        let (type_name, properties_json) = match &idx {
            IndexDefinition::Vector(v) => (
                "VECTOR",
                serde_json::to_string(&[&v.property]).unwrap_or_default(),
            ),
            IndexDefinition::FullText(f) => (
                "FULLTEXT",
                serde_json::to_string(&f.properties).unwrap_or_default(),
            ),
            IndexDefinition::Scalar(s) => (
                "SCALAR",
                serde_json::to_string(&s.properties).unwrap_or_default(),
            ),
            IndexDefinition::JsonFullText(j) => (
                "JSON_FTS",
                serde_json::to_string(&[&j.column]).unwrap_or_default(),
            ),
            IndexDefinition::Inverted(inv) => (
                "INVERTED",
                serde_json::to_string(&[&inv.property]).unwrap_or_default(),
            ),
            _ => ("UNKNOWN", String::new()),
        };

        let row = HashMap::from([
            ("state".to_string(), Value::String("ONLINE".to_string())),
            ("name".to_string(), Value::String(idx.name().to_string())),
            ("type".to_string(), Value::String(type_name.to_string())),
            ("label".to_string(), Value::String(idx.label().to_string())),
            ("properties".to_string(), Value::String(properties_json)),
        ]);
        rows.push(row);
    }

    build_scalar_batch(&rows, yield_items, schema)
}

async fn execute_schema_constraints(
    graph_ctx: &GraphExecutionContext,
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let uni_schema = graph_ctx.storage().schema_manager().schema();

    let mut rows: Vec<HashMap<String, Value>> = Vec::new();
    for c in uni_schema.constraints {
        let mut row = HashMap::new();
        row.insert("name".to_string(), Value::String(c.name));
        row.insert("enabled".to_string(), Value::Bool(c.enabled));

        match c.constraint_type {
            uni_common::core::schema::ConstraintType::Unique { properties } => {
                row.insert("type".to_string(), Value::String("UNIQUE".to_string()));
                row.insert(
                    "properties".to_string(),
                    Value::String(serde_json::to_string(&properties).unwrap_or_default()),
                );
            }
            uni_common::core::schema::ConstraintType::Exists { property } => {
                row.insert("type".to_string(), Value::String("EXISTS".to_string()));
                row.insert(
                    "properties".to_string(),
                    Value::String(serde_json::to_string(&[&property]).unwrap_or_default()),
                );
            }
            uni_common::core::schema::ConstraintType::Check { expression } => {
                row.insert("type".to_string(), Value::String("CHECK".to_string()));
                row.insert("expression".to_string(), Value::String(expression));
            }
            _ => {
                row.insert("type".to_string(), Value::String("UNKNOWN".to_string()));
            }
        }

        match c.target {
            uni_common::core::schema::ConstraintTarget::Label(l) => {
                row.insert("label".to_string(), Value::String(l));
            }
            uni_common::core::schema::ConstraintTarget::EdgeType(t) => {
                row.insert("relationshipType".to_string(), Value::String(t));
            }
            _ => {
                row.insert("target".to_string(), Value::String("UNKNOWN".to_string()));
            }
        }

        rows.push(row);
    }

    build_scalar_batch(&rows, yield_items, schema)
}

async fn execute_schema_label_info(
    graph_ctx: &GraphExecutionContext,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label_name = require_string_arg(args, 0, "uni.schema.labelInfo: first argument (label)")?;

    let uni_schema = graph_ctx.storage().schema_manager().schema();

    let mut rows: Vec<HashMap<String, Value>> = Vec::new();
    if let Some(props) = uni_schema.properties.get(&label_name) {
        for (prop_name, prop_meta) in props {
            let mut row = HashMap::new();
            row.insert("property".to_string(), Value::String(prop_name.clone()));
            row.insert(
                "dataType".to_string(),
                Value::String(format!("{:?}", prop_meta.r#type)),
            );
            row.insert("nullable".to_string(), Value::Bool(prop_meta.nullable));

            let is_indexed = uni_schema.indexes.iter().any(|idx| match idx {
                uni_common::core::schema::IndexDefinition::Vector(v) => {
                    v.label == label_name && v.property == *prop_name
                }
                uni_common::core::schema::IndexDefinition::Scalar(s) => {
                    s.label == label_name && s.properties.contains(prop_name)
                }
                uni_common::core::schema::IndexDefinition::FullText(f) => {
                    f.label == label_name && f.properties.contains(prop_name)
                }
                uni_common::core::schema::IndexDefinition::Inverted(inv) => {
                    inv.label == label_name && inv.property == *prop_name
                }
                uni_common::core::schema::IndexDefinition::JsonFullText(j) => j.label == label_name,
                _ => false,
            });
            row.insert("indexed".to_string(), Value::Bool(is_indexed));

            let unique = uni_schema.constraints.iter().any(|c| {
                if let uni_common::core::schema::ConstraintTarget::Label(l) = &c.target
                    && l == &label_name
                    && c.enabled
                    && let uni_common::core::schema::ConstraintType::Unique { properties } =
                        &c.constraint_type
                {
                    return properties.contains(prop_name);
                }
                false
            });
            row.insert("unique".to_string(), Value::Bool(unique));

            rows.push(row);
        }
    }

    build_scalar_batch(&rows, yield_items, schema)
}

/// Build a typed Arrow column from an iterator of optional `Value`s.
///
/// Dispatches on `data_type` to build the appropriate Arrow array. For types
/// not explicitly handled (Utf8 fallback), values are stringified.
fn build_typed_column<'a>(
    values: impl Iterator<Item = Option<&'a Value>>,
    num_rows: usize,
    data_type: &DataType,
) -> ArrayRef {
    match data_type {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for val in values {
                match val.and_then(|v| v.as_i64()) {
                    Some(i) => builder.append_value(i),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for val in values {
                match val.and_then(|v| v.as_f64()) {
                    Some(f) => builder.append_value(f),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for val in values {
                match val.and_then(|v| v.as_bool()) {
                    Some(b) => builder.append_value(b),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Utf8 fallback: stringify values
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for val in values {
                match val {
                    Some(Value::String(s)) => builder.append_value(s),
                    Some(v) => builder.append_value(format!("{v}")),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
    }
}

/// Build a RecordBatch from scalar-valued rows for schema procedures.
fn build_scalar_batch(
    rows: &[HashMap<String, Value>],
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    if rows.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }

    let num_rows = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (idx, (name, _alias)) in yield_items.iter().enumerate() {
        let field = schema.field(idx);
        let values = rows.iter().map(|row| row.get(name));
        columns.push(build_typed_column(values, num_rows, field.data_type()));
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(Some(batch))
}

// ---------------------------------------------------------------------------
// External/registered procedures
// ---------------------------------------------------------------------------

/// Execute an externally registered procedure (e.g., TCK test procedures).
///
/// Looks up the procedure in the `ProcedureRegistry`, evaluates arguments,
/// filters data rows by matching input columns, and projects output columns.
async fn execute_registered_procedure(
    graph_ctx: &GraphExecutionContext,
    procedure_name: &str,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let registry = graph_ctx.procedure_registry().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "Procedure '{}' not supported in DataFusion engine (no procedure registry)",
            procedure_name
        ))
    })?;

    let proc_def = registry.get(procedure_name).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "ProcedureNotFound: Unknown procedure '{}'",
            procedure_name
        ))
    })?;

    // Validate argument count
    if args.len() != proc_def.params.len() {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "InvalidNumberOfArguments: Procedure '{}' expects {} argument(s), got {}",
            proc_def.name,
            proc_def.params.len(),
            args.len()
        )));
    }

    // Validate argument types
    for (i, (arg_val, param)) in args.iter().zip(&proc_def.params).enumerate() {
        if !arg_val.is_null() && !check_proc_type_compatible(arg_val, &param.param_type) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "InvalidArgumentType: Argument {} ('{}') of procedure '{}' has incompatible type",
                i, param.name, proc_def.name
            )));
        }
    }

    // Filter data rows: keep rows where input columns match the provided args
    let filtered: Vec<&HashMap<String, Value>> = proc_def
        .data
        .iter()
        .filter(|row| {
            for (param, arg_val) in proc_def.params.iter().zip(args) {
                if let Some(row_val) = row.get(&param.name)
                    && !proc_values_match(row_val, arg_val)
                {
                    return false;
                }
            }
            true
        })
        .collect();

    // If the procedure has no yield items (void procedure), return empty batch
    if yield_items.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }

    if filtered.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }

    // Project output columns based on yield items
    // We need to map yield names back to output column names in the procedure definition
    let num_rows = filtered.len();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (idx, (name, _alias)) in yield_items.iter().enumerate() {
        let field = schema.field(idx);
        let values = filtered.iter().map(|row| row.get(name.as_str()));
        columns.push(build_typed_column(values, num_rows, field.data_type()));
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(Some(batch))
}

/// Checks whether a value is compatible with a procedure type (DF engine version).
fn check_proc_type_compatible(
    val: &Value,
    expected: &crate::query::executor::procedure::ProcedureValueType,
) -> bool {
    use crate::query::executor::procedure::ProcedureValueType;
    match expected {
        ProcedureValueType::Any => true,
        ProcedureValueType::String => val.is_string(),
        ProcedureValueType::Boolean => val.is_bool(),
        ProcedureValueType::Integer => val.is_i64(),
        ProcedureValueType::Float => val.is_f64() || val.is_i64(),
        ProcedureValueType::Number => val.is_number(),
    }
}

/// Checks whether two values match for input-column filtering (DF engine version).
fn proc_values_match(row_val: &Value, arg_val: &Value) -> bool {
    if arg_val.is_null() || row_val.is_null() {
        return arg_val.is_null() && row_val.is_null();
    }
    // Compare numbers by f64 to handle int/float cross-comparison
    if let (Some(a), Some(b)) = (row_val.as_f64(), arg_val.as_f64()) {
        return (a - b).abs() < f64::EPSILON;
    }
    row_val == arg_val
}

// ---------------------------------------------------------------------------
// Algorithm procedures
// ---------------------------------------------------------------------------

async fn execute_algo_procedure(
    graph_ctx: &GraphExecutionContext,
    procedure_name: &str,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    use futures::StreamExt;
    use uni_algo::algo::procedures::AlgoContext;

    let registry = graph_ctx.algo_registry().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Algorithm registry not available".to_string(),
        )
    })?;

    let procedure = registry.get(procedure_name).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "Unknown algorithm: {}",
            procedure_name
        ))
    })?;

    let signature = procedure.signature();

    // Convert uni_common::Value args to serde_json::Value for algo crate.
    // Note: do NOT call validate_args here — the procedure's own execute()
    // already validates and fills defaults internally.
    let serde_args: Vec<serde_json::Value> = args.iter().cloned().map(|v| v.into()).collect();

    // Build AlgoContext — no L0Manager in the DF path (read-only snapshot)
    let algo_ctx = AlgoContext::new(graph_ctx.storage().clone(), None);

    // Execute and collect stream
    let mut stream = procedure.execute(algo_ctx, serde_args);
    let mut rows = Vec::new();
    while let Some(row_res) = stream.next().await {
        // Check timeout periodically
        if rows.len() % 1000 == 0 {
            graph_ctx
                .check_timeout()
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        }
        let row =
            row_res.map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        rows.push(row);
    }

    build_algo_batch(&rows, &signature, yield_items, schema)
}

/// Convert a `serde_json::Value` to a `uni_common::Value` for column building.
fn json_to_value(jv: &serde_json::Value) -> Value {
    match jv {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        other => Value::String(other.to_string()),
    }
}

/// Build a RecordBatch from algorithm result rows.
fn build_algo_batch(
    rows: &[uni_algo::algo::procedures::AlgoResultRow],
    signature: &uni_algo::algo::procedures::ProcedureSignature,
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    if rows.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }

    let num_rows = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (idx, (yield_name, _alias)) in yield_items.iter().enumerate() {
        let sig_idx = signature
            .yields
            .iter()
            .position(|(n, _)| *n == yield_name.as_str());

        // Convert serde_json values to uni_common::Value for the shared column builder
        let uni_values: Vec<Value> = rows
            .iter()
            .map(|row| match sig_idx {
                Some(si) => json_to_value(&row.values[si]),
                None => Value::Null,
            })
            .collect();

        let field = schema.field(idx);
        let values = uni_values.iter().map(Some);
        columns.push(build_typed_column(values, num_rows, field.data_type()));
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(Some(batch))
}

// ---------------------------------------------------------------------------
// Shared search argument helpers
// ---------------------------------------------------------------------------

/// Extract a required string argument from the argument list at a given position.
fn require_string_arg(args: &[Value], index: usize, description: &str) -> DFResult<String> {
    args.get(index)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("{description} must be a string"))
        })
}

/// Extract an optional filter string from the argument list.
/// Returns `None` if the argument is missing, null, or not a string.
fn extract_optional_filter(args: &[Value], index: usize) -> Option<String> {
    args.get(index).and_then(|v| {
        if v.is_null() {
            None
        } else {
            v.as_str().map(|s| s.to_string())
        }
    })
}

/// Extract an optional float threshold from the argument list.
/// Returns `None` if the argument is missing or null.
fn extract_optional_threshold(args: &[Value], index: usize) -> Option<f64> {
    args.get(index)
        .and_then(|v| if v.is_null() { None } else { v.as_f64() })
}

/// Extract a required integer argument from the argument list at a given position.
fn require_int_arg(args: &[Value], index: usize, description: &str) -> DFResult<usize> {
    args.get(index)
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "{description} must be an integer"
            ))
        })
}

// ---------------------------------------------------------------------------
// Vector/FTS/Hybrid search procedures
// ---------------------------------------------------------------------------

/// Auto-embed a text query using the vector index's embedding configuration.
///
/// Looks up the embedding config from the index on `label.property` and uses
/// it to embed the provided text query into a vector.
async fn auto_embed_text(
    storage: &Arc<uni_store::storage::manager::StorageManager>,
    label: &str,
    property: &str,
    query_text: &str,
) -> DFResult<Vec<f32>> {
    let uni_schema = storage.schema_manager().schema();
    let index_config = uni_schema.vector_index_for_property(label, property);

    let embedding_config = index_config
        .and_then(|cfg| cfg.embedding_config.as_ref())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Cannot auto-embed: vector index for {label}.{property} has no embedding_config. \
                 Either provide a pre-computed vector or create the index with embedding options."
            ))
        })?;

    let service = uni_store::embedding::create_embedding_service(&embedding_config.model)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    let embeddings = service
        .embed(&[query_text])
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    embeddings.into_iter().next().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Embedding service returned no results".to_string(),
        )
    })
}

async fn execute_vector_query(
    graph_ctx: &GraphExecutionContext,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.vector.query: first argument (label)")?;
    let property = require_string_arg(args, 1, "uni.vector.query: second argument (property)")?;

    let query_val = args.get(2).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.vector.query: third argument (query) is required".to_string(),
        )
    })?;

    let storage = graph_ctx.storage();

    let query_vector: Vec<f32> = if let Some(query_text) = query_val.as_str() {
        auto_embed_text(storage, &label, &property, query_text).await?
    } else {
        extract_vector(query_val)?
    };

    let k = require_int_arg(args, 3, "uni.vector.query: fourth argument (k)")?;
    let filter = extract_optional_filter(args, 4);
    let threshold = extract_optional_threshold(args, 5);
    let query_ctx = graph_ctx.query_context();

    let mut results = storage
        .vector_search(
            &label,
            &property,
            &query_vector,
            k,
            filter.as_deref(),
            Some(&query_ctx),
        )
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Apply threshold post-filter (on distance)
    if let Some(max_dist) = threshold {
        results.retain(|(_, dist)| *dist <= max_dist as f32);
    }

    if results.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }

    // Calculate scores using the same logic as the old executor
    let schema_manager = storage.schema_manager();
    let uni_schema = schema_manager.schema();
    let metric = uni_schema
        .vector_index_for_property(&label, &property)
        .map(|config| config.metric.clone())
        .unwrap_or(uni_common::core::schema::DistanceMetric::L2);

    build_search_result_batch(
        &results,
        &label,
        &metric,
        yield_items,
        target_properties,
        graph_ctx,
        schema,
    )
    .await
}

// ---------------------------------------------------------------------------
// FTS search procedure
// ---------------------------------------------------------------------------

async fn execute_fts_query(
    graph_ctx: &GraphExecutionContext,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.fts.query: first argument (label)")?;
    let property = require_string_arg(args, 1, "uni.fts.query: second argument (property)")?;
    let search_term = require_string_arg(args, 2, "uni.fts.query: third argument (search_term)")?;
    let k = require_int_arg(args, 3, "uni.fts.query: fourth argument (k)")?;
    let filter = extract_optional_filter(args, 4);
    let threshold = extract_optional_threshold(args, 5);

    let storage = graph_ctx.storage();
    let query_ctx = graph_ctx.query_context();

    let mut results = storage
        .fts_search(
            &label,
            &property,
            &search_term,
            k,
            filter.as_deref(),
            Some(&query_ctx),
        )
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    if let Some(min_score) = threshold {
        results.retain(|(_, score)| *score as f64 >= min_score);
    }

    if results.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }

    // FTS uses a "fake" L2 metric for the batch builder — scores are already BM25
    // We use L2 as a placeholder; the actual score column is built differently.
    build_search_result_batch(
        &results,
        &label,
        &uni_common::core::schema::DistanceMetric::L2,
        yield_items,
        target_properties,
        graph_ctx,
        schema,
    )
    .await
}

// ---------------------------------------------------------------------------
// Hybrid search procedure
// ---------------------------------------------------------------------------

async fn execute_hybrid_search(
    graph_ctx: &GraphExecutionContext,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.search: first argument (label)")?;

    // Parse properties: {vector: '...', fts: '...'} or just a string
    let properties_val = args.get(1).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.search: second argument (properties) is required".to_string(),
        )
    })?;

    let (vector_prop, fts_prop) = if let Some(obj) = properties_val.as_object() {
        let vec_prop = obj
            .get("vector")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let fts_prop = obj
            .get("fts")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        (vec_prop, fts_prop)
    } else if let Some(prop) = properties_val.as_str() {
        // Shorthand: just property name means both vector and FTS
        (Some(prop.to_string()), Some(prop.to_string()))
    } else {
        return Err(datafusion::error::DataFusionError::Execution(
            "Properties must be an object {vector: '...', fts: '...'} or a string".to_string(),
        ));
    };

    let query_text = require_string_arg(args, 2, "uni.search: third argument (query_text)")?;

    // Arg 3: query vector (optional, can be null)
    let query_vector: Option<Vec<f32>> = args.get(3).and_then(|v| {
        if v.is_null() {
            return None;
        }
        v.as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect()
        })
    });

    let k = require_int_arg(args, 4, "uni.search: fifth argument (k)")?;
    let filter = extract_optional_filter(args, 5);

    // Arg 6: options (optional)
    let options_val = args.get(6);
    let options_map = options_val.and_then(|v| v.as_object());
    let fusion_method = options_map
        .and_then(|m| m.get("method"))
        .and_then(|v| v.as_str())
        .unwrap_or("rrf")
        .to_string();
    let alpha = options_map
        .and_then(|m| m.get("alpha"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.5) as f32;
    let over_fetch_factor = options_map
        .and_then(|m| m.get("over_fetch"))
        .and_then(|v| v.as_f64())
        .unwrap_or(2.0) as f32;
    let rrf_k = options_map
        .and_then(|m| m.get("rrf_k"))
        .and_then(|v| v.as_u64())
        .unwrap_or(60) as usize;

    let over_fetch_k = (k as f32 * over_fetch_factor).ceil() as usize;

    let storage = graph_ctx.storage();
    let query_ctx = graph_ctx.query_context();

    // Execute vector search if configured
    let mut vector_results: Vec<(Vid, f32)> = Vec::new();
    if let Some(ref vec_prop) = vector_prop {
        // Get or generate query vector
        let qvec = if let Some(ref v) = query_vector {
            v.clone()
        } else {
            // Auto-embed the query text if embedding config exists
            auto_embed_text(storage, &label, vec_prop, &query_text)
                .await
                .unwrap_or_default()
        };

        if !qvec.is_empty() {
            vector_results = storage
                .vector_search(
                    &label,
                    vec_prop,
                    &qvec,
                    over_fetch_k,
                    filter.as_deref(),
                    Some(&query_ctx),
                )
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        }
    }

    // Execute FTS search if configured
    let mut fts_results: Vec<(Vid, f32)> = Vec::new();
    if let Some(ref fts_prop) = fts_prop {
        fts_results = storage
            .fts_search(
                &label,
                fts_prop,
                &query_text,
                over_fetch_k,
                filter.as_deref(),
                Some(&query_ctx),
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    }

    // Fuse results
    let fused_results = match fusion_method.as_str() {
        "weighted" => fuse_weighted(&vector_results, &fts_results, alpha),
        _ => fuse_rrf(&vector_results, &fts_results, rrf_k),
    };

    // Limit to k results
    let final_results: Vec<_> = fused_results.into_iter().take(k).collect();

    if final_results.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }

    // Build lookup maps for original scores
    let vec_score_map: HashMap<Vid, f32> = vector_results.iter().cloned().collect();
    let fts_score_map: HashMap<Vid, f32> = fts_results.iter().cloned().collect();
    let fts_max = fts_results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);

    // Get distance metric for vector score normalization
    let uni_schema = storage.schema_manager().schema();
    let metric = vector_prop
        .as_ref()
        .and_then(|vp| {
            uni_schema
                .vector_index_for_property(&label, vp)
                .map(|config| config.metric.clone())
        })
        .unwrap_or(uni_common::core::schema::DistanceMetric::L2);

    let score_ctx = HybridScoreContext {
        vec_score_map: &vec_score_map,
        fts_score_map: &fts_score_map,
        fts_max,
        metric: &metric,
    };

    build_hybrid_search_batch(
        &final_results,
        &score_ctx,
        &label,
        yield_items,
        target_properties,
        graph_ctx,
        schema,
    )
    .await
}

/// Reciprocal Rank Fusion (RRF) for combining search results.
/// RRF score = sum(1 / (k + rank)) for each result list.
fn fuse_rrf(vec_results: &[(Vid, f32)], fts_results: &[(Vid, f32)], k: usize) -> Vec<(Vid, f32)> {
    let mut scores: HashMap<Vid, f32> = HashMap::new();

    for (rank, (vid, _)) in vec_results.iter().enumerate() {
        let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
        *scores.entry(*vid).or_default() += rrf_score;
    }

    for (rank, (vid, _)) in fts_results.iter().enumerate() {
        let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
        *scores.entry(*vid).or_default() += rrf_score;
    }

    let mut results: Vec<_> = scores.into_iter().collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

/// Weighted fusion: alpha * vec_score + (1 - alpha) * fts_score.
/// Both score sets are normalized to [0, 1] range.
fn fuse_weighted(
    vec_results: &[(Vid, f32)],
    fts_results: &[(Vid, f32)],
    alpha: f32,
) -> Vec<(Vid, f32)> {
    // Normalize vector scores (distance -> similarity)
    let vec_max = vec_results.iter().map(|(_, s)| *s).fold(f32::MIN, f32::max);
    let vec_min = vec_results.iter().map(|(_, s)| *s).fold(f32::MAX, f32::min);
    let vec_range = if vec_max > vec_min {
        vec_max - vec_min
    } else {
        1.0
    };

    let fts_max = fts_results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);

    let vec_scores: HashMap<Vid, f32> = vec_results
        .iter()
        .map(|(vid, dist)| {
            let norm = 1.0 - (dist - vec_min) / vec_range;
            (*vid, norm)
        })
        .collect();

    let fts_scores: HashMap<Vid, f32> = fts_results
        .iter()
        .map(|(vid, score)| {
            let norm = if fts_max > 0.0 { score / fts_max } else { 0.0 };
            (*vid, norm)
        })
        .collect();

    let all_vids: std::collections::HashSet<Vid> = vec_scores
        .keys()
        .chain(fts_scores.keys())
        .cloned()
        .collect();

    let mut results: Vec<(Vid, f32)> = all_vids
        .into_iter()
        .map(|vid| {
            let vec_score = *vec_scores.get(&vid).unwrap_or(&0.0);
            let fts_score = *fts_scores.get(&vid).unwrap_or(&0.0);
            let fused = alpha * vec_score + (1.0 - alpha) * fts_score;
            (vid, fused)
        })
        .collect();

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

/// Precomputed score context for hybrid search batch building.
struct HybridScoreContext<'a> {
    vec_score_map: &'a HashMap<Vid, f32>,
    fts_score_map: &'a HashMap<Vid, f32>,
    fts_max: f32,
    metric: &'a uni_common::core::schema::DistanceMetric,
}

/// Build a RecordBatch for hybrid search results with fused, vector, and FTS scores.
async fn build_hybrid_search_batch(
    results: &[(Vid, f32)],
    scores: &HybridScoreContext<'_>,
    label: &str,
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    graph_ctx: &GraphExecutionContext,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let num_rows = results.len();
    let vids: Vec<Vid> = results.iter().map(|(vid, _)| *vid).collect();
    let fused_scores: Vec<f32> = results.iter().map(|(_, s)| *s).collect();

    // Pre-load properties for node-like yields
    let property_manager = graph_ctx.property_manager();
    let query_ctx = graph_ctx.query_context();
    let uni_schema = graph_ctx.storage().schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    let has_node_yield = yield_items
        .iter()
        .any(|(name, _)| map_yield_to_canonical(name) == "node");

    let props_map = if has_node_yield {
        property_manager
            .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
    } else {
        HashMap::new()
    };

    let mut columns: Vec<ArrayRef> = Vec::new();

    for (name, alias) in yield_items {
        let output_name = alias.as_ref().unwrap_or(name);
        let canonical = map_yield_to_canonical(name);

        match canonical.as_str() {
            "node" => {
                columns.extend(build_node_yield_columns(
                    &vids,
                    label,
                    output_name,
                    target_properties,
                    &props_map,
                    label_props,
                )?);
            }
            "vid" => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for vid in &vids {
                    builder.append_value(vid.as_u64() as i64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for score in &fused_scores {
                    builder.append_value(*score);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "vector_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&dist) = scores.vec_score_map.get(vid) {
                        let score = calculate_score(dist, scores.metric);
                        builder.append_value(score);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "fts_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&raw_score) = scores.fts_score_map.get(vid) {
                        let norm = if scores.fts_max > 0.0 {
                            raw_score / scores.fts_max
                        } else {
                            0.0
                        };
                        builder.append_value(norm);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "distance" => {
                // For hybrid search, distance is the vector distance if available
                let mut builder = Float64Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&dist) = scores.vec_score_map.get(vid) {
                        builder.append_value(dist as f64);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, 0);
                for _ in 0..num_rows {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(Some(batch))
}

// ---------------------------------------------------------------------------
// Shared search result batch builder
// ---------------------------------------------------------------------------

/// Build a RecordBatch for search procedures (vector, FTS) that yield
/// both node-like and scalar columns.
async fn build_search_result_batch(
    results: &[(Vid, f32)],
    label: &str,
    metric: &uni_common::core::schema::DistanceMetric,
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    graph_ctx: &GraphExecutionContext,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let num_rows = results.len();
    let vids: Vec<Vid> = results.iter().map(|(vid, _)| *vid).collect();
    let distances: Vec<f32> = results.iter().map(|(_, d)| *d).collect();

    // Pre-compute scores
    let scores: Vec<f32> = distances
        .iter()
        .map(|dist| calculate_score(*dist, metric))
        .collect();

    // Pre-load properties for all node-like yields
    let property_manager = graph_ctx.property_manager();
    let query_ctx = graph_ctx.query_context();
    let uni_schema = graph_ctx.storage().schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    // Load properties if any node-like yield needs them
    let has_node_yield = yield_items
        .iter()
        .any(|(name, _)| map_yield_to_canonical(name) == "node");

    let props_map = if has_node_yield {
        property_manager
            .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
    } else {
        HashMap::new()
    };

    // Build columns in schema order
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (name, alias) in yield_items {
        let output_name = alias.as_ref().unwrap_or(name);
        let canonical = map_yield_to_canonical(name);

        match canonical.as_str() {
            "node" => {
                columns.extend(build_node_yield_columns(
                    &vids,
                    label,
                    output_name,
                    target_properties,
                    &props_map,
                    label_props,
                )?);
            }
            "distance" => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for dist in &distances {
                    builder.append_value(*dist as f64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for score in &scores {
                    builder.append_value(*score);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "vid" => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for vid in &vids {
                    builder.append_value(vid.as_u64() as i64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Unknown yield — emit nulls
                let mut builder = StringBuilder::with_capacity(num_rows, 0);
                for _ in 0..num_rows {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(Some(batch))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the node-yield columns (_vid, variable, _labels, property columns) shared by
/// search result batch builders. Returns the columns to append.
fn build_node_yield_columns(
    vids: &[Vid],
    label: &str,
    output_name: &str,
    target_properties: &HashMap<String, Vec<String>>,
    props_map: &HashMap<Vid, uni_common::Properties>,
    label_props: Option<&std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>>,
) -> DFResult<Vec<ArrayRef>> {
    let num_rows = vids.len();
    let mut columns = Vec::new();

    // _vid column
    let mut vid_builder = UInt64Builder::with_capacity(num_rows);
    for vid in vids {
        vid_builder.append_value(vid.as_u64());
    }
    columns.push(Arc::new(vid_builder.finish()) as ArrayRef);

    // variable column (VID as string)
    let mut var_builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
    for vid in vids {
        var_builder.append_value(vid.to_string());
    }
    columns.push(Arc::new(var_builder.finish()) as ArrayRef);

    // _labels column
    let mut labels_builder = arrow_array::builder::ListBuilder::new(StringBuilder::new());
    for _ in 0..num_rows {
        labels_builder.values().append_value(label);
        labels_builder.append(true);
    }
    columns.push(Arc::new(labels_builder.finish()) as ArrayRef);

    // Property columns
    if let Some(props) = target_properties.get(output_name) {
        for prop_name in props {
            let data_type = resolve_property_type(prop_name, label_props);
            let column = crate::query::df_graph::scan::build_property_column_static(
                vids, props_map, prop_name, &data_type,
            )?;
            columns.push(column);
        }
    }

    Ok(columns)
}

/// Extract a vector from a Value.
fn extract_vector(val: &Value) -> DFResult<Vec<f32>> {
    match val {
        Value::Vector(vec) => Ok(vec.clone()),
        Value::List(arr) => {
            let mut vec = Vec::with_capacity(arr.len());
            for v in arr {
                if let Some(f) = v.as_f64() {
                    vec.push(f as f32);
                } else {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Query vector must contain numbers".to_string(),
                    ));
                }
            }
            Ok(vec)
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Query vector must be a list or vector".to_string(),
        )),
    }
}

/// Calculate normalized score from distance based on distance metric.
fn calculate_score(distance: f32, metric: &uni_common::core::schema::DistanceMetric) -> f32 {
    match metric {
        uni_common::core::schema::DistanceMetric::Cosine => {
            // Cosine distance → similarity: (2 - d) / 2
            (2.0 - distance) / 2.0
        }
        uni_common::core::schema::DistanceMetric::Dot => distance,
        _ => {
            // L2 and others: 1 / (1 + d)
            1.0 / (1.0 + distance)
        }
    }
}
