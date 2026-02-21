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
                        "score" => {
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
                // Generic fallback: all columns as Utf8
                for (name, alias) in yield_items {
                    let col_name = alias.as_ref().unwrap_or(name);
                    fields.push(Field::new(col_name, DataType::Utf8, true));
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
        name if name.starts_with("uni.algo.") => {
            execute_algo_procedure(graph_ctx, name, args, yield_items, schema).await
        }
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "Procedure '{}' not supported in DataFusion engine",
            procedure_name
        ))),
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
        let mut row = HashMap::new();
        row.insert("state".to_string(), Value::String("ONLINE".to_string()));

        match idx {
            uni_common::core::schema::IndexDefinition::Vector(v) => {
                row.insert("name".to_string(), Value::String(v.name));
                row.insert("type".to_string(), Value::String("VECTOR".to_string()));
                row.insert("label".to_string(), Value::String(v.label));
                row.insert(
                    "properties".to_string(),
                    Value::String(serde_json::to_string(&[&v.property]).unwrap_or_default()),
                );
            }
            uni_common::core::schema::IndexDefinition::FullText(f) => {
                row.insert("name".to_string(), Value::String(f.name));
                row.insert("type".to_string(), Value::String("FULLTEXT".to_string()));
                row.insert("label".to_string(), Value::String(f.label));
                row.insert(
                    "properties".to_string(),
                    Value::String(serde_json::to_string(&f.properties).unwrap_or_default()),
                );
            }
            uni_common::core::schema::IndexDefinition::Scalar(s) => {
                row.insert("name".to_string(), Value::String(s.name));
                row.insert("type".to_string(), Value::String("SCALAR".to_string()));
                row.insert("label".to_string(), Value::String(s.label));
                row.insert(
                    "properties".to_string(),
                    Value::String(serde_json::to_string(&s.properties).unwrap_or_default()),
                );
            }
            uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                row.insert("name".to_string(), Value::String(j.name));
                row.insert("type".to_string(), Value::String("JSON_FTS".to_string()));
                row.insert("label".to_string(), Value::String(j.label));
                row.insert(
                    "properties".to_string(),
                    Value::String(serde_json::to_string(&[&j.column]).unwrap_or_default()),
                );
            }
            uni_common::core::schema::IndexDefinition::Inverted(inv) => {
                row.insert("name".to_string(), Value::String(inv.name));
                row.insert("type".to_string(), Value::String("INVERTED".to_string()));
                row.insert("label".to_string(), Value::String(inv.label));
                row.insert(
                    "properties".to_string(),
                    Value::String(serde_json::to_string(&[&inv.property]).unwrap_or_default()),
                );
            }
            _ => {
                row.insert("name".to_string(), Value::String("UNKNOWN".to_string()));
                row.insert("type".to_string(), Value::String("UNKNOWN".to_string()));
            }
        }
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
    let label_name = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "uni.schema.labelInfo: first argument (label) must be a string".to_string(),
            )
        })?
        .to_string();

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
        match field.data_type() {
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in rows {
                    if let Some(val) = row.get(name) {
                        match val {
                            Value::String(s) => builder.append_value(s),
                            other => builder.append_value(format!("{}", other)),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in rows {
                    if let Some(val) = row.get(name) {
                        if let Some(i) = val.as_i64() {
                            builder.append_value(i);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in rows {
                    if let Some(val) = row.get(name) {
                        if let Some(f) = val.as_f64() {
                            builder.append_value(f);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in rows {
                    if let Some(val) = row.get(name) {
                        if let Some(b) = val.as_bool() {
                            builder.append_value(b);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Fallback: convert everything to string
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in rows {
                    if let Some(val) = row.get(name) {
                        builder.append_value(format!("{}", val));
                    } else {
                        builder.append_null();
                    }
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
        // Find the column index in the signature yields
        let sig_idx = signature
            .yields
            .iter()
            .position(|(n, _)| *n == yield_name.as_str());

        let field = schema.field(idx);
        match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in rows {
                    if let Some(si) = sig_idx {
                        match &row.values[si] {
                            serde_json::Value::Number(n) => {
                                builder.append_value(n.as_i64().unwrap_or(0));
                            }
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in rows {
                    if let Some(si) = sig_idx {
                        match &row.values[si] {
                            serde_json::Value::Number(n) => {
                                builder.append_value(n.as_f64().unwrap_or(0.0));
                            }
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in rows {
                    if let Some(si) = sig_idx {
                        match &row.values[si] {
                            serde_json::Value::Bool(b) => builder.append_value(*b),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Utf8 fallback
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in rows {
                    if let Some(si) = sig_idx {
                        match &row.values[si] {
                            serde_json::Value::String(s) => builder.append_value(s),
                            serde_json::Value::Null => builder.append_null(),
                            other => builder.append_value(other.to_string()),
                        }
                    } else {
                        builder.append_null();
                    }
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
// Vector search procedure
// ---------------------------------------------------------------------------

async fn execute_vector_query(
    graph_ctx: &GraphExecutionContext,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "uni.vector.query: first argument (label) must be a string".to_string(),
            )
        })?
        .to_string();

    let property = args
        .get(1)
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "uni.vector.query: second argument (property) must be a string".to_string(),
            )
        })?
        .to_string();

    let query_vector = extract_vector(&args[2])?;

    let k = args.get(3).and_then(|v| v.as_u64()).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.vector.query: fourth argument (k) must be an integer".to_string(),
        )
    })? as usize;

    // Optional filter (arg 4) and threshold (arg 5)
    let filter = args.get(4).and_then(|v| {
        if v.is_null() {
            None
        } else {
            v.as_str().map(|s| s.to_string())
        }
    });

    let threshold = args
        .get(5)
        .and_then(|v| if v.is_null() { None } else { v.as_f64() });

    let storage = graph_ctx.storage();
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
    let label = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "uni.fts.query: first argument (label) must be a string".to_string(),
            )
        })?
        .to_string();

    let property = args
        .get(1)
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "uni.fts.query: second argument (property) must be a string".to_string(),
            )
        })?
        .to_string();

    let search_term = args
        .get(2)
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "uni.fts.query: third argument (search_term) must be a string".to_string(),
            )
        })?
        .to_string();

    let k = args.get(3).and_then(|v| v.as_u64()).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.fts.query: fourth argument (k) must be an integer".to_string(),
        )
    })? as usize;

    let filter = args.get(4).and_then(|v| {
        if v.is_null() {
            None
        } else {
            v.as_str().map(|s| s.to_string())
        }
    });

    let threshold = args
        .get(5)
        .and_then(|v| if v.is_null() { None } else { v.as_f64() });

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
    let mut field_idx = 0;

    for (name, alias) in yield_items {
        let output_name = alias.as_ref().unwrap_or(name);
        let canonical = map_yield_to_canonical(name);

        match canonical.as_str() {
            "node" => {
                // _vid column
                let mut vid_builder = UInt64Builder::with_capacity(num_rows);
                for vid in &vids {
                    vid_builder.append_value(vid.as_u64());
                }
                columns.push(Arc::new(vid_builder.finish()));
                field_idx += 1;

                // variable column (VID as string)
                let mut var_builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
                for vid in &vids {
                    var_builder.append_value(vid.to_string());
                }
                columns.push(Arc::new(var_builder.finish()));
                field_idx += 1;

                // _labels column
                let mut labels_builder =
                    arrow_array::builder::ListBuilder::new(StringBuilder::new());
                for _ in 0..num_rows {
                    labels_builder.values().append_value(label);
                    labels_builder.append(true);
                }
                columns.push(Arc::new(labels_builder.finish()));
                field_idx += 1;

                // Property columns
                if let Some(props) = target_properties.get(output_name.as_str()) {
                    for prop_name in props {
                        let data_type = resolve_property_type(prop_name, label_props);
                        let column = crate::query::df_graph::scan::build_property_column_static(
                            &vids, &props_map, prop_name, &data_type,
                        )?;
                        columns.push(column);
                        field_idx += 1;
                    }
                }
            }
            "distance" => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for dist in &distances {
                    builder.append_value(*dist as f64);
                }
                columns.push(Arc::new(builder.finish()));
                field_idx += 1;
            }
            "score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for score in &scores {
                    builder.append_value(*score);
                }
                columns.push(Arc::new(builder.finish()));
                field_idx += 1;
            }
            "vid" => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for vid in &vids {
                    builder.append_value(vid.as_u64() as i64);
                }
                columns.push(Arc::new(builder.finish()));
                field_idx += 1;
            }
            _ => {
                // Unknown yield — emit nulls
                let mut builder = StringBuilder::with_capacity(num_rows, 0);
                for _ in 0..num_rows {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
                field_idx += 1;
            }
        }
    }

    let _ = field_idx; // suppress unused warning

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(Some(batch))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
