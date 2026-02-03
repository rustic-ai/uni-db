// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Vector KNN search execution plan for DataFusion.
//!
//! This module provides [`GraphVectorKnnExec`], a DataFusion [`ExecutionPlan`] that
//! performs vector similarity search using the underlying vector index.
//!
//! # Example
//!
//! ```text
//! CALL uni.vector.query('Person', 'embedding', [0.1, 0.2, ...], 10)
//! YIELD node, score
//! ```

use crate::query::df_graph::GraphExecutionContext;
use arrow_array::builder::{Float32Builder, StringBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::core::id::Vid;
use uni_cypher::ast::Expr;

/// Vector KNN search execution plan.
///
/// Queries the vector index for the K nearest neighbors to a query vector,
/// returning matching vertex IDs and similarity scores.
pub struct GraphVectorKnnExec {
    /// Graph execution context for storage access.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Label ID to search in.
    label_id: u16,

    /// Label name for display.
    label_name: String,

    /// Variable name for result vertices.
    variable: String,

    /// Property name containing vector embeddings.
    property: String,

    /// Query vector expression.
    query_expr: Expr,

    /// Number of results to return.
    k: usize,

    /// Optional similarity threshold.
    threshold: Option<f32>,

    /// Query parameters for expression evaluation.
    params: HashMap<String, Value>,

    /// Output schema.
    schema: SchemaRef,

    /// Plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphVectorKnnExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphVectorKnnExec")
            .field("label_id", &self.label_id)
            .field("variable", &self.variable)
            .field("property", &self.property)
            .field("k", &self.k)
            .field("threshold", &self.threshold)
            .finish()
    }
}

impl GraphVectorKnnExec {
    /// Create a new vector KNN search execution plan.
    ///
    /// # Arguments
    ///
    /// * `graph_ctx` - Graph execution context
    /// * `label_id` - Label ID to search
    /// * `label_name` - Label name for display
    /// * `variable` - Variable name for results
    /// * `property` - Property containing vectors
    /// * `query_expr` - Expression evaluating to query vector
    /// * `k` - Number of results
    /// * `threshold` - Optional similarity threshold
    /// * `params` - Query parameters
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        label_id: u16,
        label_name: impl Into<String>,
        variable: impl Into<String>,
        property: impl Into<String>,
        query_expr: Expr,
        k: usize,
        threshold: Option<f32>,
        params: HashMap<String, Value>,
    ) -> Self {
        let variable = variable.into();
        let property = property.into();
        let label_name = label_name.into();

        let schema = Self::build_schema(&variable);
        let properties = Self::compute_properties(schema.clone());

        Self {
            graph_ctx,
            label_id,
            label_name,
            variable,
            property,
            query_expr,
            k,
            threshold,
            params,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build the output schema.
    ///
    /// Schema contains:
    /// - `_vid` - Vertex ID
    /// - `{variable}` - Variable identifier (as string for now)
    /// - `_score` - Similarity score
    fn build_schema(variable: &str) -> SchemaRef {
        let fields = vec![
            Field::new(format!("{}._vid", variable), DataType::UInt64, false),
            Field::new(variable, DataType::Utf8, false),
            Field::new(format!("{}._score", variable), DataType::Float32, true),
        ];
        Arc::new(Schema::new(fields))
    }

    /// Compute plan properties.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }

    /// Evaluate the query expression to extract the query vector.
    fn evaluate_query_vector(&self) -> DFResult<Vec<f32>> {
        let value = evaluate_simple_expr(&self.query_expr, &self.params)?;

        match value {
            Value::Array(arr) => {
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
                "Query vector must be an array".to_string(),
            )),
        }
    }
}

impl DisplayAs for GraphVectorKnnExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "GraphVectorKnnExec: label={}, property={}, k={}, variable={}",
                    self.label_name, self.property, self.k, self.variable
                )
            }
        }
    }
}

impl ExecutionPlan for GraphVectorKnnExec {
    fn name(&self) -> &str {
        "GraphVectorKnnExec"
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
                "GraphVectorKnnExec has no children".to_string(),
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

        // Evaluate query vector upfront
        let query_vector = self.evaluate_query_vector()?;

        Ok(Box::pin(VectorKnnStream::new(
            self.graph_ctx.clone(),
            self.label_name.clone(),
            self.variable.clone(),
            self.property.clone(),
            query_vector,
            self.k,
            self.threshold,
            self.schema.clone(),
            metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// State machine for vector KNN stream.
enum VectorKnnState {
    /// Initial state, ready to start search.
    Init,
    /// Executing the async search.
    Executing(Pin<Box<dyn std::future::Future<Output = DFResult<Option<RecordBatch>>> + Send>>),
    /// Stream is done.
    Done,
}

/// Stream that executes vector KNN search.
struct VectorKnnStream {
    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Label name to search.
    label_name: String,

    /// Variable name for results.
    variable: String,

    /// Property name containing vectors.
    property: String,

    /// Query vector.
    query_vector: Vec<f32>,

    /// Number of results.
    k: usize,

    /// Similarity threshold.
    threshold: Option<f32>,

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: VectorKnnState,

    /// Metrics.
    metrics: BaselineMetrics,
}

impl VectorKnnStream {
    #[expect(clippy::too_many_arguments)]
    fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        label_name: String,
        variable: String,
        property: String,
        query_vector: Vec<f32>,
        k: usize,
        threshold: Option<f32>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            graph_ctx,
            label_name,
            variable,
            property,
            query_vector,
            k,
            threshold,
            schema,
            state: VectorKnnState::Init,
            metrics,
        }
    }
}

impl Stream for VectorKnnStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, VectorKnnState::Done);

            match state {
                VectorKnnState::Init => {
                    // Clone data for async block
                    let graph_ctx = self.graph_ctx.clone();
                    let label_name = self.label_name.clone();
                    let variable = self.variable.clone();
                    let property = self.property.clone();
                    let query_vector = self.query_vector.clone();
                    let k = self.k;
                    let threshold = self.threshold;
                    let schema = self.schema.clone();

                    let fut = async move {
                        // Check timeout
                        graph_ctx.check_timeout().map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;

                        execute_vector_search(
                            &graph_ctx,
                            &label_name,
                            &variable,
                            &property,
                            &query_vector,
                            k,
                            threshold,
                            &schema,
                        )
                        .await
                    };

                    self.state = VectorKnnState::Executing(Box::pin(fut));
                    // Continue loop to poll the future
                }
                VectorKnnState::Executing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.state = VectorKnnState::Done;
                        self.metrics
                            .record_output(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
                        return Poll::Ready(batch.map(Ok));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = VectorKnnState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = VectorKnnState::Executing(fut);
                        return Poll::Pending;
                    }
                },
                VectorKnnState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for VectorKnnStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Execute the vector search and build results.
#[expect(clippy::too_many_arguments)]
async fn execute_vector_search(
    graph_ctx: &GraphExecutionContext,
    label_name: &str,
    variable: &str,
    property: &str,
    query_vector: &[f32],
    k: usize,
    threshold: Option<f32>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let storage = graph_ctx.storage();
    let query_ctx = graph_ctx.query_context();

    // Execute vector search
    let results = storage
        .vector_search(
            label_name,
            property,
            query_vector,
            k,
            None,
            Some(&query_ctx),
        )
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Filter by threshold and build result
    let mut vids = Vec::new();
    let mut scores = Vec::new();

    for (vid, distance) in results {
        // Convert distance to similarity (assuming Cosine/Dot product)
        let similarity = 1.0 - distance;

        if let Some(thresh) = threshold
            && similarity < thresh
        {
            continue;
        }

        vids.push(vid);
        scores.push(similarity);
    }

    if vids.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }

    // Build the record batch
    let batch = build_result_batch(&vids, &scores, variable, schema)?;
    Ok(Some(batch))
}

/// Build a result batch from VIDs and scores.
fn build_result_batch(
    vids: &[Vid],
    scores: &[f32],
    _variable: &str,
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let num_rows = vids.len();

    // Build _vid column
    let mut vid_builder = UInt64Builder::with_capacity(num_rows);
    for vid in vids {
        vid_builder.append_value(vid.as_u64());
    }

    // Build variable column (VID as string for now)
    let mut var_builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
    for vid in vids {
        var_builder.append_value(vid.to_string());
    }

    // Build score column
    let mut score_builder = Float32Builder::with_capacity(num_rows);
    for &score in scores {
        score_builder.append_value(score);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(vid_builder.finish()),
        Arc::new(var_builder.finish()),
        Arc::new(score_builder.finish()),
    ];

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Evaluate a simple expression to get a JSON value.
///
/// Supports:
/// - Literal values
/// - Parameter references ($param)
/// - Literal lists
fn evaluate_simple_expr(expr: &Expr, params: &HashMap<String, Value>) -> DFResult<Value> {
    match expr {
        Expr::Literal(lit) => Ok(lit.to_json_value()),

        Expr::Parameter(name) => params.get(name).cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("Parameter '{}' not found", name))
        }),

        Expr::List(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(evaluate_simple_expr(item, params)?);
            }
            Ok(Value::Array(values))
        }

        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "Unsupported expression type for vector query: {:?}",
            expr
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_cypher::ast::CypherLiteral;

    #[test]
    fn test_build_schema() {
        let schema = GraphVectorKnnExec::build_schema("n");

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(1).name(), "n");
        assert_eq!(schema.field(2).name(), "n._score");
    }

    #[test]
    fn test_evaluate_literal_list() {
        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::Float(0.1)),
            Expr::Literal(CypherLiteral::Float(0.2)),
            Expr::Literal(CypherLiteral::Float(0.3)),
        ]);

        let result = evaluate_simple_expr(&expr, &HashMap::new()).unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_evaluate_parameter() {
        let expr = Expr::Parameter("query".to_string());
        let mut params = HashMap::new();
        params.insert(
            "query".to_string(),
            Value::Array(vec![
                Value::Number(serde_json::Number::from_f64(0.1).unwrap()),
                Value::Number(serde_json::Number::from_f64(0.2).unwrap()),
            ]),
        );

        let result = evaluate_simple_expr(&expr, &params).unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }
}
