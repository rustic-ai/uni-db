// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

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

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, StringBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch};
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
use uni_common::core::schema::{DistanceMetric, PropertyMeta};
use uni_cypher::ast::Expr;
use uni_plugin::traits::index::{IndexHandle, IndexKind};

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    arrow_err, calculate_score, compute_plan_properties, evaluate_simple_expr, exec_err,
    labels_data_type,
};
use crate::query::df_graph::scan::{property_field, resolve_property_type};

/// Vector-retrieval source for a [`GraphVectorKnnExec`].
///
/// The exec is kind-agnostic above the retrieval step: threshold filter,
/// score normalization, label / vid emission, and property hydration all
/// run identically on the `Vec<(Vid, f32)>` produced here. Only the
/// retrieval call differs:
///
/// - [`VectorSource::Native`] dispatches to
///   `StorageManager::vector_search`, which routes through the built-in
///   vector backend (Lance / memory / etc.).
/// - [`VectorSource::Plugin`] dispatches to
///   [`IndexHandle::probe`] on a host-registered plugin handle (see
///   `PluginRegistry::register_index_handle`). The planner picks this
///   variant when an index-name lookup against the plugin registry
///   succeeds; this preserves the "no behavior change for built-ins"
///   invariant — native indexes never register a handle so the
///   fall-through is `Native`.
#[derive(Clone)]
pub(crate) enum VectorSource {
    /// Native built-in vector backend (default).
    Native,
    /// Plugin-supplied live handle.
    Plugin {
        /// Kind that produced the handle. Informational; kept so the
        /// planner-level dispatch log can include it.
        kind: IndexKind,
        /// The handle to probe.
        handle: Arc<dyn IndexHandle>,
    },
}

impl fmt::Debug for VectorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native => f.write_str("Native"),
            Self::Plugin { kind, .. } => f.debug_struct("Plugin").field("kind", kind).finish(),
        }
    }
}

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

    /// Target vertex properties to materialize.
    target_properties: Vec<String>,

    /// Output schema.
    schema: SchemaRef,

    /// Plan properties.
    properties: Arc<PlanProperties>,

    /// Vector-retrieval source. `Native` for the built-in path;
    /// `Plugin { handle, .. }` when the planner found a registered
    /// `IndexHandle` for this index's name in `PluginRegistry`.
    source: VectorSource,

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
        target_properties: Vec<String>,
    ) -> Self {
        let variable = variable.into();
        let property = property.into();
        let label_name = label_name.into();

        // Resolve property types from schema
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let label_props = uni_schema.properties.get(label_name.as_str());

        let schema = Self::build_schema(&variable, &target_properties, label_props);
        let properties = compute_plan_properties(schema.clone());

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
            target_properties,
            schema,
            properties,
            source: VectorSource::Native,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Create a new vector KNN search execution plan that dispatches
    /// retrieval through a plugin-registered [`IndexHandle`] instead of
    /// the native storage path.
    ///
    /// All other behavior (threshold, scoring, property hydration) is
    /// identical to [`Self::new`].
    #[expect(clippy::too_many_arguments)]
    pub fn with_plugin_source(
        graph_ctx: Arc<GraphExecutionContext>,
        label_id: u16,
        label_name: impl Into<String>,
        variable: impl Into<String>,
        property: impl Into<String>,
        query_expr: Expr,
        k: usize,
        threshold: Option<f32>,
        params: HashMap<String, Value>,
        target_properties: Vec<String>,
        kind: IndexKind,
        handle: Arc<dyn IndexHandle>,
    ) -> Self {
        let mut exec = Self::new(
            graph_ctx,
            label_id,
            label_name,
            variable,
            property,
            query_expr,
            k,
            threshold,
            params,
            target_properties,
        );
        exec.source = VectorSource::Plugin { kind, handle };
        exec
    }

    /// Build the output schema.
    ///
    /// Schema contains:
    /// - `{variable}._vid` - Vertex ID
    /// - `{variable}` - Variable identifier (as string for now)
    /// - `{variable}._score` - Similarity score
    /// - `{variable}.{prop}` - Property columns
    fn build_schema(
        variable: &str,
        target_properties: &[String],
        label_props: Option<&HashMap<String, PropertyMeta>>,
    ) -> SchemaRef {
        let mut fields = vec![
            Field::new(format!("{}._vid", variable), DataType::UInt64, false),
            Field::new(variable, DataType::Utf8, false),
            Field::new(format!("{}._labels", variable), labels_data_type(), true),
            Field::new(format!("{}._score", variable), DataType::Float32, true),
        ];

        // Add property columns
        for prop_name in target_properties {
            let col_name = format!("{}.{}", variable, prop_name);
            let arrow_type = resolve_property_type(prop_name, label_props);
            let uni_type = label_props
                .and_then(|p| p.get(prop_name))
                .map(|m| &m.r#type);
            fields.push(property_field(&col_name, arrow_type, uni_type));
        }

        Arc::new(Schema::new(fields))
    }

    /// Evaluate the query expression to extract the query vector.
    fn evaluate_query_vector(&self) -> DFResult<Vec<f32>> {
        let value = evaluate_simple_expr(&self.query_expr, &self.params, &HashMap::new())?;

        match value {
            Value::Vector(vec) => Ok(vec),
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

    /// Evaluate the query expression to a multi-vector (a list of token vectors).
    fn evaluate_query_multivector(&self) -> DFResult<Vec<Vec<f32>>> {
        let value = evaluate_simple_expr(&self.query_expr, &self.params, &HashMap::new())?;
        let Value::List(tokens) = value else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Multi-vector query must be a list of vectors".to_string(),
            ));
        };
        tokens
            .into_iter()
            .map(|tok| match tok {
                Value::Vector(v) => Ok(v),
                Value::List(inner) => inner
                    .iter()
                    .map(|x| {
                        x.as_f64().map(|f| f as f32).ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(
                                "Multi-vector query token must contain numbers".to_string(),
                            )
                        })
                    })
                    .collect(),
                _ => Err(datafusion::error::DataFusionError::Execution(
                    "Multi-vector query must be a list of vectors".to_string(),
                )),
            })
            .collect()
    }

    /// Whether the queried property is a multi-vector (`List<FixedSizeList>`) column.
    fn is_multivector_property(&self) -> bool {
        let uni_schema = self.graph_ctx.storage().schema_manager().schema();
        let label_props = uni_schema.properties.get(self.label_name.as_str());
        matches!(
            resolve_property_type(&self.property, label_props),
            DataType::List(ref inner)
                if matches!(inner.data_type(), DataType::FixedSizeList(_, _))
        )
    }
}

impl DisplayAs for GraphVectorKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphVectorKnnExec: label={}, property={}, k={}, variable={}",
            self.label_name, self.property, self.k, self.variable
        )
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

    fn properties(&self) -> &Arc<PlanProperties> {
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

        // Evaluate the query upfront: a multi-vector (ColBERT) property takes a
        // list of token vectors and routes to MaxSim retrieval; a dense property
        // takes a single vector.
        let (query_vector, multivec_query) = if self.is_multivector_property() {
            (Vec::new(), Some(self.evaluate_query_multivector()?))
        } else {
            (self.evaluate_query_vector()?, None)
        };

        Ok(Box::pin(VectorKnnStream::new(
            self.graph_ctx.clone(),
            self.label_name.clone(),
            self.variable.clone(),
            self.property.clone(),
            query_vector,
            multivec_query,
            self.k,
            self.threshold,
            self.target_properties.clone(),
            self.schema.clone(),
            self.source.clone(),
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

    /// Query vector (dense path).
    query_vector: Vec<f32>,

    /// Query multi-vector (ColBERT / MaxSim path); `Some` when the queried
    /// property is a `List<Vector>` column.
    multivec_query: Option<Vec<Vec<f32>>>,

    /// Number of results.
    k: usize,

    /// Similarity threshold.
    threshold: Option<f32>,

    /// Target vertex properties to materialize.
    target_properties: Vec<String>,

    /// Output schema.
    schema: SchemaRef,

    /// Vector-retrieval source (native or plugin handle).
    source: VectorSource,

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
        multivec_query: Option<Vec<Vec<f32>>>,
        k: usize,
        threshold: Option<f32>,
        target_properties: Vec<String>,
        schema: SchemaRef,
        source: VectorSource,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            graph_ctx,
            label_name,
            variable,
            property,
            query_vector,
            multivec_query,
            k,
            threshold,
            target_properties,
            schema,
            source,
            state: VectorKnnState::Init,
            metrics,
        }
    }
}

impl Stream for VectorKnnStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
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
                    let multivec_query = self.multivec_query.clone();
                    let k = self.k;
                    let threshold = self.threshold;
                    let target_properties = self.target_properties.clone();
                    let schema = self.schema.clone();
                    let source = self.source.clone();

                    let fut = async move {
                        // Check timeout
                        graph_ctx.check_timeout().map_err(exec_err)?;

                        execute_vector_search(
                            &graph_ctx,
                            &label_name,
                            &variable,
                            &property,
                            &query_vector,
                            multivec_query.as_deref(),
                            k,
                            threshold,
                            &target_properties,
                            &schema,
                            &source,
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
    multivec_query: Option<&[Vec<f32>]>,
    k: usize,
    threshold: Option<f32>,
    target_properties: &[String],
    schema: &SchemaRef,
    source: &VectorSource,
) -> DFResult<Option<RecordBatch>> {
    let storage = graph_ctx.storage();

    // Retrieve `(vid, distance)` pairs via the configured source.
    let results = retrieve_vid_scores(
        graph_ctx,
        label_name,
        property,
        query_vector,
        multivec_query,
        k,
        source,
    )
    .await?;

    // Look up the distance metric for this vector property so we can
    // convert raw distances into normalised similarity scores correctly.
    // Multi-vector (ColBERT) defaults to Cosine; dense defaults to L2.
    let default_metric = if multivec_query.is_some() {
        DistanceMetric::Cosine
    } else {
        DistanceMetric::L2
    };
    let metric = storage
        .schema_manager()
        .schema()
        .vector_index_for_property(label_name, property)
        .map(|cfg| cfg.metric.clone())
        .unwrap_or(default_metric);

    // Filter by threshold and build result
    let mut vids = Vec::new();
    let mut scores = Vec::new();

    for (vid, value) in results {
        // Multi-vector (ColBERT) results are already exact MaxSim similarities
        // (higher is better); dense distances are converted to a similarity here.
        let similarity = if multivec_query.is_some() {
            value
        } else {
            calculate_score(value, &metric)
        };

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

    // Build the base record batch (VID, variable, score)
    let batch = build_result_batch(
        &vids,
        &scores,
        variable,
        target_properties,
        label_name,
        graph_ctx,
        schema,
    )
    .await?;
    Ok(Some(batch))
}

/// Retrieve `(Vid, distance)` pairs for the configured [`VectorSource`].
///
/// - [`VectorSource::Native`] delegates to `StorageManager::vector_search`,
///   which routes through the built-in vector backend (Lance / memory).
/// - [`VectorSource::Plugin`] builds a 1-row probe batch carrying the
///   query vector as `FixedSizeList<Float32>`, calls
///   [`IndexHandle::probe`], then extracts the `(vid: Int64, distance:
///   Float32)` columns from the result. Plugin handles emit vids as
///   `i64`; we widen via `as u64` because graph vids are stored as
///   non-negative `u64` and test fixtures (and any sane real index) only
///   produce non-negative integers.
async fn retrieve_vid_scores(
    graph_ctx: &GraphExecutionContext,
    label_name: &str,
    property: &str,
    query_vector: &[f32],
    multivec_query: Option<&[Vec<f32>]>,
    k: usize,
    source: &VectorSource,
) -> DFResult<Vec<(Vid, f32)>> {
    match source {
        VectorSource::Native => {
            let storage = graph_ctx.storage();
            let query_ctx = graph_ctx.query_context();
            // A multi-vector property routes to MaxSim retrieval with L0
            // visibility: Lance generates candidates over flushed data and the
            // shared re-ranker merges live L0 rows and re-scores by exact MaxSim.
            // The inline predicate path uses default ANN tuning (nprobes/refine
            // are set via the `uni.vector.query` options map, which a predicate
            // cannot express) and the default over-fetch.
            if let Some(mv) = multivec_query {
                let property_manager = graph_ctx.property_manager();
                let metric = storage
                    .schema_manager()
                    .schema()
                    .vector_index_for_property(label_name, property)
                    .map(|cfg| cfg.metric.clone())
                    .unwrap_or(DistanceMetric::Cosine);
                let retrieval_k = k
                    .saturating_mul(
                        crate::query::df_graph::search_procedures::MULTIVECTOR_OVER_FETCH,
                    )
                    .max(k);
                let (ranked, _props) =
                    crate::query::df_graph::search_procedures::multivector_rerank(
                        storage,
                        property_manager,
                        &query_ctx,
                        label_name,
                        property,
                        mv,
                        k,
                        retrieval_k,
                        None,
                        uni_store::VectorQueryOpts::default(),
                        &metric,
                    )
                    .await?;
                return Ok(ranked);
            }
            storage
                .vector_search(
                    label_name,
                    property,
                    query_vector,
                    k,
                    None,
                    uni_store::VectorQueryOpts::default(),
                    Some(&query_ctx),
                )
                .await
                .map_err(exec_err)
        }
        VectorSource::Plugin { handle, .. } => {
            // Build a single-row query batch:
            //     [ vector: FixedSizeList<Float32, dim> ]
            let dim = i32::try_from(query_vector.len()).map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "query vector exceeds i32::MAX dimensions".to_string(),
                )
            })?;
            let item_field = Arc::new(Field::new("item", DataType::Float32, true));
            let mut fsl_builder =
                FixedSizeListBuilder::new(Float32Builder::with_capacity(query_vector.len()), dim)
                    .with_field(Arc::clone(&item_field));
            for &v in query_vector {
                fsl_builder.values().append_value(v);
            }
            fsl_builder.append(true);
            let fsl: FixedSizeListArray = fsl_builder.finish();

            let query_schema = Arc::new(Schema::new(vec![Field::new(
                "vector",
                DataType::FixedSizeList(item_field, dim),
                false,
            )]));
            let query_batch =
                RecordBatch::try_new(query_schema, vec![Arc::new(fsl)]).map_err(arrow_err)?;

            let result = handle.probe(&query_batch, k).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "IndexHandle::probe failed: {e:?}"
                ))
            })?;

            // Result schema is `[vid: Int64, distance: Float32]` per the
            // `IndexHandle` trait contract.
            let vid_col = result
                .column_by_name("vid")
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "IndexHandle::probe result missing `vid` column".to_string(),
                    )
                })?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "IndexHandle::probe result `vid` column is not Int64".to_string(),
                    )
                })?;
            let dist_col = result
                .column_by_name("distance")
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "IndexHandle::probe result missing `distance` column".to_string(),
                    )
                })?
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "IndexHandle::probe result `distance` column is not Float32".to_string(),
                    )
                })?;

            let mut pairs = Vec::with_capacity(result.num_rows());
            for i in 0..result.num_rows() {
                if vid_col.is_null(i) {
                    continue;
                }
                let vid_i64 = vid_col.value(i);
                let dist = if dist_col.is_null(i) {
                    f32::INFINITY
                } else {
                    dist_col.value(i)
                };
                pairs.push((Vid::from(vid_i64 as u64), dist));
            }
            Ok(pairs)
        }
    }
}

/// Build a result batch from VIDs and scores, including hydrated properties.
async fn build_result_batch(
    vids: &[Vid],
    scores: &[f32],
    _variable: &str,
    target_properties: &[String],
    label_name: &str,
    graph_ctx: &GraphExecutionContext,
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

    // Build _labels column
    let mut labels_builder = arrow_array::builder::ListBuilder::new(StringBuilder::new());
    for _vid in vids {
        labels_builder.values().append_value(label_name);
        labels_builder.append(true);
    }

    // Build score column
    let mut score_builder = Float32Builder::with_capacity(num_rows);
    for &score in scores {
        score_builder.append_value(score);
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(vid_builder.finish()),
        Arc::new(var_builder.finish()),
        Arc::new(labels_builder.finish()),
        Arc::new(score_builder.finish()),
    ];

    // Hydrate property columns
    if !target_properties.is_empty() {
        let property_manager = graph_ctx.property_manager();
        let query_ctx = graph_ctx.query_context();

        // Prune the fetch to the properties actually hydrated below, so unread
        // heavy columns (e.g. `List(Vector)`) are not decoded (issue #134).
        let props_map = property_manager
            .get_batch_vertex_props_for_label_projected(
                vids,
                label_name,
                Some(&query_ctx),
                Some(target_properties),
            )
            .await
            .map_err(exec_err)?;

        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let label_props = uni_schema.properties.get(label_name);

        for prop_name in target_properties {
            let data_type = resolve_property_type(prop_name, label_props);
            let column = crate::query::df_graph::scan::build_property_column_static(
                vids, &props_map, prop_name, &data_type,
            )
            .map_err(exec_err)?;
            columns.push(column);
        }
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(arrow_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_cypher::ast::CypherLiteral;

    #[test]
    fn test_build_schema() {
        let schema = GraphVectorKnnExec::build_schema("n", &[], None);

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(1).name(), "n");
        assert_eq!(schema.field(2).name(), "n._labels");
        assert_eq!(schema.field(3).name(), "n._score");
    }

    #[test]
    fn test_evaluate_literal_list() {
        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::Float(0.1)),
            Expr::Literal(CypherLiteral::Float(0.2)),
            Expr::Literal(CypherLiteral::Float(0.3)),
        ]);

        let result = evaluate_simple_expr(&expr, &HashMap::new(), &HashMap::new()).unwrap();
        match result {
            Value::List(arr) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn test_evaluate_parameter() {
        let expr = Expr::Parameter("query".to_string());
        let mut params = HashMap::new();
        params.insert(
            "query".to_string(),
            Value::List(vec![Value::Float(0.1), Value::Float(0.2)]),
        );

        let result = evaluate_simple_expr(&expr, &params, &HashMap::new()).unwrap();
        match result {
            Value::List(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn test_build_schema_with_extra_properties() {
        let extra_props = vec!["name".to_string(), "embedding".to_string()];
        let schema = GraphVectorKnnExec::build_schema("doc", &extra_props, None);

        // Should have base fields + extra properties
        assert!(schema.field_with_name("doc._vid").is_ok());
        assert!(schema.field_with_name("doc").is_ok());
        assert!(schema.field_with_name("doc._score").is_ok());
        assert!(
            schema.field_with_name("doc.name").is_ok(),
            "Extra property 'name' should be in schema"
        );
        assert!(
            schema.field_with_name("doc.embedding").is_ok(),
            "Extra property 'embedding' should be in schema"
        );
    }

    #[test]
    fn test_evaluate_variable() {
        // Test that a variable expression resolves to the variable's value
        let expr = Expr::Variable("x".to_string());
        let mut variables = HashMap::new();
        variables.insert(
            "x".to_string(),
            Value::List(vec![Value::Float(0.5), Value::Float(0.6)]),
        );

        let result = evaluate_simple_expr(&expr, &HashMap::new(), &variables).unwrap();
        match result {
            Value::List(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected list, got {:?}", result),
        }
    }
}
