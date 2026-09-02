// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Materialise a relationship's endpoint nodes so `startNode`/`endNode` can
//! answer with properties rather than an identity.
//!
//! [`EndpointHydrateExec`] reads a relationship column, takes the `_src` / `_dst`
//! VIDs it already carries, and appends two CypherValue columns holding the
//! endpoint nodes with their labels and properties.
//!
//! # Why this exists
//!
//! `startNode(r)` is normally answered without any lookup: `resolve_traversal_endpoints`
//! rewrites the call to the traversal's own endpoint *variable*, which is already
//! in scope with its properties materialised. That rewrite is dropped the moment
//! a projection stops carrying both endpoints — `WITH e AS rel` — and it never
//! applies at all when the relationship did not come from a traversal, as with
//! `relationships(path)`.
//!
//! What is left in those cases is the relationship value, and it does carry the
//! endpoint VIDs (`_src` / `_dst` on the edge struct). What it does not carry is
//! the endpoint's *properties*. Without them `startnode_endnode_impl` falls back
//! to a stand-in map holding only `_vid`, so `id(startNode(rel))` answers
//! correctly while `startNode(rel).name` is NULL — the silent trade that #188
//! refused in writing.
//!
//! # Why a column rather than a smarter UDF
//!
//! The properties have to be fetched from storage, which is async, and
//! `startnode_endnode_impl` is a synchronous scalar UDF with no `PropertyManager`
//! and no `QueryContext`. The pre-fetch discipline here is the one
//! [`super::bind_fixed_path`] and [`super::bind_zero_length_path`] already use:
//! collect the batch's VIDs, await one batched fetch, then build Arrow
//! synchronously.
//!
//! The hydrated columns are handed to the *existing* UDF as extra arguments —
//! it already scans `args[1..]` for a node whose `_vid` matches the endpoint —
//! so neither the UDF nor property-access compilation needed to change.

use super::GraphExecutionContext;
use super::common::{EntityPropertyCache, compute_plan_properties};
use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value;
use uni_common::core::id::Vid;

/// The column name carrying a hydrated endpoint for `rel`.
///
/// Underscore-prefixed and dotted so it cannot collide with a user variable —
/// Cypher has no way to name a variable `_endpoint.src.rel`.
pub fn endpoint_column(rel: &str, is_start: bool) -> String {
    let side = if is_start { "src" } else { "dst" };
    format!("_endpoint.{side}.{rel}")
}

/// Appends hydrated endpoint-node columns for one relationship column.
pub struct EndpointHydrateExec {
    input: Arc<dyn ExecutionPlan>,
    /// The relationship column to read `_src` / `_dst` from.
    rel_column: String,
    graph_ctx: Arc<GraphExecutionContext>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for EndpointHydrateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndpointHydrateExec")
            .field("rel_column", &self.rel_column)
            .finish()
    }
}

impl EndpointHydrateExec {
    /// Wrap `input` so both endpoints of `rel_column` are materialised.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        rel_column: String,
        graph_ctx: Arc<GraphExecutionContext>,
    ) -> Self {
        let mut fields: Vec<Field> = input
            .schema()
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();
        for is_start in [true, false] {
            fields.push(Field::new(
                endpoint_column(&rel_column, is_start),
                DataType::LargeBinary,
                true,
            ));
        }
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let properties = compute_plan_properties(schema.clone());
        Self {
            input,
            rel_column,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for EndpointHydrateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EndpointHydrateExec: endpoints of {}", self.rel_column)
    }
}

impl ExecutionPlan for EndpointHydrateExec {
    fn name(&self) -> &str {
        "EndpointHydrateExec"
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "EndpointHydrateExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.rel_column.clone(),
            self.graph_ctx.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(EndpointHydrateStream {
            input: input_stream,
            rel_column: self.rel_column.clone(),
            schema: self.schema.clone(),
            graph_ctx: self.graph_ctx.clone(),
            metrics: BaselineMetrics::new(&self.metrics, partition),
            pending: None,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// An in-flight property fetch and the batch waiting on it.
type PendingPrefetch = (
    Pin<Box<dyn std::future::Future<Output = DFResult<EntityPropertyCache>> + Send>>,
    RecordBatch,
);

struct EndpointHydrateStream {
    input: SendableRecordBatchStream,
    rel_column: String,
    schema: SchemaRef,
    graph_ctx: Arc<GraphExecutionContext>,
    metrics: BaselineMetrics,
    pending: Option<PendingPrefetch>,
}

/// The endpoint VIDs of one row, `(src, dst)` per relationship.
///
/// A scalar relationship column yields one pair per row; a *list* of
/// relationships — `relationships(path)`, which is what a list comprehension
/// iterates — yields one pair per element, so the hydrated output stays aligned
/// element-for-element with the list the comprehension flattens.
type Endpoints = Vec<(Option<Vid>, Option<Vid>)>;

impl EndpointHydrateStream {
    /// Read `(src, dst)` for every row of the relationship column.
    ///
    /// The relationship reaches us in one of two encodings — the struct built by
    /// `add_edge_structural_projection`, or a CypherValue blob — and
    /// `Value::entity_endpoints` reads both, so this decodes to `Value` rather
    /// than matching on the Arrow type.
    fn row_endpoints(&self, batch: &RecordBatch) -> DFResult<Vec<Endpoints>> {
        let Ok(idx) = batch.schema().index_of(&self.rel_column) else {
            return Ok(vec![Vec::new(); batch.num_rows()]);
        };
        let column = batch.column(idx);
        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let value =
                uni_store::storage::arrow_convert::arrow_to_value(column.as_ref(), row, None);
            out.push(match &value {
                Value::List(items) => items
                    .iter()
                    .map(|v| (endpoint_vid(v, true), endpoint_vid(v, false)))
                    .collect(),
                other => vec![(endpoint_vid(other, true), endpoint_vid(other, false))],
            });
        }
        Ok(out)
    }

    /// Is the relationship column a list, and therefore hydrated per element?
    fn column_is_list(&self, batch: &RecordBatch) -> bool {
        let Ok(idx) = batch.schema().index_of(&self.rel_column) else {
            return false;
        };
        (0..batch.num_rows()).any(|row| {
            matches!(
                uni_store::storage::arrow_convert::arrow_to_value(
                    batch.column(idx).as_ref(),
                    row,
                    None
                ),
                Value::List(_)
            )
        })
    }

    fn process_batch(
        &self,
        batch: RecordBatch,
        endpoints: &[Endpoints],
        cache: &EntityPropertyCache,
    ) -> DFResult<RecordBatch> {
        let query_ctx = self.graph_ctx.query_context();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

        let as_list = self.column_is_list(&batch);
        for is_start in [true, false] {
            let mut builder = LargeBinaryBuilder::new();
            for per_row in endpoints {
                let mut hydrated: Vec<Value> = Vec::with_capacity(per_row.len());
                for (src, dst) in per_row {
                    let vid = if is_start { src } else { dst };
                    hydrated.push(match vid {
                        Some(v) => hydrate_node(*v, &query_ctx, cache),
                        None => Value::Null,
                    });
                }
                if as_list {
                    // One hydrated node per element, in the list's own order, so
                    // the comprehension can flatten it with the same offsets it
                    // uses for the elements themselves.
                    builder.append_value(uni_common::cypher_value_codec::encode(&Value::List(
                        hydrated,
                    )));
                } else {
                    match hydrated.into_iter().next() {
                        Some(Value::Null) | None => builder.append_null(),
                        Some(node) => {
                            builder.append_value(uni_common::cypher_value_codec::encode(&node))
                        }
                    }
                }
            }
            columns.push(Arc::new(builder.finish()) as ArrayRef);
        }

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

/// Read one endpoint VID out of a relationship value.
///
/// Mirrors `extract_endpoint_vid` in the UDF layer: the struct form uses
/// `_src` / `_dst`, and the traversal's flat columns use `_src_vid` / `_dst_vid`.
fn endpoint_vid(value: &Value, is_start: bool) -> Option<Vid> {
    match value {
        Value::Edge(edge) => Some(if is_start { edge.src } else { edge.dst }),
        Value::Map(map) => {
            let keys: [&str; 2] = if is_start {
                ["_src", "_src_vid"]
            } else {
                ["_dst", "_dst_vid"]
            };
            keys.iter()
                .find_map(|k| map.get(*k))
                .and_then(|v| v.as_u64())
                .map(Vid::from)
        }
        _ => None,
    }
}

/// Build the node value for `vid`, with its labels and properties inline.
///
/// Inline rather than nested under a `properties` blob so the result decodes to
/// the same shape a structural projection produces — that is what lets
/// `startNode(rel).name` resolve through the ordinary property path.
fn hydrate_node(
    vid: Vid,
    query_ctx: &uni_store::runtime::context::QueryContext,
    cache: &EntityPropertyCache,
) -> Value {
    use uni_store::runtime::l0_visibility;

    let mut map: HashMap<String, Value> = HashMap::new();
    map.insert("_vid".to_string(), Value::Int(vid.as_u64() as i64));

    let labels = l0_visibility::get_vertex_labels(vid, query_ctx);
    map.insert(
        "_labels".to_string(),
        Value::List(labels.into_iter().map(Value::String).collect()),
    );

    let props = match cache.vertex(vid) {
        Some(p) => Some(std::borrow::Cow::Borrowed(p)),
        None => l0_visibility::get_vertex_properties(vid, query_ctx).map(std::borrow::Cow::Owned),
    };
    if let Some(props) = props {
        for (k, v) in props.iter() {
            if !k.starts_with('_') {
                map.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Map(map)
}

impl Stream for EndpointHydrateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some((mut fut, batch)) = self.pending.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(cache)) => {
                        let _timer = self.metrics.elapsed_compute().timer();
                        let endpoints = match self.row_endpoints(&batch) {
                            Ok(e) => e,
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        };
                        let result = self.process_batch(batch, &endpoints, &cache);
                        if let Ok(ref b) = result {
                            self.metrics.record_output(b.num_rows());
                        }
                        return Poll::Ready(Some(result));
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Pending => {
                        self.pending = Some((fut, batch));
                        return Poll::Pending;
                    }
                }
            }

            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let endpoints = match self.row_endpoints(&batch) {
                        Ok(e) => e,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    let vids: Vec<Vid> = endpoints
                        .iter()
                        .flatten()
                        .flat_map(|(s, d)| [*s, *d])
                        .flatten()
                        .collect();
                    let graph_ctx = self.graph_ctx.clone();
                    let fut = Box::pin(async move {
                        let query_ctx = graph_ctx.query_context();
                        EntityPropertyCache::prefetch(&graph_ctx, &query_ctx, &vids, &[]).await
                    });
                    self.pending = Some((fut, batch));
                }
                other => return other,
            }
        }
    }
}

impl RecordBatchStream for EndpointHydrateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
