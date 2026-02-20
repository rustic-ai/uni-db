// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! External ID lookup execution plan for DataFusion.
//!
//! This module provides [`GraphExtIdLookupExec`], a DataFusion [`ExecutionPlan`] that
//! looks up a single vertex by its external ID (`ext_id`) in the main vertices table.
//!
//! # Column Naming Convention
//!
//! The output schema includes:
//! - `_vid` - vertex ID
//! - `ext_id` - external ID
//! - `_label` - vertex label name
//! - `{variable}.{property}` - materialized properties

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::compute_plan_properties;
use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
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
use uni_common::core::id::Vid;
use uni_store::storage::main_vertex::MainVertexDataset;

/// Execution plan for looking up a vertex by external ID.
///
/// Queries the main vertices table to find a vertex matching the given `ext_id`,
/// then materializes the specified properties.
pub struct GraphExtIdLookupExec {
    /// Graph execution context for storage access.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Variable name for column prefixing.
    variable: String,

    /// External ID to look up.
    ext_id: String,

    /// Properties to materialize.
    projected_properties: Vec<String>,

    /// Whether the lookup is optional (OPTIONAL MATCH).
    optional: bool,

    /// Output schema.
    schema: SchemaRef,

    /// Plan properties (cached).
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphExtIdLookupExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphExtIdLookupExec")
            .field("variable", &self.variable)
            .field("ext_id", &self.ext_id)
            .field("projected_properties", &self.projected_properties)
            .field("optional", &self.optional)
            .finish()
    }
}

impl GraphExtIdLookupExec {
    /// Create a new external ID lookup executor.
    pub fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        variable: impl Into<String>,
        ext_id: impl Into<String>,
        projected_properties: Vec<String>,
        optional: bool,
    ) -> Self {
        let variable = variable.into();
        let ext_id = ext_id.into();

        // Build output schema
        let schema = Self::build_schema(&variable, &projected_properties);
        let properties = compute_plan_properties(schema.clone());

        Self {
            graph_ctx,
            variable,
            ext_id,
            projected_properties,
            optional,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build the output schema.
    fn build_schema(variable: &str, properties: &[String]) -> SchemaRef {
        let mut fields = vec![
            Field::new(format!("{}._vid", variable), DataType::UInt64, false),
            Field::new(format!("{}.ext_id", variable), DataType::Utf8, false),
            Field::new(format!("{}._label", variable), DataType::Utf8, false),
        ];

        // Add property columns with variable prefix
        for prop in properties {
            let col_name = format!("{}.{}", variable, prop);
            fields.push(Field::new(&col_name, DataType::Utf8, true));
        }

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphExtIdLookupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphExtIdLookupExec: ext_id={}, variable={}, optional={}",
            self.ext_id, self.variable, self.optional
        )
    }
}

impl ExecutionPlan for GraphExtIdLookupExec {
    fn name(&self) -> &str {
        "GraphExtIdLookupExec"
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
                "GraphExtIdLookupExec has no children".to_string(),
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

        Ok(Box::pin(ExtIdLookupStream::new(
            self.graph_ctx.clone(),
            self.variable.clone(),
            self.ext_id.clone(),
            self.projected_properties.clone(),
            self.optional,
            self.schema.clone(),
            metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// State machine for ext_id lookup stream.
enum ExtIdLookupState {
    /// Initial state, ready to start lookup.
    Init,
    /// Executing the async lookup.
    Executing(Pin<Box<dyn std::future::Future<Output = DFResult<Option<RecordBatch>>> + Send>>),
    /// Stream is done.
    Done,
}

/// Stream that looks up a vertex by external ID.
struct ExtIdLookupStream {
    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Variable name for column prefixing.
    variable: String,

    /// External ID to look up.
    ext_id: String,

    /// Properties to materialize.
    properties: Vec<String>,

    /// Whether the lookup is optional.
    optional: bool,

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: ExtIdLookupState,

    /// Metrics.
    metrics: BaselineMetrics,
}

impl ExtIdLookupStream {
    fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        variable: String,
        ext_id: String,
        properties: Vec<String>,
        optional: bool,
        schema: SchemaRef,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            graph_ctx,
            variable,
            ext_id,
            properties,
            optional,
            schema,
            state: ExtIdLookupState::Init,
            metrics,
        }
    }
}

impl Stream for ExtIdLookupStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, ExtIdLookupState::Done);

            match state {
                ExtIdLookupState::Init => {
                    // Clone data for the async block
                    let graph_ctx = self.graph_ctx.clone();
                    let variable = self.variable.clone();
                    let ext_id = self.ext_id.clone();
                    let properties = self.properties.clone();
                    let optional = self.optional;
                    let schema = self.schema.clone();

                    let fut = async move {
                        // Check timeout
                        graph_ctx.check_timeout().map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;

                        execute_lookup(
                            &graph_ctx,
                            &variable,
                            &ext_id,
                            &properties,
                            optional,
                            &schema,
                        )
                        .await
                    };

                    self.state = ExtIdLookupState::Executing(Box::pin(fut));
                    // Continue loop to poll the future
                }
                ExtIdLookupState::Executing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.state = ExtIdLookupState::Done;
                        self.metrics
                            .record_output(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
                        return Poll::Ready(batch.map(Ok));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = ExtIdLookupState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = ExtIdLookupState::Executing(fut);
                        return Poll::Pending;
                    }
                },
                ExtIdLookupState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for ExtIdLookupStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Execute the ext_id lookup and materialize properties.
async fn execute_lookup(
    graph_ctx: &GraphExecutionContext,
    variable: &str,
    ext_id: &str,
    properties: &[String],
    optional: bool,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let storage = graph_ctx.storage();
    let lancedb = storage.lancedb_store();

    // Look up vertex by ext_id
    let found_vid = MainVertexDataset::find_by_ext_id(lancedb, ext_id)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    let Some(vid) = found_vid else {
        // No match found
        if optional {
            return Ok(Some(build_null_row(variable, properties, schema)?));
        }
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    };

    // Load properties
    let property_manager = graph_ctx.property_manager();
    let query_ctx = graph_ctx.query_context();

    let props_opt = property_manager
        .get_all_vertex_props_with_ctx(vid, Some(&query_ctx))
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    let Some(props) = props_opt else {
        // Vertex was deleted
        if optional {
            return Ok(Some(build_null_row(variable, properties, schema)?));
        }
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    };

    // Get labels for the vertex
    let labels = MainVertexDataset::find_labels_by_vid(lancedb, vid)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
        .unwrap_or_default();

    let label_name = labels
        .first()
        .cloned()
        .unwrap_or_else(|| "Unknown".to_string());

    // Build the result batch
    let batch = build_result_row(
        vid,
        ext_id,
        &label_name,
        &props,
        variable,
        properties,
        schema,
    )?;
    Ok(Some(batch))
}

/// Build a null row for optional matches with no result.
fn build_null_row(
    _variable: &str,
    _properties: &[String],
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    // For optional match with no result, we return a row with nulls
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // All columns are null for the optional case
    for field in schema.fields().iter() {
        match field.data_type() {
            DataType::UInt64 => {
                columns.push(Arc::new(arrow_array::UInt64Array::from(vec![
                    None as Option<u64>,
                ])));
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                builder.append_null();
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::new();
                builder.append_null();
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Build a result row with the found vertex data.
fn build_result_row(
    vid: Vid,
    ext_id: &str,
    label: &str,
    props: &HashMap<String, uni_common::Value>,
    _variable: &str,
    properties: &[String],
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // _vid column
    columns.push(Arc::new(UInt64Array::from(vec![vid.as_u64()])));

    // ext_id column
    let mut ext_id_builder = StringBuilder::new();
    ext_id_builder.append_value(ext_id);
    columns.push(Arc::new(ext_id_builder.finish()));

    // _label column
    let mut label_builder = StringBuilder::new();
    label_builder.append_value(label);
    columns.push(Arc::new(label_builder.finish()));

    // Property columns
    for prop in properties {
        let mut builder = StringBuilder::new();
        if let Some(val) = props.get(prop) {
            match val {
                uni_common::Value::String(s) => builder.append_value(s),
                uni_common::Value::Null => builder.append_null(),
                other => builder.append_value(other.to_string()),
            }
        } else {
            builder.append_null();
        }
        columns.push(Arc::new(builder.finish()));
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_schema() {
        let schema =
            GraphExtIdLookupExec::build_schema("n", &["name".to_string(), "age".to_string()]);

        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(1).name(), "n.ext_id");
        assert_eq!(schema.field(2).name(), "n._label");
        assert_eq!(schema.field(3).name(), "n.name");
        assert_eq!(schema.field(4).name(), "n.age");
    }
}
