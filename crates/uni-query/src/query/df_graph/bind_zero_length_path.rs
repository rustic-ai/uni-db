// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Zero-length path binding execution plan for DataFusion.
//!
//! This module provides [`BindZeroLengthPathExec`], a DataFusion [`ExecutionPlan`] that
//! converts a single-node pattern `p = (a)` into a Path with one node and zero edges.
//!
//! # Example
//!
//! ```text
//! Input:   [{"a._vid": 1, "a._label": "Person", ...}]
//! Bind:    p = (a)
//! Output:  [{"a._vid": 1, "a._label": "Person", ..., "p": Path{nodes: [node1], edges: []}}]
//! ```

use super::GraphExecutionContext;
use super::common::{compute_plan_properties, labels_data_type};
use arrow_array::builder::{
    LargeBinaryBuilder, ListBuilder, StringBuilder, StructBuilder, UInt64Builder,
};
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_store::runtime::l0_visibility;

/// Execution plan that binds a zero-length path for single-node patterns.
///
/// For patterns like `p = (a)`, this creates a Path struct with one node
/// (from the bound node variable) and zero edges.
pub struct BindZeroLengthPathExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Node variable name (e.g., "a" in `p = (a)`).
    node_variable: String,

    /// Path variable name (e.g., "p" in `p = (a)`).
    path_variable: String,

    /// Graph execution context for property/label lookup.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for BindZeroLengthPathExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindZeroLengthPathExec")
            .field("node_variable", &self.node_variable)
            .field("path_variable", &self.path_variable)
            .finish()
    }
}

impl BindZeroLengthPathExec {
    /// Create a new zero-length path binding execution plan.
    ///
    /// # Arguments
    ///
    /// * `input` - Input plan providing rows with the node variable
    /// * `node_variable` - Variable name of the bound node
    /// * `path_variable` - Variable name for the path
    /// * `graph_ctx` - Graph context for property/label lookups
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        node_variable: String,
        path_variable: String,
        graph_ctx: Arc<GraphExecutionContext>,
    ) -> Self {
        // Build output schema: input schema + path variable column
        let schema = Self::build_schema(input.schema(), &path_variable);
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            node_variable,
            path_variable,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema by adding the path variable column.
    ///
    /// The path structure matches what VLP produces:
    /// - `nodes`: List of node structs with `_vid`, `_label`, `properties`
    /// - `relationships`: List of edge structs (empty for zero-length paths)
    fn build_schema(input_schema: SchemaRef, path_variable: &str) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        // Node struct fields: _vid, _labels, properties (as LargeBinary/JSON)
        let node_struct_fields = Fields::from(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_labels", labels_data_type(), true),
            Field::new("properties", DataType::LargeBinary, true),
        ]);
        let node_item = Field::new("item", DataType::Struct(node_struct_fields), true);
        let nodes_field = Field::new("nodes", DataType::List(Arc::new(node_item)), true);

        // Edge struct fields: _eid, _type_name, _src, _dst, properties
        let edge_struct_fields = Fields::from(vec![
            Field::new("_eid", DataType::UInt64, false),
            Field::new("_type_name", DataType::Utf8, false),
            Field::new("_src", DataType::UInt64, false),
            Field::new("_dst", DataType::UInt64, false),
            Field::new("properties", DataType::LargeBinary, true),
        ]);
        let edge_item = Field::new("item", DataType::Struct(edge_struct_fields), true);
        let relationships_field =
            Field::new("relationships", DataType::List(Arc::new(edge_item)), true);

        // Path struct with nodes and relationships
        let path_struct_fields = Fields::from(vec![nodes_field, relationships_field]);
        fields.push(Field::new(
            path_variable,
            DataType::Struct(path_struct_fields),
            true,
        ));

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for BindZeroLengthPathExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "BindZeroLengthPathExec: {} = ({})",
                    self.path_variable, self.node_variable
                )
            }
        }
    }
}

impl ExecutionPlan for BindZeroLengthPathExec {
    fn name(&self) -> &str {
        "BindZeroLengthPathExec"
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
            return Err(datafusion::error::DataFusionError::Plan(
                "BindZeroLengthPathExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.node_variable.clone(),
            self.path_variable.clone(),
            self.graph_ctx.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(BindZeroLengthPathStream {
            input: input_stream,
            node_variable: self.node_variable.clone(),
            schema: self.schema.clone(),
            graph_ctx: self.graph_ctx.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Stream that performs the zero-length path binding.
struct BindZeroLengthPathStream {
    /// Input stream.
    input: SendableRecordBatchStream,

    /// Node variable name.
    node_variable: String,

    /// Output schema.
    schema: SchemaRef,

    /// Graph context for lookups.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Metrics.
    metrics: BaselineMetrics,
}

/// Extract a typed value from a column at a given row index.
fn extract_column_value<T: arrow_array::Array + 'static, R>(
    batch: &RecordBatch,
    col_name: &str,
    row_idx: usize,
    extract_fn: impl FnOnce(&T, usize) -> R,
) -> Option<R> {
    let (idx, _) = batch.schema().column_with_name(col_name)?;
    let col = batch.column(idx);
    let arr = col.as_any().downcast_ref::<T>()?;
    if arr.is_valid(row_idx) {
        Some(extract_fn(arr, row_idx))
    } else {
        None
    }
}

impl BindZeroLengthPathStream {
    /// Process a single input batch.
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let num_rows = batch.num_rows();
        let query_ctx = self.graph_ctx.query_context();

        let vid_col_name = format!("{}._vid", self.node_variable);

        // Create builders for nodes and empty edges
        let node_struct_fields = Fields::from(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_labels", labels_data_type(), true),
            Field::new("properties", DataType::LargeBinary, true),
        ]);
        let edge_struct_fields = Fields::from(vec![
            Field::new("_eid", DataType::UInt64, false),
            Field::new("_type_name", DataType::Utf8, false),
            Field::new("_src", DataType::UInt64, false),
            Field::new("_dst", DataType::UInt64, false),
            Field::new("properties", DataType::LargeBinary, true),
        ]);

        let mut nodes_builder = ListBuilder::new(StructBuilder::new(
            node_struct_fields,
            vec![
                Box::new(UInt64Builder::new()),
                Box::new(ListBuilder::new(StringBuilder::new())),
                Box::new(LargeBinaryBuilder::new()),
            ],
        ));
        let mut rels_builder = ListBuilder::new(StructBuilder::from_fields(edge_struct_fields, 0));
        let mut path_validity = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let vid: Option<uni_common::core::id::Vid> = extract_column_value(
                &batch,
                &vid_col_name,
                row_idx,
                |arr: &arrow_array::UInt64Array, i| uni_common::core::id::Vid::from(arr.value(i)),
            );

            if vid.is_none() {
                nodes_builder.append(false);
                rels_builder.append(false);
                path_validity.push(false);
                continue;
            }

            self.append_node_to_builder(&mut nodes_builder, vid, &query_ctx);
            rels_builder.append(true);
            path_validity.push(true);
        }

        let nodes_array = Arc::new(nodes_builder.finish()) as ArrayRef;
        let rels_array = Arc::new(rels_builder.finish()) as ArrayRef;

        let path_array = StructArray::try_new(
            Fields::from(vec![
                Arc::new(Field::new("nodes", nodes_array.data_type().clone(), true)),
                Arc::new(Field::new(
                    "relationships",
                    rels_array.data_type().clone(),
                    true,
                )),
            ]),
            vec![nodes_array, rels_array],
            Some(arrow::buffer::NullBuffer::from(path_validity)),
        )?;

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(path_array));

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    /// Append a node entry to the nodes list builder.
    fn append_node_to_builder(
        &self,
        nodes_builder: &mut ListBuilder<StructBuilder>,
        vid: Option<uni_common::core::id::Vid>,
        query_ctx: &uni_store::QueryContext,
    ) {
        let nodes_struct = nodes_builder.values();

        let (vid_value, all_labels, props_json) = match vid {
            Some(v) => {
                let labels = l0_visibility::get_vertex_labels(v, query_ctx);
                let props = l0_visibility::get_vertex_properties(v, query_ctx)
                    .map(|p| super::common::encode_props_to_cv(&p));
                (v.as_u64(), labels, props)
            }
            None => (0, vec![], None),
        };

        nodes_struct
            .field_builder::<UInt64Builder>(0)
            .unwrap()
            .append_value(vid_value);

        let labels_builder = nodes_struct
            .field_builder::<ListBuilder<StringBuilder>>(1)
            .unwrap();
        for lbl in &all_labels {
            labels_builder.values().append_value(lbl);
        }
        labels_builder.append(true);

        let props_builder = nodes_struct.field_builder::<LargeBinaryBuilder>(2).unwrap();
        match props_json {
            Some(json) => props_builder.append_value(&json),
            None => props_builder.append_null(),
        }

        nodes_struct.append(true);
        nodes_builder.append(true);
    }
}

impl Stream for BindZeroLengthPathStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let _timer = self.metrics.elapsed_compute().timer();
                let result = self.process_batch(batch);
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for BindZeroLengthPathStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
