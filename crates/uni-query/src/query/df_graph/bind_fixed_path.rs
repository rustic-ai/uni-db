// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fixed-length path binding execution plan for DataFusion.
//!
//! This module provides [`BindFixedPathExec`], a DataFusion [`ExecutionPlan`] that
//! synthesizes a path struct from existing node and edge columns in the batch.
//!
//! Used for patterns like `p = (a)-[r]->(b)` or `p = (a)-[r1]->(b)-[r2]->(c)`
//! where the traversals are single-hop and the path variable needs to be materialized.

use super::GraphExecutionContext;
use super::common::compute_plan_properties;
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
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::l0_visibility;

/// Execution plan that binds a fixed-length path from existing node/edge columns.
///
/// For patterns like `p = (a)-[r]->(b)` or `p = (a)-[r1]->(b)-[r2]->(c)`,
/// this creates a Path struct with nodes and relationships from the already-computed
/// columns in the input batch.
pub struct BindFixedPathExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Node variable names in path order (e.g., ["a", "b"] or ["a", "b", "c"]).
    node_variables: Vec<String>,

    /// Edge variable names in path order (e.g., ["r"] or ["r1", "r2"]).
    edge_variables: Vec<String>,

    /// Path variable name (e.g., "p" in `p = (a)-[r]->(b)`).
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

impl fmt::Debug for BindFixedPathExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindFixedPathExec")
            .field("node_variables", &self.node_variables)
            .field("edge_variables", &self.edge_variables)
            .field("path_variable", &self.path_variable)
            .finish()
    }
}

impl BindFixedPathExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        node_variables: Vec<String>,
        edge_variables: Vec<String>,
        path_variable: String,
        graph_ctx: Arc<GraphExecutionContext>,
    ) -> Self {
        let schema = Self::build_schema(input.schema(), &path_variable);
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            node_variables,
            edge_variables,
            path_variable,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema by adding the path variable column.
    /// Uses the same path struct format as BindZeroLengthPathExec and VLP.
    fn build_schema(input_schema: SchemaRef, path_variable: &str) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        fields.push(build_path_struct_field(path_variable));
        Arc::new(Schema::new(fields))
    }
}

/// Build the path struct field definition used by all path binding executors.
pub fn build_path_struct_field(path_variable: &str) -> Field {
    let node_struct_fields = Fields::from(vec![
        Field::new("_vid", DataType::UInt64, false),
        Field::new("_label", DataType::Utf8, true),
        Field::new("properties", DataType::LargeBinary, true),
    ]);
    let node_item = Field::new("item", DataType::Struct(node_struct_fields), true);
    let nodes_field = Field::new("nodes", DataType::List(Arc::new(node_item)), false);

    let edge_struct_fields = Fields::from(vec![
        Field::new("_eid", DataType::UInt64, false),
        Field::new("_type_name", DataType::Utf8, false),
        Field::new("_src", DataType::UInt64, false),
        Field::new("_dst", DataType::UInt64, false),
        Field::new("properties", DataType::LargeBinary, true),
    ]);
    let edge_item = Field::new("item", DataType::Struct(edge_struct_fields), true);
    let relationships_field =
        Field::new("relationships", DataType::List(Arc::new(edge_item)), false);

    let path_struct_fields = Fields::from(vec![nodes_field, relationships_field]);
    Field::new(
        path_variable,
        DataType::Struct(path_struct_fields),
        false,
    )
}

impl DisplayAs for BindFixedPathExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "BindFixedPathExec: {} = ({}) via [{}]",
                    self.path_variable,
                    self.node_variables.join(", "),
                    self.edge_variables.join(", "),
                )
            }
        }
    }
}

impl ExecutionPlan for BindFixedPathExec {
    fn name(&self) -> &str {
        "BindFixedPathExec"
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
                "BindFixedPathExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.node_variables.clone(),
            self.edge_variables.clone(),
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

        Ok(Box::pin(BindFixedPathStream {
            input: input_stream,
            node_variables: self.node_variables.clone(),
            edge_variables: self.edge_variables.clone(),
            schema: self.schema.clone(),
            graph_ctx: self.graph_ctx.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Stream that synthesizes path structs from existing node/edge columns.
struct BindFixedPathStream {
    input: SendableRecordBatchStream,
    node_variables: Vec<String>,
    edge_variables: Vec<String>,
    schema: SchemaRef,
    graph_ctx: Arc<GraphExecutionContext>,
    metrics: BaselineMetrics,
}

impl BindFixedPathStream {
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let num_rows = batch.num_rows();
        let query_ctx = self.graph_ctx.query_context();

        // Build node and edge struct fields (same as BindZeroLengthPathExec)
        let node_struct_fields = Fields::from(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_label", DataType::Utf8, true),
            Field::new("properties", DataType::LargeBinary, true),
        ]);
        let edge_struct_fields = Fields::from(vec![
            Field::new("_eid", DataType::UInt64, false),
            Field::new("_type_name", DataType::Utf8, false),
            Field::new("_src", DataType::UInt64, false),
            Field::new("_dst", DataType::UInt64, false),
            Field::new("properties", DataType::LargeBinary, true),
        ]);

        let mut nodes_builder = ListBuilder::new(StructBuilder::from_fields(
            node_struct_fields,
            num_rows * self.node_variables.len(),
        ));
        let mut rels_builder = ListBuilder::new(StructBuilder::from_fields(
            edge_struct_fields,
            num_rows * self.edge_variables.len(),
        ));

        for row_idx in 0..num_rows {
            // Add all nodes in path order
            for node_var in &self.node_variables {
                let vid_col_name = format!("{}._vid", node_var);
                let label_col_name = format!("{}._labels", node_var);

                let vid: Option<Vid> = extract_column_value(
                    &batch,
                    &vid_col_name,
                    row_idx,
                    |arr: &arrow_array::UInt64Array, i| Vid::from(arr.value(i)),
                );

                // Try _labels first (structural projection), then _label (scan column)
                let label: Option<String> = extract_column_value(
                    &batch,
                    &label_col_name,
                    row_idx,
                    |arr: &arrow_array::StringArray, i| arr.value(i).to_string(),
                )
                .or_else(|| {
                    let alt_col = format!("{}._label", node_var);
                    extract_column_value(
                        &batch,
                        &alt_col,
                        row_idx,
                        |arr: &arrow_array::StringArray, i| arr.value(i).to_string(),
                    )
                });

                self.append_node(&mut nodes_builder, vid, label, &query_ctx);
            }
            nodes_builder.append(true);

            // Add all edges in path order
            // Edge i connects node_variables[i] to node_variables[i+1]
            for (edge_idx, edge_var) in self.edge_variables.iter().enumerate() {
                // Internal tracking columns like __eid_to_b are the column name directly;
                // named edge variables use {var}._eid format
                let eid_col_name = if edge_var.starts_with("__eid_to_") {
                    edge_var.clone()
                } else {
                    format!("{}._eid", edge_var)
                };

                let eid: Option<Eid> = extract_column_value(
                    &batch,
                    &eid_col_name,
                    row_idx,
                    |arr: &arrow_array::UInt64Array, i| Eid::from(arr.value(i)),
                );

                // Get src/dst VIDs from adjacent node variables
                let src_vid = self.node_variables.get(edge_idx).and_then(|nv| {
                    let col = format!("{}._vid", nv);
                    extract_column_value::<arrow_array::UInt64Array, u64>(
                        &batch, &col, row_idx, |arr, i| arr.value(i),
                    )
                }).unwrap_or(0);
                let dst_vid = self.node_variables.get(edge_idx + 1).and_then(|nv| {
                    let col = format!("{}._vid", nv);
                    extract_column_value::<arrow_array::UInt64Array, u64>(
                        &batch, &col, row_idx, |arr, i| arr.value(i),
                    )
                }).unwrap_or(0);

                self.append_edge(&mut rels_builder, eid, src_vid, dst_vid, &query_ctx);
            }
            rels_builder.append(true);
        }

        let nodes_array = Arc::new(nodes_builder.finish()) as ArrayRef;
        let rels_array = Arc::new(rels_builder.finish()) as ArrayRef;

        let path_array = StructArray::try_new(
            Fields::from(vec![
                Arc::new(Field::new("nodes", nodes_array.data_type().clone(), false)),
                Arc::new(Field::new(
                    "relationships",
                    rels_array.data_type().clone(),
                    false,
                )),
            ]),
            vec![nodes_array, rels_array],
            None,
        )?;

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(path_array));

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }

    fn append_node(
        &self,
        nodes_builder: &mut ListBuilder<StructBuilder>,
        vid: Option<Vid>,
        label: Option<String>,
        query_ctx: &uni_store::QueryContext,
    ) {
        let nodes_struct = nodes_builder.values();

        let (vid_value, label_value, props_json) = match vid {
            Some(v) => {
                let resolved_label = label.or_else(|| {
                    l0_visibility::get_vertex_labels(v, query_ctx)
                        .first()
                        .cloned()
                });
                let props = l0_visibility::get_vertex_properties(v, query_ctx)
                    .and_then(|p| serde_json::to_vec(&p).ok());
                (v.as_u64(), resolved_label, props)
            }
            None => (0, None, None),
        };

        nodes_struct
            .field_builder::<UInt64Builder>(0)
            .unwrap()
            .append_value(vid_value);

        let label_builder = nodes_struct.field_builder::<StringBuilder>(1).unwrap();
        match label_value {
            Some(l) => label_builder.append_value(&l),
            None => label_builder.append_null(),
        }

        let props_builder = nodes_struct.field_builder::<LargeBinaryBuilder>(2).unwrap();
        match props_json {
            Some(json) => props_builder.append_value(&json),
            None => props_builder.append_null(),
        }

        nodes_struct.append(true);
    }

    fn append_edge(
        &self,
        rels_builder: &mut ListBuilder<StructBuilder>,
        eid: Option<Eid>,
        src_vid: u64,
        dst_vid: u64,
        query_ctx: &uni_store::QueryContext,
    ) {
        let rels_struct = rels_builder.values();

        let (eid_value, type_name, props_json) = match eid {
            Some(e) => {
                let type_name = l0_visibility::get_edge_type(e, query_ctx)
                    .unwrap_or_default();
                let props = l0_visibility::get_edge_properties(e, query_ctx)
                    .and_then(|p| serde_json::to_vec(&p).ok());
                (e.as_u64(), type_name, props)
            }
            None => (0, String::new(), None),
        };

        rels_struct
            .field_builder::<UInt64Builder>(0)
            .unwrap()
            .append_value(eid_value);

        rels_struct
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(&type_name);

        rels_struct
            .field_builder::<UInt64Builder>(2)
            .unwrap()
            .append_value(src_vid);

        rels_struct
            .field_builder::<UInt64Builder>(3)
            .unwrap()
            .append_value(dst_vid);

        let props_builder = rels_struct.field_builder::<LargeBinaryBuilder>(4).unwrap();
        match props_json {
            Some(json) => props_builder.append_value(&json),
            None => props_builder.append_null(),
        }

        rels_struct.append(true);
    }
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

impl Stream for BindFixedPathStream {
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

impl RecordBatchStream for BindFixedPathStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
