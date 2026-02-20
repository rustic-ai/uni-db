// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Vectorized pattern comprehension for Cypher `[(a)-[:REL]->(b) WHERE pred | expr]`.
//!
//! Implements a `PhysicalExpr` that:
//! 1. Extracts anchor VIDs from the input batch
//! 2. Expands neighbors via CSR adjacency (synchronous)
//! 3. Materializes needed properties via `block_in_place`
//! 4. Applies optional predicate filter
//! 5. Evaluates the map expression
//! 6. Reconstructs a `LargeList` column grouped by parent row

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::{cast, filter, filter_record_batch, take};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::{
    Direction as AstDirection, Expr, NodePattern, Pattern, PatternElement, RelationshipPattern,
};
use uni_store::QueryContext;
use uni_store::runtime::l0_visibility;
use uni_store::storage::direction::Direction;

use super::GraphExecutionContext;
use crate::query::df_graph::common::{build_path_struct_field, column_as_vid_array};
use crate::query::df_graph::scan::build_property_column_static;

/// A single hop derived from the pattern's elements.
#[derive(Debug, Clone)]
pub struct TraversalStep {
    /// Resolved edge type IDs for this hop.
    pub edge_type_ids: Vec<u32>,
    /// Direction of the hop.
    pub direction: Direction,
    /// Variable name for the target node, if any.
    pub target_variable: Option<String>,
    /// Label filter for the target node, if any.
    pub target_label_name: Option<String>,
    /// Variable name for the edge, if any.
    pub edge_variable: Option<String>,
}

/// Physical expression for Cypher Pattern Comprehension:
/// `[(a)-[:REL]->(b) WHERE pred | expr]`
#[derive(Debug)]
pub struct PatternComprehensionExecExpr {
    /// Shared graph context for CSR lookups and property materialization.
    graph_ctx: Arc<GraphExecutionContext>,
    /// Column name for anchor VIDs (e.g., `"a._vid"`).
    anchor_column: String,
    /// Steps describing each hop in the pattern.
    traversal_steps: Vec<TraversalStep>,
    /// Optional path variable name.
    path_variable: Option<String>,
    /// Optional filter predicate compiled against inner schema.
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Map expression compiled against inner schema.
    map_expr: Arc<dyn PhysicalExpr>,
    /// Schema of the outer input batch.
    input_schema: Arc<Schema>,
    /// Schema of the inner (expanded) batch — outer + pattern bindings + properties.
    inner_schema: Arc<Schema>,
    /// Data type of items in the output list (result of map_expr).
    output_item_type: DataType,
    /// Vertex properties needed per variable: variable → [prop_names].
    needed_vertex_props: HashMap<String, Vec<String>>,
    /// Edge properties needed per variable: variable → [prop_names].
    needed_edge_props: HashMap<String, Vec<String>>,
}

impl Clone for PatternComprehensionExecExpr {
    fn clone(&self) -> Self {
        Self {
            graph_ctx: self.graph_ctx.clone(),
            anchor_column: self.anchor_column.clone(),
            traversal_steps: self.traversal_steps.clone(),
            path_variable: self.path_variable.clone(),
            predicate: self.predicate.clone(),
            map_expr: self.map_expr.clone(),
            input_schema: self.input_schema.clone(),
            inner_schema: self.inner_schema.clone(),
            output_item_type: self.output_item_type.clone(),
            needed_vertex_props: self.needed_vertex_props.clone(),
            needed_edge_props: self.needed_edge_props.clone(),
        }
    }
}

impl PatternComprehensionExecExpr {
    #[expect(clippy::too_many_arguments, reason = "Constructor for complex expr")]
    pub fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        anchor_column: String,
        traversal_steps: Vec<TraversalStep>,
        path_variable: Option<String>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        map_expr: Arc<dyn PhysicalExpr>,
        input_schema: Arc<Schema>,
        inner_schema: Arc<Schema>,
        output_item_type: DataType,
        needed_vertex_props: HashMap<String, Vec<String>>,
        needed_edge_props: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            graph_ctx,
            anchor_column,
            traversal_steps,
            path_variable,
            predicate,
            map_expr,
            input_schema,
            inner_schema,
            output_item_type,
            needed_vertex_props,
            needed_edge_props,
        }
    }
}

impl Display for PatternComprehensionExecExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "PatternComprehension(anchor={}, steps={})",
            self.anchor_column,
            self.traversal_steps.len()
        )
    }
}

impl PartialEq for PatternComprehensionExecExpr {
    fn eq(&self, other: &Self) -> bool {
        self.anchor_column == other.anchor_column
            && Arc::ptr_eq(&self.graph_ctx, &other.graph_ctx)
            && Arc::ptr_eq(&self.map_expr, &other.map_expr)
            && match (&self.predicate, &other.predicate) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for PatternComprehensionExecExpr {}

impl Hash for PatternComprehensionExecExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.anchor_column.hash(state);
        self.output_item_type.hash(state);
    }
}

impl PartialEq<dyn Any> for PatternComprehensionExecExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl PhysicalExpr for PatternComprehensionExecExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DFResult<DataType> {
        Ok(DataType::LargeList(Arc::new(Field::new(
            "item",
            self.output_item_type.clone(),
            true,
        ))))
    }

    fn nullable(&self, _input_schema: &Schema) -> DFResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let num_rows = batch.num_rows();

        // Step 1: Extract anchor VIDs
        let anchor_col = if let Some(col) = batch.column_by_name(&self.anchor_column) {
            col
        } else if let Some(var_name) = self.anchor_column.strip_suffix("._vid") {
            batch.column_by_name(var_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Anchor column '{}' not found in batch schema: {:?}",
                    self.anchor_column,
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                ))
            })?
        } else {
            return Err(DataFusionError::Execution(format!(
                "Anchor column '{}' not found in batch schema: {:?}",
                self.anchor_column,
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
            )));
        };
        let anchor_vid_cow = column_as_vid_array(anchor_col.as_ref())?;
        let anchor_vids: &UInt64Array = &anchor_vid_cow;

        // Step 2: CSR expansion
        // Warm CSR for all edge types and directions
        for step in &self.traversal_steps {
            log::debug!(
                "PatternComprehension: warming CSR for edge_type_ids={:?}, direction={:?}",
                step.edge_type_ids,
                step.direction
            );
            std::thread::scope(|s| {
                s.spawn(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Runtime creation failed: {e}"))
                        })?;
                    rt.block_on(
                        self.graph_ctx
                            .ensure_adjacency_warmed(&step.edge_type_ids, step.direction),
                    )
                    .map_err(|e| DataFusionError::Execution(format!("CSR warming failed: {e}")))
                })
                .join()
                .unwrap_or_else(|_| {
                    Err(DataFusionError::Execution(
                        "CSR warming thread panicked".to_string(),
                    ))
                })
            })?;
        }

        log::debug!(
            "PatternComprehension: expanding {} anchor VIDs, steps={}",
            anchor_vids.len(),
            self.traversal_steps.len()
        );

        // Expand: for each anchor VID, traverse the pattern and collect results.
        // For single-hop, this is straightforward. For multi-hop, chain expansions.
        let expansion = self.expand_pattern(anchor_vids)?;

        log::debug!(
            "PatternComprehension: expansion produced {} rows",
            expansion.row_indices.len()
        );

        // Handle empty expansion: produce LargeList of empty lists
        if expansion.row_indices.is_empty() {
            return self.build_empty_list_result(num_rows);
        }

        // Step 3: Build flat inner batch
        let indices_array = UInt32Array::from(expansion.row_indices.clone());
        let mut inner_columns: Vec<ArrayRef> = Vec::new();

        // Replicate outer columns
        for col in batch.columns() {
            inner_columns.push(take(col, &indices_array, None)?);
        }

        // Add target VID columns for each step
        for (step_idx, step) in self.traversal_steps.iter().enumerate() {
            if let Some(ref _target_var) = step.target_variable {
                inner_columns.push(Arc::new(UInt64Array::from(
                    expansion.step_target_vids[step_idx].clone(),
                )));
            }
            if let Some(ref _edge_var) = step.edge_variable {
                inner_columns.push(Arc::new(UInt64Array::from(
                    expansion.step_edge_ids[step_idx].clone(),
                )));
            }
        }

        // Step 3b: Property materialization
        let query_ctx = self.graph_ctx.query_context();
        for (step_idx, step) in self.traversal_steps.iter().enumerate() {
            // Vertex properties
            if let Some(ref target_var) = step.target_variable
                && let Some(props) = self.needed_vertex_props.get(target_var)
            {
                let vids: Vec<Vid> = expansion.step_target_vids[step_idx]
                    .iter()
                    .map(|v| Vid::from(*v))
                    .collect();

                let prop_refs: Vec<&str> = props.iter().map(|s| s.as_str()).collect();

                let props_map = std::thread::scope(|s| {
                    s.spawn(|| {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                DataFusionError::Execution(format!("Runtime creation failed: {e}"))
                            })?;
                        rt.block_on(self.graph_ctx.property_manager().get_batch_vertex_props(
                            &vids,
                            &prop_refs,
                            Some(&query_ctx),
                        ))
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Vertex prop load failed: {e}"))
                        })
                    })
                    .join()
                    .unwrap_or_else(|_| {
                        Err(DataFusionError::Execution(
                            "Vertex prop load thread panicked".to_string(),
                        ))
                    })
                })?;

                for prop in props {
                    let col = build_property_column_static(
                        &vids,
                        &props_map,
                        prop,
                        &DataType::LargeBinary,
                    )?;
                    inner_columns.push(col);
                }
            }

            // Edge properties
            if let Some(ref edge_var) = step.edge_variable
                && let Some(props) = self.needed_edge_props.get(edge_var)
            {
                let eids: Vec<Eid> = expansion.step_edge_ids[step_idx]
                    .iter()
                    .map(|e| Eid::from(*e))
                    .collect();

                let prop_refs: Vec<&str> = props.iter().map(|s| s.as_str()).collect();

                let props_map = std::thread::scope(|s| {
                    s.spawn(|| {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                DataFusionError::Execution(format!("Runtime creation failed: {e}"))
                            })?;
                        rt.block_on(self.graph_ctx.property_manager().get_batch_edge_props(
                            &eids,
                            &prop_refs,
                            Some(&query_ctx),
                        ))
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Edge prop load failed: {e}"))
                        })
                    })
                    .join()
                    .unwrap_or_else(|_| {
                        Err(DataFusionError::Execution(
                            "Edge prop load thread panicked".to_string(),
                        ))
                    })
                })?;

                // Edge props use Eid mapped to Vid keys in the HashMap
                let vid_keys: Vec<Vid> = eids.iter().map(|e| Vid::from(e.as_u64())).collect();
                for prop in props {
                    let col = build_property_column_static(
                        &vid_keys,
                        &props_map,
                        prop,
                        &DataType::LargeBinary,
                    )?;
                    inner_columns.push(col);
                }
            }
        }

        // Step 3c: Build path struct column if path variable is bound
        if self.path_variable.is_some() {
            let path_col = self.build_path_column(&expansion, anchor_vids, &query_ctx)?;
            inner_columns.push(path_col);
        }

        let inner_batch = RecordBatch::try_new(self.inner_schema.clone(), inner_columns)?;

        // Step 4: Filter
        let (filtered_batch, filtered_indices) = if let Some(pred) = &self.predicate {
            let mask = pred
                .evaluate(&inner_batch)?
                .into_array(inner_batch.num_rows())?;
            let mask = cast(&mask, &DataType::Boolean)?;
            let boolean_mask = mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Pattern comprehension predicate did not produce BooleanArray".to_string(),
                    )
                })?;

            let filtered_batch = filter_record_batch(&inner_batch, boolean_mask)?;
            let indices_array_ref: ArrayRef = Arc::new(indices_array.clone());
            let filtered_idx = filter(&indices_array_ref, boolean_mask)?;
            let filtered_idx = filtered_idx
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .clone();

            (filtered_batch, filtered_idx)
        } else {
            (inner_batch, indices_array.clone())
        };

        // Step 5: Map
        let mapped_val = self.map_expr.evaluate(&filtered_batch)?;
        let mapped_array = mapped_val.into_array(filtered_batch.num_rows())?;

        // Step 6: Reconstruct LargeList
        let new_offsets = {
            let mut offsets = Vec::with_capacity(num_rows + 1);
            offsets.push(0i64);

            let indices_slice = filtered_indices.values();
            let mut pos = 0;
            let mut current_len: i64 = 0;

            for row_idx in 0..num_rows {
                while pos < indices_slice.len() && indices_slice[pos] as usize == row_idx {
                    pos += 1;
                    current_len += 1;
                }
                offsets.push(current_len);
            }
            OffsetBuffer::new(ScalarBuffer::from(offsets))
        };

        let new_field = Arc::new(Field::new("item", mapped_array.data_type().clone(), true));
        let new_list = datafusion::arrow::array::LargeListArray::new(
            new_field,
            new_offsets,
            mapped_array,
            None,
        );

        Ok(ColumnarValue::Array(Arc::new(new_list)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        // map_expr and predicate are compiled against inner schema;
        // don't expose to DF tree traversal.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "PatternComprehension has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PatternComprehension({})", self.anchor_column)
    }
}

/// Intermediate result from pattern expansion.
struct PatternExpansion {
    /// Row index into the outer batch for each expanded row.
    row_indices: Vec<u32>,
    /// Anchor VID for each expanded row (source of the path).
    anchor_vids: Vec<u64>,
    /// Per-step target VIDs (parallel arrays with row_indices).
    step_target_vids: Vec<Vec<u64>>,
    /// Per-step edge IDs (parallel arrays with row_indices).
    step_edge_ids: Vec<Vec<u64>>,
    /// Per-step edge type IDs (parallel arrays with row_indices).
    step_edge_type_ids: Vec<Vec<u32>>,
}

impl PatternComprehensionExecExpr {
    /// Expand the pattern starting from anchor VIDs.
    ///
    /// For single-hop patterns this is a direct CSR lookup.
    /// For multi-hop, each hop chains from the previous hop's target VIDs.
    fn expand_pattern(&self, anchor_vids: &UInt64Array) -> DFResult<PatternExpansion> {
        // Start: each anchor VID is a "frontier" entry
        let mut frontier_row_indices: Vec<u32> = Vec::new();
        let mut frontier_vids: Vec<u64> = Vec::new();

        for (row_idx, vid_opt) in anchor_vids.iter().enumerate() {
            if let Some(vid_u64) = vid_opt {
                frontier_row_indices.push(row_idx as u32);
                frontier_vids.push(vid_u64);
            }
        }

        // `result_row_indices[i]` maps expanded row i back to the original outer batch row.
        let mut result_row_indices: Vec<u32> = frontier_row_indices.clone();
        // Track anchor VID for each expanded row (the original source node).
        let mut result_anchor_vids: Vec<u64> = frontier_vids.clone();

        // Track intermediate target/edge/edge-type arrays per step for multi-hop reconstruction.
        let mut accumulated_target_vids: Vec<Vec<u64>> = Vec::new();
        let mut accumulated_edge_ids: Vec<Vec<u64>> = Vec::new();
        let mut accumulated_edge_type_ids: Vec<Vec<u32>> = Vec::new();

        for step in &self.traversal_steps {
            let is_undirected = step.direction == Direction::Both;
            let query_ctx = self.graph_ctx.query_context();

            let mut new_row_indices: Vec<u32> = Vec::new();
            let mut new_anchor_vids: Vec<u64> = Vec::new();
            let mut new_target_vids: Vec<u64> = Vec::new();
            let mut new_edge_ids: Vec<u64> = Vec::new();
            let mut new_edge_type_ids: Vec<u32> = Vec::new();
            // Carry-forward columns for steps 0..step_idx
            let num_prev_cols = accumulated_target_vids.len();
            let mut new_accumulated_targets: Vec<Vec<u64>> = vec![Vec::new(); num_prev_cols];
            let mut new_accumulated_edges: Vec<Vec<u64>> =
                vec![Vec::new(); accumulated_edge_ids.len()];
            let mut new_accumulated_edge_types: Vec<Vec<u32>> =
                vec![Vec::new(); accumulated_edge_type_ids.len()];

            for (i, &src_vid_u64) in frontier_vids.iter().enumerate() {
                let vid = Vid::from(src_vid_u64);
                let outer_row = result_row_indices[i];
                let anchor_vid = result_anchor_vids[i];

                let mut seen_edges: HashSet<u64> = HashSet::new();

                for &edge_type in &step.edge_type_ids {
                    let neighbors = self.graph_ctx.get_neighbors(vid, edge_type, step.direction);

                    for (target_vid, eid) in neighbors {
                        let eid_u64 = eid.as_u64();

                        // Deduplicate edges for undirected patterns
                        if is_undirected && !seen_edges.insert(eid_u64) {
                            continue;
                        }

                        // Label filtering
                        if let Some(ref label_name) = step.target_label_name
                            && let Some(vertex_labels) =
                                l0_visibility::get_vertex_labels_optional(target_vid, &query_ctx)
                            && !vertex_labels.contains(label_name)
                        {
                            continue;
                        }

                        new_row_indices.push(outer_row);
                        new_anchor_vids.push(anchor_vid);
                        new_target_vids.push(target_vid.as_u64());
                        new_edge_ids.push(eid_u64);
                        new_edge_type_ids.push(edge_type);

                        // Carry forward accumulated columns from previous steps
                        for (col_idx, col) in accumulated_target_vids.iter().enumerate() {
                            new_accumulated_targets[col_idx].push(col[i]);
                        }
                        for (col_idx, col) in accumulated_edge_ids.iter().enumerate() {
                            new_accumulated_edges[col_idx].push(col[i]);
                        }
                        for (col_idx, col) in accumulated_edge_type_ids.iter().enumerate() {
                            new_accumulated_edge_types[col_idx].push(col[i]);
                        }
                    }
                }
            }

            // Update frontier for next hop
            frontier_vids.clone_from(&new_target_vids);
            result_row_indices = new_row_indices;
            result_anchor_vids = new_anchor_vids;

            // Append this step's data to accumulated columns
            new_accumulated_targets.push(new_target_vids);
            new_accumulated_edges.push(new_edge_ids);
            new_accumulated_edge_types.push(new_edge_type_ids);
            accumulated_target_vids = new_accumulated_targets;
            accumulated_edge_ids = new_accumulated_edges;
            accumulated_edge_type_ids = new_accumulated_edge_types;
        }

        Ok(PatternExpansion {
            row_indices: result_row_indices,
            anchor_vids: result_anchor_vids,
            step_target_vids: accumulated_target_vids,
            step_edge_ids: accumulated_edge_ids,
            step_edge_type_ids: accumulated_edge_type_ids,
        })
    }

    /// Build a LargeList result of empty lists for all rows.
    fn build_empty_list_result(&self, num_rows: usize) -> DFResult<ColumnarValue> {
        let offsets: Vec<i64> = vec![0; num_rows + 1];
        let empty_values: ArrayRef = arrow_array::new_empty_array(&self.output_item_type);
        let field = Arc::new(Field::new("item", self.output_item_type.clone(), true));
        let list = datafusion::arrow::array::LargeListArray::new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            empty_values,
            None,
        );
        Ok(ColumnarValue::Array(Arc::new(list)))
    }

    /// Build a path struct column for each expanded row.
    ///
    /// Each path consists of: nodes = [anchor, step0_target, step1_target, ...]
    /// and relationships = [step0_edge, step1_edge, ...].
    /// The path struct follows the schema from `build_path_struct_field()`.
    fn build_path_column(
        &self,
        expansion: &PatternExpansion,
        _anchor_vids: &UInt64Array,
        query_ctx: &QueryContext,
    ) -> DFResult<ArrayRef> {
        use arrow_array::builder::{
            LargeBinaryBuilder, ListBuilder, StringBuilder, StructBuilder, UInt64Builder,
        };

        let num_expanded = expansion.row_indices.len();

        let node_struct_fields: Vec<Arc<Field>> =
            crate::query::df_graph::common::node_struct_fields()
                .iter()
                .cloned()
                .collect();
        let edge_struct_fields: Vec<Arc<Field>> =
            crate::query::df_graph::common::edge_struct_fields()
                .iter()
                .cloned()
                .collect();

        let mut nodes_builder = ListBuilder::new(StructBuilder::new(
            node_struct_fields,
            vec![
                Box::new(UInt64Builder::new()),
                Box::new(ListBuilder::new(StringBuilder::new())),
                Box::new(LargeBinaryBuilder::new()),
            ],
        ));

        let mut rels_builder = ListBuilder::new(StructBuilder::new(
            edge_struct_fields,
            vec![
                Box::new(UInt64Builder::new()),
                Box::new(StringBuilder::new()),
                Box::new(UInt64Builder::new()),
                Box::new(UInt64Builder::new()),
                Box::new(LargeBinaryBuilder::new()),
            ],
        ));

        let uni_schema = self.graph_ctx.storage().schema_manager().schema();
        let num_steps = self.traversal_steps.len();

        for row_idx in 0..num_expanded {
            // Build node list: anchor + each step's target
            let anchor_vid_u64 = expansion.anchor_vids[row_idx];
            let anchor_vid = Vid::from(anchor_vid_u64);

            // Append anchor node
            super::common::append_node_to_struct(nodes_builder.values(), anchor_vid, query_ctx);

            // Append target node for each step
            for step_idx in 0..num_steps {
                let target_vid = Vid::from(expansion.step_target_vids[step_idx][row_idx]);
                super::common::append_node_to_struct(nodes_builder.values(), target_vid, query_ctx);
            }
            nodes_builder.append(true);

            // Build relationships list for each step
            for step_idx in 0..num_steps {
                let eid = Eid::from(expansion.step_edge_ids[step_idx][row_idx]);
                let edge_type_id = expansion.step_edge_type_ids[step_idx][row_idx];
                let edge_type_name = uni_schema
                    .edge_type_name_by_id_unified(edge_type_id)
                    .unwrap_or_default();

                // Determine src and dst: for the path struct, src is the node
                // *before* this edge and dst is the node *after* this edge.
                let src_vid = if step_idx == 0 {
                    anchor_vid_u64
                } else {
                    expansion.step_target_vids[step_idx - 1][row_idx]
                };
                let dst_vid = expansion.step_target_vids[step_idx][row_idx];

                super::common::append_edge_to_struct(
                    rels_builder.values(),
                    eid,
                    &edge_type_name,
                    src_vid,
                    dst_vid,
                    query_ctx,
                );
            }
            rels_builder.append(true);
        }

        let nodes_array = Arc::new(nodes_builder.finish()) as ArrayRef;
        let rels_array = Arc::new(rels_builder.finish()) as ArrayRef;

        let nodes_field = Arc::new(Field::new("nodes", nodes_array.data_type().clone(), true));
        let rels_field = Arc::new(Field::new(
            "relationships",
            rels_array.data_type().clone(),
            true,
        ));

        let path_struct = arrow_array::StructArray::try_new(
            vec![nodes_field, rels_field].into(),
            vec![nodes_array, rels_array],
            None,
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        Ok(Arc::new(path_struct))
    }
}

// ─── Pattern Analysis Functions ──────────────────────────────────────────────

/// Analyze a pattern to extract the anchor column and traversal steps.
///
/// The anchor is the first node in the pattern whose variable has `{var}._vid`
/// in the input schema. The remaining nodes/edges become traversal steps.
pub fn analyze_pattern(
    pattern: &Pattern,
    input_schema: &Schema,
    uni_schema: &UniSchema,
) -> anyhow::Result<(String, Vec<TraversalStep>)> {
    if pattern.paths.is_empty() {
        return Err(anyhow::anyhow!(
            "Pattern comprehension requires at least one path"
        ));
    }

    let path = &pattern.paths[0];
    let elements = &path.elements;

    if elements.is_empty() {
        return Err(anyhow::anyhow!(
            "Pattern comprehension path has no elements"
        ));
    }

    // Find the anchor node
    let (anchor_idx, anchor_var) = find_anchor_node(elements, input_schema)?;

    let anchor_column = format!("{}._vid", anchor_var);

    // Build traversal steps from the elements after (or around) the anchor
    let steps = build_traversal_steps(elements, anchor_idx, uni_schema)?;

    Ok((anchor_column, steps))
}

/// Find the anchor node in the pattern elements.
///
/// The anchor is the first `NodePattern` whose variable has `{var}._vid` in the
/// input schema. This identifies which node is already bound from the outer scope.
fn find_anchor_node(
    elements: &[PatternElement],
    input_schema: &Schema,
) -> anyhow::Result<(usize, String)> {
    for (idx, elem) in elements.iter().enumerate() {
        if let PatternElement::Node(node) = elem
            && let Some(ref var) = node.variable
        {
            let vid_col = format!("{}._vid", var);
            if input_schema.column_with_name(&vid_col).is_some() {
                return Ok((idx, var.clone()));
            }
        }
    }

    Err(anyhow::anyhow!(
        "No anchor node found in pattern comprehension. \
         None of the pattern variables have a corresponding `_vid` column in the input schema. \
         Schema fields: {:?}",
        input_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
    ))
}

/// Build traversal steps from pattern elements starting from the anchor.
///
/// The pattern alternates Node, Rel, Node, Rel, Node, ...
/// The anchor is at `anchor_idx`. We build steps going right from the anchor.
fn build_traversal_steps(
    elements: &[PatternElement],
    anchor_idx: usize,
    uni_schema: &UniSchema,
) -> anyhow::Result<Vec<TraversalStep>> {
    let mut steps = Vec::new();

    // Traverse right from anchor: (anchor)-[r]->(b)-[s]->(c)...
    let mut i = anchor_idx + 1;
    while i + 1 < elements.len() {
        let rel_elem = &elements[i];
        let target_elem = &elements[i + 1];

        let PatternElement::Relationship(rel) = rel_elem else {
            return Err(anyhow::anyhow!(
                "Expected relationship at pattern index {}, got {:?}",
                i,
                rel_elem
            ));
        };

        let PatternElement::Node(target_node) = target_elem else {
            return Err(anyhow::anyhow!(
                "Expected node at pattern index {}, got {:?}",
                i + 1,
                target_elem
            ));
        };

        let step = build_step_from_rel_and_node(rel, target_node, uni_schema)?;
        steps.push(step);

        i += 2;
    }

    if steps.is_empty() {
        return Err(anyhow::anyhow!(
            "Pattern comprehension has no traversal steps after anchor"
        ));
    }

    Ok(steps)
}

/// Build a single traversal step from a relationship pattern and target node.
fn build_step_from_rel_and_node(
    rel: &RelationshipPattern,
    target_node: &NodePattern,
    uni_schema: &UniSchema,
) -> anyhow::Result<TraversalStep> {
    // Resolve edge type IDs — check both schema-defined and schemaless registries.
    let edge_type_ids = if rel.types.is_empty() {
        // Untyped: traverse all edge types
        uni_schema.all_edge_type_ids()
    } else {
        rel.types
            .iter()
            .filter_map(|t| resolve_edge_type_id_unified(uni_schema, t))
            .collect()
    };

    if edge_type_ids.is_empty() && !rel.types.is_empty() {
        // Edge types were specified but none resolved — return empty step
        // that will produce no results
        return Ok(TraversalStep {
            edge_type_ids: vec![],
            direction: convert_direction(&rel.direction),
            target_variable: target_node.variable.clone(),
            target_label_name: target_node.labels.first().cloned(),
            edge_variable: rel.variable.clone(),
        });
    }

    let direction = convert_direction(&rel.direction);
    let target_label_name = target_node.labels.first().cloned();

    Ok(TraversalStep {
        edge_type_ids,
        direction,
        target_variable: target_node.variable.clone(),
        target_label_name,
        edge_variable: rel.variable.clone(),
    })
}

/// Resolve an edge type name to its ID, checking both the schema-defined
/// edge types and the schemaless registry (case-insensitive).
fn resolve_edge_type_id_unified(uni_schema: &UniSchema, type_name: &str) -> Option<u32> {
    uni_schema.edge_type_id_unified_case_insensitive(type_name)
}

/// Convert AST direction to storage direction.
fn convert_direction(ast_dir: &AstDirection) -> Direction {
    match ast_dir {
        AstDirection::Outgoing => Direction::Outgoing,
        AstDirection::Incoming => Direction::Incoming,
        AstDirection::Both => Direction::Both,
    }
}

/// Collect property references from expressions that refer to inner variables.
///
/// Walks expression trees looking for `Expr::Property(Expr::Variable(v), prop)`
/// where `v` is in `inner_vars`. Separates vertex props from edge props based on
/// whether the variable is a node or edge variable.
pub fn collect_inner_properties(
    where_clause: Option<&Expr>,
    map_expr: &Expr,
    steps: &[TraversalStep],
) -> (HashMap<String, Vec<String>>, HashMap<String, Vec<String>>) {
    let mut vertex_props: HashMap<String, Vec<String>> = HashMap::new();
    let mut edge_props: HashMap<String, Vec<String>> = HashMap::new();

    // Build sets of node and edge variable names
    let node_vars: HashSet<String> = steps
        .iter()
        .filter_map(|s| s.target_variable.clone())
        .collect();
    let edge_vars: HashSet<String> = steps
        .iter()
        .filter_map(|s| s.edge_variable.clone())
        .collect();

    // Walk expression trees
    let mut exprs_to_visit: Vec<&Expr> = vec![map_expr];
    if let Some(w) = where_clause {
        exprs_to_visit.push(w);
    }

    while let Some(expr) = exprs_to_visit.pop() {
        match expr {
            Expr::Property(base, prop) => {
                if let Expr::Variable(var) = base.as_ref() {
                    if node_vars.contains(var) {
                        vertex_props
                            .entry(var.clone())
                            .or_default()
                            .push(prop.clone());
                    } else if edge_vars.contains(var) {
                        edge_props
                            .entry(var.clone())
                            .or_default()
                            .push(prop.clone());
                    }
                }
                // Also walk the base in case it's nested
                exprs_to_visit.push(base);
            }
            Expr::BinaryOp { left, right, .. } => {
                exprs_to_visit.push(left);
                exprs_to_visit.push(right);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                exprs_to_visit.push(inner);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    exprs_to_visit.push(arg);
                }
            }
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                for (w, t) in when_then {
                    exprs_to_visit.push(w);
                    exprs_to_visit.push(t);
                }
                if let Some(e) = else_expr {
                    exprs_to_visit.push(e);
                }
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                exprs_to_visit.push(inner);
            }
            Expr::List(items) => {
                for item in items {
                    exprs_to_visit.push(item);
                }
            }
            Expr::Map(entries) => {
                for (_, v) in entries {
                    exprs_to_visit.push(v);
                }
            }
            Expr::In { expr: l, list: r } => {
                exprs_to_visit.push(l);
                exprs_to_visit.push(r);
            }
            _ => {}
        }
    }

    // Deduplicate property lists
    for props in vertex_props.values_mut() {
        props.sort();
        props.dedup();
    }
    for props in edge_props.values_mut() {
        props.sort();
        props.dedup();
    }

    (vertex_props, edge_props)
}

/// Build the inner schema for the expanded batch.
///
/// Starts with outer fields, then adds pattern binding columns
/// (_vid for target nodes, _eid for edges), property columns,
/// and a path struct column if `path_variable` is provided.
pub fn build_inner_schema(
    input_schema: &Schema,
    steps: &[TraversalStep],
    vertex_props: &HashMap<String, Vec<String>>,
    edge_props: &HashMap<String, Vec<String>>,
    path_variable: Option<&str>,
) -> Schema {
    let mut fields: Vec<Arc<Field>> = input_schema.fields().to_vec();

    for step in steps {
        // Target node VID column
        if let Some(ref target_var) = step.target_variable {
            fields.push(Arc::new(Field::new(
                format!("{}._vid", target_var),
                DataType::UInt64,
                true,
            )));
        }

        // Edge ID column
        if let Some(ref edge_var) = step.edge_variable {
            fields.push(Arc::new(Field::new(
                format!("{}._eid", edge_var),
                DataType::UInt64,
                true,
            )));
        }
    }

    // Add property columns for vertex variables
    for step in steps {
        if let Some(ref target_var) = step.target_variable
            && let Some(props) = vertex_props.get(target_var)
        {
            for prop in props {
                fields.push(Arc::new(Field::new(
                    format!("{}.{}", target_var, prop),
                    DataType::LargeBinary,
                    true,
                )));
            }
        }
    }

    // Add property columns for edge variables
    for step in steps {
        if let Some(ref edge_var) = step.edge_variable
            && let Some(props) = edge_props.get(edge_var)
        {
            for prop in props {
                fields.push(Arc::new(Field::new(
                    format!("{}.{}", edge_var, prop),
                    DataType::LargeBinary,
                    true,
                )));
            }
        }
    }

    // Add path struct column if path variable is bound
    if let Some(path_var) = path_variable {
        fields.push(Arc::new(build_path_struct_field(path_var)));
    }

    Schema::new(fields)
}
