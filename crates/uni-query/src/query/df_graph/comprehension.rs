// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::any::Any;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, RecordBatch, UInt32Array};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::{cast, filter, filter_record_batch, take};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;

/// Physical expression for Cypher List Comprehension: `[x IN list WHERE pred | expr]`
#[derive(Debug)]
pub struct ListComprehensionExecExpr {
    /// Expression producing the input list
    input_list: Arc<dyn PhysicalExpr>,
    /// Expression to map each element (projection)
    map_expr: Arc<dyn PhysicalExpr>,
    /// Optional filter predicate
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Name of the loop variable (e.g., "x")
    variable_name: String,
    /// Schema of the input batch (outer scope)
    input_schema: Arc<Schema>,
    /// Data type of the items in the output list
    output_item_type: DataType,
    /// Whether to extract VIDs from CypherValue-encoded loop variable
    /// for nested pattern comprehension anchor binding
    needs_vid_extraction: bool,
    /// Parallel lists of hydrated endpoint nodes, `(src, dst)`.
    ///
    /// `startNode(r)` inside `[r IN rels | ...]` names a loop variable, so there
    /// is no column to read the endpoint's properties from. `EndpointHydrateExec`
    /// materialises them for the *list* before the comprehension runs; these
    /// expressions read those columns, and they are flattened with the same
    /// offsets as the elements so element `i` lines up with endpoint `i`.
    endpoint_lists: Option<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
}

impl Clone for ListComprehensionExecExpr {
    fn clone(&self) -> Self {
        Self {
            input_list: self.input_list.clone(),
            map_expr: self.map_expr.clone(),
            predicate: self.predicate.clone(),
            variable_name: self.variable_name.clone(),
            input_schema: self.input_schema.clone(),
            output_item_type: self.output_item_type.clone(),
            needs_vid_extraction: self.needs_vid_extraction,
            endpoint_lists: self.endpoint_lists.clone(),
        }
    }
}

/// What the comprehension binds for each element of the list.
///
/// The loop variable itself, plus the columns that have to be derived from it
/// per element: a VID for a nested pattern comprehension's anchor, and the
/// hydrated endpoints `startNode`/`endNode` need. They travel together because
/// they are all functions of the same element.
pub struct LoopBindings {
    /// Name of the loop variable (e.g. `x` in `[x IN xs | ...]`).
    pub variable_name: String,
    /// Whether to extract VIDs from the CypherValue-encoded loop variable for
    /// nested pattern comprehension anchor binding.
    pub needs_vid_extraction: bool,
    /// Parallel lists of hydrated endpoint nodes, `(src, dst)`.
    pub endpoint_lists: Option<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
}

impl ListComprehensionExecExpr {
    pub fn new(
        input_list: Arc<dyn PhysicalExpr>,
        map_expr: Arc<dyn PhysicalExpr>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        bindings: LoopBindings,
        input_schema: Arc<Schema>,
        output_item_type: DataType,
    ) -> Self {
        Self {
            input_list,
            map_expr,
            predicate,
            variable_name: bindings.variable_name,
            input_schema,
            output_item_type,
            needs_vid_extraction: bindings.needs_vid_extraction,
            endpoint_lists: bindings.endpoint_lists,
        }
    }
}

impl Display for ListComprehensionExecExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "ListComprehension(var={}, list={})",
            self.variable_name, self.input_list
        )
    }
}

impl PartialEq for ListComprehensionExecExpr {
    fn eq(&self, other: &Self) -> bool {
        self.variable_name == other.variable_name
            && self.output_item_type == other.output_item_type
            && Arc::ptr_eq(&self.input_list, &other.input_list)
            && Arc::ptr_eq(&self.map_expr, &other.map_expr)
            && match (&self.predicate, &other.predicate) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for ListComprehensionExecExpr {}

impl Hash for ListComprehensionExecExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.variable_name.hash(state);
        self.output_item_type.hash(state);
    }
}

impl PartialEq<dyn Any> for ListComprehensionExecExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl PhysicalExpr for ListComprehensionExecExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        // Always return LargeBinary (CypherValue encoding).
        // This is consistent with ALL other list-producing operations (reverse(),
        // tail(), list_concat(), etc.) which always return LargeBinary. Returning
        // LargeList<T> for typed inputs would cause type mismatches in CASE/coalesce
        // branches when mixed with other list ops that return LargeBinary.
        Ok(DataType::LargeBinary)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // 1. Evaluate input list
        let list_val = self.input_list.evaluate(batch)?;
        let list_array = list_val.into_array(batch.num_rows())?;

        // 2. Decode CypherValue-encoded arrays (LargeBinary → LargeList<LargeBinary>)
        let list_array = if let DataType::LargeBinary = list_array.data_type() {
            crate::query::df_graph::common::cv_array_to_large_list(
                list_array.as_ref(),
                &DataType::LargeBinary,
            )?
        } else {
            list_array
        };

        // Normalize to LargeListArray
        let list_array = if let DataType::List(field) = list_array.data_type() {
            let target_type = DataType::LargeList(field.clone());
            cast(&list_array, &target_type).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Cast failed: {}", e))
            })?
        } else {
            list_array
        };

        let large_list = list_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Expected LargeListArray, got {:?}",
                    list_array.data_type()
                ))
            })?;

        let values = large_list.values();
        let offsets = large_list.offsets();
        let nulls = large_list.nulls();

        // 3. Prepare inner batch
        let num_rows = batch.num_rows();
        let num_values = values.len();
        let mut indices_builder =
            datafusion::arrow::array::UInt32Builder::with_capacity(num_values);
        for row_idx in 0..num_rows {
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            let len = end - start;
            for _ in 0..len {
                indices_builder.append_value(row_idx as u32);
            }
        }
        let indices = indices_builder.finish();

        let mut inner_columns = Vec::with_capacity(batch.num_columns() + 1);
        for col in batch.columns() {
            let taken = take(col, &indices, None).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Take failed: {}", e))
            })?;
            inner_columns.push(taken);
        }

        let mut inner_fields = batch.schema().fields().to_vec();
        let loop_field = Arc::new(Field::new(
            &self.variable_name,
            values.data_type().clone(),
            true,
        ));

        // Replace existing column if loop variable shadows an outer column,
        // otherwise append at the end — matching compile_list_comprehension's schema construction.
        if let Some(pos) = inner_fields
            .iter()
            .position(|f| f.name() == &self.variable_name)
        {
            inner_columns[pos] = values.clone();
            inner_fields[pos] = loop_field;
        } else {
            inner_columns.push(values.clone());
            inner_fields.push(loop_field);
        }

        // Flatten the hydrated endpoint lists alongside the elements. Same
        // offsets, so element `i` and endpoint `i` stay paired; a row whose list
        // is shorter (or whose column is null) contributes nulls rather than
        // shifting the alignment.
        if let Some((src_expr, dst_expr)) = &self.endpoint_lists {
            for (is_start, expr) in [(true, src_expr), (false, dst_expr)] {
                let name = crate::query::df_graph::endpoint_hydrate::endpoint_column(
                    &self.variable_name,
                    is_start,
                );
                if inner_fields.iter().any(|f| f.name() == &name) {
                    continue;
                }
                let outer = expr.evaluate(batch)?.into_array(num_rows)?;
                let flat = crate::query::df_graph::common::cv_array_to_large_list(
                    outer.as_ref(),
                    &DataType::LargeBinary,
                )?;
                let flat = flat
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::LargeListArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "endpoint hydration did not produce a list".to_string(),
                        )
                    })?;
                let aligned = align_to_offsets(flat, offsets)?;
                inner_fields.push(Arc::new(Field::new(&name, DataType::LargeBinary, true)));
                inner_columns.push(aligned);
            }
        }

        // Materialize VID column from CypherValue-encoded loop variable for nested
        // pattern comprehension anchor binding
        if self.needs_vid_extraction {
            let vid_field_name = format!("{}._vid", self.variable_name);
            if !inner_fields.iter().any(|f| f.name() == &vid_field_name) {
                let vid_field = Arc::new(Field::new(&vid_field_name, DataType::UInt64, true));
                // Find the loop variable column
                let loop_var_idx = inner_fields
                    .iter()
                    .position(|f| f.name() == &self.variable_name);
                // Only a CypherValue column carries an entity to extract an id
                // from. A comprehension over plain scalars can still contain a
                // pattern predicate — one referring to variables from the outer
                // scope — and extraction would fail on its loop column, so the
                // absence of a vid column is the correct outcome there rather
                // than an error.
                if let Some(idx) = loop_var_idx
                    && *inner_columns[idx].data_type() == DataType::LargeBinary
                {
                    let vid_array = super::common::extract_vids_from_cypher_value_column(
                        inner_columns[idx].as_ref(),
                    )?;
                    inner_fields.push(vid_field);
                    inner_columns.push(vid_array);
                }
            }
        }

        let inner_schema = Arc::new(Schema::new(inner_fields));

        let inner_batch = RecordBatch::try_new(inner_schema, inner_columns)?;

        // 4. Filter (Predicate)
        let (filtered_batch, filtered_indices) = if let Some(pred) = &self.predicate {
            let mask = pred
                .evaluate(&inner_batch)?
                .into_array(inner_batch.num_rows())?;
            let mask = cast(&mask, &DataType::Boolean)?;
            let boolean_mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();

            let filtered_batch = filter_record_batch(&inner_batch, boolean_mask)?;

            let indices_array: Arc<dyn Array> = Arc::new(indices.clone());
            let filtered_indices = filter(&indices_array, boolean_mask)?;
            let filtered_indices = filtered_indices
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .clone();

            (filtered_batch, filtered_indices)
        } else {
            (inner_batch, indices.clone())
        };

        // 5. Evaluate Map Expression
        let mapped_val = self.map_expr.evaluate(&filtered_batch)?;
        let mapped_array = mapped_val.into_array(filtered_batch.num_rows())?;

        // 6. Reconstruct ListArray
        let new_offsets = if self.predicate.is_some() {
            let num_rows = batch.num_rows();
            let mut new_offsets = Vec::with_capacity(num_rows + 1);
            new_offsets.push(0);

            let indices_slice = filtered_indices.values();
            let mut pos = 0;
            let mut current_len = 0;

            for row_idx in 0..num_rows {
                let mut count = 0;
                while pos < indices_slice.len() && indices_slice[pos] as usize == row_idx {
                    count += 1;
                    pos += 1;
                }
                current_len += count;
                new_offsets.push(current_len);
            }
            OffsetBuffer::new(ScalarBuffer::from(new_offsets))
        } else {
            offsets.clone()
        };

        let new_field = Arc::new(Field::new("item", mapped_array.data_type().clone(), true));
        let new_list = datafusion::arrow::array::LargeListArray::new(
            new_field,
            new_offsets,
            mapped_array,
            nulls.cloned(),
        );

        // Always encode the result as LargeBinary (CypherValue), consistent with
        // data_type(). typed_large_list_to_cv_array handles all element types
        // (Int64, Float64, Utf8, Boolean, Struct, LargeBinary/nested CypherValue).
        let cypher_value_array =
            crate::query::df_graph::common::typed_large_list_to_cv_array(&new_list)?;
        Ok(ColumnarValue::Array(cypher_value_array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        // Only expose input_list as a child. The map_expr and predicate are compiled
        // against an inner schema (with the loop variable) and should not be exposed
        // to DataFusion's expression tree traversal (e.g., equivalence analysis).
        vec![&self.input_list]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "ListComprehension requires exactly 1 child (input_list)".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            input_list: children[0].clone(),
            map_expr: self.map_expr.clone(),
            predicate: self.predicate.clone(),
            variable_name: self.variable_name.clone(),
            input_schema: self.input_schema.clone(),
            output_item_type: self.output_item_type.clone(),
            needs_vid_extraction: self.needs_vid_extraction,
            endpoint_lists: self.endpoint_lists.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(pred) = &self.predicate {
            write!(
                f,
                "[{} IN {} WHERE {} | {}]",
                self.variable_name, self.input_list, pred, self.map_expr
            )
        } else {
            write!(
                f,
                "[{} IN {} | {}]",
                self.variable_name, self.input_list, self.map_expr
            )
        }
    }
}

/// Flatten `list` so it lines up element-for-element with `offsets`.
///
/// The comprehension flattens its elements using the input list's own offsets.
/// A parallel list built from the same source has the same shape, but nothing
/// enforces that at runtime — a shorter row, or a null, must contribute nulls at
/// the right positions rather than sliding every later element by one. Padding
/// explicitly is what keeps `startNode(r)` paired with the `r` it belongs to.
fn align_to_offsets(
    list: &datafusion::arrow::array::LargeListArray,
    offsets: &OffsetBuffer<i64>,
) -> Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::array::{LargeBinaryArray, LargeBinaryBuilder};

    let values = list.values();
    let values = values
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "endpoint hydration produced non-binary elements".to_string(),
            )
        })?;
    let src_offsets = list.offsets();

    let mut builder = LargeBinaryBuilder::new();
    for row in 0..offsets.len().saturating_sub(1) {
        let want = (offsets[row + 1] - offsets[row]) as usize;
        let (start, have) = if row + 1 < src_offsets.len() && !list.is_null(row) {
            let s = src_offsets[row] as usize;
            (s, (src_offsets[row + 1] - src_offsets[row]) as usize)
        } else {
            (0, 0)
        };
        for i in 0..want {
            if i < have && values.is_valid(start + i) {
                builder.append_value(values.value(start + i));
            } else {
                builder.append_null();
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}
