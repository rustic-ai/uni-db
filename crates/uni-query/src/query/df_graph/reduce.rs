// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::any::Any;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;

/// Physical expression for Cypher REDUCE: `reduce(acc = init, x IN list | expr)`
///
/// Executes reduction by iterating layer-by-layer (vectorized over list index).
#[derive(Debug, Clone)]
pub struct ReduceExecExpr {
    /// Name of the accumulator variable
    accumulator_name: String,
    /// Expression for initial value
    initial_expr: Arc<dyn PhysicalExpr>,
    /// Name of the loop variable
    variable_name: String,
    /// Expression producing the list
    list_expr: Arc<dyn PhysicalExpr>,
    /// Reduction expression (update logic)
    reduce_expr: Arc<dyn PhysicalExpr>,
    /// Schema of the input batch
    input_schema: Arc<Schema>,
    /// Output data type (type of reduce_expr)
    output_type: DataType,
}

impl ReduceExecExpr {
    pub fn new(
        accumulator_name: String,
        initial_expr: Arc<dyn PhysicalExpr>,
        variable_name: String,
        list_expr: Arc<dyn PhysicalExpr>,
        reduce_expr: Arc<dyn PhysicalExpr>,
        input_schema: Arc<Schema>,
        output_type: DataType,
    ) -> Self {
        Self {
            accumulator_name,
            initial_expr,
            variable_name,
            list_expr,
            reduce_expr,
            input_schema,
            output_type,
        }
    }
}

impl Display for ReduceExecExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "reduce({} = {}, {} IN {} | {})",
            self.accumulator_name,
            self.initial_expr,
            self.variable_name,
            self.list_expr,
            self.reduce_expr
        )
    }
}

impl PartialEq for ReduceExecExpr {
    fn eq(&self, other: &Self) -> bool {
        self.accumulator_name == other.accumulator_name
            && self.variable_name == other.variable_name
            && self.output_type == other.output_type
            && Arc::ptr_eq(&self.initial_expr, &other.initial_expr)
            && Arc::ptr_eq(&self.list_expr, &other.list_expr)
            && Arc::ptr_eq(&self.reduce_expr, &other.reduce_expr)
    }
}

impl Eq for ReduceExecExpr {}

impl Hash for ReduceExecExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.accumulator_name.hash(state);
        self.variable_name.hash(state);
        self.output_type.hash(state);
    }
}

impl PartialEq<dyn Any> for ReduceExecExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl PhysicalExpr for ReduceExecExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // 1. Evaluate input list
        let list_val = self.list_expr.evaluate(batch)?;
        let list_array = list_val.into_array(batch.num_rows())?;

        // Decode CypherValue-encoded arrays (LargeBinary → LargeList<element_type>)
        // Use the accumulator type as the target element type since the reduce body
        // was compiled expecting elements to match the accumulator type.
        let list_array = if let DataType::LargeBinary = list_array.data_type() {
            let element_type = self.output_type.clone();
            crate::query::df_graph::common::cv_array_to_large_list(
                list_array.as_ref(),
                &element_type,
            )?
        } else {
            list_array
        };

        // Normalize to LargeList
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
                datafusion::error::DataFusionError::Execution("Expected LargeListArray".to_string())
            })?;

        let offsets = large_list.offsets();
        let values = large_list.values();

        // 2. Evaluate initial value -> current accumulator
        let init_val = self.initial_expr.evaluate(batch)?;
        let mut current_acc = init_val.into_array(batch.num_rows())?;

        // 3. Layer-by-layer evaluation
        // Find max length
        let mut max_len = 0;
        for window in offsets.windows(2) {
            let len = (window[1] - window[0]) as usize;
            if len > max_len {
                max_len = len;
            }
        }

        for i in 0..max_len {
            // Identify active rows (list len > i)
            let mut active_indices_builder =
                datafusion::arrow::array::UInt32Builder::with_capacity(batch.num_rows());
            let mut variable_indices_builder =
                datafusion::arrow::array::UInt32Builder::with_capacity(batch.num_rows());

            for (row_idx, window) in offsets.windows(2).enumerate() {
                let start = window[0] as usize;
                let end = window[1] as usize;
                let len = end - start;
                if i < len {
                    active_indices_builder.append_value(row_idx as u32);
                    variable_indices_builder.append_value((start + i) as u32);
                }
            }
            let active_indices = active_indices_builder.finish();
            let variable_indices = variable_indices_builder.finish();

            if active_indices.is_empty() {
                break;
            }

            // Construct inner batch for active rows
            // 1. Take outer columns using active_indices
            let mut inner_columns = Vec::with_capacity(batch.num_columns() + 2);
            for col in batch.columns() {
                let taken = datafusion::arrow::compute::take(col, &active_indices, None)?;
                inner_columns.push(taken);
            }

            // Construct inner schema with accumulator and variable fields
            let mut inner_fields = batch.schema().fields().to_vec();
            let acc_field = Arc::new(Field::new(
                &self.accumulator_name,
                current_acc.data_type().clone(),
                true,
            ));
            let var_field = Arc::new(Field::new(
                &self.variable_name,
                values.data_type().clone(),
                true,
            ));

            // 2. Take accumulator values and replace/append to columns
            let acc_taken = datafusion::arrow::compute::take(&current_acc, &active_indices, None)?;
            if let Some(pos) = inner_fields
                .iter()
                .position(|f| f.name() == &self.accumulator_name)
            {
                inner_columns[pos] = acc_taken;
                inner_fields[pos] = acc_field;
            } else {
                inner_columns.push(acc_taken);
                inner_fields.push(acc_field);
            }

            // 3. Take variable values from flattened list values and replace/append to columns
            let var_taken = datafusion::arrow::compute::take(values, &variable_indices, None)?;
            if let Some(pos) = inner_fields
                .iter()
                .position(|f| f.name() == &self.variable_name)
            {
                inner_columns[pos] = var_taken;
                inner_fields[pos] = var_field;
            } else {
                inner_columns.push(var_taken);
                inner_fields.push(var_field);
            }

            let inner_schema = Arc::new(Schema::new(inner_fields));

            let inner_batch = RecordBatch::try_new(inner_schema, inner_columns)?;

            // Evaluate reduce expr
            let new_acc_val = self.reduce_expr.evaluate(&inner_batch)?;
            let new_acc_array = new_acc_val.into_array(inner_batch.num_rows())?;

            // Scatter updates back to current_acc

            if active_indices.len() == batch.num_rows() {
                current_acc = new_acc_array;
            } else {
                let mut interleave_indices = Vec::with_capacity(batch.num_rows());
                let mut active_map = vec![None; batch.num_rows()];
                for (k, &row_idx) in active_indices.values().iter().enumerate() {
                    active_map[row_idx as usize] = Some(k);
                }

                for (row_idx, slot) in active_map.iter().enumerate() {
                    if let Some(k) = slot {
                        interleave_indices.push((1, *k)); // 1 = new_acc_array
                    } else {
                        interleave_indices.push((0, row_idx)); // 0 = current_acc
                    }
                }

                current_acc = datafusion::arrow::compute::interleave(
                    &[&current_acc, &new_acc_array],
                    &interleave_indices,
                )?;
            }
        }

        Ok(ColumnarValue::Array(current_acc))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        // Only expose expressions compiled against the outer schema.
        // reduce_expr is compiled against an inner schema (with loop variable and accumulator)
        // and should not be exposed to DataFusion's expression tree traversal.
        vec![&self.initial_expr, &self.list_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 2 {
            return Err(datafusion::error::DataFusionError::Internal(
                "Reduce requires 2 children (initial_expr, list_expr)".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            initial_expr: children[0].clone(),
            list_expr: children[1].clone(),
            reduce_expr: self.reduce_expr.clone(),
            accumulator_name: self.accumulator_name.clone(),
            variable_name: self.variable_name.clone(),
            input_schema: self.input_schema.clone(),
            output_type: self.output_type.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "reduce({} = {}, {} IN {} | {})",
            self.accumulator_name,
            self.initial_expr,
            self.variable_name,
            self.list_expr,
            self.reduce_expr
        )
    }
}
