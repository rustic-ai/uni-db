// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Vectorized quantifier expression for Cypher `ALL/ANY/SINGLE/NONE(x IN list WHERE pred)`.
//!
//! Implements three-valued null semantics required by the OpenCypher TCK:
//! - `ALL`: false if any false; null if any null (no false); true otherwise. Empty → true.
//! - `ANY`: true if any true; null if any null (no true); false otherwise. Empty → false.
//! - `SINGLE`: false if >1 true; null if nulls present with ≤1 true; true if exactly 1 true
//!   and no nulls. Empty → false.
//! - `NONE`: false if any true; null if any null (no true); true otherwise. Empty → true.

use std::any::Any;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, BooleanBuilder, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;

/// Quantifier type for boolean reduction over list elements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuantifierType {
    /// `ALL(x IN list WHERE pred)` — true iff every element satisfies pred.
    All,
    /// `ANY(x IN list WHERE pred)` — true iff at least one element satisfies pred.
    Any,
    /// `SINGLE(x IN list WHERE pred)` — true iff exactly one element satisfies pred.
    Single,
    /// `NONE(x IN list WHERE pred)` — true iff no element satisfies pred.
    None,
}

impl Display for QuantifierType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "ALL"),
            Self::Any => write!(f, "ANY"),
            Self::Single => write!(f, "SINGLE"),
            Self::None => write!(f, "NONE"),
        }
    }
}

/// Physical expression evaluating `ALL/ANY/SINGLE/NONE(x IN list WHERE pred)`.
///
/// Steps 1–4 mirror [`super::comprehension::ListComprehensionExecExpr`]: evaluate input
/// list, CypherValue-decode, normalize to `LargeList`, flatten with row indices, build inner
/// batch. Step 5 evaluates the predicate on the inner batch and performs boolean reduction
/// with three-valued null logic per parent row.
#[derive(Debug)]
pub struct QuantifierExecExpr {
    /// Expression producing the input list.
    input_list: Arc<dyn PhysicalExpr>,
    /// Predicate evaluated for each element.
    predicate: Arc<dyn PhysicalExpr>,
    /// Name of the loop variable (e.g., `"x"`).
    variable_name: String,
    /// Schema of the outer input batch.
    input_schema: Arc<Schema>,
    /// Which quantifier to apply.
    quantifier_type: QuantifierType,
}

impl Clone for QuantifierExecExpr {
    fn clone(&self) -> Self {
        Self {
            input_list: self.input_list.clone(),
            predicate: self.predicate.clone(),
            variable_name: self.variable_name.clone(),
            input_schema: self.input_schema.clone(),
            quantifier_type: self.quantifier_type,
        }
    }
}

impl QuantifierExecExpr {
    /// Create a new quantifier expression.
    ///
    /// # Arguments
    ///
    /// * `input_list` — expression producing the list to iterate
    /// * `predicate` — expression evaluated per element (compiled against inner schema)
    /// * `variable_name` — loop variable name bound to each element
    /// * `input_schema` — schema of the outer batch
    /// * `quantifier_type` — `All`, `Any`, `Single`, or `None`
    pub fn new(
        input_list: Arc<dyn PhysicalExpr>,
        predicate: Arc<dyn PhysicalExpr>,
        variable_name: String,
        input_schema: Arc<Schema>,
        quantifier_type: QuantifierType,
    ) -> Self {
        Self {
            input_list,
            predicate,
            variable_name,
            input_schema,
            quantifier_type,
        }
    }
}

impl Display for QuantifierExecExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}(var={}, list={})",
            self.quantifier_type, self.variable_name, self.input_list
        )
    }
}

impl PartialEq for QuantifierExecExpr {
    fn eq(&self, other: &Self) -> bool {
        self.variable_name == other.variable_name
            && self.quantifier_type == other.quantifier_type
            && Arc::ptr_eq(&self.input_list, &other.input_list)
            && Arc::ptr_eq(&self.predicate, &other.predicate)
    }
}

impl Eq for QuantifierExecExpr {}

impl Hash for QuantifierExecExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.variable_name.hash(state);
        self.quantifier_type.hash(state);
    }
}

impl PartialEq<dyn Any> for QuantifierExecExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl PhysicalExpr for QuantifierExecExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        // Three-valued logic can produce null results.
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();

        // --- Step 1: Evaluate input list ---
        let list_val = self.input_list.evaluate(batch)?;
        let list_array = list_val.into_array(num_rows)?;

        // --- Step 2: CypherValue decode (LargeBinary → LargeList<LargeBinary>) ---
        // Keep elements as CypherValue (LargeBinary) to match the compile-time schema.
        // The compiled predicate handles LargeBinary via CypherValue comparison/arithmetic UDFs.
        let list_array = if let DataType::LargeBinary = list_array.data_type() {
            crate::query::df_graph::common::cv_array_to_large_list(
                list_array.as_ref(),
                &DataType::LargeBinary,
            )?
        } else {
            list_array
        };

        // --- Step 3: Normalize List → LargeList ---
        let list_array = if let DataType::List(field) = list_array.data_type() {
            let target_type = DataType::LargeList(field.clone());
            cast(&list_array, &target_type).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Cast failed: {e}"))
            })?
        } else {
            list_array
        };

        // Handle Null type: all rows produce null result
        if let DataType::Null = list_array.data_type() {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for _ in 0..num_rows {
                builder.append_null();
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

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
        let list_nulls = large_list.nulls();

        // --- Step 4: Flatten — build inner batch ---
        let num_values = values.len();

        // If there are no values at all, short-circuit with empty-list semantics
        if num_values == 0 {
            return Ok(ColumnarValue::Array(Arc::new(
                self.reduce_empty_lists(num_rows, offsets, list_nulls),
            )));
        }

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
            let taken = datafusion::arrow::compute::take(col, &indices, None).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Take failed: {e}"))
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
        // otherwise append at the end — matching compile_quantifier's schema construction.
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

        let inner_schema = Arc::new(Schema::new(inner_fields));
        let inner_batch = RecordBatch::try_new(inner_schema, inner_columns)?;

        // --- Step 5: Evaluate predicate and reduce ---
        let pred_val = self.predicate.evaluate(&inner_batch).map_err(|e| {
            let err_msg = e.to_string();
            if err_msg.contains("Invalid arithmetic operation") {
                datafusion::error::DataFusionError::Execution(format!(
                    "SyntaxError: InvalidArgumentType - {}",
                    err_msg
                ))
            } else {
                e
            }
        })?;
        let pred_array = pred_val.into_array(inner_batch.num_rows())?;
        let pred_array = cast(&pred_array, &DataType::Boolean).map_err(|e| {
            let err_msg = e.to_string();
            if err_msg.contains("Invalid arithmetic operation") {
                datafusion::error::DataFusionError::Execution(format!(
                    "SyntaxError: InvalidArgumentType - {}",
                    err_msg
                ))
            } else {
                datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
            }
        })?;
        let pred_bools = pred_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Quantifier predicate did not produce BooleanArray".to_string(),
                )
            })?;

        let result = self.reduce_predicate_results(num_rows, offsets, list_nulls, pred_bools);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        // Only expose input_list. The predicate is compiled against the inner schema
        // (with the loop variable) and must not be exposed to DF tree traversal.
        vec![&self.input_list]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "QuantifierExecExpr requires exactly 1 child (input_list)".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            input_list: children[0].clone(),
            predicate: self.predicate.clone(),
            variable_name: self.variable_name.clone(),
            input_schema: self.input_schema.clone(),
            quantifier_type: self.quantifier_type,
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({} IN {} WHERE {})",
            self.quantifier_type, self.variable_name, self.input_list, self.predicate
        )
    }
}

impl QuantifierExecExpr {
    /// Reduce predicate results per parent row using three-valued null logic.
    ///
    /// For each parent row, slices the predicate boolean array using offsets and
    /// counts true/false/null, then applies the quantifier semantics.
    fn reduce_predicate_results(
        &self,
        num_rows: usize,
        offsets: &datafusion::arrow::buffer::OffsetBuffer<i64>,
        list_nulls: Option<&datafusion::arrow::buffer::NullBuffer>,
        pred_bools: &BooleanArray,
    ) -> BooleanArray {
        let mut builder = BooleanBuilder::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            // If the list itself is null, result is null
            if list_nulls.is_some_and(|n| !n.is_valid(row_idx)) {
                builder.append_null();
                continue;
            }

            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            let len = end - start;

            if len == 0 {
                // Empty list semantics
                match self.quantifier_type {
                    QuantifierType::All | QuantifierType::None => builder.append_value(true),
                    QuantifierType::Any | QuantifierType::Single => builder.append_value(false),
                }
                continue;
            }

            let mut true_count: usize = 0;
            let mut false_count: usize = 0;
            let mut null_count: usize = 0;

            for i in start..end {
                if pred_bools.is_null(i) {
                    null_count += 1;
                } else if pred_bools.value(i) {
                    true_count += 1;
                } else {
                    false_count += 1;
                }
            }

            match self.quantifier_type {
                QuantifierType::All => {
                    if false_count > 0 {
                        builder.append_value(false);
                    } else if null_count > 0 {
                        builder.append_null();
                    } else {
                        builder.append_value(true);
                    }
                }
                QuantifierType::Any => {
                    if true_count > 0 {
                        builder.append_value(true);
                    } else if null_count > 0 {
                        builder.append_null();
                    } else {
                        builder.append_value(false);
                    }
                }
                QuantifierType::Single => {
                    if true_count > 1 {
                        builder.append_value(false);
                    } else if true_count == 1 && null_count == 0 {
                        builder.append_value(true);
                    } else if true_count == 0 && null_count == 0 {
                        builder.append_value(false);
                    } else {
                        // true_count <= 1 with nulls present — indeterminate
                        builder.append_null();
                    }
                }
                QuantifierType::None => {
                    if true_count > 0 {
                        builder.append_value(false);
                    } else if null_count > 0 {
                        builder.append_null();
                    } else {
                        builder.append_value(true);
                    }
                }
            }
        }

        builder.finish()
    }

    /// Produce results for the degenerate case where every list is empty (or null).
    ///
    /// This avoids building an inner batch when there are zero flattened values.
    fn reduce_empty_lists(
        &self,
        num_rows: usize,
        offsets: &datafusion::arrow::buffer::OffsetBuffer<i64>,
        list_nulls: Option<&datafusion::arrow::buffer::NullBuffer>,
    ) -> BooleanArray {
        let mut builder = BooleanBuilder::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            if list_nulls.is_some_and(|n| !n.is_valid(row_idx)) {
                builder.append_null();
                continue;
            }

            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;

            if start == end {
                // Empty list
                match self.quantifier_type {
                    QuantifierType::All | QuantifierType::None => builder.append_value(true),
                    QuantifierType::Any | QuantifierType::Single => builder.append_value(false),
                }
            } else {
                // Should not reach here since num_values == 0, but handle defensively
                builder.append_null();
            }
        }

        builder.finish()
    }
}
