// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::any::Any;
use std::sync::Arc;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema, Field};
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::take;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;

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
        }
    }
}

impl ListComprehensionExecExpr {
    pub fn new(
        input_list: Arc<dyn PhysicalExpr>,
        map_expr: Arc<dyn PhysicalExpr>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        variable_name: String,
        input_schema: Arc<Schema>,
        output_item_type: DataType,
    ) -> Self {
        Self {
            input_list,
            map_expr,
            predicate,
            variable_name,
            input_schema,
            output_item_type,
        }
    }
}

impl Display for ListComprehensionExecExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "ListComprehension(var={}, list={})", self.variable_name, self.input_list)
    }
}

impl PartialEq for ListComprehensionExecExpr {
    fn eq(&self, other: &Self) -> bool {
        self.variable_name == other.variable_name &&
        self.output_item_type == other.output_item_type &&
        Arc::ptr_eq(&self.input_list, &other.input_list) &&
        Arc::ptr_eq(&self.map_expr, &other.map_expr) &&
        match (&self.predicate, &other.predicate) {
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
        other.downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl PhysicalExpr for ListComprehensionExecExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::LargeList(Arc::new(Field::new("item", self.output_item_type.clone(), true))))
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // 1. Evaluate input list
        let list_val = self.input_list.evaluate(batch)?;
        let list_array = list_val.into_array(batch.num_rows())?;

        // 2. Normalize to LargeListArray
        let list_array = if let DataType::List(field) = list_array.data_type() {
             let target_type = DataType::LargeList(field.clone());
             cast(&list_array, &target_type)
                .map_err(|e| datafusion::error::DataFusionError::Execution(format!("Cast failed: {}", e)))?
        } else {
             list_array
        };
        
        let large_list = list_array.as_any().downcast_ref::<datafusion::arrow::array::LargeListArray>()
            .ok_or_else(|| datafusion::error::DataFusionError::Execution(format!("Expected LargeListArray, got {:?}", list_array.data_type())))?;

        let values = large_list.values();
        let offsets = large_list.offsets();
        let nulls = large_list.nulls();

        // 3. Prepare inner batch
        let num_rows = batch.num_rows();
        let num_values = values.len();
        let mut indices_builder = datafusion::arrow::array::UInt32Builder::with_capacity(num_values);
        for row_idx in 0..num_rows {
             let start = offsets[row_idx] as usize;
             let end = offsets[row_idx+1] as usize;
             let len = end - start;
             for _ in 0..len {
                 indices_builder.append_value(row_idx as u32);
             }
        }
        let indices = indices_builder.finish();
        
        let mut inner_columns = Vec::with_capacity(batch.num_columns() + 1);
        for col in batch.columns() {
            let taken = take(col, &indices, None)
                .map_err(|e| datafusion::error::DataFusionError::Execution(format!("Take failed: {}", e)))?;
            inner_columns.push(taken);
        }
        
        inner_columns.push(values.clone());
        
        let mut inner_fields = batch.schema().fields().to_vec();
        inner_fields.push(Arc::new(Field::new(&self.variable_name, values.data_type().clone(), true)));
        let inner_schema = Arc::new(Schema::new(inner_fields));
        
        let inner_batch = RecordBatch::try_new(inner_schema, inner_columns)?;
        
        if let Some(_pred) = &self.predicate {
             return Err(datafusion::error::DataFusionError::NotImplemented("ListComprehension WHERE not yet implemented".to_string()));
        }
        
        let mapped_val = self.map_expr.evaluate(&inner_batch)?;
        let mapped_array = mapped_val.into_array(inner_batch.num_rows())?;
        
        let new_field = Arc::new(Field::new("item", mapped_array.data_type().clone(), true));
        let new_list = datafusion::arrow::array::LargeListArray::new(
            new_field,
            offsets.clone(),
            mapped_array,
            nulls.cloned(),
        );
        
        Ok(ColumnarValue::Array(Arc::new(new_list)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![&self.input_list, &self.map_expr];
        if let Some(pred) = &self.predicate {
            children.push(pred);
        }
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() < 2 {
            return Err(datafusion::error::DataFusionError::Internal("ListComprehension requires at least 2 children".to_string()));
        }
        
        let input_list = children[0].clone();
        let map_expr = children[1].clone();
        let predicate = if children.len() > 2 {
            Some(children[2].clone())
        } else {
            None
        };

        Ok(Arc::new(Self {
            input_list,
            map_expr,
            predicate,
            variable_name: self.variable_name.clone(),
            input_schema: self.input_schema.clone(),
            output_item_type: self.output_item_type.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} IN {} | {}]", self.variable_name, self.input_list, self.map_expr)
    }
}