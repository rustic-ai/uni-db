// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::any::Any;
use std::sync::Arc;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;

use arrow_schema::{DataType, Schema, Field};
use arrow_array::RecordBatch;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;

/// Physical expression for Cypher List Comprehension: `[x IN list WHERE pred | expr]`
///
/// Executes the comprehension by flattening the input list, evaluating the inner expressions
/// on the flattened data, and reconstructing the list structure.
#[derive(Debug, Clone)]
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
}

impl ListComprehensionExecExpr {
    pub fn new(
        input_list: Arc<dyn PhysicalExpr>,
        map_expr: Arc<dyn PhysicalExpr>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        variable_name: String,
        input_schema: Arc<Schema>,
    ) -> Self {
        Self {
            input_list,
            map_expr,
            predicate,
            variable_name,
            input_schema,
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
        // For children, we check referential equality for now.
        // Deep comparison of PhysicalExpr is strict in DataFusion.
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
        // Do not hash pointers as they change across deserialization/cloning if deep cloned?
        // But Arc pointers are stable.
        // Ideally we should hash the content of expressions but PhysicalExpr doesn't impl Hash.
    }
}

// Helper to downcast Any to Self
fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    any
}

impl PartialEq<dyn Any> for ListComprehensionExecExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
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
        // Output type is List(map_expr type).
        // To determine this, we need the schema of the INNER context (with `x`).
        // This is complex to determine upfront without evaluating `input_list` type.
        // For now, let's assume it returns a List of Any (LargeBinary/JSONB) or
        // try to infer if we can.
        
        // TODO: Properly infer return type. For now, return List(LargeBinary) as safe default for JSON.
        Ok(DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))))
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        todo!("Implement ListComprehension evaluation")
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
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} IN {} | {}]", self.variable_name, self.input_list, self.map_expr)
    }
}
