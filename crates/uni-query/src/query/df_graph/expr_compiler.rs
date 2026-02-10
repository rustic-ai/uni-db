// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::df_expr::{TranslationContext, cypher_expr_to_df};
use crate::query::df_graph::comprehension::ListComprehensionExecExpr;
use crate::query::df_graph::reduce::ReduceExecExpr;
use anyhow::{Result, anyhow};
use arrow_schema::{Field, Schema};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::expr::Alias;
use datafusion::physical_expr::expressions::binary;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_planner::PhysicalPlanner;
use std::sync::Arc;
use uni_cypher::ast::{BinaryOp, Expr, UnaryOp};

/// Compiler for converting Cypher expressions directly to DataFusion Physical Expressions.
pub struct CypherPhysicalExprCompiler<'a> {
    state: &'a SessionState,
    translation_ctx: Option<&'a TranslationContext>,
}

impl<'a> CypherPhysicalExprCompiler<'a> {
    pub fn new(state: &'a SessionState, translation_ctx: Option<&'a TranslationContext>) -> Self {
        Self {
            state,
            translation_ctx,
        }
    }

    /// Compile a Cypher expression into a DataFusion PhysicalExpr.
    pub fn compile(&self, expr: &Expr, input_schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                map_expr,
            } => self.compile_list_comprehension(
                variable,
                list,
                where_clause.as_deref(),
                map_expr,
                input_schema,
            ),
            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr: expression,
            } => self.compile_reduce(accumulator, init, variable, list, expression, input_schema),
            // For BinaryOp, check if children contain custom expressions
            Expr::BinaryOp { left, op, right } => {
                if Self::contains_custom_expr(left) || Self::contains_custom_expr(right) {
                    let left_phy = self.compile(left, input_schema)?;
                    let right_phy = self.compile(right, input_schema)?;
                    self.compile_binary_op(op, left_phy, right_phy, input_schema)
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                if Self::contains_custom_expr(inner) {
                    let inner_phy = self.compile(inner, input_schema)?;
                    self.compile_unary_op(op, inner_phy, input_schema)
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }
            Expr::IsNull(inner) => {
                if Self::contains_custom_expr(inner) {
                    let inner_phy = self.compile(inner, input_schema)?;
                    Ok(datafusion::physical_expr::expressions::is_null(inner_phy)
                        .map_err(|e| anyhow!("Failed to create is_null: {}", e))?)
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }
            Expr::IsNotNull(inner) => {
                if Self::contains_custom_expr(inner) {
                    let inner_phy = self.compile(inner, input_schema)?;
                    Ok(
                        datafusion::physical_expr::expressions::is_not_null(inner_phy)
                            .map_err(|e| anyhow!("Failed to create is_not_null: {}", e))?,
                    )
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }
            // In operator is Expr::In { expr, list }
            Expr::In {
                expr: left,
                list: right,
            } => {
                if Self::contains_custom_expr(left) || Self::contains_custom_expr(right) {
                    Err(anyhow!(
                        "IN operator with custom expressions not yet supported"
                    ))
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }

            // Recursively check other composite types if necessary.
            Expr::List(items) => {
                if items.iter().any(Self::contains_custom_expr) {
                    Err(anyhow!(
                        "List literals containing comprehensions not yet supported in compiler"
                    ))
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }
            Expr::Map(entries) => {
                if entries.iter().any(|(_, v)| Self::contains_custom_expr(v)) {
                    Err(anyhow!(
                        "Map literals containing comprehensions not yet supported in compiler"
                    ))
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }

            // Property access on a struct column — e.g. `x.a` where `x` is Struct
            Expr::Property(base, prop) => {
                if let Expr::Variable(var_name) = base.as_ref() {
                    if let Ok(col_idx) = input_schema.index_of(var_name) {
                        let col_type = input_schema.field(col_idx).data_type();
                        if let arrow_schema::DataType::Struct(struct_fields) = col_type {
                            // Find the struct field by name
                            let field_idx = struct_fields
                                .iter()
                                .position(|f| f.name() == prop)
                                .ok_or_else(|| {
                                    anyhow!(
                                        "Struct field '{}' not found in column '{}'. \
                                         Available: {:?}",
                                        prop,
                                        var_name,
                                        struct_fields
                                            .iter()
                                            .map(|f| f.name())
                                            .collect::<Vec<_>>()
                                    )
                                })?;
                            let output_type =
                                struct_fields[field_idx].data_type().clone();
                            let col_expr: Arc<dyn PhysicalExpr> = Arc::new(
                                datafusion::physical_expr::expressions::Column::new(
                                    var_name, col_idx,
                                ),
                            );
                            return Ok(Arc::new(StructFieldAccessExpr::new(
                                col_expr, field_idx, output_type,
                            )));
                        }
                    }
                }
                self.compile_standard(expr, input_schema)
            }

            // Default to standard compilation for leaf nodes or non-custom trees
            _ => self.compile_standard(expr, input_schema),
        }
    }

    /// Check if an expression tree contains nodes that require custom compilation.
    fn contains_custom_expr(expr: &Expr) -> bool {
        match expr {
            Expr::ListComprehension { .. } => true,
            Expr::Reduce { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_custom_expr(left) || Self::contains_custom_expr(right)
            }
            Expr::UnaryOp { expr, .. } => Self::contains_custom_expr(expr),
            Expr::FunctionCall { args, .. } => args.iter().any(Self::contains_custom_expr),
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                when_then
                    .iter()
                    .any(|(w, t)| Self::contains_custom_expr(w) || Self::contains_custom_expr(t))
                    || else_expr
                        .as_ref()
                        .map(|e| Self::contains_custom_expr(e))
                        .unwrap_or(false)
            }
            Expr::List(items) => items.iter().any(Self::contains_custom_expr),
            Expr::Map(entries) => entries.iter().any(|(_, v)| Self::contains_custom_expr(v)),
            Expr::IsNull(e) | Expr::IsNotNull(e) => Self::contains_custom_expr(e),
            Expr::In { expr: l, list: r } => {
                Self::contains_custom_expr(l) || Self::contains_custom_expr(r)
            }
            _ => false,
        }
    }

    fn compile_standard(
        &self,
        expr: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let df_expr = cypher_expr_to_df(expr, self.translation_ctx)?;
        let resolved_expr = self.resolve_udfs(df_expr)?;

        let df_schema = datafusion::common::DFSchema::try_from(input_schema.clone())?;

        // Apply type coercion to resolve type mismatches
        let coerced_expr = crate::query::df_expr::apply_type_coercion(&resolved_expr, &df_schema)?;

        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
        planner
            .create_physical_expr(&coerced_expr, &df_schema, self.state)
            .map_err(|e| anyhow!("DataFusion planning failed: {}", e))
    }

    /// Resolve UDFs in DataFusion expression using the session state registry.
    fn resolve_udfs(
        &self,
        expr: datafusion::logical_expr::Expr,
    ) -> Result<datafusion::logical_expr::Expr> {
        use datafusion::logical_expr::Expr as DfExpr;

        match expr {
            DfExpr::ScalarFunction(func) => {
                let udf_name = func.func.name();

                let resolved_args: Vec<DfExpr> = func
                    .args
                    .iter()
                    .map(|arg| self.resolve_udfs(arg.clone()))
                    .collect::<Result<Vec<_>>>()?;

                let func_ref = match self.state.scalar_functions().get(udf_name) {
                    Some(registered_udf) => registered_udf.clone(),
                    None => func.func.clone(),
                };

                Ok(DfExpr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction {
                        func: func_ref,
                        args: resolved_args,
                    },
                ))
            }
            DfExpr::BinaryExpr(binary) => {
                Ok(DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                    left: Box::new(self.resolve_udfs(*binary.left)?),
                    op: binary.op,
                    right: Box::new(self.resolve_udfs(*binary.right)?),
                }))
            }
            DfExpr::Not(inner) => Ok(DfExpr::Not(Box::new(self.resolve_udfs(*inner)?))),
            DfExpr::IsNull(inner) => Ok(DfExpr::IsNull(Box::new(self.resolve_udfs(*inner)?))),
            DfExpr::IsNotNull(inner) => Ok(DfExpr::IsNotNull(Box::new(self.resolve_udfs(*inner)?))),
            DfExpr::Negative(inner) => Ok(DfExpr::Negative(Box::new(self.resolve_udfs(*inner)?))),
            DfExpr::Alias(Alias {
                expr,
                relation,
                name,
                ..
            }) => Ok(DfExpr::Alias(Alias {
                expr: Box::new(self.resolve_udfs(*expr)?),
                relation,
                name,
                metadata: None,
            })),
            _ => Ok(expr),
        }
    }

    fn compile_list_comprehension(
        &self,
        variable: &str,
        list: &Expr,
        where_clause: Option<&Expr>,
        map_expr: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let input_list_phy = self.compile(list, input_schema)?;

        // Resolve input list type
        let list_data_type = input_list_phy.data_type(input_schema)?;
        let inner_data_type = match list_data_type {
            arrow_schema::DataType::List(field) | arrow_schema::DataType::LargeList(field) => {
                field.data_type().clone()
            }
            arrow_schema::DataType::Null => arrow_schema::DataType::Null,
            _ => {
                return Err(anyhow!(
                    "List comprehension input must be a list, got {:?}",
                    list_data_type
                ));
            }
        };

        // Create inner schema with loop variable
        let mut fields = input_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(variable, inner_data_type, true)));
        let inner_schema = Arc::new(Schema::new(fields));

        // Compile inner expressions
        let predicate_phy = if let Some(pred) = where_clause {
            Some(self.compile(pred, &inner_schema)?)
        } else {
            None
        };

        let map_phy = self.compile(map_expr, &inner_schema)?;
        let output_item_type = map_phy.data_type(&inner_schema)?;

        Ok(Arc::new(ListComprehensionExecExpr::new(
            input_list_phy,
            map_phy,
            predicate_phy,
            variable.to_string(),
            Arc::new(input_schema.clone()),
            output_item_type,
        )))
    }

    fn compile_reduce(
        &self,
        accumulator: &str,
        initial: &Expr,
        variable: &str,
        list: &Expr,
        reduce_expr: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let list_phy = self.compile(list, input_schema)?;

        let initial_phy = self.compile(initial, input_schema)?;
        let acc_type = initial_phy.data_type(input_schema)?;

        let list_data_type = list_phy.data_type(input_schema)?;
        let inner_data_type = match list_data_type {
            arrow_schema::DataType::List(field) | arrow_schema::DataType::LargeList(field) => {
                field.data_type().clone()
            }
            arrow_schema::DataType::Null => arrow_schema::DataType::Null,
            _ => {
                return Err(anyhow!(
                    "Reduce input must be a list, got {:?}",
                    list_data_type
                ));
            }
        };

        let mut fields = input_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(accumulator, acc_type.clone(), true)));
        fields.push(Arc::new(Field::new(variable, inner_data_type, true)));
        let inner_schema = Arc::new(Schema::new(fields));

        let reduce_phy = self.compile(reduce_expr, &inner_schema)?;
        let output_type = reduce_phy.data_type(&inner_schema)?;

        Ok(Arc::new(ReduceExecExpr::new(
            accumulator.to_string(),
            initial_phy,
            variable.to_string(),
            list_phy,
            reduce_phy,
            Arc::new(input_schema.clone()),
            output_type,
        )))
    }

    fn compile_binary_op(
        &self,
        op: &BinaryOp,
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // Map Cypher BinaryOp to DataFusion Operator
        use datafusion::logical_expr::Operator;

        // String operators mapping (using custom physical expr for safe type handling)
        match op {
            BinaryOp::StartsWith => {
                return Ok(Arc::new(CypherStringMatchExpr::new(
                    left,
                    right,
                    StringOp::StartsWith,
                )));
            }
            BinaryOp::EndsWith => {
                return Ok(Arc::new(CypherStringMatchExpr::new(
                    left,
                    right,
                    StringOp::EndsWith,
                )));
            }
            BinaryOp::Contains => {
                return Ok(Arc::new(CypherStringMatchExpr::new(
                    left,
                    right,
                    StringOp::Contains,
                )));
            }
            _ => {}
        }

        let df_op = match op {
            BinaryOp::Add => Operator::Plus,
            BinaryOp::Sub => Operator::Minus,
            BinaryOp::Mul => Operator::Multiply,
            BinaryOp::Div => Operator::Divide,
            BinaryOp::Mod => Operator::Modulo,
            BinaryOp::Eq => Operator::Eq,
            BinaryOp::NotEq => Operator::NotEq,
            BinaryOp::Gt => Operator::Gt,
            BinaryOp::GtEq => Operator::GtEq,
            BinaryOp::Lt => Operator::Lt,
            BinaryOp::LtEq => Operator::LtEq,
            BinaryOp::And => Operator::And,
            BinaryOp::Or => Operator::Or,
            BinaryOp::Xor => {
                return Err(anyhow!(
                    "XOR not supported via binary helper, use bitwise_xor"
                ));
            }
            BinaryOp::Regex => Operator::RegexMatch,
            BinaryOp::ApproxEq => {
                return Err(anyhow!(
                    "ApproxEq (~=) not yet supported in physical compiler"
                ));
            }
            BinaryOp::Pow => return Err(anyhow!("POW not yet supported in physical compiler")),
            _ => return Err(anyhow!("Unsupported binary op in compiler: {:?}", op)),
        };

        // Use DataFusion's binary physical expression creator which handles coercion
        binary(left, df_op, right, input_schema)
            .map_err(|e| anyhow!("Failed to create binary expression: {}", e))
    }

    fn compile_unary_op(
        &self,
        op: &UnaryOp,
        expr: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match op {
            UnaryOp::Not => datafusion::physical_expr::expressions::not(expr),
            UnaryOp::Neg => datafusion::physical_expr::expressions::negative(expr, input_schema),
        }
        .map_err(|e| anyhow!("Failed to create unary expression: {}", e))
    }
}

use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StringOp {
    StartsWith,
    EndsWith,
    Contains,
}

impl std::fmt::Display for StringOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StringOp::StartsWith => write!(f, "STARTS WITH"),
            StringOp::EndsWith => write!(f, "ENDS WITH"),
            StringOp::Contains => write!(f, "CONTAINS"),
        }
    }
}

#[derive(Debug, Eq)]
struct CypherStringMatchExpr {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: StringOp,
}

impl PartialEq for CypherStringMatchExpr {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op && self.left.eq(&other.left) && self.right.eq(&other.right)
    }
}

impl std::hash::Hash for CypherStringMatchExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.op.hash(state);
        self.left.hash(state);
        self.right.hash(state);
    }
}

impl CypherStringMatchExpr {
    fn new(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>, op: StringOp) -> Self {
        Self { left, right, op }
    }
}

impl std::fmt::Display for CypherStringMatchExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl DisplayAs for CypherStringMatchExpr {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PhysicalExpr for CypherStringMatchExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &Schema,
    ) -> datafusion::error::Result<arrow_schema::DataType> {
        Ok(arrow_schema::DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        use crate::query::df_udfs::invoke_cypher_string_op;
        use arrow_schema::Field;
        use datafusion::config::ConfigOptions;
        use datafusion::logical_expr::ScalarFunctionArgs;

        let left_val = self.left.evaluate(batch)?;
        let right_val = self.right.evaluate(batch)?;

        let args = ScalarFunctionArgs {
            args: vec![left_val, right_val],
            number_rows: batch.num_rows(),
            return_field: Arc::new(Field::new("result", arrow_schema::DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![], // Not used by invoke_cypher_string_op
        };

        match self.op {
            StringOp::StartsWith => {
                invoke_cypher_string_op(&args, "starts_with", |s, p| s.starts_with(p))
            }
            StringOp::EndsWith => {
                invoke_cypher_string_op(&args, "ends_with", |s, p| s.ends_with(p))
            }
            StringOp::Contains => invoke_cypher_string_op(&args, "contains", |s, p| s.contains(p)),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CypherStringMatchExpr::new(
            children[0].clone(),
            children[1].clone(),
            self.op,
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PartialEq<dyn PhysicalExpr> for CypherStringMatchExpr {
    fn eq(&self, other: &dyn PhysicalExpr) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<CypherStringMatchExpr>() {
            self == other
        } else {
            false
        }
    }
}

/// Physical expression for extracting a field from a struct column.
///
/// Used when list comprehension iterates over a list of structs (maps)
/// and accesses a field, e.g., `[x IN [{a: 1}] | x.a]`.
#[derive(Debug, Eq)]
struct StructFieldAccessExpr {
    /// Expression producing the struct column.
    input: Arc<dyn PhysicalExpr>,
    /// Index of the field within the struct.
    field_idx: usize,
    /// Output data type of the extracted field.
    output_type: arrow_schema::DataType,
}

impl PartialEq for StructFieldAccessExpr {
    fn eq(&self, other: &Self) -> bool {
        self.field_idx == other.field_idx && self.input.eq(&other.input)
            && self.output_type == other.output_type
    }
}

impl std::hash::Hash for StructFieldAccessExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.field_idx.hash(state);
    }
}

impl StructFieldAccessExpr {
    fn new(
        input: Arc<dyn PhysicalExpr>,
        field_idx: usize,
        output_type: arrow_schema::DataType,
    ) -> Self {
        Self {
            input,
            field_idx,
            output_type,
        }
    }
}

impl std::fmt::Display for StructFieldAccessExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}]", self.input, self.field_idx)
    }
}

impl DisplayAs for StructFieldAccessExpr {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PartialEq<dyn PhysicalExpr> for StructFieldAccessExpr {
    fn eq(&self, other: &dyn PhysicalExpr) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self.field_idx == other.field_idx && self.input.eq(&other.input)
        } else {
            false
        }
    }
}

impl PhysicalExpr for StructFieldAccessExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &Schema,
    ) -> datafusion::error::Result<arrow_schema::DataType> {
        Ok(self.output_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        use arrow_array::StructArray;

        let input_val = self.input.evaluate(batch)?;
        let array = input_val.into_array(batch.num_rows())?;

        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "StructFieldAccessExpr: input is not a StructArray".to_string(),
                )
            })?;

        let field_col = struct_array.column(self.field_idx).clone();
        Ok(datafusion::physical_plan::ColumnarValue::Array(field_col))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(StructFieldAccessExpr::new(
            children[0].clone(),
            self.field_idx,
            self.output_type.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}
