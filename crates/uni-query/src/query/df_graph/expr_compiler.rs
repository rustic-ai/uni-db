// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::df_expr::{TranslationContext, cypher_expr_to_df};
use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{execute_subplan, extract_row_params};
use crate::query::df_graph::comprehension::ListComprehensionExecExpr;
use crate::query::df_graph::pattern_comprehension::{
    PatternComprehensionExecExpr, analyze_pattern, build_inner_schema, collect_inner_properties,
};
use crate::query::df_graph::quantifier::{QuantifierExecExpr, QuantifierType};
use crate::query::df_graph::reduce::ReduceExecExpr;
use crate::query::planner::QueryPlanner;
use anyhow::{Result, anyhow};
use arrow_array::builder::BooleanBuilder;
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::expressions::binary;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::prelude::SessionContext;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, Query, UnaryOp};
use uni_store::storage::manager::StorageManager;

/// Compiler for converting Cypher expressions directly to DataFusion Physical Expressions.
pub struct CypherPhysicalExprCompiler<'a> {
    state: &'a SessionState,
    translation_ctx: Option<&'a TranslationContext>,
    graph_ctx: Option<Arc<GraphExecutionContext>>,
    uni_schema: Option<Arc<UniSchema>>,
    /// Session context for EXISTS subquery execution.
    session_ctx: Option<Arc<RwLock<SessionContext>>>,
    /// Storage manager for EXISTS subquery execution.
    storage: Option<Arc<StorageManager>>,
    /// Query parameters for EXISTS subquery execution.
    params: HashMap<String, Value>,
}

impl<'a> CypherPhysicalExprCompiler<'a> {
    pub fn new(state: &'a SessionState, translation_ctx: Option<&'a TranslationContext>) -> Self {
        Self {
            state,
            translation_ctx,
            graph_ctx: None,
            uni_schema: None,
            session_ctx: None,
            storage: None,
            params: HashMap::new(),
        }
    }

    /// Attach graph context and schema for pattern comprehension support.
    pub fn with_graph_ctx(
        mut self,
        graph_ctx: Arc<GraphExecutionContext>,
        uni_schema: Arc<UniSchema>,
    ) -> Self {
        self.graph_ctx = Some(graph_ctx);
        self.uni_schema = Some(uni_schema);
        self
    }

    /// Attach full subquery context for EXISTS support.
    pub fn with_subquery_ctx(
        mut self,
        graph_ctx: Arc<GraphExecutionContext>,
        uni_schema: Arc<UniSchema>,
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        params: HashMap<String, Value>,
    ) -> Self {
        self.graph_ctx = Some(graph_ctx);
        self.uni_schema = Some(uni_schema);
        self.session_ctx = Some(session_ctx);
        self.storage = Some(storage);
        self.params = params;
        self
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
            Expr::Quantifier {
                quantifier,
                variable,
                list,
                predicate,
            } => self.compile_quantifier(quantifier, variable, list, predicate, input_schema),
            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr: expression,
            } => self.compile_reduce(accumulator, init, variable, list, expression, input_schema),
            // For BinaryOp, check if children contain custom expressions or JSONB types
            Expr::BinaryOp { left, op, right } => {
                if Self::contains_custom_expr(left) || Self::contains_custom_expr(right) {
                    let left_phy = self.compile(left, input_schema)?;
                    let right_phy = self.compile(right, input_schema)?;
                    self.compile_binary_op(op, left_phy, right_phy, input_schema)
                } else {
                    // For Add with a list-producing operand (AST-level detection),
                    // compile through the standard path which uses cypher_expr_to_df
                    // to correctly route list + scalar to _cypher_list_concat.
                    if *op == BinaryOp::Add
                        && (Self::is_list_producing(left) || Self::is_list_producing(right))
                    {
                        return self.compile_standard(expr, input_schema);
                    }

                    // Compile sub-expressions to check their types. If either operand
                    // produces LargeBinary (JSONB), standard Arrow kernels will fail at
                    // runtime for comparisons. Route through compile_binary_op which
                    // dispatches to Cypher comparison UDFs.
                    let left_phy = self.compile(left, input_schema)?;
                    let right_phy = self.compile(right, input_schema)?;
                    let left_dt = left_phy.data_type(input_schema).ok();
                    let right_dt = right_phy.data_type(input_schema).ok();
                    let has_jsonb = left_dt
                        .as_ref()
                        .is_some_and(|t| *t == arrow_schema::DataType::LargeBinary)
                        || right_dt
                            .as_ref()
                            .is_some_and(|t| *t == arrow_schema::DataType::LargeBinary);
                    if has_jsonb {
                        self.compile_binary_op(op, left_phy, right_phy, input_schema)
                    } else {
                        self.compile_standard(expr, input_schema)
                    }
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
                if let Expr::Variable(var_name) = base.as_ref()
                    && let Ok(col_idx) = input_schema.index_of(var_name)
                    && let arrow_schema::DataType::Struct(struct_fields) =
                        input_schema.field(col_idx).data_type()
                {
                    // Find the struct field by name.
                    // Cypher semantics: accessing a missing key on a map returns null.
                    if let Some(field_idx) = struct_fields.iter().position(|f| f.name() == prop) {
                        let output_type = struct_fields[field_idx].data_type().clone();
                        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(var_name, col_idx),
                        );
                        return Ok(Arc::new(StructFieldAccessExpr::new(
                            col_expr,
                            field_idx,
                            output_type,
                        )));
                    } else {
                        // Field not found in struct → return null (Cypher semantics)
                        return Ok(Arc::new(
                            datafusion::physical_expr::expressions::Literal::new(
                                datafusion::common::ScalarValue::Null,
                            ),
                        ));
                    }
                }
                self.compile_standard(expr, input_schema)
            }

            // Bracket access on a struct column — e.g. `x['a']` where `x` is Struct
            Expr::ArrayIndex { array, index } => {
                if let Expr::Variable(var_name) = array.as_ref()
                    && let Expr::Literal(CypherLiteral::String(prop)) = index.as_ref()
                    && let Ok(col_idx) = input_schema.index_of(var_name)
                {
                    let col_type = input_schema.field(col_idx).data_type();
                    if let arrow_schema::DataType::Struct(struct_fields) = col_type {
                        if let Some(field_idx) = struct_fields.iter().position(|f| f.name() == prop)
                        {
                            let output_type = struct_fields[field_idx].data_type().clone();
                            let col_expr: Arc<dyn PhysicalExpr> =
                                Arc::new(datafusion::physical_expr::expressions::Column::new(
                                    var_name, col_idx,
                                ));
                            return Ok(Arc::new(StructFieldAccessExpr::new(
                                col_expr,
                                field_idx,
                                output_type,
                            )));
                        } else {
                            // Field not found in struct → return null (Cypher semantics)
                            return Ok(Arc::new(
                                datafusion::physical_expr::expressions::Literal::new(
                                    datafusion::common::ScalarValue::Null,
                                ),
                            ));
                        }
                    }
                }
                self.compile_standard(expr, input_schema)
            }

            // Pattern comprehension: [(a)-[:REL]->(b) WHERE pred | expr]
            Expr::PatternComprehension {
                path_variable,
                pattern,
                where_clause,
                map_expr,
            } => self.compile_pattern_comprehension(
                path_variable,
                pattern,
                where_clause.as_deref(),
                map_expr,
                input_schema,
            ),

            // EXISTS subquery: plan + execute per row, return boolean
            Expr::Exists(query) => {
                let graph_ctx = self
                    .graph_ctx
                    .as_ref()
                    .ok_or_else(|| anyhow!("EXISTS requires GraphExecutionContext"))?
                    .clone();
                let uni_schema = self
                    .uni_schema
                    .as_ref()
                    .ok_or_else(|| anyhow!("EXISTS requires UniSchema"))?
                    .clone();
                let session_ctx = self
                    .session_ctx
                    .as_ref()
                    .ok_or_else(|| anyhow!("EXISTS requires SessionContext"))?
                    .clone();
                let storage = self
                    .storage
                    .as_ref()
                    .ok_or_else(|| anyhow!("EXISTS requires StorageManager"))?
                    .clone();

                Ok(Arc::new(ExistsExecExpr::new(
                    *query.clone(),
                    graph_ctx,
                    session_ctx,
                    storage,
                    uni_schema,
                    self.params.clone(),
                )))
            }

            // Default to standard compilation for leaf nodes or non-custom trees
            _ => self.compile_standard(expr, input_schema),
        }
    }

    /// Check if an expression tree contains nodes that require custom compilation.
    pub fn contains_custom_expr(expr: &Expr) -> bool {
        match expr {
            Expr::ListComprehension { .. } => true,
            Expr::Quantifier { .. } => true,
            Expr::Reduce { .. } => true,
            Expr::PatternComprehension { .. } => true,
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
            Expr::Exists(_) => true,
            _ => false,
        }
    }

    /// Check if an expression statically produces a list value.
    /// Used to route `Add` operations to `_cypher_list_concat` instead of arithmetic.
    fn is_list_producing(expr: &Expr) -> bool {
        match expr {
            Expr::List(_) => true,
            Expr::ListComprehension { .. } => true,
            Expr::ArraySlice { .. } => true,
            // Add with a list-producing child produces a list
            Expr::BinaryOp {
                left,
                op: BinaryOp::Add,
                right,
            } => Self::is_list_producing(left) || Self::is_list_producing(right),
            Expr::FunctionCall { name, .. } => {
                // Functions known to return lists
                matches!(
                    name.as_str(),
                    "range"
                        | "tail"
                        | "reverse"
                        | "collect"
                        | "keys"
                        | "labels"
                        | "nodes"
                        | "relationships"
                )
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

        // Re-resolve UDFs after coercion (coercion may introduce new dummy UDF calls)
        let coerced_expr = self.resolve_udfs(coerced_expr)?;

        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
        planner
            .create_physical_expr(&coerced_expr, &df_schema, self.state)
            .map_err(|e| anyhow!("DataFusion planning failed: {}", e))
    }

    /// Resolve UDFs in DataFusion expression using the session state registry.
    ///
    /// Uses `TreeNode::transform_up` to traverse the entire expression tree,
    /// ensuring UDFs inside Cast, Case, InList, Between, etc. are all resolved.
    fn resolve_udfs(
        &self,
        expr: datafusion::logical_expr::Expr,
    ) -> Result<datafusion::logical_expr::Expr> {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::logical_expr::Expr as DfExpr;

        let result = expr
            .transform_up(|node| {
                if let DfExpr::ScalarFunction(ref func) = node {
                    let udf_name = func.func.name();
                    if let Some(registered_udf) = self.state.scalar_functions().get(udf_name) {
                        return Ok(Transformed::yes(DfExpr::ScalarFunction(
                            datafusion::logical_expr::expr::ScalarFunction {
                                func: registered_udf.clone(),
                                args: func.args.clone(),
                            },
                        )));
                    }
                }
                Ok(Transformed::no(node))
            })
            .map_err(|e| anyhow!("Failed to resolve UDFs: {}", e))?;
        Ok(result.data)
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
            // LargeBinary may contain JSONB-encoded arrays; elements will be LargeBinary JSONB scalars
            arrow_schema::DataType::LargeBinary => arrow_schema::DataType::LargeBinary,
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
            // LargeBinary may contain JSONB-encoded arrays. Use the accumulator type as
            // the element type so that the reduce body expression compiles correctly
            // (e.g. acc + x where both are Int64). At evaluation time, JSONB elements
            // are decoded and cast to this type.
            arrow_schema::DataType::LargeBinary => acc_type.clone(),
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

    fn compile_quantifier(
        &self,
        quantifier: &uni_cypher::ast::Quantifier,
        variable: &str,
        list: &Expr,
        predicate: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let input_list_phy = self.compile(list, input_schema)?;

        // Resolve element type from list type
        let list_data_type = input_list_phy.data_type(input_schema)?;
        let inner_data_type = match list_data_type {
            arrow_schema::DataType::List(field) | arrow_schema::DataType::LargeList(field) => {
                field.data_type().clone()
            }
            arrow_schema::DataType::Null => arrow_schema::DataType::Null,
            arrow_schema::DataType::LargeBinary => arrow_schema::DataType::LargeBinary,
            _ => {
                return Err(anyhow!(
                    "Quantifier input must be a list, got {:?}",
                    list_data_type
                ));
            }
        };

        // Create inner schema with loop variable
        let mut fields = input_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(variable, inner_data_type, true)));
        let inner_schema = Arc::new(Schema::new(fields));

        let predicate_phy = self.compile(predicate, &inner_schema)?;

        let qt = match quantifier {
            uni_cypher::ast::Quantifier::All => QuantifierType::All,
            uni_cypher::ast::Quantifier::Any => QuantifierType::Any,
            uni_cypher::ast::Quantifier::Single => QuantifierType::Single,
            uni_cypher::ast::Quantifier::None => QuantifierType::None,
        };

        Ok(Arc::new(QuantifierExecExpr::new(
            input_list_phy,
            predicate_phy,
            variable.to_string(),
            Arc::new(input_schema.clone()),
            qt,
        )))
    }

    fn compile_pattern_comprehension(
        &self,
        path_variable: &Option<String>,
        pattern: &uni_cypher::ast::Pattern,
        where_clause: Option<&Expr>,
        map_expr: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let graph_ctx = self
            .graph_ctx
            .as_ref()
            .ok_or_else(|| anyhow!("Pattern comprehension requires GraphExecutionContext"))?;
        let uni_schema = self
            .uni_schema
            .as_ref()
            .ok_or_else(|| anyhow!("Pattern comprehension requires UniSchema"))?;

        // 1. Analyze pattern to get anchor column and traversal steps
        let (anchor_col, steps) = analyze_pattern(pattern, input_schema, uni_schema)?;

        // 2. Collect needed properties from where_clause and map_expr
        let (vertex_props, edge_props) = collect_inner_properties(where_clause, map_expr, &steps);

        // 3. Build inner schema
        let inner_schema = build_inner_schema(
            input_schema,
            &steps,
            &vertex_props,
            &edge_props,
            path_variable.as_deref(),
        );

        // 4. Compile predicate and map_expr against inner schema
        let pred_phy = where_clause
            .map(|p| self.compile(p, &inner_schema))
            .transpose()?;
        let map_phy = self.compile(map_expr, &inner_schema)?;
        let output_type = map_phy.data_type(&inner_schema)?;

        // 5. Return expression
        Ok(Arc::new(PatternComprehensionExecExpr::new(
            graph_ctx.clone(),
            anchor_col,
            steps,
            path_variable.clone(),
            pred_phy,
            map_phy,
            Arc::new(input_schema.clone()),
            Arc::new(inner_schema),
            output_type,
            vertex_props,
            edge_props,
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

        // When either operand is LargeBinary (JSONB), standard Arrow comparison
        // kernels can't handle the type mismatch. Route through Cypher comparison
        // UDFs which decode JSONB to Value for comparison.
        let left_type = left.data_type(input_schema).ok();
        let right_type = right.data_type(input_schema).ok();
        let has_jsonb = left_type
            .as_ref()
            .is_some_and(|t| *t == arrow_schema::DataType::LargeBinary)
            || right_type
                .as_ref()
                .is_some_and(|t| *t == arrow_schema::DataType::LargeBinary);

        if has_jsonb {
            let udf_name = match df_op {
                Operator::Eq => Some("_cypher_equal"),
                Operator::NotEq => Some("_cypher_not_equal"),
                Operator::Gt => Some("_cypher_gt"),
                Operator::GtEq => Some("_cypher_gt_eq"),
                Operator::Lt => Some("_cypher_lt"),
                Operator::LtEq => Some("_cypher_lt_eq"),
                _ => None,
            };
            if let Some(name) = udf_name
                && let Some(udf) = self.state.scalar_functions().get(name)
            {
                let df_left = datafusion::logical_expr::Expr::Column(
                    datafusion::common::Column::new(None::<String>, "__left__"),
                );
                let df_right = datafusion::logical_expr::Expr::Column(
                    datafusion::common::Column::new(None::<String>, "__right__"),
                );
                let udf_expr = datafusion::logical_expr::Expr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction {
                        func: udf.clone(),
                        args: vec![df_left, df_right],
                    },
                );
                // Build a temporary schema with the two operand types
                let left_dt = left_type.unwrap_or(arrow_schema::DataType::LargeBinary);
                let right_dt = right_type.unwrap_or(arrow_schema::DataType::LargeBinary);
                let tmp_schema = arrow_schema::Schema::new(vec![
                    Arc::new(arrow_schema::Field::new("__left__", left_dt, true)),
                    Arc::new(arrow_schema::Field::new("__right__", right_dt, true)),
                ]);
                let df_schema = datafusion::common::DFSchema::try_from(tmp_schema.clone())?;
                let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
                let udf_phy = planner
                    .create_physical_expr(&udf_expr, &df_schema, self.state)
                    .map_err(|e| anyhow!("Failed to create JSONB comparison expr: {}", e))?;
                // Replace the dummy columns with the actual physical expressions
                let udf_phy = udf_phy
                    .with_new_children(vec![left, right])
                    .map_err(|e| anyhow!("Failed to rebind JSONB comparison children: {}", e))?;
                return Ok(udf_phy);
            }

            // List concat/append via Plus when at least one side is a list.
            // - Both LargeBinary: could be list+list (original case)
            // - At least one Arrow List type: definitely a list from make_array
            // Note: a single LargeBinary may be a JSONB scalar (e.g. from property access),
            // so we only treat it as list when both sides are LargeBinary or when paired
            // with an Arrow List type.
            let left_is_lb = left_type
                .as_ref()
                .is_some_and(|t| *t == arrow_schema::DataType::LargeBinary);
            let right_is_lb = right_type
                .as_ref()
                .is_some_and(|t| *t == arrow_schema::DataType::LargeBinary);
            let left_is_arrow_list = left_type
                .as_ref()
                .is_some_and(|t| matches!(t, arrow_schema::DataType::List(_)));
            let right_is_arrow_list = right_type
                .as_ref()
                .is_some_and(|t| matches!(t, arrow_schema::DataType::List(_)));
            let is_list_plus = df_op == Operator::Plus
                && ((left_is_lb && right_is_lb) || left_is_arrow_list || right_is_arrow_list);
            if is_list_plus
                && let Some(udf) = self.state.scalar_functions().get("_cypher_list_concat")
            {
                let df_left = datafusion::logical_expr::Expr::Column(
                    datafusion::common::Column::new(None::<String>, "__left__"),
                );
                let df_right = datafusion::logical_expr::Expr::Column(
                    datafusion::common::Column::new(None::<String>, "__right__"),
                );
                let udf_expr = datafusion::logical_expr::Expr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction {
                        func: udf.clone(),
                        args: vec![df_left, df_right],
                    },
                );
                let left_dt = left_type
                    .clone()
                    .unwrap_or(arrow_schema::DataType::LargeBinary);
                let right_dt = right_type
                    .clone()
                    .unwrap_or(arrow_schema::DataType::LargeBinary);
                let tmp_schema = arrow_schema::Schema::new(vec![
                    Arc::new(arrow_schema::Field::new("__left__", left_dt, true)),
                    Arc::new(arrow_schema::Field::new("__right__", right_dt, true)),
                ]);
                let df_schema = datafusion::common::DFSchema::try_from(tmp_schema.clone())?;
                let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
                let udf_phy = planner
                    .create_physical_expr(&udf_expr, &df_schema, self.state)
                    .map_err(|e| anyhow!("Failed to create JSONB list concat expr: {}", e))?;
                let udf_phy = udf_phy
                    .with_new_children(vec![left, right])
                    .map_err(|e| anyhow!("Failed to rebind JSONB list concat children: {}", e))?;
                return Ok(udf_phy);
            }

            // Arithmetic with JSONB: decode LargeBinary side to match the typed side
            let left_is_lb = left_type
                .as_ref()
                .is_some_and(|t| *t == arrow_schema::DataType::LargeBinary);
            let right_is_lb = right_type
                .as_ref()
                .is_some_and(|t| *t == arrow_schema::DataType::LargeBinary);
            if left_is_lb != right_is_lb {
                // One side is LB, other is typed — decode LB to match
                let target = if left_is_lb {
                    right_type.as_ref()
                } else {
                    left_type.as_ref()
                };
                if let Some(target_dt) = target {
                    let decode_udf_name = match target_dt {
                        arrow_schema::DataType::Int8
                        | arrow_schema::DataType::Int16
                        | arrow_schema::DataType::Int32
                        | arrow_schema::DataType::Int64
                        | arrow_schema::DataType::UInt8
                        | arrow_schema::DataType::UInt16
                        | arrow_schema::DataType::UInt32
                        | arrow_schema::DataType::UInt64 => Some("_jsonb_to_int64"),
                        arrow_schema::DataType::Float16
                        | arrow_schema::DataType::Float32
                        | arrow_schema::DataType::Float64 => Some("_jsonb_to_float64"),
                        arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8 => {
                            Some("_jsonb_to_utf8")
                        }
                        arrow_schema::DataType::Boolean => Some("_jsonb_to_bool"),
                        _ => None,
                    };
                    if let Some(udf_name) = decode_udf_name
                        && let Some(udf) = self.state.scalar_functions().get(udf_name)
                    {
                        let decode_side =
                            |phy_expr: Arc<dyn PhysicalExpr>,
                             is_lb: bool|
                             -> Result<Arc<dyn PhysicalExpr>> {
                                if !is_lb {
                                    return Ok(phy_expr);
                                }
                                let dummy_col = datafusion::logical_expr::Expr::Column(
                                    datafusion::common::Column::new(None::<String>, "__lb__"),
                                );
                                let udf_expr = datafusion::logical_expr::Expr::ScalarFunction(
                                    datafusion::logical_expr::expr::ScalarFunction {
                                        func: udf.clone(),
                                        args: vec![dummy_col],
                                    },
                                );
                                let tmp_schema = arrow_schema::Schema::new(vec![Arc::new(
                                    arrow_schema::Field::new(
                                        "__lb__",
                                        arrow_schema::DataType::LargeBinary,
                                        true,
                                    ),
                                )]);
                                let df_schema =
                                    datafusion::common::DFSchema::try_from(tmp_schema.clone())?;
                                let planner =
                                    datafusion::physical_planner::DefaultPhysicalPlanner::default();
                                let udf_phy = planner
                                    .create_physical_expr(&udf_expr, &df_schema, self.state)
                                    .map_err(|e| {
                                        anyhow!("Failed to create JSONB decode expr: {}", e)
                                    })?;
                                udf_phy.with_new_children(vec![phy_expr]).map_err(|e| {
                                    anyhow!("Failed to rebind JSONB decode children: {}", e)
                                })
                            };

                        let decoded_left = decode_side(left, left_is_lb)?;
                        let decoded_right = decode_side(right, right_is_lb)?;
                        return binary(decoded_left, df_op, decoded_right, input_schema).map_err(
                            |e| {
                                anyhow!(
                                    "Failed to create binary expression after JSONB decode: {}",
                                    e
                                )
                            },
                        );
                    }
                }
            }
        }

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
        self.field_idx == other.field_idx
            && self.input.eq(&other.input)
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

// ---------------------------------------------------------------------------
// EXISTS subquery physical expression
// ---------------------------------------------------------------------------

/// Physical expression that evaluates an EXISTS subquery per row.
///
/// For each input row, plans and executes the subquery with the row's columns
/// injected as parameters. Returns `true` if the subquery produces any rows.
///
/// NOT EXISTS is handled by the caller wrapping this in a NOT expression.
/// Nested EXISTS works because `execute_subplan` creates a full planner that
/// handles nested EXISTS recursively.
struct ExistsExecExpr {
    query: Query,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<SessionContext>>,
    storage: Arc<StorageManager>,
    uni_schema: Arc<UniSchema>,
    params: HashMap<String, Value>,
}

impl std::fmt::Debug for ExistsExecExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExistsExecExpr").finish_non_exhaustive()
    }
}

impl ExistsExecExpr {
    fn new(
        query: Query,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        uni_schema: Arc<UniSchema>,
        params: HashMap<String, Value>,
    ) -> Self {
        Self {
            query,
            graph_ctx,
            session_ctx,
            storage,
            uni_schema,
            params,
        }
    }
}

impl std::fmt::Display for ExistsExecExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EXISTS(<subquery>)")
    }
}

impl PartialEq<dyn PhysicalExpr> for ExistsExecExpr {
    fn eq(&self, _other: &dyn PhysicalExpr) -> bool {
        false
    }
}

impl PartialEq for ExistsExecExpr {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Eq for ExistsExecExpr {}

impl std::hash::Hash for ExistsExecExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "ExistsExecExpr".hash(state);
    }
}

impl DisplayAs for ExistsExecExpr {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PhysicalExpr for ExistsExecExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &Schema,
    ) -> datafusion::error::Result<arrow_schema::DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        let num_rows = batch.num_rows();
        let mut builder = BooleanBuilder::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let row_params = extract_row_params(batch, row_idx);
            let mut sub_params = self.params.clone();
            sub_params.extend(row_params.clone());

            // Collect variable names in scope for the subquery planner
            let vars_in_scope: Vec<String> = row_params.keys().cloned().collect();

            // Plan the subquery with correlated variables in scope
            let planner = QueryPlanner::new(self.uni_schema.clone());
            let logical_plan = match planner.plan_with_scope(self.query.clone(), vars_in_scope) {
                Ok(plan) => plan,
                Err(_) => {
                    builder.append_value(false);
                    continue;
                }
            };

            // Execute the subquery synchronously (we're inside a sync evaluate call).
            // Spawn the async work on a dedicated thread with its own runtime to
            // avoid conflicts with both single-threaded and multi-threaded runtimes.
            let graph_ctx = self.graph_ctx.clone();
            let session_ctx = self.session_ctx.clone();
            let storage = self.storage.clone();
            let uni_schema = self.uni_schema.clone();

            let result = std::thread::scope(|s| {
                s.spawn(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Failed to create runtime for EXISTS: {}",
                                e
                            ))
                        })?;
                    rt.block_on(execute_subplan(
                        &logical_plan,
                        &sub_params,
                        &graph_ctx,
                        &session_ctx,
                        &storage,
                        &uni_schema,
                    ))
                })
                .join()
                .unwrap_or_else(|_| {
                    Err(datafusion::error::DataFusionError::Execution(
                        "EXISTS subquery thread panicked".to_string(),
                    ))
                })
            });

            match result {
                Ok(batches) => {
                    let has_rows = batches.iter().any(|b| b.num_rows() > 0);
                    builder.append_value(has_rows);
                }
                Err(_) => {
                    builder.append_value(false);
                }
            }
        }

        Ok(datafusion::physical_plan::ColumnarValue::Array(Arc::new(
            builder.finish(),
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "ExistsExecExpr has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}
