// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::pushdown::PredicateAnalyzer;
use crate::query::{AGGREGATE_WINDOW_FUNCTIONS, MANUAL_WINDOW_FUNCTIONS};
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::schema::{
    DistanceMetric, EmbeddingConfig, EmbeddingModel, FullTextIndexConfig, IndexDefinition,
    JsonFtsIndexConfig, ScalarIndexConfig, ScalarIndexType, Schema, TokenizerConfig,
    VectorIndexConfig, VectorIndexType,
};
use uni_cypher::ast::{
    AlterEdgeType, AlterLabel, BinaryOp, CallKind, Clause, CreateConstraint, CreateEdgeType,
    CreateLabel, CypherLiteral, Direction, DropConstraint, DropEdgeType, DropLabel, Expr,
    MatchClause, NodePattern, PathPattern, Pattern, PatternElement, Query, RelationshipPattern,
    RemoveItem, ReturnClause, ReturnItem, SchemaCommand, SetClause, SetItem, ShortestPathMode,
    ShowConstraints, SortItem, Statement, WindowSpec, WithClause, WithRecursiveClause,
};

/// Type of variable in scope for semantic validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariableType {
    /// Node variable (from MATCH (n), CREATE (n), etc.)
    Node,
    /// Edge/relationship variable (from MATCH ()-[r]->(), etc.)
    Edge,
    /// Path variable (from MATCH p = (a)-[*]->(b), etc.)
    Path,
    /// Scalar variable (from WITH expr AS x, UNWIND list AS item, etc.)
    Scalar,
    /// Imported from outer scope with unknown type (from plan_with_scope string vars).
    /// Compatible with any concrete type — allows subqueries to re-bind the variable.
    Imported,
}

impl VariableType {
    /// Returns true if this type is compatible with the expected type.
    ///
    /// `Imported` is always compatible because the actual type is unknown at plan time.
    fn is_compatible_with(self, expected: VariableType) -> bool {
        self == expected || self == VariableType::Imported
    }
}

/// Information about a variable in scope.
#[derive(Debug, Clone)]
pub struct VariableInfo {
    pub name: String,
    pub var_type: VariableType,
}

impl VariableInfo {
    pub fn new(name: String, var_type: VariableType) -> Self {
        Self { name, var_type }
    }
}

/// Find a variable in scope by name.
fn find_var_in_scope<'a>(vars: &'a [VariableInfo], name: &str) -> Option<&'a VariableInfo> {
    vars.iter().find(|v| v.name == name)
}

/// Check if a variable is in scope.
fn is_var_in_scope(vars: &[VariableInfo], name: &str) -> bool {
    vars.iter().any(|v| v.name == name)
}

/// Add a variable to scope with type conflict validation.
/// Returns an error if the variable already exists with a different type.
fn add_var_to_scope(
    vars: &mut Vec<VariableInfo>,
    name: &str,
    var_type: VariableType,
) -> Result<()> {
    if name.is_empty() {
        return Ok(());
    }

    if let Some(existing) = vars.iter_mut().find(|v| v.name == name) {
        if existing.var_type == VariableType::Imported {
            // Imported vars upgrade to the concrete type
            existing.var_type = var_type;
        } else if var_type == VariableType::Imported || existing.var_type == var_type {
            // New type is Imported (keep existing) or same type — no conflict
        } else {
            return Err(anyhow!(
                "SyntaxError: VariableTypeConflict - Variable '{}' already defined as {:?}, cannot use as {:?}",
                name,
                existing.var_type,
                var_type
            ));
        }
    } else {
        vars.push(VariableInfo::new(name.to_string(), var_type));
    }
    Ok(())
}

/// Convert VariableInfo vec to String vec for backward compatibility
fn vars_to_strings(vars: &[VariableInfo]) -> Vec<String> {
    vars.iter().map(|v| v.name.clone()).collect()
}

/// Collect all variable names referenced in an expression
fn collect_expr_variables(expr: &Expr) -> Vec<String> {
    let mut vars = Vec::new();
    collect_expr_variables_inner(expr, &mut vars);
    vars
}

fn collect_expr_variables_inner(expr: &Expr, vars: &mut Vec<String>) {
    match expr {
        Expr::Variable(name) => {
            if !vars.contains(name) {
                vars.push(name.clone());
            }
        }
        Expr::Property(base, _) => {
            collect_expr_variables_inner(base, vars);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_variables_inner(left, vars);
            collect_expr_variables_inner(right, vars);
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsUnique(e) => {
            collect_expr_variables_inner(e, vars);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_variables_inner(arg, vars);
            }
        }
        Expr::List(items) => {
            for item in items {
                collect_expr_variables_inner(item, vars);
            }
        }
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = case_expr {
                collect_expr_variables_inner(e, vars);
            }
            for (w, t) in when_then {
                collect_expr_variables_inner(w, vars);
                collect_expr_variables_inner(t, vars);
            }
            if let Some(e) = else_expr {
                collect_expr_variables_inner(e, vars);
            }
        }
        Expr::Map(entries) => {
            for (_, v) in entries {
                collect_expr_variables_inner(v, vars);
            }
        }
        // Literals and other non-variable expressions
        _ => {}
    }
}

/// Validate function call argument types.
/// Returns error if type constraints are violated.
fn validate_function_call(name: &str, args: &[Expr], vars_in_scope: &[VariableInfo]) -> Result<()> {
    let name_lower = name.to_lowercase();

    // labels() requires Node
    if name_lower == "labels"
        && let Some(Expr::Variable(var_name)) = args.first()
        && let Some(info) = find_var_in_scope(vars_in_scope, var_name)
        && !info.var_type.is_compatible_with(VariableType::Node)
    {
        return Err(anyhow!(
            "SyntaxError: InvalidArgumentType - labels() requires a node argument"
        ));
    }

    // type() requires Edge
    if name_lower == "type"
        && let Some(Expr::Variable(var_name)) = args.first()
        && let Some(info) = find_var_in_scope(vars_in_scope, var_name)
        && !info.var_type.is_compatible_with(VariableType::Edge)
    {
        return Err(anyhow!(
            "SyntaxError: InvalidArgumentType - type() requires a relationship argument"
        ));
    }

    // properties() requires Node/Edge/Map (not scalar literals)
    if name_lower == "properties"
        && let Some(arg) = args.first()
    {
        match arg {
            Expr::Literal(CypherLiteral::Integer(_))
            | Expr::Literal(CypherLiteral::Float(_))
            | Expr::Literal(CypherLiteral::String(_))
            | Expr::Literal(CypherLiteral::Bool(_)) => {
                return Err(anyhow!(
                    "SyntaxError: InvalidArgumentType - properties() requires a node, relationship, or map"
                ));
            }
            Expr::Variable(var_name) => {
                if let Some(info) = find_var_in_scope(vars_in_scope, var_name)
                    && info.var_type == VariableType::Scalar
                {
                    return Err(anyhow!(
                        "SyntaxError: InvalidArgumentType - properties() requires a node, relationship, or map"
                    ));
                }
            }
            _ => {}
        }
    }

    // nodes()/relationships() require Path
    if (name_lower == "nodes" || name_lower == "relationships")
        && let Some(Expr::Variable(var_name)) = args.first()
        && let Some(info) = find_var_in_scope(vars_in_scope, var_name)
        && !info.var_type.is_compatible_with(VariableType::Path)
    {
        return Err(anyhow!(
            "SyntaxError: InvalidArgumentType - {}() requires a path argument",
            name_lower
        ));
    }

    Ok(())
}

/// Check if an expression is a non-boolean literal.
fn is_non_boolean_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(CypherLiteral::Integer(_))
            | Expr::Literal(CypherLiteral::Float(_))
            | Expr::Literal(CypherLiteral::String(_))
            | Expr::List(_)
    )
}

/// Validate boolean expressions (AND/OR/NOT require boolean arguments).
fn validate_boolean_expression(expr: &Expr) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, BinaryOp::And | BinaryOp::Or | BinaryOp::Xor) {
                let op_name = format!("{:?}", op).to_uppercase();
                for operand in [left.as_ref(), right.as_ref()] {
                    if is_non_boolean_literal(operand) {
                        return Err(anyhow!(
                            "SyntaxError: InvalidArgumentType - {} requires boolean arguments",
                            op_name
                        ));
                    }
                }
            }
            validate_boolean_expression(left)?;
            validate_boolean_expression(right)?;
        }
        Expr::UnaryOp {
            op: uni_cypher::ast::UnaryOp::Not,
            expr: inner,
        } => {
            if is_non_boolean_literal(inner) {
                return Err(anyhow!(
                    "SyntaxError: InvalidArgumentType - NOT requires a boolean argument"
                ));
            }
            validate_boolean_expression(inner)?;
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_boolean_expression(arg)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Validate that all variables used in an expression are in scope.
fn validate_expression_variables(expr: &Expr, vars_in_scope: &[VariableInfo]) -> Result<()> {
    let used_vars = collect_expr_variables(expr);
    for var in used_vars {
        if !is_var_in_scope(vars_in_scope, &var) {
            return Err(anyhow!(
                "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                var
            ));
        }
    }
    Ok(())
}

/// Check if an expression contains any aggregate function (recursively).
fn contains_aggregate_recursive(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            let is_agg = matches!(
                name.to_lowercase().as_str(),
                "count" | "sum" | "avg" | "min" | "max" | "collect" | "stdev" | "stdevp"
            );
            is_agg || args.iter().any(contains_aggregate_recursive)
        }
        Expr::BinaryOp { left, right, .. } => {
            contains_aggregate_recursive(left) || contains_aggregate_recursive(right)
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsUnique(e) => contains_aggregate_recursive(e),
        Expr::List(items) => items.iter().any(contains_aggregate_recursive),
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            expr.as_deref().is_some_and(contains_aggregate_recursive)
                || when_then.iter().any(|(w, t)| {
                    contains_aggregate_recursive(w) || contains_aggregate_recursive(t)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(contains_aggregate_recursive)
        }
        Expr::In { expr, list } => {
            contains_aggregate_recursive(expr) || contains_aggregate_recursive(list)
        }
        Expr::Property(base, _) => contains_aggregate_recursive(base),
        _ => false,
    }
}

/// Validate that no aggregation functions appear in WHERE clause.
fn validate_no_aggregation_in_where(predicate: &Expr) -> Result<()> {
    if contains_aggregate_recursive(predicate) {
        return Err(anyhow!(
            "SyntaxError: InvalidAggregation - Aggregation functions not allowed in WHERE"
        ));
    }
    Ok(())
}

/// Parse and validate a non-negative integer expression for SKIP or LIMIT.
/// Returns `Ok(Some(n))` for valid integers, or an error for negative/float/other values.
fn parse_non_negative_integer(expr: &Expr, clause_name: &str) -> Result<Option<usize>> {
    match expr {
        Expr::Literal(CypherLiteral::Integer(n)) => {
            if *n < 0 {
                return Err(anyhow!(
                    "SyntaxError: NegativeIntegerArgument - {} requires non-negative integer",
                    clause_name
                ));
            }
            Ok(Some(*n as usize))
        }
        Expr::Literal(CypherLiteral::Float(_)) => Err(anyhow!(
            "SyntaxError: InvalidArgumentType - {} requires integer, got float",
            clause_name
        )),
        _ => Err(anyhow!(
            "SyntaxError: InvalidArgumentType - {} must be an integer literal",
            clause_name
        )),
    }
}

/// Validate that aggregation functions are not nested.
fn validate_no_nested_aggregation(expr: &Expr) -> Result<()> {
    if let Expr::FunctionCall { name, args, .. } = expr
        && matches!(
            name.to_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "collect" | "stdev" | "stdevp"
        )
    {
        for arg in args {
            if contains_aggregate_recursive(arg) {
                return Err(anyhow!(
                    "SyntaxError: NestedAggregation - Cannot nest aggregation functions"
                ));
            }
        }
    }
    // Recurse into nested expressions
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            validate_no_nested_aggregation(left)?;
            validate_no_nested_aggregation(right)?;
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_no_nested_aggregation(arg)?;
            }
        }
        Expr::List(items) => {
            for item in items {
                validate_no_nested_aggregation(item)?;
            }
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = expr {
                validate_no_nested_aggregation(e)?;
            }
            for (w, t) in when_then {
                validate_no_nested_aggregation(w)?;
                validate_no_nested_aggregation(t)?;
            }
            if let Some(e) = else_expr {
                validate_no_nested_aggregation(e)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Validate that all variables referenced in properties are defined,
/// either in scope or in the local CREATE variable list.
fn validate_property_variables(
    properties: &Option<Expr>,
    vars_in_scope: &[VariableInfo],
    create_vars: &[&str],
) -> Result<()> {
    if let Some(props) = properties {
        for var in collect_expr_variables(props) {
            if !is_var_in_scope(vars_in_scope, &var) && !create_vars.contains(&var.as_str()) {
                return Err(anyhow!(
                    "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                    var
                ));
            }
        }
    }
    Ok(())
}

/// Check that a variable name is not already bound in scope or in the local CREATE list.
/// Used to prevent rebinding in CREATE clauses.
fn check_not_already_bound(
    name: &str,
    vars_in_scope: &[VariableInfo],
    create_vars: &[&str],
) -> Result<()> {
    if is_var_in_scope(vars_in_scope, name) {
        return Err(anyhow!(
            "SyntaxError: VariableAlreadyBound - Variable '{}' already defined",
            name
        ));
    }
    if create_vars.contains(&name) {
        return Err(anyhow!(
            "SyntaxError: VariableAlreadyBound - Variable '{}' already defined in CREATE",
            name
        ));
    }
    Ok(())
}

/// Recursively validate an expression for type errors, undefined variables, etc.
fn validate_expression(expr: &Expr, vars_in_scope: &[VariableInfo]) -> Result<()> {
    // Validate function calls
    if let Expr::FunctionCall { name, args, .. } = expr {
        validate_function_call(name, args, vars_in_scope)?;
        // Recurse into arguments
        for arg in args {
            validate_expression(arg, vars_in_scope)?;
        }
    }

    // Validate boolean operators
    validate_boolean_expression(expr)?;

    // Validate no nested aggregation
    validate_no_nested_aggregation(expr)?;

    // Recurse into sub-expressions
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            validate_expression(left, vars_in_scope)?;
            validate_expression(right, vars_in_scope)?;
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsUnique(e) => {
            validate_expression(e, vars_in_scope)?;
        }
        Expr::Property(base, prop) => {
            // Check if the base is a path variable - paths don't have properties
            if let Expr::Variable(var_name) = base.as_ref()
                && let Some(var_info) = find_var_in_scope(vars_in_scope, var_name)
                && var_info.var_type == VariableType::Path
            {
                return Err(anyhow!(
                    "SyntaxError: InvalidArgumentType - Type mismatch: expected Node or Relationship but was Path for property access '{}.{}'",
                    var_name,
                    prop
                ));
            }
            validate_expression(base, vars_in_scope)?;
        }
        Expr::List(items) => {
            for item in items {
                validate_expression(item, vars_in_scope)?;
            }
        }
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = case_expr {
                validate_expression(e, vars_in_scope)?;
            }
            for (w, t) in when_then {
                validate_expression(w, vars_in_scope)?;
                validate_expression(t, vars_in_scope)?;
            }
            if let Some(e) = else_expr {
                validate_expression(e, vars_in_scope)?;
            }
        }
        Expr::In { expr, list } => {
            validate_expression(expr, vars_in_scope)?;
            validate_expression(list, vars_in_scope)?;
        }
        _ => {}
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
    },
    Scan {
        label_id: u16,
        labels: Vec<String>,
        variable: String,
        filter: Option<Expr>,
        optional: bool,
    },
    /// Lookup vertices by ext_id using the main vertices table.
    /// Used when a query references ext_id without specifying a label.
    ExtIdLookup {
        variable: String,
        ext_id: String,
        filter: Option<Expr>,
        optional: bool,
    },
    /// Scan all vertices from main table (MATCH (n) without label).
    /// Used for schemaless queries that don't specify any label.
    ScanAll {
        variable: String,
        filter: Option<Expr>,
        optional: bool,
    },
    /// Scan main table filtering by label name (MATCH (n:Unknown)).
    /// Used for labels not defined in schema (schemaless support).
    ScanMainByLabel {
        label_name: String,
        variable: String,
        filter: Option<Expr>,
        optional: bool,
    },
    LoadCsv {
        url: String,
        variable: String,
        with_headers: bool,
        field_terminator: Option<char>,
    },
    Empty, // Produces 1 empty row
    Unwind {
        input: Box<LogicalPlan>,
        expr: Expr,
        variable: String,
    },
    Traverse {
        input: Box<LogicalPlan>,
        edge_type_ids: Vec<u32>,
        direction: Direction,
        source_variable: String,
        target_variable: String,
        target_label_id: u16,
        step_variable: Option<String>,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        target_filter: Option<Expr>,
        path_variable: Option<String>,
        edge_properties: std::collections::HashSet<String>,
        /// All variables from this OPTIONAL MATCH pattern.
        /// When any hop in the pattern fails, ALL these variables should be set to NULL.
        /// This ensures proper multi-hop OPTIONAL MATCH semantics.
        optional_pattern_vars: std::collections::HashSet<String>,
    },
    /// Traverse main edges table filtering by type name (MATCH (a)-[:Unknown]->(b)).
    /// Used for edge types not defined in schema (schemaless support).
    TraverseMainByType {
        type_name: String,
        input: Box<LogicalPlan>,
        direction: Direction,
        source_variable: String,
        target_variable: String,
        step_variable: Option<String>,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        target_filter: Option<Expr>,
        path_variable: Option<String>,
        /// All variables from this OPTIONAL MATCH pattern.
        /// When any hop in the pattern fails, ALL these variables should be set to NULL.
        optional_pattern_vars: std::collections::HashSet<String>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
        /// Variables from OPTIONAL MATCH that should preserve NULL rows.
        /// When evaluating the filter, if any of these variables are NULL,
        /// the row is preserved regardless of the predicate result.
        optional_variables: std::collections::HashSet<String>,
    },
    Create {
        input: Box<LogicalPlan>,
        pattern: Pattern,
    },
    /// Batched CREATE operations for multiple consecutive CREATE clauses.
    ///
    /// This variant combines multiple CREATE patterns into a single plan node
    /// to avoid deep recursion when executing many CREATEs sequentially.
    CreateBatch {
        input: Box<LogicalPlan>,
        patterns: Vec<Pattern>,
    },
    Merge {
        input: Box<LogicalPlan>,
        pattern: Pattern,
        on_match: Option<SetClause>,
        on_create: Option<SetClause>,
    },
    Set {
        input: Box<LogicalPlan>,
        items: Vec<SetItem>,
    },
    Remove {
        input: Box<LogicalPlan>,
        items: Vec<RemoveItem>,
    },
    Delete {
        input: Box<LogicalPlan>,
        items: Vec<Expr>,
        detach: bool,
    },
    /// FOREACH (variable IN list | clauses)
    Foreach {
        input: Box<LogicalPlan>,
        variable: String,
        list: Expr,
        body: Vec<LogicalPlan>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<SortItem>,
    },
    Limit {
        input: Box<LogicalPlan>,
        skip: Option<usize>,
        fetch: Option<usize>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    Window {
        input: Box<LogicalPlan>,
        window_exprs: Vec<Expr>,
    },
    Project {
        input: Box<LogicalPlan>,
        projections: Vec<(Expr, Option<String>)>,
    },
    CrossJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    Apply {
        input: Box<LogicalPlan>,
        subquery: Box<LogicalPlan>,
        input_filter: Option<Expr>,
    },
    RecursiveCTE {
        cte_name: String,
        initial: Box<LogicalPlan>,
        recursive: Box<LogicalPlan>,
    },
    ProcedureCall {
        procedure_name: String,
        arguments: Vec<Expr>,
        yield_items: Vec<(String, Option<String>)>,
    },
    SubqueryCall {
        input: Box<LogicalPlan>,
        subquery: Box<LogicalPlan>,
    },
    VectorKnn {
        label_id: u16,
        variable: String,
        property: String,
        query: Expr,
        k: usize,
        threshold: Option<f32>,
    },
    InvertedIndexLookup {
        label_id: u16,
        variable: String,
        property: String,
        terms: Expr,
    },
    ShortestPath {
        input: Box<LogicalPlan>,
        edge_type_ids: Vec<u32>,
        direction: Direction,
        source_variable: String,
        target_variable: String,
        target_label_id: u16,
        path_variable: String,
        /// Minimum number of hops (edges) in the path. Default is 1.
        min_hops: u32,
        /// Maximum number of hops (edges) in the path. Default is u32::MAX (unlimited).
        max_hops: u32,
    },
    /// allShortestPaths() - Returns all paths with minimum length
    AllShortestPaths {
        input: Box<LogicalPlan>,
        edge_type_ids: Vec<u32>,
        direction: Direction,
        source_variable: String,
        target_variable: String,
        target_label_id: u16,
        path_variable: String,
        /// Minimum number of hops (edges) in the path. Default is 1.
        min_hops: u32,
        /// Maximum number of hops (edges) in the path. Default is u32::MAX (unlimited).
        max_hops: u32,
    },
    QuantifiedPattern {
        input: Box<LogicalPlan>,
        pattern_plan: Box<LogicalPlan>, // Plan for one iteration
        min_iterations: u32,
        max_iterations: u32,
        path_variable: Option<String>,
        start_variable: String, // Input variable for iteration (e.g. 'a' in (a)-[:R]->(b))
        binding_variable: String, // Output variable of iteration (e.g. 'b')
    },
    // DDL Plans
    CreateVectorIndex {
        config: VectorIndexConfig,
        if_not_exists: bool,
    },
    CreateFullTextIndex {
        config: FullTextIndexConfig,
        if_not_exists: bool,
    },
    CreateScalarIndex {
        config: ScalarIndexConfig,
        if_not_exists: bool,
    },
    CreateJsonFtsIndex {
        config: JsonFtsIndexConfig,
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    ShowIndexes {
        filter: Option<String>,
    },
    Copy {
        target: String,
        source: String,
        is_export: bool,
        options: HashMap<String, Value>,
    },
    Backup {
        destination: String,
        options: HashMap<String, Value>,
    },
    Explain {
        plan: Box<LogicalPlan>,
    },
    // Admin Plans
    ShowDatabase,
    ShowConfig,
    ShowStatistics,
    Vacuum,
    Checkpoint,
    CopyTo {
        label: String,
        path: String,
        format: String,
        options: HashMap<String, Value>,
    },
    CopyFrom {
        label: String,
        path: String,
        format: String,
        options: HashMap<String, Value>,
    },
    // Schema DDL
    CreateLabel(CreateLabel),
    CreateEdgeType(CreateEdgeType),
    AlterLabel(AlterLabel),
    AlterEdgeType(AlterEdgeType),
    DropLabel(DropLabel),
    DropEdgeType(DropEdgeType),
    // Constraints
    CreateConstraint(CreateConstraint),
    DropConstraint(DropConstraint),
    ShowConstraints(ShowConstraints),
    // Transaction Plans
    Begin,
    Commit,
    Rollback,
}

/// Result of extracting ANY IN predicate
struct AnyInExtraction {
    predicate: AnyInPredicate,
    residual: Option<Expr>,
}

struct AnyInPredicate {
    variable: String,
    property: String,
    terms: Expr,
}

fn extract_any_in_predicate(expr: &Expr) -> Option<AnyInExtraction> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, BinaryOp::And) {
                if let Some(mut extraction) = extract_any_in_predicate(left) {
                    extraction.residual = Some(combine_with_and(
                        extraction.residual,
                        right.as_ref().clone(),
                    ));
                    return Some(extraction);
                }
                if let Some(mut extraction) = extract_any_in_predicate(right) {
                    extraction.residual =
                        Some(combine_with_and(extraction.residual, left.as_ref().clone()));
                    return Some(extraction);
                }
                return None;
            }
            // Check direct match
            if let Some(pred) = extract_simple_any_in(expr) {
                return Some(AnyInExtraction {
                    predicate: pred,
                    residual: None,
                });
            }
            None
        }
        _ => {
            if let Some(pred) = extract_simple_any_in(expr) {
                return Some(AnyInExtraction {
                    predicate: pred,
                    residual: None,
                });
            }
            None
        }
    }
}

fn extract_simple_any_in(expr: &Expr) -> Option<AnyInPredicate> {
    // List comprehensions are not supported, so this optimization cannot be applied
    // TODO: Re-enable when list comprehensions are re-implemented
    let _ = expr; // Suppress unused parameter warning
    None
}

/// Extracted vector similarity predicate info for optimization
struct VectorSimilarityPredicate {
    variable: String,
    property: String,
    query: Expr,
    threshold: Option<f32>,
}

/// Result of extracting vector_similarity from a predicate
struct VectorSimilarityExtraction {
    /// The extracted vector similarity predicate
    predicate: VectorSimilarityPredicate,
    /// Remaining predicates that couldn't be optimized (if any)
    residual: Option<Expr>,
}

/// Try to extract a vector_similarity predicate from an expression.
/// Matches patterns like:
/// - vector_similarity(n.embedding, [1,2,3]) > 0.8
/// - n.embedding ~= $query
///
/// Also handles AND predicates.
fn extract_vector_similarity(expr: &Expr) -> Option<VectorSimilarityExtraction> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Handle AND: check both sides for vector_similarity
            if matches!(op, BinaryOp::And) {
                // Try left side first
                if let Some(vs) = extract_simple_vector_similarity(left) {
                    return Some(VectorSimilarityExtraction {
                        predicate: vs,
                        residual: Some(right.as_ref().clone()),
                    });
                }
                // Try right side
                if let Some(vs) = extract_simple_vector_similarity(right) {
                    return Some(VectorSimilarityExtraction {
                        predicate: vs,
                        residual: Some(left.as_ref().clone()),
                    });
                }
                // Recursively check within left/right for nested ANDs
                if let Some(mut extraction) = extract_vector_similarity(left) {
                    extraction.residual = Some(combine_with_and(
                        extraction.residual,
                        right.as_ref().clone(),
                    ));
                    return Some(extraction);
                }
                if let Some(mut extraction) = extract_vector_similarity(right) {
                    extraction.residual =
                        Some(combine_with_and(extraction.residual, left.as_ref().clone()));
                    return Some(extraction);
                }
                return None;
            }

            // Simple case: direct vector_similarity comparison
            if let Some(vs) = extract_simple_vector_similarity(expr) {
                return Some(VectorSimilarityExtraction {
                    predicate: vs,
                    residual: None,
                });
            }
            None
        }
        _ => None,
    }
}

/// Helper to combine an optional expression with another using AND
fn combine_with_and(opt_expr: Option<Expr>, other: Expr) -> Expr {
    match opt_expr {
        Some(e) => Expr::BinaryOp {
            left: Box::new(e),
            op: BinaryOp::And,
            right: Box::new(other),
        },
        None => other,
    }
}

/// Extract a simple vector_similarity comparison (no AND)
fn extract_simple_vector_similarity(expr: &Expr) -> Option<VectorSimilarityPredicate> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Pattern: vector_similarity(...) > threshold or vector_similarity(...) >= threshold
            if matches!(op, BinaryOp::Gt | BinaryOp::GtEq)
                && let (Some(vs), Some(thresh)) = (
                    extract_vector_similarity_call(left),
                    extract_float_literal(right),
                )
            {
                return Some(VectorSimilarityPredicate {
                    variable: vs.0,
                    property: vs.1,
                    query: vs.2,
                    threshold: Some(thresh),
                });
            }
            // Pattern: threshold < vector_similarity(...) or threshold <= vector_similarity(...)
            if matches!(op, BinaryOp::Lt | BinaryOp::LtEq)
                && let (Some(thresh), Some(vs)) = (
                    extract_float_literal(left),
                    extract_vector_similarity_call(right),
                )
            {
                return Some(VectorSimilarityPredicate {
                    variable: vs.0,
                    property: vs.1,
                    query: vs.2,
                    threshold: Some(thresh),
                });
            }
            // Pattern: n.embedding ~= query
            if matches!(op, BinaryOp::ApproxEq)
                && let Expr::Property(var_expr, prop) = left.as_ref()
                && let Expr::Variable(var) = var_expr.as_ref()
            {
                return Some(VectorSimilarityPredicate {
                    variable: var.clone(),
                    property: prop.clone(),
                    query: right.as_ref().clone(),
                    threshold: None,
                });
            }
            None
        }
        _ => None,
    }
}

/// Extract (variable, property, query_expr) from vector_similarity(n.prop, query)
fn extract_vector_similarity_call(expr: &Expr) -> Option<(String, String, Expr)> {
    if let Expr::FunctionCall { name, args, .. } = expr
        && name.eq_ignore_ascii_case("vector_similarity")
        && args.len() == 2
    {
        // First arg should be Property(Identifier(var), prop)
        if let Expr::Property(var_expr, prop) = &args[0]
            && let Expr::Variable(var) = var_expr.as_ref()
        {
            // Second arg is query
            return Some((var.clone(), prop.clone(), args[1].clone()));
        }
    }
    None
}

/// Extract a float value from a literal expression
fn extract_float_literal(expr: &Expr) -> Option<f32> {
    match expr {
        Expr::Literal(CypherLiteral::Integer(i)) => Some(*i as f32),
        Expr::Literal(CypherLiteral::Float(f)) => Some(*f as f32),
        _ => None,
    }
}

pub struct QueryPlanner {
    schema: Arc<Schema>,
    /// Cache of parsed generation expressions, keyed by (label_name, gen_col_name).
    gen_expr_cache: std::collections::HashMap<(String, String), Expr>,
    /// Counter for generating unique anonymous variable names.
    anon_counter: std::cell::Cell<usize>,
}

struct TraverseParams<'a> {
    rel: &'a RelationshipPattern,
    target_node: &'a NodePattern,
    _source_part: &'a PatternElement,
    optional: bool,
    path_variable: Option<String>,
    /// All variables from this OPTIONAL MATCH pattern.
    /// Used to ensure multi-hop patterns correctly NULL all vars when any hop fails.
    optional_pattern_vars: std::collections::HashSet<String>,
}

impl QueryPlanner {
    pub fn new(schema: Arc<Schema>) -> Self {
        // Pre-parse all generation expressions for caching
        let mut gen_expr_cache = std::collections::HashMap::new();
        for (label, props) in &schema.properties {
            for (gen_col, meta) in props {
                if let Some(expr_str) = &meta.generation_expression
                    && let Ok(parsed_expr) = uni_cypher::parse_expression(expr_str)
                {
                    gen_expr_cache.insert((label.clone(), gen_col.clone()), parsed_expr);
                }
            }
        }
        Self {
            schema,
            gen_expr_cache,
            anon_counter: std::cell::Cell::new(0),
        }
    }

    pub fn plan(&self, query: Query) -> Result<LogicalPlan> {
        self.plan_with_scope(query, Vec::new())
    }

    pub fn plan_with_scope(&self, query: Query, vars: Vec<String>) -> Result<LogicalPlan> {
        // Apply query rewrites before planning
        let rewritten_query = crate::query::rewrite::rewrite_query(query)?;

        match rewritten_query {
            Query::Single(stmt) => self.plan_single(stmt, vars),
            Query::Union { left, right, all } => {
                let l = self.plan_with_scope(*left, vars.clone())?;
                let r = self.plan_with_scope(*right, vars)?;

                // Validate that both sides have the same column names
                let left_cols = Self::extract_projection_columns(&l);
                let right_cols = Self::extract_projection_columns(&r);

                if left_cols != right_cols {
                    return Err(anyhow!(
                        "SyntaxError: DifferentColumnsInUnion - UNION queries must have same column names"
                    ));
                }

                Ok(LogicalPlan::Union {
                    left: Box::new(l),
                    right: Box::new(r),
                    all,
                })
            }
            Query::Schema(cmd) => self.plan_schema_command(*cmd),
            Query::Transaction(cmd) => self.plan_transaction_command(cmd),
            Query::Explain(inner) => {
                let inner_plan = self.plan_with_scope(*inner, vars)?;
                Ok(LogicalPlan::Explain {
                    plan: Box::new(inner_plan),
                })
            }
            Query::TimeTravel { .. } => {
                unreachable!("TimeTravel should be resolved at API layer before planning")
            }
        }
    }

    fn next_anon_var(&self) -> String {
        let id = self.anon_counter.get();
        self.anon_counter.set(id + 1);
        format!("_anon_{}", id)
    }

    /// Extract projection column names from a logical plan.
    /// Used for UNION column validation.
    fn extract_projection_columns(plan: &LogicalPlan) -> Vec<String> {
        match plan {
            LogicalPlan::Project { projections, .. } => projections
                .iter()
                .map(|(expr, alias)| alias.clone().unwrap_or_else(|| expr.to_string_repr()))
                .collect(),
            LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::Filter { input, .. } => Self::extract_projection_columns(input),
            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut cols: Vec<String> = group_by.iter().map(|e| e.to_string_repr()).collect();
                cols.extend(aggregates.iter().map(|e| e.to_string_repr()));
                cols
            }
            _ => Vec::new(),
        }
    }

    fn plan_return_clause(
        &self,
        return_clause: &ReturnClause,
        plan: LogicalPlan,
        vars_in_scope: &[VariableInfo],
    ) -> Result<LogicalPlan> {
        let mut plan = plan;
        let mut group_by = Vec::new();
        let mut aggregates = Vec::new();
        let mut has_agg = false;
        let mut projections = Vec::new();

        for item in &return_clause.items {
            match item {
                ReturnItem::All => {
                    // RETURN * - add all variables in scope
                    for v in vars_in_scope {
                        projections.push((Expr::Variable(v.name.clone()), Some(v.name.clone())));
                        if !group_by.contains(&Expr::Variable(v.name.clone())) {
                            group_by.push(Expr::Variable(v.name.clone()));
                        }
                    }
                }
                ReturnItem::Expr { expr, alias } => {
                    if matches!(expr, Expr::Wildcard) {
                        for v in vars_in_scope {
                            projections
                                .push((Expr::Variable(v.name.clone()), Some(v.name.clone())));
                            if !group_by.contains(&Expr::Variable(v.name.clone())) {
                                group_by.push(Expr::Variable(v.name.clone()));
                            }
                        }
                    } else {
                        // Validate expression variables are defined
                        validate_expression_variables(expr, vars_in_scope)?;
                        // Validate function argument types and boolean operators
                        validate_expression(expr, vars_in_scope)?;

                        projections.push((expr.clone(), alias.clone()));
                        if expr.is_aggregate() {
                            has_agg = true;
                            aggregates.push(expr.clone());
                        } else if !group_by.contains(expr) {
                            group_by.push(expr.clone());
                        }
                    }
                }
            }
        }

        if has_agg {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
            };
        }

        let mut window_exprs = Vec::new();
        for (expr, _) in &projections {
            Self::collect_window_functions(expr, &mut window_exprs);
        }

        if let Some(order_by) = &return_clause.order_by {
            for item in order_by {
                Self::collect_window_functions(&item.expr, &mut window_exprs);
            }
        }

        let has_window_exprs = !window_exprs.is_empty();

        if has_window_exprs {
            // Before creating the Window node, we need to ensure all properties
            // referenced by window functions are available. Create a Project node
            // that loads these properties.
            let mut props_needed_for_window: Vec<Expr> = Vec::new();
            for window_expr in &window_exprs {
                Self::collect_properties_from_expr(window_expr, &mut props_needed_for_window);
            }

            // Also include non-window expressions from projections that might be needed
            // Preserve qualified names (e.g., "e.salary") as aliases for properties
            let non_window_projections: Vec<_> = projections
                .iter()
                .filter_map(|(expr, alias)| {
                    // Keep expressions that don't have window_spec
                    let keep = if let Expr::FunctionCall { window_spec, .. } = expr {
                        window_spec.is_none()
                    } else {
                        true
                    };

                    if keep {
                        // For property references, use the qualified name as alias
                        let new_alias = if matches!(expr, Expr::Property(..)) {
                            Some(expr.to_string_repr())
                        } else {
                            alias.clone()
                        };
                        Some((expr.clone(), new_alias))
                    } else {
                        None
                    }
                })
                .collect();

            if !non_window_projections.is_empty() || !props_needed_for_window.is_empty() {
                let mut intermediate_projections = non_window_projections;
                // Add any additional property references needed by window functions
                // IMPORTANT: Preserve qualified names (e.g., "e.salary") as aliases so window functions can reference them
                for prop in &props_needed_for_window {
                    if !intermediate_projections
                        .iter()
                        .any(|(e, _)| e.to_string_repr() == prop.to_string_repr())
                    {
                        let qualified_name = prop.to_string_repr();
                        intermediate_projections.push((prop.clone(), Some(qualified_name)));
                    }
                }

                if !intermediate_projections.is_empty() {
                    plan = LogicalPlan::Project {
                        input: Box::new(plan),
                        projections: intermediate_projections,
                    };
                }
            }

            // Transform property expressions in window functions to use qualified variable names
            // so that e.dept becomes "e.dept" variable that can be looked up from the row HashMap
            let transformed_window_exprs: Vec<Expr> = window_exprs
                .into_iter()
                .map(Self::transform_window_expr_properties)
                .collect();

            plan = LogicalPlan::Window {
                input: Box::new(plan),
                window_exprs: transformed_window_exprs,
            };
        }

        if let Some(order_by) = &return_clause.order_by {
            // Build an extended scope that includes RETURN aliases so ORDER BY
            // can reference them (e.g. RETURN n.age AS age ORDER BY age).
            let order_by_scope: Vec<VariableInfo> = {
                let mut scope = vars_in_scope.to_vec();
                for (expr, alias) in &projections {
                    if let Some(a) = alias
                        && !is_var_in_scope(&scope, a)
                    {
                        scope.push(VariableInfo::new(a.clone(), VariableType::Scalar));
                    } else if let Expr::Variable(v) = expr
                        && !is_var_in_scope(&scope, v)
                    {
                        scope.push(VariableInfo::new(v.clone(), VariableType::Scalar));
                    }
                }
                scope
            };
            // Validate ORDER BY expressions against the extended scope
            for item in order_by {
                validate_expression_variables(&item.expr, &order_by_scope)?;
                validate_expression(&item.expr, &order_by_scope)?;
            }
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: order_by.clone(),
            };
        }

        if return_clause.skip.is_some() || return_clause.limit.is_some() {
            let skip = return_clause
                .skip
                .as_ref()
                .map(|e| parse_non_negative_integer(e, "SKIP"))
                .transpose()?
                .flatten();
            let fetch = return_clause
                .limit
                .as_ref()
                .map(|e| parse_non_negative_integer(e, "LIMIT"))
                .transpose()?
                .flatten();

            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                skip,
                fetch,
            };
        }

        if !projections.is_empty() {
            // If we created an Aggregate or Window node, we need to adjust the final projections
            // to reference aggregate/window function results as columns instead of re-evaluating them
            let final_projections = if has_agg || has_window_exprs {
                projections
                    .into_iter()
                    .map(|(expr, alias)| {
                        // Check if this expression is an aggregate function
                        if expr.is_aggregate() && !has_window_exprs {
                            // Replace aggregate function with a column reference to its result
                            // The column name in the Aggregate output is determined by build_aggregate_result
                            // Use the same logic as build_aggregate_result for column naming
                            let col_name = Self::get_aggregate_column_name(&expr);
                            (Expr::Variable(col_name), alias)
                        }
                        // Check if this expression is a window function
                        else if let Expr::FunctionCall {
                            window_spec: Some(_),
                            ..
                        } = &expr
                        {
                            // Replace window function with a column reference to its result
                            // The column name in the Window output is the full expression string
                            let window_col_name = expr.to_string_repr();
                            // Keep the original alias for the final output
                            (Expr::Variable(window_col_name), alias)
                        } else {
                            (expr, alias)
                        }
                    })
                    .collect()
            } else {
                projections
            };

            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections: final_projections,
            };
        }

        if return_clause.distinct {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

    fn plan_single(&self, query: Statement, initial_vars: Vec<String>) -> Result<LogicalPlan> {
        let typed_vars: Vec<VariableInfo> = initial_vars
            .into_iter()
            .map(|name| VariableInfo::new(name, VariableType::Imported))
            .collect();
        self.plan_single_typed(query, typed_vars)
    }

    /// Rewrite a query then plan it, preserving typed variable scope when possible.
    ///
    /// For `Query::Single` statements, uses `plan_single_typed` to carry typed
    /// variable info through and avoid false type-conflict errors in subqueries.
    /// For unions and other compound queries, falls back to `plan_with_scope`.
    fn rewrite_and_plan_typed(
        &self,
        query: Query,
        typed_vars: &[VariableInfo],
    ) -> Result<LogicalPlan> {
        let rewritten = crate::query::rewrite::rewrite_query(query)?;
        match rewritten {
            Query::Single(stmt) => self.plan_single_typed(stmt, typed_vars.to_vec()),
            other => self.plan_with_scope(other, vars_to_strings(typed_vars)),
        }
    }

    fn plan_single_typed(
        &self,
        query: Statement,
        initial_vars: Vec<VariableInfo>,
    ) -> Result<LogicalPlan> {
        let mut plan = LogicalPlan::Empty;

        if !initial_vars.is_empty() {
            let projections = initial_vars
                .iter()
                .map(|v| (Expr::Variable(v.name.clone()), Some(v.name.clone())))
                .collect();
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections,
            };
        }

        let mut vars_in_scope: Vec<VariableInfo> = initial_vars;

        for clause in query.clauses {
            match clause {
                Clause::Match(match_clause) => {
                    plan = self.plan_match_clause(&match_clause, plan, &mut vars_in_scope)?;
                }
                Clause::Unwind(unwind) => {
                    plan = LogicalPlan::Unwind {
                        input: Box::new(plan),
                        expr: unwind.expr.clone(),
                        variable: unwind.variable.clone(),
                    };
                    add_var_to_scope(&mut vars_in_scope, &unwind.variable, VariableType::Scalar)?;
                }
                Clause::LoadCsv(load_csv) => {
                    plan = LogicalPlan::LoadCsv {
                        url: load_csv.url.clone(),
                        variable: load_csv.variable.clone(),
                        with_headers: load_csv.with_headers,
                        field_terminator: load_csv.field_terminator,
                    };
                    add_var_to_scope(&mut vars_in_scope, &load_csv.variable, VariableType::Scalar)?;
                }
                Clause::Call(call_clause) => {
                    match &call_clause.kind {
                        CallKind::Procedure {
                            procedure,
                            arguments,
                        } => {
                            // Validate for duplicate yield names (VariableAlreadyBound)
                            let mut yield_names = Vec::new();
                            for item in &call_clause.yield_items {
                                let output_name = item.alias.as_ref().unwrap_or(&item.name);
                                if yield_names.contains(output_name) {
                                    return Err(anyhow!(
                                        "SyntaxError: VariableAlreadyBound - Variable '{}' already appears in YIELD clause",
                                        output_name
                                    ));
                                }
                                yield_names.push(output_name.clone());
                            }

                            let mut yields = Vec::new();
                            for item in &call_clause.yield_items {
                                yields.push((item.name.clone(), item.alias.clone()));
                                let var_name = item.alias.as_ref().unwrap_or(&item.name);
                                // Use Imported because procedure return types are unknown
                                // at plan time (could be nodes, edges, or scalars)
                                add_var_to_scope(
                                    &mut vars_in_scope,
                                    var_name,
                                    VariableType::Imported,
                                )?;
                            }
                            plan = LogicalPlan::ProcedureCall {
                                procedure_name: procedure.clone(),
                                arguments: arguments.clone(),
                                yield_items: yields,
                            };
                        }
                        CallKind::Subquery(query) => {
                            let subquery_plan =
                                self.rewrite_and_plan_typed(*query.clone(), &vars_in_scope)?;

                            // Extract variables from subquery RETURN clause
                            let subquery_vars = Self::collect_plan_variables(&subquery_plan);

                            // Add new variables to scope (as Scalar since they come from subquery projection)
                            for var in subquery_vars {
                                if !is_var_in_scope(&vars_in_scope, &var) {
                                    add_var_to_scope(
                                        &mut vars_in_scope,
                                        &var,
                                        VariableType::Scalar,
                                    )?;
                                }
                            }

                            plan = LogicalPlan::SubqueryCall {
                                input: Box::new(plan),
                                subquery: Box::new(subquery_plan),
                            };
                        }
                    }
                }
                Clause::Merge(merge_clause) => {
                    plan = LogicalPlan::Merge {
                        input: Box::new(plan),
                        pattern: merge_clause.pattern.clone(),
                        on_match: Some(SetClause {
                            items: merge_clause.on_match.clone(),
                        }),
                        on_create: Some(SetClause {
                            items: merge_clause.on_create.clone(),
                        }),
                    };

                    for path in &merge_clause.pattern.paths {
                        for element in &path.elements {
                            if let PatternElement::Node(n) = element {
                                if let Some(v) = &n.variable
                                    && !is_var_in_scope(&vars_in_scope, v)
                                {
                                    add_var_to_scope(&mut vars_in_scope, v, VariableType::Node)?;
                                }
                            } else if let PatternElement::Relationship(r) = element
                                && let Some(v) = &r.variable
                                && !is_var_in_scope(&vars_in_scope, v)
                            {
                                add_var_to_scope(&mut vars_in_scope, v, VariableType::Edge)?;
                            }
                        }
                    }
                }
                Clause::Create(create_clause) => {
                    // Validate CREATE patterns:
                    // - Nodes with labels/properties are "creations" - can't rebind existing variables
                    // - Bare nodes (v) are "references" if bound, "creations" if not
                    // - Relationships are always creations - can't rebind
                    // - Within CREATE, each new variable can only be defined once
                    // - Variables used in properties must be defined
                    let mut create_vars: Vec<&str> = Vec::new();
                    for path in &create_clause.pattern.paths {
                        for element in &path.elements {
                            match element {
                                PatternElement::Node(n) => {
                                    validate_property_variables(
                                        &n.properties,
                                        &vars_in_scope,
                                        &create_vars,
                                    )?;

                                    if let Some(v) = n.variable.as_deref()
                                        && !v.is_empty()
                                    {
                                        // A node is a "creation" if it has labels or properties
                                        let is_creation =
                                            !n.labels.is_empty() || n.properties.is_some();

                                        if is_creation {
                                            check_not_already_bound(
                                                v,
                                                &vars_in_scope,
                                                &create_vars,
                                            )?;
                                            create_vars.push(v);
                                        }
                                        // Bare node (v) - if already in scope, it's a reference (OK)
                                        // If not in scope, it will be added to scope later
                                    }
                                }
                                PatternElement::Relationship(r) => {
                                    validate_property_variables(
                                        &r.properties,
                                        &vars_in_scope,
                                        &create_vars,
                                    )?;

                                    // Validate relationship constraints for CREATE
                                    if r.types.len() != 1 {
                                        return Err(anyhow!(
                                            "SyntaxError: NoSingleRelationshipType - Exactly one relationship type required for CREATE"
                                        ));
                                    }
                                    if r.direction == Direction::Both {
                                        return Err(anyhow!(
                                            "SyntaxError: RequiresDirectedRelationship - Only directed relationships are supported in CREATE"
                                        ));
                                    }
                                    if r.range.is_some() {
                                        return Err(anyhow!(
                                            "SyntaxError: CreatingVarLength - Variable length relationships cannot be created"
                                        ));
                                    }

                                    if let Some(v) = r.variable.as_deref()
                                        && !v.is_empty()
                                    {
                                        check_not_already_bound(v, &vars_in_scope, &create_vars)?;
                                        create_vars.push(v);
                                    }
                                }
                                PatternElement::Parenthesized { .. } => {}
                            }
                        }
                    }

                    // Batch consecutive CREATEs to avoid deep recursion
                    match &mut plan {
                        LogicalPlan::CreateBatch { patterns, .. } => {
                            // Append to existing batch
                            patterns.push(create_clause.pattern.clone());
                        }
                        LogicalPlan::Create { input, pattern } => {
                            // Convert single Create to CreateBatch with both patterns
                            let first_pattern = pattern.clone();
                            plan = LogicalPlan::CreateBatch {
                                input: input.clone(),
                                patterns: vec![first_pattern, create_clause.pattern.clone()],
                            };
                        }
                        _ => {
                            // Start new Create (may become batch if more CREATEs follow)
                            plan = LogicalPlan::Create {
                                input: Box::new(plan),
                                pattern: create_clause.pattern.clone(),
                            };
                        }
                    }
                    // Add variables from created nodes and relationships to scope
                    for path in &create_clause.pattern.paths {
                        for element in &path.elements {
                            match element {
                                PatternElement::Node(n) => {
                                    if let Some(var) = &n.variable
                                        && !var.is_empty()
                                    {
                                        add_var_to_scope(
                                            &mut vars_in_scope,
                                            var,
                                            VariableType::Node,
                                        )?;
                                    }
                                }
                                PatternElement::Relationship(r) => {
                                    if let Some(var) = &r.variable
                                        && !var.is_empty()
                                    {
                                        add_var_to_scope(
                                            &mut vars_in_scope,
                                            var,
                                            VariableType::Edge,
                                        )?;
                                    }
                                }
                                PatternElement::Parenthesized { .. } => {
                                    // Skip for now - not commonly used in CREATE
                                }
                            }
                        }
                    }
                }
                Clause::Set(set_clause) => {
                    plan = LogicalPlan::Set {
                        input: Box::new(plan),
                        items: set_clause.items.clone(),
                    };
                }
                Clause::Remove(remove_clause) => {
                    plan = LogicalPlan::Remove {
                        input: Box::new(plan),
                        items: remove_clause.items.clone(),
                    };
                }
                Clause::Delete(delete_clause) => {
                    // Validate DELETE targets - must be defined and must be entities (Node/Edge/Path)
                    for item in &delete_clause.items {
                        let vars_used = collect_expr_variables(item);
                        for var in &vars_used {
                            // Check if variable is defined
                            if let Some(info) = find_var_in_scope(&vars_in_scope, var) {
                                // Check if variable is an entity type (Node, Edge, Path), not Scalar
                                if info.var_type == VariableType::Scalar {
                                    return Err(anyhow!(
                                        "SyntaxError: InvalidArgumentType - DELETE requires node or relationship, '{}' is a scalar value",
                                        var
                                    ));
                                }
                            } else {
                                return Err(anyhow!(
                                    "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                                    var
                                ));
                            }
                        }
                    }
                    plan = LogicalPlan::Delete {
                        input: Box::new(plan),
                        items: delete_clause.items.clone(),
                        detach: delete_clause.detach,
                    };
                }
                Clause::With(with_clause) => {
                    let (new_plan, new_vars) =
                        self.plan_with_clause(&with_clause, plan, &vars_in_scope)?;
                    plan = new_plan;
                    vars_in_scope = new_vars;
                }
                Clause::WithRecursive(with_recursive) => {
                    // Plan the recursive CTE
                    plan = self.plan_with_recursive(&with_recursive, plan, &vars_in_scope)?;
                    // Add the CTE name to the scope (as Scalar since it's a table reference)
                    add_var_to_scope(
                        &mut vars_in_scope,
                        &with_recursive.name,
                        VariableType::Scalar,
                    )?;
                }
                Clause::Return(return_clause) => {
                    plan = self.plan_return_clause(&return_clause, plan, &vars_in_scope)?;
                } // All Clause variants are handled above - no catch-all needed
            }
        }

        // Wrap write operations without RETURN in Limit(0) per OpenCypher spec.
        // CREATE (n) should return 0 rows, but CREATE (n) RETURN n should return 1 row.
        // If RETURN was used, the plan will have been wrapped in Project, so we only
        // wrap terminal Create/CreateBatch nodes.
        let plan = match &plan {
            LogicalPlan::Create { .. } | LogicalPlan::CreateBatch { .. } => LogicalPlan::Limit {
                input: Box::new(plan),
                skip: None,
                fetch: Some(0),
            },
            _ => plan,
        };

        Ok(plan)
    }

    fn collect_properties_from_expr(expr: &Expr, collected: &mut Vec<Expr>) {
        match expr {
            Expr::Property(_, _) => {
                if !collected
                    .iter()
                    .any(|e| e.to_string_repr() == expr.to_string_repr())
                {
                    collected.push(expr.clone());
                }
            }
            Expr::Variable(_) => {
                // Variables are already available, don't need to project them
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_properties_from_expr(left, collected);
                Self::collect_properties_from_expr(right, collected);
            }
            Expr::FunctionCall {
                args, window_spec, ..
            } => {
                for arg in args {
                    Self::collect_properties_from_expr(arg, collected);
                }
                if let Some(spec) = window_spec {
                    for partition_expr in &spec.partition_by {
                        Self::collect_properties_from_expr(partition_expr, collected);
                    }
                    for sort_item in &spec.order_by {
                        Self::collect_properties_from_expr(&sort_item.expr, collected);
                    }
                }
            }
            Expr::List(items) => {
                for item in items {
                    Self::collect_properties_from_expr(item, collected);
                }
            }
            Expr::UnaryOp { expr: e, .. }
            | Expr::IsNull(e)
            | Expr::IsNotNull(e)
            | Expr::IsUnique(e) => {
                Self::collect_properties_from_expr(e, collected);
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                if let Some(e) = expr {
                    Self::collect_properties_from_expr(e, collected);
                }
                for (w, t) in when_then {
                    Self::collect_properties_from_expr(w, collected);
                    Self::collect_properties_from_expr(t, collected);
                }
                if let Some(e) = else_expr {
                    Self::collect_properties_from_expr(e, collected);
                }
            }
            Expr::In { expr, list } => {
                Self::collect_properties_from_expr(expr, collected);
                Self::collect_properties_from_expr(list, collected);
            }
            Expr::ArrayIndex { array, index } => {
                Self::collect_properties_from_expr(array, collected);
                Self::collect_properties_from_expr(index, collected);
            }
            Expr::ArraySlice { array, start, end } => {
                Self::collect_properties_from_expr(array, collected);
                if let Some(s) = start {
                    Self::collect_properties_from_expr(s, collected);
                }
                if let Some(e) = end {
                    Self::collect_properties_from_expr(e, collected);
                }
            }
            _ => {}
        }
    }

    fn collect_window_functions(expr: &Expr, collected: &mut Vec<Expr>) {
        if let Expr::FunctionCall { window_spec, .. } = expr {
            // Collect any function with a window spec (OVER clause)
            if window_spec.is_some() {
                if !collected
                    .iter()
                    .any(|e| e.to_string_repr() == expr.to_string_repr())
                {
                    collected.push(expr.clone());
                }
                return;
            }
        }

        match expr {
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_window_functions(left, collected);
                Self::collect_window_functions(right, collected);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    Self::collect_window_functions(arg, collected);
                }
            }
            Expr::List(items) => {
                for i in items {
                    Self::collect_window_functions(i, collected);
                }
            }
            Expr::Map(items) => {
                for (_, i) in items {
                    Self::collect_window_functions(i, collected);
                }
            }
            Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::UnaryOp { expr: e, .. } => {
                Self::collect_window_functions(e, collected);
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                if let Some(e) = expr {
                    Self::collect_window_functions(e, collected);
                }
                for (w, t) in when_then {
                    Self::collect_window_functions(w, collected);
                    Self::collect_window_functions(t, collected);
                }
                if let Some(e) = else_expr {
                    Self::collect_window_functions(e, collected);
                }
            }
            Expr::Reduce {
                init, list, expr, ..
            } => {
                Self::collect_window_functions(init, collected);
                Self::collect_window_functions(list, collected);
                Self::collect_window_functions(expr, collected);
            }
            Expr::Quantifier {
                list, predicate, ..
            } => {
                Self::collect_window_functions(list, collected);
                Self::collect_window_functions(predicate, collected);
            }
            Expr::In { expr, list } => {
                Self::collect_window_functions(expr, collected);
                Self::collect_window_functions(list, collected);
            }
            Expr::ArrayIndex { array, index } => {
                Self::collect_window_functions(array, collected);
                Self::collect_window_functions(index, collected);
            }
            Expr::ArraySlice { array, start, end } => {
                Self::collect_window_functions(array, collected);
                if let Some(s) = start {
                    Self::collect_window_functions(s, collected);
                }
                if let Some(e) = end {
                    Self::collect_window_functions(e, collected);
                }
            }
            Expr::Property(e, _) => Self::collect_window_functions(e, collected),
            Expr::CountSubquery(_) | Expr::Exists(_) => {}
            _ => {}
        }
    }

    /// Transform property expressions in manual window functions to use qualified variable names.
    ///
    /// Converts `Expr::Property(Expr::Variable("e"), "dept")` to `Expr::Variable("e.dept")`
    /// so the executor can look up values directly from the row HashMap after the
    /// intermediate projection has materialized these properties with qualified names.
    ///
    /// Transforms ALL window functions (both manual and aggregate).
    /// Properties like `e.dept` become variables like `Expr::Variable("e.dept")`.
    fn transform_window_expr_properties(expr: Expr) -> Expr {
        let Expr::FunctionCall {
            name,
            args,
            window_spec: Some(spec),
            distinct,
        } = expr
        else {
            return expr;
        };

        // Transform arguments for ALL window functions
        // Both manual (ROW_NUMBER, etc.) and aggregate (SUM, AVG, etc.) need this
        let transformed_args = args
            .into_iter()
            .map(Self::transform_property_to_variable)
            .collect();

        // CRITICAL: ALL window functions (manual and aggregate) need partition_by/order_by transformed
        let transformed_partition_by = spec
            .partition_by
            .into_iter()
            .map(Self::transform_property_to_variable)
            .collect();

        let transformed_order_by = spec
            .order_by
            .into_iter()
            .map(|item| SortItem {
                expr: Self::transform_property_to_variable(item.expr),
                ascending: item.ascending,
            })
            .collect();

        Expr::FunctionCall {
            name,
            args: transformed_args,
            window_spec: Some(WindowSpec {
                partition_by: transformed_partition_by,
                order_by: transformed_order_by,
            }),
            distinct,
        }
    }

    /// Transform a property expression to a variable expression with qualified name.
    ///
    /// `Expr::Property(Expr::Variable("e"), "dept")` becomes `Expr::Variable("e.dept")`
    fn transform_property_to_variable(expr: Expr) -> Expr {
        let Expr::Property(base, prop) = expr else {
            return expr;
        };

        match *base {
            Expr::Variable(var) => Expr::Variable(format!("{}.{}", var, prop)),
            other => Expr::Property(Box::new(Self::transform_property_to_variable(other)), prop),
        }
    }

    /// Transform VALID_AT macro into function call
    ///
    /// `e VALID_AT timestamp` becomes `uni.temporal.validAt(e, 'valid_from', 'valid_to', timestamp)`
    /// `e VALID_AT(timestamp, 'start', 'end')` becomes `uni.temporal.validAt(e, 'start', 'end', timestamp)`
    fn transform_valid_at_to_function(expr: Expr) -> Expr {
        match expr {
            Expr::ValidAt {
                entity,
                timestamp,
                start_prop,
                end_prop,
            } => {
                let start = start_prop.unwrap_or_else(|| "valid_from".to_string());
                let end = end_prop.unwrap_or_else(|| "valid_to".to_string());

                Expr::FunctionCall {
                    name: "uni.temporal.validAt".to_string(),
                    args: vec![
                        Self::transform_valid_at_to_function(*entity),
                        Expr::Literal(CypherLiteral::String(start)),
                        Expr::Literal(CypherLiteral::String(end)),
                        Self::transform_valid_at_to_function(*timestamp),
                    ],
                    distinct: false,
                    window_spec: None,
                }
            }
            // Recursively transform nested expressions
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::transform_valid_at_to_function(*left)),
                op,
                right: Box::new(Self::transform_valid_at_to_function(*right)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::transform_valid_at_to_function(*expr)),
            },
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => Expr::FunctionCall {
                name,
                args: args
                    .into_iter()
                    .map(Self::transform_valid_at_to_function)
                    .collect(),
                distinct,
                window_spec,
            },
            Expr::Property(base, prop) => {
                Expr::Property(Box::new(Self::transform_valid_at_to_function(*base)), prop)
            }
            Expr::List(items) => Expr::List(
                items
                    .into_iter()
                    .map(Self::transform_valid_at_to_function)
                    .collect(),
            ),
            Expr::In { expr, list } => Expr::In {
                expr: Box::new(Self::transform_valid_at_to_function(*expr)),
                list: Box::new(Self::transform_valid_at_to_function(*list)),
            },
            Expr::IsNull(e) => Expr::IsNull(Box::new(Self::transform_valid_at_to_function(*e))),
            Expr::IsNotNull(e) => {
                Expr::IsNotNull(Box::new(Self::transform_valid_at_to_function(*e)))
            }
            Expr::IsUnique(e) => Expr::IsUnique(Box::new(Self::transform_valid_at_to_function(*e))),
            // Other cases: return as-is
            other => other,
        }
    }

    // plan_foreach_body removed

    /// Plan a MATCH clause, handling both shortestPath and regular patterns.
    fn plan_match_clause(
        &self,
        match_clause: &MatchClause,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<VariableInfo>,
    ) -> Result<LogicalPlan> {
        let mut plan = plan;

        if match_clause.pattern.paths.is_empty() {
            return Err(anyhow!("Empty pattern"));
        }

        // Track variables introduced by this OPTIONAL MATCH
        let vars_before_pattern = vars_in_scope.len();

        for path in &match_clause.pattern.paths {
            if let Some(mode) = &path.shortest_path_mode {
                plan = self.plan_shortest_path(path, plan, vars_in_scope, mode)?;
            } else {
                plan = self.plan_path(path, plan, vars_in_scope, match_clause.optional)?;
            }
        }

        // Collect variables introduced by this OPTIONAL MATCH pattern
        let optional_vars: std::collections::HashSet<String> = if match_clause.optional {
            vars_in_scope[vars_before_pattern..]
                .iter()
                .map(|v| v.name.clone())
                .collect()
        } else {
            std::collections::HashSet::new()
        };

        // Handle WHERE clause with vector_similarity and predicate pushdown
        if let Some(predicate) = &match_clause.where_clause {
            plan = self.plan_where_clause(predicate, plan, vars_in_scope, optional_vars)?;
        }

        Ok(plan)
    }

    /// Plan a shortestPath pattern.
    fn plan_shortest_path(
        &self,
        path: &PathPattern,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<VariableInfo>,
        mode: &ShortestPathMode,
    ) -> Result<LogicalPlan> {
        let mut plan = plan;
        let elements = &path.elements;

        // Pattern must be: node-rel-node-rel-...-node (odd number of elements >= 3)
        if elements.len() < 3 || elements.len().is_multiple_of(2) {
            return Err(anyhow!(
                "shortestPath requires at least one relationship: (a)-[*]->(b)"
            ));
        }

        let source_node = match &elements[0] {
            PatternElement::Node(n) => n,
            _ => return Err(anyhow!("ShortestPath must start with a node")),
        };
        let rel = match &elements[1] {
            PatternElement::Relationship(r) => r,
            _ => {
                return Err(anyhow!(
                    "ShortestPath middle element must be a relationship"
                ));
            }
        };
        let target_node = match &elements[2] {
            PatternElement::Node(n) => n,
            _ => return Err(anyhow!("ShortestPath must end with a node")),
        };

        let source_var = source_node
            .variable
            .clone()
            .ok_or_else(|| anyhow!("Source node must have variable in shortestPath"))?;
        let target_var = target_node
            .variable
            .clone()
            .ok_or_else(|| anyhow!("Target node must have variable in shortestPath"))?;
        let path_var = path
            .variable
            .clone()
            .ok_or_else(|| anyhow!("shortestPath must be assigned to a variable"))?;

        let source_bound = is_var_in_scope(vars_in_scope, &source_var);
        let target_bound = is_var_in_scope(vars_in_scope, &target_var);

        // Plan source node if not bound
        if !source_bound {
            plan = self.plan_unbound_node(source_node, &source_var, plan, false)?;
        } else if let Some(prop_filter) =
            self.properties_to_expr(&source_var, &source_node.properties)
        {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: prop_filter,
                optional_variables: std::collections::HashSet::new(),
            };
        }

        // Plan target node if not bound
        let target_label_id = if !target_bound {
            // Use first label for target_label_id
            let target_label_name = target_node
                .labels
                .first()
                .ok_or_else(|| anyhow!("Target node must have label if not already bound"))?;
            let target_label_meta = self
                .schema
                .get_label_case_insensitive(target_label_name)
                .ok_or_else(|| anyhow!("Label {} not found", target_label_name))?;

            let target_scan = LogicalPlan::Scan {
                label_id: target_label_meta.id,
                labels: target_node.labels.clone(),
                variable: target_var.clone(),
                filter: self.properties_to_expr(&target_var, &target_node.properties),
                optional: false,
            };

            if matches!(plan, LogicalPlan::Empty) {
                plan = target_scan;
            } else {
                plan = LogicalPlan::CrossJoin {
                    left: Box::new(plan),
                    right: Box::new(target_scan),
                };
            }
            target_label_meta.id
        } else {
            if let Some(prop_filter) = self.properties_to_expr(&target_var, &target_node.properties)
            {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: prop_filter,
                    optional_variables: std::collections::HashSet::new(),
                };
            }
            0 // Wildcard for already-bound target
        };

        // Add ShortestPath operator
        let edge_type_ids = if rel.types.is_empty() {
            // If no type specified, fetch all edge types (both schema and schemaless)
            self.schema.all_edge_type_ids()
        } else {
            let mut ids = Vec::new();
            for type_name in &rel.types {
                let edge_meta = self
                    .schema
                    .edge_types
                    .get(type_name)
                    .ok_or_else(|| anyhow!("Edge type {} not found", type_name))?;
                ids.push(edge_meta.id);
            }
            ids
        };

        // Extract hop constraints from relationship pattern
        let min_hops = rel.range.as_ref().and_then(|r| r.min).unwrap_or(1);
        let max_hops = rel.range.as_ref().and_then(|r| r.max).unwrap_or(u32::MAX);

        let sp_plan = match mode {
            ShortestPathMode::Shortest => LogicalPlan::ShortestPath {
                input: Box::new(plan),
                edge_type_ids,
                direction: rel.direction.clone(),
                source_variable: source_var.clone(),
                target_variable: target_var.clone(),
                target_label_id,
                path_variable: path_var.clone(),
                min_hops,
                max_hops,
            },
            ShortestPathMode::AllShortest => LogicalPlan::AllShortestPaths {
                input: Box::new(plan),
                edge_type_ids,
                direction: rel.direction.clone(),
                source_variable: source_var.clone(),
                target_variable: target_var.clone(),
                target_label_id,
                path_variable: path_var.clone(),
                min_hops,
                max_hops,
            },
        };

        if !source_bound {
            add_var_to_scope(vars_in_scope, &source_var, VariableType::Node)?;
        }
        if !target_bound {
            add_var_to_scope(vars_in_scope, &target_var, VariableType::Node)?;
        }
        add_var_to_scope(vars_in_scope, &path_var, VariableType::Path)?;

        Ok(sp_plan)
    }
    // plan_all_shortest_paths removed (merged into plan_shortest_path)

    // plan_quantified_pattern removed (not yet supported in new AST)

    /// Plan a regular MATCH path (not shortestPath).
    fn plan_path(
        &self,
        path: &PathPattern,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<VariableInfo>,
        optional: bool,
    ) -> Result<LogicalPlan> {
        let mut plan = plan;
        let elements = &path.elements;
        let mut i = 0;

        let path_variable = path.variable.clone();

        // Check for VariableAlreadyBound: path variable already in scope
        if let Some(pv) = &path_variable
            && !pv.is_empty()
            && is_var_in_scope(vars_in_scope, pv)
        {
            return Err(anyhow!(
                "SyntaxError: VariableAlreadyBound - Variable '{}' already defined",
                pv
            ));
        }

        // Check for VariableAlreadyBound: path variable conflicts with element variables
        if let Some(pv) = &path_variable
            && !pv.is_empty()
        {
            for element in elements {
                match element {
                    PatternElement::Node(n) => {
                        if let Some(v) = &n.variable
                            && v == pv
                        {
                            return Err(anyhow!(
                                "SyntaxError: VariableAlreadyBound - Variable '{}' already defined",
                                pv
                            ));
                        }
                    }
                    PatternElement::Relationship(r) => {
                        if let Some(v) = &r.variable
                            && v == pv
                        {
                            return Err(anyhow!(
                                "SyntaxError: VariableAlreadyBound - Variable '{}' already defined",
                                pv
                            ));
                        }
                    }
                    PatternElement::Parenthesized { .. } => {}
                }
            }
        }

        // For OPTIONAL MATCH, extract all variables from this pattern upfront.
        // When any hop fails in a multi-hop pattern, ALL these variables should be NULL.
        let optional_pattern_vars: std::collections::HashSet<String> = if optional {
            let mut vars = std::collections::HashSet::new();
            for element in elements {
                match element {
                    PatternElement::Node(n) => {
                        if let Some(v) = &n.variable
                            && !v.is_empty()
                            && !is_var_in_scope(vars_in_scope, v)
                        {
                            vars.insert(v.clone());
                        }
                    }
                    PatternElement::Relationship(r) => {
                        if let Some(v) = &r.variable
                            && !v.is_empty()
                        {
                            vars.insert(v.clone());
                        }
                    }
                    PatternElement::Parenthesized { pattern, .. } => {
                        // Also check nested patterns
                        for nested_elem in &pattern.elements {
                            match nested_elem {
                                PatternElement::Node(n) => {
                                    if let Some(v) = &n.variable
                                        && !v.is_empty()
                                        && !is_var_in_scope(vars_in_scope, v)
                                    {
                                        vars.insert(v.clone());
                                    }
                                }
                                PatternElement::Relationship(r) => {
                                    if let Some(v) = &r.variable
                                        && !v.is_empty()
                                    {
                                        vars.insert(v.clone());
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            // Include path variable if present
            if let Some(pv) = &path_variable
                && !pv.is_empty()
            {
                vars.insert(pv.clone());
            }
            vars
        } else {
            std::collections::HashSet::new()
        };

        // Multi-hop path variables are now supported - path is accumulated across hops
        while i < elements.len() {
            let element = &elements[i];
            match element {
                PatternElement::Node(n) => {
                    let mut variable = n.variable.clone().unwrap_or_default();
                    if variable.is_empty() {
                        variable = self.next_anon_var();
                    }
                    let is_bound =
                        !variable.is_empty() && is_var_in_scope(vars_in_scope, &variable);

                    if is_bound {
                        // Check for type conflict - can't use an Edge/Path as a Node
                        if let Some(info) = find_var_in_scope(vars_in_scope, &variable)
                            && !info.var_type.is_compatible_with(VariableType::Node)
                        {
                            return Err(anyhow!(
                                "SyntaxError: VariableTypeConflict - Variable '{}' already defined as {:?}, cannot use as Node",
                                variable,
                                info.var_type
                            ));
                        }
                        if let Some(prop_filter) = self.properties_to_expr(&variable, &n.properties)
                        {
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate: prop_filter,
                                optional_variables: std::collections::HashSet::new(),
                            };
                        }
                    } else {
                        plan = self.plan_unbound_node(n, &variable, plan, optional)?;
                        if !variable.is_empty() {
                            add_var_to_scope(vars_in_scope, &variable, VariableType::Node)?;
                        }
                    }

                    // Look ahead for relationships
                    let mut current_source_var = variable;
                    i += 1;
                    while i < elements.len() {
                        if let PatternElement::Relationship(r) = &elements[i] {
                            if i + 1 < elements.len() {
                                let target_node_part = &elements[i + 1];
                                if let PatternElement::Node(n_target) = target_node_part {
                                    // Plan the traverse from the current source node
                                    let (new_plan, target_var) = self.plan_traverse_with_source(
                                        plan,
                                        vars_in_scope,
                                        TraverseParams {
                                            rel: r,
                                            target_node: n_target,
                                            _source_part: element,
                                            optional,
                                            path_variable: path_variable.clone(),
                                            optional_pattern_vars: optional_pattern_vars.clone(),
                                        },
                                        &current_source_var,
                                    )?;
                                    plan = new_plan;
                                    current_source_var = target_var;
                                    i += 2;
                                } else {
                                    return Err(anyhow!("Relationship must be followed by a node"));
                                }
                            } else {
                                return Err(anyhow!("Relationship cannot be the last element"));
                            }
                        } else {
                            break;
                        }
                    }
                }
                PatternElement::Relationship(_) => {
                    return Err(anyhow!("Pattern must start with a node"));
                }
                PatternElement::Parenthesized { pattern, range } => {
                    // Quantified pattern: ((a)-[:REL]->(b)){n,m}
                    // Validate: must be exactly Node-Relationship-Node
                    if pattern.elements.len() != 3 {
                        return Err(anyhow!(
                            "Quantified pattern must be (source)-[relationship]->(target)"
                        ));
                    }

                    let source_node = match &pattern.elements[0] {
                        PatternElement::Node(n) => n,
                        _ => return Err(anyhow!("Quantified pattern must start with a node")),
                    };
                    let mut relationship = match &pattern.elements[1] {
                        PatternElement::Relationship(r) => r.clone(),
                        _ => {
                            return Err(anyhow!(
                                "Quantified pattern middle element must be a relationship"
                            ));
                        }
                    };
                    let target_node = match &pattern.elements[2] {
                        PatternElement::Node(n) => n,
                        _ => return Err(anyhow!("Quantified pattern must end with a node")),
                    };

                    // Reject nested quantifiers
                    if relationship.range.is_some() {
                        return Err(anyhow!(
                            "Nested quantifiers not supported: ((a)-[:REL*n]->(b)){{m}}"
                        ));
                    }

                    // Apply quantifier to relationship range
                    relationship.range = range.clone();

                    // Plan source node
                    let source_variable = source_node
                        .variable
                        .clone()
                        .filter(|v| !v.is_empty())
                        .unwrap_or_else(|| self.next_anon_var());

                    if is_var_in_scope(vars_in_scope, &source_variable) {
                        // Source is already bound, apply property filter if needed
                        if let Some(prop_filter) =
                            self.properties_to_expr(&source_variable, &source_node.properties)
                        {
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate: prop_filter,
                                optional_variables: std::collections::HashSet::new(),
                            };
                        }
                    } else {
                        // Source is unbound, scan it
                        plan =
                            self.plan_unbound_node(source_node, &source_variable, plan, optional)?;
                        add_var_to_scope(vars_in_scope, &source_variable, VariableType::Node)?;
                    }

                    // Plan traverse with quantified relationship
                    let (new_plan, _target_var) = self.plan_traverse_with_source(
                        plan,
                        vars_in_scope,
                        TraverseParams {
                            rel: &relationship,
                            target_node,
                            _source_part: element,
                            optional,
                            path_variable: path_variable.clone(),
                            optional_pattern_vars: optional_pattern_vars.clone(),
                        },
                        &source_variable,
                    )?;
                    plan = new_plan;

                    i += 1;
                }
            }
        }

        Ok(plan)
    }

    /// Plan a traverse with an explicit source variable name.
    fn plan_traverse_with_source(
        &self,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<VariableInfo>,
        params: TraverseParams<'_>,
        source_variable: &str,
    ) -> Result<(LogicalPlan, String)> {
        let mut edge_type_ids = Vec::new();
        let mut dst_labels = Vec::new();
        let mut unknown_types = Vec::new();

        if params.rel.types.is_empty() {
            // All types - include both schema and schemaless edge types
            // This ensures MATCH (a)-[r]->(b) finds edges even when no schema is defined
            edge_type_ids = self.schema.all_edge_type_ids();
            for meta in self.schema.edge_types.values() {
                dst_labels.extend(meta.dst_labels.iter().cloned());
            }
        } else {
            for type_name in &params.rel.types {
                if let Some(edge_meta) = self.schema.edge_types.get(type_name) {
                    // Known type - use standard Traverse with type_id
                    edge_type_ids.push(edge_meta.id);
                    dst_labels.extend(edge_meta.dst_labels.iter().cloned());
                } else {
                    // Unknown type - will use TraverseMainByType
                    unknown_types.push(type_name.clone());
                }
            }
        }

        let mut target_variable = params.target_node.variable.clone().unwrap_or_default();
        if target_variable.is_empty() {
            target_variable = self.next_anon_var();
        }
        let _target_is_bound =
            !target_variable.is_empty() && is_var_in_scope(vars_in_scope, &target_variable);

        // Check for VariableTypeConflict: relationship variable used as node
        // e.g., ()-[r]-(r) where r is both the edge and a node endpoint
        if let Some(rel_var) = &params.rel.variable
            && !rel_var.is_empty()
            && rel_var == &target_variable
        {
            return Err(anyhow!(
                "SyntaxError: VariableTypeConflict - Variable '{}' already defined as relationship, cannot use as node",
                rel_var
            ));
        }

        // Check for VariableTypeConflict: node/path variable reused as relationship
        // e.g., (r)-[r]-() or r = ()-[]-(), ()-[r]-()
        if let Some(rel_var) = &params.rel.variable
            && !rel_var.is_empty()
            && let Some(info) = find_var_in_scope(vars_in_scope, rel_var)
            && !info.var_type.is_compatible_with(VariableType::Edge)
        {
            return Err(anyhow!(
                "SyntaxError: VariableTypeConflict - Variable '{}' already defined as {:?}, cannot use as relationship",
                rel_var,
                info.var_type
            ));
        }

        // Check for VariableTypeConflict: target node variable already bound as non-Node
        // e.g., ()-[r]-()-[]-(r) where r was added as Edge, now used as target node
        if _target_is_bound
            && let Some(info) = find_var_in_scope(vars_in_scope, &target_variable)
            && !info.var_type.is_compatible_with(VariableType::Node)
        {
            return Err(anyhow!(
                "SyntaxError: VariableTypeConflict - Variable '{}' already defined as {:?}, cannot use as Node",
                target_variable,
                info.var_type
            ));
        }

        // If all requested types are unknown (schemaless), use TraverseMainByType
        // This allows queries like MATCH (a)-[:UnknownType]->(b) to work
        if !unknown_types.is_empty() && edge_type_ids.is_empty() {
            // All types are unknown - use schemaless traversal
            // For now, we only support single unknown type
            if unknown_types.len() > 1 {
                return Err(anyhow!(
                    "Multiple unknown edge types in a single pattern not yet supported: {:?}",
                    unknown_types
                ));
            }

            let min_hops = params.rel.range.as_ref().and_then(|r| r.min).unwrap_or(1) as usize;
            let max_hops = params.rel.range.as_ref().and_then(|r| r.max).unwrap_or(1) as usize;
            let is_variable_length = min_hops > 1 || max_hops > 1 || min_hops != max_hops;

            let (step_var, path_var) = if is_variable_length {
                (
                    None,
                    params.rel.variable.clone().or(params.path_variable.clone()),
                )
            } else {
                (params.rel.variable.clone(), params.path_variable.clone())
            };

            let mut plan = LogicalPlan::TraverseMainByType {
                type_name: unknown_types.into_iter().next().unwrap(),
                input: Box::new(plan),
                direction: params.rel.direction.clone(),
                source_variable: source_variable.to_string(),
                target_variable: target_variable.clone(),
                step_variable: step_var.clone(),
                min_hops,
                max_hops,
                optional: params.optional,
                target_filter: self.node_filter_expr(
                    &target_variable,
                    &params.target_node.labels,
                    &params.target_node.properties,
                ),
                path_variable: path_var.clone(),
                optional_pattern_vars: params.optional_pattern_vars.clone(),
            };

            // Only apply bound target filter for Imported variables (from outer scope/subquery).
            // For regular cycle patterns like (a)-[:T]->(b)-[:T]->(a), the bound check
            // uses Parameter which requires the value to be in params (subquery context).
            if _target_is_bound
                && let Some(info) = find_var_in_scope(vars_in_scope, &target_variable)
                && info.var_type == VariableType::Imported
            {
                plan = Self::wrap_with_bound_target_filter(plan, &target_variable);
            }

            // Add the bound variables to scope
            if let Some(sv) = &step_var {
                add_var_to_scope(vars_in_scope, sv, VariableType::Edge)?;
            }
            if let Some(pv) = &path_var
                && !is_var_in_scope(vars_in_scope, pv)
            {
                add_var_to_scope(vars_in_scope, pv, VariableType::Path)?;
            }
            if !is_var_in_scope(vars_in_scope, &target_variable) {
                add_var_to_scope(vars_in_scope, &target_variable, VariableType::Node)?;
            }

            return Ok((plan, target_variable));
        }

        // If we have a mix of known and unknown types, error for now
        // (could be extended to Union of Traverse + TraverseMainByType)
        if !unknown_types.is_empty() {
            return Err(anyhow!(
                "Mixed known and unknown edge types not yet supported. Unknown: {:?}",
                unknown_types
            ));
        }

        let target_label_meta = if let Some(label_name) = params.target_node.labels.first() {
            // Use first label for target_label_id
            // For schemaless support, allow unknown target labels
            self.schema.get_label_case_insensitive(label_name)
        } else if !_target_is_bound {
            // Infer from edge type(s)
            let unique_dsts: Vec<_> = dst_labels
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            if unique_dsts.len() == 1 {
                let label_name = &unique_dsts[0];
                self.schema.get_label_case_insensitive(label_name)
            } else {
                // Multiple or no destination labels inferred - allow any target
                // This supports patterns like MATCH (a)-[:EDGE_TYPE]-(b) WHERE b:Label
                // where the edge type can connect to multiple labels
                None
            }
        } else {
            None
        };

        let min_hops = params.rel.range.as_ref().and_then(|r| r.min).unwrap_or(1) as usize;
        let max_hops = params.rel.range.as_ref().and_then(|r| r.max).unwrap_or(1) as usize;
        let is_variable_length = min_hops > 1 || max_hops > 1 || min_hops != max_hops;

        // For variable-length paths, bind the relationship variable as path_variable
        // For single-hop paths, bind it as step_variable
        let (step_var, path_var) = if is_variable_length {
            // Variable-length: bind as path_variable for Path object
            (
                None,
                params.rel.variable.clone().or(params.path_variable.clone()),
            )
        } else {
            // Single-hop: bind as step_variable for relationship object
            (params.rel.variable.clone(), params.path_variable.clone())
        };

        let mut plan = LogicalPlan::Traverse {
            input: Box::new(plan),
            edge_type_ids,
            direction: params.rel.direction.clone(),
            source_variable: source_variable.to_string(),
            target_variable: target_variable.clone(),
            target_label_id: target_label_meta.map(|m| m.id).unwrap_or(0),
            step_variable: step_var.clone(),
            min_hops,
            max_hops,
            optional: params.optional,
            target_filter: self
                .properties_to_expr(&target_variable, &params.target_node.properties),
            path_variable: path_var.clone(),
            edge_properties: std::collections::HashSet::new(),
            optional_pattern_vars: params.optional_pattern_vars.clone(),
        };

        // Only apply bound target filter for Imported variables (from outer scope/subquery).
        // For regular cycle patterns like (a)-[:T]->(b)-[:T]->(a), the bound check
        // uses Parameter which requires the value to be in params (subquery context).
        if _target_is_bound
            && let Some(info) = find_var_in_scope(vars_in_scope, &target_variable)
            && info.var_type == VariableType::Imported
        {
            plan = Self::wrap_with_bound_target_filter(plan, &target_variable);
        }

        // Add the bound variables to scope
        if let Some(sv) = &step_var {
            add_var_to_scope(vars_in_scope, sv, VariableType::Edge)?;
        }
        if let Some(pv) = &path_var
            && !is_var_in_scope(vars_in_scope, pv)
        {
            add_var_to_scope(vars_in_scope, pv, VariableType::Path)?;
        }
        if !is_var_in_scope(vars_in_scope, &target_variable) {
            add_var_to_scope(vars_in_scope, &target_variable, VariableType::Node)?;
        }

        Ok((plan, target_variable))
    }

    /// Plan an unbound node (creates a Scan, ScanAll, ScanMainByLabel, ExtIdLookup, or CrossJoin).
    fn plan_unbound_node(
        &self,
        node: &NodePattern,
        variable: &str,
        plan: LogicalPlan,
        optional: bool,
    ) -> Result<LogicalPlan> {
        // Properties handling
        let properties = match &node.properties {
            Some(Expr::Map(entries)) => entries.as_slice(),
            Some(_) => return Err(anyhow!("Node properties must be a Map")),
            None => &[],
        };

        // Check for ext_id in properties when no label is specified
        if node.labels.is_empty() {
            // Try to find ext_id property for main table lookup
            if let Some((_, ext_id_value)) = properties.iter().find(|(k, _)| k == "ext_id") {
                // Extract the ext_id value as a string
                let ext_id = match ext_id_value {
                    Expr::Literal(CypherLiteral::String(s)) => s.clone(),
                    _ => {
                        return Err(anyhow!("ext_id must be a string literal for direct lookup"));
                    }
                };

                // Build filter for remaining properties (excluding ext_id)
                let remaining_props: Vec<_> = properties
                    .iter()
                    .filter(|(k, _)| k != "ext_id")
                    .cloned()
                    .collect();

                let remaining_expr = if remaining_props.is_empty() {
                    None
                } else {
                    Some(Expr::Map(remaining_props))
                };

                let prop_filter = self.properties_to_expr(variable, &remaining_expr);

                let ext_id_lookup = LogicalPlan::ExtIdLookup {
                    variable: variable.to_string(),
                    ext_id,
                    filter: prop_filter,
                    optional,
                };

                return if matches!(plan, LogicalPlan::Empty) {
                    Ok(ext_id_lookup)
                } else {
                    Ok(LogicalPlan::CrossJoin {
                        left: Box::new(plan),
                        right: Box::new(ext_id_lookup),
                    })
                };
            }

            // No ext_id: create ScanAll for unlabeled node pattern
            let prop_filter = self.properties_to_expr(variable, &node.properties);
            let scan_all = LogicalPlan::ScanAll {
                variable: variable.to_string(),
                filter: prop_filter,
                optional,
            };

            return if matches!(plan, LogicalPlan::Empty) {
                Ok(scan_all)
            } else {
                Ok(LogicalPlan::CrossJoin {
                    left: Box::new(plan),
                    right: Box::new(scan_all),
                })
            };
        }

        // Use first label for label_id (primary label for dataset selection)
        let label_name = &node.labels[0];

        // Check if label exists in schema
        if let Some(label_meta) = self.schema.get_label_case_insensitive(label_name) {
            // Known label: use standard Scan
            let prop_filter = self.properties_to_expr(variable, &node.properties);
            let scan = LogicalPlan::Scan {
                label_id: label_meta.id,
                labels: node.labels.clone(),
                variable: variable.to_string(),
                filter: prop_filter,
                optional,
            };

            if matches!(plan, LogicalPlan::Empty) {
                Ok(scan)
            } else {
                Ok(LogicalPlan::CrossJoin {
                    left: Box::new(plan),
                    right: Box::new(scan),
                })
            }
        } else {
            // Unknown label: use ScanMainByLabel for schemaless support
            let prop_filter = self.properties_to_expr(variable, &node.properties);
            let scan_main = LogicalPlan::ScanMainByLabel {
                label_name: label_name.clone(),
                variable: variable.to_string(),
                filter: prop_filter,
                optional,
            };

            if matches!(plan, LogicalPlan::Empty) {
                Ok(scan_main)
            } else {
                Ok(LogicalPlan::CrossJoin {
                    left: Box::new(plan),
                    right: Box::new(scan_main),
                })
            }
        }
    }

    /// Plan a traverse (edge traversal between nodes).
    fn _plan_traverse(
        &self,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<VariableInfo>,
        params: TraverseParams<'_>,
    ) -> Result<LogicalPlan> {
        let source_variable = match params._source_part {
            PatternElement::Node(n) => n.variable.clone().unwrap_or_default(),
            _ => return Err(anyhow!("Source part must be a node")),
        };
        let (new_plan, _) =
            self.plan_traverse_with_source(plan, vars_in_scope, params, &source_variable)?;
        Ok(new_plan)
    }

    /// Plan a WHERE clause with vector_similarity extraction and predicate pushdown.
    ///
    /// When `optional_vars` is non-empty, the Filter will preserve rows where
    /// any of those variables are NULL (for OPTIONAL MATCH semantics).
    fn plan_where_clause(
        &self,
        predicate: &Expr,
        plan: LogicalPlan,
        vars_in_scope: &[VariableInfo],
        optional_vars: std::collections::HashSet<String>,
    ) -> Result<LogicalPlan> {
        // Validate no aggregation functions in WHERE clause
        validate_no_aggregation_in_where(predicate)?;

        // Validate all variables used are in scope
        validate_expression_variables(predicate, vars_in_scope)?;

        // Validate expression types (function args, boolean operators)
        validate_expression(predicate, vars_in_scope)?;

        let mut plan = plan;

        // Transform VALID_AT macro to function call
        let transformed_predicate = Self::transform_valid_at_to_function(predicate.clone());

        let mut current_predicate =
            self.rewrite_predicates_using_indexes(&transformed_predicate, &plan, vars_in_scope)?;

        // 0. Try to extract ANY IN predicate for Inverted Index optimization
        if let Some(extraction) = extract_any_in_predicate(&current_predicate) {
            let any_pred = &extraction.predicate;
            // Check if index exists
            if let Some(label_id) = Self::find_scan_label_id(&plan, &any_pred.variable) {
                let label_name = self.schema.label_name_by_id(label_id);
                if let Some(label) = label_name {
                    // Verify index exists in schema
                    let has_index = self.schema.indexes.iter().any(|idx| match idx {
                        IndexDefinition::Inverted(cfg) => {
                            cfg.label == label && cfg.property == any_pred.property
                        }
                        _ => false,
                    });

                    if has_index {
                        // Replace Scan with InvertedIndexLookup
                        plan = Self::replace_scan_with_inverted_lookup(
                            plan,
                            &any_pred.variable,
                            label_id,
                            &any_pred.property,
                            any_pred.terms.clone(),
                        );

                        if let Some(residual) = extraction.residual {
                            current_predicate = residual;
                        } else {
                            current_predicate = Expr::TRUE;
                        }
                    }
                }
            }
        }

        // 1. Try to extract vector_similarity predicate for optimization
        if let Some(extraction) = extract_vector_similarity(&current_predicate) {
            let vs = &extraction.predicate;
            if Self::find_scan_label_id(&plan, &vs.variable).is_some() {
                plan = Self::replace_scan_with_knn(
                    plan,
                    &vs.variable,
                    &vs.property,
                    vs.query.clone(),
                    vs.threshold,
                );
                if let Some(residual) = extraction.residual {
                    current_predicate = residual;
                } else {
                    current_predicate = Expr::TRUE;
                }
            }
        }

        // 3. Push eligible predicates to Scan OR Traverse filters
        // Note: Do NOT push predicates on optional variables (from OPTIONAL MATCH) to
        // Traverse's target_filter, because target_filter filtering doesn't preserve NULL
        // rows. Let them stay in the Filter operator which handles NULL preservation.
        for var in vars_in_scope {
            // Skip pushdown for optional variables - they need NULL preservation in Filter
            if optional_vars.contains(&var.name) {
                continue;
            }

            // Check if var is produced by a Scan
            if Self::find_scan_label_id(&plan, &var.name).is_some() {
                let (pushable, residual) =
                    Self::extract_variable_predicates(&current_predicate, &var.name);

                for pred in pushable {
                    plan = Self::push_predicate_to_scan(plan, &var.name, pred);
                }

                if let Some(r) = residual {
                    current_predicate = r;
                } else {
                    current_predicate = Expr::TRUE;
                }
            } else if Self::is_traverse_target(&plan, &var.name) {
                // Push to Traverse
                let (pushable, residual) =
                    Self::extract_variable_predicates(&current_predicate, &var.name);

                for pred in pushable {
                    plan = Self::push_predicate_to_traverse(plan, &var.name, pred);
                }

                if let Some(r) = residual {
                    current_predicate = r;
                } else {
                    current_predicate = Expr::TRUE;
                }
            }
        }

        // 4. Push predicates to Apply.input_filter
        // This filters input rows BEFORE executing correlated subqueries.
        plan = Self::push_predicates_to_apply(plan, &mut current_predicate);

        // 5. Add Filter node for any remaining predicates
        if !current_predicate.is_true_literal() {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: current_predicate,
                optional_variables: optional_vars,
            };
        }

        Ok(plan)
    }

    fn rewrite_predicates_using_indexes(
        &self,
        predicate: &Expr,
        plan: &LogicalPlan,
        vars_in_scope: &[VariableInfo],
    ) -> Result<Expr> {
        // ... (unchanged)
        let mut rewritten = predicate.clone();

        for var in vars_in_scope {
            if let Some(label_id) = Self::find_scan_label_id(plan, &var.name) {
                // Find label name
                let label_name = self.schema.label_name_by_id(label_id).map(str::to_owned);

                if let Some(label) = label_name
                    && let Some(props) = self.schema.properties.get(&label)
                {
                    for (gen_col, meta) in props {
                        if meta.generation_expression.is_some() {
                            // Use cached parsed expression
                            if let Some(schema_expr) =
                                self.gen_expr_cache.get(&(label.clone(), gen_col.clone()))
                            {
                                // Rewrite 'rewritten' replacing occurrences of schema_expr with gen_col
                                rewritten = Self::replace_expression(
                                    rewritten,
                                    schema_expr,
                                    &var.name,
                                    gen_col,
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(rewritten)
    }

    // ... (replace_expression unchanged) ...
    fn replace_expression(expr: Expr, schema_expr: &Expr, query_var: &str, gen_col: &str) -> Expr {
        // First, normalize schema_expr to use query_var
        let schema_var = schema_expr.extract_variable();

        if let Some(s_var) = schema_var {
            let target_expr = schema_expr.substitute_variable(&s_var, query_var);

            if expr == target_expr {
                return Expr::Property(
                    Box::new(Expr::Variable(query_var.to_string())),
                    gen_col.to_string(),
                );
            }
        }

        // Recurse
        match expr {
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::replace_expression(
                    *left,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                op,
                right: Box::new(Self::replace_expression(
                    *right,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::replace_expression(
                    *expr,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
            },
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => Expr::FunctionCall {
                name,
                args: args
                    .into_iter()
                    .map(|a| Self::replace_expression(a, schema_expr, query_var, gen_col))
                    .collect(),
                distinct,
                window_spec,
            },
            Expr::IsNull(expr) => Expr::IsNull(Box::new(Self::replace_expression(
                *expr,
                schema_expr,
                query_var,
                gen_col,
            ))),
            Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(Self::replace_expression(
                *expr,
                schema_expr,
                query_var,
                gen_col,
            ))),
            Expr::IsUnique(expr) => Expr::IsUnique(Box::new(Self::replace_expression(
                *expr,
                schema_expr,
                query_var,
                gen_col,
            ))),
            Expr::ArrayIndex {
                array: e,
                index: idx,
            } => Expr::ArrayIndex {
                array: Box::new(Self::replace_expression(
                    *e,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                index: Box::new(Self::replace_expression(
                    *idx,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
            },
            Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
                array: Box::new(Self::replace_expression(
                    *array,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                start: start.map(|s| {
                    Box::new(Self::replace_expression(
                        *s,
                        schema_expr,
                        query_var,
                        gen_col,
                    ))
                }),
                end: end.map(|e| {
                    Box::new(Self::replace_expression(
                        *e,
                        schema_expr,
                        query_var,
                        gen_col,
                    ))
                }),
            },
            Expr::List(exprs) => Expr::List(
                exprs
                    .into_iter()
                    .map(|e| Self::replace_expression(e, schema_expr, query_var, gen_col))
                    .collect(),
            ),
            Expr::Map(entries) => Expr::Map(
                entries
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            Self::replace_expression(v, schema_expr, query_var, gen_col),
                        )
                    })
                    .collect(),
            ),
            Expr::Property(e, prop) => Expr::Property(
                Box::new(Self::replace_expression(
                    *e,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                prop,
            ),
            Expr::Case {
                expr: case_expr,
                when_then,
                else_expr,
            } => Expr::Case {
                expr: case_expr.map(|e| {
                    Box::new(Self::replace_expression(
                        *e,
                        schema_expr,
                        query_var,
                        gen_col,
                    ))
                }),
                when_then: when_then
                    .into_iter()
                    .map(|(w, t)| {
                        (
                            Self::replace_expression(w, schema_expr, query_var, gen_col),
                            Self::replace_expression(t, schema_expr, query_var, gen_col),
                        )
                    })
                    .collect(),
                else_expr: else_expr.map(|e| {
                    Box::new(Self::replace_expression(
                        *e,
                        schema_expr,
                        query_var,
                        gen_col,
                    ))
                }),
            },
            Expr::Reduce {
                accumulator,
                init,
                variable: reduce_var,
                list,
                expr: reduce_expr,
            } => Expr::Reduce {
                accumulator,
                init: Box::new(Self::replace_expression(
                    *init,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                variable: reduce_var,
                list: Box::new(Self::replace_expression(
                    *list,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                expr: Box::new(Self::replace_expression(
                    *reduce_expr,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
            },

            // Leaf nodes (Identifier, Literal, Parameter, etc.) need no recursion
            _ => expr,
        }
    }

    /// Check if the variable is the target of a Traverse node
    fn is_traverse_target(plan: &LogicalPlan, variable: &str) -> bool {
        match plan {
            LogicalPlan::Traverse {
                target_variable,
                input,
                ..
            } => target_variable == variable || Self::is_traverse_target(input, variable),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Apply { input, .. } => Self::is_traverse_target(input, variable),
            LogicalPlan::CrossJoin { left, right } => {
                Self::is_traverse_target(left, variable)
                    || Self::is_traverse_target(right, variable)
            }
            _ => false,
        }
    }

    /// Push a predicate into a Traverse's target_filter for the specified variable
    fn push_predicate_to_traverse(
        plan: LogicalPlan,
        variable: &str,
        predicate: Expr,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::Traverse {
                input,
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id,
                step_variable,
                min_hops,
                max_hops,
                optional,
                target_filter,
                path_variable,
                edge_properties,
                optional_pattern_vars,
            } => {
                if target_variable == variable {
                    // Found the traverse producing this variable
                    let new_filter = match target_filter {
                        Some(existing) => Some(Expr::BinaryOp {
                            left: Box::new(existing),
                            op: BinaryOp::And,
                            right: Box::new(predicate),
                        }),
                        None => Some(predicate),
                    };
                    LogicalPlan::Traverse {
                        input,
                        edge_type_ids,
                        direction,
                        source_variable,
                        target_variable,
                        target_label_id,
                        step_variable,
                        min_hops,
                        max_hops,
                        optional,
                        target_filter: new_filter,
                        path_variable,
                        edge_properties,
                        optional_pattern_vars,
                    }
                } else {
                    // Recurse into input
                    LogicalPlan::Traverse {
                        input: Box::new(Self::push_predicate_to_traverse(
                            *input, variable, predicate,
                        )),
                        edge_type_ids,
                        direction,
                        source_variable,
                        target_variable,
                        target_label_id,
                        step_variable,
                        min_hops,
                        max_hops,
                        optional,
                        target_filter,
                        path_variable,
                        edge_properties,
                        optional_pattern_vars,
                    }
                }
            }
            LogicalPlan::Filter {
                input,
                predicate: p,
                optional_variables: opt_vars,
            } => LogicalPlan::Filter {
                input: Box::new(Self::push_predicate_to_traverse(
                    *input, variable, predicate,
                )),
                predicate: p,
                optional_variables: opt_vars,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::push_predicate_to_traverse(
                    *input, variable, predicate,
                )),
                projections,
            },
            LogicalPlan::CrossJoin { left, right } => {
                // Check which side has the variable
                if Self::is_traverse_target(&left, variable) {
                    LogicalPlan::CrossJoin {
                        left: Box::new(Self::push_predicate_to_traverse(
                            *left, variable, predicate,
                        )),
                        right,
                    }
                } else {
                    LogicalPlan::CrossJoin {
                        left,
                        right: Box::new(Self::push_predicate_to_traverse(
                            *right, variable, predicate,
                        )),
                    }
                }
            }
            other => other,
        }
    }

    /// Plan a WITH clause, handling aggregations and projections.
    fn plan_with_clause(
        &self,
        with_clause: &WithClause,
        plan: LogicalPlan,
        vars_in_scope: &[VariableInfo],
    ) -> Result<(LogicalPlan, Vec<VariableInfo>)> {
        let mut plan = plan;
        let mut group_by: Vec<Expr> = Vec::new();
        let mut aggregates: Vec<Expr> = Vec::new();
        let mut has_agg = false;
        let mut projections = Vec::new();
        let mut new_vars: Vec<VariableInfo> = Vec::new();

        for item in &with_clause.items {
            match item {
                ReturnItem::All => {
                    // WITH * - add all variables in scope
                    for v in vars_in_scope {
                        projections.push((Expr::Variable(v.name.clone()), Some(v.name.clone())));
                    }
                    new_vars.extend(vars_in_scope.iter().cloned());
                }
                ReturnItem::Expr { expr, alias } => {
                    if matches!(expr, Expr::Wildcard) {
                        for v in vars_in_scope {
                            projections
                                .push((Expr::Variable(v.name.clone()), Some(v.name.clone())));
                        }
                        new_vars.extend(vars_in_scope.iter().cloned());
                    } else {
                        projections.push((expr.clone(), alias.clone()));
                        if expr.is_aggregate() {
                            has_agg = true;
                            aggregates.push(expr.clone());
                        } else if !group_by.contains(expr) {
                            group_by.push(expr.clone());
                        }

                        // WITH creates scalar bindings for its outputs
                        if let Some(a) = alias {
                            new_vars.push(VariableInfo::new(a.clone(), VariableType::Scalar));
                        } else if let Expr::Variable(v) = expr {
                            // Preserve the original type if the variable is just passed through
                            if let Some(existing) = find_var_in_scope(vars_in_scope, v) {
                                new_vars.push(existing.clone());
                            } else {
                                new_vars.push(VariableInfo::new(v.clone(), VariableType::Scalar));
                            }
                        }
                    }
                }
            }
        }

        if has_agg {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
            };

            // Insert a renaming Project so downstream clauses (WHERE, RETURN)
            // can reference the WITH aliases instead of raw column names.
            let rename_projections: Vec<(Expr, Option<String>)> = projections
                .iter()
                .map(|(expr, alias)| {
                    let col_name = if expr.is_aggregate() {
                        aggregate_column_name(expr)
                    } else {
                        expr.to_string_repr()
                    };
                    (Expr::Variable(col_name), alias.clone())
                })
                .collect();
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections: rename_projections,
            };
        } else if !projections.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections,
            };
        }

        // Validate and apply ORDER BY for WITH clause
        // Build an extended scope that includes both WITH aliases and original variables
        // so ORDER BY can reference either (mirrors RETURN ORDER BY handling).
        if let Some(order_by) = &with_clause.order_by {
            let order_by_scope: Vec<VariableInfo> = {
                let mut scope = new_vars.clone();
                for v in vars_in_scope {
                    if !is_var_in_scope(&scope, &v.name) {
                        scope.push(v.clone());
                    }
                }
                scope
            };
            for item in order_by {
                validate_expression_variables(&item.expr, &order_by_scope)?;
                validate_expression(&item.expr, &order_by_scope)?;
                // Aggregation functions not allowed in ORDER BY of WITH
                if contains_aggregate_recursive(&item.expr) {
                    return Err(anyhow!(
                        "SyntaxError: InvalidAggregation - Aggregation functions not allowed in ORDER BY of WITH"
                    ));
                }
            }
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: order_by.clone(),
            };
        }

        // Validate and apply SKIP/LIMIT for WITH clause
        let skip = with_clause
            .skip
            .as_ref()
            .map(|e| parse_non_negative_integer(e, "SKIP"))
            .transpose()?
            .flatten();
        let fetch = with_clause
            .limit
            .as_ref()
            .map(|e| parse_non_negative_integer(e, "LIMIT"))
            .transpose()?
            .flatten();

        if skip.is_some() || fetch.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                skip,
                fetch,
            };
        }

        if let Some(predicate) = &with_clause.where_clause {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: predicate.clone(),
                optional_variables: std::collections::HashSet::new(),
            };
        }

        if with_clause.distinct {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        Ok((plan, new_vars))
    }

    fn plan_with_recursive(
        &self,
        with_recursive: &WithRecursiveClause,
        _prev_plan: LogicalPlan,
        vars_in_scope: &[VariableInfo],
    ) -> Result<LogicalPlan> {
        // WITH RECURSIVE requires a UNION query with anchor and recursive parts
        match &*with_recursive.query {
            Query::Union { left, right, .. } => {
                // Plan the anchor (initial) query with current scope
                let initial_plan = self.rewrite_and_plan_typed(*left.clone(), vars_in_scope)?;

                // Plan the recursive query with the CTE name added to scope
                // so it can reference itself
                let mut recursive_scope = vars_in_scope.to_vec();
                recursive_scope.push(VariableInfo::new(
                    with_recursive.name.clone(),
                    VariableType::Scalar,
                ));
                let recursive_plan =
                    self.rewrite_and_plan_typed(*right.clone(), &recursive_scope)?;

                Ok(LogicalPlan::RecursiveCTE {
                    cte_name: with_recursive.name.clone(),
                    initial: Box::new(initial_plan),
                    recursive: Box::new(recursive_plan),
                })
            }
            _ => Err(anyhow::anyhow!(
                "WITH RECURSIVE requires a UNION query with anchor and recursive parts"
            )),
        }
    }

    pub fn properties_to_expr(&self, variable: &str, properties: &Option<Expr>) -> Option<Expr> {
        let entries = match properties {
            Some(Expr::Map(entries)) => entries,
            _ => return None,
        };

        if entries.is_empty() {
            return None;
        }
        let mut final_expr = None;
        for (prop, val_expr) in entries {
            let eq_expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable(variable.to_string())),
                    prop.clone(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(val_expr.clone()),
            };

            if let Some(e) = final_expr {
                final_expr = Some(Expr::BinaryOp {
                    left: Box::new(e),
                    op: BinaryOp::And,
                    right: Box::new(eq_expr),
                });
            } else {
                final_expr = Some(eq_expr);
            }
        }
        final_expr
    }

    /// Build a filter expression from node properties and labels.
    ///
    /// This is used for TraverseMainByType where we need to filter target nodes
    /// by both labels and properties. Label checks use hasLabel(variable, 'label').
    pub fn node_filter_expr(
        &self,
        variable: &str,
        labels: &[String],
        properties: &Option<Expr>,
    ) -> Option<Expr> {
        let mut final_expr = None;

        // Add label checks using hasLabel(variable, 'label')
        for label in labels {
            let label_check = Expr::FunctionCall {
                name: "hasLabel".to_string(),
                args: vec![
                    Expr::Variable(variable.to_string()),
                    Expr::Literal(CypherLiteral::String(label.clone())),
                ],
                distinct: false,
                window_spec: None,
            };

            final_expr = match final_expr {
                Some(e) => Some(Expr::BinaryOp {
                    left: Box::new(e),
                    op: BinaryOp::And,
                    right: Box::new(label_check),
                }),
                None => Some(label_check),
            };
        }

        // Add property checks
        if let Some(prop_expr) = self.properties_to_expr(variable, properties) {
            final_expr = match final_expr {
                Some(e) => Some(Expr::BinaryOp {
                    left: Box::new(e),
                    op: BinaryOp::And,
                    right: Box::new(prop_expr),
                }),
                None => Some(prop_expr),
            };
        }

        final_expr
    }

    /// Create a filter plan that ensures traversed target matches a bound variable.
    ///
    /// Used in EXISTS subquery patterns where the target is already bound.
    /// Compares the target's VID against the parameter value.
    fn wrap_with_bound_target_filter(plan: LogicalPlan, target_variable: &str) -> LogicalPlan {
        let bound_check = Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable(target_variable.to_string())),
                "_vid".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Property(
                Box::new(Expr::Parameter(target_variable.to_string())),
                "_vid".to_string(),
            )),
        };
        LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: bound_check,
            optional_variables: std::collections::HashSet::new(),
        }
    }

    /// Replace a Scan node matching the variable with a VectorKnn node
    fn replace_scan_with_knn(
        plan: LogicalPlan,
        variable: &str,
        property: &str,
        query: Expr,
        threshold: Option<f32>,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan {
                label_id,
                labels,
                variable: scan_var,
                filter,
                optional,
            } => {
                if scan_var == variable {
                    // Inject any existing scan filter into VectorKnn?
                    // VectorKnn doesn't support pre-filtering natively in logical plan yet (except threshold).
                    // Typically filter is applied post-Knn or during Knn if supported.
                    // For now, we assume filter is residual or handled by `extract_vector_similarity` which separates residual.
                    // If `filter` is present on Scan, it must be preserved.
                    // We can wrap VectorKnn in Filter if Scan had filter.

                    let knn = LogicalPlan::VectorKnn {
                        label_id,
                        variable: variable.to_string(),
                        property: property.to_string(),
                        query,
                        k: 100, // Default K, should push down LIMIT
                        threshold,
                    };

                    if let Some(f) = filter {
                        LogicalPlan::Filter {
                            input: Box::new(knn),
                            predicate: f,
                            optional_variables: std::collections::HashSet::new(),
                        }
                    } else {
                        knn
                    }
                } else {
                    LogicalPlan::Scan {
                        label_id,
                        labels,
                        variable: scan_var.clone(),
                        filter,
                        optional,
                    }
                }
            }
            LogicalPlan::Filter {
                input,
                predicate,
                optional_variables,
            } => LogicalPlan::Filter {
                input: Box::new(Self::replace_scan_with_knn(
                    *input, variable, property, query, threshold,
                )),
                predicate,
                optional_variables,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::replace_scan_with_knn(
                    *input, variable, property, query, threshold,
                )),
                projections,
            },
            LogicalPlan::Limit { input, skip, fetch } => {
                // If we encounter Limit, we should ideally push K down to VectorKnn
                // But replace_scan_with_knn is called from plan_where_clause which is inside plan_match.
                // Limit comes later.
                // To support Limit pushdown, we need a separate optimizer pass or do it in plan_single.
                LogicalPlan::Limit {
                    input: Box::new(Self::replace_scan_with_knn(
                        *input, variable, property, query, threshold,
                    )),
                    skip,
                    fetch,
                }
            }
            LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
                left: Box::new(Self::replace_scan_with_knn(
                    *left,
                    variable,
                    property,
                    query.clone(),
                    threshold,
                )),
                right: Box::new(Self::replace_scan_with_knn(
                    *right, variable, property, query, threshold,
                )),
            },
            other => other,
        }
    }

    /// Find the label_id for a Scan node matching the given variable
    fn find_scan_label_id(plan: &LogicalPlan, variable: &str) -> Option<u16> {
        match plan {
            LogicalPlan::Scan {
                label_id,
                variable: var,
                ..
            } if var == variable => Some(*label_id),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Apply { input, .. } => Self::find_scan_label_id(input, variable),
            LogicalPlan::CrossJoin { left, right } => Self::find_scan_label_id(left, variable)
                .or_else(|| Self::find_scan_label_id(right, variable)),
            LogicalPlan::Traverse { input, .. } => Self::find_scan_label_id(input, variable),
            _ => None,
        }
    }

    fn replace_scan_with_inverted_lookup(
        plan: LogicalPlan,
        variable: &str,
        label_id: u16,
        property: &str,
        terms: Expr,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan { variable: v, .. } if v == variable => {
                LogicalPlan::InvertedIndexLookup {
                    label_id,
                    variable: v,
                    property: property.to_string(),
                    terms,
                }
            }
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::replace_scan_with_inverted_lookup(
                    *input, variable, label_id, property, terms,
                )),
                projections,
            },
            LogicalPlan::Filter {
                input,
                predicate,
                optional_variables,
            } => LogicalPlan::Filter {
                input: Box::new(Self::replace_scan_with_inverted_lookup(
                    *input, variable, label_id, property, terms,
                )),
                predicate,
                optional_variables,
            },
            LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
                left: Box::new(Self::replace_scan_with_inverted_lookup(
                    *left,
                    variable,
                    label_id,
                    property,
                    terms.clone(),
                )),
                right: Box::new(Self::replace_scan_with_inverted_lookup(
                    *right, variable, label_id, property, terms,
                )),
            },
            _ => plan,
        }
    }

    /// Push a predicate into a Scan's filter for the specified variable
    fn push_predicate_to_scan(plan: LogicalPlan, variable: &str, predicate: Expr) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan {
                label_id,
                labels,
                variable: var,
                filter,
                optional,
            } if var == variable => {
                // Merge the predicate with existing filter
                let new_filter = match filter {
                    Some(existing) => Some(Expr::BinaryOp {
                        left: Box::new(existing),
                        op: BinaryOp::And,
                        right: Box::new(predicate),
                    }),
                    None => Some(predicate),
                };
                LogicalPlan::Scan {
                    label_id,
                    labels,
                    variable: var,
                    filter: new_filter,
                    optional,
                }
            }
            LogicalPlan::Filter {
                input,
                predicate: p,
                optional_variables: opt_vars,
            } => LogicalPlan::Filter {
                input: Box::new(Self::push_predicate_to_scan(*input, variable, predicate)),
                predicate: p,
                optional_variables: opt_vars,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::push_predicate_to_scan(*input, variable, predicate)),
                projections,
            },
            LogicalPlan::CrossJoin { left, right } => {
                // Check which side has the variable
                if Self::find_scan_label_id(&left, variable).is_some() {
                    LogicalPlan::CrossJoin {
                        left: Box::new(Self::push_predicate_to_scan(*left, variable, predicate)),
                        right,
                    }
                } else {
                    LogicalPlan::CrossJoin {
                        left,
                        right: Box::new(Self::push_predicate_to_scan(*right, variable, predicate)),
                    }
                }
            }
            LogicalPlan::Traverse {
                input,
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id,
                step_variable,
                min_hops,
                max_hops,
                optional,
                target_filter,
                path_variable,
                edge_properties,
                optional_pattern_vars,
            } => LogicalPlan::Traverse {
                input: Box::new(Self::push_predicate_to_scan(*input, variable, predicate)),
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id,
                step_variable,
                min_hops,
                max_hops,
                optional,
                target_filter,
                path_variable,
                edge_properties,
                optional_pattern_vars,
            },
            other => other,
        }
    }

    /// Extract predicates that reference only the specified variable
    fn extract_variable_predicates(predicate: &Expr, variable: &str) -> (Vec<Expr>, Option<Expr>) {
        let analyzer = PredicateAnalyzer::new(None);
        let analysis = analyzer.analyze(predicate, variable);

        // Return pushable predicates and combined residual
        let residual = if analysis.residual.is_empty() {
            None
        } else {
            let mut iter = analysis.residual.into_iter();
            let first = iter.next().unwrap();
            Some(iter.fold(first, |acc, e| Expr::BinaryOp {
                left: Box::new(acc),
                op: BinaryOp::And,
                right: Box::new(e),
            }))
        };

        (analysis.pushable, residual)
    }

    // =====================================================================
    // Apply Predicate Pushdown - Helper Functions
    // =====================================================================

    /// Split AND-connected predicates into a list.
    fn split_and_conjuncts(expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut result = Self::split_and_conjuncts(left);
                result.extend(Self::split_and_conjuncts(right));
                result
            }
            _ => vec![expr.clone()],
        }
    }

    /// Combine predicates with AND.
    fn combine_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        if predicates.is_empty() {
            return None;
        }
        let mut result = predicates[0].clone();
        for pred in predicates.iter().skip(1) {
            result = Expr::BinaryOp {
                left: Box::new(result),
                op: BinaryOp::And,
                right: Box::new(pred.clone()),
            };
        }
        Some(result)
    }

    /// Collect all variable names referenced in an expression.
    fn collect_expr_variables(expr: &Expr) -> std::collections::HashSet<String> {
        let mut vars = std::collections::HashSet::new();
        Self::collect_expr_variables_impl(expr, &mut vars);
        vars
    }

    fn collect_expr_variables_impl(expr: &Expr, vars: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::Variable(name) => {
                vars.insert(name.clone());
            }
            Expr::Property(inner, _) => {
                if let Expr::Variable(name) = inner.as_ref() {
                    vars.insert(name.clone());
                } else {
                    Self::collect_expr_variables_impl(inner, vars);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_expr_variables_impl(left, vars);
                Self::collect_expr_variables_impl(right, vars);
            }
            Expr::UnaryOp { expr, .. } => Self::collect_expr_variables_impl(expr, vars),
            Expr::IsNull(e) | Expr::IsNotNull(e) => Self::collect_expr_variables_impl(e, vars),
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    Self::collect_expr_variables_impl(arg, vars);
                }
            }
            Expr::List(items) => {
                for item in items {
                    Self::collect_expr_variables_impl(item, vars);
                }
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                if let Some(e) = expr {
                    Self::collect_expr_variables_impl(e, vars);
                }
                for (w, t) in when_then {
                    Self::collect_expr_variables_impl(w, vars);
                    Self::collect_expr_variables_impl(t, vars);
                }
                if let Some(e) = else_expr {
                    Self::collect_expr_variables_impl(e, vars);
                }
            }
            _ => {}
        }
    }

    /// Collect all variables produced by a logical plan.
    fn collect_plan_variables(plan: &LogicalPlan) -> std::collections::HashSet<String> {
        let mut vars = std::collections::HashSet::new();
        Self::collect_plan_variables_impl(plan, &mut vars);
        vars
    }

    fn collect_plan_variables_impl(
        plan: &LogicalPlan,
        vars: &mut std::collections::HashSet<String>,
    ) {
        match plan {
            LogicalPlan::Scan { variable, .. } => {
                vars.insert(variable.clone());
            }
            LogicalPlan::Traverse {
                target_variable,
                step_variable,
                input,
                path_variable,
                ..
            } => {
                vars.insert(target_variable.clone());
                if let Some(sv) = step_variable {
                    vars.insert(sv.clone());
                }
                if let Some(pv) = path_variable {
                    vars.insert(pv.clone());
                }
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Filter { input, .. } => Self::collect_plan_variables_impl(input, vars),
            LogicalPlan::Project { input, projections } => {
                for (expr, alias) in projections {
                    if let Some(a) = alias {
                        vars.insert(a.clone());
                    } else if let Expr::Variable(v) = expr {
                        vars.insert(v.clone());
                    }
                }
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Apply {
                input, subquery, ..
            } => {
                Self::collect_plan_variables_impl(input, vars);
                Self::collect_plan_variables_impl(subquery, vars);
            }
            LogicalPlan::CrossJoin { left, right } => {
                Self::collect_plan_variables_impl(left, vars);
                Self::collect_plan_variables_impl(right, vars);
            }
            LogicalPlan::Unwind {
                input, variable, ..
            } => {
                vars.insert(variable.clone());
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Aggregate { input, .. } => {
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Distinct { input } => {
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Sort { input, .. } => {
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Limit { input, .. } => {
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::VectorKnn { variable, .. } => {
                vars.insert(variable.clone());
            }
            LogicalPlan::ProcedureCall { yield_items, .. } => {
                for (name, alias) in yield_items {
                    vars.insert(alias.clone().unwrap_or_else(|| name.clone()));
                }
            }
            LogicalPlan::ShortestPath {
                input,
                path_variable,
                ..
            } => {
                vars.insert(path_variable.clone());
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::AllShortestPaths {
                input,
                path_variable,
                ..
            } => {
                vars.insert(path_variable.clone());
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => {
                Self::collect_plan_variables_impl(initial, vars);
                Self::collect_plan_variables_impl(recursive, vars);
            }
            LogicalPlan::SubqueryCall {
                input, subquery, ..
            } => {
                Self::collect_plan_variables_impl(input, vars);
                Self::collect_plan_variables_impl(subquery, vars);
            }
            _ => {}
        }
    }

    /// Extract predicates that only reference variables from Apply's input.
    /// Returns (input_only_predicates, remaining_predicates).
    fn extract_apply_input_predicates(
        predicate: &Expr,
        input_variables: &std::collections::HashSet<String>,
        subquery_new_variables: &std::collections::HashSet<String>,
    ) -> (Vec<Expr>, Vec<Expr>) {
        let conjuncts = Self::split_and_conjuncts(predicate);
        let mut input_preds = Vec::new();
        let mut remaining = Vec::new();

        for conj in conjuncts {
            let vars = Self::collect_expr_variables(&conj);

            // Predicate only references input variables (none from subquery)
            let refs_input_only = vars.iter().all(|v| input_variables.contains(v));
            let refs_any_subquery = vars.iter().any(|v| subquery_new_variables.contains(v));

            if refs_input_only && !refs_any_subquery && !vars.is_empty() {
                input_preds.push(conj);
            } else {
                remaining.push(conj);
            }
        }

        (input_preds, remaining)
    }

    /// Push eligible predicates into Apply.input_filter.
    /// This filters input rows BEFORE executing the correlated subquery.
    fn push_predicates_to_apply(plan: LogicalPlan, current_predicate: &mut Expr) -> LogicalPlan {
        match plan {
            LogicalPlan::Apply {
                input,
                subquery,
                input_filter,
            } => {
                // Collect variables from input plan
                let input_vars = Self::collect_plan_variables(&input);

                // Collect NEW variables introduced by subquery (not in input)
                let subquery_vars = Self::collect_plan_variables(&subquery);
                let new_subquery_vars: std::collections::HashSet<String> =
                    subquery_vars.difference(&input_vars).cloned().collect();

                // Extract predicates that only reference input variables
                let (input_preds, remaining) = Self::extract_apply_input_predicates(
                    current_predicate,
                    &input_vars,
                    &new_subquery_vars,
                );

                // Update current_predicate to only remaining predicates
                *current_predicate = if remaining.is_empty() {
                    Expr::TRUE
                } else {
                    Self::combine_predicates(remaining).unwrap()
                };

                // Combine extracted predicates with existing input_filter
                let new_input_filter = if input_preds.is_empty() {
                    input_filter
                } else {
                    let extracted = Self::combine_predicates(input_preds).unwrap();
                    match input_filter {
                        Some(existing) => Some(Expr::BinaryOp {
                            left: Box::new(existing),
                            op: BinaryOp::And,
                            right: Box::new(extracted),
                        }),
                        None => Some(extracted),
                    }
                };

                // Recurse into input plan
                let new_input = Self::push_predicates_to_apply(*input, current_predicate);

                LogicalPlan::Apply {
                    input: Box::new(new_input),
                    subquery,
                    input_filter: new_input_filter,
                }
            }
            // Recurse into other plan nodes
            LogicalPlan::Filter {
                input,
                predicate,
                optional_variables,
            } => LogicalPlan::Filter {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                predicate,
                optional_variables,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                projections,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                order_by,
            },
            LogicalPlan::Limit { input, skip, fetch } => LogicalPlan::Limit {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                skip,
                fetch,
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                group_by,
                aggregates,
            },
            LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
                left: Box::new(Self::push_predicates_to_apply(*left, current_predicate)),
                right: Box::new(Self::push_predicates_to_apply(*right, current_predicate)),
            },
            LogicalPlan::Traverse {
                input,
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id,
                step_variable,
                min_hops,
                max_hops,
                optional,
                target_filter,
                path_variable,
                edge_properties,
                optional_pattern_vars,
            } => LogicalPlan::Traverse {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id,
                step_variable,
                min_hops,
                max_hops,
                optional,
                target_filter,
                path_variable,
                edge_properties,
                optional_pattern_vars,
            },
            other => other,
        }
    }

    /// Get the column name for an aggregate expression.
    /// This must match the logic in executor's build_aggregate_result.
    fn get_aggregate_column_name(expr: &Expr) -> String {
        aggregate_column_name(expr)
    }
}

/// Get the expected column name for an aggregate expression.
///
/// This must match the logic in executor's `build_aggregate_result` and is used
/// by both the logical planner (to create column references) and the physical
/// planner (to rename DataFusion's auto-generated aggregate column names).
pub fn aggregate_column_name(expr: &Expr) -> String {
    match expr {
        Expr::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            // Special-case COUNT to uppercase the function name
            if name.eq_ignore_ascii_case("count") {
                if args.is_empty() {
                    // COUNT(*) - empty args
                    "COUNT(*)".to_string()
                } else {
                    // COUNT(expr) - format with uppercase COUNT
                    let args_str: Vec<_> = args.iter().map(|e| e.to_string_repr()).collect();
                    let distinct_str = if *distinct { "DISTINCT " } else { "" };
                    format!("COUNT({}{})", distinct_str, args_str.join(", "))
                }
            } else {
                expr.to_string_repr()
            }
        }
        _ => expr.to_string_repr(),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExplainOutput {
    pub plan_text: String,
    pub index_usage: Vec<IndexUsage>,
    pub cost_estimates: CostEstimates,
    pub warnings: Vec<String>,
    pub suggestions: Vec<IndexSuggestion>,
}

/// Suggestion for creating an index to improve query performance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexSuggestion {
    pub label_or_type: String,
    pub property: String,
    pub index_type: String,
    pub reason: String,
    pub create_statement: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexUsage {
    pub label_or_type: String,
    pub property: String,
    pub index_type: String,
    pub used: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CostEstimates {
    pub estimated_rows: f64,
    pub estimated_cost: f64,
}

impl QueryPlanner {
    pub fn explain_plan(&self, ast: Query) -> Result<ExplainOutput> {
        let plan = self.plan(ast)?;
        self.explain_logical_plan(&plan)
    }

    pub fn explain_logical_plan(&self, plan: &LogicalPlan) -> Result<ExplainOutput> {
        let index_usage = self.analyze_index_usage(plan)?;
        let cost_estimates = self.estimate_costs(plan)?;
        let suggestions = self.collect_index_suggestions(plan);
        let warnings = Vec::new();
        let plan_text = format!("{:#?}", plan);

        Ok(ExplainOutput {
            plan_text,
            index_usage,
            cost_estimates,
            warnings,
            suggestions,
        })
    }

    fn analyze_index_usage(&self, plan: &LogicalPlan) -> Result<Vec<IndexUsage>> {
        let mut usage = Vec::new();
        self.collect_index_usage(plan, &mut usage);
        Ok(usage)
    }

    fn collect_index_usage(&self, plan: &LogicalPlan, usage: &mut Vec<IndexUsage>) {
        match plan {
            LogicalPlan::Scan { .. } => {
                // Placeholder: Scan might use index if it was optimized
                // Ideally LogicalPlan::Scan should store if it uses index.
                // But typically Planner converts Scan to specific index scan or we infer it here.
            }
            LogicalPlan::VectorKnn {
                label_id, property, ..
            } => {
                let label_name = self.schema.label_name_by_id(*label_id).unwrap_or("?");
                usage.push(IndexUsage {
                    label_or_type: label_name.to_string(),
                    property: property.clone(),
                    index_type: "VECTOR".to_string(),
                    used: true,
                    reason: None,
                });
            }
            LogicalPlan::Explain { plan } => self.collect_index_usage(plan, usage),
            LogicalPlan::Filter { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Project { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Limit { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Sort { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Aggregate { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Traverse { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                self.collect_index_usage(left, usage);
                self.collect_index_usage(right, usage);
            }
            _ => {}
        }
    }

    fn estimate_costs(&self, _plan: &LogicalPlan) -> Result<CostEstimates> {
        Ok(CostEstimates {
            estimated_rows: 100.0,
            estimated_cost: 10.0,
        })
    }

    /// Collect index suggestions based on query patterns.
    ///
    /// Currently detects:
    /// - Temporal predicates from `uni.validAt()` function calls
    /// - Temporal predicates from `VALID_AT` macro expansion
    fn collect_index_suggestions(&self, plan: &LogicalPlan) -> Vec<IndexSuggestion> {
        let mut suggestions = Vec::new();
        self.collect_temporal_suggestions(plan, &mut suggestions);
        suggestions
    }

    /// Recursively collect temporal index suggestions from the plan.
    fn collect_temporal_suggestions(
        &self,
        plan: &LogicalPlan,
        suggestions: &mut Vec<IndexSuggestion>,
    ) {
        match plan {
            LogicalPlan::Filter {
                input, predicate, ..
            } => {
                // Check for temporal patterns in the predicate
                self.detect_temporal_pattern(predicate, suggestions);
                // Recurse into input
                self.collect_temporal_suggestions(input, suggestions);
            }
            LogicalPlan::Explain { plan } => self.collect_temporal_suggestions(plan, suggestions),
            LogicalPlan::Project { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Limit { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Sort { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Aggregate { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Traverse { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                self.collect_temporal_suggestions(left, suggestions);
                self.collect_temporal_suggestions(right, suggestions);
            }
            _ => {}
        }
    }

    /// Detect temporal predicate patterns and suggest indexes.
    ///
    /// Detects two patterns:
    /// 1. `uni.validAt(node, 'start_prop', 'end_prop', time)` function call
    /// 2. `node.valid_from <= time AND (node.valid_to IS NULL OR node.valid_to > time)` from VALID_AT macro
    fn detect_temporal_pattern(&self, expr: &Expr, suggestions: &mut Vec<IndexSuggestion>) {
        match expr {
            // Pattern 1: uni.temporal.validAt() function call
            Expr::FunctionCall { name, args, .. }
                if name.eq_ignore_ascii_case("uni.temporal.validAt")
                    || name.eq_ignore_ascii_case("validAt") =>
            {
                // args[0] = node, args[1] = start_prop, args[2] = end_prop, args[3] = time
                if args.len() >= 2 {
                    let start_prop =
                        if let Some(Expr::Literal(CypherLiteral::String(s))) = args.get(1) {
                            s.clone()
                        } else {
                            "valid_from".to_string()
                        };

                    // Try to extract label from the node expression
                    if let Some(var) = args.first().and_then(|e| e.extract_variable()) {
                        self.suggest_temporal_index(&var, &start_prop, suggestions);
                    }
                }
            }

            // Pattern 2: VALID_AT macro expansion - look for property <= time pattern
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                // Check left side for `prop <= time` pattern (temporal start condition)
                if let Expr::BinaryOp {
                    left: prop_expr,
                    op: BinaryOp::LtEq,
                    ..
                } = left.as_ref()
                    && let Expr::Property(base, prop_name) = prop_expr.as_ref()
                    && (prop_name == "valid_from"
                        || prop_name.contains("start")
                        || prop_name.contains("from")
                        || prop_name.contains("begin"))
                    && let Some(var) = base.extract_variable()
                {
                    self.suggest_temporal_index(&var, prop_name, suggestions);
                }

                // Recurse into both sides of AND
                self.detect_temporal_pattern(left.as_ref(), suggestions);
                self.detect_temporal_pattern(right.as_ref(), suggestions);
            }

            // Recurse into other binary ops
            Expr::BinaryOp { left, right, .. } => {
                self.detect_temporal_pattern(left.as_ref(), suggestions);
                self.detect_temporal_pattern(right.as_ref(), suggestions);
            }

            _ => {}
        }
    }

    /// Suggest a scalar index for a temporal property if one doesn't already exist.
    fn suggest_temporal_index(
        &self,
        _variable: &str,
        property: &str,
        suggestions: &mut Vec<IndexSuggestion>,
    ) {
        // Check if a scalar index already exists for this property
        // We need to check all labels since we may not know the exact label from the variable
        let mut has_index = false;

        for index in &self.schema.indexes {
            if let IndexDefinition::Scalar(config) = index
                && config.properties.contains(&property.to_string())
            {
                has_index = true;
                break;
            }
        }

        if !has_index {
            // Avoid duplicate suggestions
            let already_suggested = suggestions.iter().any(|s| s.property == property);
            if !already_suggested {
                suggestions.push(IndexSuggestion {
                    label_or_type: "(detected from temporal query)".to_string(),
                    property: property.to_string(),
                    index_type: "SCALAR (BTree)".to_string(),
                    reason: format!(
                        "Temporal queries using '{}' can benefit from a scalar index for range scans",
                        property
                    ),
                    create_statement: format!(
                        "CREATE INDEX idx_{} FOR (n:YourLabel) ON (n.{})",
                        property, property
                    ),
                });
            }
        }
    }

    /// Helper functions for expression normalization
    /// Normalize an expression for storage: strip variable prefixes
    /// For simple property: u.email -> "email"
    /// For expressions: lower(u.email) -> "lower(email)"
    fn normalize_expression_for_storage(expr: &Expr) -> String {
        match expr {
            Expr::Property(base, prop) if matches!(**base, Expr::Variable(_)) => prop.clone(),
            _ => {
                // Serialize expression and strip variable prefix
                let expr_str = expr.to_string_repr();
                Self::strip_variable_prefix(&expr_str)
            }
        }
    }

    /// Strip variable references like "u.prop" from expression strings
    /// Converts "lower(u.email)" to "lower(email)"
    fn strip_variable_prefix(expr_str: &str) -> String {
        use regex::Regex;
        // Match patterns like "word.property" and replace with just "property"
        let re = Regex::new(r"\b\w+\.(\w+)").unwrap();
        re.replace_all(expr_str, "$1").to_string()
    }

    /// Plan a schema command from the new AST
    fn plan_schema_command(&self, cmd: SchemaCommand) -> Result<LogicalPlan> {
        match cmd {
            SchemaCommand::CreateVectorIndex(c) => {
                // Parse index type from options (default: IvfPq)
                let index_type = if let Some(type_val) = c.options.get("type") {
                    match type_val.as_str() {
                        Some("hnsw") => VectorIndexType::Hnsw {
                            m: 16,
                            ef_construction: 200,
                            ef_search: 100,
                        },
                        Some("flat") => VectorIndexType::Flat,
                        _ => VectorIndexType::IvfPq {
                            num_partitions: 256,
                            num_sub_vectors: 16,
                            bits_per_subvector: 8,
                        },
                    }
                } else {
                    VectorIndexType::IvfPq {
                        num_partitions: 256,
                        num_sub_vectors: 16,
                        bits_per_subvector: 8,
                    }
                };

                // Parse embedding config from options
                let embedding_config = if let Some(emb_val) = c.options.get("embedding") {
                    Self::parse_embedding_config(emb_val)?
                } else {
                    None
                };

                let config = VectorIndexConfig {
                    name: c.name,
                    label: c.label,
                    property: c.property,
                    metric: DistanceMetric::Cosine,
                    index_type,
                    embedding_config,
                };
                Ok(LogicalPlan::CreateVectorIndex {
                    config,
                    if_not_exists: c.if_not_exists,
                })
            }
            SchemaCommand::CreateFullTextIndex(cfg) => Ok(LogicalPlan::CreateFullTextIndex {
                config: FullTextIndexConfig {
                    name: cfg.name,
                    label: cfg.label,
                    properties: cfg.properties,
                    tokenizer: TokenizerConfig::Standard,
                    with_positions: true,
                },
                if_not_exists: cfg.if_not_exists,
            }),
            SchemaCommand::CreateScalarIndex(cfg) => {
                // Convert expressions to storage strings (strip variable prefix)
                let properties: Vec<String> = cfg
                    .expressions
                    .iter()
                    .map(Self::normalize_expression_for_storage)
                    .collect();

                Ok(LogicalPlan::CreateScalarIndex {
                    config: ScalarIndexConfig {
                        name: cfg.name,
                        label: cfg.label,
                        properties,
                        index_type: ScalarIndexType::BTree,
                        where_clause: cfg.where_clause.map(|e| e.to_string_repr()),
                    },
                    if_not_exists: cfg.if_not_exists,
                })
            }
            SchemaCommand::CreateJsonFtsIndex(cfg) => {
                let with_positions = cfg
                    .options
                    .get("with_positions")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                Ok(LogicalPlan::CreateJsonFtsIndex {
                    config: JsonFtsIndexConfig {
                        name: cfg.name,
                        label: cfg.label,
                        column: cfg.column,
                        paths: Vec::new(),
                        with_positions,
                    },
                    if_not_exists: cfg.if_not_exists,
                })
            }
            SchemaCommand::DropIndex(drop) => Ok(LogicalPlan::DropIndex {
                name: drop.name,
                if_exists: false, // new AST doesn't have if_exists for DROP INDEX yet
            }),
            SchemaCommand::CreateConstraint(c) => Ok(LogicalPlan::CreateConstraint(c)),
            SchemaCommand::DropConstraint(c) => Ok(LogicalPlan::DropConstraint(c)),
            SchemaCommand::CreateLabel(c) => Ok(LogicalPlan::CreateLabel(c)),
            SchemaCommand::CreateEdgeType(c) => Ok(LogicalPlan::CreateEdgeType(c)),
            SchemaCommand::AlterLabel(c) => Ok(LogicalPlan::AlterLabel(c)),
            SchemaCommand::AlterEdgeType(c) => Ok(LogicalPlan::AlterEdgeType(c)),
            SchemaCommand::DropLabel(c) => Ok(LogicalPlan::DropLabel(c)),
            SchemaCommand::DropEdgeType(c) => Ok(LogicalPlan::DropEdgeType(c)),
            SchemaCommand::ShowConstraints(c) => Ok(LogicalPlan::ShowConstraints(c)),
            SchemaCommand::ShowIndexes(c) => Ok(LogicalPlan::ShowIndexes { filter: c.filter }),
            SchemaCommand::ShowDatabase => Ok(LogicalPlan::ShowDatabase),
            SchemaCommand::ShowConfig => Ok(LogicalPlan::ShowConfig),
            SchemaCommand::ShowStatistics => Ok(LogicalPlan::ShowStatistics),
            SchemaCommand::Vacuum => Ok(LogicalPlan::Vacuum),
            SchemaCommand::Checkpoint => Ok(LogicalPlan::Checkpoint),
            SchemaCommand::Backup { path } => Ok(LogicalPlan::Backup {
                destination: path,
                options: HashMap::new(),
            }),
            SchemaCommand::CopyTo(cmd) => Ok(LogicalPlan::CopyTo {
                label: cmd.label,
                path: cmd.path,
                format: cmd.format,
                options: cmd.options,
            }),
            SchemaCommand::CopyFrom(cmd) => Ok(LogicalPlan::CopyFrom {
                label: cmd.label,
                path: cmd.path,
                format: cmd.format,
                options: cmd.options,
            }),
        }
    }

    fn plan_transaction_command(
        &self,
        cmd: uni_cypher::ast::TransactionCommand,
    ) -> Result<LogicalPlan> {
        use uni_cypher::ast::TransactionCommand;
        match cmd {
            TransactionCommand::Begin => Ok(LogicalPlan::Begin),
            TransactionCommand::Commit => Ok(LogicalPlan::Commit),
            TransactionCommand::Rollback => Ok(LogicalPlan::Rollback),
        }
    }

    fn parse_embedding_config(emb_val: &serde_json::Value) -> Result<Option<EmbeddingConfig>> {
        let obj = emb_val
            .as_object()
            .ok_or_else(|| anyhow!("embedding option must be an object"))?;

        // Parse provider (required)
        let provider = obj
            .get("provider")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("embedding.provider is required"))?;

        // Parse model (required)
        let model_name = obj
            .get("model")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("embedding.model is required"))?;

        // Parse source properties (required)
        let source_properties = obj
            .get("source")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("embedding.source is required and must be an array"))?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();

        if source_properties.is_empty() {
            return Err(anyhow!(
                "embedding.source must contain at least one property"
            ));
        }

        // Create embedding model based on provider
        let model = match provider {
            "fastembed" => EmbeddingModel::FastEmbed {
                model_name: model_name.to_string(),
                cache_dir: None,
                max_length: None,
            },
            "openai" => EmbeddingModel::OpenAI {
                model: model_name.to_string(),
                api_key_env: "OPENAI_API_KEY".to_string(),
                dimensions: None,
            },
            "ollama" => EmbeddingModel::Ollama {
                model: model_name.to_string(),
                host: "http://localhost:11434".to_string(),
            },
            _ => return Err(anyhow!("Unsupported embedding provider: {}", provider)),
        };

        Ok(Some(EmbeddingConfig {
            model,
            source_properties,
            batch_size: 32,
        }))
    }
}

/// Check if a function name is a manual window function (ROW_NUMBER, RANK, etc.).
pub fn is_manual_window_function(name: &str) -> bool {
    MANUAL_WINDOW_FUNCTIONS.contains(&name.to_uppercase().as_str())
}

/// Check if a function name is an aggregate window function (SUM, AVG, etc.).
pub fn is_aggregate_window_function(name: &str) -> bool {
    AGGREGATE_WINDOW_FUNCTIONS.contains(&name.to_uppercase().as_str())
}

/// Classify window expressions into manual and DataFusion-backed groups.
///
/// Returns: (manual_exprs, datafusion_exprs)
pub fn classify_window_expressions(exprs: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
    let mut manual = Vec::new();
    let mut datafusion = Vec::new();

    for expr in exprs {
        if let Expr::FunctionCall {
            name,
            window_spec: Some(_),
            ..
        } = expr
        {
            if is_manual_window_function(name) {
                manual.push(expr.clone());
            } else if is_aggregate_window_function(name) {
                datafusion.push(expr.clone());
            }
        }
    }

    (manual, datafusion)
}

/// Collect all properties referenced anywhere in the LogicalPlan tree.
///
/// This is critical for window functions: properties must be materialized
/// at the Scan node so they're available for window operations later.
///
/// Returns a mapping of variable name → property names (e.g., "e" → {"dept", "salary"}).
pub fn collect_properties_from_plan(
    plan: &LogicalPlan,
) -> HashMap<String, std::collections::HashSet<String>> {
    let mut properties: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    collect_properties_recursive(plan, &mut properties);
    properties
}

/// Recursively walk the LogicalPlan tree and collect all property references.
fn collect_properties_recursive(
    plan: &LogicalPlan,
    properties: &mut HashMap<String, std::collections::HashSet<String>>,
) {
    match plan {
        LogicalPlan::Window {
            input,
            window_exprs,
        } => {
            // Collect from window expressions
            for expr in window_exprs {
                collect_properties_from_expr_into(expr, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Project { input, projections } => {
            for (expr, _alias) in projections {
                collect_properties_from_expr_into(expr, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Sort { input, order_by } => {
            for sort_item in order_by {
                collect_properties_from_expr_into(&sort_item.expr, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            collect_properties_from_expr_into(predicate, properties);
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            for expr in group_by {
                collect_properties_from_expr_into(expr, properties);
            }
            for expr in aggregates {
                collect_properties_from_expr_into(expr, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Scan {
            filter: Some(expr), ..
        } => {
            collect_properties_from_expr_into(expr, properties);
        }
        LogicalPlan::Scan { filter: None, .. } => {}
        LogicalPlan::ExtIdLookup {
            filter: Some(expr), ..
        } => {
            collect_properties_from_expr_into(expr, properties);
        }
        LogicalPlan::ExtIdLookup { filter: None, .. } => {}
        LogicalPlan::Traverse {
            input,
            target_filter,
            step_variable: _,
            ..
        } => {
            if let Some(expr) = target_filter {
                collect_properties_from_expr_into(expr, properties);
            }
            // Note: Edge properties (step_variable) will be collected from expressions
            // that reference them. The edge_properties field in LogicalPlan is populated
            // later during physical planning based on this collected map.
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Unwind { input, expr, .. } => {
            collect_properties_from_expr_into(expr, properties);
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Create { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Merge { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Set { input, items } => {
            for item in items {
                if let SetItem::Property { value, .. } = item {
                    collect_properties_from_expr_into(value, properties);
                }
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Remove { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Delete { input, items, .. } => {
            for expr in items {
                collect_properties_from_expr_into(expr, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Foreach {
            input, list, body, ..
        } => {
            collect_properties_from_expr_into(list, properties);
            for plan in body {
                collect_properties_recursive(plan, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Limit { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::CrossJoin { left, right } => {
            collect_properties_recursive(left, properties);
            collect_properties_recursive(right, properties);
        }
        LogicalPlan::Apply {
            input,
            subquery,
            input_filter,
        } => {
            if let Some(expr) = input_filter {
                collect_properties_from_expr_into(expr, properties);
            }
            collect_properties_recursive(input, properties);
            collect_properties_recursive(subquery, properties);
        }
        LogicalPlan::Union { left, right, .. } => {
            collect_properties_recursive(left, properties);
            collect_properties_recursive(right, properties);
        }
        LogicalPlan::RecursiveCTE {
            initial, recursive, ..
        } => {
            collect_properties_recursive(initial, properties);
            collect_properties_recursive(recursive, properties);
        }
        LogicalPlan::ProcedureCall { arguments, .. } => {
            for arg in arguments {
                collect_properties_from_expr_into(arg, properties);
            }
        }
        LogicalPlan::VectorKnn { query, .. } => {
            collect_properties_from_expr_into(query, properties);
        }
        LogicalPlan::InvertedIndexLookup { terms, .. } => {
            collect_properties_from_expr_into(terms, properties);
        }
        LogicalPlan::ShortestPath { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::AllShortestPaths { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Distinct { input } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::QuantifiedPattern {
            input,
            pattern_plan,
            ..
        } => {
            collect_properties_recursive(input, properties);
            collect_properties_recursive(pattern_plan, properties);
        }
        // DDL and other plans don't reference properties
        _ => {}
    }
}

/// Collect properties from an expression into a HashMap.
fn collect_properties_from_expr_into(
    expr: &Expr,
    properties: &mut HashMap<String, std::collections::HashSet<String>>,
) {
    match expr {
        Expr::PatternComprehension {
            where_clause,
            map_expr,
            ..
        } => {
            // Collect properties from the WHERE clause and map expression.
            // The pattern itself creates local bindings that don't need
            // property collection from the outer scope.
            if let Some(where_expr) = where_clause {
                collect_properties_from_expr_into(where_expr, properties);
            }
            collect_properties_from_expr_into(map_expr, properties);
        }
        Expr::Variable(name) => {
            // Handle transformed property expressions like "e.dept" (after transform_window_expr_properties)
            if let Some((var, prop)) = name.split_once('.') {
                properties
                    .entry(var.to_string())
                    .or_default()
                    .insert(prop.to_string());
            }
        }
        Expr::Property(base, name) => {
            // Extract variable name from the base expression
            if let Expr::Variable(var) = base.as_ref() {
                properties
                    .entry(var.clone())
                    .or_default()
                    .insert(name.clone());
            }
            // Also recurse into the base expression
            collect_properties_from_expr_into(base, properties);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_properties_from_expr_into(left, properties);
            collect_properties_from_expr_into(right, properties);
        }
        Expr::FunctionCall {
            name,
            args,
            window_spec,
            ..
        } => {
            // Analyze function for property requirements (pushdown hydration)
            analyze_function_property_requirements(name, args, properties);

            // Collect from arguments
            for arg in args {
                collect_properties_from_expr_into(arg, properties);
            }

            // Collect from window spec (PARTITION BY, ORDER BY)
            if let Some(spec) = window_spec {
                for part_expr in &spec.partition_by {
                    collect_properties_from_expr_into(part_expr, properties);
                }
                for sort_item in &spec.order_by {
                    collect_properties_from_expr_into(&sort_item.expr, properties);
                }
            }
        }
        Expr::UnaryOp { expr, .. } => {
            collect_properties_from_expr_into(expr, properties);
        }
        Expr::List(items) => {
            for item in items {
                collect_properties_from_expr_into(item, properties);
            }
        }
        Expr::Map(entries) => {
            for (_key, value) in entries {
                collect_properties_from_expr_into(value, properties);
            }
        }
        Expr::ListComprehension {
            list,
            where_clause,
            map_expr,
            ..
        } => {
            collect_properties_from_expr_into(list, properties);
            if let Some(where_expr) = where_clause {
                collect_properties_from_expr_into(where_expr, properties);
            }
            collect_properties_from_expr_into(map_expr, properties);
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            if let Some(scrutinee_expr) = expr {
                collect_properties_from_expr_into(scrutinee_expr, properties);
            }
            for (when, then) in when_then {
                collect_properties_from_expr_into(when, properties);
                collect_properties_from_expr_into(then, properties);
            }
            if let Some(default_expr) = else_expr {
                collect_properties_from_expr_into(default_expr, properties);
            }
        }
        Expr::Quantifier {
            list, predicate, ..
        } => {
            collect_properties_from_expr_into(list, properties);
            collect_properties_from_expr_into(predicate, properties);
        }
        Expr::Reduce {
            init, list, expr, ..
        } => {
            collect_properties_from_expr_into(init, properties);
            collect_properties_from_expr_into(list, properties);
            collect_properties_from_expr_into(expr, properties);
        }
        Expr::Exists(_) | Expr::CountSubquery(_) | Expr::CollectSubquery(_) => {
            // Subqueries have their own scope; no property collection needed
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::IsUnique(expr) => {
            collect_properties_from_expr_into(expr, properties);
        }
        Expr::In { expr, list } => {
            collect_properties_from_expr_into(expr, properties);
            collect_properties_from_expr_into(list, properties);
        }
        Expr::ArrayIndex { array, index } => {
            // Dynamic property access: e[prop] → need all properties
            if let Expr::Variable(var) = array.as_ref() {
                properties
                    .entry(var.clone())
                    .or_default()
                    .insert("*".to_string());
            }
            collect_properties_from_expr_into(array, properties);
            collect_properties_from_expr_into(index, properties);
        }
        Expr::ArraySlice { array, start, end } => {
            collect_properties_from_expr_into(array, properties);
            if let Some(start_expr) = start {
                collect_properties_from_expr_into(start_expr, properties);
            }
            if let Some(end_expr) = end {
                collect_properties_from_expr_into(end_expr, properties);
            }
        }
        Expr::ValidAt {
            entity,
            timestamp,
            start_prop,
            end_prop,
        } => {
            // Extract property requirements from ValidAt expression
            if let Expr::Variable(var) = entity.as_ref() {
                if let Some(prop) = start_prop {
                    properties
                        .entry(var.clone())
                        .or_default()
                        .insert(prop.clone());
                }
                if let Some(prop) = end_prop {
                    properties
                        .entry(var.clone())
                        .or_default()
                        .insert(prop.clone());
                }
            }
            collect_properties_from_expr_into(entity, properties);
            collect_properties_from_expr_into(timestamp, properties);
        }
        Expr::MapProjection { base, items } => {
            collect_properties_from_expr_into(base, properties);
            for item in items {
                if let uni_cypher::ast::MapProjectionItem::LiteralEntry(_, expr) = item {
                    collect_properties_from_expr_into(expr, properties);
                }
            }
        }
        // Literals, parameters, wildcard don't reference properties
        Expr::Literal(_) | Expr::Parameter(_) | Expr::Wildcard => {}
    }
}

/// Analyze function calls to extract property requirements for pushdown hydration
///
/// This function examines function calls and their arguments to determine which properties
/// need to be loaded for entity arguments. For example:
/// - validAt(e, 'start', 'end', ts) -> e needs {start, end}
/// - keys(n) -> n needs all properties (*)
///
/// The extracted requirements are added to the properties map for later use during
/// scan planning.
fn analyze_function_property_requirements(
    name: &str,
    args: &[Expr],
    properties: &mut HashMap<String, std::collections::HashSet<String>>,
) {
    use crate::query::function_props::get_function_spec;

    /// Helper to mark a variable as needing all properties.
    fn mark_wildcard(
        var: &str,
        properties: &mut HashMap<String, std::collections::HashSet<String>>,
    ) {
        properties
            .entry(var.to_string())
            .or_default()
            .insert("*".to_string());
    }

    let Some(spec) = get_function_spec(name) else {
        // Unknown function: conservatively require all properties for variable args
        for arg in args {
            if let Expr::Variable(var) = arg {
                mark_wildcard(var, properties);
            }
        }
        return;
    };

    // Extract property names from string literal arguments
    for &(prop_arg_idx, entity_arg_idx) in spec.property_name_args {
        let entity_arg = args.get(entity_arg_idx);
        let prop_arg = args.get(prop_arg_idx);

        match (entity_arg, prop_arg) {
            (Some(Expr::Variable(var)), Some(Expr::Literal(CypherLiteral::String(prop)))) => {
                properties
                    .entry(var.clone())
                    .or_default()
                    .insert(prop.clone());
            }
            (Some(Expr::Variable(var)), Some(Expr::Parameter(_))) => {
                // Parameter property name: need all properties
                mark_wildcard(var, properties);
            }
            _ => {}
        }
    }

    // Handle full entity requirement (keys(), properties())
    if spec.needs_full_entity {
        for &idx in spec.entity_args {
            if let Some(Expr::Variable(var)) = args.get(idx) {
                mark_wildcard(var, properties);
            }
        }
    }
}

#[cfg(test)]
mod pushdown_tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_validat_extracts_property_names() {
        // validAt(e, 'start', 'end', ts) → e: {start, end}
        let mut properties = HashMap::new();

        let args = vec![
            Expr::Variable("e".to_string()),
            Expr::Literal(CypherLiteral::String("start".to_string())),
            Expr::Literal(CypherLiteral::String("end".to_string())),
            Expr::Variable("ts".to_string()),
        ];

        analyze_function_property_requirements("uni.temporal.validAt", &args, &mut properties);

        assert!(properties.contains_key("e"));
        let e_props: HashSet<String> = ["start".to_string(), "end".to_string()]
            .iter()
            .cloned()
            .collect();
        assert_eq!(properties.get("e").unwrap(), &e_props);
    }

    #[test]
    fn test_keys_requires_wildcard() {
        // keys(n) → n: {*}
        let mut properties = HashMap::new();

        let args = vec![Expr::Variable("n".to_string())];

        analyze_function_property_requirements("keys", &args, &mut properties);

        assert!(properties.contains_key("n"));
        let n_props: HashSet<String> = ["*".to_string()].iter().cloned().collect();
        assert_eq!(properties.get("n").unwrap(), &n_props);
    }

    #[test]
    fn test_properties_requires_wildcard() {
        // properties(n) → n: {*}
        let mut properties = HashMap::new();

        let args = vec![Expr::Variable("n".to_string())];

        analyze_function_property_requirements("properties", &args, &mut properties);

        assert!(properties.contains_key("n"));
        let n_props: HashSet<String> = ["*".to_string()].iter().cloned().collect();
        assert_eq!(properties.get("n").unwrap(), &n_props);
    }

    #[test]
    fn test_unknown_function_conservative() {
        // customUdf(e) → e: {*}
        let mut properties = HashMap::new();

        let args = vec![Expr::Variable("e".to_string())];

        analyze_function_property_requirements("customUdf", &args, &mut properties);

        assert!(properties.contains_key("e"));
        let e_props: HashSet<String> = ["*".to_string()].iter().cloned().collect();
        assert_eq!(properties.get("e").unwrap(), &e_props);
    }

    #[test]
    fn test_parameter_property_name() {
        // validAt(e, $start, $end, ts) → e: {*}
        let mut properties = HashMap::new();

        let args = vec![
            Expr::Variable("e".to_string()),
            Expr::Parameter("start".to_string()),
            Expr::Parameter("end".to_string()),
            Expr::Variable("ts".to_string()),
        ];

        analyze_function_property_requirements("uni.temporal.validAt", &args, &mut properties);

        assert!(properties.contains_key("e"));
        assert!(properties.get("e").unwrap().contains("*"));
    }

    #[test]
    fn test_validat_expr_extracts_properties() {
        // Test Expr::ValidAt variant property extraction
        let mut properties = HashMap::new();

        let validat_expr = Expr::ValidAt {
            entity: Box::new(Expr::Variable("e".to_string())),
            timestamp: Box::new(Expr::Variable("ts".to_string())),
            start_prop: Some("valid_from".to_string()),
            end_prop: Some("valid_to".to_string()),
        };

        collect_properties_from_expr_into(&validat_expr, &mut properties);

        assert!(properties.contains_key("e"));
        assert!(properties.get("e").unwrap().contains("valid_from"));
        assert!(properties.get("e").unwrap().contains("valid_to"));
    }

    #[test]
    fn test_array_index_requires_wildcard() {
        // e[prop] → e: {*}
        let mut properties = HashMap::new();

        let array_index_expr = Expr::ArrayIndex {
            array: Box::new(Expr::Variable("e".to_string())),
            index: Box::new(Expr::Variable("prop".to_string())),
        };

        collect_properties_from_expr_into(&array_index_expr, &mut properties);

        assert!(properties.contains_key("e"));
        assert!(properties.get("e").unwrap().contains("*"));
    }

    #[test]
    fn test_property_access_extraction() {
        // e.name → e: {name}
        let mut properties = HashMap::new();

        let prop_access = Expr::Property(
            Box::new(Expr::Variable("e".to_string())),
            "name".to_string(),
        );

        collect_properties_from_expr_into(&prop_access, &mut properties);

        assert!(properties.contains_key("e"));
        assert!(properties.get("e").unwrap().contains("name"));
    }
}
