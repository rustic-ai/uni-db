// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::pushdown::PredicateAnalyzer;
use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uni_common::Value;
use uni_common::core::schema::{
    DistanceMetric, EmbeddingConfig, EmbeddingModel, FullTextIndexConfig, IndexDefinition,
    JsonFtsIndexConfig, ScalarIndexConfig, ScalarIndexType, Schema, TokenizerConfig,
    VectorIndexConfig, VectorIndexType,
};
use uni_cypher::ast::{
    AlterEdgeType, AlterLabel, BinaryOp, CallKind, Clause, CreateConstraint, CreateEdgeType,
    CreateLabel, CypherLiteral, Direction, DropConstraint, DropEdgeType, DropLabel, Expr,
    MatchClause, MergeClause, NodePattern, PathPattern, Pattern, PatternElement, Query,
    RelationshipPattern, RemoveItem, ReturnClause, ReturnItem, SchemaCommand, SetClause, SetItem,
    ShortestPathMode, ShowConstraints, SortItem, Statement, WindowSpec, WithClause,
    WithRecursiveClause,
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
    /// Could hold a map or dynamic value — property access is allowed.
    Scalar,
    /// Scalar from a known non-graph literal (int, float, bool, string, list).
    /// Property access is NOT allowed on these at compile time.
    ScalarLiteral,
    /// Imported from outer scope with unknown type (from plan_with_scope string vars).
    /// Compatible with any concrete type — allows subqueries to re-bind the variable.
    Imported,
}

impl VariableType {
    /// Returns true if this type is compatible with the expected type.
    ///
    /// `Imported` is always compatible because the actual type is unknown at plan time.
    fn is_compatible_with(self, expected: VariableType) -> bool {
        self == expected
            || self == VariableType::Imported
            // ScalarLiteral behaves like Scalar for compatibility checks
            || (self == VariableType::ScalarLiteral && expected == VariableType::Scalar)
    }
}

/// Information about a variable in scope.
#[derive(Debug, Clone)]
pub struct VariableInfo {
    pub name: String,
    pub var_type: VariableType,
    /// True if this is a variable-length path (VLP) step variable.
    /// VLP step variables are typed as Edge but semantically hold edge lists.
    pub is_vlp: bool,
}

impl VariableInfo {
    pub fn new(name: String, var_type: VariableType) -> Self {
        Self {
            name,
            var_type,
            is_vlp: false,
        }
    }
}

/// Find a variable in scope by name.
fn find_var_in_scope<'a>(vars: &'a [VariableInfo], name: &str) -> Option<&'a VariableInfo> {
    vars.iter().find(|v| v.name == name)
}

/// Check if a variable is in scope.
fn is_var_in_scope(vars: &[VariableInfo], name: &str) -> bool {
    find_var_in_scope(vars, name).is_some()
}

/// Check if an expression contains a pattern predicate.
fn contains_pattern_predicate(expr: &Expr) -> bool {
    if matches!(
        expr,
        Expr::Exists {
            from_pattern_predicate: true,
            ..
        }
    ) {
        return true;
    }
    let mut found = false;
    expr.for_each_child(&mut |child| {
        if !found {
            found = contains_pattern_predicate(child);
        }
    });
    found
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
        } else if matches!(
            existing.var_type,
            VariableType::Scalar | VariableType::ScalarLiteral
        ) && matches!(var_type, VariableType::Node | VariableType::Edge)
        {
            // Scalar can be used as Node/Edge in CREATE context — a scalar
            // holding a node/edge reference is valid for pattern use
            existing.var_type = var_type;
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

fn infer_with_output_type(expr: &Expr, vars_in_scope: &[VariableInfo]) -> VariableType {
    match expr {
        Expr::Variable(v) => find_var_in_scope(vars_in_scope, v)
            .map(|info| info.var_type)
            .unwrap_or(VariableType::Scalar),
        Expr::Literal(CypherLiteral::Null) => VariableType::Imported,
        // Known non-graph literals: property access is NOT valid on these.
        Expr::Literal(CypherLiteral::Integer(_))
        | Expr::Literal(CypherLiteral::Float(_))
        | Expr::Literal(CypherLiteral::String(_))
        | Expr::Literal(CypherLiteral::Bool(_)) => VariableType::ScalarLiteral,
        Expr::FunctionCall { name, args, .. } => {
            let lower = name.to_lowercase();
            if lower == "coalesce" {
                infer_coalesce_type(args, vars_in_scope)
            } else if lower == "collect" && !args.is_empty() {
                let collected = infer_with_output_type(&args[0], vars_in_scope);
                if matches!(
                    collected,
                    VariableType::Node
                        | VariableType::Edge
                        | VariableType::Path
                        | VariableType::Imported
                ) {
                    collected
                } else {
                    VariableType::Scalar
                }
            } else {
                VariableType::Scalar
            }
        }
        // WITH list literals/expressions produce scalar list values. Preserving
        // entity typing here causes invalid node/edge reuse in later MATCH clauses
        // (e.g. WITH [n] AS users; MATCH (users)-->() should fail at compile time).
        // Lists are ScalarLiteral since property access is not valid on them.
        Expr::List(_) => VariableType::ScalarLiteral,
        _ => VariableType::Scalar,
    }
}

fn infer_coalesce_type(args: &[Expr], vars_in_scope: &[VariableInfo]) -> VariableType {
    let mut resolved: Option<VariableType> = None;
    let mut saw_imported = false;
    for arg in args {
        let t = infer_with_output_type(arg, vars_in_scope);
        match t {
            VariableType::Node | VariableType::Edge | VariableType::Path => {
                if let Some(existing) = resolved {
                    if existing != t {
                        return VariableType::Scalar;
                    }
                } else {
                    resolved = Some(t);
                }
            }
            VariableType::Imported => saw_imported = true,
            VariableType::Scalar | VariableType::ScalarLiteral => {}
        }
    }
    if let Some(t) = resolved {
        t
    } else if saw_imported {
        VariableType::Imported
    } else {
        VariableType::Scalar
    }
}

fn infer_unwind_output_type(expr: &Expr, vars_in_scope: &[VariableInfo]) -> VariableType {
    match expr {
        Expr::Variable(v) => find_var_in_scope(vars_in_scope, v)
            .map(|info| info.var_type)
            .unwrap_or(VariableType::Scalar),
        Expr::FunctionCall { name, args, .. }
            if name.eq_ignore_ascii_case("collect") && !args.is_empty() =>
        {
            infer_with_output_type(&args[0], vars_in_scope)
        }
        Expr::List(items) => {
            let mut inferred: Option<VariableType> = None;
            for item in items {
                let t = infer_with_output_type(item, vars_in_scope);
                if !matches!(
                    t,
                    VariableType::Node
                        | VariableType::Edge
                        | VariableType::Path
                        | VariableType::Imported
                ) {
                    return VariableType::Scalar;
                }
                if let Some(existing) = inferred {
                    if existing != t
                        && t != VariableType::Imported
                        && existing != VariableType::Imported
                    {
                        return VariableType::Scalar;
                    }
                    if existing == VariableType::Imported && t != VariableType::Imported {
                        inferred = Some(t);
                    }
                } else {
                    inferred = Some(t);
                }
            }
            inferred.unwrap_or(VariableType::Scalar)
        }
        _ => VariableType::Scalar,
    }
}

/// Collect all variable names referenced in an expression
fn collect_expr_variables(expr: &Expr) -> Vec<String> {
    let mut vars = Vec::new();
    collect_expr_variables_inner(expr, &mut vars);
    vars
}

fn collect_expr_variables_inner(expr: &Expr, vars: &mut Vec<String>) {
    let mut add_var = |name: &String| {
        if !vars.contains(name) {
            vars.push(name.clone());
        }
    };

    match expr {
        Expr::Variable(name) => add_var(name),
        Expr::Property(base, _) => collect_expr_variables_inner(base, vars),
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_variables_inner(left, vars);
            collect_expr_variables_inner(right, vars);
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsUnique(e) => collect_expr_variables_inner(e, vars),
        Expr::FunctionCall { args, .. } => {
            for a in args {
                collect_expr_variables_inner(a, vars);
            }
        }
        Expr::List(items) => {
            for item in items {
                collect_expr_variables_inner(item, vars);
            }
        }
        Expr::In { expr: e, list } => {
            collect_expr_variables_inner(e, vars);
            collect_expr_variables_inner(list, vars);
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
        Expr::LabelCheck { expr, .. } => collect_expr_variables_inner(expr, vars),
        Expr::ArrayIndex { array, index } => {
            collect_expr_variables_inner(array, vars);
            collect_expr_variables_inner(index, vars);
        }
        Expr::ArraySlice { array, start, end } => {
            collect_expr_variables_inner(array, vars);
            if let Some(s) = start {
                collect_expr_variables_inner(s, vars);
            }
            if let Some(e) = end {
                collect_expr_variables_inner(e, vars);
            }
        }
        // Skip Quantifier/Reduce/ListComprehension/PatternComprehension —
        // they introduce local variable bindings not in outer scope.
        _ => {}
    }
}

/// Rewrite ORDER BY expressions to resolve projection aliases back to their source expressions.
///
/// Example: `RETURN r AS rel ORDER BY rel.id` becomes `ORDER BY r.id` so Sort can run
/// before the final RETURN projection without losing alias semantics.
fn rewrite_order_by_expr_with_aliases(expr: &Expr, aliases: &HashMap<String, Expr>) -> Expr {
    let repr = expr.to_string_repr();
    if let Some(rewritten) = aliases.get(&repr) {
        return rewritten.clone();
    }

    match expr {
        Expr::Variable(name) => aliases.get(name).cloned().unwrap_or_else(|| expr.clone()),
        Expr::Property(base, prop) => Expr::Property(
            Box::new(rewrite_order_by_expr_with_aliases(base, aliases)),
            prop.clone(),
        ),
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_order_by_expr_with_aliases(left, aliases)),
            op: *op,
            right: Box::new(rewrite_order_by_expr_with_aliases(right, aliases)),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_order_by_expr_with_aliases(inner, aliases)),
        },
        Expr::FunctionCall {
            name,
            args,
            distinct,
            window_spec,
        } => Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_order_by_expr_with_aliases(a, aliases))
                .collect(),
            distinct: *distinct,
            window_spec: window_spec.clone(),
        },
        Expr::List(items) => Expr::List(
            items
                .iter()
                .map(|item| rewrite_order_by_expr_with_aliases(item, aliases))
                .collect(),
        ),
        Expr::Map(entries) => Expr::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), rewrite_order_by_expr_with_aliases(v, aliases)))
                .collect(),
        ),
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => Expr::Case {
            expr: case_expr
                .as_ref()
                .map(|e| Box::new(rewrite_order_by_expr_with_aliases(e, aliases))),
            when_then: when_then
                .iter()
                .map(|(w, t)| {
                    (
                        rewrite_order_by_expr_with_aliases(w, aliases),
                        rewrite_order_by_expr_with_aliases(t, aliases),
                    )
                })
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_order_by_expr_with_aliases(e, aliases))),
        },
        // Skip Quantifier/Reduce/ListComprehension/PatternComprehension —
        // they introduce local variable bindings that could shadow aliases.
        _ => expr.clone(),
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
            | Expr::Literal(CypherLiteral::Bool(_))
            | Expr::List(_) => {
                return Err(anyhow!(
                    "SyntaxError: InvalidArgumentType - properties() requires a node, relationship, or map"
                ));
            }
            Expr::Variable(var_name) => {
                if let Some(info) = find_var_in_scope(vars_in_scope, var_name)
                    && matches!(
                        info.var_type,
                        VariableType::Scalar | VariableType::ScalarLiteral
                    )
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

    // size() does NOT accept Path arguments (length() on paths IS valid — returns relationship count)
    if name_lower == "size"
        && let Some(Expr::Variable(var_name)) = args.first()
        && let Some(info) = find_var_in_scope(vars_in_scope, var_name)
        && info.var_type == VariableType::Path
    {
        return Err(anyhow!(
            "SyntaxError: InvalidArgumentType - size() requires a string, list, or map argument"
        ));
    }

    // length()/size() do NOT accept Node or single-Edge arguments.
    // VLP step variables (e.g. `r` in `-[r*1..2]->`) are typed as Edge
    // but are actually edge lists — size()/length() is valid on those.
    if (name_lower == "length" || name_lower == "size")
        && let Some(Expr::Variable(var_name)) = args.first()
        && let Some(info) = find_var_in_scope(vars_in_scope, var_name)
        && (info.var_type == VariableType::Node
            || (info.var_type == VariableType::Edge && !info.is_vlp))
    {
        return Err(anyhow!(
            "SyntaxError: InvalidArgumentType - {}() requires a string, list, or path argument",
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
            | Expr::Map(_)
    )
}

/// Validate boolean expressions (AND/OR/NOT require boolean arguments).
fn validate_boolean_expression(expr: &Expr) -> Result<()> {
    // Check AND/OR/XOR operands and NOT operand for non-boolean literals
    if let Expr::BinaryOp { left, op, right } = expr
        && matches!(op, BinaryOp::And | BinaryOp::Or | BinaryOp::Xor)
    {
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
    if let Expr::UnaryOp {
        op: uni_cypher::ast::UnaryOp::Not,
        expr: inner,
    } = expr
        && is_non_boolean_literal(inner)
    {
        return Err(anyhow!(
            "SyntaxError: InvalidArgumentType - NOT requires a boolean argument"
        ));
    }
    let mut result = Ok(());
    expr.for_each_child(&mut |child| {
        if result.is_ok() {
            result = validate_boolean_expression(child);
        }
    });
    result
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

/// Check if a function name (lowercase) is an aggregate function.
fn is_aggregate_function_name(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "collect"
            | "stdev"
            | "stdevp"
            | "percentiledisc"
            | "percentilecont"
    )
}

/// Returns true if the expression is a window function (FunctionCall with window_spec).
fn is_window_function(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::FunctionCall {
            window_spec: Some(_),
            ..
        }
    )
}

/// Returns true when `expr` reports `is_aggregate()` but is NOT itself a bare
/// aggregate FunctionCall (or CountSubquery/CollectSubquery). In other words,
/// the aggregate lives *inside* a wrapper expression (e.g. a ListComprehension,
/// size() call, BinaryOp, etc.).
fn is_compound_aggregate(expr: &Expr) -> bool {
    if !expr.is_aggregate() {
        return false;
    }
    match expr {
        Expr::FunctionCall {
            name, window_spec, ..
        } => {
            // A bare aggregate FunctionCall is NOT compound
            if window_spec.is_some() {
                return true; // window wrapping an aggregate — treat as compound
            }
            !is_aggregate_function_name(name)
        }
        // Subquery aggregates are "bare" (not compound)
        Expr::CountSubquery(_) | Expr::CollectSubquery(_) => false,
        // Everything else (ListComprehension, BinaryOp, etc.) is compound
        _ => true,
    }
}

/// Recursively collect all bare aggregate FunctionCall sub-expressions from
/// `expr`. Stops recursing into the *arguments* of an aggregate (we only want
/// the outermost aggregate boundaries).
///
/// For `ListComprehension`, `Quantifier`, and `Reduce`, only the `list` field
/// is searched because the body (`map_expr`, `predicate`, `expr`) references
/// the loop variable, not outer-scope aggregates.
fn extract_inner_aggregates(expr: &Expr) -> Vec<Expr> {
    let mut out = Vec::new();
    extract_inner_aggregates_rec(expr, &mut out);
    out
}

fn extract_inner_aggregates_rec(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::FunctionCall {
            name, window_spec, ..
        } if window_spec.is_none() && is_aggregate_function_name(name) => {
            // Found a bare aggregate — collect it and stop recursing
            out.push(expr.clone());
        }
        Expr::CountSubquery(_) | Expr::CollectSubquery(_) => {
            out.push(expr.clone());
        }
        // For list comprehension, only search the `list` source for aggregates
        Expr::ListComprehension { list, .. } => {
            extract_inner_aggregates_rec(list, out);
        }
        // For quantifier, only search the `list` source
        Expr::Quantifier { list, .. } => {
            extract_inner_aggregates_rec(list, out);
        }
        // For reduce, search `init` and `list` (not the body `expr`)
        Expr::Reduce { init, list, .. } => {
            extract_inner_aggregates_rec(init, out);
            extract_inner_aggregates_rec(list, out);
        }
        // Standard recursive cases
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                extract_inner_aggregates_rec(arg, out);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_inner_aggregates_rec(left, out);
            extract_inner_aggregates_rec(right, out);
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsUnique(e) => extract_inner_aggregates_rec(e, out),
        Expr::Property(base, _) => extract_inner_aggregates_rec(base, out),
        Expr::List(items) => {
            for item in items {
                extract_inner_aggregates_rec(item, out);
            }
        }
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = case_expr {
                extract_inner_aggregates_rec(e, out);
            }
            for (w, t) in when_then {
                extract_inner_aggregates_rec(w, out);
                extract_inner_aggregates_rec(t, out);
            }
            if let Some(e) = else_expr {
                extract_inner_aggregates_rec(e, out);
            }
        }
        Expr::In {
            expr: in_expr,
            list,
        } => {
            extract_inner_aggregates_rec(in_expr, out);
            extract_inner_aggregates_rec(list, out);
        }
        Expr::ArrayIndex { array, index } => {
            extract_inner_aggregates_rec(array, out);
            extract_inner_aggregates_rec(index, out);
        }
        Expr::ArraySlice { array, start, end } => {
            extract_inner_aggregates_rec(array, out);
            if let Some(s) = start {
                extract_inner_aggregates_rec(s, out);
            }
            if let Some(e) = end {
                extract_inner_aggregates_rec(e, out);
            }
        }
        Expr::Map(entries) => {
            for (_, v) in entries {
                extract_inner_aggregates_rec(v, out);
            }
        }
        _ => {}
    }
}

/// Return a copy of `expr` with every inner aggregate FunctionCall replaced by
/// `Expr::Variable(aggregate_column_name(agg))`.
///
/// For `ListComprehension`/`Quantifier`/`Reduce`, only the `list` field is
/// rewritten (the body references the loop variable, not outer-scope columns).
fn replace_aggregates_with_columns(expr: &Expr) -> Expr {
    match expr {
        Expr::FunctionCall {
            name, window_spec, ..
        } if window_spec.is_none() && is_aggregate_function_name(name) => {
            // Replace bare aggregate with column reference
            Expr::Variable(aggregate_column_name(expr))
        }
        Expr::CountSubquery(_) | Expr::CollectSubquery(_) => {
            Expr::Variable(aggregate_column_name(expr))
        }
        Expr::ListComprehension {
            variable,
            list,
            where_clause,
            map_expr,
        } => Expr::ListComprehension {
            variable: variable.clone(),
            list: Box::new(replace_aggregates_with_columns(list)),
            where_clause: where_clause.clone(), // don't touch — references loop var
            map_expr: map_expr.clone(),         // don't touch — references loop var
        },
        Expr::Quantifier {
            quantifier,
            variable,
            list,
            predicate,
        } => Expr::Quantifier {
            quantifier: *quantifier,
            variable: variable.clone(),
            list: Box::new(replace_aggregates_with_columns(list)),
            predicate: predicate.clone(), // don't touch — references loop var
        },
        Expr::Reduce {
            accumulator,
            init,
            variable,
            list,
            expr: body,
        } => Expr::Reduce {
            accumulator: accumulator.clone(),
            init: Box::new(replace_aggregates_with_columns(init)),
            variable: variable.clone(),
            list: Box::new(replace_aggregates_with_columns(list)),
            expr: body.clone(), // don't touch — references loop var
        },
        Expr::FunctionCall {
            name,
            args,
            distinct,
            window_spec,
        } => Expr::FunctionCall {
            name: name.clone(),
            args: args.iter().map(replace_aggregates_with_columns).collect(),
            distinct: *distinct,
            window_spec: window_spec.clone(),
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(replace_aggregates_with_columns(left)),
            op: *op,
            right: Box::new(replace_aggregates_with_columns(right)),
        },
        Expr::UnaryOp { op, expr: e } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(replace_aggregates_with_columns(e)),
        },
        Expr::IsNull(e) => Expr::IsNull(Box::new(replace_aggregates_with_columns(e))),
        Expr::IsNotNull(e) => Expr::IsNotNull(Box::new(replace_aggregates_with_columns(e))),
        Expr::IsUnique(e) => Expr::IsUnique(Box::new(replace_aggregates_with_columns(e))),
        Expr::Property(base, prop) => Expr::Property(
            Box::new(replace_aggregates_with_columns(base)),
            prop.clone(),
        ),
        Expr::List(items) => {
            Expr::List(items.iter().map(replace_aggregates_with_columns).collect())
        }
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => Expr::Case {
            expr: case_expr
                .as_ref()
                .map(|e| Box::new(replace_aggregates_with_columns(e))),
            when_then: when_then
                .iter()
                .map(|(w, t)| {
                    (
                        replace_aggregates_with_columns(w),
                        replace_aggregates_with_columns(t),
                    )
                })
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(replace_aggregates_with_columns(e))),
        },
        Expr::In {
            expr: in_expr,
            list,
        } => Expr::In {
            expr: Box::new(replace_aggregates_with_columns(in_expr)),
            list: Box::new(replace_aggregates_with_columns(list)),
        },
        Expr::ArrayIndex { array, index } => Expr::ArrayIndex {
            array: Box::new(replace_aggregates_with_columns(array)),
            index: Box::new(replace_aggregates_with_columns(index)),
        },
        Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
            array: Box::new(replace_aggregates_with_columns(array)),
            start: start
                .as_ref()
                .map(|e| Box::new(replace_aggregates_with_columns(e))),
            end: end
                .as_ref()
                .map(|e| Box::new(replace_aggregates_with_columns(e))),
        },
        Expr::Map(entries) => Expr::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), replace_aggregates_with_columns(v)))
                .collect(),
        ),
        // Leaf expressions — return as-is
        other => other.clone(),
    }
}

/// Check if an expression contains any aggregate function (recursively).
fn contains_aggregate_recursive(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            is_aggregate_function_name(name) || args.iter().any(contains_aggregate_recursive)
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
        Expr::ListComprehension { list, .. } => {
            // Only check the list source — where_clause/map_expr reference the loop variable
            contains_aggregate_recursive(list)
        }
        Expr::Quantifier { list, .. } => contains_aggregate_recursive(list),
        Expr::Reduce { init, list, .. } => {
            contains_aggregate_recursive(init) || contains_aggregate_recursive(list)
        }
        Expr::ArrayIndex { array, index } => {
            contains_aggregate_recursive(array) || contains_aggregate_recursive(index)
        }
        Expr::ArraySlice { array, start, end } => {
            contains_aggregate_recursive(array)
                || start.as_deref().is_some_and(contains_aggregate_recursive)
                || end.as_deref().is_some_and(contains_aggregate_recursive)
        }
        Expr::Map(entries) => entries.iter().any(|(_, v)| contains_aggregate_recursive(v)),
        _ => false,
    }
}

/// Check if an expression contains a non-deterministic function (e.g. rand()).
fn contains_non_deterministic(expr: &Expr) -> bool {
    if matches!(expr, Expr::FunctionCall { name, .. } if name.eq_ignore_ascii_case("rand")) {
        return true;
    }
    let mut found = false;
    expr.for_each_child(&mut |child| {
        if !found {
            found = contains_non_deterministic(child);
        }
    });
    found
}

fn collect_aggregate_reprs(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            if is_aggregate_function_name(name) {
                out.insert(expr.to_string_repr());
                return;
            }
            for arg in args {
                collect_aggregate_reprs(arg, out);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_aggregate_reprs(left, out);
            collect_aggregate_reprs(right, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsUnique(expr) => collect_aggregate_reprs(expr, out),
        Expr::List(items) => {
            for item in items {
                collect_aggregate_reprs(item, out);
            }
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = expr {
                collect_aggregate_reprs(e, out);
            }
            for (w, t) in when_then {
                collect_aggregate_reprs(w, out);
                collect_aggregate_reprs(t, out);
            }
            if let Some(e) = else_expr {
                collect_aggregate_reprs(e, out);
            }
        }
        Expr::In { expr, list } => {
            collect_aggregate_reprs(expr, out);
            collect_aggregate_reprs(list, out);
        }
        Expr::Property(base, _) => collect_aggregate_reprs(base, out),
        Expr::ListComprehension { list, .. } => {
            collect_aggregate_reprs(list, out);
        }
        Expr::Quantifier { list, .. } => {
            collect_aggregate_reprs(list, out);
        }
        Expr::Reduce { init, list, .. } => {
            collect_aggregate_reprs(init, out);
            collect_aggregate_reprs(list, out);
        }
        Expr::ArrayIndex { array, index } => {
            collect_aggregate_reprs(array, out);
            collect_aggregate_reprs(index, out);
        }
        Expr::ArraySlice { array, start, end } => {
            collect_aggregate_reprs(array, out);
            if let Some(s) = start {
                collect_aggregate_reprs(s, out);
            }
            if let Some(e) = end {
                collect_aggregate_reprs(e, out);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone)]
enum NonAggregateRef {
    Var(String),
    Property {
        repr: String,
        base_var: Option<String>,
    },
}

fn collect_non_aggregate_refs(expr: &Expr, inside_agg: bool, out: &mut Vec<NonAggregateRef>) {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            if is_aggregate_function_name(name) {
                return;
            }
            for arg in args {
                collect_non_aggregate_refs(arg, inside_agg, out);
            }
        }
        Expr::Variable(v) if !inside_agg => out.push(NonAggregateRef::Var(v.clone())),
        Expr::Property(base, _) if !inside_agg => {
            let base_var = if let Expr::Variable(v) = base.as_ref() {
                Some(v.clone())
            } else {
                None
            };
            out.push(NonAggregateRef::Property {
                repr: expr.to_string_repr(),
                base_var,
            });
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_non_aggregate_refs(left, inside_agg, out);
            collect_non_aggregate_refs(right, inside_agg, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsUnique(expr) => collect_non_aggregate_refs(expr, inside_agg, out),
        Expr::List(items) => {
            for item in items {
                collect_non_aggregate_refs(item, inside_agg, out);
            }
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = expr {
                collect_non_aggregate_refs(e, inside_agg, out);
            }
            for (w, t) in when_then {
                collect_non_aggregate_refs(w, inside_agg, out);
                collect_non_aggregate_refs(t, inside_agg, out);
            }
            if let Some(e) = else_expr {
                collect_non_aggregate_refs(e, inside_agg, out);
            }
        }
        Expr::In { expr, list } => {
            collect_non_aggregate_refs(expr, inside_agg, out);
            collect_non_aggregate_refs(list, inside_agg, out);
        }
        // For ListComprehension/Quantifier/Reduce, only recurse into the `list`
        // source. The body references the loop variable, not outer-scope vars.
        Expr::ListComprehension { list, .. } => {
            collect_non_aggregate_refs(list, inside_agg, out);
        }
        Expr::Quantifier { list, .. } => {
            collect_non_aggregate_refs(list, inside_agg, out);
        }
        Expr::Reduce { init, list, .. } => {
            collect_non_aggregate_refs(init, inside_agg, out);
            collect_non_aggregate_refs(list, inside_agg, out);
        }
        _ => {}
    }
}

fn validate_with_order_by_aggregate_item(
    expr: &Expr,
    projected_aggregate_reprs: &HashSet<String>,
    projected_simple_reprs: &HashSet<String>,
    projected_aliases: &HashSet<String>,
) -> Result<()> {
    let mut aggregate_reprs = HashSet::new();
    collect_aggregate_reprs(expr, &mut aggregate_reprs);
    for agg in aggregate_reprs {
        if !projected_aggregate_reprs.contains(&agg) {
            return Err(anyhow!(
                "SyntaxError: UndefinedVariable - Aggregation expression '{}' is not projected in WITH",
                agg
            ));
        }
    }

    let mut refs = Vec::new();
    collect_non_aggregate_refs(expr, false, &mut refs);
    refs.retain(|r| match r {
        NonAggregateRef::Var(v) => !projected_aliases.contains(v),
        NonAggregateRef::Property { repr, .. } => !projected_simple_reprs.contains(repr),
    });

    let mut dedup = HashSet::new();
    refs.retain(|r| {
        let key = match r {
            NonAggregateRef::Var(v) => format!("v:{v}"),
            NonAggregateRef::Property { repr, .. } => format!("p:{repr}"),
        };
        dedup.insert(key)
    });

    if refs.len() > 1 {
        return Err(anyhow!(
            "SyntaxError: AmbiguousAggregationExpression - ORDER BY item mixes aggregation with multiple non-grouping references"
        ));
    }

    if let Some(r) = refs.first() {
        return match r {
            NonAggregateRef::Var(v) => Err(anyhow!(
                "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                v
            )),
            NonAggregateRef::Property { base_var, .. } => Err(anyhow!(
                "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                base_var
                    .clone()
                    .unwrap_or_else(|| "<property-base>".to_string())
            )),
        };
    }

    Ok(())
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

#[derive(Debug, Clone, Copy)]
enum ConstNumber {
    Int(i64),
    Float(f64),
}

impl ConstNumber {
    fn to_f64(self) -> f64 {
        match self {
            Self::Int(v) => v as f64,
            Self::Float(v) => v,
        }
    }
}

fn eval_const_numeric_expr(
    expr: &Expr,
    params: &std::collections::HashMap<String, uni_common::Value>,
) -> Result<ConstNumber> {
    match expr {
        Expr::Literal(CypherLiteral::Integer(n)) => Ok(ConstNumber::Int(*n)),
        Expr::Literal(CypherLiteral::Float(f)) => Ok(ConstNumber::Float(*f)),
        Expr::Parameter(name) => match params.get(name) {
            Some(uni_common::Value::Int(n)) => Ok(ConstNumber::Int(*n)),
            Some(uni_common::Value::Float(f)) => Ok(ConstNumber::Float(*f)),
            Some(uni_common::Value::Null) => Err(anyhow!(
                "TypeError: InvalidArgumentType - expected numeric value for parameter ${}, got null",
                name
            )),
            Some(other) => Err(anyhow!(
                "TypeError: InvalidArgumentType - expected numeric value for parameter ${}, got {:?}",
                name,
                other
            )),
            None => Err(anyhow!(
                "SyntaxError: InvalidArgumentType - expression is not a constant integer expression"
            )),
        },
        Expr::UnaryOp {
            op: uni_cypher::ast::UnaryOp::Neg,
            expr,
        } => match eval_const_numeric_expr(expr, params)? {
            ConstNumber::Int(v) => Ok(ConstNumber::Int(-v)),
            ConstNumber::Float(v) => Ok(ConstNumber::Float(-v)),
        },
        Expr::BinaryOp { left, op, right } => {
            let l = eval_const_numeric_expr(left, params)?;
            let r = eval_const_numeric_expr(right, params)?;
            match op {
                BinaryOp::Add => match (l, r) {
                    (ConstNumber::Int(a), ConstNumber::Int(b)) => Ok(ConstNumber::Int(a + b)),
                    _ => Ok(ConstNumber::Float(l.to_f64() + r.to_f64())),
                },
                BinaryOp::Sub => match (l, r) {
                    (ConstNumber::Int(a), ConstNumber::Int(b)) => Ok(ConstNumber::Int(a - b)),
                    _ => Ok(ConstNumber::Float(l.to_f64() - r.to_f64())),
                },
                BinaryOp::Mul => match (l, r) {
                    (ConstNumber::Int(a), ConstNumber::Int(b)) => Ok(ConstNumber::Int(a * b)),
                    _ => Ok(ConstNumber::Float(l.to_f64() * r.to_f64())),
                },
                BinaryOp::Div => Ok(ConstNumber::Float(l.to_f64() / r.to_f64())),
                BinaryOp::Mod => match (l, r) {
                    (ConstNumber::Int(a), ConstNumber::Int(b)) => Ok(ConstNumber::Int(a % b)),
                    _ => Ok(ConstNumber::Float(l.to_f64() % r.to_f64())),
                },
                BinaryOp::Pow => Ok(ConstNumber::Float(l.to_f64().powf(r.to_f64()))),
                _ => Err(anyhow!(
                    "SyntaxError: InvalidArgumentType - unsupported operator in constant expression"
                )),
            }
        }
        Expr::FunctionCall { name, args, .. } => {
            let lower = name.to_lowercase();
            match lower.as_str() {
                "rand" if args.is_empty() => {
                    use rand::Rng;
                    let mut rng = rand::thread_rng();
                    Ok(ConstNumber::Float(rng.r#gen::<f64>()))
                }
                "tointeger" | "toint" if args.len() == 1 => {
                    match eval_const_numeric_expr(&args[0], params)? {
                        ConstNumber::Int(v) => Ok(ConstNumber::Int(v)),
                        ConstNumber::Float(v) => Ok(ConstNumber::Int(v.trunc() as i64)),
                    }
                }
                "ceil" if args.len() == 1 => Ok(ConstNumber::Float(
                    eval_const_numeric_expr(&args[0], params)?.to_f64().ceil(),
                )),
                "floor" if args.len() == 1 => Ok(ConstNumber::Float(
                    eval_const_numeric_expr(&args[0], params)?.to_f64().floor(),
                )),
                "abs" if args.len() == 1 => match eval_const_numeric_expr(&args[0], params)? {
                    ConstNumber::Int(v) => Ok(ConstNumber::Int(v.abs())),
                    ConstNumber::Float(v) => Ok(ConstNumber::Float(v.abs())),
                },
                _ => Err(anyhow!(
                    "SyntaxError: InvalidArgumentType - expression is not a constant integer expression"
                )),
            }
        }
        _ => Err(anyhow!(
            "SyntaxError: InvalidArgumentType - expression is not a constant integer expression"
        )),
    }
}

/// Parse and validate a non-negative integer expression for SKIP or LIMIT.
/// Returns `Ok(Some(n))` for valid constants, or an error for negative/float/non-constant values.
fn parse_non_negative_integer(
    expr: &Expr,
    clause_name: &str,
    params: &std::collections::HashMap<String, uni_common::Value>,
) -> Result<Option<usize>> {
    let referenced_vars = collect_expr_variables(expr);
    if !referenced_vars.is_empty() {
        return Err(anyhow!(
            "SyntaxError: NonConstantExpression - {} requires expression independent of row variables",
            clause_name
        ));
    }

    let value = eval_const_numeric_expr(expr, params)?;
    let as_int = match value {
        ConstNumber::Int(v) => v,
        ConstNumber::Float(v) => {
            if !v.is_finite() || (v.fract().abs() > f64::EPSILON) {
                return Err(anyhow!(
                    "SyntaxError: InvalidArgumentType - {} requires integer, got float",
                    clause_name
                ));
            }
            v as i64
        }
    };
    if as_int < 0 {
        return Err(anyhow!(
            "SyntaxError: NegativeIntegerArgument - {} requires non-negative integer",
            clause_name
        ));
    }
    Ok(Some(as_int as usize))
}

/// Validate that aggregation functions are not nested.
fn validate_no_nested_aggregation(expr: &Expr) -> Result<()> {
    if let Expr::FunctionCall { name, args, .. } = expr
        && is_aggregate_function_name(name)
    {
        for arg in args {
            if contains_aggregate_recursive(arg) {
                return Err(anyhow!(
                    "SyntaxError: NestedAggregation - Cannot nest aggregation functions"
                ));
            }
            if contains_non_deterministic(arg) {
                return Err(anyhow!(
                    "SyntaxError: NonConstantExpression - Non-deterministic function inside aggregation"
                ));
            }
        }
    }
    let mut result = Ok(());
    expr.for_each_child(&mut |child| {
        if result.is_ok() {
            result = validate_no_nested_aggregation(child);
        }
    });
    result
}

/// Validate that an expression does not access properties or labels of
/// deleted entities. `type(r)` on a deleted relationship is allowed per
/// OpenCypher spec, but `n.prop` and `labels(n)` are not.
fn validate_no_deleted_entity_access(
    expr: &Expr,
    deleted_vars: &std::collections::HashSet<String>,
) -> Result<()> {
    // Check n.prop on a deleted variable
    if let Expr::Property(inner, _) = expr
        && let Expr::Variable(name) = inner.as_ref()
        && deleted_vars.contains(name)
    {
        return Err(anyhow!(
            "EntityNotFound: DeletedEntityAccess - Cannot access properties of deleted entity '{}'",
            name
        ));
    }
    // Check labels(n) or keys(n) on a deleted variable
    if let Expr::FunctionCall { name, args, .. } = expr
        && matches!(name.to_lowercase().as_str(), "labels" | "keys")
        && args.len() == 1
        && let Expr::Variable(var) = &args[0]
        && deleted_vars.contains(var)
    {
        return Err(anyhow!(
            "EntityNotFound: DeletedEntityAccess - Cannot access {} of deleted entity '{}'",
            name.to_lowercase(),
            var
        ));
    }
    let mut result = Ok(());
    expr.for_each_child(&mut |child| {
        if result.is_ok() {
            result = validate_no_deleted_entity_access(child, deleted_vars);
        }
    });
    result
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

fn build_merge_scope(pattern: &Pattern, vars_in_scope: &[VariableInfo]) -> Vec<VariableInfo> {
    let mut scope = vars_in_scope.to_vec();

    for path in &pattern.paths {
        if let Some(path_var) = &path.variable
            && !path_var.is_empty()
            && !is_var_in_scope(&scope, path_var)
        {
            scope.push(VariableInfo::new(path_var.clone(), VariableType::Path));
        }
        for element in &path.elements {
            match element {
                PatternElement::Node(n) => {
                    if let Some(v) = &n.variable
                        && !v.is_empty()
                        && !is_var_in_scope(&scope, v)
                    {
                        scope.push(VariableInfo::new(v.clone(), VariableType::Node));
                    }
                }
                PatternElement::Relationship(r) => {
                    if let Some(v) = &r.variable
                        && !v.is_empty()
                        && !is_var_in_scope(&scope, v)
                    {
                        scope.push(VariableInfo::new(v.clone(), VariableType::Edge));
                    }
                }
                PatternElement::Parenthesized { .. } => {}
            }
        }
    }

    scope
}

fn validate_merge_set_item(item: &SetItem, vars_in_scope: &[VariableInfo]) -> Result<()> {
    match item {
        SetItem::Property { expr, value } => {
            validate_expression_variables(expr, vars_in_scope)?;
            validate_expression(expr, vars_in_scope)?;
            validate_expression_variables(value, vars_in_scope)?;
            validate_expression(value, vars_in_scope)?;
            if contains_pattern_predicate(expr) || contains_pattern_predicate(value) {
                return Err(anyhow!(
                    "SyntaxError: UnexpectedSyntax - Pattern predicates are not allowed in SET"
                ));
            }
        }
        SetItem::Variable { variable, value } | SetItem::VariablePlus { variable, value } => {
            if !is_var_in_scope(vars_in_scope, variable) {
                return Err(anyhow!(
                    "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                    variable
                ));
            }
            validate_expression_variables(value, vars_in_scope)?;
            validate_expression(value, vars_in_scope)?;
            if contains_pattern_predicate(value) {
                return Err(anyhow!(
                    "SyntaxError: UnexpectedSyntax - Pattern predicates are not allowed in SET"
                ));
            }
        }
        SetItem::Labels { variable, .. } => {
            if !is_var_in_scope(vars_in_scope, variable) {
                return Err(anyhow!(
                    "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                    variable
                ));
            }
        }
    }

    Ok(())
}

/// Reject MERGE patterns containing null property values (e.g. `MERGE ({k: null})`).
/// The OpenCypher spec requires all property values in MERGE to be non-null.
fn reject_null_merge_properties(properties: &Option<Expr>) -> Result<()> {
    if let Some(Expr::Map(entries)) = properties {
        for (key, value) in entries {
            if matches!(value, Expr::Literal(CypherLiteral::Null)) {
                return Err(anyhow!(
                    "SemanticError: MergeReadOwnWrites - MERGE cannot use null property value for '{}'",
                    key
                ));
            }
        }
    }
    Ok(())
}

fn validate_merge_clause(merge_clause: &MergeClause, vars_in_scope: &[VariableInfo]) -> Result<()> {
    for path in &merge_clause.pattern.paths {
        for element in &path.elements {
            match element {
                PatternElement::Node(n) => {
                    if let Some(Expr::Parameter(_)) = &n.properties {
                        return Err(anyhow!(
                            "SyntaxError: InvalidParameterUse - Parameters cannot be used as node predicates"
                        ));
                    }
                    reject_null_merge_properties(&n.properties)?;
                    // VariableAlreadyBound: reject if a bound variable is used
                    // as a standalone MERGE node or introduces new labels/properties.
                    // Bare endpoint references like (a) in MERGE (a)-[:R]->(b) are valid.
                    if let Some(variable) = &n.variable
                        && !variable.is_empty()
                        && is_var_in_scope(vars_in_scope, variable)
                    {
                        let is_standalone = path.elements.len() == 1;
                        let has_new_labels = !n.labels.is_empty();
                        let has_new_properties = n.properties.is_some();
                        if is_standalone || has_new_labels || has_new_properties {
                            return Err(anyhow!(
                                "SyntaxError: VariableAlreadyBound - Variable '{}' already defined",
                                variable
                            ));
                        }
                    }
                }
                PatternElement::Relationship(r) => {
                    if let Some(variable) = &r.variable
                        && !variable.is_empty()
                        && is_var_in_scope(vars_in_scope, variable)
                    {
                        return Err(anyhow!(
                            "SyntaxError: VariableAlreadyBound - Variable '{}' already defined",
                            variable
                        ));
                    }
                    if r.types.len() != 1 {
                        return Err(anyhow!(
                            "SyntaxError: NoSingleRelationshipType - Exactly one relationship type required for MERGE"
                        ));
                    }
                    if r.range.is_some() {
                        return Err(anyhow!(
                            "SyntaxError: CreatingVarLength - Variable length relationships cannot be created"
                        ));
                    }
                    if let Some(Expr::Parameter(_)) = &r.properties {
                        return Err(anyhow!(
                            "SyntaxError: InvalidParameterUse - Parameters cannot be used as relationship predicates"
                        ));
                    }
                    reject_null_merge_properties(&r.properties)?;
                }
                PatternElement::Parenthesized { .. } => {}
            }
        }
    }

    let merge_scope = build_merge_scope(&merge_clause.pattern, vars_in_scope);
    for item in &merge_clause.on_create {
        validate_merge_set_item(item, &merge_scope)?;
    }
    for item in &merge_clause.on_match {
        validate_merge_set_item(item, &merge_scope)?;
    }

    Ok(())
}

/// Recursively validate an expression for type errors, undefined variables, etc.
fn validate_expression(expr: &Expr, vars_in_scope: &[VariableInfo]) -> Result<()> {
    // Validate boolean operators and nested aggregation first
    validate_boolean_expression(expr)?;
    validate_no_nested_aggregation(expr)?;

    // Helper to validate multiple expressions
    fn validate_all(exprs: &[Expr], vars: &[VariableInfo]) -> Result<()> {
        for e in exprs {
            validate_expression(e, vars)?;
        }
        Ok(())
    }

    match expr {
        Expr::FunctionCall { name, args, .. } => {
            validate_function_call(name, args, vars_in_scope)?;
            validate_all(args, vars_in_scope)
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expression(left, vars_in_scope)?;
            validate_expression(right, vars_in_scope)
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsUnique(e) => validate_expression(e, vars_in_scope),
        Expr::Property(base, prop) => {
            if let Expr::Variable(var_name) = base.as_ref()
                && let Some(var_info) = find_var_in_scope(vars_in_scope, var_name)
            {
                // Paths don't have properties
                if var_info.var_type == VariableType::Path {
                    return Err(anyhow!(
                        "SyntaxError: InvalidArgumentType - Type mismatch: expected Node or Relationship but was Path for property access '{}.{}'",
                        var_name,
                        prop
                    ));
                }
                // Known non-graph literals (int, float, bool, string, list) don't have properties
                if var_info.var_type == VariableType::ScalarLiteral {
                    return Err(anyhow!(
                        "TypeError: InvalidArgumentType - Property access on a non-graph element is not allowed"
                    ));
                }
            }
            validate_expression(base, vars_in_scope)
        }
        Expr::List(items) => validate_all(items, vars_in_scope),
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
            Ok(())
        }
        Expr::In { expr: e, list } => {
            validate_expression(e, vars_in_scope)?;
            validate_expression(list, vars_in_scope)
        }
        Expr::Exists {
            query,
            from_pattern_predicate: true,
        } => {
            // Pattern predicates cannot introduce new named variables.
            // Extract named vars from inner MATCH pattern, check each is in scope.
            if let Query::Single(stmt) = query.as_ref() {
                for clause in &stmt.clauses {
                    if let Clause::Match(m) = clause {
                        for path in &m.pattern.paths {
                            for elem in &path.elements {
                                match elem {
                                    PatternElement::Node(n) => {
                                        if let Some(var) = &n.variable
                                            && !is_var_in_scope(vars_in_scope, var)
                                        {
                                            return Err(anyhow!(
                                                "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                                                var
                                            ));
                                        }
                                    }
                                    PatternElement::Relationship(r) => {
                                        if let Some(var) = &r.variable
                                            && !is_var_in_scope(vars_in_scope, var)
                                        {
                                            return Err(anyhow!(
                                                "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                                                var
                                            ));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// One step (hop) in a Quantified Path Pattern sub-pattern.
/// Used by `LogicalPlan::Traverse` when `qpp_steps` is `Some`.
#[derive(Debug, Clone)]
pub struct QppStepInfo {
    pub edge_type_ids: Vec<u32>,
    pub direction: Direction,
    pub target_label: Option<String>,
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
    /// Scan main vertices table by label name(s) for schemaless support.
    /// When labels has multiple entries, uses intersection semantics (must have ALL labels).
    ScanMainByLabels {
        labels: Vec<String>,
        variable: String,
        filter: Option<Expr>,
        optional: bool,
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
        /// Whether this is a variable-length pattern (has `*` range specifier).
        /// When true, step_variable holds a list of edges (even for *1..1).
        is_variable_length: bool,
        /// All variables from this OPTIONAL MATCH pattern.
        /// When any hop in the pattern fails, ALL these variables should be set to NULL.
        /// This ensures proper multi-hop OPTIONAL MATCH semantics.
        optional_pattern_vars: std::collections::HashSet<String>,
        /// Variable names (node + edge) from the current MATCH clause scope.
        /// Used for relationship uniqueness scoping: only edge ID columns whose
        /// associated variable is in this set participate in uniqueness filtering.
        /// Variables from previous disconnected MATCH clauses are excluded.
        scope_match_variables: std::collections::HashSet<String>,
        /// Edge property predicate for VLP inline filtering (instead of post-Filter).
        edge_filter_expr: Option<Expr>,
        /// Path traversal semantics (Trail by default for OpenCypher).
        path_mode: crate::query::df_graph::nfa::PathMode,
        /// QPP steps for multi-hop quantified path patterns.
        /// `None` for simple VLP patterns; `Some` for QPP with per-step edge types/constraints.
        /// When present, `min_hops`/`max_hops` are derived from iterations × steps.len().
        qpp_steps: Option<Vec<QppStepInfo>>,
    },
    /// Traverse main edges table filtering by type name(s) (MATCH (a)-[:Unknown]->(b)).
    /// Used for edge types not defined in schema (schemaless support).
    /// Supports OR relationship types like `[:KNOWS|HATES]` via multiple type_names.
    TraverseMainByType {
        type_names: Vec<String>,
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
        /// Whether this is a variable-length pattern (has `*` range specifier).
        /// When true, step_variable holds a list of edges (even for *1..1).
        is_variable_length: bool,
        /// All variables from this OPTIONAL MATCH pattern.
        /// When any hop in the pattern fails, ALL these variables should be set to NULL.
        optional_pattern_vars: std::collections::HashSet<String>,
        /// Variables belonging to the current MATCH clause scope.
        /// Used for relationship uniqueness scoping: only edge columns whose
        /// associated variable is in this set participate in uniqueness filtering.
        scope_match_variables: std::collections::HashSet<String>,
        /// Edge property predicate for VLP inline filtering (instead of post-Filter).
        edge_filter_expr: Option<Expr>,
        /// Path traversal semantics (Trail by default for OpenCypher).
        path_mode: crate::query::df_graph::nfa::PathMode,
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
    /// Bind a zero-length path (single node pattern with path variable).
    /// E.g., `p = (a)` creates a Path with one node and zero edges.
    BindZeroLengthPath {
        input: Box<LogicalPlan>,
        node_variable: String,
        path_variable: String,
    },
    /// Bind a fixed-length path from already-computed node and edge columns.
    /// E.g., `p = (a)-[r]->(b)` or `p = (a)-[r1]->(b)-[r2]->(c)`.
    BindPath {
        input: Box<LogicalPlan>,
        node_variables: Vec<String>,
        edge_variables: Vec<String>,
        path_variable: String,
    },
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
    /// Optional query parameters for resolving $param in SKIP/LIMIT.
    params: std::collections::HashMap<String, uni_common::Value>,
}

struct TraverseParams<'a> {
    rel: &'a RelationshipPattern,
    target_node: &'a NodePattern,
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
            params: std::collections::HashMap::new(),
        }
    }

    /// Set query parameters for resolving `$param` references in SKIP/LIMIT.
    pub fn with_params(
        mut self,
        params: std::collections::HashMap<String, uni_common::Value>,
    ) -> Self {
        self.params = params;
        self
    }

    pub fn plan(&self, query: Query) -> Result<LogicalPlan> {
        self.plan_with_scope(query, Vec::new())
    }

    pub fn plan_with_scope(&self, query: Query, vars: Vec<String>) -> Result<LogicalPlan> {
        // Apply query rewrites before planning
        let rewritten_query = crate::query::rewrite::rewrite_query(query)?;
        if Self::has_mixed_union_modes(&rewritten_query) {
            return Err(anyhow!(
                "SyntaxError: InvalidClauseComposition - Cannot mix UNION and UNION ALL in the same query"
            ));
        }

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

    fn collect_union_modes(query: &Query, out: &mut HashSet<bool>) {
        match query {
            Query::Union { left, right, all } => {
                out.insert(*all);
                Self::collect_union_modes(left, out);
                Self::collect_union_modes(right, out);
            }
            Query::Explain(inner) => Self::collect_union_modes(inner, out),
            Query::TimeTravel { query, .. } => Self::collect_union_modes(query, out),
            Query::Single(_) | Query::Schema(_) | Query::Transaction(_) => {}
        }
    }

    fn has_mixed_union_modes(query: &Query) -> bool {
        let mut modes = HashSet::new();
        Self::collect_union_modes(query, &mut modes);
        modes.len() > 1
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
            LogicalPlan::Union { left, right, .. } => {
                let left_cols = Self::extract_projection_columns(left);
                if left_cols.is_empty() {
                    Self::extract_projection_columns(right)
                } else {
                    left_cols
                }
            }
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
        let mut compound_agg_exprs: Vec<Expr> = Vec::new();
        let mut has_agg = false;
        let mut projections = Vec::new();
        let mut projected_aggregate_reprs: HashSet<String> = HashSet::new();
        let mut projected_simple_reprs: HashSet<String> = HashSet::new();
        let mut projected_aliases: HashSet<String> = HashSet::new();

        for item in &return_clause.items {
            match item {
                ReturnItem::All => {
                    // RETURN * - add all user-named variables in scope
                    // (anonymous variables like _anon_0 are excluded)
                    let user_vars: Vec<_> = vars_in_scope
                        .iter()
                        .filter(|v| !v.name.starts_with("_anon_"))
                        .collect();
                    if user_vars.is_empty() {
                        return Err(anyhow!(
                            "SyntaxError: NoVariablesInScope - RETURN * is not allowed when there are no variables in scope"
                        ));
                    }
                    for v in user_vars {
                        projections.push((Expr::Variable(v.name.clone()), Some(v.name.clone())));
                        if !group_by.contains(&Expr::Variable(v.name.clone())) {
                            group_by.push(Expr::Variable(v.name.clone()));
                        }
                        projected_aliases.insert(v.name.clone());
                        projected_simple_reprs.insert(v.name.clone());
                    }
                }
                ReturnItem::Expr {
                    expr,
                    alias,
                    source_text,
                } => {
                    if matches!(expr, Expr::Wildcard) {
                        for v in vars_in_scope {
                            projections
                                .push((Expr::Variable(v.name.clone()), Some(v.name.clone())));
                            if !group_by.contains(&Expr::Variable(v.name.clone())) {
                                group_by.push(Expr::Variable(v.name.clone()));
                            }
                            projected_aliases.insert(v.name.clone());
                            projected_simple_reprs.insert(v.name.clone());
                        }
                    } else {
                        // Validate expression variables are defined
                        validate_expression_variables(expr, vars_in_scope)?;
                        // Validate function argument types and boolean operators
                        validate_expression(expr, vars_in_scope)?;
                        // Pattern predicates are not allowed in RETURN
                        if contains_pattern_predicate(expr) {
                            return Err(anyhow!(
                                "SyntaxError: UnexpectedSyntax - Pattern predicates are not allowed in RETURN"
                            ));
                        }

                        // Use source text as column name when no explicit alias
                        let effective_alias = alias.clone().or_else(|| source_text.clone());
                        projections.push((expr.clone(), effective_alias));
                        if expr.is_aggregate() && !is_compound_aggregate(expr) {
                            // Bare aggregate — push directly
                            has_agg = true;
                            aggregates.push(expr.clone());
                            projected_aggregate_reprs.insert(expr.to_string_repr());
                        } else if !is_window_function(expr)
                            && (expr.is_aggregate() || contains_aggregate_recursive(expr))
                        {
                            // Compound aggregate or expression containing aggregates —
                            // extract the inner bare aggregates for the Aggregate node
                            has_agg = true;
                            compound_agg_exprs.push(expr.clone());
                            for inner in extract_inner_aggregates(expr) {
                                let repr = inner.to_string_repr();
                                if !projected_aggregate_reprs.contains(&repr) {
                                    aggregates.push(inner);
                                    projected_aggregate_reprs.insert(repr);
                                }
                            }
                        } else if !group_by.contains(expr) {
                            group_by.push(expr.clone());
                            if matches!(expr, Expr::Variable(_) | Expr::Property(_, _)) {
                                projected_simple_reprs.insert(expr.to_string_repr());
                            }
                        }

                        if let Some(a) = alias {
                            if projected_aliases.contains(a) {
                                return Err(anyhow!(
                                    "SyntaxError: ColumnNameConflict - Duplicate column name '{}' in RETURN",
                                    a
                                ));
                            }
                            projected_aliases.insert(a.clone());
                        } else if let Expr::Variable(v) = expr {
                            if projected_aliases.contains(v) {
                                return Err(anyhow!(
                                    "SyntaxError: ColumnNameConflict - Duplicate column name '{}' in RETURN",
                                    v
                                ));
                            }
                            projected_aliases.insert(v.clone());
                        }
                    }
                }
            }
        }

        // Validate compound aggregate expressions: non-aggregate refs must be
        // individually present in the group_by as simple variables or properties.
        if has_agg {
            let group_by_reprs: HashSet<String> =
                group_by.iter().map(|e| e.to_string_repr()).collect();
            for expr in &compound_agg_exprs {
                let mut refs = Vec::new();
                collect_non_aggregate_refs(expr, false, &mut refs);
                for r in &refs {
                    let is_covered = match r {
                        NonAggregateRef::Var(v) => group_by_reprs.contains(v),
                        NonAggregateRef::Property { repr, .. } => group_by_reprs.contains(repr),
                    };
                    if !is_covered {
                        return Err(anyhow!(
                            "SyntaxError: AmbiguousAggregationExpression - Expression mixes aggregation with non-grouped reference"
                        ));
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
            let alias_exprs: HashMap<String, Expr> = projections
                .iter()
                .filter_map(|(expr, alias)| {
                    alias.as_ref().map(|a| {
                        // ORDER BY is planned before the final RETURN projection.
                        // In aggregate contexts, aliases must resolve to the
                        // post-aggregate output columns, not raw aggregate calls.
                        let rewritten = if has_agg && !has_window_exprs {
                            if expr.is_aggregate() && !is_compound_aggregate(expr) {
                                Expr::Variable(aggregate_column_name(expr))
                            } else if is_compound_aggregate(expr)
                                || (!expr.is_aggregate() && contains_aggregate_recursive(expr))
                            {
                                replace_aggregates_with_columns(expr)
                            } else {
                                Expr::Variable(expr.to_string_repr())
                            }
                        } else {
                            expr.clone()
                        };
                        (a.clone(), rewritten)
                    })
                })
                .collect();

            // Build an extended scope that includes RETURN aliases so ORDER BY
            // can reference them (e.g. RETURN n.age AS age ORDER BY age).
            let order_by_scope: Vec<VariableInfo> = if return_clause.distinct {
                // DISTINCT in RETURN narrows ORDER BY visibility to returned columns.
                // Keep aliases and directly returned variables in scope.
                let mut scope = Vec::new();
                for (expr, alias) in &projections {
                    if let Some(a) = alias
                        && !is_var_in_scope(&scope, a)
                    {
                        scope.push(VariableInfo::new(a.clone(), VariableType::Scalar));
                    }
                    if let Expr::Variable(v) = expr
                        && !is_var_in_scope(&scope, v)
                    {
                        scope.push(VariableInfo::new(v.clone(), VariableType::Scalar));
                    }
                }
                scope
            } else {
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
                // DISTINCT allows ORDER BY on the same projected expression
                // even when underlying variables are not otherwise visible.
                let matches_projected_expr = return_clause.distinct
                    && projections
                        .iter()
                        .any(|(expr, _)| expr.to_string_repr() == item.expr.to_string_repr());
                if !matches_projected_expr {
                    validate_expression_variables(&item.expr, &order_by_scope)?;
                    validate_expression(&item.expr, &order_by_scope)?;
                }
                let has_aggregate_in_item = contains_aggregate_recursive(&item.expr);
                if has_aggregate_in_item && !has_agg {
                    return Err(anyhow!(
                        "SyntaxError: InvalidAggregation - Aggregation functions not allowed in ORDER BY after RETURN"
                    ));
                }
                if has_agg && has_aggregate_in_item {
                    validate_with_order_by_aggregate_item(
                        &item.expr,
                        &projected_aggregate_reprs,
                        &projected_simple_reprs,
                        &projected_aliases,
                    )?;
                }
            }
            let rewritten_order_by: Vec<SortItem> = order_by
                .iter()
                .map(|item| SortItem {
                    expr: {
                        let mut rewritten =
                            rewrite_order_by_expr_with_aliases(&item.expr, &alias_exprs);
                        if has_agg && !has_window_exprs {
                            rewritten = replace_aggregates_with_columns(&rewritten);
                        }
                        rewritten
                    },
                    ascending: item.ascending,
                })
                .collect();
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: rewritten_order_by,
            };
        }

        if return_clause.skip.is_some() || return_clause.limit.is_some() {
            let skip = return_clause
                .skip
                .as_ref()
                .map(|e| parse_non_negative_integer(e, "SKIP", &self.params))
                .transpose()?
                .flatten();
            let fetch = return_clause
                .limit
                .as_ref()
                .map(|e| parse_non_negative_integer(e, "LIMIT", &self.params))
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
                        if expr.is_aggregate() && !is_compound_aggregate(&expr) && !has_window_exprs
                        {
                            // Bare aggregate — replace with column reference
                            let col_name = Self::get_aggregate_column_name(&expr);
                            (Expr::Variable(col_name), alias)
                        } else if !has_window_exprs
                            && (is_compound_aggregate(&expr)
                                || (!expr.is_aggregate() && contains_aggregate_recursive(&expr)))
                        {
                            // Compound aggregate — replace inner aggregates with
                            // column references, keep outer expression for Project
                            (replace_aggregates_with_columns(&expr), alias)
                        }
                        // For grouped RETURN projections, reference the pre-computed
                        // group-by output column instead of re-evaluating the expression
                        // against the aggregate schema (which no longer has original vars).
                        else if has_agg
                            && !has_window_exprs
                            && !matches!(expr, Expr::Variable(_) | Expr::Property(_, _))
                        {
                            (Expr::Variable(expr.to_string_repr()), alias)
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
            // Project bound variables from outer scope as parameters.
            // These come from the enclosing query's row (passed as sub_params in EXISTS evaluation).
            // Use Parameter expressions to read from params, not Variable which would read from input row.
            let projections = initial_vars
                .iter()
                .map(|v| (Expr::Parameter(v.name.clone()), Some(v.name.clone())))
                .collect();
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections,
            };
        }

        let mut vars_in_scope: Vec<VariableInfo> = initial_vars;
        // Track variables introduced by CREATE clauses so we can distinguish
        // MATCH-introduced variables (which cannot be re-created as bare nodes)
        // from CREATE-introduced variables (which can be referenced as bare nodes).
        let mut create_introduced_vars: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        // Track variables targeted by DELETE so we can reject property/label
        // access on deleted entities in subsequent RETURN clauses.
        let mut deleted_vars: std::collections::HashSet<String> = std::collections::HashSet::new();

        let clause_count = query.clauses.len();
        for (clause_idx, clause) in query.clauses.into_iter().enumerate() {
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
                    let unwind_out_type = infer_unwind_output_type(&unwind.expr, &vars_in_scope);
                    add_var_to_scope(&mut vars_in_scope, &unwind.variable, unwind_out_type)?;
                }
                Clause::Call(call_clause) => {
                    match &call_clause.kind {
                        CallKind::Procedure {
                            procedure,
                            arguments,
                        } => {
                            // Validate that procedure arguments don't contain aggregation functions
                            for arg in arguments {
                                if contains_aggregate_recursive(arg) {
                                    return Err(anyhow!(
                                        "SyntaxError: InvalidAggregation - Aggregation expressions are not allowed as arguments to procedure calls"
                                    ));
                                }
                            }

                            let has_yield_star = call_clause.yield_items.len() == 1
                                && call_clause.yield_items[0].name == "*"
                                && call_clause.yield_items[0].alias.is_none();
                            if has_yield_star && clause_idx + 1 < clause_count {
                                return Err(anyhow!(
                                    "SyntaxError: UnexpectedSyntax - YIELD * is only allowed in standalone procedure calls"
                                ));
                            }

                            // Validate for duplicate yield names (VariableAlreadyBound)
                            let mut yield_names = Vec::new();
                            for item in &call_clause.yield_items {
                                if item.name == "*" {
                                    continue;
                                }
                                let output_name = item.alias.as_ref().unwrap_or(&item.name);
                                if yield_names.contains(output_name) {
                                    return Err(anyhow!(
                                        "SyntaxError: VariableAlreadyBound - Variable '{}' already appears in YIELD clause",
                                        output_name
                                    ));
                                }
                                // Check against existing scope (in-query CALL must not shadow)
                                if clause_idx > 0
                                    && vars_in_scope.iter().any(|v| v.name == *output_name)
                                {
                                    return Err(anyhow!(
                                        "SyntaxError: VariableAlreadyBound - Variable '{}' already declared in outer scope",
                                        output_name
                                    ));
                                }
                                yield_names.push(output_name.clone());
                            }

                            let mut yields = Vec::new();
                            for item in &call_clause.yield_items {
                                if item.name == "*" {
                                    continue;
                                }
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
                            let proc_plan = LogicalPlan::ProcedureCall {
                                procedure_name: procedure.clone(),
                                arguments: arguments.clone(),
                                yield_items: yields.clone(),
                            };

                            if matches!(plan, LogicalPlan::Empty) {
                                // Standalone CALL (first clause) — use directly
                                plan = proc_plan;
                            } else if yields.is_empty() {
                                // In-query CALL with no YIELD (void procedure):
                                // preserve the input rows unchanged
                            } else {
                                // In-query CALL with YIELD: cross-join input × procedure output
                                plan = LogicalPlan::Apply {
                                    input: Box::new(plan),
                                    subquery: Box::new(proc_plan),
                                    input_filter: None,
                                };
                            }
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
                    validate_merge_clause(&merge_clause, &vars_in_scope)?;

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
                        if let Some(path_var) = &path.variable
                            && !path_var.is_empty()
                            && !is_var_in_scope(&vars_in_scope, path_var)
                        {
                            add_var_to_scope(&mut vars_in_scope, path_var, VariableType::Path)?;
                        }
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
                        let is_standalone_node = path.elements.len() == 1;
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
                                        } else if is_standalone_node
                                            && is_var_in_scope(&vars_in_scope, v)
                                            && !create_introduced_vars.contains(v)
                                        {
                                            // Standalone bare node referencing a variable from a
                                            // non-CREATE clause (e.g. MATCH (a) CREATE (a)) — invalid.
                                            // Bare nodes used as relationship endpoints
                                            // (e.g. CREATE (a)-[:R]->(b)) are valid references.
                                            return Err(anyhow!(
                                                "SyntaxError: VariableAlreadyBound - '{}'",
                                                v
                                            ));
                                        } else if !create_vars.contains(&v) {
                                            // New bare variable — register it
                                            create_vars.push(v);
                                        }
                                        // else: bare reference to same-CREATE or previous-CREATE variable — OK
                                    }
                                }
                                PatternElement::Relationship(r) => {
                                    validate_property_variables(
                                        &r.properties,
                                        &vars_in_scope,
                                        &create_vars,
                                    )?;

                                    if let Some(v) = r.variable.as_deref()
                                        && !v.is_empty()
                                    {
                                        check_not_already_bound(v, &vars_in_scope, &create_vars)?;
                                        create_vars.push(v);
                                    }

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
                                        create_introduced_vars.insert(var.clone());
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
                                        create_introduced_vars.insert(var.clone());
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
                    // Validate SET value expressions
                    for item in &set_clause.items {
                        match item {
                            SetItem::Property { value, .. }
                            | SetItem::Variable { value, .. }
                            | SetItem::VariablePlus { value, .. } => {
                                validate_expression_variables(value, &vars_in_scope)?;
                                validate_expression(value, &vars_in_scope)?;
                                if contains_pattern_predicate(value) {
                                    return Err(anyhow!(
                                        "SyntaxError: UnexpectedSyntax - Pattern predicates are not allowed in SET"
                                    ));
                                }
                            }
                            SetItem::Labels { .. } => {}
                        }
                    }
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
                    // Validate DELETE targets
                    for item in &delete_clause.items {
                        // DELETE n:Label is invalid syntax (label expressions not allowed)
                        if matches!(item, Expr::LabelCheck { .. }) {
                            return Err(anyhow!(
                                "SyntaxError: InvalidDelete - DELETE requires a simple variable reference, not a label expression"
                            ));
                        }
                        let vars_used = collect_expr_variables(item);
                        // Reject expressions with no variable references (e.g. DELETE 1+1)
                        if vars_used.is_empty() {
                            return Err(anyhow!(
                                "SyntaxError: InvalidArgumentType - DELETE requires node or relationship, not a literal expression"
                            ));
                        }
                        for var in &vars_used {
                            // Check if variable is defined
                            if find_var_in_scope(&vars_in_scope, var).is_none() {
                                return Err(anyhow!(
                                    "SyntaxError: UndefinedVariable - Variable '{}' not defined",
                                    var
                                ));
                            }
                        }
                        // Strict type check only for simple variable references —
                        // complex expressions (property access, array index, etc.)
                        // may resolve to a node/edge at runtime even if the base
                        // variable is typed as Scalar (e.g. nodes(p)[0]).
                        if let Expr::Variable(name) = item
                            && let Some(info) = find_var_in_scope(&vars_in_scope, name)
                            && matches!(
                                info.var_type,
                                VariableType::Scalar | VariableType::ScalarLiteral
                            )
                        {
                            return Err(anyhow!(
                                "SyntaxError: InvalidArgumentType - DELETE requires node or relationship, '{}' is a scalar value",
                                name
                            ));
                        }
                    }
                    // Track deleted variables for later validation
                    for item in &delete_clause.items {
                        if let Expr::Variable(name) = item {
                            deleted_vars.insert(name.clone());
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
                    // Check for property/label access on deleted entities
                    if !deleted_vars.is_empty() {
                        for item in &return_clause.items {
                            if let ReturnItem::Expr { expr, .. } = item {
                                validate_no_deleted_entity_access(expr, &deleted_vars)?;
                            }
                        }
                    }
                    plan = self.plan_return_clause(&return_clause, plan, &vars_in_scope)?;
                } // All Clause variants are handled above - no catch-all needed
            }
        }

        // Wrap write operations without RETURN in Limit(0) per OpenCypher spec.
        // CREATE (n) should return 0 rows, but CREATE (n) RETURN n should return 1 row.
        // If RETURN was used, the plan will have been wrapped in Project, so we only
        // wrap terminal Create/CreateBatch/Delete/Set/Remove nodes.
        let plan = match &plan {
            LogicalPlan::Create { .. }
            | LogicalPlan::CreateBatch { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Set { .. }
            | LogicalPlan::Remove { .. }
            | LogicalPlan::Merge { .. } => LogicalPlan::Limit {
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
            Expr::CountSubquery(_) | Expr::Exists { .. } => {}
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
                plan =
                    self.plan_shortest_path(path, plan, vars_in_scope, mode, vars_before_pattern)?;
            } else {
                plan = self.plan_path(
                    path,
                    plan,
                    vars_in_scope,
                    match_clause.optional,
                    vars_before_pattern,
                )?;
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
        _vars_before_pattern: usize,
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

            plan = Self::join_with_plan(plan, target_scan);
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
    /// Plan a regular MATCH path (not shortestPath).
    fn plan_path(
        &self,
        path: &PathPattern,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<VariableInfo>,
        optional: bool,
        vars_before_pattern: usize,
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
                            && !is_var_in_scope(vars_in_scope, v)
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
                                        && !is_var_in_scope(vars_in_scope, v)
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

        // Pre-scan path elements for bound edge variables from previous MATCH clauses.
        // These must participate in Trail mode (relationship uniqueness) enforcement
        // across ALL segments in this path, so that VLP segments like [*0..1] don't
        // traverse through edges already claimed by a bound relationship [r].
        let path_bound_edge_vars: std::collections::HashSet<String> = {
            let mut bound = std::collections::HashSet::new();
            for element in elements {
                if let PatternElement::Relationship(rel) = element
                    && let Some(ref var_name) = rel.variable
                    && !var_name.is_empty()
                    && vars_in_scope[..vars_before_pattern]
                        .iter()
                        .any(|v| v.name == *var_name)
                {
                    bound.insert(var_name.clone());
                }
            }
            bound
        };

        // Track if any traverses were added (for zero-length path detection)
        let mut had_traverses = false;
        // Track the node variable for zero-length path binding
        let mut single_node_variable: Option<String> = None;
        // Collect node/edge variables for BindPath (fixed-length path binding)
        let mut path_node_vars: Vec<String> = Vec::new();
        let mut path_edge_vars: Vec<String> = Vec::new();
        // Track the last processed outer node variable for QPP source binding.
        // In `(a)((x)-[:R]->(y)){n}(b)`, the QPP source is `a`, not `x`.
        let mut last_outer_node_var: Option<String> = None;

        // Multi-hop path variables are now supported - path is accumulated across hops
        while i < elements.len() {
            let element = &elements[i];
            match element {
                PatternElement::Node(n) => {
                    let mut variable = n.variable.clone().unwrap_or_default();
                    if variable.is_empty() {
                        variable = self.next_anon_var();
                    }
                    // Track first node variable for zero-length path
                    if single_node_variable.is_none() {
                        single_node_variable = Some(variable.clone());
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
                        if let Some(node_filter) =
                            self.node_filter_expr(&variable, &n.labels, &n.properties)
                        {
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate: node_filter,
                                optional_variables: std::collections::HashSet::new(),
                            };
                        }
                    } else {
                        plan = self.plan_unbound_node(n, &variable, plan, optional)?;
                        if !variable.is_empty() {
                            add_var_to_scope(vars_in_scope, &variable, VariableType::Node)?;
                        }
                    }

                    // Track source node for BindPath
                    if path_variable.is_some() && path_node_vars.is_empty() {
                        path_node_vars.push(variable.clone());
                    }

                    // Look ahead for relationships
                    let mut current_source_var = variable;
                    last_outer_node_var = Some(current_source_var.clone());
                    i += 1;
                    while i < elements.len() {
                        if let PatternElement::Relationship(r) = &elements[i] {
                            if i + 1 < elements.len() {
                                let target_node_part = &elements[i + 1];
                                if let PatternElement::Node(n_target) = target_node_part {
                                    // For VLP traversals, pass path_variable through
                                    // For fixed-length, we use BindPath instead
                                    let is_vlp = r.range.is_some();
                                    let traverse_path_var =
                                        if is_vlp { path_variable.clone() } else { None };

                                    // If we're about to start a VLP segment and there are
                                    // collected fixed-hop path vars, create an intermediate
                                    // BindPath for the fixed prefix first. The VLP will then
                                    // extend this existing path.
                                    if is_vlp
                                        && let Some(pv) = path_variable.as_ref()
                                        && !path_node_vars.is_empty()
                                    {
                                        plan = LogicalPlan::BindPath {
                                            input: Box::new(plan),
                                            node_variables: std::mem::take(&mut path_node_vars),
                                            edge_variables: std::mem::take(&mut path_edge_vars),
                                            path_variable: pv.clone(),
                                        };
                                        if !is_var_in_scope(vars_in_scope, pv) {
                                            add_var_to_scope(
                                                vars_in_scope,
                                                pv,
                                                VariableType::Path,
                                            )?;
                                        }
                                    }

                                    // Plan the traverse from the current source node
                                    let (new_plan, target_var, effective_target) = self
                                        .plan_traverse_with_source(
                                            plan,
                                            vars_in_scope,
                                            TraverseParams {
                                                rel: r,
                                                target_node: n_target,
                                                optional,
                                                path_variable: traverse_path_var,
                                                optional_pattern_vars: optional_pattern_vars
                                                    .clone(),
                                            },
                                            &current_source_var,
                                            vars_before_pattern,
                                            &path_bound_edge_vars,
                                        )?;
                                    plan = new_plan;

                                    // Track edge/target node for BindPath
                                    if path_variable.is_some() && !is_vlp {
                                        // Use the edge variable if given, otherwise use
                                        // the internal tracking column pattern.
                                        // Use effective_target (which may be __rebound_x
                                        // for bound-target traversals) to match the actual
                                        // column name produced by GraphTraverseExec.
                                        if let Some(ev) = &r.variable {
                                            path_edge_vars.push(ev.clone());
                                        } else {
                                            path_edge_vars
                                                .push(format!("__eid_to_{}", effective_target));
                                        }
                                        path_node_vars.push(target_var.clone());
                                    }

                                    current_source_var = target_var;
                                    last_outer_node_var = Some(current_source_var.clone());
                                    had_traverses = true;
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
                    // Validate: odd number of elements (node-rel-node[-rel-node]*)
                    if pattern.elements.len() < 3 || pattern.elements.len() % 2 == 0 {
                        return Err(anyhow!(
                            "Quantified pattern must have node-relationship-node structure (odd number >= 3 elements)"
                        ));
                    }

                    let source_node = match &pattern.elements[0] {
                        PatternElement::Node(n) => n,
                        _ => return Err(anyhow!("Quantified pattern must start with a node")),
                    };

                    // Extract all relationship-node pairs (QPP steps)
                    let mut qpp_rels: Vec<(&RelationshipPattern, &NodePattern)> = Vec::new();
                    for pair_idx in (1..pattern.elements.len()).step_by(2) {
                        let rel = match &pattern.elements[pair_idx] {
                            PatternElement::Relationship(r) => r,
                            _ => {
                                return Err(anyhow!(
                                    "Quantified pattern element at position {} must be a relationship",
                                    pair_idx
                                ));
                            }
                        };
                        let node = match &pattern.elements[pair_idx + 1] {
                            PatternElement::Node(n) => n,
                            _ => {
                                return Err(anyhow!(
                                    "Quantified pattern element at position {} must be a node",
                                    pair_idx + 1
                                ));
                            }
                        };
                        // Reject nested quantifiers
                        if rel.range.is_some() {
                            return Err(anyhow!(
                                "Nested quantifiers not supported: ((a)-[:REL*n]->(b)){{m}}"
                            ));
                        }
                        qpp_rels.push((rel, node));
                    }

                    // Check if there's an outer target node after the Parenthesized element.
                    // In syntax like `(a)((x)-[:LINK]->(y)){2,4}(b)`, the `(b)` is the outer
                    // target that should receive the traversal result.
                    let inner_target_node = qpp_rels.last().unwrap().1;
                    let outer_target_node = if i + 1 < elements.len() {
                        match &elements[i + 1] {
                            PatternElement::Node(n) => Some(n),
                            _ => None,
                        }
                    } else {
                        None
                    };
                    // Use the outer target for variable binding and filters; inner target
                    // labels are used for state constraints within the NFA.
                    let target_node = outer_target_node.unwrap_or(inner_target_node);

                    // For simple 3-element single-hop QPP without intermediate label constraints,
                    // fall back to existing VLP behavior (copy range to relationship).
                    let use_simple_vlp = qpp_rels.len() == 1
                        && inner_target_node
                            .labels
                            .first()
                            .and_then(|l| self.schema.get_label_case_insensitive(l))
                            .is_none();

                    // Plan source node.
                    // In `(a)((x)-[:R]->(y)){n}(b)`, the QPP source is the preceding
                    // outer node `a`, NOT the inner `x`. If there's a preceding outer
                    // node variable, use it; otherwise fall back to the inner source.
                    let source_variable = if let Some(ref outer_src) = last_outer_node_var {
                        // The preceding outer node is already bound and in scope
                        // Apply any property filters from the inner source node
                        if let Some(prop_filter) =
                            self.properties_to_expr(outer_src, &source_node.properties)
                        {
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate: prop_filter,
                                optional_variables: std::collections::HashSet::new(),
                            };
                        }
                        outer_src.clone()
                    } else {
                        let sv = source_node
                            .variable
                            .clone()
                            .filter(|v| !v.is_empty())
                            .unwrap_or_else(|| self.next_anon_var());

                        if is_var_in_scope(vars_in_scope, &sv) {
                            // Source is already bound, apply property filter if needed
                            if let Some(prop_filter) =
                                self.properties_to_expr(&sv, &source_node.properties)
                            {
                                plan = LogicalPlan::Filter {
                                    input: Box::new(plan),
                                    predicate: prop_filter,
                                    optional_variables: std::collections::HashSet::new(),
                                };
                            }
                        } else {
                            // Source is unbound, scan it
                            plan = self.plan_unbound_node(source_node, &sv, plan, optional)?;
                            add_var_to_scope(vars_in_scope, &sv, VariableType::Node)?;
                        }
                        sv
                    };

                    if use_simple_vlp {
                        // Simple single-hop QPP: apply range to relationship and use VLP path
                        let mut relationship = qpp_rels[0].0.clone();
                        relationship.range = range.clone();

                        let (new_plan, _target_var, _effective_target) = self
                            .plan_traverse_with_source(
                                plan,
                                vars_in_scope,
                                TraverseParams {
                                    rel: &relationship,
                                    target_node,
                                    optional,
                                    path_variable: path_variable.clone(),
                                    optional_pattern_vars: optional_pattern_vars.clone(),
                                },
                                &source_variable,
                                vars_before_pattern,
                                &path_bound_edge_vars,
                            )?;
                        plan = new_plan;
                    } else {
                        // Multi-hop QPP: build QppStepInfo list and create Traverse with qpp_steps
                        let mut qpp_step_infos = Vec::new();
                        let mut all_edge_type_ids = Vec::new();

                        for (rel, node) in &qpp_rels {
                            let mut step_edge_type_ids = Vec::new();
                            if rel.types.is_empty() {
                                step_edge_type_ids = self.schema.all_edge_type_ids();
                            } else {
                                for type_name in &rel.types {
                                    if let Some(edge_meta) = self.schema.edge_types.get(type_name) {
                                        step_edge_type_ids.push(edge_meta.id);
                                    }
                                }
                            }
                            all_edge_type_ids.extend_from_slice(&step_edge_type_ids);

                            let target_label = node.labels.first().and_then(|l| {
                                self.schema.get_label_case_insensitive(l).map(|_| l.clone())
                            });

                            qpp_step_infos.push(QppStepInfo {
                                edge_type_ids: step_edge_type_ids,
                                direction: rel.direction.clone(),
                                target_label,
                            });
                        }

                        // Deduplicate edge type IDs for adjacency warming
                        all_edge_type_ids.sort_unstable();
                        all_edge_type_ids.dedup();

                        // Compute iteration bounds from range
                        let hops_per_iter = qpp_step_infos.len();
                        const QPP_DEFAULT_MAX_HOPS: usize = 100;
                        let (min_iter, max_iter) = if let Some(range) = range {
                            let min = range.min.unwrap_or(1) as usize;
                            let max = range
                                .max
                                .map(|m| m as usize)
                                .unwrap_or(QPP_DEFAULT_MAX_HOPS / hops_per_iter);
                            (min, max)
                        } else {
                            (1, 1)
                        };
                        let min_hops = min_iter * hops_per_iter;
                        let max_hops = max_iter * hops_per_iter;

                        // Target variable from the last node in the QPP sub-pattern
                        let target_variable = target_node
                            .variable
                            .clone()
                            .filter(|v| !v.is_empty())
                            .unwrap_or_else(|| self.next_anon_var());

                        let target_is_bound = is_var_in_scope(vars_in_scope, &target_variable);

                        // Determine target label for the final node
                        let target_label_meta = target_node
                            .labels
                            .first()
                            .and_then(|l| self.schema.get_label_case_insensitive(l));

                        // Collect scope match variables
                        let mut scope_match_variables: std::collections::HashSet<String> =
                            vars_in_scope[vars_before_pattern..]
                                .iter()
                                .map(|v| v.name.clone())
                                .collect();
                        scope_match_variables.insert(target_variable.clone());

                        // Handle bound target: use rebound variable for traverse
                        let rebound_target_var = if target_is_bound {
                            Some(target_variable.clone())
                        } else {
                            None
                        };
                        let effective_target_var = if let Some(ref bv) = rebound_target_var {
                            format!("__rebound_{}", bv)
                        } else {
                            target_variable.clone()
                        };

                        plan = LogicalPlan::Traverse {
                            input: Box::new(plan),
                            edge_type_ids: all_edge_type_ids,
                            direction: qpp_rels[0].0.direction.clone(),
                            source_variable: source_variable.to_string(),
                            target_variable: effective_target_var.clone(),
                            target_label_id: target_label_meta.map(|m| m.id).unwrap_or(0),
                            step_variable: None, // QPP doesn't expose intermediate edges
                            min_hops,
                            max_hops,
                            optional,
                            target_filter: self.node_filter_expr(
                                &target_variable,
                                &target_node.labels,
                                &target_node.properties,
                            ),
                            path_variable: path_variable.clone(),
                            edge_properties: std::collections::HashSet::new(),
                            is_variable_length: true,
                            optional_pattern_vars: optional_pattern_vars.clone(),
                            scope_match_variables,
                            edge_filter_expr: None,
                            path_mode: crate::query::df_graph::nfa::PathMode::Trail,
                            qpp_steps: Some(qpp_step_infos),
                        };

                        // Handle bound target: filter rebound results against original variable
                        if let Some(ref btv) = rebound_target_var {
                            // Filter: __rebound_x._vid = x._vid
                            let filter_pred = Expr::BinaryOp {
                                left: Box::new(Expr::Property(
                                    Box::new(Expr::Variable(effective_target_var.clone())),
                                    "_vid".to_string(),
                                )),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Property(
                                    Box::new(Expr::Variable(btv.clone())),
                                    "_vid".to_string(),
                                )),
                            };
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate: filter_pred,
                                optional_variables: if optional {
                                    optional_pattern_vars.clone()
                                } else {
                                    std::collections::HashSet::new()
                                },
                            };
                        }

                        // Add target variable to scope
                        if !target_is_bound {
                            add_var_to_scope(vars_in_scope, &target_variable, VariableType::Node)?;
                        }

                        // Add path variable to scope
                        if let Some(ref pv) = path_variable
                            && !pv.is_empty()
                            && !is_var_in_scope(vars_in_scope, pv)
                        {
                            add_var_to_scope(vars_in_scope, pv, VariableType::Path)?;
                        }
                    }
                    had_traverses = true;

                    // Skip the outer target node if we consumed it
                    if outer_target_node.is_some() {
                        i += 2; // skip both Parenthesized and the following Node
                    } else {
                        i += 1;
                    }
                }
            }
        }

        // If this is a single-node pattern with a path variable, bind the zero-length path
        // E.g., `p = (a)` should create a Path with one node and zero edges
        if let Some(ref path_var) = path_variable
            && !path_var.is_empty()
            && !had_traverses
            && let Some(node_var) = single_node_variable
        {
            plan = LogicalPlan::BindZeroLengthPath {
                input: Box::new(plan),
                node_variable: node_var,
                path_variable: path_var.clone(),
            };
            add_var_to_scope(vars_in_scope, path_var, VariableType::Path)?;
        }

        // Bind fixed-length path from collected node/edge variables
        if let Some(ref path_var) = path_variable
            && !path_var.is_empty()
            && had_traverses
            && !path_node_vars.is_empty()
            && !is_var_in_scope(vars_in_scope, path_var)
        {
            plan = LogicalPlan::BindPath {
                input: Box::new(plan),
                node_variables: path_node_vars,
                edge_variables: path_edge_vars,
                path_variable: path_var.clone(),
            };
            add_var_to_scope(vars_in_scope, path_var, VariableType::Path)?;
        }

        Ok(plan)
    }

    /// Plan a traverse with an explicit source variable name.
    ///
    /// Returns `(plan, target_variable, effective_target_variable)` where:
    /// - `target_variable` is the semantic variable name for downstream scope
    /// - `effective_target_variable` is the actual column-name prefix used by
    ///   the traverse (may be `__rebound_x` for bound-target patterns)
    fn plan_traverse_with_source(
        &self,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<VariableInfo>,
        params: TraverseParams<'_>,
        source_variable: &str,
        vars_before_pattern: usize,
        path_bound_edge_vars: &std::collections::HashSet<String>,
    ) -> Result<(LogicalPlan, String, String)> {
        // Check for parameter used as relationship predicate
        if let Some(Expr::Parameter(_)) = &params.rel.properties {
            return Err(anyhow!(
                "SyntaxError: InvalidParameterUse - Parameters cannot be used as relationship predicates"
            ));
        }

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

        // Deduplicate edge type IDs and unknown types ([:T|:T] → [:T])
        edge_type_ids.sort_unstable();
        edge_type_ids.dedup();
        unknown_types.sort_unstable();
        unknown_types.dedup();

        let mut target_variable = params.target_node.variable.clone().unwrap_or_default();
        if target_variable.is_empty() {
            target_variable = self.next_anon_var();
        }
        let target_is_bound =
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

        // Check for VariableTypeConflict/RelationshipUniquenessViolation
        // e.g., (r)-[r]-() or r = ()-[]-(), ()-[r]-()
        // Also: (a)-[r]->()-[r]->(a) where r is reused as relationship in same pattern
        // BUT: MATCH (a)-[r]->() WITH r MATCH ()-[r]->() is ALLOWED (r is bound from previous clause)
        let mut bound_edge_var: Option<String> = None;
        let mut bound_edge_list_var: Option<String> = None;
        if let Some(rel_var) = &params.rel.variable
            && !rel_var.is_empty()
            && let Some(info) = find_var_in_scope(vars_in_scope, rel_var)
        {
            let is_from_previous_clause = vars_in_scope[..vars_before_pattern]
                .iter()
                .any(|v| v.name == *rel_var);

            if info.var_type == VariableType::Edge {
                // Check if this edge variable comes from a previous clause (before this MATCH)
                if is_from_previous_clause {
                    // Edge variable bound from previous clause - this is allowed
                    // We'll filter the traversal to match this specific edge
                    bound_edge_var = Some(rel_var.clone());
                } else {
                    // Same relationship variable used twice in the same MATCH clause
                    return Err(anyhow!(
                        "SyntaxError: RelationshipUniquenessViolation - Relationship variable '{}' is already used in this pattern",
                        rel_var
                    ));
                }
            } else if params.rel.range.is_some()
                && is_from_previous_clause
                && matches!(
                    info.var_type,
                    VariableType::Scalar | VariableType::ScalarLiteral
                )
            {
                // Allow VLP rebound against a previously bound relationship list
                // (e.g. WITH [r1, r2] AS rs ... MATCH ()-[rs*]->()).
                bound_edge_list_var = Some(rel_var.clone());
            } else if !info.var_type.is_compatible_with(VariableType::Edge) {
                return Err(anyhow!(
                    "SyntaxError: VariableTypeConflict - Variable '{}' already defined as {:?}, cannot use as relationship",
                    rel_var,
                    info.var_type
                ));
            }
        }

        // Check for VariableTypeConflict: target node variable already bound as non-Node
        // e.g., ()-[r]-()-[]-(r) where r was added as Edge, now used as target node
        if target_is_bound
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
        // Also supports OR relationship types like MATCH (a)-[:KNOWS|HATES]->(b)
        if !unknown_types.is_empty() && edge_type_ids.is_empty() {
            // All types are unknown - use schemaless traversal

            let is_variable_length = params.rel.range.is_some();

            const DEFAULT_MAX_HOPS: usize = 100;
            let (min_hops, max_hops) = if let Some(range) = &params.rel.range {
                let min = range.min.unwrap_or(1) as usize;
                let max = range.max.map(|m| m as usize).unwrap_or(DEFAULT_MAX_HOPS);
                (min, max)
            } else {
                (1, 1)
            };

            // For both single-hop and variable-length paths:
            // - step_var is the relationship variable (r in `()-[r]->()` or `()-[r*]->()`)
            //   Single-hop: step_var holds a single edge object
            //   VLP: step_var holds a list of edge objects
            // - path_var is the named path variable (p in `p = (a)-[r*]->(b)`)
            let step_var = params.rel.variable.clone();
            let path_var = params.path_variable.clone();

            // Compute scope_match_variables for relationship uniqueness scoping.
            let mut scope_match_variables: std::collections::HashSet<String> = vars_in_scope
                [vars_before_pattern..]
                .iter()
                .map(|v| v.name.clone())
                .collect();
            if let Some(ref sv) = step_var {
                // Only add the step variable to scope if it's NOT rebound from a previous clause.
                // Rebound edges (bound_edge_var is set) should not participate in uniqueness
                // filtering because the second MATCH intentionally reuses the same edge.
                if bound_edge_var.is_none() {
                    scope_match_variables.insert(sv.clone());
                }
            }
            scope_match_variables.insert(target_variable.clone());
            // Include bound edge variables from this path for cross-segment Trail mode
            // enforcement. This ensures VLP segments like [*0..1] don't traverse through
            // edges already claimed by a bound relationship [r] in the same path.
            // Exclude the CURRENT segment's bound edge: the schemaless path doesn't use
            // __rebound_ renaming, so the BFS must be free to match the bound edge itself.
            scope_match_variables.extend(
                path_bound_edge_vars
                    .iter()
                    .filter(|v| bound_edge_var.as_ref() != Some(*v))
                    .cloned(),
            );

            let mut plan = LogicalPlan::TraverseMainByType {
                type_names: unknown_types,
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
                is_variable_length,
                optional_pattern_vars: params.optional_pattern_vars.clone(),
                scope_match_variables,
                edge_filter_expr: if is_variable_length {
                    let filter_var = step_var
                        .clone()
                        .unwrap_or_else(|| "__anon_edge".to_string());
                    self.properties_to_expr(&filter_var, &params.rel.properties)
                } else {
                    None
                },
                path_mode: crate::query::df_graph::nfa::PathMode::Trail,
            };

            // Only apply bound target filter for Imported variables (from outer scope/subquery).
            // For regular cycle patterns like (a)-[:T]->(b)-[:T]->(a), the bound check
            // uses Parameter which requires the value to be in params (subquery context).
            if target_is_bound
                && let Some(info) = find_var_in_scope(vars_in_scope, &target_variable)
                && info.var_type == VariableType::Imported
            {
                plan = Self::wrap_with_bound_target_filter(plan, &target_variable);
            }

            // Apply relationship property predicates for fixed-length schemaless
            // traversals (e.g., [r:KNOWS {name: 'monkey'}]).
            // For VLP, predicates are stored inline in edge_filter_expr (above).
            // For fixed-length, wrap as a Filter node for post-traverse evaluation.
            if !is_variable_length
                && let Some(edge_var_name) = step_var.as_ref()
                && let Some(edge_prop_filter) =
                    self.properties_to_expr(edge_var_name, &params.rel.properties)
            {
                let filter_optional_vars = if params.optional {
                    params.optional_pattern_vars.clone()
                } else {
                    std::collections::HashSet::new()
                };
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: edge_prop_filter,
                    optional_variables: filter_optional_vars,
                };
            }

            // Add the bound variables to scope
            if let Some(sv) = &step_var {
                add_var_to_scope(vars_in_scope, sv, VariableType::Edge)?;
                if is_variable_length
                    && let Some(info) = vars_in_scope.iter_mut().find(|v| v.name == *sv)
                {
                    info.is_vlp = true;
                }
            }
            if let Some(pv) = &path_var
                && !is_var_in_scope(vars_in_scope, pv)
            {
                add_var_to_scope(vars_in_scope, pv, VariableType::Path)?;
            }
            if !is_var_in_scope(vars_in_scope, &target_variable) {
                add_var_to_scope(vars_in_scope, &target_variable, VariableType::Node)?;
            }

            return Ok((plan, target_variable.clone(), target_variable));
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
        } else if !target_is_bound {
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

        // Check if this is a variable-length pattern (has range specifier like *1..3)
        let is_variable_length = params.rel.range.is_some();

        // For VLP patterns, default min to 1 and max to a reasonable limit.
        // For single-hop patterns (no range), both are 1.
        const DEFAULT_MAX_HOPS: usize = 100;
        let (min_hops, max_hops) = if let Some(range) = &params.rel.range {
            let min = range.min.unwrap_or(1) as usize;
            let max = range.max.map(|m| m as usize).unwrap_or(DEFAULT_MAX_HOPS);
            (min, max)
        } else {
            (1, 1)
        };

        // step_var is the relationship variable (r in `()-[r]->()` or `()-[r*]->()`)
        //   Single-hop: step_var holds a single edge object
        //   VLP: step_var holds a list of edge objects
        // path_var is the named path variable (p in `p = (a)-[r*]->(b)`)
        let step_var = params.rel.variable.clone();
        let path_var = params.path_variable.clone();

        // If we have a bound edge variable from a previous clause, use a temp variable
        // for the Traverse step, then filter to match the bound edge
        let rebound_var = bound_edge_var
            .as_ref()
            .or(bound_edge_list_var.as_ref())
            .cloned();
        let effective_step_var = if let Some(ref bv) = rebound_var {
            Some(format!("__rebound_{}", bv))
        } else {
            step_var.clone()
        };

        // If we have a bound target variable from a previous clause (e.g. WITH),
        // use a temp variable for the Traverse step, then filter to match the bound
        // target — mirroring the bound edge pattern above.
        let rebound_target_var = if target_is_bound && !target_variable.is_empty() {
            let is_imported = find_var_in_scope(vars_in_scope, &target_variable)
                .map(|info| info.var_type == VariableType::Imported)
                .unwrap_or(false);
            if !is_imported {
                Some(target_variable.clone())
            } else {
                None
            }
        } else {
            None
        };

        let effective_target_var = if let Some(ref bv) = rebound_target_var {
            format!("__rebound_{}", bv)
        } else {
            target_variable.clone()
        };

        // Collect all variables (node + edge) from the current MATCH clause scope
        // for relationship uniqueness scoping. Edge ID columns (both named `r._eid`
        // and anonymous `__eid_to_target`) are only included in uniqueness filtering
        // if their associated variable is in this set. This prevents relationship
        // uniqueness from being enforced across disconnected MATCH clauses.
        let mut scope_match_variables: std::collections::HashSet<String> = vars_in_scope
            [vars_before_pattern..]
            .iter()
            .map(|v| v.name.clone())
            .collect();
        // Include the current traverse's edge variable (not yet added to vars_in_scope)
        if let Some(ref sv) = effective_step_var {
            scope_match_variables.insert(sv.clone());
        }
        // Include the target variable (not yet added to vars_in_scope)
        scope_match_variables.insert(effective_target_var.clone());
        // Include bound edge variables from this path for cross-segment Trail mode
        // enforcement (same as the schemaless path above).
        scope_match_variables.extend(path_bound_edge_vars.iter().cloned());

        let mut plan = LogicalPlan::Traverse {
            input: Box::new(plan),
            edge_type_ids,
            direction: params.rel.direction.clone(),
            source_variable: source_variable.to_string(),
            target_variable: effective_target_var.clone(),
            target_label_id: target_label_meta.map(|m| m.id).unwrap_or(0),
            step_variable: effective_step_var.clone(),
            min_hops,
            max_hops,
            optional: params.optional,
            target_filter: self.node_filter_expr(
                &target_variable,
                &params.target_node.labels,
                &params.target_node.properties,
            ),
            path_variable: path_var.clone(),
            edge_properties: std::collections::HashSet::new(),
            is_variable_length,
            optional_pattern_vars: params.optional_pattern_vars.clone(),
            scope_match_variables,
            edge_filter_expr: if is_variable_length {
                // Use the step variable name, or a fallback for anonymous edges.
                // The variable name is used by properties_to_expr to build
                // `var.prop = value` expressions. For BFS property checking,
                // only the property name and value matter (the variable name
                // is stripped during extraction).
                let filter_var = effective_step_var
                    .clone()
                    .unwrap_or_else(|| "__anon_edge".to_string());
                self.properties_to_expr(&filter_var, &params.rel.properties)
            } else {
                None
            },
            path_mode: crate::query::df_graph::nfa::PathMode::Trail,
            qpp_steps: None,
        };

        // Pre-compute optional variables set for filter nodes in this traverse.
        // Used by relationship property filters and bound-edge filters below.
        let filter_optional_vars = if params.optional {
            params.optional_pattern_vars.clone()
        } else {
            std::collections::HashSet::new()
        };

        // Apply relationship property predicates (e.g. [r {k: v}]).
        // For VLP, predicates are stored inline in edge_filter_expr (above).
        // For fixed-length, wrap as a Filter node for post-traverse evaluation.
        if !is_variable_length
            && let Some(edge_var_name) = effective_step_var.as_ref()
            && let Some(edge_prop_filter) =
                self.properties_to_expr(edge_var_name, &params.rel.properties)
        {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: edge_prop_filter,
                optional_variables: filter_optional_vars.clone(),
            };
        }

        // Only apply bound target filter for Imported variables (from outer scope/subquery).
        // For regular cycle patterns like (a)-[:T]->(b)-[:T]->(a), the bound check
        // uses Parameter which requires the value to be in params (subquery context).
        if target_is_bound
            && let Some(info) = find_var_in_scope(vars_in_scope, &target_variable)
            && info.var_type == VariableType::Imported
        {
            plan = Self::wrap_with_bound_target_filter(plan, &target_variable);
        }

        // If we have a bound edge variable, add a filter to match it
        if let Some(ref bv) = bound_edge_var {
            let temp_var = format!("__rebound_{}", bv);
            let bound_check = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable(temp_var)),
                    "_eid".to_string(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Property(
                    Box::new(Expr::Variable(bv.clone())),
                    "_eid".to_string(),
                )),
            };
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: bound_check,
                optional_variables: filter_optional_vars.clone(),
            };
        }

        // If we have a bound relationship list variable for a VLP pattern,
        // add a filter to match the traversed relationship list exactly.
        if let Some(ref bv) = bound_edge_list_var {
            let temp_var = format!("__rebound_{}", bv);
            let temp_eids = Expr::ListComprehension {
                variable: "__rebound_edge".to_string(),
                list: Box::new(Expr::Variable(temp_var)),
                where_clause: None,
                map_expr: Box::new(Expr::FunctionCall {
                    name: "toInteger".to_string(),
                    args: vec![Expr::Property(
                        Box::new(Expr::Variable("__rebound_edge".to_string())),
                        "_eid".to_string(),
                    )],
                    distinct: false,
                    window_spec: None,
                }),
            };
            let bound_eids = Expr::ListComprehension {
                variable: "__bound_edge".to_string(),
                list: Box::new(Expr::Variable(bv.clone())),
                where_clause: None,
                map_expr: Box::new(Expr::FunctionCall {
                    name: "toInteger".to_string(),
                    args: vec![Expr::Property(
                        Box::new(Expr::Variable("__bound_edge".to_string())),
                        "_eid".to_string(),
                    )],
                    distinct: false,
                    window_spec: None,
                }),
            };
            let bound_list_check = Expr::BinaryOp {
                left: Box::new(temp_eids),
                op: BinaryOp::Eq,
                right: Box::new(bound_eids),
            };
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: bound_list_check,
                optional_variables: filter_optional_vars.clone(),
            };
        }

        // If we have a bound target variable (non-imported), add a filter to constrain
        // the traversal output to match the previously bound target node.
        if let Some(ref bv) = rebound_target_var {
            let temp_var = format!("__rebound_{}", bv);
            let bound_check = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable(temp_var.clone())),
                    "_vid".to_string(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Property(
                    Box::new(Expr::Variable(bv.clone())),
                    "_vid".to_string(),
                )),
            };
            // For OPTIONAL MATCH, include the rebound variable in optional_variables
            // so that OptionalFilterExec excludes it from the grouping key and
            // properly nullifies it in recovery rows when all matches are filtered out.
            // Without this, each traverse result creates its own group (keyed by
            // __rebound_c._vid), and null-row recovery emits a spurious null row
            // for every non-matching target instead of one per source group.
            let mut rebound_filter_vars = filter_optional_vars;
            if params.optional {
                rebound_filter_vars.insert(temp_var);
            }
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: bound_check,
                optional_variables: rebound_filter_vars,
            };
        }

        // Add the bound variables to scope
        // Skip adding the edge variable if it's already bound from a previous clause
        if let Some(sv) = &step_var
            && bound_edge_var.is_none()
            && bound_edge_list_var.is_none()
        {
            add_var_to_scope(vars_in_scope, sv, VariableType::Edge)?;
            if is_variable_length
                && let Some(info) = vars_in_scope.iter_mut().find(|v| v.name == *sv)
            {
                info.is_vlp = true;
            }
        }
        if let Some(pv) = &path_var
            && !is_var_in_scope(vars_in_scope, pv)
        {
            add_var_to_scope(vars_in_scope, pv, VariableType::Path)?;
        }
        if !is_var_in_scope(vars_in_scope, &target_variable) {
            add_var_to_scope(vars_in_scope, &target_variable, VariableType::Node)?;
        }

        Ok((plan, target_variable, effective_target_var))
    }

    /// Combine a new scan plan with an existing plan.
    ///
    /// If the existing plan is `Empty`, returns the new plan directly.
    /// Otherwise, wraps them in a `CrossJoin`.
    fn join_with_plan(existing: LogicalPlan, new: LogicalPlan) -> LogicalPlan {
        if matches!(existing, LogicalPlan::Empty) {
            new
        } else {
            LogicalPlan::CrossJoin {
                left: Box::new(existing),
                right: Box::new(new),
            }
        }
    }

    /// Split node map predicates into scan-pushable and residual filters.
    ///
    /// A predicate is scan-pushable when its value expression references only
    /// the node variable itself (or no variables). Predicates referencing other
    /// in-scope variables (correlated predicates) are returned as residual so
    /// they can be applied after joining with the existing plan.
    fn split_node_property_filters_for_scan(
        &self,
        variable: &str,
        properties: &Option<Expr>,
    ) -> (Option<Expr>, Option<Expr>) {
        let entries = match properties {
            Some(Expr::Map(entries)) => entries,
            _ => return (None, None),
        };

        if entries.is_empty() {
            return (None, None);
        }

        let mut pushdown_entries = Vec::new();
        let mut residual_entries = Vec::new();

        for (prop, val_expr) in entries {
            let vars = collect_expr_variables(val_expr);
            if vars.iter().all(|v| v == variable) {
                pushdown_entries.push((prop.clone(), val_expr.clone()));
            } else {
                residual_entries.push((prop.clone(), val_expr.clone()));
            }
        }

        let pushdown_map = if pushdown_entries.is_empty() {
            None
        } else {
            Some(Expr::Map(pushdown_entries))
        };
        let residual_map = if residual_entries.is_empty() {
            None
        } else {
            Some(Expr::Map(residual_entries))
        };

        (
            self.properties_to_expr(variable, &pushdown_map),
            self.properties_to_expr(variable, &residual_map),
        )
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
            Some(Expr::Parameter(_)) => {
                return Err(anyhow!(
                    "SyntaxError: InvalidParameterUse - Parameters cannot be used as node predicates"
                ));
            }
            Some(_) => return Err(anyhow!("Node properties must be a Map")),
            None => &[],
        };

        let has_existing_scope = !matches!(plan, LogicalPlan::Empty);

        let apply_residual_filter = |input: LogicalPlan, residual: Option<Expr>| -> LogicalPlan {
            if let Some(predicate) = residual {
                LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                    optional_variables: std::collections::HashSet::new(),
                }
            } else {
                input
            }
        };

        let (node_scan_filter, node_residual_filter) = if has_existing_scope {
            self.split_node_property_filters_for_scan(variable, &node.properties)
        } else {
            (self.properties_to_expr(variable, &node.properties), None)
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

                let (prop_filter, residual_filter) = if has_existing_scope {
                    self.split_node_property_filters_for_scan(variable, &remaining_expr)
                } else {
                    (self.properties_to_expr(variable, &remaining_expr), None)
                };

                let ext_id_lookup = LogicalPlan::ExtIdLookup {
                    variable: variable.to_string(),
                    ext_id,
                    filter: prop_filter,
                    optional,
                };

                let joined = Self::join_with_plan(plan, ext_id_lookup);
                return Ok(apply_residual_filter(joined, residual_filter));
            }

            // No ext_id: create ScanAll for unlabeled node pattern
            let scan_all = LogicalPlan::ScanAll {
                variable: variable.to_string(),
                filter: node_scan_filter,
                optional,
            };

            let joined = Self::join_with_plan(plan, scan_all);
            return Ok(apply_residual_filter(joined, node_residual_filter));
        }

        // Use first label for label_id (primary label for dataset selection)
        let label_name = &node.labels[0];

        // Check if label exists in schema
        if let Some(label_meta) = self.schema.get_label_case_insensitive(label_name) {
            // Known label: use standard Scan
            let scan = LogicalPlan::Scan {
                label_id: label_meta.id,
                labels: node.labels.clone(),
                variable: variable.to_string(),
                filter: node_scan_filter,
                optional,
            };

            let joined = Self::join_with_plan(plan, scan);
            Ok(apply_residual_filter(joined, node_residual_filter))
        } else {
            // Unknown label: use ScanMainByLabels for schemaless support
            let scan_main = LogicalPlan::ScanMainByLabels {
                labels: node.labels.clone(),
                variable: variable.to_string(),
                filter: node_scan_filter,
                optional,
            };

            let joined = Self::join_with_plan(plan, scan_main);
            Ok(apply_residual_filter(joined, node_residual_filter))
        }
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

        // Check that WHERE predicate isn't a bare node/edge/path variable
        if let Expr::Variable(var_name) = predicate
            && let Some(info) = find_var_in_scope(vars_in_scope, var_name)
            && matches!(
                info.var_type,
                VariableType::Node | VariableType::Edge | VariableType::Path
            )
        {
            return Err(anyhow!(
                "SyntaxError: InvalidArgumentType - Type mismatch: expected Boolean but was {:?}",
                info.var_type
            ));
        }

        let mut plan = plan;

        // Transform VALID_AT macro to function call
        let transformed_predicate = Self::transform_valid_at_to_function(predicate.clone());

        let mut current_predicate =
            self.rewrite_predicates_using_indexes(&transformed_predicate, &plan, vars_in_scope)?;

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
                is_variable_length,
                optional_pattern_vars,
                scope_match_variables,
                edge_filter_expr,
                path_mode,
                qpp_steps,
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
                        is_variable_length,
                        optional_pattern_vars,
                        scope_match_variables,
                        edge_filter_expr,
                        path_mode,
                        qpp_steps,
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
                        is_variable_length,
                        optional_pattern_vars,
                        scope_match_variables,
                        edge_filter_expr,
                        path_mode,
                        qpp_steps,
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
        let mut compound_agg_exprs: Vec<Expr> = Vec::new();
        let mut has_agg = false;
        let mut projections = Vec::new();
        let mut new_vars: Vec<VariableInfo> = Vec::new();
        let mut projected_aggregate_reprs: HashSet<String> = HashSet::new();
        let mut projected_simple_reprs: HashSet<String> = HashSet::new();
        let mut projected_aliases: HashSet<String> = HashSet::new();
        let mut has_unaliased_non_variable_expr = false;

        for item in &with_clause.items {
            match item {
                ReturnItem::All => {
                    // WITH * - add all variables in scope
                    for v in vars_in_scope {
                        projections.push((Expr::Variable(v.name.clone()), Some(v.name.clone())));
                        projected_aliases.insert(v.name.clone());
                        projected_simple_reprs.insert(v.name.clone());
                    }
                    new_vars.extend(vars_in_scope.iter().cloned());
                }
                ReturnItem::Expr { expr, alias, .. } => {
                    if matches!(expr, Expr::Wildcard) {
                        for v in vars_in_scope {
                            projections
                                .push((Expr::Variable(v.name.clone()), Some(v.name.clone())));
                            projected_aliases.insert(v.name.clone());
                            projected_simple_reprs.insert(v.name.clone());
                        }
                        new_vars.extend(vars_in_scope.iter().cloned());
                    } else {
                        // Validate expression variables and syntax
                        validate_expression_variables(expr, vars_in_scope)?;
                        validate_expression(expr, vars_in_scope)?;
                        // Pattern predicates are not allowed in WITH
                        if contains_pattern_predicate(expr) {
                            return Err(anyhow!(
                                "SyntaxError: UnexpectedSyntax - Pattern predicates are not allowed in WITH"
                            ));
                        }

                        projections.push((expr.clone(), alias.clone()));
                        if expr.is_aggregate() && !is_compound_aggregate(expr) {
                            // Bare aggregate — push directly
                            has_agg = true;
                            aggregates.push(expr.clone());
                            projected_aggregate_reprs.insert(expr.to_string_repr());
                        } else if !is_window_function(expr)
                            && (expr.is_aggregate() || contains_aggregate_recursive(expr))
                        {
                            // Compound aggregate or expression containing aggregates
                            has_agg = true;
                            compound_agg_exprs.push(expr.clone());
                            for inner in extract_inner_aggregates(expr) {
                                let repr = inner.to_string_repr();
                                if !projected_aggregate_reprs.contains(&repr) {
                                    aggregates.push(inner);
                                    projected_aggregate_reprs.insert(repr);
                                }
                            }
                        } else if !group_by.contains(expr) {
                            group_by.push(expr.clone());
                            if matches!(expr, Expr::Variable(_) | Expr::Property(_, _)) {
                                projected_simple_reprs.insert(expr.to_string_repr());
                            }
                        }

                        // Preserve non-scalar type information when WITH aliases
                        // entity/path-capable expressions.
                        if let Some(a) = alias {
                            if projected_aliases.contains(a) {
                                return Err(anyhow!(
                                    "SyntaxError: ColumnNameConflict - Duplicate column name '{}' in WITH",
                                    a
                                ));
                            }
                            let inferred = infer_with_output_type(expr, vars_in_scope);
                            new_vars.push(VariableInfo::new(a.clone(), inferred));
                            projected_aliases.insert(a.clone());
                        } else if let Expr::Variable(v) = expr {
                            if projected_aliases.contains(v) {
                                return Err(anyhow!(
                                    "SyntaxError: ColumnNameConflict - Duplicate column name '{}' in WITH",
                                    v
                                ));
                            }
                            // Preserve the original type if the variable is just passed through
                            if let Some(existing) = find_var_in_scope(vars_in_scope, v) {
                                new_vars.push(existing.clone());
                            } else {
                                new_vars.push(VariableInfo::new(v.clone(), VariableType::Scalar));
                            }
                            projected_aliases.insert(v.clone());
                        } else {
                            has_unaliased_non_variable_expr = true;
                        }
                    }
                }
            }
        }

        // Collect extra variables that need to survive the projection stage
        // for later WHERE / ORDER BY evaluation, then strip them afterwards.
        let projected_names: std::collections::HashSet<&str> =
            new_vars.iter().map(|v| v.name.as_str()).collect();
        let mut passthrough_extras: Vec<String> = Vec::new();
        let mut seen_passthrough: HashSet<String> = HashSet::new();

        if let Some(predicate) = &with_clause.where_clause {
            for name in collect_expr_variables(predicate) {
                if !projected_names.contains(name.as_str())
                    && find_var_in_scope(vars_in_scope, &name).is_some()
                    && seen_passthrough.insert(name.clone())
                {
                    passthrough_extras.push(name);
                }
            }
        }

        // Non-aggregating WITH allows ORDER BY to reference incoming variables.
        // Carry those variables through the projection so Sort can resolve them.
        if !has_agg && let Some(order_by) = &with_clause.order_by {
            for item in order_by {
                for name in collect_expr_variables(&item.expr) {
                    if !projected_names.contains(name.as_str())
                        && find_var_in_scope(vars_in_scope, &name).is_some()
                        && seen_passthrough.insert(name.clone())
                    {
                        passthrough_extras.push(name);
                    }
                }
            }
        }

        let needs_cleanup = !passthrough_extras.is_empty();
        for extra in &passthrough_extras {
            projections.push((Expr::Variable(extra.clone()), Some(extra.clone())));
        }

        // Validate compound aggregate expressions: non-aggregate refs must be
        // individually present in the group_by as simple variables or properties.
        if has_agg {
            let group_by_reprs: HashSet<String> =
                group_by.iter().map(|e| e.to_string_repr()).collect();
            for expr in &compound_agg_exprs {
                let mut refs = Vec::new();
                collect_non_aggregate_refs(expr, false, &mut refs);
                for r in &refs {
                    let is_covered = match r {
                        NonAggregateRef::Var(v) => group_by_reprs.contains(v),
                        NonAggregateRef::Property { repr, .. } => group_by_reprs.contains(repr),
                    };
                    if !is_covered {
                        return Err(anyhow!(
                            "SyntaxError: AmbiguousAggregationExpression - Expression mixes aggregation with non-grouped reference"
                        ));
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
                    if expr.is_aggregate() && !is_compound_aggregate(expr) {
                        // Bare aggregate — reference by column name
                        (Expr::Variable(aggregate_column_name(expr)), alias.clone())
                    } else if is_compound_aggregate(expr)
                        || (!expr.is_aggregate() && contains_aggregate_recursive(expr))
                    {
                        // Compound aggregate — replace inner aggregates with
                        // column references, keep outer expression
                        (replace_aggregates_with_columns(expr), alias.clone())
                    } else {
                        (Expr::Variable(expr.to_string_repr()), alias.clone())
                    }
                })
                .collect();
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections: rename_projections,
            };
        } else if !projections.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections: projections.clone(),
            };
        }

        // Apply the WHERE filter (post-projection, with extras still visible).
        if let Some(predicate) = &with_clause.where_clause {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: predicate.clone(),
                optional_variables: std::collections::HashSet::new(),
            };
        }

        // Validate and apply ORDER BY for WITH clause.
        // Keep pre-WITH vars in scope for parser compatibility, then apply
        // stricter checks for aggregate-containing ORDER BY items.
        if let Some(order_by) = &with_clause.order_by {
            // Build a mapping from aliases and projected expression reprs to
            // output columns of the preceding Project/Aggregate pipeline.
            let with_order_aliases: HashMap<String, Expr> = projections
                .iter()
                .flat_map(|(expr, alias)| {
                    let output_col = if let Some(a) = alias {
                        a.clone()
                    } else if expr.is_aggregate() && !is_compound_aggregate(expr) {
                        aggregate_column_name(expr)
                    } else {
                        expr.to_string_repr()
                    };

                    let mut entries = Vec::new();
                    // ORDER BY alias
                    if let Some(a) = alias {
                        entries.push((a.clone(), Expr::Variable(output_col.clone())));
                    }
                    // ORDER BY projected expression (e.g. me.age)
                    entries.push((expr.to_string_repr(), Expr::Variable(output_col)));
                    entries
                })
                .collect();

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
                let has_aggregate_in_item = contains_aggregate_recursive(&item.expr);
                if has_aggregate_in_item && !has_agg {
                    return Err(anyhow!(
                        "SyntaxError: InvalidAggregation - Aggregation functions not allowed in ORDER BY of WITH"
                    ));
                }
                if has_agg && has_aggregate_in_item {
                    validate_with_order_by_aggregate_item(
                        &item.expr,
                        &projected_aggregate_reprs,
                        &projected_simple_reprs,
                        &projected_aliases,
                    )?;
                }
            }
            let rewritten_order_by: Vec<SortItem> = order_by
                .iter()
                .map(|item| {
                    let mut expr =
                        rewrite_order_by_expr_with_aliases(&item.expr, &with_order_aliases);
                    if has_agg {
                        // Rewrite any aggregate calls to the aggregate output
                        // columns produced by Aggregate.
                        expr = replace_aggregates_with_columns(&expr);
                        // Then re-map projected property expressions to aliases
                        // from the WITH projection.
                        expr = rewrite_order_by_expr_with_aliases(&expr, &with_order_aliases);
                    }
                    SortItem {
                        expr,
                        ascending: item.ascending,
                    }
                })
                .collect();
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: rewritten_order_by,
            };
        }

        // Non-variable expressions in WITH must be aliased.
        // This check is intentionally placed after ORDER BY validation so
        // higher-priority semantic errors (e.g., ambiguous aggregation in
        // ORDER BY) can surface first.
        if has_unaliased_non_variable_expr {
            return Err(anyhow!(
                "SyntaxError: NoExpressionAlias - All non-variable expressions in WITH must be aliased"
            ));
        }

        // Validate and apply SKIP/LIMIT for WITH clause
        let skip = with_clause
            .skip
            .as_ref()
            .map(|e| parse_non_negative_integer(e, "SKIP", &self.params))
            .transpose()?
            .flatten();
        let fetch = with_clause
            .limit
            .as_ref()
            .map(|e| parse_non_negative_integer(e, "LIMIT", &self.params))
            .transpose()?
            .flatten();

        if skip.is_some() || fetch.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                skip,
                fetch,
            };
        }

        // Strip passthrough columns that were only needed by WHERE / ORDER BY.
        if needs_cleanup {
            let cleanup_projections: Vec<(Expr, Option<String>)> = new_vars
                .iter()
                .map(|v| (Expr::Variable(v.name.clone()), Some(v.name.clone())))
                .collect();
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections: cleanup_projections,
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
        // Compare the traverse-discovered target's VID against the parameter VID.
        // Left side: column "{var}._vid" from the traverse output.
        // Right side: parameter "{var}._vid" read directly from sub_params.
        // We use Parameter("{var}._vid") rather than Property(Parameter("{var}"), "_vid")
        // because the parameter value is a plain VID integer, not a struct with fields.
        let bound_check = Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable(target_variable.to_string())),
                "_vid".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Parameter(format!("{}._vid", target_variable))),
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
                        variable: scan_var,
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
                is_variable_length,
                optional_pattern_vars,
                scope_match_variables,
                edge_filter_expr,
                path_mode,
                qpp_steps,
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
                is_variable_length,
                optional_pattern_vars,
                scope_match_variables,
                edge_filter_expr,
                path_mode,
                qpp_steps,
            },
            other => other,
        }
    }

    /// Extract predicates that reference only the specified variable
    fn extract_variable_predicates(predicate: &Expr, variable: &str) -> (Vec<Expr>, Option<Expr>) {
        let analyzer = PredicateAnalyzer::new();
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
            Expr::LabelCheck { expr, .. } => Self::collect_expr_variables_impl(expr, vars),
            // Skip Quantifier/Reduce/ListComprehension/PatternComprehension —
            // they introduce local variable bindings not in outer scope.
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
                is_variable_length,
                optional_pattern_vars,
                scope_match_variables,
                edge_filter_expr,
                path_mode,
                qpp_steps,
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
                is_variable_length,
                optional_pattern_vars,
                scope_match_variables,
                edge_filter_expr,
                path_mode,
                qpp_steps,
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
/// This is the single source of truth for aggregate column naming, used by:
/// - Logical planner (to create column references)
/// - Physical planner (to rename DataFusion's auto-generated column names)
/// - Fallback executor (to name result columns)
pub fn aggregate_column_name(expr: &Expr) -> String {
    expr.to_string_repr()
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

    fn parse_embedding_config(emb_val: &Value) -> Result<Option<EmbeddingConfig>> {
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
        LogicalPlan::ScanAll {
            filter: Some(expr), ..
        } => {
            collect_properties_from_expr_into(expr, properties);
        }
        LogicalPlan::ScanAll { filter: None, .. } => {}
        LogicalPlan::ScanMainByLabels {
            filter: Some(expr), ..
        } => {
            collect_properties_from_expr_into(expr, properties);
        }
        LogicalPlan::ScanMainByLabels { filter: None, .. } => {}
        LogicalPlan::TraverseMainByType {
            input,
            target_filter,
            ..
        } => {
            if let Some(expr) = target_filter {
                collect_properties_from_expr_into(expr, properties);
            }
            collect_properties_recursive(input, properties);
        }
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
        LogicalPlan::Create { input, pattern } => {
            // Mark variables referenced in CREATE patterns with "*" so plan_scan
            // adds structural projections (bare entity columns). Without this,
            // execute_create_pattern() can't find bound variables and creates
            // spurious new nodes instead of using existing MATCH'd ones.
            mark_pattern_variables(pattern, properties);
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::CreateBatch { input, patterns } => {
            for pattern in patterns {
                mark_pattern_variables(pattern, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Merge {
            input,
            pattern,
            on_match,
            on_create,
        } => {
            mark_pattern_variables(pattern, properties);
            if let Some(set_clause) = on_match {
                mark_set_item_variables(&set_clause.items, properties);
            }
            if let Some(set_clause) = on_create {
                mark_set_item_variables(&set_clause.items, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Set { input, items } => {
            mark_set_item_variables(items, properties);
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::Remove { input, items } => {
            for item in items {
                match item {
                    RemoveItem::Property(expr) => {
                        // REMOVE n.prop — collect the property and mark the variable
                        // with "*" so full structural projection is applied.
                        collect_properties_from_expr_into(expr, properties);
                        if let Expr::Property(base, _) = expr
                            && let Expr::Variable(var) = base.as_ref()
                        {
                            properties
                                .entry(var.clone())
                                .or_default()
                                .insert("*".to_string());
                        }
                    }
                    RemoveItem::Labels { variable, .. } => {
                        // REMOVE n:Label — mark n with "*"
                        properties
                            .entry(variable.clone())
                            .or_default()
                            .insert("*".to_string());
                    }
                }
            }
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
        LogicalPlan::BindZeroLengthPath { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::BindPath { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::SubqueryCall { input, subquery } => {
            collect_properties_recursive(input, properties);
            collect_properties_recursive(subquery, properties);
        }
        // DDL and other plans don't reference properties
        _ => {}
    }
}

/// Mark target variables from SET items with "*" and collect value expressions.
fn mark_set_item_variables(
    items: &[SetItem],
    properties: &mut HashMap<String, std::collections::HashSet<String>>,
) {
    for item in items {
        match item {
            SetItem::Property { expr, value } => {
                // SET n.prop = val — mark n via the property expr, collect from value.
                // Also mark the variable with "*" for full structural projection so
                // edge identity fields (_src/_dst) are available for write operations.
                collect_properties_from_expr_into(expr, properties);
                collect_properties_from_expr_into(value, properties);
                if let Expr::Property(base, _) = expr
                    && let Expr::Variable(var) = base.as_ref()
                {
                    properties
                        .entry(var.clone())
                        .or_default()
                        .insert("*".to_string());
                }
            }
            SetItem::Labels { variable, .. } => {
                // SET n:Label — need full access to n
                properties
                    .entry(variable.clone())
                    .or_default()
                    .insert("*".to_string());
            }
            SetItem::Variable { variable, value } | SetItem::VariablePlus { variable, value } => {
                // SET n = {props} or SET n += {props}
                properties
                    .entry(variable.clone())
                    .or_default()
                    .insert("*".to_string());
                collect_properties_from_expr_into(value, properties);
            }
        }
    }
}

/// Mark all variables in a CREATE/MERGE pattern with "*" so that plan_scan
/// adds structural projections (bare entity Struct columns) for them.
/// This is needed so that execute_create_pattern() can find bound variables
/// in the row HashMap and reuse existing nodes instead of creating new ones.
fn mark_pattern_variables(
    pattern: &Pattern,
    properties: &mut HashMap<String, std::collections::HashSet<String>>,
) {
    for path in &pattern.paths {
        if let Some(ref v) = path.variable {
            properties
                .entry(v.clone())
                .or_default()
                .insert("*".to_string());
        }
        for element in &path.elements {
            match element {
                PatternElement::Node(n) => {
                    if let Some(ref v) = n.variable {
                        properties
                            .entry(v.clone())
                            .or_default()
                            .insert("*".to_string());
                    }
                    // Also collect properties from inline property expressions
                    if let Some(ref props) = n.properties {
                        collect_properties_from_expr_into(props, properties);
                    }
                }
                PatternElement::Relationship(r) => {
                    if let Some(ref v) = r.variable {
                        properties
                            .entry(v.clone())
                            .or_default()
                            .insert("*".to_string());
                    }
                    if let Some(ref props) = r.properties {
                        collect_properties_from_expr_into(props, properties);
                    }
                }
                PatternElement::Parenthesized { pattern, .. } => {
                    let sub = Pattern {
                        paths: vec![pattern.as_ref().clone()],
                    };
                    mark_pattern_variables(&sub, properties);
                }
            }
        }
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
            } else {
                // Bare variable (e.g., RETURN n) — needs all properties materialized
                properties
                    .entry(name.clone())
                    .or_default()
                    .insert("*".to_string());
            }
        }
        Expr::Property(base, name) => {
            // Extract variable name from the base expression
            if let Expr::Variable(var) = base.as_ref() {
                properties
                    .entry(var.clone())
                    .or_default()
                    .insert(name.clone());
                // Don't recurse into Variable — that would mark it as a bare
                // variable reference (adding "*") when it's just a property base.
            } else {
                // Recurse for complex base expressions (nested property, function call, etc.)
                collect_properties_from_expr_into(base, properties);
            }
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
        Expr::Exists { query, .. } => {
            // Walk into EXISTS body to collect property references for outer-scope variables.
            // This ensures correlated properties (e.g., a.city inside EXISTS where a is outer)
            // are included in the outer scan's property list. Extra properties collected for
            // inner-only variables are harmless — the outer scan ignores unknown variable names.
            collect_properties_from_subquery(query, properties);
        }
        Expr::CountSubquery(query) | Expr::CollectSubquery(query) => {
            collect_properties_from_subquery(query, properties);
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::IsUnique(expr) => {
            collect_properties_from_expr_into(expr, properties);
        }
        Expr::In { expr, list } => {
            collect_properties_from_expr_into(expr, properties);
            collect_properties_from_expr_into(list, properties);
        }
        Expr::ArrayIndex { array, index } => {
            if let Expr::Variable(var) = array.as_ref() {
                if let Expr::Literal(CypherLiteral::String(prop_name)) = index.as_ref() {
                    // Static string key: e['name'] → only need that specific property
                    properties
                        .entry(var.clone())
                        .or_default()
                        .insert(prop_name.clone());
                } else {
                    // Dynamic property access: e[prop] → need all properties
                    properties
                        .entry(var.clone())
                        .or_default()
                        .insert("*".to_string());
                }
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
                match item {
                    uni_cypher::ast::MapProjectionItem::Property(prop) => {
                        if let Expr::Variable(var) = base.as_ref() {
                            properties
                                .entry(var.clone())
                                .or_default()
                                .insert(prop.clone());
                        }
                    }
                    uni_cypher::ast::MapProjectionItem::AllProperties => {
                        if let Expr::Variable(var) = base.as_ref() {
                            properties
                                .entry(var.clone())
                                .or_default()
                                .insert("*".to_string());
                        }
                    }
                    uni_cypher::ast::MapProjectionItem::LiteralEntry(_, expr) => {
                        collect_properties_from_expr_into(expr, properties);
                    }
                    uni_cypher::ast::MapProjectionItem::Variable(_) => {}
                }
            }
        }
        Expr::LabelCheck { expr, .. } => {
            collect_properties_from_expr_into(expr, properties);
        }
        // Parameters reference outer-scope variables (e.g., $p in correlated subqueries).
        // Mark them with "*" so the outer scan produces structural projections that
        // extract_row_params can resolve.
        Expr::Parameter(name) => {
            properties
                .entry(name.clone())
                .or_default()
                .insert("*".to_string());
        }
        // Literals and wildcard don't reference properties
        Expr::Literal(_) | Expr::Wildcard => {}
    }
}

/// Walk a subquery (EXISTS/COUNT/COLLECT body) and collect property references.
///
/// This is needed so that correlated property accesses like `a.city` inside
/// `WHERE EXISTS { (a)-[:KNOWS]->(b) WHERE b.city = a.city }` cause the outer
/// scan to include `a.city` in its projected columns.
fn collect_properties_from_subquery(
    query: &Query,
    properties: &mut HashMap<String, std::collections::HashSet<String>>,
) {
    match query {
        Query::Single(stmt) => {
            for clause in &stmt.clauses {
                match clause {
                    Clause::Match(m) => {
                        if let Some(ref wc) = m.where_clause {
                            collect_properties_from_expr_into(wc, properties);
                        }
                    }
                    Clause::With(w) => {
                        for item in &w.items {
                            if let ReturnItem::Expr { expr, .. } = item {
                                collect_properties_from_expr_into(expr, properties);
                            }
                        }
                        if let Some(ref wc) = w.where_clause {
                            collect_properties_from_expr_into(wc, properties);
                        }
                    }
                    Clause::Return(r) => {
                        for item in &r.items {
                            if let ReturnItem::Expr { expr, .. } = item {
                                collect_properties_from_expr_into(expr, properties);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Query::Union { left, right, .. } => {
            collect_properties_from_subquery(left, properties);
            collect_properties_from_subquery(right, properties);
        }
        _ => {}
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
