// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::pushdown::{PredicateAnalyzer, try_label_or_to_union, try_type_or_to_union};
use anyhow::{Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use parking_lot::RwLock;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use uni_common::Value;
use uni_common::core::schema::{
    AnalyzerConfig, BaseTokenizer, EmbeddingConfig, FtsLanguage, FullTextIndexConfig,
    IndexDefinition, JsonFtsIndexConfig, ScalarIndexConfig, ScalarIndexType, Schema,
    SparseVectorIndexConfig, TokenizerConfig, VectorIndexConfig,
};
use uni_cypher::ast::{
    AlterEdgeType, AlterLabel, BinaryOp, CallKind, Clause, CreateConstraint, CreateEdgeType,
    CreateLabel, CypherLiteral, Direction, DropConstraint, DropEdgeType, DropLabel, Expr,
    MatchClause, MergeClause, NodePattern, PathPattern, Pattern, PatternElement, Query,
    RelationshipPattern, RemoveItem, ReturnClause, ReturnItem, SchemaCommand, SetClause, SetItem,
    ShortestPathMode, ShowConstraints, SortItem, Statement, UnaryOp, WindowSpec, WithClause,
    WithRecursiveClause,
};

/// Sentinel column name inserted into a variable's property set to request
/// that the planner build the bare struct column (`add_structural_projection`)
/// WITHOUT pulling the full schema.
///
/// Emitted by `mark_set_item_variables` for `SetItem::Property` targets only.
/// Other SET variants (`Labels`, `Variable`, `VariablePlus`) and REMOVE still
/// emit `"*"` because they replace/merge the whole node.
///
/// **Union semantics:** When both `"*"` and the sentinel appear in the same
/// variable's HashSet (e.g. `SET n.x = 1 RETURN n` collects both), `"*"`
/// dominates — schema expansion still happens. The sentinel only changes
/// behavior when it's the sole structural marker present.
///
/// Reserved-name convention: the double-underscore prefix marks this as
/// internal. Schema validation should reject user-declared properties with
/// this name (deferred follow-up).
pub(crate) const STRUCT_ONLY_SENTINEL: &str = "__set_struct__";

/// Provenance marker for a bare entity variable forwarded through a WITH
/// projection (`WITH n …`).
///
/// Emitted instead of `"*"` so [`reconcile_passthrough_properties`] can tell a
/// *forwarded* variable — which only needs the properties actually accessed
/// downstream — from one genuinely returned whole. Always resolved to either
/// `"*"` or [`STRUCT_ONLY_SENTINEL`] before scan planning; it never reaches a
/// scan (a defensive filter treats a stray marker like the struct-only one).
pub(crate) const WITH_PASSTHROUGH_SENTINEL: &str = "__with_passthrough__";

/// Prefix for a transient marker recording that a projected variable is an
/// alias of another (`WITH n AS m` records `__alias_of__n` on `m`).
///
/// Recorded by [`collect_properties_from_plan`] during the same complete plan
/// walk that gathers properties — so alias discovery is guaranteed complete —
/// and consumed and removed by [`reconcile_passthrough_properties`]. Never
/// Reserved key in the collected-properties map holding the UNWIND source
/// variables that nothing else in the plan reads.
///
/// Not a Cypher variable: the leading and trailing double underscores keep it
/// out of the namespace user variables live in, the same trick the sentinels
/// below use for property names.
pub(crate) const DEAD_UNWIND_SOURCES_KEY: &str = "__dead_unwind_sources__";

/// Key recorded when a subquery body projects `RETURN *`.
///
/// `mark_dead_unwind_sources` reasons from absence, so a projection that names
/// nothing proves nothing. It already stands down for a `LogicalPlan`-level
/// wildcard; a body-level one is the same hazard reached through the AST, and
/// measurement confirms `RETURN *` inside a body does export outer-scope
/// variables (a correlated `q` shows up beside the body's own bindings).
pub(crate) const SUBQUERY_WILDCARD_KEY: &str = "__subquery_wildcard__";

/// reaches scan planning.
pub(crate) const ALIAS_OF_PREFIX: &str = "__alias_of__";

/// Matches a `variable.property` reference so `strip_variable_prefix` can
/// rewrite it to the bare property name. Compiled once — the index-DDL /
/// EXPLAIN path would otherwise recompile it on every call.
static VAR_PREFIX_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b\w+\.(\w+)").unwrap());

/// Type of variable in scope for semantic validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariableType {
    /// Node variable (from MATCH (n), CREATE (n), etc.)
    Node,
    /// Edge/relationship variable (from `MATCH ()-[r]->()`, etc.)
    Edge,
    /// Path variable (from `MATCH p = (a)-[*]->(b)`, etc.)
    Path,
    /// A list of nodes: a GQL group variable bound by a quantified path
    /// pattern, or the result of `nodes(p)`.
    ///
    /// Deliberately a distinct variant rather than a `List(Box<VariableType>)`
    /// payload: `VariableType` is `Copy` and is compared by equality at ~90
    /// sites, so every one of those comparisons excludes a list *by
    /// construction* — using a group variable where a node is expected fails
    /// without a bespoke check at each site.
    NodeList,
    /// A list of relationships: a group variable bound by a quantified path
    /// pattern, a variable-length pattern's step variable, `shortestPath`'s
    /// bound relationship, or the result of `relationships(p)`.
    EdgeList,
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

/// Information about a variable in scope during planning.
#[derive(Debug, Clone)]
pub struct VariableInfo {
    /// Variable name as written in the query.
    pub name: String,
    /// Semantic type of the variable.
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

/// Does a pattern predicate appear somewhere a boolean is not expected?
///
/// A pattern in an expression is a *predicate*, not a value. openCypher permits
/// one wherever a boolean belongs and rejects it everywhere else, and the TCK
/// pins both halves:
///
/// - `Pattern1` [22] / [23] — `RETURN (n)-[]->()` and `WITH (n)-[]->() AS x`
///   are `SyntaxError: UnexpectedSyntax`: a projection is not a boolean context.
/// - `List6` [6] — `RETURN size((a)-->())` likewise; `size` wants a list.
/// - `Pattern1` [19]-[21] — `WHERE NOT (n)-[:REL2]-()` and conjunctions of
///   pattern predicates must *work*.
/// - `List6` [7] — `size([(a)-->(b) | b])`, a pattern *comprehension*, is a list
///   and is fine.
///
/// So the test cannot be "does this expression contain a pattern predicate",
/// which was the original guard and rejected the legal cases along with the
/// illegal ones, nor "is the projected expression itself one", which lets
/// `size((a)-->())` through. It is positional: descend, tracking whether the
/// current position expects a boolean.
fn pattern_predicate_in_non_boolean_position(expr: &Expr) -> bool {
    fn walk(e: &Expr, boolean_ok: bool) -> bool {
        match e {
            Expr::Exists {
                from_pattern_predicate: true,
                ..
            } => !boolean_ok,
            // Boolean connectives propagate a boolean context to their operands.
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: inner,
            } => walk(inner, true),
            Expr::BinaryOp {
                left,
                op: BinaryOp::And | BinaryOp::Or | BinaryOp::Xor,
                right,
            } => walk(left, true) || walk(right, true),
            // `NOT` also reaches here spelled as a call, which is how LDBC writes
            // it: `not((liker)-[:KNOWS]-(person))`.
            Expr::FunctionCall { name, args, .. } if name.eq_ignore_ascii_case("not") => {
                args.iter().any(|a| walk(a, true))
            }
            // A comprehension's filter is a boolean context; the list it draws
            // from and the value it produces are not.
            Expr::ListComprehension {
                list,
                map_expr,
                where_clause,
                ..
            } => {
                walk(list, false)
                    || walk(map_expr, false)
                    || where_clause.as_ref().is_some_and(|w| walk(w, true))
            }
            // Anything else: children sit in a non-boolean position unless one of
            // the arms above says otherwise.
            other => {
                let mut found = false;
                other.for_each_child(&mut |child| {
                    if !found {
                        found = walk(child, false);
                    }
                });
                found
            }
        }
    }
    // A projection is not a boolean context.
    walk(expr, false)
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

/// The element noun for a list-typed binding, or `None` if not a list.
///
/// Used by diagnostics to name what the variable actually holds.
fn list_element_noun(t: VariableType) -> Option<&'static str> {
    match t {
        VariableType::NodeList => Some("node"),
        VariableType::EdgeList => Some("relationship"),
        _ => None,
    }
}

/// The binding type for a relationship variable: a single edge for a
/// fixed-length pattern, a list of edges for a variable-length one.
fn edge_binding_type(is_variable_length: bool) -> VariableType {
    if is_variable_length {
        VariableType::EdgeList
    } else {
        VariableType::Edge
    }
}

/// Convert VariableInfo vec to String vec for backward compatibility
///
/// Lossy: the name survives, the type does not. Callers that round-trip scope
/// through this re-import variables as `Imported`, which is compatible with
/// everything — including, deliberately, the list types.
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
        | Expr::Literal(CypherLiteral::Bool(_))
        | Expr::Literal(CypherLiteral::Bytes(_)) => VariableType::ScalarLiteral,
        Expr::FunctionCall { name, args, .. } => {
            let lower = name.to_lowercase();
            if lower == "coalesce" {
                infer_coalesce_type(args, vars_in_scope)
            } else if lower == "nodes" {
                VariableType::NodeList
            } else if lower == "relationships" {
                VariableType::EdgeList
            } else if lower == "collect" && !args.is_empty() {
                let collected = infer_with_output_type(&args[0], vars_in_scope);
                match collected {
                    // `collect` aggregates *into a list*, so collecting nodes
                    // yields a node list, exactly as `nodes()` does above.
                    // Returning the element type unchanged made
                    // `WITH collect(n) AS ns` look like a single node, which
                    // rejected `size(ns)` as "size() requires a string, list, or
                    // path argument" and would equally have let `MATCH (ns)-->()`
                    // through. `unwrap_list_type` maps these back for `UNWIND`,
                    // so consuming the list one element at a time is unaffected.
                    VariableType::Node => VariableType::NodeList,
                    VariableType::Edge => VariableType::EdgeList,
                    // `Path` has no list counterpart to map to, and `Imported`
                    // is deliberately opaque; both are carried through as before
                    // rather than collapsed to `Scalar`.
                    VariableType::Path | VariableType::Imported => collected,
                    _ => VariableType::Scalar,
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
            VariableType::Node
            | VariableType::Edge
            | VariableType::Path
            | VariableType::NodeList
            | VariableType::EdgeList => {
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

/// The element type produced by unwrapping one level of list-ness.
///
/// `UNWIND` over a list of graph elements yields the elements themselves — this
/// is the inverse of the list binding and the supported way to consume a group
/// variable as individual nodes or relationships.
fn unwrap_list_type(t: VariableType) -> VariableType {
    match t {
        VariableType::NodeList => VariableType::Node,
        VariableType::EdgeList => VariableType::Edge,
        other => other,
    }
}

fn infer_unwind_output_type(expr: &Expr, vars_in_scope: &[VariableInfo]) -> VariableType {
    match expr {
        Expr::Variable(v) => find_var_in_scope(vars_in_scope, v)
            .map(|info| unwrap_list_type(info.var_type))
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

/// Collect the names of `$param` references in a constant-foldable expression.
///
/// Walks the variants that `eval_const_numeric_expr` accepts (the only shapes a
/// successfully-folded `LIMIT`/`SKIP` expression can take): parameters,
/// literals, unary/binary arithmetic, and the whitelisted numeric functions.
/// Used to tell the plan cache which parameter values were baked into the plan.
fn collect_expr_parameters(expr: &Expr, names: &mut Vec<String>) {
    match expr {
        Expr::Parameter(name) => {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        Expr::UnaryOp { expr: e, .. } => collect_expr_parameters(e, names),
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_parameters(left, names);
            collect_expr_parameters(right, names);
        }
        Expr::FunctionCall { args, .. } => {
            for a in args {
                collect_expr_parameters(a, names);
            }
        }
        _ => {}
    }
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

    // length()/size() do NOT accept a single Node or Edge. They are valid on
    // the list types — a VLP/shortestPath step variable, a group variable, or
    // a `nodes(p)`/`relationships(p)` result — which are separate variants and
    // so are not caught here.
    if (name_lower == "length" || name_lower == "size")
        && let Some(Expr::Variable(var_name)) = args.first()
        && let Some(info) = find_var_in_scope(vars_in_scope, var_name)
        && matches!(info.var_type, VariableType::Node | VariableType::Edge)
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
        let op_name = format!("{op:?}").to_uppercase();
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
            | "stddev"
            | "stdevp"
            | "stddevp"
            | "variance"
            | "variancep"
            | "percentiledisc"
            | "percentilecont"
            | "btic_min"
            | "btic_max"
            | "btic_span_agg"
            | "btic_count_at"
    ) || uni_cypher::is_known_plugin_aggregate(name)
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
        // A bare aggregate is what we are looking for — collect it and stop;
        // its own arguments cannot contain another top-level aggregate.
        Expr::FunctionCall {
            name, window_spec, ..
        } if window_spec.is_none() && is_aggregate_function_name(name) => {
            out.push(expr.clone());
        }
        // Subquery aggregates count as aggregates at this node. `for_each_child`
        // never descends into them, so they must be collected here or not at all.
        Expr::CountSubquery(_) | Expr::CollectSubquery(_) => out.push(expr.clone()),
        other => other.for_each_child_in_scope(&mut |child| {
            extract_inner_aggregates_rec(child, out);
        }),
    }
}

/// Return a copy of `expr` with every inner aggregate FunctionCall replaced by
/// `Expr::Variable(aggregate_column_name(agg))`.
///
/// For `ListComprehension`/`Quantifier`/`Reduce`, only the `list` field is
/// rewritten (the body references the loop variable, not outer-scope columns).
fn replace_aggregates_with_columns(expr: &Expr) -> Expr {
    match expr {
        // A bare aggregate becomes a reference to the column the aggregation
        // stage will produce; its arguments are consumed by that stage.
        Expr::FunctionCall {
            name, window_spec, ..
        } if window_spec.is_none() && is_aggregate_function_name(name) => {
            Expr::Variable(aggregate_column_name(expr))
        }
        // Subquery aggregates likewise. `map_children` never descends into
        // them, so they must be rewritten here or not at all.
        Expr::CountSubquery(_) | Expr::CollectSubquery(_) => {
            Expr::Variable(aggregate_column_name(expr))
        }
        // Everything else rewrites its in-scope children. The scoped map leaves
        // comprehension bodies alone — they reference the loop variable, not a
        // column the aggregation stage emits.
        other => other
            .clone()
            .map_children_in_scope(&mut |child| replace_aggregates_with_columns(&child)),
    }
}

/// Check if an expression contains any aggregate function (recursively).
fn contains_aggregate_recursive(expr: &Expr) -> bool {
    if matches!(expr, Expr::FunctionCall { name, .. } if is_aggregate_function_name(name)) {
        return true;
    }
    // `for_each_child_in_scope`, not `for_each_child`: an aggregate inside a
    // comprehension's body belongs to the comprehension, not to the query
    // containing it. Using the scoped walk keeps that narrowing while picking
    // up the variants the old hand-rolled match silently missed through its
    // `_ => false` arm — `MapProjection`, `ValidAt` and `LabelCheck`. Missing
    // `MapProjection` made `RETURN n{.name, c: count(*)}` fail to plan.
    let mut found = false;
    expr.for_each_child_in_scope(&mut |child| {
        if !found {
            found = contains_aggregate_recursive(child);
        }
    });
    found
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
        // An aggregate contributes its own repr; its arguments are grouped over.
        Expr::FunctionCall { name, .. } if is_aggregate_function_name(name) => {
            out.insert(expr.to_string_repr());
        }
        other => other.for_each_child_in_scope(&mut |child| {
            collect_aggregate_reprs(child, out);
        }),
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

fn collect_non_aggregate_refs(expr: &Expr, out: &mut Vec<NonAggregateRef>) {
    match expr {
        // An aggregate's arguments are grouped over — they are not group keys.
        Expr::FunctionCall { name, .. } if is_aggregate_function_name(name) => {}
        Expr::Variable(v) => out.push(NonAggregateRef::Var(v.clone())),
        // A property reference is itself a key; do not descend into its base.
        Expr::Property(base, _) => {
            let base_var = match base.as_ref() {
                Expr::Variable(v) => Some(v.clone()),
                _ => None,
            };
            out.push(NonAggregateRef::Property {
                repr: expr.to_string_repr(),
                base_var,
            });
        }
        // Everything else contributes whatever its in-scope children do. The
        // old hand-rolled match ended in `_ => {}`, so `MapProjection`,
        // `ValidAt`, `LabelCheck`, `Map`, `ArrayIndex` and `ArraySlice` never
        // yielded group keys — which is why `RETURN n{.name, c: count(*)}`
        // planned without `n.name` in the grouping and failed on its schema.
        other => other.for_each_child_in_scope(&mut |child| {
            collect_non_aggregate_refs(child, out);
        }),
    }
}

/// Validate compound aggregate expressions: non-aggregate refs must be
/// individually present in the group_by as simple variables or properties.
fn validate_compound_aggregates(compound_agg_exprs: &[Expr], group_by: &[Expr]) -> Result<()> {
    let group_by_reprs: HashSet<String> = group_by.iter().map(|e| e.to_string_repr()).collect();
    for expr in compound_agg_exprs {
        let mut refs = Vec::new();
        collect_non_aggregate_refs(expr, &mut refs);
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
    Ok(())
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
    collect_non_aggregate_refs(expr, &mut refs);
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
    params: &HashMap<String, uni_common::Value>,
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
                    use rand::RngExt;
                    let mut rng = rand::rng();
                    Ok(ConstNumber::Float(rng.random::<f64>()))
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
    params: &HashMap<String, uni_common::Value>,
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
fn validate_no_deleted_entity_access(expr: &Expr, deleted_vars: &HashSet<String>) -> Result<()> {
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

/// Flatten every label name appearing in a `Pattern` (across all paths
/// and node elements). Used by the M5 follow-up #6 write-rejection
/// guard to refuse CREATE/MERGE that names a virtual catalog-resolved
/// label.
fn collect_pattern_labels(pattern: &uni_cypher::ast::Pattern) -> Vec<String> {
    let mut out = Vec::new();
    for path in &pattern.paths {
        for element in &path.elements {
            if let PatternElement::Node(n) = element {
                for l in n.labels.names() {
                    out.push(l.clone());
                }
            }
        }
    }
    out
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
                // A list of graph elements has no properties of its own. Say
                // so here rather than letting it reach the `index` UDF, which
                // reports "list index must be an integer" and points at the
                // wrong thing.
                if let Some(kind) = list_element_noun(var_info.var_type) {
                    return Err(anyhow!(
                        "TypeError: InvalidArgumentType - Type mismatch: expected Node or \
                         Relationship but was a list of {kind}s for property access '{}.{}'; \
                         '{}' is a group variable bound by a quantified path pattern — use \
                         [item IN {} | item.{}] to read the property of each element, or \
                         last({}).{} for the final one",
                        var_name,
                        prop,
                        var_name,
                        var_name,
                        prop,
                        var_name,
                        prop
                    ));
                }
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
///
/// Used by `LogicalPlan::Traverse` when `qpp_steps` is `Some`.
#[derive(Debug, Clone)]
pub struct QppStepInfo {
    /// Edge type IDs that this step can traverse.
    pub edge_type_ids: Vec<u32>,
    /// Traversal direction for this step.
    pub direction: Direction,
    /// Optional label constraint on the target node.
    pub target_label: Option<String>,
    /// Predicate from an inline property map on this hop's relationship.
    /// Per-hop, because a quantified pattern can constrain each hop
    /// differently — one predicate for the whole traversal is not enough.
    pub edge_filter_expr: Option<Expr>,
    /// Predicate from an inline property map on this hop's *target node*.
    /// Read alongside `target_label`, which only ever carried the label.
    pub target_property_expr: Option<Expr>,
    /// This hop's relationship variable, when the user named it. Bound as a
    /// GQL group variable — a list with one relationship per iteration.
    pub edge_variable: Option<String>,
    /// This hop's target node variable, when the user named it. Bound as a
    /// group variable — a list with one node per iteration.
    pub target_variable: Option<String>,
}

/// Phase 5a-impl: per-type fusion strategy for `LogicalPlan::FusedIndexScan`.
///
/// `#[non_exhaustive]` so Phase 5b can add `AnnRerank` and `Bm25Rrf`
/// without breaking downstream pattern-match exhaustiveness.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FusionKind {
    /// Union of parent + fork-local BTree hits, deduped by VID.
    BtreeUnion,
    /// k-way merge of pre-sorted parent + fork streams (ORDER BY).
    SortedKWayMerge,
    /// Fork-first UID lookup; falls back to parent on miss. Used
    /// when a fork rebinds an external UID and queries must see the
    /// fork's binding before the parent's.
    VidUidForkFirst,
    /// Phase 5b — vector ANN rerank: top-k from primary's index +
    /// top-k from fork-local index, merged and reranked by exact
    /// distance. Recall ≥ 95% per spec §8.2.
    AnnRerank,
    /// Phase 5b — BM25 reciprocal rank fusion: ranked lists from
    /// primary's and fork-local FTS indexes combined via standard
    /// RRF (`score = sum 1 / (k_rrf + rank_i)`, k_rrf = 60).
    Bm25Rrf,
    /// M4 — hybrid RRF that includes a learned-sparse (SPLADE) source:
    /// emitted for `uni.search` whose properties map carries a `sparse`
    /// key, fused via N-ary RRF in `run_hybrid_search`. Independent of
    /// fork-local indexes.
    SparseRrf,
    /// M4 — sparse dot-product rerank: the `uni.sparse.query` analogue of
    /// [`FusionKind::AnnRerank`], fusing primary's and fork-local sparse
    /// indexes. Reserved: emitted once fork-local sparse indexes land
    /// (issue #95 Task #4 introduces `ForkLocalIndexKind::Sparse`).
    SparseDot,
}

/// Logical query plan produced by [`QueryPlanner`].
///
/// Each variant represents one step in the Cypher execution pipeline.
/// Plans are tree-structured — leaf nodes produce rows, intermediate nodes
/// transform or join them, and the root node defines the final output.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// UNION / UNION ALL of two sub-plans.
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        /// When `true`, duplicate rows are preserved (UNION ALL semantics).
        all: bool,
    },
    /// Scan vertices of a single labeled dataset.
    Scan {
        label_id: u16,
        labels: Vec<String>,
        variable: String,
        filter: Option<Expr>,
        optional: bool,
    },
    /// Phase 5a-impl: fused scan over both primary's index and the
    /// forked session's fork-local index. Emitted by the planner only
    /// when (a) the session is forked AND (b) `StorageManager::has_fork_index`
    /// returns `true` for the target column and fusion kind. Otherwise the planner
    /// keeps emitting `Scan` and Lance's `base_paths` chain transparently
    /// covers parent-inherited indexes.
    ///
    /// `kind` selects the per-type fusion strategy:
    /// - `BtreeUnion` — union of parent + fork hits, dedup by VID.
    /// - `SortedKWayMerge` — k-way merge of two pre-sorted streams.
    /// - `VidUidForkFirst` — probe fork's branch first, fall back to
    ///   parent's UID index on miss.
    FusedIndexScan {
        label_id: u16,
        labels: Vec<String>,
        variable: String,
        filter: Option<Expr>,
        optional: bool,
        kind: FusionKind,
    },
    /// Phase 5b followup: planner-side observability marker for the
    /// lossy fusion types. Wraps the original `VectorKnn` or
    /// `InvertedIndexLookup` (or any future leaf operator whose
    /// shape differs from `Scan`) without changing its fields, so
    /// the physical planner can decay it to `inner` unchanged.
    ///
    /// Runtime behavior is identical to running `inner` directly;
    /// the wrap is purely for explain-plan and runtime-stats
    /// observability. The actual fusion happens at the
    /// `BranchedBackend` layer (per-branch Lance reads via
    /// `base_paths`), exactly as in Phase 5b's core ship.
    FusedIndexScanWrapped {
        inner: Box<LogicalPlan>,
        kind: FusionKind,
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
    /// Produces exactly one empty row (used to bootstrap pipelines with no source).
    Empty,
    /// UNWIND: expand a list expression into one row per element.
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
        edge_properties: HashSet<String>,
        /// Whether this is a variable-length pattern (has `*` range specifier).
        /// When true, step_variable holds a list of edges (even for *1..1).
        is_variable_length: bool,
        /// All variables from this OPTIONAL MATCH pattern.
        /// When any hop in the pattern fails, ALL these variables should be set to NULL.
        /// This ensures proper multi-hop OPTIONAL MATCH semantics.
        optional_pattern_vars: HashSet<String>,
        /// Variable names (node + edge) from the current MATCH clause scope.
        /// Used for relationship uniqueness scoping: only edge ID columns whose
        /// associated variable is in this set participate in uniqueness filtering.
        /// Variables from previous disconnected MATCH clauses are excluded.
        scope_match_variables: HashSet<String>,
        /// Edge property predicate for VLP inline filtering (instead of post-Filter).
        edge_filter_expr: Option<Expr>,
        /// Path traversal semantics (Trail by default for OpenCypher).
        path_mode: crate::query::df_graph::nfa::PathMode,
        /// QPP steps for multi-hop quantified path patterns.
        /// `None` for simple VLP patterns; `Some` for QPP with per-step edge types/constraints.
        /// When present, `min_hops`/`max_hops` are derived from iterations × steps.len().
        qpp_steps: Option<Vec<QppStepInfo>>,
        /// The quantified pattern's inner *source* node variable, when named.
        /// It has no owning step — it is node position 0 of each iteration —
        /// so it cannot live on `qpp_steps`. Bound as a group variable.
        qpp_inner_source: Option<String>,
    },
    /// Traverse main edges table filtering by type name(s) (`MATCH (a)-[:Unknown]->(b)`).
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
        optional_pattern_vars: HashSet<String>,
        /// Variables belonging to the current MATCH clause scope.
        /// Used for relationship uniqueness scoping: only edge columns whose
        /// associated variable is in this set participate in uniqueness filtering.
        scope_match_variables: HashSet<String>,
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
        optional_variables: HashSet<String>,
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
        /// Relationship variable bound by the pattern, e.g. `r` in
        /// `shortestPath((a)-[r:E*]->(b))`. Holds the path's relationships as a
        /// list, exactly as an ordinary variable-length pattern's step variable
        /// does. `None` when the relationship is anonymous.
        step_variable: Option<String>,
        /// Minimum number of hops (edges) in the path. Default is 1.
        min_hops: u32,
        /// Maximum number of hops (edges) in the path. Default is u32::MAX (unlimited).
        max_hops: u32,
        /// Predicate from an inline property map on the relationship, e.g.
        /// `-[:E {tag: 'keep'}]->`. Evaluated *during* expansion, not after:
        /// the shortest path among permitted edges is not the unconstrained
        /// shortest path filtered. `None` when the pattern carries no map.
        edge_filter_expr: Option<Expr>,
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
        /// Relationship variable bound by the pattern, e.g. `r` in
        /// `shortestPath((a)-[r:E*]->(b))`. Holds the path's relationships as a
        /// list, exactly as an ordinary variable-length pattern's step variable
        /// does. `None` when the relationship is anonymous.
        step_variable: Option<String>,
        /// Minimum number of hops (edges) in the path. Default is 1.
        min_hops: u32,
        /// Maximum number of hops (edges) in the path. Default is u32::MAX (unlimited).
        max_hops: u32,
        /// Predicate from an inline property map on the relationship, e.g.
        /// `-[:E {tag: 'keep'}]->`. Evaluated *during* expansion, not after:
        /// the shortest path among permitted edges is not the unconstrained
        /// shortest path filtered. `None` when the pattern carries no map.
        edge_filter_expr: Option<Expr>,
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
    /// Scored sparse-vector (SPLADE / learned-sparse) index. Reached via
    /// `CREATE VECTOR INDEX … OPTIONS{type:'sparse'}`, which shares the vector
    /// DDL surface but is a distinct index kind.
    CreateSparseIndex {
        config: SparseVectorIndexConfig,
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

    // ── Locy variants ──────────────────────────────────────────
    /// Top-level Locy program: stratified rules + commands.
    LocyProgram {
        strata: Vec<super::planner_locy_types::LocyStratum>,
        commands: Vec<super::planner_locy_types::LocyCommand>,
        derived_scan_registry: Arc<super::df_graph::locy_fixpoint::DerivedScanRegistry>,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        /// Active probability semiring (rollout D-7). Defaults to
        /// `AddMultProb` (Phase 1/2 byte-identical behavior). `BddExact`
        /// is selected by `LocyConfig::resolve()` when `exact_probability`
        /// is true.
        semiring_kind: uni_locy::SemiringKind,
        /// Phase B Slice 3: per-evaluation registry of neural classifiers
        /// keyed by model name. Empty for programs without `CREATE MODEL`.
        classifier_registry: Arc<uni_locy::ClassifierRegistry>,
        /// Phase B follow-up: optional memoization cache. `None` →
        /// runtime creates a fresh per-query cache; `Some` → shared
        /// across queries (caller-managed).
        classifier_cache: Option<Arc<uni_locy::ModelInvocationCache>>,
        /// Phase C B1-B3 follow-up: per-query side-channel store
        /// for per-invocation (raw, calibrated, confidence_band)
        /// records. Flows alongside `classifier_cache` into
        /// `LocyProgramExec`.
        classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    },
    /// FOLD operator: lattice-join non-key columns per KEY group.
    LocyFold {
        input: Box<LogicalPlan>,
        key_columns: Vec<String>,
        fold_bindings: Vec<(String, Expr)>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
    },
    /// BEST BY operator: select best row per KEY group by ordered criteria.
    LocyBestBy {
        input: Box<LogicalPlan>,
        key_columns: Vec<String>,
        /// (expression, ascending) pairs.
        criteria: Vec<(Expr, bool)>,
    },
    /// PRIORITY operator: keep only highest-priority clause's rows per KEY group.
    LocyPriority {
        input: Box<LogicalPlan>,
        key_columns: Vec<String>,
    },
    /// Scan a derived relation's in-memory buffer during fixpoint iteration.
    LocyDerivedScan {
        scan_index: usize,
        data: Arc<RwLock<Vec<RecordBatch>>>,
        schema: SchemaRef,
    },
    /// Compact projection for Locy YIELD — emits ONLY the listed expressions,
    /// without carrying through helper/property columns like the regular Project.
    LocyProject {
        input: Box<LogicalPlan>,
        projections: Vec<(Expr, Option<String>)>,
        /// Expected output Arrow type per projection (for CAST support).
        target_types: Vec<DataType>,
    },
    /// Phase B A4: invoke registered neural classifiers against the
    /// input batches and overwrite the per-invocation placeholder
    /// column with each row's predicted probability. Wraps a Locy
    /// clause body plan when `CompiledClause.model_invocations` is
    /// non-empty; transparent (passes batches through unchanged) when
    /// the list is empty.
    ///
    /// Registry and cache are carried on the node so that
    /// `execute_subplan` — which spins up a fresh
    /// `HybridPhysicalPlanner` per call — can lower it to a physical
    /// `LocyModelInvokeExec` without depending on planner-side
    /// runtime state.
    LocyModelInvoke {
        input: Box<LogicalPlan>,
        invocations: Vec<uni_locy::ModelInvocation>,
        classifier_registry: Arc<uni_locy::ClassifierRegistry>,
        classifier_cache: Option<Arc<uni_locy::ModelInvocationCache>>,
        /// Phase C B1-B3 follow-up: per-query side-channel store
        /// for per-invocation (raw, calibrated, confidence_band)
        /// records. `LocyModelInvokeExec` writes here after each
        /// classifier call; EXPLAIN reads via collect_neural_calls
        /// to surface NeuralProvenance for ALONG/FOLD-position
        /// invocations and Mode B re-execution paths.
        classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
        /// Phase D D3 runtime: one handle per `path_context.source_rule`
        /// referenced by any invocation on this node. The handle's
        /// `data: Arc<RwLock<Vec<RecordBatch>>>` is shared with the
        /// `DerivedScanRegistry`; the source rule's derived facts are
        /// already converged by the time this node executes (the
        /// dependency-graph builder ensures source rules sit in
        /// earlier strata).
        path_context_handles: std::collections::HashMap<
            String,
            super::df_graph::locy_model_invoke::PathContextHandle,
        >,
    },
}

impl LogicalPlan {
    /// Mutable access to this node's single child input, when it has one.
    ///
    /// `..` works in a *pattern* even though enum struct-variants have no
    /// functional-update syntax for *construction* — which is what makes this
    /// one line per variant while rebuilding a node by hand is not.
    fn input_mut(&mut self) -> Option<&mut LogicalPlan> {
        match self {
            LogicalPlan::Unwind { input, .. }
            | LogicalPlan::Traverse { input, .. }
            | LogicalPlan::TraverseMainByType { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Create { input, .. }
            | LogicalPlan::CreateBatch { input, .. }
            | LogicalPlan::Merge { input, .. }
            | LogicalPlan::Set { input, .. }
            | LogicalPlan::Remove { input, .. }
            | LogicalPlan::Delete { input, .. }
            | LogicalPlan::Foreach { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Apply { input, .. }
            | LogicalPlan::SubqueryCall { input, .. }
            | LogicalPlan::ShortestPath { input, .. }
            | LogicalPlan::AllShortestPaths { input, .. }
            | LogicalPlan::QuantifiedPattern { input, .. }
            | LogicalPlan::BindZeroLengthPath { input, .. }
            | LogicalPlan::BindPath { input, .. }
            | LogicalPlan::LocyFold { input, .. }
            | LogicalPlan::LocyBestBy { input, .. }
            | LogicalPlan::LocyPriority { input, .. }
            | LogicalPlan::LocyProject { input, .. }
            | LogicalPlan::LocyModelInvoke { input, .. } => Some(input),
            _ => None,
        }
    }

    /// This node's child plans, in evaluation order.
    ///
    /// The immutable counterpart to [`Self::input_mut`], widened to the
    /// multi-child variants so a read-only survey can reach every node. A
    /// missing arm here silently hides a subtree from any analysis built on it,
    /// so the single-input list is kept identical to `input_mut`'s.
    fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right, .. } => {
                vec![left, right]
            }
            LogicalPlan::Apply {
                input, subquery, ..
            }
            | LogicalPlan::SubqueryCall { input, subquery } => vec![input, subquery],
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => vec![initial, recursive],
            LogicalPlan::Explain { plan } => vec![plan],
            LogicalPlan::QuantifiedPattern {
                input,
                pattern_plan,
                ..
            } => vec![input, pattern_plan],
            LogicalPlan::Foreach { input, body, .. } => {
                let mut kids = vec![&**input];
                kids.extend(body.iter());
                kids
            }
            LogicalPlan::Unwind { input, .. }
            | LogicalPlan::Traverse { input, .. }
            | LogicalPlan::TraverseMainByType { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Create { input, .. }
            | LogicalPlan::CreateBatch { input, .. }
            | LogicalPlan::Merge { input, .. }
            | LogicalPlan::Set { input, .. }
            | LogicalPlan::Remove { input, .. }
            | LogicalPlan::Delete { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::ShortestPath { input, .. }
            | LogicalPlan::AllShortestPaths { input, .. }
            | LogicalPlan::BindZeroLengthPath { input, .. }
            | LogicalPlan::BindPath { input, .. }
            | LogicalPlan::FusedIndexScanWrapped { inner: input, .. }
            | LogicalPlan::LocyFold { input, .. }
            | LogicalPlan::LocyBestBy { input, .. }
            | LogicalPlan::LocyPriority { input, .. }
            | LogicalPlan::LocyProject { input, .. }
            | LogicalPlan::LocyModelInvoke { input, .. } => vec![input],
            _ => Vec::new(),
        }
    }

    /// Replace this node's single child input, leaving every other field intact.
    ///
    /// Rust has no functional update (`..rest`) for enum struct-variants, so
    /// swapping one field of a 19-field variant like `Traverse` otherwise means
    /// destructuring and rebuilding all nineteen at the call site — and
    /// silently dropping whichever one you forget. The plan rewriters carried
    /// that 38-line dance once each.
    ///
    /// Nodes with no single input are returned unchanged, matching the
    /// `other => other` arm those rewriters already had.
    #[must_use]
    pub fn map_input(mut self, f: impl FnOnce(LogicalPlan) -> LogicalPlan) -> Self {
        if let Some(input) = self.input_mut() {
            let taken = std::mem::replace(input, LogicalPlan::Empty);
            *input = f(taken);
        }
        self
    }
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

/// Translates a parsed Cypher AST into a [`LogicalPlan`].
///
/// `QueryPlanner` applies semantic validation (variable scoping, label
/// resolution, type checking) and produces a plan tree that the executor
/// can run against storage.
#[derive(Debug)]
pub struct QueryPlanner {
    schema: Arc<Schema>,
    /// Cache of parsed generation expressions, keyed by (label_name, gen_col_name).
    gen_expr_cache: HashMap<(String, String), Expr>,
    /// Counter for generating unique anonymous variable names.
    anon_counter: std::sync::atomic::AtomicUsize,
    /// Optional query parameters for resolving $param in SKIP/LIMIT.
    params: HashMap<String, uni_common::Value>,
    /// Optional plugin registry consulted when label / edge-type / identifier
    /// resolution misses the local schema (M5b — Catalog / ReplacementScan).
    plugin_registry: Option<Arc<uni_plugin::PluginRegistry>>,
    /// Gate for replacement-scan dispatch on unknown identifiers (M5b).
    replacement_scans_enabled: bool,
    /// Names of parameters folded into a `LIMIT`/`SKIP` position during the
    /// plan. The resulting `LogicalPlan::Limit` bakes the concrete values in, so
    /// a plan cache keyed on query text must additionally key on these
    /// parameters' values (see `folded_limit_skip_params`). Interior-mutable
    /// because `plan` takes `&self`.
    folded_limit_skip_params: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

struct TraverseParams<'a> {
    rel: &'a RelationshipPattern,
    target_node: &'a NodePattern,
    optional: bool,
    path_variable: Option<String>,
    /// All variables from this OPTIONAL MATCH pattern.
    /// Used to ensure multi-hop patterns correctly NULL all vars when any hop fails.
    optional_pattern_vars: HashSet<String>,
}

/// Which `plan_where_clause` rewriter a reachability check is standing in for.
///
/// Each predicate/label/KNN rewriter recurses through a fixed set of
/// "transparent" plan nodes and its own base node, falling through
/// `other => other` for everything else. [`QueryPlanner::rewrite_target_reachable`]
/// mirrors exactly that per-rewriter descent so consumption of a predicate can be
/// gated on whether the rewriter can actually apply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewriteTarget {
    /// `replace_scan_with_knn`: base `Scan(var)`; descends Filter/Project/Limit/CrossJoin.
    Knn,
    /// `push_predicate_to_scan`: base `Scan|ScanAll(var)`; descends Filter/Project/CrossJoin/Traverse.
    Scan,
    /// `replace_scan_all_with_label_union`: base `ScanAll(var)`; descends Filter/Project/CrossJoin/Traverse.
    LabelUnion,
    /// `push_predicate_to_traverse`: base `Traverse(target==var)`; descends Filter/Project/CrossJoin/Traverse.
    TraverseTarget,
}

impl QueryPlanner {
    /// Create a new planner for the given schema.
    ///
    /// Pre-parses all generation expressions defined in the schema so that
    /// repeated plan calls avoid redundant parsing.
    pub fn new(schema: Arc<Schema>) -> Self {
        // Pre-parse all generation expressions for caching
        let mut gen_expr_cache = HashMap::new();
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
            anon_counter: std::sync::atomic::AtomicUsize::new(0),
            params: HashMap::new(),
            plugin_registry: None,
            replacement_scans_enabled: false,
            folded_limit_skip_params: std::sync::Mutex::new(std::collections::BTreeSet::new()),
        }
    }

    /// Graph schema this planner resolves labels and property types against.
    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Record the parameters referenced by a successfully-folded `LIMIT`/`SKIP`
    /// expression so the caller's plan cache can key on their values.
    fn note_folded_limit_skip(&self, expr: &Expr) {
        let mut names = Vec::new();
        collect_expr_parameters(expr, &mut names);
        if !names.is_empty()
            && let Ok(mut acc) = self.folded_limit_skip_params.lock()
        {
            acc.extend(names);
        }
    }

    /// Parameter names folded into `LIMIT`/`SKIP` positions during the last
    /// [`plan`](Self::plan).
    ///
    /// The cached plan bakes these values in, so a text-keyed plan cache must
    /// fold their current values into its key — otherwise two calls differing
    /// only in a LIMIT/SKIP parameter would wrongly share one cached plan.
    /// Returns an empty vector when no parameter was folded.
    #[must_use]
    pub fn folded_limit_skip_params(&self) -> Vec<String> {
        self.folded_limit_skip_params
            .lock()
            .map(|acc| acc.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Set query parameters for resolving `$param` references in SKIP/LIMIT.
    pub fn with_params(mut self, params: HashMap<String, uni_common::Value>) -> Self {
        self.params = params;
        self
    }

    /// Attach a plugin registry for catalog / replacement-scan fallbacks
    /// (M5b). When absent, label / edge-type resolution behaves exactly as
    /// before; when present, an unknown label is offered to each
    /// `CatalogProvider` before erroring.
    #[must_use]
    pub fn with_plugin_registry(mut self, registry: Arc<uni_plugin::PluginRegistry>) -> Self {
        self.plugin_registry = Some(registry);
        self
    }

    /// Enable replacement-scan dispatch on unknown identifiers (M5b §4.23).
    /// Default off; opt-in only.
    #[must_use]
    pub fn with_replacement_scans(mut self, enabled: bool) -> Self {
        self.replacement_scans_enabled = enabled;
        self
    }

    /// Allocate (or look up) a virtual label ID for `name` by consulting
    /// every registered `CatalogProvider` and then every registered
    /// `ReplacementScanProvider` (only the latter when the replacement-
    /// scan gate is on). On a first claim the catalog table is stashed
    /// on the host's [`uni_plugin::PluginRegistry`] under a freshly
    /// allocated virtual ID; subsequent calls with the same name return
    /// the cached ID and refresh the stashed table.
    ///
    /// Returns `None` if no provider claims the label or no plugin
    /// registry is attached. Returns `Some((id, table))` on a hit; the
    /// `id` lies in `[VIRTUAL_LABEL_ID_START, VIRTUAL_LABEL_ID_SENTINEL)`.
    /// Errors are surfaced as `Some(Err(_))`-equivalent via `Result`.
    fn allocate_virtual_label(
        &self,
        name: &str,
    ) -> Result<Option<(u16, Arc<dyn uni_plugin::traits::catalog::CatalogTable>)>> {
        let Some(registry) = self.plugin_registry.as_ref() else {
            return Ok(None);
        };
        // 1. CatalogProvider (always consulted, no gate — Batch 2 semantics).
        let mut claimed: Option<Arc<dyn uni_plugin::traits::catalog::CatalogTable>> = None;
        for cat in registry.catalogs() {
            if let Some(t) = cat.resolve_label(name) {
                claimed = Some(t);
                break;
            }
        }
        // 2. ReplacementScanProvider (gated). Only consult if no
        //    CatalogProvider already claimed.
        if claimed.is_none() {
            use uni_plugin::traits::catalog::{Replacement, ReplacementRequest};
            if let Some(Replacement::CatalogTable(t)) =
                self.consult_replacement_scan(ReplacementRequest::Label(name))
            {
                claimed = Some(t);
            }
        }
        let Some(table) = claimed else {
            return Ok(None);
        };
        let id = registry
            .register_virtual_label(name, Arc::clone(&table))
            .map_err(|e| anyhow!("virtual label registration failed for `{name}`: {e}"))?;
        Ok(Some((id, table)))
    }

    /// Reject any write operation that names a label currently allocated
    /// as a virtual (catalog-backed) label. Catalog tables are read-only
    /// in this milestone — there is no write-back path through
    /// `CatalogTable::scan` to the originating provider, so silently
    /// allowing the write would produce ghosted state on the host side
    /// without affecting the external catalog. Errors with a clear,
    /// actionable message.
    fn reject_virtual_label_writes(&self, labels: &[String], op: &str) -> Result<()> {
        let Some(registry) = self.plugin_registry.as_ref() else {
            return Ok(());
        };
        for label in labels {
            if registry.virtual_label_by_name(label).is_some() {
                return Err(anyhow!(
                    "Cannot {op} on virtual (catalog-resolved) label `{label}` — virtual \
                     labels are read-only; write back via the originating catalog \
                     instead"
                ));
            }
        }
        Ok(())
    }

    /// Edge-type analog of [`Self::allocate_virtual_label`].
    fn allocate_virtual_edge_type(
        &self,
        name: &str,
    ) -> Result<Option<(u32, Arc<dyn uni_plugin::traits::catalog::CatalogTable>)>> {
        let Some(registry) = self.plugin_registry.as_ref() else {
            return Ok(None);
        };
        let mut claimed: Option<Arc<dyn uni_plugin::traits::catalog::CatalogTable>> = None;
        for cat in registry.catalogs() {
            if let Some(t) = cat.resolve_edge_type(name) {
                claimed = Some(t);
                break;
            }
        }
        let Some(table) = claimed else {
            return Ok(None);
        };
        let id = registry
            .register_virtual_edge_type(name, Arc::clone(&table))
            .map_err(|e| anyhow!("virtual edge-type registration failed for `{name}`: {e}"))?;
        Ok(Some((id, table)))
    }

    /// Try to resolve an unknown identifier through replacement-scan providers
    /// (gated by [`Self::with_replacement_scans`]). Returns the first
    /// [`Replacement`] any registered provider produces, or `None` if the
    /// gate is off, no registry is attached, or no provider claims the
    /// identifier. First-match wins (mirrors DuckDB).
    pub(crate) fn consult_replacement_scan(
        &self,
        request: uni_plugin::traits::catalog::ReplacementRequest<'_>,
    ) -> Option<uni_plugin::traits::catalog::Replacement> {
        if !self.replacement_scans_enabled {
            return None;
        }
        let registry = self.plugin_registry.as_ref()?;
        for r in registry.replacement_scans().iter() {
            if let Some(replacement) = r.replace(&request) {
                tracing::debug!(
                    target: "uni.plugin.registry",
                    ?request,
                    ?replacement,
                    "identifier resolved via ReplacementScanProvider"
                );
                return Some(replacement);
            }
        }
        None
    }

    /// Resolve a user-typed procedure name against the attached plugin
    /// registry, applying the same namespace-prefix rules as
    /// `ProcedureRegistry::resolve_user_procedure` (host-coupled
    /// procedure dispatch). Returns `true` if any namespace claims the
    /// name. Used by the procedure-call replacement-scan gate to decide
    /// whether to consult before substituting.
    fn procedure_resolves(&self, user_name: &str) -> bool {
        let Some(registry) = self.plugin_registry.as_ref() else {
            return false;
        };
        // Try every namespace/local split (first-dot → last-dot) so dotted
        // plugin ids resolve alongside the first-dot M9/builtin convention.
        // Mirrors `ProcedureRegistry::resolve_user_procedure`.
        if uni_plugin::QName::candidate_splits(user_name).any(|q| registry.procedure(&q).is_some())
        {
            return true;
        }
        let stripped = user_name.strip_prefix("uni.").unwrap_or(user_name);
        for plugin_id in ["uni", "builtin", "apoc-core", "custom"] {
            if registry
                .procedure(&uni_plugin::QName::new(plugin_id, stripped))
                .is_some()
            {
                return true;
            }
        }
        false
    }

    /// Construct a [`uni_plugin::QName`] from a user-typed identifier for
    /// passing to [`Replacement`]-scan providers. If the name is dotted,
    /// the last segment is the local and the rest is the namespace
    /// (mirroring `QName::parse`). Bare names — which Cypher allows for
    /// procedures (`CALL foo()`) and functions (`RETURN foo(x)`) — are
    /// encoded with the conventional `"user"` namespace; providers that
    /// want to match a bare-typed name should inspect `.local()`.
    fn qname_from_user(name: &str) -> uni_plugin::QName {
        uni_plugin::QName::parse(name).unwrap_or_else(|_| uni_plugin::QName::new("user", name))
    }

    /// Apply `ReplacementScanProvider`-driven function rewrites to the
    /// query's AST. When the gate is off or no registry is attached, the
    /// walker is short-circuited and the query is returned unchanged.
    /// Otherwise, every [`uni_cypher::ast::Expr::FunctionCall`] is offered
    /// to registered providers (first-match wins); a returned
    /// `Replacement::Function(new_qname)` substitutes the name in place.
    /// Rewrite depth is capped at 1 — the rewritten name is NOT re-
    /// consulted (a chained `A→B→A` provider therefore stops after the
    /// first hop). Wrong-variant returns (`CatalogTable`, `Procedure`)
    /// error immediately.
    fn rewrite_function_calls_in_query(
        &self,
        query: uni_cypher::ast::Query,
    ) -> Result<uni_cypher::ast::Query> {
        if !self.replacement_scans_enabled || self.plugin_registry.is_none() {
            return Ok(query);
        }
        let mut rename = |name: &str| -> Result<Option<String>> {
            let qname = Self::qname_from_user(name);
            use uni_plugin::traits::catalog::{Replacement, ReplacementRequest};
            match self.consult_replacement_scan(ReplacementRequest::Function(&qname)) {
                Some(Replacement::Function(new_qname)) => {
                    // Cypher function-call dispatch is bare-name-keyed
                    // (the per-category translators in `df_expr` match on
                    // `name.to_uppercase()` against bare local strings —
                    // "UPPER", "ABS", etc.). When the provider returns a
                    // synthetic-namespace target (`builtin.*` or `user.*`),
                    // strip the namespace so the AST name is what those
                    // dispatchers expect; for plugin-namespaced targets,
                    // preserve the full dotted form (matches how users
                    // type them).
                    let rewritten = match new_qname.namespace() {
                        "builtin" | "user" => new_qname.local().to_string(),
                        _ => new_qname.to_string(),
                    };
                    tracing::debug!(
                        target: "uni.plugin.registry",
                        from = %name,
                        to = %rewritten,
                        "function call rerouted via ReplacementScanProvider"
                    );
                    Ok(Some(rewritten))
                }
                Some(other) => Err(anyhow!(
                    "ReplacementScanProvider returned wrong variant for Function \
                     request `{}`: expected `Function`, got {:?}",
                    name,
                    other
                )),
                None => Ok(None),
            }
        };
        crate::query::rewrite::function_rename::rewrite_function_calls_in_query(query, &mut rename)
    }

    /// Plan a Cypher query with no pre-bound variables.
    pub fn plan(&self, query: Query) -> Result<LogicalPlan> {
        self.plan_with_scope(query, Vec::new())
    }

    /// Plan a Cypher query with a set of externally pre-bound variable names.
    ///
    /// `vars` lists variable names already in scope before this query executes
    /// (e.g., from an enclosing Locy rule body).
    ///
    /// Every logical plan this crate hands out is produced here, so this is
    /// where a rewrite that the plan is not *valid* without belongs — as
    /// opposed to `rewrite_for_fork_fusion` and `fuse_create_set`, which need
    /// live storage state and only forfeit an optimisation when skipped, and so
    /// are applied by the API layer. `resolve_traversal_endpoints` is of the
    /// first kind: eight call sites construct a plan from Cypher and each one
    /// that skipped it would fail `startNode(r)` over a MATCH-bound
    /// relationship rather than merely plan it worse.
    pub fn plan_with_scope(&self, query: Query, vars: Vec<String>) -> Result<LogicalPlan> {
        Ok(resolve_traversal_endpoints(
            self.plan_with_scope_unresolved(query, vars)?,
        ))
    }

    fn plan_with_scope_unresolved(&self, query: Query, vars: Vec<String>) -> Result<LogicalPlan> {
        // Apply query rewrites before planning
        let rewritten_query = crate::query::rewrite::rewrite_query(query)?;
        // M5 follow-up #5: function-call rewrite via ReplacementScanProvider.
        // Done as an AST pass *before* planning so the rewritten name flows
        // through every downstream stage (translation, UDF resolution,
        // execution) as if the user had typed it. No-op when the gate is
        // off or no provider claims the call. First-match wins; hard-cap
        // at one rewrite per call site (the rewritten name is NOT re-
        // consulted) — see `rewrite_function_calls_in_query`.
        let rewritten_query = self.rewrite_function_calls_in_query(rewritten_query)?;
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
            Query::Single(_) | Query::Schema(_) => {}
        }
    }

    fn has_mixed_union_modes(query: &Query) -> bool {
        let mut modes = HashSet::new();
        Self::collect_union_modes(query, &mut modes);
        modes.len() > 1
    }

    pub(crate) fn next_anon_var(&self) -> String {
        let id = self
            .anon_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("_anon_{}", id)
    }

    /// Column names used to validate that both `UNION` branches agree.
    ///
    /// Delegates to [`projection_columns`]; an unknown shape yields an empty
    /// list, which compares equal to another unknown and so does not reject
    /// the union on its own.
    fn extract_projection_columns(plan: &LogicalPlan) -> Vec<String> {
        projection_columns(plan).unwrap_or_default()
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
                        // A pattern is a predicate, not a value, so it cannot be
                        // projected on its own — but it may appear inside an
                        // expression here, as it may inside WHERE.
                        if pattern_predicate_in_non_boolean_position(expr) {
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

        if has_agg {
            validate_compound_aggregates(&compound_agg_exprs, &group_by)?;
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

        // SKIP/LIMIT are parsed here (so a bad expression errors early and the
        // folded-param note is recorded) but the `Limit` node is added AFTER the
        // Project/Distinct below: openCypher applies SKIP/LIMIT to the DISTINCT
        // result, not to the pre-deduplication rows. Adding it here would `LIMIT`
        // before `DISTINCT` and return too few (or wrong) rows.
        let skip_limit = if return_clause.skip.is_some() || return_clause.limit.is_some() {
            let skip = return_clause
                .skip
                .as_ref()
                .map(|e| {
                    self.note_folded_limit_skip(e);
                    parse_non_negative_integer(e, "SKIP", &self.params)
                })
                .transpose()?
                .flatten();
            let fetch = return_clause
                .limit
                .as_ref()
                .map(|e| {
                    self.note_folded_limit_skip(e);
                    parse_non_negative_integer(e, "LIMIT", &self.params)
                })
                .transpose()?
                .flatten();
            Some((skip, fetch))
        } else {
            None
        };

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
                            let col_name = aggregate_column_name(&expr);
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

        // SKIP/LIMIT last — applied to the projected, deduplicated rows.
        if let Some((skip, fetch)) = skip_limit {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                skip,
                fetch,
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
        let mut create_introduced_vars: HashSet<String> = HashSet::new();
        // Track variables targeted by DELETE so we can reject property/label
        // access on deleted entities in subsequent RETURN clauses.
        let mut deleted_vars: HashSet<String> = HashSet::new();

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
                            // M5 follow-up #5: if replacement-scan dispatch is
                            // enabled and the procedure name does not resolve
                            // against the plugin registry, consult registered
                            // `ReplacementScanProvider`s. A `Replacement::Procedure`
                            // substitutes the call's target name in the logical
                            // plan; the rewritten name must itself resolve or
                            // we error immediately (no second-tier consult — caps
                            // rewrite depth at one).
                            let procedure_name = if self.replacement_scans_enabled
                                && !self.procedure_resolves(procedure)
                            {
                                use uni_plugin::traits::catalog::{
                                    Replacement, ReplacementRequest,
                                };
                                let qname = Self::qname_from_user(procedure);
                                match self
                                    .consult_replacement_scan(ReplacementRequest::Procedure(&qname))
                                {
                                    Some(Replacement::Procedure(new_qname)) => {
                                        let rewritten = new_qname.to_string();
                                        if !self.procedure_resolves(&rewritten) {
                                            return Err(anyhow!(
                                                "ReplacementScanProvider rerouted procedure \
                                                 `{}` to `{}`, which also did not resolve",
                                                procedure,
                                                rewritten
                                            ));
                                        }
                                        tracing::debug!(
                                            target: "uni.plugin.registry",
                                            from = %procedure,
                                            to = %rewritten,
                                            "procedure rerouted via ReplacementScanProvider"
                                        );
                                        rewritten
                                    }
                                    Some(other) => {
                                        return Err(anyhow!(
                                            "ReplacementScanProvider returned wrong variant \
                                             for Procedure request `{}`: expected \
                                             `Procedure`, got {:?}",
                                            procedure,
                                            other
                                        ));
                                    }
                                    None => procedure.clone(),
                                }
                            } else {
                                procedure.clone()
                            };
                            let proc_plan = LogicalPlan::ProcedureCall {
                                procedure_name,
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

                            // Apply a post-YIELD WHERE predicate. The grammar nests
                            // `WHERE <expr>` inside `yield_clause` (cypher.pest), so
                            // `CALL ... YIELD ... WHERE ...` stores the predicate on
                            // the CALL node rather than as a standalone clause. Without
                            // consuming it here it was silently dropped, returning
                            // unfiltered rows. Mirrors the MATCH-WHERE (plan_where_clause)
                            // and WITH-WHERE handling. YIELD vars are already in scope.
                            if let Some(predicate) = &call_clause.where_clause {
                                plan = self.plan_where_clause(
                                    predicate,
                                    plan,
                                    &vars_in_scope,
                                    HashSet::new(),
                                )?;
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
                    // M5 follow-up #6: virtual (catalog-resolved) labels are
                    // read-only — reject MERGE that names one.
                    let merge_labels = collect_pattern_labels(&merge_clause.pattern);
                    self.reject_virtual_label_writes(&merge_labels, "MERGE")?;

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
                    // M5 follow-up #6: virtual (catalog-resolved) labels are
                    // read-only — reject CREATE that names one.
                    let create_labels = collect_pattern_labels(&create_clause.pattern);
                    self.reject_virtual_label_writes(&create_labels, "CREATE")?;
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
            Expr::Property(_, _)
                if !collected
                    .iter()
                    .any(|e| e.to_string_repr() == expr.to_string_repr()) =>
            {
                collected.push(expr.clone());
            }
            Expr::Property(_, _) => {}
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

    /// Rewrite system-metadata function calls (`id(v)`, `created_at(v)`,
    /// `updated_at(v)`) to direct property access on the corresponding
    /// internal column (`v._vid`, `v._created_at`, `v._updated_at`). This
    /// normalization enables predicate pushdown via the Property pattern
    /// recognized by `PredicateAnalyzer`.
    ///
    /// All three functions share the same shape: single-arg, argument
    /// must be a node/edge variable, returns the column value directly.
    fn rewrite_id_to_vid(expr: Expr, vars_in_scope: &[VariableInfo]) -> Expr {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } if args.len() == 1 && Self::metadata_function_column(&name, None).is_some() => {
                if let Expr::Variable(ref var) = args[0] {
                    // `id()` resolves to `_eid` for an edge binding and `_vid`
                    // for a node — edge rows expose `_eid`, not `_vid`. Mirror
                    // the projection path (`df_expr.rs` translate of `id`).
                    let var_type = find_var_in_scope(vars_in_scope, var).map(|v| v.var_type);
                    let column = Self::metadata_function_column(&name, var_type)
                        .unwrap()
                        .to_string();
                    Expr::Property(Box::new(Expr::Variable(var.clone())), column)
                } else {
                    Expr::FunctionCall {
                        name,
                        args,
                        distinct,
                        window_spec,
                    }
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::rewrite_id_to_vid(*left, vars_in_scope)),
                op,
                right: Box::new(Self::rewrite_id_to_vid(*right, vars_in_scope)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::rewrite_id_to_vid(*inner, vars_in_scope)),
            },
            other => other,
        }
    }

    /// Return the internal column name for a system-metadata function, or
    /// `None` if the name is not one of the recognised metadata functions.
    ///
    /// `id()` maps to `_eid` when its argument is a relationship
    /// (`VariableType::Edge`) and `_vid` otherwise; `var_type` is `None` when the
    /// caller only needs the is-metadata-function test.
    fn metadata_function_column(
        name: &str,
        var_type: Option<VariableType>,
    ) -> Option<&'static str> {
        if name.eq_ignore_ascii_case("id") {
            if matches!(var_type, Some(VariableType::Edge)) {
                Some("_eid")
            } else {
                Some("_vid")
            }
        } else if name.eq_ignore_ascii_case("created_at") {
            Some("_created_at")
        } else if name.eq_ignore_ascii_case("updated_at") {
            Some("_updated_at")
        } else {
            None
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
        let optional_vars: HashSet<String> = if match_clause.optional {
            vars_in_scope[vars_before_pattern..]
                .iter()
                .map(|v| v.name.clone())
                .collect()
        } else {
            HashSet::new()
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
        // Only a single (source)-[rel]->(target) hop is planned below — the
        // planner reads elements[0..3] and ignores any further hops. Reject a
        // multi-hop shortestPath rather than SILENTLY dropping the extra
        // relationship/node constraints (which would return wrong matches). This
        // mirrors Neo4j, which rejects a multi-relationship shortestPath pattern.
        if elements.len() > 3 {
            return Err(anyhow!(
                "shortestPath supports a single relationship pattern \
                 (a)-[*]->(b); a multi-hop pattern with intermediate nodes is \
                 not supported"
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
        // An inline property map on the relationship is a filter, and it must
        // gate expansion rather than the result set: the shortest path among
        // permitted edges is not the unconstrained shortest path filtered.
        // `properties_to_expr` only uses the variable name as the expression's
        // prefix, and the predicate is consumed by a storage scan rather than a
        // projected column, so the `"__anon_edge"` sentinel used by the
        // variable-length path (see `plan_traverse_with_source`) is sufficient
        // when the relationship binds no variable of its own. Issue #166.
        let step_var = rel.variable.clone().filter(|v| !v.is_empty());
        let edge_filter_expr = {
            let edge_var = step_var
                .clone()
                .unwrap_or_else(|| "__anon_edge".to_string());
            self.properties_to_expr(&edge_var, &rel.properties)
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
                optional_variables: HashSet::new(),
            };
        }

        // Plan target node if not bound
        let target_label_id = if !target_bound {
            // Use first label for target_label_id
            let target_label_name = target_node
                .labels
                .first()
                .ok_or_else(|| anyhow!("Target node must have label if not already bound"))?;
            // Native lookup first; then consult `CatalogProvider` /
            // `ReplacementScanProvider` and allocate a virtual label-id
            // (M5b follow-up #6). Virtual ids dispatch to
            // `CatalogVertexScanExec` at physical-plan time.
            let target_label_id =
                if let Some(meta) = self.schema.get_label_case_insensitive(target_label_name) {
                    meta.id
                } else if let Some((vid, _)) = self.allocate_virtual_label(target_label_name)? {
                    vid
                } else {
                    return Err(anyhow!("Label {} not found", target_label_name));
                };

            let target_scan = LogicalPlan::Scan {
                label_id: target_label_id,
                labels: target_node.labels.names().to_vec(),
                variable: target_var.clone(),
                filter: self.properties_to_expr(&target_var, &target_node.properties),
                optional: false,
            };

            plan = Self::join_with_plan(plan, target_scan);
            target_label_id
        } else {
            if let Some(prop_filter) = self.properties_to_expr(&target_var, &target_node.properties)
            {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: prop_filter,
                    optional_variables: HashSet::new(),
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
                let id = if let Some(meta) = self.schema.edge_types.get(type_name) {
                    meta.id
                } else if let Some((vid, _)) = self.allocate_virtual_edge_type(type_name)? {
                    vid
                } else {
                    return Err(anyhow!("Edge type {} not found", type_name));
                };
                ids.push(id);
            }
            ids
        };

        // Extract hop constraints from relationship pattern
        let min_hops = rel.range.as_ref().and_then(|r| r.min).unwrap_or(1);
        let max_hops = rel.range.as_ref().and_then(|r| r.max).unwrap_or(u32::MAX);
        // A lower bound above 1 would require the search to keep going past the
        // first sighting of the target, and to relax the visited-set semantics
        // that make the walk terminate. Refuse it rather than return a path
        // that quietly violates the bound the user wrote.
        if min_hops > 1 {
            return Err(anyhow!(
                "shortestPath does not support a minimum hop count above 1 \
                 (got *{min_hops}..); it always returns the shortest match, so \
                 a higher lower bound would need a different search"
            ));
        }

        let sp_plan = match mode {
            ShortestPathMode::Shortest => LogicalPlan::ShortestPath {
                input: Box::new(plan),
                edge_type_ids,
                direction: rel.direction.clone(),
                source_variable: source_var.clone(),
                target_variable: target_var.clone(),
                target_label_id,
                path_variable: path_var.clone(),
                step_variable: step_var.clone(),
                min_hops,
                max_hops,
                edge_filter_expr: edge_filter_expr.clone(),
            },
            ShortestPathMode::AllShortest => LogicalPlan::AllShortestPaths {
                input: Box::new(plan),
                edge_type_ids,
                direction: rel.direction.clone(),
                source_variable: source_var.clone(),
                target_variable: target_var.clone(),
                target_label_id,
                path_variable: path_var.clone(),
                step_variable: step_var.clone(),
                min_hops,
                max_hops,
                edge_filter_expr: edge_filter_expr.clone(),
            },
        };

        if !source_bound {
            add_var_to_scope(vars_in_scope, &source_var, VariableType::Node)?;
        }
        if !target_bound {
            add_var_to_scope(vars_in_scope, &target_var, VariableType::Node)?;
        }
        add_var_to_scope(vars_in_scope, &path_var, VariableType::Path)?;
        if let Some(sv) = &step_var {
            // Variable-length by construction, so always a list.
            add_var_to_scope(vars_in_scope, sv, VariableType::EdgeList)?;
        }

        Ok(sp_plan)
    }
    /// Plan a MATCH pattern into a LogicalPlan (Scan → Traverse chains).
    ///
    /// This is a public entry point for the Locy plan builder to reuse the
    /// existing pattern-planning logic for clause bodies.
    pub fn plan_pattern(
        &self,
        pattern: &Pattern,
        initial_vars: &[VariableInfo],
    ) -> Result<LogicalPlan> {
        let mut vars_in_scope: Vec<VariableInfo> = initial_vars.to_vec();
        let vars_before_pattern = vars_in_scope.len();
        let mut plan = LogicalPlan::Empty;
        for path in &pattern.paths {
            plan = self.plan_path(path, plan, &mut vars_in_scope, false, vars_before_pattern)?;
        }
        Ok(plan)
    }

    /// A path rewritten to start at its bound end, or `None` to plan as written.
    ///
    /// [`Self::plan_path`] walks elements left to right and scans the first node
    /// when it is unbound. If the *last* node is the bound one, that scan is a
    /// full `ScanAll` cross-joined against the incoming rows, and the binding is
    /// only reapplied as a filter above the traversal — where
    /// `try_plan_cross_join_as_hash_join` can no longer recover it. Measured at
    /// SF1: `(forum)-[:CONTAINER_OF]->(post)` 349 ms against
    /// `(post)<-[:CONTAINER_OF]-(forum)` not finishing, for the same rows.
    ///
    /// Reversing is semantics-preserving because `source_variable` names the
    /// traversal *start*, not the arrow's tail: `endpoints_for_direction`
    /// resolves `(source = a, Incoming)` and `(source = b, Outgoing)` to the
    /// same start and end, so `startNode`/`endNode` are unaffected. `Incoming`
    /// is not a slow path either — the CSR is keyed by `(edge_type, Direction)`
    /// with separate `fwd`/`bwd` datasets.
    ///
    /// Deliberately narrow. A bound node in the *middle* is a join-ordering
    /// problem that reversal does not solve, and paths carrying a path variable,
    /// a quantified segment, or a shortestPath mode are left alone rather than
    /// reversed with their accompanying machinery.
    fn reversed_for_bound_anchor(
        path: &PathPattern,
        vars_in_scope: &[VariableInfo],
    ) -> Option<PathPattern> {
        // A path variable binds nodes and edges in traversal order, so a
        // reversed plan would bind `p` backwards. Out of scope here.
        if path.variable.is_some() || path.shortest_path_mode.is_some() {
            return None;
        }
        // Fewer than three elements is a bare node: nothing to reorder.
        if path.elements.len() < 3 {
            return None;
        }
        // Quantified segments carry per-step directions of their own.
        if path
            .elements
            .iter()
            .any(|e| matches!(e, PatternElement::Parenthesized { .. }))
        {
            return None;
        }

        let bound_node = |element: &PatternElement| match element {
            PatternElement::Node(n) => n
                .variable
                .as_deref()
                .is_some_and(|v| !v.is_empty() && is_var_in_scope(vars_in_scope, v)),
            _ => false,
        };

        // Only the both-ends case is decidable here: reversal helps exactly when
        // the far end is bound and the near end is not.
        if bound_node(path.elements.first()?) || !bound_node(path.elements.last()?) {
            return None;
        }

        let mut elements: Vec<PatternElement> = path.elements.iter().rev().cloned().collect();
        for element in &mut elements {
            if let PatternElement::Relationship(rel) = element {
                rel.direction = match rel.direction {
                    Direction::Outgoing => Direction::Incoming,
                    Direction::Incoming => Direction::Outgoing,
                    Direction::Both => Direction::Both,
                };
            }
        }

        Some(PathPattern {
            variable: None,
            elements,
            shortest_path_mode: None,
        })
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
        // Start the walk at the bound end when the pattern was written from the
        // unbound one; see `reversed_for_bound_anchor`.
        let reversed_storage;
        let path = match Self::reversed_for_bound_anchor(path, vars_in_scope) {
            Some(reversed) => {
                reversed_storage = reversed;
                &reversed_storage
            }
            None => path,
        };

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
        let mut optional_pattern_vars: HashSet<String> = if optional {
            let mut vars = HashSet::new();
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
            HashSet::new()
        };

        // Pre-scan path elements for bound edge variables from previous MATCH clauses.
        // These must participate in Trail mode (relationship uniqueness) enforcement
        // across ALL segments in this path, so that VLP segments like [*0..1] don't
        // traverse through edges already claimed by a bound relationship [r].
        let path_bound_edge_vars: HashSet<String> = {
            let mut bound = HashSet::new();
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
                    if optional && !is_bound {
                        optional_pattern_vars.insert(variable.clone());
                    }

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
                                optional_variables: HashSet::new(),
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
                                    let target_was_bound =
                                        n_target.variable.as_ref().is_some_and(|v| {
                                            !v.is_empty() && is_var_in_scope(vars_in_scope, v)
                                        });
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
                                    if optional && !target_was_bound {
                                        optional_pattern_vars.insert(target_var.clone());
                                    }

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
                    // Use the outer target for filters and labels; the inner
                    // target's label is also used for NFA state constraints.
                    let target_node = outer_target_node.unwrap_or(inner_target_node);

                    // GQL grouping follows *quantification*, not parentheses: a
                    // parenthesized pattern with no quantifier binds ordinary
                    // singletons, exactly as it does today.
                    let is_quantified = range.is_some();

                    // The inner variables of a quantified pattern are group
                    // variables, so they never name an endpoint. When no outer
                    // node is adjacent, the endpoint gets an anonymous column —
                    // the inner node's labels and property map still constrain
                    // it, only its *name* stops leaking out. Issue: GQL says
                    // `((a)-[:E]->(b)){2} RETURN a.id` is a type error, not the
                    // start node.
                    let binds_group_vars = is_quantified;

                    // For simple 3-element single-hop QPP without intermediate label constraints,
                    // fall back to existing VLP behavior (copy range to relationship).
                    // Delegating to the ordinary VLP path binds the inner node
                    // and relationship as *singletons*, so it is only valid
                    // when nothing inside the pattern is named — otherwise a
                    // one-hop quantified pattern would silently disagree with
                    // a two-hop one about what its inner variables mean.
                    let inner_is_anonymous = !binds_group_vars
                        || (source_node.variable.as_ref().is_none_or(|v| v.is_empty())
                            && qpp_rels.iter().all(|(rel, node)| {
                                rel.variable.as_ref().is_none_or(|v| v.is_empty())
                                    && node.variable.as_ref().is_none_or(|v| v.is_empty())
                            }));
                    let use_simple_vlp = qpp_rels.len() == 1
                        && inner_is_anonymous
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
                                optional_variables: HashSet::new(),
                            };
                        }
                        outer_src.clone()
                    } else {
                        let sv = source_node
                            .variable
                            .clone()
                            .filter(|v| !v.is_empty() && !binds_group_vars)
                            .unwrap_or_else(|| self.next_anon_var());

                        if is_var_in_scope(vars_in_scope, &sv) {
                            // Source is already bound, apply property filter if needed
                            if let Some(prop_filter) =
                                self.properties_to_expr(&sv, &source_node.properties)
                            {
                                plan = LogicalPlan::Filter {
                                    input: Box::new(plan),
                                    predicate: prop_filter,
                                    optional_variables: HashSet::new(),
                                };
                            }
                        } else {
                            // Source is unbound, scan it
                            plan = self.plan_unbound_node(source_node, &sv, plan, optional)?;
                            add_var_to_scope(vars_in_scope, &sv, VariableType::Node)?;
                            if optional {
                                optional_pattern_vars.insert(sv.clone());
                            }
                        }
                        sv
                    };

                    if use_simple_vlp {
                        // Simple single-hop QPP: apply range to relationship and use VLP path
                        let mut relationship = qpp_rels[0].0.clone();
                        relationship.range = range.clone();

                        let target_was_bound = target_node
                            .variable
                            .as_ref()
                            .is_some_and(|v| !v.is_empty() && is_var_in_scope(vars_in_scope, v));
                        let (new_plan, target_var, _effective_target) = self
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
                        if optional && !target_was_bound {
                            optional_pattern_vars.insert(target_var);
                        }
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
                                // A quantified pattern is planned against edge
                                // type *ids*; there is no schemaless main-table
                                // equivalent of the QPP operator. Leaving the
                                // list empty would make the traversal match
                                // nothing — a planner limitation reported as an
                                // empty result, which is exactly the silent
                                // failure worth refusing instead.
                                if step_edge_type_ids.is_empty() {
                                    return Err(anyhow!(
                                        "Quantified path patterns require schema-declared \
                                         relationship types, but {} {} not declared. Declare the \
                                         type, or write the sub-pattern without inner variable \
                                         names so it can be planned as a variable-length pattern.",
                                        rel.types
                                            .iter()
                                            .map(|t| format!("'{t}'"))
                                            .collect::<Vec<_>>()
                                            .join(", "),
                                        if rel.types.len() == 1 { "is" } else { "are" }
                                    ));
                                }
                            }
                            all_edge_type_ids.extend_from_slice(&step_edge_type_ids);

                            let target_label = node.labels.first().and_then(|l| {
                                self.schema.get_label_case_insensitive(l).map(|_| l.clone())
                            });

                            // Both maps are filters and both were dropped: the
                            // relationship's was refused outright, the target
                            // node's was silently ignored because only
                            // `labels.first()` was ever read. Issue #166 family.
                            let edge_filter_expr = {
                                let edge_var = rel
                                    .variable
                                    .clone()
                                    .unwrap_or_else(|| "__anon_edge".to_string());
                                self.properties_to_expr(&edge_var, &rel.properties)
                            };
                            let target_property_expr = {
                                let node_var = node
                                    .variable
                                    .clone()
                                    .filter(|v| !v.is_empty())
                                    .unwrap_or_else(|| "__anon_node".to_string());
                                self.properties_to_expr(&node_var, &node.properties)
                            };

                            qpp_step_infos.push(QppStepInfo {
                                edge_type_ids: step_edge_type_ids,
                                direction: rel.direction.clone(),
                                target_label,
                                edge_filter_expr,
                                target_property_expr,
                                edge_variable: rel.variable.clone().filter(|v| !v.is_empty()),
                                target_variable: node.variable.clone().filter(|v| !v.is_empty()),
                            });
                        }

                        // Deduplicate edge type IDs for adjacency warming
                        all_edge_type_ids.sort_unstable();
                        all_edge_type_ids.dedup();

                        let inner_source_var = binds_group_vars
                            .then(|| source_node.variable.clone())
                            .flatten()
                            .filter(|v| !v.is_empty());
                        let group_bindings = if binds_group_vars {
                            let step_names: Vec<(Option<String>, Option<String>)> = qpp_step_infos
                                .iter()
                                .map(|s| (s.edge_variable.clone(), s.target_variable.clone()))
                                .collect();
                            crate::query::df_graph::nfa::qpp_group_bindings(
                                inner_source_var.as_deref(),
                                &step_names,
                            )
                        } else {
                            Vec::new()
                        };

                        // Two positions sharing a name would need two columns
                        // with one name, which is unrepresentable — and under
                        // GQL they are distinct group variables anyway.
                        let mut seen_group_names: HashSet<&str> = HashSet::new();
                        for b in &group_bindings {
                            if !seen_group_names.insert(b.name.as_str()) {
                                return Err(anyhow!(
                                    "SyntaxError: VariableTypeConflict - Variable '{}' is bound at \
                                     more than one position inside the same quantified pattern; \
                                     each position is a separate group variable and needs its own name",
                                    b.name
                                ));
                            }
                            if is_var_in_scope(vars_in_scope, &b.name) {
                                return Err(anyhow!(
                                    "SyntaxError: VariableAlreadyBound - Variable '{}' is already \
                                     bound outside the quantified pattern and cannot be re-bound \
                                     as a group variable inside it",
                                    b.name
                                ));
                            }
                        }

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

                        // The endpoint's name comes from the adjacent *outer*
                        // node. Falling back to the inner node's name is only
                        // correct for an unquantified pattern; under a
                        // quantifier that name belongs to a group variable.
                        let target_variable = outer_target_node
                            .and_then(|n| n.variable.clone())
                            .filter(|v| !v.is_empty())
                            .or_else(|| {
                                (!binds_group_vars)
                                    .then(|| inner_target_node.variable.clone())
                                    .flatten()
                                    .filter(|v| !v.is_empty())
                            })
                            .unwrap_or_else(|| self.next_anon_var());

                        let target_is_bound = is_var_in_scope(vars_in_scope, &target_variable);

                        // Determine target label for the final node
                        let target_label_meta = target_node
                            .labels
                            .first()
                            .and_then(|l| self.schema.get_label_case_insensitive(l));

                        // Collect scope match variables
                        let mut scope_match_variables: HashSet<String> = vars_in_scope
                            [vars_before_pattern..]
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
                            edge_properties: HashSet::new(),
                            is_variable_length: true,
                            optional_pattern_vars: optional_pattern_vars.clone(),
                            scope_match_variables,
                            edge_filter_expr: None,
                            path_mode: crate::query::df_graph::nfa::PathMode::Trail,
                            qpp_steps: Some(qpp_step_infos),
                            qpp_inner_source: inner_source_var.clone(),
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
                                    HashSet::new()
                                },
                            };
                        }

                        // Add target variable to scope
                        if !target_is_bound {
                            add_var_to_scope(vars_in_scope, &target_variable, VariableType::Node)?;
                        }

                        // Register the inner variables as GQL group variables:
                        // each holds one element per iteration of the quantifier.
                        for b in &group_bindings {
                            let ty = match b.kind {
                                crate::query::df_graph::nfa::QppGroupKind::Node => {
                                    VariableType::NodeList
                                }
                                crate::query::df_graph::nfa::QppGroupKind::Edge => {
                                    VariableType::EdgeList
                                }
                            };
                            add_var_to_scope(vars_in_scope, &b.name, ty)?;
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
                        // This QPP consumed the following outer node as its target.
                        // A subsequent consecutive QPP must anchor at THAT node, not
                        // the stale earlier source — so advance last_outer_node_var
                        // to the consumed target's variable (only the Node arm did
                        // this before, so `(a)(qpp1)(b)(qpp2)(c)` wrongly anchored
                        // qpp2 at `a`).
                        if let Some(v) = target_node.variable.as_ref().filter(|v| !v.is_empty()) {
                            last_outer_node_var = Some(v.clone());
                        }
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
        path_bound_edge_vars: &HashSet<String>,
    ) -> Result<(LogicalPlan, String, String)> {
        // Check for parameter used as relationship predicate
        if let Some(Expr::Parameter(_)) = &params.rel.properties {
            return Err(anyhow!(
                "SyntaxError: InvalidParameterUse - Parameters cannot be used as relationship predicates"
            ));
        }

        let mut edge_type_ids = Vec::new();
        let mut dst_labels = Vec::new();
        // Endpoint-label inference for an *unlabelled* target has to know which
        // side of the edge the traversal actually lands on, so both sides are
        // collected. Using `dst_labels` regardless of direction constrained an
        // incoming traversal to the label it started from and silently returned
        // no rows.
        let mut src_labels = Vec::new();
        let mut unknown_types = Vec::new();

        if params.rel.types.is_empty() {
            // All types - include both schema and schemaless edge types
            // This ensures MATCH (a)-[r]->(b) finds edges even when no schema is defined
            edge_type_ids = self.schema.all_edge_type_ids();
            for meta in self.schema.edge_types.values() {
                dst_labels.extend(meta.dst_labels.iter().cloned());
                src_labels.extend(meta.src_labels.iter().cloned());
            }
        } else {
            for type_name in &params.rel.types {
                if let Some(edge_meta) = self.schema.edge_types.get(type_name) {
                    // Known type - use standard Traverse with type_id
                    edge_type_ids.push(edge_meta.id);
                    dst_labels.extend(edge_meta.dst_labels.iter().cloned());
                    src_labels.extend(edge_meta.src_labels.iter().cloned());
                } else if let Some((vid, _)) = self.allocate_virtual_edge_type(type_name)? {
                    // M5b.3: virtual edge type (plugin-registered CatalogTable).
                    // Resolving it into `edge_type_ids` (not `unknown_types`)
                    // lets the regular `Traverse` planner build a structured
                    // plan that the physical planner can dispatch to a
                    // `CatalogEdgeScanExec` mid-pattern.
                    edge_type_ids.push(vid);
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
            let step_var = params.rel.variable.clone().or_else(|| {
                // An anonymous relationship carrying an inline property map still
                // needs a name. `properties_to_expr` builds `var.prop = value`, and
                // the physical planner materializes edge property columns keyed by
                // the step variable — so with no name there is nothing to filter on
                // and nothing to filter with. Nodes already synthesize an anonymous
                // variable for exactly this reason (see `target_variable` above);
                // relationships did not, so the map was parsed and then silently
                // discarded and `-[:E {k: v}]->` matched every E edge. Issue #166.
                // Variable-length patterns are excluded: they carry the predicate
                // inline via `edge_filter_expr`, where the name is stripped anyway.
                (params.rel.properties.is_some() && params.rel.range.is_none())
                    .then(|| self.next_anon_var())
            });
            let path_var = params.path_variable.clone();

            // Compute scope_match_variables for relationship uniqueness scoping.
            let mut scope_match_variables: HashSet<String> = vars_in_scope[vars_before_pattern..]
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
                    HashSet::new()
                };
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: edge_prop_filter,
                    optional_variables: filter_optional_vars,
                };
            }

            // Add the bound variables to scope
            if let Some(sv) = &step_var {
                add_var_to_scope(vars_in_scope, sv, edge_binding_type(is_variable_length))?;
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

        // Resolve target label to either a schema id or a virtual id from the
        // plugin registry. Mid-pattern virtual-label dispatch (M5b.3) requires
        // the virtual id to flow into `Traverse.target_label_id` so the
        // physical planner can layer a `CatalogVertexScanExec` join on the
        // traverse output. Mirrors the schema-then-virtual fallthrough used
        // by single-vertex `Scan` planning (~`plan_node_pattern` below).
        let mut virtual_target_label_id: Option<u16> = None;
        let target_label_meta = if let Some(label_name) = params.target_node.labels.first() {
            // Use first label for target_label_id
            // For schemaless support, allow unknown target labels
            match self.schema.get_label_case_insensitive(label_name) {
                Some(meta) => Some(meta),
                None => {
                    if let Some((vid, _)) = self.allocate_virtual_label(label_name)? {
                        virtual_target_label_id = Some(vid);
                    }
                    None
                }
            }
        } else if !target_is_bound {
            // Infer the unlabelled target's label from the edge type(s) — but
            // from the side the traversal actually *lands on*, which depends on
            // direction:
            //
            //   (a:A)-[:R]->(x)   x is on the destination side  -> dst_labels
            //   (a:A)<-[:R]-(x)   x is on the source side       -> src_labels
            //   (a:A)-[:R]-(x)    either side                   -> no constraint
            //
            // Using `dst_labels` unconditionally constrained an incoming
            // traversal to the label it had just come *from*, so
            // `MATCH (b:B)<-[:R]-()` matched nothing at all while
            // `MATCH (b:B)<-[:R]-(a:A)` returned the right rows — a silent wrong
            // answer, with the data untouched on disk.
            //
            // The single-label guard below is why this hid for so long: an edge
            // type with two or more labels on the inferred side already fell
            // through to "allow any target" and behaved correctly.
            let candidates = match params.rel.direction {
                Direction::Outgoing => Some(dst_labels),
                Direction::Incoming => Some(src_labels),
                // Undirected reaches both sides. Constraining to either would
                // drop the other half, so leave the target unconstrained; a
                // `WHERE x:Label` still narrows it.
                Direction::Both => None,
            };
            let unique: Vec<_> = candidates
                .unwrap_or_default()
                .into_iter()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            if unique.len() == 1 {
                let label_name = &unique[0];
                self.schema.get_label_case_insensitive(label_name)
            } else {
                // Multiple or no labels inferred - allow any target.
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
        let step_var = params.rel.variable.clone().or_else(|| {
            // An anonymous relationship carrying an inline property map still
            // needs a name. `properties_to_expr` builds `var.prop = value`, and
            // the physical planner materializes edge property columns keyed by
            // the step variable — so with no name there is nothing to filter on
            // and nothing to filter with. Nodes already synthesize an anonymous
            // variable for exactly this reason (see `target_variable` above);
            // relationships did not, so the map was parsed and then silently
            // discarded and `-[:E {k: v}]->` matched every E edge. Issue #166.
            // Variable-length patterns are excluded: they carry the predicate
            // inline via `edge_filter_expr`, where the name is stripped anyway.
            (params.rel.properties.is_some() && params.rel.range.is_none())
                .then(|| self.next_anon_var())
        });
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
        let mut scope_match_variables: HashSet<String> = vars_in_scope[vars_before_pattern..]
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
            target_label_id: target_label_meta
                .map(|m| m.id)
                .or(virtual_target_label_id)
                .unwrap_or(0),
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
            edge_properties: HashSet::new(),
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
            qpp_inner_source: None,
        };

        // Pre-compute optional variables set for filter nodes in this traverse.
        // Used by relationship property filters and bound-edge filters below.
        let filter_optional_vars = if params.optional {
            params.optional_pattern_vars.clone()
        } else {
            HashSet::new()
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
            add_var_to_scope(vars_in_scope, sv, edge_binding_type(is_variable_length))?;
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

    /// Decide whether per-label `Scan` branches for a label disjunction can
    /// safely be combined under `LogicalPlan::Union`. Returns `true` iff every
    /// label in `labels` is registered in the schema AND every pair shares an
    /// identical property name+type set.
    ///
    /// When this returns `false`, the disjunction must fall back to a single
    /// `ScanMainByLabels` over all labels — otherwise DataFusion's
    /// `UnionExec::try_new` panics in `union_schema` because the per-label
    /// `GraphScanExec` outputs (`_vid` + `_labels` + per-label projected
    /// properties) have different field counts. Issue rustic-ai/uni-db#62.
    ///
    /// We deliberately compare full schema property sets rather than only the
    /// properties referenced by the current query: at this logical-planning
    /// stage we have not yet collected `all_properties`, and `*` wildcards
    /// (e.g. from unknown function calls) would expand per-label downstream
    /// in `df_planner::resolve_properties` even when the query text only
    /// touches common columns.
    fn label_branches_share_property_schema(&self, labels: &[String]) -> bool {
        if labels.len() < 2 {
            return true;
        }
        let mut iter = labels.iter();
        let first = iter.next().expect("len >= 2");
        let Some(first_props) = self.schema.properties.get(first) else {
            return false;
        };
        for label in iter {
            let Some(props) = self.schema.properties.get(label) else {
                return false;
            };
            if props.len() != first_props.len() {
                return false;
            }
            for (name, meta) in first_props {
                let Some(other_meta) = props.get(name) else {
                    return false;
                };
                if meta.r#type != other_meta.r#type {
                    return false;
                }
            }
        }
        true
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
                    optional_variables: HashSet::new(),
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

        // Label disjunction `(n:A|B|C)` — emit Union of label-scoped Scans.
        //
        // Storage fact: a multi-labeled vertex is fanned out into every
        // per-label table it carries (uni-store/src/runtime/writer.rs's
        // `push_vertex_to_labels`), so the same vid can appear in both the
        // `A` scan and the `B` scan of a disjunctive query. Use
        // `Union { all: false }` so the combined result deduplicates by row
        // contents (which include the vid) rather than emitting the same
        // vertex twice. The single-label-disjunction case (`Disjunction(["A"])`)
        // is encoded the same way the parser already encodes single edge
        // types, and reduces to one Scan with no Union wrapping.
        if node.labels.is_proper_disjunction() {
            let label_names: Vec<String> = node.labels.names().to_vec();

            // Per-label branches under a `Union` only line up when every
            // branch produces the same Arrow schema. The narrow-scan
            // `Scan` path resolves columns *per label*, so heterogeneous
            // property sets (or any schemaless label in the mix) yield
            // mismatched widths and DataFusion's `UnionExec::try_new`
            // panics inside `union_schema` (issue rustic-ai/uni-db#62).
            //
            // For those cases, lower every branch to a *single-label*
            // `ScanMainByLabels` instead. The schemaless main-table scan
            // resolves columns from `all_properties` directly (no per-label
            // expansion), so all branches emit a uniform schema and the
            // outer `Union { all: false }` deduplicates correctly. We
            // keep the per-branch Union shape (rather than collapsing to
            // a single multi-label scan) because multi-label
            // `ScanMainByLabels` has AND/intersection semantics — wrong
            // for a disjunction.
            let use_main_table_branches = !self.label_branches_share_property_schema(&label_names);

            let mut branches: Vec<LogicalPlan> = Vec::with_capacity(label_names.len());
            for label_name in &label_names {
                let branch = if use_main_table_branches {
                    LogicalPlan::ScanMainByLabels {
                        labels: vec![label_name.clone()],
                        variable: variable.to_string(),
                        filter: node_scan_filter.clone(),
                        optional,
                    }
                } else {
                    let meta = self
                        .schema
                        .get_label_case_insensitive(label_name)
                        .expect("share_property_schema true implies all labels in schema");
                    LogicalPlan::Scan {
                        label_id: meta.id,
                        labels: vec![label_name.clone()],
                        variable: variable.to_string(),
                        filter: node_scan_filter.clone(),
                        optional,
                    }
                };
                branches.push(branch);
            }
            // Left-leaning Union: Union(Union(A, B), C). All inner
            // unions dedupe by row, so the outer one does too.
            let mut iter = branches.into_iter();
            let mut union_plan = iter
                .next()
                .expect("is_proper_disjunction implies at least 2 labels");
            for next in iter {
                union_plan = LogicalPlan::Union {
                    left: Box::new(union_plan),
                    right: Box::new(next),
                    all: false,
                };
            }
            let joined = Self::join_with_plan(plan, union_plan);
            return Ok(apply_residual_filter(joined, node_residual_filter));
        }

        // Use first label for label_id (primary label for dataset selection)
        let label_name = &node.labels[0];

        // Check if label exists in schema
        if let Some(label_meta) = self.schema.get_label_case_insensitive(label_name) {
            // Known label: use standard Scan
            let scan = LogicalPlan::Scan {
                label_id: label_meta.id,
                labels: node.labels.names().to_vec(),
                variable: variable.to_string(),
                filter: node_scan_filter,
                optional,
            };

            let joined = Self::join_with_plan(plan, scan);
            Ok(apply_residual_filter(joined, node_residual_filter))
        } else {
            // Unknown label. Try a CatalogProvider / ReplacementScanProvider
            // claim first: on success allocate a virtual label-ID and emit a
            // regular `Scan` against the virtual id (`df_planner` dispatches
            // to `CatalogVertexScanExec`). When no provider claims and the
            // replacement-scan gate is on, strict-mode errors. When the gate
            // is off and no provider claims, preserve today's silent-empty
            // schemaless `ScanMainByLabels` behavior bit-for-bit.
            if let Some((virtual_id, _)) = self.allocate_virtual_label(label_name)? {
                let scan = LogicalPlan::Scan {
                    label_id: virtual_id,
                    labels: node.labels.names().to_vec(),
                    variable: variable.to_string(),
                    filter: node_scan_filter,
                    optional,
                };
                let joined = Self::join_with_plan(plan, scan);
                return Ok(apply_residual_filter(joined, node_residual_filter));
            }
            if self.replacement_scans_enabled {
                return Err(anyhow!(
                    "Label `{}` is not defined in schema and no \
                     CatalogProvider or ReplacementScanProvider claimed it; \
                     strict-mode (replacement_scans=true) requires the label \
                     to resolve",
                    label_name
                ));
            }

            let scan_main = LogicalPlan::ScanMainByLabels {
                labels: node.labels.names().to_vec(),
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
        optional_vars: HashSet<String>,
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

        // Rewrite id(var) to var._vid (or var._eid for an edge) so
        // PredicateAnalyzer can push it down.
        let transformed_predicate = Self::rewrite_id_to_vid(transformed_predicate, vars_in_scope);

        let mut current_predicate =
            self.rewrite_predicates_using_indexes(&transformed_predicate, &plan, vars_in_scope)?;

        // 1. Try to extract vector_similarity predicate for optimization
        if let Some(extraction) = extract_vector_similarity(&current_predicate) {
            let vs = &extraction.predicate;
            // Only consume the vector predicate if the KNN rewriter can actually
            // reach the Scan; otherwise leave it in `current_predicate` so it
            // becomes a residual Filter instead of being silently dropped.
            if Self::rewrite_target_reachable(&plan, &vs.variable, RewriteTarget::Knn) {
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

        // 2. Label/type disjunction → narrow-scan rewrite.
        //
        // `WHERE n:A OR n:B` and `WHERE type(r) = 'A' OR type(r) = 'B'`
        // are functionally identical to the inline forms `(n:A|B)` and
        // `[r:A|B]`, but a literal pattern lowering would route them
        // through `Filter(LabelCheck OR LabelCheck)` over `ScanAll` —
        // a full vertex/edge scan plus residual filter, missing the
        // narrow-scan fast-path that the inline forms get for free.
        // Detect those OR-chains here and rewrite the upstream
        // `ScanAll` / `Traverse` accordingly.
        let conjuncts = Self::split_and_conjuncts(&current_predicate);
        let mut keep: Vec<Expr> = Vec::with_capacity(conjuncts.len());
        for conj in conjuncts {
            let mut consumed = false;
            for var in vars_in_scope {
                if optional_vars.contains(&var.name) {
                    continue;
                }
                // Node label disjunction → Union of label-scoped Scans.
                // Gate on reachability by the label-union rewriter (not the
                // laxer `is_scan_all_for`), so a `ScanAll` sitting under a
                // Sort/Limit/Aggregate/Apply/Union — which the rewriter can't
                // rebuild — leaves the conjunct in `keep` as a residual Filter.
                if Self::rewrite_target_reachable(&plan, &var.name, RewriteTarget::LabelUnion)
                    && let Some(labels) = try_label_or_to_union(&conj, &var.name)
                {
                    plan = self.replace_scan_all_with_label_union(plan, &var.name, &labels, false);
                    consumed = true;
                    break;
                }
                // Edge type disjunction → merge into Traverse.edge_type_ids.
                if let Some(types) = try_type_or_to_union(&conj, &var.name)
                    && Self::merge_traverse_types_for(&plan, &var.name, &types).is_some()
                {
                    let mut ids: Vec<u32> = Vec::with_capacity(types.len());
                    let mut all_known = true;
                    for t in &types {
                        match self.schema.edge_types.get(t) {
                            Some(meta) => ids.push(meta.id),
                            None => {
                                all_known = false;
                                break;
                            }
                        }
                    }
                    if all_known {
                        plan = Self::set_traverse_edge_type_ids(plan, &var.name, ids);
                        consumed = true;
                        break;
                    }
                }
            }
            if !consumed {
                keep.push(conj);
            }
        }
        current_predicate = Self::combine_predicates(keep).unwrap_or(Expr::TRUE);

        // 3. Push eligible predicates to Scan OR Traverse filters
        // Note: Do NOT push predicates on optional variables (from OPTIONAL MATCH) to
        // Traverse's target_filter, because target_filter filtering doesn't preserve NULL
        // rows. Let them stay in the Filter operator which handles NULL preservation.
        for var in vars_in_scope {
            // Skip pushdown for optional variables - they need NULL preservation in Filter
            if optional_vars.contains(&var.name) {
                continue;
            }

            // Check if var is produced by a Scan the pushdown rewriter can reach.
            // Gate on `rewrite_target_reachable` (not the laxer
            // `find_scan_label_id`), so a scan under Sort/Limit/Aggregate/Apply —
            // which `push_predicate_to_scan` can't rebuild — keeps its predicate
            // in `current_predicate` as a residual Filter instead of dropping it.
            if Self::rewrite_target_reachable(&plan, &var.name, RewriteTarget::Scan) {
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
            } else if Self::rewrite_target_reachable(
                &plan,
                &var.name,
                RewriteTarget::TraverseTarget,
            ) {
                // Push to Traverse (same reachability discipline).
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

    /// Returns `true` iff `variable` is bound to a `ScanAll` operator
    /// (somewhere under `plan`). Used to gate the
    /// `WHERE n:A OR n:B` → `Union(Scan{A}, Scan{B})` rewrite — we only
    /// fire it when the variable is currently doing a full vertex scan,
    /// not when it's already bound to a labeled `Scan`.
    fn is_scan_all_for(plan: &LogicalPlan, variable: &str) -> bool {
        match plan {
            LogicalPlan::ScanAll { variable: var, .. } => var == variable,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Apply { input, .. }
            | LogicalPlan::Traverse { input, .. } => Self::is_scan_all_for(input, variable),
            LogicalPlan::CrossJoin { left, right } => {
                Self::is_scan_all_for(left, variable) || Self::is_scan_all_for(right, variable)
            }
            LogicalPlan::Union { left, right, .. } => {
                Self::is_scan_all_for(left, variable) || Self::is_scan_all_for(right, variable)
            }
            _ => false,
        }
    }

    /// Replace the `ScanAll` for `variable` in `plan` with a left-leaning
    /// `Union` of label-scoped `Scan` (or `ScanMainByLabels` for unknown
    /// labels) operators built from `labels`. Used by the
    /// `WHERE n:A OR n:B` rewrite.
    fn replace_scan_all_with_label_union(
        &self,
        plan: LogicalPlan,
        variable: &str,
        labels: &[String],
        optional: bool,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::ScanAll {
                variable: var,
                filter,
                optional: scan_optional,
            } if var == variable => {
                // Heterogeneous (or any-schemaless) disjunction: route every
                // branch through a single-label `ScanMainByLabels` so all
                // branches emit a uniform schemaless schema. Avoids the
                // DataFusion `union_schema` panic. See `plan_unbound_node`
                // and issue rustic-ai/uni-db#62.
                let use_main_table_branches = !self.label_branches_share_property_schema(labels);

                let mut branches: Vec<LogicalPlan> = Vec::with_capacity(labels.len());
                for label in labels {
                    let branch = if use_main_table_branches {
                        LogicalPlan::ScanMainByLabels {
                            labels: vec![label.clone()],
                            variable: variable.to_string(),
                            filter: filter.clone(),
                            optional: scan_optional || optional,
                        }
                    } else {
                        let meta = self
                            .schema
                            .get_label_case_insensitive(label)
                            .expect("share_property_schema true implies all labels in schema");
                        LogicalPlan::Scan {
                            label_id: meta.id,
                            labels: vec![label.clone()],
                            variable: variable.to_string(),
                            filter: filter.clone(),
                            optional: scan_optional || optional,
                        }
                    };
                    branches.push(branch);
                }
                let mut iter = branches.into_iter();
                let mut union_plan = iter.next().expect("at least one label");
                for next in iter {
                    union_plan = LogicalPlan::Union {
                        left: Box::new(union_plan),
                        right: Box::new(next),
                        all: false,
                    };
                }
                union_plan
            }
            LogicalPlan::Filter {
                input,
                predicate,
                optional_variables,
            } => LogicalPlan::Filter {
                input: Box::new(
                    self.replace_scan_all_with_label_union(*input, variable, labels, optional),
                ),
                predicate,
                optional_variables,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(
                    self.replace_scan_all_with_label_union(*input, variable, labels, optional),
                ),
                projections,
            },
            LogicalPlan::CrossJoin { left, right } => {
                if Self::is_scan_all_for(&left, variable) {
                    LogicalPlan::CrossJoin {
                        left: Box::new(
                            self.replace_scan_all_with_label_union(
                                *left, variable, labels, optional,
                            ),
                        ),
                        right,
                    }
                } else {
                    LogicalPlan::CrossJoin {
                        left,
                        right: Box::new(
                            self.replace_scan_all_with_label_union(
                                *right, variable, labels, optional,
                            ),
                        ),
                    }
                }
            }
            other @ LogicalPlan::Traverse { .. } => other.map_input(|child| {
                self.replace_scan_all_with_label_union(child, variable, labels, optional)
            }),
            other => other,
        }
    }

    /// Returns `Some(())` iff `variable` is the `step_variable` (i.e. the
    /// edge variable) of some `Traverse` operator in `plan`. Used to gate
    /// the `WHERE type(r) = 'A' OR type(r) = 'B'` rewrite — we need a
    /// Traverse whose types we can merge into.
    fn merge_traverse_types_for(
        plan: &LogicalPlan,
        edge_var: &str,
        _types: &[String],
    ) -> Option<()> {
        match plan {
            LogicalPlan::Traverse {
                step_variable,
                input,
                ..
            } => {
                if step_variable.as_deref() == Some(edge_var) {
                    Some(())
                } else {
                    Self::merge_traverse_types_for(input, edge_var, _types)
                }
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Apply { input, .. } => {
                Self::merge_traverse_types_for(input, edge_var, _types)
            }
            LogicalPlan::CrossJoin { left, right } | LogicalPlan::Union { left, right, .. } => {
                Self::merge_traverse_types_for(left, edge_var, _types)
                    .or_else(|| Self::merge_traverse_types_for(right, edge_var, _types))
            }
            _ => None,
        }
    }

    /// Replace `edge_type_ids` on the Traverse whose `step_variable`
    /// equals `edge_var`. Used by the type-OR rewrite.
    fn set_traverse_edge_type_ids(
        plan: LogicalPlan,
        edge_var: &str,
        new_ids: Vec<u32>,
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
                qpp_inner_source,
            } => {
                let matches_var = step_variable.as_deref() == Some(edge_var);
                let recursed_input = if matches_var {
                    input
                } else {
                    Box::new(Self::set_traverse_edge_type_ids(
                        *input,
                        edge_var,
                        new_ids.clone(),
                    ))
                };
                LogicalPlan::Traverse {
                    input: recursed_input,
                    edge_type_ids: if matches_var { new_ids } else { edge_type_ids },
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
                    qpp_inner_source,
                }
            }
            LogicalPlan::Filter {
                input,
                predicate,
                optional_variables,
            } => LogicalPlan::Filter {
                input: Box::new(Self::set_traverse_edge_type_ids(*input, edge_var, new_ids)),
                predicate,
                optional_variables,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::set_traverse_edge_type_ids(*input, edge_var, new_ids)),
                projections,
            },
            LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
                left: Box::new(Self::set_traverse_edge_type_ids(
                    *left,
                    edge_var,
                    new_ids.clone(),
                )),
                right: Box::new(Self::set_traverse_edge_type_ids(*right, edge_var, new_ids)),
            },
            other => other,
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
                qpp_inner_source,
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
                        qpp_inner_source,
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
                        qpp_inner_source,
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
                        // See the RETURN projection above: bare pattern only.
                        if pattern_predicate_in_non_boolean_position(expr) {
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
        let projected_names: HashSet<&str> = new_vars.iter().map(|v| v.name.as_str()).collect();
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

        if has_agg {
            validate_compound_aggregates(&compound_agg_exprs, &group_by)?;
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
                optional_variables: HashSet::new(),
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
            .map(|e| {
                self.note_folded_limit_skip(e);
                parse_non_negative_integer(e, "SKIP", &self.params)
            })
            .transpose()?
            .flatten();
        let fetch = with_clause
            .limit
            .as_ref()
            .map(|e| {
                self.note_folded_limit_skip(e);
                parse_non_negative_integer(e, "LIMIT", &self.params)
            })
            .transpose()?
            .flatten();

        // SKIP/LIMIT applied to the DISTINCT result — the `Limit` node is added
        // AFTER the Distinct below, not here (openCypher applies it to the
        // deduplicated rows, not the pre-distinct ones).
        let skip_limit = (skip.is_some() || fetch.is_some()).then_some((skip, fetch));

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

        // SKIP/LIMIT last — applied to the projected, deduplicated rows.
        if let Some((skip, fetch)) = skip_limit {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                skip,
                fetch,
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
    /// Compares the target's VID against the bound variable's VID.
    fn wrap_with_bound_target_filter(plan: LogicalPlan, target_variable: &str) -> LogicalPlan {
        // Compare the traverse-discovered target's VID against the bound variable's VID.
        // Left side: Property access on the variable from current scope.
        // Right side: Variable column "{var}._vid" from traverse output (outer scope).
        // We use Variable("{var}._vid") to access the VID column from the traverse output,
        // not Property(Variable("{var}"), "_vid") because the column is already flattened.
        let bound_check = Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable(target_variable.to_string())),
                "_vid".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Variable(format!("{}._vid", target_variable))),
        };
        LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: bound_check,
            optional_variables: HashSet::new(),
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
                            optional_variables: HashSet::new(),
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
            LogicalPlan::ScanAll { variable: var, .. } if var == variable => Some(0),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Apply { input, .. } => Self::find_scan_label_id(input, variable),
            LogicalPlan::CrossJoin { left, right } => Self::find_scan_label_id(left, variable)
                .or_else(|| Self::find_scan_label_id(right, variable)),
            LogicalPlan::Traverse { input, .. } => Self::find_scan_label_id(input, variable),
            // Every other node is a non-target for scan-label lookup. This is
            // deliberately conservative: only plain `Scan`/`ScanAll` are
            // predicate-push targets, and we must NOT descend through nodes
            // that would change semantics if a predicate were pushed past them
            // (Distinct/Window/Unwind/Aggregate-output/path binders), nor
            // through leaves that bind the variable in a non-pushable form.
            // The match is exhaustive — no `_ => None` — so a new variant is
            // forced to be classified here (the #131 bug class).
            // `Scan`/`ScanAll` reach here only when their `var == variable`
            // guard above failed (a different variable) → not this scan.
            LogicalPlan::Scan { .. }
            | LogicalPlan::ScanAll { .. }
            | LogicalPlan::FusedIndexScan { .. }
            | LogicalPlan::FusedIndexScanWrapped { .. }
            | LogicalPlan::ExtIdLookup { .. }
            | LogicalPlan::ScanMainByLabels { .. }
            | LogicalPlan::VectorKnn { .. }
            | LogicalPlan::InvertedIndexLookup { .. }
            | LogicalPlan::ProcedureCall { .. }
            | LogicalPlan::TraverseMainByType { .. }
            | LogicalPlan::ShortestPath { .. }
            | LogicalPlan::AllShortestPaths { .. }
            | LogicalPlan::QuantifiedPattern { .. }
            | LogicalPlan::BindZeroLengthPath { .. }
            | LogicalPlan::BindPath { .. }
            | LogicalPlan::Unwind { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::RecursiveCTE { .. }
            | LogicalPlan::SubqueryCall { .. }
            | LogicalPlan::Create { .. }
            | LogicalPlan::CreateBatch { .. }
            | LogicalPlan::Merge { .. }
            | LogicalPlan::Set { .. }
            | LogicalPlan::Remove { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Foreach { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::LocyProgram { .. }
            | LogicalPlan::LocyFold { .. }
            | LogicalPlan::LocyBestBy { .. }
            | LogicalPlan::LocyPriority { .. }
            | LogicalPlan::LocyDerivedScan { .. }
            | LogicalPlan::LocyProject { .. }
            | LogicalPlan::LocyModelInvoke { .. }
            | LogicalPlan::Empty
            | LogicalPlan::CreateVectorIndex { .. }
            | LogicalPlan::CreateSparseIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateScalarIndex { .. }
            | LogicalPlan::CreateJsonFtsIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::ShowIndexes { .. }
            | LogicalPlan::Copy { .. }
            | LogicalPlan::Backup { .. }
            | LogicalPlan::ShowDatabase
            | LogicalPlan::ShowConfig
            | LogicalPlan::ShowStatistics
            | LogicalPlan::Vacuum
            | LogicalPlan::Checkpoint
            | LogicalPlan::CopyTo { .. }
            | LogicalPlan::CopyFrom { .. }
            | LogicalPlan::CreateLabel(_)
            | LogicalPlan::CreateEdgeType(_)
            | LogicalPlan::AlterLabel(_)
            | LogicalPlan::AlterEdgeType(_)
            | LogicalPlan::DropLabel(_)
            | LogicalPlan::DropEdgeType(_)
            | LogicalPlan::CreateConstraint(_)
            | LogicalPlan::DropConstraint(_)
            | LogicalPlan::ShowConstraints(_) => None,
        }
    }

    /// Whether the given rewriter can actually reach its target node for `variable`.
    ///
    /// The `plan_where_clause` gates (`find_scan_label_id` / `is_scan_all_for` /
    /// `is_traverse_target`) descend Sort/Limit/Aggregate/Apply/Union, but the
    /// sibling rewriters do NOT rebuild those nodes — they fall through
    /// `other => other`. So a predicate whose target sits under one of those
    /// nodes is marked "consumed" by the gate yet silently not applied, dropping
    /// the WHERE/label/vector predicate. Consumption must instead be gated on
    /// this check, which descends only the "transparent" nodes each rewriter
    /// actually rebuilds — so an unreachable predicate stays in the residual and
    /// becomes a correct (if unoptimized) `Filter`. Keep the descent arms here in
    /// lockstep with the matching rewriter.
    fn rewrite_target_reachable(plan: &LogicalPlan, variable: &str, target: RewriteTarget) -> bool {
        // Base-node match (the node the rewriter actually rewrites).
        match plan {
            LogicalPlan::Scan { variable: var, .. } if var == variable => {
                if matches!(target, RewriteTarget::Knn | RewriteTarget::Scan) {
                    return true;
                }
            }
            LogicalPlan::ScanAll { variable: var, .. } if var == variable => {
                if matches!(target, RewriteTarget::Scan | RewriteTarget::LabelUnion) {
                    return true;
                }
            }
            LogicalPlan::Traverse {
                target_variable, ..
            } if target_variable == variable => {
                if matches!(target, RewriteTarget::TraverseTarget) {
                    return true;
                }
            }
            _ => {}
        }
        // Transparent descent — each rewriter's recursive arms, and no more.
        match plan {
            LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
                Self::rewrite_target_reachable(input, variable, target)
            }
            // Only `replace_scan_with_knn` recurses through `Limit`.
            LogicalPlan::Limit { input, .. } if matches!(target, RewriteTarget::Knn) => {
                Self::rewrite_target_reachable(input, variable, target)
            }
            // Scan/label-union/traverse-target rewriters recurse through `Traverse`;
            // the KNN rewriter does not.
            LogicalPlan::Traverse { input, .. }
                if matches!(
                    target,
                    RewriteTarget::Scan | RewriteTarget::LabelUnion | RewriteTarget::TraverseTarget
                ) =>
            {
                Self::rewrite_target_reachable(input, variable, target)
            }
            LogicalPlan::CrossJoin { left, right } => {
                Self::rewrite_target_reachable(left, variable, target)
                    || Self::rewrite_target_reachable(right, variable, target)
            }
            _ => false,
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
            LogicalPlan::ScanAll {
                variable: var,
                filter,
                optional,
            } if var == variable => {
                let new_filter = match filter {
                    Some(existing) => Some(Expr::BinaryOp {
                        left: Box::new(existing),
                        op: BinaryOp::And,
                        right: Box::new(predicate),
                    }),
                    None => Some(predicate),
                };
                LogicalPlan::ScanAll {
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
            other @ LogicalPlan::Traverse { .. } => {
                other.map_input(|child| Self::push_predicate_to_scan(child, variable, predicate))
            }
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
    fn collect_expr_variables(expr: &Expr) -> HashSet<String> {
        let mut vars = HashSet::new();
        Self::collect_expr_variables_impl(expr, &mut vars);
        vars
    }

    fn collect_expr_variables_impl(expr: &Expr, vars: &mut HashSet<String>) {
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
    fn collect_plan_variables(plan: &LogicalPlan) -> HashSet<String> {
        let mut vars = HashSet::new();
        Self::collect_plan_variables_impl(plan, &mut vars);
        vars
    }

    fn collect_plan_variables_impl(plan: &LogicalPlan, vars: &mut HashSet<String>) {
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
                step_variable,
                ..
            }
            | LogicalPlan::AllShortestPaths {
                input,
                path_variable,
                step_variable,
                ..
            } => {
                vars.insert(path_variable.clone());
                if let Some(sv) = step_variable {
                    vars.insert(sv.clone());
                }
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
            // Remaining scan leaves bind a single node variable (parity with the
            // `Scan`/`VectorKnn` arms above; see `collect_variable_kinds`). The
            // match is intentionally exhaustive — no `_ => {}` — so a new
            // variable-producing variant must be classified here rather than
            // silently dropped (the #131 bug class).
            LogicalPlan::FusedIndexScan { variable, .. }
            | LogicalPlan::ExtIdLookup { variable, .. }
            | LogicalPlan::ScanAll { variable, .. }
            | LogicalPlan::ScanMainByLabels { variable, .. }
            | LogicalPlan::InvertedIndexLookup { variable, .. } => {
                vars.insert(variable.clone());
            }
            LogicalPlan::FusedIndexScanWrapped { inner, .. } => {
                Self::collect_plan_variables_impl(inner, vars);
            }
            LogicalPlan::TraverseMainByType {
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
            LogicalPlan::BindPath {
                input,
                node_variables,
                edge_variables,
                path_variable,
            } => {
                for v in node_variables.iter().chain(edge_variables) {
                    vars.insert(v.clone());
                }
                vars.insert(path_variable.clone());
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::BindZeroLengthPath {
                input,
                node_variable,
                path_variable,
            } => {
                vars.insert(node_variable.clone());
                vars.insert(path_variable.clone());
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::QuantifiedPattern {
                input,
                pattern_plan,
                path_variable,
                start_variable,
                binding_variable,
                ..
            } => {
                vars.insert(start_variable.clone());
                vars.insert(binding_variable.clone());
                if let Some(pv) = path_variable {
                    vars.insert(pv.clone());
                }
                Self::collect_plan_variables_impl(input, vars);
                Self::collect_plan_variables_impl(pattern_plan, vars);
            }
            LogicalPlan::Union { left, right, .. } => {
                Self::collect_plan_variables_impl(left, vars);
                Self::collect_plan_variables_impl(right, vars);
            }
            LogicalPlan::Window { input, .. }
            | LogicalPlan::Create { input, .. }
            | LogicalPlan::CreateBatch { input, .. }
            | LogicalPlan::Merge { input, .. }
            | LogicalPlan::Set { input, .. }
            | LogicalPlan::Remove { input, .. }
            | LogicalPlan::Delete { input, .. }
            | LogicalPlan::Foreach { input, .. } => {
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Explain { plan } => {
                Self::collect_plan_variables_impl(plan, vars);
            }
            // Locy program/post-fixpoint operators and all leaf / DDL / admin
            // statements bind no Cypher-visible variables in this context.
            LogicalPlan::LocyProgram { .. }
            | LogicalPlan::LocyFold { .. }
            | LogicalPlan::LocyBestBy { .. }
            | LogicalPlan::LocyPriority { .. }
            | LogicalPlan::LocyDerivedScan { .. }
            | LogicalPlan::LocyProject { .. }
            | LogicalPlan::LocyModelInvoke { .. }
            | LogicalPlan::Empty
            | LogicalPlan::CreateVectorIndex { .. }
            | LogicalPlan::CreateSparseIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateScalarIndex { .. }
            | LogicalPlan::CreateJsonFtsIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::ShowIndexes { .. }
            | LogicalPlan::Copy { .. }
            | LogicalPlan::Backup { .. }
            | LogicalPlan::ShowDatabase
            | LogicalPlan::ShowConfig
            | LogicalPlan::ShowStatistics
            | LogicalPlan::Vacuum
            | LogicalPlan::Checkpoint
            | LogicalPlan::CopyTo { .. }
            | LogicalPlan::CopyFrom { .. }
            | LogicalPlan::CreateLabel(_)
            | LogicalPlan::CreateEdgeType(_)
            | LogicalPlan::AlterLabel(_)
            | LogicalPlan::AlterEdgeType(_)
            | LogicalPlan::DropLabel(_)
            | LogicalPlan::DropEdgeType(_)
            | LogicalPlan::CreateConstraint(_)
            | LogicalPlan::DropConstraint(_)
            | LogicalPlan::ShowConstraints(_) => {}
        }
    }

    /// Extract predicates that only reference variables from Apply's input.
    /// Returns (input_only_predicates, remaining_predicates).
    /// Whether an operand is one the Apply `input_filter` evaluator can resolve.
    ///
    /// `df_graph::apply::resolve_expr_value` resolves only literals, bare
    /// variables, and `var.key` properties; anything else resolves to `Null`.
    fn apply_operand_supported(expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) | Expr::Variable(_) => true,
            Expr::Property(base, _) => matches!(base.as_ref(), Expr::Variable(_)),
            _ => false,
        }
    }

    /// Whether `expr` is a shape the Apply `input_filter` evaluator handles.
    ///
    /// `df_graph::apply::evaluate_filter` handles And/Or/Not over comparisons
    /// (`Eq`/`NotEq`/`Lt`/`LtEq`/`Gt`/`GtEq`) whose operands pass
    /// [`Self::apply_operand_supported`], plus a bare truth test on such an
    /// operand. EVERY other operator or shape (STARTS WITH, CONTAINS, IN,
    /// arithmetic, CASE, regex, function calls) silently evaluates to
    /// `false`/`Null` there — and `NOT <unsupported>` inverts to `true`. Such
    /// predicates must NOT be pushed into `input_filter`; they stay as a residual
    /// `Filter` that evaluates them correctly. Keep in lockstep with
    /// `df_graph/apply.rs`.
    fn apply_input_filter_supported(expr: &Expr) -> bool {
        use uni_cypher::ast::{BinaryOp, UnaryOp};
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOp::And | BinaryOp::Or => {
                    Self::apply_input_filter_supported(left)
                        && Self::apply_input_filter_supported(right)
                }
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq => {
                    Self::apply_operand_supported(left) && Self::apply_operand_supported(right)
                }
                _ => false,
            },
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => Self::apply_input_filter_supported(expr),
            // Bare truth test: the evaluator's catch-all resolves the value and
            // reads `.as_bool()`, which is only meaningful for these operands.
            other => Self::apply_operand_supported(other),
        }
    }

    fn extract_apply_input_predicates(
        predicate: &Expr,
        input_variables: &HashSet<String>,
        subquery_new_variables: &HashSet<String>,
    ) -> (Vec<Expr>, Vec<Expr>) {
        let conjuncts = Self::split_and_conjuncts(predicate);
        let mut input_preds = Vec::new();
        let mut remaining = Vec::new();

        for conj in conjuncts {
            let vars = Self::collect_expr_variables(&conj);

            // Predicate only references input variables (none from subquery)
            let refs_input_only = vars.iter().all(|v| input_variables.contains(v));
            let refs_any_subquery = vars.iter().any(|v| subquery_new_variables.contains(v));

            // Only push shapes the input_filter evaluator can evaluate correctly;
            // otherwise leave the conjunct as a residual Filter (which handles the
            // full expression grammar) rather than have the evaluator silently
            // mis-evaluate it to `false`.
            if refs_input_only
                && !refs_any_subquery
                && !vars.is_empty()
                && Self::apply_input_filter_supported(&conj)
            {
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
                let new_subquery_vars: HashSet<String> =
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
            other @ LogicalPlan::Traverse { .. } => {
                other.map_input(|child| Self::push_predicates_to_apply(child, current_predicate))
            }
            other => other,
        }
    }
}

/// The user-visible output column names of `plan`, in query order.
///
/// `None` means the plan carries no projection at its top level. That is a
/// genuine answer for DDL and admin plans, and callers must treat it as
/// "unknown" rather than reconstructing a column list from the result rows:
/// a row map carries the traversal's internal helper columns (`b._vid`,
/// `b._labels`, `b.name`) alongside the projected ones, so guessing from it
/// returns whichever key happens to sort first. That is how
/// `MATCH (a:P)-[:KNOWS]->(b:P) RETURN b AS n UNION ALL ...` came to return
/// the node's `_labels` list instead of the node (#190), and why
/// `RETURN DISTINCT b AS n` did the same with no union in sight.
///
/// The match is deliberately exhaustive. Two copies of this logic existed and
/// disagreed about `Union` and `Distinct`; each disagreement was a silent
/// wrong answer, and neither is a shape anyone would call exotic. A new
/// `LogicalPlan` variant must be a compile error here rather than a variant
/// that quietly joins the guessing path.
pub fn projection_columns(plan: &LogicalPlan) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::Project { projections, .. } => Some(
            projections
                .iter()
                .map(|(expr, alias)| alias.clone().unwrap_or_else(|| expr.to_string_repr()))
                .collect(),
        ),
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            let mut names: Vec<String> = group_by.iter().map(|e| e.to_string_repr()).collect();
            names.extend(aggregates.iter().map(|e| e.to_string_repr()));
            Some(names)
        }
        // Row-preserving wrappers: the columns are whatever the input projects.
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Filter { input, .. } => projection_columns(input),
        // Both branches are validated to carry the same column names when the
        // union is built (`plan_with_scope`), so either side answers for both.
        // The right side is consulted only when the left cannot answer.
        LogicalPlan::Union { left, right, .. } => {
            projection_columns(left).or_else(|| projection_columns(right))
        }
        // No top-level projection. Listed rather than matched with `_` so that
        // adding a variant forces a decision here.
        LogicalPlan::Scan { .. }
        | LogicalPlan::FusedIndexScan { .. }
        | LogicalPlan::FusedIndexScanWrapped { .. }
        | LogicalPlan::ExtIdLookup { .. }
        | LogicalPlan::ScanAll { .. }
        | LogicalPlan::ScanMainByLabels { .. }
        | LogicalPlan::Empty
        | LogicalPlan::Unwind { .. }
        | LogicalPlan::Traverse { .. }
        | LogicalPlan::TraverseMainByType { .. }
        | LogicalPlan::Create { .. }
        | LogicalPlan::CreateBatch { .. }
        | LogicalPlan::Merge { .. }
        | LogicalPlan::Set { .. }
        | LogicalPlan::Remove { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Foreach { .. }
        | LogicalPlan::Window { .. }
        | LogicalPlan::CrossJoin { .. }
        | LogicalPlan::Apply { .. }
        | LogicalPlan::RecursiveCTE { .. }
        | LogicalPlan::ProcedureCall { .. }
        | LogicalPlan::SubqueryCall { .. }
        | LogicalPlan::VectorKnn { .. }
        | LogicalPlan::InvertedIndexLookup { .. }
        | LogicalPlan::ShortestPath { .. }
        | LogicalPlan::AllShortestPaths { .. }
        | LogicalPlan::QuantifiedPattern { .. }
        | LogicalPlan::CreateVectorIndex { .. }
        | LogicalPlan::CreateSparseIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateScalarIndex { .. }
        | LogicalPlan::CreateJsonFtsIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::ShowIndexes { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Backup { .. }
        | LogicalPlan::Explain { .. }
        | LogicalPlan::ShowDatabase
        | LogicalPlan::ShowConfig
        | LogicalPlan::ShowStatistics
        | LogicalPlan::Vacuum
        | LogicalPlan::Checkpoint
        | LogicalPlan::CopyTo { .. }
        | LogicalPlan::CopyFrom { .. }
        | LogicalPlan::CreateLabel(..)
        | LogicalPlan::CreateEdgeType(..)
        | LogicalPlan::AlterLabel(..)
        | LogicalPlan::AlterEdgeType(..)
        | LogicalPlan::DropLabel(..)
        | LogicalPlan::DropEdgeType(..)
        | LogicalPlan::CreateConstraint(..)
        | LogicalPlan::DropConstraint(..)
        | LogicalPlan::ShowConstraints(..)
        | LogicalPlan::BindZeroLengthPath { .. }
        | LogicalPlan::BindPath { .. }
        | LogicalPlan::LocyProgram { .. }
        | LogicalPlan::LocyFold { .. }
        | LogicalPlan::LocyBestBy { .. }
        | LogicalPlan::LocyPriority { .. }
        | LogicalPlan::LocyDerivedScan { .. }
        | LogicalPlan::LocyProject { .. }
        | LogicalPlan::LocyModelInvoke { .. } => None,
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

/// Output produced by `EXPLAIN` — a human-readable plan with index and cost info.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExplainOutput {
    /// Debug-formatted logical plan tree.
    pub plan_text: String,
    /// Index availability report for each scan in the plan.
    pub index_usage: Vec<IndexUsage>,
    /// Rough row and cost estimates for the full plan.
    pub cost_estimates: CostEstimates,
    /// Planner warnings (e.g., missing index, forced full scan).
    pub warnings: Vec<String>,
    /// Suggested indexes that would improve this query.
    pub suggestions: Vec<IndexSuggestion>,
}

/// Suggestion for creating an index to improve query performance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexSuggestion {
    /// Label or edge type that would benefit from the index.
    pub label_or_type: String,
    /// Property to index.
    pub property: String,
    /// Recommended index type (e.g., `"SCALAR"`, `"VECTOR"`).
    pub index_type: String,
    /// Human-readable explanation of the performance benefit.
    pub reason: String,
    /// Ready-to-execute Cypher statement to create the index.
    pub create_statement: String,
}

/// Index availability report for a single scan operator.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexUsage {
    pub label_or_type: String,
    pub property: String,
    pub index_type: String,
    /// Whether the index was actually used for this scan.
    pub used: bool,
    /// Human-readable explanation of why the index was or was not used.
    pub reason: Option<String>,
}

/// Rough cost and row count estimates for a complete logical plan.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CostEstimates {
    /// Estimated number of rows the plan will produce.
    pub estimated_rows: f64,
    /// Abstract cost units (lower is cheaper).
    pub estimated_cost: f64,
}

impl QueryPlanner {
    /// Plan a query and produce an EXPLAIN report (plan text, index usage, costs).
    pub fn explain_plan(&self, ast: Query) -> Result<ExplainOutput> {
        let plan = self.plan(ast)?;
        self.explain_logical_plan(&plan)
    }

    /// Produce an EXPLAIN report for an already-planned logical plan.
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
            LogicalPlan::Scan {
                label_id,
                variable,
                filter: Some(filter),
                ..
            } => {
                // Detect indexed-property pushdown — issues #57, #247. Run the
                // same analyzer the physical planner uses; if it reports an
                // index hit, surface it in EXPLAIN under the type that serves
                // it, so a BTree is not reported as a HASH.
                if let Some(label_name) = self.schema.label_name_by_id(*label_id) {
                    let analyzer = crate::query::pushdown::IndexAwareAnalyzer::new(&self.schema);
                    let strategy = analyzer.analyze(filter, variable, *label_id);
                    for (prop, kind) in strategy.indexed_equality_columns {
                        let kind_name = match kind {
                            ScalarIndexType::Hash => "HASH",
                            ScalarIndexType::BTree => "BTREE",
                            ref other => {
                                usage.push(IndexUsage {
                                    label_or_type: label_name.to_string(),
                                    property: prop,
                                    index_type: format!("{other:?}").to_uppercase(),
                                    used: true,
                                    reason: Some(
                                        "Scalar index point lookup pushed into Lance scan"
                                            .to_string(),
                                    ),
                                });
                                continue;
                            }
                        };
                        usage.push(IndexUsage {
                            label_or_type: label_name.to_string(),
                            property: prop,
                            index_type: kind_name.to_string(),
                            used: true,
                            reason: Some(format!(
                                "{kind_name} index point lookup pushed into Lance scan"
                            )),
                        });
                    }
                }
            }
            // An unfiltered scan uses no index.
            LogicalPlan::Scan { .. } => {}
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
                if (name.eq_ignore_ascii_case("uni.temporal.validAt")
                    || name.eq_ignore_ascii_case("validAt"))
                    && args.len() >= 2 =>
            {
                // args[0] = node, args[1] = start_prop, args[2] = end_prop, args[3] = time
                let start_prop = if let Some(Expr::Literal(CypherLiteral::String(s))) = args.get(1)
                {
                    s.clone()
                } else {
                    "valid_from".to_string()
                };

                // Try to extract label from the node expression
                if let Some(var) = args.first().and_then(|e| e.extract_variable()) {
                    self.suggest_temporal_index(&var, &start_prop, suggestions);
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
        VAR_PREFIX_RE.replace_all(expr_str, "$1").to_string()
    }

    /// Plan a schema command from the new AST
    fn plan_schema_command(&self, cmd: SchemaCommand) -> Result<LogicalPlan> {
        match cmd {
            SchemaCommand::CreateVectorIndex(c) => {
                use uni_common::vector_index_opts::{
                    VectorIndexOpts, build_vector_index_type, parse_vector_metric,
                };
                // `CREATE VECTOR INDEX … OPTIONS{type:'sparse'}` shares the vector DDL
                // surface but is a scored inverted index, not a dense ANN — route it to
                // the sparse path (mirrors the `uni.schema.createIndex` SPARSE arm in
                // `ddl_procedures.rs`). `build_vector_index_type` has no "sparse" case
                // and would otherwise fall through to the dense IVF_PQ default.
                if c.options.get("type").and_then(|v| v.as_str()) == Some("sparse") {
                    let dimensions = self
                        .schema
                        .properties
                        .get(&c.label)
                        .and_then(|props| props.get(&c.property))
                        .and_then(|meta| match &meta.r#type {
                            uni_common::DataType::SparseVector { dimensions } => Some(*dimensions),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Property '{}' is not a SparseVector column; cannot create a sparse index",
                                c.property
                            )
                        })?;
                    let quantize = c
                        .options
                        .get("quantize")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    // `OPTIONS{type:'sparse', embedding:{alias, source}}` auto-embeds
                    // a text column into the sparse column (same parser as dense).
                    let embedding_config = match c.options.get("embedding") {
                        Some(emb_val) => Self::parse_embedding_config(emb_val)?,
                        None => None,
                    };
                    let config = SparseVectorIndexConfig {
                        name: c.name,
                        label: c.label,
                        property: c.property,
                        dimensions,
                        quantize,
                        embedding_config,
                        metadata: Default::default(),
                    };
                    return Ok(LogicalPlan::CreateSparseIndex {
                        config,
                        if_not_exists: c.if_not_exists,
                    });
                }
                // Accept either a numeric value (`partitions: 256`) or a quoted string
                // (`partitions: '256'`) — Cypher map literals produce the former.
                let opt = |key: &str| -> Option<u32> {
                    c.options.get(key).and_then(|v| {
                        v.as_u64()
                            .map(|n| n as u32)
                            .or_else(|| v.as_str().and_then(|s| s.parse::<u32>().ok()))
                    })
                };
                let opt_u8 = |key: &str| -> Option<u8> {
                    c.options.get(key).and_then(|v| {
                        v.as_u64()
                            .map(|n| n as u8)
                            .or_else(|| v.as_str().and_then(|s| s.parse::<u8>().ok()))
                    })
                };
                let opt_u64 = |key: &str| -> Option<u64> {
                    c.options.get(key).and_then(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                    })
                };
                // Single source of truth (shared with the `uni.create_vector_index`
                // procedure) so dense / native-multivector / MUVERA behave identically.
                let index_type = build_vector_index_type(&VectorIndexOpts {
                    type_name: c.options.get("type").and_then(|v| v.as_str()),
                    partitions: opt("partitions"),
                    m: opt("m"),
                    ef_construction: opt("ef_construction"),
                    sub_vectors: opt("sub_vectors"),
                    num_bits: opt_u8("num_bits"),
                    k_sim: opt("k_sim"),
                    reps: opt("reps"),
                    d_proj: opt("d_proj"),
                    seed: opt_u64("seed"),
                    inner: c.options.get("inner").and_then(|v| v.as_str()),
                });

                // Parse embedding config from options
                let embedding_config = if let Some(emb_val) = c.options.get("embedding") {
                    Self::parse_embedding_config(emb_val)?
                } else {
                    None
                };

                // Parse the distance metric from OPTIONS (default Cosine).
                let metric = parse_vector_metric(c.options.get("metric").and_then(|v| v.as_str()))?;

                let config = VectorIndexConfig {
                    name: c.name,
                    label: c.label,
                    property: c.property,
                    metric,
                    index_type,
                    embedding_config,
                    metadata: Default::default(),
                    // Resolved from the column's dimensionality by
                    // `resolve_vector_index_defaults` before the definition is
                    // persisted.
                    default_refine_factor: None,
                };
                Ok(LogicalPlan::CreateVectorIndex {
                    config,
                    if_not_exists: c.if_not_exists,
                })
            }
            SchemaCommand::CreateFullTextIndex(cfg) => {
                let tokenizer = Self::parse_tokenizer_options(&cfg.options)?;
                Ok(LogicalPlan::CreateFullTextIndex {
                    config: FullTextIndexConfig {
                        name: cfg.name,
                        label: cfg.label,
                        properties: cfg.properties,
                        tokenizer,
                        with_positions: true,
                        metadata: Default::default(),
                    },
                    if_not_exists: cfg.if_not_exists,
                })
            }
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
                        metadata: Default::default(),
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
                        metadata: Default::default(),
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

    /// Parse `CREATE FULLTEXT INDEX ... OPTIONS { ... }` into a [`TokenizerConfig`].
    ///
    /// With no analyzer-related options the result is [`TokenizerConfig::Standard`]
    /// (the unchanged default). Recognized keys:
    /// `analyzer`/`tokenizer` (base tokenizer name), `language`, `stemmer`/`stem`,
    /// `stopwords` (bool or explicit list), `ascii_folding`, `lower_case`,
    /// `max_token_length`, `ngram_min`/`ngram_max`.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown `language`, an unusable `ngram_min`/`ngram_max`
    /// range, or an option value of the wrong type.
    fn parse_tokenizer_options(
        options: &std::collections::HashMap<String, Value>,
    ) -> Result<TokenizerConfig> {
        // Keys that, when present, opt into the richer analyzer pipeline.
        const ANALYZER_KEYS: [&str; 11] = [
            "analyzer",
            "tokenizer",
            "language",
            "stemmer",
            "stem",
            "stopwords",
            "stop_words",
            "ascii_folding",
            "lower_case",
            "max_token_length",
            "ngram_min",
        ];
        let has_ngram_max = options.contains_key("ngram_max");
        if !has_ngram_max && !ANALYZER_KEYS.iter().any(|k| options.contains_key(*k)) {
            // No FTS analyzer options → keep the historical default.
            return Ok(TokenizerConfig::Standard);
        }

        let base_raw = options
            .get("analyzer")
            .or_else(|| options.get("tokenizer"))
            .and_then(|v| v.as_str());
        let ngram_min = options.get("ngram_min").and_then(|v| v.as_i64());
        let ngram_max = options.get("ngram_max").and_then(|v| v.as_i64());

        let base = match base_raw.map(|s| s.to_ascii_lowercase()).as_deref() {
            Some("standard") | Some("simple") | None => BaseTokenizer::Simple,
            Some("whitespace") => BaseTokenizer::Whitespace,
            Some("raw") | Some("keyword") => BaseTokenizer::Raw,
            Some("ngram") => {
                let min = ngram_min.unwrap_or(3);
                let max = ngram_max.unwrap_or_else(|| min.max(3));
                if min < 1 || min > max {
                    return Err(anyhow!(
                        "invalid ngram options: ngram_min ({min}) must be >= 1 and <= ngram_max ({max})"
                    ));
                }
                BaseTokenizer::Ngram {
                    min: min as u32,
                    max: max as u32,
                }
            }
            // Passthrough for backend-native tokenizers (e.g. "jieba/default",
            // "lindera/ipadic"); preserve the original casing.
            Some(_) => BaseTokenizer::Custom(base_raw.unwrap().to_string()),
        };

        let language = match options.get("language").and_then(|v| v.as_str()) {
            None => FtsLanguage::English,
            Some(s) => Self::parse_fts_language(s)?,
        };

        // Filters default to the analyzer's own defaults (all enabled).
        let defaults = AnalyzerConfig::default();
        let lower_case = options
            .get("lower_case")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.lower_case);
        let stem = options
            .get("stemmer")
            .or_else(|| options.get("stem"))
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.stem);
        let ascii_folding = options
            .get("ascii_folding")
            .and_then(|v| v.as_bool())
            .unwrap_or(defaults.ascii_folding);
        let max_token_length = options
            .get("max_token_length")
            .and_then(|v| v.as_i64())
            .map(|n| n.max(0) as u32);

        // `stopwords`/`stop_words` accepts either a bool toggle or an explicit
        // list of stop words (which also enables removal).
        let stopwords_val = options
            .get("stopwords")
            .or_else(|| options.get("stop_words"));
        let (remove_stop_words, custom_stop_words) = match stopwords_val {
            None => (defaults.remove_stop_words, None),
            Some(v) => {
                if let Some(arr) = v.as_array() {
                    let words: Vec<String> = arr
                        .iter()
                        .filter_map(|w| w.as_str().map(|s| s.to_string()))
                        .collect();
                    (true, Some(words))
                } else if let Some(b) = v.as_bool() {
                    (b, None)
                } else {
                    return Err(anyhow!(
                        "invalid `stopwords` option: expected a boolean or a list of strings"
                    ));
                }
            }
        };

        Ok(TokenizerConfig::Analyzer(AnalyzerConfig {
            base,
            language,
            lower_case,
            stem,
            remove_stop_words,
            custom_stop_words,
            ascii_folding,
            max_token_length,
        }))
    }

    /// Map a language name (case-insensitive) onto an [`FtsLanguage`].
    ///
    /// # Errors
    ///
    /// Returns an error for an unrecognized language name.
    fn parse_fts_language(s: &str) -> Result<FtsLanguage> {
        let lang = match s.to_ascii_lowercase().as_str() {
            "arabic" => FtsLanguage::Arabic,
            "danish" => FtsLanguage::Danish,
            "dutch" => FtsLanguage::Dutch,
            "english" => FtsLanguage::English,
            "finnish" => FtsLanguage::Finnish,
            "french" => FtsLanguage::French,
            "german" => FtsLanguage::German,
            "greek" => FtsLanguage::Greek,
            "hungarian" => FtsLanguage::Hungarian,
            "italian" => FtsLanguage::Italian,
            "norwegian" => FtsLanguage::Norwegian,
            "portuguese" => FtsLanguage::Portuguese,
            "romanian" => FtsLanguage::Romanian,
            "russian" => FtsLanguage::Russian,
            "spanish" => FtsLanguage::Spanish,
            "swedish" => FtsLanguage::Swedish,
            "tamil" => FtsLanguage::Tamil,
            "turkish" => FtsLanguage::Turkish,
            other => {
                return Err(anyhow!(
                    "unknown FTS language '{other}' (expected one of: arabic, danish, dutch, \
                     english, finnish, french, german, greek, hungarian, italian, norwegian, \
                     portuguese, romanian, russian, spanish, swedish, tamil, turkish)"
                ));
            }
        };
        Ok(lang)
    }

    fn parse_embedding_config(emb_val: &Value) -> Result<Option<EmbeddingConfig>> {
        let obj = emb_val
            .as_object()
            .ok_or_else(|| anyhow!("embedding option must be an object"))?;

        // Parse alias (required)
        let alias = obj
            .get("alias")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("embedding.alias is required"))?;

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

        let batch_size = obj
            .get("batch_size")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(32);

        let document_prefix = obj
            .get("document_prefix")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let query_prefix = obj
            .get("query_prefix")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(Some(EmbeddingConfig {
            alias: alias.to_string(),
            source_properties,
            batch_size,
            document_prefix,
            query_prefix,
        }))
    }
}

/// Collect all properties referenced anywhere in the LogicalPlan tree.
///
/// This is critical for window functions: properties must be materialized
/// at the Scan node so they're available for window operations later.
///
/// Returns a mapping of variable name → property names (e.g., "e" → {"dept", "salary"}).
pub fn collect_properties_from_plan(plan: &LogicalPlan) -> HashMap<String, HashSet<String>> {
    let mut properties: HashMap<String, HashSet<String>> = HashMap::new();
    collect_properties_recursive(plan, &mut properties);
    properties
}

/// Resolve WITH-passthrough provenance markers into concrete projection markers.
///
/// A bare entity variable forwarded through a WITH projection is tagged with
/// [`WITH_PASSTHROUGH_SENTINEL`] by [`collect_properties_from_plan`]. This pass
/// converts every such marker to either `"*"` — the variable is returned whole,
/// another site genuinely needs all its properties, or it is not safely
/// narrowable — or [`STRUCT_ONLY_SENTINEL`], meaning only the downstream-accessed
/// properties are materialized and the wide columns are skipped. No marker
/// survives this pass.
///
/// `narrowable` lists the variables that may be narrowed: vertex/edge entities
/// whose struct is built from flat property columns. Path / edge-list / unknown
/// variables are always kept wide. If the query's output shape cannot be
/// identified, every forwarded variable is kept wide (safe default).
pub(crate) fn reconcile_passthrough_properties(
    plan: &LogicalPlan,
    properties: &mut HashMap<String, HashSet<String>>,
    narrowable: &HashSet<String>,
) {
    // 1. Extract alias→source links recorded by the Project walk (`WITH n AS m`
    //    records `__alias_of__n` on `m`), removing the transient markers.
    //    Discovery is complete because the recording happened during the same
    //    full plan traversal that gathered properties.
    let mut alias_source: HashMap<String, String> = HashMap::new();
    for (var, set) in properties.iter_mut() {
        let mut source = None;
        set.retain(|p| match p.strip_prefix(ALIAS_OF_PREFIX) {
            Some(src) => {
                source = Some(src.to_string());
                false
            }
            None => true,
        });
        if let Some(src) = source {
            alias_source.insert(var.clone(), src);
        }
    }

    // Fold each alias's accessed properties onto its source. This is
    // unconditional and safe: if the source is later kept wide the extra
    // properties are subsumed by "*", and if it is narrowed it needs exactly
    // these. A single non-cascading pass (props are read from a pre-fold
    // snapshot) suffices because rename *chains* keep their endpoints wide (see
    // `keep_wide` below), so cascading folds are never required for correctness.
    let real_props: HashMap<String, Vec<String>> = properties
        .iter()
        .map(|(v, set)| {
            let props: Vec<String> = set
                .iter()
                .filter(|p| {
                    p.as_str() != "*"
                        && p.as_str() != STRUCT_ONLY_SENTINEL
                        && p.as_str() != WITH_PASSTHROUGH_SENTINEL
                })
                .cloned()
                .collect();
            (v.clone(), props)
        })
        .collect();
    for (alias, src) in &alias_source {
        if let Some(props) = real_props.get(alias)
            && !props.is_empty()
        {
            properties
                .entry(src.clone())
                .or_default()
                .extend(props.iter().cloned());
        }
    }

    // 2. Determine which variables must stay wide (returned whole). A bare
    //    entity in the terminal projection keeps every property; if it is an
    //    alias, its source must stay wide too.
    let terminal = terminal_projection(plan);
    let mut returned_whole: HashSet<String> = HashSet::new();
    match terminal {
        Some(projections) => {
            for (expr, _alias) in projections {
                if let Expr::Variable(v) = expr
                    && !v.contains('.')
                {
                    returned_whole.insert(v.clone());
                    if let Some(src) = alias_source.get(v) {
                        returned_whole.insert(src.clone());
                    }
                }
            }
        }
        None => {
            // Output shape unknown (write op, UNION, …) — keep every forwarded
            // variable wide rather than risk narrowing a returned entity.
            for set in properties.values_mut() {
                if set.remove(WITH_PASSTHROUGH_SENTINEL) {
                    set.insert("*".to_string());
                }
            }
            return;
        }
    }

    // 3. Resolve each passthrough marker to "*" (kept wide) or the struct-only
    //    sentinel (narrowed to the folded, accessed properties).
    for (var, set) in properties.iter_mut() {
        if !set.remove(WITH_PASSTHROUGH_SENTINEL) {
            continue;
        }
        // Keep wide when the variable is returned whole, is already required
        // whole by another site ("*"), is not a narrowable entity (paths, edge
        // lists, unknown kinds), or participates in a rename chain — either it
        // is itself an alias of something, or one of its aliases is renamed
        // further (a chain endpoint whose narrowed set could not be kept
        // consistent across every level). Otherwise materialize only the
        // accessed properties via a struct-only projection.
        let keep_wide = set.contains("*")
            || returned_whole.contains(var)
            || !narrowable.contains(var)
            || alias_source.contains_key(var)
            || has_chained_alias(&alias_source, var);
        if keep_wide {
            set.insert("*".to_string());
        } else {
            set.insert(STRUCT_ONLY_SENTINEL.to_string());
        }
    }
}

/// True if any alias of `v` is itself renamed further (`WITH v AS m … WITH m AS
/// p`), making `v` a rename-chain endpoint that must stay wide.
fn has_chained_alias(alias_source: &HashMap<String, String>, v: &str) -> bool {
    alias_source
        .iter()
        .filter(|(_, src)| src.as_str() == v)
        .any(|(alias, _)| alias_source.values().any(|s| s == alias))
}

/// Descend result-shaping wrappers (`Sort`/`Limit`/`Distinct`) to the outermost
/// projection — the query's terminal output shape — or `None` if the root is
/// not a projection-topped plan.
fn terminal_projection(plan: &LogicalPlan) -> Option<&Vec<(Expr, Option<String>)>> {
    match plan {
        LogicalPlan::Project { projections, .. } => Some(projections),
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => terminal_projection(input),
        _ => None,
    }
}

/// Record, under [`DEAD_UNWIND_SOURCES_KEY`], every `UNWIND` source variable
/// that nothing else in the plan reads.
///
/// `UNWIND xs AS x` consumes `xs`, but the list column keeps flowing: every
/// operator above it copies its input columns forward, and a traversal copies
/// them once **per fan-out row**. So a collected list of *n* entities unwound
/// and then traversed is re-materialised `rows × n` times, in
/// `GraphUnwindStream::build_output_batch`'s `take` over the input columns —
/// which is `rows × list_size` bytes and is the 14 TB allocation that aborts
/// the process on LDBC SNB IC6 and IC9 at SF1 (#184). Inserting a bare `WITH f`
/// after the `UNWIND` makes the identical query answer correctly, because the
/// projection drops the list; this does the same thing without the user having
/// to know.
///
/// Liveness is decided by absence, not by a top-down required-set walk: a
/// source is dead when the *whole* plan, with the `UNWIND` expressions
/// themselves blanked out, never mentions it. Re-using
/// [`collect_properties_from_plan`] for that is the point — it is the
/// exhaustive walker this crate already maintains, so a plan variant added
/// later cannot quietly escape the analysis and leave a live column pruned.
///
/// Three deliberate refusals, each of which would otherwise be a wrong answer
/// rather than a slow query:
///
/// - **`RETURN *` / `WITH *`.** A wildcard names nothing, so absence proves
///   nothing. Any wildcard anywhere and the whole analysis stands down — both a
///   `LogicalPlan::Project` wildcard and one inside a subquery body, which is
///   AST hanging off an expression and so invisible to the plan-level survey.
/// - **A source unwound more than once.** Blanking removes every `UNWIND`
///   expression at once, so two `UNWIND xs` nodes would each look unreferenced
///   by the other. Only a source used by exactly one is considered.
/// - **A non-variable source.** `UNWIND range(1,10) AS i` has no column to
///   drop; only a bare variable is a candidate.
pub(crate) fn mark_dead_unwind_sources(
    plan: &LogicalPlan,
    properties: &mut HashMap<String, HashSet<String>>,
) {
    let mut sources: HashMap<String, usize> = HashMap::new();
    let mut saw_wildcard = false;
    survey_unwind_sources(plan, &mut sources, &mut saw_wildcard);
    if saw_wildcard || sources.is_empty() {
        return;
    }

    let blanked = blank_unwind_sources(plan.clone());
    let referenced = collect_properties_from_plan(&blanked);
    // A `RETURN *` in a subquery body is a wildcard the plan-level survey above
    // cannot see, because the body is AST hanging off an expression rather than
    // a `LogicalPlan::Project`. It names nothing, so absence proves nothing.
    if referenced.contains_key(SUBQUERY_WILDCARD_KEY) {
        return;
    }

    let dead: HashSet<String> = sources
        .into_iter()
        .filter(|(name, uses)| *uses == 1 && !is_read_anywhere(&referenced, name))
        .map(|(name, _)| name)
        .collect();
    if !dead.is_empty() {
        properties.insert(DEAD_UNWIND_SOURCES_KEY.to_string(), dead);
    }
}

/// True when the collected map records an actual *read* of `name`.
///
/// Presence in the map is not enough. `WITH collect(x) AS xs` records
/// `xs → __alias_of__collect(x)` on the alias, which is provenance for
/// `reconcile_passthrough_properties` — a note about where `xs` came *from*,
/// not evidence that anyone consumes it. Counting that as a read makes every
/// collected list look live, which is exactly the case #184 is about. A genuine
/// read leaves a property name, a `*`, or a passthrough sentinel.
fn is_read_anywhere(referenced: &HashMap<String, HashSet<String>>, name: &str) -> bool {
    referenced
        .get(name)
        .is_some_and(|props| props.iter().any(|p| !p.starts_with(ALIAS_OF_PREFIX)))
}

/// Count `UNWIND` sources that are bare variables, and notice any wildcard.
fn survey_unwind_sources(
    plan: &LogicalPlan,
    sources: &mut HashMap<String, usize>,
    saw_wildcard: &mut bool,
) {
    if let LogicalPlan::Unwind { expr, .. } = plan
        && let Expr::Variable(name) = expr
        && !name.contains('.')
    {
        *sources.entry(name.clone()).or_insert(0) += 1;
    }
    // Only a projection that *is* a wildcard — `RETURN *` / `WITH *`. Not a
    // wildcard nested inside an expression: `count(*)` carries an
    // `Expr::Wildcard` argument and names nothing extra, so treating it as one
    // would disable pruning for essentially every aggregate query, including
    // the LDBC shapes this exists for.
    if let LogicalPlan::Project { projections, .. } = plan
        && projections.iter().any(|(e, _)| matches!(e, Expr::Wildcard))
    {
        *saw_wildcard = true;
    }
    for child in plan.children() {
        survey_unwind_sources(child, sources, saw_wildcard);
    }
}

/// Replace each `UNWIND`'s source expression with a null literal, so the only
/// remaining mentions of a source variable are genuine other readers.
fn blank_unwind_sources(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Unwind {
            input,
            expr,
            variable,
        } => {
            let expr = if matches!(&expr, Expr::Variable(v) if !v.contains('.')) {
                Expr::Literal(CypherLiteral::Null)
            } else {
                expr
            };
            LogicalPlan::Unwind {
                input: Box::new(blank_unwind_sources(*input)),
                expr,
                variable,
            }
        }
        LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
            left: Box::new(blank_unwind_sources(*left)),
            right: Box::new(blank_unwind_sources(*right)),
            all,
        },
        LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
            left: Box::new(blank_unwind_sources(*left)),
            right: Box::new(blank_unwind_sources(*right)),
        },
        LogicalPlan::Apply {
            input,
            subquery,
            input_filter,
        } => LogicalPlan::Apply {
            input: Box::new(blank_unwind_sources(*input)),
            subquery: Box::new(blank_unwind_sources(*subquery)),
            input_filter,
        },
        LogicalPlan::SubqueryCall { input, subquery } => LogicalPlan::SubqueryCall {
            input: Box::new(blank_unwind_sources(*input)),
            subquery: Box::new(blank_unwind_sources(*subquery)),
        },
        LogicalPlan::RecursiveCTE {
            cte_name,
            initial,
            recursive,
        } => LogicalPlan::RecursiveCTE {
            cte_name,
            initial: Box::new(blank_unwind_sources(*initial)),
            recursive: Box::new(blank_unwind_sources(*recursive)),
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(blank_unwind_sources(*plan)),
        },
        other => other.map_input(blank_unwind_sources),
    }
}

/// Recursively walk the LogicalPlan tree and collect all property references.
fn collect_properties_recursive(
    plan: &LogicalPlan,
    properties: &mut HashMap<String, HashSet<String>>,
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
            for (expr, alias) in projections {
                // A bare entity variable forwarded through a projection
                // (`WITH n`, `WITH n AS m`, `RETURN n`) would otherwise hit the
                // bare-`Variable` arm and mark the source "*", pulling the full
                // schema even when only narrow properties are accessed downstream
                // (issue #134 family). Emit a provenance marker on the source
                // instead; for a rename also record the alias→source link so
                // `reconcile_passthrough_properties` can fold the alias's
                // accessed properties back onto the source. That pass keeps "*"
                // for variables returned whole and downgrades the rest to a
                // struct-only projection of the accessed properties.
                if let Expr::Variable(src) = expr
                    && !src.contains('.')
                {
                    properties
                        .entry(src.clone())
                        .or_default()
                        .insert(WITH_PASSTHROUGH_SENTINEL.to_string());
                    if let Some(alias) = alias
                        && alias != src
                    {
                        properties
                            .entry(alias.clone())
                            .or_default()
                            .insert(format!("{ALIAS_OF_PREFIX}{src}"));
                    }
                } else {
                    collect_properties_from_expr_into(expr, properties);
                }
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
                // A bare entity variable used as a group key needs only the
                // entity's *identity*; grouping cannot depend on a property the
                // query never reads. Left to the bare-`Variable` arm it marks
                // the source "*", which pulls the full schema — `_all_props`
                // and `overflow_json` included — into the scan *and* into the
                // physical group key, which appends every `{v}.`-prefixed
                // column beside the entity struct.
                //
                // On LDBC SF1 that is what made
                // `MATCH (p:Person)-[:KNOWS]-() WITH p, count(*) RETURN p.id`
                // request 1.76 GB and blow the query memory pool, for a query
                // that reads one property (#196).
                //
                // Same provenance marker the `Project` arm above emits, and the
                // same downstream treatment: `reconcile_passthrough_properties`
                // keeps "*" for variables returned whole and downgrades the
                // rest to a struct-only projection of the properties actually
                // accessed.
                if let Expr::Variable(src) = expr
                    && !src.contains('.')
                {
                    properties
                        .entry(src.clone())
                        .or_default()
                        .insert(WITH_PASSTHROUGH_SENTINEL.to_string());
                } else {
                    collect_properties_from_expr_into(expr, properties);
                }
            }
            for expr in aggregates {
                // Aggregate *arguments* are unchanged: `collect(n)` really does
                // return the entity whole, and narrowing it would be a wrong
                // answer rather than a smaller one.
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
        LogicalPlan::LocyProject {
            input, projections, ..
        } => {
            for (expr, _alias) in projections {
                match expr {
                    // Bare variable in LocyProject: only need _vid for node variables
                    // (plan_locy_project extracts VID directly). Adding "*" would create
                    // a structural Struct column that conflicts with derived scan columns.
                    Expr::Variable(name) if !name.contains('.') => {
                        properties
                            .entry(name.clone())
                            .or_default()
                            .insert("_vid".to_string());
                    }
                    _ => collect_properties_from_expr_into(expr, properties),
                }
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::LocyFold {
            input,
            fold_bindings,
            ..
        } => {
            for (_name, expr) in fold_bindings {
                collect_properties_from_expr_into(expr, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::LocyBestBy {
            input, criteria, ..
        } => {
            for (expr, _asc) in criteria {
                collect_properties_from_expr_into(expr, properties);
            }
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::LocyPriority { input, .. } => {
            collect_properties_recursive(input, properties);
        }
        LogicalPlan::LocyModelInvoke { input, .. } => {
            // Model invocations don't introduce new property accesses
            // — feature expressions are lifted to hidden YIELD items
            // by `extract_model_invocations` (uni-locy typecheck) and
            // their property refs are already collected via the
            // wrapped LocyProject's projection walk.
            collect_properties_recursive(input, properties);
        }
        // A fork-fused scan carries the same equality filter as the plain
        // `Scan` it replaced; collect its properties so the filtered column is
        // materialized (parity with the `Scan { filter: Some }` arm). The
        // wrapped form forwards to its inner scan. Missing these is the #131
        // bug class — a dropped filter column risks under-materialization.
        LogicalPlan::FusedIndexScan {
            filter: Some(expr), ..
        } => {
            collect_properties_from_expr_into(expr, properties);
        }
        LogicalPlan::FusedIndexScan { filter: None, .. } => {}
        LogicalPlan::FusedIndexScanWrapped { inner, .. } => {
            collect_properties_recursive(inner, properties);
        }
        LogicalPlan::Explain { plan } => {
            collect_properties_recursive(plan, properties);
        }
        // Nodes that reference no node properties: the Locy program node, Locy
        // derived scans (read materialized derived columns, not graph
        // properties), and every leaf / DDL / admin statement. The match is
        // exhaustive — no `_ => {}` — so a new variant must be classified here.
        LogicalPlan::LocyProgram { .. }
        | LogicalPlan::LocyDerivedScan { .. }
        | LogicalPlan::Empty
        | LogicalPlan::CreateVectorIndex { .. }
        | LogicalPlan::CreateSparseIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateScalarIndex { .. }
        | LogicalPlan::CreateJsonFtsIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::ShowIndexes { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Backup { .. }
        | LogicalPlan::ShowDatabase
        | LogicalPlan::ShowConfig
        | LogicalPlan::ShowStatistics
        | LogicalPlan::Vacuum
        | LogicalPlan::Checkpoint
        | LogicalPlan::CopyTo { .. }
        | LogicalPlan::CopyFrom { .. }
        | LogicalPlan::CreateLabel(_)
        | LogicalPlan::CreateEdgeType(_)
        | LogicalPlan::AlterLabel(_)
        | LogicalPlan::AlterEdgeType(_)
        | LogicalPlan::DropLabel(_)
        | LogicalPlan::DropEdgeType(_)
        | LogicalPlan::CreateConstraint(_)
        | LogicalPlan::DropConstraint(_)
        | LogicalPlan::ShowConstraints(_) => {}
    }
}

/// Mark target variables from SET items with "*" and collect value expressions.
fn mark_set_item_variables(items: &[SetItem], properties: &mut HashMap<String, HashSet<String>>) {
    for item in items {
        match item {
            SetItem::Property { expr, value } => {
                // SET n.prop = val — mark n with STRUCT_ONLY_SENTINEL so the
                // scan builds the bare `n` struct column (needed for executor
                // `row.get(var_name)`) WITHOUT pulling the full schema. The
                // explicit `prop` is collected via `collect_properties_from_expr_into`
                // below and joins the variable's HashSet alongside the sentinel.
                //
                // If the same variable is also referenced bare elsewhere
                // (e.g. `SET n.x = 1 RETURN n`), `collect_properties_from_expr_into`
                // inserts "*" through the bare-Variable path; "*" dominates
                // the sentinel in `resolve_properties`, so the full schema
                // is still pulled when actually required.
                collect_properties_from_expr_into(expr, properties);
                collect_properties_from_expr_into(value, properties);
                if let Expr::Property(base, _) = expr
                    && let Expr::Variable(var) = base.as_ref()
                {
                    properties
                        .entry(var.clone())
                        .or_default()
                        .insert(STRUCT_ONLY_SENTINEL.to_string());
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
fn mark_pattern_variables(pattern: &Pattern, properties: &mut HashMap<String, HashSet<String>>) {
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
    properties: &mut HashMap<String, HashSet<String>>,
) {
    match expr {
        Expr::PatternComprehension {
            pattern,
            where_clause,
            map_expr,
            ..
        } => {
            // The pattern's *variables* are local bindings and need nothing
            // collected. Its inline property maps and element-level WHERE
            // clauses are a different matter — they read outer scope, and
            // missing them let a live UNWIND source be pruned (#197).
            collect_properties_from_pattern(pattern, properties);
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
            // Analyze function for property requirements (pushdown hydration).
            // Returns the entity-argument indices it authoritatively handled.
            let handled_args = analyze_function_property_requirements(name, args, properties);

            // Collect from arguments, but skip bare-variable entity args already
            // accounted for by the analysis above. Re-recursing into them would
            // hit the bare-`Variable` arm and mark them "*", pulling the full
            // schema and defeating column projection (issue #134). Non-entity
            // args — and non-variable entity args — are still walked so nested
            // property accesses such as `sum(n.x)` are collected.
            //
            // This applies to DISTINCT aggregates too: `count(DISTINCT n)` is
            // rewritten in df_planner to dedup on the entity's identity column
            // (`n._vid`/`_eid`, a base column), so the property struct no longer
            // needs materializing. `collect(DISTINCT n)` is `no_entity` (never
            // skipped) and still widens to "*", correctly, as it returns whole
            // nodes.
            for (i, arg) in args.iter().enumerate() {
                if handled_args.contains(&i) && matches!(arg, Expr::Variable(v) if !v.contains('.'))
                {
                    continue;
                }
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

/// Walk an AST pattern and collect the property references its elements carry.
///
/// A node or relationship pattern holds two expressions besides its label and
/// variable — an inline property map (`(r:P {name: xs[0]})`) and an inline
/// `WHERE` (`(r:P WHERE r.name IN xs)`) — and both can read outer-scope
/// variables. The pattern's *variables* are local bindings and need nothing
/// collected; its *expressions* are not.
///
/// Missing these is not merely pessimistic. `mark_dead_unwind_sources` proves an
/// `UNWIND` source dead by absence, so a read it cannot see is a read that does
/// not exist, and the source column is dropped out from under the reader
/// (#197).
fn collect_properties_from_pattern(
    pattern: &Pattern,
    properties: &mut HashMap<String, HashSet<String>>,
) {
    for path in &pattern.paths {
        collect_properties_from_path_pattern(path, properties);
    }
}

/// Walk one path of a pattern, recursing through parenthesized sub-paths.
fn collect_properties_from_path_pattern(
    path: &PathPattern,
    properties: &mut HashMap<String, HashSet<String>>,
) {
    for element in &path.elements {
        match element {
            PatternElement::Node(NodePattern {
                properties: props,
                where_clause,
                ..
            })
            | PatternElement::Relationship(RelationshipPattern {
                properties: props,
                where_clause,
                ..
            }) => {
                if let Some(props) = props {
                    collect_properties_from_expr_into(props, properties);
                }
                if let Some(where_clause) = where_clause {
                    collect_properties_from_expr_into(where_clause, properties);
                }
            }
            PatternElement::Parenthesized { pattern, .. } => {
                collect_properties_from_path_pattern(pattern, properties);
            }
        }
    }
}

/// Collect the property references carried by a list of `SET` items.
fn collect_properties_from_set_items(
    items: &[SetItem],
    properties: &mut HashMap<String, HashSet<String>>,
) {
    for item in items {
        match item {
            SetItem::Property { expr, value } => {
                collect_properties_from_expr_into(expr, properties);
                collect_properties_from_expr_into(value, properties);
            }
            SetItem::Variable { value, .. } | SetItem::VariablePlus { value, .. } => {
                collect_properties_from_expr_into(value, properties);
            }
            // Label mutation names a variable and literal labels, no expression.
            SetItem::Labels { .. } => {}
        }
    }
}

/// Collect the property references carried by a projection list plus its
/// `ORDER BY` / `SKIP` / `LIMIT` tail.
fn collect_properties_from_return_items(
    items: &[ReturnItem],
    order_by: Option<&Vec<SortItem>>,
    skip: Option<&Expr>,
    limit: Option<&Expr>,
    properties: &mut HashMap<String, HashSet<String>>,
) {
    for item in items {
        match item {
            ReturnItem::Expr { expr, .. } => collect_properties_from_expr_into(expr, properties),
            // `RETURN *` names nothing, so it cannot be recorded as a read of
            // anything — and that is exactly why it is dangerous to an analysis
            // that reasons from absence. Flag it and let
            // `mark_dead_unwind_sources` stand down.
            ReturnItem::All => {
                properties
                    .entry(SUBQUERY_WILDCARD_KEY.to_string())
                    .or_default()
                    .insert("*".to_string());
            }
        }
    }
    for sort in order_by.into_iter().flatten() {
        collect_properties_from_expr_into(&sort.expr, properties);
    }
    for expr in skip.into_iter().chain(limit) {
        collect_properties_from_expr_into(expr, properties);
    }
}

/// Walk a subquery (EXISTS/COUNT/COLLECT body) and collect property references.
///
/// This is needed so that correlated property accesses like `a.city` inside
/// `WHERE EXISTS { (a)-[:KNOWS]->(b) WHERE b.city = a.city }` cause the outer
/// scan to include `a.city` in its projected columns.
///
/// Both matches below are **exhaustive on purpose**. For most consumers of the
/// property map an under-report costs a wasted column; for
/// `mark_dead_unwind_sources` it inverts — an unrecorded read is
/// indistinguishable from no read, so the source is proven dead and deleted
/// (#197). A new `Clause` or `Query` variant must therefore be a compile error
/// here, not a silent gap. Do not add a `_ => {}` arm.
fn collect_properties_from_subquery(
    query: &Query,
    properties: &mut HashMap<String, HashSet<String>>,
) {
    match query {
        Query::Single(stmt) => {
            for clause in &stmt.clauses {
                collect_properties_from_subquery_clause(clause, properties);
            }
        }
        Query::Union { left, right, .. } => {
            collect_properties_from_subquery(left, properties);
            collect_properties_from_subquery(right, properties);
        }
        Query::Explain(inner) => collect_properties_from_subquery(inner, properties),
        Query::TimeTravel { query, .. } => collect_properties_from_subquery(query, properties),
        // DDL and admin commands read no query variables.
        Query::Schema(_) => {}
    }
}

/// Collect the property references one clause of a subquery body carries.
///
/// See [`collect_properties_from_subquery`] for why this match is exhaustive.
fn collect_properties_from_subquery_clause(
    clause: &Clause,
    properties: &mut HashMap<String, HashSet<String>>,
) {
    match clause {
        Clause::Match(m) => {
            collect_properties_from_pattern(&m.pattern, properties);
            if let Some(ref wc) = m.where_clause {
                collect_properties_from_expr_into(wc, properties);
            }
        }
        Clause::With(w) => {
            collect_properties_from_return_items(
                &w.items,
                w.order_by.as_ref(),
                w.skip.as_ref(),
                w.limit.as_ref(),
                properties,
            );
            if let Some(ref wc) = w.where_clause {
                collect_properties_from_expr_into(wc, properties);
            }
        }
        Clause::Return(r) => collect_properties_from_return_items(
            &r.items,
            r.order_by.as_ref(),
            r.skip.as_ref(),
            r.limit.as_ref(),
            properties,
        ),
        Clause::WithRecursive(wr) => {
            collect_properties_from_subquery(&wr.query, properties);
            collect_properties_from_return_items(&wr.items, None, None, None, properties);
        }
        Clause::Unwind(u) => collect_properties_from_expr_into(&u.expr, properties),
        Clause::Call(c) => {
            match &c.kind {
                CallKind::Procedure { arguments, .. } => {
                    for arg in arguments {
                        collect_properties_from_expr_into(arg, properties);
                    }
                }
                CallKind::Subquery(inner) => collect_properties_from_subquery(inner, properties),
            }
            if let Some(ref wc) = c.where_clause {
                collect_properties_from_expr_into(wc, properties);
            }
        }
        // Mutation clauses cannot appear in an EXISTS/COUNT/COLLECT body today,
        // but they are cheap to handle and must not become a silent gap if the
        // grammar ever admits them.
        Clause::Create(c) => collect_properties_from_pattern(&c.pattern, properties),
        Clause::Merge(m) => {
            collect_properties_from_pattern(&m.pattern, properties);
            collect_properties_from_set_items(&m.on_match, properties);
            collect_properties_from_set_items(&m.on_create, properties);
        }
        Clause::Set(s) => collect_properties_from_set_items(&s.items, properties),
        Clause::Remove(r) => {
            for item in &r.items {
                match item {
                    RemoveItem::Property(expr) => {
                        collect_properties_from_expr_into(expr, properties)
                    }
                    RemoveItem::Labels { .. } => {}
                }
            }
        }
        Clause::Delete(d) => {
            for expr in &d.items {
                collect_properties_from_expr_into(expr, properties);
            }
        }
    }
}

/// Analyze function calls to extract property requirements for pushdown hydration.
///
/// This function examines function calls and their arguments to determine which properties
/// need to be loaded for entity arguments. For example:
/// - validAt(e, 'start', 'end', ts) -> e needs {start, end}
/// - keys(n) -> n needs all properties (*)
///
/// The extracted requirements are added to the properties map for later use during
/// scan planning.
///
/// Returns the argument indices this analysis authoritatively accounts for (the
/// function's entity arguments). The caller uses these to avoid re-recursing into
/// those bare-variable arguments, which would otherwise mark them with `*` and
/// defeat column projection. See issue #134.
fn analyze_function_property_requirements(
    name: &str,
    args: &[Expr],
    properties: &mut HashMap<String, HashSet<String>>,
) -> Vec<usize> {
    use crate::query::function_props::get_function_spec;

    /// Helper to mark a variable as needing all properties.
    fn mark_wildcard(var: &str, properties: &mut HashMap<String, HashSet<String>>) {
        properties
            .entry(var.to_string())
            .or_default()
            .insert("*".to_string());
    }

    // System-managed timestamp functions: require only the corresponding
    // `_created_at` / `_updated_at` column, not full entity materialization.
    if name.eq_ignore_ascii_case("created_at") || name.eq_ignore_ascii_case("updated_at") {
        if let Some(Expr::Variable(var)) = args.first() {
            let col = if name.eq_ignore_ascii_case("created_at") {
                "_created_at"
            } else {
                "_updated_at"
            };
            properties
                .entry(var.clone())
                .or_default()
                .insert(col.to_string());
        }
        // The single entity argument is fully accounted for here.
        return vec![0];
    }

    let Some(spec) = get_function_spec(name) else {
        // Unknown function: conservatively require all properties for variable args.
        // Nothing is authoritatively narrowed, so claim no handled arguments.
        for arg in args {
            if let Expr::Variable(var) = arg {
                mark_wildcard(var, properties);
            }
        }
        return Vec::new();
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

    // The spec's entity arguments are authoritatively handled above (a specific
    // property set, `*`, or nothing at all). The caller must not re-recurse into
    // these bare-variable arguments, which would mark them `*` and defeat
    // projection (issue #134).
    spec.entity_args.to_vec()
}

// ============================================================================
// Phase 5a-impl — fork-aware fusion rewrite
// ============================================================================

/// Trait that exposes the per-fork "is there a fork-local index for
/// `(label, column)`?" lookup. Implemented for `StorageManager` so
/// callers don't need to depend on the fork module directly; tests
/// can mock by implementing it on a `HashMap`.
pub trait ForkIndexLookup {
    /// Whether a fork-local index of `kind` exists for `(label,
    /// column)`. A column can carry several kinds at once (e.g. a
    /// `ScalarBtree` for equality plus a `FullText` for search), so
    /// each fusion site probes for the exact kind it intends to emit
    /// rather than reading a single stored value.
    fn fork_index_has(
        &self,
        label: &str,
        column: &str,
        kind: uni_store::fork::ForkLocalIndexKind,
    ) -> bool;

    /// Phase 5b followup: resolve a label id, then dispatch to
    /// `fork_index_has`. Used by the rewrite when wrapping
    /// `VectorKnn` and `InvertedIndexLookup` nodes which carry
    /// `label_id: u16` rather than the label name. Default returns
    /// `false`; the `StorageManager` impl resolves via its
    /// `schema_manager`.
    fn fork_index_has_label_id(
        &self,
        _label_id: u16,
        _column: &str,
        _kind: uni_store::fork::ForkLocalIndexKind,
    ) -> bool {
        false
    }
}

impl ForkIndexLookup for uni_store::storage::StorageManager {
    fn fork_index_has(
        &self,
        label: &str,
        column: &str,
        kind: uni_store::fork::ForkLocalIndexKind,
    ) -> bool {
        self.has_fork_index(label, column, kind)
    }

    fn fork_index_has_label_id(
        &self,
        label_id: u16,
        column: &str,
        kind: uni_store::fork::ForkLocalIndexKind,
    ) -> bool {
        let schema = self.schema_manager().schema();
        match schema.label_name_by_id(label_id) {
            Some(label_name) => self.has_fork_index(label_name, column, kind),
            None => false,
        }
    }
}

/// Fold a trailing `SET var.prop = value` into the freshly-created entity's
/// inline property map, eliminating the separate `Set` write pass.
///
/// Rewrites `CREATE (a)-[r:T]->(b) SET r.x = e.v` into the equivalent of
/// `CREATE (a)-[r:T {x: e.v}]->(b)`, so the plan collapses from `Set → Create`
/// to a single `Create`. This removes an entire read-modify-write operator
/// (`MutationSetExec`) — measured at ~38% of per-edge `UNWIND … CREATE … SET`
/// execution — that the bulk write path never pays.
///
/// # Examples
///
/// ```ignore
/// // CREATE (a)-[r:LINK]->(b) SET r.role = e.role   ==>
/// // CREATE (a)-[r:LINK {role: e.role}]->(b)
/// let fused = fuse_create_set(plan);
/// ```
///
/// The fold is **all-or-nothing per `SET` clause** and only fires when every
/// item is safe:
/// - the item is the simple `Variable.property = value` form (not `+=`, label
///   set `SET n:L`, or whole-entity map assignment `SET n = {...}`),
/// - the target variable is introduced by the immediately-preceding
///   `Create`/`CreateBatch` (a MATCHed variable is left untouched),
/// - the target element's inline properties are absent or a map literal (a
///   parameter-map form such as `CREATE (n $props)` cannot be merged),
/// - the value references no variable created in the same statement, so
///   evaluating it at create time is observably identical to SET time.
///
/// When any item fails these checks the whole `Set` node is preserved, keeping
/// semantics unchanged. The pass is idempotent: a plan with no fusable
/// `Set`/`Create` adjacency passes through untouched.
#[must_use]
pub fn fuse_create_set(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Set { input, items } => {
            // Fuse any deeper adjacency first so chained
            // `CREATE … SET … CREATE … SET` collapses bottom-up.
            let input = fuse_create_set(*input);
            match input {
                LogicalPlan::Create {
                    input: child,
                    pattern,
                } => {
                    let bound_vars = crate::query::df_planner::collect_plan_variables(&child);
                    match try_fuse_set_items(std::slice::from_ref(&pattern), &items, &bound_vars) {
                        Some(mut patterns) => LogicalPlan::Create {
                            input: child,
                            // try_fuse_set_items returns exactly as many patterns
                            // as it was given (one here).
                            pattern: patterns
                                .pop()
                                .expect("one pattern in yields one pattern out"),
                        },
                        None => LogicalPlan::Set {
                            input: Box::new(LogicalPlan::Create {
                                input: child,
                                pattern,
                            }),
                            items,
                        },
                    }
                }
                LogicalPlan::CreateBatch {
                    input: child,
                    patterns,
                } => {
                    let bound_vars = crate::query::df_planner::collect_plan_variables(&child);
                    match try_fuse_set_items(&patterns, &items, &bound_vars) {
                        Some(fused) => LogicalPlan::CreateBatch {
                            input: child,
                            patterns: fused,
                        },
                        None => LogicalPlan::Set {
                            input: Box::new(LogicalPlan::CreateBatch {
                                input: child,
                                patterns,
                            }),
                            items,
                        },
                    }
                }
                other => LogicalPlan::Set {
                    input: Box::new(other),
                    items,
                },
            }
        }
        // Recurse through the operators that can sit above a write clause so a
        // `Set` under RETURN/ORDER BY/LIMIT is still reached. This mirrors the
        // pragmatic recursion of `rewrite_for_fork_fusion`: variants that never
        // sit above a write clause fall through `other => other` unchanged.
        LogicalPlan::Project { input, projections } => LogicalPlan::Project {
            input: Box::new(fuse_create_set(*input)),
            projections,
        },
        LogicalPlan::Limit { input, skip, fetch } => LogicalPlan::Limit {
            input: Box::new(fuse_create_set(*input)),
            skip,
            fetch,
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(fuse_create_set(*input)),
            order_by,
        },
        LogicalPlan::Filter {
            input,
            predicate,
            optional_variables,
        } => LogicalPlan::Filter {
            input: Box::new(fuse_create_set(*input)),
            predicate,
            optional_variables,
        },
        LogicalPlan::Create { input, pattern } => LogicalPlan::Create {
            input: Box::new(fuse_create_set(*input)),
            pattern,
        },
        LogicalPlan::CreateBatch { input, patterns } => LogicalPlan::CreateBatch {
            input: Box::new(fuse_create_set(*input)),
            patterns,
        },
        other => other,
    }
}

/// Try to fold every `SET` item into the given CREATE patterns.
///
/// Returns the rewritten patterns when *all* items fuse safely (see
/// [`fuse_create_set`] for the conditions); returns `None` the moment any item
/// is unfusable, so the caller can keep the original `Set` node untouched.
///
/// `bound_vars` are the variables produced by the CREATE's input plan (e.g. an
/// upstream MATCH). A CREATE pattern may *reuse* such a variable as an endpoint
/// (`MATCH (a) CREATE (a)-[r:T]->(b)`), so `pattern_variable_names` alone cannot
/// tell a freshly-created variable from a reused one. Reused variables are
/// excluded from `owner`: a `SET` on them must not fuse, because the executor
/// skips inline properties on already-bound elements (which would silently drop
/// the write).
fn try_fuse_set_items(
    patterns: &[Pattern],
    items: &[SetItem],
    bound_vars: &HashSet<String>,
) -> Option<Vec<Pattern>> {
    // Map each freshly-created variable to the index of the pattern that
    // introduces it, skipping any variable already bound upstream.
    let mut owner: HashMap<String, usize> = HashMap::new();
    for (idx, pattern) in patterns.iter().enumerate() {
        for var in crate::query::df_graph::mutation_common::pattern_variable_names(pattern) {
            if bound_vars.contains(&var) {
                continue;
            }
            owner.entry(var).or_insert(idx);
        }
    }

    let mut out = patterns.to_vec();
    for item in items {
        let SetItem::Property { expr, value } = item else {
            return None; // `+=`, label set, or whole-entity map assignment
        };
        let Expr::Property(base, prop) = expr else {
            return None; // not a property target
        };
        let Expr::Variable(var) = base.as_ref() else {
            return None; // e.g. `n[expr].x` or a deeper path
        };
        let Some(&idx) = owner.get(var) else {
            return None; // target is a MATCHed (not created) variable
        };
        // Evaluating the value at create time must equal evaluating it at SET
        // time: reject any reference to a variable created in this statement
        // (its value may not yet exist when the element is constructed).
        if collect_expr_variables(value)
            .iter()
            .any(|referenced| owner.contains_key(referenced))
        {
            return None;
        }
        if !merge_pattern_property(&mut out[idx], var, prop, value) {
            return None; // element absent or has a non-map property form
        }
    }
    Some(out)
}

/// Merge `var.prop = value` into the matching element's inline property map.
///
/// Returns `false` (leaving the pattern unchanged) when the variable's element
/// is not found or its existing properties are a non-map expression that cannot
/// be merged. Any pre-existing entry for `prop` is replaced so the SET's
/// last-write-wins precedence is preserved.
fn merge_pattern_property(pattern: &mut Pattern, var: &str, prop: &str, value: &Expr) -> bool {
    for path in &mut pattern.paths {
        if merge_into_elements(&mut path.elements, var, prop, value) {
            return true;
        }
    }
    false
}

/// Recursive worker for [`merge_pattern_property`] over a list of elements.
fn merge_into_elements(
    elements: &mut [PatternElement],
    var: &str,
    prop: &str,
    value: &Expr,
) -> bool {
    for element in elements {
        match element {
            PatternElement::Node(n) if n.variable.as_deref() == Some(var) => {
                return set_map_property(&mut n.properties, prop, value.clone());
            }
            PatternElement::Relationship(r) if r.variable.as_deref() == Some(var) => {
                return set_map_property(&mut r.properties, prop, value.clone());
            }
            PatternElement::Parenthesized { pattern, .. } => {
                if merge_into_elements(&mut pattern.elements, var, prop, value) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// Set `prop = value` on an optional inline property map, last-write-wins.
///
/// Returns `false` without mutating when the properties are present but are not
/// a map literal (e.g. `CREATE (n $params)`), which cannot accept a single key.
fn set_map_property(props: &mut Option<Expr>, prop: &str, value: Expr) -> bool {
    match props {
        None => {
            *props = Some(Expr::Map(vec![(prop.to_string(), value)]));
            true
        }
        Some(Expr::Map(entries)) => {
            entries.retain(|(k, _)| k != prop);
            entries.push((prop.to_string(), value));
            true
        }
        Some(_) => false,
    }
}

/// Resolve `startNode(r)` / `endNode(r)` to the node variable the traversal that
/// bound `r` already has in scope.
///
/// A relationship bound by a MATCH traversal has no whole-entity column: the
/// traversal produces `r._eid` and `r._type` and nothing else, so the
/// `startnode`/`endnode` UDF — which expects the relationship value as its first
/// argument — plans against a bare `r` column that does not exist and the query
/// fails with `Schema error: No field named r` (#187). The endpoint VIDs are not
/// on the edge columns either; what *is* available is the traversal's own
/// `source_variable` and `target_variable`, which are exactly the two nodes the
/// functions have to return.
///
/// So the resolution is static. For a single-hop traversal with a known
/// direction, `startNode(r)` is the variable bound at the relationship's tail and
/// `endNode(r)` the one at its head — reversed when the traversal runs against
/// the arrow. Rewriting to that variable makes the endpoint an ordinary variable
/// reference, which is worth more than making the UDF work: this pass runs before
/// `collect_properties_from_plan`, so `startNode(r).name` narrows to the single
/// column `n.name` instead of materialising the whole endpoint entity.
///
/// Deliberately **not** rewritten, each falling through to the UDF unchanged:
/// - undirected patterns (`-[r]-`), where which end is the start is a per-row
///   fact the plan cannot know;
/// - variable-length and quantified patterns, where the step variable holds a
///   *list* of relationships rather than one;
/// - a relationship that reaches the call any other way — bound by
///   `MERGE`/`CREATE`, or carried in a list through `UNWIND`/`collect` — where a
///   whole-entity column genuinely exists and the UDF path already works.
///
/// A binding stops being usable where its endpoints stop being projected, so at
/// each `Project` the map is narrowed to the relationships whose two endpoints
/// both survive, following bare-variable renames (`WITH n AS m`). `UNWIND` over a
/// name drops that name's binding for the same reason.
///
/// Idempotent: a rewritten plan holds no `startNode`/`endNode` call over a
/// traversal-bound relationship, so a second run changes nothing.
#[must_use]
pub fn resolve_traversal_endpoints(plan: LogicalPlan) -> LogicalPlan {
    let mut endpoints = EndpointScope::default();
    resolve_endpoints_node(plan, &mut endpoints)
}

/// The column an undirected traversal carries its per-row orientation on.
///
/// True when the row's *source* variable is bound to the vid Lance stores as the
/// edge's `_src_vid` — that is, when the traversal walked the edge forwards.
pub(crate) const COL_FWD: &str = "_fwd";

/// The node variables a traversal binds at the two ends of its relationship.
#[derive(Clone, Debug)]
enum TraversalEndpoints {
    /// The hop's direction is known at plan time, so each end is one variable.
    Static {
        /// Bound at the relationship's tail — what `startNode` returns.
        start: String,
        /// Bound at the relationship's head — what `endNode` returns.
        end: String,
    },
    /// An undirected hop. Which end is the relationship's tail is a per-row
    /// fact, so both candidates are kept and the choice is deferred to a `CASE`
    /// over the traversal's [`COL_FWD`] column.
    PerRow {
        /// The traversal's own source variable — the tail when `_fwd` is true.
        source: String,
        /// The traversal's own target variable — the tail when `_fwd` is false.
        target: String,
    },
}

impl TraversalEndpoints {
    /// The two node variables this binding depends on, in no particular order.
    ///
    /// Used to decide whether the binding survives a projection: it does only
    /// while both variables are still in scope.
    fn variables(&self) -> (&str, &str) {
        match self {
            Self::Static { start, end } => (start, end),
            Self::PerRow { source, target } => (source, target),
        }
    }

    /// The same binding with its two variables renamed, preserving the variant.
    fn renamed(&self, first: String, second: String) -> Self {
        match self {
            Self::Static { .. } => Self::Static {
                start: first,
                end: second,
            },
            Self::PerRow { .. } => Self::PerRow {
                source: first,
                target: second,
            },
        }
    }
}

/// What the endpoint pass carries up the plan tree.
///
/// `bindings` is the relationship -> endpoints map. `renames` exists because
/// this pass runs *after* planning: an `Aggregate`'s output columns were already
/// named by `Expr::to_string_repr()`, and the projection above refers to them by
/// that rendered string. Rewriting `collect(startNode(e).name)` into
/// `collect(x.name)` therefore changes the column's name out from under its own
/// consumer, which surfaces as `No field named "collect(startNode(e).name)"`.
/// Recording old-repr -> new-repr here and applying it to every expression above
/// keeps the two in step.
#[derive(Clone, Default)]
struct EndpointScope {
    bindings: HashMap<String, TraversalEndpoints>,
    renames: HashMap<String, String>,
}

/// Rewrite one expression for this scope: resolve endpoint calls, then follow
/// any aggregate-output renames the rewrite caused below.
fn rewrite_scoped(expr: Expr, scope: &EndpointScope) -> Expr {
    let expr = rewrite_endpoint_calls(expr, &scope.bindings);
    apply_renames(expr, &scope.renames)
}

/// Apply [`rewrite_scoped`] through an `Option<Expr>`.
fn rewrite_scoped_opt(expr: Option<Expr>, scope: &EndpointScope) -> Option<Expr> {
    expr.map(|e| rewrite_scoped(e, scope))
}

/// Follow renamed aggregate outputs, which are referenced as bare variables.
fn apply_renames(expr: Expr, renames: &HashMap<String, String>) -> Expr {
    if renames.is_empty() {
        return expr;
    }
    if let Expr::Variable(name) = &expr
        && let Some(renamed) = renames.get(name)
    {
        return Expr::Variable(renamed.clone());
    }
    expr.map_children(&mut |child| apply_renames(child, renames))
}

/// Pair a traversal's source/target variables with the relationship's ends.
///
/// `Direction::Incoming` means the traversal walks against the arrow, so the
/// traversal's *target* is the relationship's tail. `Direction::Both` cannot be
/// settled at plan time and becomes a [`TraversalEndpoints::PerRow`] binding
/// rather than nothing: the orientation is a fact the traversal knows per row
/// and reports on [`COL_FWD`], so the call is still resolvable — just not to a
/// single variable.
fn endpoints_for_direction(
    direction: &Direction,
    source_variable: &str,
    target_variable: &str,
) -> Option<TraversalEndpoints> {
    Some(match direction {
        Direction::Outgoing => TraversalEndpoints::Static {
            start: source_variable.to_string(),
            end: target_variable.to_string(),
        },
        Direction::Incoming => TraversalEndpoints::Static {
            start: target_variable.to_string(),
            end: source_variable.to_string(),
        },
        Direction::Both => TraversalEndpoints::PerRow {
            source: source_variable.to_string(),
            target: target_variable.to_string(),
        },
    })
}

/// The relationship an endpoint call names, when it is one of ours.
///
/// Returns the relationship variable and whether the call was `startNode`.
fn endpoint_call_target<'e>(
    expr: &'e Expr,
    endpoints: &HashMap<String, TraversalEndpoints>,
) -> Option<(&'e str, bool)> {
    let Expr::FunctionCall { name, args, .. } = expr else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    let Expr::Variable(rel) = &args[0] else {
        return None;
    };
    if !endpoints.contains_key(rel) {
        return None;
    }
    match name.to_ascii_lowercase().as_str() {
        "startnode" => Some((rel.as_str(), true)),
        "endnode" => Some((rel.as_str(), false)),
        _ => None,
    }
}

/// Replace every `startNode(r)`/`endNode(r)` over a known relationship with the
/// endpoint variable, leaving every other call untouched.
///
/// A statically-directed hop resolves outright. An undirected one cannot: which
/// end is the relationship's tail is a per-row fact, so there is no single
/// variable to rewrite to. Those are handled by
/// [`lift_per_row_endpoints`] in a second pass.
fn rewrite_endpoint_calls(expr: Expr, endpoints: &HashMap<String, TraversalEndpoints>) -> Expr {
    let expr = rewrite_static_endpoint_calls(expr, endpoints);
    lift_per_row_endpoints(expr, endpoints)
}

/// Resolve the calls whose endpoint is known at plan time.
fn rewrite_static_endpoint_calls(
    expr: Expr,
    endpoints: &HashMap<String, TraversalEndpoints>,
) -> Expr {
    if let Some((rel, is_start)) = endpoint_call_target(&expr, endpoints)
        && let Some(TraversalEndpoints::Static { start, end }) = endpoints.get(rel)
    {
        return Expr::Variable(if is_start { start.clone() } else { end.clone() });
    }
    expr.map_children(&mut |child| rewrite_static_endpoint_calls(child, endpoints))
}

/// The first undirected relationship an endpoint call in `expr` names.
fn first_per_row_rel(
    expr: &Expr,
    endpoints: &HashMap<String, TraversalEndpoints>,
) -> Option<String> {
    if let Some((rel, _)) = endpoint_call_target(expr, endpoints)
        && matches!(endpoints.get(rel), Some(TraversalEndpoints::PerRow { .. }))
    {
        return Some(rel.to_string());
    }
    let mut found = None;
    expr.for_each_child(&mut |child| {
        if found.is_none() {
            found = first_per_row_rel(child, endpoints);
        }
    });
    found
}

/// Replace `startNode(rel)`/`endNode(rel)` with fixed variables for one orientation.
fn substitute_endpoints(
    expr: Expr,
    rel: &str,
    start: &str,
    end: &str,
    endpoints: &HashMap<String, TraversalEndpoints>,
) -> Expr {
    if let Some((called, is_start)) = endpoint_call_target(&expr, endpoints)
        && called == rel
    {
        return Expr::Variable(if is_start {
            start.to_string()
        } else {
            end.to_string()
        });
    }
    expr.map_children(&mut |child| substitute_endpoints(child, rel, start, end, endpoints))
}

/// Resolve undirected endpoint calls by duplicating their enclosing expression
/// under a `CASE` on the traversal's per-row orientation.
///
/// `startNode(r).name` becomes
/// `CASE WHEN r._fwd THEN x.name ELSE y.name END`. Both branches reference
/// variables already in scope, so the endpoint's properties are materialised by
/// the ordinary property-collection pass — there is no lookup, and no
/// `{_vid}`-only stand-in that would make `id(startNode(r))` work while
/// `startNode(r).name` returned NULL.
///
/// **The `CASE` is never lifted across an aggregate.** `_fwd` varies per row
/// while an aggregate spans rows, so
/// `CASE WHEN r._fwd THEN count(x) ELSE count(y) END` is not
/// `count(CASE WHEN r._fwd THEN x ELSE y END)`: the first splits one group into
/// two and silently undercounts. Where the expression contains an aggregate the
/// rewrite descends into it instead, so the `CASE` lands inside the aggregate's
/// argument where it belongs.
fn lift_per_row_endpoints(expr: Expr, endpoints: &HashMap<String, TraversalEndpoints>) -> Expr {
    let Some(rel) = first_per_row_rel(&expr, endpoints) else {
        return expr;
    };
    if expr.is_aggregate() {
        return expr.map_children(&mut |child| lift_per_row_endpoints(child, endpoints));
    }
    let Some(TraversalEndpoints::PerRow { source, target }) = endpoints.get(&rel) else {
        return expr;
    };
    let forward = substitute_endpoints(expr.clone(), &rel, source, target, endpoints);
    let reverse = substitute_endpoints(expr, &rel, target, source, endpoints);
    Expr::Case {
        expr: None,
        when_then: vec![(
            Expr::Property(Box::new(Expr::Variable(rel.clone())), COL_FWD.to_string()),
            lift_per_row_endpoints(forward, endpoints),
        )],
        else_expr: Some(Box::new(lift_per_row_endpoints(reverse, endpoints))),
    }
}

/// Drop every binding that names `variable`, which is about to be rebound.
fn invalidate_binding(endpoints: &mut EndpointScope, variable: &str) {
    endpoints.bindings.retain(|rel, bound| {
        let (a, b) = bound.variables();
        rel != variable && a != variable && b != variable
    });
}

/// Narrow the bindings to those whose relationship and both endpoints survive a
/// projection, renaming through bare-variable aliases (`WITH n AS m`).
fn narrow_endpoints_through_projection(
    endpoints: &mut EndpointScope,
    projections: &[(Expr, Option<String>)],
) {
    // `WITH *` / `RETURN *` forwards the whole scope, so nothing is lost.
    if projections.iter().any(|(e, _)| matches!(e, Expr::Wildcard)) {
        return;
    }
    // Variable -> the names it is visible under above this projection. A dotted
    // name is a property access that survived an earlier transform, not a
    // whole-entity passthrough.
    let mut visible_as: HashMap<&str, String> = HashMap::new();
    for (expr, alias) in projections {
        if let Expr::Variable(v) = expr
            && !v.contains('.')
        {
            visible_as
                .entry(v.as_str())
                .or_insert_with(|| alias.clone().unwrap_or_else(|| v.clone()));
        }
    }
    let narrowed: HashMap<String, TraversalEndpoints> = endpoints
        .bindings
        .iter()
        .filter_map(|(rel, bound)| {
            let rel_out = visible_as.get(rel.as_str())?;
            let (a, b) = bound.variables();
            let first = visible_as.get(a)?;
            let second = visible_as.get(b)?;
            Some((
                rel_out.clone(),
                bound.renamed(first.clone(), second.clone()),
            ))
        })
        .collect();
    endpoints.bindings = narrowed;
}

/// The relationship binding a traversal contributes, when it is a single hop in
/// a known direction. Anything else — a variable-length step variable holding a
/// list, an undirected pattern, an unnamed relationship — contributes nothing.
fn single_hop_binding(
    direction: &Direction,
    source_variable: &str,
    target_variable: &str,
    step_variable: Option<&String>,
    is_variable_length: bool,
    min_hops: usize,
    max_hops: usize,
) -> Option<(String, TraversalEndpoints)> {
    let step = step_variable?;
    if is_variable_length || min_hops != 1 || max_hops != 1 {
        return None;
    }
    let bound = endpoints_for_direction(direction, source_variable, target_variable)?;
    Some((step.clone(), bound))
}

/// Rewrite one node, then update `endpoints` to what is in scope above it.
///
/// `endpoints` is threaded in/out: on entry it holds the bindings visible from
/// below, on return the bindings visible to this node's parent.
fn resolve_endpoints_node(plan: LogicalPlan, endpoints: &mut EndpointScope) -> LogicalPlan {
    match plan {
        // A traversal both carries expressions of its own and contributes the
        // binding every rewrite above it depends on.
        plan @ (LogicalPlan::Traverse { .. } | LogicalPlan::TraverseMainByType { .. }) => {
            let mut plan = plan.map_input(|input| resolve_endpoints_node(input, endpoints));
            let binding = match &mut plan {
                LogicalPlan::Traverse {
                    direction,
                    source_variable,
                    target_variable,
                    step_variable,
                    is_variable_length,
                    min_hops,
                    max_hops,
                    target_filter,
                    edge_filter_expr,
                    ..
                }
                | LogicalPlan::TraverseMainByType {
                    direction,
                    source_variable,
                    target_variable,
                    step_variable,
                    is_variable_length,
                    min_hops,
                    max_hops,
                    target_filter,
                    edge_filter_expr,
                    ..
                } => {
                    *target_filter = rewrite_scoped_opt(target_filter.take(), endpoints);
                    *edge_filter_expr = rewrite_scoped_opt(edge_filter_expr.take(), endpoints);
                    single_hop_binding(
                        direction,
                        source_variable,
                        target_variable,
                        step_variable.as_ref(),
                        *is_variable_length,
                        *min_hops,
                        *max_hops,
                    )
                }
                _ => None,
            };
            if let Some((step, bound)) = binding {
                endpoints.bindings.insert(step, bound);
            }
            plan
        }
        LogicalPlan::Filter {
            input,
            predicate,
            optional_variables,
        } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            LogicalPlan::Filter {
                input,
                predicate: rewrite_scoped(predicate, endpoints),
                optional_variables,
            }
        }
        LogicalPlan::Project { input, projections } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            let projections: Vec<(Expr, Option<String>)> = projections
                .into_iter()
                .map(|(expr, alias)| (rewrite_scoped(expr, endpoints), alias))
                .collect();
            narrow_endpoints_through_projection(endpoints, &projections);
            LogicalPlan::Project { input, projections }
        }
        LogicalPlan::Sort { input, order_by } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            let order_by = order_by
                .into_iter()
                .map(|item| SortItem {
                    expr: rewrite_scoped(item.expr, endpoints),
                    ascending: item.ascending,
                })
                .collect();
            LogicalPlan::Sort { input, order_by }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            // An aggregate's outputs are named by their rendered expression, and
            // the projection above refers to them by that name, so a rewrite here
            // has to be published upward or it renames a column its own consumer
            // is still looking for.
            let rewrite_and_record = |expr: Expr, scope: &mut EndpointScope| -> Expr {
                let before = expr.to_string_repr();
                let after = rewrite_scoped(expr, scope);
                let after_repr = after.to_string_repr();
                if before != after_repr {
                    scope.renames.insert(before, after_repr);
                }
                after
            };
            let group_by: Vec<Expr> = group_by
                .into_iter()
                .map(|expr| rewrite_and_record(expr, endpoints))
                .collect();
            let aggregates = aggregates
                .into_iter()
                .map(|expr| rewrite_and_record(expr, endpoints))
                .collect();
            // Only the grouping keys survive an aggregation, so a binding
            // outlives it on exactly the terms a projection would give it.
            let surviving: Vec<(Expr, Option<String>)> =
                group_by.iter().map(|e| (e.clone(), None)).collect();
            narrow_endpoints_through_projection(endpoints, &surviving);
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            }
        }
        LogicalPlan::Window {
            input,
            window_exprs,
        } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            let window_exprs = window_exprs
                .into_iter()
                .map(|expr| rewrite_scoped(expr, endpoints))
                .collect();
            LogicalPlan::Window {
                input,
                window_exprs,
            }
        }
        LogicalPlan::Unwind {
            input,
            expr,
            variable,
        } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            let expr = rewrite_scoped(expr, endpoints);
            invalidate_binding(endpoints, &variable);
            LogicalPlan::Unwind {
                input,
                expr,
                variable,
            }
        }
        LogicalPlan::Delete {
            input,
            items,
            detach,
        } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            let items = items
                .into_iter()
                .map(|expr| rewrite_scoped(expr, endpoints))
                .collect();
            LogicalPlan::Delete {
                input,
                items,
                detach,
            }
        }
        // Each branch is its own scope, and nothing a branch binds is reliably
        // addressable above the union, so the outer scope keeps only what it
        // arrived with.
        LogicalPlan::Union { left, right, all } => {
            let outer = endpoints.clone();
            let mut left_scope = outer.clone();
            let left = Box::new(resolve_endpoints_node(*left, &mut left_scope));
            let mut right_scope = outer.clone();
            let right = Box::new(resolve_endpoints_node(*right, &mut right_scope));
            *endpoints = outer;
            LogicalPlan::Union { left, right, all }
        }
        // A cross join concatenates two disjoint sets of variables, so both
        // sides' bindings are addressable above it.
        LogicalPlan::CrossJoin { left, right } => {
            let left = Box::new(resolve_endpoints_node(*left, endpoints));
            let right = Box::new(resolve_endpoints_node(*right, endpoints));
            LogicalPlan::CrossJoin { left, right }
        }
        // The subquery sees the outer bindings but does not export its own.
        LogicalPlan::Apply {
            input,
            subquery,
            input_filter,
        } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            let input_filter = rewrite_scoped_opt(input_filter, endpoints);
            let mut inner = endpoints.clone();
            let subquery = Box::new(resolve_endpoints_node(*subquery, &mut inner));
            LogicalPlan::Apply {
                input,
                subquery,
                input_filter,
            }
        }
        LogicalPlan::SubqueryCall { input, subquery } => {
            let input = Box::new(resolve_endpoints_node(*input, endpoints));
            let mut inner = endpoints.clone();
            let subquery = Box::new(resolve_endpoints_node(*subquery, &mut inner));
            LogicalPlan::SubqueryCall { input, subquery }
        }
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(resolve_endpoints_node(*plan, endpoints)),
        },
        // Everything else either carries no expression that can name a
        // relationship endpoint or is a leaf. `map_input` still walks the single
        // child, so a traversal underneath is reached and a rewrite above it
        // still fires.
        other => other.map_input(|input| resolve_endpoints_node(input, endpoints)),
    }
}

/// Walk a [`LogicalPlan`] tree and rewrite each `Scan` whose target
/// `(label, column)` has a registered fork-local index into the
/// matching `FusedIndexScan` variant.
///
/// Phase 5a-impl Step 4 covers `VidUidForkFirst`; Steps 5 and 6 add
/// `BtreeUnion` and `SortedKWayMerge` by extending `kind_for_filter`.
///
/// Idempotent: a tree that already contains `FusedIndexScan` nodes
/// passes through unchanged.
#[must_use]
pub fn rewrite_for_fork_fusion<L: ForkIndexLookup>(plan: LogicalPlan, lookup: &L) -> LogicalPlan {
    rewrite_node(plan, lookup)
}

fn rewrite_node<L: ForkIndexLookup>(plan: LogicalPlan, lookup: &L) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan {
            label_id,
            labels,
            variable,
            filter,
            optional,
        } => {
            // VidUid fusion only fires on a single-label scan with an
            // equality filter on a registered UID column. BTree and
            // Sorted will extend this match in Steps 5 and 6.
            let kind = if labels.len() == 1
                && let Some(col) = filter
                    .as_ref()
                    .and_then(|f| equality_target_column(f, &variable))
            {
                // Equality-scan fusion only applies to the scalar-equality
                // kinds (uid-equality first, then btree). A column can carry
                // several fork-local index kinds (e.g. `ScalarBtree` +
                // `FullText`), so probe for the equality-appropriate one in a
                // deterministic order rather than reading whichever value
                // happened to be stored.
                [
                    uni_store::fork::ForkLocalIndexKind::VidUid,
                    uni_store::fork::ForkLocalIndexKind::ScalarBtree,
                ]
                .into_iter()
                .find(|k| lookup.fork_index_has(&labels[0], &col, *k))
                .and_then(into_fusion_kind)
            } else {
                None
            };
            match kind {
                Some(kind) => LogicalPlan::FusedIndexScan {
                    label_id,
                    labels,
                    variable,
                    filter,
                    optional,
                    kind,
                },
                None => LogicalPlan::Scan {
                    label_id,
                    labels,
                    variable,
                    filter,
                    optional,
                },
            }
        }
        // Phase 5b followup: wrap lossy leaf operators when a
        // matching fork-local index has been registered. The wrap
        // preserves the original node's fields (the physical
        // planner unwraps and recurses); only the explain-plan
        // surface and runtime-stats operator name change. The
        // actual fusion still happens at the `BranchedBackend`
        // layer via Lance's per-branch reads.
        //
        // The CALL-style vector/FTS queries land as `ProcedureCall`
        // (not the dedicated `VectorKnn`/`InvertedIndexLookup`
        // operators); recognize those by procedure name and the
        // shape of their first two arguments (`label, column, ...`).
        LogicalPlan::ProcedureCall {
            procedure_name,
            arguments,
            yield_items,
        } => {
            let kind = procedure_call_fusion_kind(&procedure_name, &arguments, lookup);
            let inner = LogicalPlan::ProcedureCall {
                procedure_name,
                arguments,
                yield_items,
            };
            match kind {
                Some(kind) => LogicalPlan::FusedIndexScanWrapped {
                    inner: Box::new(inner),
                    kind,
                },
                None => inner,
            }
        }
        LogicalPlan::VectorKnn {
            label_id,
            variable,
            property,
            query,
            k,
            threshold,
        } => {
            if lookup.fork_index_has_label_id(
                label_id,
                &property,
                uni_store::fork::ForkLocalIndexKind::Vector,
            ) && let Some(kind) = into_fusion_kind(uni_store::fork::ForkLocalIndexKind::Vector)
            {
                LogicalPlan::FusedIndexScanWrapped {
                    inner: Box::new(LogicalPlan::VectorKnn {
                        label_id,
                        variable,
                        property,
                        query,
                        k,
                        threshold,
                    }),
                    kind,
                }
            } else {
                LogicalPlan::VectorKnn {
                    label_id,
                    variable,
                    property,
                    query,
                    k,
                    threshold,
                }
            }
        }
        LogicalPlan::InvertedIndexLookup {
            label_id,
            variable,
            property,
            terms,
        } => {
            if lookup.fork_index_has_label_id(
                label_id,
                &property,
                uni_store::fork::ForkLocalIndexKind::FullText,
            ) && let Some(kind) = into_fusion_kind(uni_store::fork::ForkLocalIndexKind::FullText)
            {
                LogicalPlan::FusedIndexScanWrapped {
                    inner: Box::new(LogicalPlan::InvertedIndexLookup {
                        label_id,
                        variable,
                        property,
                        terms,
                    }),
                    kind,
                }
            } else {
                LogicalPlan::InvertedIndexLookup {
                    label_id,
                    variable,
                    property,
                    terms,
                }
            }
        }
        // Tree-recursive variants — only the ones that can carry a
        // Scan in their subtree need to recurse here. Adding more is
        // safe (a missing recursion just means fusion doesn't fire
        // for that nested context, not incorrect results).
        LogicalPlan::Filter {
            input,
            predicate,
            optional_variables,
        } => LogicalPlan::Filter {
            input: Box::new(rewrite_node(*input, lookup)),
            predicate,
            optional_variables,
        },
        LogicalPlan::Project { input, projections } => LogicalPlan::Project {
            input: Box::new(rewrite_node(*input, lookup)),
            projections,
        },
        LogicalPlan::Limit { input, skip, fetch } => LogicalPlan::Limit {
            input: Box::new(rewrite_node(*input, lookup)),
            skip,
            fetch,
        },
        LogicalPlan::Sort { input, order_by } => {
            // Phase 5a-impl Sorted fusion: when the immediate child
            // is a single-label Scan AND the sole sort key is a
            // single-column property reference on that scan's
            // variable AND the column has a fork-local Sorted index
            // registered, rewrite to FusedIndexScan { SortedKWayMerge }.
            // Otherwise recurse normally.
            let new_input = match (*input, &order_by[..]) {
                (
                    LogicalPlan::Scan {
                        label_id,
                        labels,
                        variable,
                        filter,
                        optional,
                    },
                    [single_sort],
                ) if labels.len() == 1
                    && let Some(col) = column_of_scan_variable(&single_sort.expr, &variable)
                    && lookup.fork_index_has(
                        &labels[0],
                        &col,
                        uni_store::fork::ForkLocalIndexKind::Sorted,
                    ) =>
                {
                    LogicalPlan::FusedIndexScan {
                        label_id,
                        labels,
                        variable,
                        filter,
                        optional,
                        kind: FusionKind::SortedKWayMerge,
                    }
                }
                (other_input, _) => rewrite_node(other_input, lookup),
            };
            LogicalPlan::Sort {
                input: Box::new(new_input),
                order_by,
            }
        }
        LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
            left: Box::new(rewrite_node(*left, lookup)),
            right: Box::new(rewrite_node(*right, lookup)),
            all,
        },
        // Everything else passes through unchanged. Adding more
        // arms is purely additive — fusion just doesn't fire inside
        // un-recursed-into subtrees.
        other => other,
    }
}

/// Phase 5b followup: inspect a CALL-style procedure invocation
/// for a `(label, column)` pair and check whether a fork-local
/// index has been registered for it.
///
/// Recognizes:
/// - `uni.vector.query(label, column, query_vec, k)` → `AnnRerank`
///   when a `Vector` fork-local index exists.
/// - `uni.fts.query(label, column, query, k)` → `Bm25Rrf` when a
///   `FullText` fork-local index exists.
/// - `uni.sparse.query(label, column, query_vec, k)` → `SparseDot`
///   when a `Sparse` fork-local index marker exists.
///
/// Returns `None` for any other procedure (no rewrite) or when the
/// registry has no matching entry.
fn procedure_call_fusion_kind<L: ForkIndexLookup>(
    procedure_name: &str,
    arguments: &[Expr],
    lookup: &L,
) -> Option<FusionKind> {
    if arguments.len() < 2 {
        return None;
    }

    // `uni.search` hybrid: a `sparse` key in the inline properties map means the
    // call fuses a learned-sparse source via RRF (`run_hybrid_search`). This is
    // independent of fork-local indexes, so it is not gated on `lookup`.
    // Limitation: a properties map passed as a `$param` (not an inline
    // `Expr::Map`) is opaque here and stays unlabeled.
    if procedure_name == "uni.search" {
        if let Expr::Map(entries) = &arguments[1]
            && entries.iter().any(|(key, _)| key.as_str() == "sparse")
        {
            return Some(FusionKind::SparseRrf);
        }
        return None;
    }

    let label = match &arguments[0] {
        Expr::Literal(uni_cypher::ast::CypherLiteral::String(s)) => s.as_str(),
        _ => return None,
    };
    let column = match &arguments[1] {
        Expr::Literal(uni_cypher::ast::CypherLiteral::String(s)) => s.as_str(),
        _ => return None,
    };
    let expected = match procedure_name {
        "uni.vector.query" => uni_store::fork::ForkLocalIndexKind::Vector,
        "uni.fts.query" => uni_store::fork::ForkLocalIndexKind::FullText,
        // `uni.sparse.query` fork-fusion observability: a registered fork-local
        // `Sparse` marker (issue #95 Task #4) switches the call to the `SparseDot`
        // fused operator. Retrieval itself is a brute-force branch scan re-scored
        // by `sparse_dot` (`StorageManager::sparse_search`); the marker drives the
        // planner/EXPLAIN view, the `AnnRerank`/`Bm25Rrf` analogue.
        "uni.sparse.query" => uni_store::fork::ForkLocalIndexKind::Sparse,
        _ => return None,
    };
    if !lookup.fork_index_has(label, column, expected) {
        return None;
    }
    into_fusion_kind(expected)
}

/// Map a fork-local index kind to its planner-side fusion variant.
/// Returns `None` for any future `ForkLocalIndexKind` we don't yet
/// know how to fuse — the caller falls back to a regular Scan.
fn into_fusion_kind(kind: uni_store::fork::ForkLocalIndexKind) -> Option<FusionKind> {
    use uni_store::fork::ForkLocalIndexKind as K;
    match kind {
        K::VidUid => Some(FusionKind::VidUidForkFirst),
        K::ScalarBtree => Some(FusionKind::BtreeUnion),
        K::Sorted => Some(FusionKind::SortedKWayMerge),
        K::Vector => Some(FusionKind::AnnRerank),
        K::FullText => Some(FusionKind::Bm25Rrf),
        K::Sparse => Some(FusionKind::SparseDot),
        // `ForkLocalIndexKind` is `#[non_exhaustive]`; future kinds
        // we don't yet handle are silently passed through as a
        // regular Scan so a forward-incompatible binary doesn't
        // panic — just misses the fusion opportunity.
        _ => None,
    }
}

/// Inspect a Scan filter `Expr` for a single-column equality predicate
/// against the scan's variable. Returns the column name if the
/// predicate matches the shape `variable.column = <literal_or_param>`
/// (or its commuted form). Returns `None` for any other shape — fusion
/// only fires on the simple case in Phase 5a-impl.
fn equality_target_column(filter: &Expr, scan_variable: &str) -> Option<String> {
    let (lhs, rhs) = match filter {
        Expr::BinaryOp {
            left,
            op: uni_cypher::ast::BinaryOp::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };
    // Try lhs = column-of-scan-var, rhs = literal/param; or commuted.
    if let Some(col) = column_of_scan_variable(lhs, scan_variable)
        && is_constant_or_param(rhs)
    {
        return Some(col);
    }
    if let Some(col) = column_of_scan_variable(rhs, scan_variable)
        && is_constant_or_param(lhs)
    {
        return Some(col);
    }
    None
}

fn column_of_scan_variable(expr: &Expr, scan_variable: &str) -> Option<String> {
    if let Expr::Property(base, prop) = expr
        && let Expr::Variable(v) = base.as_ref()
        && v == scan_variable
    {
        return Some(prop.clone());
    }
    None
}

fn is_constant_or_param(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_) | Expr::Parameter(_))
}

#[cfg(test)]
mod pushdown_tests {
    use super::*;

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

    // R6 / uni-query[22]: the Apply input_filter gate must admit ONLY the shapes
    // the df_graph::apply fast-path evaluator can resolve, and reject every other
    // operator/shape (which stays as a residual Filter). This is the load-bearing
    // invariant that keeps the two files in lockstep; guard it directly.
    #[test]
    fn apply_input_filter_gate_accepts_only_supported_shapes() {
        use uni_cypher::ast::{BinaryOp, UnaryOp};

        fn prop(var: &str, key: &str) -> Expr {
            Expr::Property(Box::new(Expr::Variable(var.into())), key.into())
        }
        fn lit_s(s: &str) -> Expr {
            Expr::Literal(CypherLiteral::String(s.into()))
        }
        fn lit_i(i: i64) -> Expr {
            Expr::Literal(CypherLiteral::Integer(i))
        }
        let cmp = |op: BinaryOp, l: Expr, r: Expr| Expr::BinaryOp {
            left: Box::new(l),
            op,
            right: Box::new(r),
        };

        // ACCEPTED: comparisons over literal/variable/var.key operands, And/Or/Not,
        // and a bare property truth test.
        assert!(QueryPlanner::apply_input_filter_supported(&cmp(
            BinaryOp::Eq,
            prop("a", "name"),
            lit_s("Alice")
        )));
        assert!(QueryPlanner::apply_input_filter_supported(&cmp(
            BinaryOp::Gt,
            prop("a", "x"),
            lit_i(10)
        )));
        assert!(QueryPlanner::apply_input_filter_supported(&cmp(
            BinaryOp::And,
            cmp(BinaryOp::Eq, prop("a", "name"), lit_s("Alice")),
            cmp(BinaryOp::Gt, prop("a", "x"), lit_i(1)),
        )));
        assert!(QueryPlanner::apply_input_filter_supported(&Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(cmp(BinaryOp::Eq, prop("a", "name"), lit_s("Bob"))),
        }));
        assert!(QueryPlanner::apply_input_filter_supported(&prop(
            "a", "active"
        )));

        // REJECTED: string operators, regex, arithmetic operand, IN, CASE,
        // function calls, and NOT/OR wrapping an unsupported shape.
        for op in [
            BinaryOp::StartsWith,
            BinaryOp::EndsWith,
            BinaryOp::Contains,
            BinaryOp::Regex,
        ] {
            assert!(
                !QueryPlanner::apply_input_filter_supported(&cmp(
                    op,
                    prop("a", "name"),
                    lit_s("x")
                )),
                "string/regex operator must be rejected: {op:?}"
            );
        }
        // Arithmetic operand under a supported comparison.
        assert!(!QueryPlanner::apply_input_filter_supported(&cmp(
            BinaryOp::Gt,
            cmp(BinaryOp::Add, prop("a", "x"), lit_i(1)),
            lit_i(100),
        )));
        // IN — dedicated Expr variant.
        assert!(!QueryPlanner::apply_input_filter_supported(&Expr::In {
            expr: Box::new(prop("a", "name")),
            list: Box::new(Expr::List(vec![lit_s("Zed")])),
        }));
        // CASE — dedicated Expr variant.
        assert!(!QueryPlanner::apply_input_filter_supported(&Expr::Case {
            expr: None,
            when_then: vec![(lit_s("x"), lit_s("y"))],
            else_expr: None,
        }));
        // Function call as an operand.
        assert!(!QueryPlanner::apply_input_filter_supported(&cmp(
            BinaryOp::Eq,
            Expr::FunctionCall {
                name: "toUpper".into(),
                args: vec![prop("a", "name")],
                distinct: false,
                window_spec: None,
            },
            lit_s("ALICE"),
        )));
        // NOT / OR wrapping an unsupported shape must not sneak through.
        assert!(!QueryPlanner::apply_input_filter_supported(
            &Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(cmp(BinaryOp::Contains, prop("a", "name"), lit_s("li"))),
            }
        ));
        assert!(!QueryPlanner::apply_input_filter_supported(&cmp(
            BinaryOp::Or,
            cmp(BinaryOp::Eq, prop("a", "name"), lit_s("Alice")),
            cmp(BinaryOp::Contains, prop("a", "name"), lit_s("li")),
        )));
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

    // ---- issue #134: scalar-function-over-entity must not leak "*" ----

    /// Build a non-window, non-distinct function-call expression for tests.
    fn func(name: &str, args: Vec<Expr>) -> Expr {
        Expr::FunctionCall {
            name: name.to_string(),
            args,
            distinct: false,
            window_spec: None,
        }
    }

    fn collect(expr: &Expr) -> HashMap<String, HashSet<String>> {
        let mut properties = HashMap::new();
        collect_properties_from_expr_into(expr, &mut properties);
        properties
    }

    /// True if `var` was marked as needing all properties (`*`).
    fn widened(properties: &HashMap<String, HashSet<String>>, var: &str) -> bool {
        properties.get(var).is_some_and(|s| s.contains("*"))
    }

    #[test]
    fn test_entity_arg_functions_do_not_widen() {
        // id(n)/elementId(n)/count(n) take an entity but need no properties:
        // the variable must NOT be widened to "*" (would defeat projection).
        for name in ["id", "elementId", "count"] {
            let properties = collect(&func(name, vec![Expr::Variable("n".to_string())]));
            assert!(
                !widened(&properties, "n"),
                "{name}(n) must not widen n to '*'"
            );
        }
    }

    #[test]
    fn test_type_and_endpoint_functions_do_not_widen() {
        // type(r)/startNode(r)/endNode(r) need only edge metadata, not props.
        for name in ["type", "startNode", "endNode"] {
            let properties = collect(&func(name, vec![Expr::Variable("r".to_string())]));
            assert!(
                !widened(&properties, "r"),
                "{name}(r) must not widen r to '*'"
            );
        }
    }

    #[test]
    fn test_created_at_maps_to_timestamp_column_only() {
        // created_at(n) → n: {_created_at}, never "*".
        let properties = collect(&func("created_at", vec![Expr::Variable("n".to_string())]));
        assert!(!widened(&properties, "n"), "created_at(n) must not widen n");
        assert!(properties.get("n").unwrap().contains("_created_at"));
    }

    #[test]
    fn test_full_entity_functions_still_widen() {
        // Guard against over-fixing: keys(n)/properties(n) genuinely need "*".
        for name in ["keys", "properties"] {
            let properties = collect(&func(name, vec![Expr::Variable("n".to_string())]));
            assert!(widened(&properties, "n"), "{name}(n) must still widen n");
        }
    }

    #[test]
    fn test_collect_whole_node_still_widens() {
        // collect(n) materializes whole nodes into a list — "*" is correct here.
        let properties = collect(&func("collect", vec![Expr::Variable("n".to_string())]));
        assert!(widened(&properties, "n"), "collect(n) must widen n to '*'");
    }

    #[test]
    fn test_distinct_count_over_entity_does_not_widen() {
        // count(DISTINCT r) dedups on the identity column (`r._eid`, rewritten in
        // df_planner), so the property struct must NOT be materialized — r must
        // not widen to '*' (issue #134 family; df_planner handles the identity
        // rewrite so openCypher Return6 [16] still passes).
        let call = Expr::FunctionCall {
            name: "count".to_string(),
            args: vec![Expr::Variable("r".to_string())],
            distinct: true,
            window_spec: None,
        };
        let properties = collect(&call);
        assert!(
            !widened(&properties, "r"),
            "count(DISTINCT r) must not widen r to '*'"
        );
    }

    #[test]
    fn test_aggregate_over_property_narrows() {
        // sum(n.x) reads only n.x — no wildcard, just the accessed property.
        let arg = Expr::Property(Box::new(Expr::Variable("n".to_string())), "x".to_string());
        let properties = collect(&func("sum", vec![arg]));
        assert!(!widened(&properties, "n"), "sum(n.x) must not widen n");
        assert!(properties.get("n").unwrap().contains("x"));
    }

    // ---- WITH-passthrough narrowing (issue #134 family, Phase B) ----

    fn scan(var: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            label_id: 0,
            labels: vec!["Doc".to_string()],
            variable: var.to_string(),
            filter: None,
            optional: false,
        }
    }

    fn project(input: LogicalPlan, items: Vec<(Expr, Option<String>)>) -> LogicalPlan {
        LogicalPlan::Project {
            input: Box::new(input),
            projections: items,
        }
    }

    fn prop(var: &str, name: &str) -> Expr {
        Expr::Property(Box::new(Expr::Variable(var.to_string())), name.to_string())
    }

    /// Run the full collect + reconcile pipeline over `plan`, treating every
    /// listed variable as a narrowable (Node/Edge) entity.
    fn reconciled(plan: &LogicalPlan, narrowable: &[&str]) -> HashMap<String, HashSet<String>> {
        let mut props = collect_properties_from_plan(plan);
        let set: HashSet<String> = narrowable.iter().map(|s| s.to_string()).collect();
        reconcile_passthrough_properties(plan, &mut props, &set);
        props
    }

    fn aggregate(input: LogicalPlan, group_by: Vec<Expr>, aggregates: Vec<Expr>) -> LogicalPlan {
        LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by,
            aggregates,
        }
    }

    /// `MATCH (p)-[]-() WITH p, count(*) RETURN p.id` must materialise only
    /// `id` for `p`.
    ///
    /// A group key needs the entity's *identity*; grouping cannot depend on a
    /// property the query never reads. Marking it "*" pulled the whole schema —
    /// `_all_props` and `overflow_json` included — into both the scan and the
    /// physical group key, which is what made this shape request 1.76 GB at
    /// LDBC SF1 for a query that reads one property (#196).
    #[test]
    fn test_group_key_entity_does_not_widen() {
        let plan = project(
            aggregate(
                scan("p"),
                vec![Expr::Variable("p".to_string())],
                vec![func("count", vec![Expr::Wildcard])],
            ),
            vec![(prop("p", "id"), Some("id".to_string()))],
        );
        let props = reconciled(&plan, &["p"]);
        assert!(
            !widened(&props, "p"),
            "a bare group key widened p to '*': {:?}",
            props.get("p")
        );
        assert!(
            props.get("p").unwrap().contains("id"),
            "the property the query actually reads must survive: {:?}",
            props.get("p")
        );
    }

    /// The control that must not move. `collect(n)` returns the entity whole, so
    /// narrowing it would be a wrong answer rather than a smaller one — and the
    /// fix above touches group keys only, never aggregate arguments.
    #[test]
    fn test_group_key_narrowing_does_not_touch_aggregate_arguments() {
        let plan = project(
            aggregate(
                scan("n"),
                vec![],
                vec![func("collect", vec![Expr::Variable("n".to_string())])],
            ),
            vec![(Expr::Variable("collect(n)".to_string()), None)],
        );
        let props = reconciled(&plan, &["n"]);
        assert!(
            widened(&props, "n"),
            "collect(n) must still widen n to '*': {:?}",
            props.get("n")
        );
    }

    /// A group key that *is* returned whole stays wide. `reconcile_passthrough_properties`
    /// already makes that call for projections; this pins that the group-key
    /// marker reaches the same decision rather than narrowing unconditionally.
    #[test]
    fn test_group_key_returned_whole_stays_wide() {
        let plan = project(
            aggregate(
                scan("p"),
                vec![Expr::Variable("p".to_string())],
                vec![func("count", vec![Expr::Wildcard])],
            ),
            vec![(Expr::Variable("p".to_string()), None)],
        );
        let props = reconciled(&plan, &["p"]);
        assert!(
            widened(&props, "p"),
            "a group key returned whole must stay '*': {:?}",
            props.get("p")
        );
    }

    #[test]
    fn test_with_passthrough_narrows_forwarded_variable() {
        // MATCH (n) WITH n RETURN n.title → n materializes only {title}.
        let plan = project(
            project(scan("n"), vec![(Expr::Variable("n".to_string()), None)]),
            vec![(prop("n", "title"), None)],
        );
        let props = reconciled(&plan, &["n"]);
        let n = props.get("n").expect("n present");
        assert!(!n.contains("*"), "forwarded n must not stay wide");
        assert!(n.contains(STRUCT_ONLY_SENTINEL), "n must be struct-only");
        assert!(n.contains("title"), "n must keep the accessed property");
        assert!(!n.contains(WITH_PASSTHROUGH_SENTINEL), "no marker survives");
    }

    #[test]
    fn test_returned_whole_entity_stays_wide() {
        // MATCH (n) WITH n RETURN n → n is returned whole, must keep "*".
        let plan = project(
            project(scan("n"), vec![(Expr::Variable("n".to_string()), None)]),
            vec![(Expr::Variable("n".to_string()), None)],
        );
        let props = reconciled(&plan, &["n"]);
        assert!(
            props.get("n").unwrap().contains("*"),
            "returned n stays wide"
        );
    }

    #[test]
    fn test_with_rename_folds_alias_props_onto_source() {
        // MATCH (n) WITH n AS m RETURN m.title → source n materializes {title}.
        let plan = project(
            project(
                scan("n"),
                vec![(Expr::Variable("n".to_string()), Some("m".to_string()))],
            ),
            vec![(prop("m", "title"), None)],
        );
        let props = reconciled(&plan, &["n"]);
        let n = props.get("n").expect("source n present");
        assert!(!n.contains("*"), "renamed source must not stay wide");
        assert!(n.contains(STRUCT_ONLY_SENTINEL));
        assert!(
            n.contains("title"),
            "alias's accessed property must fold onto source (silent-NULL guard)"
        );
    }

    #[test]
    fn test_with_rename_returned_whole_keeps_source_wide() {
        // MATCH (n) WITH n AS m RETURN m → m returns the whole entity, so the
        // source n must stay wide (else m loses properties).
        let plan = project(
            project(
                scan("n"),
                vec![(Expr::Variable("n".to_string()), Some("m".to_string()))],
            ),
            vec![(Expr::Variable("m".to_string()), None)],
        );
        let props = reconciled(&plan, &["n"]);
        assert!(
            props.get("n").unwrap().contains("*"),
            "source of a whole-returned alias stays wide"
        );
    }

    /// Locates where IC4's oversized `DISTINCT` key comes from (#203).
    ///
    /// `WITH DISTINCT tag, post RETURN count(*)` reads no property of either
    /// entity, yet at LDBC SF1 it asks for 1.4 GB in
    /// `GroupedHashAggregateStream` — `DISTINCT` plans as a grouped aggregate
    /// keyed on every projected column, so an entity's full struct becomes hash
    /// key material. Measured on the same store, the identical query keyed on
    /// `tag.id, post.id` at the same 522 952 distinct rows completes.
    ///
    /// **This is a control, not a repro — it passed the first time it was run.**
    /// It was written to test whether the width enters through the *projection*
    /// path, and the answer is no: both variables narrow here, and
    /// `resolve_properties` turns a sentinel-only set into an empty projection.
    /// The `Traverse` arm likewise collects only from `target_filter`, so the
    /// whole logical layer is exonerated and #203's width enters below it, in
    /// physical target hydration.
    ///
    /// Kept because it pins that exoneration: if this ever goes red, the
    /// projection path has regressed and #203's analysis needs revisiting.
    #[test]
    fn test_issue_203_distinct_over_entities_narrows_when_nothing_is_read() {
        let forwarded = project(
            scan("post"),
            vec![
                (Expr::Variable("tag".to_string()), None),
                (Expr::Variable("post".to_string()), None),
            ],
        );
        let plan = project(
            LogicalPlan::Distinct {
                input: Box::new(forwarded),
            },
            vec![(func("count", vec![]), Some("n".to_string()))],
        );

        let props = reconciled(&plan, &["tag", "post"]);
        for var in ["tag", "post"] {
            let set = props
                .get(var)
                .unwrap_or_else(|| panic!("{var} must be collected"));
            assert!(
                !set.contains("*"),
                "{var} is forwarded only into a DISTINCT and no property of it is \
                 ever read, so its identity suffices — keeping it wide puts the \
                 whole struct in the hash key (#203). Got: {set:?}"
            );
        }
    }

    #[test]
    fn test_non_narrowable_variable_stays_wide() {
        // A forwarded variable that is not a narrowable entity (e.g. a path)
        // must be kept wide.
        let plan = project(
            project(scan("p"), vec![(Expr::Variable("p".to_string()), None)]),
            vec![(prop("p", "x"), None)],
        );
        let props = reconciled(&plan, &[]); // p NOT narrowable
        assert!(
            props.get("p").unwrap().contains("*"),
            "non-entity stays wide"
        );
    }

    #[test]
    fn test_issue_134_dense_scan_projects_only_referenced_column() {
        // Mirrors issue #134: `RETURN id(n), similar_to([n.embedding], [$q])`.
        // Only `embedding` must be projected; id(n) must not pull the full
        // schema (which would decode unread wide columns like a ColBERT list).
        let mut properties = HashMap::new();
        collect_properties_from_expr_into(
            &func("id", vec![Expr::Variable("n".to_string())]),
            &mut properties,
        );
        collect_properties_from_expr_into(
            &func(
                "similar_to",
                vec![
                    Expr::List(vec![Expr::Property(
                        Box::new(Expr::Variable("n".to_string())),
                        "embedding".to_string(),
                    )]),
                    Expr::List(vec![Expr::Parameter("q".to_string())]),
                ],
            ),
            &mut properties,
        );

        assert!(!widened(&properties, "n"), "n must not be widened to '*'");
        let n_props = properties.get("n").expect("n should need embedding");
        assert!(n_props.contains("embedding"));
        assert_eq!(n_props.len(), 1, "only embedding should be projected");
    }
}

#[cfg(test)]
mod fts_tokenizer_option_tests {
    use super::*;
    use uni_common::Value;
    use uni_common::core::schema::{BaseTokenizer, FtsLanguage, TokenizerConfig};

    fn opts(pairs: &[(&str, Value)]) -> std::collections::HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn no_options_yields_standard() {
        let cfg = QueryPlanner::parse_tokenizer_options(&opts(&[])).unwrap();
        assert_eq!(cfg, TokenizerConfig::Standard);
    }

    #[test]
    fn analyzer_language_stemmer_stopwords_roundtrip() {
        let o = opts(&[
            ("analyzer", Value::String("standard".into())),
            ("language", Value::String("english".into())),
            ("stemmer", Value::Bool(true)),
            (
                "stopwords",
                Value::List(vec![Value::String("the".into()), Value::String("a".into())]),
            ),
        ]);
        let cfg = QueryPlanner::parse_tokenizer_options(&o).unwrap();
        match cfg {
            TokenizerConfig::Analyzer(a) => {
                assert_eq!(a.base, BaseTokenizer::Simple);
                assert_eq!(a.language, FtsLanguage::English);
                assert!(a.stem);
                assert!(a.remove_stop_words);
                assert_eq!(
                    a.custom_stop_words,
                    Some(vec!["the".to_string(), "a".to_string()])
                );
            }
            other => panic!("expected Analyzer, got {other:?}"),
        }
    }

    #[test]
    fn whitespace_analyzer_maps_base() {
        let o = opts(&[("tokenizer", Value::String("whitespace".into()))]);
        let cfg = QueryPlanner::parse_tokenizer_options(&o).unwrap();
        match cfg {
            TokenizerConfig::Analyzer(a) => assert_eq!(a.base, BaseTokenizer::Whitespace),
            other => panic!("expected Analyzer, got {other:?}"),
        }
    }

    #[test]
    fn ngram_options_map_bounds() {
        let o = opts(&[
            ("analyzer", Value::String("ngram".into())),
            ("ngram_min", Value::Int(2)),
            ("ngram_max", Value::Int(4)),
        ]);
        let cfg = QueryPlanner::parse_tokenizer_options(&o).unwrap();
        match cfg {
            TokenizerConfig::Analyzer(a) => {
                assert_eq!(a.base, BaseTokenizer::Ngram { min: 2, max: 4 })
            }
            other => panic!("expected Analyzer, got {other:?}"),
        }
    }

    #[test]
    fn ngram_bad_bounds_rejected() {
        let o = opts(&[
            ("analyzer", Value::String("ngram".into())),
            ("ngram_min", Value::Int(5)),
            ("ngram_max", Value::Int(2)),
        ]);
        assert!(QueryPlanner::parse_tokenizer_options(&o).is_err());
    }

    #[test]
    fn unknown_language_rejected() {
        let o = opts(&[("language", Value::String("klingon".into()))]);
        let err = QueryPlanner::parse_tokenizer_options(&o)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown FTS language"), "{err}");
    }

    #[test]
    fn custom_tokenizer_passthrough() {
        let o = opts(&[("analyzer", Value::String("jieba/default".into()))]);
        let cfg = QueryPlanner::parse_tokenizer_options(&o).unwrap();
        match cfg {
            TokenizerConfig::Analyzer(a) => {
                assert_eq!(a.base, BaseTokenizer::Custom("jieba/default".into()))
            }
            other => panic!("expected Analyzer, got {other:?}"),
        }
    }

    #[test]
    fn stopwords_bool_false_disables() {
        let o = opts(&[
            ("analyzer", Value::String("standard".into())),
            ("stopwords", Value::Bool(false)),
        ]);
        let cfg = QueryPlanner::parse_tokenizer_options(&o).unwrap();
        match cfg {
            TokenizerConfig::Analyzer(a) => {
                assert!(!a.remove_stop_words);
                assert_eq!(a.custom_stop_words, None);
            }
            other => panic!("expected Analyzer, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod dead_unwind_source_tests {
    use super::*;

    /// `WITH collect(x) AS xs UNWIND xs AS f …`, with `body` above the UNWIND.
    ///
    /// The lower projection is a bare-variable passthrough of the aggregate's
    /// output column, which is the shape the real planner produces and the one
    /// that matters: it records `xs → __alias_of__collect(friend)`, so a
    /// liveness test based on mere presence in the properties map calls `xs`
    /// live and prunes nothing.
    fn collect_unwind_plan(body: Vec<(Expr, Option<String>)>) -> LogicalPlan {
        let collected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Empty),
            projections: vec![(
                Expr::Variable("collect(friend)".to_string()),
                Some("xs".to_string()),
            )],
        };
        let unwound = LogicalPlan::Unwind {
            input: Box::new(collected),
            expr: Expr::Variable("xs".to_string()),
            variable: "f".to_string(),
        };
        LogicalPlan::Project {
            input: Box::new(unwound),
            projections: body,
        }
    }

    fn dead(plan: &LogicalPlan) -> HashSet<String> {
        let mut properties = HashMap::new();
        mark_dead_unwind_sources(plan, &mut properties);
        properties
            .get(DEAD_UNWIND_SOURCES_KEY)
            .cloned()
            .unwrap_or_default()
    }

    #[test]
    fn a_collected_list_is_dead_once_unwind_has_consumed_it() {
        // Only `f` is read above, so the list must not ride through (#184).
        let plan = collect_unwind_plan(vec![(Expr::Variable("f".to_string()), None)]);
        assert!(dead(&plan).contains("xs"), "xs should be prunable");
    }

    #[test]
    fn a_list_still_returned_is_not_dead() {
        let plan = collect_unwind_plan(vec![
            (Expr::Variable("f".to_string()), None),
            (Expr::Variable("xs".to_string()), None),
        ]);
        assert!(
            !dead(&plan).contains("xs"),
            "xs is returned, so pruning it would lose a column"
        );
    }

    #[test]
    fn a_property_read_of_the_list_keeps_it() {
        let plan = collect_unwind_plan(vec![(
            Expr::FunctionCall {
                name: "size".to_string(),
                args: vec![Expr::Variable("xs".to_string())],
                distinct: false,
                window_spec: None,
            },
            Some("n".to_string()),
        )]);
        assert!(!dead(&plan).contains("xs"), "size(xs) reads xs");
    }

    #[test]
    fn a_wildcard_anywhere_stands_the_analysis_down() {
        // `RETURN *` names nothing, so absence from the map proves nothing.
        let plan = collect_unwind_plan(vec![(Expr::Wildcard, None)]);
        assert!(
            dead(&plan).is_empty(),
            "a wildcard must disable pruning entirely"
        );
    }

    #[test]
    fn a_list_unwound_twice_is_not_pruned() {
        // Blanking removes both UNWIND expressions at once, so each would look
        // unreferenced by the other. Only a single use is safe.
        let inner = collect_unwind_plan(vec![(Expr::Variable("f".to_string()), None)]);
        let plan = LogicalPlan::Unwind {
            input: Box::new(inner),
            expr: Expr::Variable("xs".to_string()),
            variable: "g".to_string(),
        };
        assert!(
            !dead(&plan).contains("xs"),
            "two UNWINDs over one list must not prune it"
        );
    }

    #[test]
    fn a_non_variable_source_has_no_column_to_drop() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Unwind {
                input: Box::new(LogicalPlan::Empty),
                expr: Expr::List(vec![Expr::Literal(CypherLiteral::Integer(1))]),
                variable: "i".to_string(),
            }),
            projections: vec![(Expr::Variable("i".to_string()), None)],
        };
        assert!(dead(&plan).is_empty());
    }

    // ---- #197: a read hidden inside a subquery body ----
    //
    // `mark_dead_unwind_sources` proves a source dead by *absence*, so any read
    // the AST walker cannot see is a read that does not exist and the column is
    // dropped out from under its reader. Each case below is a shape whose only
    // read of `xs` sits somewhere the walker used to skip; all four were
    // measured returning `{"xs"}` — wrongly dead — before the fix.

    use uni_cypher::ast::{LabelExpr, UnwindClause};

    /// A one-element MATCH pattern, `(r:P {name: <value>})`.
    fn match_with_inline_property(value: Expr) -> Clause {
        Clause::Match(MatchClause {
            optional: false,
            pattern: Pattern {
                paths: vec![PathPattern {
                    variable: None,
                    elements: vec![PatternElement::Node(NodePattern {
                        variable: Some("r".to_string()),
                        labels: LabelExpr::Conjunction(vec!["P".to_string()]),
                        properties: Some(Expr::Map(vec![("name".to_string(), value)])),
                        where_clause: None,
                    })],
                    shortest_path_mode: None,
                }],
            },
            where_clause: None,
            for_update: false,
        })
    }

    fn single(clauses: Vec<Clause>) -> Box<Query> {
        Box::new(Query::Single(Statement { clauses }))
    }

    /// `… RETURN f, EXISTS { <body> }` above the UNWIND.
    fn plan_with_exists_body(body: Box<Query>) -> LogicalPlan {
        collect_unwind_plan(vec![
            (Expr::Variable("f".to_string()), None),
            (
                Expr::Exists {
                    query: body,
                    from_pattern_predicate: false,
                },
                Some("e".to_string()),
            ),
        ])
    }

    #[test]
    fn a_read_in_an_exists_pattern_property_map_keeps_the_list() {
        // `EXISTS { MATCH (r:P {name: xs}) }` — the read is in the pattern, not
        // in the clause's WHERE, so the Match arm used to walk straight past it.
        let plan = plan_with_exists_body(single(vec![match_with_inline_property(Expr::Variable(
            "xs".to_string(),
        ))]));
        assert!(
            !dead(&plan).contains("xs"),
            "an inline property map reads xs"
        );
    }

    #[test]
    fn a_read_in_a_pattern_element_where_keeps_the_list() {
        // `EXISTS { MATCH (r WHERE xs) }` — a pattern element carries its own
        // WHERE, distinct from the clause's.
        let body = single(vec![Clause::Match(MatchClause {
            optional: false,
            pattern: Pattern {
                paths: vec![PathPattern {
                    variable: None,
                    elements: vec![PatternElement::Node(NodePattern {
                        variable: Some("r".to_string()),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: Some(Expr::Variable("xs".to_string())),
                    })],
                    shortest_path_mode: None,
                }],
            },
            where_clause: None,
            for_update: false,
        })]);
        assert!(
            !dead(&plan_with_exists_body(body)).contains("xs"),
            "a pattern-element WHERE reads xs"
        );
    }

    #[test]
    fn a_read_in_a_subquery_unwind_keeps_the_list() {
        // `COUNT { UNWIND xs AS y }`. `survey_unwind_sources` walks the
        // LogicalPlan, so an UNWIND living in an AST subquery body is invisible
        // to it — the read has to come from this walker or from nowhere.
        let body = single(vec![Clause::Unwind(UnwindClause {
            expr: Expr::Variable("xs".to_string()),
            variable: "y".to_string(),
        })]);
        let plan = collect_unwind_plan(vec![
            (Expr::Variable("f".to_string()), None),
            (Expr::CountSubquery(body), Some("c".to_string())),
        ]);
        assert!(!dead(&plan).contains("xs"), "the body's UNWIND reads xs");
    }

    #[test]
    fn a_read_in_a_pattern_comprehension_property_map_keeps_the_list() {
        // Same omission, different consumer: the comprehension arm collected
        // its WHERE and map expression but never its pattern.
        let plan = collect_unwind_plan(vec![
            (Expr::Variable("f".to_string()), None),
            (
                Expr::PatternComprehension {
                    path_variable: None,
                    pattern: Pattern {
                        paths: vec![PathPattern {
                            variable: None,
                            elements: vec![PatternElement::Node(NodePattern {
                                variable: Some("b".to_string()),
                                labels: LabelExpr::Empty,
                                properties: Some(Expr::Map(vec![(
                                    "tag".to_string(),
                                    Expr::Variable("xs".to_string()),
                                )])),
                                where_clause: None,
                            })],
                            shortest_path_mode: None,
                        }],
                    },
                    where_clause: None,
                    map_expr: Box::new(Expr::Variable("b.id".to_string())),
                },
                Some("l".to_string()),
            ),
        ]);
        assert!(
            !dead(&plan).contains("xs"),
            "the comprehension's pattern reads xs"
        );
    }

    #[test]
    fn a_read_in_an_exists_where_keeps_the_list() {
        // The control: this arm was always handled. If it ever reports `xs`
        // dead the walker has regressed wholesale, not just in the new arms.
        let body = single(vec![Clause::Match(MatchClause {
            optional: false,
            pattern: Pattern { paths: vec![] },
            where_clause: Some(Expr::Variable("xs".to_string())),
            for_update: false,
        })]);
        assert!(
            !dead(&plan_with_exists_body(body)).contains("xs"),
            "a clause WHERE reads xs"
        );
    }

    #[test]
    fn a_wildcard_inside_a_subquery_body_stands_the_analysis_down() {
        // `EXISTS { MATCH (r:P) RETURN * }`. The body's `*` is AST hanging off
        // an expression, so `survey_unwind_sources` — which only inspects
        // `LogicalPlan::Project` — cannot see it. Measured: a body `RETURN *`
        // does export outer-scope variables, so absence proves nothing here
        // either and the whole analysis must stand down.
        let body = single(vec![
            match_with_inline_property(Expr::Literal(CypherLiteral::String("b".to_string()))),
            Clause::Return(ReturnClause {
                distinct: false,
                items: vec![ReturnItem::All],
                order_by: None,
                skip: None,
                limit: None,
            }),
        ]);
        assert!(
            dead(&plan_with_exists_body(body)).is_empty(),
            "a wildcard in a subquery body must disable pruning entirely"
        );
    }

    #[test]
    fn a_subquery_that_never_mentions_the_list_still_prunes() {
        // The control in the other direction. Collecting more must not make
        // *everything* look live, or #184's pruning is silently disabled.
        let body = single(vec![match_with_inline_property(Expr::Literal(
            CypherLiteral::String("b".to_string()),
        ))]);
        assert!(
            dead(&plan_with_exists_body(body)).contains("xs"),
            "a body that does not read xs must leave it prunable"
        );
    }
}

#[cfg(test)]
mod dead_unwind_wildcard_tests {
    use super::*;

    /// `count(*)` must not read as `RETURN *`.
    ///
    /// Both carry an `Expr::Wildcard`, but only the bare projection widens the
    /// scope. Recursing into expressions to find one made every aggregate query
    /// opt out of pruning — including LDBC IC6, whose final clause is a
    /// `count`.
    #[test]
    fn count_star_is_not_a_wildcard_projection() {
        let collected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Empty),
            projections: vec![(
                Expr::Variable("collect(friend)".to_string()),
                Some("xs".to_string()),
            )],
        };
        let unwound = LogicalPlan::Unwind {
            input: Box::new(collected),
            expr: Expr::Variable("xs".to_string()),
            variable: "f".to_string(),
        };
        let plan = LogicalPlan::Project {
            input: Box::new(unwound),
            projections: vec![(
                Expr::FunctionCall {
                    name: "count".to_string(),
                    args: vec![Expr::Wildcard],
                    distinct: false,
                    window_spec: None,
                },
                Some("c".to_string()),
            )],
        };
        let mut properties = HashMap::new();
        mark_dead_unwind_sources(&plan, &mut properties);
        assert!(
            properties
                .get(DEAD_UNWIND_SOURCES_KEY)
                .is_some_and(|d| d.contains("xs")),
            "count(*) must not stand the analysis down"
        );
    }

    /// A bare `RETURN *` still does.
    #[test]
    fn a_bare_wildcard_projection_still_stands_it_down() {
        let collected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Empty),
            projections: vec![(
                Expr::Variable("collect(friend)".to_string()),
                Some("xs".to_string()),
            )],
        };
        let unwound = LogicalPlan::Unwind {
            input: Box::new(collected),
            expr: Expr::Variable("xs".to_string()),
            variable: "f".to_string(),
        };
        let plan = LogicalPlan::Project {
            input: Box::new(unwound),
            projections: vec![(Expr::Wildcard, None)],
        };
        let mut properties = HashMap::new();
        mark_dead_unwind_sources(&plan, &mut properties);
        assert!(!properties.contains_key(DEAD_UNWIND_SOURCES_KEY));
    }
}
