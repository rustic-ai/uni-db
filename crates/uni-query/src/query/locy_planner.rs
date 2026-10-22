// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Locy plan builder — translates a [`CompiledProgram`] into a `LogicalPlan::LocyProgram`.
//!
//! Stratifies rules, plans clause bodies via [`QueryPlanner::plan_pattern()`],
//! and assembles [`LocyStratum`] / [`LocyRulePlan`] / [`LocyClausePlan`] trees with
//! `LocyDerivedScan` nodes for IS-reference data injection.
//!
//! **Scope:** Strata (rules + clauses) only. Commands (`DERIVE`, `QUERY`, `ASSUME`,
//! `ABDUCE`, `EXPLAIN`) are dispatched by the orchestrator after strata evaluation;
//! dedicated command plan building is a future enhancement.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use parking_lot::RwLock;

use uni_common::core::schema::Schema;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, PatternElement};
use uni_cypher::locy_ast::{
    LocyBinaryOp, LocyExpr, RuleCondition, RuleOutput, resolve_yield_column_names,
};
use uni_locy::binding_plan::{
    AnchorSource, BindingStep, ClauseBindingPlan, ColumnKind, Exposure, PositionAction, VarSource,
};
use uni_locy::types::{
    CompiledClause, CompiledCommand, CompiledProgram, CompiledRule, Stratum, YieldColumn,
};

/// The variables of a rule's clauses that hold an entity id, over every clause.
///
/// MATCH node variables, and variables an IS reference binds to a node or a
/// relationship column of the referenced rule: all of them are carried as a
/// `UInt64` id. The kind of an IS-reference position comes from the clause's
/// binding plan (`uni_locy::binding_plan`), which infers it from the
/// referenced rule's YIELD expression — never from whether the column is a
/// KEY, which mis-typed a scalar KEY as a node (issue #300). A clause without
/// a plan contributes its MATCH nodes only.
fn collect_node_vars(clauses: &[CompiledClause]) -> HashSet<String> {
    let mut node_vars = HashSet::new();
    for clause in clauses {
        let Some(plan) = clause.binding.plan() else {
            collect_match_node_vars(clause, &mut node_vars);
            continue;
        };
        for (var, source) in &plan.scope {
            let id = match source {
                VarSource::Match(kind) => *kind == ColumnKind::Node,
                VarSource::RefPosition { r, pos } => {
                    plan.refs[*r].positions[*pos].kind != ColumnKind::Scalar
                }
                VarSource::Generator { .. } | VarSource::YieldAlias(_) => false,
            };
            if id {
                node_vars.insert(var.clone());
            }
        }
    }
    node_vars
}

/// The referenced rule and column a clause variable reads through an IS
/// reference: the column a position binds it to, or the value column it
/// names bare (`Exposure`).
///
/// `None` when the variable has another source, or the clause has no plan.
/// An ambiguous exposure has no source either: reading one is the compile
/// error `AmbiguousRuleColumn`, and should a read reach the planner anyway,
/// [`ClauseScope`] refuses the projection that reads it as an internal error,
/// so the type inferred here is never used.
fn plan_sourced_column<'p>(clause: &'p CompiledClause, var: &str) -> Option<(&'p str, &'p str)> {
    let plan = clause.binding.plan()?;
    match plan.scope.get(var) {
        Some(VarSource::RefPosition { r, pos }) => {
            let binding = &plan.refs[*r];
            Some((
                binding.rule.as_str(),
                binding.positions[*pos].column.as_str(),
            ))
        }
        Some(_) => None,
        None => {
            let (column, exposure) = plan.exposure.get_key_value(var)?;
            match exposure {
                Exposure::Unique { r, .. } => Some((plan.refs[*r].rule.as_str(), column.as_str())),
                Exposure::Ambiguous(_) => None,
            }
        }
    }
}

/// Collect node variable names from a single clause's MATCH pattern only.
///
/// Only these variables have expanded `{var}._vid`, `{var}._labels`, `{var}._all_props`
/// columns in the graph scan output. IS-ref target/subject variables exist as plain
/// columns in the derived scan and should NOT be rewritten to `{var}._vid`.
fn collect_match_node_vars(clause: &CompiledClause, node_vars: &mut HashSet<String>) {
    for path in &clause.match_pattern.paths {
        for elem in &path.elements {
            if let PatternElement::Node(np) = elem
                && let Some(var) = &np.variable
            {
                node_vars.insert(var.clone());
            }
        }
    }
}

/// The labels or edge types a clause's MATCH pattern pins on an entity
/// variable.
#[derive(Debug, Clone, PartialEq)]
enum PatternScope {
    /// A vertex carrying every one of these labels (`(n:A:B)`, or `(n)`).
    AllOf(Vec<String>),
    /// An entity carrying exactly one of these (`[e:R|Q]`, `(n:A|B)`).
    OneOf(Vec<String>),
}

/// Every entity variable of a clause's MATCH pattern, with the labels or edge
/// types the pattern pins on it. `None` marks one whose scope is not a single
/// label set: a variable-length relationship (a list), or a name written with
/// conflicting label expressions.
type EntityScopes = HashMap<String, Option<PatternScope>>;

/// Map the entity variables in a clause's MATCH pattern to their scopes.
///
/// Used to resolve a property-access expression's declared type from the graph
/// schema (e.g. `a.id` where `a` is `(:Node)` → `Node.id`).
fn clause_var_labels(clause: &CompiledClause) -> EntityScopes {
    use uni_cypher::ast::LabelExpr;
    fn bind(scopes: &mut EntityScopes, var: &str, scope: Option<PatternScope>) {
        let Some(existing) = scopes.get_mut(var) else {
            scopes.insert(var.to_string(), scope);
            return;
        };
        *existing = match (existing.take(), scope) {
            // A label repeated on a vertex: it carries all of them.
            (Some(PatternScope::AllOf(mut a)), Some(PatternScope::AllOf(b))) => {
                a.extend(b);
                a.sort();
                a.dedup();
                Some(PatternScope::AllOf(a))
            }
            (Some(a), Some(b)) if a == b => Some(a),
            _ => None,
        };
    }
    let scope_of = |labels: &LabelExpr, node: bool| match labels {
        LabelExpr::Empty if node => PatternScope::AllOf(Vec::new()),
        LabelExpr::Empty => PatternScope::OneOf(Vec::new()),
        LabelExpr::Conjunction(v) => PatternScope::AllOf(v.clone()),
        LabelExpr::Disjunction(v) => PatternScope::OneOf(v.clone()),
    };
    let mut scopes = EntityScopes::new();
    for path in &clause.match_pattern.paths {
        for elem in &path.elements {
            match elem {
                PatternElement::Node(np) => {
                    if let Some(var) = &np.variable {
                        // An unlabelled occurrence adds nothing to a labelled one.
                        if np.labels.names().is_empty() && scopes.contains_key(var) {
                            continue;
                        }
                        bind(&mut scopes, var, Some(scope_of(&np.labels, true)));
                    }
                }
                PatternElement::Relationship(rp) => {
                    if let Some(var) = &rp.variable {
                        let scope = rp.range.is_none().then(|| scope_of(&rp.types, false));
                        bind(&mut scopes, var, scope);
                    }
                }
                _ => {}
            }
        }
    }
    scopes
}

/// The Arrow type the schema declares for `var.prop`, from the labels or edge
/// types the pattern pins on `var` alone.
///
/// `None` when the schema does not type it there: `var` is not a pattern
/// entity, its scope declares no `prop`, or the declarations disagree. Its
/// column then carries the stored Cypher value (`LargeBinary`), whatever
/// another label happens to declare under the same name.
fn property_arrow_type(
    schema: &Schema,
    var_labels: &EntityScopes,
    var: &str,
    prop: &str,
) -> Option<DataType> {
    use uni_common::core::schema::EntityScope;
    let meta = match var_labels.get(var)?.as_ref()? {
        PatternScope::AllOf(labels) => {
            schema.scoped_property_meta(EntityScope::AllOf(labels), prop)
        }
        PatternScope::OneOf(labels) => {
            schema.scoped_property_meta(EntityScope::OneOf(labels), prop)
        }
    }?;
    Some(meta.r#type.to_arrow())
}

/// The Arrow type of each ALONG column, unified over every clause binding it.
///
/// An accumulation that stays integral (`e.w`, `prev.q + e.w`, `prev.n * 2`
/// over integer properties and literals) is Int64; any float part makes it
/// Float64; a property declared nowhere (a schemaless graph) leaves it the
/// dynamic CypherValue the arithmetic produces. Every ALONG column was Float64,
/// so an integer path cost came back as `3.0` — and above 2^53, wrong.
fn along_column_types(clauses: &[CompiledClause], schema: &Schema) -> HashMap<String, DataType> {
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum Kind {
        /// `prev.x`: whatever the column is.
        Neutral,
        Int,
        Float,
        Dynamic,
    }
    fn kind(expr: &LocyExpr, schema: &Schema, labels: &EntityScopes) -> Kind {
        match expr {
            LocyExpr::PrevRef(_) => Kind::Neutral,
            LocyExpr::Cypher(Expr::Literal(CypherLiteral::Integer(_))) => Kind::Int,
            LocyExpr::Cypher(Expr::Literal(CypherLiteral::Float(_))) => Kind::Float,
            LocyExpr::Cypher(Expr::Property(object, prop)) => {
                let Expr::Variable(var) = object.as_ref() else {
                    return Kind::Float;
                };
                // Not typed by the variable's own labels: the stored value,
                // whatever another label declares under the same name.
                match property_arrow_type(schema, labels, var, prop) {
                    Some(DataType::Int64 | DataType::Int32 | DataType::UInt64) => Kind::Int,
                    Some(DataType::LargeBinary) | None => Kind::Dynamic,
                    Some(_) => Kind::Float,
                }
            }
            LocyExpr::Cypher(_) => Kind::Float,
            LocyExpr::BinaryOp { left, op, right } => {
                let k = kind(left, schema, labels).max(kind(right, schema, labels));
                match op {
                    LocyBinaryOp::Pow if k != Kind::Dynamic => Kind::Float,
                    _ => k,
                }
            }
            LocyExpr::UnaryOp(_, inner) => kind(inner, schema, labels),
        }
    }
    // A probability is numeric by definition, and the complement / noisy-OR
    // arithmetic reads it as Float64 (see `infer_yield_type_rec`).
    let prob_names: HashSet<String> = clauses
        .iter()
        .filter_map(|c| match &c.output {
            RuleOutput::Yield(y) => Some(y.items.iter()),
            _ => None,
        })
        .flatten()
        .filter(|item| item.is_prob)
        .filter_map(|item| match &item.expr {
            Expr::Variable(v) => Some(v.clone()),
            _ => None,
        })
        .collect();
    let mut kinds: HashMap<String, Kind> = HashMap::new();
    for clause in clauses {
        let labels = clause_var_labels(clause);
        for binding in &clause.along {
            let k = kind(&binding.expr, schema, &labels);
            let slot = kinds.entry(binding.name.clone()).or_insert(Kind::Neutral);
            if k > *slot {
                *slot = k;
            }
        }
    }
    kinds
        .into_iter()
        .map(|(name, k)| {
            let k = if prob_names.contains(&name) {
                Kind::Float
            } else {
                k
            };
            let dt = match k {
                Kind::Int => DataType::Int64,
                Kind::Dynamic => DataType::LargeBinary,
                Kind::Neutral | Kind::Float => DataType::Float64,
            };
            (name, dt)
        })
        .collect()
}

/// The type of a FOLD aggregate's argument, when it can be told.
///
/// `MIN`/`MAX` (and their monotone forms) return their input's type, and
/// `COLLECT` keeps its elements', so the planner must not cast their input to
/// the Float64 every other numeric fold uses: an integer property came back as
/// a float, losing precision above 2^53.
fn fold_argument_type(
    aggregate: &Expr,
    schema: &Schema,
    var_labels: &EntityScopes,
    node_vars: &HashSet<String>,
) -> Option<DataType> {
    let Expr::FunctionCall { args, .. } = aggregate else {
        return None;
    };
    match args.first()? {
        Expr::Property(object, prop) => {
            let Expr::Variable(var) = object.as_ref() else {
                return None;
            };
            // Not typed by the variable's own labels: the value arrives as
            // the stored Cypher value, which the aggregate reads as is.
            // Casting it to Float64 turned an integer MAX into `0.0`.
            Some(
                property_arrow_type(schema, var_labels, var, prop)
                    .unwrap_or_else(undeclared_property_arrow_type),
            )
        }
        arg @ Expr::Literal(_) => Some(infer_expr_type(arg, node_vars)),
        _ => None,
    }
}

/// The aggregate name of a FOLD binding's expression, upper-cased.
fn fold_function_name(aggregate: &Expr) -> Option<String> {
    match aggregate {
        Expr::FunctionCall { name, .. } => Some(name.to_uppercase()),
        _ => None,
    }
}

/// The Arrow type the scan emits for a property its label does not declare.
///
/// This must mirror `df_graph::scan` exactly: a schema'd scan reads an
/// undeclared property from the overflow map, and a schemaless one types a
/// property only from the scanned labels' own declarations, so both emit
/// `LargeBinary` (the stored Cypher value). Guessing differently makes the
/// projection's declared type disagree with the column it receives, and the
/// coercion nulls or garbles the value. Both used to take the type of the
/// first label that declared the property anywhere in the schema, which read
/// an integer stored on another label as a float, and a string as NULL.
fn undeclared_property_arrow_type() -> DataType {
    DataType::LargeBinary
}

/// The variables a clause's generator conditions bind.
fn clause_generator_outputs(clause: &CompiledClause) -> HashSet<&str> {
    clause
        .where_conditions
        .iter()
        .filter_map(|cond| match cond {
            RuleCondition::Generator(generator) => Some(generator),
            _ => None,
        })
        .flat_map(|generator| generator.outputs.iter().map(String::as_str))
        .collect()
}

/// Infer the Arrow DataType for a yield column based on its expression in the first clause.
///
/// `rule_catalog` lets a yield column that merely forwards a NON-KEY value column
/// brought in by a positive IS-reference (e.g. `WHERE (p,c) IS pc_mapped YIELD ... infringement`)
/// resolve to that source column's real type instead of defaulting to LargeUtf8.
/// Without this, the derived-scan schema of such a rule mis-types the column as
/// Utf8 while its materialized data carries the true type (Float64/Int64), and a
/// downstream rule that scans it fails with an Arrow schema mismatch.
///
/// `is_key`, `schema`, and `var_labels` let an integer-typed property in a KEY
/// position keep its `Int64` type instead of being widened to the default
/// `Float64` (issue #94); `var_labels` describes `first_clause`'s node vars.
#[expect(
    clippy::too_many_arguments,
    reason = "yield-type inference needs the full clause context: name sets, catalog and schema"
)]
fn infer_yield_type(
    name: &str,
    first_clause: &CompiledClause,
    node_vars: &HashSet<String>,
    fold_output_names: &HashSet<&str>,
    along_types: &HashMap<String, DataType>,
    rule_catalog: &HashMap<String, CompiledRule>,
    is_key: bool,
    schema: &Schema,
    var_labels: &EntityScopes,
) -> DataType {
    let mut visited = HashSet::new();
    infer_yield_type_rec(
        name,
        first_clause,
        node_vars,
        fold_output_names,
        along_types,
        rule_catalog,
        is_key,
        schema,
        var_labels,
        &mut visited,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "mirrors `infer_yield_type`'s context, plus the cycle-guard visited set"
)]
fn infer_yield_type_rec(
    name: &str,
    first_clause: &CompiledClause,
    node_vars: &HashSet<String>,
    fold_output_names: &HashSet<&str>,
    along_types: &HashMap<String, DataType>,
    rule_catalog: &HashMap<String, CompiledRule>,
    is_key: bool,
    schema: &Schema,
    var_labels: &EntityScopes,
    visited: &mut HashSet<String>,
) -> DataType {
    // FOLD outputs — type depends on the aggregate function.
    // COUNT/MCOUNT produce Int64; SUM/MSUM/AVG produce Float64, but SUM/AVG
    // over a Cypher value produce a Cypher value.
    if fold_output_names.contains(name) {
        if let Some(fold) = first_clause.fold.iter().find(|fb| fb.name == name)
            && let Expr::FunctionCall { name: fn_name, .. } = &fold.aggregate
        {
            match fn_name.to_uppercase().as_str() {
                "COUNT" | "MCOUNT" => return DataType::Int64,
                // A list of the inputs, carried as a Cypher value.
                "COLLECT" => return DataType::LargeBinary,
                "MIN" | "MAX" | "MMIN" | "MMAX" => {
                    if let Some(dt) =
                        fold_argument_type(&fold.aggregate, schema, var_labels, node_vars)
                    {
                        return dt;
                    }
                }
                // Over a Cypher value (a schemaless or duration property), a
                // Cypher value: a float, or a duration. The input stays the
                // Cypher value too, since this is also its column type; cast to
                // Float64, a duration was refused.
                "SUM" | "AVG"
                    if fold_argument_type(&fold.aggregate, schema, var_labels, node_vars)
                        == Some(DataType::LargeBinary) =>
                {
                    return DataType::LargeBinary;
                }
                _ => {}
            }
        }
        return DataType::Float64;
    }
    // ALONG bindings: the type unified over every clause (`along_column_types`).
    if let Some(dt) = along_types.get(name) {
        return dt.clone();
    }
    // Look at the yield expression from the first clause. The UInt64 (node-VID)
    // shortcut is gated on the expression being a bare node Variable — NOT on the
    // column name — so a property-access KEY whose name happens to equal a node
    // var (e.g. `YIELD KEY a.id AS a`) is typed from its property expression
    // instead of being mis-typed as a VID (issue #94).
    if let RuleOutput::Yield(yc) = &first_clause.output {
        let item_names = resolve_yield_column_names(&yc.items);
        for (item, item_name) in yc.items.iter().zip(item_names.iter()) {
            if item_name == name {
                if let Expr::Variable(v) = &item.expr {
                    // A bare Variable naming a node var is a whole-node KEY → UInt64.
                    if node_vars.contains(v) {
                        return DataType::UInt64;
                    }
                    // A relationship is carried by its EID, as a node by its
                    // VID, and hydrated into the edge after the fixpoint.
                    // Typed by the catch-all as a string, the edge was cast
                    // away and the rule yielded NULL for it.
                    if let Some(edge) = match_edge_vars(first_clause, 0)
                        .into_iter()
                        .find(|e| e.name == *v)
                    {
                        return match edge.kind {
                            DerivKind::Edge => DataType::UInt64,
                            _ => DataType::LargeBinary,
                        };
                    }
                    // A generator output carries whatever the generator emits
                    // (`LocyGenerateExec` types it from the signature): a
                    // Cypher value holds it whatever that is.
                    if clause_generator_outputs(first_clause).contains(v.as_str()) {
                        return DataType::LargeBinary;
                    }
                    // A bare Variable referencing an ALONG name takes its type.
                    // Without this, `ew AS link_weight` would infer
                    // Variable("ew") as LargeUtf8.
                    if let Some(dt) = along_types.get(v.as_str()) {
                        return dt.clone();
                    }
                    // A bare Variable naming a NON-KEY value column carried in by a
                    // positive IS-reference: resolve its type from the source rule
                    // rather than defaulting to LargeUtf8.
                    if let Some(dt) =
                        infer_is_ref_value_col_type(v, first_clause, rule_catalog, schema, visited)
                    {
                        return dt;
                    }
                }
                // A bare property-access column carries its schema-declared
                // type. The default `Property → Float64` rule (`infer_expr_type`)
                // is only correct for numeric properties; a non-numeric property
                // (DateTime/Duration/Btic/String/Bytes/Bool) would be widened to
                // Float64 and forced through an unsupported projection cast
                // ("Casting Struct to Float64") or cast-collapsed to Null, and an
                // integer property would lose its Int64 type. Look the real type
                // up from the schema for both KEY and value columns; for a
                // schemaless property the type is unknown, so default to
                // LargeBinary (the cv-encoded storage type) and let it pass
                // through unchanged to be decoded at read time. Generalizes the
                // Int64-only KEY fix from issue #94 to every property (issue
                // #112: a schemaless/typed property KEY projected Null).
                if let Expr::Property(object, prop) = &item.expr
                    && let Expr::Variable(var) = object.as_ref()
                {
                    if let Some(dt) = property_arrow_type(schema, var_labels, var, prop) {
                        return dt;
                    }
                    // Not typed by the variable's own labels or edge types: a
                    // schemaless label, an unlabelled node, an edge type that
                    // does not declare it, or not a pattern entity at all. The
                    // column carries the stored Cypher value, so declare
                    // LargeBinary: the declared and actual types match, no
                    // coercion runs, and the value is decoded at read time.
                    // Taking the type some other label declared for the same
                    // name read an integer stored here as a float and a string
                    // as an error.
                    //
                    // PROB columns are excluded: a probability is numeric by
                    // definition and the complement / noisy-OR arithmetic reads
                    // it as a real Float64, so there the coercion is right.
                    if !item.is_prob {
                        return undeclared_property_arrow_type();
                    }
                }
                let _ = is_key;
                if let Some(dt) = arithmetic_yield_type(
                    &item.expr,
                    first_clause,
                    node_vars,
                    along_types,
                    rule_catalog,
                    schema,
                    var_labels,
                    visited,
                ) {
                    return dt;
                }
                return infer_expr_type(&item.expr, node_vars);
            }
        }
    }
    // No explicit yield item matched. A column named after a node var carries its
    // VID (UInt64); otherwise try an IS-ref-forwarded value column.
    if node_vars.contains(name) {
        return DataType::UInt64;
    }
    if let Some(dt) = infer_is_ref_value_col_type(name, first_clause, rule_catalog, schema, visited)
    {
        return dt;
    }
    DataType::LargeUtf8
}

/// The type of an arithmetic YIELD expression, typed as ALONG types its
/// accumulations (`along_column_types`): integral when every operand is, a
/// float when one is, and a Cypher value (`LargeBinary`) when an operand's type
/// is only known at run time — a property no schema declares, or a value
/// column that is itself a Cypher value. `None` for anything else, which
/// falls back to [`infer_expr_type`].
///
/// `infer_expr_type` typed every property `Float64` and every referenced
/// value variable as a string, so `e.w + e.w` and `p + e.w` over integers
/// came back `2.0` / `3.0` while `e.w AS d` stayed `2` — one column holding
/// both, depending on which clause derived the row.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors `infer_yield_type_rec`'s context"
)]
fn arithmetic_yield_type(
    expr: &Expr,
    clause: &CompiledClause,
    node_vars: &HashSet<String>,
    along_types: &HashMap<String, DataType>,
    rule_catalog: &HashMap<String, CompiledRule>,
    schema: &Schema,
    var_labels: &EntityScopes,
    visited: &mut HashSet<String>,
) -> Option<DataType> {
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum Kind {
        Int,
        Float,
        Dynamic,
    }
    #[expect(clippy::too_many_arguments, reason = "recursive helper, same context")]
    fn kind(
        expr: &Expr,
        clause: &CompiledClause,
        node_vars: &HashSet<String>,
        along_types: &HashMap<String, DataType>,
        rule_catalog: &HashMap<String, CompiledRule>,
        schema: &Schema,
        var_labels: &EntityScopes,
        visited: &mut HashSet<String>,
    ) -> Option<Kind> {
        let of_type = |dt: DataType| match dt {
            DataType::Int64 | DataType::Int32 | DataType::UInt64 => Some(Kind::Int),
            DataType::Float64 | DataType::Float32 => Some(Kind::Float),
            DataType::LargeBinary => Some(Kind::Dynamic),
            _ => None,
        };
        match expr {
            Expr::Literal(CypherLiteral::Integer(_)) => Some(Kind::Int),
            Expr::Literal(CypherLiteral::Float(_)) => Some(Kind::Float),
            Expr::Property(object, prop) => {
                let Expr::Variable(var) = object.as_ref() else {
                    return None;
                };
                match property_arrow_type(schema, var_labels, var, prop) {
                    Some(dt) => of_type(dt),
                    None => Some(Kind::Dynamic),
                }
            }
            Expr::Variable(v) if node_vars.contains(v) => None,
            Expr::Variable(v) => match along_types.get(v.as_str()) {
                Some(dt) => of_type(dt.clone()),
                None => infer_is_ref_value_col_type(v, clause, rule_catalog, schema, visited)
                    .and_then(of_type),
            },
            Expr::UnaryOp {
                op: uni_cypher::ast::UnaryOp::Neg,
                expr: inner,
            } => kind(
                inner,
                clause,
                node_vars,
                along_types,
                rule_catalog,
                schema,
                var_labels,
                visited,
            ),
            Expr::BinaryOp { left, op, right } => {
                use uni_cypher::ast::BinaryOp::{Add, Div, Mod, Mul, Pow, Sub};
                if !matches!(op, Add | Sub | Mul | Div | Mod | Pow) {
                    return None;
                }
                let l = kind(
                    left,
                    clause,
                    node_vars,
                    along_types,
                    rule_catalog,
                    schema,
                    var_labels,
                    visited,
                )?;
                let r = kind(
                    right,
                    clause,
                    node_vars,
                    along_types,
                    rule_catalog,
                    schema,
                    var_labels,
                    visited,
                )?;
                let k = l.max(r);
                Some(if matches!(op, Pow) && k != Kind::Dynamic {
                    Kind::Float
                } else {
                    k
                })
            }
            _ => None,
        }
    }
    // Only an expression: a bare property or variable is typed by the
    // caller's own rules.
    if !matches!(expr, Expr::BinaryOp { .. } | Expr::UnaryOp { .. }) {
        return None;
    }
    let k = kind(
        expr,
        clause,
        node_vars,
        along_types,
        rule_catalog,
        schema,
        var_labels,
        visited,
    )?;
    Some(match k {
        Kind::Int => DataType::Int64,
        Kind::Float => DataType::Float64,
        Kind::Dynamic => DataType::LargeBinary,
    })
}

/// The type of the referenced column `var` reads through one of `clause`'s
/// positive IS references (see [`plan_sourced_column`]), inferred from the
/// referenced rule. `visited` (keyed `rule::col`) guards against cyclic
/// inference through recursive rules.
fn infer_is_ref_value_col_type(
    var: &str,
    clause: &CompiledClause,
    rule_catalog: &HashMap<String, CompiledRule>,
    schema: &Schema,
    visited: &mut HashSet<String>,
) -> Option<DataType> {
    let (rule_name, col) = plan_sourced_column(clause, var)?;
    let rule = rule_catalog.get(rule_name)?;
    if !visited.insert(format!("{rule_name}::{col}")) {
        return Some(DataType::LargeUtf8); // cycle guard
    }
    let Some(src_clause) = rule.clauses.first() else {
        return Some(DataType::LargeUtf8);
    };
    let is_key = rule
        .yield_schema
        .iter()
        .any(|yc| yc.is_key && yc.name == col);
    let src_node_vars = collect_node_vars(&rule.clauses);
    let src_fold: HashSet<&str> = src_clause.fold.iter().map(|fb| fb.name.as_str()).collect();
    let src_along = along_column_types(&rule.clauses, schema);
    let src_var_labels = clause_var_labels(src_clause);
    Some(infer_yield_type_rec(
        col,
        src_clause,
        &src_node_vars,
        &src_fold,
        &src_along,
        rule_catalog,
        is_key,
        schema,
        &src_var_labels,
        visited,
    ))
}

/// Infer Arrow DataType from a Cypher expression.
fn infer_expr_type(expr: &Expr, node_vars: &HashSet<String>) -> DataType {
    match expr {
        Expr::Variable(v) if node_vars.contains(v) => DataType::UInt64,
        Expr::Literal(CypherLiteral::Integer(_)) => DataType::Int64,
        Expr::Literal(CypherLiteral::Float(_)) => DataType::Float64,
        Expr::Literal(CypherLiteral::String(_)) => DataType::LargeUtf8,
        Expr::Literal(CypherLiteral::Bool(_)) => DataType::Boolean,
        Expr::Literal(CypherLiteral::Null) => DataType::LargeUtf8,
        // A duration accessor on a computed value — `duration.inDays(a, b).days`,
        // `.months`, `.secondsOfMinute`, … — is integral. Every arm of
        // `datetime::duration_component` returns `Value::Int`, so typing these
        // `Float64` made `derived` report `20.0` where the value is `20`.
        //
        // Restricted to a non-`Variable` object on purpose: `n.days` is an
        // ordinary node property whose type the schema decides, and a column
        // that merely happens to be called `days` must not be forced to Int64.
        Expr::Property(object, prop)
            if !matches!(object.as_ref(), Expr::Variable(_))
                && uni_query_functions::datetime::is_duration_accessor(prop) =>
        {
            DataType::Int64
        }
        Expr::Property(_, _) => DataType::Float64,
        // Binary operations: infer from operator and operand types.
        Expr::BinaryOp { left, op, right } => {
            use uni_cypher::ast::BinaryOp::*;
            match op {
                // Comparison and logical operators always return Boolean.
                Eq | NotEq | Lt | LtEq | Gt | GtEq | And | Or | Xor | Regex | Contains
                | StartsWith | EndsWith => DataType::Boolean,
                // Arithmetic operators: infer from operands.
                Add | Sub | Mul | Div | Mod | Pow | ApproxEq => {
                    let lt = infer_expr_type(left, node_vars);
                    let rt = infer_expr_type(right, node_vars);
                    // If either operand is Float64, result is Float64.
                    if lt == DataType::Float64 || rt == DataType::Float64 {
                        DataType::Float64
                    } else if lt == DataType::Int64 && rt == DataType::Int64 {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    }
                }
            }
        }
        // Unary operations: infer from inner expression.
        Expr::UnaryOp { op, expr: inner } => {
            use uni_cypher::ast::UnaryOp;
            match op {
                UnaryOp::Not => DataType::Boolean,
                UnaryOp::Neg => infer_expr_type(inner, node_vars),
            }
        }
        Expr::IsNull(_) | Expr::IsNotNull(_) => DataType::Boolean,
        // Lists and maps, and the expressions that build them: no single Arrow
        // type holds every such value (a list of encoded values, a nested
        // list, a map), so the column is a Cypher value. The catch-all below
        // typed them as strings, and the projection cast a list to its text.
        Expr::List(_)
        | Expr::Map(_)
        | Expr::ListComprehension { .. }
        | Expr::PatternComprehension { .. }
        | Expr::MapProjection { .. }
        | Expr::CollectSubquery(_)
        | Expr::Reduce { .. } => DataType::LargeBinary,
        // Function calls: infer return type from function name.
        Expr::FunctionCall { name, args, .. } => {
            match name.to_uppercase().as_str() {
                // Numeric functions → Float64
                "SIMILAR_TO" | "ABS" | "SQRT" | "LOG" | "LOG10" | "EXP" | "CEIL" | "FLOOR"
                | "ROUND" | "SIGN" | "RAND" | "TOFLOAT" | "TODOUBLE" | "COS" | "SIN" | "TAN"
                | "ACOS" | "ASIN" | "ATAN" | "ATAN2" | "DEGREES" | "RADIANS" | "PI" | "E"
                | "DISTANCE" => DataType::Float64,
                // Integer-returning functions
                "TOINTEGER" | "LENGTH" | "SIZE" | "ID" => DataType::Int64,
                // String-returning functions
                "TOSTRING" | "TOLOWER" | "TOUPPER" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE"
                | "SUBSTRING" | "LEFT" | "RIGHT" | "REVERSE" | "TYPE" => DataType::LargeUtf8,
                // Boolean-returning functions
                "EXISTS" | "STARTSWITH" | "ENDSWITH" | "CONTAINS" => DataType::Boolean,
                // Aggregates that return Float64
                "SUM" | "AVG" | "MAX" | "MIN" => DataType::Float64,
                "COUNT" => DataType::Int64,
                // Unknown function: try to infer from first argument
                _ => {
                    if let Some(first_arg) = args.first() {
                        infer_expr_type(first_arg, node_vars)
                    } else {
                        DataType::LargeUtf8
                    }
                }
            }
        }
        _ => DataType::LargeUtf8,
    }
}

use super::df_graph::common::build_edge_list_field;
use super::df_graph::locy_fixpoint::{DerivedScanEntry, DerivedScanRegistry, DerivedScanView};
use super::planner::{
    LogicalPlan, QueryPlanner, VariableInfo, WhereScope, collect_expr_variables, is_var_in_scope,
};
use super::planner_locy_types::{
    DERIV_EVENT_COL, DERIV_ISNOT_COL_PREFIX, DERIV_ISPROB_COL_PREFIX, DERIV_ISREF_COL_PREFIX,
    FOLD_DISCRIMINATOR_COL_PREFIX, ISNOT_VID_COL_PREFIX, ISPROB_COL_PREFIX, LIN_EVENT_COL,
    LIN_NOT_COL_PREFIX, LIN_PROB_COL_PREFIX, LIN_REF_COL_PREFIX, LIN_SUP_COL_PREFIX,
    LIN_SUPP_COL_PREFIX, LocyClausePlan, LocyCommand, LocyIsRef, LocyRulePlan, LocyStratum,
    LocyYieldColumn, NegatedLineageRef, NegationKey, PositiveProbRef, ProbRead, SupportRef,
};

// ---------------------------------------------------------------------------
// DerivedScanHandle
// ---------------------------------------------------------------------------

/// Internal handle tracking a single derived-scan injection point.
///
/// Each handle corresponds to a `LocyDerivedScan` node in the plan tree and
/// a `DerivedScanEntry` in the resulting registry. The `Arc<RwLock>` data handle
/// is shared between the plan node and registry entry so that `FixpointExec`
/// can inject data that flows into the subplan.
#[derive(Clone)]
struct DerivedScanHandle {
    rule_name: String,
    scan_index: usize,
    is_self_ref: bool,
    /// Which view of the target rule's relation this scan reads (issue #162).
    view: DerivedScanView,
    /// One reference of a non-linear clause, never shared (see
    /// `DerivedScanEntry::exclusive`).
    exclusive: bool,
    data: Arc<RwLock<Vec<RecordBatch>>>,
    schema: SchemaRef,
}

// ---------------------------------------------------------------------------
// LocyPlanBuilder
// ---------------------------------------------------------------------------

/// The plan of a stratum whose facts are reused rather than derived.
///
/// Carries what `LocyProgramExec` reads of a reused stratum — its rules'
/// names and which of them yield columns — and no clause bodies. The YIELD
/// columns' types are not inferred (that needs the clauses) and are never
/// read: the reused facts bring their own schema.
fn reused_stratum_stub(stratum: &Stratum) -> LocyStratum {
    LocyStratum {
        id: stratum.id,
        rules: stratum
            .rules
            .iter()
            .map(|rule| LocyRulePlan {
                name: rule.name.clone(),
                clauses: Vec::new(),
                yield_schema: rule
                    .yield_schema
                    .iter()
                    .map(|yc| LocyYieldColumn {
                        name: yc.name.clone(),
                        is_key: yc.is_key,
                        is_prob: yc.is_prob,
                        data_type: DataType::Null,
                    })
                    .collect(),
                priority: rule.priority,
                fold_bindings: Vec::new(),
                require: Vec::new(),
                having: Vec::new(),
                best_by_criteria: Vec::new(),
                yield_projection: Vec::new(),
                deriv_columns: Vec::new(),
            })
            .collect(),
        is_recursive: stratum.is_recursive,
        depends_on: stratum.depends_on.clone(),
    }
}

/// Builds a `LogicalPlan::LocyProgram` from a [`CompiledProgram`].
///
/// The builder translates each stratum, rule, and clause into corresponding
/// plan-level types, wiring `LocyDerivedScan` nodes for IS-reference data
/// injection. The returned `DerivedScanRegistry` shares `Arc<RwLock>` handles
/// with the plan nodes so that `FixpointExec` can populate derived data at
/// execution time.
pub struct LocyPlanBuilder<'a> {
    planner: &'a QueryPlanner,
    derived_scan_handles: RefCell<Vec<DerivedScanHandle>>,
    /// Plugin registry used to resolve Locy aggregates for the recursive-stratum
    /// monotonicity check. Defaults to the process-wide built-in registry; hosts
    /// with user-registered aggregates should override via
    /// [`Self::with_plugin_registry`].
    plugin_registry: std::sync::Arc<uni_plugin::PluginRegistry>,
    /// Positions of strata whose facts the run copies from an earlier
    /// evaluation (incremental `ASSUME` / `ABDUCE`). They are never
    /// evaluated, so their clause bodies are not planned; see
    /// [`Self::with_reused_strata`].
    reused_strata: std::collections::BTreeSet<usize>,
    /// Whether the run records provenance; see [`Self::with_provenance`].
    provenance: bool,
}

/// Neural-classifier-related plumbing threaded through `build_stratum` →
/// `build_rule` → `build_clause`. Bundled to avoid the
/// too-many-arguments smell on those builders.
#[derive(Clone)]
struct ClassifierContext {
    registry: Arc<uni_locy::ClassifierRegistry>,
    cache: Option<Arc<uni_locy::ModelInvocationCache>>,
    provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
}

/// Lookup state shared by the clause builder: target rule catalog,
/// the in-progress stratum's rule names, and the clause's bound node
/// variables. Bundled to keep `build_clause` under the
/// too-many-arguments threshold.
/// The derivation-discriminator columns for a rule (issue #159), in projection
/// order: the clause index, then one column per discriminating variable.
///
/// A discriminating variable is a MATCH-bound **node** or **relationship**
/// variable that no clause of the rule yields bare — precisely the binding
/// that distinguishes one derivation of an output row from another, and which
/// projection would otherwise erase. See [`DerivKind`] for the column each
/// kind contributes. Relationships matter because parallel edges and distinct
/// paths bind the same nodes: with nodes alone, two stakes `x -[30]-> a` and
/// `x -[40]-> a` shared one derivation key and the fixpoint kept only one
/// (issue #294). Anonymous relationships are covered through
/// [`name_anonymous_edges`].
///
/// Returns empty unless the rule is recursive *and* carries `FOLD` or `ALONG`.
/// The restriction is a correctness requirement, not an optimisation: a
/// key-only recursive rule such as a transitive closure depends on set
/// semantics, and widening its dedup key both explodes its fact count and
/// breaks termination on cyclic data.
///
/// The union is taken across clauses and sorted, so every clause of the rule
/// emits an identical schema in a stable order — `reconcile_schema` treats the
/// first non-empty batch's shape as the rule's fact identity, so an
/// iteration-order-dependent schema would be non-deterministic.
fn derivation_discriminator_columns(
    rule: &CompiledRule,
    is_recursive: bool,
    stratum_rule_names: &HashSet<String>,
    rule_catalog: &HashMap<String, CompiledRule>,
) -> Vec<(String, DataType)> {
    let mut columns: Vec<(String, DataType)> =
        derivation_discriminators(rule, is_recursive, stratum_rule_names, rule_catalog)
            .map_or_else(Vec::new, |vars| {
                std::iter::once((
                    format!("{FOLD_DISCRIMINATOR_COL_PREFIX}clause"),
                    DataType::Int64,
                ))
                .chain(vars.iter().map(|d| (d.column(), d.kind.data_type())))
                .collect()
            });
    // The lineage columns of an `MNOR`/`MPROD` rule, recursive or not: the
    // row event, then each slot's, following the discriminators in every
    // clause's projection.
    if prob_column_is_probability_fold(rule) && prefold_prob_column(rule).is_some() {
        columns.push((
            DERIV_EVENT_COL.to_string(),
            crate::query::df_graph::locy_row_hash::row_hash_type(),
        ));
    }
    // Transient lineage columns are never part of a fact's identity, so only
    // an `MNOR`/`MPROD` rule's are listed (the flag does not change them).
    for slot in prob_ref_slots(rule, rule_catalog, false)
        .iter()
        .filter(|s| s.lineage == LineageMode::Deriv)
    {
        if slot.negated {
            columns.push((slot.key_column(), DataType::Binary));
            continue;
        }
        columns.push((
            slot.key_column(),
            crate::query::df_graph::locy_row_hash::row_hash_type(),
        ));
        columns.push((slot.prob_column(), DataType::Float64));
    }
    columns
}

/// A reference, from a clause of a rule with a PROB column, to a rule that has
/// one too. A positive reference's probability flows into the calling row's
/// PROB value (see [`ProbRead`]); a negated one's complement does.
///
/// A negated reference has a slot only in a rule that keeps lineage: its
/// column (`__deriv_isnot_*`, or `__lin_not_*`) carries the negated facts the
/// complement read, so exact inference can conjoin the negation of their
/// formulas instead of an independent `1 - p` factor.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProbRefSlot {
    /// The clause holding the reference.
    clause_index: usize,
    /// Its position among the clause's positive references (or, for a
    /// negated reference, its negated references), in text order.
    occurrence: usize,
    /// Whether the reference is `IS NOT`.
    negated: bool,
    /// How the calling rule keeps its rows' lineage for exact inference and
    /// shared-proof detection.
    lineage: LineageMode,
    /// The calling rule's PROB column as the clause body names it.
    into_column: String,
}

/// How a rule with a PROB column keeps its derivation rows' lineage: each
/// row's event, the facts it references and the facts its negations read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineageMode {
    /// No lineage: a referenced probability is only multiplied in.
    None,
    /// An `MNOR`/`MPROD` rule: its pre-fold rows are its lineage, and the
    /// lineage columns are derivation columns (`__deriv_*`) of those rows.
    Deriv,
    /// A PROB rule without a fold, when provenance is recorded: its facts
    /// are its rows, so the lineage columns are transient (`__lin_*`, see
    /// [`LIN_COL_PREFIX`](crate::query::planner_locy_types::LIN_COL_PREFIX)), read before the facts are deduplicated.
    Transient,
}

/// How `rule` keeps its rows' lineage, `provenance` saying whether the run
/// records provenance (only then is a transient lineage read).
fn rule_lineage_mode(rule: &CompiledRule, provenance: bool) -> LineageMode {
    if prefold_prob_column(rule).is_none() {
        LineageMode::None
    } else if prob_column_is_probability_fold(rule) {
        LineageMode::Deriv
    } else if provenance && rule.clauses.iter().all(|c| c.fold.is_empty()) {
        LineageMode::Transient
    } else {
        LineageMode::None
    }
}

impl ProbRefSlot {
    /// Whether the calling rule keeps lineage, so the slot names the fact.
    fn keeps_lineage(&self) -> bool {
        self.lineage != LineageMode::None
    }

    /// The hidden column carrying the referenced fact's PROB value.
    fn prob_column(&self) -> String {
        let prefix = match self.lineage {
            LineageMode::Deriv => DERIV_ISPROB_COL_PREFIX,
            LineageMode::Transient => LIN_PROB_COL_PREFIX,
            LineageMode::None => ISPROB_COL_PREFIX,
        };
        format!("{prefix}{}_{}", self.clause_index, self.occurrence)
    }

    /// The hidden column carrying the hash of the referenced fact's KEY, or,
    /// for a negated reference, the negated facts the complement read.
    fn key_column(&self) -> String {
        let prefix = match (self.lineage, self.negated) {
            (LineageMode::Transient, true) => LIN_NOT_COL_PREFIX,
            (LineageMode::Transient, false) => LIN_REF_COL_PREFIX,
            (_, true) => DERIV_ISNOT_COL_PREFIX,
            (_, false) => DERIV_ISREF_COL_PREFIX,
        };
        format!("{prefix}{}_{}", self.clause_index, self.occurrence)
    }
}

/// A positive reference no [`ProbRefSlot`] names a fact for (one to a rule
/// without a PROB column, or from a rule that keeps no lineage), when
/// provenance is recorded: its transient columns name the referenced fact for
/// EXPLAIN alone (see [`SupportRef`]).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SupportSlot {
    /// The clause holding the reference.
    clause_index: usize,
    /// Its position among the clause's positive references, in text order.
    occurrence: usize,
    /// Whether the referenced rule has a PROB column, whose value the slot
    /// carries too.
    target_has_prob: bool,
}

impl SupportSlot {
    /// The hidden column carrying the hash of the referenced fact's KEY.
    fn key_column(&self) -> String {
        format!(
            "{LIN_SUP_COL_PREFIX}{}_{}",
            self.clause_index, self.occurrence
        )
    }

    /// The hidden column carrying the referenced fact's PROB value.
    fn prob_column(&self) -> String {
        format!(
            "{LIN_SUPP_COL_PREFIX}{}_{}",
            self.clause_index, self.occurrence
        )
    }
}

/// Every positive reference of `rule` that no [`ProbRefSlot`] names a fact
/// for, in clause then text order; empty unless `provenance`.
fn support_slots(
    rule: &CompiledRule,
    rule_catalog: &HashMap<String, CompiledRule>,
    provenance: bool,
) -> Vec<SupportSlot> {
    if !provenance {
        return Vec::new();
    }
    let lineage = rule_lineage_mode(rule, provenance);
    let mut slots = Vec::new();
    for (clause_index, clause) in rule.clauses.iter().enumerate() {
        let positives = clause.where_conditions.iter().filter_map(|c| match c {
            RuleCondition::IsReference(r) if !r.negated => Some(r),
            _ => None,
        });
        for (occurrence, is_ref) in positives.enumerate() {
            let target_has_prob = rule_catalog
                .get(&is_ref.rule_name.to_string())
                .is_some_and(|t| t.yield_schema.iter().any(|c| c.is_prob));
            if target_has_prob && lineage != LineageMode::None {
                // A lineage slot names this fact already.
                continue;
            }
            slots.push(SupportSlot {
                clause_index,
                occurrence,
                target_has_prob,
            });
        }
    }
    slots
}

/// The PROB column of `rule` as its clause bodies name it before any FOLD: the
/// FOLD variable when the PROB column is a fold output, the YIELD name for a
/// rule without FOLD. `None` when the rule has no PROB column, or when its PROB
/// column is computed after the fold (`YIELD 1 - total AS x PROB`), where no
/// per-row value exists to multiply a referenced probability into.
fn prefold_prob_column(rule: &CompiledRule) -> Option<String> {
    let prob = rule.yield_schema.iter().find(|c| c.is_prob)?;
    if let Some((fold_var, _)) = rule_fold_layout(rule)
        .into_iter()
        .find(|(_, yield_name)| *yield_name == prob.name)
    {
        return Some(fold_var);
    }
    rule.clauses
        .iter()
        .all(|c| c.fold.is_empty())
        .then(|| prob.name.clone())
}

/// Whether `rule`'s PROB column is an `MNOR` or `MPROD` fold.
fn prob_column_is_probability_fold(rule: &CompiledRule) -> bool {
    let Some(prob) = rule.yield_schema.iter().find(|c| c.is_prob) else {
        return false;
    };
    let Some((fold_var, _)) = rule_fold_layout(rule)
        .into_iter()
        .find(|(_, yield_name)| *yield_name == prob.name)
    else {
        return false;
    };
    rule.clauses.iter().flat_map(|c| c.fold.iter()).any(|fb| {
        fb.name == fold_var
            && fold_function_name(&fb.aggregate)
                .is_some_and(|n| matches!(n.to_uppercase().as_str(), "MNOR" | "MPROD"))
    })
}

/// Every reference to a PROB rule whose probability flows into `rule`'s PROB
/// column, in clause then text order, positive references of a clause before
/// its negated ones (see [`ProbRefSlot`]). Negated references have slots only
/// when `rule` keeps lineage; `provenance` says whether the run records
/// provenance, which a transient lineage needs ([`rule_lineage_mode`]).
fn prob_ref_slots(
    rule: &CompiledRule,
    rule_catalog: &HashMap<String, CompiledRule>,
    provenance: bool,
) -> Vec<ProbRefSlot> {
    let Some(into_column) = prefold_prob_column(rule) else {
        return Vec::new();
    };
    let lineage = rule_lineage_mode(rule, provenance);
    let mut slots = Vec::new();
    for (clause_index, clause) in rule.clauses.iter().enumerate() {
        let positives = clause.where_conditions.iter().filter_map(|c| match c {
            RuleCondition::IsReference(r) if !r.negated => Some(r),
            _ => None,
        });
        for (occurrence, is_ref) in positives.enumerate() {
            let target_has_prob = rule_catalog
                .get(&is_ref.rule_name.to_string())
                .is_some_and(|t| t.yield_schema.iter().any(|c| c.is_prob));
            if target_has_prob {
                slots.push(ProbRefSlot {
                    clause_index,
                    occurrence,
                    negated: false,
                    lineage,
                    into_column: into_column.clone(),
                });
            }
        }
        if lineage == LineageMode::None {
            continue;
        }
        let negatives = clause.where_conditions.iter().filter_map(|c| match c {
            RuleCondition::IsReference(r) if r.negated => Some(r),
            _ => None,
        });
        for (occurrence, is_ref) in negatives.enumerate() {
            let target_has_prob = rule_catalog
                .get(&is_ref.rule_name.to_string())
                .is_some_and(|t| t.yield_schema.iter().any(|c| c.is_prob));
            if target_has_prob {
                slots.push(ProbRefSlot {
                    clause_index,
                    occurrence,
                    negated: true,
                    lineage,
                    into_column: into_column.clone(),
                });
            }
        }
    }
    slots
}

/// The row-event hash expression for a clause (see [`DERIV_EVENT_COL`]), or
/// `None` when the row is its own event.
///
/// `expr` is the resolved PROB expression, `factored` the batch columns of
/// the referenced probabilities it reads as product factors, and `entity`
/// the identity column of a name that is a node or relationship.
fn row_event_expr(
    expr: &Expr,
    factored: &HashSet<String>,
    entity: &dyn Fn(&str) -> Option<String>,
) -> Option<Expr> {
    fn factors(expr: &Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: uni_cypher::ast::BinaryOp::Mul,
                right,
            } => {
                factors(left, out);
                factors(right, out);
            }
            other => out.push(other.clone()),
        }
    }
    fn names(expr: &Expr, out: &mut Vec<String>) {
        if let Expr::Variable(v) = expr {
            out.push(v.clone());
        }
        expr.for_each_child_in_scope(&mut |child| names(child, out));
    }
    let mut product = Vec::new();
    factors(expr, &mut product);
    product.retain(|f| !matches!(f, Expr::Variable(v) if factored.contains(v)));
    let mut read = Vec::new();
    for factor in &product {
        names(factor, &mut read);
    }
    // A value that is no graph property (a referenced value column, a
    // generator output) is identified by no node or relationship.
    let mut identities: Vec<(String, String)> = Vec::new();
    for name in read {
        let column = entity(&name)?;
        if !identities.iter().any(|(n, _)| *n == name) {
            identities.push((name, column));
        }
    }
    if identities.is_empty() {
        return None;
    }
    let shape: Vec<String> = product
        .into_iter()
        .map(|factor| {
            let renamed = map_variables(factor, &|name| {
                identities
                    .iter()
                    .position(|(n, _)| n == name)
                    .map(|i| Expr::Variable(format!("#{i}")))
            });
            format!("{renamed:?}")
        })
        .collect();
    let args = std::iter::once(Expr::Literal(CypherLiteral::String(shape.join(" * "))))
        .chain(
            identities
                .into_iter()
                .map(|(_, column)| Expr::Variable(column)),
        )
        .collect();
    Some(Expr::FunctionCall {
        name: crate::query::df_graph::locy_row_hash::ROW_HASH_FN.to_string(),
        args,
        distinct: false,
        window_spec: None,
    })
}

/// How `expr` — a clause's resolved PROB expression — reads a referenced
/// probability, any of whose batch columns `names` lists (see [`ProbRead`]).
fn classify_prob_read(expr: &Expr, names: &HashSet<String>) -> ProbRead {
    fn count_reads(expr: &Expr, names: &HashSet<String>) -> usize {
        let own = usize::from(matches!(expr, Expr::Variable(v) if names.contains(v)));
        let mut children = 0;
        expr.for_each_child_in_scope(&mut |child| children += count_reads(child, names));
        own + children
    }
    fn factors<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: uni_cypher::ast::BinaryOp::Mul,
                right,
            } => {
                factors(left, out);
                factors(right, out);
            }
            other => out.push(other),
        }
    }
    let reads = count_reads(expr, names);
    if reads == 0 {
        return ProbRead::Absent;
    }
    let mut product = Vec::new();
    factors(expr, &mut product);
    let direct = product
        .iter()
        .filter(|f| matches!(f, Expr::Variable(v) if names.contains(v)))
        .count();
    if direct == 1 && reads == 1 {
        ProbRead::Factor
    } else {
        ProbRead::Opaque
    }
}

/// The discriminating variables behind [`derivation_discriminator_columns`],
/// sorted by column name, or `None` when the rule needs no discriminators.
fn derivation_discriminators(
    rule: &CompiledRule,
    is_recursive: bool,
    stratum_rule_names: &HashSet<String>,
    rule_catalog: &HashMap<String, CompiledRule>,
) -> Option<Vec<DerivVar>> {
    let carries_fold_or_along = rule
        .clauses
        .iter()
        .any(|c| !c.fold.is_empty() || !c.along.is_empty());
    if !is_recursive || !carries_fold_or_along {
        return None;
    }

    let yielded_bare: HashSet<&str> = rule
        .clauses
        .iter()
        .filter_map(|c| match &c.output {
            RuleOutput::Yield(y) => Some(y.items.iter()),
            _ => None,
        })
        .flatten()
        .filter_map(|item| match &item.expr {
            Expr::Variable(v) => Some(v.as_str()),
            _ => None,
        })
        .collect();

    let mut vars: Vec<DerivVar> = rule
        .clauses
        .iter()
        .enumerate()
        .flat_map(|(clause_index, c)| {
            let mut nodes = HashSet::new();
            collect_match_node_vars(c, &mut nodes);
            nodes
                .into_iter()
                .map(|name| DerivVar {
                    name,
                    kind: DerivKind::Node,
                })
                .chain(match_edge_vars(c, clause_index))
        })
        .filter(|d| !yielded_bare.contains(d.name.as_str()))
        .chain(
            rule.clauses
                .iter()
                .enumerate()
                .flat_map(|(clause_index, c)| {
                    referenced_fact_vars(c, clause_index, stratum_rule_names, rule_catalog)
                }),
        )
        .collect();
    vars.sort_by_key(DerivVar::column);
    vars.dedup();
    Some(vars)
}

/// A MATCH-bound variable whose identity discriminates derivations (#159).
#[derive(Debug, Clone, PartialEq, Eq)]
struct DerivVar {
    name: String,
    kind: DerivKind,
}

/// What a discriminating variable binds, which decides the column it projects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DerivKind {
    /// A node: its `_vid`, as `__deriv_vid_{var}`.
    Node,
    /// A single-hop relationship: its `_eid`, as `__deriv_eid_{var}`.
    Edge,
    /// A variable-length relationship: its whole edge list, as
    /// `__deriv_path_{var}`. Two distinct paths between the same endpoints
    /// differ only here.
    EdgeList,
    /// The fact a positive same-stratum IS-reference read, as
    /// `__deriv_ref_{name}`: a hash of the referenced row (`locy_row_hash`).
    /// Two derivations through the same nodes and edges that extended
    /// different facts — two sub-paths with equal values — differ only here.
    Ref {
        /// The clause holding the reference.
        clause_index: usize,
        /// Its position among the clause's positive references.
        occurrence: usize,
    },
}

impl DerivKind {
    /// The Arrow type of the projected discriminator column.
    fn data_type(self) -> DataType {
        match self {
            Self::Node | Self::Edge => DataType::UInt64,
            Self::EdgeList => build_edge_list_field("_").data_type().clone(),
            Self::Ref { .. } => crate::query::df_graph::locy_row_hash::row_hash_type(),
        }
    }
}

impl DerivVar {
    /// The hidden discriminator column this variable is projected as.
    fn column(&self) -> String {
        let kind = match self.kind {
            DerivKind::Node => "vid",
            DerivKind::Edge => "eid",
            DerivKind::EdgeList => "path",
            DerivKind::Ref { .. } => "ref",
        };
        format!("{FOLD_DISCRIMINATOR_COL_PREFIX}{kind}_{}", self.name)
    }

    /// The scan column holding this variable's identity.
    fn id_column(&self) -> String {
        match self.kind {
            DerivKind::Node => format!("{}._vid", self.name),
            DerivKind::Edge => format!("{}._eid", self.name),
            DerivKind::EdgeList | DerivKind::Ref { .. } => self.name.clone(),
        }
    }
}

/// The referenced-fact discriminators of one clause: one per positive
/// IS-reference to a rule of the same stratum that the clause reads per
/// derivation (the Contributions view). A reference reading the Folded view
/// gets none — that row is its KEY with the child's *current* folded value, so
/// hashing it would give a re-derived contribution a new identity each
/// iteration, and it would be added rather than replace the old one.
///
/// The view rule here must match the one `build_clause` applies.
fn referenced_fact_vars(
    clause: &CompiledClause,
    clause_index: usize,
    stratum_rule_names: &HashSet<String>,
    rule_catalog: &HashMap<String, CompiledRule>,
) -> Vec<DerivVar> {
    clause
        .where_conditions
        .iter()
        .filter_map(|c| match c {
            RuleCondition::IsReference(r) if !r.negated => Some(r),
            _ => None,
        })
        .enumerate()
        .filter(|(_, r)| {
            let target = r.rule_name.to_string();
            let same_stratum = stratum_rule_names.contains(&target);
            let target_has_fold = rule_catalog
                .get(&target)
                .is_some_and(|t| t.clauses.iter().any(|c| !c.fold.is_empty()));
            let folded_view = same_stratum && target_has_fold && clause.along.is_empty();
            same_stratum && !folded_view
        })
        .map(|(occurrence, _)| DerivVar {
            name: format!("{clause_index}_{occurrence}"),
            kind: DerivKind::Ref {
                clause_index,
                occurrence,
            },
        })
        .collect()
}

/// The relationship variables of a clause's MATCH, in pattern order.
///
/// A single-hop relationship is identified by its `_eid`, a variable-length one
/// by its edge list. An anonymous relationship is reported under the name
/// [`name_anonymous_edges`] gives it, so anonymous parallel edges and paths are
/// told apart too.
fn match_edge_vars(clause: &CompiledClause, clause_index: usize) -> Vec<DerivVar> {
    let mut vars = Vec::new();
    for (path_index, path) in clause.match_pattern.paths.iter().enumerate() {
        for (elem_index, elem) in path.elements.iter().enumerate() {
            if let PatternElement::Relationship(rp) = elem {
                vars.push(DerivVar {
                    name: rp.variable.clone().unwrap_or_else(|| {
                        anonymous_edge_name(clause_index, path_index, elem_index)
                    }),
                    kind: if rp.range.is_none() {
                        DerivKind::Edge
                    } else {
                        DerivKind::EdgeList
                    },
                });
            }
        }
    }
    vars
}

/// The hidden variable an anonymous relationship is bound to.
///
/// Positional, so the name is the same whether it is computed from the
/// compiled rule (as an IS-ref target's discriminator schema is) or from the
/// renamed clause the body is planned from.
fn anonymous_edge_name(clause_index: usize, path_index: usize, elem_index: usize) -> String {
    format!("__anon_edge_{clause_index}_{path_index}_{elem_index}")
}

/// A copy of `clause` with every anonymous relationship bound to its
/// [`anonymous_edge_name`], so the scan exposes the identity its derivation
/// discriminator reads (issue #294). Binding a variable does not change which
/// rows the MATCH produces.
fn name_anonymous_edges(clause: &CompiledClause, clause_index: usize) -> CompiledClause {
    let mut named = clause.clone();
    for (path_index, path) in named.match_pattern.paths.iter_mut().enumerate() {
        for (elem_index, elem) in path.elements.iter_mut().enumerate() {
            if let PatternElement::Relationship(rp) = elem
                && rp.variable.is_none()
            {
                rp.variable = Some(anonymous_edge_name(clause_index, path_index, elem_index));
            }
        }
    }
    named
}

/// Whether `fb` is a non-DISTINCT `COUNT` / `MCOUNT`.
///
/// Such a fold only needs to know whether each input is NULL, which is what
/// lets [`count_input_marker`] replace the input with an integer.
fn is_count_fold(fb: &uni_cypher::locy_ast::FoldBinding) -> bool {
    matches!(
        &fb.aggregate,
        Expr::FunctionCall { name, distinct: false, .. }
            if name.eq_ignore_ascii_case("COUNT") || name.eq_ignore_ascii_case("MCOUNT")
    )
}

/// `CASE WHEN <input> IS NULL THEN NULL ELSE 1 END`: a count's input reduced
/// to what the count observes.
///
/// A count over a node or relationship used to carry the entity itself into
/// the fold column, where a sibling clause's integer seed met it and the
/// batches could not be concatenated. A MATCH-bound entity is tested through
/// its identity column, which the scan always exposes.
fn count_input_marker(input: Expr, clause: &CompiledClause, clause_index: usize) -> Expr {
    let mut nodes = HashSet::new();
    collect_match_node_vars(clause, &mut nodes);
    let edges: HashSet<String> = match_edge_vars(clause, clause_index)
        .into_iter()
        .filter(|d| d.kind == DerivKind::Edge)
        .map(|d| d.name)
        .collect();
    let probe = match &input {
        Expr::Variable(v) if nodes.contains(v) => Expr::Variable(format!("{v}._vid")),
        Expr::Variable(v) if edges.contains(v) => Expr::Variable(format!("{v}._eid")),
        _ => input,
    };
    Expr::Case {
        expr: None,
        when_then: vec![(
            Expr::IsNull(Box::new(probe)),
            Expr::Literal(CypherLiteral::Null),
        )],
        else_expr: Some(Box::new(Expr::Literal(CypherLiteral::Integer(1)))),
    }
}

/// A FOLD rule's FOLD-input columns in the order its FOLD clause projects
/// them, as `(FOLD variable, YIELD column)`.
///
/// The FOLD clause projects its YIELD columns other than FOLD outputs in YIELD
/// order, then one input column per aggregate that has an argument, named by
/// the FOLD variable. A sibling clause that seeds those columns is unioned
/// with it by position, so it must lay its columns out the same way, which
/// YIELD order gives only when every KEY is yielded before every FOLD output:
/// `YIELD 1 AS s, KEY a` beside `FOLD s = ... YIELD s, KEY a` unioned the seed
/// value with the KEY and failed with "concatenate arrays of different data
/// types". Empty for a rule without FOLD.
fn rule_fold_layout(rule: &CompiledRule) -> Vec<(String, String)> {
    let Some(fold_clause) = rule.clauses.iter().find(|c| !c.fold.is_empty()) else {
        return Vec::new();
    };
    let yield_name_of: HashMap<&str, String> = match &fold_clause.output {
        RuleOutput::Yield(yc) => resolve_yield_column_names(&yc.items)
            .into_iter()
            .zip(yc.items.iter())
            .filter_map(|(name, item)| match &item.expr {
                Expr::Variable(var) => Some((var.as_str(), name)),
                _ => None,
            })
            .collect(),
        _ => HashMap::new(),
    };
    fold_clause
        .fold
        .iter()
        .filter(|fb| matches!(&fb.aggregate, Expr::FunctionCall { args, .. } if !args.is_empty()))
        .map(|fb| {
            let yield_name = yield_name_of
                .get(fb.name.as_str())
                .cloned()
                .unwrap_or_else(|| fb.name.clone());
            (fb.name.clone(), yield_name)
        })
        .collect()
}

/// One type for each column the rule's clauses would project with different
/// types, keyed by YIELD column name, and FOLD inputs by FOLD variable.
///
/// Each clause infers its column types from its own expressions, and the
/// clauses' batches are then unioned, so two clauses typing one column
/// differently fail ("concatenate arrays of different data types"): `r.w` of
/// a property no schema declares is a Cypher value, `e.w + 1` a float. Mixed
/// integers and floats unify to a float, anything else to a Cypher value,
/// which carries any value. A column every clause types the same is absent,
/// so a rule whose clauses agree plans as before. Seeded FOLD columns and
/// ALONG columns are unified separately (`fold_input_column_types`,
/// `along_column_types`); `COLLECT` keeps its elements' type.
fn clause_column_types(
    rule: &CompiledRule,
    rule_catalog: &HashMap<String, CompiledRule>,
    schema: &Schema,
) -> HashMap<String, DataType> {
    if rule.clauses.len() < 2 {
        return HashMap::new();
    }
    let along_types = along_column_types(&rule.clauses, schema);
    let seeded: HashSet<String> = rule_fold_layout(rule)
        .into_iter()
        .map(|(_, yield_name)| yield_name)
        .collect();
    // The node variables the planner types every clause with: MATCH nodes
    // and nodes an IS reference binds, over all clauses. A per-clause MATCH
    // set would type `p` bound by `(parent, p) IS r` unlike the base clause's
    // MATCH `p`, and unify a node column into a Cypher value.
    let node_vars = collect_node_vars(&rule.clauses);
    let mut types: HashMap<String, Vec<DataType>> = HashMap::new();
    for clause in &rule.clauses {
        let RuleOutput::Yield(yc) = &clause.output else {
            continue;
        };
        let fold_names: HashSet<&str> = clause.fold.iter().map(|f| f.name.as_str()).collect();
        let var_labels = clause_var_labels(clause);
        let infer = |name: &str, is_key: bool| {
            infer_yield_type(
                name,
                clause,
                &node_vars,
                &fold_names,
                &along_types,
                rule_catalog,
                is_key,
                schema,
                &var_labels,
            )
        };
        for (name, item) in resolve_yield_column_names(&yc.items)
            .into_iter()
            .zip(&yc.items)
        {
            let fold_output = expr_references_fold_output(&item.expr, &fold_names);
            let seed = clause.fold.is_empty() && seeded.contains(&name);
            if fold_output || seed || along_types.contains_key(&name) {
                continue;
            }
            let ty = infer(&name, item.is_key);
            types.entry(name).or_default().push(ty);
        }
        for fb in &clause.fold {
            let has_argument =
                matches!(&fb.aggregate, Expr::FunctionCall { args, .. } if !args.is_empty());
            if !has_argument || fold_function_name(&fb.aggregate).as_deref() == Some("COLLECT") {
                continue;
            }
            types
                .entry(fb.name.clone())
                .or_default()
                .push(infer(&fb.name, false));
        }
    }
    types
        .into_iter()
        .filter(|(_, tys)| tys.iter().any(|t| *t != tys[0]))
        .map(|(name, tys)| {
            let numeric = tys
                .iter()
                .all(|t| matches!(t, DataType::Int64 | DataType::Float64));
            let ty = if numeric {
                DataType::Float64
            } else {
                DataType::LargeBinary
            };
            (name, ty)
        })
        .collect()
}

/// The type each FOLD-output column carries *before* the fold, keyed by the
/// rule's yield column name.
///
/// A FOLD aggregates every row of its rule per KEY, including the rows of a
/// clause with no FOLD that yields the same column: such a clause seeds the
/// column, and its value is one more input to the aggregate (SQL's
/// `AGG(v) FROM (seeds UNION ALL inputs) GROUP BY key`). Both kinds of row
/// share one column, so the seed must be projected with the fold input's type.
/// Without this, `YIELD KEY e, 0 AS v` beside `FOLD v = MSUM(r.pct)` produced
/// an Int64 batch next to a Float64 one and the merge failed.
fn fold_input_column_types(
    rule: &CompiledRule,
    rule_catalog: &HashMap<String, CompiledRule>,
    schema: &Schema,
) -> HashMap<String, DataType> {
    let Some(fold_clause) = rule.clauses.iter().find(|c| !c.fold.is_empty()) else {
        return HashMap::new();
    };
    let RuleOutput::Yield(yc) = &fold_clause.output else {
        return HashMap::new();
    };
    let fold_names: HashSet<&str> = fold_clause.fold.iter().map(|f| f.name.as_str()).collect();
    let mut node_vars = HashSet::new();
    collect_match_node_vars(fold_clause, &mut node_vars);
    let var_labels = clause_var_labels(fold_clause);
    // By name: the rule's schema is in binding order, the clause's items in
    // YIELD order.
    let item_by_name: HashMap<String, &uni_cypher::locy_ast::LocyYieldItem> =
        resolve_yield_column_names(&yc.items)
            .into_iter()
            .zip(yc.items.iter())
            .collect();
    rule.yield_schema
        .iter()
        .filter_map(|col| item_by_name.get(&col.name).map(|item| (col, *item)))
        .filter_map(|(col, item)| {
            let Expr::Variable(fold_name) = &item.expr else {
                return None;
            };
            fold_names.contains(fold_name.as_str()).then(|| {
                let ty = infer_yield_type(
                    fold_name,
                    fold_clause,
                    &node_vars,
                    &fold_names,
                    &HashMap::new(),
                    rule_catalog,
                    false,
                    schema,
                    &var_labels,
                );
                (col.name.clone(), ty)
            })
        })
        .collect()
}

struct ClauseCtx<'a> {
    stratum_rule_names: &'a HashSet<String>,
    rule_catalog: &'a HashMap<String, CompiledRule>,
    node_vars: &'a HashSet<String>,
    /// Rule-level derivation-discriminator variables (issue #159), sorted.
    ///
    /// The **union across the rule's clauses**, so every clause emits the same
    /// schema: a clause that binds the variable projects `{var}._vid`, one that
    /// does not projects a typed NULL. Empty unless the rule is recursive and
    /// carries FOLD or ALONG. See [`FOLD_DISCRIMINATOR_COL_PREFIX`].
    deriv_vars: &'a [DerivVar],
    /// Target types for the columns a sibling clause folds into, keyed by
    /// yield column name. A clause with no FOLD that yields such a column is
    /// seeding it, and its value is aggregated alongside the fold's inputs, so
    /// it must carry the fold input's type. See [`fold_input_column_types`].
    seed_types: &'a HashMap<String, DataType>,
    /// The rule's FOLD-input columns in the order its FOLD clause projects
    /// them, as `(FOLD variable, YIELD column)`. See [`rule_fold_layout`].
    fold_layout: &'a [(String, String)],
    /// Whether this rule's stratum is recursive — needed to compute a
    /// same-stratum IS-ref target's own discriminator columns.
    is_recursive: bool,
    /// This clause's index within the rule, projected as `__deriv_clause` so
    /// two clauses' rows can never collide through NULL padding.
    clause_index: usize,
    /// The rule's positive references to PROB rules, across all its clauses
    /// (see [`prob_ref_slots`]). A lineage-keeping rule projects every slot's
    /// columns in every clause, NULL outside the slot's own clause.
    prob_slots: &'a [ProbRefSlot],
    /// For a rule that keeps lineage ([`rule_lineage_mode`]): its PROB column
    /// as the clause bodies name it. Every clause then projects the row-event
    /// column, [`DERIV_EVENT_COL`] or, when `transient_lineage`,
    /// [`LIN_EVENT_COL`].
    lineage_prob_column: Option<&'a str>,
    /// Whether the rule's lineage is transient ([`LineageMode::Transient`]).
    transient_lineage: bool,
    /// The rule's positive references no [`ProbRefSlot`] names a fact for,
    /// across all its clauses (see [`support_slots`]). Every clause projects
    /// every slot's columns, NULL outside the slot's own clause.
    support_slots: &'a [SupportSlot],
}

impl<'a> LocyPlanBuilder<'a> {
    /// Create a new plan builder backed by the given `QueryPlanner`.
    pub fn new(planner: &'a QueryPlanner) -> Self {
        Self {
            planner,
            derived_scan_handles: RefCell::new(Vec::new()),
            plugin_registry: crate::query::df_graph::locy_fold::default_locy_plugin_registry(),
            reused_strata: std::collections::BTreeSet::new(),
            provenance: false,
        }
    }

    /// Plan for a run that records provenance (a provenance tracker is
    /// attached: `CompiledProgram::needs_provenance_tracker`).
    ///
    /// The clause bodies then also project the transient columns
    /// ([`LIN_COL_PREFIX`](crate::query::planner_locy_types::LIN_COL_PREFIX)) that name, for each derivation row, the facts it
    /// references — for EXPLAIN — and, for a PROB rule without a fold, its
    /// lineage — for exact inference over the facts that reference it.
    /// Without a tracker nothing reads them, so they are not projected.
    #[must_use]
    pub fn with_provenance(mut self, provenance: bool) -> Self {
        self.provenance = provenance;
        self
    }

    /// Skip planning the strata at `positions`, whose facts the run reuses.
    ///
    /// A reused stratum is not evaluated: `LocyProgramExec` copies its rules'
    /// facts by name and reads nothing else of the stratum (see
    /// [`crate::query::df_graph::ReusedStrata`]). Planning its clause bodies
    /// was the dominant cost of a hypothetical re-evaluation that reuses
    /// most of the program, so such a stratum is emitted with its rules'
    /// names and YIELD columns only. The plan must then be run with exactly
    /// these positions reused; evaluating it otherwise derives nothing for
    /// them.
    #[must_use]
    pub fn with_reused_strata(mut self, positions: std::collections::BTreeSet<usize>) -> Self {
        self.reused_strata = positions;
        self
    }

    /// Replace the plugin registry used for aggregate monotonicity resolution.
    #[must_use]
    pub fn with_plugin_registry(
        mut self,
        registry: std::sync::Arc<uni_plugin::PluginRegistry>,
    ) -> Self {
        self.plugin_registry = registry;
        self
    }

    /// Build a full `LogicalPlan::LocyProgram` with embedded `DerivedScanRegistry`.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
    ) -> Result<LogicalPlan> {
        // Legacy entry point — defaults to AddMultProb. New code paths
        // should call `build_program_plan_with_semiring_and_classifiers`.
        self.build_program_plan_with_semiring_and_classifiers(
            compiled,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            uni_locy::SemiringKind::AddMultProb,
            Arc::new(uni_locy::ClassifierRegistry::new()),
        )
    }

    /// Build a `LocyProgram` plan threading an explicit semiring through.
    /// Compatibility wrapper; for full Slice 3 wiring use
    /// `build_program_plan_with_semiring_and_classifiers`.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan_with_semiring(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: uni_locy::SemiringKind,
    ) -> Result<LogicalPlan> {
        self.build_program_plan_with_semiring_and_classifiers(
            compiled,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            semiring_kind,
            Arc::new(uni_locy::ClassifierRegistry::new()),
        )
    }

    /// Phase B Slice 3 entry: threads both the active semiring and the
    /// runtime classifier registry through the plan.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan_with_semiring_and_classifiers(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: uni_locy::SemiringKind,
        classifier_registry: Arc<uni_locy::ClassifierRegistry>,
    ) -> Result<LogicalPlan> {
        self.build_program_plan_with_full_neural(
            compiled,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            semiring_kind,
            classifier_registry,
            None,
            None,
        )
    }

    /// Phase B follow-up: full entry threading both the classifier
    /// registry and the optional memoization cache.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan_with_full_neural(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: uni_locy::SemiringKind,
        classifier_registry: Arc<uni_locy::ClassifierRegistry>,
        classifier_cache: Option<Arc<uni_locy::ModelInvocationCache>>,
        classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    ) -> Result<LogicalPlan> {
        let mut strata = Vec::with_capacity(compiled.strata.len());

        let classifiers = ClassifierContext {
            registry: Arc::clone(&classifier_registry),
            cache: classifier_cache.as_ref().map(Arc::clone),
            provenance_store: classifier_provenance_store.as_ref().map(Arc::clone),
        };
        for (pos, stratum) in compiled.strata.iter().enumerate() {
            if self.reused_strata.contains(&pos) {
                strata.push(reused_stratum_stub(stratum));
                continue;
            }
            let rule_names: HashSet<String> =
                stratum.rules.iter().map(|r| r.name.clone()).collect();
            let locy_stratum =
                self.build_stratum(stratum, &compiled.rule_catalog, &rule_names, &classifiers)?;
            strata.push(locy_stratum);
        }

        let registry = self.build_registry();
        let plan = LogicalPlan::LocyProgram {
            strata,
            commands: self.build_commands_with_models(&compiled.commands, &compiled.model_catalog),
            derived_scan_registry: Arc::new(registry),
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            semiring_kind,
            classifier_registry,
            classifier_cache,
            classifier_provenance_store,
        };

        Ok(plan)
    }

    /// Build `LocyCommand` list from compiled commands.
    ///
    /// Commands carry AST data for dispatch by the caller (e.g., `evaluate_native`)
    /// via the orchestrator after strata evaluation completes.
    fn build_commands_with_models(
        &self,
        commands: &[CompiledCommand],
        model_catalog: &HashMap<String, uni_locy::CompiledModel>,
    ) -> Vec<LocyCommand> {
        commands
            .iter()
            .map(|cmd| match cmd {
                CompiledCommand::GoalQuery(gq) => LocyCommand::GoalQuery {
                    goal_query: gq.clone(),
                },
                CompiledCommand::ExplainRule(er) => LocyCommand::ExplainRule {
                    explain_rule: er.clone(),
                },
                CompiledCommand::Abduce(aq) => LocyCommand::Abduce {
                    abduce_query: aq.clone(),
                },
                CompiledCommand::Assume(ca) => LocyCommand::Assume {
                    compiled_assume: ca.clone(),
                },
                CompiledCommand::DeriveCommand(dc) => LocyCommand::Derive {
                    derive_command: dc.clone(),
                },
                CompiledCommand::Cypher(q) => LocyCommand::Cypher { query: q.clone() },
                CompiledCommand::Calibrate(cc) => {
                    let inputs = model_catalog
                        .get(&cc.model_name)
                        .map(|m| m.inputs.clone())
                        .unwrap_or_default();
                    LocyCommand::Calibrate {
                        calibrate: cc.clone(),
                        model_inputs: inputs,
                    }
                }
                CompiledCommand::Validate(cv) => LocyCommand::Validate {
                    validate: cv.clone(),
                },
            })
            .collect()
    }

    // -- Stratum --------------------------------------------------------

    fn build_stratum(
        &self,
        stratum: &Stratum,
        rule_catalog: &HashMap<String, CompiledRule>,
        stratum_rule_names: &HashSet<String>,
        classifiers: &ClassifierContext,
    ) -> Result<LocyStratum> {
        let mut rules = Vec::with_capacity(stratum.rules.len());
        for rule in &stratum.rules {
            rules.push(self.build_rule(
                rule,
                stratum.is_recursive,
                stratum_rule_names,
                rule_catalog,
                classifiers,
            )?);
        }

        Ok(LocyStratum {
            id: stratum.id,
            rules,
            is_recursive: stratum.is_recursive,
            depends_on: stratum.depends_on.clone(),
        })
    }

    // -- Rule -----------------------------------------------------------

    fn build_rule(
        &self,
        rule: &CompiledRule,
        is_recursive: bool,
        stratum_rule_names: &HashSet<String>,
        rule_catalog: &HashMap<String, CompiledRule>,
        classifiers: &ClassifierContext,
    ) -> Result<LocyRulePlan> {
        // Collect node variable names from match patterns for VID-based joins
        let node_vars = collect_node_vars(&rule.clauses);

        // Derivation discriminators for a recursive FOLD / ALONG rule (#159).
        // See `derivation_discriminator_columns`.
        let deriv_columns =
            derivation_discriminator_columns(rule, is_recursive, stratum_rule_names, rule_catalog);
        let deriv_vars =
            derivation_discriminators(rule, is_recursive, stratum_rule_names, rule_catalog)
                .unwrap_or_default();
        // Columns a sibling clause folds into, and ALONG columns: every clause
        // must emit them with one type, unified over the rule.
        let mut seed_types = fold_input_column_types(rule, rule_catalog, self.planner.schema());
        // A FOLD's type wins over an ALONG binding of the same name.
        for (name, dt) in along_column_types(&rule.clauses, self.planner.schema()) {
            seed_types.entry(name).or_insert(dt);
        }

        let fold_layout = rule_fold_layout(rule);
        // Columns the clauses type differently, unified; a FOLD input's type
        // also decides the type of the column seeding it.
        let unified = clause_column_types(rule, rule_catalog, self.planner.schema());
        for (fold_var, yield_name) in &fold_layout {
            if let Some(dt) = unified.get(fold_var) {
                seed_types.insert(yield_name.clone(), dt.clone());
            }
        }
        seed_types.extend(unified.iter().map(|(k, v)| (k.clone(), v.clone())));

        let prob_slots = prob_ref_slots(rule, rule_catalog, self.provenance);
        let support_slots = support_slots(rule, rule_catalog, self.provenance);
        let lineage = rule_lineage_mode(rule, self.provenance);
        let lineage_prob_column = (lineage != LineageMode::None)
            .then(|| prefold_prob_column(rule))
            .flatten();
        let mut clauses = Vec::with_capacity(rule.clauses.len());
        for (clause_index, clause) in rule.clauses.iter().enumerate() {
            let named;
            let clause = if deriv_vars.is_empty() {
                clause
            } else {
                named = name_anonymous_edges(clause, clause_index);
                &named
            };
            clauses.push(self.build_clause(
                clause,
                &rule.yield_schema,
                is_recursive,
                ClauseCtx {
                    stratum_rule_names,
                    rule_catalog,
                    node_vars: &node_vars,
                    deriv_vars: &deriv_vars,
                    seed_types: &seed_types,
                    fold_layout: &fold_layout,
                    is_recursive,
                    clause_index,
                    prob_slots: &prob_slots,
                    lineage_prob_column: lineage_prob_column.as_deref(),
                    transient_lineage: lineage == LineageMode::Transient,
                    support_slots: &support_slots,
                },
                classifiers,
            )?);
        }

        // All clauses share the same schema; derive metadata from first clause
        let first_clause = rule.clauses.first();
        // HAVING / BEST BY / FOLD-output metadata must come from the SAME clause
        // that provides the FOLD (mirroring `fold_bindings` below), not clause 0:
        // a base clause (no FOLD) followed by a recursive clause with
        // `FOLD ... HAVING ... BEST BY` would otherwise silently lose its HAVING
        // filter and BEST BY pruning. Falls back to clause 0 when no clause folds.
        let fold_clause = rule
            .clauses
            .iter()
            .find(|c| !c.fold.is_empty())
            .or(first_clause);

        // Collect fold bindings from the first clause that has them.
        // A rule may have a base clause (no FOLD) plus recursive clauses (with FOLD);
        // we need the FOLD metadata from whichever clause provides it.
        //
        // Each entry is (fold_name, yield_alias, aggregate_expr). The yield_alias
        // is the output column name from the YIELD clause (may differ from fold_name
        // when the user writes e.g. `YIELD ... n AS support`).
        let fold_bindings: Vec<(String, String, Expr)> = rule
            .clauses
            .iter()
            .find(|c| !c.fold.is_empty())
            .map(|c| {
                // Build fold_name → yield alias mapping from the YIELD items.
                let yield_alias_map: HashMap<&str, &str> = match &c.output {
                    RuleOutput::Yield(yc) => yc
                        .items
                        .iter()
                        .filter_map(|item| {
                            if let Expr::Variable(ref v) = item.expr {
                                item.alias.as_deref().map(|alias| (v.as_str(), alias))
                            } else {
                                None
                            }
                        })
                        .collect(),
                    _ => HashMap::new(),
                };
                c.fold
                    .iter()
                    .map(|fb| {
                        let alias = yield_alias_map
                            .get(fb.name.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| fb.name.clone());
                        (fb.name.clone(), alias, fb.aggregate.clone())
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Build fold_name → yield_alias substitution map for HAVING / BEST BY
        // expressions that reference FOLD outputs by their original name.
        let fold_alias_subs: HashMap<String, String> = fold_bindings
            .iter()
            .filter(|(name, alias, _)| name != alias)
            .map(|(name, alias, _)| (name.clone(), alias.clone()))
            .collect();

        let having = fold_clause
            .map(|c| {
                c.having
                    .iter()
                    .map(|expr| substitute_fold_aliases(expr.clone(), &fold_alias_subs))
                    .collect()
            })
            .unwrap_or_default();
        // #265: same alias substitution as HAVING — a REQUIRE naming a fold
        // output by its original name must resolve against the snapshot the
        // same way, or it would silently match nothing.
        let require: Vec<Expr> = fold_clause
            .map(|c| {
                c.require
                    .iter()
                    .map(|expr| substitute_fold_aliases(expr.clone(), &fold_alias_subs))
                    .collect()
            })
            .unwrap_or_default();
        let best_by_criteria = fold_clause
            .and_then(|c| c.best_by.as_ref())
            .map(|bb| {
                bb.items
                    .iter()
                    .map(|item| {
                        (
                            substitute_fold_aliases(item.expr.clone(), &fold_alias_subs),
                            item.ascending,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let fold_output_names: HashSet<&str> = fold_clause
            .map(|c| c.fold.iter().map(|fb| fb.name.as_str()).collect())
            .unwrap_or_default();
        let graph_schema = self.planner.schema();
        let along_types = along_column_types(&rule.clauses, graph_schema);
        let var_labels = first_clause.map(clause_var_labels).unwrap_or_default();

        let yield_schema: Vec<LocyYieldColumn> = rule
            .yield_schema
            .iter()
            .map(|yc| {
                let data_type = match first_clause {
                    _ if unified.contains_key(&yc.name)
                        && !fold_output_names.contains(yc.name.as_str()) =>
                    {
                        unified[&yc.name].clone()
                    }
                    Some(fc) => infer_yield_type(
                        &yc.name,
                        fc,
                        &node_vars,
                        &fold_output_names,
                        &along_types,
                        rule_catalog,
                        yc.is_key,
                        graph_schema,
                        &var_labels,
                    ),
                    None => DataType::LargeUtf8,
                };
                LocyYieldColumn {
                    name: yc.name.clone(),
                    is_key: yc.is_key,
                    is_prob: yc.is_prob,
                    data_type,
                }
            })
            .collect();

        // Post-fold YIELD projection specs.
        //
        // A YIELD expression that references a FOLD output but is not a bare
        // fold-output variable (e.g. `total * 2.0 AS score`) cannot be produced
        // in the pre-fold body — the aggregate does not exist there. Record the
        // full yield-column projection so it can run POST-fold (`apply_post_fold_
        // projection`). Only populated when at least one column is such a
        // computed expression; otherwise the common path is unchanged (FoldExec
        // output already matches `yield_schema`).
        let yield_item_exprs: HashMap<String, &Expr> = match first_clause.map(|c| &c.output) {
            Some(RuleOutput::Yield(yc)) => resolve_yield_column_names(&yc.items)
                .into_iter()
                .zip(yc.items.iter())
                .map(|(name, item)| (name, &item.expr))
                .collect(),
            _ => HashMap::new(),
        };
        let has_computed_fold_expr = yield_schema.iter().any(|yc| {
            !yc.is_key
                && yield_item_exprs.get(&yc.name).is_some_and(|e| {
                    expr_references_fold_output(e, &fold_output_names)
                        && !matches!(e, Expr::Variable(v) if fold_output_names.contains(v.as_str()))
                })
        });
        let yield_projection: Vec<(String, Expr)> = if has_computed_fold_expr {
            yield_schema
                .iter()
                .map(|yc| {
                    // KEY and bare fold-output columns are already present in the
                    // post-fold batch under their yield name — pass them through.
                    // Computed expressions over fold outputs are evaluated, with
                    // fold-var → alias substitution so references match the
                    // post-fold column names (`FoldBinding.output_name`).
                    let expr = if yc.is_key {
                        Expr::Variable(yc.name.clone())
                    } else {
                        match yield_item_exprs.get(&yc.name) {
                            Some(e) => substitute_fold_aliases((*e).clone(), &fold_alias_subs),
                            None => Expr::Variable(yc.name.clone()),
                        }
                    };
                    (yc.name.clone(), expr)
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(LocyRulePlan {
            name: rule.name.clone(),
            clauses,
            yield_schema,
            priority: rule.priority,
            fold_bindings,
            require,
            having,
            best_by_criteria,
            yield_projection,
            deriv_columns,
        })
    }

    // -- Clause ---------------------------------------------------------

    fn build_clause(
        &self,
        clause: &CompiledClause,
        yield_cols: &[YieldColumn],
        is_recursive: bool,
        ctx: ClauseCtx<'_>,
        classifiers: &ClassifierContext,
    ) -> Result<LocyClausePlan> {
        let stratum_rule_names = ctx.stratum_rule_names;
        let rule_catalog = ctx.rule_catalog;
        let node_vars = ctx.node_vars;
        let classifier_registry = Arc::clone(&classifiers.registry);
        let classifier_cache = classifiers.cache.as_ref().map(Arc::clone);
        let classifier_provenance_store = classifiers.provenance_store.as_ref().map(Arc::clone);

        // Reject non-monotone FOLD aggregates in recursive strata using the
        // plugin registry's Semilattice metadata. Defense in depth: the
        // uni-locy typecheck pass usually rejects this upstream, but
        // direct LocyPlanBuilder consumers (in-process tests, future API
        // surfaces) might bypass that pass.
        if is_recursive {
            for fold in &clause.fold {
                let fname = match &fold.aggregate {
                    uni_cypher::ast::Expr::FunctionCall { name, .. } => name.clone(),
                    _ => {
                        anyhow::bail!(
                            "FOLD '{}' aggregate must be a function call (e.g., SUM(x))",
                            fold.name
                        );
                    }
                };
                match crate::query::df_graph::locy_fold::locy_monotonicity_verdict(
                    &self.plugin_registry,
                    &fname,
                ) {
                    Some(true) => {}
                    Some(false) | None => {
                        anyhow::bail!(
                            "non-monotonic aggregate '{}' in recursive rule clause (FOLD '{}')",
                            fname,
                            fold.name
                        );
                    }
                }
            }
        }

        // Validate every IS reference before consulting the binding plan, so a
        // clause naming an unknown rule, or binding more columns than the rule
        // yields, fails with that error. The compiler checks the same; this
        // guards programs built without it.
        for condition in &clause.where_conditions {
            let RuleCondition::IsReference(is_ref) = condition else {
                continue;
            };
            let target_rule_name = is_ref.rule_name.to_string();
            let target_rule = rule_catalog
                .get(&target_rule_name)
                .with_context(|| format!("IS-reference to unknown rule '{}'", target_rule_name))?;
            if uni_locy::is_binding::bound_columns(
                &target_rule.yield_schema,
                is_ref.subjects.len(),
                is_ref.target.is_some(),
            )
            .is_none()
            {
                bail!(
                    "IS-reference to '{}': arity mismatch — {} bindings \
                     provided but rule yields only {} columns",
                    target_rule_name,
                    is_ref.subjects.len() + usize::from(is_ref.target.is_some()),
                    target_rule.yield_schema.len(),
                );
            }
        }

        // The one binding decision for this clause (`uni_locy::binding_plan`):
        // which occurrence of each variable binds it, and in which order the
        // body's conditions run. Every name below is resolved through it.
        let binding = clause.binding.plan().with_context(|| {
            format!(
                "internal error: clause {} reached the Locy planner without a binding plan",
                ctx.clause_index
            )
        })?;

        // An `IS NOT` never compares a value column: the binding plan reports
        // a bound variable there as `IsNotBindsValueColumn`, a compile error
        // raised wherever plans are attached, so no plan reaching here has one.
        debug_assert!(
            binding.refs.iter().filter(|r| r.negated).all(|r| r
                .positions
                .iter()
                .all(|p| p.is_key || p.action == PositionAction::Existential)),
            "an IS NOT value comparison reached the planner; the binding plan rejects it"
        );

        // The body's names, resolved through the plan, plus the columns
        // `LocyModelInvoke` adds, which the plan does not source.
        let scope = ClauseScope {
            plan: binding,
            passthrough: clause
                .model_invocations
                .iter()
                .map(|m| m.output_column.clone())
                .collect(),
        };
        // A projected expression (YIELD, a FOLD input) also reads ALONG names,
        // which shadow the body's and are inlined after resolution. A FOLD
        // output is never resolved: a YIELD over one is produced after the
        // fold, and a FOLD input reads the body's name (`FOLD cost =
        // MSUM(cost * e.q)` reads the referenced `cost`).
        let projection_scope = ClauseScope {
            plan: binding,
            passthrough: scope
                .passthrough
                .iter()
                .cloned()
                .chain(clause.along.iter().map(|a| a.name.clone()))
                .collect(),
        };

        // Step 1: the WHERE conjuncts that read MATCH variables only reach the
        // pattern.
        //
        // This used to run *after* the pattern was planned, which forced the
        // predicate into a hand-built `Filter` above it. A `Filter` node is
        // lowered straight to `FilterExec`, and index selection reads only
        // `Scan.filter`, so a rule-body WHERE consulted no index and filtered a
        // fully materialised scan — the same predicate written inline as
        // `(e:Entity {blocked: true})` was measured at a fifth of the cost
        // (#226). Hoisting the partition lets the predicate be planned *with*
        // the pattern instead. Every other conjunct is a `Filter` step of the
        // plan, placed after the step that produces its last name.
        let immediate_filter_exprs: Vec<&Expr> = binding
            .immediate_filters
            .iter()
            .map(|&index| filter_condition(clause, index))
            .collect::<Result<_>>()?;

        // Step 2: MATCH pattern → Scan→Traverse chain.
        //
        // The immediate conjuncts are handed in as anchoring hints, so an
        // equality on the pattern's *far* node can seed the scan rather than
        // being re-applied above the traversal.
        let where_anchored = if immediate_filter_exprs.is_empty() {
            HashMap::new()
        } else {
            QueryPlanner::equality_anchored_properties(&combine_with_and(&immediate_filter_exprs))
        };
        // Each positive reference's scan columns, by occurrence, for its
        // referenced-fact discriminator (`DerivKind::Ref`).
        let mut ref_scan_columns: HashMap<usize, Vec<String>> = HashMap::new();
        let positive_occurrence: HashMap<usize, usize> = binding
            .refs
            .iter()
            .enumerate()
            .filter(|(_, r)| !r.negated)
            .enumerate()
            .map(|(occurrence, (r, _))| (r, occurrence))
            .collect();
        // The derived scan a positive reference joins, under its prefix — or,
        // for a lower-stratum anchor's seed, under `seed_prefix`, so the seed's
        // columns never collide with the reference's own join.
        // P5 (R3): a clause joining two or more references into its own
        // stratum gets one handle per reference, so the fixpoint can feed each
        // position its own view (delta at one, old or full facts at the
        // others) rather than the whole relation everywhere.
        let exclusive_scans = binding
            .refs
            .iter()
            .filter(|r| !r.negated && stratum_rule_names.contains(r.rule.as_str()))
            .count()
            >= 2;
        let mut exclusive_handles: HashMap<usize, DerivedScanHandle> = HashMap::new();
        let mut stratum_scans: Vec<usize> = Vec::new();
        let mut derived_scan = |r: usize, seed_prefix: Option<&str>| -> Result<LogicalPlan> {
            let ref_binding = &binding.refs[r];
            let target_rule_name = ref_binding.rule.as_str();
            let target_rule = rule_catalog
                .get(target_rule_name)
                .with_context(|| format!("IS-reference to unknown rule '{}'", target_rule_name))?;
            let is_self_ref = stratum_rule_names.contains(target_rule_name);
            // A same-stratum handle is fed live pre-fold facts, so it
            // must declare the TARGET rule's discriminators — which in a
            // mutually-recursive stratum need not match this rule's.
            let target_deriv = derivation_discriminator_columns(
                target_rule,
                ctx.is_recursive,
                stratum_rule_names,
                rule_catalog,
            );
            // Issue #162: a positive same-stratum reference to a rule
            // that carries FOLD reads the folded view, so this clause
            // folds one value per child rather than one per child
            // *derivation*.
            //
            // ALONG opts the whole clause out. `prev.x` reads a column
            // of this same scan, so there is no way to give the fold a
            // folded value and `prev` a per-path one through one handle
            // — and a path accumulator that read a KEY's aggregate
            // would stop being a path accumulator. An ALONG clause
            // therefore keeps contributions; a sibling clause of the
            // same rule that folds an inherited value still gets the
            // folded view, which is why the two views are keyed
            // separately.
            let target_has_fold = target_rule.clauses.iter().any(|c| !c.fold.is_empty());
            let view = if is_self_ref && target_has_fold && clause.along.is_empty() {
                DerivedScanView::Folded
            } else {
                DerivedScanView::Contributions
            };
            let mut handle = self.get_or_create_derived_scan_handle_with_deriv(
                target_rule_name,
                target_rule,
                is_self_ref,
                rule_catalog,
                &target_deriv,
                view,
                false,
            );
            if is_self_ref && exclusive_scans {
                // The shared handle above stays the one negation and
                // provenance resolve the reference to.
                handle = exclusive_handles
                    .entry(r)
                    .or_insert_with(|| {
                        self.get_or_create_derived_scan_handle_with_deriv(
                            target_rule_name,
                            target_rule,
                            is_self_ref,
                            rule_catalog,
                            &target_deriv,
                            view,
                            true,
                        )
                    })
                    .clone();
            }
            if is_self_ref && !stratum_scans.contains(&handle.scan_index) {
                stratum_scans.push(handle.scan_index);
            }
            let scan_schema =
                alias_derived_schema(&handle.schema, seed_prefix.unwrap_or(&ref_binding.prefix));
            if seed_prefix.is_none()
                && let Some(&occurrence) = positive_occurrence.get(&r)
            {
                ref_scan_columns.insert(
                    occurrence,
                    scan_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect(),
                );
            }
            Ok(LogicalPlan::LocyDerivedScan {
                scan_index: handle.scan_index,
                data: handle.data.clone(),
                schema: scan_schema,
            })
        };

        // Issue #301: an anchored clause reads its anchoring reference first
        // and walks the pattern from the nodes it names — from whichever end
        // of the path the node is on — so a recursive clause costs the
        // referenced delta rather than the whole relation.
        //
        // A recursive anchor seeds the walk with the reference's own rows: the
        // anchoring position's test is the seed join, and the reference's
        // other positions are applied at its step below. A lower-stratum
        // anchor (P4 / A2) seeds it with the distinct anchored vids only, and
        // the reference is joined in full at its step, every test included:
        // the seed is a semi-join, so a lower stratum's facts never repeat a
        // walk for two rows sharing the anchored node, and the binding is the
        // unanchored plan's exactly.
        let mut pattern_vars: Vec<VariableInfo> = Vec::new();
        let mut plan = match binding.anchor {
            Some(anchor) => {
                let position = &binding.refs[anchor.r].positions[anchor.pos];
                let (source, vid_column) = match anchor.source {
                    AnchorSource::Recursive => {
                        (derived_scan(anchor.r, None)?, position.physical.clone())
                    }
                    AnchorSource::LowerStratum => {
                        let seed_prefix = "__anchor_";
                        let seed = format!("{seed_prefix}{}", position.column);
                        let source = LogicalPlan::Distinct {
                            input: Box::new(LogicalPlan::Project {
                                input: Box::new(derived_scan(anchor.r, Some(seed_prefix))?),
                                projections: vec![(
                                    Expr::Variable(seed.clone()),
                                    Some(seed.clone()),
                                )],
                            }),
                        };
                        (source, seed)
                    }
                };
                self.planner
                    .plan_pattern_from_anchor(
                        &clause.match_pattern,
                        &position.variable,
                        source,
                        &vid_column,
                        &mut pattern_vars,
                        &where_anchored,
                    )
                    .context("planning MATCH pattern from its anchoring IS reference")?
            }
            None => self
                .planner
                .plan_pattern_scoped(
                    &clause.match_pattern,
                    &[],
                    &mut pattern_vars,
                    &where_anchored,
                )
                .context("planning MATCH pattern")?,
        };

        // Step 2b: push what the pattern's own scope can account for.
        //
        // `plan_where_clause` validates every variable against that scope and
        // rejects anything it does not know, so a Locy-only binding — a
        // generator output, an ALONG variable, a column another clause
        // introduces — must not be handed to it. Partitioning on the scope is
        // deliberate rather than catching the error afterwards: a fallback
        // triggered by a failure cannot tell "this predicate belongs above the
        // pattern" from "pushing it broke", and would quietly keep working
        // while the optimisation stopped happening.
        let (pushable, unpushable): (Vec<&Expr>, Vec<&Expr>) = immediate_filter_exprs
            .iter()
            .partition(|e| expr_vars_all_in_scope(e, &pattern_vars));

        if !pushable.is_empty() {
            let predicate = combine_with_and(&pushable);
            plan = self
                .planner
                .plan_where_clause(&predicate, plan, &pattern_vars, WhereScope::Mandatory)
                .context("planning rule-body WHERE")?;
        }
        if !unpushable.is_empty() {
            let predicate = combine_with_and(&unpushable);
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
                optional_variables: HashSet::new(),
            };
        }

        // Step 3: the body's IS references, generators and remaining filters,
        // in the binding plan's order.
        //
        // Every positive reference joins its derived scan under its own
        // prefix (`__isref{k}_`), the first included, so no referenced column
        // enters the batch under a bare name another name could capture
        // (issue #300). A reference's tests — positions whose variable is
        // bound elsewhere — form one conjunction directly on its CrossJoin,
        // which the physical planner lowers to a multi-key hash join.
        for step in &binding.steps {
            match *step {
                BindingStep::Ref(r) => {
                    let ref_binding = &binding.refs[r];
                    // A recursive anchoring reference was joined below the
                    // pattern; a lower-stratum one only seeded it.
                    let anchor_pos = binding
                        .anchor
                        .filter(|a| a.r == r && a.source == AnchorSource::Recursive)
                        .map(|a| a.pos);
                    if anchor_pos.is_none() {
                        plan = LogicalPlan::CrossJoin {
                            left: Box::new(plan),
                            right: Box::new(derived_scan(r, None)?),
                        };
                    }

                    // Every test of this reference, in one Filter directly on
                    // its CrossJoin: `o._vid = __isref1_o AND ...`. For the
                    // anchoring reference, every test but the anchoring one,
                    // which the seeded walk already enforced.
                    let tests = ref_binding
                        .positions
                        .iter()
                        .enumerate()
                        .filter(|&(pos, p)| {
                            Some(pos) != anchor_pos
                                && matches!(
                                    p.action,
                                    PositionAction::TestNode | PositionAction::TestScalar
                                )
                        })
                        .map(|(_, p)| p)
                        .map(|p| Ok(equals(scope.identity(&p.variable)?, &p.physical)))
                        .collect::<Result<Vec<Expr>>>()?;
                    if !tests.is_empty() {
                        plan = LogicalPlan::Filter {
                            input: Box::new(plan),
                            predicate: combine_with_and(&tests.iter().collect::<Vec<_>>()),
                            optional_variables: HashSet::new(),
                        };
                    }

                    // A node the reference binds is materialized as a proper
                    // node (with `._vid`, `._labels` and property columns), so
                    // property access on it works: a `ScanAll` joined on its
                    // vid. The binding plan binds each variable once, so a node
                    // is never scanned twice — a second `ScanAll` cross-joins
                    // the node with itself and inflates any aggregate over the
                    // joined value columns.
                    //
                    // The join key is `Property(var, "_vid")`, not the
                    // flattened `var._vid` column name: that shape is what the
                    // physical planner recognises as a vid lookup
                    // (`VidLookupJoinExec`), so the scan reads the bound nodes
                    // by vid. Written flat, it was a hash join over a full
                    // `ScanAll` on every fixpoint iteration (P4, the #301
                    // leftover).
                    for position in ref_binding
                        .positions
                        .iter()
                        .filter(|p| p.action == PositionAction::BindNode)
                    {
                        plan = LogicalPlan::CrossJoin {
                            left: Box::new(plan),
                            right: Box::new(LogicalPlan::ScanAll {
                                variable: position.variable.clone(),
                                filter: None,
                                optional: false,
                            }),
                        };
                        plan = LogicalPlan::Filter {
                            input: Box::new(plan),
                            predicate: equals(
                                Expr::Property(
                                    Box::new(Expr::Variable(position.variable.clone())),
                                    "_vid".to_string(),
                                ),
                                &position.physical,
                            ),
                            optional_variables: HashSet::new(),
                        };
                    }
                }
                BindingStep::Generator(cond) => {
                    let RuleCondition::Generator(generator) = &clause.where_conditions[cond] else {
                        bail!("internal error: binding plan step {cond} is not a generator");
                    };
                    let generator_binding = binding
                        .generators
                        .iter()
                        .find(|g| g.condition_index == cond)
                        .with_context(|| {
                            format!("internal error: binding plan has no generator {cond}")
                        })?;
                    plan = LogicalPlan::LocyGenerate {
                        input: Box::new(plan),
                        name: generator.name.to_string(),
                        args: generator
                            .args
                            .iter()
                            .map(|arg| scope.resolve(arg.clone()))
                            .collect::<Result<_>>()?,
                        outputs: generator_binding
                            .outputs
                            .iter()
                            .map(|o| o.physical.clone())
                            .collect(),
                    };
                    // An output another condition binds is renamed and tested.
                    let tests = generator_binding
                        .outputs
                        .iter()
                        .filter(|o| !o.binds)
                        .map(|o| Ok(equals(scope.identity(&o.variable)?, &o.physical)))
                        .collect::<Result<Vec<Expr>>>()?;
                    if !tests.is_empty() {
                        plan = LogicalPlan::Filter {
                            input: Box::new(plan),
                            predicate: combine_with_and(&tests.iter().collect::<Vec<_>>()),
                            optional_variables: HashSet::new(),
                        };
                    }
                }
                BindingStep::Filter(cond) => {
                    let predicate = scope.resolve(filter_condition(clause, cond)?.clone())?;
                    plan = LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate,
                        optional_variables: HashSet::new(),
                    };
                }
                // Negations add no plan node: the fixpoint anti-joins the
                // projected body against the negated rule's facts, which it
                // reads through the rule's derived-scan handle.
                BindingStep::AntiJoin(r) => {
                    let target_rule_name = binding.refs[r].rule.as_str();
                    let target_rule = rule_catalog.get(target_rule_name).with_context(|| {
                        format!("IS-reference to unknown rule '{}'", target_rule_name)
                    })?;
                    let target_deriv = derivation_discriminator_columns(
                        target_rule,
                        ctx.is_recursive,
                        stratum_rule_names,
                        rule_catalog,
                    );
                    self.get_or_create_derived_scan_handle_with_deriv(
                        target_rule_name,
                        target_rule,
                        stratum_rule_names.contains(target_rule_name),
                        rule_catalog,
                        &target_deriv,
                        DerivedScanView::Contributions,
                        false,
                    );
                }
            }
        }

        // The IS-reference metadata, in textual order, with each negation's
        // key: its KEY positions that some literal binds, read from a hidden
        // column carrying the binder's identity through `LocyProject`. The
        // anti-join runs after the projection, so a subject read by whatever
        // YIELD called it was unresolvable when YIELD renamed or dropped it,
        // and the negation silently became a no-op (issue #158).
        let mut is_refs = Vec::with_capacity(binding.refs.len());
        let mut negation_projections: Vec<(Expr, String)> = Vec::new();
        let mut negated_index: usize = 0;
        for ref_binding in &binding.refs {
            let RuleCondition::IsReference(is_ref) =
                &clause.where_conditions[ref_binding.condition_index]
            else {
                bail!(
                    "internal error: binding plan reference {} is not an IS reference",
                    ref_binding.condition_index
                );
            };
            let (target_has_prob, target_prob_col) = rule_catalog
                .get(&ref_binding.rule)
                .and_then(|r| {
                    r.yield_schema
                        .iter()
                        .find(|c| c.is_prob)
                        .map(|c| (true, Some(c.name.clone())))
                })
                .unwrap_or((false, None));
            let mut negation_keys = Vec::new();
            if ref_binding.negated {
                let k = negated_index;
                negated_index += 1;
                for position in ref_binding.positions.iter().filter(|p| p.is_key) {
                    if position.action == PositionAction::Existential {
                        bail!(
                            "IS NOT reference to '{}' binds KEY column `{}` to `{}`, which no \
                             MATCH pattern, positive IS reference or generator of the clause binds",
                            ref_binding.rule,
                            position.column,
                            position.variable,
                        );
                    }
                    let hidden = format!("{ISNOT_VID_COL_PREFIX}{k}_{}", position.variable);
                    if !negation_projections.iter().any(|(_, h)| *h == hidden) {
                        negation_projections
                            .push((scope.identity(&position.variable)?, hidden.clone()));
                    }
                    negation_keys.push(NegationKey {
                        variable: position.variable.clone(),
                        body_column: hidden,
                        fact_column: position.column.clone(),
                        kind: position.kind,
                    });
                }
            }
            is_refs.push(LocyIsRef {
                rule_name: ref_binding.rule.clone(),
                subjects: is_ref
                    .subjects
                    .iter()
                    .map(|s| Expr::Variable(s.clone()))
                    .collect(),
                target: is_ref.target.as_ref().map(|t| Expr::Variable(t.clone())),
                negated: is_ref.negated,
                target_has_prob,
                target_prob_col,
                negation_keys,
                binding_columns: ref_binding
                    .positions
                    .iter()
                    .map(|p| p.column.clone())
                    .collect(),
                prob_ref: None,
                negated_lineage: None,
                support_ref: None,
            });
        }
        // Filled in once the projection is built (the classification reads
        // the projected PROB expression), keyed by index into `binding.refs`
        // — the order of `is_refs`.
        let mut prob_refs_by_ref: HashMap<usize, PositiveProbRef> = HashMap::new();
        // The same for each negated reference's lineage column.
        let mut negated_lineage_by_ref: HashMap<usize, NegatedLineageRef> = HashMap::new();

        let along_bindings: Vec<String> = clause.along.iter().map(|a| a.name.clone()).collect();

        // ALONG's `prev` reads the one reference into the clause's own
        // recursive group, under that reference's prefix.
        let prev_ref = binding.prev_ref.map(|r| &binding.refs[r]);
        if !clause.along.is_empty() {
            let prev_fields: Option<(&str, HashSet<&str>)> = prev_ref.map(|r| {
                let fields = rule_catalog
                    .get(&r.rule)
                    .map(|rule| {
                        rule.yield_schema
                            .iter()
                            .map(|yc| yc.name.as_str())
                            .collect()
                    })
                    .unwrap_or_default();
                (r.rule.as_str(), fields)
            });
            for along in &clause.along {
                validate_prev_refs(&along.expr, prev_fields.as_ref())?;
            }
        }
        let prev_prefix = prev_ref.map(|r| r.prefix.as_str());

        // Steps 4+5+6: ALONG + YIELD + PRIORITY as a single projection
        let along_map: HashMap<&str, &LocyExpr> = clause
            .along
            .iter()
            .map(|a| (a.name.as_str(), &a.expr))
            .collect();

        // Build FOLD output name → aggregate input expression mapping.
        // FOLD outputs (e.g., `total` from `FOLD total = SUM(r.amount)`) are not
        // columns in the MATCH output; the clause projection must include the
        // aggregate's input expression (e.g., `r.amount`) so the FOLD operator
        // can find it later.
        let fold_output_names: HashSet<&str> =
            clause.fold.iter().map(|fb| fb.name.as_str()).collect();
        let fold_input_map: HashMap<&str, &Expr> = clause
            .fold
            .iter()
            .filter_map(|fb| {
                // Extract the first argument from the aggregate function call
                if let Expr::FunctionCall { args, .. } = &fb.aggregate {
                    args.first().map(|arg| (fb.name.as_str(), arg))
                } else {
                    None
                }
            })
            .collect();

        // Build yield name → original expression mapping from clause output.
        // This preserves literals (`'low' AS label`) and property accesses
        // (`e.cost AS cost`) that would otherwise be lost as bare Variable lookups.
        let yield_expr_map: HashMap<String, &Expr> = match &clause.output {
            RuleOutput::Yield(yc) => resolve_yield_column_names(&yc.items)
                .into_iter()
                .zip(yc.items.iter())
                .map(|(name, item)| (name, &item.expr))
                .collect(),
            _ => HashMap::new(),
        };

        let along_types = along_column_types(std::slice::from_ref(clause), self.planner.schema());

        // Pre-compute rewritten ALONG expressions for variable substitution.
        // When a YIELD expression references an ALONG name (e.g., `ew * 2.0 AS score`
        // where `ALONG ew = e.weight`), the Variable("ew") must be replaced with the
        // underlying expression Property("e", "weight") because "ew" is not a column
        // in the input plan schema.
        let rewritten_along: HashMap<&str, Expr> = along_map
            .iter()
            .map(|(&name, locy_expr)| {
                Ok((name, rewrite_locy_expr(locy_expr, prev_prefix, &scope)?))
            })
            .collect::<Result<_>>()?;
        // A projected expression: its names resolved, ALONG names inlined, and
        // a bare node or relationship read through its identity column.
        let project = |expr: Expr| -> Result<Expr> {
            let resolved = substitute_along_vars(projection_scope.resolve(expr)?, &rewritten_along);
            Ok(projection_scope.top_level(resolved))
        };

        let mut projections = Vec::new();
        let mut target_types = Vec::new();
        let var_labels = clause_var_labels(clause);
        let graph_schema = self.planner.schema();
        // A clause that seeds a sibling FOLD lays its columns out as the FOLD
        // clause does: the other YIELD columns in order, then each seeded
        // column under its FOLD variable, in FOLD order (`rule_fold_layout`).
        let seeded_name = |yc: &YieldColumn| -> Option<&String> {
            clause
                .fold
                .is_empty()
                .then(|| {
                    ctx.fold_layout
                        .iter()
                        .find(|(_, y)| *y == yc.name)
                        .map(|(f, _)| f)
                })
                .flatten()
        };
        let ordered_cols: Vec<(&YieldColumn, &String)> = yield_cols
            .iter()
            .filter(|yc| seeded_name(yc).is_none())
            .map(|yc| (yc, &yc.name))
            .chain(ctx.fold_layout.iter().filter_map(|(fold_var, yield_name)| {
                let yc = yield_cols.iter().find(|yc| &yc.name == yield_name)?;
                seeded_name(yc).map(|_| (yc, fold_var))
            }))
            .collect();
        for (yc, column_name) in ordered_cols {
            let expr = if let Some(along_expr) = rewritten_along.get(yc.name.as_str()) {
                scope.top_level(along_expr.clone())
            } else if fold_output_names.contains(yc.name.as_str()) {
                // FOLD output column — produced by FoldExec after aggregation.
                // Its input column is projected separately (see the FOLD-input
                // pass below) AS the FOLD variable name, independent of any
                // YIELD alias, so the runtime always resolves it by name.
                continue;
            } else if let Some(orig_expr) = yield_expr_map.get(&yc.name) {
                // A YIELD expression that references a FOLD output must NOT be
                // projected in the pre-fold body: a bare fold-output variable
                // (`n AS support`) is produced directly by FoldExec, and a
                // computed expression over fold outputs (`total * 2.0 AS score`)
                // is produced by the post-fold projection stage. Either way the
                // aggregate does not exist here yet.
                let inlined = substitute_along_vars((*orig_expr).clone(), &rewritten_along);
                if expr_references_fold_output(&inlined, &fold_output_names) {
                    continue;
                }
                project((*orig_expr).clone())?
            } else {
                project(Expr::Variable(yc.name.clone()))?
            };
            projections.push((expr, Some(column_name.clone())));
            // A FOLD clause never reaches here with a FOLD output, and
            // `seed_types` holds a KEY column only when the clauses type it
            // differently, so the rule-wide type applies to every clause.
            let unified = ctx.seed_types.get(&yc.name).cloned();
            target_types.push(unified.unwrap_or_else(|| {
                infer_yield_type(
                    &yc.name,
                    clause,
                    node_vars,
                    &fold_output_names,
                    &along_types,
                    rule_catalog,
                    yc.is_key,
                    graph_schema,
                    &var_labels,
                )
            }));
        }

        // FOLD-input projection (issue #145 root fix).
        //
        // Every aggregate's argument column must be present in the body batch
        // under the FOLD variable name (`fb.name`) — that is exactly the name
        // `convert_fold_bindings` records in `FoldBinding::input_col_name` and
        // the runtime (`FoldExec`, `FixpointExec`) resolves by. Projecting the
        // input here, keyed on the FOLD variable rather than the YIELD column
        // name, makes resolution independent of whether YIELD renames the
        // aggregate (`FOLD total = SUM(e.x) ... YIELD ... total AS sum_out`),
        // which is the root cause of the silent-zeroing corruption in #145.
        // The aggregated *output* column (named by the YIELD alias) is produced
        // later by FoldExec; it must not be projected here.
        for fb in &clause.fold {
            let Some(fold_input) = fold_input_map.get(fb.name.as_str()) else {
                // COUNTALL / COUNT(*) has no input column to carry through.
                continue;
            };
            let expr = project((*fold_input).clone())?;
            let expr = if is_count_fold(fb) {
                count_input_marker(expr, clause, ctx.clause_index)
            } else {
                expr
            };
            projections.push((expr, Some(fb.name.clone())));
            // `COLLECT` keeps its elements' values: its input is carried as a
            // Cypher value, which every column type encodes to (its output is
            // a list, typed separately). Passed through in its column type, a
            // declared `Int32`, `Date` or `DateTime` column was refused.
            let collect_input = (fold_function_name(&fb.aggregate).as_deref() == Some("COLLECT"))
                .then_some(DataType::LargeBinary);
            let unified = || ctx.seed_types.get(&fb.name).cloned();
            target_types.push(collect_input.or_else(unified).unwrap_or_else(|| {
                infer_yield_type(
                    &fb.name,
                    clause,
                    node_vars,
                    &fold_output_names,
                    &along_types,
                    rule_catalog,
                    false, // a FOLD input is never a KEY column
                    graph_schema,
                    &var_labels,
                )
            }));
        }

        // Add __priority literal column if present
        if let Some(priority) = clause.priority {
            projections.push((
                Expr::Literal(CypherLiteral::Integer(priority)),
                Some("__priority".to_string()),
            ));
            target_types.push(DataType::Int64);
        }

        // Derivation-discriminator columns for a recursive FOLD / ALONG rule
        // (issue #159). See `FOLD_DISCRIMINATOR_COL_PREFIX`.
        //
        // These are pushed HERE — before the untyped `__feat_*` /
        // `__isnot_vid_*` tails below — because `target_types` is index-parallel
        // with `projections` and those tails deliberately push no `target_types`
        // entry. Appending after them would desynchronize the two vectors.
        //
        // Every clause emits the same set, so the clauses stay union-compatible:
        // a clause that binds the variable projects its `_vid`, one that does
        // not projects a typed NULL.
        if !ctx.deriv_vars.is_empty() {
            // The clause index, so a NULL-padded row from one clause can never
            // collide with a real row from another.
            projections.push((
                Expr::Literal(CypherLiteral::Integer(ctx.clause_index as i64)),
                Some(format!("{FOLD_DISCRIMINATOR_COL_PREFIX}clause")),
            ));
            target_types.push(DataType::Int64);

            let mut bound_here = HashSet::new();
            collect_match_node_vars(clause, &mut bound_here);
            bound_here.extend(
                match_edge_vars(clause, ctx.clause_index)
                    .into_iter()
                    .map(|d| d.name),
            );
            for var in ctx.deriv_vars {
                // `Expr::Variable("a._vid")` is a raw dotted column reference,
                // resolved against the input schema by full name — the same
                // mechanism the issue #158 columns use.
                //
                // The explicit target type is load-bearing for the NULL arm:
                // `CypherLiteral::Null` compiles to `DataType::Null`, which is
                // neither string nor numeric, so it misses `plan_locy_project`'s
                // cross-domain guard and reaches the catch-all cast. Inferring
                // the type instead would make it `LargeUtf8`, which IS
                // cross-domain against `UInt64` — the cast would be skipped and
                // the clauses would silently diverge in schema. For the bound
                // arm the cast is a no-op.
                let expr = match var.kind {
                    DerivKind::Ref {
                        clause_index,
                        occurrence,
                    } => match ref_scan_columns.get(&occurrence) {
                        Some(columns) if clause_index == ctx.clause_index => Expr::FunctionCall {
                            name: crate::query::df_graph::locy_row_hash::ROW_HASH_FN.to_string(),
                            args: columns.iter().cloned().map(Expr::Variable).collect(),
                            distinct: false,
                            window_spec: None,
                        },
                        _ => Expr::Literal(CypherLiteral::Null),
                    },
                    _ if bound_here.contains(&var.name) => Expr::Variable(var.id_column()),
                    _ => Expr::Literal(CypherLiteral::Null),
                };
                projections.push((expr, Some(var.column())));
                target_types.push(var.kind.data_type());
            }
        }

        // Positive references to PROB rules (see `ProbRead`): each carries the
        // referenced fact's probability, and — for an `MNOR`/`MPROD` rule,
        // which keeps lineage — the hash of the referenced fact's KEY, into
        // the projected body. A lineage rule projects every slot of every
        // clause (typed NULL outside the slot's own clause) so the clauses stay
        // union-compatible; these follow the discriminators, matching
        // `derivation_discriminator_columns`.
        let positive_refs_in_text_order: Vec<usize> = {
            let mut refs: Vec<usize> = (0..binding.refs.len())
                .filter(|&r| !binding.refs[r].negated)
                .collect();
            refs.sort_by_key(|&r| binding.refs[r].condition_index);
            refs
        };
        let negated_refs_in_text_order: Vec<usize> = {
            let mut refs: Vec<usize> = (0..binding.refs.len())
                .filter(|&r| binding.refs[r].negated)
                .collect();
            refs.sort_by_key(|&r| binding.refs[r].condition_index);
            refs
        };
        let slot_ref = |slot: &ProbRefSlot| -> Option<usize> {
            let refs = if slot.negated {
                &negated_refs_in_text_order
            } else {
                &positive_refs_in_text_order
            };
            (slot.clause_index == ctx.clause_index)
                .then(|| refs.get(slot.occurrence).copied())
                .flatten()
        };
        let ref_prob_columns = |r: usize| -> Result<(String, Vec<String>)> {
            let ref_binding = &binding.refs[r];
            let target = rule_catalog
                .get(&ref_binding.rule)
                .with_context(|| format!("IS-reference to unknown rule '{}'", ref_binding.rule))?;
            let prob = target
                .yield_schema
                .iter()
                .find(|c| c.is_prob)
                .with_context(|| {
                    format!(
                        "internal error: IS-reference to '{}' has no PROB column",
                        ref_binding.rule
                    )
                })?;
            let keys = target
                .yield_schema
                .iter()
                .filter(|c| c.is_key)
                .map(|c| format!("{}{}", ref_binding.prefix, c.name))
                .collect();
            Ok((format!("{}{}", ref_binding.prefix, prob.name), keys))
        };
        let mut factored_prob_names: HashSet<String> = HashSet::new();
        let mut opaque_prob_read = false;
        // How this clause's PROB expression reads each of its references'
        // probabilities, from the expression as projected.
        for slot in ctx.prob_slots.iter().filter(|s| !s.negated) {
            let Some(r) = slot_ref(slot) else {
                continue;
            };
            let ref_binding = &binding.refs[r];
            let (prob_physical, _) = ref_prob_columns(r)?;
            let target_prob = prob_physical
                .strip_prefix(ref_binding.prefix.as_str())
                .unwrap_or(&prob_physical)
                .to_string();
            let mut names: HashSet<String> = HashSet::from([prob_physical]);
            for position in ref_binding
                .positions
                .iter()
                .filter(|p| p.column == target_prob)
            {
                names.insert(position.physical.clone());
                if let Ok(Expr::Variable(column)) = scope.identity(&position.variable) {
                    names.insert(column);
                }
            }
            let read = projections
                .iter()
                .find(|(_, alias)| alias.as_deref() == Some(slot.into_column.as_str()))
                .map_or(ProbRead::Absent, |(expr, _)| {
                    classify_prob_read(expr, &names)
                });
            match read {
                ProbRead::Factor => factored_prob_names.extend(names),
                ProbRead::Opaque => opaque_prob_read = true,
                ProbRead::Absent => {}
            }
            prob_refs_by_ref.insert(
                r,
                PositiveProbRef {
                    prob_column: slot.prob_column(),
                    key_column: slot.keeps_lineage().then(|| slot.key_column()),
                    into_column: slot.into_column.clone(),
                    read,
                },
            );
        }

        // The row event of a lineage rule (see `DERIV_EVENT_COL`): every
        // clause projects it, NULL when the row is its own event.
        if let Some(prob_column) = ctx.lineage_prob_column {
            let event = projections
                .iter()
                .find(|(_, alias)| alias.as_deref() == Some(prob_column))
                .filter(|_| !opaque_prob_read)
                .and_then(|(expr, _)| {
                    row_event_expr(expr, &factored_prob_names, &|name| match projection_scope
                        .source(name)
                    {
                        Some(NameSource::Entity(column)) => Some(column),
                        _ => None,
                    })
                })
                .unwrap_or(Expr::Literal(CypherLiteral::Null));
            let event_column = if ctx.transient_lineage {
                LIN_EVENT_COL
            } else {
                DERIV_EVENT_COL
            };
            projections.push((event, Some(event_column.to_string())));
            target_types.push(crate::query::df_graph::locy_row_hash::row_hash_type());
        }
        let mut transient_prob_projections: Vec<(Expr, String)> = Vec::new();
        for slot in ctx.prob_slots {
            let own = slot_ref(slot);
            if slot.negated {
                // Filled by the complement, which alone resolves the negated
                // facts (`locy_complement::apply_negated_is_refs`).
                if let Some(r) = own {
                    let target = rule_catalog.get(&binding.refs[r].rule).with_context(|| {
                        format!("IS-reference to unknown rule '{}'", binding.refs[r].rule)
                    })?;
                    negated_lineage_by_ref.insert(
                        r,
                        NegatedLineageRef {
                            column: slot.key_column(),
                            target_key_columns: target
                                .yield_schema
                                .iter()
                                .filter(|c| c.is_key)
                                .map(|c| c.name.clone())
                                .collect(),
                        },
                    );
                }
                projections.push((Expr::Literal(CypherLiteral::Null), Some(slot.key_column())));
                target_types.push(DataType::Binary);
                continue;
            }
            if slot.keeps_lineage() {
                let (key_expr, prob_expr) = match own {
                    Some(r) => {
                        let (prob, keys) = ref_prob_columns(r)?;
                        (
                            Expr::FunctionCall {
                                name: crate::query::df_graph::locy_row_hash::ROW_HASH_FN
                                    .to_string(),
                                args: keys.into_iter().map(Expr::Variable).collect(),
                                distinct: false,
                                window_spec: None,
                            },
                            Expr::Variable(prob),
                        )
                    }
                    None => (
                        Expr::Literal(CypherLiteral::Null),
                        Expr::Literal(CypherLiteral::Null),
                    ),
                };
                projections.push((key_expr, Some(slot.key_column())));
                target_types.push(crate::query::df_graph::locy_row_hash::row_hash_type());
                projections.push((prob_expr, Some(slot.prob_column())));
                target_types.push(DataType::Float64);
            } else if let Some(r) = own {
                let (prob, _) = ref_prob_columns(r)?;
                transient_prob_projections.push((Expr::Variable(prob), slot.prob_column()));
            }
        }
        // The provenance-only columns of the references no slot above names
        // a fact for (see `SupportSlot`), typed NULL outside their own clause
        // so the clauses stay union-compatible.
        let mut support_refs_by_ref: HashMap<usize, SupportRef> = HashMap::new();
        for slot in ctx.support_slots {
            let own = (slot.clause_index == ctx.clause_index)
                .then(|| positive_refs_in_text_order.get(slot.occurrence).copied())
                .flatten();
            let (key_expr, prob_expr) = match own {
                Some(r) => {
                    let ref_binding = &binding.refs[r];
                    let target = rule_catalog.get(&ref_binding.rule).with_context(|| {
                        format!("IS-reference to unknown rule '{}'", ref_binding.rule)
                    })?;
                    let keys: Vec<Expr> = target
                        .yield_schema
                        .iter()
                        .filter(|c| c.is_key)
                        .map(|c| Expr::Variable(format!("{}{}", ref_binding.prefix, c.name)))
                        .collect();
                    let prob = target
                        .yield_schema
                        .iter()
                        .find(|c| c.is_prob)
                        .map(|c| Expr::Variable(format!("{}{}", ref_binding.prefix, c.name)));
                    support_refs_by_ref.insert(
                        r,
                        SupportRef {
                            key_column: slot.key_column(),
                            prob_column: slot.target_has_prob.then(|| slot.prob_column()),
                        },
                    );
                    (
                        Expr::FunctionCall {
                            name: crate::query::df_graph::locy_row_hash::ROW_HASH_FN.to_string(),
                            args: keys,
                            distinct: false,
                            window_spec: None,
                        },
                        prob.unwrap_or(Expr::Literal(CypherLiteral::Null)),
                    )
                }
                None => (
                    Expr::Literal(CypherLiteral::Null),
                    Expr::Literal(CypherLiteral::Null),
                ),
            };
            projections.push((key_expr, Some(slot.key_column())));
            target_types.push(crate::query::df_graph::locy_row_hash::row_hash_type());
            if slot.target_has_prob {
                projections.push((prob_expr, Some(slot.prob_column())));
                target_types.push(DataType::Float64);
            }
        }
        // Hidden YIELD items emitted by `extract_model_invocations` for
        // property feature exprs (e.g. `scorer(s.tier)`). These columns
        // (named `__feat_<var>_<prop>`) flow through the body batch so
        // `apply_model_invocations` can read property values per row;
        // `record_batches_to_locy_rows` strips them by prefix before
        // returning the user-visible rows.
        //
        // We deliberately push to `projections` AFTER `target_types` is
        // already populated for the user-visible columns + priority.
        // `plan_locy_project` reads `target_types.get(i)` and skips
        // coercion when the entry is absent (returns `None`); leaving
        // these out preserves the property's native storage type
        // (Utf8 / LargeBinary / Int64 / Float64 / etc.) end-to-end
        // into `apply_model_invocations`.
        for hidden in &clause.hidden_yield_cols {
            if let Some(orig_expr) = yield_expr_map.get(hidden) {
                projections.push((project((*orig_expr).clone())?, Some(hidden.clone())));
            }
        }

        // Hidden identity columns for negated IS-ref KEY positions (issue
        // #158). Pushed after the typed columns and with no `target_types`
        // entry, for the same reason as the `__feat_*` columns above:
        // `plan_locy_project` skips the coercion cast when `target_types.get(i)`
        // is `None`, preserving the native type the anti-join compares.
        for (expr, hidden) in negation_projections {
            projections.push((expr, Some(hidden)));
        }
        // The transient probability columns, untyped for the same reason;
        // the fixpoint strips them once multiplied in.
        for (expr, hidden) in transient_prob_projections {
            projections.push((expr, Some(hidden)));
        }

        // BEST BY over names the projection does not keep (a property of a
        // MATCH variable, a referenced value column): each such criterion is
        // projected under a hidden `__bestby_{i}` column, ranked on, and
        // stripped once BEST BY has run. Only for non-FOLD clauses; see Step 8.
        let projected_names: Vec<String> = projections
            .iter()
            .filter_map(|(_, alias)| alias.clone())
            .collect();
        let yield_names: HashSet<&str> = yield_cols.iter().map(|yc| yc.name.as_str()).collect();
        let mut best_by_criteria: Option<Vec<(Expr, bool)>> = None;
        let mut best_by_hidden = false;
        // Not for a clause with an `IS NOT` either: the fixpoint anti-joins
        // (or multiplies in the complement) *after* the body, so ranking here
        // kept the best row per KEY before negation removed it, and the
        // runner-up that survives negation was already gone — a silent wrong
        // answer, under full and semi-naive feeding alike (found by the P5
        // oracle families). The rule-level BEST BY (`merge_best_by` in a
        // recursive stratum, the post-fixpoint chain otherwise) ranks after
        // negation; the body ranking is only an early prune.
        let negates = binding.refs.iter().any(|r| r.negated);
        if let Some(best_by) = clause
            .best_by
            .as_ref()
            .filter(|_| clause.fold.is_empty() && !negates)
        {
            let mut criteria = Vec::with_capacity(best_by.items.len());
            for (i, item) in best_by.items.iter().enumerate() {
                let reads_yield_only = collect_expr_variables(&item.expr)
                    .iter()
                    .all(|v| yield_names.contains(v.as_str()));
                if reads_yield_only {
                    criteria.push((item.expr.clone(), item.ascending));
                    continue;
                }
                // A YIELD alias the criterion names is its YIELD expression.
                let inlined = map_variables(item.expr.clone(), &|name| {
                    if projection_scope.source(name).is_some() || !yield_names.contains(name) {
                        return None;
                    }
                    yield_expr_map.get(name).map(|e| (*e).clone())
                });
                let hidden = format!("__bestby_{i}");
                projections.push((project(inlined)?, Some(hidden.clone())));
                best_by_hidden = true;
                criteria.push((Expr::Variable(hidden), item.ascending));
            }
            best_by_criteria = Some(criteria);
        }

        // Phase B A4 follow-up: when the clause has neural-model
        // invocations (extracted from YIELD / ALONG / FOLD positions
        // and replaced with `Variable("__model_<n>_<idx>")`
        // references), wrap the pre-projection plan with
        // `LocyModelInvoke` so the synthesized columns exist in the
        // input schema of `LocyProject`. Downstream FOLD aggregates
        // also see those columns (FoldExec reads projected columns).
        if !clause.model_invocations.is_empty() {
            // Phase D D3 runtime: for every distinct source rule
            // referenced by a path-context invocation on this clause,
            // mint a `DerivedScanHandle` (non-self-ref → full cross-stratum
            // facts) and surface it as a `PathContextHandle` on the
            // logical plan node. The `data` Arc is shared with
            // `DerivedScanRegistry`, so the fixpoint loop's writes flow
            // through here without further plumbing.
            let mut path_context_handles: HashMap<
                String,
                super::df_graph::locy_model_invoke::PathContextHandle,
            > = HashMap::new();
            for inv in &clause.model_invocations {
                if let Some(pc) = &inv.path_context {
                    if path_context_handles.contains_key(&pc.source_rule) {
                        continue;
                    }
                    let target_rule = rule_catalog.get(&pc.source_rule).ok_or_else(|| {
                        anyhow::anyhow!(
                            "model '{}' path_context references undefined rule '{}'",
                            inv.model_name,
                            pc.source_rule
                        )
                    })?;
                    let handle = self.get_or_create_derived_scan_handle(
                        &pc.source_rule,
                        target_rule,
                        false,
                        rule_catalog,
                    );
                    path_context_handles.insert(
                        pc.source_rule.clone(),
                        super::df_graph::locy_model_invoke::PathContextHandle {
                            source_rule: pc.source_rule.clone(),
                            data: handle.data,
                            schema: handle.schema,
                        },
                    );
                }
            }
            // The feature expressions read the body batch, so their names
            // are resolved to its columns here: the runtime resolvers look
            // each one up by exact name. The plan's copy keeps the written
            // expressions, which EXPLAIN evaluates against fact rows.
            let invocations = clause
                .model_invocations
                .iter()
                .map(|inv| {
                    let mut inv = inv.clone();
                    inv.feature_exprs = inv
                        .feature_exprs
                        .into_iter()
                        .map(|e| scope.resolve_identity(e))
                        .collect::<Result<_>>()?;
                    Ok(inv)
                })
                .collect::<Result<Vec<_>>>()?;
            plan = LogicalPlan::LocyModelInvoke {
                input: Box::new(plan),
                invocations,
                classifier_registry: Arc::clone(&classifier_registry),
                classifier_cache: classifier_cache.as_ref().map(Arc::clone),
                classifier_provenance_store: classifier_provenance_store.as_ref().map(Arc::clone),
                path_context_handles,
            };
        }

        plan = LogicalPlan::LocyProject {
            input: Box::new(plan),
            projections,
            target_types,
        };

        // Step 7: Non-recursive FOLD — handled by apply_post_fixpoint_chain.
        //
        // Fold is always applied post-fixpoint (both recursive and non-recursive).
        // Wrapping the body with LocyFold here would double-apply the aggregate,
        // producing wrong results for COUNT/AVG where f(f(x)) ≠ f(x).

        // Step 8: BEST BY wrapping.
        //
        // Only for non-FOLD clauses. When the clause has a FOLD, BEST BY ranks on
        // the *aggregated* value and must run POST-fold — which the rule-level
        // pipeline already does (`merge_best_by` during fixpoint and the post-
        // fixpoint chain in `locy_fixpoint.rs`, using the alias-substituted
        // criteria). Wrapping the pre-fold body here would instead rank the raw
        // per-row input column, pruning rows before aggregation and corrupting
        // the result (and, for a renamed aggregate, referencing a column that
        // only exists pre-fold). There is no sensible pre-fold reading of
        // "best by an aggregate", so skip it for FOLD clauses.
        if let Some(criteria) = best_by_criteria {
            let key_columns: Vec<String> = yield_cols
                .iter()
                .filter(|yc| yc.is_key)
                .map(|yc| yc.name.clone())
                .collect();

            plan = LogicalPlan::LocyBestBy {
                input: Box::new(plan),
                key_columns,
                criteria,
            };
            if best_by_hidden {
                // Strip the hidden criterion columns: every other column
                // passes through by its exact name.
                plan = LogicalPlan::LocyProject {
                    input: Box::new(plan),
                    projections: projected_names
                        .into_iter()
                        .map(|name| (Expr::Variable(name.clone()), Some(name)))
                        .collect(),
                    target_types: Vec::new(),
                };
            }
        }

        // Structural guard (issue #300): every name the body reads is a column
        // some node below it produced. A bare name the plan does not source
        // fails here, in debug builds and so in CI, instead of reaching
        // DataFusion, which resolves it by column name and captures whatever
        // column happens to carry it.
        #[cfg(debug_assertions)]
        check_sources(
            &pattern_vars.iter().map(|v| v.name.clone()).collect(),
            &plan,
        )?;

        for (index, prob_ref) in prob_refs_by_ref {
            if let Some(is_ref) = is_refs.get_mut(index) {
                is_ref.prob_ref = Some(prob_ref);
            }
        }
        for (index, lineage) in negated_lineage_by_ref {
            if let Some(is_ref) = is_refs.get_mut(index) {
                is_ref.negated_lineage = Some(lineage);
            }
        }
        for (index, support) in support_refs_by_ref {
            if let Some(is_ref) = is_refs.get_mut(index) {
                is_ref.support_ref = Some(support);
            }
        }

        Ok(LocyClausePlan {
            body: plan,
            is_refs,
            along_bindings,
            priority: clause.priority,
            model_invocations: clause.model_invocations.clone(),
            stratum_scans,
        })
    }

    // -- DerivedScanHandle management -----------------------------------

    /// Get or create a derived scan handle for a rule.
    ///
    /// Handles are keyed by `(rule_name, is_self_ref, view)` — a self-referential
    /// scan (receives delta data) and a cross-stratum scan (receives full facts)
    /// for the same rule need separate handles, as do the two
    /// [`DerivedScanView`]s. This convenience wrapper asks for the
    /// contributions view; callers that may need the folded one go through
    /// `get_or_create_derived_scan_handle_with_deriv` directly.
    fn get_or_create_derived_scan_handle(
        &self,
        rule_name: &str,
        target_rule: &CompiledRule,
        is_self_ref: bool,
        rule_catalog: &HashMap<String, CompiledRule>,
    ) -> DerivedScanHandle {
        self.get_or_create_derived_scan_handle_with_deriv(
            rule_name,
            target_rule,
            is_self_ref,
            rule_catalog,
            &[],
            DerivedScanView::Contributions,
            false,
        )
    }

    /// As [`Self::get_or_create_derived_scan_handle`], but widens a **self-ref**
    /// handle's schema with the rule's derivation-discriminator columns.
    ///
    /// A same-stratum (self-ref) handle is fed the `FixpointState`'s live facts,
    /// which during iteration are *pre-fold* and therefore carry the
    /// discriminators (issue #159). A cross-stratum handle reads the published,
    /// already-stripped facts and must stay narrow. `DerivedScanExec` re-stamps
    /// its declared schema onto every batch, so a mismatch here is a hard Arrow
    /// error, not a silent fallback.
    ///
    /// An `exclusive` handle is always minted fresh and never reused: it is
    /// one reference of a non-linear clause, which the fixpoint feeds apart
    /// from every other scan of the same rule (P5, R3).
    #[expect(
        clippy::too_many_arguments,
        reason = "the handle key plus the schema inputs"
    )]
    fn get_or_create_derived_scan_handle_with_deriv(
        &self,
        rule_name: &str,
        target_rule: &CompiledRule,
        is_self_ref: bool,
        rule_catalog: &HashMap<String, CompiledRule>,
        deriv_columns: &[(String, DataType)],
        view: DerivedScanView,
        exclusive: bool,
    ) -> DerivedScanHandle {
        let mut handles = self.derived_scan_handles.borrow_mut();

        // Reuse existing handle with same (rule_name, is_self_ref, view). The
        // view is part of the key because both views of one rule can be live
        // at once — see `DerivedScanView`.
        if !exclusive
            && let Some(handle) = handles.iter().find(|h| {
                !h.exclusive
                    && h.rule_name == rule_name
                    && h.is_self_ref == is_self_ref
                    && h.view == view
            })
        {
            return handle.clone();
        }

        let scan_index = handles.len();
        let base =
            yield_schema_to_arrow_from_rule(target_rule, rule_catalog, self.planner.schema());
        let schema = if is_self_ref && !deriv_columns.is_empty() {
            let mut fields: Vec<Arc<arrow_schema::Field>> = base.fields().iter().cloned().collect();
            for (name, dt) in deriv_columns {
                fields.push(Arc::new(arrow_schema::Field::new(name, dt.clone(), true)));
            }
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            base
        };
        let data = Arc::new(RwLock::new(Vec::new()));
        let handle = DerivedScanHandle {
            rule_name: rule_name.to_string(),
            scan_index,
            is_self_ref,
            view,
            exclusive,
            data,
            schema,
        };
        handles.push(handle.clone());
        handle
    }

    /// Convert internal handles into a [`DerivedScanRegistry`].
    fn build_registry(&self) -> DerivedScanRegistry {
        let handles = self.derived_scan_handles.borrow();
        let mut registry = DerivedScanRegistry::new();
        for handle in handles.iter() {
            registry.add(DerivedScanEntry {
                scan_index: handle.scan_index,
                rule_name: handle.rule_name.clone(),
                is_self_ref: handle.is_self_ref,
                view: handle.view,
                exclusive: handle.exclusive,
                data: handle.data.clone(),
                schema: handle.schema.clone(),
            });
        }
        registry
    }
}

// ---------------------------------------------------------------------------
// Structural guard: every read of a clause body is a produced column
// ---------------------------------------------------------------------------

/// The names a plan node outputs, as [`check_sources`] tracks them.
#[cfg(any(debug_assertions, test))]
#[derive(Default)]
struct Produced {
    /// Entity variables: a MATCH variable or a node a reference binds
    /// (`ScanAll`). Read bare, through a property access, or as a dotted
    /// expansion column (`a._vid`, `a._labels`).
    entities: HashSet<String>,
    /// Plain columns, read by their exact name.
    columns: HashSet<String>,
}

#[cfg(any(debug_assertions, test))]
impl Produced {
    fn reads(&self, name: &str) -> bool {
        self.columns.contains(name)
            || self.entities.contains(name)
            || name
                .split_once('.')
                .is_some_and(|(entity, _)| self.entities.contains(entity))
    }

    fn extend(&mut self, other: Self) {
        self.entities.extend(other.entities);
        self.columns.extend(other.columns);
    }
}

/// The names `expr` reads from its input: variables outside a comprehension's,
/// quantifier's or `reduce`'s own bindings.
#[cfg(any(debug_assertions, test))]
fn expr_reads(expr: &Expr) -> Vec<String> {
    fn walk(expr: &Expr, out: &mut Vec<String>) {
        if let Expr::Variable(v) = expr {
            out.push(v.clone());
        }
        expr.for_each_child_in_scope(&mut |child| walk(child, out));
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// Whether `plan` contains a node the Locy clause builder adds above the
/// MATCH pattern. A subtree without one is the pattern's own plan, which the
/// Cypher planner validated against the pattern's scope.
#[cfg(any(debug_assertions, test))]
fn has_locy_node(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::LocyDerivedScan { .. }
            | LogicalPlan::LocyGenerate { .. }
            | LogicalPlan::LocyModelInvoke { .. }
            | LogicalPlan::LocyProject { .. }
            | LogicalPlan::LocyBestBy { .. }
    ) || plan.children().into_iter().any(has_locy_node)
}

/// Checks that every expression of a clause body reads only names a node
/// below it produced.
///
/// The MATCH pattern's plan produces `pattern`, the variables the Cypher
/// planner recorded for it (the binding plan's MATCH variables, plus the
/// hidden names of anonymous relationships); a derived scan produces its
/// (prefixed) schema; a `ScanAll` the node it binds;
/// a generator its physical outputs; a model invocation its output columns;
/// a projection its aliases. A name that is none of these is one the
/// clause builder emitted without resolving it through [`ClauseScope`]:
/// DataFusion would resolve it by name and capture an unrelated column
/// (issue #300).
///
/// # Errors
///
/// An internal error naming the first unproduced name, or a node kind the
/// clause builder never emits above the pattern.
#[cfg(any(debug_assertions, test))]
fn check_sources(pattern: &HashSet<String>, body: &LogicalPlan) -> Result<()> {
    fn require(node: &str, expr: &Expr, input: &Produced) -> Result<()> {
        if let Some(name) = expr_reads(expr).into_iter().find(|n| !input.reads(n)) {
            bail!(
                "internal error: a Locy clause's {node} reads `{name}`, which no input column \
                 produces; every name must be resolved through the clause's binding plan"
            );
        }
        Ok(())
    }
    fn produced(pattern: &HashSet<String>, plan: &LogicalPlan) -> Result<Produced> {
        if !has_locy_node(plan) && !matches!(plan, LogicalPlan::ScanAll { .. }) {
            return Ok(Produced {
                entities: pattern.clone(),
                columns: HashSet::new(),
            });
        }
        Ok(match plan {
            LogicalPlan::LocyDerivedScan { schema, .. } => Produced {
                entities: HashSet::new(),
                columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
            },
            LogicalPlan::ScanAll { variable, .. } => Produced {
                entities: HashSet::from([variable.clone()]),
                columns: HashSet::new(),
            },
            LogicalPlan::CrossJoin { left, right } => {
                let mut names = produced(pattern, left)?;
                names.extend(produced(pattern, right)?);
                names
            }
            LogicalPlan::Filter {
                input, predicate, ..
            } => {
                let names = produced(pattern, input)?;
                require("WHERE filter", predicate, &names)?;
                names
            }
            LogicalPlan::LocyGenerate {
                input,
                args,
                outputs,
                ..
            } => {
                let mut names = produced(pattern, input)?;
                for arg in args {
                    require("generator argument", arg, &names)?;
                }
                names.columns.extend(outputs.iter().cloned());
                names
            }
            LogicalPlan::LocyModelInvoke {
                input, invocations, ..
            } => {
                let mut names = produced(pattern, input)?;
                for invocation in invocations {
                    for feature in &invocation.feature_exprs {
                        require("model feature", feature, &names)?;
                    }
                }
                names
                    .columns
                    .extend(invocations.iter().map(|i| i.output_column.clone()));
                names
            }
            LogicalPlan::LocyProject {
                input, projections, ..
            } => {
                let names = produced(pattern, input)?;
                for (expr, _) in projections {
                    require("projection", expr, &names)?;
                }
                Produced {
                    entities: HashSet::new(),
                    columns: projections
                        .iter()
                        .filter_map(|(_, alias)| alias.clone())
                        .collect(),
                }
            }
            LogicalPlan::LocyBestBy {
                input, criteria, ..
            } => {
                let names = produced(pattern, input)?;
                for (expr, _) in criteria {
                    require("BEST BY criterion", expr, &names)?;
                }
                names
            }
            // A lower-stratum anchor's seed: the distinct anchored vids of
            // the reference, under the seed column's own name.
            LogicalPlan::Project { input, projections } => {
                let names = produced(pattern, input)?;
                for (expr, _) in projections {
                    require("anchor seed", expr, &names)?;
                }
                Produced {
                    entities: HashSet::new(),
                    columns: projections
                        .iter()
                        .filter_map(|(_, alias)| alias.clone())
                        .collect(),
                }
            }
            LogicalPlan::Distinct { input } => produced(pattern, input)?,
            // A pattern walked from its anchoring reference (issue #301): the
            // pattern planner's own nodes, over the join that seeds the walk
            // with the referenced nodes. They produce the pattern's variables
            // on top of whatever the seed produced; their expressions were
            // validated by the Cypher planner against the pattern's scope.
            LogicalPlan::Traverse { .. }
            | LogicalPlan::TraverseMainByType { .. }
            | LogicalPlan::BindPath { .. }
            | LogicalPlan::BindZeroLengthPath { .. } => {
                let mut names = Produced {
                    entities: pattern.clone(),
                    columns: HashSet::new(),
                };
                for child in plan.children() {
                    names.extend(produced(pattern, child)?);
                }
                names
            }
            other => {
                let node = format!("{other:?}");
                let kind = node.split([' ', '{', '(']).next().unwrap_or_default();
                bail!("internal error: unexpected plan node above a Locy clause's pattern: {kind}")
            }
        })
    }
    produced(pattern, body).map(|_| ())
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Combine multiple expressions with AND.
fn combine_with_and(exprs: &[&Expr]) -> Expr {
    assert!(!exprs.is_empty());
    let mut result = exprs[0].clone();
    for expr in &exprs[1..] {
        result = Expr::BinaryOp {
            left: Box::new(result),
            op: BinaryOp::And,
            right: Box::new((*expr).clone()),
        };
    }
    result
}

/// Build the join predicate for an IS-reference.
///
/// Returns true if `e` or any sub-expression references a variable in `vars`.
///
/// Used to partition WHERE conditions into those that can be applied before
/// IS-ref joins (immediate) vs those that must wait until after (deferred),
/// when conditions reference IS-ref target variables not yet in the plan.
/// Every variable `expr` names is already in `vars`.
///
/// The gate on handing a conjunct to `plan_where_clause`, which validates
/// against the pattern's scope and errors on anything outside it. Locy binds
/// names the Cypher pattern never sees — generator outputs, `ALONG` variables,
/// columns another clause introduces — so a predicate over one of those belongs
/// in a `Filter` above the pattern and must not be offered for pushdown.
fn expr_vars_all_in_scope(expr: &Expr, vars: &[VariableInfo]) -> bool {
    collect_expr_variables(expr)
        .iter()
        .all(|v| is_var_in_scope(vars, v))
}

/// Rename every field of a derived scan's schema with `prefix`.
///
/// Used for every positive IS reference of a clause, with the prefix its
/// binding plan assigns, so no referenced column enters the batch under a
/// bare name another name could capture (see `build_clause` Step 3).
fn alias_derived_schema(schema: &SchemaRef, prefix: &str) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            f.as_ref()
                .clone()
                .with_name(format!("{prefix}{}", f.name()))
        })
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// `left = right`, with `right` a column of the joined batch.
fn equals(left: Expr, right: &str) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Variable(right.to_string())),
    }
}

/// The expression of the WHERE condition at `index`, which the binding plan
/// lists as a filter.
fn filter_condition(clause: &CompiledClause, index: usize) -> Result<&Expr> {
    match clause.where_conditions.get(index) {
        Some(RuleCondition::Expression(expr)) => Ok(expr),
        _ => bail!("internal error: binding plan filter {index} is not a WHERE expression"),
    }
}

/// Where a name a clause expression reads gets its value in the batch.
enum NameSource {
    /// A node or relationship the Cypher expression compiler knows as an
    /// entity; the column holds its identity (`_vid` / `_eid`).
    Entity(String),
    /// A plain column.
    Column(String),
    /// A name left as written: a list-valued MATCH variable, or a name
    /// [`ClauseScope::passthrough`] lists.
    Passthrough,
}

/// The names a clause body may read, resolved through its binding plan.
///
/// The single place a clause expression's names become batch columns: a
/// name the plan does not source is an internal error that names it, never
/// a bare column reference DataFusion would resolve by name — which captured
/// whatever column happened to carry that name (issue #300).
struct ClauseScope<'p> {
    plan: &'p ClauseBindingPlan,
    /// Names resolved later or elsewhere: the columns `LocyModelInvoke` adds,
    /// and, in a projection, ALONG names.
    passthrough: HashSet<String>,
}

impl ClauseScope<'_> {
    /// Where `var` gets its value, or `None` when nothing sources it.
    ///
    /// An ambiguous exposure sources nothing: reading one is the compile
    /// error `AmbiguousRuleColumn`, so a read reaching the planner is an
    /// internal error ([`Self::unsourced`]), never a guess between the
    /// references that expose it.
    fn source(&self, var: &str) -> Option<NameSource> {
        if self.passthrough.contains(var) {
            return Some(NameSource::Passthrough);
        }
        let plan = self.plan;
        match plan.scope.get(var) {
            Some(VarSource::Match(ColumnKind::Node)) => {
                Some(NameSource::Entity(format!("{var}._vid")))
            }
            Some(VarSource::Match(ColumnKind::Edge)) => {
                Some(NameSource::Entity(format!("{var}._eid")))
            }
            Some(VarSource::Match(ColumnKind::Scalar)) => Some(NameSource::Passthrough),
            Some(VarSource::RefPosition { r, pos }) => {
                let position = &plan.refs[*r].positions[*pos];
                // A node a reference binds is also scanned (`ScanAll`), so
                // the expression compiler knows it as an entity.
                Some(if position.kind == ColumnKind::Node {
                    NameSource::Entity(position.physical.clone())
                } else {
                    NameSource::Column(position.physical.clone())
                })
            }
            Some(VarSource::Generator { physical, .. }) => {
                Some(NameSource::Column(physical.clone()))
            }
            // Only an `IS NOT` subject, which reads the node it names.
            Some(VarSource::YieldAlias(source)) => self.source(source),
            None => match plan.exposure.get(var)? {
                Exposure::Unique { physical, .. } => Some(NameSource::Column(physical.clone())),
                Exposure::Ambiguous(_) => None,
            },
        }
    }

    /// The internal error for a read of `var` that nothing sources.
    fn unsourced(&self, var: &str) -> anyhow::Error {
        if let Some(Exposure::Ambiguous(owners)) = self.plan.exposure.get(var) {
            let refs: Vec<&str> = owners
                .iter()
                .map(|&o| self.plan.refs[o].rule.as_str())
                .collect();
            return anyhow::anyhow!(
                "internal error: a Locy clause reads `{var}`, which {} all expose; the \
                 compiler rejects this read as ambiguous",
                refs.join(" and ")
            );
        }
        anyhow::anyhow!(
            "internal error: a Locy clause reads `{var}`, which its binding plan does not source"
        )
    }

    /// The column holding `var`'s value or identity, for an equality test.
    ///
    /// A MATCH relationship is read as `e._eid` through a property access,
    /// the form the join classifier attributes to `e`'s side of a join.
    fn identity(&self, var: &str) -> Result<Expr> {
        if let Some(VarSource::Match(ColumnKind::Edge)) = self.plan.scope.get(var)
            && !self.passthrough.contains(var)
        {
            return Ok(Expr::Property(
                Box::new(Expr::Variable(var.to_string())),
                "_eid".to_string(),
            ));
        }
        match self.source(var) {
            Some(NameSource::Entity(column) | NameSource::Column(column)) => {
                Ok(Expr::Variable(column))
            }
            Some(NameSource::Passthrough) => Ok(Expr::Variable(var.to_string())),
            None => Err(self.unsourced(var)),
        }
    }

    /// `expr` with every name resolved to the batch: plain columns are
    /// renamed to their physical column, entities and passthrough names are
    /// kept for the expression compiler.
    fn resolve(&self, expr: Expr) -> Result<Expr> {
        let missing = RefCell::new(None::<String>);
        let resolved = map_variables(expr, &|name| match self.source(name) {
            Some(NameSource::Column(column)) => Some(Expr::Variable(column)),
            Some(NameSource::Entity(_) | NameSource::Passthrough) => None,
            None => {
                missing.borrow_mut().get_or_insert_with(|| name.to_string());
                None
            }
        });
        match missing.into_inner() {
            Some(var) => Err(self.unsourced(&var)),
            None => Ok(resolved),
        }
    }

    /// A resolved projection: a bare entity is read through its identity
    /// column, which is how a rule's facts carry nodes and relationships.
    fn top_level(&self, expr: Expr) -> Expr {
        match &expr {
            Expr::Variable(var) => match self.source(var) {
                Some(NameSource::Entity(column)) => Expr::Variable(column),
                _ => expr,
            },
            _ => expr,
        }
    }

    /// `expr` with every variable outside a property access read through its
    /// identity column, as the model-feature resolvers look columns up: a
    /// node feature `scorer(s)` or `pagerank(s)` reads `s`'s vid.
    fn resolve_identity(&self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::Variable(var) => self.identity(&var),
            Expr::Property(object, prop) if matches!(object.as_ref(), Expr::Variable(_)) => {
                Ok(Expr::Property(Box::new(self.resolve(*object)?), prop))
            }
            other => {
                let error = RefCell::new(None::<anyhow::Error>);
                let resolved = other.map_children_in_scope(&mut |child| match self
                    .resolve_identity(child.clone())
                {
                    Ok(e) => e,
                    Err(e) => {
                        error.borrow_mut().get_or_insert(e);
                        child
                    }
                });
                match error.into_inner() {
                    Some(e) => Err(e),
                    None => Ok(resolved),
                }
            }
        }
    }
}

/// Rewrite a [`LocyExpr`] into a Cypher [`Expr`] over the clause's batch.
///
/// `PrevRef(field)` reads `field` of the recursive reference ALONG's `prev`
/// denotes, under that reference's prefix (`prev_prefix`); the Cypher parts
/// are resolved through `scope`.
fn rewrite_locy_expr(
    expr: &LocyExpr,
    prev_prefix: Option<&str>,
    scope: &ClauseScope<'_>,
) -> Result<Expr> {
    match expr {
        LocyExpr::PrevRef(field) => {
            let prefix = prev_prefix
                .with_context(|| format!("prev.{field} has no recursive IS reference to read"))?;
            Ok(Expr::Variable(format!("{prefix}{field}")))
        }
        LocyExpr::Cypher(e) => scope.resolve(e.clone()),
        LocyExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_locy_expr(left, prev_prefix, scope)?),
            op: locy_op_to_cypher_op(op),
            right: Box::new(rewrite_locy_expr(right, prev_prefix, scope)?),
        }),
        LocyExpr::UnaryOp(op, inner) => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_locy_expr(inner, prev_prefix, scope)?),
        }),
    }
}

/// Rewrite `Variable` nodes in an expression tree.
///
/// At each `Variable(name)`, replaces the node with `f(name)` when that returns
/// `Some`, leaving it unchanged otherwise. Substitution is not re-entered: the
/// expression `f` returns is inserted as-is and never re-scanned, so a rewrite
/// mapping a name onto itself cannot loop.
///
/// This is the single source of truth for variable substitution in Locy
/// planning — the ALONG-inlining, clause-name resolution and FOLD-alias
/// rewrites are all thin wrappers over it.
///
/// Recursion is delegated to [`Expr::map_children_in_scope`], which reaches the
/// full variant set through `Expr::map_children` — the canonical exhaustive
/// match, with no wildcard arm, so a new `Expr` variant fails to compile there
/// until it is classified. This function previously hand-matched 14 arms and
/// ended in `other => other`, silently dropping ten sub-expression-carrying
/// variants (`Exists`, `CountSubquery`, `CollectSubquery`, `Quantifier`,
/// `Reduce`, `ListComprehension`, `PatternComprehension`, `ValidAt`,
/// `MapProjection`, `LabelCheck`) — the "No field named …" class it claimed to
/// prevent. Locy reaches all of them: `locy_yield_item` and `fold_expression`
/// re-parse an arbitrary Cypher `expression` wholesale.
///
/// Four positions are deliberately NOT rewritten:
///
/// * **Subquery bodies** (`Exists`, `CountSubquery`, `CollectSubquery`) own a
///   `Box<Query>`, not an `Expr`. A `Fn(&str) -> Option<Expr>` cannot express a
///   rewrite over a whole query, and Locy binds no name a subquery could
///   legally reference.
/// * **Comprehension bodies** — `ListComprehension`'s where/map, `Quantifier`'s
///   predicate, `Reduce`'s body — are scoped to the comprehension's own loop
///   variable, so only their outer-scope children (`list`, `init`) are visited.
///   Rewriting inside would capture: `ALONG w = e.weight` combined with
///   `[e IN xs | e + w]` would inline `e.weight` under a binder also named `e`.
///   `PatternComprehension` is skipped whole, since its shadowing set is every
///   variable its `pattern` binds rather than a single field.
/// * **`MapProjectionItem::Variable`** holds a `String`, not an `Expr`, so
///   `n{w}` cannot receive an expression-valued rewrite.
/// * **`FunctionCall::window_spec`** (its `partition_by` / `order_by`
///   expressions) is not visited. That is an upstream gap in
///   `Expr::map_children` shared by every consumer, not specific to Locy.
fn map_variables(expr: Expr, f: &dyn Fn(&str) -> Option<Expr>) -> Expr {
    match expr {
        Expr::Variable(name) => f(&name).unwrap_or(Expr::Variable(name)),
        other => other.map_children_in_scope(&mut |e| map_variables(e, f)),
    }
}

/// Rename `Variable` nodes according to a name -> name map.
///
/// The body of [`substitute_fold_aliases`].
fn rename_variables(expr: Expr, renames: &HashMap<String, String>) -> Expr {
    if renames.is_empty() {
        return expr;
    }
    map_variables(expr, &|name| {
        renames.get(name).map(|n| Expr::Variable(n.clone()))
    })
}

/// Substitute `Variable(name)` nodes matching ALONG binding names with their
/// rewritten expressions, inlining the underlying expression.
///
/// Allows YIELD/FOLD expressions like `ew * 2.0` to reference an ALONG binding
/// (`ALONG ew = e.weight`) by inlining it to `e.weight * 2.0`. Exhaustive over
/// all `Expr` variants via [`map_variables`].
fn substitute_along_vars(expr: Expr, along: &HashMap<&str, Expr>) -> Expr {
    if along.is_empty() {
        return expr;
    }
    map_variables(expr, &|name| along.get(name).cloned())
}

/// Rename FOLD output variables to their YIELD aliases in HAVING / BEST BY exprs.
///
/// When a FOLD binding is aliased (`FOLD n = COUNT(*)` with `YIELD ... n AS support`),
/// HAVING and BEST BY reference `n` but after FoldExec the column is named `support`;
/// this rewrites `Variable("n")` → `Variable("support")`. Exhaustive over all `Expr`
/// variants via [`map_variables`], so a fold output nested in `CASE`/`IN`/etc. is
/// also renamed.
fn substitute_fold_aliases(expr: Expr, aliases: &HashMap<String, String>) -> Expr {
    rename_variables(expr, aliases)
}

/// Return whether `expr` references any FOLD-output variable.
///
/// Reuses the exhaustive [`map_variables`] walk (the single source of truth for
/// expression traversal) so every variant — including a variable nested inside
/// `CASE`/`IN`/a list — is inspected. Used to decide that a YIELD expression
/// must be produced POST-fold rather than in the pre-fold body.
fn expr_references_fold_output(expr: &Expr, fold_output_names: &HashSet<&str>) -> bool {
    let found = std::cell::Cell::new(false);
    let _ = map_variables(expr.clone(), &|name| {
        if fold_output_names.contains(name) {
            found.set(true);
        }
        None
    });
    found.get()
}

/// Map [`LocyBinaryOp`] to Cypher [`BinaryOp`].
pub(crate) fn locy_op_to_cypher_op(op: &LocyBinaryOp) -> BinaryOp {
    match op {
        LocyBinaryOp::Add => BinaryOp::Add,
        LocyBinaryOp::Sub => BinaryOp::Sub,
        LocyBinaryOp::Mul => BinaryOp::Mul,
        LocyBinaryOp::Div => BinaryOp::Div,
        LocyBinaryOp::Mod => BinaryOp::Mod,
        LocyBinaryOp::Pow => BinaryOp::Pow,
        LocyBinaryOp::And => BinaryOp::And,
        LocyBinaryOp::Or => BinaryOp::Or,
        LocyBinaryOp::Xor => BinaryOp::Xor,
    }
}

/// Build an Arrow schema from a compiled rule's yield columns using inferred types.
///
/// Computes target-rule node vars and infers types using the same logic as `build_rule`,
/// ensuring the derived scan schema matches `yield_columns_to_arrow_schema()` in
/// `locy_program.rs`.
fn yield_schema_to_arrow_from_rule(
    target_rule: &CompiledRule,
    rule_catalog: &HashMap<String, CompiledRule>,
    schema: &Schema,
) -> SchemaRef {
    let target_node_vars = collect_node_vars(&target_rule.clauses);
    let first_clause = target_rule.clauses.first();
    let fold_names: HashSet<&str> = first_clause
        .map(|c| c.fold.iter().map(|fb| fb.name.as_str()).collect())
        .unwrap_or_default();
    let along_types = along_column_types(&target_rule.clauses, schema);
    let var_labels = first_clause.map(clause_var_labels).unwrap_or_default();
    // A column some clause folds into is typed by that fold, even when the
    // first clause only seeds it; see `fold_input_column_types`.
    let fold_types = fold_input_column_types(target_rule, rule_catalog, schema);

    let fields: Vec<Field> = target_rule
        .yield_schema
        .iter()
        .map(|yc| {
            if let Some(dt) = fold_types.get(&yc.name).filter(|_| !yc.is_key) {
                return Field::new(&yc.name, dt.clone(), true);
            }
            let dt = match first_clause {
                Some(fc) => infer_yield_type(
                    &yc.name,
                    fc,
                    &target_node_vars,
                    &fold_names,
                    &along_types,
                    rule_catalog,
                    yc.is_key,
                    schema,
                    &var_labels,
                ),
                None => DataType::LargeUtf8,
            };
            Field::new(&yc.name, dt, true)
        })
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Validate that every `PrevRef` field in a [`LocyExpr`] is a column of the
/// recursive reference ALONG's `prev` reads.
///
/// `prev` is `(rule, its yield columns)` for the clause's one positive
/// reference into its own recursive group, `None` when it has none.
fn validate_prev_refs(expr: &LocyExpr, prev: Option<&(&str, HashSet<&str>)>) -> Result<()> {
    match expr {
        LocyExpr::PrevRef(field) => {
            let Some((rule, fields)) = prev else {
                bail!(
                    "prev.{field} references field '{field}', but the clause has no \
                     IS reference into its own recursive group for prev to read",
                );
            };
            if !fields.contains(field.as_str()) {
                bail!(
                    "prev.{field} references field '{field}' not found in the yield \
                     schema of IS-reference '{rule}'",
                );
            }
            Ok(())
        }
        LocyExpr::Cypher(_) => Ok(()),
        LocyExpr::BinaryOp { left, right, .. } => {
            validate_prev_refs(left, prev)?;
            validate_prev_refs(right, prev)
        }
        LocyExpr::UnaryOp(_, inner) => validate_prev_refs(inner, prev),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use uni_cypher::ast::{LabelExpr, NodePattern, PathPattern, Pattern, PatternElement, UnaryOp};
    use uni_cypher::locy_ast::{
        AlongBinding, BestByClause, BestByItem, FoldBinding, IsReference, LocyBinaryOp, LocyExpr,
        LocyYieldItem, QualifiedName, RuleCondition, RuleOutput, YieldClause,
    };
    use uni_locy::types::{CompiledClause, CompiledProgram, CompiledRule, Stratum, YieldColumn};

    use crate::query::planner::LogicalPlan;

    // -- Test helpers ---------------------------------------------------

    fn test_schema() -> Arc<uni_common::core::schema::Schema> {
        Arc::new(uni_common::core::schema::Schema {
            schema_version: 1,
            labels: HashMap::new(),
            edge_types: HashMap::new(),
            properties: HashMap::new(),
            indexes: vec![],
            constraints: vec![],
            schemaless_registry: Default::default(),
            ..Default::default()
        })
    }

    fn test_planner() -> QueryPlanner {
        QueryPlanner::new(test_schema())
    }

    fn test_classifier_ctx() -> ClassifierContext {
        ClassifierContext {
            registry: Arc::new(uni_locy::ClassifierRegistry::new()),
            cache: None,
            provenance_store: None,
        }
    }

    fn yield_col(name: &str, is_key: bool) -> YieldColumn {
        YieldColumn {
            name: name.to_string(),
            is_key,
            is_prob: false,
        }
    }

    fn qname(name: &str) -> QualifiedName {
        QualifiedName {
            parts: vec![name.to_string()],
        }
    }

    /// A minimal pattern: `(n)` — single node.
    fn node_pattern(var: &str) -> Pattern {
        Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements: vec![PatternElement::Node(NodePattern {
                    variable: Some(var.to_string()),
                    labels: LabelExpr::Empty,
                    properties: None,
                    where_clause: None,
                })],
                shortest_path_mode: None,
            }],
        }
    }

    /// A two-node pattern: `(a)-[e]->(b)`
    fn edge_pattern(a: &str, e: &str, b: &str) -> Pattern {
        use uni_cypher::ast::{Direction, RelationshipPattern};
        Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements: vec![
                    PatternElement::Node(NodePattern {
                        variable: Some(a.to_string()),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Relationship(RelationshipPattern {
                        variable: Some(e.to_string()),
                        types: LabelExpr::Empty,
                        direction: Direction::Outgoing,
                        range: None,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Node(NodePattern {
                        variable: Some(b.to_string()),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                ],
                shortest_path_mode: None,
            }],
        }
    }

    fn simple_yield_output(names: &[&str]) -> RuleOutput {
        RuleOutput::Yield(YieldClause {
            items: names
                .iter()
                .map(|n| LocyYieldItem {
                    is_key: false,
                    is_prob: false,
                    expr: Expr::Variable(n.to_string()),
                    alias: None,
                })
                .collect(),
        })
    }

    fn simple_clause(pattern: Pattern, yield_names: &[&str]) -> CompiledClause {
        CompiledClause {
            match_pattern: pattern,
            where_conditions: vec![],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(yield_names),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        }
    }

    /// Create a minimal CompiledRule for use in derived scan handle tests.
    fn test_compiled_rule(yield_cols: &[YieldColumn]) -> CompiledRule {
        CompiledRule {
            name: "test".to_string(),
            clauses: vec![simple_clause(node_pattern("n"), &[])],
            yield_schema: yield_cols.to_vec(),
            priority: None,
            parameters: Vec::new(),
        }
    }

    /// `clause` with the binding plan the compiler would attach against
    /// `catalog`, a reference to a rule in `names` counting as recursive.
    fn planned(
        clause: &CompiledClause,
        catalog: &HashMap<String, CompiledRule>,
        names: &HashSet<String>,
    ) -> CompiledClause {
        let kinds = uni_locy::ColumnKinds::infer(catalog);
        let mut clause = clause.clone();
        clause.binding =
            ClauseBindingPlan::compute("test", &clause, catalog, &kinds, &|t| names.contains(t))
                .map_or(uni_locy::ClauseBinding::Pending, |p| {
                    uni_locy::ClauseBinding::Ready(Arc::new(p))
                });
        clause
    }

    /// `rule` with every clause [`planned`].
    fn planned_rule(
        rule: &CompiledRule,
        catalog: &HashMap<String, CompiledRule>,
        names: &HashSet<String>,
    ) -> CompiledRule {
        let mut rule = rule.clone();
        rule.clauses = rule
            .clauses
            .iter()
            .map(|c| planned(c, catalog, names))
            .collect();
        rule
    }

    /// `stratum` with every rule [`planned_rule`].
    fn planned_stratum(
        stratum: &Stratum,
        catalog: &HashMap<String, CompiledRule>,
        names: &HashSet<String>,
    ) -> Stratum {
        Stratum {
            rules: stratum
                .rules
                .iter()
                .map(|r| planned_rule(r, catalog, names))
                .collect(),
            ..stratum.clone()
        }
    }

    fn make_program(
        strata: Vec<Stratum>,
        rule_catalog: HashMap<String, CompiledRule>,
    ) -> CompiledProgram {
        let mut program = CompiledProgram {
            strata,
            rule_catalog,
            model_catalog: HashMap::new(),
            warnings: vec![],
            commands: vec![],
        };
        uni_locy::attach_binding_plans(&mut program).expect("binding plans attach");
        program
    }

    fn make_rule(
        name: &str,
        clauses: Vec<CompiledClause>,
        yield_schema: Vec<YieldColumn>,
    ) -> CompiledRule {
        CompiledRule {
            name: name.to_string(),
            clauses,
            yield_schema,
            priority: None,
            parameters: Vec::new(),
        }
    }

    fn plan_is_project(plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Project { .. } | LogicalPlan::LocyProject { .. }
        )
    }

    fn plan_is_filter(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Filter { .. })
    }

    fn plan_is_cross_join(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::CrossJoin { .. })
    }

    fn plan_is_derived_scan(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyDerivedScan { .. })
    }

    fn plan_is_fold(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyFold { .. })
    }

    fn plan_is_best_by(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyBestBy { .. })
    }

    // ===================================================================
    // Unit Tests — LocyExpr Rewriting
    // ===================================================================

    /// `rewrite_locy_expr` with an unprefixed `prev` and `x` in scope.
    fn rewrite_bare(expr: &LocyExpr) -> Result<Expr> {
        let plan = ClauseBindingPlan {
            steps: vec![],
            refs: vec![],
            generators: vec![],
            scope: std::collections::BTreeMap::new(),
            exposure: std::collections::BTreeMap::new(),
            immediate_filters: vec![],
            prev_ref: None,
            anchor: None,
            diagnostics: vec![],
        };
        let scope = ClauseScope {
            plan: &plan,
            passthrough: HashSet::from(["x".to_string()]),
        };
        rewrite_locy_expr(expr, Some(""), &scope)
    }

    #[test]
    fn test_rewrite_prev_ref() {
        let expr = LocyExpr::PrevRef("cost".to_string());
        let result = rewrite_bare(&expr).unwrap();
        assert_eq!(result, Expr::Variable("cost".to_string()));
    }

    #[test]
    fn test_rewrite_cypher_passthrough() {
        let inner = Expr::Literal(CypherLiteral::Integer(42));
        let expr = LocyExpr::Cypher(inner.clone());
        let result = rewrite_bare(&expr).unwrap();
        assert_eq!(result, inner);
    }

    #[test]
    fn test_rewrite_binary_add() {
        let expr = LocyExpr::BinaryOp {
            left: Box::new(LocyExpr::PrevRef("x".to_string())),
            op: LocyBinaryOp::Add,
            right: Box::new(LocyExpr::Cypher(Expr::Literal(CypherLiteral::Integer(1)))),
        };
        let result = rewrite_bare(&expr).unwrap();
        assert_eq!(
            result,
            Expr::BinaryOp {
                left: Box::new(Expr::Variable("x".to_string())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(1))),
            }
        );
    }

    #[test]
    fn test_rewrite_nested_binary() {
        // (prev.a + prev.b) * prev.c
        let expr = LocyExpr::BinaryOp {
            left: Box::new(LocyExpr::BinaryOp {
                left: Box::new(LocyExpr::PrevRef("a".to_string())),
                op: LocyBinaryOp::Add,
                right: Box::new(LocyExpr::PrevRef("b".to_string())),
            }),
            op: LocyBinaryOp::Mul,
            right: Box::new(LocyExpr::PrevRef("c".to_string())),
        };
        let result = rewrite_bare(&expr).unwrap();
        let expected = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Variable("a".to_string())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Variable("b".to_string())),
            }),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Variable("c".to_string())),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_rewrite_unary_not() {
        let expr = LocyExpr::UnaryOp(
            UnaryOp::Not,
            Box::new(LocyExpr::Cypher(Expr::Variable("x".to_string()))),
        );
        let result = rewrite_bare(&expr).unwrap();
        assert_eq!(
            result,
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(Expr::Variable("x".to_string())),
            }
        );
    }

    #[test]
    fn test_locy_op_to_cypher_op_all() {
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Add), BinaryOp::Add);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Sub), BinaryOp::Sub);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Mul), BinaryOp::Mul);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Div), BinaryOp::Div);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Mod), BinaryOp::Mod);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Pow), BinaryOp::Pow);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::And), BinaryOp::And);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Or), BinaryOp::Or);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Xor), BinaryOp::Xor);
    }

    // ===================================================================
    // Unit Tests — DerivedScanHandle Registry
    // ===================================================================

    #[test]
    fn test_handle_allocation_new() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true), yield_col("m", false)];
        let rule = test_compiled_rule(&cols);
        let handle =
            builder.get_or_create_derived_scan_handle("reachable", &rule, false, &HashMap::new());
        assert_eq!(handle.scan_index, 0);
        assert_eq!(handle.rule_name, "reachable");
        assert!(handle.data.read().is_empty());
    }

    #[test]
    fn test_handle_reuse_same_rule() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true), yield_col("m", false)];
        let rule = test_compiled_rule(&cols);
        let h1 =
            builder.get_or_create_derived_scan_handle("reachable", &rule, false, &HashMap::new());
        let h2 =
            builder.get_or_create_derived_scan_handle("reachable", &rule, false, &HashMap::new());
        assert!(Arc::ptr_eq(&h1.data, &h2.data));
        assert_eq!(h1.scan_index, h2.scan_index);
    }

    #[test]
    fn test_handle_different_rules() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        let h1 =
            builder.get_or_create_derived_scan_handle("reachable", &rule, false, &HashMap::new());
        let h2 =
            builder.get_or_create_derived_scan_handle("connected", &rule, false, &HashMap::new());
        assert_eq!(h1.scan_index, 0);
        assert_eq!(h2.scan_index, 1);
        assert!(!Arc::ptr_eq(&h1.data, &h2.data));
    }

    #[test]
    fn test_registry_conversion() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        builder.get_or_create_derived_scan_handle("rule_a", &rule, false, &HashMap::new());
        builder.get_or_create_derived_scan_handle("rule_b", &rule, true, &HashMap::new());

        let registry = builder.build_registry();
        assert!(registry.get(0).is_some());
        assert!(registry.get(1).is_some());
        assert_eq!(registry.get(0).unwrap().rule_name, "rule_a");
        assert_eq!(registry.get(1).unwrap().rule_name, "rule_b");
    }

    #[test]
    fn test_registry_self_ref_flag() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        builder.get_or_create_derived_scan_handle("self_rule", &rule, true, &HashMap::new());
        builder.get_or_create_derived_scan_handle("cross_rule", &rule, false, &HashMap::new());

        let registry = builder.build_registry();
        assert!(registry.get(0).unwrap().is_self_ref);
        assert!(!registry.get(1).unwrap().is_self_ref);
    }

    // ===================================================================
    // Unit Tests — build_stratum
    // ===================================================================

    #[test]
    fn test_non_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("base".to_string(), rule.clone());

        let stratum = Stratum {
            id: 0,
            rules: vec![rule],
            is_recursive: false,
            depends_on: vec![],
        };
        let names: HashSet<String> = ["base".to_string()].into();
        let result = builder
            .build_stratum(
                &planned_stratum(&stratum, &catalog, &names),
                &catalog,
                &names,
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.id, 0);
        assert!(!result.is_recursive);
        assert_eq!(result.rules.len(), 1);
        assert_eq!(result.rules[0].name, "base");
    }

    #[test]
    fn test_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reach",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("reach".to_string(), rule.clone());

        let stratum = Stratum {
            id: 1,
            rules: vec![rule],
            is_recursive: true,
            depends_on: vec![0],
        };
        let names: HashSet<String> = ["reach".to_string()].into();
        let result = builder
            .build_stratum(
                &planned_stratum(&stratum, &catalog, &names),
                &catalog,
                &names,
                &test_classifier_ctx(),
            )
            .unwrap();

        assert!(result.is_recursive);
        assert_eq!(result.depends_on, vec![0]);
    }

    #[test]
    fn test_stratum_depends_on() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "derived",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("derived".to_string(), rule.clone());

        let stratum = Stratum {
            id: 2,
            rules: vec![rule],
            is_recursive: false,
            depends_on: vec![0, 1],
        };
        let names: HashSet<String> = ["derived".to_string()].into();
        let result = builder
            .build_stratum(
                &planned_stratum(&stratum, &catalog, &names),
                &catalog,
                &names,
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.depends_on, vec![0, 1]);
    }

    #[test]
    fn test_stratum_multiple_rules() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rules: Vec<CompiledRule> = ["a", "b", "c"]
            .iter()
            .map(|name| {
                make_rule(
                    name,
                    vec![simple_clause(node_pattern("n"), &["n"])],
                    vec![yield_col("n", true)],
                )
            })
            .collect();
        let mut catalog = HashMap::new();
        for r in &rules {
            catalog.insert(r.name.clone(), r.clone());
        }

        let stratum = Stratum {
            id: 0,
            rules: rules.clone(),
            is_recursive: true,
            depends_on: vec![],
        };
        let names: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let result = builder
            .build_stratum(
                &planned_stratum(&stratum, &catalog, &names),
                &catalog,
                &names,
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.rules.len(), 3);
        let rule_names: Vec<&str> = result.rules.iter().map(|r| r.name.as_str()).collect();
        assert!(rule_names.contains(&"a"));
        assert!(rule_names.contains(&"b"));
        assert!(rule_names.contains(&"c"));
    }

    // ===================================================================
    // Unit Tests — build_rule
    // ===================================================================

    #[test]
    fn test_rule_name_and_schema() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reachable",
            vec![simple_clause(edge_pattern("n", "e", "m"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), rule.clone())]);
        let names: HashSet<String> = ["reachable".to_string()].into();

        let result = builder
            .build_rule(
                &planned_rule(&rule, &catalog, &names),
                false,
                &names,
                &catalog,
                &test_classifier_ctx(),
            )
            .unwrap();
        assert_eq!(result.name, "reachable");
        assert_eq!(result.yield_schema.len(), 2);
        assert_eq!(result.yield_schema[0].name, "n");
        assert!(result.yield_schema[0].is_key);
        assert_eq!(result.yield_schema[1].name, "m");
        assert!(!result.yield_schema[1].is_key);
    }

    #[test]
    fn test_rule_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let mut rule = make_rule(
            "prio",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        rule.priority = Some(5);
        let catalog = HashMap::from([("prio".to_string(), rule.clone())]);
        let names: HashSet<String> = ["prio".to_string()].into();

        let result = builder
            .build_rule(
                &planned_rule(&rule, &catalog, &names),
                false,
                &names,
                &catalog,
                &test_classifier_ctx(),
            )
            .unwrap();
        assert_eq!(result.priority, Some(5));
    }

    #[test]
    fn test_rule_no_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "noprio",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("noprio".to_string(), rule.clone())]);
        let names: HashSet<String> = ["noprio".to_string()].into();

        let result = builder
            .build_rule(
                &planned_rule(&rule, &catalog, &names),
                false,
                &names,
                &catalog,
                &test_classifier_ctx(),
            )
            .unwrap();
        assert_eq!(result.priority, None);
    }

    #[test]
    fn test_rule_multiple_clauses() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "multi",
            vec![
                simple_clause(node_pattern("n"), &["n"]),
                simple_clause(node_pattern("n"), &["n"]),
                simple_clause(node_pattern("n"), &["n"]),
            ],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("multi".to_string(), rule.clone())]);
        let names: HashSet<String> = ["multi".to_string()].into();

        let result = builder
            .build_rule(
                &planned_rule(&rule, &catalog, &names),
                false,
                &names,
                &catalog,
                &test_classifier_ctx(),
            )
            .unwrap();
        assert_eq!(result.clauses.len(), 3);
    }

    #[test]
    fn test_rule_yield_schema_key_flags() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "keyed",
            vec![simple_clause(edge_pattern("a", "b", "c"), &["a", "b", "c"])],
            vec![
                yield_col("a", true),
                yield_col("b", false),
                yield_col("c", true),
            ],
        );
        let catalog = HashMap::from([("keyed".to_string(), rule.clone())]);
        let names: HashSet<String> = ["keyed".to_string()].into();

        let result = builder
            .build_rule(
                &planned_rule(&rule, &catalog, &names),
                false,
                &names,
                &catalog,
                &test_classifier_ctx(),
            )
            .unwrap();
        assert!(result.yield_schema[0].is_key);
        assert!(!result.yield_schema[1].is_key);
        assert!(result.yield_schema[2].is_key);
    }

    // ===================================================================
    // Integration Tests — build_clause
    // ===================================================================

    #[test]
    fn test_clause_simple_match_yield() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = simple_clause(node_pattern("n"), &["n"]);
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert!(plan_is_project(&result.body));
        assert!(result.is_refs.is_empty());
        assert!(result.along_bindings.is_empty());
    }

    /// A rule-body `WHERE` over the pattern's own variables must reach
    /// `Scan.filter`, not sit in a `Filter` above the scan (#226).
    ///
    /// The distinction is not cosmetic. `df_planner` lowers a `Filter` node
    /// straight to `FilterExec`, and `build_indexed_property_pushdown` reads
    /// **only** `Scan.filter`, so a predicate left above the scan consults no
    /// index and filters a fully materialised scan. Measured on an indexed
    /// boolean over 40k vertices, the rule-body spelling cost 5.8x plain Cypher
    /// while the identical predicate written inline as `(e:Entity {b: true})`
    /// cost 1.1x — the whole difference being which node held the predicate.
    ///
    /// Asserting the plan shape rather than a duration is what makes this a
    /// gate: a timing guard on the same behaviour would be at the mercy of the
    /// machine, and would still pass if pushdown regressed on a fast enough box.
    #[test]
    fn issue_226_rule_body_where_reaches_the_scan() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        // `n.age > 21` — a property access on the pattern's own variable, so
        // every name it uses is in the pattern's scope and it is pushable.
        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![RuleCondition::Expression(Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".to_string())),
                    "age".to_string(),
                )),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(21))),
            })],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Project → (scan). Anything between the projection and the scan means
        // the predicate did not make it down.
        let mut node = &result.body;
        while let LogicalPlan::Project { input, .. } | LogicalPlan::LocyProject { input, .. } = node
        {
            node = input;
        }
        let filter = match node {
            LogicalPlan::ScanAll { filter, .. } | LogicalPlan::Scan { filter, .. } => filter,
            LogicalPlan::Filter { .. } => panic!(
                "the rule-body WHERE is still a Filter node above the scan, so it \
                 reaches neither Scan.filter nor index selection (#226)"
            ),
            other => panic!("expected a scan under the projection, got {other:?}"),
        };
        assert!(
            filter.is_some(),
            "the scan carries no filter, so `n.age > 21` was dropped or left above it"
        );
    }

    #[test]
    fn test_clause_with_where_filter() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![RuleCondition::Expression(Expr::BinaryOp {
                left: Box::new(Expr::Variable("n".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(21))),
            })],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Project wraps Filter wraps Scan
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(plan_is_filter(input));
        }
    }

    #[test]
    fn test_clause_with_single_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(result.is_refs[0].rule_name, "reachable");
        assert!(!result.is_refs[0].negated);

        // Structure: Project { Filter { CrossJoin { Scan, DerivedScan } } }
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(plan_is_filter(input));
            if let LogicalPlan::Filter { input, .. } = input.as_ref() {
                assert!(plan_is_cross_join(input));
                if let LogicalPlan::CrossJoin { right, .. } = input.as_ref() {
                    assert!(plan_is_derived_scan(right));
                }
            }
        }
    }

    #[test]
    fn test_clause_with_is_ref_to_target() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: Some("y".to_string()),
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x", "y"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("x", true), yield_col("y", false)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(
            result.is_refs[0].target,
            Some(Expr::Variable("y".to_string()))
        );
    }

    #[test]
    fn test_clause_with_negated_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: true,
            })],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert!(result.is_refs[0].negated);

        // No CrossJoin for negated — anti-join deferred to fixpoint
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(!plan_is_cross_join(input));
            assert!(!plan_is_filter(input));
        }
    }

    #[test]
    fn test_clause_with_multiple_is_refs() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule_a = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let rule_b = make_rule(
            "connected",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([
            ("reachable".to_string(), rule_a),
            ("connected".to_string(), rule_b),
        ]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![
                RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("reachable"),
                    target: None,
                    negated: false,
                }),
                RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("connected"),
                    target: None,
                    negated: false,
                }),
            ],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 2);
        assert_eq!(result.is_refs[0].rule_name, "reachable");
        assert_eq!(result.is_refs[1].rule_name, "connected");
    }

    // ── map_variables traversal (Tier 1.3) ───────────────────────────

    /// Ten sub-expression-carrying `Expr` variants were silently dropped by the
    /// old `other => other` arm. Locy reaches them because `locy_yield_item` and
    /// `fold_expression` re-parse an arbitrary Cypher `expression` wholesale.
    #[test]
    fn map_variables_rewrites_newly_traversed_variants() {
        let sub = |name: &str| -> Option<Expr> {
            (name == "w")
                .then(|| Expr::Property(Box::new(Expr::Variable("e".into())), "weight".into()))
        };
        let w = || Expr::Variable("w".into());
        let inlined = Expr::Property(Box::new(Expr::Variable("e".into())), "weight".into());

        // LabelCheck.expr
        let got = map_variables(
            Expr::LabelCheck {
                expr: Box::new(w()),
                labels: vec!["L".into()],
            },
            &sub,
        );
        match got {
            Expr::LabelCheck { expr, .. } => assert_eq!(*expr, inlined),
            other => panic!("expected LabelCheck, got {other:?}"),
        }

        // MapProjection literal-entry value
        let got = map_variables(
            Expr::MapProjection {
                base: Box::new(Expr::Variable("n".into())),
                items: vec![uni_cypher::ast::MapProjectionItem::LiteralEntry(
                    "k".into(),
                    Box::new(w()),
                )],
            },
            &sub,
        );
        match got {
            Expr::MapProjection { items, .. } => match &items[0] {
                uni_cypher::ast::MapProjectionItem::LiteralEntry(_, v) => {
                    assert_eq!(**v, inlined)
                }
                other => panic!("expected LiteralEntry, got {other:?}"),
            },
            other => panic!("expected MapProjection, got {other:?}"),
        }

        // ListComprehension.list — the outer-scope child
        let got = map_variables(
            Expr::ListComprehension {
                variable: "x".into(),
                list: Box::new(w()),
                where_clause: None,
                map_expr: Box::new(Expr::Variable("x".into())),
            },
            &sub,
        );
        match got {
            Expr::ListComprehension { list, .. } => assert_eq!(*list, inlined),
            other => panic!("expected ListComprehension, got {other:?}"),
        }

        // Reduce.init and Reduce.list — both outer-scope
        let got = map_variables(
            Expr::Reduce {
                accumulator: "acc".into(),
                init: Box::new(w()),
                variable: "x".into(),
                list: Box::new(w()),
                expr: Box::new(Expr::Variable("acc".into())),
            },
            &sub,
        );
        match got {
            Expr::Reduce { init, list, .. } => {
                assert_eq!(*init, inlined);
                assert_eq!(*list, inlined);
            }
            other => panic!("expected Reduce, got {other:?}"),
        }
    }

    /// The negative half, and the reason this delegates to
    /// `map_children_in_scope` rather than `map_children`.
    ///
    /// A comprehension body is scoped to its own loop variable. Rewriting there
    /// would capture — `ALONG w = e.weight` plus `[e IN xs | e + w]` would
    /// inline `e.weight` under a binder also named `e`. `substitute_along_vars`
    /// inlines arbitrary expressions, so this is reachable, not theoretical.
    #[test]
    fn map_variables_does_not_capture_comprehension_binders() {
        let sub = |name: &str| -> Option<Expr> {
            (name == "e")
                .then(|| Expr::Property(Box::new(Expr::Variable("edge".into())), "weight".into()))
        };
        let body = || {
            Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Variable("e".into())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(1))),
            })
        };

        let input = Expr::ListComprehension {
            variable: "e".into(),
            list: Box::new(Expr::Variable("xs".into())),
            where_clause: Some(body()),
            map_expr: body(),
        };
        let got = map_variables(input.clone(), &sub);
        assert_eq!(got, input, "comprehension body must not be rewritten");

        // A whole PatternComprehension is skipped: its shadowing set is every
        // variable its `pattern` binds, not one field.
        let quant = Expr::Quantifier {
            quantifier: uni_cypher::ast::Quantifier::All,
            variable: "e".into(),
            list: Box::new(Expr::Variable("xs".into())),
            predicate: body(),
        };
        assert_eq!(
            map_variables(quant.clone(), &sub),
            quant,
            "quantifier predicate must not be rewritten"
        );
    }

    /// `expr_references_fold_output` reuses the same walk, so widening it also
    /// widens fold-output detection — in the right direction: such expressions
    /// were already failing at plan time against a column that does not exist
    /// pre-fold, and are now correctly deferred.
    #[test]
    fn expr_references_fold_output_sees_newly_traversed_variants() {
        let mut names = HashSet::new();
        names.insert("total");

        assert!(expr_references_fold_output(
            &Expr::LabelCheck {
                expr: Box::new(Expr::Variable("total".into())),
                labels: vec!["L".into()],
            },
            &names
        ));

        // Documented limit: a comprehension body is still not scanned.
        assert!(!expr_references_fold_output(
            &Expr::ListComprehension {
                variable: "x".into(),
                list: Box::new(Expr::Variable("xs".into())),
                where_clause: None,
                map_expr: Box::new(Expr::Variable("total".into())),
            },
            &names
        ));
    }

    #[test]
    fn test_clause_with_along() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "cost"])],
            vec![yield_col("n", true), yield_col("cost", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: edge_pattern("a", "e", "b"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["a".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::BinaryOp {
                    left: Box::new(LocyExpr::PrevRef("cost".to_string())),
                    op: LocyBinaryOp::Add,
                    right: Box::new(LocyExpr::Cypher(Expr::Literal(CypherLiteral::Integer(1)))),
                },
            }],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["a", "b", "cost"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [
            yield_col("a", true),
            yield_col("b", false),
            yield_col("cost", false),
        ];
        // `prev` reads the clause's reference into its own recursive group.
        let names: HashSet<String> = ["reachable".to_string()].into();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.along_bindings, vec!["cost".to_string()]);

        // The YIELD projection should contain the rewritten ALONG expression
        if let LogicalPlan::Project { projections, .. } = &result.body {
            let cost_proj = projections
                .iter()
                .find(|(_, alias)| alias.as_deref() == Some("cost"))
                .unwrap();
            assert!(matches!(cost_proj.0, Expr::BinaryOp { .. }));
        }
    }

    #[test]
    fn test_clause_with_fold_non_recursive() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Property(
                        Box::new(Expr::Variable("n".to_string())),
                        "cost".to_string(),
                    )],
                    distinct: false,
                    window_spec: None,
                },
            }],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Fold is deferred to apply_post_fixpoint_chain (not in body)
        assert!(!plan_is_fold(&result.body));
        assert!(plan_is_project(&result.body));
    }

    #[test]
    fn test_clause_fold_skipped_recursive() {
        // Probes that a monotone FOLD in a recursive clause is deferred
        // from the body plan to the fixpoint engine (not whether the
        // monotonicity check passes — that's covered by the dedicated
        // validation tests above). Uses `MMAX` because non-monotone
        // aggregates are now rejected outright in recursive strata.
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "best".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "MMAX".to_string(),
                    args: vec![Expr::Property(
                        Box::new(Expr::Variable("n".to_string())),
                        "cost".to_string(),
                    )],
                    distinct: false,
                    window_spec: None,
                },
            }],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n", "best"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("n", true), yield_col("best", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                true,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Recursive: no LocyFold (deferred to FixpointExec)
        assert!(!plan_is_fold(&result.body));
        assert!(plan_is_project(&result.body));
    }

    #[test]
    fn test_clause_with_best_by() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: Some(BestByClause {
                items: vec![BestByItem {
                    expr: Expr::Variable("cost".to_string()),
                    ascending: true,
                }],
            }),
            output: RuleOutput::Yield(YieldClause {
                items: vec![
                    LocyYieldItem {
                        is_key: false,
                        is_prob: false,
                        expr: Expr::Variable("n".to_string()),
                        alias: None,
                    },
                    LocyYieldItem {
                        is_key: false,
                        is_prob: false,
                        expr: Expr::Property(
                            Box::new(Expr::Variable("n".to_string())),
                            "cost".to_string(),
                        ),
                        alias: Some("cost".to_string()),
                    },
                ],
            }),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("n", true), yield_col("cost", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert!(plan_is_best_by(&result.body));
        if let LogicalPlan::LocyBestBy {
            key_columns,
            criteria,
            ..
        } = &result.body
        {
            assert_eq!(key_columns, &["n".to_string()]);
            assert_eq!(criteria.len(), 1);
            assert!(criteria[0].1); // ascending
        }
    }

    #[test]
    fn test_clause_with_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let mut clause = simple_clause(node_pattern("n"), &["n"]);
        clause.priority = Some(3);
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.priority, Some(3));
        if let LogicalPlan::Project { projections, .. } = &result.body {
            let prio = projections
                .iter()
                .find(|(_, alias)| alias.as_deref() == Some("__priority"));
            assert!(prio.is_some());
            if let Some((Expr::Literal(CypherLiteral::Integer(v)), _)) = prio {
                assert_eq!(*v, 3);
            }
        }
    }

    #[test]
    fn test_clause_mixed_features() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "cost"])],
            vec![yield_col("n", true), yield_col("cost", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::PrevRef("cost".to_string()),
            }],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            require: vec![],
            having: vec![],
            best_by: Some(BestByClause {
                items: vec![BestByItem {
                    expr: Expr::Variable("total".to_string()),
                    ascending: true,
                }],
            }),
            output: simple_yield_output(&["x", "cost", "total"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [
            yield_col("x", true),
            yield_col("cost", false),
            yield_col("total", false),
        ];
        // `prev` reads the clause's reference into its own recursive group.
        let names: HashSet<String> = ["reachable".to_string()].into();

        let result = builder
            .build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Layered: Project { Filter { CrossJoin { .. } } }.
        //
        // Both FOLD and BEST BY are deferred to the post-fold rule pipeline
        // (`merge_best_by` + `apply_post_fixpoint_chain`). BEST BY here ranks on
        // the aggregated `total`, which only exists post-fold; wrapping a
        // pre-fold `LocyBestBy` around the body would instead rank the raw
        // per-row input column and prune rows before aggregation (the #145
        // BEST-BY corruption). So the clause body top is the YIELD Project, not
        // a LocyBestBy.
        assert!(
            !plan_is_best_by(&result.body),
            "FOLD clause must not wrap a pre-fold BEST BY"
        );
        assert!(plan_is_project(&result.body));
        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(result.along_bindings, vec!["cost".to_string()]);
    }

    // ===================================================================
    // Integration Tests — build_program_plan
    // ===================================================================

    #[test]
    fn test_program_single_non_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("base".to_string(), rule.clone())]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule],
                is_recursive: false,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();

        if let LogicalPlan::LocyProgram {
            strata, commands, ..
        } = &plan
        {
            assert_eq!(strata.len(), 1);
            assert!(!strata[0].is_recursive);
            assert!(commands.is_empty());
        } else {
            panic!("Expected LocyProgram");
        }
    }

    #[test]
    fn test_program_single_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reach",
            vec![CompiledClause {
                match_pattern: edge_pattern("a", "e", "b"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["a".to_string()],
                    rule_name: qname("reach"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                require: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["a", "b"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
                binding: uni_locy::ClauseBinding::Pending,
            }],
            vec![yield_col("a", true), yield_col("b", false)],
        );
        let catalog = HashMap::from([("reach".to_string(), rule.clone())]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule],
                is_recursive: true,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        let entry = registry.get(0).expect("should have scan entry");
        assert!(entry.is_self_ref);
        assert_eq!(entry.rule_name, "reach");
    }

    #[test]
    fn test_program_two_strata_topological() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base_rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let derived_rule = make_rule(
            "derived",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                require: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
                binding: uni_locy::ClauseBinding::Pending,
            }],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("base".to_string(), base_rule.clone()),
            ("derived".to_string(), derived_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![derived_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata.len(), 2);
            assert_eq!(strata[0].id, 0);
            assert_eq!(strata[1].id, 1);
            assert_eq!(strata[1].depends_on, vec![0]);
        } else {
            panic!("Expected LocyProgram");
        }
    }

    #[test]
    fn test_program_cross_stratum_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base_rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let user_rule = make_rule(
            "user",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                require: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
                binding: uni_locy::ClauseBinding::Pending,
            }],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("base".to_string(), base_rule.clone()),
            ("user".to_string(), user_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![user_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        let entry = registry.get(0).expect("should have scan entry");
        assert!(!entry.is_self_ref);
    }

    #[test]
    fn test_program_multi_rule_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule_a = make_rule(
            "rule_a",
            vec![CompiledClause {
                match_pattern: node_pattern("n"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["n".to_string()],
                    rule_name: qname("rule_b"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                require: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["n"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
                binding: uni_locy::ClauseBinding::Pending,
            }],
            vec![yield_col("n", true)],
        );
        let rule_b = make_rule(
            "rule_b",
            vec![CompiledClause {
                match_pattern: node_pattern("n"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["n".to_string()],
                    rule_name: qname("rule_a"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                require: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["n"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
                binding: uni_locy::ClauseBinding::Pending,
            }],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([
            ("rule_a".to_string(), rule_a.clone()),
            ("rule_b".to_string(), rule_b.clone()),
        ]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule_a, rule_b],
                is_recursive: true,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata[0].rules.len(), 2);
        }

        let entries_a = registry.entries_for_rule("rule_a");
        let entries_b = registry.entries_for_rule("rule_b");
        assert_eq!(entries_a.len(), 1);
        assert_eq!(entries_b.len(), 1);
    }

    #[test]
    fn test_program_registry_arc_sharing() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "shared",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let user_rule = make_rule(
            "user",
            vec![
                CompiledClause {
                    match_pattern: node_pattern("x"),
                    where_conditions: vec![RuleCondition::IsReference(IsReference {
                        subjects: vec!["x".to_string()],
                        rule_name: qname("shared"),
                        target: None,
                        negated: false,
                    })],
                    along: vec![],
                    fold: vec![],
                    require: vec![],
                    having: vec![],
                    best_by: None,
                    output: simple_yield_output(&["x"]),
                    priority: None,
                    model_invocations: vec![],
                    hidden_yield_cols: vec![],
                    binding: uni_locy::ClauseBinding::Pending,
                },
                CompiledClause {
                    match_pattern: node_pattern("y"),
                    where_conditions: vec![RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("shared"),
                        target: None,
                        negated: false,
                    })],
                    along: vec![],
                    fold: vec![],
                    require: vec![],
                    having: vec![],
                    best_by: None,
                    output: RuleOutput::Yield(YieldClause {
                        items: vec![LocyYieldItem {
                            is_key: false,
                            is_prob: false,
                            expr: Expr::Variable("y".to_string()),
                            alias: Some("x".to_string()),
                        }],
                    }),
                    priority: None,
                    model_invocations: vec![],
                    hidden_yield_cols: vec![],
                    binding: uni_locy::ClauseBinding::Pending,
                },
            ],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("shared".to_string(), target_rule.clone()),
            ("user".to_string(), user_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![target_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![user_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        // Both clauses should share the same Arc<RwLock> via the registry
        let entries = registry.entries_for_rule("shared");
        assert_eq!(entries.len(), 1);

        // Verify both DerivedScan nodes in the plan share the same data handle
        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            let user_strat = &strata[1];
            let clauses = &user_strat.rules[0].clauses;
            assert_eq!(clauses.len(), 2);

            fn extract_derived_scan_data(
                plan: &LogicalPlan,
            ) -> Option<Arc<RwLock<Vec<RecordBatch>>>> {
                match plan {
                    LogicalPlan::LocyDerivedScan { data, .. } => Some(data.clone()),
                    LogicalPlan::CrossJoin { left, right } => {
                        extract_derived_scan_data(left).or_else(|| extract_derived_scan_data(right))
                    }
                    LogicalPlan::Filter { input, .. }
                    | LogicalPlan::Project { input, .. }
                    | LogicalPlan::LocyProject { input, .. } => extract_derived_scan_data(input),
                    _ => None,
                }
            }

            let data1 = extract_derived_scan_data(&clauses[0].body)
                .expect("clause 0 should have DerivedScan");
            let data2 = extract_derived_scan_data(&clauses[1].body)
                .expect("clause 1 should have DerivedScan");
            assert!(Arc::ptr_eq(&data1, &data2));
        }
    }

    #[test]
    fn test_program_empty_strata() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let program = make_program(vec![], HashMap::new());

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram {
            strata, commands, ..
        } = &plan
        {
            assert!(strata.is_empty());
            assert!(commands.is_empty());
        } else {
            panic!("Expected LocyProgram");
        }

        assert!(registry.get(0).is_none());
    }

    #[test]
    fn test_program_complex_3_strata() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let intermediate = make_rule(
            "intermediate",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                require: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
                binding: uni_locy::ClauseBinding::Pending,
            }],
            vec![yield_col("x", true)],
        );
        let recursive = make_rule(
            "recursive",
            vec![CompiledClause {
                match_pattern: node_pattern("y"),
                where_conditions: vec![
                    RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("intermediate"),
                        target: None,
                        negated: false,
                    }),
                    RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("recursive"),
                        target: None,
                        negated: false,
                    }),
                ],
                along: vec![],
                fold: vec![],
                require: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["y"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
                binding: uni_locy::ClauseBinding::Pending,
            }],
            vec![yield_col("y", true)],
        );

        let catalog = HashMap::from([
            ("base".to_string(), base.clone()),
            ("intermediate".to_string(), intermediate.clone()),
            ("recursive".to_string(), recursive.clone()),
        ]);

        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![intermediate],
                    is_recursive: false,
                    depends_on: vec![0],
                },
                Stratum {
                    id: 2,
                    rules: vec![recursive],
                    is_recursive: true,
                    depends_on: vec![1],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata.len(), 3);
            assert!(!strata[0].is_recursive);
            assert!(!strata[1].is_recursive);
            assert!(strata[2].is_recursive);
            assert_eq!(strata[2].depends_on, vec![1]);
        }

        // "intermediate" is cross-stratum ref (from stratum 2, target in stratum 1)
        let inter_entries = registry.entries_for_rule("intermediate");
        assert_eq!(inter_entries.len(), 1);
        assert!(!inter_entries[0].is_self_ref);

        // "recursive" is self-ref (within stratum 2)
        let rec_entries = registry.entries_for_rule("recursive");
        assert_eq!(rec_entries.len(), 1);
        assert!(rec_entries[0].is_self_ref);
    }

    // ===================================================================
    // Integration Tests — Validation Errors
    // ===================================================================

    #[test]
    fn test_validation_is_ref_unknown_rule() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("nonexistent"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("x", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder.build_clause(
            &planned(&clause, &catalog, &names),
            &yield_cols,
            false,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
                deriv_vars: &[],
                seed_types: &HashMap::new(),
                fold_layout: &[],
                is_recursive: false,
                clause_index: 0,
                prob_slots: &[],
                lineage_prob_column: None,
                transient_lineage: false,
                support_slots: &[],
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"), "Error: {}", err);
    }

    #[test]
    fn test_validation_is_ref_arity_mismatch() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "target",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("target".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string(), "y".to_string()],
                rule_name: qname("target"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder.build_clause(
            &planned(&clause, &catalog, &names),
            &yield_cols,
            false,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
                deriv_vars: &[],
                seed_types: &HashMap::new(),
                fold_layout: &[],
                is_recursive: false,
                clause_index: 0,
                prob_slots: &[],
                lineage_prob_column: None,
                transient_lineage: false,
                support_slots: &[],
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("arity mismatch"), "Error: {}", err);
    }

    #[test]
    fn test_validation_along_prev_field_not_in_schema() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::PrevRef("nonexistent".to_string()),
            }],
            fold: vec![],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x", "cost"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("x", true), yield_col("cost", false)];
        let names = HashSet::new();

        let result = builder.build_clause(
            &planned(&clause, &catalog, &names),
            &yield_cols,
            false,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
                deriv_vars: &[],
                seed_types: &HashMap::new(),
                fold_layout: &[],
                is_recursive: false,
                clause_index: 0,
                prob_slots: &[],
                lineage_prob_column: None,
                transient_lineage: false,
                support_slots: &[],
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"), "Error: {}", err);
    }

    #[test]
    fn test_validation_fold_non_monotonic_rejected_in_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder.build_clause(
            &planned(&clause, &catalog, &names),
            &yield_cols,
            true,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
                deriv_vars: &[],
                seed_types: &HashMap::new(),
                fold_layout: &[],
                is_recursive: false,
                clause_index: 0,
                prob_slots: &[],
                lineage_prob_column: None,
                transient_lineage: false,
                support_slots: &[],
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_err(), "SUM in a recursive stratum must reject");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("non-monotonic aggregate 'SUM'"),
            "error must name the aggregate; got: {err}"
        );
    }

    #[test]
    fn test_validation_fold_monotonic_accepted_in_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        for name in ["MMAX", "MMIN", "MNOR", "MPROD", "MSUM"] {
            let clause = CompiledClause {
                match_pattern: node_pattern("n"),
                where_conditions: vec![],
                along: vec![],
                fold: vec![FoldBinding {
                    name: "score".to_string(),
                    aggregate: Expr::FunctionCall {
                        name: name.to_string(),
                        args: vec![Expr::Property(
                            Box::new(Expr::Variable("n".to_string())),
                            "cost".to_string(),
                        )],
                        distinct: false,
                        window_spec: None,
                    },
                }],
                require: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["n", "score"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
                binding: uni_locy::ClauseBinding::Pending,
            };
            let yield_cols = [yield_col("n", true), yield_col("score", false)];
            let catalog = HashMap::new();
            let names = HashSet::new();

            let result = builder.build_clause(
                &planned(&clause, &catalog, &names),
                &yield_cols,
                true,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                    deriv_vars: &[],
                    seed_types: &HashMap::new(),
                    fold_layout: &[],
                    is_recursive: false,
                    clause_index: 0,
                    prob_slots: &[],
                    lineage_prob_column: None,
                    transient_lineage: false,
                    support_slots: &[],
                },
                &test_classifier_ctx(),
            );
            assert!(
                result.is_ok(),
                "{name} should be accepted in recursive stratum; got: {:?}",
                result.err()
            );
        }
    }

    #[test]
    fn test_validation_fold_non_monotonic_accepted_in_non_recursive_stratum() {
        // SUM is fine in a non-recursive rule — the check only triggers for
        // is_recursive=true.
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Property(
                        Box::new(Expr::Variable("n".to_string())),
                        "cost".to_string(),
                    )],
                    distinct: false,
                    window_spec: None,
                },
            }],
            require: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
            binding: uni_locy::ClauseBinding::Pending,
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder.build_clause(
            &planned(&clause, &catalog, &names),
            &yield_cols,
            false,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
                deriv_vars: &[],
                seed_types: &HashMap::new(),
                fold_layout: &[],
                is_recursive: false,
                clause_index: 0,
                prob_slots: &[],
                lineage_prob_column: None,
                transient_lineage: false,
                support_slots: &[],
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_ok(), "SUM in non-recursive stratum is valid");
    }

    // ===================================================================
    // Structural guard (`check_sources`, issue #300)
    // ===================================================================

    const N_RULES: &str = "\
        CREATE RULE r1 AS MATCH (a:N) YIELD KEY a, a.d AS score\n\
        CREATE RULE r2 AS MATCH (a:N) YIELD KEY a, 1 AS w\n\
        CREATE RULE link AS MATCH (a:N)-[:R]->(b:N) YIELD KEY a, KEY b\n\
        CREATE RULE blocked AS MATCH (n:N) WHERE n.bad = true YIELD KEY n\n\
        CREATE RULE tag AS MATCH (n:N)-[:HAS]->(t:T) YIELD KEY n, KEY t.k AS c\n\
        CREATE RULE er AS MATCH (a:N)-[e:R]->(b:N) YIELD KEY e, e.w AS ew\n";

    /// Compiles `src` and plans it with an empty graph schema.
    fn plan_source(src: &str) -> Result<(CompiledProgram, LogicalPlan)> {
        let ast = uni_cypher::parse_locy(src).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        let program = uni_locy::compile(&ast)?;
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let plan = builder.build_program_plan(
            &program,
            1000,
            std::time::Duration::from_secs(30),
            256 * 1024 * 1024,
            true,
            false,
            1e-15,
            false,
            1000,
            0,
        )?;
        Ok((program, plan))
    }

    /// Runs [`check_sources`] over every planned clause of `src`, each against
    /// its compiled clause's binding plan; returns how many it checked.
    fn guard_every_clause(src: &str) -> usize {
        let (program, plan) = plan_source(src).unwrap_or_else(|e| panic!("{e:#}\n{src}"));
        let LogicalPlan::LocyProgram { strata, .. } = &plan else {
            panic!("expected LocyProgram");
        };
        let mut checked = 0;
        for rule in strata.iter().flat_map(|s| &s.rules) {
            let compiled = &program.rule_catalog[&rule.name];
            for (index, (clause, compiled_clause)) in
                rule.clauses.iter().zip(&compiled.clauses).enumerate()
            {
                let binding = compiled_clause.binding.plan().expect("binding plan");
                let pattern: HashSet<String> = binding
                    .scope
                    .iter()
                    .filter(|(_, source)| matches!(source, VarSource::Match(_)))
                    .map(|(name, _)| name.clone())
                    .chain(
                        match_edge_vars(compiled_clause, index)
                            .into_iter()
                            .map(|v| v.name),
                    )
                    .collect();
                check_sources(&pattern, &clause.body)
                    .unwrap_or_else(|e| panic!("{}: {e:#}\n{src}", rule.name));
                checked += 1;
            }
        }
        checked
    }

    /// Each Step 3 shape of `build_clause`, in every conjunct order where the
    /// order matters, plans without a single unproduced read.
    #[test]
    fn guard_accepts_every_step3_shape() {
        let shapes: &[(&str, &[&str])] = &[
            // A reference anchored on a MATCH node: a node test.
            ("anchored test", &["a IS r1"]),
            // A unique exposure read by a filter placed after its reference.
            ("filter on an exposure", &["a IS r1", "score > 5"]),
            // A node the reference binds: `ScanAll` joined on its vid.
            ("bind node", &["a IS link TO b", "b.x = 1"]),
            // Two references: the second tests the first's node.
            ("two references", &["a IS link TO b", "b IS r1"]),
            // A scalar KEY bound, then tested by a second reference.
            ("bind scalar", &["a IS tag TO col", "a IS tag TO col"]),
            // A value position tested between two references.
            ("test scalar", &["(a, s) IS r1", "(a, s) IS r2"]),
            // A generator over a MATCH property.
            ("generator", &["f(a.k) -> (x)", "x > 1"]),
            // A reference beats a generator as binder: renamed output + test.
            ("generator tested", &["f(a.k) -> (col)", "a IS tag TO col"]),
            // A negation before and after its binder.
            ("negation", &["NOT b IS blocked", "a IS link TO b"]),
        ];
        for (name, conditions) in shapes {
            let n = conditions.len();
            let mut orders = vec![(0..n).collect::<Vec<_>>()];
            if n == 2 {
                orders.push(vec![1, 0]);
            }
            for order in orders {
                let body: Vec<&str> = order.iter().map(|&i| conditions[i]).collect();
                let yield_items = match *name {
                    "bind node" | "two references" | "negation" => "KEY a, KEY b",
                    "filter on an exposure" => "KEY a, score",
                    "bind scalar" | "generator tested" => "KEY a, col",
                    "test scalar" => "KEY a, s",
                    "generator" => "KEY a, x",
                    _ => "KEY a",
                };
                let src = format!(
                    "{N_RULES}CREATE RULE q AS MATCH (a:N) WHERE {} YIELD {yield_items}\n",
                    body.join(", ")
                );
                assert!(guard_every_clause(&src) >= 7, "{name}");
            }
        }
    }

    /// The shapes outside the WHERE: a relationship tested against an edge
    /// KEY, ALONG `prev`, a FOLD input over an exposure, BEST BY over a
    /// referenced value column (a hidden `__bestby_` column), and #300's P1.
    #[test]
    fn guard_accepts_projection_shapes() {
        let sources = [
            format!(
                "{N_RULES}CREATE RULE q AS MATCH (a:N)-[e:R]->(b:N) WHERE e IS er YIELD KEY a, ew\n"
            ),
            "CREATE RULE path AS MATCH (a:N)-[e:E]->(b:N) ALONG cost = e.w \
             YIELD KEY a, KEY b, cost\n\
             CREATE RULE path AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS path TO b \
             ALONG cost = prev.cost + e.w YIELD KEY a, KEY b, cost\n"
                .to_string(),
            format!(
                "{N_RULES}CREATE RULE q AS MATCH (a:N) WHERE a IS r1 FOLD t = SUM(score) \
                 YIELD KEY a, t\n"
            ),
            format!(
                "{N_RULES}CREATE RULE q AS MATCH (a:N) WHERE a IS r1 BEST BY score DESC \
                 YIELD KEY a\n"
            ),
            "CREATE RULE stake AS MATCH (o:E)-[r:OWNS]->(e:E) FOLD pct = MAX(r.pct) \
             YIELD KEY o, KEY e, pct\n\
             CREATE RULE seed AS MATCH (e:E) WHERE e.uid = 'x' YIELD KEY e, 100.0 AS agg\n\
             CREATE RULE p1 AS MATCH (o:E) WHERE o IS seed AND (o, e, pct) IS stake \
             YIELD KEY e, pct\n\
             CREATE RULE p1r AS MATCH (o:E) WHERE (o, e, pct) IS stake AND o IS seed \
             YIELD KEY e, pct\n"
                .to_string(),
        ];
        for src in &sources {
            assert!(guard_every_clause(src) > 0, "{src}");
        }
    }

    /// Teeth: a bare `Expr::Variable` naming a referenced column the plan
    /// only produces under its prefix is refused, naming it.
    #[test]
    fn guard_rejects_a_bare_variable() {
        let plan = HashSet::from(["o".to_string()]);
        let joined = |predicate: Expr| LogicalPlan::Filter {
            input: Box::new(LogicalPlan::CrossJoin {
                left: Box::new(LogicalPlan::ScanAll {
                    variable: "o".to_string(),
                    filter: None,
                    optional: false,
                }),
                right: Box::new(LogicalPlan::LocyDerivedScan {
                    scan_index: 0,
                    data: Arc::new(RwLock::new(Vec::new())),
                    schema: Arc::new(ArrowSchema::new(vec![
                        Field::new("__isref0_e", DataType::UInt64, true),
                        Field::new("__isref0_pct", DataType::Float64, true),
                    ])),
                }),
            }),
            predicate,
            optional_variables: HashSet::new(),
        };
        let gt = |name: &str| Expr::BinaryOp {
            left: Box::new(Expr::Variable(name.to_string())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(CypherLiteral::Float(1.0))),
        };
        let error = check_sources(&plan, &joined(gt("pct")))
            .expect_err("a bare `pct` is not a produced column");
        assert!(error.to_string().contains("`pct`"), "{error:#}");
        // The prefixed column and an entity expansion are produced.
        check_sources(&plan, &joined(gt("__isref0_pct"))).expect("prefixed column");
        check_sources(&plan, &joined(gt("o._vid"))).expect("entity expansion");
        // A projection over a name no input produces is refused too.
        let projected = LogicalPlan::LocyProject {
            input: Box::new(joined(gt("__isref0_pct"))),
            projections: vec![(Expr::Variable("e".to_string()), Some("e".to_string()))],
            target_types: vec![],
        };
        let error = check_sources(&plan, &projected).expect_err("a bare `e`");
        assert!(error.to_string().contains("`e`"), "{error:#}");
    }

    /// Teeth through the real planner: a binding plan whose exposure points
    /// at the bare column name — the pre-#300 first-reference layout — makes
    /// `build_clause` emit an unprefixed read, which the guard refuses.
    #[test]
    fn guard_rejects_a_planner_emitting_an_unprefixed_read() {
        let src =
            format!("{N_RULES}CREATE RULE q AS MATCH (a:N) WHERE a IS r1, score > 5 YIELD KEY a\n");
        let ast = uni_cypher::parse_locy(&src).expect("parses");
        let mut program = uni_locy::compile(&ast).expect("compiles");
        let mut tamper = |rule: &mut CompiledRule| {
            if rule.name != "q" {
                return;
            }
            let clause = &mut rule.clauses[0];
            let mut plan = clause.binding.plan().expect("plan").clone();
            let Some(Exposure::Unique { physical, .. }) = plan.exposure.get_mut("score") else {
                panic!("score is a unique exposure");
            };
            *physical = "score".to_string();
            clause.binding = uni_locy::ClauseBinding::Ready(Arc::new(plan));
        };
        program.rule_catalog.values_mut().for_each(&mut tamper);
        program
            .strata
            .iter_mut()
            .flat_map(|s| &mut s.rules)
            .for_each(&mut tamper);
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let error = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .expect_err("the guard refuses the bare read");
        let message = format!("{error:#}");
        assert!(
            message.contains("`score`") && message.contains("no input column"),
            "{message}"
        );
    }
}
