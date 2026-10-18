// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The one binding decision for each clause: which occurrence of a variable
//! binds it, and which ones test it.
//!
//! A clause body introduces variables in three places: the MATCH pattern, the
//! positions of positive IS references, and generator outputs. Before this
//! module four components decided on their own whether a name was bound at a
//! given point: the compiler's scope check (an unordered set), the type checker
//! (left to right), the planner (left to right, with KEY positions always
//! tests), and EXPLAIN (anything binds). So a program meant different things
//! on different paths, and swapping two WHERE conjuncts changed the answer
//! (issue #300).
//!
//! A [`ClauseBindingPlan`] makes that decision once, independently of the
//! textual order of the WHERE conditions:
//!
//! * MATCH variables are bound first.
//! * Positive IS references are scheduled so that a reference with bound KEY
//!   positions (a semi-join on the KEY) goes before generators, and a
//!   reference with no bound KEY goes after the generators that are ready.
//! * The first scheduled occurrence of a variable binds it; every other
//!   occurrence tests it. A reference beats a generator as the binder of a
//!   name both produce.
//! * Each filter goes right after the step that makes its last name
//!   available; filters over MATCH variables only run immediately.
//! * `IS NOT` references run last, as anti-joins.
//! * At most one positive reference testing a MATCH node on a KEY is the
//!   clause's [`Anchor`]: the planner reads it before the pattern and walks
//!   the pattern from the nodes it names, from whichever end of the path the
//!   node is on. A reference into the clause's own recursive group is
//!   preferred, so a fixpoint iteration costs the referenced delta rather
//!   than the whole relation (issue #301); failing that, a reference to a
//!   lower stratum anchors, so a clause over a few lower-stratum facts does
//!   not scan and walk the whole label (P4 / A2).
//!
//! The plan also records, per IS-reference position, the [`ColumnKind`] the
//! referenced column holds — a node, a relationship or a scalar — inferred
//! from the referenced rule's YIELD expression by [`ColumnKinds::infer`]
//! rather than from whether the column is a KEY.
//!
//! Problems the plan finds are collected in
//! [`ClauseBindingPlan::diagnostics`]; the compiler attaches the plan to every
//! [`CompiledClause`] whose referenced rules it knows, through
//! [`attach_binding_plans`] and the type checker, and raises the first
//! diagnostic as the compile error. A clause that references a rule the
//! compiler does not have stays [`ClauseBinding::Pending`] until
//! [`attach_binding_plans`] runs against a catalog that has it.
//!
//! The plan is the single source of truth for binding. Its consumers:
//!
//! * the compiler's scope check (`compiler::scope`), whose rule-body scope is
//!   the plan's [`ClauseBindingPlan::scope`] plus its unique exposures, and
//!   which alone reports an unbound name;
//! * the `IS NOT` checks — subject kind, value comparison, KEY safety — which
//!   are this plan's diagnostics;
//! * the Locy planner, EXPLAIN (`locy_delta`), the anti-join, the feature
//!   resolvers and the YIELD type inference in `uni-query`.

// Rust guideline compliant

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use uni_cypher::ast::{Expr, PathPattern, Pattern, PatternElement};
use uni_cypher::locy_ast::{LocyExpr, LocyYieldItem, RuleCondition, RuleOutput};

use crate::compiler::errors::LocyCompileError;
use crate::compiler::free_variables;
use crate::compiler::typecheck::collect_prev_refs;
use crate::is_binding::{binding_sequence, bound_columns};
use crate::types::{CompiledClause, CompiledProgram, CompiledRule};

/// The kind of value a variable or a rule column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnKind {
    /// A node, identified by its vid.
    Node,
    /// A relationship, identified by its eid.
    Edge,
    /// Any other value: a property, a literal, an aggregate, a list.
    Scalar,
}

impl std::fmt::Display for ColumnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.describe())
    }
}

impl ColumnKind {
    /// The kind's name as diagnostics print it.
    #[must_use]
    pub fn describe(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "relationship",
            Self::Scalar => "scalar",
        }
    }
}

/// What a clause does with the variable at one IS-reference position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionAction {
    /// The first occurrence of a node variable: it is bound to the column.
    BindNode,
    /// The first occurrence of a relationship or scalar variable.
    BindScalar,
    /// A node variable bound elsewhere: the column must equal it.
    TestNode,
    /// A relationship or scalar variable bound elsewhere.
    TestScalar,
    /// A value position of an `IS NOT` reference whose variable is bound
    /// nowhere: existentially quantified and ignored.
    Existential,
}

/// One variable of an IS reference and the column it takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionBinding {
    /// The variable written at this position.
    pub variable: String,
    /// The referenced rule's column, per [`binding_sequence`].
    pub column: String,
    /// The column's name in the joined batch: the reference's prefix plus
    /// [`Self::column`].
    pub physical: String,
    /// Whether the column is one of the referenced rule's KEY columns.
    pub is_key: bool,
    /// What the column holds.
    pub kind: ColumnKind,
    /// Whether this occurrence binds or tests the variable.
    pub action: PositionAction,
}

/// The binding of one IS reference of a clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsRefBinding {
    /// Index of the reference in the clause's `where_conditions`.
    pub condition_index: usize,
    /// The referenced rule, as the rule catalog keys it.
    pub rule: String,
    /// Whether this is an `IS NOT` reference.
    pub negated: bool,
    /// Prefix of every column the reference joins into the batch.
    ///
    /// `__isref{k}_` for the `k`-th positive reference in textual order,
    /// the first included, so no referenced column ever enters the batch
    /// under a bare name another name could capture. `__isnot{k}_` for the
    /// `k`-th negated reference.
    pub prefix: String,
    /// The subjects, then the `TO` target, in order.
    pub positions: Vec<PositionBinding>,
}

/// One output variable of a generator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratorOutput {
    /// The output variable.
    pub variable: String,
    /// The output's column in the batch: the variable itself when the
    /// generator binds it, else `__gen{i}_{variable}`, tested for equality
    /// with the variable's binder.
    pub physical: String,
    /// Whether this generator is the variable's binder.
    pub binds: bool,
}

/// The binding of one generator condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratorBinding {
    /// Index of the generator in the clause's `where_conditions`.
    pub condition_index: usize,
    /// Its outputs in declaration order.
    pub outputs: Vec<GeneratorOutput>,
}

/// One step of a clause body's evaluation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingStep {
    /// Join the positive IS reference at this index of
    /// [`ClauseBindingPlan::refs`].
    Ref(usize),
    /// Run the generator at this `where_conditions` index.
    Generator(usize),
    /// Apply the filter at this `where_conditions` index.
    Filter(usize),
    /// Anti-join the `IS NOT` reference at this index of
    /// [`ClauseBindingPlan::refs`].
    AntiJoin(usize),
}

/// Where a clause variable gets its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VarSource {
    /// Bound by the MATCH pattern.
    Match(ColumnKind),
    /// Bound by position `pos` of reference `r` of
    /// [`ClauseBindingPlan::refs`].
    RefPosition {
        /// Index into [`ClauseBindingPlan::refs`].
        r: usize,
        /// Index into that reference's positions.
        pos: usize,
    },
    /// Bound by the generator at `where_conditions` index `cond`.
    Generator {
        /// Index of the generator in `where_conditions`.
        cond: usize,
        /// The output's column in the batch.
        physical: String,
    },
    /// A YIELD alias of the named node variable, used as an `IS NOT` subject.
    YieldAlias(String),
}

/// How a bare name reaches a referenced rule's value column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exposure {
    /// Exactly one reference exposes the column.
    Unique {
        /// Index into [`ClauseBindingPlan::refs`].
        r: usize,
        /// The column's name in the batch.
        physical: String,
    },
    /// Several references expose a column of this name; reading it is an
    /// error.
    Ambiguous(Vec<usize>),
}

/// The positive reference a clause's MATCH pattern is walked from.
///
/// The MATCH pattern is otherwise planned on its own, from a scan of its first
/// (or best-anchored) node, and every positive reference is joined to the
/// result afterwards. For a reference into the clause's own recursive group
/// that walks the whole relation on every fixpoint iteration, however small the
/// referenced delta is (issue #301). An anchored clause reads the reference
/// first and starts the walk at the nodes it names.
///
/// Chosen by [`ClauseBindingPlan::compute`]; see [`ClauseBindingPlan::anchor`]
/// for the rule.
///
/// Anchoring changes the plan's shape, never its binding: the anchoring
/// position stays a [`PositionAction::TestNode`] of a MATCH variable, and every
/// other position keeps the action the schedule gave it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Anchor {
    /// Index into [`ClauseBindingPlan::refs`] of the anchoring reference.
    pub r: usize,
    /// Index into that reference's positions of the KEY position whose MATCH
    /// node the walk starts from.
    pub pos: usize,
    /// Which kind of reference anchors, which decides how the walk is seeded.
    pub source: AnchorSource,
}

/// The kind of reference an [`Anchor`] is, and so how the planner seeds the walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorSource {
    /// A reference into the clause's own recursive group (issue #301).
    ///
    /// Its rows are the fixpoint's delta. The walk is seeded by the
    /// reference's rows themselves, and the anchoring position's test is the
    /// join that seeds it, so the reference is read once.
    Recursive,
    /// A reference to a rule of a lower stratum, whose facts are complete
    /// before this clause runs.
    ///
    /// The walk is seeded by the *distinct* nodes of the anchoring column,
    /// and the reference is still joined in full at its own step, every test
    /// included. The seed is a semi-join: it removes pattern rows the full
    /// join would reject anyway, and never repeats a walk for two facts that
    /// share the anchored node.
    LowerStratum,
}

/// The order-independent binding decision for one clause.
///
/// See the module documentation for the algorithm.
#[derive(Debug, Clone, PartialEq)]
pub struct ClauseBindingPlan {
    /// The evaluation order, after the MATCH pattern and
    /// [`Self::immediate_filters`].
    pub steps: Vec<BindingStep>,
    /// Every IS reference, positive and negated, in textual order.
    pub refs: Vec<IsRefBinding>,
    /// Every generator, in textual order.
    pub generators: Vec<GeneratorBinding>,
    /// The binder of every variable of the clause body.
    pub scope: BTreeMap<String, VarSource>,
    /// The value columns readable by bare name: positive references' value
    /// columns that no subject or target binds, and that no real variable
    /// shadows. KEY columns are never exposed.
    pub exposure: BTreeMap<String, Exposure>,
    /// `where_conditions` indices of the filters that read MATCH variables
    /// only.
    pub immediate_filters: Vec<usize>,
    /// The positive reference ALONG's `prev` reads: the one reference to a
    /// rule of the clause's own recursive group.
    pub prev_ref: Option<usize>,
    /// The reference the MATCH pattern is walked from, if any.
    ///
    /// A positive reference qualifies when one of its KEY positions tests a
    /// node the MATCH pattern binds as a plain node element (not inside a
    /// quantified group, nor in a shortestPath or a path bound to a variable).
    /// The node may be at either end of its path, or in the middle: the
    /// pattern planner walks a path from whichever of its nodes is bound,
    /// reversing it or splitting it there. The planner reads the reference
    /// before the pattern and walks the pattern from the referenced nodes. The
    /// reference's other positions keep their actions.
    ///
    /// * A reference into the clause's own recursive group is preferred
    ///   ([`AnchorSource::Recursive`]): it is what a fixpoint re-reads every
    ///   iteration, and semi-naive evaluation keeps it small, so an iteration
    ///   costs the referenced delta rather than the whole relation (issue
    ///   #301).
    /// * Failing one, a reference to a lower stratum anchors
    ///   ([`AnchorSource::LowerStratum`]), unless another node of the pattern
    ///   carries an equality — an inline property map or a WHERE conjunct
    ///   `v.p = <constant>` — which the pattern planner would start the walk
    ///   from instead (`reversed_for_bound_anchor`'s rank). A seed is a set of
    ///   vids the anchor node is looked up by, so it composes with an
    ///   equality on the anchored node itself, but not with a better start
    ///   elsewhere. Whether the lookup reads the label by vid or in one pass
    ///   is decided at run time by the fact count against the label's row
    ///   count (`VidLookupJoinExec`), where both are known.
    ///
    /// Among several candidates of one kind, the one with the smallest
    /// `(rule, subject variables)` wins — deterministic, and independent of
    /// the order of the WHERE conditions.
    pub anchor: Option<Anchor>,
    /// What the plan found wrong with the clause.
    pub diagnostics: Vec<LocyCompileError>,
}

/// A clause's binding plan, once the compiler can compute it.
#[derive(Debug, Clone, Default)]
pub enum ClauseBinding {
    /// Not computed: the clause references a rule whose schema the compiler
    /// does not have, or it was built by hand.
    #[default]
    Pending,
    /// The computed plan.
    Ready(Arc<ClauseBindingPlan>),
}

impl ClauseBinding {
    /// The plan, when it is computed.
    #[must_use]
    pub fn plan(&self) -> Option<&ClauseBindingPlan> {
        match self {
            Self::Pending => None,
            Self::Ready(plan) => Some(plan),
        }
    }
}

/// Meet of two kinds, where `None` is the unknown top element.
fn meet(a: Option<ColumnKind>, b: Option<ColumnKind>) -> Option<ColumnKind> {
    match (a, b) {
        (None, other) | (other, None) => other,
        (Some(x), Some(y)) if x == y => Some(x),
        _ => Some(ColumnKind::Scalar),
    }
}

/// The kind of every column of every rule in a catalog.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnKinds {
    rules: HashMap<String, HashMap<String, ColumnKind>>,
}

impl ColumnKinds {
    /// Infers each column's kind from the rules' YIELD expressions.
    ///
    /// A FOLD or ALONG name is a scalar. A bare variable has its binder's
    /// kind: a MATCH node is a node, a single-hop MATCH relationship is a
    /// relationship, a variable-length relationship, a path or a quantified
    /// group variable is a scalar (a list), an IS-reference position has the
    /// kind of the column it takes, and a generator output is a scalar. Any
    /// other expression is a scalar. KEY columns follow the same rule: a KEY
    /// is not a node because it is a KEY.
    ///
    /// A column is a node only if every clause of its rule yields a node
    /// there, and likewise for relationships. Recursion is resolved as a
    /// greatest fixpoint: every column starts unknown and only ever demotes,
    /// and a column still unknown at the end (reached only through a cycle)
    /// is a node.
    #[must_use]
    pub fn infer(catalog: &HashMap<String, CompiledRule>) -> Self {
        let mut current: HashMap<String, HashMap<String, Option<ColumnKind>>> = catalog
            .iter()
            .map(|(key, rule)| {
                let cols = rule
                    .yield_schema
                    .iter()
                    .map(|c| (c.name.clone(), None))
                    .collect();
                (key.clone(), cols)
            })
            .collect();
        loop {
            let lookup = |rule: &str, position: usize| -> Option<ColumnKind> {
                let target = catalog.get(rule)?;
                let column = binding_sequence(&target.yield_schema)
                    .into_iter()
                    .nth(position)?;
                current.get(rule)?.get(&column.name).copied().flatten()
            };
            let mut next = HashMap::with_capacity(current.len());
            for (key, rule) in catalog {
                let mut cols: HashMap<String, Option<ColumnKind>> = rule
                    .yield_schema
                    .iter()
                    .map(|c| (c.name.clone(), None))
                    .collect();
                for clause in &rule.clauses {
                    let RuleOutput::Yield(yc) = &clause.output else {
                        continue;
                    };
                    let var_kinds = clause_var_kinds(clause, &lookup);
                    let names = uni_cypher::locy_ast::resolve_yield_column_names(&yc.items);
                    for (item, name) in yc.items.iter().zip(&names) {
                        if let Some(slot) = cols.get_mut(name) {
                            *slot = meet(*slot, yield_item_kind(clause, item, &var_kinds));
                        }
                    }
                }
                next.insert(key.clone(), cols);
            }
            if next == current {
                break;
            }
            current = next;
        }
        let rules = current
            .into_iter()
            .map(|(key, cols)| {
                let cols = cols
                    .into_iter()
                    .map(|(name, kind)| (name, kind.unwrap_or(ColumnKind::Node)))
                    .collect();
                (key, cols)
            })
            .collect();
        Self { rules }
    }

    /// The kind of `column` of `rule`, when both are known.
    #[must_use]
    pub fn kind(&self, rule: &str, column: &str) -> Option<ColumnKind> {
        self.rules.get(rule)?.get(column).copied()
    }

    /// The columns of `rule` of kind `kind`, in no particular order.
    pub fn columns_of_kind(&self, rule: &str, kind: ColumnKind) -> impl Iterator<Item = &str> {
        self.rules
            .get(rule)
            .into_iter()
            .flatten()
            .filter(move |(_, k)| **k == kind)
            .map(|(name, _)| name.as_str())
    }
}

/// The kind of the column a YIELD item produces, `None` when still unknown.
fn yield_item_kind(
    clause: &CompiledClause,
    item: &LocyYieldItem,
    var_kinds: &HashMap<String, Option<ColumnKind>>,
) -> Option<ColumnKind> {
    let Expr::Variable(v) = &item.expr else {
        return Some(ColumnKind::Scalar);
    };
    if clause.fold.iter().any(|f| &f.name == v) || clause.along.iter().any(|a| &a.name == v) {
        return Some(ColumnKind::Scalar);
    }
    var_kinds
        .get(v)
        .copied()
        .unwrap_or(Some(ColumnKind::Scalar))
}

/// The kind of every variable a clause body binds, `None` when unknown.
///
/// `position_kind` answers the kind of a referenced rule's column at an IS
/// position, `None` when unknown.
fn clause_var_kinds(
    clause: &CompiledClause,
    position_kind: &dyn Fn(&str, usize) -> Option<ColumnKind>,
) -> HashMap<String, Option<ColumnKind>> {
    let mut kinds: HashMap<String, Option<ColumnKind>> = match_var_kinds(&clause.match_pattern)
        .into_iter()
        .map(|(v, k)| (v, Some(k)))
        .collect();
    let match_vars: HashSet<String> = kinds.keys().cloned().collect();
    let mut generated = Vec::new();
    for condition in &clause.where_conditions {
        match condition {
            RuleCondition::IsReference(is_ref) if !is_ref.negated => {
                let rule = is_ref.rule_name.to_string();
                for (position, var) in is_ref.subjects.iter().chain(&is_ref.target).enumerate() {
                    if match_vars.contains(var) {
                        continue;
                    }
                    let kind = position_kind(&rule, position);
                    kinds
                        .entry(var.clone())
                        .and_modify(|k| *k = meet(*k, kind))
                        .or_insert(kind);
                }
            }
            RuleCondition::Generator(generator) => generated.extend(&generator.outputs),
            _ => {}
        }
    }
    for output in generated {
        kinds
            .entry(output.clone())
            .or_insert(Some(ColumnKind::Scalar));
    }
    kinds
}

/// The kind of every variable a MATCH pattern binds.
fn match_var_kinds(pattern: &Pattern) -> HashMap<String, ColumnKind> {
    fn add(kinds: &mut HashMap<String, ColumnKind>, var: &str, kind: ColumnKind) {
        kinds
            .entry(var.to_string())
            .and_modify(|k| {
                *k = meet(Some(*k), Some(kind)).unwrap_or(ColumnKind::Scalar);
            })
            .or_insert(kind);
    }
    fn walk(path: &PathPattern, quantified: bool, kinds: &mut HashMap<String, ColumnKind>) {
        if let Some(v) = &path.variable {
            add(kinds, v, ColumnKind::Scalar);
        }
        for element in &path.elements {
            match element {
                PatternElement::Node(n) => {
                    if let Some(v) = &n.variable {
                        let kind = if quantified {
                            ColumnKind::Scalar
                        } else {
                            ColumnKind::Node
                        };
                        add(kinds, v, kind);
                    }
                }
                PatternElement::Relationship(r) => {
                    if let Some(v) = &r.variable {
                        let kind = if quantified || r.range.is_some() {
                            ColumnKind::Scalar
                        } else {
                            ColumnKind::Edge
                        };
                        add(kinds, v, kind);
                    }
                }
                // A quantified group binds each inner variable to a list.
                PatternElement::Parenthesized { pattern, range } => {
                    walk(pattern, quantified || range.is_some(), kinds);
                }
            }
        }
    }
    let mut kinds = HashMap::new();
    for path in &pattern.paths {
        walk(path, false, &mut kinds);
    }
    kinds
}

/// Pushes `diagnostic` unless an equal one is already recorded.
fn push_unique(diagnostics: &mut Vec<LocyCompileError>, diagnostic: LocyCompileError) {
    if !diagnostics.contains(&diagnostic) {
        diagnostics.push(diagnostic);
    }
}

/// The free variables of a list of expressions, in order.
fn names_read<'a>(exprs: impl IntoIterator<Item = &'a Expr>) -> Vec<String> {
    exprs.into_iter().flat_map(free_variables).collect()
}

/// The Cypher parts of an ALONG expression, skipping `prev.<column>`.
fn along_cypher_parts<'a>(expr: &'a LocyExpr, out: &mut Vec<&'a Expr>) {
    match expr {
        LocyExpr::PrevRef(_) => {}
        LocyExpr::Cypher(e) => out.push(e),
        LocyExpr::BinaryOp { left, right, .. } => {
            along_cypher_parts(left, out);
            along_cypher_parts(right, out);
        }
        LocyExpr::UnaryOp(_, inner) => along_cypher_parts(inner, out),
    }
}

/// Mutable state of [`ClauseBindingPlan::compute`]'s scheduler.
struct Scheduler<'a> {
    rule: &'a str,
    clause: &'a CompiledClause,
    plan: ClauseBindingPlan,
    /// Kind of every bound variable.
    var_kinds: HashMap<String, ColumnKind>,
    /// Names readable so far: bound variables and scheduled exposures.
    available: HashSet<String>,
    /// Filters not yet placed, as `(condition index, names read)`.
    pending_filters: Vec<(usize, Vec<String>)>,
    /// Positive references not yet scheduled.
    scheduled_refs: HashSet<usize>,
}

impl Scheduler<'_> {
    fn is_bound(&self, var: &str) -> bool {
        self.plan.scope.contains_key(var)
    }

    fn kind_mismatch(&self, r: usize, pos: usize, found: ColumnKind) -> LocyCompileError {
        let binding = &self.plan.refs[r];
        let position = &binding.positions[pos];
        if binding.negated && position.kind == ColumnKind::Node {
            // Keeps the literal `IS NOT subject` message negation failures
            // have always carried.
            return LocyCompileError::IsNotSubjectNotANode {
                rule: self.rule.to_string(),
                variable: position.variable.clone(),
            };
        }
        LocyCompileError::IsRefKindMismatch {
            rule: self.rule.to_string(),
            target: binding.rule.clone(),
            variable: position.variable.clone(),
            column: position.column.clone(),
            expected: position.kind,
            found,
        }
    }

    /// Schedules positive reference `r`: binds its unbound variables and
    /// tests the bound ones.
    fn schedule_ref(&mut self, r: usize) {
        self.plan.steps.push(BindingStep::Ref(r));
        self.scheduled_refs.insert(r);
        for pos in 0..self.plan.refs[r].positions.len() {
            let position = &self.plan.refs[r].positions[pos];
            let (var, kind) = (position.variable.clone(), position.kind);
            let action = if self.is_bound(&var) {
                let found = self.var_kinds[&var];
                if found != kind {
                    let diagnostic = self.kind_mismatch(r, pos, found);
                    push_unique(&mut self.plan.diagnostics, diagnostic);
                }
                if kind == ColumnKind::Node {
                    PositionAction::TestNode
                } else {
                    PositionAction::TestScalar
                }
            } else {
                self.plan
                    .scope
                    .insert(var.clone(), VarSource::RefPosition { r, pos });
                self.var_kinds.insert(var.clone(), kind);
                self.available.insert(var);
                if kind == ColumnKind::Node {
                    PositionAction::BindNode
                } else {
                    PositionAction::BindScalar
                }
            };
            self.plan.refs[r].positions[pos].action = action;
        }
        let ready: Vec<String> = self
            .plan
            .exposure
            .iter()
            .filter(|(_, exposure)| match exposure {
                Exposure::Unique { r: owner, .. } => *owner == r,
                Exposure::Ambiguous(owners) => {
                    owners.contains(&r) && owners.iter().all(|o| self.scheduled_refs.contains(o))
                }
            })
            .map(|(name, _)| name.clone())
            .collect();
        self.available.extend(ready);
        self.place_filters();
    }

    /// Schedules generator `g` of [`ClauseBindingPlan::generators`].
    fn schedule_generator(&mut self, g: usize) {
        let cond = self.plan.generators[g].condition_index;
        self.plan.steps.push(BindingStep::Generator(cond));
        for o in 0..self.plan.generators[g].outputs.len() {
            let var = self.plan.generators[g].outputs[o].variable.clone();
            let output = if self.is_bound(&var) {
                GeneratorOutput {
                    physical: format!("__gen{cond}_{var}"),
                    variable: var,
                    binds: false,
                }
            } else {
                self.plan.scope.insert(
                    var.clone(),
                    VarSource::Generator {
                        cond,
                        physical: var.clone(),
                    },
                );
                self.var_kinds.insert(var.clone(), ColumnKind::Scalar);
                self.available.insert(var.clone());
                GeneratorOutput {
                    physical: var.clone(),
                    variable: var,
                    binds: true,
                }
            };
            self.plan.generators[g].outputs[o] = output;
        }
        self.place_filters();
    }

    /// Places every pending filter whose names are now all available.
    fn place_filters(&mut self) {
        let available = &self.available;
        let (ready, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending_filters)
            .into_iter()
            .partition(|(_, names)| names.iter().all(|n| available.contains(n)));
        self.pending_filters = waiting;
        self.plan
            .steps
            .extend(ready.into_iter().map(|(cond, _)| BindingStep::Filter(cond)));
    }

    /// The names a generator's arguments read.
    fn generator_args(&self, g: usize) -> Vec<String> {
        let cond = self.plan.generators[g].condition_index;
        match &self.clause.where_conditions[cond] {
            RuleCondition::Generator(generator) => names_read(&generator.args),
            _ => Vec::new(),
        }
    }

    /// Schedules every positive reference and generator, then the leftovers.
    fn schedule_body(&mut self, positive: &[usize]) {
        let mut refs_left: Vec<usize> = positive.to_vec();
        let mut gens_left: Vec<usize> = (0..self.plan.generators.len()).collect();
        loop {
            let score = |s: &Self, r: usize| {
                let positions = &s.plan.refs[r].positions;
                let keys = positions
                    .iter()
                    .filter(|p| p.is_key && s.is_bound(&p.variable))
                    .count();
                let bound = positions.iter().filter(|p| s.is_bound(&p.variable)).count();
                (keys, bound)
            };
            // An anchored reference: a semi-join on a bound KEY.
            let anchored = refs_left
                .iter()
                .enumerate()
                .filter(|(_, r)| score(self, **r).0 > 0)
                .max_by(|(ia, a), (ib, b)| score(self, **a).cmp(&score(self, **b)).then(ib.cmp(ia)))
                .map(|(i, _)| i);
            if let Some(i) = anchored {
                let r = refs_left.remove(i);
                self.schedule_ref(r);
                continue;
            }
            // A ready generator, unless a reference still to come binds one
            // of its outputs: a reference beats a generator as binder.
            let ready = gens_left.iter().position(|&g| {
                let args_ready = self
                    .generator_args(g)
                    .iter()
                    .all(|n| self.available.contains(n));
                let outputs_free = self.plan.generators[g].outputs.iter().all(|o| {
                    !refs_left.iter().any(|&r| {
                        self.plan.refs[r]
                            .positions
                            .iter()
                            .any(|p| p.variable == o.variable)
                    })
                });
                args_ready && outputs_free
            });
            if let Some(i) = ready {
                let g = gens_left.remove(i);
                self.schedule_generator(g);
                continue;
            }
            // An unanchored reference.
            let unanchored = refs_left
                .iter()
                .enumerate()
                .max_by(|(ia, a), (ib, b)| {
                    score(self, **a).1.cmp(&score(self, **b).1).then(ib.cmp(ia))
                })
                .map(|(i, _)| i);
            if let Some(i) = unanchored {
                let r = refs_left.remove(i);
                self.schedule_ref(r);
                continue;
            }
            break;
        }
        self.schedule_leftover_generators(&gens_left);
        // Filters reading a name nothing makes available still get a step.
        // The name is unbound, which the compiler's scope check reports
        // (`compiler::scope`): it owns every "unbound name" diagnostic, so
        // the plan raises none of its own.
        let leftovers = std::mem::take(&mut self.pending_filters);
        self.plan.steps.extend(
            leftovers
                .into_iter()
                .map(|(cond, _)| BindingStep::Filter(cond)),
        );
    }

    /// Reports generators that can never run because they feed each other,
    /// then schedules every leftover generator anyway so each condition has a
    /// step.
    ///
    /// A leftover whose missing argument no other generator produces reads an
    /// unbound name, which the compiler's scope check reports.
    fn schedule_leftover_generators(&mut self, gens_left: &[usize]) {
        if gens_left.is_empty() {
            return;
        }
        let outputs: HashSet<String> = gens_left
            .iter()
            .flat_map(|&g| {
                self.plan.generators[g]
                    .outputs
                    .iter()
                    .map(|o| o.variable.clone())
            })
            .collect();
        let mut cyclic = Vec::new();
        for &g in gens_left {
            let missing: Vec<String> = self
                .generator_args(g)
                .into_iter()
                .filter(|n| !self.available.contains(n))
                .collect();
            if missing.iter().any(|n| outputs.contains(n)) {
                let cond = self.plan.generators[g].condition_index;
                if let RuleCondition::Generator(generator) = &self.clause.where_conditions[cond] {
                    cyclic.push(generator.name.clone());
                }
            }
        }
        if !cyclic.is_empty() {
            push_unique(
                &mut self.plan.diagnostics,
                LocyCompileError::GeneratorCycle {
                    rule: self.rule.to_string(),
                    generators: cyclic,
                },
            );
        }
        for &g in gens_left {
            self.schedule_generator(g);
        }
    }

    /// Schedules every `IS NOT` reference as an anti-join, last.
    fn schedule_negations(&mut self, negated: &[usize]) {
        let aliases: HashMap<String, String> = match &self.clause.output {
            RuleOutput::Yield(yc) => yc
                .items
                .iter()
                .filter_map(|item| match (&item.alias, &item.expr) {
                    (Some(alias), Expr::Variable(v)) => Some((alias.clone(), v.clone())),
                    _ => None,
                })
                .collect(),
            RuleOutput::Derive(_) => HashMap::new(),
        };
        for &r in negated {
            self.plan.steps.push(BindingStep::AntiJoin(r));
            for pos in 0..self.plan.refs[r].positions.len() {
                let position = &self.plan.refs[r].positions[pos];
                let (var, kind, is_key) =
                    (position.variable.clone(), position.kind, position.is_key);
                let test = if kind == ColumnKind::Node {
                    PositionAction::TestNode
                } else {
                    PositionAction::TestScalar
                };
                if !is_key {
                    // A fact is identified by its KEY; negation cannot compare
                    // a value, and an unbound value variable is ignored.
                    let action = if self.is_bound(&var) {
                        push_unique(
                            &mut self.plan.diagnostics,
                            LocyCompileError::IsNotBindsValueColumn {
                                rule: self.rule.to_string(),
                                target: self.plan.refs[r].rule.clone(),
                                variable: var.clone(),
                                column: position.column.clone(),
                            },
                        );
                        test
                    } else {
                        PositionAction::Existential
                    };
                    self.plan.refs[r].positions[pos].action = action;
                    continue;
                }
                // A YIELD alias of a bound node denotes that node.
                if !self.is_bound(&var)
                    && let Some(source) = aliases.get(&var)
                    && self.var_kinds.get(source) == Some(&ColumnKind::Node)
                {
                    self.plan
                        .scope
                        .insert(var.clone(), VarSource::YieldAlias(source.clone()));
                    self.var_kinds.insert(var.clone(), ColumnKind::Node);
                }
                let action = if let Some(&found) = self.var_kinds.get(&var) {
                    if found != kind {
                        let diagnostic = self.kind_mismatch(r, pos, found);
                        push_unique(&mut self.plan.diagnostics, diagnostic);
                    }
                    test
                } else {
                    let diagnostic = if kind == ColumnKind::Node {
                        LocyCompileError::IsNotSubjectNotANode {
                            rule: self.rule.to_string(),
                            variable: var.clone(),
                        }
                    } else {
                        LocyCompileError::IsNotKeyUnbound {
                            rule: self.rule.to_string(),
                            target: self.plan.refs[r].rule.clone(),
                            variable: var.clone(),
                        }
                    };
                    push_unique(&mut self.plan.diagnostics, diagnostic);
                    PositionAction::Existential
                };
                self.plan.refs[r].positions[pos].action = action;
            }
        }
    }

    /// Reports every read of an ambiguous exposure, slot by slot.
    fn check_ambiguous_reads(&mut self) {
        let clause = self.clause;
        let along: HashSet<String> = clause.along.iter().map(|a| a.name.clone()).collect();
        let folds: HashSet<String> = clause.fold.iter().map(|f| f.name.clone()).collect();
        let yield_items: &[LocyYieldItem] = match &clause.output {
            RuleOutput::Yield(yc) => &yc.items,
            RuleOutput::Derive(_) => &[],
        };
        let yield_aliases: HashSet<String> = yield_items
            .iter()
            .filter_map(|item| match (&item.alias, &item.expr) {
                (Some(alias), Expr::Variable(v)) if v == alias => None,
                (Some(alias), _) => Some(alias.clone()),
                (None, _) => None,
            })
            .collect();
        let mut along_exprs = Vec::new();
        for binding in &clause.along {
            along_cypher_parts(&binding.expr, &mut along_exprs);
        }
        let pre_fold_yield = yield_items
            .iter()
            .map(|i| &i.expr)
            .filter(|e| !free_variables(e).iter().any(|v| folds.contains(v)));
        let empty = HashSet::new();
        let best_by_shadow: HashSet<String> = along
            .iter()
            .chain(&folds)
            .chain(&yield_aliases)
            .cloned()
            .collect();
        let slots: Vec<(&str, Vec<String>, &HashSet<String>)> = vec![
            (
                "WHERE",
                names_read(clause.where_conditions.iter().filter_map(|c| match c {
                    RuleCondition::Expression(e) => Some(e),
                    _ => None,
                })),
                &empty,
            ),
            (
                "generator arguments",
                names_read(clause.where_conditions.iter().flat_map(|c| match c {
                    RuleCondition::Generator(g) => g.args.iter().collect(),
                    _ => Vec::new(),
                })),
                &empty,
            ),
            ("ALONG", names_read(along_exprs), &along),
            (
                "FOLD",
                names_read(clause.fold.iter().map(|f| &f.aggregate)),
                &along,
            ),
            ("YIELD", names_read(pre_fold_yield), &along),
            (
                "BEST BY",
                names_read(
                    clause
                        .best_by
                        .iter()
                        .flat_map(|b| b.items.iter().map(|i| &i.expr)),
                ),
                &best_by_shadow,
            ),
        ];
        let mut reported = HashSet::new();
        for (slot, names, shadow) in slots {
            for name in names {
                if shadow.contains(&name) || reported.contains(&name) {
                    continue;
                }
                let Some(Exposure::Ambiguous(owners)) = self.plan.exposure.get(&name) else {
                    continue;
                };
                let refs = owners
                    .iter()
                    .map(|&o| self.plan.refs[o].rule.clone())
                    .collect();
                self.plan
                    .diagnostics
                    .push(LocyCompileError::AmbiguousRuleColumn {
                        rule: self.rule.to_string(),
                        column: name.clone(),
                        slot: slot.to_string(),
                        refs,
                    });
                reported.insert(name);
            }
        }
    }
}

impl ClauseBindingPlan {
    /// Computes the binding plan of `clause` of rule `rule`.
    ///
    /// `catalog` holds every rule the clause may reference, keyed as its IS
    /// references name them, and `kinds` its [`ColumnKinds`]. `same_scc`
    /// answers whether a referenced rule belongs to `rule`'s own recursive
    /// group; it picks [`Self::prev_ref`].
    ///
    /// The result does not depend on the order of the clause's WHERE
    /// conditions, except for tie-breaks between equally anchored references
    /// and for the numbering of prefixes. Returns `None` when the clause
    /// references a rule `catalog` lacks, or binds more positions than a
    /// referenced rule has columns; the compiler reports the latter, and the
    /// former belongs to a rule registered elsewhere.
    #[must_use]
    pub fn compute(
        rule: &str,
        clause: &CompiledClause,
        catalog: &HashMap<String, CompiledRule>,
        kinds: &ColumnKinds,
        same_scc: &dyn Fn(&str) -> bool,
    ) -> Option<Self> {
        let mut plan = Self {
            steps: Vec::new(),
            refs: Vec::new(),
            generators: Vec::new(),
            scope: BTreeMap::new(),
            exposure: BTreeMap::new(),
            immediate_filters: Vec::new(),
            prev_ref: None,
            anchor: None,
            diagnostics: Vec::new(),
        };
        let mut var_kinds = HashMap::new();
        for (var, kind) in match_var_kinds(&clause.match_pattern) {
            plan.scope.insert(var.clone(), VarSource::Match(kind));
            var_kinds.insert(var, kind);
        }

        let (mut positive, mut negated) = (Vec::new(), Vec::new());
        let mut filters = Vec::new();
        for (index, condition) in clause.where_conditions.iter().enumerate() {
            match condition {
                RuleCondition::IsReference(is_ref) => {
                    let target = is_ref.rule_name.to_string();
                    let target_rule = catalog.get(&target)?;
                    let columns = bound_columns(
                        &target_rule.yield_schema,
                        is_ref.subjects.len(),
                        is_ref.target.is_some(),
                    )?;
                    let prefix = if is_ref.negated {
                        format!("__isnot{}_", negated.len())
                    } else {
                        format!("__isref{}_", positive.len())
                    };
                    let positions = is_ref
                        .subjects
                        .iter()
                        .chain(&is_ref.target)
                        .zip(columns)
                        .map(|(var, col)| PositionBinding {
                            variable: var.clone(),
                            column: col.name.clone(),
                            physical: format!("{prefix}{}", col.name),
                            is_key: col.is_key,
                            kind: kinds.kind(&target, &col.name).unwrap_or(ColumnKind::Node),
                            action: PositionAction::Existential,
                        })
                        .collect();
                    if is_ref.negated {
                        negated.push(plan.refs.len());
                    } else {
                        positive.push(plan.refs.len());
                    }
                    plan.refs.push(IsRefBinding {
                        condition_index: index,
                        rule: target,
                        negated: is_ref.negated,
                        prefix,
                        positions,
                    });
                }
                RuleCondition::Generator(generator) => plan.generators.push(GeneratorBinding {
                    condition_index: index,
                    outputs: generator
                        .outputs
                        .iter()
                        .map(|o| GeneratorOutput {
                            variable: o.clone(),
                            physical: o.clone(),
                            binds: true,
                        })
                        .collect(),
                }),
                RuleCondition::Expression(expr) => filters.push((index, free_variables(expr))),
            }
        }

        // Exposure: value columns no position binds, unless a real variable
        // of the body shadows the name.
        let mut real: HashSet<&str> = plan.scope.keys().map(String::as_str).collect();
        for &r in &positive {
            real.extend(plan.refs[r].positions.iter().map(|p| p.variable.as_str()));
        }
        for generator in &plan.generators {
            real.extend(generator.outputs.iter().map(|o| o.variable.as_str()));
        }
        let mut exposed: BTreeMap<String, Vec<(usize, String)>> = BTreeMap::new();
        for &r in &positive {
            let binding = &plan.refs[r];
            let schema = &catalog[&binding.rule].yield_schema;
            for col in binding_sequence(schema)
                .into_iter()
                .skip(binding.positions.len())
                .filter(|c| !c.is_key && !real.contains(c.name.as_str()))
            {
                exposed
                    .entry(col.name.clone())
                    .or_default()
                    .push((r, format!("{}{}", binding.prefix, col.name)));
            }
        }
        plan.exposure = exposed
            .into_iter()
            .map(|(name, mut owners)| {
                let exposure = if owners.len() == 1 {
                    let (r, physical) = owners.remove(0);
                    Exposure::Unique { r, physical }
                } else {
                    Exposure::Ambiguous(owners.into_iter().map(|(r, _)| r).collect())
                };
                (name, exposure)
            })
            .collect();

        let available: HashSet<String> = plan.scope.keys().cloned().collect();
        let (immediate, pending): (Vec<_>, Vec<_>) = filters
            .into_iter()
            .partition(|(_, names)| names.iter().all(|n| available.contains(n)));
        plan.immediate_filters = immediate.into_iter().map(|(cond, _)| cond).collect();

        // The one positive reference into the clause's recursive group.
        let recursive: Vec<usize> = positive
            .iter()
            .copied()
            .filter(|&r| same_scc(&plan.refs[r].rule))
            .collect();
        match recursive.as_slice() {
            [only] => plan.prev_ref = Some(*only),
            [] => {}
            _ => {
                if let Some(field) = clause
                    .along
                    .iter()
                    .flat_map(|a| collect_prev_refs(&a.expr))
                    .next()
                {
                    plan.diagnostics.push(LocyCompileError::AmbiguousPrevRef {
                        rule: rule.to_string(),
                        field,
                    });
                }
            }
        }

        let mut scheduler = Scheduler {
            rule,
            clause,
            plan,
            var_kinds,
            available,
            pending_filters: pending,
            scheduled_refs: HashSet::new(),
        };
        scheduler.schedule_body(&positive);
        scheduler.schedule_negations(&negated);
        scheduler.check_ambiguous_reads();
        let mut plan = scheduler.plan;
        plan.anchor = choose_anchor(&plan, clause, &positive, same_scc);
        Some(plan)
    }
}

/// The anchor of a scheduled plan, per [`ClauseBindingPlan::anchor`].
fn choose_anchor(
    plan: &ClauseBindingPlan,
    clause: &CompiledClause,
    positive: &[usize],
    same_scc: &dyn Fn(&str) -> bool,
) -> Option<Anchor> {
    let walkable = walkable_node_variables(&clause.match_pattern);
    let candidates = |source: AnchorSource, eligible: &dyn Fn(&str) -> bool| {
        positive
            .iter()
            .copied()
            .filter(|&r| same_scc(&plan.refs[r].rule) == (source == AnchorSource::Recursive))
            .filter_map(|r| {
                let pos = plan.refs[r].positions.iter().position(|p| {
                    p.is_key
                        && p.action == PositionAction::TestNode
                        && matches!(
                            plan.scope.get(&p.variable),
                            Some(VarSource::Match(ColumnKind::Node))
                        )
                        && walkable.contains(p.variable.as_str())
                        && eligible(&p.variable)
                })?;
                Some(Anchor { r, pos, source })
            })
            .min_by(|a, b| anchor_rank(plan, a).cmp(&anchor_rank(plan, b)))
    };
    candidates(AnchorSource::Recursive, &|_| true).or_else(|| {
        let equalities = equality_anchored_nodes(plan, clause);
        candidates(AnchorSource::LowerStratum, &|var| {
            equalities.iter().all(|other| *other == var)
        })
    })
}

/// The deterministic tie-break between candidate anchors of one kind.
fn anchor_rank(plan: &ClauseBindingPlan, anchor: &Anchor) -> (String, Vec<String>) {
    let binding = &plan.refs[anchor.r];
    (
        binding.rule.clone(),
        binding
            .positions
            .iter()
            .map(|p| p.variable.clone())
            .collect(),
    )
}

/// The MATCH node variables an equality could start the walk from.
///
/// A node with an inline property map, or one that a WHERE conjunct over MATCH
/// variables only equates a property of to an expression of no variable
/// (`v.uid = 'x'`, `$p = v.uid`). These are the nodes the pattern planner
/// ranks as anchors on its own (`reversed_for_bound_anchor`), with or without
/// an index behind them, so a lower-stratum seed elsewhere would override a
/// start the planner already considers better.
fn equality_anchored_nodes<'c>(
    plan: &ClauseBindingPlan,
    clause: &'c CompiledClause,
) -> HashSet<&'c str> {
    fn conjuncts<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: uni_cypher::ast::BinaryOp::And,
                right,
            } => {
                conjuncts(left, out);
                conjuncts(right, out);
            }
            other => out.push(other),
        }
    }
    fn equated_node<'e>(expr: &'e Expr) -> Option<&'e str> {
        let Expr::BinaryOp {
            left,
            op: uni_cypher::ast::BinaryOp::Eq,
            right,
        } = expr
        else {
            return None;
        };
        let side = |property: &'e Expr, other: &Expr| match property {
            Expr::Property(inner, _) if free_variables(other).is_empty() => match inner.as_ref() {
                Expr::Variable(v) => Some(v.as_str()),
                _ => None,
            },
            _ => None,
        };
        side(left, right).or_else(|| side(right, left))
    }

    let mut out = HashSet::new();
    for path in &clause.match_pattern.paths {
        for element in &path.elements {
            if let PatternElement::Node(n) = element
                && n.properties.is_some()
                && let Some(v) = n.variable.as_deref()
            {
                out.insert(v);
            }
        }
    }
    for &index in &plan.immediate_filters {
        if let Some(RuleCondition::Expression(expr)) = clause.where_conditions.get(index) {
            let mut parts = Vec::new();
            conjuncts(expr, &mut parts);
            out.extend(parts.into_iter().filter_map(equated_node));
        }
    }
    out
}

/// The node variables a walk may start from: those written as a plain node
/// element of a path that is neither a shortestPath nor carries a path
/// variable, and never inside a quantified group.
///
/// A variable that also appears where a walk cannot start from it — in a
/// quantified group, where it names a list — is excluded altogether.
fn walkable_node_variables(pattern: &Pattern) -> HashSet<&str> {
    fn quantified<'a>(path: &'a PathPattern, out: &mut HashSet<&'a str>) {
        for element in &path.elements {
            match element {
                PatternElement::Node(n) => out.extend(n.variable.as_deref()),
                PatternElement::Relationship(_) => {}
                PatternElement::Parenthesized { pattern, .. } => quantified(pattern, out),
            }
        }
    }
    let mut plain = HashSet::new();
    let mut excluded = HashSet::new();
    for path in &pattern.paths {
        let walk_from = path.shortest_path_mode.is_none() && path.variable.is_none();
        for element in &path.elements {
            match element {
                PatternElement::Node(n) => {
                    if let Some(v) = n.variable.as_deref().filter(|v| !v.is_empty()) {
                        if walk_from {
                            plain.insert(v);
                        } else {
                            excluded.insert(v);
                        }
                    }
                }
                PatternElement::Relationship(_) => {}
                PatternElement::Parenthesized { pattern, .. } => {
                    quantified(pattern, &mut excluded);
                }
            }
        }
    }
    plain.retain(|v| !excluded.contains(v));
    plain
}

/// The binding of every clause of `rule`, against `catalog`.
fn bindings_for_rule(
    rule: &CompiledRule,
    catalog: &HashMap<String, CompiledRule>,
    kinds: &ColumnKinds,
    scc_of: &dyn Fn(&str) -> Option<usize>,
) -> Vec<ClauseBinding> {
    let own = scc_of(&rule.name);
    let same_scc =
        |target: &str| own.is_some() && catalog.get(target).and_then(|t| scc_of(&t.name)) == own;
    rule.clauses
        .iter()
        .map(|clause| {
            ClauseBindingPlan::compute(&rule.name, clause, catalog, kinds, &same_scc)
                .map_or(ClauseBinding::Pending, |plan| {
                    ClauseBinding::Ready(Arc::new(plan))
                })
        })
        .collect()
}

/// Sets each rule's clause bindings from `bindings`, in clause order.
fn assign(rule: &mut CompiledRule, bindings: Vec<ClauseBinding>) {
    for (clause, binding) in rule.clauses.iter_mut().zip(bindings) {
        clause.binding = binding;
    }
}

/// The first diagnostic of any plan of `rules`, rules in name order and
/// clauses in order, so the error a program reports is the same on every run.
fn first_diagnostic<'a>(
    rules: impl IntoIterator<Item = &'a CompiledRule>,
) -> Option<LocyCompileError> {
    let mut rules: Vec<&CompiledRule> = rules.into_iter().collect();
    rules.sort_by(|a, b| a.name.cmp(&b.name));
    rules
        .into_iter()
        .flat_map(|rule| &rule.clauses)
        .filter_map(|clause| clause.binding.plan())
        .find_map(|plan| plan.diagnostics.first().cloned())
}

/// Computes and attaches the binding plan of every clause of `catalog`, then
/// raises the first diagnostic any plan records.
///
/// `scc_of` maps a rule name to its recursive group.
///
/// # Errors
///
/// The first [`ClauseBindingPlan::diagnostics`] entry, rules in name order.
pub(crate) fn attach_to_catalog(
    catalog: &mut HashMap<String, CompiledRule>,
    scc_of: &dyn Fn(&str) -> Option<usize>,
) -> Result<(), LocyCompileError> {
    let kinds = ColumnKinds::infer(catalog);
    let computed: Vec<(String, Vec<ClauseBinding>)> = catalog
        .iter()
        .map(|(key, rule)| {
            (
                key.clone(),
                bindings_for_rule(rule, catalog, &kinds, scc_of),
            )
        })
        .collect();
    for (key, bindings) in computed {
        if let Some(rule) = catalog.get_mut(&key) {
            assign(rule, bindings);
        }
    }
    first_diagnostic(catalog.values()).map_or(Ok(()), Err)
}

/// Computes and attaches the binding plan of every clause of `program`.
///
/// Recomputes every plan from the program's current `rule_catalog`, so it is
/// idempotent and must be called again whenever the catalog changes: after
/// registered rules are merged in, and for an ASSUME body planned against its
/// parent's rules. Both the catalog's rules and the strata's copies of them
/// are updated. A rule's recursive group is the first stratum that lists it.
///
/// Clauses that reference a rule the catalog lacks stay
/// [`ClauseBinding::Pending`]. Problems a plan finds are recorded in its
/// [`ClauseBindingPlan::diagnostics`], and the first one is returned.
///
/// # Errors
///
/// The first [`ClauseBindingPlan::diagnostics`] entry of any plan, rules in
/// name order: a clause that only became plannable against this catalog —
/// one referencing a registered rule, or an ASSUME body referencing its
/// parent's rules — is checked here rather than at compile time.
pub fn attach_binding_plans(program: &mut CompiledProgram) -> Result<(), LocyCompileError> {
    let mut positions: HashMap<String, usize> = HashMap::new();
    for (index, stratum) in program.strata.iter().enumerate() {
        for rule in &stratum.rules {
            positions.entry(rule.name.clone()).or_insert(index);
        }
    }
    let scc_of = |name: &str| positions.get(name).copied();
    let catalog = &program.rule_catalog;
    let kinds = ColumnKinds::infer(catalog);
    let in_catalog: Vec<(String, Vec<ClauseBinding>)> = catalog
        .iter()
        .map(|(key, rule)| {
            (
                key.clone(),
                bindings_for_rule(rule, catalog, &kinds, &scc_of),
            )
        })
        .collect();
    let in_strata: Vec<Vec<Vec<ClauseBinding>>> = program
        .strata
        .iter()
        .map(|stratum| {
            stratum
                .rules
                .iter()
                .map(|rule| bindings_for_rule(rule, catalog, &kinds, &scc_of))
                .collect()
        })
        .collect();
    for (key, bindings) in in_catalog {
        if let Some(rule) = program.rule_catalog.get_mut(&key) {
            assign(rule, bindings);
        }
    }
    for (stratum, rules) in program.strata.iter_mut().zip(in_strata) {
        for (rule, bindings) in stratum.rules.iter_mut().zip(rules) {
            assign(rule, bindings);
        }
    }
    first_diagnostic(program.rule_catalog.values()).map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_cypher::parse_locy;

    const STAKE: &str = "CREATE RULE stake AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE r.pct IS NOT NULL \
        FOLD pct = MAX(r.pct) YIELD KEY o, KEY e, pct\n";
    const SEED: &str =
        "CREATE RULE seed AS MATCH (e:E) WHERE e.uid = 'x' YIELD KEY e, 100.0 AS agg\n";

    fn compiled(src: &str) -> CompiledProgram {
        crate::compile(&parse_locy(src).expect("program parses")).expect("program compiles")
    }

    /// The error compiling `src` raises; plan diagnostics are compile errors.
    fn compile_error(src: &str) -> LocyCompileError {
        match crate::compile(&parse_locy(src).expect("program parses")) {
            Ok(_) => panic!("program compiles, but an error was expected:\n{src}"),
            Err(e) => e,
        }
    }

    /// Index of the first clause of `rule` that has WHERE conditions.
    fn clause_index(program: &CompiledProgram, rule: &str) -> usize {
        program.rule_catalog[rule]
            .clauses
            .iter()
            .position(|c| !c.where_conditions.is_empty())
            .expect("a clause with WHERE conditions")
    }

    /// The plan of `rule`'s clause with its WHERE conditions in `order`.
    fn plan_in_order(program: &CompiledProgram, rule: &str, order: &[usize]) -> ClauseBindingPlan {
        let mut clause = program.rule_catalog[rule].clauses[clause_index(program, rule)].clone();
        clause.where_conditions = order
            .iter()
            .map(|&i| clause.where_conditions[i].clone())
            .collect();
        let mut positions = HashMap::new();
        for (index, stratum) in program.strata.iter().enumerate() {
            for r in &stratum.rules {
                positions.entry(r.name.clone()).or_insert(index);
            }
        }
        let own = positions.get(rule).copied();
        let kinds = ColumnKinds::infer(&program.rule_catalog);
        ClauseBindingPlan::compute(rule, &clause, &program.rule_catalog, &kinds, &|target| {
            own.is_some() && positions.get(target).copied() == own
        })
        .expect("every referenced rule is known")
    }

    fn permutations(n: usize) -> Vec<Vec<usize>> {
        if n == 0 {
            return vec![Vec::new()];
        }
        let mut out = Vec::new();
        for rest in permutations(n - 1) {
            for at in 0..=rest.len() {
                let mut p = rest.clone();
                p.insert(at, n - 1);
                out.push(p);
            }
        }
        out
    }

    /// Each variable's binder as `rule.column`, `match` or `gen`, which does
    /// not depend on reference numbering.
    fn binders(plan: &ClauseBindingPlan) -> BTreeMap<String, String> {
        plan.scope
            .iter()
            .map(|(var, source)| {
                let binder = match source {
                    VarSource::Match(kind) => format!("match {kind:?}"),
                    VarSource::RefPosition { r, pos } => {
                        let p = &plan.refs[*r].positions[*pos];
                        format!("{}.{} {:?}", plan.refs[*r].rule, p.column, p.action)
                    }
                    VarSource::Generator { .. } => "gen".to_string(),
                    VarSource::YieldAlias(of) => format!("alias {of}"),
                };
                (var.clone(), binder)
            })
            .collect()
    }

    /// Asserts every order of `rule`'s WHERE gives the same binders and
    /// diagnostics, and returns the plan of the textual order.
    fn order_independent(program: &CompiledProgram, rule: &str) -> ClauseBindingPlan {
        let n = program.rule_catalog[rule].clauses[clause_index(program, rule)]
            .where_conditions
            .len();
        let identity: Vec<usize> = (0..n).collect();
        let base = plan_in_order(program, rule, &identity);
        for order in permutations(n) {
            let plan = plan_in_order(program, rule, &order);
            assert_eq!(
                binders(&plan),
                binders(&base),
                "binders differ in order {order:?}"
            );
            assert_eq!(
                plan.diagnostics.len(),
                base.diagnostics.len(),
                "diagnostics differ in order {order:?}: {:?} vs {:?}",
                plan.diagnostics,
                base.diagnostics
            );
            assert!(
                matches!(plan.steps.last(), Some(BindingStep::AntiJoin(_)))
                    || !plan.refs.iter().any(|r| r.negated),
                "anti-joins run last"
            );
        }
        base
    }

    /// The anchor of the plan of `rule`'s first clause with WHERE conditions,
    /// as `(referenced rule, anchored variable)`, in every WHERE order.
    fn anchors(program: &CompiledProgram, rule: &str) -> Vec<Option<(String, String)>> {
        anchors_with_source(program, rule)
            .into_iter()
            .map(|a| a.map(|(r, v, _)| (r, v)))
            .collect()
    }

    /// [`anchors`] with each anchor's [`AnchorSource`].
    fn anchors_with_source(
        program: &CompiledProgram,
        rule: &str,
    ) -> Vec<Option<(String, String, AnchorSource)>> {
        let n = program.rule_catalog[rule].clauses[clause_index(program, rule)]
            .where_conditions
            .len();
        permutations(n)
            .into_iter()
            .map(|order| {
                let plan = plan_in_order(program, rule, &order);
                plan.anchor.map(|a| {
                    let binding = &plan.refs[a.r];
                    // Anchoring never changes binding: the anchoring
                    // position is a test of a MATCH node.
                    assert_eq!(binding.positions[a.pos].action, PositionAction::TestNode);
                    (
                        binding.rule.clone(),
                        binding.positions[a.pos].variable.clone(),
                        a.source,
                    )
                })
            })
            .collect()
    }

    #[test]
    fn issue_301_anchor_is_the_recursive_reference_in_every_order() {
        // `seed` is a lower stratum and also tests `o` on a KEY; only the
        // reference into `r`'s own recursive group anchors.
        let program = compiled(&format!(
            "{SEED}CREATE RULE r AS MATCH (e:E {{uid: 'y'}}) YIELD KEY e, 1.0 AS agg\n\
             CREATE RULE r AS MATCH (o:E)-[x:OWNS]->(e:E) \
             WHERE x.pct IS NOT NULL AND o IS seed AND o IS r YIELD KEY e, x.pct AS agg"
        ));
        let all = anchors(&program, "r");
        assert!(
            all.iter()
                .all(|a| a == &Some(("r".to_string(), "o".to_string()))),
            "{all:?}"
        );
    }

    #[test]
    fn issue_301_no_anchor_without_a_walkable_reference() {
        // A node bound under a path variable is not walked from.
        let program = compiled(
            "CREATE RULE r AS MATCH (e:E {uid: 'y'}) YIELD KEY e\n\
             CREATE RULE r AS MATCH p = (o:E)-[:OWNS]->(e:E) WHERE o IS r YIELD KEY e",
        );
        assert!(anchors(&program, "r").iter().all(Option::is_none));
    }

    #[test]
    fn p4_lower_stratum_reference_anchors_without_a_recursive_one() {
        // Both lower-stratum references test `o` on a KEY; the smaller
        // `(rule, subjects)` wins in every order, and it is a lower-stratum
        // anchor.
        let program = compiled(&format!(
            "{STAKE}{SEED}CREATE RULE r AS MATCH (o:E) WHERE o IS seed AND (o, e, pct) IS stake \
             YIELD KEY e, pct"
        ));
        let all = anchors_with_source(&program, "r");
        assert!(
            all.iter().all(|a| a
                == &Some((
                    "seed".to_string(),
                    "o".to_string(),
                    AnchorSource::LowerStratum
                ))),
            "{all:?}"
        );
        // A recursive reference still wins over a lower-stratum one.
        let program = compiled(&format!(
            "{SEED}CREATE RULE r AS MATCH (e:E {{uid: 'y'}}) YIELD KEY e\n\
             CREATE RULE r AS MATCH (o:E)-[:OWNS]->(e:E) WHERE e IS seed AND o IS r YIELD KEY e"
        ));
        let all = anchors_with_source(&program, "r");
        assert!(
            all.iter()
                .all(|a| a == &Some(("r".to_string(), "o".to_string(), AnchorSource::Recursive))),
            "{all:?}"
        );
    }

    #[test]
    fn p4_anchor_on_the_walks_target() {
        // A1b: the reference tests the walk's target `m`; `b` is bound by it.
        let program = compiled(
            "CREATE RULE r AS MATCH (a:E)-[:OWNS]->(b:E) YIELD KEY a, KEY b\n\
             CREATE RULE r AS MATCH (a:E)-[:OWNS]->(m:E) WHERE a.uid <> 'z' AND m IS r TO b \
             YIELD KEY a, KEY b",
        );
        let all = anchors_with_source(&program, "r");
        assert!(
            all.iter()
                .all(|a| a == &Some(("r".to_string(), "m".to_string(), AnchorSource::Recursive))),
            "{all:?}"
        );
        let plan = order_independent(&program, "r");
        assert_eq!(binders(&plan)["b"], "r.b BindNode");
    }

    #[test]
    fn p4_equality_elsewhere_in_the_pattern_keeps_the_planners_start() {
        // `f` carries an equality the pattern planner starts from; a
        // lower-stratum seed on `o` would override it.
        for f in ["(f:E {uid: 'k'})", "(f:E)"] {
            let filter = if f == "(f:E)" { "f.uid = 'k' AND " } else { "" };
            let program = compiled(&format!(
                "{SEED}CREATE RULE r AS MATCH (o:E)-[:OWNS]->{f} WHERE {filter}o IS seed \
                 YIELD KEY f"
            ));
            assert!(
                anchors(&program, "r").iter().all(Option::is_none),
                "{f} {filter}"
            );
        }
        // An equality on the anchored node itself composes with the seed, and
        // a non-equality elsewhere does not count.
        let program = compiled(&format!(
            "{SEED}CREATE RULE r AS MATCH (o:E)-[:OWNS]->(f:E) \
             WHERE o.uid = 'x' AND f.uid <> 'k' AND o IS seed YIELD KEY f"
        ));
        let all = anchors_with_source(&program, "r");
        assert!(
            all.iter().all(|a| a
                == &Some((
                    "seed".to_string(),
                    "o".to_string(),
                    AnchorSource::LowerStratum
                ))),
            "{all:?}"
        );
        // A recursive anchor ignores equalities: the delta is the frontier.
        let program = compiled(
            "CREATE RULE r AS MATCH (e:E {uid: 'y'}) YIELD KEY e\n\
             CREATE RULE r AS MATCH (o:E)-[:OWNS]->(e:E {uid: 'k'}) WHERE o IS r YIELD KEY e",
        );
        let all = anchors_with_source(&program, "r");
        assert!(
            all.iter()
                .all(|a| a == &Some(("r".to_string(), "o".to_string(), AnchorSource::Recursive))),
            "{all:?}"
        );
    }

    #[test]
    fn p1_stake_binds_e_in_both_orders_and_seed_key_is_not_exposed() {
        let program = compiled(&format!(
            "{STAKE}{SEED}CREATE RULE r AS MATCH (o:E) WHERE o IS seed AND (o, e, pct) IS stake \
             YIELD KEY e, pct"
        ));
        let plan = order_independent(&program, "r");
        let b = binders(&plan);
        assert_eq!(b["o"], "match Node");
        assert_eq!(b["e"], "stake.e BindNode");
        assert_eq!(b["pct"], "stake.pct BindScalar");
        // seed's KEY `e` is never exposed; only its value column is.
        assert_eq!(plan.exposure.keys().collect::<Vec<_>>(), ["agg"]);
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
        // Every positive reference is prefixed, the first included.
        assert!(plan.refs.iter().all(|r| r.prefix.starts_with("__isref")));
        let seed = plan
            .refs
            .iter()
            .find(|r| r.rule == "seed")
            .expect("seed ref");
        assert_eq!(seed.positions[0].action, PositionAction::TestNode);
    }

    #[test]
    fn p3_permuted_names_bind_by_position() {
        let program = compiled(&format!(
            "{STAKE}CREATE RULE q AS MATCH (n:E) WHERE n.uid = 'x' AND (e, o) IS stake \
             YIELD KEY e, KEY o"
        ));
        let plan = order_independent(&program, "q");
        let b = binders(&plan);
        assert_eq!(b["e"], "stake.o BindNode");
        assert_eq!(b["o"], "stake.e BindNode");
        assert_eq!(
            plan.immediate_filters.len(),
            1,
            "the filter reads MATCH only"
        );
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
    }

    #[test]
    fn q1a_scalar_key_is_scalar() {
        let program = compiled(
            "CREATE RULE tag AS MATCH (n:N)-[:HAS]->(t:T) YIELD KEY n, KEY t.k AS c\n\
             CREATE RULE q AS MATCH (a:N) WHERE a IS tag TO col YIELD KEY a, col",
        );
        let kinds = ColumnKinds::infer(&program.rule_catalog);
        assert_eq!(kinds.kind("tag", "n"), Some(ColumnKind::Node));
        assert_eq!(kinds.kind("tag", "c"), Some(ColumnKind::Scalar));
        let plan = order_independent(&program, "q");
        let position = &plan.refs[0].positions[1];
        assert_eq!(position.kind, ColumnKind::Scalar);
        assert_eq!(position.action, PositionAction::BindScalar);
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
    }

    #[test]
    fn kinds_follow_recursion_and_yield_expressions() {
        let program = compiled(
            "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
             CREATE RULE reach AS MATCH (a:N) WHERE a IS reach TO m, m IS reach TO b \
             YIELD KEY a, KEY b\n\
             CREATE RULE ed AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, e\n",
        );
        let kinds = ColumnKinds::infer(&program.rule_catalog);
        assert_eq!(kinds.kind("reach", "b"), Some(ColumnKind::Node));
        assert_eq!(kinds.kind("ed", "e"), Some(ColumnKind::Edge));
    }

    #[test]
    fn q2_prev_reads_the_recursive_reference_in_both_orders() {
        let program = compiled(
            "CREATE RULE disc AS MATCH (a:N) YIELD KEY a, a.d AS cost\n\
             CREATE RULE path AS MATCH (a:N)-[e:E]->(b:N) ALONG cost = e.w YIELD KEY a, KEY b, cost\n\
             CREATE RULE path AS MATCH (a:N)-[e:E]->(m:N) WHERE a IS disc, m IS path TO b \
             ALONG cost = prev.cost + e.w YIELD KEY a, KEY b, cost",
        );
        for order in [[0, 1], [1, 0]] {
            let plan = plan_in_order(&program, "path", &order);
            let prev = plan.prev_ref.expect("one recursive reference");
            assert_eq!(plan.refs[prev].rule, "path", "order {order:?}");
            assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
        }
    }

    #[test]
    fn prev_with_two_recursive_references_is_ambiguous() {
        let base = "CREATE RULE path AS MATCH (a:N)-[e:E]->(b:N) ALONG cost = e.w \
                    YIELD KEY a, KEY b, cost\n";
        let error = compile_error(&format!(
            "{base}CREATE RULE path AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS path TO x, \
             x IS path TO b ALONG cost = prev.cost + e.w YIELD KEY a, KEY b, cost"
        ));
        assert!(
            matches!(&error, LocyCompileError::AmbiguousPrevRef { field, .. } if field == "cost"),
            "{error:?}"
        );
        // Without a `prev` read, two recursive references are legal, and
        // neither is the one `prev` would denote.
        let program = compiled(&format!(
            "{base}CREATE RULE path AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS path TO x, \
             x IS path TO b ALONG cost = e.w YIELD KEY a, KEY b, cost"
        ));
        let plan = plan_in_order(&program, "path", &[0, 1]);
        assert_eq!(plan.prev_ref, None);
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
    }

    #[test]
    fn q3_reading_a_name_two_references_expose_is_ambiguous() {
        let rules = "CREATE RULE r1 AS MATCH (a:N) YIELD KEY a, a.d AS score\n\
                     CREATE RULE r2 AS MATCH (a:N) YIELD KEY a, 1 AS score\n";
        let conditions = ["a IS r1", "a IS r2", "score > 5"];
        for order in permutations(conditions.len()) {
            let body: Vec<&str> = order.iter().map(|&i| conditions[i]).collect();
            let error = compile_error(&format!(
                "{rules}CREATE RULE q AS MATCH (a:N) WHERE {} YIELD KEY a",
                body.join(", ")
            ));
            assert!(
                matches!(&error, LocyCompileError::AmbiguousRuleColumn { column, slot, refs, .. }
                    if column == "score" && slot == "WHERE" && refs.len() == 2),
                "order {order:?}: {error:?}"
            );
            let message = error.to_string();
            assert!(
                message.contains("ambiguous") && message.contains("score"),
                "{message}"
            );
        }

        // Exposing the duplicate without reading it is legal.
        let control = compiled(&format!(
            "{rules}CREATE RULE q AS MATCH (a:N) WHERE a IS r1, a IS r2 YIELD KEY a"
        ));
        let plan = order_independent(&control, "q");
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
        assert!(matches!(plan.exposure["score"], Exposure::Ambiguous(ref o) if o.len() == 2));
        // Binding one positionally removes the ambiguity.
        let positional = compiled(&format!(
            "{rules}CREATE RULE q AS MATCH (a:N) WHERE (a, s1) IS r1, a IS r2, score > 5 \
             YIELD KEY a, s1"
        ));
        let plan = order_independent(&positional, "q");
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
        assert!(matches!(plan.exposure["score"], Exposure::Unique { .. }));
    }

    #[test]
    fn p5_negation_before_its_binder_has_no_diagnostic() {
        let program = compiled(
            "CREATE RULE blocked1 AS MATCH (b:E) WHERE b.uid = 'a' YIELD KEY b\n\
             CREATE RULE link AS MATCH (p:E)-[:OWNS]->(c:E) YIELD KEY p, KEY c\n\
             CREATE RULE q AS MATCH (a:E) WHERE a IS link TO b AND NOT b IS blocked1 \
             YIELD KEY a, KEY b",
        );
        let plan = order_independent(&program, "q");
        assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
        let reversed = plan_in_order(&program, "q", &[1, 0]);
        assert!(
            reversed.diagnostics.is_empty(),
            "{:?}",
            reversed.diagnostics
        );
        let negated = reversed
            .refs
            .iter()
            .find(|r| r.negated)
            .expect("negated ref");
        assert_eq!(negated.positions[0].action, PositionAction::TestNode);
        assert!(matches!(
            reversed.steps.last(),
            Some(BindingStep::AntiJoin(_))
        ));
    }

    #[test]
    fn negated_key_bound_nowhere_is_reported() {
        let error = compile_error(
            "CREATE RULE tag AS MATCH (n:N)-[:HAS]->(t:T) YIELD KEY n, KEY t.k AS c\n\
             CREATE RULE flagged AS MATCH (n:N) WHERE n.bad = true YIELD KEY n\n\
             CREATE RULE q AS MATCH (a:N) WHERE a IS NOT tag TO col YIELD KEY a",
        );
        assert!(
            matches!(&error, LocyCompileError::IsNotKeyUnbound { variable, .. } if variable == "col"),
            "{error:?}"
        );
    }

    #[test]
    fn edge_at_a_node_key_is_a_kind_mismatch() {
        let error = compile_error(
            "CREATE RULE flagged AS MATCH (n:N) WHERE n.bad = true YIELD KEY n \
             CREATE RULE probe AS MATCH (a:N)-[e:R]->(b:N) WHERE e IS flagged \
             YIELD KEY a, KEY b",
        );
        assert_eq!(
            error,
            LocyCompileError::IsRefKindMismatch {
                rule: "probe".into(),
                target: "flagged".into(),
                variable: "e".into(),
                column: "n".into(),
                expected: ColumnKind::Node,
                found: ColumnKind::Edge,
            }
        );
    }

    #[test]
    fn mutually_dependent_generators_are_a_cycle() {
        let error = compile_error(
            "CREATE RULE q AS MATCH (a:N) WHERE f(y) -> (x), g(x) -> (y) YIELD KEY a, x",
        );
        assert!(
            matches!(&error, LocyCompileError::GeneratorCycle { generators, .. } if generators.len() == 2),
            "{error:?}"
        );
    }

    /// A generator argument nothing binds is reported once, by the scope
    /// check, which owns every unbound-name diagnostic; the plan adds none.
    #[test]
    fn an_unbound_generator_argument_is_an_unbound_variable() {
        let error =
            compile_error("CREATE RULE q AS MATCH (a:N) WHERE f(ghost) -> (x) YIELD KEY a, x");
        assert!(
            matches!(&error, LocyCompileError::UnboundVariable { variable, slot, .. }
                if variable == "ghost" && slot == "generator arguments"),
            "{error:?}"
        );
    }

    #[test]
    fn a_reference_beats_a_generator_as_binder() {
        let program = compiled(
            "CREATE RULE tag AS MATCH (n:N)-[:HAS]->(t:T) YIELD KEY n, KEY t.k AS c\n\
             CREATE RULE q AS MATCH (a:N) WHERE f(a.k) -> (col), a IS tag TO col YIELD KEY a, col",
        );
        let plan = order_independent(&program, "q");
        assert_eq!(binders(&plan)["col"], "tag.c BindScalar");
        let output = &plan.generators[0].outputs[0];
        assert!(!output.binds);
        assert!(output.physical.starts_with("__gen"));
    }

    #[test]
    fn compile_attaches_a_plan_to_every_clause() {
        let program = compiled(&format!(
            "{STAKE}{SEED}CREATE RULE r AS MATCH (o:E) WHERE o IS seed AND (o, e, pct) IS stake \
             YIELD KEY e, pct"
        ));
        for rule in program.rule_catalog.values() {
            assert!(
                rule.clauses.iter().all(|c| c.binding.plan().is_some()),
                "{}",
                rule.name
            );
        }
        for stratum in &program.strata {
            for rule in &stratum.rules {
                assert!(
                    rule.clauses.iter().all(|c| c.binding.plan().is_some()),
                    "{}",
                    rule.name
                );
            }
        }
        let mut again = program.clone();
        attach_binding_plans(&mut again).expect("attaches");
        let plan = |p: &CompiledProgram| p.rule_catalog["r"].clauses[0].binding.plan().cloned();
        assert_eq!(plan(&again), plan(&program), "attaching is idempotent");
    }
}
