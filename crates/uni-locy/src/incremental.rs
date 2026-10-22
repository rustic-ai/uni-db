// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Which strata a hypothetical change can affect (`ASSUME`, `ABDUCE`, A4).
//!
//! `ASSUME { mutations } THEN { … }` and every `ABDUCE` candidate re-derive the
//! program in a mutated state. A stratum none of whose rules can see the change
//! derives exactly the facts it derived before the change, so those facts can
//! be reused instead of recomputed. This module decides *which* strata that is,
//! from the syntax alone: the mutation's write [`MutationFootprint`] against
//! each rule's read set, closed over rule references.
//!
//! # The criterion
//!
//! A stratum is **affected** when any of its rules
//!
//! * is **opaque** — its result can depend on state this analysis does not
//!   model: a subquery, `EXISTS` or pattern comprehension, a generator
//!   predicate, a neural-model invocation, a namespaced (plugin) function, a
//!   retrieval function, or a non-deterministic function; or
//! * **reads what the mutation writes** (see [`RuleReads::is_affected_by`]):
//!   a node pattern whose labels can match a node whose label membership
//!   changed (created, deleted, labelled, unlabelled), a relationship pattern
//!   whose types can match a created or deleted relationship, or a property
//!   key whose value may change on an existing entity; or
//! * **references** (`IS` or `IS NOT`) a rule in an affected stratum, or a
//!   rule the program does not hold.
//!
//! Every other stratum is unaffected, and its facts in the mutated state are
//! equal to its facts before the mutation. The argument: a rule's facts are a
//! function of the graph elements its patterns can match, the property values
//! its expressions read, and the facts of the rules it references. A mutation
//! that changes none of those inputs leaves the function's value unchanged.
//!
//! The analysis is conservative in every place it cannot be exact. A node
//! deletion is treated as changing every label and every relationship type
//! (the deleted node's other labels and its detached edges are not known
//! statically); `SET n = {…}` / `SET n += …` change every property; an
//! unanalysable clause (`CALL`, `WITH RECURSIVE`) makes the whole mutation
//! opaque. A function receiving an entity as a bare argument (`labels(n)`,
//! `keys(n)`, a custom UDF) may read any property and label of it, so the rule
//! reads all of them. Facts that name an entity hold only its id; the
//! entity's properties are read afresh when facts are hydrated, which the
//! re-evaluation does in the mutated state for reused facts too.
//!
//! The caller adds two conditions this module cannot see: the reused facts
//! must have been derived from the same graph state the mutation was applied
//! to, by the same program, under the same evaluation settings.

// Rust guideline compliant

use std::collections::{BTreeSet, HashMap, HashSet};

use uni_cypher::ast::{
    Clause, CypherLiteral, Expr, LabelExpr, MapProjectionItem, Pattern, PatternElement, Query,
    RemoveItem, SetItem,
};
use uni_cypher::locy_ast::{LocyExpr, RuleCondition, RuleOutput};

use crate::names::name_matches;
use crate::types::{CompiledClause, CompiledProgram, CompiledRule};

/// A set of names that may also be "every name".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum NameSet {
    /// No name.
    #[default]
    Empty,
    /// Exactly these names.
    Some(BTreeSet<String>),
    /// Every name, including ones not known statically.
    All,
}

impl NameSet {
    /// Adds one name.
    pub fn insert(&mut self, name: &str) {
        match self {
            Self::All => {}
            Self::Some(set) => {
                set.insert(name.to_string());
            }
            Self::Empty => *self = Self::Some(BTreeSet::from([name.to_string()])),
        }
    }

    /// Makes the set every name.
    pub fn set_all(&mut self) {
        *self = Self::All;
    }

    /// Adds every name of `other`.
    pub fn extend(&mut self, other: &Self) {
        match other {
            Self::Empty => {}
            Self::All => self.set_all(),
            Self::Some(names) => {
                for n in names {
                    self.insert(n);
                }
            }
        }
    }

    /// Whether the set holds no name.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Whether some name is in both sets (`All` meets anything non-empty).
    #[must_use]
    pub fn meets(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, _) | (_, Self::Empty) => false,
            (Self::All, _) | (_, Self::All) => true,
            (Self::Some(a), Self::Some(b)) => a.iter().any(|n| b.contains(n)),
        }
    }
}

/// What a mutation can change in the graph.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MutationFootprint {
    /// Whether any node's existence or label membership may change.
    ///
    /// A node created without a label sets this with [`Self::node_labels`]
    /// empty: it is visible to unlabelled node patterns only.
    pub nodes_changed: bool,
    /// Labels whose membership may change: labels of created nodes, labels
    /// added or removed, every label for a deleted node.
    pub node_labels: NameSet,
    /// Relationship types whose relationships may be created or deleted.
    pub edge_types: NameSet,
    /// Property keys whose value may change on an existing node or edge.
    pub properties: NameSet,
    /// An entity that existed before the mutation may be changed or removed
    /// (`SET`, `REMOVE`, `DELETE`, a `MERGE` action). When false the mutation
    /// only adds entities, so every pre-existing entity reads back unchanged.
    pub entities_modified: bool,
    /// The mutation holds a clause this analysis does not model: every
    /// stratum is affected.
    pub opaque: bool,
}

impl MutationFootprint {
    /// Whether the mutation can change nothing a rule reads.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.opaque
            && !self.nodes_changed
            && self.edge_types.is_empty()
            && self.properties.is_empty()
    }

    /// Whether every entity that existed before the mutation reads back
    /// unchanged after it (same labels, same properties, still present).
    ///
    /// Then facts hydrated before the mutation are still correctly hydrated.
    #[must_use]
    pub fn preserves_existing_entities(&self) -> bool {
        !self.opaque && !self.entities_modified && self.properties.is_empty()
    }

    /// Adds what `other` can change: the footprint of running both
    /// mutations, in either order.
    pub fn extend(&mut self, other: &Self) {
        self.nodes_changed |= other.nodes_changed;
        self.node_labels.extend(&other.node_labels);
        self.edge_types.extend(&other.edge_types);
        self.properties.extend(&other.properties);
        self.entities_modified |= other.entities_modified;
        self.opaque |= other.opaque;
    }

    fn change_everything_about_nodes(&mut self) {
        self.nodes_changed = true;
        self.node_labels.set_all();
        self.edge_types.set_all();
    }
}

/// Computes the footprint of a mutation query (an `ASSUME` block or an
/// `ABDUCE` candidate's Cypher).
///
/// A query that is not a single statement is opaque.
#[must_use]
pub fn query_footprint(query: &Query) -> MutationFootprint {
    match query {
        Query::Single(statement) => mutation_footprint(&statement.clauses),
        _ => MutationFootprint {
            opaque: true,
            ..MutationFootprint::default()
        },
    }
}

/// Whether a Cypher command can write: anything but a read-only statement.
///
/// Syntactic: a statement made only of `MATCH`, `WITH`, `UNWIND` and
/// `RETURN` clauses (or a `UNION` of such, or an `EXPLAIN`) reads; every
/// other clause, `CALL` included, may write.
#[must_use]
pub fn query_may_write(query: &Query) -> bool {
    match query {
        Query::Single(statement) => statement.clauses.iter().any(|c| {
            !matches!(
                c,
                Clause::Match(_) | Clause::With(_) | Clause::Unwind(_) | Clause::Return(_)
            )
        }),
        Query::Union { left, right, .. } => query_may_write(left) || query_may_write(right),
        Query::Explain(_) => false,
        _ => true,
    }
}

/// The footprint of a Cypher command run as a Locy command, when it may
/// write; `None` for a read-only statement.
///
/// The facts a later command of the same program reads must be derived again
/// in the state such a command leaves, for the strata the footprint can
/// affect.
#[must_use]
pub fn command_write_footprint(query: &Query) -> Option<MutationFootprint> {
    query_may_write(query).then(|| query_footprint(query))
}

/// Computes the footprint of a sequence of Cypher clauses.
#[must_use]
pub fn mutation_footprint(clauses: &[Clause]) -> MutationFootprint {
    let mut fp = MutationFootprint::default();
    let mut scope = Scope::default();
    for clause in clauses {
        footprint_clause(clause, &mut scope, &mut fp);
    }
    fp
}

/// Variables bound so far in a mutation, with the relationship types a
/// relationship variable can have.
#[derive(Debug, Default)]
struct Scope {
    bound: HashSet<String>,
    /// Relationship variable → its possible types; `All` when untyped or
    /// bound to a variable-length pattern (a list of relationships).
    rel_types: HashMap<String, NameSet>,
}

impl Scope {
    fn bind_pattern(&mut self, pattern: &Pattern) {
        for path in &pattern.paths {
            if let Some(v) = &path.variable {
                self.bound.insert(v.clone());
            }
            self.bind_elements(&path.elements);
        }
    }

    fn bind_elements(&mut self, elements: &[PatternElement]) {
        for element in elements {
            match element {
                PatternElement::Node(n) => {
                    if let Some(v) = &n.variable {
                        self.bound.insert(v.clone());
                    }
                }
                PatternElement::Relationship(r) => {
                    if let Some(v) = &r.variable {
                        self.bound.insert(v.clone());
                        let mut types = label_set(&r.types);
                        if r.range.is_some() {
                            types.set_all();
                        }
                        self.rel_types.entry(v.clone()).or_default().extend(&types);
                    }
                }
                PatternElement::Parenthesized { pattern, .. } => {
                    if let Some(v) = &pattern.variable {
                        self.bound.insert(v.clone());
                    }
                    self.bind_elements(&pattern.elements);
                }
            }
        }
    }
}

/// The names of a label or type expression; `All` when it is empty (no
/// constraint: every label or type).
fn label_set(labels: &LabelExpr) -> NameSet {
    match labels {
        LabelExpr::Empty => NameSet::All,
        LabelExpr::Conjunction(names) | LabelExpr::Disjunction(names) => {
            let mut set = NameSet::Empty;
            for n in names {
                set.insert(n);
            }
            set
        }
    }
}

fn footprint_clause(clause: &Clause, scope: &mut Scope, fp: &mut MutationFootprint) {
    match clause {
        Clause::Match(m) => scope.bind_pattern(&m.pattern),
        Clause::Unwind(u) => {
            scope.bound.insert(u.variable.clone());
        }
        Clause::With(w) => {
            for item in &w.items {
                if let uni_cypher::ast::ReturnItem::Expr {
                    alias: Some(alias), ..
                } = item
                {
                    scope.bound.insert(alias.clone());
                }
            }
        }
        Clause::Return(_) => {}
        Clause::Create(c) => {
            created_elements(&c.pattern, scope, fp);
            scope.bind_pattern(&c.pattern);
        }
        Clause::Merge(m) => {
            created_elements(&m.pattern, scope, fp);
            scope.bind_pattern(&m.pattern);
            for item in m.on_match.iter().chain(&m.on_create) {
                fp.entities_modified = true;
                set_item(item, fp);
            }
        }
        Clause::Set(s) => {
            fp.entities_modified = true;
            for item in &s.items {
                set_item(item, fp);
            }
        }
        Clause::Remove(r) => {
            fp.entities_modified = true;
            for item in &r.items {
                match item {
                    RemoveItem::Property(expr) => property_target(expr, fp),
                    RemoveItem::Labels { labels, .. } => {
                        fp.nodes_changed = true;
                        for l in labels {
                            fp.node_labels.insert(l);
                        }
                    }
                }
            }
        }
        Clause::Delete(d) => {
            fp.entities_modified = true;
            for item in &d.items {
                match item {
                    Expr::Variable(v) => match scope.rel_types.get(v) {
                        // A relationship variable: only its types lose an edge.
                        Some(types) => fp.edge_types.extend(types),
                        // A node (or a path, or a variable this analysis did
                        // not see bound): its other labels and its edges are
                        // unknown.
                        None => fp.change_everything_about_nodes(),
                    },
                    _ => fp.change_everything_about_nodes(),
                }
            }
        }
        Clause::Foreach(f) => {
            scope.bound.insert(f.variable.clone());
            for inner in &f.body {
                footprint_clause(inner, scope, fp);
            }
        }
        // A procedure may write anything; a recursive CTE or a subquery is not
        // modelled here.
        Clause::Call(_) | Clause::WithRecursive(_) => fp.opaque = true,
    }
}

/// Records what a `CREATE` / `MERGE` pattern may create.
fn created_elements(pattern: &Pattern, scope: &Scope, fp: &mut MutationFootprint) {
    fn walk(elements: &[PatternElement], scope: &Scope, fp: &mut MutationFootprint) {
        for element in elements {
            match element {
                PatternElement::Node(n) => {
                    let already_bound =
                        n.variable.as_ref().is_some_and(|v| scope.bound.contains(v));
                    if !already_bound {
                        fp.nodes_changed = true;
                        if let LabelExpr::Conjunction(names) | LabelExpr::Disjunction(names) =
                            &n.labels
                        {
                            for l in names {
                                fp.node_labels.insert(l);
                            }
                        }
                    }
                }
                PatternElement::Relationship(r) => fp.edge_types.extend(&label_set(&r.types)),
                PatternElement::Parenthesized { pattern, .. } => walk(&pattern.elements, scope, fp),
            }
        }
    }
    for path in &pattern.paths {
        walk(&path.elements, scope, fp);
    }
}

fn set_item(item: &SetItem, fp: &mut MutationFootprint) {
    match item {
        SetItem::Property { expr, .. } => property_target(expr, fp),
        SetItem::Labels { labels, .. } => {
            fp.nodes_changed = true;
            for l in labels {
                fp.node_labels.insert(l);
            }
        }
        // `SET n = {…}` replaces every property; `SET n += m` may add any key.
        SetItem::Variable { .. } | SetItem::VariablePlus { .. } => fp.properties.set_all(),
    }
}

/// The property a `SET`/`REMOVE` target names; every property when the
/// target is not a plain `x.key`.
fn property_target(expr: &Expr, fp: &mut MutationFootprint) {
    match expr {
        Expr::Property(_, key) => fp.properties.insert(key),
        _ => fp.properties.set_all(),
    }
}

/// What a rule's own body can read from the graph.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleReads {
    /// Labels its node patterns and label checks constrain to; `All` when a
    /// node pattern has no label (it sees every node).
    pub labels: NameSet,
    /// Relationship types its relationship patterns constrain to; `All` when
    /// one is untyped.
    pub edge_types: NameSet,
    /// Property keys it reads.
    pub properties: NameSet,
    /// The body can depend on state this analysis does not model.
    pub opaque: bool,
}

impl RuleReads {
    /// Whether a mutation with footprint `fp` can change what this body reads.
    #[must_use]
    pub fn is_affected_by(&self, fp: &MutationFootprint) -> bool {
        if self.opaque || fp.opaque {
            return true;
        }
        // A node whose membership changed is seen by every unlabelled pattern
        // and by every pattern naming one of its changed labels.
        let nodes = fp.nodes_changed
            && (matches!(self.labels, NameSet::All) || self.labels.meets(&fp.node_labels));
        nodes || self.edge_types.meets(&fp.edge_types) || self.properties.meets(&fp.properties)
    }
}

/// Functions whose result depends on more than their arguments.
///
/// Recomputed in every hypothetical state, never reused: a reused fact would
/// carry the value of the earlier call.
const NON_DETERMINISTIC: &[&str] = &["rand", "randomuuid", "timestamp"];

/// Temporal constructors that read the clock when called without arguments.
const CLOCK_WHEN_NULLARY: &[&str] = &["date", "datetime", "time", "localtime", "localdatetime"];

/// Functions that score against an index or corpus: a change to *other*
/// entities (corpus statistics, the index) can change their value.
const RETRIEVAL: &[&str] = &["similar_to", "semantic_match", "vector_similarity", "score"];

/// Functions that read every label or property of an entity argument.
const INTROSPECTIVE: &[&str] = &["labels", "keys", "properties"];

/// Functions that read no property or label of a bare entity argument: they
/// read its id, count or collect it, or take a non-entity value.
const ID_ONLY: &[&str] = &[
    "id",
    "elementid",
    "type",
    "count",
    "collect",
    "size",
    "length",
    "coalesce",
    "head",
    "last",
    "tail",
    "reverse",
    "isnull",
    "isnotnull",
    "sum",
    "avg",
    "min",
    "max",
    "msum",
    "mmax",
    "mmin",
    "mcount",
    "mnor",
    "mprod",
    "abs",
    "ceil",
    "floor",
    "round",
    "sqrt",
    "exp",
    "log",
    "log10",
    "sign",
    "tointeger",
    "tofloat",
    "toboolean",
    "range",
];

/// Collects what one rule's clauses read.
#[must_use]
pub fn rule_reads(rule: &CompiledRule) -> RuleReads {
    let mut reads = RuleReads::default();
    for clause in &rule.clauses {
        clause_reads(clause, &mut reads);
    }
    reads
}

fn clause_reads(clause: &CompiledClause, reads: &mut RuleReads) {
    if !clause.model_invocations.is_empty() {
        reads.opaque = true;
    }
    pattern_reads(&clause.match_pattern, reads);
    for cond in &clause.where_conditions {
        match cond {
            // A dependency, closed over by `affected_strata`.
            RuleCondition::IsReference(_) => {}
            RuleCondition::Expression(e) => expr_reads(e, reads),
            // A table-valued plugin may read anything.
            RuleCondition::Generator(_) => reads.opaque = true,
        }
    }
    for along in &clause.along {
        locy_expr_reads(&along.expr, reads);
    }
    for fold in &clause.fold {
        expr_reads(&fold.aggregate, reads);
    }
    for e in clause.require.iter().chain(&clause.having) {
        expr_reads(e, reads);
    }
    if let Some(best) = &clause.best_by {
        for item in &best.items {
            expr_reads(&item.expr, reads);
        }
    }
    match &clause.output {
        RuleOutput::Yield(y) => {
            for item in &y.items {
                expr_reads(&item.expr, reads);
            }
        }
        // A DERIVE rule derives no facts the fixpoint stores; its body runs
        // when the DERIVE command does. Treated as opaque so a stratum holding
        // one is never skipped on its account.
        RuleOutput::Derive(_) => reads.opaque = true,
    }
}

fn pattern_reads(pattern: &Pattern, reads: &mut RuleReads) {
    for path in &pattern.paths {
        // A named path's value carries its nodes and relationships whole.
        if path.variable.is_some() {
            reads.properties.set_all();
        }
        elements_reads(&path.elements, reads);
    }
}

fn elements_reads(elements: &[PatternElement], reads: &mut RuleReads) {
    for element in elements {
        match element {
            PatternElement::Node(n) => {
                reads.labels.extend(&label_set(&n.labels));
                if let Some(props) = &n.properties {
                    property_map_reads(props, reads);
                }
                if let Some(w) = &n.where_clause {
                    expr_reads(w, reads);
                }
            }
            PatternElement::Relationship(r) => {
                reads.edge_types.extend(&label_set(&r.types));
                // A variable-length relationship variable is a list of whole
                // relationships.
                if r.range.is_some() && r.variable.is_some() {
                    reads.properties.set_all();
                }
                if let Some(props) = &r.properties {
                    property_map_reads(props, reads);
                }
                if let Some(w) = &r.where_clause {
                    expr_reads(w, reads);
                }
            }
            PatternElement::Parenthesized { pattern, .. } => {
                if pattern.variable.is_some() {
                    reads.properties.set_all();
                }
                elements_reads(&pattern.elements, reads);
            }
        }
    }
}

/// An inline `{key: value}` constraint reads each key; any other map
/// expression (a parameter) may constrain any key.
fn property_map_reads(props: &Expr, reads: &mut RuleReads) {
    match props {
        Expr::Map(entries) => {
            for (key, value) in entries {
                reads.properties.insert(key);
                expr_reads(value, reads);
            }
        }
        other => {
            reads.properties.set_all();
            expr_reads(other, reads);
        }
    }
}

fn locy_expr_reads(expr: &LocyExpr, reads: &mut RuleReads) {
    match expr {
        LocyExpr::PrevRef(_) => {}
        LocyExpr::Cypher(e) => expr_reads(e, reads),
        LocyExpr::BinaryOp { left, right, .. } => {
            locy_expr_reads(left, reads);
            locy_expr_reads(right, reads);
        }
        LocyExpr::UnaryOp(_, e) => locy_expr_reads(e, reads),
    }
}

fn expr_reads(expr: &Expr, reads: &mut RuleReads) {
    match expr {
        Expr::Property(_, key) => reads.properties.insert(key),
        Expr::FunctionCall { name, args, .. } => {
            let lower = name.to_ascii_lowercase();
            let opaque = lower.contains('.')
                || NON_DETERMINISTIC.contains(&lower.as_str())
                || RETRIEVAL.contains(&lower.as_str())
                || (args.is_empty() && CLOCK_WHEN_NULLARY.contains(&lower.as_str()));
            if opaque {
                reads.opaque = true;
            } else if INTROSPECTIVE.contains(&lower.as_str()) {
                reads.labels.set_all();
                reads.properties.set_all();
            } else if !ID_ONLY.contains(&lower.as_str())
                && args.iter().any(|a| matches!(a, Expr::Variable(_)))
            {
                // An entity passed whole to an unknown function: it may read
                // any of the entity's properties or labels.
                reads.labels.set_all();
                reads.properties.set_all();
            }
        }
        Expr::Exists { .. }
        | Expr::CountSubquery(_)
        | Expr::CollectSubquery(_)
        | Expr::PatternComprehension { .. } => reads.opaque = true,
        Expr::MapProjection { items, .. } => {
            for item in items {
                match item {
                    MapProjectionItem::Property(key) => reads.properties.insert(key),
                    MapProjectionItem::AllProperties | MapProjectionItem::Variable(_) => {
                        reads.properties.set_all();
                    }
                    MapProjectionItem::LiteralEntry(..) => {}
                }
            }
        }
        Expr::ArrayIndex { index, .. } => match index.as_ref() {
            // `n['key']` on an entity reads that key.
            Expr::Literal(CypherLiteral::String(key)) => reads.properties.insert(key),
            Expr::Literal(CypherLiteral::Integer(_)) => {}
            _ => reads.properties.set_all(),
        },
        Expr::ValidAt { .. } => reads.properties.set_all(),
        Expr::LabelCheck { labels, .. } => {
            for l in labels {
                reads.labels.insert(l);
            }
        }
        _ => {}
    }
    expr.for_each_child(&mut |child| expr_reads(child, reads));
}

/// The rule names a clause references through `IS` / `IS NOT`.
fn referenced_rules(rule: &CompiledRule) -> Vec<String> {
    rule.clauses
        .iter()
        .flat_map(|c| &c.where_conditions)
        .filter_map(|c| match c {
            RuleCondition::IsReference(r) => Some(r.rule_name.to_string()),
            _ => None,
        })
        .collect()
}

/// For each stratum of `program`, by position, whether a mutation with
/// footprint `fp` can change its facts. See the module docs for the
/// criterion.
#[must_use]
pub fn affected_strata(program: &CompiledProgram, fp: &MutationFootprint) -> Vec<bool> {
    let mut affected = vec![false; program.strata.len()];
    // Rule name → stratum position, for reference closure.
    let positions: Vec<(&str, usize)> = program
        .strata
        .iter()
        .enumerate()
        .flat_map(|(i, s)| s.rules.iter().map(move |r| (r.name.as_str(), i)))
        .collect();
    // `depends_on` holds stratum ids; used only when ids are unique.
    let mut id_to_pos: HashMap<usize, usize> = HashMap::new();
    let ids_unique = program
        .strata
        .iter()
        .enumerate()
        .all(|(pos, s)| id_to_pos.insert(s.id, pos).is_none());

    // Strata are in dependency order, so one forward pass closes over
    // references to earlier strata; a reference into the same stratum is
    // covered by deciding the stratum as a whole.
    for (pos, stratum) in program.strata.iter().enumerate() {
        let direct = stratum
            .rules
            .iter()
            .any(|r| rule_reads(r).is_affected_by(fp));
        let through_reference = stratum.rules.iter().any(|r| {
            referenced_rules(r).iter().any(|reference| {
                let mut targets = positions
                    .iter()
                    .filter(|(name, _)| name_matches(name, reference))
                    .peekable();
                // A reference the program does not hold: not provably unaffected.
                if targets.peek().is_none() {
                    return true;
                }
                targets.any(|&(_, target)| target != pos && affected[target])
            })
        });
        let through_depends_on = ids_unique
            && stratum
                .depends_on
                .iter()
                .any(|id| id_to_pos.get(id).is_some_and(|&p| p != pos && affected[p]));
        affected[pos] = direct || through_reference || through_depends_on;
    }
    affected
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile(src: &str) -> CompiledProgram {
        let program = uni_cypher::parse_locy(src).expect("parse");
        crate::compile(&program).expect("compile")
    }

    fn mutation(src: &str) -> MutationFootprint {
        let query = uni_cypher::parse(src).expect("parse mutation");
        query_footprint(&query)
    }

    /// Affected flag per rule name (each test program has one rule per stratum).
    fn affected_by_rule(program: &CompiledProgram, fp: &MutationFootprint) -> Vec<(String, bool)> {
        let flags = affected_strata(program, fp);
        let mut out: Vec<(String, bool)> = program
            .strata
            .iter()
            .zip(flags)
            .flat_map(|(s, f)| s.rules.iter().map(move |r| (r.name.clone(), f)))
            .collect();
        out.sort();
        out
    }

    const LAYERED: &str = "\
        CREATE RULE knows AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b\n\
        CREATE RULE reach AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b\n\
        CREATE RULE reach AS MATCH (a:Person)-[:KNOWS]->(m:Person) WHERE m IS reach TO b YIELD KEY a, KEY b\n\
        CREATE RULE owns AS MATCH (c:Company)-[:OWNS]->(d:Company) YIELD KEY c, KEY d\n\
        CREATE RULE big AS MATCH (c:Company) WHERE c.size > 10 YIELD KEY c\n\
        CREATE RULE lonely AS MATCH (p:Person) WHERE p IS NOT knows YIELD KEY p\n";

    fn flag(rows: &[(String, bool)], name: &str) -> bool {
        rows.iter().find(|(n, _)| n == name).expect("rule").1
    }

    #[test]
    fn edge_creation_affects_only_rules_reading_its_type_and_their_dependants() {
        let program = compile(LAYERED);
        let fp = mutation(
            "MATCH (a:Person {name: 'x'}), (b:Person {name: 'y'}) CREATE (a)-[:KNOWS]->(b)",
        );
        assert!(!fp.nodes_changed, "{fp:?}");
        let rows = affected_by_rule(&program, &fp);
        assert!(flag(&rows, "knows"));
        assert!(flag(&rows, "reach"));
        // Reads Person nodes only, but references `knows` through IS NOT.
        assert!(flag(&rows, "lonely"));
        assert!(!flag(&rows, "owns"));
        assert!(!flag(&rows, "big"));
    }

    #[test]
    fn extended_footprint_affects_what_either_mutation_affects() {
        let program = compile(LAYERED);
        let mut fp = mutation(
            "MATCH (a:Person {name: 'x'}), (b:Person {name: 'y'}) CREATE (a)-[:KNOWS]->(b)",
        );
        fp.extend(&mutation("CREATE (:Company {size: 3})"));
        let rows = affected_by_rule(&program, &fp);
        assert!(flag(&rows, "knows"));
        assert!(flag(&rows, "big"));
        assert!(flag(&rows, "owns"));
        assert!(fp.preserves_existing_entities(), "{fp:?}");
        fp.extend(&mutation("MATCH (c:Company) SET c.size = 1"));
        assert!(!fp.preserves_existing_entities(), "{fp:?}");
        let mut opaque = MutationFootprint::default();
        opaque.extend(&MutationFootprint {
            opaque: true,
            ..MutationFootprint::default()
        });
        assert!(affected_strata(&program, &opaque).iter().all(|a| *a));
    }

    #[test]
    fn node_creation_affects_rules_reading_its_label() {
        let program = compile(LAYERED);
        let rows = affected_by_rule(&program, &mutation("CREATE (:Company {size: 3})"));
        assert!(flag(&rows, "owns"));
        assert!(flag(&rows, "big"));
        assert!(!flag(&rows, "knows"));
        assert!(!flag(&rows, "reach"));
        assert!(!flag(&rows, "lonely"));
    }

    #[test]
    fn unrelated_label_affects_nothing() {
        let program = compile(LAYERED);
        let fp = mutation("CREATE (:Audit {at: 1})-[:LOGGED]->(:Audit)");
        assert!(affected_strata(&program, &fp).iter().all(|a| !a));
    }

    #[test]
    fn property_set_affects_only_rules_reading_the_key() {
        let program = compile(LAYERED);
        let rows = affected_by_rule(
            &program,
            &mutation("MATCH (c:Company {name: 'acme'}) SET c.size = 99"),
        );
        assert!(flag(&rows, "big"));
        assert!(!flag(&rows, "owns"));
        assert!(!flag(&rows, "knows"));
        let rows = affected_by_rule(
            &program,
            &mutation("MATCH (c:Company {name: 'acme'}) SET c.note = 'x'"),
        );
        assert!(rows.iter().all(|(_, a)| !a), "{rows:?}");
    }

    #[test]
    fn node_deletion_affects_every_rule_reading_the_graph() {
        let program = compile(LAYERED);
        let fp = mutation("MATCH (c:Audit {id: 1}) DETACH DELETE c");
        assert!(affected_strata(&program, &fp).iter().all(|a| *a));
    }

    #[test]
    fn typed_relationship_deletion_affects_only_its_type() {
        let program = compile(LAYERED);
        let rows = affected_by_rule(
            &program,
            &mutation("MATCH (:Company)-[r:OWNS]->(:Company) DELETE r"),
        );
        assert!(flag(&rows, "owns"));
        assert!(!flag(&rows, "big"));
        assert!(!flag(&rows, "knows"));
        // An untyped relationship variable may be of any type.
        let rows = affected_by_rule(&program, &mutation("MATCH ()-[r]->() DELETE r"));
        assert!(flag(&rows, "knows") && flag(&rows, "owns"));
        assert!(!flag(&rows, "big"));
    }

    #[test]
    fn map_replacement_and_label_changes() {
        let program = compile(LAYERED);
        // `SET n = {...}` may change any key, `big` reads `size`.
        let rows = affected_by_rule(&program, &mutation("MATCH (n:Audit) SET n = {a: 1}"));
        assert!(flag(&rows, "big"));
        assert!(!flag(&rows, "knows"));
        // Adding the Company label to some node.
        let rows = affected_by_rule(&program, &mutation("MATCH (n:Audit) SET n:Company"));
        assert!(flag(&rows, "big") && flag(&rows, "owns"));
        assert!(!flag(&rows, "knows"));
    }

    #[test]
    fn unlabelled_pattern_sees_every_node_creation() {
        let program = compile("CREATE RULE any_node AS MATCH (n) WHERE n.v > 0 YIELD KEY n\n");
        assert!(affected_strata(&program, &mutation("CREATE (:Whatever)"))[0]);
        // An edge between existing nodes creates no node.
        let fp = mutation("MATCH (a:X), (b:X) CREATE (a)-[:R]->(b)");
        assert!(!affected_strata(&program, &fp)[0]);
    }

    #[test]
    fn opaque_constructs_are_always_affected() {
        let fp = mutation("CREATE (:Audit)");
        for src in [
            "CREATE RULE r AS MATCH (p:Person) WHERE rand() > 0.5 YIELD KEY p\n",
            "CREATE RULE r AS MATCH (p:Person) WHERE exists((p)-[:KNOWS]->()) YIELD KEY p\n",
            "CREATE RULE r AS MATCH (p:Person) YIELD KEY p, keys(p) AS k\n",
        ] {
            let program = compile(src);
            // `rand()` and `exists(...)` are opaque. `keys(p)` reads every
            // label, and creating any node changes label membership.
            assert!(affected_strata(&program, &fp)[0], "{src}");
        }
        // A procedure call in the mutation is opaque.
        let fp = mutation("CALL db.labels() YIELD label RETURN label");
        assert!(fp.opaque);
    }

    #[test]
    fn only_pure_additions_preserve_existing_entities() {
        assert!(mutation("CREATE (:Company {size: 3})").preserves_existing_entities());
        assert!(
            mutation("MATCH (a:P), (b:P) CREATE (a)-[:R {w: 1}]->(b)")
                .preserves_existing_entities()
        );
        for src in [
            "MATCH (a:P) SET a.x = 1",
            "MATCH (a:P) SET a:Q",
            "MATCH (a:P) REMOVE a:P",
            "MATCH ()-[r:R]->() DELETE r",
            "MERGE (a:P {k: 1}) ON MATCH SET a.k = 2",
            "CALL db.labels() YIELD label RETURN label",
        ] {
            assert!(!mutation(src).preserves_existing_entities(), "{src}");
        }
    }

    #[test]
    fn foreach_and_merge_are_walked() {
        let fp = mutation("FOREACH (i IN [1, 2] | CREATE (:Company {size: i}))");
        assert!(
            fp.nodes_changed
                && fp
                    .node_labels
                    .meets(&NameSet::Some(["Company".into()].into()))
        );
        let fp = mutation("MERGE (c:Company {name: 'x'}) ON MATCH SET c.size = 1");
        assert!(fp.properties.meets(&NameSet::Some(["size".into()].into())));
        assert!(fp.nodes_changed);
    }
}
