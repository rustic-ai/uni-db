// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Goal-directed evaluation of `QUERY` commands: the magic-sets rewrite.
//!
//! Locy evaluates bottom-up: every stratum runs to its fixpoint and the
//! commands read the published facts. A `QUERY reach WHERE a.uid = 'x'`
//! therefore derived the whole relation and filtered it afterwards — 76k
//! facts for a 19-row answer in the A1 measurement
//! (`docs/proposals/perf_class_measurements_2026_10_17.md`).
//!
//! [`rewrite_goal_directed`] rewrites such a program, before it is planned,
//! into an ordinary Locy program that derives only what the goal can reach.
//! The planner and the fixpoint run the result unchanged. The rewrite is the
//! classic magic-sets transformation (Bancilhon, Maier, Sagiv, Ullman 1986;
//! Beeri and Ramakrishnan 1991), specialized to Locy:
//!
//! * **Adornments are over KEY columns only, and only node columns.** A goal
//!   conjunct `c.prop = <literal | $param>` (or `c.prop IN [...]`) whose only
//!   variable is a node KEY column `c` of the queried rule binds `c`. A KEY
//!   position is *adornable* when the column is a node in every clause and
//!   every clause yields it as a bare variable, so a clause can be guarded by
//!   an IS reference on that variable.
//! * **A magic rule per adorned rule.** For a rule `r` adorned `α` there is a
//!   copy `__locy_gd{g}_r_α` and a magic rule `__locy_gdm{g}_r_α` holding the
//!   node tuples the copy is asked for. Every clause of the copy gains the
//!   guard `(bound head vars) IS __locy_gdm{g}_r_α`. The goal's magic rule is
//!   seeded by a clause `MATCH (c[:L]) WHERE <goal conjuncts> YIELD KEY c`.
//! * **Sideways information passing is through the MATCH pattern only.** In a
//!   clause, the variables bound at an IS reference are the head's bound
//!   variables plus every MATCH node connected to one of them by the
//!   pattern. A reference to an adorned callee whose KEY positions are bound
//!   this way calls the callee's copy, and the clause contributes a magic
//!   propagation clause: its MATCH paths that touch the bound variables, the
//!   guard, the filters over those variables, `YIELD KEY <call vars>`. Magic
//!   rules therefore read only the graph and other magic rules, never a copy
//!   or an original rule: the rewrite adds no recursion through a copy, so the
//!   rewritten program stratifies whenever the original does. The binding plan
//!   (#300) already binds MATCH variables before any reference, so this SIP
//!   agrees with it for every order of the WHERE conjuncts. Bindings that
//!   flow out of one IS reference into another (non-linear recursion's second
//!   call) are not passed; that call reads its callee in full, which is
//!   correct and only less selective.
//!
//! # When the rewrite applies
//!
//! Magic sets preserve answers for positive Datalog; the restriction of an
//! adorned rule is exactly the facts whose bound KEY values the goal can
//! reach. The rewrite applies a construct only where it has an argument that
//! the answer is unchanged, and otherwise answers the goal by full
//! evaluation ([`Fallback`]):
//!
//! | Construct in an adorned rule's stratum | Treatment |
//! |---|---|
//! | plain set-semantic rule, recursive or not | adorned |
//! | `IS` to a callee that is unsafe or unbound | callee read in full (original rule) |
//! | `IS NOT` | target read in full, never adorned (the stratified-negation-safe variant) |
//! | FOLD / BEST BY / post-FOLD WHERE / REQUIRE, non-recursive stratum | adorned: a guard on KEY columns keeps or drops whole KEY groups, so every per-group operation is unchanged |
//! | the same in a recursive stratum | fallback: intermediate iterations differ, and BEST BY pruning or a per-iteration REQUIRE is not order-independent in general |
//! | ALONG (per-derivation-path facts, #159) | fallback |
//! | PROB columns, MNOR / MPROD, `IS NOT` of a PROB rule | fallback: shared-proof and BDD bookkeeping is per evaluated group set |
//! | PRIORITY, generators, neural models, DERIVE output | fallback |
//!
//! Only top-level `QUERY` commands are rewritten. `EXPLAIN RULE` (its
//! derivation trees would name the copies and the magic guards), `DERIVE`,
//! `ABDUCE` and every command inside an `ASSUME` are answered by full
//! evaluation. The QUERY keeps its whole WHERE, so the seed only has to be a
//! superset of the goal's bindings, and a goal conjunct the rewrite does not
//! understand is still applied.
//!
//! # Observable effects
//!
//! QUERY answers are unchanged. The copies and magic rules are hidden from
//! `LocyResult`'s derived facts (see [`is_goal_directed_rule`]), and an
//! original rule evaluated only for rewritten goals is not evaluated at all.
//! What happens to such a rule depends on the mode:
//!
//! * [`GoalDirected::Auto`], the default: it is derived on first access to
//!   the result's derived facts, against the snapshot the evaluation read,
//!   by [`materialization_program`], so the derived facts are exactly what
//!   [`GoalDirected::Off`] gives. Programs for which that cannot hold are
//!   evaluated in full ([`AutoFallback`]).
//! * [`GoalDirected::On`]: it is absent from the derived facts — never
//!   present with a partial relation.
//!
//! [`GoalDirected::Shadow`] keeps every original rule, so the derived facts
//! are exactly what they are with the rewrite off while QUERY reads the
//! copies; it exists to verify the rewrite. The profile shows the hidden
//! rules, which is how to see that the rewrite engaged.

// Rust guideline compliant

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use uni_common::Value;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, LabelExpr, NodePattern, Pattern};
use uni_cypher::ast::{PathPattern, PatternElement};
use uni_cypher::locy_ast::{
    IsReference, LocyYieldItem, QualifiedName, RuleCondition, RuleOutput, YieldClause,
    resolve_yield_column_names,
};

use crate::binding_plan::{ClauseBinding, ColumnKind, ColumnKinds, VarSource};
use crate::compiler::free_variables;
use crate::is_binding::binding_sequence;
use crate::names::{name_matches, resolve_unique};
use crate::types::YieldColumn;
use crate::types::{CompiledClause, CompiledCommand, CompiledProgram, CompiledRule, Stratum};

/// Prefix of every rule the rewrite adds; see [`is_goal_directed_rule`].
pub const GOAL_DIRECTED_PREFIX: &str = "__locy_gd";

/// Whether and how `QUERY` commands are answered goal-directed.
///
/// See the [module documentation](self) for the rewrite and its effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GoalDirected {
    /// Every rule is evaluated in full (the behavior before
    /// [`Self::Auto`] became the default).
    Off,
    /// The default. Qualifying `QUERY` commands are answered by the
    /// rewritten program and rules only they needed are not evaluated, as in
    /// [`Self::On`], but `LocyResult`'s derived facts are unchanged: such a
    /// rule is derived on first access, against the snapshot the evaluation
    /// read (`uni_locy::LazyDerived`).
    ///
    /// A program for which the left-out rules could not be derived later
    /// with the facts a full evaluation would give is evaluated in full, as
    /// [`Self::Off`] does: see [`auto_fallback`]. The runtime adds the cases
    /// it alone can see (an evaluation inside a transaction, whose state
    /// cannot be held past the call). [`rewrite_goal_directed`] treats `Auto`
    /// as `On`; the caller decides whether `Auto` applies.
    #[default]
    Auto,
    /// Qualifying `QUERY` commands are answered by the rewritten program, and
    /// rules only they needed are not evaluated, nor published in
    /// `LocyResult`'s derived facts.
    On,
    /// As [`Self::On`], but every original rule is still evaluated and
    /// published, so `derived` is unchanged. Costs more than [`Self::Off`];
    /// it verifies that the rewritten answers equal the full ones.
    Shadow,
}

/// Why [`GoalDirected::Auto`] evaluates a program in full.
///
/// `Auto` leaves rules out only when they can be derived later, against the
/// snapshot the evaluation read, with exactly the facts full evaluation
/// derives. The program-level cases where that does not hold:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoFallback {
    /// A command writes (a `DERIVE`, a Cypher write). The derived facts are
    /// read with their entities as the commands left them, a state no
    /// snapshot taken before the commands holds.
    WriteCommand,
    /// A command records provenance (`EXPLAIN RULE`), which can change the
    /// facts' probabilities in a way an evaluation without it does not
    /// reproduce.
    Provenance,
    /// The evaluation's state cannot be held past the call: an evaluation
    /// inside a transaction (its private writes keep changing, and its reads
    /// must reach its own conflict check), or one with no database handle to
    /// derive with later. Reported by the runtime, never by
    /// [`auto_fallback`].
    NoSnapshot,
}

/// Why [`GoalDirected::Auto`] evaluates `program` in full, if a reason
/// visible in the program itself applies; see [`AutoFallback`].
///
/// # Examples
///
/// ```
/// use uni_locy::magic::{AutoFallback, auto_fallback};
///
/// let src = "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
///            QUERY r WHERE a.name = 'x' RETURN b";
/// let program = uni_locy::compile(&uni_cypher::parse_locy(src).unwrap()).unwrap();
/// assert_eq!(auto_fallback(&program), None);
/// ```
#[must_use]
pub fn auto_fallback(program: &CompiledProgram) -> Option<AutoFallback> {
    let writes = program.commands.iter().any(|c| match c {
        CompiledCommand::DeriveCommand(_) => true,
        CompiledCommand::Cypher(q) => crate::incremental::query_may_write(q),
        _ => false,
    });
    if writes {
        return Some(AutoFallback::WriteCommand);
    }
    if program
        .commands
        .iter()
        .any(|c| matches!(c, CompiledCommand::ExplainRule(_)))
    {
        return Some(AutoFallback::Provenance);
    }
    None
}

/// The program that derives `rules` of `program`: the strata of `rules` and
/// of every rule they reference, and no commands.
///
/// This is what derives the rules [`GoalDirected::Auto`] left out. `program`
/// is the program as compiled, before [`rewrite_goal_directed`].
#[must_use]
pub fn materialization_program(program: &CompiledProgram, rules: &[String]) -> CompiledProgram {
    let mut out = program.clone();
    out.commands.clear();
    let evaluated: Vec<&CompiledRule> = program.strata.iter().flat_map(|s| &s.rules).collect();
    let mut needed: HashSet<String> = HashSet::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = rules.iter().cloned().collect();
    while let Some(reference) = queue.pop_front() {
        if !seen.insert(reference.clone()) {
            continue;
        }
        for rule in &evaluated {
            if name_matches(&rule.name, &reference) && needed.insert(rule.name.clone()) {
                queue.extend(references(rule));
            }
        }
    }
    for stratum in &mut out.strata {
        stratum.rules.retain(|r| needed.contains(&r.name));
    }
    out.strata.retain(|s| !s.rules.is_empty());
    out
}

/// Whether `name` is a rule the goal-directed rewrite added.
///
/// Such rules are an evaluation detail: the runtime leaves them out of
/// `LocyResult::derived`.
///
/// # Examples
///
/// ```
/// use uni_locy::magic::is_goal_directed_rule;
///
/// assert!(is_goal_directed_rule("__locy_gdm0_reach_bf"));
/// assert!(!is_goal_directed_rule("reach"));
/// ```
#[must_use]
pub fn is_goal_directed_rule(name: &str) -> bool {
    name.starts_with(GOAL_DIRECTED_PREFIX)
}

/// What the rewrite did with each `QUERY` command it considered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GoalDirectedReport {
    /// One entry per top-level `QUERY`, in command order.
    pub goals: Vec<GoalReport>,
}

impl GoalDirectedReport {
    /// The number of goals answered by the rewritten program.
    #[must_use]
    pub fn rewritten(&self) -> usize {
        self.goals
            .iter()
            .filter(|g| matches!(g.outcome, GoalOutcome::Rewritten { .. }))
            .count()
    }

    /// The original rule each generated rule stands for: every copy and
    /// magic rule of every rewritten goal, mapped to the catalog name of the
    /// rule it was generated from.
    ///
    /// For reporting evaluation detail (an interrupted rule, say) in terms of
    /// the program's own rules.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use uni_locy::magic::{GoalDirected, rewrite_goal_directed};
    ///
    /// let src = "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
    ///            QUERY reach WHERE a.name = 'x' RETURN b";
    /// let program = uni_locy::compile(&uni_cypher::parse_locy(src).unwrap()).unwrap();
    /// let (_, report) = rewrite_goal_directed(program, GoalDirected::On, &HashMap::new());
    /// let origins = report.generated_rule_origins();
    /// assert_eq!(origins["__locy_gd0_reach_bf"], "reach");
    /// assert_eq!(origins["__locy_gdm0_reach_bf"], "reach");
    /// ```
    #[must_use]
    pub fn generated_rule_origins(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for goal in &self.goals {
            let GoalOutcome::Rewritten { adorned, .. } = &goal.outcome else {
                continue;
            };
            for entry in adorned {
                let Some((rule, tag)) = entry.rsplit_once('/') else {
                    continue;
                };
                let suffix = format!("{}_{}_{tag}", goal.command, sanitize(rule));
                out.insert(format!("{GOAL_DIRECTED_PREFIX}{suffix}"), rule.to_string());
                out.insert(format!("{GOAL_DIRECTED_PREFIX}m{suffix}"), rule.to_string());
            }
        }
        out
    }
}

/// The rewrite's decision for one `QUERY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalReport {
    /// Index of the command in `CompiledProgram::commands`.
    pub command: usize,
    /// The queried rule, as the catalog keys it.
    pub rule: String,
    /// Rewritten, or why not.
    pub outcome: GoalOutcome,
}

/// Whether a goal was rewritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalOutcome {
    /// The goal reads `answered_by`, a copy of its rule.
    Rewritten {
        /// The copy the command now reads.
        answered_by: String,
        /// Every adorned rule, as `rule/adornment` (`b` bound, `f` free per
        /// KEY column).
        adorned: Vec<String>,
    },
    /// The goal is answered by full evaluation.
    Fallback(Fallback),
}

/// Why a goal is answered by full evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fallback {
    /// No WHERE conjunct binds an adornable KEY column.
    NoBoundKey,
    /// The queried rule's stratum holds a construct the rewrite does not
    /// handle.
    Unsafe {
        /// The offending rule.
        rule: String,
        /// The construct.
        reason: Unsafe,
    },
    /// The queried rule is not in the evaluated strata.
    NotEvaluated,
    /// A generated rule name is already taken.
    NameCollision(String),
    /// The rewritten program failed its binding check; the message is the
    /// compile error. Never expected, and reported rather than raised.
    Binding(String),
}

/// A construct for which the rewrite has no answer-preservation argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsafe {
    /// A PROB column.
    Prob,
    /// An MNOR or MPROD aggregate.
    ProbAggregate,
    /// `IS NOT` of a rule with a PROB column (a complement).
    ProbComplement,
    /// FOLD, BEST BY, post-FOLD WHERE or REQUIRE in a recursive stratum.
    RecursiveAggregation,
    /// ALONG: one fact per derivation path.
    Along,
    /// PRIORITY.
    Priority,
    /// A generator condition.
    Generator,
    /// A neural model invocation.
    Neural,
    /// A DERIVE rule.
    DeriveOutput,
    /// A clause without a computed binding plan.
    BindingPending,
}

/// Rewrites qualifying `QUERY` commands of `program` for goal-directed evaluation.
///
/// Returns the program to evaluate and what happened to each `QUERY`. With
/// [`GoalDirected::Off`], or when no goal qualifies, the program comes back
/// unchanged. [`GoalDirected::Auto`] rewrites as [`GoalDirected::On`] does:
/// deciding whether `Auto` applies to a program ([`auto_fallback`]) and
/// deriving the rules it leaves out is the caller's part. `params` are the evaluation's parameters: a `$param` in a goal
/// conjunct seeds only when its value is a string, an integer or a boolean
/// (or a list of those for `IN`).
///
/// The input must have its binding plans attached, as the compiler and
/// `attach_binding_plans` leave it; a clause without one is never adorned.
/// The rewritten program has its plans recomputed. Should that fail — a
/// defect, since every generated clause is a guarded copy or a guarded
/// projection of a clause that bound — the original program is returned and
/// every goal reports [`Fallback::Binding`].
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use uni_locy::magic::{GoalDirected, rewrite_goal_directed};
///
/// let src = "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
///            CREATE RULE reach AS MATCH (a:N)-[:E]->(m:N) WHERE m IS reach TO b YIELD KEY a, KEY b\n\
///            QUERY reach WHERE a.name = 'x' RETURN b";
/// let program = uni_locy::compile(&uni_cypher::parse_locy(src).unwrap()).unwrap();
/// let (rewritten, report) = rewrite_goal_directed(program, GoalDirected::On, &HashMap::new());
/// assert_eq!(report.rewritten(), 1);
/// assert!(rewritten.strata.iter().all(|s| s.rules.iter().all(|r| r.name != "reach")));
/// ```
#[must_use]
pub fn rewrite_goal_directed(
    program: CompiledProgram,
    mode: GoalDirected,
    params: &HashMap<String, Value>,
) -> (CompiledProgram, GoalDirectedReport) {
    if mode == GoalDirected::Off {
        return (program, GoalDirectedReport::default());
    }
    let ctx = Context::new(&program);
    let mut report = GoalDirectedReport::default();
    let mut rewrites: Vec<(usize, GoalRewrite)> = Vec::new();
    let mut taken: HashSet<String> = program.rule_catalog.keys().cloned().collect();
    for (index, command) in program.commands.iter().enumerate() {
        let CompiledCommand::GoalQuery(query) = command else {
            continue;
        };
        let written = query.rule_name.to_string();
        let rule = resolve_unique(program.rule_catalog.keys().map(String::as_str), &written)
            .unwrap_or(written.as_str())
            .to_string();
        let outcome = match rewrite_goal(&ctx, index, &rule, query.where_expr.as_ref(), params) {
            Ok(rewrite) => match rewrite.rules.iter().find(|r| taken.contains(&r.name)) {
                Some(clash) => GoalOutcome::Fallback(Fallback::NameCollision(clash.name.clone())),
                None => {
                    taken.extend(rewrite.rules.iter().map(|r| r.name.clone()));
                    let outcome = GoalOutcome::Rewritten {
                        answered_by: rewrite.answered_by.clone(),
                        adorned: rewrite.adorned.clone(),
                    };
                    rewrites.push((index, rewrite));
                    outcome
                }
            },
            Err(fallback) => GoalOutcome::Fallback(fallback),
        };
        report.goals.push(GoalReport {
            command: index,
            rule,
            outcome,
        });
    }
    if rewrites.is_empty() {
        return (program, report);
    }
    match assemble(&program, rewrites, mode) {
        Ok(rewritten) => (rewritten, report),
        Err(message) => {
            for goal in &mut report.goals {
                if matches!(goal.outcome, GoalOutcome::Rewritten { .. }) {
                    goal.outcome = GoalOutcome::Fallback(Fallback::Binding(message.clone()));
                }
            }
            (program, report)
        }
    }
}

// ── Analysis ────────────────────────────────────────────────────────────────

/// What the rewrite needs to know about the input program.
struct Context<'a> {
    program: &'a CompiledProgram,
    kinds: ColumnKinds,
    /// Index into `program.strata` of each evaluated rule.
    stratum_of: HashMap<&'a str, usize>,
    /// Per stratum, the first unsafe construct of any of its rules.
    unsafe_stratum: Vec<Option<(String, Unsafe)>>,
}

impl<'a> Context<'a> {
    fn new(program: &'a CompiledProgram) -> Self {
        let mut stratum_of = HashMap::new();
        for (index, stratum) in program.strata.iter().enumerate() {
            for rule in &stratum.rules {
                stratum_of.entry(rule.name.as_str()).or_insert(index);
            }
        }
        let unsafe_stratum = program
            .strata
            .iter()
            .map(|s| stratum_unsafe(s, &program.rule_catalog))
            .collect();
        Self {
            program,
            kinds: ColumnKinds::infer(&program.rule_catalog),
            stratum_of,
            unsafe_stratum,
        }
    }

    fn rule(&self, name: &str) -> Option<&'a CompiledRule> {
        self.program.rule_catalog.get(name)
    }

    /// The catalog key an as-written reference names, when unique.
    fn resolve(&self, written: &str) -> Option<&'a str> {
        resolve_unique(
            self.program.rule_catalog.keys().map(String::as_str),
            written,
        )
    }

    /// Why `rule` cannot be adorned, if it cannot.
    fn unsafe_reason(&self, rule: &str) -> Option<Fallback> {
        let Some(&index) = self.stratum_of.get(rule) else {
            return Some(Fallback::NotEvaluated);
        };
        self.unsafe_stratum[index]
            .as_ref()
            .map(|(rule, reason)| Fallback::Unsafe {
                rule: rule.clone(),
                reason: *reason,
            })
    }

    /// The KEY column names of `rule`, in binding order.
    fn key_columns(&self, rule: &str) -> Vec<String> {
        self.rule(rule)
            .map(|r| {
                binding_sequence(&r.yield_schema)
                    .into_iter()
                    .filter(|c| c.is_key)
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Per KEY position of `rule`, whether it can carry a binding.
    ///
    /// A position can when its column is a node and every clause yields it
    /// as a bare node variable that the MATCH pattern or an IS reference
    /// binds, so the clause can be guarded on that variable.
    fn adornable(&self, rule: &str) -> Vec<bool> {
        let Some(compiled) = self.rule(rule) else {
            return Vec::new();
        };
        self.key_columns(rule)
            .iter()
            .map(|column| {
                self.kinds.kind(rule, column) == Some(ColumnKind::Node)
                    && compiled
                        .clauses
                        .iter()
                        .all(|clause| head_variable(clause, column).is_some())
            })
            .collect()
    }
}

/// The variable a clause yields as `column`, when it is a bare node variable
/// the clause binds through MATCH or an IS reference.
fn head_variable(clause: &CompiledClause, column: &str) -> Option<String> {
    let RuleOutput::Yield(yc) = &clause.output else {
        return None;
    };
    let plan = clause.binding.plan()?;
    let names = resolve_yield_column_names(&yc.items);
    let item = yc.items.iter().zip(&names).find(|(_, n)| *n == column)?.0;
    let Expr::Variable(var) = &item.expr else {
        return None;
    };
    if clause.fold.iter().any(|f| &f.name == var) || clause.along.iter().any(|a| &a.name == var) {
        return None;
    }
    match plan.scope.get(var)? {
        VarSource::Match(ColumnKind::Node) | VarSource::RefPosition { .. } => Some(var.clone()),
        _ => None,
    }
}

/// The first construct of `stratum` the rewrite has no argument for.
fn stratum_unsafe(
    stratum: &Stratum,
    catalog: &HashMap<String, CompiledRule>,
) -> Option<(String, Unsafe)> {
    let has_prob = |name: &str| {
        resolve_unique(catalog.keys().map(String::as_str), name)
            .and_then(|key| catalog.get(key))
            .is_none_or(|r| r.yield_schema.iter().any(|c| c.is_prob))
    };
    for rule in &stratum.rules {
        let flag = |reason: Unsafe| Some((rule.name.clone(), reason));
        if rule.yield_schema.iter().any(|c| c.is_prob) {
            return flag(Unsafe::Prob);
        }
        if rule.priority.is_some() {
            return flag(Unsafe::Priority);
        }
        for clause in &rule.clauses {
            if matches!(clause.output, RuleOutput::Derive(_)) {
                return flag(Unsafe::DeriveOutput);
            }
            if clause.priority.is_some() {
                return flag(Unsafe::Priority);
            }
            if !clause.model_invocations.is_empty() || !clause.hidden_yield_cols.is_empty() {
                return flag(Unsafe::Neural);
            }
            if matches!(clause.binding, ClauseBinding::Pending) {
                return flag(Unsafe::BindingPending);
            }
            if !clause.along.is_empty() {
                return flag(Unsafe::Along);
            }
            let aggregates = !clause.fold.is_empty()
                || clause.best_by.is_some()
                || !clause.require.is_empty()
                || !clause.having.is_empty();
            if stratum.is_recursive && aggregates {
                return flag(Unsafe::RecursiveAggregation);
            }
            if clause.fold.iter().any(|f| is_prob_aggregate(&f.aggregate)) {
                return flag(Unsafe::ProbAggregate);
            }
            for condition in &clause.where_conditions {
                match condition {
                    RuleCondition::Generator(_) => return flag(Unsafe::Generator),
                    RuleCondition::IsReference(r)
                        if r.negated && has_prob(&r.rule_name.to_string()) =>
                    {
                        return flag(Unsafe::ProbComplement);
                    }
                    _ => {}
                }
            }
        }
    }
    None
}

/// Whether a FOLD aggregate is a probabilistic one.
fn is_prob_aggregate(aggregate: &Expr) -> bool {
    matches!(aggregate, Expr::FunctionCall { name, .. }
        if matches!(name.to_uppercase().as_str(), "MNOR" | "MPROD"))
}

// ── One goal ────────────────────────────────────────────────────────────────

/// The rules one goal adds, and the copy its command reads.
struct GoalRewrite {
    answered_by: String,
    adorned: Vec<String>,
    /// The original rules that have copies.
    adorned_originals: BTreeSet<String>,
    /// Copies and magic rules.
    rules: Vec<CompiledRule>,
}

/// Names the copy and magic rule of each `(rule, adornment)` of one goal.
struct Interner {
    goal: usize,
    names: BTreeMap<(String, Vec<bool>), (String, String)>,
    queue: VecDeque<(String, Vec<bool>)>,
}

impl Interner {
    /// The `(copy, magic)` names of `rule` adorned `adornment`, queueing it
    /// when new.
    fn intern(&mut self, rule: &str, adornment: &[bool]) -> (String, String) {
        let key = (rule.to_string(), adornment.to_vec());
        if let Some(names) = self.names.get(&key) {
            return names.clone();
        }
        let tag = format!("{}_{}", sanitize(rule), adornment_tag(adornment));
        let names = (
            format!("{GOAL_DIRECTED_PREFIX}{}_{tag}", self.goal),
            format!("{GOAL_DIRECTED_PREFIX}m{}_{tag}", self.goal),
        );
        self.names.insert(key.clone(), names.clone());
        self.queue.push_back(key);
        names
    }
}

/// `b`/`f` per KEY position.
fn adornment_tag(adornment: &[bool]) -> String {
    adornment
        .iter()
        .map(|&b| if b { 'b' } else { 'f' })
        .collect()
}

/// A rule name as a dot-free identifier, so no bare reference can match it.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn rewrite_goal(
    ctx: &Context<'_>,
    goal: usize,
    rule: &str,
    where_expr: Option<&Expr>,
    params: &HashMap<String, Value>,
) -> Result<GoalRewrite, Fallback> {
    if ctx.rule(rule).is_none() || !ctx.stratum_of.contains_key(rule) {
        return Err(Fallback::NotEvaluated);
    }
    let keys = ctx.key_columns(rule);
    let adornable = ctx.adornable(rule);
    let mut seeds: Vec<Vec<Expr>> = vec![Vec::new(); keys.len()];
    let mut conjuncts = Vec::new();
    if let Some(w) = where_expr {
        split_conjuncts(w, &mut conjuncts);
    }
    for conjunct in conjuncts {
        if let Some(column) = seed_column(conjunct, params)
            && let Some(pos) = keys.iter().position(|k| *k == column)
            && adornable[pos]
        {
            seeds[pos].push(conjunct.clone());
        }
    }
    let adornment: Vec<bool> = seeds.iter().map(|s| !s.is_empty()).collect();
    if !adornment.iter().any(|&b| b) {
        return Err(Fallback::NoBoundKey);
    }
    if let Some(fallback) = ctx.unsafe_reason(rule) {
        return Err(fallback);
    }

    let mut interner = Interner {
        goal,
        names: BTreeMap::new(),
        queue: VecDeque::new(),
    };
    let (answered_by, goal_magic) = interner.intern(rule, &adornment);
    let mut copies: Vec<CompiledRule> = Vec::new();
    let mut magic: BTreeMap<String, (Vec<YieldColumn>, Vec<CompiledClause>)> = BTreeMap::new();
    // The parameters of the rules each magic rule's clauses come from.
    let mut magic_parameters: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut adorned = Vec::new();
    let mut adorned_originals = BTreeSet::new();

    while let Some((name, ad)) = interner.queue.pop_front() {
        let original = ctx.rule(&name).ok_or(Fallback::NotEvaluated)?;
        let (copy_name, magic_name) = interner.names[&(name.clone(), ad.clone())].clone();
        let key_names = ctx.key_columns(&name);
        magic.entry(magic_name.clone()).or_insert_with(|| {
            let schema = key_names
                .iter()
                .zip(&ad)
                .filter(|(_, b)| **b)
                .map(|(n, _)| YieldColumn {
                    name: n.clone(),
                    is_key: true,
                    is_prob: false,
                })
                .collect();
            (schema, Vec::new())
        });
        let mut clauses = Vec::with_capacity(original.clauses.len());
        for clause in &original.clauses {
            let (copy, propagations) = adorn_clause(
                ctx,
                &name,
                &ad,
                &key_names,
                &magic_name,
                clause,
                &mut interner,
            )?;
            clauses.push(copy);
            for (target, schema, propagation) in propagations {
                let inherited = magic_parameters.entry(target.clone()).or_default();
                for p in &original.parameters {
                    if !inherited.contains(p) {
                        inherited.push(p.clone());
                    }
                }
                magic
                    .entry(target)
                    .or_insert_with(|| (schema, Vec::new()))
                    .1
                    .push(propagation);
            }
        }
        adorned.push(format!("{name}/{}", adornment_tag(&ad)));
        adorned_originals.insert(name.clone());
        copies.push(CompiledRule {
            name: copy_name,
            clauses,
            yield_schema: original.yield_schema.clone(),
            priority: original.priority,
            parameters: original.parameters.clone(),
        });
    }

    let seed = seed_clause(ctx, rule, &keys, &adornment, &seeds);
    let goal_entry = magic.get_mut(&goal_magic).ok_or(Fallback::NotEvaluated)?;
    goal_entry.1.push(seed);

    let mut rules = copies;
    for (name, (yield_schema, clauses)) in magic {
        rules.push(CompiledRule {
            parameters: magic_parameters.remove(&name).unwrap_or_default(),
            name,
            clauses,
            yield_schema,
            priority: None,
        });
    }
    Ok(GoalRewrite {
        answered_by,
        adorned,
        adorned_originals,
        rules,
    })
}

/// Appends the top-level AND conjuncts of `expr` to `out`.
fn split_conjuncts<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// The column a goal conjunct binds, when it has a shape the seed can carry.
///
/// Accepted: `c.p = v`, `v = c.p` and `c.p IN [v, …]` (or `IN $list`), where
/// `v` is a string, integer or boolean literal or a parameter bound to one.
/// Floats are refused: the seed is evaluated by the clause-body engine and
/// the QUERY filter by the in-memory evaluator, and equality across integer
/// and float is where the two could plausibly disagree; a refused conjunct
/// costs selectivity, never an answer.
fn seed_column(conjunct: &Expr, params: &HashMap<String, Value>) -> Option<String> {
    fn column(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Property(base, _) => match base.as_ref() {
                Expr::Variable(v) => Some(v.clone()),
                _ => None,
            },
            _ => None,
        }
    }
    fn plain(value: &Value) -> bool {
        matches!(value, Value::String(_) | Value::Int(_) | Value::Bool(_))
    }
    let scalar = |expr: &Expr| match expr {
        Expr::Literal(CypherLiteral::String(_) | CypherLiteral::Integer(_)) => true,
        Expr::Literal(CypherLiteral::Bool(_)) => true,
        Expr::Parameter(p) => params.get(p).is_some_and(plain),
        _ => false,
    };
    match conjunct {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => match (column(left), column(right)) {
            (Some(c), None) if scalar(right) => Some(c),
            (None, Some(c)) if scalar(left) => Some(c),
            _ => None,
        },
        Expr::In { expr, list } => {
            let c = column(expr)?;
            let ok = match list.as_ref() {
                Expr::List(items) => !items.is_empty() && items.iter().all(scalar),
                Expr::Parameter(p) => match params.get(p) {
                    Some(Value::List(items)) => !items.is_empty() && items.iter().all(plain),
                    _ => false,
                },
                _ => false,
            };
            ok.then_some(c)
        }
        _ => None,
    }
}

/// The seed clause of the goal's magic rule.
fn seed_clause(
    ctx: &Context<'_>,
    rule: &str,
    keys: &[String],
    adornment: &[bool],
    seeds: &[Vec<Expr>],
) -> CompiledClause {
    let mut paths = Vec::new();
    let mut conditions = Vec::new();
    let mut items = Vec::new();
    for (pos, column) in keys.iter().enumerate() {
        if !adornment[pos] {
            continue;
        }
        paths.push(node_path(column, seed_label(ctx, rule, column)));
        conditions.extend(seeds[pos].iter().cloned().map(RuleCondition::Expression));
        items.push(key_item(column, column));
    }
    clause(Pattern { paths }, conditions, items)
}

/// A label every node `rule` yields as `column` carries, when one is known.
///
/// Labels in the seed are only a narrowing: a clause binding the variable
/// through an IS reference, or with a label disjunction, gives no label.
fn seed_label(ctx: &Context<'_>, rule: &str, column: &str) -> LabelExpr {
    let Some(compiled) = ctx.rule(rule) else {
        return LabelExpr::Empty;
    };
    let mut common: Option<BTreeSet<String>> = None;
    for clause in &compiled.clauses {
        let Some(var) = head_variable(clause, column) else {
            return LabelExpr::Empty;
        };
        let mut labels = BTreeSet::new();
        let mut seen = false;
        for path in &clause.match_pattern.paths {
            for element in &path.elements {
                if let PatternElement::Node(node) = element
                    && node.variable.as_deref() == Some(var.as_str())
                {
                    seen = true;
                    if let LabelExpr::Conjunction(ls) = &node.labels {
                        labels.extend(ls.iter().cloned());
                    }
                }
            }
        }
        if !seen {
            return LabelExpr::Empty;
        }
        common = Some(match common {
            None => labels,
            Some(prev) => prev.intersection(&labels).cloned().collect(),
        });
    }
    common
        .and_then(|c| c.into_iter().next())
        .map_or(LabelExpr::Empty, |l| LabelExpr::Conjunction(vec![l]))
}

/// A magic propagation clause: `(target magic, its schema, the clause)`.
type Propagation = (String, Vec<YieldColumn>, CompiledClause);

/// Copies `clause` of `rule` adorned `adornment`, and derives its magic
/// propagation clauses.
fn adorn_clause(
    ctx: &Context<'_>,
    rule: &str,
    adornment: &[bool],
    key_names: &[String],
    own_magic: &str,
    clause: &CompiledClause,
    interner: &mut Interner,
) -> Result<(CompiledClause, Vec<Propagation>), Fallback> {
    let unsafe_rule = |reason| Fallback::Unsafe {
        rule: rule.to_string(),
        reason,
    };
    if clause.binding.plan().is_none() {
        return Err(unsafe_rule(Unsafe::BindingPending));
    }
    let head: Vec<String> = key_names
        .iter()
        .zip(adornment)
        .filter(|(_, b)| **b)
        .map(|(column, _)| head_variable(clause, column))
        .collect::<Option<_>>()
        .ok_or_else(|| unsafe_rule(Unsafe::BindingPending))?;

    let components = Components::of(&clause.match_pattern);
    let bound_components: BTreeSet<usize> =
        head.iter().filter_map(|v| components.of_var(v)).collect();
    let is_bound = |var: &str| {
        head.iter().any(|h| h == var)
            || components
                .of_var(var)
                .is_some_and(|c| bound_components.contains(&c))
    };

    let mut conditions = Vec::with_capacity(clause.where_conditions.len() + 1);
    let mut propagations = Vec::new();
    for condition in &clause.where_conditions {
        let RuleCondition::IsReference(reference) = condition else {
            conditions.push(condition.clone());
            continue;
        };
        let callee = ctx.resolve(&reference.rule_name.to_string());
        let adorned_call = match callee {
            Some(callee) if !reference.negated && ctx.unsafe_reason(callee).is_none() => {
                let callee_adornable = ctx.adornable(callee);
                let vars: Vec<&String> = reference
                    .subjects
                    .iter()
                    .chain(reference.target.iter())
                    .collect();
                let beta: Vec<bool> = callee_adornable
                    .iter()
                    .enumerate()
                    .map(|(p, ok)| *ok && vars.get(p).is_some_and(|v| is_bound(v)))
                    .collect();
                beta.iter().any(|&b| b).then_some((callee, beta, vars))
            }
            _ => None,
        };
        let Some((callee, beta, vars)) = adorned_call else {
            conditions.push(condition.clone());
            continue;
        };
        let (copy, callee_magic) = interner.intern(callee, &beta);
        conditions.push(RuleCondition::IsReference(IsReference {
            rule_name: single(&copy),
            ..reference.clone()
        }));
        let call_vars: Vec<String> = vars
            .iter()
            .zip(&beta)
            .filter(|(_, b)| **b)
            .map(|(v, _)| (*v).clone())
            .collect();
        if callee == rule && beta == adornment && call_vars == head {
            // `magic(x) :- magic(x)`: the call asks for what was asked.
            continue;
        }
        let callee_keys = ctx.key_columns(callee);
        let schema: Vec<YieldColumn> = callee_keys
            .iter()
            .zip(&beta)
            .filter(|(_, b)| **b)
            .map(|(n, _)| YieldColumn {
                name: n.clone(),
                is_key: true,
                is_prob: false,
            })
            .collect();
        let items = call_vars
            .iter()
            .zip(&schema)
            .map(|(v, c)| key_item(v, &c.name))
            .collect();
        propagations.push((
            callee_magic,
            schema,
            magic_clause(
                clause,
                &head,
                own_magic,
                &components,
                &bound_components,
                items,
            ),
        ));
    }
    conditions.push(guard(&head, own_magic));
    let copy = CompiledClause {
        where_conditions: conditions,
        binding: ClauseBinding::Pending,
        ..clause.clone()
    };
    Ok((copy, propagations))
}

/// The magic propagation clause for one call in `clause`.
///
/// Its MATCH is the clause's paths that touch a bound component, plus a bare
/// `(v)` for each head variable those paths do not bind; its WHERE is the
/// head guard and every filter over its variables.
fn magic_clause(
    clause: &CompiledClause,
    head: &[String],
    own_magic: &str,
    components: &Components,
    bound_components: &BTreeSet<usize>,
    items: Vec<LocyYieldItem>,
) -> CompiledClause {
    let mut paths: Vec<PathPattern> = clause
        .match_pattern
        .paths
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            components
                .path_component(*i)
                .is_some_and(|c| bound_components.contains(&c))
        })
        .map(|(_, p)| p.clone())
        .collect();
    let mut available: HashSet<String> = HashSet::new();
    for path in &paths {
        path_variables(path, &mut available);
    }
    for var in head {
        if !available.contains(var) {
            paths.push(node_path(var, LabelExpr::Empty));
            available.insert(var.clone());
        }
    }
    let mut conditions = vec![guard(head, own_magic)];
    for condition in &clause.where_conditions {
        if let RuleCondition::Expression(expr) = condition
            && !has_local_scope(expr)
            && free_variables(expr).iter().all(|v| available.contains(v))
        {
            conditions.push(condition.clone());
        }
    }
    self::clause(Pattern { paths }, conditions, items)
}

/// Whether `expr` holds a subquery or a construct binding its own variables,
/// whose free names the scope walk does not report.
fn has_local_scope(expr: &Expr) -> bool {
    if matches!(
        expr,
        Expr::Exists { .. }
            | Expr::CountSubquery(_)
            | Expr::CollectSubquery(_)
            | Expr::PatternComprehension { .. }
            | Expr::ListComprehension { .. }
            | Expr::Quantifier { .. }
            | Expr::Reduce { .. }
    ) {
        return true;
    }
    let mut found = false;
    expr.for_each_child(&mut |child| found |= has_local_scope(child));
    found
}

/// Every variable a path names: nodes, relationships, the path, and inside
/// quantified groups.
fn path_variables(path: &PathPattern, out: &mut HashSet<String>) {
    out.extend(path.variable.iter().cloned());
    for element in &path.elements {
        match element {
            PatternElement::Node(n) => out.extend(n.variable.iter().cloned()),
            PatternElement::Relationship(r) => out.extend(r.variable.iter().cloned()),
            PatternElement::Parenthesized { pattern, .. } => path_variables(pattern, out),
        }
    }
}

/// Connected components of a MATCH pattern's top-level node variables.
///
/// Every top-level node of one path is connected to every other; paths
/// sharing a node variable are one component. Nodes inside a quantified group
/// are lists, not nodes, and take no part.
struct Components {
    var: HashMap<String, usize>,
    path: Vec<Option<usize>>,
}

impl Components {
    fn of(pattern: &Pattern) -> Self {
        let mut parent: Vec<usize> = Vec::new();
        fn find(parent: &mut [usize], mut x: usize) -> usize {
            while parent[x] != x {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            x
        }
        let mut var: HashMap<String, usize> = HashMap::new();
        let mut path_root: Vec<Option<usize>> = Vec::with_capacity(pattern.paths.len());
        for path in &pattern.paths {
            let mut root: Option<usize> = None;
            for element in &path.elements {
                let PatternElement::Node(NodePattern {
                    variable: Some(v), ..
                }) = element
                else {
                    continue;
                };
                let id = *var.entry(v.clone()).or_insert_with(|| {
                    parent.push(parent.len());
                    parent.len() - 1
                });
                match root {
                    None => root = Some(id),
                    Some(r) => {
                        let (a, b) = (find(&mut parent, r), find(&mut parent, id));
                        parent[b] = a;
                    }
                }
            }
            path_root.push(root);
        }
        let var = var
            .into_iter()
            .map(|(k, id)| (k, find(&mut parent, id)))
            .collect();
        let path = path_root
            .into_iter()
            .map(|r| r.map(|r| find(&mut parent, r)))
            .collect();
        Self { var, path }
    }

    fn of_var(&self, var: &str) -> Option<usize> {
        self.var.get(var).copied()
    }

    fn path_component(&self, index: usize) -> Option<usize> {
        self.path.get(index).copied().flatten()
    }
}

// ── AST helpers ─────────────────────────────────────────────────────────────

fn single(name: &str) -> QualifiedName {
    QualifiedName {
        parts: vec![name.to_string()],
    }
}

fn guard(head: &[String], magic: &str) -> RuleCondition {
    RuleCondition::IsReference(IsReference {
        subjects: head.to_vec(),
        rule_name: single(magic),
        target: None,
        negated: false,
    })
}

fn node_path(var: &str, labels: LabelExpr) -> PathPattern {
    PathPattern {
        variable: None,
        elements: vec![PatternElement::Node(NodePattern {
            variable: Some(var.to_string()),
            labels,
            properties: None,
            where_clause: None,
        })],
        shortest_path_mode: None,
    }
}

fn key_item(var: &str, alias: &str) -> LocyYieldItem {
    LocyYieldItem {
        is_key: true,
        is_prob: false,
        expr: Expr::Variable(var.to_string()),
        alias: Some(alias.to_string()),
    }
}

fn clause(
    match_pattern: Pattern,
    where_conditions: Vec<RuleCondition>,
    items: Vec<LocyYieldItem>,
) -> CompiledClause {
    CompiledClause {
        match_pattern,
        where_conditions,
        along: Vec::new(),
        fold: Vec::new(),
        require: Vec::new(),
        having: Vec::new(),
        best_by: None,
        output: RuleOutput::Yield(YieldClause { items }),
        priority: None,
        model_invocations: Vec::new(),
        hidden_yield_cols: Vec::new(),
        binding: ClauseBinding::Pending,
    }
}

// ── Assembly ────────────────────────────────────────────────────────────────

/// Builds the program to evaluate from the per-goal rewrites.
fn assemble(
    program: &CompiledProgram,
    rewrites: Vec<(usize, GoalRewrite)>,
    mode: GoalDirected,
) -> Result<CompiledProgram, String> {
    let mut out = program.clone();
    let mut adorned_originals: BTreeSet<String> = BTreeSet::new();
    let mut new_rules: Vec<CompiledRule> = Vec::new();
    for (command, rewrite) in rewrites {
        if let Some(CompiledCommand::GoalQuery(query)) = out.commands.get_mut(command) {
            query.rule_name = single(&rewrite.answered_by);
        }
        adorned_originals.extend(rewrite.adorned_originals);
        new_rules.extend(rewrite.rules);
    }

    if matches!(mode, GoalDirected::On | GoalDirected::Auto) {
        prune_originals(&mut out, &adorned_originals, &new_rules);
    }

    let next_id = out.strata.iter().map(|s| s.id + 1).max().unwrap_or(0);
    let mut id_of: HashMap<String, usize> = HashMap::new();
    for stratum in &out.strata {
        for rule in &stratum.rules {
            id_of.insert(rule.name.clone(), stratum.id);
        }
    }
    let original_ids = id_of.clone();
    let sccs = strongly_connected(&new_rules);
    let by_name: HashMap<&str, &CompiledRule> =
        new_rules.iter().map(|r| (r.name.as_str(), r)).collect();
    for (k, scc) in sccs.iter().enumerate() {
        for name in scc {
            id_of.insert(name.clone(), next_id + k);
        }
    }
    for (k, scc) in sccs.into_iter().enumerate() {
        let members: HashSet<&str> = scc.iter().map(String::as_str).collect();
        let mut depends_on = BTreeSet::new();
        let mut is_recursive = scc.len() > 1;
        for name in &scc {
            for reference in references(by_name[name.as_str()]) {
                if members.contains(reference.as_str()) {
                    is_recursive = true;
                    continue;
                }
                let id = id_of.get(&reference).copied().or_else(|| {
                    original_ids
                        .iter()
                        .find(|(key, _)| name_matches(key, &reference))
                        .map(|(_, id)| *id)
                });
                depends_on.extend(id);
            }
        }
        let mut rules: Vec<CompiledRule> = scc
            .iter()
            .map(|name| by_name[name.as_str()].clone())
            .collect();
        rules.sort_by(|a, b| a.name.cmp(&b.name));
        out.strata.push(Stratum {
            id: next_id + k,
            rules,
            is_recursive,
            depends_on: depends_on.into_iter().collect(),
        });
    }
    for rule in new_rules {
        out.rule_catalog.insert(rule.name.clone(), rule);
    }
    crate::attach_binding_plans(&mut out).map_err(|e| e.to_string())?;
    Ok(out)
}

/// The rule names a rule's clauses reference, as written.
fn references(rule: &CompiledRule) -> Vec<String> {
    rule.clauses
        .iter()
        .flat_map(|c| &c.where_conditions)
        .filter_map(|c| match c {
            RuleCondition::IsReference(r) => Some(r.rule_name.to_string()),
            _ => None,
        })
        .collect()
}

/// Drops from the strata every adorned original that nothing but a rewritten
/// goal needed.
///
/// The roots are every evaluated rule that has no copy, the commands as
/// rewritten, and the generated rules; everything they reach is kept, matched
/// by [`name_matches`] so a doubtful reference keeps its rule.
fn prune_originals(
    program: &mut CompiledProgram,
    adorned: &BTreeSet<String>,
    new_rules: &[CompiledRule],
) {
    let evaluated: Vec<&CompiledRule> = program.strata.iter().flat_map(|s| &s.rules).collect();
    let mut queue: VecDeque<String> = VecDeque::new();
    for rule in &evaluated {
        if !adorned.contains(&rule.name) {
            queue.push_back(rule.name.clone());
        }
    }
    for rule in new_rules {
        queue.extend(references(rule));
    }
    let mut commands_only = program.clone();
    commands_only.strata.clear();
    queue.extend(crate::prune::collect_referenced_rule_names(&commands_only));

    let mut needed: HashSet<String> = HashSet::new();
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(reference) = queue.pop_front() {
        if !seen.insert(reference.clone()) {
            continue;
        }
        for rule in &evaluated {
            if name_matches(&rule.name, &reference) && needed.insert(rule.name.clone()) {
                queue.extend(references(rule));
            }
        }
    }
    for stratum in &mut program.strata {
        stratum.rules.retain(|r| needed.contains(&r.name));
    }
    program.strata.retain(|s| !s.rules.is_empty());
}

/// Strongly connected components of the generated rules, dependencies first.
fn strongly_connected(rules: &[CompiledRule]) -> Vec<Vec<String>> {
    let index_of: HashMap<&str, usize> = rules
        .iter()
        .enumerate()
        .map(|(i, r)| (r.name.as_str(), i))
        .collect();
    let edges: Vec<Vec<usize>> = rules
        .iter()
        .map(|r| {
            references(r)
                .iter()
                .filter_map(|n| index_of.get(n.as_str()).copied())
                .collect()
        })
        .collect();

    struct Tarjan<'g> {
        edges: &'g [Vec<usize>],
        index: Vec<Option<usize>>,
        low: Vec<usize>,
        on_stack: Vec<bool>,
        stack: Vec<usize>,
        next: usize,
        out: Vec<Vec<usize>>,
    }
    impl Tarjan<'_> {
        fn visit(&mut self, v: usize) {
            self.index[v] = Some(self.next);
            self.low[v] = self.next;
            self.next += 1;
            self.stack.push(v);
            self.on_stack[v] = true;
            for i in 0..self.edges[v].len() {
                let w = self.edges[v][i];
                match self.index[w] {
                    None => {
                        self.visit(w);
                        self.low[v] = self.low[v].min(self.low[w]);
                    }
                    Some(iw) if self.on_stack[w] => self.low[v] = self.low[v].min(iw),
                    Some(_) => {}
                }
            }
            if Some(self.low[v]) == self.index[v] {
                let mut scc = Vec::new();
                while let Some(w) = self.stack.pop() {
                    self.on_stack[w] = false;
                    scc.push(w);
                    if w == v {
                        break;
                    }
                }
                self.out.push(scc);
            }
        }
    }
    let n = rules.len();
    let mut t = Tarjan {
        edges: &edges,
        index: vec![None; n],
        low: vec![0; n],
        on_stack: vec![false; n],
        stack: Vec::new(),
        next: 0,
        out: Vec::new(),
    };
    for v in 0..n {
        if t.index[v].is_none() {
            t.visit(v);
        }
    }
    t.out
        .into_iter()
        .map(|scc| scc.into_iter().map(|i| rules[i].name.clone()).collect())
        .collect()
}

#[cfg(test)]
mod tests;
