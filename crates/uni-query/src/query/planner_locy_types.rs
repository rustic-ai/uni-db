// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Supporting types for Locy logical plan variants.
//!
//! These types describe the structure of a Locy program after planning:
//! strata, rules, clauses, IS-references, yield schemas, and top-level commands.

use arrow_schema::DataType;

use super::planner::LogicalPlan;
use uni_cypher::ast::{Expr, Query};
use uni_cypher::locy_ast::{AbduceQuery, DeriveCommand, ExplainRule, GoalQuery};
use uni_locy::types::CompiledAssume;

/// A stratum in the stratified evaluation order.
///
/// Each stratum contains rules that can be evaluated together (possibly recursively).
/// Strata are ordered by dependency: stratum N depends only on strata < N.
#[derive(Debug, Clone)]
pub struct LocyStratum {
    /// Stratum index (0-based).
    pub id: usize,
    /// Rules in this stratum.
    pub rules: Vec<LocyRulePlan>,
    /// Whether this stratum requires fixpoint iteration.
    pub is_recursive: bool,
    /// Indices of strata this one depends on.
    pub depends_on: Vec<usize>,
}

/// A planned Locy rule (one named derived relation).
#[derive(Debug, Clone)]
pub struct LocyRulePlan {
    /// Rule name (e.g., `reachable`).
    pub name: String,
    /// Clauses (one per `<-` body). Multiple clauses form a union.
    pub clauses: Vec<LocyClausePlan>,
    /// Output schema columns.
    pub yield_schema: Vec<LocyYieldColumn>,
    /// Optional priority weight for PRIORITY semantics.
    pub priority: Option<i64>,
    /// FOLD bindings for post-fixpoint aggregation (fold_name, yield_alias, aggregate_expr).
    /// The yield_alias is the output column name from YIELD (may differ from fold_name
    /// when the user writes e.g. `YIELD ... n AS support`).
    pub fold_bindings: Vec<(String, String, Expr)>,
    /// Post-FOLD filter expressions (HAVING semantics).
    /// Post-FOLD definitional threshold (`REQUIRE`, issue #265).
    ///
    /// Applied to every iteration's folded snapshot rather than once at the
    /// end, so it constrains what a recursive rule can derive. Aliases are
    /// substituted exactly as for [`Self::having`].
    pub require: Vec<Expr>,
    pub having: Vec<Expr>,
    /// BEST BY criteria for post-fixpoint selection (expr, ascending).
    pub best_by_criteria: Vec<(Expr, bool)>,
    /// Post-fold YIELD projection specs `(output_name, expr)`.
    ///
    /// Non-empty only when a YIELD column is a computed expression over a FOLD
    /// output (e.g. `total * 2.0 AS score`), which cannot be produced pre-fold.
    /// When present it lists every yield column in schema order; the runtime
    /// evaluates each expression against the post-fold batch to build the final
    /// output. Empty for the common case (FoldExec output already matches the
    /// yield schema).
    pub yield_projection: Vec<(String, Expr)>,
    /// Hidden derivation-discriminator columns `(name, type)`, in the order the
    /// planner projects them onto every clause (issue #159).
    ///
    /// Recorded here so the fixpoint plan can widen its Arrow yield schema to
    /// match, without re-deriving the analysis from the compiled rule — the
    /// two must agree exactly or dedup, provenance and the clause union all
    /// disagree about the row shape. Empty unless the rule is recursive and
    /// carries FOLD or ALONG. See [`FOLD_DISCRIMINATOR_COL_PREFIX`].
    pub deriv_columns: Vec<(String, DataType)>,
}

/// A single clause (body) of a Locy rule.
#[derive(Debug, Clone)]
pub struct LocyClausePlan {
    /// The planned query body (Scan → Traverse → Filter → Project chain).
    pub body: LogicalPlan,
    /// IS-references to other derived relations in this clause.
    pub is_refs: Vec<LocyIsRef>,
    /// ALONG binding variable names.
    pub along_bindings: Vec<String>,
    /// Optional priority value for this clause.
    pub priority: Option<i64>,
    /// Phase B Slice 3: neural-model invocations lifted from YIELD items
    /// by the compiler. Carried through planning to the fixpoint
    /// executor where they're evaluated post-body-projection.
    pub model_invocations: Vec<uni_locy::ModelInvocation>,
}

/// An IS-reference from a clause body to another derived relation.
#[derive(Debug, Clone)]
pub struct LocyIsRef {
    /// The target rule name.
    pub rule_name: String,
    /// Subject variable bindings (FROM arguments).
    pub subjects: Vec<Expr>,
    /// Target variable binding (TO argument), if any.
    pub target: Option<Expr>,
    /// Whether this is a negated IS-reference (`NOT IS`).
    pub negated: bool,
    /// Whether the target rule has a PROB column.
    pub target_has_prob: bool,
    /// Name of the PROB column in the target rule, if any.
    pub target_prob_col: Option<String>,
    /// For negated IS-refs: subject/target variable → the hidden `_vid` column
    /// the planner projected for it (`__isnot{n}_{var}`).
    ///
    /// The anti-join runs *after* `LocyProject`, so resolving a subject by its
    /// bare variable name only works when YIELD happens to project that exact
    /// name. A subject that YIELD renamed with `AS`, or did not project at all,
    /// was previously unresolvable — and the anti-join silently emitted the
    /// rows it was asked to exclude (issue #158).
    ///
    /// Carrying `{var}._vid` through the projection under a reserved name makes
    /// resolution depend on node identity rather than on what YIELD chose to
    /// call things. Empty for positive IS-refs, and for subjects that are not
    /// MATCH-bound node variables (relationship variables expose `_eid`, not
    /// `_vid`; scalar and ALONG-bound subjects have neither) — those keep the
    /// by-name path and its error.
    pub subject_vid_cols: std::collections::HashMap<String, String>,
}

/// Prefix for the hidden `_vid` columns described on
/// [`LocyIsRef::subject_vid_cols`].
///
/// Two constraints, both load-bearing:
/// * It must not begin with `__prob_complement_`, which the anti-join
///   post-processing scans for by prefix.
/// * These projections are pushed **last**, so the positional
///   `0..yield_schema.len()` scans in provenance recording stay aligned even if
///   a strip is ever missed.
pub const ISNOT_VID_COL_PREFIX: &str = "__isnot_vid_";

/// Prefix for the hidden derivation-discriminator columns that keep distinct
/// derivations distinct inside a recursive `FOLD` / `ALONG` rule (issue #159).
///
/// # The problem
///
/// `MATCH (p)-[e:HAS]->(c) FOLD cost = MSUM(cost * e.q) YIELD KEY p, cost`
/// projects the child `c` away, so a parent with N children of equal cost
/// yields N *identical* `(p, cost)` rows. All-column dedup collapses them and
/// the fold then aggregates one value — a bill-of-materials rollup returns 1.0
/// instead of N. MNOR/MPROD are wrong the same way: two children at p=0.5 give
/// 0.5 instead of 0.75.
///
/// The intended semantics are documented, if indirectly: "FOLD aggregates
/// across **paths**" (`docs/complete_locy.md`), `MCOUNT = acc + 1` is
/// meaningless over a value-set, the BDD proof model reasons per *derivation
/// row*, and the TCK already requires two equal-valued rows to contribute twice
/// in a *non*-recursive rule. The all-column dedup comment justifies itself
/// solely by termination and was never a semantic commitment.
///
/// # Why a discriminator, and not something cheaper
///
/// Dedup is doing two jobs at once: suppressing *re-derivations* (which is what
/// bounds the fixpoint) and collapsing *equal values* (the bug). Two simpler
/// fixes were prototyped and both failed:
///
/// * Aggregating per iteration and replacing per KEY (as `BEST BY` does) breaks
///   because a self-ref reads pre-fold contribution rows, so a parent joins a
///   multi-contribution child more than once.
/// * Skipping dedup entirely for FOLD rules hits the iteration limit **even on
///   a DAG**, because a non-recursive base clause re-emits its rows every
///   iteration and dedup is what suppresses them.
///
/// So the dedup key must separate "same derivation re-emitted" (drop) from
/// "different derivation, equal values" (keep). These columns are that
/// separation. They are vids, so the domain stays finite and termination is
/// preserved; a genuinely divergent recursive aggregate (an unbounded sum
/// around a cycle) exhausts the iteration limit and surfaces as
/// `LocyIncomplete`, which is the documented backstop.
///
/// # Scope
///
/// **Only rules carrying `FOLD` or `ALONG`.** A key-only recursive rule such as
/// a transitive closure depends on set semantics: applying this globally turns
/// one test's 1350-fact closure into 58 050 and does not terminate on cyclic
/// fixtures.
pub const FOLD_DISCRIMINATOR_COL_PREFIX: &str = "__deriv_";

/// A column in a rule's yield schema.
#[derive(Debug, Clone)]
pub struct LocyYieldColumn {
    /// Column name.
    pub name: String,
    /// Whether this column is a KEY column.
    pub is_key: bool,
    /// Whether this column is a PROB column (probability annotation).
    pub is_prob: bool,
    /// Arrow data type for this column (inferred from yield expressions).
    pub data_type: DataType,
}

/// A top-level Locy command to execute after fixpoint evaluation.
///
/// Commands carry compiled AST data and are dispatched by the caller
/// (e.g., `evaluate_native`) via the orchestrator after strata evaluation.
#[derive(Debug, Clone)]
pub enum LocyCommand {
    /// Query a derived relation: `QUERY rulename WHERE expr`
    GoalQuery { goal_query: GoalQuery },
    /// Derive facts into the database: `DERIVE rulename`
    Derive { derive_command: DeriveCommand },
    /// Assume facts and evaluate a body: `ASSUME { ... } THEN { ... }`
    Assume { compiled_assume: CompiledAssume },
    /// Explain a rule's derivation: `EXPLAIN RULE rulename WHERE expr`
    ExplainRule { explain_rule: ExplainRule },
    /// Abduce missing facts: `ABDUCE rulename WHERE expr`
    Abduce { abduce_query: AbduceQuery },
    /// Pass-through Cypher statement.
    Cypher { query: Query },
    /// Phase C C2: `CALIBRATE` statement. Carries a snapshot of the
    /// referenced model's input bindings so the runtime can build
    /// `ClassifyInput`s without needing access to the full
    /// `CompiledProgram.model_catalog`.
    Calibrate {
        calibrate: uni_locy::CompiledCalibrate,
        model_inputs: Vec<uni_locy::CompiledInputBinding>,
    },
    /// Phase C C3: `VALIDATE` statement. The compiled form already
    /// carries the rule's PROB column name; no auxiliary snapshot is
    /// needed (the runtime queries derived facts by rule name).
    Validate {
        validate: uni_locy::CompiledValidate,
    },
}
