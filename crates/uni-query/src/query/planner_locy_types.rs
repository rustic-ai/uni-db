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
    /// Scan indices of the body's derived scans into its own stratum, in
    /// first-use order, one per distinct handle (P5). A clause with two or
    /// more holds only exclusive handles, which the fixpoint feeds per pass.
    pub stratum_scans: Vec<usize>,
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
    /// For a negated IS-ref: the anti-join key, one component per KEY
    /// position some literal of the clause binds (`uni_locy::binding_plan`).
    ///
    /// The anti-join runs *after* `LocyProject`, so resolving a subject by its
    /// bare variable name only worked when YIELD happened to project that
    /// exact name. A subject that YIELD renamed with `AS`, or did not project
    /// at all, was unresolvable — and the anti-join silently emitted the rows
    /// it was asked to exclude (issue #158). So the planner carries each
    /// binder's identity (a node's `_vid`, a relationship's `_eid`, a scalar's
    /// column) through the projection under a reserved hidden name. A value
    /// position bound nowhere is existentially quantified and absent. Empty
    /// for positive IS-refs.
    pub negation_keys: Vec<NegationKey>,
    /// The referenced rule's column bound by each subject, in order, then by
    /// the target if there is one (`uni_locy::is_binding::bound_columns`).
    ///
    /// Resolved once at planning time, where the rule catalog is at hand, so
    /// the executor's anti-join and provenance join bind the same columns as
    /// the planner's positive join instead of taking the derived scan's fields
    /// by position.
    pub binding_columns: Vec<String>,
    /// For a positive reference to a PROB rule from a rule with a PROB
    /// column: where the referenced probability travels and how the clause
    /// already reads it. `None` otherwise, and the reference is a plain
    /// semi-join.
    pub prob_ref: Option<PositiveProbRef>,
    /// For a negated reference to a PROB rule from a rule that keeps lineage
    /// (an `MNOR`/`MPROD` rule, or a PROB rule without a fold when
    /// provenance is recorded): the hidden column the complement fills with
    /// the negated facts it read, for exact inference. `None` otherwise.
    pub negated_lineage: Option<NegatedLineageRef>,
    /// For a positive reference no lineage column identifies (one to a rule
    /// without a PROB column, or from a rule that keeps no lineage), when
    /// provenance is recorded: the transient columns naming the referenced
    /// fact, so EXPLAIN shows it as the derivation's child. `None` otherwise.
    pub support_ref: Option<SupportRef>,
}

/// The transient columns naming the fact a positive reference read, for
/// provenance only (see [`LocyIsRef::support_ref`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportRef {
    /// The hidden body column (`__lin_sup_{clause}_{occurrence}`) holding the
    /// hash of the referenced fact's KEY columns.
    pub key_column: String,
    /// The hidden body column holding the referenced fact's PROB value, when
    /// the referenced rule has one. Without it, the referenced fact is
    /// certain: its probability is 1.
    pub prob_column: Option<String>,
}

/// Where a negated reference records the facts its complement read (see
/// [`LocyIsRef::negated_lineage`]).
///
/// The complement multiplies `1 - p` into the row, `p` the noisy-OR of every
/// fact of the negated rule the row's negation key matches. Exact inference
/// instead conjoins the negation of those facts' formulas, which is not the
/// same when the negated facts share lineage with the rest of the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegatedLineageRef {
    /// The hidden body column (`__deriv_isnot_{clause}_{occurrence}`): per
    /// row, each matched fact's KEY hash and probability (see
    /// `locy_lineage::encode_negated_facts`). NULL when nothing matched.
    pub column: String,
    /// The negated rule's KEY columns in yield order, hashed as the rule's
    /// own lineage hashes its facts.
    pub target_key_columns: Vec<String>,
}

/// How a clause's PROB expression reads the probability of one positive
/// reference to a PROB rule.
///
/// A positive reference to a PROB rule is a probabilistic condition: the row
/// holds only when the referenced fact does, so the fact's probability is
/// multiplied into the row's PROB value, as `IS NOT` multiplies `1 - p`. A
/// clause that already reads that probability (`FOLD p = MNOR(p)`) must not
/// count it a second time; this classification decides which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbRead {
    /// The PROB expression does not read the referenced probability: it is
    /// multiplied in.
    Absent,
    /// The PROB expression is a product with the referenced probability as
    /// exactly one factor (`p`, `e.w * p`): it is already counted once.
    Factor,
    /// The PROB expression reads the referenced probability other than as one
    /// product factor (`1 - p`, `p * p`, `max(p, e.w)`). Nothing is multiplied
    /// in (never counting it twice), exact inference treats the row as an
    /// independent event, and a `ProbabilityReadNotFactor` warning is raised.
    Opaque,
}

/// The hidden columns and classification of one positive reference to a PROB
/// rule (see [`LocyIsRef::prob_ref`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositiveProbRef {
    /// The hidden body column holding the referenced fact's PROB value.
    pub prob_column: String,
    /// The hidden body column holding a hash of the referenced fact's KEY
    /// columns, for exact inference. Set only when the calling rule keeps
    /// lineage: its PROB column is an `MNOR`/`MPROD` fold, whose rows are
    /// kept as lineage, or it has no fold and provenance is recorded (a
    /// transient column, see [`LIN_COL_PREFIX`]).
    pub key_column: Option<String>,
    /// The calling rule's PROB column as the clause body names it (the FOLD
    /// variable for a folded PROB column).
    pub into_column: String,
    /// How the clause's PROB expression reads the referenced probability.
    pub read: ProbRead,
}

/// Prefix of the transient hidden column carrying a referenced probability
/// into a clause whose rule keeps no lineage. Stripped once multiplied in.
pub const ISPROB_COL_PREFIX: &str = "__isprob_";

/// Prefix of the persistent hidden column carrying a referenced probability in
/// an `MNOR`/`MPROD` rule's pre-fold rows. It starts with
/// [`FOLD_DISCRIMINATOR_COL_PREFIX`] so it is nullable and stripped like the
/// discriminators, but it is a value, not an identity: it is excluded from the
/// derivation key.
pub const DERIV_ISPROB_COL_PREFIX: &str = "__deriv_isprob_";

/// Prefix of the persistent hidden column carrying the hash of a referenced
/// fact's KEY columns in an `MNOR`/`MPROD` rule's pre-fold rows.
pub const DERIV_ISREF_COL_PREFIX: &str = "__deriv_isref_";

/// Prefix of the persistent hidden column carrying the facts a negated
/// reference's complement read, in an `MNOR`/`MPROD` rule's pre-fold rows (see
/// [`NegatedLineageRef`]).
pub const DERIV_ISNOT_COL_PREFIX: &str = "__deriv_isnot_";

/// The row-event column of an `MNOR`/`MPROD` rule's pre-fold rows.
///
/// A row's own probability is its PROB expression with the referenced
/// probabilities it reads factored out — `e.p` in `MNOR(e.p * w)`. Two rows
/// whose local part is the same expression over the same nodes and
/// relationships are one event: the direct `s→t` row of a reachability rule
/// and its row through a self-loop at `t` both rest on the edge `s→t`. The
/// column holds a hash of the expression's shape and those identities; NULL
/// when the local part reads no node or relationship, which makes the row its
/// own event.
pub const DERIV_EVENT_COL: &str = "__deriv_event";

/// Prefix of every transient lineage column.
///
/// A PROB rule without a fold has no pre-fold rows to keep its lineage in:
/// its facts are its rows, deduplicated. When provenance is recorded, its
/// clause bodies project the lineage columns under this prefix instead of
/// [`FOLD_DISCRIMINATOR_COL_PREFIX`] ([`LIN_REF_COL_PREFIX`],
/// [`LIN_PROB_COL_PREFIX`], [`LIN_NOT_COL_PREFIX`], [`LIN_EVENT_COL`]), as do
/// the provenance-only columns of every other rule ([`LIN_SUP_COL_PREFIX`],
/// [`LIN_SUPP_COL_PREFIX`]). They are read from a clause's rows before the
/// facts are deduplicated, then dropped, so they are never part of a fact's
/// identity.
pub const LIN_COL_PREFIX: &str = "__lin_";

/// [`DERIV_ISREF_COL_PREFIX`]'s transient counterpart (see [`LIN_COL_PREFIX`]).
pub const LIN_REF_COL_PREFIX: &str = "__lin_ref_";

/// [`DERIV_ISPROB_COL_PREFIX`]'s transient counterpart (see [`LIN_COL_PREFIX`]).
pub const LIN_PROB_COL_PREFIX: &str = "__lin_prob_";

/// [`DERIV_ISNOT_COL_PREFIX`]'s transient counterpart (see [`LIN_COL_PREFIX`]).
pub const LIN_NOT_COL_PREFIX: &str = "__lin_not_";

/// [`DERIV_EVENT_COL`]'s transient counterpart (see [`LIN_COL_PREFIX`]).
pub const LIN_EVENT_COL: &str = "__lin_event";

/// Prefix of the transient column holding the KEY hash of a fact a positive
/// reference read, for provenance only (see [`SupportRef`]).
pub const LIN_SUP_COL_PREFIX: &str = "__lin_sup_";

/// Prefix of the transient column holding the probability of a fact a
/// positive reference read, for provenance only (see [`SupportRef`]).
pub const LIN_SUPP_COL_PREFIX: &str = "__lin_supp_";

/// One component of a negated IS-reference's anti-join key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegationKey {
    /// The clause variable at the KEY position.
    pub variable: String,
    /// The hidden column of the projected clause body holding the binder's
    /// identity (`__isnot_vid_{k}_{var}`).
    pub body_column: String,
    /// The negated rule's KEY column it is compared with.
    pub fact_column: String,
    /// What the column holds. A node component must be a `UInt64` vid on
    /// both sides; any other component is compared by value.
    pub kind: uni_locy::ColumnKind,
}

/// Prefix for the hidden identity columns described on
/// [`LocyIsRef::negation_keys`].
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
