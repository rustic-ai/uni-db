//! Intermediate representation the oracle evaluates over.
//!
//! These types are the oracle's *own* minimal IR — deliberately independent of
//! the engine's AST/compiled forms — so the oracle and engine share no
//! evaluation code. The [`generator`](crate::generator) emits this IR alongside
//! the equivalent Locy program text from a single source of truth.
//!
//! A fact is a tuple of `i64`s (seeded node `id`s and scalar values, kept apart
//! by each rule column's [`ColKind`]). A plain
//! rule's relation is a set of such tuples; a `FOLD` rule's is one row per
//! group; an `ALONG` rule's is a bag, one row per derivation path; a `BEST BY`
//! rule's is one row per key. A rule is a union of clauses, each a relational
//! join over base tuples plus `IS` / `IS NOT` references to other relations.

// Rust guideline compliant

use std::collections::{BTreeSet, HashMap};

use crate::surface::SurfaceProgram;

/// One derived or base fact: a tuple of `i64` keys (seeded node `id`s).
///
/// The oracle works entirely in `i64` space; the differential harness recovers
/// these ids from the engine's whole-node `YIELD` output via `properties["id"]`.
pub type Tuple = Vec<i64>;

/// A reference from a clause body to another relation: `subjects IS rule [TO target]`.
///
/// The reference names the referenced fact's columns **by position**: the
/// [`subjects`](IsRef::subjects) take columns `0..s`, the
/// [`target`](IsRef::target) (if any) column `s`, and the
/// [`values`](IsRef::values) the columns after it. A position may be a KEY or a
/// value column of the referenced rule; a tuple subject `(o, e, pct)` reaches
/// past the KEYs into the values.
///
/// Binding is order-independent unification: at each position a variable that
/// is bound anywhere else in the clause body (by the `MATCH`, or by another
/// positive reference) is *tested*, an unbound one is *bound*, and a variable
/// repeated across positions binds once and then tests. Which literal binds a
/// variable never changes the clause's answer.
///
/// For a negated reference (`IS NOT`) every variable at a KEY position must be
/// bound by a positive literal; a variable at a value position that nothing
/// binds is existential. The clause keeps only bindings for which *no* fact of
/// the referenced relation unifies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsRef {
    /// Name of the referenced relation (rule).
    pub rule: String,
    /// Local variable names supplying the subject (lookup-key) columns, in order.
    pub subjects: Vec<String>,
    /// Local variable at the reference's `TO` column (the one after the
    /// subjects), if any.
    pub target: Option<String>,
    /// Local variables at the referenced fact's remaining columns, in order,
    /// after the subjects and the target: an `ALONG` value read as `prev.q`, or
    /// a recursive `FOLD`'s folded value.
    pub values: Vec<String>,
}

/// A variable a clause computes after its references are joined:
/// `constant + factor * sum of vars` — an `ALONG` step (`prev.q + e.w`), a fold
/// input (`s + e.w`), a constant seed (`1 AS s`), or a probability in
/// [`PROB_SCALE`] units (`e.w / 10.0` is `factor = PROB_SCALE / 10`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Computed {
    /// The variable bound.
    pub name: String,
    /// Variables summed.
    pub vars: Vec<String>,
    /// Multiplies the sum.
    pub factor: i64,
    /// Added to the product.
    pub constant: i64,
}

/// A probability `p` is held as the integer `round(p * PROB_SCALE)`.
pub const PROB_SCALE: i64 = 1_000_000_000;

/// A probabilistic `IS NOT` (#PROB): when both the referenced rule and this
/// one carry a PROB column, the reference is not an anti-join but a factor:
/// the referenced facts matching the subjects are combined by noisy-OR into
/// `q`, and the clause's probability column is multiplied by `1 - q` (by 1
/// when none match).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbComplement {
    /// The reference (`negated` form; no target).
    pub reference: IsRef,
    /// The referenced rule's PROB column.
    pub prob_column: usize,
    /// This clause's PROB variable, multiplied in place.
    pub prob_var: String,
}

/// A positive reference to a PROB rule from a clause with a PROB variable.
///
/// The row holds only when the referenced fact does, so the fact's
/// probability is multiplied into the clause's PROB variable — the positive
/// mirror of [`ProbComplement`]. A clause whose PROB expression already reads
/// the referenced probability as a factor has no `ProbFactor` for it: it is
/// counted once, by that expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbFactor {
    /// Index of the reference in the clause's
    /// [`pos_refs`](OracleClause::pos_refs).
    pub reference: usize,
    /// The referenced rule's PROB column.
    pub prob_column: usize,
    /// This clause's PROB variable, multiplied in place.
    pub prob_var: String,
}

/// The kind of value a rule column or a clause variable holds.
///
/// Every value is an `i64` in the oracle, so the kind is what keeps values of
/// different kinds apart: a node is its seeded `id`, an integer is itself, a
/// string is an id the generator interns, and a probability is fixed-point
/// (see [`PROB_SCALE`]). Two values of different kinds are never compared;
/// a reference that would compare them is an [`IsRefKindMismatch`] compile
/// error. In particular a scalar KEY is never conflated with a node, even when
/// its integer equals some node's id.
///
/// [`IsRefKindMismatch`]: crate::eval::OracleError::IsRefKindMismatch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColKind {
    /// A node, held as its seeded `id` property.
    Node,
    /// An integer scalar.
    Int,
    /// A string scalar, held as a generator-interned id.
    Str,
    /// A probability in [`PROB_SCALE`] fixed-point units.
    Prob,
}

/// One output column of a rule: its `YIELD` name and its kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleColumn {
    /// The column name, as `YIELD` exposes it.
    pub name: String,
    /// The kind of the values in this column.
    pub kind: ColKind,
}

impl RuleColumn {
    /// A column named `name` holding values of `kind`.
    #[must_use]
    pub fn new(name: impl Into<String>, kind: ColKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }
}

/// A read of a referenced rule's column by name, binding a clause variable.
///
/// As a [bare read](OracleClause::bare_reads) it is the body reading `score`
/// where some positive reference exposes a `score` value column; as a
/// [`prev` read](OracleClause::prev_reads) it is `ALONG ... prev.score`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRead {
    /// The referenced rule's column name.
    pub column: String,
    /// The clause variable holding the column's value. It unifies like any
    /// other reference position: tested when bound elsewhere, else bound.
    pub var: String,
}

impl ColumnRead {
    /// A read of `column` into `var`.
    #[must_use]
    pub fn new(column: impl Into<String>, var: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            var: var.into(),
        }
    }
}

/// A comparison operator of a [`Filter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `=`
    Eq,
    /// `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

impl CmpOp {
    /// Whether `lhs op rhs` holds.
    #[must_use]
    pub fn holds(self, lhs: i64, rhs: i64) -> bool {
        match self {
            Self::Eq => lhs == rhs,
            Self::Ne => lhs != rhs,
            Self::Lt => lhs < rhs,
            Self::Le => lhs <= rhs,
            Self::Gt => lhs > rhs,
            Self::Ge => lhs >= rhs,
        }
    }
}

/// One side of a [`Filter`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operand {
    /// A clause variable's value.
    Var(String),
    /// A constant.
    Const(i64),
}

/// A `WHERE` conjunct over clause variables: `lhs op rhs`.
///
/// A filter is a conjunct, so where it sits among the `WHERE` conditions does
/// not matter: it applies once every variable it reads is bound, which is after
/// the positive literals. Every variable it reads must be bound by one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    /// Left operand.
    pub lhs: Operand,
    /// The comparison.
    pub op: CmpOp,
    /// Right operand.
    pub rhs: Operand,
}

/// One clause (a single `CREATE RULE ... AS` definition) in relational-skeleton form.
///
/// Evaluation of a clause is: start from [`base`](OracleClause::base) bindings,
/// natural-join every positive reference (in any order: the result is the same
/// bag), apply the [`filters`](OracleClause::filters), anti-join each negated
/// reference, compute, then project to [`yield_vars`](OracleClause::yield_vars).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OracleClause {
    /// Base tuples the clause's `MATCH` contributes, known by construction from
    /// the generated graph. Each tuple is indexed by [`var_cols`](Self::var_cols).
    pub base: Vec<Tuple>,
    /// Column index, within each [`base`](Self::base) tuple, of each local variable.
    pub var_cols: HashMap<String, usize>,
    /// Kind of each `MATCH`-bound variable, when known. A variable without an
    /// entry takes its kind from the first reference position it meets, and is
    /// otherwise unchecked.
    pub var_kinds: HashMap<String, ColKind>,
    /// Positive `IS` references, natural-joined; their order is immaterial.
    pub pos_refs: Vec<IsRef>,
    /// Negated `IS NOT` references, applied as anti-joins after every
    /// positive literal, wherever they appear in the program text.
    pub neg_refs: Vec<IsRef>,
    /// `WHERE` conjuncts over bound variables.
    pub filters: Vec<Filter>,
    /// Bare-name reads of referenced value columns.
    ///
    /// Each resolves to the single positive reference that exposes the
    /// column: a **value** column of the referenced rule at a position the
    /// reference does not name. KEY columns are never exposed. A column two
    /// references expose is an [`AmbiguousRuleColumn`] error when read (and
    /// legal when not read); a column none exposes is an [`UnknownColumn`].
    ///
    /// [`AmbiguousRuleColumn`]: crate::eval::OracleError::AmbiguousRuleColumn
    /// [`UnknownColumn`]: crate::eval::OracleError::UnknownColumn
    pub bare_reads: Vec<ColumnRead>,
    /// `ALONG` `prev.<column>` reads, resolved to the clause's one positive
    /// reference to its own rule (any column of it, by name).
    pub prev_reads: Vec<ColumnRead>,
    /// Local variables projected to the `YIELD KEY` columns, in output order.
    pub yield_vars: Vec<String>,
    /// Variables computed after the references, in order.
    pub computed: Vec<Computed>,
    /// Probabilistic `IS NOT` references, applied after `computed`.
    pub prob_complements: Vec<ProbComplement>,
    /// Positive references to PROB rules whose probability is multiplied
    /// into the clause's PROB variable, applied after `computed`.
    pub prob_factors: Vec<ProbFactor>,
}

/// A rule: a named relation defined as the union of its [`clauses`](OracleRule::clauses).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleRule {
    /// The relation name (matches the engine's derived-relation key).
    pub name: String,
    /// The columns of each fact, KEY columns first, in `YIELD` order.
    ///
    /// For a `FOLD` rule these are the fact's columns (keys, then one per
    /// aggregate), not a clause's projected row.
    pub columns: Vec<RuleColumn>,
    /// How many leading [`columns`](Self::columns) are `KEY` columns.
    pub key_count: usize,
    /// Clauses whose results are unioned into this relation.
    pub clauses: Vec<OracleClause>,
    /// A `FOLD` over the rule's rows, when the rule aggregates. A clause may
    /// reference the rule itself: it then reads the **folded** relation (one
    /// row per key, from the previous iteration), and the rule is recomputed
    /// until no folded value moves.
    pub fold: Option<OracleFold>,
    /// The rule's facts are one row per derivation path (an `ALONG` rule,
    /// #159): a **bag**, recomputed from scratch each iteration, in which two
    /// paths with equal values are two facts. Terminates only on acyclic input.
    pub per_path: bool,
    /// `BEST BY`: one row per key, the one with the best criterion.
    pub best: Option<OracleBest>,
}

/// `BEST BY <column> ASC|DESC` over a rule's rows, grouped by its leading keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleBest {
    /// Leading key columns of each row.
    pub key_count: usize,
    /// The criterion's column.
    pub criterion: usize,
    /// `ASC` keeps the least criterion, `DESC` the greatest.
    pub ascending: bool,
}

/// A FOLD aggregate the oracle evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
    /// `COUNT(*)`: rows in the group.
    CountStar,
    /// `COUNT(x)`: non-null inputs in the group (inputs are never null here).
    Count,
    /// `SUM(x)` / `MSUM(x)`.
    Sum,
    /// `MIN(x)` / `MMIN(x)`.
    Min,
    /// `MAX(x)` / `MMAX(x)`.
    Max,
    /// `MNOR(x)`: `1 - prod(1 - x)`, inputs and output in [`PROB_SCALE`] units.
    NoisyOr,
    /// `MPROD(x)`: `prod(x)`, inputs and output in [`PROB_SCALE`] units.
    Product,
}

/// One aggregate of a [`OracleFold`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldAgg {
    /// The aggregate.
    pub agg: Agg,
    /// Column of the clause's projected row holding the input; `None` for
    /// [`Agg::CountStar`].
    pub input: Option<usize>,
}

/// A `FOLD` over a rule's rows: `YIELD KEY k1..kn, agg1, agg2, ...`.
///
/// Each clause projects its `yield_vars` as the `key_count` key columns
/// followed by the aggregates' input columns. Unlike a plain rule, which is a
/// **set**, the fold reads the **bag** of those rows — every binding counts,
/// so parallel edges contribute once each — grouped by the key columns. The
/// fact for a group is its key followed by each aggregate's value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleFold {
    /// Leading key columns of each projected row.
    pub key_count: usize,
    /// The aggregates, in output order.
    pub aggs: Vec<FoldAgg>,
}

/// A stratified program: rules grouped into dependency-ordered strata.
///
/// The generator owns stratum order (it controls program shape); negated
/// references only target rules in strictly earlier strata, so each stratum can
/// be driven to a least fixpoint before the next begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleProgram {
    /// Strata in dependency order; each inner `Vec` is one stratum's rules.
    pub strata: Vec<Vec<OracleRule>>,
}

/// How a relation column's values come back from the engine.
///
/// The differential harness decodes every engine value by its column's wire
/// kind and nothing else: a value of another shape is a divergence, never
/// coerced. In particular an integer column that comes back as a node is
/// reported even when the node's `id` equals the integer (seeded ids equal
/// VIDs, so the coercion would hide exactly the "a KEY is a VID" confusion).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WireKind {
    /// A node, decoded to its seeded `id` property.
    Node,
    /// An integer: `Value::Int` only.
    Int,
    /// A `SUM`/`MSUM` result: an integral `Value::Float`, or a `Value::Int`.
    IntegralFloat,
    /// A string, decoded through [`Generated::strings`].
    Str,
    /// A probability, decoded to [`PROB_SCALE`] fixed point.
    Prob,
}

/// A program shape a generated case exercises; tallied per case so a run
/// can prove it reached the shapes it targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Feature {
    /// A clause with two or more positive `IS` references.
    MultiRef,
    /// A subject at a KEY position bound by a reference, not by the `MATCH`.
    RefBoundKey,
    /// A tuple subject at a value position bound by the reference itself.
    RefBoundValue,
    /// A tuple-form reference binding at least one of its subjects.
    RefBoundTuple,
    /// A `TO` variable bound by one reference and used by another.
    RefBoundTo,
    /// A body variable named like a column of a referenced rule, at another
    /// position than that column's.
    NameCollision,
    /// Two positive references expose a value column of the same name.
    SharedValueName,
    /// The program reads an ambiguous column: a compile error is expected.
    AmbiguousExpected,
    /// `ALONG prev.q` beside another reference exposing `q`.
    PrevBesideValue,
    /// A reference position at an integer KEY column.
    ScalarKeyInt,
    /// A reference position at a string KEY column.
    ScalarKeyStr,
    /// `IS NOT` written before the conjunct that binds one of its variables.
    NegBeforeBinder,
    /// `IS NOT` written after the conjunct that binds its variables.
    NegAfterBinder,
    /// A negation in the `NOT x IS r` form.
    NegPrefixForm,
    /// A negation in the `x IS NOT r` form.
    NegSuffixForm,
}

/// The single-source-of-truth triple emitted by a generator builder.
///
/// The same parameters produce all three faces — the engine consumes
/// [`base_graph_cypher`](Generated::base_graph_cypher) + [`program_text`](Generated::program_text),
/// while the oracle consumes [`oracle_rules`](Generated::oracle_rules) — so the
/// two sides share inputs but no evaluation code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generated {
    /// Cypher that seeds the base graph (nodes carry an integer `id` property).
    pub base_graph_cypher: String,
    /// The Locy program the engine evaluates: [`surface`](Self::surface)
    /// rendered canonically.
    pub program_text: String,
    /// The program text as structured data, renderable under a renaming
    /// and a conjunct order.
    pub surface: SurfaceProgram,
    /// The oracle IR equivalent of `program_text`.
    pub oracle_rules: OracleProgram,
    /// `YIELD` column names per derived relation; drives `FactRow` extraction.
    pub key_schema: HashMap<String, Vec<String>>,
    /// Per derived relation, the wire kind of each [`key_schema`](Self::key_schema) column.
    pub col_kinds: HashMap<String, Vec<WireKind>>,
    /// The id the oracle holds for each string the graph carries.
    pub strings: HashMap<String, i64>,
    /// Per relation, the shapes its clauses exercise (binding-family rules only).
    pub rel_features: HashMap<String, BTreeSet<Feature>>,
    /// Every shape the case exercises: the union of [`rel_features`](Self::rel_features).
    pub features: BTreeSet<Feature>,
}

impl Generated {
    /// The rules `rel` depends on, `rel` included, through any reference.
    #[must_use]
    pub fn dependency_closure(&self, rel: &str) -> BTreeSet<String> {
        let rules: HashMap<&str, &OracleRule> = self
            .oracle_rules
            .strata
            .iter()
            .flatten()
            .map(|r| (r.name.as_str(), r))
            .collect();
        let mut out = BTreeSet::new();
        let mut stack = vec![rel.to_string()];
        while let Some(name) = stack.pop() {
            if !out.insert(name.clone()) {
                continue;
            }
            let Some(rule) = rules.get(name.as_str()) else {
                continue;
            };
            for clause in &rule.clauses {
                let refs = clause
                    .pos_refs
                    .iter()
                    .chain(&clause.neg_refs)
                    .chain(clause.prob_complements.iter().map(|pc| &pc.reference));
                for r in refs {
                    stack.push(r.rule.clone());
                }
            }
        }
        out
    }

    /// The case restricted to the rules `keep` accepts: program text,
    /// oracle IR, schemas and features alike.
    ///
    /// The caller keeps the kept set closed under dependencies (see
    /// [`dependency_closure`](Self::dependency_closure)).
    #[must_use]
    pub fn retain_rules(&self, keep: &dyn Fn(&str) -> bool) -> Self {
        let surface = self.surface.retain(keep);
        let strata: Vec<Vec<OracleRule>> = self
            .oracle_rules
            .strata
            .iter()
            .map(|s| s.iter().filter(|r| keep(&r.name)).cloned().collect())
            .filter(|s: &Vec<OracleRule>| !s.is_empty())
            .collect();
        let pick = |m: &HashMap<String, Vec<String>>| -> HashMap<String, Vec<String>> {
            m.iter()
                .filter(|(k, _)| keep(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        let rel_features: HashMap<String, BTreeSet<Feature>> = self
            .rel_features
            .iter()
            .filter(|(k, _)| keep(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Self {
            base_graph_cypher: self.base_graph_cypher.clone(),
            program_text: surface.canonical_text(),
            surface,
            oracle_rules: OracleProgram { strata },
            key_schema: pick(&self.key_schema),
            col_kinds: self
                .col_kinds
                .iter()
                .filter(|(k, _)| keep(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            strings: self.strings.clone(),
            features: rel_features.values().flatten().copied().collect(),
            rel_features,
        }
    }
}
