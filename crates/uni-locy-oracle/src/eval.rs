//! The naive Datalog fixpoint evaluator — the reference the engine must match.
//!
//! Evaluates an [`OracleProgram`] stratum by stratum,
//! re-deriving every clause to a least fixpoint with structural set semantics.
//! Unoptimized and obviously correct by construction; that is the whole point.
//!
//! The structural set semantics are what make this a useful oracle: a clause's
//! output feeds a [`HashSet`], so duplicate tuples emitted via different
//! intermediates collapse automatically — at *any* scale, with no threshold.
//! That is precisely the property the engine's optimized anti-join must preserve.
//!
//! # Binding
//!
//! A clause body is a natural join: the `MATCH` tuples and every positive `IS`
//! reference are joined on their shared variables, in whatever order the
//! references appear, and the join gives the same bag in every order. At each
//! reference position a bound variable tests and an unbound one binds (see
//! [`IsRef`]). Filters, negated references, computed
//! variables and the projection follow, in that order.
//!
//! # Static checks
//!
//! [`check_program`] resolves every clause before anything is evaluated: bare
//! and `prev` column reads, the kinds at every reference position, the
//! variables each negation, filter, computation and projection needs. A
//! program it rejects models a Locy program the engine must also refuse to
//! compile (or, for [`OracleError::Malformed`], a generator bug). Every check
//! is static because every row of a clause binds the same variables, so a
//! program that passes evaluates without failing.

// Rust guideline compliant

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use crate::ir::{
    Agg, ColKind, ColumnRead, IsRef, Operand, OracleBest, OracleClause, OracleFold, OracleProgram,
    OracleRule, PROB_SCALE, RuleColumn, Tuple,
};

/// A relation: the set of derived (or base) fact tuples for one rule.
pub type Relation = HashSet<Tuple>;

/// A rule's facts as a sorted bag: a set for a plain rule, one row per group
/// for a `FOLD` or `BEST BY` rule, and one row per derivation path (duplicates
/// kept) for an `ALONG` rule.
pub type Bag = Vec<Tuple>;

/// Iterations a stratum may take before the oracle gives up: only a per-path
/// or folded rule over a cycle fails to converge, which is a generator bug.
const MAX_ITERATIONS: usize = 100_000;

/// Why an [`OracleProgram`] cannot be evaluated.
///
/// Every variant except [`Malformed`](Self::Malformed) models a compile error
/// the engine must also raise for the equivalent Locy program (see
/// [`is_language_error`](Self::is_language_error)); `Malformed` is an IR the
/// generator should never have built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleError {
    /// A bare read of a value column that two or more positive references
    /// expose. Bind it positionally instead: `(o, e, p1) IS stake`.
    AmbiguousRuleColumn {
        /// The rule whose clause reads the column.
        rule: String,
        /// The column read.
        column: String,
        /// The rules of the references exposing it, sorted.
        exposed_by: Vec<String>,
    },
    /// A bare or `prev` read of a column no reference exposes — including a
    /// KEY column, which is never exposed by name.
    UnknownColumn {
        /// The rule whose clause reads the column.
        rule: String,
        /// The column read.
        column: String,
    },
    /// A `prev` read in a clause without exactly one positive reference to
    /// its own rule.
    AmbiguousPrevRef {
        /// The rule whose clause reads `prev`.
        rule: String,
        /// How many self-references the clause has (0, or 2 or more).
        self_refs: usize,
    },
    /// A variable at a KEY position of an `IS NOT` that no positive literal
    /// binds.
    IsNotKeyUnbound {
        /// The rule whose clause holds the negation.
        rule: String,
        /// The negated reference's rule.
        reference: String,
        /// The unbound variable.
        var: String,
    },
    /// A variable that meets two kinds: a reference position of one kind and
    /// a binding (or another position) of another — a node at a scalar KEY,
    /// say.
    IsRefKindMismatch {
        /// The rule whose clause holds the reference.
        rule: String,
        /// The referenced rule.
        reference: String,
        /// The referenced column.
        column: String,
        /// The variable at that column.
        var: String,
        /// The column's kind.
        expected: ColKind,
        /// The variable's kind.
        found: ColKind,
    },
    /// A filter, computation, probability or projection reads a variable no
    /// positive literal (or earlier computation) binds.
    UnboundVariable {
        /// The rule whose clause reads it.
        rule: String,
        /// The variable.
        var: String,
        /// Where it is read: `"filter"`, `"computed"`, `"prob"` or `"yield"`.
        context: &'static str,
    },
    /// A reference to a rule the program does not define.
    UnknownRule {
        /// The rule whose clause holds the reference.
        rule: String,
        /// The undefined rule.
        reference: String,
    },
    /// The IR is inconsistent with itself: a generator bug.
    Malformed {
        /// The offending rule.
        rule: String,
        /// What is wrong.
        detail: String,
    },
}

impl OracleError {
    /// Whether this models a compile error of the Locy program itself, which
    /// the engine must also raise, rather than a malformed IR.
    #[must_use]
    pub fn is_language_error(&self) -> bool {
        !matches!(self, Self::Malformed { .. })
    }

    /// The rule whose clause the error is in.
    #[must_use]
    pub fn rule(&self) -> &str {
        match self {
            Self::AmbiguousRuleColumn { rule, .. }
            | Self::UnknownColumn { rule, .. }
            | Self::AmbiguousPrevRef { rule, .. }
            | Self::IsNotKeyUnbound { rule, .. }
            | Self::IsRefKindMismatch { rule, .. }
            | Self::UnboundVariable { rule, .. }
            | Self::UnknownRule { rule, .. }
            | Self::Malformed { rule, .. } => rule,
        }
    }
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AmbiguousRuleColumn {
                rule,
                column,
                exposed_by,
            } => write!(
                f,
                "rule {rule}: column `{column}` is ambiguous, exposed by {exposed_by:?}; \
                 bind it positionally"
            ),
            Self::UnknownColumn { rule, column } => {
                write!(f, "rule {rule}: no reference exposes column `{column}`")
            }
            Self::AmbiguousPrevRef { rule, self_refs } => write!(
                f,
                "rule {rule}: `prev` needs exactly one self-reference, found {self_refs}"
            ),
            Self::IsNotKeyUnbound {
                rule,
                reference,
                var,
            } => write!(
                f,
                "rule {rule}: `{var}` at a KEY position of `IS NOT {reference}` is never bound"
            ),
            Self::IsRefKindMismatch {
                rule,
                reference,
                column,
                var,
                expected,
                found,
            } => write!(
                f,
                "rule {rule}: `{var}` is {found:?} but {reference}.{column} is {expected:?}"
            ),
            Self::UnboundVariable { rule, var, context } => {
                write!(f, "rule {rule}: {context} reads unbound `{var}`")
            }
            Self::UnknownRule { rule, reference } => {
                write!(f, "rule {rule}: references undefined rule {reference}")
            }
            Self::Malformed { rule, detail } => write!(f, "rule {rule}: malformed IR: {detail}"),
        }
    }
}

impl std::error::Error for OracleError {}

/// A probability from its [`PROB_SCALE`] integer form.
fn from_scale(x: i64) -> f64 {
    x as f64 / PROB_SCALE as f64
}

/// A probability in [`PROB_SCALE`] integer form.
fn to_scale(p: f64) -> i64 {
    (p * PROB_SCALE as f64).round() as i64
}

/// The columns of one rule, as a reference to it sees them.
#[derive(Debug, Clone, Copy)]
struct Schema<'a> {
    columns: &'a [RuleColumn],
    key_count: usize,
}

/// Every rule's [`Schema`], by name.
type Schemas<'a> = HashMap<&'a str, Schema<'a>>;

/// A reference resolved to the fact positions it constrains.
#[derive(Debug, Clone)]
struct Lookup {
    /// The referenced rule.
    rule: String,
    /// `(column index, variable)` for every named position: explicit
    /// positions, then any bare or `prev` read resolved to this reference.
    positions: Vec<(usize, String)>,
}

/// A probabilistic complement resolved to its positions.
#[derive(Debug, Clone)]
struct Complement {
    lookup: Lookup,
    prob_column: usize,
    prob_var: String,
}

/// A clause after [`resolve_clause`]: every read resolved, every check passed.
#[derive(Debug, Clone)]
struct Plan<'c> {
    clause: &'c OracleClause,
    positives: Vec<Lookup>,
    negatives: Vec<Lookup>,
    complements: Vec<Complement>,
}

/// One row of a clause body: each bound variable's value.
type Binding = HashMap<String, i64>;

/// The explicit positions of `r`: subjects, then the target, then values.
fn explicit_positions(r: &IsRef) -> Vec<(usize, String)> {
    r.subjects
        .iter()
        .chain(&r.target)
        .chain(&r.values)
        .cloned()
        .enumerate()
        .collect()
}

/// Unifies `fact` with `binding` at `positions`: a bound variable must equal
/// its column, an unbound one is bound (once; a repeat then tests). Returns
/// the extended binding, or `None` when some position disagrees.
fn unify(binding: &Binding, fact: &[i64], positions: &[(usize, String)]) -> Option<Binding> {
    let mut extended = binding.clone();
    for (col, var) in positions {
        let value = fact[*col];
        match extended.get(var) {
            Some(&bound) if bound != value => return None,
            Some(_) => {}
            None => {
                extended.insert(var.clone(), value);
            }
        }
    }
    Some(extended)
}

/// Builds every rule's schema, rejecting a rule inconsistent with itself.
fn schemas(program: &OracleProgram) -> Result<Schemas<'_>, OracleError> {
    let mut out = Schemas::new();
    for rule in program.strata.iter().flatten() {
        let malformed = |detail: String| OracleError::Malformed {
            rule: rule.name.clone(),
            detail,
        };
        if rule.key_count > rule.columns.len() {
            return Err(malformed(format!(
                "key_count {} exceeds {} columns",
                rule.key_count,
                rule.columns.len()
            )));
        }
        let mut names = HashSet::new();
        if let Some(dup) = rule.columns.iter().find(|c| !names.insert(&c.name)) {
            return Err(malformed(format!("duplicate column `{}`", dup.name)));
        }
        if let Some(fold) = &rule.fold
            && (fold.key_count != rule.key_count
                || rule.columns.len() != fold.key_count + fold.aggs.len())
        {
            return Err(malformed(
                "FOLD keys/aggregates disagree with columns".into(),
            ));
        }
        if let Some(best) = &rule.best
            && (best.key_count != rule.key_count || best.criterion >= rule.columns.len())
        {
            return Err(malformed("BEST BY disagrees with columns".into()));
        }
        let schema = Schema {
            columns: &rule.columns,
            key_count: rule.key_count,
        };
        if out.insert(rule.name.as_str(), schema).is_some() {
            return Err(malformed("rule defined twice".into()));
        }
    }
    Ok(out)
}

/// Checks a program statically: every compile error the engine must raise for
/// the equivalent Locy program, and every inconsistency in the IR.
///
/// Passing guarantees [`evaluate_bags`] cannot fail on a binding: every row of
/// a clause binds the same variables, so the checks here are exact.
///
/// # Errors
/// The first [`OracleError`] found, in program order: an ambiguous or unknown
/// column read, a `prev` read without exactly one self-reference, an unbound
/// variable at an `IS NOT` KEY position, a kind mismatch at a reference
/// position, an unbound variable read by a filter, computation or projection,
/// an undefined rule, or a malformed IR.
pub fn check_program(program: &OracleProgram) -> Result<(), OracleError> {
    let schemas = schemas(program)?;
    for rule in program.strata.iter().flatten() {
        for clause in &rule.clauses {
            resolve_clause(rule, clause, &schemas)?;
        }
    }
    Ok(())
}

/// The schema of `reference`, as seen from a clause of `rule`.
fn schema_of<'a>(
    schemas: &Schemas<'a>,
    rule: &OracleRule,
    reference: &str,
) -> Result<Schema<'a>, OracleError> {
    schemas
        .get(reference)
        .copied()
        .ok_or_else(|| OracleError::UnknownRule {
            rule: rule.name.clone(),
            reference: reference.to_string(),
        })
}

/// Resolves `r`'s explicit positions against its schema, rejecting an arity
/// the referenced rule does not have.
fn lookup_of(r: &IsRef, schema: Schema<'_>, rule: &OracleRule) -> Result<Lookup, OracleError> {
    let positions = explicit_positions(r);
    if positions.len() > schema.columns.len() {
        return Err(OracleError::Malformed {
            rule: rule.name.clone(),
            detail: format!(
                "reference to {} names {} positions, it has {} columns",
                r.rule,
                positions.len(),
                schema.columns.len()
            ),
        });
    }
    Ok(Lookup {
        rule: r.rule.clone(),
        positions,
    })
}

/// Resolves one clause of `rule`: the reads, the kinds and every variable
/// the clause needs bound. See [`check_program`] for the errors.
fn resolve_clause<'c>(
    rule: &OracleRule,
    clause: &'c OracleClause,
    schemas: &Schemas<'_>,
) -> Result<Plan<'c>, OracleError> {
    let name = || rule.name.clone();
    let malformed = |detail: String| OracleError::Malformed {
        rule: name(),
        detail,
    };
    if let Some((var, col)) = clause
        .var_cols
        .iter()
        .find(|(_, col)| clause.base.iter().any(|row| **col >= row.len()))
    {
        return Err(malformed(format!(
            "`{var}` reads missing base column {col}"
        )));
    }

    // Positive references and the value columns each exposes by bare name:
    // value columns at positions it does not name.
    let mut positives = Vec::with_capacity(clause.pos_refs.len());
    let mut exposed: BTreeMap<&str, Vec<(usize, usize)>> = BTreeMap::new();
    for (i, r) in clause.pos_refs.iter().enumerate() {
        let schema = schema_of(schemas, rule, &r.rule)?;
        let lookup = lookup_of(r, schema, rule)?;
        let first_exposed = lookup.positions.len().max(schema.key_count);
        for (col, column) in schema.columns.iter().enumerate().skip(first_exposed) {
            exposed
                .entry(column.name.as_str())
                .or_default()
                .push((i, col));
        }
        positives.push(lookup);
    }
    for ColumnRead { column, var } in &clause.bare_reads {
        match exposed.get(column.as_str()).map(Vec::as_slice) {
            None | Some([]) => {
                return Err(OracleError::UnknownColumn {
                    rule: name(),
                    column: column.clone(),
                });
            }
            Some(&[(i, col)]) => positives[i].positions.push((col, var.clone())),
            Some(many) => {
                let mut exposed_by: Vec<String> = many
                    .iter()
                    .map(|&(i, _)| positives[i].rule.clone())
                    .collect();
                exposed_by.sort();
                return Err(OracleError::AmbiguousRuleColumn {
                    rule: name(),
                    column: column.clone(),
                    exposed_by,
                });
            }
        }
    }
    if !clause.prev_reads.is_empty() {
        let self_refs: Vec<usize> = (0..positives.len())
            .filter(|&i| positives[i].rule == rule.name)
            .collect();
        let &[own] = self_refs.as_slice() else {
            return Err(OracleError::AmbiguousPrevRef {
                rule: name(),
                self_refs: self_refs.len(),
            });
        };
        for ColumnRead { column, var } in &clause.prev_reads {
            let col = rule
                .columns
                .iter()
                .position(|c| &c.name == column)
                .ok_or_else(|| OracleError::UnknownColumn {
                    rule: name(),
                    column: column.clone(),
                })?;
            positives[own].positions.push((col, var.clone()));
        }
    }

    // What the positive literals bind, and the kind of each variable.
    let mut bound: HashSet<&str> = clause.var_cols.keys().map(String::as_str).collect();
    let mut kinds: HashMap<String, ColKind> = clause.var_kinds.clone();
    for lookup in &positives {
        let schema = schemas[lookup.rule.as_str()];
        for (col, var) in &lookup.positions {
            bound.insert(var);
            check_kind(rule, lookup, schema, *col, var, &mut kinds, true)?;
        }
    }

    for f in &clause.filters {
        for operand in [&f.lhs, &f.rhs] {
            if let Operand::Var(v) = operand
                && !bound.contains(v.as_str())
            {
                return Err(OracleError::UnboundVariable {
                    rule: name(),
                    var: v.clone(),
                    context: "filter",
                });
            }
        }
    }

    // Negations: KEY positions must be bound, value positions may be
    // existential.
    let negated =
        |r: &IsRef, kinds: &mut HashMap<String, ColKind>| -> Result<Lookup, OracleError> {
            let schema = schema_of(schemas, rule, &r.rule)?;
            let lookup = lookup_of(r, schema, rule)?;
            for (col, var) in &lookup.positions {
                if *col < schema.key_count && !bound.contains(var.as_str()) {
                    return Err(OracleError::IsNotKeyUnbound {
                        rule: name(),
                        reference: r.rule.clone(),
                        var: var.clone(),
                    });
                }
                check_kind(rule, &lookup, schema, *col, var, kinds, false)?;
            }
            Ok(lookup)
        };
    let mut negatives = Vec::with_capacity(clause.neg_refs.len());
    for r in &clause.neg_refs {
        negatives.push(negated(r, &mut kinds)?);
    }
    for pf in &clause.prob_factors {
        let Some(lookup) = positives.get(pf.reference) else {
            return Err(malformed(format!(
                "PROB factor reference {} is out of range",
                pf.reference
            )));
        };
        if pf.prob_column >= schemas[lookup.rule.as_str()].columns.len() {
            return Err(malformed(format!(
                "PROB column {} is out of range",
                pf.prob_column
            )));
        }
    }
    let mut complements = Vec::with_capacity(clause.prob_complements.len());
    for pc in &clause.prob_complements {
        let lookup = negated(&pc.reference, &mut kinds)?;
        if pc.prob_column >= schemas[pc.reference.rule.as_str()].columns.len() {
            return Err(malformed(format!(
                "PROB column {} is out of range",
                pc.prob_column
            )));
        }
        complements.push(Complement {
            lookup,
            prob_column: pc.prob_column,
            prob_var: pc.prob_var.clone(),
        });
    }

    let unbound = |var: &str, context| OracleError::UnboundVariable {
        rule: name(),
        var: var.to_string(),
        context,
    };
    for c in &clause.computed {
        if let Some(v) = c.vars.iter().find(|v| !bound.contains(v.as_str())) {
            return Err(unbound(v, "computed"));
        }
        bound.insert(&c.name);
    }
    if let Some(var) = clause
        .prob_complements
        .iter()
        .map(|pc| &pc.prob_var)
        .chain(clause.prob_factors.iter().map(|pf| &pf.prob_var))
        .find(|v| !bound.contains(v.as_str()))
    {
        return Err(unbound(var, "prob"));
    }
    if let Some(v) = clause
        .yield_vars
        .iter()
        .find(|v| !bound.contains(v.as_str()))
    {
        return Err(unbound(v, "yield"));
    }
    let width_ok = match &rule.fold {
        Some(fold) => {
            clause.yield_vars.len() >= fold.key_count
                && fold
                    .aggs
                    .iter()
                    .filter_map(|a| a.input)
                    .all(|i| i < clause.yield_vars.len())
        }
        None => clause.yield_vars.len() == rule.columns.len(),
    };
    if !width_ok {
        return Err(malformed(format!(
            "clause yields {} columns, the rule has {}",
            clause.yield_vars.len(),
            rule.columns.len()
        )));
    }
    Ok(Plan {
        clause,
        positives,
        negatives,
        complements,
    })
}

/// Checks `var`'s kind against column `col` of `lookup`'s rule. A positive
/// position (`binds`) also records the kind of a variable seen first here.
fn check_kind(
    rule: &OracleRule,
    lookup: &Lookup,
    schema: Schema<'_>,
    col: usize,
    var: &str,
    kinds: &mut HashMap<String, ColKind>,
    binds: bool,
) -> Result<(), OracleError> {
    let column = &schema.columns[col];
    match kinds.get(var) {
        Some(&found) if found != column.kind => Err(OracleError::IsRefKindMismatch {
            rule: rule.name.clone(),
            reference: lookup.rule.clone(),
            column: column.name.clone(),
            var: var.to_string(),
            expected: column.kind,
            found,
        }),
        Some(_) => Ok(()),
        None => {
            if binds {
                kinds.insert(var.to_string(), column.kind);
            }
            Ok(())
        }
    }
}

/// Evaluates a program to its least fixpoint, returning each rule's relation
/// as a set. See [`evaluate_bags`] for the multiplicities.
///
/// # Panics
/// As [`evaluate_bags`].
#[must_use]
pub fn evaluate(program: &OracleProgram) -> HashMap<String, Relation> {
    evaluate_bags(program)
        .into_iter()
        .map(|(name, bag)| (name, bag.into_iter().collect()))
        .collect()
}

/// Evaluates a program to its fixpoint, returning each rule's facts as a bag.
///
/// The fallible form is [`try_evaluate_bags`]; use it for a program that is
/// meant to model a compile error.
///
/// # Panics
/// Panics if [`check_program`] rejects the program, or a stratum does not
/// converge (a per-path or folded rule over a cycle) — generator bugs.
#[must_use]
pub fn evaluate_bags(program: &OracleProgram) -> HashMap<String, Bag> {
    try_evaluate_bags(program).unwrap_or_else(|e| panic!("oracle program rejected: {e}"))
}

/// Checks a program with [`check_program`], then evaluates it to its fixpoint,
/// returning each rule's facts as a bag.
///
/// Strata are processed in order. Within a stratum every rule is recomputed
/// from the current relations until none changes: a plain rule accumulates
/// (a set), while a `FOLD`, `BEST BY` or per-path rule is *replaced* by its
/// recomputation — so a self-reference reads the previous iteration's folded
/// values, best rows or paths. Because negated references only target strictly
/// earlier strata, those relations are complete when a later stratum reads them.
///
/// # Errors
/// The [`OracleError`] from [`check_program`].
///
/// # Panics
/// Panics if a stratum does not converge (a per-path or folded rule over a
/// cycle) — a generator bug.
pub fn try_evaluate_bags(program: &OracleProgram) -> Result<HashMap<String, Bag>, OracleError> {
    let schemas = schemas(program)?;
    let mut strata: Vec<Vec<(&OracleRule, Vec<Plan<'_>>)>> = Vec::new();
    for stratum in &program.strata {
        let mut planned = Vec::with_capacity(stratum.len());
        for rule in stratum {
            let plans = rule
                .clauses
                .iter()
                .map(|c| resolve_clause(rule, c, &schemas))
                .collect::<Result<Vec<_>, _>>()?;
            planned.push((rule, plans));
        }
        strata.push(planned);
    }

    let mut rels: HashMap<String, Bag> = HashMap::new();
    for stratum in &strata {
        let mut iterations = 0;
        loop {
            let mut changed = false;
            for (rule, plans) in stratum {
                let rows: Vec<Tuple> = plans.iter().flat_map(|p| eval_plan(p, &rels)).collect();
                let mut next = if let Some(fold) = &rule.fold {
                    fold_rows(fold, rows)
                } else if let Some(best) = &rule.best {
                    best_rows(best, rows)
                } else if rule.per_path {
                    rows
                } else {
                    let mut all = rels.get(&rule.name).cloned().unwrap_or_default();
                    all.extend(rows);
                    all.sort();
                    all.dedup();
                    all
                };
                next.sort();
                if rels.get(&rule.name) != Some(&next) {
                    changed = true;
                    rels.insert(rule.name.clone(), next);
                }
            }
            if !changed {
                break;
            }
            iterations += 1;
            assert!(
                iterations < MAX_ITERATIONS,
                "a stratum did not converge: a per-path or folded rule over a cycle"
            );
        }
    }
    Ok(rels)
}

/// A FOLD over the bag of a rule's rows, grouped by key: one fact per group.
///
/// # Panics
/// Panics if a row is shorter than the fold's columns (a generator bug).
fn fold_rows(fold: &OracleFold, rows: Vec<Tuple>) -> Bag {
    let mut groups: HashMap<Tuple, Vec<Tuple>> = HashMap::new();
    for row in rows {
        groups
            .entry(row[..fold.key_count].to_vec())
            .or_default()
            .push(row);
    }
    groups
        .into_iter()
        .map(|(key, rows)| {
            let mut fact = key;
            for a in &fold.aggs {
                let inputs = || rows.iter().map(|r| r[a.input.expect("an input column")]);
                fact.push(match a.agg {
                    Agg::CountStar => rows.len() as i64,
                    Agg::Count => inputs().count() as i64,
                    Agg::Sum => inputs().sum(),
                    Agg::Min => inputs().min().expect("a group has a row"),
                    Agg::Max => inputs().max().expect("a group has a row"),
                    Agg::NoisyOr => {
                        to_scale(1.0 - inputs().map(|x| 1.0 - from_scale(x)).product::<f64>())
                    }
                    Agg::Product => to_scale(inputs().map(from_scale).product::<f64>()),
                });
            }
            fact
        })
        .collect()
}

/// `BEST BY`: per key, the rows with the best criterion (duplicates removed).
fn best_rows(best: &OracleBest, rows: Vec<Tuple>) -> Bag {
    let mut winners: HashMap<Tuple, Vec<Tuple>> = HashMap::new();
    for row in rows {
        let group = winners.entry(row[..best.key_count].to_vec()).or_default();
        let better = |a: i64, b: i64| if best.ascending { a < b } else { a > b };
        match group.first().map(|r| r[best.criterion]) {
            Some(current) if better(current, row[best.criterion]) => {}
            Some(current) if current == row[best.criterion] => group.push(row),
            _ => *group = vec![row],
        }
    }
    let mut out: Bag = winners.into_values().flatten().collect();
    out.sort();
    out.dedup();
    out
}

/// The value of a filter operand in `binding`.
fn operand(binding: &Binding, o: &Operand) -> i64 {
    match o {
        Operand::Var(v) => binding[v],
        Operand::Const(c) => *c,
    }
}

/// Evaluates one resolved clause against the current relations: join,
/// filter, anti-join, compute, project.
///
/// The pipeline is: seed bindings from the clause's base tuples, natural-join
/// every positive reference (each fact unifies with a binding at the
/// reference's positions), keep the bindings every filter accepts, drop those
/// some fact of a negated reference unifies with, compute the derived
/// variables and the probabilistic complements, then project to the `YIELD`
/// columns. Every row of a referenced bag joins, so a per-path relation
/// contributes once per path. The join is a natural join of bags, which is
/// commutative and associative, so the order of the positive references does
/// not change the result.
///
/// # Panics
/// Never for a plan from [`resolve_clause`], which checked every variable
/// read here is bound.
fn eval_plan(plan: &Plan<'_>, rels: &HashMap<String, Bag>) -> Vec<Tuple> {
    let clause = plan.clause;
    let mut bindings: Vec<Binding> = clause
        .base
        .iter()
        .map(|row| {
            clause
                .var_cols
                .iter()
                .map(|(var, &col)| (var.clone(), row[col]))
                .collect()
        })
        .collect();

    let empty = Bag::new();
    let facts = |rule: &str| rels.get(rule).unwrap_or(&empty);
    // Each binding carries the product of the referenced probabilities its
    // `ProbFactor`s multiply in, beside it (bindings are plain maps).
    let mut factors: Vec<f64> = vec![1.0; bindings.len()];
    for (i, lookup) in plan.positives.iter().enumerate() {
        let referenced = facts(&lookup.rule);
        let column = clause
            .prob_factors
            .iter()
            .find(|pf| pf.reference == i)
            .map(|pf| pf.prob_column);
        let mut joined = Vec::new();
        let mut joined_factors = Vec::new();
        for (b, factor) in bindings.iter().zip(&factors) {
            for fact in referenced {
                if let Some(next) = unify(b, fact, &lookup.positions) {
                    joined.push(next);
                    joined_factors.push(match column {
                        Some(c) => factor * from_scale(fact[c]),
                        None => *factor,
                    });
                }
            }
        }
        bindings = joined;
        factors = joined_factors;
    }

    let mut paired: Vec<(Binding, f64)> = bindings.into_iter().zip(factors).collect();
    paired.retain(|(b, _)| {
        clause
            .filters
            .iter()
            .all(|f| f.op.holds(operand(b, &f.lhs), operand(b, &f.rhs)))
    });

    for lookup in &plan.negatives {
        let banned = facts(&lookup.rule);
        paired.retain(|(b, _)| {
            !banned
                .iter()
                .any(|fact| unify(b, fact, &lookup.positions).is_some())
        });
    }

    let mut bindings = Vec::with_capacity(paired.len());
    for (mut b, factor) in paired {
        for c in &clause.computed {
            let value = c.constant + c.factor * c.vars.iter().map(|v| b[v]).sum::<i64>();
            b.insert(c.name.clone(), value);
        }
        for pc in &plan.complements {
            let none = facts(&pc.lookup.rule)
                .iter()
                .filter(|fact| unify(&b, fact, &pc.lookup.positions).is_some())
                .map(|fact| 1.0 - from_scale(fact[pc.prob_column]))
                .product::<f64>();
            // `1 - q` with `q = 1 - none`: the complement is `none` itself.
            let p = from_scale(b[&pc.prob_var]) * none;
            b.insert(pc.prob_var.clone(), to_scale(p));
        }
        if let Some(pf) = clause.prob_factors.first() {
            // Every factor of a clause multiplies its one PROB variable.
            let p = from_scale(b[&pf.prob_var]) * factor;
            b.insert(pf.prob_var.clone(), to_scale(p));
        }
        bindings.push(b);
    }
    bindings
        .into_iter()
        .map(|b| clause.yield_vars.iter().map(|v| b[v]).collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::{
        build_complement, build_layered_dag, build_union, expected_closure_size,
    };
    use crate::ir::{ColKind, IsRef, OracleClause, OracleProgram, OracleRule, RuleColumn};

    /// Columns from `(name, kind)` pairs.
    fn cols(spec: &[(&str, ColKind)]) -> Vec<RuleColumn> {
        spec.iter().map(|(n, k)| RuleColumn::new(*n, *k)).collect()
    }

    /// Oracle closure must equal the closed-form size across a grid spanning 300.
    #[test]
    fn closure_matches_closed_form_grid() {
        for stages in 1..6 {
            for width in 1..11 {
                let g = build_layered_dag(stages, width);
                let rels = evaluate(&g.oracle_rules);
                let got = rels.get("reaches").map_or(0, Relation::len);
                assert_eq!(
                    got,
                    expected_closure_size(stages, width),
                    "stages={stages} width={width}"
                );
            }
        }
    }

    /// Specific cases that straddle the engine's 300-fact threshold.
    #[test]
    fn closure_matches_closed_form_across_threshold() {
        for &(s, w) in &[(3, 10), (2, 20), (6, 5), (4, 15)] {
            let g = build_layered_dag(s, w);
            let rels = evaluate(&g.oracle_rules);
            assert_eq!(
                rels["reaches"].len(),
                expected_closure_size(s, w),
                "stages={s} width={w}"
            );
        }
    }

    /// Evaluation is order-independent: two runs yield identical relations.
    #[test]
    fn evaluation_is_deterministic() {
        let g = build_layered_dag(3, 8);
        assert_eq!(evaluate(&g.oracle_rules), evaluate(&g.oracle_rules));
    }

    /// Generated complement: `unreached` = all-pairs minus the closure, exactly.
    #[test]
    fn build_complement_oracle_is_exact() {
        for &(s, w) in &[(3, 5), (4, 12)] {
            let g = build_complement(s, w);
            let rels = evaluate(&g.oracle_rules);
            let total_pairs = (s * w) * (s * w);
            assert_eq!(
                rels["unreached"].len(),
                total_pairs - expected_closure_size(s, w),
                "({s},{w})"
            );
        }
    }

    /// Generated union: `linked` = disjoint forward ∪ reverse = `2 · |edges|`.
    #[test]
    fn build_union_oracle_is_exact() {
        for &(s, w) in &[(2, 4), (3, 5)] {
            let g = build_union(s, w);
            let rels = evaluate(&g.oracle_rules);
            let forward = w * w * s.saturating_sub(1);
            assert_eq!(rels["linked"].len(), 2 * forward, "({s},{w})");
        }
    }

    /// A FOLD reads the bag: two identical parallel edges count twice.
    #[test]
    fn fold_counts_every_row() {
        use crate::ir::{Agg, FoldAgg, OracleFold};
        // Edges [src, dst, index, weight]: two identical 0->1 edges and 0->2.
        let edges: Vec<Tuple> = vec![vec![0, 1, 0, 2], vec![0, 1, 1, 2], vec![0, 2, 2, 5]];
        let deg = OracleRule {
            per_path: false,
            best: None,
            name: "deg".to_string(),
            columns: cols(&[
                ("a", ColKind::Node),
                ("n", ColKind::Int),
                ("s", ColKind::Int),
                ("lo", ColKind::Int),
                ("hi", ColKind::Int),
                ("c", ColKind::Int),
            ]),
            key_count: 1,
            clauses: vec![OracleClause {
                computed: Vec::new(),
                prob_complements: Vec::new(),
                base: edges,
                var_cols: HashMap::from([("a".to_string(), 0), ("w".to_string(), 3)]),
                pos_refs: Vec::new(),
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "w".to_string()],
                ..OracleClause::default()
            }],
            fold: Some(OracleFold {
                key_count: 1,
                aggs: [
                    (Agg::CountStar, None),
                    (Agg::Sum, Some(1)),
                    (Agg::Min, Some(1)),
                    (Agg::Max, Some(1)),
                    (Agg::Count, Some(1)),
                ]
                .into_iter()
                .map(|(agg, input)| FoldAgg { agg, input })
                .collect(),
            }),
        };
        let rels = evaluate(&OracleProgram {
            strata: vec![vec![deg]],
        });
        let expected: Relation = [vec![0, 3, 9, 2, 5, 3]].into_iter().collect();
        assert_eq!(rels["deg"], expected);
    }

    /// Generator-built rules for a hand-sized graph, evaluated as bags.
    fn shape_bags(
        edges: Vec<crate::generator::RandomEdge>,
        nodes: usize,
        configure: impl FnOnce(&mut crate::generator::RandomShape),
    ) -> HashMap<String, Bag> {
        let mut shape = crate::generator::RandomShape {
            nodes,
            edges,
            reverse: false,
            union: false,
            negation: false,
            folds: [false; 3],
            along: false,
            rollup: false,
            best: None,
            prob: false,
            binding: crate::generator::BindingShape::none(),
            recursion: crate::generator::RecursionShape::none(),
        };
        configure(&mut shape);
        evaluate_bags(&crate::generator::build_random_program(&shape).oracle_rules)
    }

    /// ALONG facts are per path: 0->1 then two parallel 1->2 edges of equal
    /// weight give two `(0, 2, 3)` facts, and a FOLD over them counts both.
    #[test]
    fn along_is_one_fact_per_path() {
        let rels = shape_bags(
            vec![(0, 1, 1, false), (1, 2, 2, false), (1, 2, 2, false)],
            3,
            |s| {
                s.along = true;
            },
        );
        assert_eq!(
            rels["cost"],
            vec![
                vec![0, 1, 1],
                vec![0, 2, 3],
                vec![0, 2, 3],
                vec![1, 2, 2],
                vec![1, 2, 2]
            ]
        );
        assert_eq!(rels["tot"], vec![vec![0, 3, 7], vec![1, 2, 4]]);
    }

    /// BEST BY ASC over a cycle is the shortest path; DESC over the acyclic
    /// edges the longest.
    #[test]
    fn best_by_is_the_shortest_and_longest_path() {
        // 0 -1-> 1 -1-> 2, 0 -5-> 2, 2 -1-> 0 (a cycle).
        let edges = vec![
            (0, 1, 1, false),
            (1, 2, 1, false),
            (0, 2, 5, false),
            (2, 0, 1, false),
        ];
        let short = shape_bags(edges.clone(), 3, |s| s.best = Some(true));
        let pick = |rels: &HashMap<String, Bag>, a: i64, b: i64| {
            rels["opt"]
                .iter()
                .find(|r| r[0] == a && r[1] == b)
                .map(|r| r[2])
        };
        assert_eq!(pick(&short, 0, 2), Some(2));
        assert_eq!(pick(&short, 0, 0), Some(3));
        assert_eq!(pick(&short, 2, 1), Some(2));
        let long = shape_bags(edges, 3, |s| s.best = Some(false));
        assert_eq!(pick(&long, 0, 2), Some(5));
        assert_eq!(pick(&long, 2, 0), None, "the 2->0 edge is not acyclic");
    }

    /// A recursive FOLD reads each child's folded value: `s(a) = 1 +
    /// sum over a->c of (s(c) + w)`.
    #[test]
    fn recursive_fold_rolls_up_folded_values() {
        // 0 -2-> 1 -3-> 2, 0 -1-> 2. s(2)=1, s(1)=1+(1+3)=5, s(0)=1+(5+2)+(1+1)=10.
        let rels = shape_bags(
            vec![(0, 1, 2, false), (1, 2, 3, false), (0, 2, 1, false)],
            3,
            |s| {
                s.rollup = true;
            },
        );
        assert_eq!(rels["roll"], vec![vec![0, 10], vec![1, 5], vec![2, 1]]);
    }

    /// MNOR / MPROD over the bag of edges, and the probabilistic complement.
    #[test]
    fn probabilistic_rules() {
        // Two parallel 0->1 edges (0.2, 0.5) and 1->0 (0.3).
        let rels = shape_bags(
            vec![(0, 1, 2, false), (0, 1, 5, false), (1, 0, 3, false)],
            2,
            |s| {
                s.prob = true;
            },
        );
        let scale = |p: f64| (p * PROB_SCALE as f64).round() as i64;
        assert_eq!(
            rels["nor"],
            vec![vec![0, 1, scale(0.6)], vec![1, 0, scale(0.3)]]
        );
        assert_eq!(
            rels["prd"],
            vec![vec![0, 1, scale(0.1)], vec![1, 0, scale(0.3)]]
        );
        assert_eq!(
            rels["pr"],
            vec![
                vec![0, 1, scale(0.2)],
                vec![0, 1, scale(0.5)],
                vec![1, 0, scale(0.3)]
            ]
        );
        assert_eq!(
            rels["safe"],
            vec![
                vec![0, 0, scale(1.0)],
                vec![0, 1, scale(0.4)],
                vec![1, 0, scale(0.7)],
                vec![1, 1, scale(1.0)],
            ]
        );
    }

    /// A positive reference to a PROB rule multiplies the referenced
    /// probability in. Where no two proofs of a `hop` fact share an edge,
    /// that is its possible-worlds probability: over every subset of the
    /// independent edges (edge `w` present with probability `w / 10`), the
    /// weight of the worlds holding an edge `a -> m` and an edge `m -> b`.
    #[test]
    fn positive_reference_is_the_possible_worlds_probability_without_sharing() {
        let mut seed: u64 = 0x0dd_ba11;
        let mut next = |bound: u64| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) % bound
        };
        let mut checked = 0;
        for _ in 0..40 {
            let nodes = 3;
            let edges: Vec<(i64, i64, i64)> = (0..2 + next(5))
                .map(|_| (next(nodes) as i64, next(nodes) as i64, 1 + next(9) as i64))
                .collect();
            let rels = shape_bags(
                edges.iter().map(|&(a, b, w)| (a, b, w, false)).collect(),
                nodes as usize,
                |s| s.prob = true,
            );
            for fact in rels.get("hop").into_iter().flatten() {
                let (a, b) = (fact[0], fact[1]);
                // Shared lineage: two `a -> m` edges to one `m` both extend
                // `nor(m, b)`, or a self-loop is both the hop and its `nor`.
                let firsts: Vec<i64> = edges
                    .iter()
                    .filter(|e| e.0 == a && edges.iter().any(|f| f.0 == e.1 && f.1 == b))
                    .map(|e| e.1)
                    .collect();
                let mut distinct = firsts.clone();
                distinct.sort_unstable();
                distinct.dedup();
                if distinct.len() != firsts.len() || firsts.contains(&a) {
                    continue;
                }
                let mut worlds = 0.0;
                for world in 0u32..(1 << edges.len()) {
                    let on = |i: usize| world & (1 << i) != 0;
                    let weight: f64 = edges
                        .iter()
                        .enumerate()
                        .map(|(i, e)| {
                            let p = e.2 as f64 / 10.0;
                            if on(i) { p } else { 1.0 - p }
                        })
                        .product();
                    let holds = edges.iter().enumerate().any(|(i, e)| {
                        on(i)
                            && e.0 == a
                            && edges
                                .iter()
                                .enumerate()
                                .any(|(j, f)| on(j) && f.0 == e.1 && f.1 == b)
                    });
                    if holds {
                        worlds += weight;
                    }
                }
                let scaled = (worlds * PROB_SCALE as f64).round() as i64;
                assert!(
                    (fact[2] - scaled).abs() <= 2,
                    "hop({a},{b}) over {edges:?}: oracle {}, possible worlds {scaled}",
                    fact[2]
                );
                checked += 1;
            }
        }
        assert!(
            checked > 10,
            "only {checked} unshared hop facts were checked"
        );
    }

    /// A probabilistic `IS NOT` multiplies in `1 - p`. `safe(a, b)` negates
    /// the one `nor(a, b)` fact and reads nothing else, so `1 - p` is its
    /// possible-worlds probability: the weight of the worlds with no edge
    /// `a -> b`. (Default mode only: where the negated fact shares an edge
    /// with the rest of a row, `1 - p` is not exact, and the engine reports
    /// `SharedProbabilisticDependency`; the oracle models default mode.)
    #[test]
    fn probabilistic_complement_is_the_possible_worlds_probability_without_sharing() {
        let mut seed: u64 = 0x5afe_ba11;
        let mut next = |bound: u64| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) % bound
        };
        let mut checked = 0;
        for _ in 0..20 {
            let nodes = 3;
            let edges: Vec<(i64, i64, i64)> = (0..2 + next(5))
                .map(|_| (next(nodes) as i64, next(nodes) as i64, 1 + next(9) as i64))
                .collect();
            let rels = shape_bags(
                edges.iter().map(|&(a, b, w)| (a, b, w, false)).collect(),
                nodes as usize,
                |s| s.prob = true,
            );
            for fact in rels.get("safe").into_iter().flatten() {
                let (a, b) = (fact[0], fact[1]);
                let worlds: f64 = edges
                    .iter()
                    .filter(|e| e.0 == a && e.1 == b)
                    .map(|e| 1.0 - e.2 as f64 / 10.0)
                    .product();
                let scaled = (worlds * PROB_SCALE as f64).round() as i64;
                assert!(
                    (fact[2] - scaled).abs() <= 2,
                    "safe({a},{b}) over {edges:?}: oracle {}, possible worlds {scaled}",
                    fact[2]
                );
                checked += 1;
            }
        }
        assert!(checked > 20, "only {checked} safe facts were checked");
    }

    /// Stratified `IS NOT`: `unreached` is the exact complement of `reaches`.
    ///
    /// Graph 0->1->2. reaches = {(0,1),(1,2),(0,2)}; all 9 ordered pairs minus
    /// those 3 = 6 unreached pairs. Exercises the anti-join + stratum ordering.
    #[test]
    fn stratified_complement_is_exact() {
        let edges: Vec<Tuple> = vec![vec![0, 1], vec![1, 2]];
        let all_pairs: Vec<Tuple> = (0..3)
            .flat_map(|a| (0..3).map(move |b| vec![a, b]))
            .collect();

        let reaches = OracleRule {
            per_path: false,
            best: None,
            fold: None,
            name: "reaches".to_string(),
            columns: cols(&[("a", ColKind::Node), ("b", ColKind::Node)]),
            key_count: 2,
            clauses: vec![
                OracleClause {
                    computed: Vec::new(),
                    prob_complements: Vec::new(),
                    base: edges.clone(),
                    var_cols: HashMap::from([("a".to_string(), 0), ("b".to_string(), 1)]),
                    pos_refs: Vec::new(),
                    neg_refs: Vec::new(),
                    yield_vars: vec!["a".to_string(), "b".to_string()],
                    ..OracleClause::default()
                },
                OracleClause {
                    computed: Vec::new(),
                    prob_complements: Vec::new(),
                    base: edges,
                    var_cols: HashMap::from([("a".to_string(), 0), ("mid".to_string(), 1)]),
                    pos_refs: vec![IsRef {
                        values: Vec::new(),
                        rule: "reaches".to_string(),
                        subjects: vec!["mid".to_string()],
                        target: Some("b".to_string()),
                    }],
                    neg_refs: Vec::new(),
                    yield_vars: vec!["a".to_string(), "b".to_string()],
                    ..OracleClause::default()
                },
            ],
        };
        let unreached = OracleRule {
            per_path: false,
            best: None,
            fold: None,
            name: "unreached".to_string(),
            columns: cols(&[("a", ColKind::Node), ("b", ColKind::Node)]),
            key_count: 2,
            clauses: vec![OracleClause {
                computed: Vec::new(),
                prob_complements: Vec::new(),
                base: all_pairs,
                var_cols: HashMap::from([("a".to_string(), 0), ("b".to_string(), 1)]),
                pos_refs: Vec::new(),
                neg_refs: vec![IsRef {
                    values: Vec::new(),
                    rule: "reaches".to_string(),
                    subjects: vec!["a".to_string(), "b".to_string()],
                    target: None,
                }],
                yield_vars: vec!["a".to_string(), "b".to_string()],
                ..OracleClause::default()
            }],
        };

        let program = OracleProgram {
            strata: vec![vec![reaches], vec![unreached]],
        };
        let rels = evaluate(&program);

        let expected_reaches: Relation = [vec![0, 1], vec![1, 2], vec![0, 2]].into_iter().collect();
        assert_eq!(rels["reaches"], expected_reaches);

        let expected_unreached: Relation = [
            vec![0, 0],
            vec![1, 0],
            vec![1, 1],
            vec![2, 0],
            vec![2, 1],
            vec![2, 2],
        ]
        .into_iter()
        .collect();
        assert_eq!(rels["unreached"], expected_unreached);
    }

    // ── Order-independent binding (#300) ────────────────────────────────────

    use crate::ir::{CmpOp, ColumnRead, Filter, Operand};
    use proptest::prelude::*;

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    fn isref(rule: &str, subjects: &[&str], target: Option<&str>, values: &[&str]) -> IsRef {
        IsRef {
            rule: rule.to_string(),
            subjects: strs(subjects),
            target: target.map(str::to_string),
            values: strs(values),
        }
    }

    /// A plain rule with no clauses: a schema for hand-written relations.
    fn schema_rule(name: &str, spec: &[(&str, ColKind)], key_count: usize) -> OracleRule {
        OracleRule {
            name: name.to_string(),
            columns: cols(spec),
            key_count,
            clauses: Vec::new(),
            fold: None,
            per_path: false,
            best: None,
        }
    }

    /// `owner` holding the single clause `clause`.
    fn with_clause(mut owner: OracleRule, clause: OracleClause) -> OracleRule {
        owner.clauses = vec![clause];
        owner
    }

    /// Evaluates `owner`'s first clause once against the hand relations
    /// `rels`, with `others` supplying the referenced schemas: the sorted bag,
    /// or the static error.
    fn eval_once(
        owner: &OracleRule,
        others: &[OracleRule],
        rels: &[(&str, Bag)],
    ) -> Result<Bag, OracleError> {
        let program = OracleProgram {
            strata: vec![others.to_vec(), vec![owner.clone()]],
        };
        let schemas = schemas(&program)?;
        let owner = &program.strata[1][0];
        let plan = resolve_clause(owner, &owner.clauses[0], &schemas)?;
        let rels: HashMap<String, Bag> = rels
            .iter()
            .map(|(n, b)| ((*n).to_string(), b.clone()))
            .collect();
        let mut out = eval_plan(&plan, &rels);
        out.sort();
        Ok(out)
    }

    /// Every permutation of `items`.
    fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut out = Vec::new();
        for i in 0..items.len() {
            let mut rest = items.to_vec();
            let head = rest.remove(i);
            for mut tail in permutations(&rest) {
                tail.insert(0, head.clone());
                out.push(tail);
            }
        }
        out
    }

    /// `owner` evaluated under every order of its clause's positive
    /// references; asserts they agree and returns the common answer.
    fn eval_every_order(
        owner: &OracleRule,
        others: &[OracleRule],
        rels: &[(&str, Bag)],
    ) -> Result<Bag, OracleError> {
        let clause = &owner.clauses[0];
        let mut answers = permutations(&clause.pos_refs).into_iter().map(|refs| {
            let mut permuted = owner.clone();
            permuted.clauses[0].pos_refs = refs;
            eval_once(&permuted, others, rels)
        });
        let first = answers.next().expect("one order at least");
        for other in answers {
            assert_eq!(
                other, first,
                "answer depends on the order of the references"
            );
        }
        first
    }

    /// Nodes `0..n` as single-column `MATCH` tuples.
    fn nodes(n: i64) -> Vec<Tuple> {
        (0..n).map(|id| vec![id]).collect()
    }

    fn seed_and_stake() -> Vec<OracleRule> {
        vec![
            schema_rule("seed", &[("o", ColKind::Node)], 1),
            schema_rule(
                "stake",
                &[
                    ("o", ColKind::Node),
                    ("e", ColKind::Node),
                    ("pct", ColKind::Int),
                ],
                2,
            ),
        ]
    }

    fn seed_and_stake_facts() -> Vec<(&'static str, Bag)> {
        vec![
            ("seed", vec![vec![0]]),
            ("stake", vec![vec![0, 1, 60], vec![2, 3, 40]]),
        ]
    }

    /// P1 (#300): `o IS seed, (o, e, pct) IS stake` is `{(1, 60)}` in both
    /// orders, with `o` bound by the `MATCH` or by the references alone.
    #[test]
    fn p1_seed_and_stake_agree_in_both_orders() {
        let reader = schema_rule("r", &[("e", ColKind::Node), ("pct", ColKind::Int)], 1);
        let refs = vec![
            isref("seed", &["o"], None, &[]),
            isref("stake", &["o", "e", "pct"], None, &[]),
        ];
        let matched = with_clause(
            reader.clone(),
            OracleClause {
                base: nodes(4),
                var_cols: HashMap::from([("o".to_string(), 0)]),
                pos_refs: refs.clone(),
                yield_vars: strs(&["e", "pct"]),
                ..OracleClause::default()
            },
        );
        let unmatched = with_clause(
            reader,
            OracleClause {
                base: vec![Vec::new()],
                pos_refs: refs,
                yield_vars: strs(&["e", "pct"]),
                ..OracleClause::default()
            },
        );
        for owner in [matched, unmatched] {
            assert_eq!(
                eval_every_order(&owner, &seed_and_stake(), &seed_and_stake_facts()),
                Ok(vec![vec![1, 60]])
            );
        }
    }

    /// P3: subjects named against the rule's KEY names (`(e, o) IS stake`)
    /// bind by position, and a variable repeated in one reference binds once
    /// then tests.
    #[test]
    fn tuple_subjects_bind_key_positions() {
        let owner = with_clause(
            schema_rule("r", &[("e", ColKind::Node), ("o", ColKind::Node)], 2),
            OracleClause {
                base: nodes(4),
                var_cols: HashMap::from([("o".to_string(), 0)]),
                pos_refs: vec![isref("stake", &["e", "o"], None, &[])],
                yield_vars: strs(&["e", "o"]),
                ..OracleClause::default()
            },
        );
        assert_eq!(
            eval_once(&owner, &seed_and_stake(), &seed_and_stake_facts()),
            Ok(vec![vec![0, 1], vec![2, 3]])
        );

        let pair = schema_rule("pair", &[("x", ColKind::Int), ("y", ColKind::Int)], 2);
        let diagonal = with_clause(
            schema_rule("d", &[("x", ColKind::Int)], 1),
            OracleClause {
                base: vec![Vec::new()],
                pos_refs: vec![isref("pair", &["x", "x"], None, &[])],
                yield_vars: strs(&["x"]),
                ..OracleClause::default()
            },
        );
        assert_eq!(
            eval_once(
                &diagonal,
                &[pair],
                &[("pair", vec![vec![1, 1], vec![1, 2]])]
            ),
            Ok(vec![vec![1]])
        );
    }

    /// Q1-A: `a IS tag TO c` with an Int KEY binds `c` to the integer, and
    /// using that integer as a node is a kind mismatch in every order — even
    /// though the integer equals a node's id.
    #[test]
    fn scalar_key_target_stays_a_scalar() {
        let tag = schema_rule("tag", &[("a", ColKind::Node), ("c", ColKind::Int)], 2);
        let seed = schema_rule("seed", &[("o", ColKind::Node)], 1);
        let facts = [
            ("tag", vec![vec![0, 1], vec![1, 7]]),
            ("seed", vec![vec![1]]),
        ];
        let clause = OracleClause {
            base: nodes(2),
            var_cols: HashMap::from([("a".to_string(), 0)]),
            var_kinds: HashMap::from([("a".to_string(), ColKind::Node)]),
            pos_refs: vec![isref("tag", &["a"], Some("c"), &[])],
            yield_vars: strs(&["a", "c"]),
            ..OracleClause::default()
        };
        let owner = with_clause(
            schema_rule("r", &[("a", ColKind::Node), ("c", ColKind::Int)], 2),
            clause.clone(),
        );
        let others = [tag.clone(), seed.clone()];
        assert_eq!(
            eval_once(&owner, &others, &facts),
            Ok(vec![vec![0, 1], vec![1, 7]])
        );

        let mut mixed = owner;
        mixed.clauses[0]
            .pos_refs
            .push(isref("seed", &["c"], None, &[]));
        for refs in permutations(&mixed.clauses[0].pos_refs) {
            let mut permuted = mixed.clone();
            permuted.clauses[0].pos_refs = refs;
            let err = eval_once(&permuted, &others, &facts).expect_err("a node at an Int KEY");
            assert!(
                matches!(&err, OracleError::IsRefKindMismatch { var, .. } if var == "c"),
                "{err}"
            );
            assert!(err.is_language_error());
            let program = OracleProgram {
                strata: vec![others.to_vec(), vec![permuted]],
            };
            assert_eq!(check_program(&program), Err(err));
        }
    }

    /// P5: an `IS NOT` whose KEY is bound only by a positive reference is
    /// legal; one whose KEY nothing binds is `IsNotKeyUnbound`; an unbound
    /// value position is existential, a bound one tests.
    #[test]
    fn negation_needs_bound_keys_and_quantifies_values() {
        let link = schema_rule("link", &[("a", ColKind::Node), ("b", ColKind::Node)], 2);
        let banned = schema_rule("banned", &[("b", ColKind::Node)], 1);
        let score = schema_rule("score", &[("a", ColKind::Node), ("v", ColKind::Int)], 1);
        let others = [link, banned, score];
        let facts = [
            ("link", vec![vec![0, 1], vec![1, 2]]),
            ("banned", vec![vec![2]]),
            ("score", vec![vec![0, 5]]),
        ];
        let ab = schema_rule("r", &[("a", ColKind::Node), ("b", ColKind::Node)], 2);
        let guarded = |neg: IsRef| {
            with_clause(
                ab.clone(),
                OracleClause {
                    base: nodes(3),
                    var_cols: HashMap::from([("a".to_string(), 0)]),
                    pos_refs: vec![isref("link", &["a"], Some("b"), &[])],
                    neg_refs: vec![neg],
                    yield_vars: strs(&["a", "b"]),
                    ..OracleClause::default()
                },
            )
        };
        assert_eq!(
            eval_once(
                &guarded(isref("banned", &["b"], None, &[])),
                &others,
                &facts
            ),
            Ok(vec![vec![0, 1]])
        );
        assert_eq!(
            eval_once(
                &guarded(isref("banned", &["z"], None, &[])),
                &others,
                &facts
            ),
            Err(OracleError::IsNotKeyUnbound {
                rule: "r".to_string(),
                reference: "banned".to_string(),
                var: "z".to_string(),
            })
        );

        let scored = |base: Vec<Tuple>, var_cols: &[(&str, usize)], neg: IsRef| {
            let yields: Vec<&str> = var_cols.iter().map(|(v, _)| *v).collect();
            with_clause(
                schema_rule(
                    "s",
                    &yields
                        .iter()
                        .map(|v| (*v, ColKind::Int))
                        .collect::<Vec<_>>(),
                    yields.len(),
                ),
                OracleClause {
                    base,
                    var_cols: var_cols
                        .iter()
                        .map(|(v, c)| ((*v).to_string(), *c))
                        .collect(),
                    neg_refs: vec![neg],
                    yield_vars: strs(&yields),
                    ..OracleClause::default()
                },
            )
        };
        // `NOT (a, x) IS score`, `x` unbound: any score of `a` excludes it.
        assert_eq!(
            eval_once(
                &scored(
                    nodes(3),
                    &[("a", 0)],
                    isref("score", &["a", "x"], None, &[])
                ),
                &others,
                &facts
            ),
            Ok(vec![vec![1], vec![2]])
        );
        // `NOT (a, w) IS score`, `w` bound: only the matching score excludes.
        assert_eq!(
            eval_once(
                &scored(
                    vec![vec![0, 5], vec![0, 6], vec![1, 5]],
                    &[("a", 0), ("w", 1)],
                    isref("score", &["a", "w"], None, &[])
                ),
                &others,
                &facts
            ),
            Ok(vec![vec![0, 6], vec![1, 5]])
        );
    }

    /// Q3: reading a value column two references expose is
    /// `AmbiguousRuleColumn`; exposing it without reading is legal; binding
    /// it positionally in one reference leaves the other the only exposer;
    /// a KEY column is never exposed.
    #[test]
    fn bare_reads_resolve_to_the_unique_exposer() {
        let seed = schema_rule("seed", &[("o", ColKind::Node), ("score", ColKind::Int)], 1);
        let other = schema_rule("other", &[("o", ColKind::Node), ("score", ColKind::Int)], 1);
        let others = [seed, other];
        let facts = [("seed", vec![vec![0, 5]]), ("other", vec![vec![0, 9]])];
        let reader = |refs: Vec<IsRef>, reads: &[(&str, &str)], yields: &[&str]| {
            with_clause(
                schema_rule(
                    "r",
                    &yields
                        .iter()
                        .map(|v| (*v, ColKind::Int))
                        .collect::<Vec<_>>(),
                    1,
                ),
                OracleClause {
                    base: nodes(2),
                    var_cols: HashMap::from([("o".to_string(), 0)]),
                    pos_refs: refs,
                    bare_reads: reads.iter().map(|(c, v)| ColumnRead::new(*c, *v)).collect(),
                    yield_vars: strs(yields),
                    ..OracleClause::default()
                },
            )
        };
        let both = || {
            vec![
                isref("seed", &["o"], None, &[]),
                isref("other", &["o"], None, &[]),
            ]
        };
        assert_eq!(
            eval_every_order(
                &reader(both(), &[("score", "s")], &["o", "s"]),
                &others,
                &facts
            ),
            Err(OracleError::AmbiguousRuleColumn {
                rule: "r".to_string(),
                column: "score".to_string(),
                exposed_by: strs(&["other", "seed"]),
            })
        );
        assert_eq!(
            eval_every_order(&reader(both(), &[], &["o"]), &others, &facts),
            Ok(vec![vec![0]])
        );
        assert_eq!(
            eval_every_order(
                &reader(
                    vec![
                        isref("seed", &["o", "s1"], None, &[]),
                        isref("other", &["o"], None, &[]),
                    ],
                    &[("score", "s")],
                    &["o", "s1", "s"],
                ),
                &others,
                &facts
            ),
            Ok(vec![vec![0, 5, 9]])
        );
        assert_eq!(
            eval_once(
                &reader(vec![isref("seed", &[], None, &[])], &[("o", "x")], &["x"]),
                &others,
                &facts
            ),
            Err(OracleError::UnknownColumn {
                rule: "r".to_string(),
                column: "o".to_string(),
            })
        );
    }

    /// Q2: `prev.q` reads the clause's self-reference even when another
    /// reference also exposes a `q`; a `prev` read needs exactly one
    /// self-reference.
    #[test]
    fn prev_reads_resolve_to_the_self_reference() {
        let cost = schema_rule(
            "cost",
            &[
                ("a", ColKind::Node),
                ("b", ColKind::Node),
                ("q", ColKind::Int),
            ],
            2,
        );
        let side = schema_rule("side", &[("m", ColKind::Node), ("q", ColKind::Int)], 1);
        let facts = [("cost", vec![vec![1, 2, 10]]), ("side", vec![vec![1, 100]])];
        let step = |refs: Vec<IsRef>| {
            with_clause(
                cost.clone(),
                OracleClause {
                    base: vec![vec![0, 1, 1]],
                    var_cols: HashMap::from([
                        ("a".to_string(), 0),
                        ("m".to_string(), 1),
                        ("w".to_string(), 2),
                    ]),
                    pos_refs: refs,
                    prev_reads: vec![ColumnRead::new("q", "p")],
                    computed: vec![crate::ir::Computed {
                        name: "v".to_string(),
                        vars: strs(&["p", "w"]),
                        factor: 1,
                        constant: 0,
                    }],
                    yield_vars: strs(&["a", "b", "v"]),
                    ..OracleClause::default()
                },
            )
        };
        let others = [side];
        assert_eq!(
            eval_every_order(
                &step(vec![
                    isref("cost", &["m"], Some("b"), &[]),
                    isref("side", &["m"], None, &[]),
                ]),
                &others,
                &facts
            ),
            Ok(vec![vec![0, 2, 11]])
        );
        for (refs, n) in [
            (vec![isref("side", &["m"], None, &[])], 0),
            (
                vec![
                    isref("cost", &["m"], Some("b"), &[]),
                    isref("cost", &["m"], Some("b"), &[]),
                ],
                2,
            ),
        ] {
            assert_eq!(
                eval_once(&step(refs), &others, &facts),
                Err(OracleError::AmbiguousPrevRef {
                    rule: "cost".to_string(),
                    self_refs: n,
                })
            );
        }
    }

    /// A filter applies once its variables are bound, wherever a reference
    /// binds them; a filter on an unbound variable is an error.
    #[test]
    fn filters_read_reference_bound_variables() {
        let stake_reader = |lhs: &str| {
            with_clause(
                schema_rule("r", &[("e", ColKind::Node)], 1),
                OracleClause {
                    base: vec![Vec::new()],
                    pos_refs: vec![isref("stake", &["o", "e", "pct"], None, &[])],
                    filters: vec![Filter {
                        lhs: Operand::Var(lhs.to_string()),
                        op: CmpOp::Gt,
                        rhs: Operand::Const(50),
                    }],
                    yield_vars: strs(&["e"]),
                    ..OracleClause::default()
                },
            )
        };
        assert_eq!(
            eval_once(
                &stake_reader("pct"),
                &seed_and_stake(),
                &seed_and_stake_facts()
            ),
            Ok(vec![vec![1]])
        );
        assert!(matches!(
            eval_once(
                &stake_reader("zz"),
                &seed_and_stake(),
                &seed_and_stake_facts()
            ),
            Err(OracleError::UnboundVariable {
                context: "filter",
                ..
            })
        ));
    }

    /// Variables a random clause draws from.
    const POOL: usize = 5;

    /// One referenced rule of a random clause: arity, KEY count, facts.
    type RandomRule = (usize, usize, Vec<Vec<i64>>);

    /// A random clause: referenced rules, `MATCH` variables and tuples,
    /// positive references `(rule, variables, last one is a TO target)`, an
    /// optional negation and an optional filter.
    #[derive(Debug, Clone)]
    struct RandomClause {
        rules: Vec<RandomRule>,
        base_vars: Vec<usize>,
        base_rows: Vec<Vec<i64>>,
        refs: Vec<(usize, Vec<usize>, bool)>,
        neg: Option<(usize, Vec<usize>)>,
        filter: Option<(usize, usize)>,
    }

    fn random_clause() -> impl Strategy<Value = RandomClause> {
        let rule = (1usize..=3).prop_flat_map(|arity| {
            (
                Just(arity),
                1..=arity,
                proptest::collection::vec(proptest::collection::vec(0i64..3, arity), 0..6),
            )
        });
        (
            proptest::collection::vec(rule, 3),
            proptest::sample::subsequence((0..POOL).collect::<Vec<_>>(), 0..=2),
        )
            .prop_flat_map(|(rules, base_vars)| {
                let arities: Vec<usize> = rules.iter().map(|r| r.0).collect();
                let reference = |arities: Vec<usize>| {
                    (0..arities.len()).prop_flat_map(move |i| {
                        (Just(i), proptest::collection::vec(0..POOL, 1..=arities[i]))
                    })
                };
                let width = base_vars.len();
                (
                    Just(rules),
                    Just(base_vars),
                    proptest::collection::vec(proptest::collection::vec(0i64..3, width), 0..5),
                    proptest::collection::vec(
                        (reference(arities.clone()), any::<bool>())
                            .prop_map(|((i, vars), t)| (i, vars, t)),
                        1..=4,
                    ),
                    proptest::option::of(reference(arities)),
                    proptest::option::of((0..POOL, 0..POOL)),
                )
            })
            .prop_map(
                |(rules, base_vars, base_rows, refs, neg, filter)| RandomClause {
                    rules,
                    base_vars,
                    base_rows,
                    refs,
                    neg,
                    filter,
                },
            )
    }

    impl RandomClause {
        /// The schemas, the clause's owner and the hand relations.
        fn build(&self) -> (Vec<OracleRule>, OracleRule, Vec<(String, Bag)>) {
            let var = |i: usize| format!("v{i}");
            let mut others = Vec::new();
            let mut facts = Vec::new();
            for (i, (arity, keys, rows)) in self.rules.iter().enumerate() {
                let names: Vec<String> = (0..*arity).map(|c| format!("c{c}")).collect();
                let spec: Vec<(&str, ColKind)> =
                    names.iter().map(|n| (n.as_str(), ColKind::Int)).collect();
                others.push(schema_rule(&format!("r{i}"), &spec, *keys));
                facts.push((format!("r{i}"), rows.clone()));
            }
            let to_ref = |(i, vars, target): &(usize, Vec<usize>, bool)| {
                let mut names: Vec<String> = vars.iter().map(|&v| var(v)).collect();
                let target = (*target && names.len() > 1).then(|| names.pop()).flatten();
                IsRef {
                    rule: format!("r{i}"),
                    subjects: names,
                    target,
                    values: Vec::new(),
                }
            };
            let pos_refs: Vec<IsRef> = self.refs.iter().map(to_ref).collect();
            let mut bound: Vec<String> = self.base_vars.iter().map(|&v| var(v)).collect();
            bound.extend(pos_refs.iter().flat_map(explicit_positions).map(|(_, v)| v));
            bound.sort();
            bound.dedup();
            let base = if self.base_vars.is_empty() {
                vec![Vec::new()]
            } else {
                self.base_rows.clone()
            };
            let clause = OracleClause {
                base,
                var_cols: self
                    .base_vars
                    .iter()
                    .enumerate()
                    .map(|(c, &v)| (var(v), c))
                    .collect(),
                pos_refs,
                neg_refs: self
                    .neg
                    .iter()
                    .map(|(i, vars)| to_ref(&(*i, vars.clone(), false)))
                    .collect(),
                filters: self
                    .filter
                    .iter()
                    .filter(|(l, r)| bound.contains(&var(*l)) && bound.contains(&var(*r)))
                    .map(|&(l, r)| Filter {
                        lhs: Operand::Var(var(l)),
                        op: CmpOp::Le,
                        rhs: Operand::Var(var(r)),
                    })
                    .collect(),
                yield_vars: bound.clone(),
                ..OracleClause::default()
            };
            let spec: Vec<(&str, ColKind)> =
                bound.iter().map(|n| (n.as_str(), ColKind::Int)).collect();
            let owner = with_clause(schema_rule("out", &spec, bound.len()), clause);
            (others, owner, facts)
        }
    }

    proptest! {
        /// Every clause gives an equal bag (or the same error) under every
        /// order of its positive references.
        #[test]
        fn clause_answer_is_independent_of_reference_order(case in random_clause()) {
            let (others, owner, facts) = case.build();
            let facts: Vec<(&str, Bag)> =
                facts.iter().map(|(n, b)| (n.as_str(), b.clone())).collect();
            // `eval_every_order` asserts the orders agree.
            let _ = eval_every_order(&owner, &others, &facts);
        }

        /// Every generated program passes the static checks, and every rule's
        /// columns are the names the engine-side harness extracts.
        #[test]
        fn generated_programs_check_and_name_their_columns(
            g in crate::generator::random_program_strategy()
        ) {
            // Only the shared-value family's ambiguous read is rejected.
            match check_program(&g.oracle_rules) {
                Ok(()) => prop_assert!(!g.features.contains(&crate::ir::Feature::AmbiguousExpected)),
                Err(OracleError::AmbiguousRuleColumn { .. }) => {
                    prop_assert!(g.features.contains(&crate::ir::Feature::AmbiguousExpected));
                }
                Err(e) => prop_assert!(false, "{}\n{}", e, g.program_text),
            }
            for rule in g.oracle_rules.strata.iter().flatten() {
                let names: Vec<String> = rule.columns.iter().map(|c| c.name.clone()).collect();
                prop_assert_eq!(&names, &g.key_schema[&rule.name]);
            }
        }
    }
}
