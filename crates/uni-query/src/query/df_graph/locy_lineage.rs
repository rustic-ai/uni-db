// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probability through positive `IS` references, and the lineage exact
//! inference reads.
//!
//! # Semantics
//!
//! A fact of a PROB rule holds with the probability its PROB column carries.
//! A positive reference `x IS r` to such a rule is a probabilistic condition:
//! the row holds only when the referenced fact does. So:
//!
//! * **Default mode** multiplies the referenced probability into the calling
//!   row's PROB value ([`apply_positive_prob_factors`]), as `IS NOT` multiplies
//!   `1 - p`. A clause whose PROB expression already reads that probability as
//!   one product factor (`FOLD p = MNOR(p)`) counts it once: nothing more is
//!   multiplied in. A read that is not one product factor (`1 - p`) is never
//!   multiplied in either, and is reported (`ProbabilityReadNotFactor`).
//! * **Exact mode** evaluates each fact of an `MNOR`/`MPROD` rule as the
//!   formula `OP over its rows of (row event AND each referenced fact's
//!   formula)` under possible-worlds semantics, where OP is OR for `MNOR` and
//!   AND for `MPROD`, every row event is an independent variable, and a fact
//!   with no recorded lineage (a non-fold PROB rule's) is one variable.
//!
//! The row event's own probability is the row's PROB value divided by the
//! referenced probabilities it carries, which recovers the clause's local
//! weight in both the multiplied and the already-a-factor case.
//!
//! When no fact's formula reads a variable twice, the default value of each
//! fact is already its possible-worlds probability, so the two modes agree.
//! A repeated variable (two proofs through one fact, or a cycle) is reported
//! as `SharedProbabilisticDependency`, and only those facts are recomputed.

// Rust guideline compliant

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock as StdRwLock};

use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, Float64Array, RecordBatch};
use biodivine_lib_bdd::{Bdd, BddVariable, BddVariableSet};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use uni_common::Value;
use uni_locy::{RuntimeWarning, RuntimeWarningCode, SemiringKind};

use super::common::{arrow_err, extract_scalar_key};
use super::locy_bdd::bdd_probability;
use super::locy_fixpoint::{
    FixpointRulePlan, IsRefBinding, pre_fold_column_name, yield_positions_in,
};
use super::locy_row_hash::{ROW_HASH_WIDTH, row_hash};
use crate::query::planner_locy_types::{DERIV_EVENT_COL, ISPROB_COL_PREFIX, ProbRead};

/// The identity of a fact: the hash of its KEY columns.
type FactKey = [u8; ROW_HASH_WIDTH as usize];

/// A fact of a lineage-keeping rule: `(rule name, KEY hash)`.
type FactId = (String, FactKey);

/// Fails with an internal error naming `what`.
fn internal(what: String) -> DataFusionError {
    DataFusionError::Internal(what)
}

/// Reads row `row` of `col` as a probability, whatever its encoding.
///
/// A body column's type follows its expression: `Float64` for a declared
/// property or a literal, an encoded Cypher value for an undeclared one.
///
/// # Errors
///
/// Fails when the value is neither NULL nor a number.
fn f64_at(col: &dyn Array, row: usize, name: &str) -> DFResult<Option<f64>> {
    if col.is_null(row) {
        return Ok(None);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(a.value(row)));
    }
    let value = uni_store::storage::arrow_convert::arrow_to_value(col, row, None)
        .map_err(|e| internal(format!("Locy PROB column `{name}` failed to decode: {e}")))?;
    match value {
        Value::Null => Ok(None),
        Value::Float(f) => Ok(Some(f)),
        Value::Int(i) => Ok(Some(i as f64)),
        other => Err(DataFusionError::Execution(format!(
            "Locy PROB column `{name}` holds a non-numeric value {other:?}"
        ))),
    }
}

/// `values` as an array of `like`'s type: `Float64` stays a float column, any
/// other encoding is rebuilt through the shared value conversion.
fn f64_column(values: Vec<Option<f64>>, like: &ArrayRef) -> DFResult<ArrayRef> {
    if like.data_type() == &arrow_schema::DataType::Float64 {
        return Ok(Arc::new(Float64Array::from(values)));
    }
    let values: Vec<Value> = values
        .into_iter()
        .map(|v| v.map_or(Value::Null, Value::Float))
        .collect();
    uni_store::storage::arrow_convert::values_to_array(&values, like.data_type())
        .map_err(|e| internal(format!("Locy PROB column failed to re-encode: {e}")))
}

/// Multiplies each positive reference's probability into the clause's PROB
/// column, then drops the transient probability columns.
///
/// Only references the clause does not already read are multiplied
/// ([`ProbRead::Absent`]); a probability the PROB expression reads is counted
/// by that expression. A NULL referenced probability leaves the row as is,
/// as a NULL complement factor does for `IS NOT`.
///
/// # Errors
///
/// Fails when a probability or PROB column the plan names is missing from the
/// batch: skipping it would silently drop the condition's probability.
pub(crate) fn apply_positive_prob_factors(
    batches: Vec<RecordBatch>,
    bindings: &[IsRefBinding],
) -> DFResult<Vec<RecordBatch>> {
    let multiplied: Vec<(&str, &str)> = bindings
        .iter()
        .filter_map(|b| b.prob_ref.as_ref())
        .filter(|r| r.read == ProbRead::Absent)
        .map(|r| (r.prob_column.as_str(), r.into_column.as_str()))
        .collect();
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let schema = batch.schema();
        let transient = schema
            .fields()
            .iter()
            .any(|f| f.name().starts_with(ISPROB_COL_PREFIX));
        if multiplied.is_empty() && !transient {
            out.push(batch);
            continue;
        }
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        if batch.num_rows() > 0 {
            for (prob_column, into_column) in &multiplied {
                let into = schema.index_of(into_column).map_err(|_| {
                    internal(format!(
                        "Locy PROB column `{into_column}` is missing from the clause body; \
                         a positive IS reference's probability would be dropped"
                    ))
                })?;
                let factor = schema.index_of(prob_column).map_err(|_| {
                    internal(format!(
                        "Locy IS-reference probability column `{prob_column}` is missing \
                         from the clause body; its probability would be dropped"
                    ))
                })?;
                let values = (0..batch.num_rows())
                    .map(|row| {
                        let v = f64_at(columns[into].as_ref(), row, into_column)?;
                        let p = f64_at(columns[factor].as_ref(), row, prob_column)?;
                        Ok(match (v, p) {
                            (Some(v), Some(p)) => Some(v * p.clamp(0.0, 1.0)),
                            (v, _) => v,
                        })
                    })
                    .collect::<DFResult<Vec<Option<f64>>>>()?;
                columns[into] = f64_column(values, &columns[into])?;
            }
        }
        let keep: Vec<usize> = (0..schema.fields().len())
            .filter(|&i| !schema.field(i).name().starts_with(ISPROB_COL_PREFIX))
            .collect();
        let fields: Vec<arrow_schema::Field> = keep
            .iter()
            .map(|&i| {
                let field = schema.field(i).clone();
                field.with_data_type(columns[i].data_type().clone())
            })
            .collect();
        let kept: Vec<ArrayRef> = keep.iter().map(|&i| Arc::clone(&columns[i])).collect();
        let new_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        ));
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        out.push(RecordBatch::try_new_with_options(new_schema, kept, &options).map_err(arrow_err)?);
    }
    Ok(out)
}

/// Raises `ProbabilityReadNotFactor` once per rule for a clause whose PROB
/// expression reads a referenced probability other than as one product
/// factor.
pub(crate) fn warn_opaque_prob_reads(
    rule: &FixpointRulePlan,
    warnings_slot: &Arc<StdRwLock<Vec<RuntimeWarning>>>,
) {
    let opaque: Vec<&str> = rule
        .clauses
        .iter()
        .flat_map(|c| c.is_ref_bindings.iter())
        .filter(|b| {
            b.prob_ref
                .as_ref()
                .is_some_and(|r| r.read == ProbRead::Opaque)
        })
        .map(|b| b.rule_name.as_str())
        .collect();
    if opaque.is_empty() {
        return;
    }
    let Ok(mut warnings) = warnings_slot.write() else {
        return;
    };
    if warnings
        .iter()
        .any(|w| w.code == RuntimeWarningCode::ProbabilityReadNotFactor && w.rule_name == rule.name)
    {
        return;
    }
    warnings.push(RuntimeWarning {
        code: RuntimeWarningCode::ProbabilityReadNotFactor,
        message: format!(
            "Rule '{}' reads the probability of {} other than as one product factor of \
             its PROB value. The referenced probability is not multiplied in again, and \
             exact inference treats each such row as an independent event.",
            rule.name,
            opaque.join(", ")
        ),
        rule_name: rule.name.clone(),
        variable_count: None,
        key_group: None,
    });
}

/// One derivation row of a lineage fact.
#[derive(Debug, Clone)]
enum RowTerm {
    /// The row's own event, of probability `local`, and every referenced fact.
    Conj { local: f64, refs: Vec<RefTerm> },
    /// A row whose PROB value cannot be decomposed: one independent event.
    Opaque { prob: f64 },
}

/// A referenced fact, with the probability the row read for it.
#[derive(Debug, Clone)]
struct RefTerm {
    fact: FactId,
    prob: f64,
}

/// The lineage of one `MNOR`/`MPROD` rule: each fact's derivation rows.
#[derive(Debug)]
struct RuleLineage {
    /// `MPROD` (all rows) rather than `MNOR` (any row).
    conjunction: bool,
    facts: HashMap<FactKey, Vec<RowTerm>>,
    /// Per fact, each row's event identity (`__deriv_event`), index-parallel
    /// with `facts`: rows sharing one are one event. `None` makes the row its
    /// own event.
    events: HashMap<FactKey, Vec<Option<FactKey>>>,
}

/// The lineage of every `MNOR`/`MPROD` rule evaluated so far in a program run.
///
/// Recorded from each rule's pre-fold rows after its stratum converges, and
/// read by exact inference and shared-proof detection of the rules that
/// reference it. Re-recording a rule (an `ASSUME` re-evaluation) replaces it.
#[derive(Debug, Default)]
pub(crate) struct LineageStore {
    rules: StdRwLock<HashMap<String, Arc<RuleLineage>>>,
}

/// A variable of a lineage formula.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Var {
    /// Row `index` of a lineage fact, which is its own event.
    Row(FactId, usize),
    /// A row event shared by every row resting on the same graph elements.
    Event(FactKey),
    /// A referenced fact with no recorded lineage.
    Base(FactId),
}

/// What [`LineageStore::correct`] found for one rule.
#[derive(Debug, Default)]
pub(crate) struct LineageOutcome {
    /// Whether some fact's formula reads one variable twice.
    pub shared: bool,
}

/// Exact-inference settings and output slots for [`LineageStore::correct`].
pub(crate) struct ExactCtx<'a> {
    /// Whether to recompute shared facts (`exact_probability`).
    pub exact: bool,
    /// The variable budget per fact (`max_bdd_variables`).
    pub max_bdd_variables: usize,
    /// The active semiring; `TopKProofs` suppresses the shared-proof warning.
    pub semiring_kind: SemiringKind,
    /// Where warnings go.
    pub warnings_slot: &'a Arc<StdRwLock<Vec<RuntimeWarning>>>,
    /// Rules with a fact that exceeded the budget, with its KEY.
    pub approximate_slot: &'a Arc<StdRwLock<HashMap<String, Vec<String>>>>,
}

impl LineageStore {
    /// Creates an empty store.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Records `rule`'s pre-fold rows as lineage. A rule without an
    /// `MNOR`/`MPROD` fold records nothing.
    ///
    /// # Errors
    ///
    /// Fails when a KEY, PROB or lineage column is missing from a batch.
    pub(crate) fn record_rule(
        &self,
        rule: &FixpointRulePlan,
        pre_fold: &[RecordBatch],
    ) -> DFResult<()> {
        let Some(fold) = rule
            .fold_bindings
            .iter()
            .find(|fb| fb.aggregate.is_probability_aggregate())
        else {
            return Ok(());
        };
        let prob_name = pre_fold_column_name(rule, fold.input_col_index).ok_or_else(|| {
            internal(format!(
                "rule '{}': PROB fold input column {} out of range",
                rule.name, fold.input_col_index
            ))
        })?;
        let refs: Vec<(&IsRefBinding, &str, &str, ProbRead)> = rule
            .clauses
            .iter()
            .flat_map(|c| c.is_ref_bindings.iter())
            .filter_map(|b| {
                let r = b.prob_ref.as_ref()?;
                Some((b, r.key_column.as_deref()?, r.prob_column.as_str(), r.read))
            })
            .collect();
        let mut facts: HashMap<FactKey, Vec<RowTerm>> = HashMap::new();
        let mut events: HashMap<FactKey, Vec<Option<FactKey>>> = HashMap::new();
        for batch in pre_fold.iter().filter(|b| b.num_rows() > 0) {
            let schema = batch.schema();
            let (_, key_indices) = yield_positions_in(rule, &schema)?;
            let key_columns: Vec<ArrayRef> = key_indices
                .iter()
                .map(|&i| Arc::clone(batch.column(i)))
                .collect();
            let column = |name: &str| -> DFResult<&ArrayRef> {
                schema.index_of(name).map(|i| batch.column(i)).map_err(|_| {
                    internal(format!(
                        "rule '{}': lineage column `{name}` is missing from a fact batch",
                        rule.name
                    ))
                })
            };
            let prob_column = column(prob_name)?;
            let event_column = schema
                .index_of(DERIV_EVENT_COL)
                .ok()
                .map(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            internal(format!(
                                "rule '{}': `{DERIV_EVENT_COL}` is not a row hash",
                                rule.name
                            ))
                        })
                })
                .transpose()?;
            let ref_columns = refs
                .iter()
                .map(|(binding, key, prob, read)| {
                    let key = column(key)?
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            internal(format!(
                                "rule '{}': lineage column `{key}` is not a row hash",
                                rule.name
                            ))
                        })?;
                    Ok((binding.rule_name.as_str(), key, column(prob)?, *prob, *read))
                })
                .collect::<DFResult<Vec<_>>>()?;
            for row in 0..batch.num_rows() {
                // A NULL input is skipped by the fold, so it is no proof.
                let Some(weight) = f64_at(prob_column.as_ref(), row, prob_name)? else {
                    continue;
                };
                let weight = weight.clamp(0.0, 1.0);
                let mut opaque = false;
                let mut divisor = 1.0;
                let mut row_refs = Vec::new();
                for (target, key, prob, name, read) in &ref_columns {
                    if key.is_null(row) {
                        continue;
                    }
                    // A NULL referenced probability was not multiplied in:
                    // the condition is certain.
                    let Some(p) = f64_at(prob.as_ref(), row, name)? else {
                        continue;
                    };
                    let p = p.clamp(0.0, 1.0);
                    if *read == ProbRead::Opaque {
                        opaque = true;
                        continue;
                    }
                    divisor *= p;
                    let fact_key: FactKey = key.value(row).try_into().map_err(|_| {
                        internal(format!("rule '{}': malformed row hash", rule.name))
                    })?;
                    row_refs.push(RefTerm {
                        fact: ((*target).to_string(), fact_key),
                        prob: p,
                    });
                }
                let term = if opaque {
                    RowTerm::Opaque { prob: weight }
                } else {
                    let local = if divisor > 0.0 {
                        (weight / divisor).clamp(0.0, 1.0)
                    } else {
                        0.0
                    };
                    RowTerm::Conj {
                        local,
                        refs: row_refs,
                    }
                };
                let event = match event_column {
                    Some(col) if !opaque && !col.is_null(row) => {
                        Some(col.value(row).try_into().map_err(|_| {
                            internal(format!("rule '{}': malformed row hash", rule.name))
                        })?)
                    }
                    _ => None,
                };
                let key = row_hash(&key_columns, row)?;
                facts.entry(key).or_default().push(term);
                events.entry(key).or_default().push(event);
            }
        }
        if let Ok(mut rules) = self.rules.write() {
            rules.insert(
                rule.name.clone(),
                Arc::new(RuleLineage {
                    conjunction: !fold.aggregate.is_noisy_or(),
                    facts,
                    events,
                }),
            );
        }
        Ok(())
    }

    /// A consistent view of every recorded rule.
    fn snapshot(&self) -> HashMap<String, Arc<RuleLineage>> {
        self.rules.read().map(|r| r.clone()).unwrap_or_default()
    }

    /// Reports shared lineage and cross-group correlation for `rule`, and in
    /// exact mode replaces each fact whose formula reads a variable twice by
    /// one row carrying its possible-worlds probability.
    ///
    /// `rule` must have been recorded with [`Self::record_rule`].
    ///
    /// # Errors
    ///
    /// Fails when a batch lacks a KEY or PROB column, or a reference names a
    /// fact the referenced rule's lineage does not hold.
    pub(crate) fn correct(
        &self,
        rule: &FixpointRulePlan,
        pre_fold: Vec<RecordBatch>,
        ctx: &ExactCtx<'_>,
    ) -> DFResult<(Vec<RecordBatch>, LineageOutcome)> {
        let Some(fold) = rule
            .fold_bindings
            .iter()
            .find(|fb| fb.aggregate.is_probability_aggregate())
        else {
            return Ok((pre_fold, LineageOutcome::default()));
        };
        let snapshot = self.snapshot();
        let Some(lineage) = snapshot.get(&rule.name) else {
            return Ok((pre_fold, LineageOutcome::default()));
        };
        let analysis = Analysis::run(&snapshot, &rule.name, ANALYSIS_SUPPORT_CAP)?;
        let shared: Vec<&FactKey> = lineage
            .facts
            .keys()
            .filter(|k| analysis.locally_shared(&(rule.name.clone(), **k)))
            .collect();
        let outcome = LineageOutcome {
            shared: !shared.is_empty(),
        };
        self.warn(rule, lineage, &snapshot, outcome.shared, ctx);
        if !ctx.exact {
            return Ok((pre_fold, outcome));
        }

        let needs_exact: HashSet<FactKey> = lineage
            .facts
            .keys()
            .filter(|k| !analysis.read_once(&(rule.name.clone(), **k)))
            .copied()
            .collect();
        if needs_exact.is_empty() {
            return Ok((pre_fold, outcome));
        }
        let prob_name = pre_fold_column_name(rule, fold.input_col_index)
            .ok_or_else(|| internal(format!("rule '{}': PROB column out of range", rule.name)))?;
        let mut exact: HashMap<FactKey, Option<f64>> = HashMap::new();
        let mut kept_one: HashSet<FactKey> = HashSet::new();
        let mut out = Vec::with_capacity(pre_fold.len());
        for batch in pre_fold {
            if batch.num_rows() == 0 {
                out.push(batch);
                continue;
            }
            let schema = batch.schema();
            let (_, key_indices) = yield_positions_in(rule, &schema)?;
            let key_columns: Vec<ArrayRef> = key_indices
                .iter()
                .map(|&i| Arc::clone(batch.column(i)))
                .collect();
            let prob_index = schema.index_of(prob_name).map_err(|_| {
                internal(format!(
                    "rule '{}': PROB column `{prob_name}` is missing from a fact batch",
                    rule.name
                ))
            })?;
            let mut keep: Vec<u32> = Vec::new();
            let mut values: Vec<Option<f64>> = Vec::new();
            for row in 0..batch.num_rows() {
                let key = row_hash(&key_columns, row)?;
                let current = f64_at(batch.column(prob_index).as_ref(), row, prob_name)?;
                if !needs_exact.contains(&key) {
                    keep.push(row as u32);
                    values.push(current);
                    continue;
                }
                let probability = match exact.get(&key) {
                    Some(p) => *p,
                    None => {
                        let p = exact_probability(
                            &snapshot,
                            (rule.name.clone(), key),
                            &batch,
                            &key_indices,
                            row,
                            ctx,
                        )?;
                        exact.insert(key, p);
                        p
                    }
                };
                match probability {
                    // Over budget: every row stays, folded independently.
                    None => {
                        keep.push(row as u32);
                        values.push(current);
                    }
                    // One row carries the fact's exact probability.
                    Some(p) => {
                        if kept_one.insert(key) {
                            keep.push(row as u32);
                            values.push(Some(p));
                        }
                    }
                }
            }
            let indices = arrow_array::UInt32Array::from(keep);
            let mut columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
                .collect::<Result<_, _>>()
                .map_err(arrow_err)?;
            columns[prob_index] = f64_column(values, &columns[prob_index])?;
            let fields: Vec<arrow_schema::Field> = schema
                .fields()
                .iter()
                .zip(&columns)
                .map(|(f, c)| f.as_ref().clone().with_data_type(c.data_type().clone()))
                .collect();
            let new_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
                fields,
                schema.metadata().clone(),
            ));
            out.push(RecordBatch::try_new(new_schema, columns).map_err(arrow_err)?);
        }
        Ok((out, outcome))
    }
}

/// The exact probability of `root`, or `None` (with `BddLimitExceeded`) when
/// its formula needs more than the budget's variables.
fn exact_probability(
    snapshot: &HashMap<String, Arc<RuleLineage>>,
    root: FactId,
    batch: &RecordBatch,
    key_indices: &[usize],
    row: usize,
    ctx: &ExactCtx<'_>,
) -> DFResult<Option<f64>> {
    let closure = closure_of(snapshot, &root)?;
    let mut vars: Vec<(Var, f64)> = Vec::new();
    let mut var_index: HashMap<Var, usize> = HashMap::new();
    let mut intern = |var: Var, p: f64, vars: &mut Vec<(Var, f64)>| -> usize {
        *var_index.entry(var.clone()).or_insert_with(|| {
            vars.push((var, p));
            vars.len() - 1
        })
    };
    for fact in &closure {
        for (index, term) in rows_of(snapshot, fact)?.iter().enumerate() {
            let p = match term {
                RowTerm::Conj { local, refs } => {
                    for r in refs {
                        if !has_lineage(snapshot, &r.fact) {
                            intern(Var::Base(r.fact.clone()), r.prob, &mut vars);
                        }
                    }
                    *local
                }
                RowTerm::Opaque { prob } => *prob,
            };
            intern(row_var(snapshot, fact, index), p, &mut vars);
        }
    }
    let rule_name = &root.0;
    if vars.len() > ctx.max_bdd_variables || vars.len() > usize::from(u16::MAX) {
        let key_desc = format!("{:?}", extract_scalar_key(batch, key_indices, row));
        if let Ok(mut warnings) = ctx.warnings_slot.write()
            && !warnings.iter().any(|w| {
                w.code == RuntimeWarningCode::BddLimitExceeded
                    && w.rule_name == *rule_name
                    && w.key_group.as_deref() == Some(&key_desc)
            })
        {
            warnings.push(RuntimeWarning {
                code: RuntimeWarningCode::BddLimitExceeded,
                message: format!(
                    "Rule '{rule_name}': BDD variable limit exceeded ({} > {}). \
                         Falling back to independence-mode result.",
                    vars.len(),
                    ctx.max_bdd_variables
                ),
                rule_name: rule_name.clone(),
                variable_count: Some(vars.len()),
                key_group: Some(key_desc.clone()),
            });
        }
        if let Ok(mut approx) = ctx.approximate_slot.write() {
            approx.entry(rule_name.clone()).or_default().push(key_desc);
        }
        return Ok(None);
    }

    // `vars.len()` is at most `u16::MAX` (checked above).
    let set = BddVariableSet::new_anonymous(vars.len() as u16);
    let bdd_vars: Vec<BddVariable> = set.variables();
    let var_bdd = |v: &Var| -> Bdd { set.mk_var(bdd_vars[var_index[v]]) };
    // Least fixpoint: every fact false, then re-derive until stable. A
    // derivation through a cycle needs the fact it derives, so it adds
    // nothing; each round adds proofs one step longer, so `closure.len()`
    // rounds reach the fixpoint.
    let mut phi: HashMap<&FactId, Bdd> = closure.iter().map(|f| (f, set.mk_false())).collect();
    for _ in 0..=closure.len() {
        let mut changed = false;
        for fact in closure.iter().rev() {
            let lineage = &snapshot[&fact.0];
            let mut combined: Option<Bdd> = None;
            for (index, term) in rows_of(snapshot, fact)?.iter().enumerate() {
                let mut t = var_bdd(&row_var(snapshot, fact, index));
                if let RowTerm::Conj { refs, .. } = term {
                    for r in refs {
                        let operand = match phi.get(&r.fact) {
                            Some(b) => b.clone(),
                            None => var_bdd(&Var::Base(r.fact.clone())),
                        };
                        t = t.and(&operand);
                    }
                }
                combined = Some(match combined {
                    None => t,
                    Some(acc) if lineage.conjunction => acc.and(&t),
                    Some(acc) => acc.or(&t),
                });
            }
            let new = combined.unwrap_or_else(|| set.mk_false());
            if phi[fact] != new {
                phi.insert(fact, new);
                changed = true;
            }
        }
        if !changed {
            let probs: HashMap<BddVariable, f64> = vars
                .iter()
                .enumerate()
                .map(|(i, (_, p))| (bdd_vars[i], *p))
                .collect();
            return Ok(Some(bdd_probability(&phi[&root], &probs)));
        }
    }
    Err(internal(format!(
        "rule '{rule_name}': exact inference did not reach a fixpoint"
    )))
}

impl LineageStore {
    /// Raises `SharedProbabilisticDependency` and `CrossGroupCorrelationNotExact`
    /// for `rule`, at most once each.
    fn warn(
        &self,
        rule: &FixpointRulePlan,
        lineage: &RuleLineage,
        snapshot: &HashMap<String, Arc<RuleLineage>>,
        shared: bool,
        ctx: &ExactCtx<'_>,
    ) {
        // Facts of this rule consuming each referenced fact (or base event).
        let mut consumers: HashMap<&FactId, HashSet<&FactKey>> = HashMap::new();
        for (key, rows) in &lineage.facts {
            for term in rows {
                if let RowTerm::Conj { refs, .. } = term {
                    for r in refs {
                        consumers.entry(&r.fact).or_default().insert(key);
                    }
                }
            }
        }
        let crossing: Vec<&FactId> = consumers
            .iter()
            .filter(|(_, groups)| groups.len() > 1)
            .map(|(fact, _)| *fact)
            .collect();
        let Ok(mut warnings) = ctx.warnings_slot.write() else {
            return;
        };
        let warned = |warnings: &Vec<RuntimeWarning>, code: RuntimeWarningCode| {
            warnings
                .iter()
                .any(|w| w.code == code && w.rule_name == rule.name)
        };
        if !crossing.is_empty()
            && !warned(&warnings, RuntimeWarningCode::CrossGroupCorrelationNotExact)
        {
            let mut example: Vec<String> = crossing
                .iter()
                .map(|(target, key)| {
                    let short: String = key.iter().take(8).map(|b| format!("{b:02x}")).collect();
                    let kind = if snapshot.contains_key(target) {
                        "fact"
                    } else {
                        "base fact"
                    };
                    format!("{kind} {target}:{short}")
                })
                .collect();
            example.sort();
            warnings.push(RuntimeWarning {
                code: RuntimeWarningCode::CrossGroupCorrelationNotExact,
                message: format!(
                    "Rule '{}': {} IS-ref base fact(s) are shared across different \
                     KEY groups. BDD corrects per-group probabilities but cannot \
                     account for cross-group correlations.",
                    rule.name,
                    crossing.len()
                ),
                rule_name: rule.name.clone(),
                variable_count: Some(crossing.len()),
                key_group: example.first().cloned(),
            });
        }
        // Under `TopKProofs` the fold's own inclusion-exclusion corrects
        // within-group sharing, so the "may overestimate" premise is false.
        let suppress = matches!(ctx.semiring_kind, SemiringKind::TopKProofs { .. });
        if shared
            && !suppress
            && !warned(&warnings, RuntimeWarningCode::SharedProbabilisticDependency)
        {
            warnings.push(RuntimeWarning {
                code: RuntimeWarningCode::SharedProbabilisticDependency,
                message: format!(
                    "Rule '{}' aggregates with MNOR/MPROD but some proof paths \
                     share intermediate facts, violating the independence assumption. \
                     Results may overestimate probability.",
                    rule.name
                ),
                rule_name: rule.name.clone(),
                variable_count: None,
                key_group: None,
            });
        }
    }
}

/// The variable of row `index` of lineage fact `fact`.
fn row_var(snapshot: &HashMap<String, Arc<RuleLineage>>, fact: &FactId, index: usize) -> Var {
    snapshot
        .get(&fact.0)
        .and_then(|l| l.events.get(&fact.1))
        .and_then(|e| e.get(index).copied().flatten())
        .map_or_else(|| Var::Row(fact.clone(), index), Var::Event)
}

/// Whether `fact`'s rule recorded lineage.
fn has_lineage(snapshot: &HashMap<String, Arc<RuleLineage>>, fact: &FactId) -> bool {
    snapshot.contains_key(&fact.0)
}

/// The rows of a lineage fact.
///
/// # Errors
///
/// Fails when the rule recorded lineage but not this fact: a reference that
/// resolves to no fact would otherwise be silently certain or absent.
fn rows_of<'s>(
    snapshot: &'s HashMap<String, Arc<RuleLineage>>,
    fact: &FactId,
) -> DFResult<&'s [RowTerm]> {
    snapshot
        .get(&fact.0)
        .and_then(|l| l.facts.get(&fact.1))
        .map(Vec::as_slice)
        .ok_or_else(|| {
            internal(format!(
                "Locy lineage: a positive IS reference names a fact rule '{}' does not hold",
                fact.0
            ))
        })
}

/// Every lineage fact `root` depends on, `root` first, in discovery order.
fn closure_of(
    snapshot: &HashMap<String, Arc<RuleLineage>>,
    root: &FactId,
) -> DFResult<Vec<FactId>> {
    let mut seen: HashSet<FactId> = HashSet::from([root.clone()]);
    let mut order = vec![root.clone()];
    let mut queue = VecDeque::from([root.clone()]);
    while let Some(fact) = queue.pop_front() {
        for term in rows_of(snapshot, &fact)? {
            if let RowTerm::Conj { refs, .. } = term {
                for r in refs {
                    if has_lineage(snapshot, &r.fact) && seen.insert(r.fact.clone()) {
                        order.push(r.fact.clone());
                        queue.push_back(r.fact.clone());
                    }
                }
            }
        }
    }
    Ok(order)
}

/// The most variables shared-lineage analysis tracks for one fact. Past it
/// the fact counts as shared: the warning says results *may* overestimate,
/// and exact mode then recomputes it (or reports `BddLimitExceeded`). It is
/// independent of `max_bdd_variables`, which bounds exact inference only.
const ANALYSIS_SUPPORT_CAP: usize = 4096;

/// The variables a fact's formula reads, or `Overflow` past the cap.
#[derive(Debug, Clone)]
enum Support {
    Vars(HashSet<Var>),
    Overflow,
}

impl Support {
    fn len(&self) -> Option<usize> {
        match self {
            Self::Vars(v) => Some(v.len()),
            Self::Overflow => None,
        }
    }

    /// The union of `parts`, `Overflow` past `cap` variables.
    fn union<'a>(parts: impl IntoIterator<Item = &'a Support>, cap: usize) -> Support {
        let mut vars = HashSet::new();
        for part in parts {
            match part {
                Self::Overflow => return Self::Overflow,
                Self::Vars(v) => vars.extend(v.iter().cloned()),
            }
            if vars.len() > cap {
                return Self::Overflow;
            }
        }
        Self::Vars(vars)
    }

    /// Whether `parts` read no variable twice between them.
    fn disjoint<'a>(parts: impl IntoIterator<Item = &'a Support> + Clone, cap: usize) -> bool {
        let total: Option<usize> = parts.clone().into_iter().map(Support::len).sum();
        match (total, Self::union(parts, cap).len()) {
            (Some(total), Some(union)) => total == union,
            _ => false,
        }
    }
}

/// Shared-lineage analysis over every fact a rule's facts depend on.
///
/// Facts are visited in strongly connected components, dependencies first,
/// so each support is computed once. A fact on a cycle reads itself and is
/// shared by definition.
struct Analysis {
    /// Facts whose own rows, or one row's parts, read a variable twice.
    local: HashSet<FactId>,
    /// Facts that, or some fact they depend on, are in `local`.
    tainted: HashSet<FactId>,
}

impl Analysis {
    fn locally_shared(&self, fact: &FactId) -> bool {
        self.local.contains(fact)
    }

    fn read_once(&self, fact: &FactId) -> bool {
        !self.tainted.contains(fact)
    }

    fn run(snapshot: &HashMap<String, Arc<RuleLineage>>, rule: &str, cap: usize) -> DFResult<Self> {
        let roots: Vec<FactId> = snapshot
            .get(rule)
            .map(|l| l.facts.keys().map(|k| (rule.to_string(), *k)).collect())
            .unwrap_or_default();
        let sccs = sccs_from(snapshot, &roots)?;
        let mut support: HashMap<FactId, Support> = HashMap::new();
        let mut local: HashSet<FactId> = HashSet::new();
        let mut tainted: HashSet<FactId> = HashSet::new();
        let ref_support = |r: &RefTerm, support: &HashMap<FactId, Support>| -> Support {
            support
                .get(&r.fact)
                .cloned()
                .unwrap_or_else(|| Support::Vars(HashSet::from([Var::Base(r.fact.clone())])))
        };
        // Tarjan yields components dependencies first.
        for component in sccs {
            let members: HashSet<&FactId> = component.iter().collect();
            let cyclic = component.len() > 1 || {
                let only = &component[0];
                rows_of(snapshot, only)?.iter().any(|t| match t {
                    RowTerm::Conj { refs, .. } => refs.iter().any(|r| r.fact == *only),
                    RowTerm::Opaque { .. } => false,
                })
            };
            if cyclic {
                let mut parts: Vec<Support> = Vec::new();
                for fact in &component {
                    for (index, term) in rows_of(snapshot, fact)?.iter().enumerate() {
                        parts.push(Support::Vars(HashSet::from([row_var(
                            snapshot, fact, index,
                        )])));
                        if let RowTerm::Conj { refs, .. } = term {
                            for r in refs.iter().filter(|r| !members.contains(&r.fact)) {
                                parts.push(ref_support(r, &support));
                                if tainted.contains(&r.fact) {
                                    tainted.insert(fact.clone());
                                }
                            }
                        }
                    }
                }
                let shared = Support::union(&parts, cap);
                for fact in component {
                    support.insert(fact.clone(), shared.clone());
                    local.insert(fact.clone());
                    tainted.insert(fact);
                }
                continue;
            }
            let fact = component.into_iter().next().ok_or_else(|| {
                internal("Locy lineage: empty strongly connected component".into())
            })?;
            let mut rows: Vec<Support> = Vec::new();
            let mut overlap = false;
            let mut inherited = false;
            for (index, term) in rows_of(snapshot, &fact)?.iter().enumerate() {
                let mut parts = vec![Support::Vars(HashSet::from([row_var(
                    snapshot, &fact, index,
                )]))];
                if let RowTerm::Conj { refs, .. } = term {
                    for r in refs {
                        parts.push(ref_support(r, &support));
                        inherited |= tainted.contains(&r.fact);
                    }
                }
                overlap |= !Support::disjoint(&parts, cap);
                rows.push(Support::union(&parts, cap));
            }
            overlap |= !Support::disjoint(&rows, cap);
            if overlap {
                local.insert(fact.clone());
            }
            if overlap || inherited {
                tainted.insert(fact.clone());
            }
            support.insert(fact, Support::union(&rows, cap));
        }
        Ok(Self { local, tainted })
    }
}

/// The strongly connected components of the lineage graph reachable from
/// `roots`, dependencies first (Tarjan, iterative).
fn sccs_from(
    snapshot: &HashMap<String, Arc<RuleLineage>>,
    roots: &[FactId],
) -> DFResult<Vec<Vec<FactId>>> {
    let successors = |fact: &FactId| -> DFResult<Vec<FactId>> {
        let mut out = Vec::new();
        for term in rows_of(snapshot, fact)? {
            if let RowTerm::Conj { refs, .. } = term {
                out.extend(
                    refs.iter()
                        .filter(|r| has_lineage(snapshot, &r.fact))
                        .map(|r| r.fact.clone()),
                );
            }
        }
        Ok(out)
    };
    let mut index: HashMap<FactId, usize> = HashMap::new();
    let mut low: HashMap<FactId, usize> = HashMap::new();
    let mut on_stack: HashSet<FactId> = HashSet::new();
    let mut stack: Vec<FactId> = Vec::new();
    let mut components: Vec<Vec<FactId>> = Vec::new();
    let mut next = 0usize;
    for root in roots {
        if index.contains_key(root) {
            continue;
        }
        // Each frame: the fact and its successors still to visit.
        let mut frames: Vec<(FactId, Vec<FactId>)> = Vec::new();
        index.insert(root.clone(), next);
        low.insert(root.clone(), next);
        next += 1;
        stack.push(root.clone());
        on_stack.insert(root.clone());
        frames.push((root.clone(), successors(root)?));
        while let Some((fact, pending)) = frames.last_mut() {
            if let Some(succ) = pending.pop() {
                if !index.contains_key(&succ) {
                    index.insert(succ.clone(), next);
                    low.insert(succ.clone(), next);
                    next += 1;
                    stack.push(succ.clone());
                    on_stack.insert(succ.clone());
                    let succs = successors(&succ)?;
                    frames.push((succ, succs));
                } else if on_stack.contains(&succ) {
                    let l = low[fact].min(index[&succ]);
                    low.insert(fact.clone(), l);
                }
                continue;
            }
            let fact = fact.clone();
            frames.pop();
            if let Some((parent, _)) = frames.last() {
                let l = low[parent].min(low[&fact]);
                low.insert(parent.clone(), l);
            }
            if low[&fact] == index[&fact] {
                let mut component = Vec::new();
                while let Some(member) = stack.pop() {
                    on_stack.remove(&member);
                    let done = member == fact;
                    component.push(member);
                    if done {
                        break;
                    }
                }
                components.push(component);
            }
        }
    }
    Ok(components)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::df_graph::locy_fixpoint::FixpointClausePlan;
    use crate::query::df_graph::locy_fold::FoldBinding;
    use crate::query::planner::LogicalPlan;
    use crate::query::planner_locy_types::PositiveProbRef;
    use arrow_array::StringArray;
    use arrow_array::builder::FixedSizeBinaryBuilder;
    use arrow_schema::{DataType, Field, Schema};

    const KEY_COL: &str = "__deriv_isref_0_0";
    const PROB_COL: &str = "__deriv_isprob_0_0";

    /// Rule `r`: yield `(a KEY, p)`, `FOLD p = MNOR(..)`, one clause holding a
    /// positive reference to the PROB rule `base` that the clause does not
    /// read, so its probability was multiplied in.
    fn rule() -> FixpointRulePlan {
        FixpointRulePlan {
            name: "r".into(),
            clauses: vec![FixpointClausePlan {
                body_logical: LogicalPlan::Empty,
                is_ref_bindings: vec![IsRefBinding {
                    derived_scan_index: 0,
                    rule_name: "base".into(),
                    is_self_ref: false,
                    negated: false,
                    anti_join_cols: Vec::new(),
                    target_has_prob: true,
                    target_prob_col: Some("q".into()),
                    provenance_join_cols: Vec::new(),
                    prob_ref: Some(PositiveProbRef {
                        prob_column: PROB_COL.into(),
                        key_column: Some(KEY_COL.into()),
                        into_column: "p".into(),
                        read: ProbRead::Absent,
                    }),
                }],
                priority: None,
                along_bindings: Vec::new(),
                model_invocations: Vec::new(),
                stratum_scans: Vec::new(),
            }],
            yield_schema: Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("p", DataType::Float64, true),
            ])),
            key_column_indices: vec![0],
            priority: None,
            has_fold: true,
            fold_bindings: vec![FoldBinding {
                output_name: "p".into(),
                name: smol_str::SmolStr::new_static("MNOR"),
                aggregate: Arc::new(uni_plugin_builtin::locy_aggregates::MnorAgg),
                input_col_index: 1,
                input_col_name: Some("p".into()),
                distinct: false,
            }],
            require: Vec::new(),
            having: Vec::new(),
            has_best_by: false,
            best_by_criteria: Vec::new(),
            has_priority: false,
            deterministic: true,
            prob_column_name: Some("p".into()),
            non_linear: false,
            yield_projection: Vec::new(),
        }
    }

    fn hashes(values: &[Option<[u8; 16]>]) -> ArrayRef {
        let mut b = FixedSizeBinaryBuilder::new(ROW_HASH_WIDTH);
        for v in values {
            match v {
                Some(v) => b.append_value(v).unwrap(),
                None => b.append_null(),
            }
        }
        Arc::new(b.finish())
    }

    /// A pre-fold batch of `r` with its columns in an order unlike the yield
    /// schema: `(isprob, p, isref, a)`.
    fn permuted(rows: &[(&str, f64, Option<f64>)]) -> RecordBatch {
        let base_fact = [7u8; 16];
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(PROB_COL, DataType::Float64, true),
                Field::new("p", DataType::Float64, true),
                Field::new(KEY_COL, DataType::FixedSizeBinary(ROW_HASH_WIDTH), true),
                Field::new("a", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                hashes(
                    &rows
                        .iter()
                        .map(|r| r.2.map(|_| base_fact))
                        .collect::<Vec<_>>(),
                ),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn ctx<'a>(
        warnings: &'a Arc<StdRwLock<Vec<RuntimeWarning>>>,
        approximate: &'a Arc<StdRwLock<HashMap<String, Vec<String>>>>,
        max_bdd_variables: usize,
    ) -> ExactCtx<'a> {
        ExactCtx {
            exact: true,
            max_bdd_variables,
            semiring_kind: SemiringKind::AddMultProb,
            warnings_slot: warnings,
            approximate_slot: approximate,
        }
    }

    fn rows_of_output(out: &[RecordBatch]) -> Vec<(String, f64)> {
        let mut rows = Vec::new();
        for batch in out {
            let a = batch.column(batch.schema().index_of("a").unwrap());
            let a = a.as_any().downcast_ref::<StringArray>().unwrap();
            let p = batch.column(batch.schema().index_of("p").unwrap());
            let p = p.as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..batch.num_rows() {
                rows.push((a.value(i).to_string(), p.value(i)));
            }
        }
        rows.sort_by(|l, r| l.0.cmp(&r.0));
        rows
    }

    /// Exact inference groups and reads a pre-fold batch by column NAME.
    ///
    /// Both `x` rows (local weight 0.5) rest on the base fact (0.6), which
    /// default mode multiplied in: each row is 0.3. The exact disjunction is
    /// `B ∧ (L1 ∨ L2)` = 0.6 · 0.75 = 0.45, where independence would give
    /// 1 − 0.7² = 0.51. Read by position, the KEY would be the probability
    /// column and nothing would group.
    #[test]
    fn exact_wmc_resolves_key_and_hash_columns_by_name() {
        let rule = rule();
        let batch = permuted(&[
            ("x", 0.3, Some(0.6)),
            ("x", 0.3, Some(0.6)),
            ("y", 0.3, None),
        ]);
        let store = LineageStore::new();
        store
            .record_rule(&rule, std::slice::from_ref(&batch))
            .unwrap();
        let warnings = Arc::new(StdRwLock::new(Vec::new()));
        let approximate = Arc::new(StdRwLock::new(HashMap::new()));
        let (out, outcome) = store
            .correct(&rule, vec![batch], &ctx(&warnings, &approximate, 64))
            .unwrap();
        assert!(outcome.shared, "the two `x` rows share the base fact");
        let rows = rows_of_output(&out);
        assert_eq!(rows.len(), 2, "one row per corrected fact: {rows:?}");
        assert_eq!(rows[0].0, "x");
        assert!((rows[0].1 - 0.45).abs() < 1e-12, "{rows:?}");
        assert_eq!(rows[1], ("y".to_string(), 0.3));
    }

    /// Over the variable budget, every row stays and `BddLimitExceeded` names
    /// the group.
    #[test]
    fn over_budget_keeps_the_independence_rows() {
        let rule = rule();
        let batch = permuted(&[("x", 0.3, Some(0.6)), ("x", 0.3, Some(0.6))]);
        let store = LineageStore::new();
        store
            .record_rule(&rule, std::slice::from_ref(&batch))
            .unwrap();
        let warnings = Arc::new(StdRwLock::new(Vec::new()));
        let approximate = Arc::new(StdRwLock::new(HashMap::new()));
        let (out, _) = store
            .correct(&rule, vec![batch], &ctx(&warnings, &approximate, 2))
            .unwrap();
        assert_eq!(rows_of_output(&out).len(), 2);
        let warnings = warnings.read().unwrap();
        let limit = warnings
            .iter()
            .find(|w| w.code == RuntimeWarningCode::BddLimitExceeded)
            .expect("BddLimitExceeded");
        assert_eq!(limit.variable_count, Some(3));
        assert!(approximate.read().unwrap().contains_key("r"));
    }

    /// A pre-fold batch missing a column the lineage reads fails instead of
    /// recording whichever columns happen to sit at those positions.
    #[test]
    fn exact_wmc_fails_closed_on_missing_yield_column() {
        let rule = rule();
        let narrow = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["x"]))],
        )
        .unwrap();
        let err = LineageStore::new()
            .record_rule(&rule, std::slice::from_ref(&narrow))
            .expect_err("a missing yield column is an error");
        assert!(err.to_string().contains("'p'"), "{err}");
    }

    /// A reference to a fact the referenced rule's lineage does not hold is an
    /// error, not a silently certain or absent condition.
    #[test]
    fn a_dangling_reference_fails_closed() {
        let mut base = rule();
        base.name = "base".into();
        let store = LineageStore::new();
        // `base` records lineage, but not the fact `r` references.
        store
            .record_rule(&base, &[permuted(&[("z", 0.4, None)])])
            .unwrap();
        let r = rule();
        let batch = permuted(&[("x", 0.3, Some(0.6)), ("x", 0.3, Some(0.6))]);
        store.record_rule(&r, std::slice::from_ref(&batch)).unwrap();
        let warnings = Arc::new(StdRwLock::new(Vec::new()));
        let approximate = Arc::new(StdRwLock::new(HashMap::new()));
        let err = store
            .correct(&r, vec![batch], &ctx(&warnings, &approximate, 64))
            .expect_err("dangling reference");
        assert!(err.to_string().contains("does not hold"), "{err}");
    }

    /// The default-mode multiply reads the PROB column by name and drops the
    /// transient probability columns.
    #[test]
    fn positive_factor_is_multiplied_into_the_named_prob_column() {
        let binding = IsRefBinding {
            derived_scan_index: 0,
            rule_name: "base".into(),
            is_self_ref: false,
            negated: false,
            anti_join_cols: Vec::new(),
            target_has_prob: true,
            target_prob_col: Some("q".into()),
            provenance_join_cols: Vec::new(),
            prob_ref: Some(PositiveProbRef {
                prob_column: "__isprob_0_0".into(),
                key_column: None,
                into_column: "conf".into(),
                read: ProbRead::Absent,
            }),
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__isprob_0_0", DataType::Float64, true),
                Field::new("conf", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![Some(0.5), None])),
                Arc::new(Float64Array::from(vec![0.8, 0.8])),
            ],
        )
        .unwrap();
        let out = apply_positive_prob_factors(vec![batch], std::slice::from_ref(&binding)).unwrap();
        let batch = &out[0];
        assert_eq!(batch.num_columns(), 1, "the transient column is dropped");
        let conf = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((conf.value(0) - 0.4).abs() < 1e-12);
        assert_eq!(
            conf.value(1),
            0.8,
            "a NULL probability leaves the row as is"
        );
    }
}
