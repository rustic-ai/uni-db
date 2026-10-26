// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probability through `IS` references, and the lineage that exact inference,
//! `TopKProofs` and EXPLAIN read.
//!
//! # Semantics
//!
//! A fact of a PROB rule holds with the probability its PROB column carries.
//! A positive reference `x IS r` to such a rule is a probabilistic condition:
//! the row holds only when the referenced fact does. A negated reference
//! `x IS NOT r` holds only when no fact of `r` its key matches does. So:
//!
//! * **Default mode** multiplies the referenced probability into the calling
//!   row's PROB value ([`apply_positive_prob_factors`]), and `IS NOT`
//!   multiplies `1 - p`, `p` the noisy-OR of the matched facts
//!   (`locy_complement`). Both treat the referenced facts as independent of
//!   the rest of the row. A clause whose PROB expression already reads a
//!   positive reference's probability as one product factor
//!   (`FOLD p = MNOR(p)`) counts it once: nothing more is multiplied in. A
//!   read that is not one product factor (`1 - p`) is never multiplied in
//!   either, and is reported (`ProbabilityReadNotFactor`).
//! * **Exact mode** evaluates each fact of an `MNOR`/`MPROD` rule as the
//!   formula `OP over its rows of (row event AND each positively referenced
//!   fact's formula AND NOT each negated fact's formula)` under possible-worlds
//!   semantics, where OP is OR for `MNOR` and AND for `MPROD`, every row event
//!   is an independent variable. A fact of a PROB rule without a fold is the
//!   formula of its own row (`row event AND each positively referenced fact
//!   AND NOT each negated fact`), the facts sharing its KEY ORed, recorded
//!   when provenance is ([`LineageStore::record_rows`]); a fact with no
//!   recorded lineage is one variable.
//! * **`TopKProofs { k }`** evaluates the same formula with each `MNOR` fact
//!   restricted to its `k` most probable rows (its proofs).
//!
//! The row event's own probability is the row's PROB value divided by the
//! referenced probabilities and complements it carries, which recovers the
//! clause's local weight in both the multiplied and the already-a-factor case.
//!
//! When no fact's formula reads a variable twice, the default value of each
//! fact is already its possible-worlds probability, so the modes agree. A
//! repeated variable (two proofs through one fact, a fact and a negated fact
//! resting on one edge, or a cycle) is reported as
//! `SharedProbabilisticDependency`, and only those facts are recomputed.
//!
//! # One reference resolution
//!
//! Which facts a derivation row references is read in one place,
//! [`RefResolver`], from the hidden columns the planner projects into every
//! pre-fold row of an `MNOR`/`MPROD` rule: `__deriv_isref_*` (the hash of a
//! positively referenced fact's KEY, subject and `TO` target included) and
//! `__deriv_isnot_*` (the facts a complement read). Lineage, the provenance
//! EXPLAIN shows and the proofs `TopKProofs` keeps are all built from it.

// Rust guideline compliant

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock as StdRwLock};

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Float64Array, RecordBatch};
use biodivine_lib_bdd::{Bdd, BddVariable, BddVariableSet};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use uni_common::Value;
use uni_locy::{
    DependencyResolution, FEATURE_COL_PREFIX, InferenceMode, ProbabilityDiagnostic, RuntimeWarning,
    RuntimeWarningCode, SemiringKind,
};

use super::common::{arrow_err, extract_scalar_key};
use super::locy_bdd::bdd_probability;
use super::locy_fixpoint::{FixpointRulePlan, pre_fold_column_name, yield_positions_in};
use super::locy_row_hash::{ROW_HASH_WIDTH, row_hash};
use crate::query::planner_locy_types::{
    DERIV_EVENT_COL, ISPROB_COL_PREFIX, LIN_COL_PREFIX, LIN_EVENT_COL, ProbRead,
};

/// The identity of a fact: the hash of its KEY columns.
pub(crate) type FactKey = [u8; ROW_HASH_WIDTH as usize];

/// A fact of a lineage-keeping rule: `(rule name, KEY hash)`.
type FactId = (String, FactKey);

/// The width of one encoded [`NegatedFact`]: its KEY hash, then its
/// probability as little-endian `f64` bits.
const NEGATED_FACT_WIDTH: usize = ROW_HASH_WIDTH as usize + 8;

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
pub(crate) fn f64_at(col: &dyn Array, row: usize, name: &str) -> DFResult<Option<f64>> {
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

/// A fact a negated reference's complement read: its KEY hash and the
/// probability the complement used.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NegatedFact {
    /// The hash of the negated fact's KEY columns, as its rule's lineage
    /// hashes it.
    pub key: FactKey,
    /// The negated fact's probability.
    pub prob: f64,
}

/// The columns of `batch` named `names`, in that order, for hashing a fact's
/// KEY as its rule's lineage does.
///
/// # Errors
///
/// Fails when a column is missing: a fact hashed over fewer columns would be
/// a different fact.
pub(crate) fn fact_key_columns(batch: &RecordBatch, names: &[String]) -> DFResult<Vec<ArrayRef>> {
    names
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map(|i| Arc::clone(batch.column(i)))
                .map_err(|_| {
                    internal(format!(
                        "Locy `IS NOT` lineage: KEY column `{name}` is missing from the \
                         negated rule's facts"
                    ))
                })
        })
        .collect()
}

/// Encodes, per row, the facts a negated reference's complement read into a
/// negated reference's lineage column (`__deriv_isnot_*`). A row that matched
/// no fact is NULL.
pub(crate) fn encode_negated_facts(rows: &[&[NegatedFact]]) -> ArrayRef {
    let mut builder = BinaryBuilder::with_capacity(
        rows.len(),
        rows.iter().map(|r| r.len() * NEGATED_FACT_WIDTH).sum(),
    );
    for facts in rows {
        if facts.is_empty() {
            builder.append_null();
            continue;
        }
        let mut bytes = Vec::with_capacity(facts.len() * NEGATED_FACT_WIDTH);
        for fact in *facts {
            bytes.extend_from_slice(&fact.key);
            bytes.extend_from_slice(&fact.prob.to_le_bytes());
        }
        builder.append_value(&bytes);
    }
    Arc::new(builder.finish())
}

/// Decodes one row of a negated reference's lineage column.
///
/// # Errors
///
/// Fails on a value that is not a whole number of encoded facts.
fn decode_negated_facts(bytes: &[u8], column: &str) -> DFResult<Vec<NegatedFact>> {
    let (chunks, rest) = bytes.as_chunks::<NEGATED_FACT_WIDTH>();
    if !rest.is_empty() {
        return Err(internal(format!(
            "Locy lineage column `{column}` holds a malformed negated-fact list"
        )));
    }
    Ok(chunks
        .iter()
        .map(|chunk| {
            let (key, prob) = chunk.split_at(ROW_HASH_WIDTH as usize);
            let mut k = [0u8; ROW_HASH_WIDTH as usize];
            k.copy_from_slice(key);
            let mut p = [0u8; 8];
            p.copy_from_slice(prob);
            NegatedFact {
                key: k,
                prob: f64::from_le_bytes(p),
            }
        })
        .collect())
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
    bindings: &[super::locy_fixpoint::IsRefBinding],
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

/// Whether `name` is a transient body column that no fact carries: a lineage
/// or provenance column ([`LIN_COL_PREFIX`]) or a model invocation's property
/// feature ([`FEATURE_COL_PREFIX`]).
pub(crate) fn is_transient_column(name: &str) -> bool {
    name.starts_with(LIN_COL_PREFIX) || name.starts_with(FEATURE_COL_PREFIX)
}

/// Drops the transient columns ([`is_transient_column`]) from a clause's
/// rows, once they have been read: they are never part of a fact.
///
/// A feature column kept in the facts made them one column wider than the
/// rule's declared schema, and a rule reading a model-backed relation
/// through `IS` failed to scan it (issue #314).
///
/// # Errors
///
/// Fails when a batch cannot be rebuilt without them.
pub(crate) fn strip_transient_columns(batches: Vec<RecordBatch>) -> DFResult<Vec<RecordBatch>> {
    batches
        .into_iter()
        .map(|batch| {
            let schema = batch.schema();
            if !schema
                .fields()
                .iter()
                .any(|f| is_transient_column(f.name()))
            {
                return Ok(batch);
            }
            let keep: Vec<usize> = (0..schema.fields().len())
                .filter(|&i| !is_transient_column(schema.field(i).name()))
                .collect();
            batch.project(&keep).map_err(arrow_err)
        })
        .collect()
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
        probability: None,
    });
}

/// A referenced fact, with the probability the row read for it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RefTerm {
    /// The referenced fact.
    pub fact: (String, FactKey),
    /// The probability the row read for it (default mode's value).
    pub prob: f64,
}

/// The facts one derivation row references, as [`RefResolver`] reads them.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RowRefs {
    /// Each positively referenced fact (`x IS r`) that is part of the row's
    /// lineage: a probabilistic condition.
    pub positive: Vec<RefTerm>,
    /// Each other positively referenced fact, for provenance only: one of a
    /// rule without a PROB column (certain, probability 1), or one a rule
    /// keeping no lineage read (with the probability it read).
    pub support: Vec<RefTerm>,
    /// Each negated reference (`x IS NOT r`): the facts its complement read.
    /// The row holds only when none of them does.
    pub negated: Vec<Vec<RefTerm>>,
    /// Whether the PROB expression reads a referenced probability other than
    /// as one product factor: the row is then one independent event (its
    /// references are still listed, as the facts it was derived from).
    pub opaque: bool,
    /// The product of the referenced probabilities and complements the row's
    /// PROB value carries, which divides it to recover the row's own weight.
    pub divisor: f64,
}

/// The hidden reference columns of one binding, as resolved in a batch.
enum RefColumn<'a> {
    /// A positive reference: the referenced KEY hash, its probability.
    Positive {
        rule: &'a str,
        key: &'a FixedSizeBinaryArray,
        prob: &'a ArrayRef,
        prob_name: &'a str,
        read: ProbRead,
    },
    /// A negated reference: the facts its complement read.
    Negated {
        rule: &'a str,
        facts: &'a BinaryArray,
        name: &'a str,
    },
    /// A reference named for provenance only: the referenced KEY hash, and
    /// its probability when the referenced rule has one.
    Support {
        rule: &'a str,
        key: &'a FixedSizeBinaryArray,
        prob: Option<(&'a ArrayRef, &'a str)>,
    },
}

/// Which hidden reference columns a [`RefResolver`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefScope {
    /// Every one the plan names: a clause's rows before the transient
    /// columns ([`LIN_COL_PREFIX`](crate::query::planner_locy_types::LIN_COL_PREFIX)) are dropped.
    All,
    /// Only those kept with the facts (`__deriv_*`): a batch of facts, whose
    /// transient columns are gone.
    Kept,
    /// Only the transient ones: a clause's rows, whose kept columns are read
    /// from the facts instead.
    Transient,
}

/// Resolves which facts each row of a batch of a rule's pre-fold rows
/// references, from the rule's hidden reference columns.
///
/// This is the one reference resolution: lineage ([`LineageStore`]), the
/// provenance EXPLAIN shows, and the proofs `TopKProofs` keeps all read it. A
/// reference with no hidden column (one to a rule without a PROB column, or
/// from a rule that keeps no lineage) contributes nothing: it is a certain
/// condition, not an event.
pub(crate) struct RefResolver<'a> {
    columns: Vec<RefColumn<'a>>,
}

impl<'a> RefResolver<'a> {
    /// The resolver for `batch`, a batch of `rule`'s pre-fold rows.
    ///
    /// # Errors
    ///
    /// Fails when a hidden column the plan names, within `scope`, is missing
    /// from the batch or is not of its type: a reference read from the wrong
    /// column would be silently certain or absent.
    pub(crate) fn new(
        rule: &'a FixpointRulePlan,
        batch: &'a RecordBatch,
        scope: RefScope,
    ) -> DFResult<Self> {
        let in_scope = |name: &str| match scope {
            RefScope::All => true,
            RefScope::Kept => !name.starts_with(LIN_COL_PREFIX),
            RefScope::Transient => name.starts_with(LIN_COL_PREFIX),
        };
        let column = |name: &str| -> DFResult<&'a ArrayRef> {
            batch
                .schema()
                .index_of(name)
                .map(|i| batch.column(i))
                .map_err(|_| {
                    internal(format!(
                        "rule '{}': lineage column `{name}` is missing from a fact batch",
                        rule.name
                    ))
                })
        };
        let mut columns = Vec::new();
        for binding in rule.clauses.iter().flat_map(|c| c.is_ref_bindings.iter()) {
            if let Some(r) = binding.prob_ref.as_ref()
                && let Some(key_name) = r.key_column.as_deref()
                && in_scope(key_name)
            {
                let key = column(key_name)?
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        internal(format!(
                            "rule '{}': lineage column `{key_name}` is not a row hash",
                            rule.name
                        ))
                    })?;
                columns.push(RefColumn::Positive {
                    rule: binding.rule_name.as_str(),
                    key,
                    prob: column(&r.prob_column)?,
                    prob_name: r.prob_column.as_str(),
                    read: r.read,
                });
            }
            if let Some(sup) = binding.support_ref.as_ref()
                && in_scope(&sup.key_column)
            {
                let key = column(&sup.key_column)?
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        internal(format!(
                            "rule '{}': provenance column `{}` is not a row hash",
                            rule.name, sup.key_column
                        ))
                    })?;
                let prob = match sup.prob_column.as_deref() {
                    Some(name) => Some((column(name)?, name)),
                    None => None,
                };
                columns.push(RefColumn::Support {
                    rule: binding.rule_name.as_str(),
                    key,
                    prob,
                });
            }
            if let Some(n) = binding
                .negated_lineage
                .as_ref()
                .filter(|n| in_scope(&n.column))
            {
                let facts = column(&n.column)?
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        internal(format!(
                            "rule '{}': lineage column `{}` is not a negated-fact list",
                            rule.name, n.column
                        ))
                    })?;
                columns.push(RefColumn::Negated {
                    rule: binding.rule_name.as_str(),
                    facts,
                    name: n.column.as_str(),
                });
            }
        }
        Ok(Self { columns })
    }

    /// Whether the rule has any hidden reference column.
    pub(crate) fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// The facts row `row` references.
    ///
    /// A NULL hidden column belongs to another clause's reference, or (for a
    /// negated reference) matched no fact: no condition. A NULL referenced
    /// probability was not multiplied in, so the condition is certain.
    ///
    /// # Errors
    ///
    /// Fails on a malformed hash, list or probability.
    pub(crate) fn row(&self, row: usize) -> DFResult<RowRefs> {
        let mut refs = RowRefs {
            divisor: 1.0,
            ..RowRefs::default()
        };
        for column in &self.columns {
            match column {
                RefColumn::Positive {
                    rule,
                    key,
                    prob,
                    prob_name,
                    read,
                } => {
                    if key.is_null(row) {
                        continue;
                    }
                    let Some(p) = f64_at(prob.as_ref(), row, prob_name)? else {
                        continue;
                    };
                    let p = p.clamp(0.0, 1.0);
                    // An opaque read multiplied nothing in.
                    if *read == ProbRead::Opaque {
                        refs.opaque = true;
                    } else {
                        refs.divisor *= p;
                    }
                    let fact_key: FactKey = key
                        .value(row)
                        .try_into()
                        .map_err(|_| internal(format!("rule '{rule}': malformed row hash")))?;
                    refs.positive.push(RefTerm {
                        fact: ((*rule).to_string(), fact_key),
                        prob: p,
                    });
                }
                RefColumn::Negated { rule, facts, name } => {
                    if facts.is_null(row) {
                        continue;
                    }
                    let matched = decode_negated_facts(facts.value(row), name)?;
                    if matched.is_empty() {
                        continue;
                    }
                    for fact in &matched {
                        refs.divisor *= 1.0 - fact.prob.clamp(0.0, 1.0);
                    }
                    refs.negated.push(
                        matched
                            .into_iter()
                            .map(|f| RefTerm {
                                fact: ((*rule).to_string(), f.key),
                                prob: f.prob.clamp(0.0, 1.0),
                            })
                            .collect(),
                    );
                }
                RefColumn::Support { rule, key, prob } => {
                    // NULL: another clause's reference.
                    if key.is_null(row) {
                        continue;
                    }
                    let p = match prob {
                        Some((column, name)) => f64_at(column.as_ref(), row, name)?.unwrap_or(1.0),
                        // A fact of a rule without a PROB column is certain.
                        None => 1.0,
                    };
                    let fact_key: FactKey = key
                        .value(row)
                        .try_into()
                        .map_err(|_| internal(format!("rule '{rule}': malformed row hash")))?;
                    refs.support.push(RefTerm {
                        fact: ((*rule).to_string(), fact_key),
                        prob: p.clamp(0.0, 1.0),
                    });
                }
            }
        }
        Ok(refs)
    }
}

/// One derivation row of a lineage fact.
#[derive(Debug, Clone)]
enum RowTerm {
    /// The row's own event, of probability `local`, every positively
    /// referenced fact, and the negation of every negated reference.
    Conj {
        local: f64,
        /// The row's PROB value: its probability under default mode, which
        /// ranks it among the fact's proofs.
        weight: f64,
        refs: Vec<RefTerm>,
        negs: Vec<Vec<RefTerm>>,
    },
    /// A row whose PROB value cannot be decomposed: one independent event.
    Opaque { prob: f64 },
}

impl RowTerm {
    /// The row's PROB value.
    fn weight(&self) -> f64 {
        match self {
            Self::Conj { weight, .. } => *weight,
            Self::Opaque { prob } => *prob,
        }
    }

    /// Every fact the row references, positively or negated.
    fn referenced(&self) -> impl Iterator<Item = &RefTerm> {
        let (refs, negs): (&[RefTerm], &[Vec<RefTerm>]) = match self {
            Self::Conj { refs, negs, .. } => (refs, negs),
            Self::Opaque { .. } => (&[], &[]),
        };
        refs.iter().chain(negs.iter().flatten())
    }
}

/// The lineage of one `MNOR`/`MPROD` rule, or PROB rule without a fold: each
/// fact's derivation rows.
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

/// The lineage of every `MNOR`/`MPROD` rule, and of every PROB rule without
/// a fold, evaluated so far in a program run.
///
/// Recorded from each `MNOR`/`MPROD` rule's pre-fold rows after its stratum
/// converges ([`Self::record_rule`]), and from a PROB rule's derivation rows
/// as its clauses produce them, kept for its published facts
/// ([`Self::record_rows`], [`Self::publish_rows`]). Read by exact inference,
/// `TopKProofs` and shared-proof detection of the rules that reference it.
/// Re-recording a rule (an `ASSUME` re-evaluation) replaces it.
#[derive(Debug, Default)]
pub(crate) struct LineageStore {
    rules: StdRwLock<HashMap<String, Arc<RuleLineage>>>,
    /// Rows of PROB rules without a fold, accumulated before their facts
    /// are published ([`Self::record_rows`]).
    pending: StdRwLock<HashMap<String, PendingLineage>>,
}

/// One derivation row as lineage: its fact's KEY hash, its term and its
/// event identity.
type LineageRow = (FactKey, RowTerm, Option<FactKey>);

/// What distinguishes one derivation of a row from another: the row
/// (`identity`), its event and the facts it references and negates.
fn derivation_key(identity: &[u8], term: &RowTerm, event: Option<&FactKey>) -> Vec<u8> {
    let mut key = identity.to_vec();
    key.push(0);
    if let Some(event) = event {
        key.push(1);
        key.extend_from_slice(event);
    }
    match term {
        RowTerm::Opaque { prob } => {
            key.push(2);
            key.extend_from_slice(&prob.to_le_bytes());
        }
        RowTerm::Conj { refs, negs, .. } => {
            for r in refs {
                key.push(3);
                key.extend_from_slice(r.fact.0.as_bytes());
                key.push(0);
                key.extend_from_slice(&r.fact.1);
            }
            for group in negs {
                key.push(4);
                for r in group {
                    key.extend_from_slice(r.fact.0.as_bytes());
                    key.push(0);
                    key.extend_from_slice(&r.fact.1);
                }
            }
        }
    }
    key
}

/// The distinct derivation rows of a PROB rule without a fold seen so far,
/// in order: each yield row's identity, its fact's KEY hash, its term and
/// its event; and the [`derivation_key`] of each.
#[derive(Debug, Default)]
struct PendingLineage {
    rows: Vec<(Vec<u8>, LineageRow)>,
    seen: HashSet<Vec<u8>>,
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
    /// Whether some fact's formula reads one variable twice. The runtime
    /// reports it through the shared-dependency diagnostic instead
    /// (`report_shared_dependency`), once the recomputation has run.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by the unit tests of the sharing analysis")
    )]
    pub shared: bool,
}

/// Exact-inference settings and output slots for [`LineageStore::correct`].
pub(crate) struct ExactCtx<'a> {
    /// Whether to recompute shared facts (`exact_probability`).
    pub exact: bool,
    /// The variable budget per fact (`max_bdd_variables`).
    pub max_bdd_variables: usize,
    /// The active semiring. `TopKProofs { k }` recomputes shared facts too,
    /// each `MNOR` fact over its `k` most probable rows, and suppresses the
    /// shared-proof warning.
    pub semiring_kind: SemiringKind,
    /// Where warnings go.
    pub warnings_slot: &'a Arc<StdRwLock<Vec<RuntimeWarning>>>,
    /// Rules with a fact that exceeded the budget, with its KEY.
    pub approximate_slot: &'a Arc<StdRwLock<HashMap<String, Vec<String>>>>,
}

impl ExactCtx<'_> {
    /// The proof budget per fact under `TopKProofs`.
    fn top_k(&self) -> Option<usize> {
        match self.semiring_kind {
            SemiringKind::TopKProofs { k } if k > 0 => Some(k as usize),
            _ => None,
        }
    }

    /// How shared facts are evaluated: recomputed under `TopKProofs` and in
    /// exact mode, combined as independent otherwise.
    fn inference_mode(&self) -> InferenceMode {
        if self.top_k().is_some() {
            InferenceMode::TopKProofs
        } else if self.exact {
            InferenceMode::Exact
        } else {
            InferenceMode::Approximate
        }
    }

    /// The variable limit a fact's formula is evaluated under: the
    /// configured one, capped by the BDD library's variable range.
    fn effective_bdd_variable_limit(&self) -> usize {
        self.max_bdd_variables.min(usize::from(u16::MAX))
    }
}

/// The KEY groups of one rule whose exact recomputation needed more
/// variables than the limit, each with its KEY and the count it needed.
#[derive(Default)]
struct Fallbacks(std::cell::RefCell<Vec<(String, usize)>>);

/// Reports what became of `rule`'s shared probabilistic dependencies (issue
/// #315), once evaluation has decided it, at most once per rule.
///
/// `groups` is how many of its facts rest on shared evidence, `example` one
/// of them, `fallbacks` those whose recomputation fell back, and `premise`
/// the sentence that names the sharing. In a mode that resolves sharing
/// with no fallback, the report is the informational
/// `SharedDependencyResolved`; otherwise it is the
/// `SharedProbabilisticDependency` warning, saying whether resolution was
/// not attempted or fell back, and why. The warning was raised before the
/// recomputation ran, so an exact result was reported as a possible
/// overestimate.
fn report_shared_dependency(
    rule: &str,
    groups: usize,
    example: Option<String>,
    fallbacks: &Fallbacks,
    premise: &str,
    ctx: &ExactCtx<'_>,
) {
    if groups == 0 {
        return;
    }
    let fallbacks = fallbacks.0.borrow();
    let mode = ctx.inference_mode();
    let limits = |d: ProbabilityDiagnostic| ProbabilityDiagnostic {
        configured_bdd_variable_limit: Some(ctx.max_bdd_variables),
        effective_bdd_variable_limit: Some(ctx.effective_bdd_variable_limit()),
        ..d
    };
    let warning = match (mode, fallbacks.is_empty()) {
        (InferenceMode::Approximate, _) => RuntimeWarning {
            code: RuntimeWarningCode::SharedProbabilisticDependency,
            message: format!(
                "{premise} Results may overestimate probability: exact_probability is \
                 off, so the shared evidence was counted once per proof."
            ),
            rule_name: rule.to_string(),
            variable_count: None,
            key_group: example,
            probability: Some(ProbabilityDiagnostic {
                inference_mode: mode,
                resolution: DependencyResolution::NotAttempted,
                reason: Some("exact_probability_disabled".to_string()),
                groups_detected: groups,
                groups_fallen_back: groups,
                configured_bdd_variable_limit: None,
                effective_bdd_variable_limit: None,
            }),
        },
        (_, true) => RuntimeWarning {
            code: RuntimeWarningCode::SharedDependencyResolved,
            message: format!(
                "Rule '{rule}': {groups} KEY group(s) rest on shared evidence; {} \
                 evaluated them without the independence assumption.",
                match mode {
                    InferenceMode::TopKProofs => "top-k proof inference",
                    _ => "exact inference",
                }
            ),
            rule_name: rule.to_string(),
            variable_count: None,
            key_group: example,
            probability: Some(limits(ProbabilityDiagnostic {
                inference_mode: mode,
                resolution: DependencyResolution::Completed,
                reason: None,
                groups_detected: groups,
                groups_fallen_back: 0,
                configured_bdd_variable_limit: None,
                effective_bdd_variable_limit: None,
            })),
        },
        (_, false) => RuntimeWarning {
            code: RuntimeWarningCode::SharedProbabilisticDependency,
            message: format!(
                "{premise} Results may overestimate probability: {} of {groups} shared \
                 group(s) needed more than max_bdd_variables ({}) and kept the \
                 independence value.",
                fallbacks.len(),
                ctx.max_bdd_variables
            ),
            rule_name: rule.to_string(),
            variable_count: fallbacks.iter().map(|(_, n)| *n).max(),
            key_group: fallbacks.first().map(|(k, _)| k.clone()),
            probability: Some(limits(ProbabilityDiagnostic {
                inference_mode: mode,
                resolution: DependencyResolution::Fallback,
                reason: Some("bdd_variable_limit_exceeded".to_string()),
                groups_detected: groups,
                groups_fallen_back: fallbacks.len(),
                configured_bdd_variable_limit: None,
                effective_bdd_variable_limit: None,
            })),
        },
    };
    let Ok(mut warnings) = ctx.warnings_slot.write() else {
        return;
    };
    let existing = warnings.iter().position(|w| {
        w.rule_name == rule
            && matches!(
                w.code,
                RuntimeWarningCode::SharedProbabilisticDependency
                    | RuntimeWarningCode::SharedDependencyResolved
            )
    });
    match existing {
        None => warnings.push(warning),
        // A later evaluation of the rule that did not resolve its sharing
        // outranks an earlier one that did.
        Some(i)
            if warnings[i].code == RuntimeWarningCode::SharedDependencyResolved
                && warning.code == RuntimeWarningCode::SharedProbabilisticDependency =>
        {
            warnings[i] = warning;
        }
        Some(_) => {}
    }
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
        let mut facts: HashMap<FactKey, Vec<RowTerm>> = HashMap::new();
        let mut events: HashMap<FactKey, Vec<Option<FactKey>>> = HashMap::new();
        for batch in pre_fold.iter().filter(|b| b.num_rows() > 0) {
            let rows = lineage_rows(rule, batch, prob_name, DERIV_EVENT_COL, RefScope::Kept)?;
            for (key, term, event) in rows.into_iter().flatten() {
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

    /// Accumulates the derivation rows of `rule`, a PROB rule without a fold,
    /// from a clause's rows before its transient lineage columns are dropped
    /// (see [`crate::query::planner_locy_types::LIN_COL_PREFIX`]). A rule
    /// without them records nothing.
    ///
    /// A fact of such a rule is its row, deduplicated, and holds when any of
    /// its derivations does: two derivations of one row resting on different
    /// events or referenced facts are both its lineage. A derivation seen
    /// again (the same row over the same event and facts, as a later
    /// fixpoint iteration can produce it) is recorded once. The rows become
    /// the rule's lineage only when its facts are published
    /// ([`Self::publish_rows`]).
    ///
    /// # Errors
    ///
    /// Fails when a KEY, PROB or lineage column is missing from a batch.
    pub(crate) fn record_rows(
        &self,
        rule: &FixpointRulePlan,
        rows: &[RecordBatch],
    ) -> DFResult<()> {
        let Some(prob_name) = rule.prob_column_name.as_deref() else {
            return Ok(());
        };
        if !rule.fold_bindings.is_empty() {
            return Ok(());
        }
        // Every clause of a rule keeping a transient lineage projects its
        // event column.
        if !rows
            .iter()
            .any(|b| b.schema().index_of(LIN_EVENT_COL).is_ok())
        {
            return Ok(());
        }
        let Ok(mut pending) = self.pending.write() else {
            return Ok(());
        };
        let entry = pending.entry(rule.name.clone()).or_default();
        for batch in rows.iter().filter(|b| b.num_rows() > 0) {
            let (yield_indices, _) = yield_positions_in(rule, &batch.schema())?;
            let terms = lineage_rows(rule, batch, prob_name, LIN_EVENT_COL, RefScope::All)?;
            for (row, term) in terms.into_iter().enumerate() {
                let Some((key, term, event)) = term else {
                    continue;
                };
                let identity = super::locy_fixpoint::fact_hash_key(batch, &yield_indices, row);
                if entry
                    .seen
                    .insert(derivation_key(&identity, &term, event.as_ref()))
                {
                    entry.rows.push((identity, (key, term, event)));
                }
            }
        }
        Ok(())
    }

    /// Makes the rows [`Self::record_rows`] accumulated for `rule` its
    /// lineage, restricted to its published facts `facts`, and returns the
    /// facts with their PROB values corrected where the independence value
    /// is not exact. A row that a post-fixpoint operator (PRIORITY, BEST BY)
    /// dropped is no fact, and is not one of its fact's derivations either.
    /// Replaces any lineage the rule had.
    ///
    /// A fact's PROB value is its row's: the row's weight times the
    /// probabilities it referenced, as if they were independent, from one
    /// derivation. That is its possible-worlds probability only when the
    /// row's formula reads no variable twice and the row has one
    /// derivation. Otherwise `SharedProbabilisticDependency` is raised, and
    /// in exact mode (and under `TopKProofs`) the value is recomputed from
    /// the formula: the OR of the row's derivations, each its event AND the
    /// facts it references AND NOT the facts it negates. Over the variable
    /// budget it keeps its value and is reported as approximate.
    ///
    /// # Errors
    ///
    /// Fails when a published fact batch lacks a yield or PROB column, or a
    /// reference names a fact its rule's lineage does not hold.
    pub(crate) fn publish_rows(
        &self,
        rule: &FixpointRulePlan,
        facts: Vec<RecordBatch>,
        ctx: &ExactCtx<'_>,
    ) -> DFResult<Vec<RecordBatch>> {
        let Some(rows) = self
            .pending
            .write()
            .ok()
            .and_then(|mut p| p.remove(&rule.name))
        else {
            return Ok(facts);
        };
        let mut published: HashSet<Vec<u8>> = HashSet::new();
        for batch in facts.iter().filter(|b| b.num_rows() > 0) {
            let (yield_indices, _) = yield_positions_in(rule, &batch.schema())?;
            for row in 0..batch.num_rows() {
                published.insert(super::locy_fixpoint::fact_hash_key(
                    batch,
                    &yield_indices,
                    row,
                ));
            }
        }
        let mut by_fact: HashMap<FactKey, Vec<RowTerm>> = HashMap::new();
        let mut events: HashMap<FactKey, Vec<Option<FactKey>>> = HashMap::new();
        // Each published row's derivations: its KEY and their indices among
        // the KEY's rows.
        let mut derivations: HashMap<Vec<u8>, (FactKey, Vec<usize>)> = HashMap::new();
        for (identity, (key, term, event)) in rows.rows {
            if published.contains(&identity) {
                let rows_of_key = by_fact.entry(key).or_default();
                derivations
                    .entry(identity)
                    .or_insert_with(|| (key, Vec::new()))
                    .1
                    .push(rows_of_key.len());
                rows_of_key.push(term);
                events.entry(key).or_default().push(event);
            }
        }
        if let Ok(mut rules) = self.rules.write() {
            rules.insert(
                rule.name.clone(),
                Arc::new(RuleLineage {
                    // Facts sharing a KEY are alternatives: a reference to
                    // the KEY reads any of them, and `IS NOT` none.
                    conjunction: false,
                    facts: by_fact,
                    events,
                }),
            );
        }
        if ctx.semiring_kind == SemiringKind::MaxMinProb {
            return Ok(facts);
        }
        self.correct_rows(rule, facts, &derivations, ctx)
    }

    /// The correction [`Self::publish_rows`] describes, over the rows'
    /// `derivations`.
    fn correct_rows(
        &self,
        rule: &FixpointRulePlan,
        facts: Vec<RecordBatch>,
        derivations: &HashMap<Vec<u8>, (FactKey, Vec<usize>)>,
        ctx: &ExactCtx<'_>,
    ) -> DFResult<Vec<RecordBatch>> {
        let Some(prob_name) = rule.prob_column_name.as_deref() else {
            return Ok(facts);
        };
        let snapshot = self.snapshot();
        let analysis = Analysis::run(&snapshot, &rule.name, ANALYSIS_SUPPORT_CAP)?;
        let inexact = |key: &FactKey, rows: &[usize]| {
            rows.len() > 1
                || analysis.locally_shared(&(rule.name.clone(), *key))
                || !analysis.read_once(&(rule.name.clone(), *key))
        };
        // Shared only through a fact it references is that rule's warning;
        // this rule's is for its own rows. Reported once the recomputation
        // has run, by what it achieved.
        let shared_groups: HashSet<&FactKey> = derivations
            .values()
            .filter(|(key, rows)| {
                rows.len() > 1 || analysis.locally_shared(&(rule.name.clone(), *key))
            })
            .map(|(key, _)| key)
            .collect();
        let fallbacks = Fallbacks::default();
        let report = |fallbacks: &Fallbacks| {
            report_shared_dependency(
                &rule.name,
                shared_groups.len(),
                None,
                fallbacks,
                &format!(
                    "Rule '{}' has PROB facts derived more than one way, or whose \
                     derivation reads one piece of evidence twice, violating the \
                     independence assumption.",
                    rule.name
                ),
                ctx,
            );
        };
        if !ctx.exact && ctx.top_k().is_none() {
            report(&fallbacks);
            return Ok(facts);
        }
        let mut out = Vec::with_capacity(facts.len());
        for batch in facts {
            if batch.num_rows() == 0 {
                out.push(batch);
                continue;
            }
            let schema = batch.schema();
            let (yield_indices, key_indices) = yield_positions_in(rule, &schema)?;
            let prob_index = schema.index_of(prob_name).map_err(|_| {
                internal(format!(
                    "rule '{}': PROB column `{prob_name}` is missing from a fact batch",
                    rule.name
                ))
            })?;
            let mut values: Vec<Option<f64>> = Vec::with_capacity(batch.num_rows());
            let mut changed = false;
            for row in 0..batch.num_rows() {
                let current = f64_at(batch.column(prob_index).as_ref(), row, prob_name)?;
                let identity = super::locy_fixpoint::fact_hash_key(&batch, &yield_indices, row);
                let corrected = match derivations.get(&identity) {
                    Some((key, rows)) if current.is_some() && inexact(key, rows) => {
                        fact_probability(
                            &snapshot,
                            (rule.name.clone(), *key),
                            Some(rows),
                            &BudgetReport {
                                batch: &batch,
                                key_indices: &key_indices,
                                row,
                                ctx,
                                fallbacks: &fallbacks,
                            },
                        )?
                    }
                    _ => None,
                };
                changed |= corrected.is_some();
                values.push(corrected.or(current));
            }
            if !changed {
                out.push(batch);
                continue;
            }
            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
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
        report(&fallbacks);
        Ok(out)
    }

    /// A consistent view of every recorded rule.
    fn snapshot(&self) -> HashMap<String, Arc<RuleLineage>> {
        self.rules.read().map(|r| r.clone()).unwrap_or_default()
    }

    /// Reports shared lineage and cross-group correlation for `rule`, and
    /// replaces each fact whose value it recomputes by one row carrying it:
    /// in exact mode and under `TopKProofs`, a fact whose formula reads a
    /// variable twice; under `TopKProofs { k }`, also an `MNOR` fact with
    /// more than `k` rows, evaluated over its `k` most probable.
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
        self.warn(rule, lineage, &snapshot, ctx);
        let fallbacks = Fallbacks::default();
        let report = |fallbacks: &Fallbacks| {
            let example = shared
                .iter()
                .map(|k| {
                    k.iter()
                        .take(8)
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                })
                .min();
            report_shared_dependency(
                &rule.name,
                shared.len(),
                example,
                fallbacks,
                &format!(
                    "Rule '{}' aggregates with MNOR/MPROD but some proof paths share \
                     intermediate facts, violating the independence assumption.",
                    rule.name
                ),
                ctx,
            );
        };
        let top_k = ctx.top_k();
        if !ctx.exact && top_k.is_none() {
            report(&fallbacks);
            return Ok((pre_fold, outcome));
        }

        // The rows each recomputed fact is evaluated over: all of them, or
        // under `TopKProofs { k }` an `MNOR` fact's `k` most probable.
        let mut needs: HashMap<FactKey, Option<Vec<usize>>> = HashMap::new();
        for (key, rows) in &lineage.facts {
            let pruned = match top_k {
                Some(k) if !lineage.conjunction && rows.len() > k => Some(top_rows(rows, k)),
                _ => None,
            };
            if pruned.is_some() || !analysis.read_once(&(rule.name.clone(), *key)) {
                needs.insert(*key, pruned);
            }
        }
        if needs.is_empty() {
            report(&fallbacks);
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
                let Some(rows) = needs.get(&key) else {
                    keep.push(row as u32);
                    values.push(current);
                    continue;
                };
                let probability = match exact.get(&key) {
                    Some(p) => *p,
                    None => {
                        let p = fact_probability(
                            &snapshot,
                            (rule.name.clone(), key),
                            rows.as_deref(),
                            &BudgetReport {
                                batch: &batch,
                                key_indices: &key_indices,
                                row,
                                ctx,
                                fallbacks: &fallbacks,
                            },
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
                    // One row carries the fact's probability.
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
        report(&fallbacks);
        Ok((out, outcome))
    }
}

/// Each row of `batch`, a batch of `rule`'s derivation rows, as lineage:
/// the KEY hash of its fact, its term and its event identity (from
/// `event_column`). `None` for a row whose PROB value (`prob_name`) is NULL,
/// which no fold reads and no fact carries.
///
/// # Errors
///
/// Fails when a KEY, PROB or lineage column within `scope` is missing from
/// the batch, or of the wrong type.
fn lineage_rows(
    rule: &FixpointRulePlan,
    batch: &RecordBatch,
    prob_name: &str,
    event_column: &str,
    scope: RefScope,
) -> DFResult<Vec<Option<LineageRow>>> {
    let schema = batch.schema();
    let (_, key_indices) = yield_positions_in(rule, &schema)?;
    let key_columns: Vec<ArrayRef> = key_indices
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect();
    let prob_column = schema
        .index_of(prob_name)
        .map(|i| batch.column(i))
        .map_err(|_| {
            internal(format!(
                "rule '{}': lineage column `{prob_name}` is missing from a fact batch",
                rule.name
            ))
        })?;
    let event_column = schema
        .index_of(event_column)
        .ok()
        .map(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    internal(format!(
                        "rule '{}': `{event_column}` is not a row hash",
                        rule.name
                    ))
                })
        })
        .transpose()?;
    let resolver = RefResolver::new(rule, batch, scope)?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        // A NULL input is skipped by the fold, so it is no proof.
        let Some(weight) = f64_at(prob_column.as_ref(), row, prob_name)? else {
            out.push(None);
            continue;
        };
        let weight = weight.clamp(0.0, 1.0);
        let refs = resolver.row(row)?;
        let term = if refs.opaque {
            RowTerm::Opaque { prob: weight }
        } else {
            // A divisor of 0 means a referenced probability of 0 or a negated
            // one of 1, so the weight is 0 as well. Such a default value is
            // also exact (a certain or impossible fact stays so under any
            // correlation), so the row's own weight is never read.
            let local = if refs.divisor > 0.0 {
                (weight / refs.divisor).clamp(0.0, 1.0)
            } else {
                0.0
            };
            RowTerm::Conj {
                local,
                weight,
                refs: refs.positive,
                negs: refs.negated,
            }
        };
        let event = match event_column {
            Some(col) if !refs.opaque && !col.is_null(row) => Some(
                col.value(row)
                    .try_into()
                    .map_err(|_| internal(format!("rule '{}': malformed row hash", rule.name)))?,
            ),
            _ => None,
        };
        out.push(Some((row_hash(&key_columns, row)?, term, event)));
    }
    Ok(out)
}

/// The indices of the `k` rows of highest PROB value, ties broken by
/// position so the choice is deterministic.
fn top_rows(rows: &[RowTerm], k: usize) -> Vec<usize> {
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|&a, &b| {
        rows[b]
            .weight()
            .total_cmp(&rows[a].weight())
            .then(a.cmp(&b))
    });
    order.truncate(k);
    order.sort_unstable();
    order
}

/// Where [`fact_probability`] reports a fact over the variable budget.
struct BudgetReport<'a, 'c> {
    batch: &'a RecordBatch,
    key_indices: &'a [usize],
    row: usize,
    ctx: &'a ExactCtx<'c>,
    /// Where the fact is recorded when it falls back.
    fallbacks: &'a Fallbacks,
}

/// The possible-worlds probability of `root`, over only its rows `rows` when
/// given (the facts it references keep all theirs), or `None` (with
/// `BddLimitExceeded`) when its formula needs more than the budget's
/// variables.
fn fact_probability(
    snapshot: &HashMap<String, Arc<RuleLineage>>,
    root: FactId,
    rows: Option<&[usize]>,
    report: &BudgetReport<'_, '_>,
) -> DFResult<Option<f64>> {
    // Dependencies first: a negated fact sits in a lower stratum, so no
    // component holds a negation of one of its own members.
    let components = sccs_from(snapshot, std::slice::from_ref(&root))?;
    let mut vars: Vec<(Var, f64)> = Vec::new();
    let mut var_index: HashMap<Var, usize> = HashMap::new();
    let mut intern = |var: Var, p: f64, vars: &mut Vec<(Var, f64)>| {
        var_index.entry(var.clone()).or_insert_with(|| {
            vars.push((var, p));
            vars.len() - 1
        });
    };
    for fact in components.iter().flatten() {
        for (index, term) in rows_of(snapshot, fact)?.iter().enumerate() {
            let p = match term {
                RowTerm::Conj { local, .. } => {
                    for r in term.referenced() {
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
    let ctx = report.ctx;
    if vars.len() > ctx.effective_bdd_variable_limit() {
        let key_desc = format!(
            "{:?}",
            extract_scalar_key(report.batch, report.key_indices, report.row)
        );
        report
            .fallbacks
            .0
            .borrow_mut()
            .push((key_desc.clone(), vars.len()));
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
                probability: Some(ProbabilityDiagnostic {
                    inference_mode: ctx.inference_mode(),
                    resolution: DependencyResolution::Fallback,
                    reason: Some("bdd_variable_limit_exceeded".to_string()),
                    groups_detected: 1,
                    groups_fallen_back: 1,
                    configured_bdd_variable_limit: Some(ctx.max_bdd_variables),
                    effective_bdd_variable_limit: Some(ctx.effective_bdd_variable_limit()),
                }),
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
    let mut phi: HashMap<FactId, Bdd> = HashMap::new();
    // One row: its event, every referenced fact, no negated one.
    let row_formula =
        |fact: &FactId, index: usize, term: &RowTerm, phi: &HashMap<FactId, Bdd>| -> Bdd {
            let operand = |r: &RefTerm| match phi.get(&r.fact) {
                Some(b) => b.clone(),
                None => var_bdd(&Var::Base(r.fact.clone())),
            };
            let mut t = var_bdd(&row_var(snapshot, fact, index));
            if let RowTerm::Conj { refs, negs, .. } = term {
                for r in refs {
                    t = t.and(&operand(r));
                }
                for group in negs {
                    let mut any = set.mk_false();
                    for r in group {
                        any = any.or(&operand(r));
                    }
                    t = t.and_not(&any);
                }
            }
            t
        };
    let combine =
        |fact: &FactId, selected: Option<&[usize]>, phi: &HashMap<FactId, Bdd>| -> DFResult<Bdd> {
            let conjunction = snapshot[&fact.0].conjunction;
            let mut combined: Option<Bdd> = None;
            for (index, term) in rows_of(snapshot, fact)?.iter().enumerate() {
                if selected.is_some_and(|s| !s.contains(&index)) {
                    continue;
                }
                let t = row_formula(fact, index, term, phi);
                combined = Some(match combined {
                    None => t,
                    Some(acc) if conjunction => acc.and(&t),
                    Some(acc) => acc.or(&t),
                });
            }
            Ok(combined.unwrap_or_else(|| set.mk_false()))
        };
    // Each component is a least fixpoint over the formulas of the components
    // before it: every member false, then re-derived until stable. A
    // derivation through a cycle needs the fact it derives, so it adds
    // nothing, and each round adds proofs one step longer, so
    // `component.len()` rounds reach the fixpoint. Within a component every
    // reference is positive, so the iteration is monotone.
    for component in &components {
        for fact in component {
            phi.insert(fact.clone(), set.mk_false());
        }
        let mut stable = false;
        for _ in 0..=component.len() {
            let mut changed = false;
            for fact in component {
                let new = combine(fact, None, &phi)?;
                if phi[fact] != new {
                    phi.insert(fact.clone(), new);
                    changed = true;
                }
            }
            if !changed {
                stable = true;
                break;
            }
        }
        if !stable {
            return Err(internal(format!(
                "rule '{rule_name}': exact inference did not reach a fixpoint"
            )));
        }
    }
    let formula = match rows {
        Some(selected) => combine(&root, Some(selected), &phi)?,
        None => phi[&root].clone(),
    };
    let probs: HashMap<BddVariable, f64> = vars
        .iter()
        .enumerate()
        .map(|(i, (_, p))| (bdd_vars[i], *p))
        .collect();
    Ok(Some(bdd_probability(&formula, &probs)))
}

impl LineageStore {
    /// Raises `CrossGroupCorrelationNotExact` for `rule`, at most once. Its
    /// shared dependencies are reported after their recomputation
    /// ([`report_shared_dependency`]).
    fn warn(
        &self,
        rule: &FixpointRulePlan,
        lineage: &RuleLineage,
        snapshot: &HashMap<String, Arc<RuleLineage>>,
        ctx: &ExactCtx<'_>,
    ) {
        // Facts of this rule consuming each referenced fact (or base event).
        let mut consumers: HashMap<&FactId, HashSet<&FactKey>> = HashMap::new();
        for (key, rows) in &lineage.facts {
            for term in rows {
                for r in term.referenced() {
                    consumers.entry(&r.fact).or_default().insert(key);
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
                probability: None,
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
                "Locy lineage: an IS reference names a fact rule '{}' does not hold",
                fact.0
            ))
        })
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
/// shared by definition. A negated fact's variables count like a positively
/// referenced one's: a row that conjoins a fact and the negation of another
/// over one variable is not their independent product.
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
                rows_of(snapshot, only)?
                    .iter()
                    .any(|t| t.referenced().any(|r| r.fact == *only))
            };
            if cyclic {
                let mut parts: Vec<Support> = Vec::new();
                for fact in &component {
                    for (index, term) in rows_of(snapshot, fact)?.iter().enumerate() {
                        parts.push(Support::Vars(HashSet::from([row_var(
                            snapshot, fact, index,
                        )])));
                        for r in term.referenced().filter(|r| !members.contains(&r.fact)) {
                            parts.push(ref_support(r, &support));
                            if tainted.contains(&r.fact) {
                                tainted.insert(fact.clone());
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
                for r in term.referenced() {
                    parts.push(ref_support(r, &support));
                    inherited |= tainted.contains(&r.fact);
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
/// `roots`, dependencies first (Tarjan, iterative). A fact's successors are
/// the lineage facts its rows reference, positively or negated.
fn sccs_from(
    snapshot: &HashMap<String, Arc<RuleLineage>>,
    roots: &[FactId],
) -> DFResult<Vec<Vec<FactId>>> {
    let successors = |fact: &FactId| -> DFResult<Vec<FactId>> {
        let mut out = Vec::new();
        for term in rows_of(snapshot, fact)? {
            out.extend(
                term.referenced()
                    .filter(|r| has_lineage(snapshot, &r.fact))
                    .map(|r| r.fact.clone()),
            );
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
    use crate::query::df_graph::locy_fixpoint::{FixpointClausePlan, IsRefBinding};
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
                    prob_ref: Some(PositiveProbRef {
                        prob_column: PROB_COL.into(),
                        key_column: Some(KEY_COL.into()),
                        into_column: "p".into(),
                        read: ProbRead::Absent,
                    }),
                    negated_lineage: None,
                    support_ref: None,
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
            prob_ref: Some(PositiveProbRef {
                prob_column: "__isprob_0_0".into(),
                key_column: None,
                into_column: "conf".into(),
                read: ProbRead::Absent,
            }),
            negated_lineage: None,
            support_ref: None,
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

    const NOT_COL: &str = "__deriv_isnot_0_0";

    /// Rule `r` with a second reference, `IS NOT base`, whose complement
    /// records the facts it read in `__deriv_isnot_0_0`.
    fn rule_with_negation() -> FixpointRulePlan {
        let mut rule = rule();
        rule.clauses[0].is_ref_bindings.push(IsRefBinding {
            derived_scan_index: 1,
            rule_name: "base".into(),
            is_self_ref: false,
            negated: true,
            anti_join_cols: Vec::new(),
            target_has_prob: true,
            target_prob_col: Some("q".into()),
            prob_ref: None,
            negated_lineage: Some(crate::query::planner_locy_types::NegatedLineageRef {
                column: NOT_COL.into(),
                target_key_columns: vec!["a".into()],
            }),
            support_ref: None,
        });
        rule
    }

    /// A pre-fold batch of [`rule_with_negation`]: per row `(a, p, positive
    /// fact and its probability, negated facts)`.
    /// One row of [`negation_batch`]: `a`, `p`, the positive fact and its
    /// probability, the negated facts.
    type NegationRow<'a> = (&'a str, f64, Option<([u8; 16], f64)>, &'a [NegatedFact]);

    fn negation_batch(rows: &[NegationRow<'_>]) -> RecordBatch {
        let negated: Vec<&[NegatedFact]> = rows.iter().map(|r| r.3).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("p", DataType::Float64, true),
                Field::new(KEY_COL, DataType::FixedSizeBinary(ROW_HASH_WIDTH), true),
                Field::new(PROB_COL, DataType::Float64, true),
                Field::new(NOT_COL, DataType::Binary, true),
            ])),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                hashes(&rows.iter().map(|r| r.2.map(|f| f.0)).collect::<Vec<_>>()),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.2.map(|f| f.1)).collect::<Vec<_>>(),
                )),
                encode_negated_facts(&negated),
            ],
        )
        .unwrap()
    }

    /// A row that needs a fact and the negation of the same fact cannot
    /// hold. Default mode multiplied `p · (1 − p)` into it as independent
    /// factors; exact mode conjoins the negated literal, and the reuse of
    /// the fact's variable is shared lineage.
    #[test]
    fn a_negated_reference_is_a_negated_literal_of_the_fact() {
        let rule = rule_with_negation();
        let fact = [3u8; 16];
        // Local weight 1: 1 · 0.6 · (1 − 0.6).
        let batch = negation_batch(&[(
            "x",
            0.24,
            Some((fact, 0.6)),
            &[NegatedFact {
                key: fact,
                prob: 0.6,
            }],
        )]);
        let store = LineageStore::new();
        store
            .record_rule(&rule, std::slice::from_ref(&batch))
            .unwrap();
        let warnings = Arc::new(StdRwLock::new(Vec::new()));
        let approximate = Arc::new(StdRwLock::new(HashMap::new()));
        let (out, outcome) = store
            .correct(&rule, vec![batch], &ctx(&warnings, &approximate, 64))
            .unwrap();
        assert!(outcome.shared, "the fact and its negation share a variable");
        let rows = rows_of_output(&out);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].1.abs() < 1e-12, "x ∧ ¬x is impossible: {rows:?}");
    }

    /// A negated reference matching several facts negates their
    /// disjunction, and recovers the row's own weight from their noisy-OR
    /// complement. Here no variable repeats, so default mode is exact and
    /// nothing is recomputed: the row keeps `w · (1 − 0.5)(1 − 0.2)`.
    #[test]
    fn a_negated_reference_without_sharing_is_read_once() {
        let rule = rule_with_negation();
        let negated = [
            NegatedFact {
                key: [1u8; 16],
                prob: 0.5,
            },
            NegatedFact {
                key: [2u8; 16],
                prob: 0.2,
            },
        ];
        let batch = negation_batch(&[("x", 0.9 * 0.5 * 0.8, None, &negated)]);
        let store = LineageStore::new();
        store
            .record_rule(&rule, std::slice::from_ref(&batch))
            .unwrap();
        let warnings = Arc::new(StdRwLock::new(Vec::new()));
        let approximate = Arc::new(StdRwLock::new(HashMap::new()));
        let (out, outcome) = store
            .correct(&rule, vec![batch], &ctx(&warnings, &approximate, 64))
            .unwrap();
        assert!(!outcome.shared);
        let rows = rows_of_output(&out);
        assert!((rows[0].1 - 0.36).abs() < 1e-12, "{rows:?}");
        // The recorded row: local 0.9, two negated facts.
        let lineage = &store.snapshot()["r"];
        let terms: Vec<&RowTerm> = lineage.facts.values().flatten().collect();
        match terms.as_slice() {
            [RowTerm::Conj { local, negs, .. }] => {
                assert!((local - 0.9).abs() < 1e-12);
                assert_eq!(negs.len(), 1);
                assert_eq!(negs[0].len(), 2);
            }
            other => panic!("{other:?}"),
        }
    }

    /// The negated-fact list round-trips, and a malformed one fails.
    #[test]
    fn negated_facts_round_trip() {
        let facts = [NegatedFact {
            key: [9u8; 16],
            prob: 0.25,
        }];
        let column = encode_negated_facts(&[&facts, &[]]);
        let column = column.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(column.is_null(1), "no matched fact is NULL");
        assert_eq!(decode_negated_facts(column.value(0), "c").unwrap(), facts);
        assert!(decode_negated_facts(&[0u8; 5], "c").is_err());
    }

    /// `TopKProofs { k }` evaluates an `MNOR` fact over its `k` most
    /// probable rows; a fact with no more than `k` rows and no shared
    /// variable keeps its rows.
    #[test]
    fn topk_keeps_the_k_most_probable_rows() {
        let rule = rule();
        let batch = permuted(&[
            ("x", 0.3, None),
            ("x", 0.5, None),
            ("x", 0.2, None),
            ("y", 0.4, None),
            ("y", 0.1, None),
        ]);
        let store = LineageStore::new();
        store
            .record_rule(&rule, std::slice::from_ref(&batch))
            .unwrap();
        let warnings = Arc::new(StdRwLock::new(Vec::new()));
        let approximate = Arc::new(StdRwLock::new(HashMap::new()));
        let mut c = ctx(&warnings, &approximate, 64);
        c.exact = false;
        c.semiring_kind = SemiringKind::TopKProofs { k: 2 };
        let (out, _) = store.correct(&rule, vec![batch], &c).unwrap();
        let rows = rows_of_output(&out);
        // x: 1 − (1 − 0.5)(1 − 0.3), its 0.2 row pruned; y: both rows kept.
        assert_eq!(rows.len(), 3, "{rows:?}");
        assert_eq!(rows[0].0, "x");
        assert!((rows[0].1 - 0.65).abs() < 1e-12, "{rows:?}");
        let mut y: Vec<f64> = rows[1..].iter().map(|r| r.1).collect();
        y.sort_by(f64::total_cmp);
        assert_eq!(y, vec![0.1, 0.4]);
    }

    /// The references a row resolves to are read by column NAME, and a
    /// hidden column the plan names but the batch lacks is an error rather
    /// than a silently certain condition.
    #[test]
    fn the_resolver_reads_references_by_name_and_fails_closed() {
        let rule = rule();
        let batch = permuted(&[("x", 0.3, Some(0.6)), ("y", 0.3, None)]);
        let resolver = RefResolver::new(&rule, &batch, RefScope::All).unwrap();
        let x = resolver.row(0).unwrap();
        assert_eq!(x.positive.len(), 1);
        assert_eq!(x.positive[0].fact, ("base".to_string(), [7u8; 16]));
        assert!((x.divisor - 0.6).abs() < 1e-12);
        assert!(resolver.row(1).unwrap().positive.is_empty());
        let narrow = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["x"]))],
        )
        .unwrap();
        let err = RefResolver::new(&rule, &narrow, RefScope::All)
            .err()
            .expect("a missing hidden column is an error");
        assert!(err.to_string().contains(KEY_COL), "{err}");
    }
}
