// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! IS-NOT (negation) semantics over Arrow batches.
//!
//! Two complementary strategies for a negated body condition, plus the
//! post-processing step that folds their output back into the rule's PROB
//! column:
//!
//! * [`apply_prob_complement_composite`] — probabilistic complement, used when
//!   the stratum carries probabilities.
//! * [`apply_anti_join_composite`] — boolean anti-join, used otherwise.
//! * [`multiply_prob_factors`] — multiplies the `__prob_complement_*` factors
//!   into the PROB column and strips the internal columns.
//!
//! [`super::locy_delta`] carries the row-based (`Vec<FactRow>`) counterparts
//! used by the SLG / EXPLAIN dispatch path.

use crate::query::df_graph::common::arrow_err;
use crate::query::planner_locy_types::NegationKey;
use arrow_array::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uni_locy::ColumnKind;

/// Which side of a negation join key is being resolved. Error messages only.
#[derive(Clone, Copy)]
enum KeySide {
    /// The consuming rule's body output — the `IS NOT` subject.
    Body,
    /// The negated rule's derived facts.
    NegatedFacts,
}

/// Resolves the composite negation key's column indices, or fails.
///
/// **This must never degrade to "return the rows unfiltered".** Every one of
/// these lookups used to fall back to passing the batch through, which emitted
/// precisely the rows the negation was asked to exclude — a silent, fail-open
/// wrong answer in the direction that matters (exemption guards, access-control
/// filters). See <https://github.com/rustic-ai/uni-db/issues/158>.
///
/// A missing body column is a user-facing condition: the anti-join runs after
/// `LocyProject`, so the subject must be a projected output column. A missing
/// negated-facts column is schema drift between a rule's declared YIELD schema
/// and its materialized facts, and is a bug rather than a user error — both are
/// reported, neither is swallowed.
fn resolve_key_indices(
    batch: &RecordBatch,
    join_cols: &[NegationKey],
    side: KeySide,
) -> datafusion::error::Result<Vec<usize>> {
    let schema = batch.schema();
    let mut indices = Vec::with_capacity(join_cols.len());
    for key in join_cols {
        let name = match side {
            KeySide::Body => &key.body_column,
            KeySide::NegatedFacts => &key.fact_column,
        };
        let Ok(idx) = schema.index_of(name) else {
            let available = schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(datafusion::error::DataFusionError::Plan(match side {
                // The planner carries every key component's binder through
                // `LocyProject` under a hidden name, whatever YIELD called it,
                // so this is a defensive backstop: its advice must not repeat
                // the old "add it to your YIELD" suggestion.
                KeySide::Body => format!(
                    "Locy `IS NOT` cannot resolve key column `{name}`. Available \
                     columns: [{available}]. The planner carries the binder of every \
                     KEY position of a negation through projection as a hidden \
                     column; reaching this point means that column was expected but \
                     is absent from the projected batch."
                ),
                KeySide::NegatedFacts => format!(
                    "Locy `IS NOT` cannot resolve column `{name}` in the negated \
                     rule's derived facts. Available columns: [{available}]. This is \
                     schema drift between the negated rule's declared YIELD schema \
                     and its materialized facts."
                ),
            }));
        };
        indices.push(idx);
    }
    Ok(indices)
}

/// Verifies every node component of a negation key is a `UInt64` vid column.
///
/// Like [`resolve_key_indices`], a mismatch used to pass the batch through
/// unfiltered. It now fails, for the same reason. Relationship and scalar
/// components are compared by value and need no such check.
fn verify_key_columns_are_vids(
    batch: &RecordBatch,
    join_cols: &[NegationKey],
    indices: &[usize],
) -> datafusion::error::Result<()> {
    use arrow_schema::DataType;
    for (key, &ci) in join_cols.iter().zip(indices) {
        if key.kind != ColumnKind::Node {
            continue;
        }
        let col = batch.column(ci);
        if col.data_type() != &DataType::UInt64 {
            let field = batch.schema().field(ci).clone();
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "Locy `IS NOT` key column `{}` has type {:?}, expected UInt64 (a node \
                 id). The negated rule's KEY column `{}` holds a node, so the key \
                 component joins on node identity.",
                field.name(),
                col.data_type(),
                key.fact_column,
            )));
        }
    }
    Ok(())
}

/// The composite negation key of every row of `batch`, encoded as bytes that
/// compare equal exactly when every component is equal by value.
///
/// Each key column is cast to `types` — the negated facts' column types — so
/// a body column and the fact column it is compared with share one encoding;
/// a column that cannot be cast is an error, never a silently empty key. A
/// row with a NULL component has no key (`None`): it is never banned, and a
/// negated fact with one bans nothing.
fn negation_key_rows(
    batch: &RecordBatch,
    join_cols: &[NegationKey],
    side: KeySide,
    types: &[arrow_schema::DataType],
) -> datafusion::error::Result<Vec<Option<Box<[u8]>>>> {
    use arrow_array::Array as _;
    use arrow_row::{RowConverter, SortField};

    let indices = resolve_key_indices(batch, join_cols, side)?;
    verify_key_columns_are_vids(batch, join_cols, &indices)?;
    let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(indices.len());
    for ((key, &ci), ty) in join_cols.iter().zip(&indices).zip(types) {
        let col = batch.column(ci);
        let col = if col.data_type() == ty {
            Arc::clone(col)
        } else {
            arrow::compute::cast(col, ty).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Locy `IS NOT` cannot compare `{}` ({:?}) with the negated rule's KEY \
                     column `{}` ({ty:?}): {e}",
                    key.body_column,
                    col.data_type(),
                    key.fact_column,
                ))
            })?
        };
        columns.push(col);
    }
    let converter = RowConverter::new(types.iter().cloned().map(SortField::new).collect())
        .map_err(arrow_err)?;
    let rows = converter.convert_columns(&columns).map_err(arrow_err)?;
    Ok((0..batch.num_rows())
        .map(|row| {
            columns
                .iter()
                .all(|c| !c.is_null(row))
                .then(|| Box::<[u8]>::from(rows.row(row).as_ref()))
        })
        .collect())
}

/// The key column types of the negated facts, from their first batch.
fn negation_key_types(
    neg: &RecordBatch,
    join_cols: &[NegationKey],
) -> datafusion::error::Result<Vec<arrow_schema::DataType>> {
    let indices = resolve_key_indices(neg, join_cols, KeySide::NegatedFacts)?;
    Ok(indices
        .iter()
        .map(|&ci| neg.column(ci).data_type().clone())
        .collect())
}

/// Drops the hidden `IS NOT` subject-`_vid` columns after the anti-join has run.
///
/// The planner projects each negation key component's binder under a reserved
/// name so the anti-join resolves it by identity rather than by whatever YIELD
/// called it (issue #158, see `LocyIsRef::negation_keys`). Those columns must not
/// outlive the anti-join.
///
/// **Placement is load-bearing.** This must run before the batches reach
/// `merge_delta`, whose `reconcile_schema` adopts the first non-empty batch's
/// schema as the rule's *fact identity* and would permanently widen the
/// relation; and before `write_facts_to_registry`, which silently falls back to
/// the batch's own schema on mismatch rather than erroring — corrupting
/// cross-stratum IS-refs with no diagnostic at all.
///
/// A no-op when no hidden column is present, so it is safe to call
/// unconditionally on every clause-body result.
pub fn strip_isnot_vid_columns(
    batches: Vec<RecordBatch>,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use crate::query::planner_locy_types::ISNOT_VID_COL_PREFIX;

    let mut result = Vec::with_capacity(batches.len());
    for batch in batches {
        let keep: Vec<usize> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !f.name().starts_with(ISNOT_VID_COL_PREFIX))
            .map(|(i, _)| i)
            .collect();
        if keep.len() == batch.num_columns() {
            result.push(batch);
            continue;
        }
        let fields: Vec<_> = keep
            .iter()
            .map(|&i| batch.schema().field(i).clone())
            .collect();
        let columns: Vec<_> = keep.iter().map(|&i| batch.column(i).clone()).collect();
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        result.push(RecordBatch::try_new(schema, columns).map_err(arrow_err)?);
    }
    Ok(result)
}

/// Drops the hidden derivation-discriminator columns (issue #159).
///
/// Mirrors [`strip_isnot_vid_columns`], but runs at a different point: those
/// exist only to survive the anti-join and are stripped *before* `merge_delta`,
/// whereas these exist precisely to survive **dedup**, so they must not be
/// removed until the fixpoint has converged.
///
/// Stripping columns does not re-dedup, so an ALONG rule's equal-valued sibling
/// rows remain separate rows afterwards — they simply lose the hidden columns.
/// That is what lets everything downstream (the derived store, cross-stratum
/// IS-ref scans, the user-visible surface) keep its original narrow schema.
///
/// A `FOLD` rule never needs this: `FoldExec` emits KEY plus fold columns only,
/// so the fold drops them for free. It is `ALONG`-without-`FOLD` rules that
/// would otherwise leak wide batches into `write_facts_to_registry`, which
/// falls back *silently* on a schema mismatch and then fails at scan time in
/// `DerivedScanExec`, which re-stamps its declared schema.
pub fn strip_derivation_discriminator_columns(
    batches: Vec<RecordBatch>,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use crate::query::planner_locy_types::FOLD_DISCRIMINATOR_COL_PREFIX;

    let mut result = Vec::with_capacity(batches.len());
    for batch in batches {
        let keep: Vec<usize> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !f.name().starts_with(FOLD_DISCRIMINATOR_COL_PREFIX))
            .map(|(i, _)| i)
            .collect();
        if keep.len() == batch.num_columns() {
            result.push(batch);
            continue;
        }
        let fields: Vec<_> = keep
            .iter()
            .map(|&i| batch.schema().field(i).clone())
            .collect();
        let columns: Vec<_> = keep.iter().map(|&i| batch.column(i).clone()).collect();
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        result.push(RecordBatch::try_new(schema, columns).map_err(arrow_err)?);
    }
    Ok(result)
}

/// The negated facts of a probabilistic `IS NOT`, indexed by composite key.
///
/// Each composite key maps to the noisy-OR of the probabilities of the facts
/// that carry it. Built by `build_prob_index`, probed by
/// `probe_prob_complement`.
#[derive(Debug)]
pub(crate) struct ProbIndex {
    /// The negated facts' key column types; `None` when there are no facts.
    types: Option<Vec<arrow_schema::DataType>>,
    /// Composite key to combined probability.
    probs: HashMap<Box<[u8]>, f64>,
    /// Composite key to each fact carrying it: its KEY hash and probability.
    /// Built only for a reference that keeps lineage (see
    /// [`crate::query::planner_locy_types::NegatedLineageRef`]).
    facts: Option<HashMap<Box<[u8]>, Vec<super::locy_lineage::NegatedFact>>>,
}

/// Indexes `neg_facts` for a probabilistic complement over `join_cols`.
///
/// With `lineage_keys` (the negated rule's KEY columns, for a reference that
/// keeps lineage) the index also records, per composite key, each fact's KEY
/// hash and probability.
///
/// # Errors
///
/// An unresolvable key or PROB column in the negated facts (never a silently
/// empty index), or a PROB value that is not a number.
pub(crate) fn build_prob_index(
    neg_facts: &[RecordBatch],
    join_cols: &[NegationKey],
    prob_col: &str,
    lineage_keys: Option<&[String]>,
) -> datafusion::error::Result<ProbIndex> {
    let types = match neg_facts.first() {
        Some(neg) => Some(negation_key_types(neg, join_cols)?),
        None => None,
    };

    // Build composite-key → probability lookup from negative facts.
    let mut probs: HashMap<Box<[u8]>, f64> = HashMap::new();
    let mut facts: Option<HashMap<Box<[u8]>, Vec<super::locy_lineage::NegatedFact>>> =
        lineage_keys.map(|_| HashMap::new());
    if let Some(types) = &types {
        for batch in neg_facts {
            let keys = negation_key_rows(batch, join_cols, KeySide::NegatedFacts, types)?;
            let prob_idx = batch.schema().index_of(prob_col).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Locy `IS NOT` probabilistic complement cannot resolve PROB column \
                     `{prob_col}` in the negated rule's derived facts. Dropping the batch \
                     would silently yield a complement factor of 1.0, i.e. no negation."
                ))
            })?;
            let prob_arr = batch.column(prob_idx);
            let fact_keys = lineage_keys
                .map(|names| super::locy_lineage::fact_key_columns(batch, names))
                .transpose()?;
            for (row, key) in keys.into_iter().enumerate() {
                let Some(key) = key else {
                    continue;
                };
                // A NULL probability is no evidence for the negated fact.
                let p =
                    super::locy_lineage::f64_at(prob_arr.as_ref(), row, prob_col)?.unwrap_or(0.0);
                if let (Some(facts), Some(columns)) = (facts.as_mut(), fact_keys.as_deref()) {
                    facts
                        .entry(key.clone())
                        .or_default()
                        .push(super::locy_lineage::NegatedFact {
                            key: super::locy_row_hash::row_hash(columns, row)?,
                            prob: p.clamp(0.0, 1.0),
                        });
                }
                // Noisy-OR combination for duplicate composite keys.
                probs
                    .entry(key)
                    .and_modify(|existing| {
                        *existing = 1.0 - (1.0 - *existing) * (1.0 - p);
                    })
                    .or_insert(p);
            }
        }
    }
    Ok(ProbIndex {
        types,
        probs,
        facts,
    })
}

/// Adds `complement_col_name` to each batch: `1 - p` for a body key the
/// index holds, `1.0` otherwise.
///
/// # Errors
///
/// An unresolvable or uncastable body key column.
pub(crate) fn probe_prob_complement(
    batches: Vec<RecordBatch>,
    index: &ProbIndex,
    join_cols: &[NegationKey],
    complement_col_name: &str,
    lineage_column: Option<&str>,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow_array::Float64Array;

    let mut result = Vec::new();
    for batch in batches {
        let body_keys = match &index.types {
            Some(types) => Some(negation_key_rows(&batch, join_cols, KeySide::Body, types)?),
            None => {
                let indices = resolve_key_indices(&batch, join_cols, KeySide::Body)?;
                verify_key_columns_are_vids(&batch, join_cols, &indices)?;
                None
            }
        };
        let complements: Vec<f64> = match &body_keys {
            Some(keys) => keys
                .iter()
                .map(|key| {
                    let p = key
                        .as_ref()
                        .and_then(|key| index.probs.get(key).copied())
                        .unwrap_or(0.0);
                    1.0 - p
                })
                .collect(),
            None => vec![1.0; batch.num_rows()],
        };

        let complement_arr = Float64Array::from(complements);
        let mut columns: Vec<arrow_array::ArrayRef> = batch.columns().to_vec();
        if let Some(name) = lineage_column {
            let position = batch.schema().index_of(name).map_err(|_| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Locy `IS NOT` lineage column `{name}` is missing from the clause body; \
                     exact inference would read the negation as independent"
                ))
            })?;
            let matched: Vec<&[super::locy_lineage::NegatedFact]> = (0..batch.num_rows())
                .map(|row| {
                    body_keys
                        .as_ref()
                        .and_then(|keys| keys[row].as_ref())
                        .and_then(|key| index.facts.as_ref()?.get(key))
                        .map_or(&[][..], Vec::as_slice)
                })
                .collect();
            columns[position] = super::locy_lineage::encode_negated_facts(&matched);
        }
        columns.push(Arc::new(complement_arr));

        let mut fields: Vec<Arc<arrow_schema::Field>> =
            batch.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(arrow_schema::Field::new(
            complement_col_name,
            arrow_schema::DataType::Float64,
            true,
        )));

        let new_schema = Arc::new(arrow_schema::Schema::new(fields));
        let new_batch = RecordBatch::try_new(new_schema, columns).map_err(arrow_err)?;
        result.push(new_batch);
    }
    Ok(result)
}

/// Probabilistic complement for composite (multi-column) join keys.
///
/// Builds a composite key from all `join_cols` fact columns in `neg_facts`,
/// maps each composite key to a probability via noisy-OR combination, then
/// adds a single `complement_col_name` column with `1 - p` for matched keys
/// and `1.0` for absent keys.
///
/// # Errors
///
/// As `build_prob_index` and `probe_prob_complement`.
pub fn apply_prob_complement_composite(
    batches: Vec<RecordBatch>,
    neg_facts: &[RecordBatch],
    join_cols: &[NegationKey],
    prob_col: &str,
    complement_col_name: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let index = build_prob_index(neg_facts, join_cols, prob_col, None)?;
    probe_prob_complement(batches, &index, join_cols, complement_col_name, None)
}

/// The composite keys of a boolean `IS NOT`'s negated facts.
///
/// Built by `build_banned_index`, probed by `probe_anti_join`.
#[derive(Debug)]
pub(crate) struct BannedIndex {
    /// The negated facts' key column types; `None` when there are no facts.
    types: Option<Vec<arrow_schema::DataType>>,
    /// Every composite key some negated fact carries.
    banned: HashSet<Box<[u8]>>,
}

/// Collects the composite keys of `neg_facts` over `join_cols`.
///
/// # Errors
///
/// An unresolvable key column in the negated facts (never a silently empty
/// set).
pub(crate) fn build_banned_index(
    neg_facts: &[RecordBatch],
    join_cols: &[NegationKey],
) -> datafusion::error::Result<BannedIndex> {
    let Some(first) = neg_facts.first() else {
        return Ok(BannedIndex {
            types: None,
            banned: HashSet::new(),
        });
    };
    let types = negation_key_types(first, join_cols)?;

    // Collect composite keys from the negated rule's derived facts.
    let mut banned: HashSet<Box<[u8]>> = HashSet::new();
    for batch in neg_facts {
        banned.extend(
            negation_key_rows(batch, join_cols, KeySide::NegatedFacts, &types)?
                .into_iter()
                .flatten(),
        );
    }
    Ok(BannedIndex {
        types: Some(types),
        banned,
    })
}

/// Keeps the rows of `batches` whose composite body key `index` does not hold.
///
/// # Errors
///
/// An unresolvable or uncastable body key column.
pub(crate) fn probe_anti_join(
    batches: Vec<RecordBatch>,
    index: &BannedIndex,
    join_cols: &[NegationKey],
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow::compute::filter_record_batch;
    use arrow_array::BooleanArray;

    let Some(types) = &index.types else {
        return Ok(batches);
    };
    // Returning every row is correct here *only* because the build can no
    // longer skip a batch: it errors rather than `continue`-ing on an
    // unresolvable column. Previously a schema mismatch emptied `banned`, and
    // this fast path then laundered "we gave up" into "nothing was excluded".
    // Keep that coupling in mind before reintroducing any skip in the build.
    if index.banned.is_empty() {
        return Ok(batches);
    }

    // Filter body batches: keep rows where composite left key NOT IN banned.
    let mut result = Vec::new();
    for batch in batches {
        let keep: Vec<bool> = negation_key_rows(&batch, join_cols, KeySide::Body, types)?
            .into_iter()
            // A null key is never banned.
            .map(|key| key.is_none_or(|key| !index.banned.contains(&key)))
            .collect();
        let keep_arr = BooleanArray::from(keep);
        let filtered = filter_record_batch(&batch, &keep_arr).map_err(arrow_err)?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }
    Ok(result)
}

/// Boolean anti-join for composite (multi-column) join keys.
///
/// Builds the set of composite keys of `neg_facts` from the `join_cols` fact
/// columns, then filters `batches` to keep only rows whose composite body key
/// is NOT in the set. Node components compare by vid, every other component
/// by value.
///
/// # Errors
///
/// As `build_banned_index` and `probe_anti_join`.
pub fn apply_anti_join_composite(
    batches: Vec<RecordBatch>,
    neg_facts: &[RecordBatch],
    join_cols: &[NegationKey],
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let index = build_banned_index(neg_facts, join_cols)?;
    probe_anti_join(batches, &index, join_cols)
}

/// One built negation index: the anti-join's banned keys or the complement's
/// probabilities.
#[derive(Debug)]
enum NegationIndex {
    Banned(BannedIndex),
    Prob(ProbIndex),
}

/// What a [`NegationIndex`] was built from: the negated scan, the fact-side
/// key columns, the PROB column for a complement, and the negated rule's KEY
/// columns when the complement records lineage.
type NegationIndexKey = (usize, Vec<String>, Option<String>, Option<Vec<String>>);

/// Negation indexes reused across the iterations of one fixpoint (R5).
///
/// A negated reference always targets a lower stratum (cyclic negation is a
/// compile error), so its facts are fixed while a stratum iterates, and
/// rebuilding the banned set or probability map per clause per iteration
/// cost O(|negated facts|) every iteration regardless of the delta. Each
/// entry keeps the batches it was built from, and is reused only while the
/// scan still holds those very batches (compared by column `Arc` identity —
/// the retained clones keep the addresses from being reused), so a rewritten
/// handle is rebuilt rather than served stale.
#[derive(Debug, Default)]
pub(crate) struct NegationIndexCache {
    entries: HashMap<NegationIndexKey, (Vec<RecordBatch>, NegationIndex)>,
}

/// Whether `a` and `b` are the same batches, by column `Arc` identity.
fn same_batches(a: &[RecordBatch], b: &[RecordBatch]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.num_rows() == y.num_rows()
                && x.num_columns() == y.num_columns()
                && x.columns()
                    .iter()
                    .zip(y.columns())
                    .all(|(c, d)| Arc::ptr_eq(c, d))
        })
}

impl NegationIndexCache {
    /// The index for `key` over `neg_facts`, building it when absent or
    /// built from other batches. Returns whether it was built.
    fn get_or_build(
        &mut self,
        key: NegationIndexKey,
        neg_facts: &[RecordBatch],
        build: impl FnOnce() -> datafusion::error::Result<NegationIndex>,
    ) -> datafusion::error::Result<(&NegationIndex, bool)> {
        let fresh = match self.entries.get(&key) {
            Some((source, _)) => !same_batches(source, neg_facts),
            None => true,
        };
        if fresh {
            let index = build()?;
            self.entries
                .insert(key.clone(), (neg_facts.to_vec(), index));
        }
        let (_, index) = self.entries.get(&key).expect("inserted above");
        Ok((index, fresh))
    }
}

/// Multiply `__prob_complement_*` columns into the rule's PROB column and clean up.
///
/// After IS NOT probabilistic complement semantics have added `__prob_complement_*`
/// columns to clause results, this function:
/// 1. Computes the product of all complement factor columns
/// 2. Multiplies the product into the existing PROB column (if any)
/// 3. Removes the internal `__prob_complement_*` columns from the output
///
/// If the rule has no PROB column, complement columns are simply removed
/// (the complement information is discarded and IS NOT acts as a keep-all).
///
/// **That keep-all is a known fail-open hole, deliberately left in place.**
/// Unlike the resolution failures in this module — which now error — it is a
/// semantics question (should a probabilistic `IS NOT` on a non-probabilistic
/// rule fall back to a boolean anti-join, or be rejected at compile time?)
/// rather than a missing check, so it needs a decision rather than a guard. It
/// is unreachable from the batch path, which gates on
/// `binding.target_has_prob && rule.prob_column_name.is_some()`, but reachable
/// via the SLG path.
pub fn multiply_prob_factors(
    batches: Vec<RecordBatch>,
    prob_col: Option<&str>,
    complement_cols: &[String],
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow_array::{Array as _, Float64Array};

    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        if batch.num_rows() == 0 {
            // Remove complement columns from empty batches
            let keep: Vec<usize> = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| !complement_cols.contains(f.name()))
                .map(|(i, _)| i)
                .collect();
            let fields: Vec<_> = keep
                .iter()
                .map(|&i| batch.schema().field(i).clone())
                .collect();
            let cols: Vec<_> = keep.iter().map(|&i| batch.column(i).clone()).collect();
            let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
            result.push(
                RecordBatch::try_new(schema, cols).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?,
            );
            continue;
        }

        let num_rows = batch.num_rows();

        // 1. Compute product of all complement factors
        let mut combined = vec![1.0f64; num_rows];
        for col_name in complement_cols {
            // A missing column used to be skipped, leaving the factor at 1.0 —
            // i.e. the negation silently contributing nothing. `complement_cols`
            // is derived by scanning these batches' own schema at the call site,
            // so a miss is impossible by construction and must be loud.
            let idx = batch.schema().index_of(col_name).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Locy `IS NOT` complement column `{col_name}` is missing from the \
                     batch it was collected from; a factor of 1.0 would silently \
                     discard the negation."
                ))
            })?;
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Expected Float64 for complement column {col_name}"
                    ))
                })?;
            for (i, val) in combined.iter_mut().enumerate().take(num_rows) {
                if !arr.is_null(i) {
                    *val *= arr.value(i);
                }
            }
        }

        // 2. If there's a PROB column, multiply combined into it
        let final_prob: Vec<f64> = if let Some(prob_name) = prob_col {
            // A declared-but-absent PROB column used to fall through to
            // `combined`, silently discarding the complement. `prob_col` comes
            // from the rule's YIELD schema, so its absence means the projection
            // dropped a declared column — a bug, not a tolerable state.
            let idx = batch.schema().index_of(prob_name).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Locy rule declares PROB column `{prob_name}` but it is absent from \
                     the projected batch; the `IS NOT` complement would be silently \
                     discarded."
                ))
            })?;
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Expected Float64 for PROB column {prob_name}"
                    ))
                })?;
            (0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        combined[i]
                    } else {
                        arr.value(i) * combined[i]
                    }
                })
                .collect()
        } else {
            // No PROB column on the rule: the documented keep-all noted above.
            combined
        };

        let new_prob_array: arrow_array::ArrayRef =
            std::sync::Arc::new(Float64Array::from(final_prob));

        // 3. Build output: replace PROB column, remove complement columns
        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for (idx, field) in batch.schema().fields().iter().enumerate() {
            if complement_cols.contains(field.name()) {
                continue;
            }
            if prob_col.is_some_and(|p| field.name() == p) {
                fields.push(field.clone());
                columns.push(new_prob_array.clone());
            } else {
                fields.push(field.clone());
                columns.push(batch.column(idx).clone());
            }
        }

        let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
        result.push(RecordBatch::try_new(schema, columns).map_err(arrow_err)?);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

/// Applies a clause's negated `IS NOT` references to its body rows.
///
/// For each negated binding with a non-empty negated relation: a probabilistic
/// complement column when both sides carry PROB (`prob_column` is the
/// consuming rule's PROB column), the boolean anti-join otherwise. The
/// recursive fixpoint and the non-recursive stratum loop both run exactly
/// this, so it lives in one place.
///
/// When `ops` is `Some` (the `profile()` path), each application appends a
/// synthetic operator — `LocyIsNotAntiJoin` or `LocyIsNotComplement` — with the
/// rows it emitted and its wall time. Neither step is a physical-plan node, so
/// without these entries the profile tree could not show them at all.
///
/// # Errors
///
/// Propagates the anti-join's and the complement's errors (an unresolvable
/// negation key column fails rather than passing rows through).
pub(crate) fn apply_negated_is_refs(
    mut batches: Vec<RecordBatch>,
    bindings: &[super::locy_fixpoint::IsRefBinding],
    registry: &super::locy_fixpoint::DerivedScanRegistry,
    prob_column: Option<&str>,
    mut cache: Option<&mut NegationIndexCache>,
    mut ops: Option<&mut Vec<crate::query::executor::core::OperatorStats>>,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    for (position, binding) in bindings.iter().enumerate() {
        if !binding.negated || binding.anti_join_cols.is_empty() {
            continue;
        }
        let Some(entry) = registry.get(binding.derived_scan_index) else {
            continue;
        };
        let neg_facts = entry.data.read().clone();
        if neg_facts.is_empty() {
            continue;
        }
        let started = std::time::Instant::now();
        let complement_into = match &binding.target_prob_col {
            Some(prob_col) if binding.target_has_prob && prob_column.is_some() => Some(prob_col),
            // `target_has_prob` without a PROB column on either side falls
            // back to the boolean anti-join.
            _ => None,
        };
        // Only a complement records which facts it read.
        let lineage = complement_into.and(binding.negated_lineage.as_ref());
        let key: NegationIndexKey = (
            binding.derived_scan_index,
            binding
                .anti_join_cols
                .iter()
                .map(|k| k.fact_column.clone())
                .collect(),
            complement_into.cloned(),
            lineage.map(|l| l.target_key_columns.clone()),
        );
        let build = || -> datafusion::error::Result<NegationIndex> {
            Ok(match complement_into {
                Some(prob_col) => NegationIndex::Prob(build_prob_index(
                    &neg_facts,
                    &binding.anti_join_cols,
                    prob_col,
                    lineage.map(|l| l.target_key_columns.as_slice()),
                )?),
                None => {
                    NegationIndex::Banned(build_banned_index(&neg_facts, &binding.anti_join_cols)?)
                }
            })
        };
        let owned;
        let (index, built) = match cache.as_deref_mut() {
            Some(cache) => cache.get_or_build(key, &neg_facts, build)?,
            None => {
                owned = build()?;
                (&owned, true)
            }
        };
        if built && let Some(ops) = ops.as_deref_mut() {
            // The build reads every negated fact; reported apart from the
            // probe so a profile shows whether it ran once or per iteration.
            ops.push(super::locy_profile::synthetic_operator(
                "LocyIsNotBuild",
                neg_facts.iter().map(RecordBatch::num_rows).sum(),
                started.elapsed(),
            ));
        }
        let probe_started = std::time::Instant::now();
        let name = match index {
            NegationIndex::Prob(index) => {
                // One column per reference: two negations of one rule in a
                // clause each multiply their own factor. Named by the rule
                // alone, the second column shadowed the first, which was then
                // multiplied in twice and the second never.
                let complement_col = format!("__prob_complement_{position}_{}", binding.rule_name);
                batches = probe_prob_complement(
                    batches,
                    index,
                    &binding.anti_join_cols,
                    &complement_col,
                    lineage.map(|l| l.column.as_str()),
                )?;
                "LocyIsNotComplement"
            }
            NegationIndex::Banned(index) => {
                batches = probe_anti_join(batches, index, &binding.anti_join_cols)?;
                "LocyIsNotAntiJoin"
            }
        };
        if let Some(ops) = ops.as_deref_mut() {
            let rows = batches.iter().map(RecordBatch::num_rows).sum();
            ops.push(super::locy_profile::synthetic_operator(
                name,
                rows,
                probe_started.elapsed(),
            ));
        }
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};

    fn node_key(column: &str) -> NegationKey {
        NegationKey {
            variable: column.to_string(),
            body_column: column.to_string(),
            fact_column: column.to_string(),
            kind: ColumnKind::Node,
        }
    }

    fn make_vid_prob_batch(vids: &[u64], probs: &[f64]) -> RecordBatch {
        use arrow_array::UInt64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("vid", DataType::UInt64, true),
            Field::new("prob", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vids.to_vec())),
                Arc::new(Float64Array::from(probs.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_prob_complement_basic() {
        // neg has VID=1 with prob=0.7 → complement=0.3; VID=2 absent → complement=1.0
        let body = make_vid_prob_batch(&[1, 2], &[0.9, 0.8]);
        let neg = make_vid_prob_batch(&[1], &[0.7]);
        let join_cols = vec![node_key("vid")];
        let result = apply_prob_complement_composite(
            vec![body],
            &[neg],
            &join_cols,
            "prob",
            "__complement_0",
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // VID=1: complement = 1 - 0.7 = 0.3
        assert!(
            (complement.value(0) - 0.3).abs() < 1e-10,
            "expected 0.3, got {}",
            complement.value(0)
        );
        // VID=2: absent from neg → complement = 1.0
        assert!(
            (complement.value(1) - 1.0).abs() < 1e-10,
            "expected 1.0, got {}",
            complement.value(1)
        );
    }

    #[test]
    fn test_prob_complement_noisy_or_duplicates() {
        // neg has VID=1 twice with prob=0.3 and prob=0.5
        // Combined via noisy-OR: 1-(1-0.3)(1-0.5) = 0.65
        // Complement = 1 - 0.65 = 0.35
        let body = make_vid_prob_batch(&[1], &[0.9]);
        let neg = make_vid_prob_batch(&[1, 1], &[0.3, 0.5]);
        let join_cols = vec![node_key("vid")];
        let result = apply_prob_complement_composite(
            vec![body],
            &[neg],
            &join_cols,
            "prob",
            "__complement_0",
        )
        .unwrap();
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (complement.value(0) - 0.35).abs() < 1e-10,
            "expected 0.35, got {}",
            complement.value(0)
        );
    }

    #[test]
    fn test_prob_complement_empty_neg() {
        // Empty neg_facts → body passes through with complement=1.0
        let body = make_vid_prob_batch(&[1, 2], &[0.5, 0.6]);
        let join_cols = vec![node_key("vid")];
        let result =
            apply_prob_complement_composite(vec![body], &[], &join_cols, "prob", "__complement_0")
                .unwrap();
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..2 {
            assert!(
                (complement.value(i) - 1.0).abs() < 1e-10,
                "row {}: expected 1.0, got {}",
                i,
                complement.value(i)
            );
        }
    }

    #[test]
    fn test_anti_join_basic() {
        // body [1,2,3], neg [2] → result [1,3]
        use arrow_array::UInt64Array;
        let body = make_vid_prob_batch(&[1, 2, 3], &[0.5, 0.6, 0.7]);
        let neg = make_vid_prob_batch(&[2], &[0.0]);
        let join_cols = vec![node_key("vid")];
        let result = apply_anti_join_composite(vec![body], &[neg], &join_cols).unwrap();
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        let vids = batch
            .column_by_name("vid")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(vids.value(0), 1);
        assert_eq!(vids.value(1), 3);
    }

    #[test]
    fn test_anti_join_empty_neg() {
        // Empty neg → all rows kept
        let body = make_vid_prob_batch(&[1, 2, 3], &[0.5, 0.6, 0.7]);
        let join_cols = vec![node_key("vid")];
        let result = apply_anti_join_composite(vec![body], &[], &join_cols).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_anti_join_all_excluded() {
        // neg covers all body rows → empty result
        let body = make_vid_prob_batch(&[1, 2], &[0.5, 0.6]);
        let neg = make_vid_prob_batch(&[1, 2], &[0.0, 0.0]);
        let join_cols = vec![node_key("vid")];
        let result = apply_anti_join_composite(vec![body], &[neg], &join_cols).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_multiply_prob_single_complement() {
        // prob=0.8, complement=0.5 → output prob=0.4; complement col removed
        let body = make_vid_prob_batch(&[1], &[0.8]);
        // Add a complement column
        let complement_arr = Float64Array::from(vec![0.5]);
        let mut cols: Vec<arrow_array::ArrayRef> = body.columns().to_vec();
        cols.push(Arc::new(complement_arr));
        let mut fields: Vec<Arc<Field>> = body.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(
            "__complement_0",
            DataType::Float64,
            true,
        )));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let result =
            multiply_prob_factors(vec![batch], Some("prob"), &["__complement_0".to_string()])
                .unwrap();
        assert_eq!(result.len(), 1);
        let out = &result[0];
        // Complement column should be removed
        assert!(out.column_by_name("__complement_0").is_none());
        let prob = out
            .column_by_name("prob")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (prob.value(0) - 0.4).abs() < 1e-10,
            "expected 0.4, got {}",
            prob.value(0)
        );
    }

    #[test]
    fn test_multiply_prob_multiple_complements() {
        // prob=0.8, c1=0.5, c2=0.6 → 0.8×0.5×0.6=0.24
        let body = make_vid_prob_batch(&[1], &[0.8]);
        let c1 = Float64Array::from(vec![0.5]);
        let c2 = Float64Array::from(vec![0.6]);
        let mut cols: Vec<arrow_array::ArrayRef> = body.columns().to_vec();
        cols.push(Arc::new(c1));
        cols.push(Arc::new(c2));
        let mut fields: Vec<Arc<Field>> = body.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("__c1", DataType::Float64, true)));
        fields.push(Arc::new(Field::new("__c2", DataType::Float64, true)));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let result = multiply_prob_factors(
            vec![batch],
            Some("prob"),
            &["__c1".to_string(), "__c2".to_string()],
        )
        .unwrap();
        let out = &result[0];
        assert!(out.column_by_name("__c1").is_none());
        assert!(out.column_by_name("__c2").is_none());
        let prob = out
            .column_by_name("prob")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (prob.value(0) - 0.24).abs() < 1e-10,
            "expected 0.24, got {}",
            prob.value(0)
        );
    }

    #[test]
    fn test_multiply_prob_no_prob_column() {
        // No prob column → combined complements become the output
        use arrow_array::UInt64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("vid", DataType::UInt64, true),
            Field::new("__c1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(Float64Array::from(vec![0.7])),
            ],
        )
        .unwrap();

        let result = multiply_prob_factors(vec![batch], None, &["__c1".to_string()]).unwrap();
        let out = &result[0];
        // __c1 should be removed since it's a complement column
        assert!(out.column_by_name("__c1").is_none());
        // Only vid column remains
        assert_eq!(out.num_columns(), 1);
    }
}
