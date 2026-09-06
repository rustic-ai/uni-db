//! Built-in Locy aggregate registrations.
//!
//! Implements every built-in Locy fold aggregate as a [`LocyAggregate`]
//! trait object. Each aggregate carries its [`Semilattice`] metadata so
//! the fixpoint engine's monotonicity proofs are explicit.
//!
//! These impls are also the **runtime executor** for non-recursive `FOLD`:
//! `uni_query`'s `FoldExec` dispatches each key group through
//! [`LocyAggState::ingest_indices`] + [`LocyAggState::finalize`]. The bodies
//! below are therefore the single source of truth for fold math and are kept
//! byte-identical to the pre-trait executor (noisy-OR complement-product,
//! bounded-product log-space underflow, `MIN`/`MAX` input-type preservation,
//! `COLLECT` cypher-codec `LargeBinary` encoding).

// Rust guideline compliant

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array};
use arrow_schema::DataType;
use datafusion::scalar::ScalarValue;
use uni_plugin::traits::locy::{
    FoldContext, FoldSemiring, LocyAggState, LocyAggregate, Semilattice,
};
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName};

/// Strict-domain violation error code (probability aggregate input ∉ `[0, 1]`).
const CODE_STRICT_DOMAIN: u32 = 0x501;
/// A non-null cell could not be read as a value the aggregate accepts.
///
/// #233 Tier 1: these cells used to be counted and then skipped, so a column
/// of a numeric type `numeric_at` did not recognise produced a confident
/// wrong answer rather than an error — `SUM`/`AVG` of 0.0, `MNOR` of 0.0 (an
/// assertion of "impossible") and `MPROD` of 1.0 ("certain").
const CODE_UNREADABLE_CELL: u32 = 0x502;

/// Register all built-in Locy aggregates into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a built-in qname is
/// already taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.locy_aggregate(QName::builtin("MIN"), Arc::new(MinAgg))?;
    r.locy_aggregate(QName::builtin("MAX"), Arc::new(MaxAgg))?;
    r.locy_aggregate(QName::builtin("SUM"), Arc::new(SumAgg))?;
    r.locy_aggregate(QName::builtin("MSUM"), Arc::new(MSumAgg))?;
    r.locy_aggregate(QName::builtin("COUNT"), Arc::new(CountAgg))?;
    r.locy_aggregate(QName::builtin("COUNTALL"), Arc::new(CountAllAgg))?;
    r.locy_aggregate(QName::builtin("AVG"), Arc::new(AvgAgg))?;
    r.locy_aggregate(QName::builtin("COLLECT"), Arc::new(CollectAgg))?;
    r.locy_aggregate(QName::builtin("MNOR"), Arc::new(MnorAgg))?;
    r.locy_aggregate(QName::builtin("MPROD"), Arc::new(MprodAgg))?;
    Ok(())
}

// =========================================================================
// Shared helpers
// =========================================================================

/// Convert row `row_idx` of Arrow column `col` to a [`uni_common::Value`].
///
/// Ported verbatim from the pre-trait `FoldExec` executor so `COLLECT`
/// produces byte-identical `cypher_value_codec` payloads. `LargeBinary`
/// cells are decoded back through the codec; unsupported types map to
/// [`uni_common::Value::Null`].
fn cell_value(col: &dyn Array, row_idx: usize) -> Result<uni_common::Value, FnError> {
    if col.is_null(row_idx) {
        return Ok(uni_common::Value::Null);
    }
    Ok(match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            uni_common::Value::Int(arr.value(row_idx))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            uni_common::Value::Float(arr.value(row_idx))
        }
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            uni_common::Value::String(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap();
            uni_common::Value::String(arr.value(row_idx).to_string())
        }
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap();
            uni_common::Value::Bool(arr.value(row_idx))
        }
        DataType::LargeBinary => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()
                .unwrap();
            let bytes = arr.value(row_idx);
            // #233 Tier 1: a failed decode used to become `Value::Null`, so a
            // COLLECT list carried a null in place of a value that exists and
            // could not be read — indistinguishable from a genuine null.
            uni_common::cypher_value_codec::decode(bytes).map_err(|e| {
                FnError::new(
                    CODE_UNREADABLE_CELL,
                    format!("COLLECT: cannot decode a non-null cell: {e}"),
                )
            })?
        }
        // #233 Tier 1: an unsupported type used to collapse to `Value::Null`,
        // silently replacing every value in the column.
        other => {
            return Err(FnError::new(
                CODE_UNREADABLE_CELL,
                format!("COLLECT: cannot read a non-null cell of type {other}"),
            ));
        }
    })
}

fn downcast_state<S: LocyAggState + 'static>(other: &dyn LocyAggState) -> Result<&S, FnError> {
    other.as_any().downcast_ref::<S>().ok_or_else(|| {
        FnError::new(
            CODE_STRICT_DOMAIN,
            "LocyAggState::merge invoked with mismatched concrete state",
        )
    })
}

// =========================================================================
// MIN / MAX — idem ∧ comm ∧ assoc ∧ monotone ∧ has_top; input-type-preserving
// =========================================================================

/// `MIN` aggregate — bounded floor with monotonic-decreasing semantics.
#[derive(Debug)]
pub struct MinAgg;

impl LocyAggregate for MinAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::BOUNDED_MIN_MAX
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn output_type_for_input(&self, input: &DataType) -> DataType {
        input.clone()
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MinMaxState::new(true))
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(f64::INFINITY)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(if val < accum { val } else { accum })
    }
}

/// `MAX` aggregate — bounded ceiling with monotonic-increasing semantics.
#[derive(Debug)]
pub struct MaxAgg;

impl LocyAggregate for MaxAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::BOUNDED_MIN_MAX
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn output_type_for_input(&self, input: &DataType) -> DataType {
        input.clone()
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MinMaxState::new(false))
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(f64::NEG_INFINITY)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(if val > accum { val } else { accum })
    }
}

/// Type-preserving min/max state shared by [`MinAgg`] / [`MaxAgg`].
///
/// `Int64` / `Float64` columns use native `min`/`max` (byte-identical to the
/// pre-trait executor, including its NaN handling). Other column types fall
/// back to [`ScalarValue`] ordering and preserve the input type. `proto`
/// records the input type so an all-null group finalizes to a typed null.
#[derive(Debug)]
struct MinMaxState {
    is_min: bool,
    dtype: Option<DataType>,
    i64_acc: Option<i64>,
    f64_acc: Option<f64>,
    other_acc: Option<ScalarValue>,
    proto: Option<ScalarValue>,
}

impl MinMaxState {
    fn new(is_min: bool) -> Self {
        Self {
            is_min,
            dtype: None,
            i64_acc: None,
            f64_acc: None,
            other_acc: None,
            proto: None,
        }
    }
}

impl LocyAggState for MinMaxState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest_indices(
        &mut self,
        col: &dyn Array,
        indices: &[usize],
        _cx: &FoldContext,
    ) -> Result<(), FnError> {
        if self.dtype.is_none() {
            self.dtype = Some(col.data_type().clone());
        }
        for &i in indices {
            if self.proto.is_none() {
                self.proto = Some(scalar_from_array(col, i)?);
            }
            if col.is_null(i) {
                continue;
            }
            match col.data_type() {
                DataType::Int64 => {
                    let v = col.as_any().downcast_ref::<Int64Array>().unwrap().value(i);
                    self.i64_acc = Some(match self.i64_acc {
                        None => v,
                        Some(cur) if self.is_min => cur.min(v),
                        Some(cur) => cur.max(v),
                    });
                }
                DataType::Float64 => {
                    let v = col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(i);
                    self.f64_acc = Some(match self.f64_acc {
                        None => v,
                        Some(cur) if self.is_min => cur.min(v),
                        Some(cur) => cur.max(v),
                    });
                }
                _ => {
                    let v = scalar_from_array(col, i)?;
                    self.other_acc = Some(match self.other_acc.take() {
                        None => v,
                        Some(cur) => {
                            let keep_v = matches!(
                                (v.partial_cmp(&cur), self.is_min),
                                (Some(Ordering::Less), true) | (Some(Ordering::Greater), false)
                            );
                            if keep_v { v } else { cur }
                        }
                    });
                }
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<MinMaxState>(other)?;
        if self.dtype.is_none() {
            self.dtype = o.dtype.clone();
        }
        if self.proto.is_none() {
            self.proto = o.proto.clone();
        }
        if let Some(v) = o.i64_acc {
            self.i64_acc = Some(match self.i64_acc {
                None => v,
                Some(cur) if self.is_min => cur.min(v),
                Some(cur) => cur.max(v),
            });
        }
        if let Some(v) = o.f64_acc {
            self.f64_acc = Some(match self.f64_acc {
                None => v,
                Some(cur) if self.is_min => cur.min(v),
                Some(cur) => cur.max(v),
            });
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        match self.dtype.as_ref() {
            Some(DataType::Int64) => Ok(ScalarValue::Int64(self.i64_acc)),
            Some(DataType::Float64) => Ok(ScalarValue::Float64(self.f64_acc)),
            Some(_) => match (&self.other_acc, &self.proto) {
                (Some(v), _) => Ok(v.clone()),
                (None, Some(proto)) => Ok(proto.clone()),
                (None, None) => Ok(ScalarValue::Float64(None)),
            },
            None => Ok(ScalarValue::Float64(None)),
        }
    }
}

/// Read row `i` of `col` as a typed [`ScalarValue`] (null-aware).
fn scalar_from_array(col: &dyn Array, i: usize) -> Result<ScalarValue, FnError> {
    ScalarValue::try_from_array(col, i)
        .map_err(|e| FnError::new(FnError::CODE_TYPE_COERCION, e.to_string()))
}

// =========================================================================
// SUM — comm ∧ assoc (monotone over non-negative; rejected in recursive)
// =========================================================================

/// `SUM` aggregate — additive monoid over `f64`.
///
/// Not monotone in general (negative inputs can decrease totals). The
/// compiler should reject recursive use unless inputs are constrained
/// non-negative; the [`Semilattice::monotone_join`] flag is `false`.
#[derive(Debug)]
pub struct SumAgg;

impl LocyAggregate for SumAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::NON_MONOTONE
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(SumState::default())
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(accum + val)
    }
}

/// `MSUM` aggregate — monotone sum-of-non-negatives.
///
/// Identical runtime to [`SumAgg`] but declares
/// [`Semilattice::monotone_join`] = `true` (the caller guarantees
/// non-negative inputs, so the running sum is monotone across fixpoint
/// iterations and sound in recursive Locy strata). `has_top` is `false`.
#[derive(Debug)]
pub struct MSumAgg;

impl LocyAggregate for MSumAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice {
            idempotent: false,
            commutative: true,
            associative: true,
            monotone_join: true,
            has_top: false,
        }
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(SumState::default())
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(accum + val)
    }
}

/// Sum state. `has_value` tracks non-null presence so an empty/all-null
/// group finalizes to `NULL` (matching the executor's `sum_f64` → `None`).
#[derive(Debug, Default)]
struct SumState {
    value: f64,
    has_value: bool,
}

impl LocyAggState for SumState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest_indices(
        &mut self,
        col: &dyn Array,
        indices: &[usize],
        _cx: &FoldContext,
    ) -> Result<(), FnError> {
        for &i in indices {
            if col.is_null(i) {
                continue;
            }
            // #233 Tier 1: `has_value` used to be set before the read, so an
            // unreadable cell made SUM report 0.0 instead of null or an error.
            let Some(v) = numeric_at(col, i) else {
                return Err(unreadable_cell("SUM", col));
            };
            self.has_value = true;
            self.value += v;
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<SumState>(other)?;
        self.value += o.value;
        self.has_value |= o.has_value;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        if self.has_value {
            Ok(ScalarValue::Float64(Some(self.value)))
        } else {
            Ok(ScalarValue::Float64(None))
        }
    }
}

// =========================================================================
// COUNT — non-null count; monotone but unbounded (top = ∞)
// =========================================================================

/// `COUNT` aggregate — counts non-null rows (`count(x)` semantics).
#[derive(Debug)]
pub struct CountAgg;

impl LocyAggregate for CountAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::COUNT
    }
    fn output_type(&self) -> DataType {
        DataType::Int64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(CountState::default())
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(accum + val)
    }
}

/// Counts non-null rows in each group.
#[derive(Debug, Default)]
struct CountState {
    value: i64,
}

impl LocyAggState for CountState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest_indices(
        &mut self,
        col: &dyn Array,
        indices: &[usize],
        _cx: &FoldContext,
    ) -> Result<(), FnError> {
        self.value += indices.iter().filter(|&&i| !col.is_null(i)).count() as i64;
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        self.value += downcast_state::<CountState>(other)?.value;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Int64(Some(self.value)))
    }
}

// =========================================================================
// COUNTALL — counts every row in the group (`count(*)` semantics)
// =========================================================================

/// `COUNTALL` aggregate — counts every row regardless of nullity.
///
/// The zero-argument `COUNT()` / `MCOUNT()` form. Distinct from [`CountAgg`]
/// (which skips nulls) so trait dispatch needs no name-based special case.
#[derive(Debug)]
pub struct CountAllAgg;

impl LocyAggregate for CountAllAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::COUNT
    }
    fn output_type(&self) -> DataType {
        DataType::Int64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(CountAllState::default())
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(accum + val)
    }
}

/// Counts every row in each group, ignoring the (absent) input column.
#[derive(Debug, Default)]
struct CountAllState {
    value: i64,
}

impl LocyAggState for CountAllState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest_indices(
        &mut self,
        _col: &dyn Array,
        indices: &[usize],
        _cx: &FoldContext,
    ) -> Result<(), FnError> {
        self.value += indices.len() as i64;
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        self.value += downcast_state::<CountAllState>(other)?.value;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Int64(Some(self.value)))
    }
}

// =========================================================================
// AVG — comm ∧ assoc (non-monotone)
// =========================================================================

/// `AVG` aggregate — arithmetic mean; non-monotone.
#[derive(Debug)]
pub struct AvgAgg;

impl LocyAggregate for AvgAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::NON_MONOTONE
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(AvgState::default())
    }
}

/// Accumulates non-null count and numeric sum; finalizes `sum / count`.
#[derive(Debug, Default)]
struct AvgState {
    sum: f64,
    count: i64,
}

impl LocyAggState for AvgState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest_indices(
        &mut self,
        col: &dyn Array,
        indices: &[usize],
        _cx: &FoldContext,
    ) -> Result<(), FnError> {
        for &i in indices {
            if col.is_null(i) {
                continue;
            }
            // #233 Tier 1: `count` used to be incremented before the read, so
            // an unreadable cell inflated the divisor without contributing to
            // the sum — dragging the average toward zero silently.
            let Some(v) = numeric_at(col, i) else {
                return Err(unreadable_cell("AVG", col));
            };
            self.count += 1;
            self.sum += v;
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<AvgState>(other)?;
        self.sum += o.sum;
        self.count += o.count;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        if self.count == 0 {
            return Ok(ScalarValue::Float64(None));
        }
        // M-DOCUMENTED-MAGIC: dividing by `count` as f64 is exact for the
        // sub-2^53 range we expect for fixpoint aggregations.
        Ok(ScalarValue::Float64(Some(self.sum / self.count as f64)))
    }
}

// =========================================================================
// COLLECT — comm ∧ assoc ∧ monotone (multiset semilattice)
// =========================================================================

/// `COLLECT` aggregate — assembles values into a list.
///
/// Output is a `cypher_value_codec`-encoded `LargeBinary` list, byte-identical
/// to the pre-trait executor (so downstream `LocyProject` decode is unchanged).
#[derive(Debug)]
pub struct CollectAgg;

impl LocyAggregate for CollectAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice {
            idempotent: false,
            commutative: true,
            associative: true,
            // Monotone under multiset inclusion — adding rows never shrinks.
            monotone_join: true,
            has_top: false,
        }
    }
    fn output_type(&self) -> DataType {
        DataType::LargeBinary
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(CollectState::default())
    }
}

/// Collects non-null cells as typed [`uni_common::Value`]s.
#[derive(Debug, Default)]
struct CollectState {
    values: Vec<uni_common::Value>,
}

impl LocyAggState for CollectState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest_indices(
        &mut self,
        col: &dyn Array,
        indices: &[usize],
        _cx: &FoldContext,
    ) -> Result<(), FnError> {
        for &i in indices {
            if col.is_null(i) {
                continue;
            }
            self.values.push(cell_value(col, i)?);
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<CollectState>(other)?;
        self.values.extend_from_slice(&o.values);
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        let encoded =
            uni_common::cypher_value_codec::encode(&uni_common::Value::List(self.values.clone()));
        Ok(ScalarValue::LargeBinary(Some(encoded)))
    }
}

// =========================================================================
// MNOR — noisy-OR (1 − ∏(1 − p_i)); idem ∧ comm ∧ assoc ∧ monotone, top=1
// =========================================================================

/// `MNOR` (noisy-OR) aggregate.
///
/// `1 − ∏(1 − pᵢ)`: combines independent probabilistic evidence into a
/// monotone-increasing cumulative belief bounded by 1.0.
#[derive(Debug)]
pub struct MnorAgg;

impl LocyAggregate for MnorAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice {
            idempotent: true,
            commutative: true,
            associative: true,
            monotone_join: true,
            has_top: true,
        }
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MnorState::default())
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, strict: bool) -> Result<f64, FnError> {
        let p = check_domain(val, strict, "MNOR")?;
        // 1 − (1 − accum)·(1 − p) = accum + p − accum·p
        Ok(1.0 - (1.0 - accum) * (1.0 - p))
    }
    fn is_probability_aggregate(&self) -> bool {
        true
    }
    fn is_noisy_or(&self) -> bool {
        true
    }
}

/// Noisy-OR / max-disjunction state.
///
/// `AddMult` accumulates the complement product `∏(1 − pᵢ)` (byte-identical
/// to the executor's `noisy_or_f64`); `MaxMin` tracks `max(pᵢ)`.
#[derive(Debug)]
struct MnorState {
    complement_product: f64,
    max: f64,
    has_value: bool,
    semiring: FoldSemiring,
}

impl Default for MnorState {
    fn default() -> Self {
        Self {
            complement_product: 1.0,
            max: 0.0,
            has_value: false,
            semiring: FoldSemiring::AddMult,
        }
    }
}

impl LocyAggState for MnorState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest_indices(
        &mut self,
        col: &dyn Array,
        indices: &[usize],
        cx: &FoldContext,
    ) -> Result<(), FnError> {
        self.semiring = cx.semiring;
        for &i in indices {
            if col.is_null(i) {
                continue;
            }
            // #233 Tier 1: skipping an unreadable cell left the identity
            // element in place, so MNOR reported a CONFIDENT probability
            // (0.0 = impossible, 1.0 = certain) built from rows it never read.
            let Some(raw) = numeric_at(col, i) else {
                return Err(unreadable_cell("MNOR", col));
            };
            self.has_value = true;
            let p = check_domain(raw, cx.strict, "MNOR")?;
            match cx.semiring {
                FoldSemiring::AddMult => self.complement_product *= 1.0 - p,
                FoldSemiring::MaxMin => self.max = self.max.max(p),
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<MnorState>(other)?;
        self.complement_product *= o.complement_product;
        self.max = self.max.max(o.max);
        self.has_value |= o.has_value;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        if !self.has_value {
            return Ok(ScalarValue::Float64(None));
        }
        let v = match self.semiring {
            FoldSemiring::AddMult => 1.0 - self.complement_product,
            FoldSemiring::MaxMin => self.max,
        };
        Ok(ScalarValue::Float64(Some(v)))
    }
    fn is_at_top(&self) -> bool {
        // M-DOCUMENTED-MAGIC: 1e-12 tolerates f64 drift across long fixpoints
        // without short-circuiting prematurely.
        match self.semiring {
            FoldSemiring::AddMult => self.complement_product <= 1e-12,
            FoldSemiring::MaxMin => self.max >= 1.0 - 1e-12,
        }
    }
}

// =========================================================================
// MPROD — bounded product (∏ p_i); idem ∧ comm ∧ assoc ∧ monotone-DOWN, top=0
// =========================================================================

/// `MPROD` (bounded product) aggregate.
#[derive(Debug)]
pub struct MprodAgg;

impl LocyAggregate for MprodAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice {
            idempotent: true,
            commutative: true,
            associative: true,
            monotone_join: true,
            has_top: true,
        }
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MprodState::default())
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(1.0)
    }
    fn update_step(&self, accum: f64, val: f64, strict: bool) -> Result<f64, FnError> {
        let p = check_domain(val, strict, "MPROD")?;
        Ok(accum * p)
    }
    fn is_probability_aggregate(&self) -> bool {
        true
    }
    // is_noisy_or stays false (default) — MPROD is bounded-product, not noisy-OR.
}

/// Bounded-product / min-conjunction state.
///
/// `AddMult` replicates `product_f64` exactly: log-space switch once the
/// running product drops below `epsilon`, plus an early-out on the first
/// zero (which also stops scanning the rest of the group). `MaxMin` tracks
/// `min(pᵢ)`.
#[derive(Debug)]
struct MprodState {
    product: f64,
    log_sum: f64,
    use_log: bool,
    zero: bool,
    min: f64,
    has_value: bool,
    semiring: FoldSemiring,
}

impl Default for MprodState {
    fn default() -> Self {
        Self {
            product: 1.0,
            log_sum: 0.0,
            use_log: false,
            zero: false,
            min: 1.0,
            has_value: false,
            semiring: FoldSemiring::AddMult,
        }
    }
}

impl LocyAggState for MprodState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest_indices(
        &mut self,
        col: &dyn Array,
        indices: &[usize],
        cx: &FoldContext,
    ) -> Result<(), FnError> {
        self.semiring = cx.semiring;
        for &i in indices {
            if col.is_null(i) {
                continue;
            }
            // #233 Tier 1: skipping an unreadable cell left the identity
            // element in place, so MPROD reported a CONFIDENT probability
            // (0.0 = impossible, 1.0 = certain) built from rows it never read.
            let Some(raw) = numeric_at(col, i) else {
                return Err(unreadable_cell("MPROD", col));
            };
            self.has_value = true;
            let p = check_domain(raw, cx.strict, "MPROD")?;
            match cx.semiring {
                FoldSemiring::AddMult => {
                    // Byte-identical to `product_f64`: a zero short-circuits
                    // the whole group (later rows are not scanned).
                    if p == 0.0 {
                        self.zero = true;
                        break;
                    }
                    if self.use_log {
                        self.log_sum += p.ln();
                    } else {
                        self.product *= p;
                        if self.product < cx.epsilon {
                            self.log_sum = self.product.ln();
                            self.use_log = true;
                        }
                    }
                }
                FoldSemiring::MaxMin => self.min = self.min.min(p),
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<MprodState>(other)?;
        self.has_value |= o.has_value;
        self.zero |= o.zero;
        self.min = self.min.min(o.min);
        // Each state's product value is `log_sum.exp()` once it has switched to
        // log space, else `product`. The old code multiplied BOTH factors for a
        // log-space `other` (`o.product * o.log_sum.exp()`), double-counting the
        // pre-switch product that `log_sum` already contains (at the switch
        // `log_sum = ln(product)`, plus later `ln` terms). Fold `other`'s single
        // value into whichever representation self is currently using.
        let other_product = if o.use_log {
            o.log_sum.exp()
        } else {
            o.product
        };
        if self.use_log {
            self.log_sum += other_product.ln();
        } else {
            self.product *= other_product;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        if !self.has_value {
            return Ok(ScalarValue::Float64(None));
        }
        let v = match self.semiring {
            FoldSemiring::AddMult => {
                if self.zero {
                    0.0
                } else if self.use_log {
                    self.log_sum.exp()
                } else {
                    self.product
                }
            }
            FoldSemiring::MaxMin => self.min,
        };
        Ok(ScalarValue::Float64(Some(v)))
    }
    fn is_at_top(&self) -> bool {
        match self.semiring {
            FoldSemiring::AddMult => self.zero || (!self.use_log && self.product <= 1e-12),
            FoldSemiring::MaxMin => self.min <= 1e-12,
        }
    }
}

// =========================================================================
// Probability-domain helpers
// =========================================================================

/// Read row `i` as `f64` (widening `Int64`); `None` for null / non-numeric.
fn numeric_at(col: &dyn Array, i: usize) -> Option<f64> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    };

    // #233 Tier 1: this recognised only Float64 and Int64, so an Int32 or
    // UInt64 column — perfectly valid numeric input — yielded `None` and the
    // callers counted the row while contributing nothing to the accumulator.
    match col.data_type() {
        DataType::Float64 => Some(col.as_primitive::<Float64Type>().value(i)),
        DataType::Float32 => Some(f64::from(col.as_primitive::<Float32Type>().value(i))),
        DataType::Int64 => Some(col.as_primitive::<Int64Type>().value(i) as f64),
        DataType::Int32 => Some(f64::from(col.as_primitive::<Int32Type>().value(i))),
        DataType::Int16 => Some(f64::from(col.as_primitive::<Int16Type>().value(i))),
        DataType::Int8 => Some(f64::from(col.as_primitive::<Int8Type>().value(i))),
        DataType::UInt64 => Some(col.as_primitive::<UInt64Type>().value(i) as f64),
        DataType::UInt32 => Some(f64::from(col.as_primitive::<UInt32Type>().value(i))),
        DataType::UInt16 => Some(f64::from(col.as_primitive::<UInt16Type>().value(i))),
        DataType::UInt8 => Some(f64::from(col.as_primitive::<UInt8Type>().value(i))),
        _ => None,
    }
}

/// The error raised when a non-null cell cannot be read by an aggregate.
fn unreadable_cell(agg: &str, col: &dyn Array) -> FnError {
    FnError::new(
        CODE_UNREADABLE_CELL,
        format!(
            "{agg}: cannot read a non-null cell of type {} as a number",
            col.data_type()
        ),
    )
}

/// Validate a probability-domain input and return the clamped value.
///
/// In strict mode an input outside `[0, 1]` is an error; otherwise it is
/// clamped to `[0, 1]` with a warning. `agg` names the aggregate for the
/// message (`"MNOR"` / `"MPROD"`), matching the pre-trait executor text.
///
/// # Errors
///
/// Returns [`FnError`] (code `0x501`) in strict mode for out-of-domain input.
fn check_domain(val: f64, strict: bool, agg: &str) -> Result<f64, FnError> {
    if !(0.0..=1.0).contains(&val) {
        if strict {
            return Err(FnError::new(
                CODE_STRICT_DOMAIN,
                format!("strict_probability_domain: {agg} input {val} is outside [0, 1]"),
            ));
        }
        tracing::warn!(
            "{agg} input {val} outside [0,1], clamped to {}",
            val.clamp(0.0, 1.0)
        );
    }
    Ok(val.clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use arrow_schema::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    fn one_col_batch(values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let arr = Arc::new(Float64Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    fn int32_batch(values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let arr = Arc::new(arrow_array::Int32Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    /// An Int32 column must aggregate, not silently contribute nothing.
    ///
    /// #233 Tier 1. `numeric_at` recognised only Float64 and Int64, and each
    /// aggregate marked the row as seen BEFORE attempting the read, so an
    /// Int32 column produced a confident wrong answer instead of an error:
    /// SUM 0.0, AVG 0.0, MNOR 0.0 (asserting "impossible") and MPROD 1.0
    /// (asserting "certain") — over rows none of which were ever read.
    #[test]
    fn an_int32_column_is_aggregated_rather_than_counted_and_skipped() {
        let batch = int32_batch(vec![Some(1), None, Some(2), Some(3)]);

        let mut sum = SumAgg.create();
        sum.ingest(&batch, 0).expect("SUM ingests Int32");
        match sum.finalize().expect("finalize") {
            ScalarValue::Float64(Some(v)) => assert_eq!(
                v, 6.0,
                "SUM over Int32 must be 6.0; 0.0 is the value the skipped-cell path produced"
            ),
            other => panic!("expected Float64(Some), got {other:?}"),
        }

        let mut avg = AvgAgg.create();
        avg.ingest(&batch, 0).expect("AVG ingests Int32");
        match avg.finalize().expect("finalize") {
            ScalarValue::Float64(Some(v)) => assert!(
                (v - 2.0).abs() < f64::EPSILON,
                "AVG over Int32 must be 2.0, got {v}; counting unreadable cells drags it to 0"
            ),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    /// A cell no aggregate can read is an error, not a confident probability.
    #[test]
    fn an_unreadable_cell_is_an_error_not_a_confident_probability() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let arr = Arc::new(arrow_array::StringArray::from(vec![Some("nope")]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let mut sum = SumAgg.create();
        let err = sum
            .ingest(&batch, 0)
            .expect_err("a non-numeric non-null cell must not be silently skipped");
        assert_eq!(err.code, CODE_UNREADABLE_CELL);
    }

    #[test]
    fn min_finds_smallest_skipping_nulls() {
        let agg = MinAgg;
        let mut s = agg.create();
        s.ingest(
            &one_col_batch(vec![Some(3.0), None, Some(1.0), Some(2.0)]),
            0,
        )
        .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert_eq!(v, 1.0),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    #[test]
    fn max_finds_largest_skipping_nulls() {
        let agg = MaxAgg;
        let mut s = agg.create();
        s.ingest(
            &one_col_batch(vec![Some(3.0), None, Some(1.0), Some(2.0)]),
            0,
        )
        .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert_eq!(v, 3.0),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    #[test]
    fn min_preserves_int64_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let arr = Arc::new(Int64Array::from(vec![Some(3), None, Some(1), Some(2)]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let mut s = MinAgg.create();
        s.ingest(&batch, 0).unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Int64(Some(v)) => assert_eq!(v, 1),
            other => panic!("expected Int64(Some), got {other:?}"),
        }
        assert_eq!(
            MinAgg.output_type_for_input(&DataType::Int64),
            DataType::Int64
        );
    }

    #[test]
    fn sum_adds_skipping_nulls() {
        let agg = SumAgg;
        let mut s = agg.create();
        s.ingest(
            &one_col_batch(vec![Some(1.0), None, Some(2.5), Some(0.5)]),
            0,
        )
        .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert_eq!(v, 4.0),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    #[test]
    fn sum_empty_yields_null() {
        let s = SumAgg.create();
        match s.finalize().unwrap() {
            ScalarValue::Float64(None) => {}
            other => panic!("expected Float64(None), got {other:?}"),
        }
    }

    #[test]
    fn count_skips_nulls() {
        let agg = CountAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(1.0), None, Some(2.0)]), 0)
            .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Int64(Some(v)) => assert_eq!(v, 2),
            other => panic!("expected Int64(Some), got {other:?}"),
        }
    }

    #[test]
    fn countall_counts_every_row() {
        let agg = CountAllAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(1.0), None, Some(2.0)]), 0)
            .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Int64(Some(v)) => assert_eq!(v, 3),
            other => panic!("expected Int64(Some), got {other:?}"),
        }
    }

    #[test]
    fn avg_computes_mean() {
        let agg = AvgAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(2.0), Some(4.0), Some(6.0)]), 0)
            .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert_eq!(v, 4.0),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    #[test]
    fn avg_empty_yields_null() {
        let agg = AvgAgg;
        let s = agg.create();
        match s.finalize().unwrap() {
            ScalarValue::Float64(None) => {}
            other => panic!("expected Float64(None), got {other:?}"),
        }
    }

    #[test]
    fn mnor_saturates_at_one() {
        let agg = MnorAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(0.5), Some(0.5)]), 0)
            .unwrap();
        // 1 − (1 − 0.5)·(1 − 0.5) = 1 − 0.25 = 0.75
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert!((v - 0.75).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
        assert!(!s.is_at_top());

        s.ingest(&one_col_batch(vec![Some(1.0)]), 0).unwrap();
        // Adding a 1.0 saturates noisy-OR.
        assert!(s.is_at_top());
    }

    #[test]
    fn mprod_saturates_at_zero() {
        let agg = MprodAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(0.5), Some(0.4)]), 0)
            .unwrap();
        // 0.5 × 0.4 = 0.2
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert!((v - 0.2).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
        assert!(!s.is_at_top());

        s.ingest(&one_col_batch(vec![Some(0.0)]), 0).unwrap();
        assert!(s.is_at_top());
    }

    #[test]
    fn mnor_strict_rejects_out_of_domain() {
        let cx = FoldContext {
            strict: true,
            epsilon: 1e-15,
            semiring: FoldSemiring::AddMult,
        };
        let mut s = MnorAgg.create();
        let col = Float64Array::from(vec![Some(1.5)]);
        let err = s.ingest_indices(&col, &[0], &cx).unwrap_err();
        assert!(err.message.contains("strict_probability_domain"));
    }

    #[test]
    fn mprod_log_space_avoids_underflow() {
        // 0.5^50 underflows naive accumulation far less than f64 min, but the
        // log-space switch keeps it representable and positive.
        let cx = FoldContext {
            strict: false,
            epsilon: 1e-15,
            semiring: FoldSemiring::AddMult,
        };
        let mut s = MprodAgg.create();
        let col = Float64Array::from(vec![Some(0.5); 50]);
        let idx: Vec<usize> = (0..50).collect();
        s.ingest_indices(&col, &idx, &cx).unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => {
                assert!(v > 0.0, "log-space should keep the product positive");
                assert!((v.log2() + 50.0).abs() < 1e-6, "0.5^50 == 2^-50");
            }
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn collect_encodes_largebinary_list() {
        let agg = CollectAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(1.0), Some(2.0)]), 0)
            .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::LargeBinary(Some(bytes)) => {
                let decoded = uni_common::cypher_value_codec::decode(&bytes).unwrap();
                assert_eq!(
                    decoded,
                    uni_common::Value::List(vec![
                        uni_common::Value::Float(1.0),
                        uni_common::Value::Float(2.0),
                    ])
                );
            }
            other => panic!("expected LargeBinary(Some), got {other:?}"),
        }
        assert_eq!(CollectAgg.output_type(), DataType::LargeBinary);
    }

    #[test]
    fn semilattice_metadata_matches_expectations() {
        assert_eq!(MinAgg.semilattice(), Semilattice::BOUNDED_MIN_MAX);
        assert_eq!(MaxAgg.semilattice(), Semilattice::BOUNDED_MIN_MAX);
        assert_eq!(SumAgg.semilattice(), Semilattice::NON_MONOTONE);
        assert_eq!(CountAgg.semilattice(), Semilattice::COUNT);
        assert_eq!(CountAllAgg.semilattice(), Semilattice::COUNT);
        assert_eq!(AvgAgg.semilattice(), Semilattice::NON_MONOTONE);
        assert!(MnorAgg.semilattice().monotone_join);
        assert!(MnorAgg.semilattice().has_top);
        assert!(MprodAgg.semilattice().monotone_join);
        assert!(MprodAgg.semilattice().has_top);
    }
}
