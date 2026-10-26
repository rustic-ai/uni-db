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
use uni_common::cypher_sum::CypherSum;
use uni_plugin::traits::locy::{
    FoldContext, FoldDirection, FoldSemiring, LocyAggState, LocyAggregate, Semilattice, ValueOrder,
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
    /// `MIN`/`MMIN`: the running minimum only falls.
    fn direction(&self) -> FoldDirection {
        FoldDirection::NonIncreasing
    }

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
    /// `MAX`/`MMAX`: the running maximum only rises.
    fn direction(&self) -> FoldDirection {
        FoldDirection::NonDecreasing
    }

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
/// pre-trait executor, including its NaN handling). A Cypher-value
/// (`LargeBinary`) column is decoded and ordered by the host's
/// [`ValueOrder`]: its encoded bytes do not sort as its values do, so
/// `MIN(-1, 1)` was 1 and `MIN('b', 'aa')` was `'b'`. Other column types fall
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
    order: Option<ValueOrder>,
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
            order: None,
        }
    }

    /// Whether `v` replaces `cur` as the running minimum (or maximum).
    fn replaces(&self, v: &ScalarValue, cur: &ScalarValue) -> Result<bool, FnError> {
        let ord = match (v, cur) {
            (ScalarValue::LargeBinary(Some(a)), ScalarValue::LargeBinary(Some(b))) => {
                let order = self.order.ok_or_else(|| {
                    FnError::new(
                        CODE_UNREADABLE_CELL,
                        "MIN/MAX: ordering Cypher values needs the host's value order",
                    )
                })?;
                let decode = |bytes: &[u8]| {
                    uni_common::cypher_value_codec::decode(bytes).map_err(|e| {
                        FnError::new(
                            CODE_UNREADABLE_CELL,
                            format!("MIN/MAX: cannot decode a non-null cell: {e}"),
                        )
                    })
                };
                Some((order.0)(&decode(a)?, &decode(b)?))
            }
            _ => v.partial_cmp(cur),
        };
        Ok(matches!(
            (ord, self.is_min),
            (Some(Ordering::Less), true) | (Some(Ordering::Greater), false)
        ))
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
        cx: &FoldContext,
    ) -> Result<(), FnError> {
        if self.dtype.is_none() {
            self.dtype = Some(col.data_type().clone());
        }
        if self.order.is_none() {
            self.order = cx.order;
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
                        Some(cur) if self.replaces(&v, &cur)? => v,
                        Some(cur) => cur,
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
        if self.order.is_none() {
            self.order = o.order;
        }
        // The other column types' running value was not merged at all.
        if let Some(v) = o.other_acc.clone() {
            self.other_acc = Some(match self.other_acc.take() {
                None => v,
                Some(cur) if self.replaces(&v, &cur)? => v,
                Some(cur) => cur,
            });
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

/// `SUM` aggregate — Cypher's `sum`.
///
/// Integers sum to an exact integer, continuing as a float only if the total
/// leaves the 64-bit range (Neo4j's rule); floats sum to a float; a Cypher
/// value column takes numbers or durations. Not monotone in general
/// (negative inputs can decrease totals). The compiler should reject
/// recursive use unless inputs are constrained non-negative; the
/// [`Semilattice::monotone_join`] flag is `false`.
#[derive(Debug)]
pub struct SumAgg;

impl LocyAggregate for SumAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::NON_MONOTONE
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    /// Returns `LargeBinary` for integer or Cypher-value inputs, otherwise `Float64`.
    fn output_type_for_input(&self, input: &DataType) -> DataType {
        sum_total_type(input)
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
/// Identical runtime to [`SumAgg`], numbers only, but declares
/// [`Semilattice::monotone_join`] = `true` (the caller guarantees
/// non-negative inputs, so the running sum is monotone across fixpoint
/// iterations and sound in recursive Locy strata). `has_top` is `false`.
#[derive(Debug)]
pub struct MSumAgg;

impl LocyAggregate for MSumAgg {
    /// `MSUM`: only over NON-NEGATIVE inputs, which is the
    /// same precondition `MsumNonNegativity` already warns about. A negative
    /// term would let the sum fall and a lower-bound `REQUIRE` oscillate.
    fn direction(&self) -> FoldDirection {
        FoldDirection::NonDecreasing
    }

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
    /// Returns `LargeBinary` for integer or Cypher-value inputs, otherwise `Float64`.
    fn output_type_for_input(&self, input: &DataType) -> DataType {
        sum_total_type(input)
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        // A recursive sum is monotone only over non-negative numbers: no
        // durations.
        Box::new(SumState {
            numbers_only: true,
            ..SumState::default()
        })
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(accum + val)
    }
}

/// Sum state.
///
/// An integer column and a Cypher-value column (a schemaless or duration
/// property) accumulate through [`CypherSum`], as Cypher's `sum` does: an
/// integer total stays exact, continuing as a float only past the 64-bit
/// range, durations add up, and anything that is neither a number nor a
/// duration is an error. Integers used to be summed as `f64`, so two ledger
/// amounts of 2^53 and 1 summed to 2^53 (issue #316). `MSUM`
/// (`numbers_only`) refuses durations. A float column sums in `f64`.
///
/// A group with no non-null value sums to 0 — `Int(0)` through
/// [`CypherSum`], `0.0` over floats — as Cypher's `sum` does: the same
/// aggregate means the same thing in both languages.
#[derive(Debug, Default)]
struct SumState {
    value: f64,
    cypher: Option<CypherSum>,
    numbers_only: bool,
}

impl SumState {
    fn name(&self) -> &'static str {
        if self.numbers_only { "MSUM" } else { "SUM" }
    }
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
        let name = self.name();
        let dt = col.data_type();
        if dt == &DataType::LargeBinary || dt.is_integer() {
            // Set before the rows, so a group of NULLs still sums to the
            // Cypher value `0` its column type promises.
            let numbers_only = self.numbers_only;
            let total = self.cypher.get_or_insert_default();
            for &i in indices {
                if col.is_null(i) {
                    continue;
                }
                if dt.is_integer() {
                    add_integer_cell(total, col, i, name)?;
                } else if numbers_only {
                    add_number_cell(total, col, i, name)?;
                } else {
                    add_cypher_cell(total, col, i, name)?;
                }
            }
            return Ok(());
        }
        for &i in indices {
            if col.is_null(i) {
                continue;
            }
            // #233 Tier 1: an unreadable cell is an error, never silently
            // skipped — a sum of nothing is 0.0, so skipping would hide it.
            let Some(v) = numeric_at(col, i) else {
                return Err(unreadable_cell(name, col));
            };
            self.value += v;
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<SumState>(other)?;
        self.value += o.value;
        if let Some(other_total) = &o.cypher {
            self.cypher
                .get_or_insert_default()
                .merge(other_total)
                .map_err(|e| cypher_total_error(self.name(), &e))?;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(match &self.cypher {
            Some(total) => cypher_total_value(total.sum()),
            None => ScalarValue::Float64(Some(self.value)),
        })
    }
}

/// The column `SUM` / `MSUM` fold to.
///
/// An integer or Cypher-value (`LargeBinary`) column sums to a Cypher value:
/// an exact integer that becomes a float only if it overflows, or for a
/// Cypher-value column a duration. Neither fits one Arrow type, so the
/// column holds the encoded value. Any other column sums to Float64.
fn sum_total_type(input: &DataType) -> DataType {
    if input == &DataType::LargeBinary || input.is_integer() {
        DataType::LargeBinary
    } else {
        DataType::Float64
    }
}

/// A Cypher-value column (`LargeBinary`) folds by `AVG` to a Cypher value;
/// any other column to Float64.
fn cypher_total_type(input: &DataType) -> DataType {
    if input == &DataType::LargeBinary {
        DataType::LargeBinary
    } else {
        DataType::Float64
    }
}

/// Adds row `i` of the integer column `col` to `total`: exactly, or for an
/// unsigned value past `i64::MAX` as the float it overflows to.
fn add_integer_cell(
    total: &mut CypherSum,
    col: &dyn Array,
    i: usize,
    agg: &str,
) -> Result<(), FnError> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    };
    let added = match col.data_type() {
        DataType::Int64 => total.add_int(col.as_primitive::<Int64Type>().value(i)),
        DataType::Int32 => total.add_int(i64::from(col.as_primitive::<Int32Type>().value(i))),
        DataType::Int16 => total.add_int(i64::from(col.as_primitive::<Int16Type>().value(i))),
        DataType::Int8 => total.add_int(i64::from(col.as_primitive::<Int8Type>().value(i))),
        DataType::UInt32 => total.add_int(i64::from(col.as_primitive::<UInt32Type>().value(i))),
        DataType::UInt16 => total.add_int(i64::from(col.as_primitive::<UInt16Type>().value(i))),
        DataType::UInt8 => total.add_int(i64::from(col.as_primitive::<UInt8Type>().value(i))),
        DataType::UInt64 => {
            let v = col.as_primitive::<UInt64Type>().value(i);
            match i64::try_from(v) {
                Ok(v) => total.add_int(v),
                Err(_) => total.add_float(v as f64),
            }
        }
        _ => return Err(unreadable_cell(agg, col)),
    };
    added.map_err(|e| cypher_total_error(agg, &e))
}

/// Adds the Cypher value in row `i` of `col` to `total`.
fn add_cypher_cell(
    total: &mut CypherSum,
    col: &dyn Array,
    i: usize,
    agg: &str,
) -> Result<(), FnError> {
    let value = decode_cypher_cell(col, i, agg)?;
    total.add(&value).map_err(|e| cypher_total_error(agg, &e))
}

/// Adds the number in row `i` of the Cypher-value column `col` to `total`;
/// NULL is skipped, and anything else (a duration, a string) is an error.
fn add_number_cell(
    total: &mut CypherSum,
    col: &dyn Array,
    i: usize,
    agg: &str,
) -> Result<(), FnError> {
    let added = match decode_cypher_cell(col, i, agg)? {
        uni_common::Value::Null => return Ok(()),
        uni_common::Value::Int(n) => total.add_int(n),
        uni_common::Value::Float(f) => total.add_float(f),
        other => {
            return Err(FnError::new(
                CODE_UNREADABLE_CELL,
                format!(
                    "{agg} takes numbers, got {}",
                    uni_common::cypher_sum::describe(&other)
                ),
            ));
        }
    };
    added.map_err(|e| cypher_total_error(agg, &e))
}

/// Decodes the Cypher value in row `i` of the `LargeBinary` column `col`.
fn decode_cypher_cell(col: &dyn Array, i: usize, agg: &str) -> Result<uni_common::Value, FnError> {
    use arrow_array::cast::AsArray;
    uni_common::cypher_value_codec::decode(col.as_binary::<i64>().value(i)).map_err(|e| {
        FnError::new(
            CODE_UNREADABLE_CELL,
            format!("{agg}: cannot decode a non-null cell: {e}"),
        )
    })
}

fn cypher_total_error(agg: &str, e: &anyhow::Error) -> FnError {
    FnError::new(CODE_UNREADABLE_CELL, format!("{agg}: {e}"))
}

/// A `SUM` / `AVG` over Cypher values, as a Cypher value: an integer sum stays
/// an integer, as Cypher's `sum`; a float; a duration; NULL for no average.
fn cypher_total_value(value: uni_common::Value) -> ScalarValue {
    ScalarValue::LargeBinary(
        (!value.is_null()).then(|| uni_common::cypher_value_codec::encode(&value)),
    )
}

// =========================================================================
// COUNT — non-null count; monotone but unbounded (top = ∞)
// =========================================================================

/// `COUNT` aggregate — counts non-null rows (`count(x)` semantics).
#[derive(Debug)]
pub struct CountAgg;

impl LocyAggregate for CountAgg {
    /// `COUNT`/`MCOUNT`: the derived set only grows.
    fn direction(&self) -> FoldDirection {
        FoldDirection::NonDecreasing
    }

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
    /// `COUNTALL`: the derived set only grows.
    fn direction(&self) -> FoldDirection {
        FoldDirection::NonDecreasing
    }

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
    /// A Cypher-value column averages to a Cypher value: a float, or a
    /// duration.
    fn output_type_for_input(&self, input: &DataType) -> DataType {
        cypher_total_type(input)
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(AvgState::default())
    }
}

/// Accumulates non-null count and numeric sum; finalizes `sum / count`. A
/// Cypher-value column accumulates through [`CypherSum`], as for `SUM`.
#[derive(Debug, Default)]
struct AvgState {
    sum: f64,
    count: i64,
    cypher: Option<CypherSum>,
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
        if col.data_type() == &DataType::LargeBinary {
            let total = self.cypher.get_or_insert_default();
            for &i in indices {
                if !col.is_null(i) {
                    add_cypher_cell(total, col, i, "AVG")?;
                    self.count += 1;
                }
            }
            return Ok(());
        }
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
        if let Some(other_total) = &o.cypher {
            self.cypher
                .get_or_insert_default()
                .merge(other_total)
                .map_err(|e| cypher_total_error("AVG", &e))?;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        if let Some(total) = &self.cypher {
            return Ok(cypher_total_value(total.avg(self.count.unsigned_abs())));
        }
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
    /// `MNOR`: noisy-OR over `[0, 1]` only rises.
    fn direction(&self) -> FoldDirection {
        FoldDirection::NonDecreasing
    }

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
    /// `MPROD`: a product of values in `[0, 1]` only falls.
    fn direction(&self) -> FoldDirection {
        FoldDirection::NonIncreasing
    }

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
        // A dynamically typed (CypherValue) cell holding a number: a schemaless
        // property, or an ALONG column over one. Anything else stays unreadable.
        DataType::LargeBinary => {
            match uni_common::cypher_value_codec::decode(col.as_binary::<i64>().value(i)) {
                Ok(uni_common::Value::Int(n)) => Some(n as f64),
                Ok(uni_common::Value::Float(f)) => Some(f),
                _ => None,
            }
        }
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

    /// A Cypher-value column is ordered by the host's order, not its bytes:
    /// `-1` encodes after `1`, so a byte comparison made `MIN(-1, 1)` 1.
    #[test]
    fn min_max_order_cypher_values_by_the_host_order() {
        fn by_number(a: &uni_common::Value, b: &uni_common::Value) -> Ordering {
            a.as_f64()
                .partial_cmp(&b.as_f64())
                .unwrap_or(Ordering::Equal)
        }
        let col = arrow_array::LargeBinaryArray::from(vec![
            Some(uni_common::cypher_value_codec::encode(&uni_common::Value::Int(1)).as_slice()),
            Some(uni_common::cypher_value_codec::encode(&uni_common::Value::Int(-1)).as_slice()),
        ]);
        let cx = FoldContext {
            order: Some(ValueOrder(by_number)),
            ..FoldContext::default()
        };
        for (agg, want) in [(MinAgg.create(), -1), (MaxAgg.create(), 1)] {
            let mut s = agg;
            s.ingest_indices(&col, &[0, 1], &cx).unwrap();
            let ScalarValue::LargeBinary(Some(bytes)) = s.finalize().unwrap() else {
                panic!("expected a Cypher value");
            };
            assert_eq!(
                uni_common::cypher_value_codec::decode(&bytes).unwrap(),
                uni_common::Value::Int(want)
            );
        }
        // Without an order, the column is refused rather than byte-ordered.
        let mut s = MinAgg.create();
        assert!(
            s.ingest_indices(&col, &[0, 1], &FoldContext::default())
                .is_err()
        );
    }
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
        assert_eq!(
            decoded(sum.finalize().expect("finalize")),
            uni_common::Value::Int(6),
            "SUM over Int32 must be 6; 0 is the value the skipped-cell path produced"
        );

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
    fn sum_empty_yields_zero() {
        // A sum of nothing is 0.0, as in Cypher — for an empty group and for an
        // all-NULL one.
        for agg in [&SumAgg as &dyn LocyAggregate, &MSumAgg] {
            let s = agg.create();
            assert_eq!(s.finalize().unwrap(), ScalarValue::Float64(Some(0.0)));
            let mut s = agg.create();
            s.ingest(&one_col_batch(vec![None, None]), 0).unwrap();
            assert_eq!(s.finalize().unwrap(), ScalarValue::Float64(Some(0.0)));
            // Over an integer column, the integer 0 its column type promises.
            let mut s = agg.create();
            s.ingest(&int64_batch(vec![None, None]), 0).unwrap();
            assert_eq!(decoded(s.finalize().unwrap()), uni_common::Value::Int(0));
        }
    }

    fn int64_batch(values: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn cypher_batch(values: Vec<uni_common::Value>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::LargeBinary,
            true,
        )]));
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(uni_common::cypher_value_codec::encode)
            .collect();
        let arr = arrow_array::LargeBinaryArray::from(
            encoded
                .iter()
                .map(|b| Some(b.as_slice()))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    /// The Cypher value a `SUM` finalized to.
    fn decoded(v: ScalarValue) -> uni_common::Value {
        let ScalarValue::LargeBinary(Some(bytes)) = v else {
            panic!("expected a Cypher value, got {v:?}");
        };
        uni_common::cypher_value_codec::decode(&bytes).unwrap()
    }

    /// Issue #316: `SUM` / `MSUM` of integers are exact integers, typed or
    /// schemaless, and continue as a float only past the 64-bit range. They
    /// were summed in `f64`, so 2^53 + 1 was 2^53.
    #[test]
    fn sum_of_integers_is_exact_and_widens_on_overflow() {
        use uni_common::Value;
        let two_53 = 1_i64 << 53;
        for agg in [&SumAgg as &dyn LocyAggregate, &MSumAgg] {
            assert_eq!(
                agg.output_type_for_input(&DataType::Int64),
                DataType::LargeBinary
            );
            assert_eq!(
                agg.output_type_for_input(&DataType::Float64),
                DataType::Float64
            );
            for (values, want) in [
                (vec![two_53, 1], Value::Int(two_53 + 1)),
                (vec![i64::MAX - 1, 1], Value::Int(i64::MAX)),
                (vec![i64::MAX, 1], Value::Float(2f64.powi(63))),
                (vec![i64::MAX, 1, -1], Value::Float(2f64.powi(63))),
            ] {
                let mut s = agg.create();
                s.ingest(&int64_batch(values.iter().copied().map(Some).collect()), 0)
                    .unwrap();
                assert_eq!(decoded(s.finalize().unwrap()), want, "{values:?}");
                let mut s = agg.create();
                s.ingest(
                    &cypher_batch(values.iter().copied().map(Value::Int).collect()),
                    0,
                )
                .unwrap();
                assert_eq!(decoded(s.finalize().unwrap()), want, "{values:?}");
            }
            // Partial states merge exactly too.
            let mut a = agg.create();
            a.ingest(&int64_batch(vec![Some(two_53)]), 0).unwrap();
            let mut b = agg.create();
            b.ingest(&int64_batch(vec![Some(1)]), 0).unwrap();
            a.merge(b.as_ref()).unwrap();
            assert_eq!(decoded(a.finalize().unwrap()), Value::Int(two_53 + 1));
            // A float among them makes a float.
            let mut s = agg.create();
            s.ingest(&cypher_batch(vec![Value::Int(1), Value::Float(0.5)]), 0)
                .unwrap();
            assert_eq!(decoded(s.finalize().unwrap()), Value::Float(1.5));
        }
    }

    /// `MSUM` takes numbers alone: a duration in a Cypher-value column is an
    /// error, where `SUM` adds it.
    #[test]
    fn msum_refuses_a_duration_that_sum_adds() {
        let d = uni_common::Value::Temporal(uni_common::TemporalValue::Duration {
            months: 0,
            days: 1,
            nanos: 0,
        });
        let batch = cypher_batch(vec![d.clone(), d]);
        let mut s = SumAgg.create();
        s.ingest(&batch, 0).unwrap();
        assert!(matches!(
            decoded(s.finalize().unwrap()),
            uni_common::Value::Temporal(_)
        ));
        let err = MSumAgg
            .create()
            .ingest(&batch, 0)
            .expect_err("MSUM takes numbers only");
        assert!(err.message.contains("number"), "{}", err.message);
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
            ..FoldContext::default()
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
            ..FoldContext::default()
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
