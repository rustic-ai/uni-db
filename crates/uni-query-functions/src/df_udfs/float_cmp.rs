// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Float comparison and equivalence keys with openCypher semantics.
//!
//! Arrow's comparison kernels, and therefore DataFusion's native `=`, `<`,
//! hash grouping and `DISTINCT`, order `f64` by IEEE *total* order
//! (`f64::total_cmp`): `-0.0 < 0.0`, `NaN = NaN` (for equal bit patterns), a
//! NaN with its sign bit set sorts below `-inf`. openCypher
//! (CIP2016-06-14) compares floats numerically instead:
//!
//! * `-0.0 = 0.0`, and neither is less than the other;
//! * `NaN = x` is false and `NaN <> x` is true, for every number `x`
//!   including `NaN`; every `<`, `<=`, `>`, `>=` involving `NaN` is false;
//! * equivalence (grouping, `DISTINCT`) is equality, except that any two NaNs
//!   are equivalent.
//!
//! Rust's own `f64` operators implement exactly the comparison half, so
//! [`FloatCmp::apply`] is a thin dispatch over them. [`canonical_float`] is
//! the equivalence half: it maps every member of an equivalence class to one
//! bit pattern, so a bit-exact hash or comparison of the key groups by
//! Cypher equivalence.
//!
//! An integer compared with a float is compared exactly, by value, as
//! `cypher_eq` and `cypher_partial_cmp` do; it is never widened to `f64`,
//! which rounds above 2^53.

use super::*;
use arrow::compute::kernels::cast::cast;

/// One of the six comparison operators, applied to two floats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FloatCmp {
    /// `=`
    Eq,
    /// `<>`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
}

impl FloatCmp {
    /// The operator for a DataFusion comparison operator, `None` otherwise.
    pub fn from_operator(op: datafusion::logical_expr::Operator) -> Option<Self> {
        use datafusion::logical_expr::Operator as O;
        Some(match op {
            O::Eq => Self::Eq,
            O::NotEq => Self::NotEq,
            O::Lt => Self::Lt,
            O::LtEq => Self::LtEq,
            O::Gt => Self::Gt,
            O::GtEq => Self::GtEq,
            _ => return None,
        })
    }

    /// Name of the UDF implementing this operator.
    ///
    /// Matched by name when the planner resolves its placeholder UDFs, so it
    /// must stay identical to what [`create_cypher_float_compare_udf`]
    /// registers.
    pub fn udf_name(self) -> &'static str {
        match self {
            Self::Eq => "_cypher_float_eq",
            Self::NotEq => "_cypher_float_not_eq",
            Self::Lt => "_cypher_float_lt",
            Self::LtEq => "_cypher_float_lt_eq",
            Self::Gt => "_cypher_float_gt",
            Self::GtEq => "_cypher_float_gt_eq",
        }
    }

    /// Every operator, for registration.
    pub const ALL: [Self; 6] = [
        Self::Eq,
        Self::NotEq,
        Self::Lt,
        Self::LtEq,
        Self::Gt,
        Self::GtEq,
    ];

    /// Compare `l` with `r` numerically.
    ///
    /// Rust's `f64` operators are IEEE comparisons, which is what openCypher
    /// specifies: `-0.0 == 0.0`, every ordering involving NaN is false, and
    /// `NaN != x` holds for every `x`.
    #[inline]
    pub fn apply(self, l: f64, r: f64) -> bool {
        match self {
            Self::Eq => l == r,
            Self::NotEq => l != r,
            Self::Lt => l < r,
            Self::LtEq => l <= r,
            Self::Gt => l > r,
            Self::GtEq => l >= r,
        }
    }
}

/// Map `f` to the one representative of its Cypher equivalence class.
///
/// `-0.0` becomes `0.0` and every NaN becomes [`f64::NAN`]; every other value
/// is returned unchanged. Two floats are equivalent exactly when their
/// canonical forms have the same bit pattern.
#[inline]
pub fn canonical_float(f: f64) -> f64 {
    if f == 0.0 {
        0.0
    } else if f.is_nan() {
        f64::NAN
    } else {
        f
    }
}

/// [`canonical_float`] for `f32`.
#[inline]
fn canonical_float32(f: f32) -> f32 {
    if f == 0.0 {
        0.0
    } else if f.is_nan() {
        f32::NAN
    } else {
        f
    }
}

/// Build the comparison UDF for `op`: `(Float64 | Int64, Float64 | Int64) ->
/// Boolean`.
///
/// `null` on either side gives `null`. The planner casts narrower integers to
/// `Int64` and `Float32` to `Float64` before calling it; an `Int64` operand is
/// kept, not widened, and compared with a float exactly
/// ([`FloatCmp::apply_int_float`]).
pub fn create_cypher_float_compare_udf(op: FloatCmp) -> ScalarUDF {
    use datafusion::logical_expr::TypeSignature;
    let pair = |l: DataType, r: DataType| TypeSignature::Exact(vec![l, r]);
    ScalarUDF::new_from_impl(CypherFloatCompareUdf {
        op,
        signature: Signature::one_of(
            vec![
                pair(DataType::Float64, DataType::Float64),
                pair(DataType::Int64, DataType::Float64),
                pair(DataType::Float64, DataType::Int64),
                pair(DataType::Int64, DataType::Int64),
            ],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherFloatCompareUdf {
    op: FloatCmp,
    signature: Signature,
}

/// One number, from either side of a comparison.
#[derive(Clone, Copy)]
enum Num {
    I(i64),
    F(f64),
}

impl FloatCmp {
    /// Compare an integer with a float exactly.
    ///
    /// The integer is not widened to `f64`, which rounds above 2^53 and made
    /// `9007199254740993 = 9007199254740992.0` true. A NaN float is
    /// incomparable: every operator is false except `<>`.
    #[inline]
    pub fn apply_int_float(self, i: i64, f: f64) -> bool {
        if f.is_nan() {
            return self == Self::NotEq;
        }
        self.holds(uni_common::cmp_i64_f64(i, f))
    }

    /// Whether `ord` (left against right) satisfies this operator.
    #[inline]
    fn holds(self, ord: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering as O;
        match self {
            Self::Eq => ord == O::Equal,
            Self::NotEq => ord != O::Equal,
            Self::Lt => ord == O::Less,
            Self::LtEq => ord != O::Greater,
            Self::Gt => ord == O::Greater,
            Self::GtEq => ord != O::Less,
        }
    }

    /// The operator with its operands swapped: `a op b` is `b op.flip() a`.
    #[inline]
    pub fn flip(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::NotEq => Self::NotEq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
        }
    }

    #[inline]
    fn apply_num(self, l: Num, r: Num) -> bool {
        match (l, r) {
            (Num::F(l), Num::F(r)) => self.apply(l, r),
            (Num::I(l), Num::I(r)) => self.holds(l.cmp(&r)),
            (Num::I(i), Num::F(f)) => self.apply_int_float(i, f),
            (Num::F(f), Num::I(i)) => self.flip().apply_int_float(i, f),
        }
    }
}

/// The argument as an `Int64Array` or `Float64Array`, or as a scalar number.
enum NumArg {
    Ints(Int64Array),
    Floats(Float64Array),
    Scalar(Option<Num>),
}

impl NumArg {
    /// Row `i`'s number, `None` for a null.
    #[inline]
    fn get(&self, i: usize) -> Option<Num> {
        match self {
            Self::Ints(a) => a.is_valid(i).then(|| Num::I(a.value(i))),
            Self::Floats(a) => a.is_valid(i).then(|| Num::F(a.value(i))),
            Self::Scalar(s) => *s,
        }
    }
}

fn num_arg(arg: &ColumnarValue, name: &str) -> DFResult<NumArg> {
    let err = |dt: &DataType| {
        datafusion::error::DataFusionError::Execution(format!(
            "{name}(): expected a number, got {dt}"
        ))
    };
    match arg {
        ColumnarValue::Array(a) => {
            if a.data_type().is_integer() {
                let a = cast(a, &DataType::Int64)?;
                let a = a
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| err(a.data_type()))?;
                Ok(NumArg::Ints(a.clone()))
            } else {
                let a = cast(a, &DataType::Float64)?;
                let a = a
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| err(a.data_type()))?;
                Ok(NumArg::Floats(a.clone()))
            }
        }
        ColumnarValue::Scalar(s) if s.data_type().is_integer() => {
            match s.cast_to(&DataType::Int64)? {
                ScalarValue::Int64(v) => Ok(NumArg::Scalar(v.map(Num::I))),
                other => Err(err(&other.data_type())),
            }
        }
        ColumnarValue::Scalar(s) => match s.cast_to(&DataType::Float64)? {
            ScalarValue::Float64(v) => Ok(NumArg::Scalar(v.map(Num::F))),
            other => Err(err(&other.data_type())),
        },
    }
}

impl ScalarUDFImpl for CypherFloatCompareUdf {
    fn name(&self) -> &str {
        self.op.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let name = self.op.udf_name();
        let [l, r] = args.args.as_slice() else {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{name}(): requires 2 arguments"
            )));
        };
        let op = self.op;
        let (l, r) = (num_arg(l, name)?, num_arg(r, name)?);
        if let (NumArg::Scalar(l), NumArg::Scalar(r)) = (&l, &r) {
            let v = l.zip(*r).map(|(l, r)| op.apply_num(l, r));
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(v)));
        }
        let out = match (&l, &r) {
            // The common case keeps the vectorized kernel.
            (NumArg::Floats(a), NumArg::Floats(b)) => {
                BooleanArray::from_binary(a, b, |l, r| op.apply(l, r))
            }
            (NumArg::Floats(a), NumArg::Scalar(Some(Num::F(s)))) => {
                BooleanArray::from_unary(a, |v| op.apply(v, *s))
            }
            (NumArg::Scalar(Some(Num::F(s))), NumArg::Floats(a)) => {
                BooleanArray::from_unary(a, |v| op.apply(*s, v))
            }
            _ => (0..args.number_rows)
                .map(|i| Some(op.apply_num(l.get(i)?, r.get(i)?)))
                .collect(),
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// Build the `_cypher_float_key` UDF: a float's equivalence-class key.
///
/// `Float64 -> Float64` and `Float32 -> Float32`, through
/// [`canonical_float`]. Grouping, `DISTINCT` and `UNION` key a float column
/// on it so `-0.0` and `0.0` form one group, and every NaN one. The type is
/// preserved so wrapping a group key does not change the output schema.
pub fn create_cypher_float_key_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherFloatKeyUdf {
        signature: Signature::uniform(
            1,
            vec![DataType::Float64, DataType::Float32],
            Volatility::Immutable,
        ),
    })
}

/// Name of the UDF [`create_cypher_float_key_udf`] builds.
pub const CYPHER_FLOAT_KEY_UDF: &str = "_cypher_float_key";

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherFloatKeyUdf {
    signature: Signature,
}

impl ScalarUDFImpl for CypherFloatKeyUdf {
    fn name(&self) -> &str {
        CYPHER_FLOAT_KEY_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Float64))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(datafusion::error::DataFusionError::Execution(
                "_cypher_float_key(): requires 1 argument".to_string(),
            ));
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Float64(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(v.map(canonical_float)),
            )),
            ColumnarValue::Scalar(ScalarValue::Float32(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float32(v.map(canonical_float32)),
            )),
            ColumnarValue::Array(a) => {
                if let Some(a) = a.as_any().downcast_ref::<Float64Array>() {
                    let out: Float64Array = a.unary(canonical_float);
                    Ok(ColumnarValue::Array(Arc::new(out)))
                } else if let Some(a) = a.as_any().downcast_ref::<Float32Array>() {
                    let out: Float32Array = a.unary(canonical_float32);
                    Ok(ColumnarValue::Array(Arc::new(out)))
                } else {
                    Err(datafusion::error::DataFusionError::Execution(format!(
                        "_cypher_float_key(): expected a float array, got {}",
                        a.data_type()
                    )))
                }
            }
            other => Err(datafusion::error::DataFusionError::Execution(format!(
                "_cypher_float_key(): expected a float, got {other:?}"
            ))),
        }
    }
}

/// Canonical encoding of a CypherValue for grouping, or `None` when `bytes`
/// is already canonical.
///
/// A `CypherValue` column groups by its encoded bytes, so `-0.0` and `0.0`
/// (and NaNs of either sign) were different groups there too, directly or
/// nested in a list or map, as were `1` and `1.0`. Every float inside is
/// replaced by its integer when it is integral and fits `i64`, and by
/// [`canonical_float`] otherwise, and the value re-encoded. Nested values are tagged
/// blobs, so a value with no `TAG_FLOAT` byte anywhere holds no float and is
/// skipped without decoding; other kinds (entities, vectors, handles) are left
/// alone, as is anything that fails to decode.
pub fn canonical_cypher_value_key(bytes: &[u8]) -> Option<Vec<u8>> {
    use uni_common::cypher_value_codec::{TAG_FLOAT, TAG_LIST, TAG_MAP, decode, encode};
    let tag = *bytes.first()?;
    if !matches!(tag, TAG_FLOAT | TAG_LIST | TAG_MAP) || !bytes.contains(&TAG_FLOAT) {
        return None;
    }
    let value = decode(bytes).ok()?;
    let mut changed = false;
    let canonical = canonical_value(value, &mut changed);
    changed.then(|| encode(&canonical))
}

/// The one representative of `f`'s equivalence class among all numbers.
///
/// An integral float that an `i64` holds exactly is that integer (`1.0` is
/// `1`, `-0.0` is `0`); any other float is [`canonical_float`]. Two numbers
/// are equivalent exactly when their representatives are identical.
pub(crate) fn canonical_number(f: f64) -> Value {
    uni_common::f64_to_exact_i64(f).map_or_else(|| Value::Float(canonical_float(f)), Value::Int)
}

/// The one representative of `value`'s Cypher equivalence class.
///
/// Every float, at any depth of lists and maps, becomes its integer when it
/// is integral and an `i64` holds it exactly, and its [`canonical_float`]
/// otherwise. Two values are equivalent (`1` and `1.0`, `[1]` and `[1.0]`,
/// `0.0` and `-0.0`, any two NaNs) exactly when their representatives are
/// equal. Use it as a key only: the value a DISTINCT keeps is the first one
/// seen, not the representative.
///
/// # Examples
/// ```
/// use uni_common::Value;
/// use uni_query_functions::df_udfs::canonical_cypher_value;
///
/// assert_eq!(canonical_cypher_value(Value::Float(1.0)), Value::Int(1));
/// assert_eq!(
///     canonical_cypher_value(Value::Float(9_223_372_036_854_775_808.0)),
///     Value::Float(9_223_372_036_854_775_808.0)
/// );
/// ```
#[must_use]
pub fn canonical_cypher_value(value: Value) -> Value {
    canonical_value(value, &mut false)
}

/// `value` with every float, at any depth of lists and maps, canonicalized.
///
/// An integral float that an `i64` holds exactly becomes that integer, so
/// `1` and `1.0` (and `0`, `0.0`, `-0.0`) share one key: they are equal
/// numbers, and openCypher equivalence follows equality.
fn canonical_value(value: Value, changed: &mut bool) -> Value {
    match value {
        Value::Float(f) => {
            let c = canonical_number(f);
            if !matches!(c, Value::Float(c) if c.to_bits() == f.to_bits()) {
                *changed = true;
            }
            c
        }
        Value::List(items) => Value::List(
            items
                .into_iter()
                .map(|v| canonical_value(v, changed))
                .collect(),
        ),
        Value::Map(map) => Value::Map(
            map.into_iter()
                .map(|(k, v)| (k, canonical_value(v, changed)))
                .collect(),
        ),
        other => other,
    }
}

/// Name of the UDF [`create_cypher_value_key_udf`] builds.
pub const CYPHER_VALUE_KEY_UDF: &str = "_cypher_value_key";

/// Build the `_cypher_value_key` UDF: `LargeBinary -> LargeBinary` through
/// [`canonical_cypher_value_key`], the CypherValue counterpart of
/// `_cypher_float_key`.
pub fn create_cypher_value_key_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherValueKeyUdf {
        signature: Signature::exact(vec![DataType::LargeBinary], Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherValueKeyUdf {
    signature: Signature,
}

impl ScalarUDFImpl for CypherValueKeyUdf {
    fn name(&self) -> &str {
        CYPHER_VALUE_KEY_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(datafusion::error::DataFusionError::Execution(
                "_cypher_value_key(): requires 1 argument".to_string(),
            ));
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::LargeBinary(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(
                    v.as_ref()
                        .map(|b| canonical_cypher_value_key(b).unwrap_or_else(|| b.clone())),
                )))
            }
            ColumnarValue::Array(a) => {
                let a = a
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "_cypher_value_key(): expected LargeBinary, got {}",
                            a.data_type()
                        ))
                    })?;
                if !a
                    .iter()
                    .flatten()
                    .any(|b| b.contains(&uni_common::cypher_value_codec::TAG_FLOAT))
                {
                    return Ok(ColumnarValue::Array(Arc::new(a.clone())));
                }
                let out: LargeBinaryArray = a
                    .iter()
                    .map(|v| v.map(|b| canonical_cypher_value_key(b).unwrap_or_else(|| b.to_vec())))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
            other => Err(datafusion::error::DataFusionError::Execution(format!(
                "_cypher_value_key(): expected LargeBinary, got {other:?}"
            ))),
        }
    }
}

/// Name of the aggregate [`create_cypher_first_seen_udaf`] builds.
pub const CYPHER_FIRST_SEEN_UDAF: &str = "_cypher_first_seen";

/// Build the `_cypher_first_seen` aggregate: the first value its group saw.
///
/// Grouping on [`canonical_cypher_value_key`] makes `1` and `1.0` one group,
/// but the key itself is the canonical form (`1`), not a value the query
/// produced. openCypher keeps one of the equal values; Neo4j keeps the first
/// one seen, so `UNWIND [1.0, 1] AS x RETURN DISTINCT x` is `1.0`. A
/// `CypherValue` group key is grouped on its canonical key and emitted through
/// this aggregate over the original column. The output field is the input
/// field, metadata included.
pub fn create_cypher_first_seen_udaf() -> datafusion::logical_expr::AggregateUDF {
    datafusion::logical_expr::AggregateUDF::from(CypherFirstSeenUdaf {
        signature: Signature::exact(vec![DataType::LargeBinary], Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherFirstSeenUdaf {
    signature: Signature,
}

impl datafusion::logical_expr::AggregateUDFImpl for CypherFirstSeenUdaf {
    fn name(&self) -> &str {
        CYPHER_FIRST_SEEN_UDAF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn return_field(
        &self,
        args: &[arrow::datatypes::FieldRef],
    ) -> DFResult<arrow::datatypes::FieldRef> {
        args.first().cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "{CYPHER_FIRST_SEEN_UDAF}(): requires 1 argument"
            ))
        })
    }

    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        Ok(Box::new(FirstSeenAccumulator { first: None }))
    }

    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DFResult<Vec<arrow::datatypes::FieldRef>> {
        Ok(vec![
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_value", args.name),
                DataType::LargeBinary,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_seen", args.name),
                DataType::Boolean,
                false,
            )),
        ])
    }
}

/// The first value of the group: `None` until a row arrives, then the row's
/// value (`Some(None)` for a null).
#[derive(Debug)]
struct FirstSeenAccumulator {
    first: Option<Option<Vec<u8>>>,
}

impl FirstSeenAccumulator {
    fn binary(arr: &ArrayRef) -> DFResult<&LargeBinaryArray> {
        arr.as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{CYPHER_FIRST_SEEN_UDAF}(): expected LargeBinary, got {}",
                    arr.data_type()
                ))
            })
    }
}

impl datafusion::logical_expr::Accumulator for FirstSeenAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if self.first.is_none()
            && let Some(arr) = values.first()
            && !arr.is_empty()
        {
            let a = Self::binary(arr)?;
            self.first = Some(a.is_valid(0).then(|| a.value(0).to_vec()));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if self.first.is_some() {
            return Ok(());
        }
        let [values, seen] = states else {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{CYPHER_FIRST_SEEN_UDAF}(): expected 2 state columns"
            )));
        };
        let values = Self::binary(values)?;
        let seen = seen
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{CYPHER_FIRST_SEEN_UDAF}(): expected a Boolean state"
                ))
            })?;
        if let Some(i) = (0..seen.len()).find(|&i| seen.is_valid(i) && seen.value(i)) {
            self.first = Some(values.is_valid(i).then(|| values.value(i).to_vec()));
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::LargeBinary(self.first.clone().flatten()),
            ScalarValue::Boolean(Some(self.first.is_some())),
        ])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::LargeBinary(self.first.clone().flatten()))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .first
                .as_ref()
                .and_then(|v| v.as_ref())
                .map_or(0, Vec::capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLES: [f64; 8] = [
        -0.0,
        0.0,
        1.0,
        -1.5,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        -f64::NAN,
    ];

    /// The comparison agrees with `cypher_partial_cmp` / `cypher_eq`, the
    /// value-level reference, on every pair of samples.
    #[test]
    fn comparison_matches_the_value_level_reference() {
        use crate::expr_eval::cypher_eq;
        for l in SAMPLES {
            for r in SAMPLES {
                let (lv, rv) = (Value::Float(l), Value::Float(r));
                assert_eq!(
                    Some(FloatCmp::Eq.apply(l, r)),
                    cypher_eq(&lv, &rv),
                    "{l} = {r}"
                );
                assert_eq!(FloatCmp::NotEq.apply(l, r), !FloatCmp::Eq.apply(l, r));
            }
        }
        assert!(FloatCmp::Eq.apply(-0.0, 0.0));
        assert!(!FloatCmp::Lt.apply(-0.0, 0.0));
        assert!(!FloatCmp::Eq.apply(f64::NAN, f64::NAN));
        assert!(FloatCmp::NotEq.apply(f64::NAN, f64::NAN));
        for op in [FloatCmp::Lt, FloatCmp::LtEq, FloatCmp::Gt, FloatCmp::GtEq] {
            assert!(!op.apply(f64::NAN, 1.0));
            assert!(!op.apply(-f64::NAN, 1.0));
            assert!(!op.apply(1.0, f64::NAN));
        }
    }

    /// Two samples share a key bit pattern exactly when they are equivalent:
    /// equal, or both NaN.
    #[test]
    fn key_is_one_bit_pattern_per_equivalence_class() {
        for l in SAMPLES {
            for r in SAMPLES {
                let equivalent = l == r || (l.is_nan() && r.is_nan());
                assert_eq!(
                    canonical_float(l).to_bits() == canonical_float(r).to_bits(),
                    equivalent,
                    "{l} vs {r}"
                );
            }
        }
    }

    /// Nested floats are canonicalized; values without one are untouched.
    #[test]
    fn cypher_value_key_canonicalizes_nested_floats() {
        use uni_common::cypher_value_codec::encode;
        let key = |v: &Value| {
            let b = encode(v);
            canonical_cypher_value_key(&b).unwrap_or(b)
        };
        assert_eq!(key(&Value::Float(-0.0)), key(&Value::Float(0.0)));
        assert_eq!(key(&Value::Float(-f64::NAN)), key(&Value::Float(f64::NAN)));
        assert_eq!(
            key(&Value::List(vec![
                Value::Float(-0.0),
                Value::String("a".into())
            ])),
            key(&Value::List(vec![
                Value::Float(0.0),
                Value::String("a".into())
            ]))
        );
        assert_ne!(key(&Value::Float(1.0)), key(&Value::Float(-1.0)));
        assert!(canonical_cypher_value_key(&encode(&Value::Int(3))).is_none());
        assert!(canonical_cypher_value_key(&encode(&Value::Float(2.5))).is_none());
    }

    /// The array paths, scalar-on-either-side included, propagate nulls.
    #[test]
    fn udf_handles_arrays_scalars_and_nulls() {
        use arrow_schema::Field;
        let udf = create_cypher_float_compare_udf(FloatCmp::Eq);
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![Some(-0.0), None, Some(f64::NAN)]));
        let run = |l: ColumnarValue, r: ColumnarValue| {
            let field = Arc::new(Field::new("x", DataType::Float64, true));
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![l, r],
                arg_fields: vec![field.clone(), field],
                number_rows: 3,
                return_field: Arc::new(Field::new("r", DataType::Boolean, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap()
            .into_array(3)
            .unwrap()
        };
        let zero = ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0)));
        let want = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let got = run(ColumnarValue::Array(arr.clone()), zero.clone());
        assert_eq!(got.as_any().downcast_ref::<BooleanArray>().unwrap(), &want);
        let got = run(zero, ColumnarValue::Array(arr.clone()));
        assert_eq!(got.as_any().downcast_ref::<BooleanArray>().unwrap(), &want);
        let got = run(ColumnarValue::Array(arr.clone()), ColumnarValue::Array(arr));
        assert_eq!(
            got.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![Some(true), None, Some(false)])
        );
        let null = ColumnarValue::Scalar(ScalarValue::Float64(None));
        let got = run(
            null,
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![1.0; 3]))),
        );
        assert_eq!(got.null_count(), 3);
    }
}
