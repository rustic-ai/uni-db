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
//! An integer compared with a float is widened to `f64` first, as
//! `cypher_eq` and `cypher_partial_cmp` do.

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

/// Build the comparison UDF for `op`: `(Float64, Float64) -> Boolean`.
///
/// `null` on either side gives `null`. The planner casts integer and `Float32`
/// operands to `Float64` before calling it.
pub fn create_cypher_float_compare_udf(op: FloatCmp) -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherFloatCompareUdf {
        op,
        signature: Signature::exact(
            vec![DataType::Float64, DataType::Float64],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherFloatCompareUdf {
    op: FloatCmp,
    signature: Signature,
}

/// The argument as a `Float64Array`, or as a scalar `Option<f64>`.
enum FloatArg {
    Array(ArrayRef),
    Scalar(Option<f64>),
}

fn float_arg(arg: &ColumnarValue, name: &str) -> DFResult<FloatArg> {
    match arg {
        ColumnarValue::Array(a) => {
            let a = if a.data_type() == &DataType::Float64 {
                a.clone()
            } else {
                cast(a, &DataType::Float64)?
            };
            Ok(FloatArg::Array(a))
        }
        ColumnarValue::Scalar(s) => match s.cast_to(&DataType::Float64)? {
            ScalarValue::Float64(v) => Ok(FloatArg::Scalar(v)),
            other => Err(datafusion::error::DataFusionError::Execution(format!(
                "{name}(): expected a float, got {other:?}"
            ))),
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
        let as_f64 = |a: &ArrayRef| -> DFResult<Float64Array> {
            a.as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "{name}(): expected Float64, got {}",
                        a.data_type()
                    ))
                })
        };
        let out = match (float_arg(l, name)?, float_arg(r, name)?) {
            (FloatArg::Scalar(l), FloatArg::Scalar(r)) => {
                let v = l.zip(r).map(|(l, r)| op.apply(l, r));
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(v)));
            }
            (FloatArg::Array(_), FloatArg::Scalar(None))
            | (FloatArg::Scalar(None), FloatArg::Array(_)) => {
                BooleanArray::new_null(args.number_rows)
            }
            (FloatArg::Array(a), FloatArg::Scalar(Some(s))) => {
                BooleanArray::from_unary(&as_f64(&a)?, |v| op.apply(v, s))
            }
            (FloatArg::Scalar(Some(s)), FloatArg::Array(a)) => {
                BooleanArray::from_unary(&as_f64(&a)?, |v| op.apply(s, v))
            }
            (FloatArg::Array(a), FloatArg::Array(b)) => {
                BooleanArray::from_binary(&as_f64(&a)?, &as_f64(&b)?, |l, r| op.apply(l, r))
            }
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
/// nested in a list or map. Every float inside is replaced by
/// [`canonical_float`] and the value re-encoded. Nested values are tagged
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

/// `value` with every float, at any depth of lists and maps, canonicalized.
fn canonical_value(value: Value, changed: &mut bool) -> Value {
    match value {
        Value::Float(f) => {
            let c = canonical_float(f);
            if c.to_bits() != f.to_bits() {
                *changed = true;
            }
            Value::Float(c)
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
