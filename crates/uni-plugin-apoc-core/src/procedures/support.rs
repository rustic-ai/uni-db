//! Shared plumbing for the APOC-core procedure modules.
//!
//! Each `apoc.*` namespace module (`bitwise`, `text`, `math`, …) models its
//! procedures as a `Copy` discriminant enum implementing [`ApocProc`]. This
//! module factors out the machinery that would otherwise be copy-pasted into
//! every file: the registration loop, the per-enum signature cache, the
//! columnar-argument extractors, the single-row Arrow result builders, and the
//! `RecordBatch` → stream tail.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::adapter_common::batch_builder::{batch_into_stream, single_row_record_batch};
use uni_plugin::traits::procedure::{ProcedurePlugin, ProcedureSignature};
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName};

/// Upper bound on the length of a synthesized string output (`text.repeat`'s
/// total length, `create.uuids`' row count). Caps pathological inputs so a
/// single call cannot exhaust memory.
pub(super) const MAX_SYNTHESIZED_LEN: usize = 1_000_000;

/// `FnError` codes for the `RecordBatch::try_new` failure tail, one per module.
/// These only fire on an Arrow schema/array mismatch (an internal invariant
/// violation), never on user input.
pub(super) mod batch_err {
    /// `bitwise.*` result-batch construction failure.
    pub(crate) const BITWISE: u32 = 0x700;
    /// `text.*` result-batch construction failure.
    pub(crate) const TEXT: u32 = 0x701;
    /// `math.*` result-batch construction failure.
    pub(crate) const MATH: u32 = 0x702;
    /// `number.*` result-batch construction failure.
    pub(crate) const NUMBER: u32 = 0x703;
    /// `convert.*` result-batch construction failure.
    pub(crate) const CONVERT: u32 = 0x704;
    /// `create.*` result-batch construction failure.
    pub(crate) const CREATE: u32 = 0x705;
}

/// `math.coth` undefined-at-zero error code.
pub(super) const CODE_MATH_DOMAIN: u32 = 0x800;

/// A discriminant enum describing one APOC-core namespace's procedures.
///
/// Implementors are `Copy` unit-like enums; the blanket [`register_all`] loop
/// registers every variant with its cached signature.
pub(super) trait ApocProc: ProcedurePlugin + Copy + 'static {
    /// Every variant of this namespace, in registration order.
    const ALL: &'static [Self];

    /// Fully-qualified name (`apoc-core` plugin id + local path).
    fn qname(&self) -> QName;

    /// Position of this variant within [`ALL`](Self::ALL); used to index the
    /// per-enum signature cache.
    fn index(&self) -> usize;

    /// Build this variant's signature from scratch (called at most once per
    /// variant, on first cache miss).
    fn build_signature(&self) -> ProcedureSignature;
}

/// Look up `proc`'s signature in `cache`, materializing the whole `ALL` table
/// on first use. The cache holds one entry per variant, indexed by
/// [`ApocProc::index`], so a namespace pays a single allocation pass rather
/// than one `OnceLock` static per variant.
pub(super) fn cached_signature<P: ApocProc>(
    cache: &'static OnceLock<Vec<ProcedureSignature>>,
    proc: &P,
) -> &'static ProcedureSignature {
    let sigs = cache.get_or_init(|| P::ALL.iter().map(ApocProc::build_signature).collect());
    &sigs[proc.index()]
}

/// Register every variant of `P` into `r` using its cached signature.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub(super) fn register_all<P: ApocProc>(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    for proc in P::ALL {
        r.procedure(proc.qname(), proc.signature().clone(), Arc::new(*proc))?;
    }
    Ok(())
}

/// Wrap a single-row `(schema, array)` pair into the one-batch stream every
/// procedure returns. `batch_code`/`label` tag the (invariant-only) failure.
pub(super) fn one_row_stream(
    schema: SchemaRef,
    array: Arc<dyn Array>,
    batch_code: u32,
    label: &str,
) -> Result<SendableRecordBatchStream, FnError> {
    let batch = single_row_record_batch(schema, vec![array])
        .map_err(|e| FnError::new(batch_code, format!("{label}: {e}")))?;
    Ok(batch_into_stream(batch))
}

/// Single-column `result` schema of the given type and nullability.
fn result_schema(ty: DataType, nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("result", ty, nullable)]))
}

/// Non-null single-row `Utf8` result.
pub(super) fn string_result(s: String) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(StringArray::from(vec![s])) as Arc<dyn Array>;
    (result_schema(DataType::Utf8, false), arr)
}

/// Non-null single-row `Boolean` result.
pub(super) fn bool_result(b: bool) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(BooleanArray::from(vec![b])) as Arc<dyn Array>;
    (result_schema(DataType::Boolean, false), arr)
}

/// Non-null single-row `Int64` result.
pub(super) fn int_result(n: i64) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(Int64Array::from(vec![n])) as Arc<dyn Array>;
    (result_schema(DataType::Int64, false), arr)
}

/// Non-null single-row `Float64` result.
pub(super) fn float_result(v: f64) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(Float64Array::from(vec![v])) as Arc<dyn Array>;
    (result_schema(DataType::Float64, false), arr)
}

/// Nullable single-row `Utf8` result.
pub(super) fn nullable_string_result(s: Option<String>) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(StringArray::from(vec![s])) as Arc<dyn Array>;
    (result_schema(DataType::Utf8, true), arr)
}

/// Nullable single-row `Boolean` result.
pub(super) fn nullable_bool_result(b: Option<bool>) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(BooleanArray::from(vec![b])) as Arc<dyn Array>;
    (result_schema(DataType::Boolean, true), arr)
}

/// Nullable single-row `Int64` result.
pub(super) fn nullable_int_result(i: Option<i64>) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(Int64Array::from(vec![i])) as Arc<dyn Array>;
    (result_schema(DataType::Int64, true), arr)
}

/// Nullable single-row `Float64` result.
pub(super) fn nullable_float_result(f: Option<f64>) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(Float64Array::from(vec![f])) as Arc<dyn Array>;
    (result_schema(DataType::Float64, true), arr)
}

/// How a `Float64` argument is coerced when an integer is requested.
///
/// The APOC namespaces deliberately disagree on this — see [`extract_i64`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FloatToInt {
    /// Reject any float argument (`bitwise`, `text`).
    Reject,
    /// Silently truncate toward zero (`math`, matching Java APOC).
    Truncate,
}

/// Extract a non-null `i64` from `args[idx]`.
///
/// `label` prefixes every error message. `float_policy` and `accept_array`
/// reproduce the historically divergent behavior of the three original copies
/// (math truncated floats and rejected arrays; bitwise rejected floats but
/// accepted an `Int64Array`; text rejected both) — they are explicit
/// parameters precisely so each call site keeps its prior semantics.
pub(super) fn extract_i64(
    args: &[ColumnarValue],
    idx: usize,
    label: &str,
    float_policy: FloatToInt,
    accept_array: bool,
) -> Result<i64, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v)))
            if float_policy == FloatToInt::Truncate =>
        {
            Ok(*v as i64)
        }
        ColumnarValue::Array(arr) if accept_array => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "expected Int64Array"))?;
            if a.is_empty() || a.is_null(0) {
                Err(FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("{label}: integer argument must not be null"),
                ))
            } else {
                Ok(a.value(0))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: integer argument required"),
        )),
    }
}

/// Extract a non-null `f64` from `args[idx]`. Accepts `Int64` (widened) and
/// `Float64` scalars and `Float64Array`. `label` prefixes every error message.
pub(super) fn extract_f64(args: &[ColumnarValue], idx: usize, label: &str) -> Result<f64, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v as f64),
        ColumnarValue::Array(arr) => {
            if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                if a.is_empty() || a.is_null(0) {
                    Err(FnError::new(
                        FnError::CODE_UNEXPECTED_NULL,
                        format!("{label}: numeric argument must not be null"),
                    ))
                } else {
                    Ok(a.value(0))
                }
            } else {
                Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("{label}: expected Float64Array"),
                ))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: numeric argument required"),
        )),
    }
}

/// Extract a non-null `String` from `args[idx]`.
///
/// `accept_array` selects whether a `StringArray` first element is accepted
/// (`text` does; `number` only takes scalars). `label` prefixes errors.
pub(super) fn extract_string(
    args: &[ColumnarValue],
    idx: usize,
    label: &str,
    accept_array: bool,
) -> Result<String, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
            Ok(s.clone())
        }
        ColumnarValue::Array(arr) if accept_array => {
            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                if a.is_empty() || a.is_null(0) {
                    Err(FnError::new(
                        FnError::CODE_UNEXPECTED_NULL,
                        format!("{label}: string argument must not be null"),
                    ))
                } else {
                    Ok(a.value(0).to_owned())
                }
            } else {
                Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("{label}: expected StringArray"),
                ))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: string argument required"),
        )),
    }
}

/// Reject a synthesis request whose output would exceed a cap.
///
/// `text.repeat` and `create.uuids` both bound their output at
/// [`MAX_SYNTHESIZED_LEN`]. Both used to clamp the request with `min()`, so an
/// over-cap call returned a *truncated* result indistinguishable from a
/// complete one — `apoc.create.uuids(2_000_000)` quietly yielded 1,000,000
/// rows. The cap itself is fine; the silence was the defect. Naming the cap in
/// an error is the only way a caller can tell a full answer from a clipped one.
///
/// `requested` and `max` are in `unit`s (repetitions, rows); `max` is derived
/// from [`MAX_SYNTHESIZED_LEN`], which the message also names so the operator
/// can see where the bound comes from.
///
/// # Errors
///
/// Returns [`FnError::CODE_RESOURCE_LIMIT`] when `requested` exceeds `max`.
///
/// # Examples
///
/// ```ignore
/// reject_over_cap("create.uuids", "UUIDs", 5, 10)?; // ok
/// assert!(reject_over_cap("create.uuids", "UUIDs", 20, 10).is_err());
/// ```
pub(super) fn reject_over_cap(
    label: &str,
    unit: &str,
    requested: u64,
    max: u64,
) -> Result<(), FnError> {
    if requested > max {
        return Err(FnError::new(
            FnError::CODE_RESOURCE_LIMIT,
            format!(
                "{label}: requested {requested} {unit} exceeds the cap of {max} \
                 (MAX_SYNTHESIZED_LEN = {MAX_SYNTHESIZED_LEN}); \
                 split the call into smaller batches"
            ),
        ));
    }
    Ok(())
}

/// A numeric prefix scanned out of a string, normalized for `str::parse`.
struct DecimalPrefix {
    /// Sign + digits (+ optional fraction / exponent), grouping separators
    /// removed. Always parseable by `f64::from_str`.
    cleaned: String,
    /// `true` when the prefix carried neither a fraction nor an exponent, so
    /// it can be parsed as an `i64` without going through `f64`.
    is_integral: bool,
}

/// Scan the leading numeric prefix of `s`, APOC/`DecimalFormat`-style.
///
/// Java's `DecimalFormat.parse` — what Neo4j's APOC uses — accepts grouping
/// separators and stops at the first character it cannot consume, rather than
/// demanding that the whole string be numeric. `"1,234"` parses as 1234 and
/// `"3.7px"` as 3.7. Genuine garbage (no digits at all) still yields `None`.
///
/// Accepts: optional `+`/`-`, ASCII digits with `,` grouping separators
/// between digits, an optional `.`-fraction, and an optional `e`/`E` exponent.
fn scan_decimal_prefix(s: &str) -> Option<DecimalPrefix> {
    let chars: Vec<char> = s.trim().chars().collect();
    let len = chars.len();
    let mut i = 0usize;
    let mut out = String::with_capacity(len);

    if let Some(&c) = chars.first()
        && (c == '+' || c == '-')
    {
        if c == '-' {
            out.push('-');
        }
        i = 1;
    }

    let mut digits = 0usize;
    while i < len {
        let c = chars[i];
        if c.is_ascii_digit() {
            out.push(c);
            digits += 1;
            i += 1;
        } else if c == ',' && digits > 0 && chars.get(i + 1).is_some_and(char::is_ascii_digit) {
            // A grouping separator: consumed but not emitted.
            i += 1;
        } else {
            break;
        }
    }
    if digits == 0 {
        return None;
    }

    let mut is_integral = true;

    if chars.get(i) == Some(&'.') && chars.get(i + 1).is_some_and(char::is_ascii_digit) {
        is_integral = false;
        out.push('.');
        i += 1;
        while i < len && chars[i].is_ascii_digit() {
            out.push(chars[i]);
            i += 1;
        }
    }

    if matches!(chars.get(i), Some('e' | 'E')) {
        let mut j = i + 1;
        let sign = match chars.get(j) {
            Some(&c @ ('+' | '-')) => {
                j += 1;
                Some(c)
            }
            _ => None,
        };
        if chars.get(j).is_some_and(char::is_ascii_digit) {
            is_integral = false;
            out.push('e');
            if let Some(c) = sign {
                out.push(c);
            }
            i = j;
            while i < len && chars[i].is_ascii_digit() {
                out.push(chars[i]);
                i += 1;
            }
        }
    }

    Some(DecimalPrefix {
        cleaned: out,
        is_integral,
    })
}

/// Parse the leading numeric prefix of `s` as an `i64`, truncating toward zero.
///
/// Matches APOC's `DecimalFormat`-backed behavior: `"3.7"` → 3, `"1,234"` →
/// 1234, `"12abc"` → 12. Returns `None` for genuine garbage (no digits), for a
/// non-finite value, and for anything outside the `i64` range — NULL on
/// garbage is correct APOC behavior and is preserved.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(parse_i64_prefix("3.7"), Some(3));
/// assert_eq!(parse_i64_prefix("-3.7"), Some(-3));
/// assert_eq!(parse_i64_prefix("1,234"), Some(1234));
/// assert_eq!(parse_i64_prefix("not a number"), None);
/// ```
pub(super) fn parse_i64_prefix(s: &str) -> Option<i64> {
    let prefix = scan_decimal_prefix(s)?;
    if prefix.is_integral {
        return prefix.cleaned.parse::<i64>().ok();
    }
    let f = prefix.cleaned.parse::<f64>().ok()?;
    if !f.is_finite() {
        return None;
    }
    let truncated = f.trunc();
    // `i64::MAX as f64` rounds up, so compare against the exclusive bound.
    if truncated < -(2f64.powi(63)) || truncated >= 2f64.powi(63) {
        return None;
    }
    Some(truncated as i64)
}

/// Parse the leading numeric prefix of `s` as an `f64`.
///
/// Matches APOC's `DecimalFormat`-backed behavior: `"1,234.5"` → 1234.5,
/// `"3.7px"` → 3.7. Returns `None` for genuine garbage (no digits).
///
/// # Examples
///
/// ```ignore
/// assert_eq!(parse_f64_prefix("1,234.5"), Some(1234.5));
/// assert_eq!(parse_f64_prefix("nope"), None);
/// ```
pub(super) fn parse_f64_prefix(s: &str) -> Option<f64> {
    scan_decimal_prefix(s).and_then(|p| p.cleaned.parse::<f64>().ok())
}

#[cfg(test)]
mod support_tests {
    use super::*;

    #[test]
    fn decimal_prefix_int_rules() {
        assert_eq!(parse_i64_prefix("42"), Some(42));
        assert_eq!(parse_i64_prefix("3.7"), Some(3));
        assert_eq!(parse_i64_prefix("-3.7"), Some(-3));
        assert_eq!(parse_i64_prefix("1,234"), Some(1234));
        assert_eq!(parse_i64_prefix("  1,234,567  "), Some(1_234_567));
        assert_eq!(parse_i64_prefix("12abc"), Some(12));
        // Garbage stays NULL — the APOC contract.
        assert_eq!(parse_i64_prefix("not a number"), None);
        assert_eq!(parse_i64_prefix(""), None);
        assert_eq!(parse_i64_prefix("-"), None);
        assert_eq!(parse_i64_prefix(",123"), None);
    }

    #[test]
    fn decimal_prefix_float_rules() {
        assert_eq!(parse_f64_prefix("2.5"), Some(2.5));
        assert_eq!(parse_f64_prefix("1,234.5"), Some(1234.5));
        assert_eq!(parse_f64_prefix("3.7px"), Some(3.7));
        assert_eq!(parse_f64_prefix("1e3"), Some(1000.0));
        assert_eq!(parse_f64_prefix("nope"), None);
    }

    #[test]
    fn over_cap_is_rejected_and_named() {
        assert!(reject_over_cap("x", "rows", 5, 10).is_ok());
        let err = reject_over_cap("x", "rows", 20, 10).expect_err("over cap");
        assert!(err.to_string().contains("10"), "{err}");
    }
}
