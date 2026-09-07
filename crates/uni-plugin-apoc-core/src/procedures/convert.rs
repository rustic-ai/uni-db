// Rust guideline compliant
//! `apoc.convert.*` analogue — type conversions over Cypher primitives.
//!
//! Mirrors Neo4j's `apoc.convert.*` namespace. These overlap somewhat
//! with Cypher's built-in `toString`/`toInteger`/`toFloat`/`toBoolean`
//! functions but are exposed as procedures (`CALL ... YIELD`) for
//! parity and for the cases where users want explicit conversion
//! semantics in the procedure-result shape.
//!
//! Initial set: `convert.toString`, `convert.toBoolean`,
//! `convert.toInteger`, `convert.toFloat`. NULL-tolerant: invalid
//! coercions yield NULL rather than erroring.
//!
//! Every argument is declared [`ArgType::CypherValue`], so a non-primitive
//! (list, map, node, …) arrives as an opaque `LargeBinary` envelope rather
//! than a typed `ScalarValue`. `convert.toString` decodes that envelope and
//! renders the value the way Neo4j does (`[1, 2, 3]`, `{k: v}`); it used to
//! fall through a catch-all and return NULL, turning real data into nulls in
//! any ported query that stringified a collection.
//!
//! `convert.toInteger` / `convert.toFloat` parse strings the way APOC's
//! `DecimalFormat`-backed conversions do — see `number`'s module docs — via
//! the shared `support::parse_i64_prefix` / `parse_f64_prefix`.

use std::sync::OnceLock;

use arrow_schema::{DataType, Field};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use uni_common::Value;

use super::support::{
    self, ApocProc, batch_err, nullable_bool_result, nullable_float_result, nullable_int_result,
    nullable_string_result,
};

/// Decode the `LargeBinary` envelope a non-primitive Cypher value arrives in.
///
/// Two encoders reach a plugin today and both are live:
///
/// * the procedure CALL path (`uni-query`'s `procedure_call::value_to_columnar`)
///   emits `serde_json` bytes, and
/// * the scalar-function adapter (`executor::plugin_adapter`) emits the tagged
///   `cypher_value_codec` encoding.
///
/// The two are unambiguous at the first byte — every codec tag is < 22, while
/// every byte JSON can start a document with is >= `b'"'` (34) — so trying the
/// codec first and falling back to JSON cannot mis-route a payload.
fn decode_cypher_envelope(bytes: &[u8]) -> Option<Value> {
    if let Ok(v) = uni_common::cypher_value_codec::decode(bytes) {
        return Some(v);
    }
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .map(Value::from)
}

/// Render a decoded Cypher value the way Neo4j's `apoc.convert.toString` does.
///
/// [`Value`]'s `Display` is already Cypher-shaped — a list renders as
/// `[1, 2, 3]` and a map as `{k: v}`, matching Java's `Collection::toString` —
/// so this only has to keep NULL as NULL rather than the literal `"null"`.
fn render_cypher_value(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// Register `uni.convert.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    support::register_all::<ConvertProc>(r)
}

fn build_sig(yields_type: DataType, arg_doc: &str, docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("value"),
            ty: ArgType::CypherValue,
            default: None,
            doc: arg_doc.to_owned(),
        }],
        yields: vec![Field::new("result", yields_type, true)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::enum_variant_names)] // matches APOC's naming: toString/toBoolean/...
enum ConvertProc {
    ToString,
    ToBoolean,
    ToInteger,
    ToFloat,
}

impl ConvertProc {
    fn arg_doc(&self) -> &'static str {
        match self {
            Self::ToString => "Value to coerce to string.",
            Self::ToBoolean => "Value to coerce to boolean.",
            Self::ToInteger => "Value to coerce to integer.",
            Self::ToFloat => "Value to coerce to float.",
        }
    }

    /// Canonical docstring per variant. The `register_into` versions
    /// were descriptive; we keep them over the `OnceLock` placeholders.
    fn docs(&self) -> &'static str {
        match self {
            Self::ToString => {
                "Coerce any Cypher value to its default string representation \
                 (a list renders as `[1, 2, 3]`, a map as `{k: v}`); NULL on null input."
            }
            Self::ToBoolean => {
                "Coerce a primitive to boolean. Strings 'true'/'false' match (case-insensitive); other strings yield NULL."
            }
            Self::ToInteger => {
                "Coerce a primitive to integer. Strings parse Java `DecimalFormat`-style \
                 (\"1,234\" -> 1234, \"3.7\" -> 3); NULL when no digits are present."
            }
            Self::ToFloat => {
                "Coerce a primitive to float. Strings parse Java `DecimalFormat`-style \
                 (\"1,234.5\" -> 1234.5); NULL when no digits are present."
            }
        }
    }

    fn yields_type(&self) -> DataType {
        match self {
            Self::ToString => DataType::Utf8,
            Self::ToBoolean => DataType::Boolean,
            Self::ToInteger => DataType::Int64,
            Self::ToFloat => DataType::Float64,
        }
    }
}

impl ApocProc for ConvertProc {
    const ALL: &'static [Self] = &[
        Self::ToString,
        Self::ToBoolean,
        Self::ToInteger,
        Self::ToFloat,
    ];

    fn qname(&self) -> QName {
        match self {
            Self::ToString => QName::new("apoc-core", "convert.toString"),
            Self::ToBoolean => QName::new("apoc-core", "convert.toBoolean"),
            Self::ToInteger => QName::new("apoc-core", "convert.toInteger"),
            Self::ToFloat => QName::new("apoc-core", "convert.toFloat"),
        }
    }

    fn index(&self) -> usize {
        *self as usize
    }

    fn build_signature(&self) -> ProcedureSignature {
        build_sig(self.yields_type(), self.arg_doc(), self.docs())
    }
}

impl ProcedurePlugin for ConvertProc {
    fn signature(&self) -> &ProcedureSignature {
        static CACHE: OnceLock<Vec<ProcedureSignature>> = OnceLock::new();
        support::cached_signature(&CACHE, self)
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let val = args.first().ok_or_else(|| {
            FnError::new(FnError::CODE_TYPE_COERCION, "convert: missing argument")
        })?;
        let scalar = match val {
            ColumnarValue::Scalar(s) => s.clone(),
            ColumnarValue::Array(_) => {
                return Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    "convert: array argument not supported in single-call procedure shape",
                ));
            }
        };
        let (schema, array) = match self {
            Self::ToString => {
                let result: Option<String> = match scalar {
                    ScalarValue::Null => None,
                    ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
                    ScalarValue::Int64(Some(i)) => Some(i.to_string()),
                    ScalarValue::Float64(Some(f)) => Some(f.to_string()),
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s),
                    // Non-primitives (list, map, node, …) transport as an
                    // opaque `LargeBinary` envelope. Decode and render rather
                    // than falling through to NULL.
                    ScalarValue::LargeBinary(Some(bytes)) => decode_cypher_envelope(&bytes)
                        .as_ref()
                        .and_then(render_cypher_value),
                    _ => None,
                };
                nullable_string_result(result)
            }
            Self::ToBoolean => {
                let result: Option<bool> = match scalar {
                    ScalarValue::Null => None,
                    ScalarValue::Boolean(b) => b,
                    ScalarValue::Int64(Some(i)) => Some(i != 0),
                    ScalarValue::Float64(Some(f)) => Some(f != 0.0),
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        match s.to_lowercase().as_str() {
                            "true" => Some(true),
                            "false" => Some(false),
                            _ => None,
                        }
                    }
                    _ => None,
                };
                nullable_bool_result(result)
            }
            Self::ToInteger => {
                let result: Option<i64> = match scalar {
                    ScalarValue::Null => None,
                    ScalarValue::Boolean(Some(b)) => Some(if b { 1 } else { 0 }),
                    ScalarValue::Int64(i) => i,
                    // `convert` guards `is_finite` before truncating, unlike
                    // `math`'s unguarded float→int truncation; preserved as-is.
                    ScalarValue::Float64(Some(f)) if f.is_finite() => Some(f as i64),
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        support::parse_i64_prefix(&s)
                    }
                    _ => None,
                };
                nullable_int_result(result)
            }
            Self::ToFloat => {
                let result: Option<f64> = match scalar {
                    ScalarValue::Null => None,
                    ScalarValue::Boolean(Some(b)) => Some(if b { 1.0 } else { 0.0 }),
                    ScalarValue::Int64(Some(i)) => Some(i as f64),
                    ScalarValue::Float64(f) => f,
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        support::parse_f64_prefix(&s)
                    }
                    _ => None,
                };
                nullable_float_result(result)
            }
        };
        support::one_row_stream(schema, array, batch_err::CONVERT, "convert")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
    use futures::StreamExt;

    async fn invoke(proc: ConvertProc, scalar: ScalarValue) -> RecordBatch {
        let mut stream = proc
            .invoke(
                ProcedureContext::default(),
                &[ColumnarValue::Scalar(scalar)],
            )
            .unwrap();
        stream.next().await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn to_string_int() {
        let b = invoke(ConvertProc::ToString, ScalarValue::Int64(Some(42))).await;
        let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.value(0), "42");
    }

    #[tokio::test]
    async fn to_boolean_string_true() {
        let b = invoke(
            ConvertProc::ToBoolean,
            ScalarValue::Utf8(Some("TRUE".into())),
        )
        .await;
        let a = b.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(a.value(0));
    }

    #[tokio::test]
    async fn to_integer_float_truncates() {
        let b = invoke(ConvertProc::ToInteger, ScalarValue::Float64(Some(3.9))).await;
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), 3);
    }

    #[tokio::test]
    async fn to_float_int_widens() {
        let b = invoke(ConvertProc::ToFloat, ScalarValue::Int64(Some(7))).await;
        let a = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((a.value(0) - 7.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn to_integer_unparseable_string_returns_null() {
        let b = invoke(
            ConvertProc::ToInteger,
            ScalarValue::Utf8(Some("not-a-number".into())),
        )
        .await;
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(a.is_null(0));
    }
}
