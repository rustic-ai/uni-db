// Rust guideline compliant
//! `apoc.number.*` analogue — number formatting and parsing.
//!
//! Initial set: `number.parseInt`, `number.parseFloat`,
//! `number.toString`. More formatting (pattern-based) lands as a
//! follow-up — Rust has no direct equivalent of Java's `DecimalFormat`
//! patterns, so format procedures use the `format!` syntax.
//!
//! **Parsing**, however, does follow `DecimalFormat`. APOC's `parseInt` /
//! `parseFloat` are backed by `DecimalFormat.parse`, which accepts grouping
//! separators and stops at the first character it cannot consume rather than
//! demanding the whole string be numeric: `"1,234"` → 1234 and `"3.7"` → 3
//! (integer variants truncate toward zero). This module used to call
//! `str::parse` and returned NULL for both. Genuine garbage — a string with
//! no digits at all — still yields NULL. `support::parse_i64_prefix` /
//! `parse_f64_prefix` are the single implementation, shared with
//! `convert.toInteger` / `convert.toFloat`.

use std::sync::OnceLock;

use arrow_schema::{DataType, Field};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use super::support::{
    self, ApocProc, batch_err, nullable_float_result, nullable_int_result, string_result,
};

/// Register `uni.number.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    support::register_all::<NumberProc>(r)
}

fn parse_int_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("text"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "String representation of an integer.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Int64, true)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn parse_float_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("text"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "String representation of a float.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Float64, true)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn float_to_string_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("value"),
            ty: ArgType::Primitive(DataType::Float64),
            default: None,
            doc: "Numeric value to format.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

#[derive(Debug, Clone, Copy)]
enum NumberProc {
    ParseInt,
    ParseFloat,
    ToString,
}

impl NumberProc {
    /// Canonical docstring per variant. The `register_into` versions
    /// were descriptive ("Parse a string as a 64-bit signed integer; ...")
    /// whereas the `OnceLock` versions were placeholders ("parseInt").
    /// We keep the descriptive ones as canonical.
    fn docs(&self) -> &'static str {
        match self {
            Self::ParseInt => {
                "Parse a string as a 64-bit signed integer, Java `DecimalFormat`-style: \
                 grouping separators are accepted and a leading numeric prefix is taken \
                 (\"1,234\" -> 1234, \"3.7\" -> 3). NULL when the string holds no digits."
            }
            Self::ParseFloat => {
                "Parse a string as a 64-bit float, Java `DecimalFormat`-style: grouping \
                 separators are accepted and a leading numeric prefix is taken \
                 (\"1,234.5\" -> 1234.5). NULL when the string holds no digits."
            }
            Self::ToString => "Format a number as its default string representation.",
        }
    }
}

impl ApocProc for NumberProc {
    const ALL: &'static [Self] = &[Self::ParseInt, Self::ParseFloat, Self::ToString];

    fn qname(&self) -> QName {
        match self {
            Self::ParseInt => QName::new("apoc-core", "number.parseInt"),
            Self::ParseFloat => QName::new("apoc-core", "number.parseFloat"),
            Self::ToString => QName::new("apoc-core", "number.toString"),
        }
    }

    fn index(&self) -> usize {
        *self as usize
    }

    fn build_signature(&self) -> ProcedureSignature {
        match self {
            Self::ParseInt => parse_int_sig(self.docs()),
            Self::ParseFloat => parse_float_sig(self.docs()),
            Self::ToString => float_to_string_sig(self.docs()),
        }
    }
}

impl ProcedurePlugin for NumberProc {
    fn signature(&self) -> &ProcedureSignature {
        static CACHE: OnceLock<Vec<ProcedureSignature>> = OnceLock::new();
        support::cached_signature(&CACHE, self)
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        // `number` only accepts scalar arguments (no array fast-path).
        let (schema, array) = match self {
            Self::ParseInt => {
                let s = support::extract_string(args, 0, "number", false)?;
                let parsed: Option<i64> = support::parse_i64_prefix(&s);
                nullable_int_result(parsed)
            }
            Self::ParseFloat => {
                let s = support::extract_string(args, 0, "number", false)?;
                let parsed: Option<f64> = support::parse_f64_prefix(&s);
                nullable_float_result(parsed)
            }
            Self::ToString => {
                // Format an Int64 argument EXACTLY. Widening it through f64
                // (extract_f64's `*v as f64`) corrupts integers above 2^53
                // (finding [2]); only genuine floats take the f64 path.
                let s = match support::extract_i64(
                    args,
                    0,
                    "number",
                    support::FloatToInt::Reject,
                    true,
                ) {
                    Ok(i) => i.to_string(),
                    Err(_) => format!("{}", support::extract_f64(args, 0, "number")?),
                };
                string_result(s)
            }
        };
        support::one_row_stream(schema, array, batch_err::NUMBER, "number")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, Int64Array, StringArray};
    use datafusion::scalar::ScalarValue;
    use futures::StreamExt;

    #[tokio::test]
    async fn parse_int_succeeds_on_valid_int() {
        let cols = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "42".to_owned(),
        )))];
        let mut stream = NumberProc::ParseInt
            .invoke(ProcedureContext::default(), &cols)
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[tokio::test]
    async fn parse_int_returns_null_on_failure() {
        let cols = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "not a number".to_owned(),
        )))];
        let mut stream = NumberProc::ParseInt
            .invoke(ProcedureContext::default(), &cols)
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(col.is_null(0));
    }

    #[tokio::test]
    async fn parse_float_succeeds_on_valid_float() {
        let cols = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "2.5".to_owned(),
        )))];
        let mut stream = NumberProc::ParseFloat
            .invoke(ProcedureContext::default(), &cols)
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 2.5).abs() < 1e-12);
    }

    #[tokio::test]
    async fn to_string_formats_float() {
        let cols = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(2.5)))];
        let mut stream = NumberProc::ToString
            .invoke(ProcedureContext::default(), &cols)
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "2.5");
    }
}
