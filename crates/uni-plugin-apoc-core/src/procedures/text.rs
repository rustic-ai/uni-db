// Rust guideline compliant
//! `apoc.text.*` analogue — text manipulation procedures.
//!
//! Mirrors Neo4j's `apoc.text.*` namespace. These are perf-critical
//! enough to live in `uni-plugin-apoc-core` (Rust) rather than the Lua
//! `uni-plugin-apoc-ext` companion — string transformations called per
//! row in large scans would be throughput-fatal across a Lua-host
//! boundary.
//!
//! Initial set: `text.toUpper`, `text.toLower`, `text.replace`,
//! `text.reverse`. Additional procedures (`text.split`, `text.regexGroups`,
//! `text.distance`, `text.fuzzyMatch`, etc.) land as user demand surfaces.
//!
//! Index units are uniform across this module: every position and length is
//! counted in Unicode scalar values (chars), matching Neo4j/Java. `text.length`
//! and `text.indexOf` are therefore interchangeable units — `indexOf` used to
//! return a UTF-8 **byte** offset (Rust's `str::find`), which disagreed with
//! both `text.length` and Neo4j (`indexOf('cafés','s')` gave 5, not 4).

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
    self, ApocProc, FloatToInt, MAX_SYNTHESIZED_LEN, batch_err, bool_result, int_result,
    string_result,
};

/// Register `uni.text.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    support::register_all::<TextProc>(r)
}

fn binary_str_bool_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("text"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Input string.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("substring"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "String to test against.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Boolean, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn unary_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("text"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "Input string.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn ternary_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("text"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Input string.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("search"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Substring to find.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("replacement"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Replacement string.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn length_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("text"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "Input string.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Int64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn repeat_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("text"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Input string.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("count"),
                ty: ArgType::Primitive(DataType::Int64),
                default: None,
                doc: "How many times to repeat.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn index_of_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("text"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "String to search in.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("substring"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "String to find.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Int64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

/// All text procedures via one discriminant.
#[derive(Debug, Clone, Copy)]
enum TextProc {
    ToUpper,
    ToLower,
    Replace,
    Reverse,
    Trim,
    LTrim,
    RTrim,
    Contains,
    StartsWith,
    EndsWith,
    Length,
    Repeat,
    IndexOf,
}

impl TextProc {
    /// Canonical docstring per variant. The `register_into` strings
    /// were descriptive (e.g. "Uppercase a string.") while the
    /// `OnceLock` placeholders were terse ("Uppercase."). We keep the
    /// descriptive form.
    fn docs(&self) -> &'static str {
        match self {
            Self::ToUpper => "Uppercase a string.",
            Self::ToLower => "Lowercase a string.",
            Self::Replace => "Replace every occurrence of `search` in `text` with `replacement`.",
            Self::Reverse => "Reverse a string by Unicode scalar values.",
            Self::Trim => "Trim Unicode whitespace from both ends of a string.",
            Self::LTrim => "Trim Unicode whitespace from the left end of a string.",
            Self::RTrim => "Trim Unicode whitespace from the right end of a string.",
            Self::Contains => "Returns true if `text` contains `substring`.",
            Self::StartsWith => "Returns true if `text` starts with `prefix`.",
            Self::EndsWith => "Returns true if `text` ends with `suffix`.",
            Self::Length => "Length of a string in Unicode scalar values (chars).",
            Self::Repeat => {
                "Repeat `text` `count` times. Errors if the result would exceed the \
                 1,000,000-character synthesis cap."
            }
            Self::IndexOf => {
                "Character index of the first occurrence of `substring` in `text`, or -1 if \
                 not present. Counted in Unicode scalar values, same unit as `text.length`."
            }
        }
    }
}

impl ApocProc for TextProc {
    const ALL: &'static [Self] = &[
        Self::ToUpper,
        Self::ToLower,
        Self::Replace,
        Self::Reverse,
        Self::Trim,
        Self::LTrim,
        Self::RTrim,
        Self::Contains,
        Self::StartsWith,
        Self::EndsWith,
        Self::Length,
        Self::Repeat,
        Self::IndexOf,
    ];

    fn qname(&self) -> QName {
        match self {
            Self::ToUpper => QName::new("apoc-core", "text.toUpper"),
            Self::ToLower => QName::new("apoc-core", "text.toLower"),
            Self::Replace => QName::new("apoc-core", "text.replace"),
            Self::Reverse => QName::new("apoc-core", "text.reverse"),
            Self::Trim => QName::new("apoc-core", "text.trim"),
            Self::LTrim => QName::new("apoc-core", "text.lTrim"),
            Self::RTrim => QName::new("apoc-core", "text.rTrim"),
            Self::Contains => QName::new("apoc-core", "text.contains"),
            Self::StartsWith => QName::new("apoc-core", "text.startsWith"),
            Self::EndsWith => QName::new("apoc-core", "text.endsWith"),
            Self::Length => QName::new("apoc-core", "text.length"),
            Self::Repeat => QName::new("apoc-core", "text.repeat"),
            Self::IndexOf => QName::new("apoc-core", "text.indexOf"),
        }
    }

    fn index(&self) -> usize {
        *self as usize
    }

    fn build_signature(&self) -> ProcedureSignature {
        match self {
            Self::ToUpper
            | Self::ToLower
            | Self::Reverse
            | Self::Trim
            | Self::LTrim
            | Self::RTrim => unary_sig(self.docs()),
            Self::Replace => ternary_sig(self.docs()),
            Self::Contains | Self::StartsWith | Self::EndsWith => binary_str_bool_sig(self.docs()),
            Self::Length => length_sig(self.docs()),
            Self::Repeat => repeat_sig(self.docs()),
            Self::IndexOf => index_of_sig(self.docs()),
        }
    }
}

impl ProcedurePlugin for TextProc {
    fn signature(&self) -> &ProcedureSignature {
        static CACHE: OnceLock<Vec<ProcedureSignature>> = OnceLock::new();
        support::cached_signature(&CACHE, self)
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        // `text` accepts a string array's first element; integer args
        // (`repeat`'s count) must be scalar and reject floats.
        let str_arg = |idx| support::extract_string(args, idx, "text", true);
        // Each branch builds a 1-row batch with the procedure's declared
        // yield schema. Boolean/Int outputs avoid the string path.
        let (schema, array) = match self {
            Self::ToUpper => string_result(str_arg(0)?.to_uppercase()),
            Self::ToLower => string_result(str_arg(0)?.to_lowercase()),
            Self::Reverse => string_result(str_arg(0)?.chars().rev().collect()),
            Self::Replace => {
                let text = str_arg(0)?;
                let search = str_arg(1)?;
                let replacement = str_arg(2)?;
                string_result(text.replace(&search, &replacement))
            }
            Self::Trim => string_result(str_arg(0)?.trim().to_owned()),
            Self::LTrim => string_result(str_arg(0)?.trim_start().to_owned()),
            Self::RTrim => string_result(str_arg(0)?.trim_end().to_owned()),
            Self::Contains => bool_result(str_arg(0)?.contains(str_arg(1)?.as_str())),
            Self::StartsWith => bool_result(str_arg(0)?.starts_with(str_arg(1)?.as_str())),
            Self::EndsWith => bool_result(str_arg(0)?.ends_with(str_arg(1)?.as_str())),
            Self::Length => int_result(str_arg(0)?.chars().count() as i64),
            Self::Repeat => {
                let s = str_arg(0)?;
                let count = support::extract_i64(args, 1, "text", FloatToInt::Reject, false)?;
                if count < 0 {
                    return Err(FnError::new(
                        FnError::CODE_TYPE_COERCION,
                        "text.repeat: count must be non-negative",
                    ));
                }
                // Cap on the total synthesized *length* (`count * item_len`),
                // not the raw count, so `MAX_SYNTHESIZED_LEN` actually bounds the
                // output size and prevents OOM on pathological inputs. An empty
                // base yields an empty string for any count, so it needs no cap.
                //
                // The cap is enforced by *erroring*, not by clamping: a silent
                // `min()` returned a truncated string the caller could not tell
                // apart from a complete one. Same defect, same remedy, same
                // helper as `create.uuids`.
                let item_len = s.len();
                if let Some(max_reps) = MAX_SYNTHESIZED_LEN.checked_div(item_len) {
                    support::reject_over_cap(
                        "text.repeat",
                        "repetitions",
                        count.unsigned_abs(),
                        max_reps as u64,
                    )?;
                }
                string_result(s.repeat(count as usize))
            }
            Self::IndexOf => {
                let haystack = str_arg(0)?;
                let needle = str_arg(1)?;
                // Neo4j's `apoc.text.indexOf` returns a CHARACTER index, and
                // `text.length` here already counts characters. `str::find`
                // reports a UTF-8 byte offset, so convert: count the chars in
                // the prefix that precedes the match.
                let idx = haystack.find(needle.as_str()).map_or(-1, |byte_offset| {
                    haystack[..byte_offset].chars().count() as i64
                });
                int_result(idx)
            }
        };
        support::one_row_stream(schema, array, batch_err::TEXT, "text")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use datafusion::scalar::ScalarValue;
    use futures::StreamExt;

    async fn invoke_one(proc: TextProc, args: Vec<&str>) -> String {
        let cols: Vec<ColumnarValue> = args
            .into_iter()
            .map(|v| ColumnarValue::Scalar(ScalarValue::Utf8(Some(v.to_owned()))))
            .collect();
        let mut stream = proc.invoke(ProcedureContext::default(), &cols).unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        col.value(0).to_owned()
    }

    #[tokio::test]
    async fn to_upper_uppercases() {
        assert_eq!(invoke_one(TextProc::ToUpper, vec!["hello"]).await, "HELLO");
    }

    #[tokio::test]
    async fn to_lower_lowercases() {
        assert_eq!(invoke_one(TextProc::ToLower, vec!["HELLO"]).await, "hello");
    }

    #[tokio::test]
    async fn reverse_reverses() {
        assert_eq!(invoke_one(TextProc::Reverse, vec!["abc"]).await, "cba");
    }

    #[tokio::test]
    async fn replace_replaces_substring() {
        assert_eq!(
            invoke_one(TextProc::Replace, vec!["foo bar foo", "foo", "baz"]).await,
            "baz bar baz"
        );
    }

    #[tokio::test]
    async fn reverse_handles_unicode() {
        // Reversing by chars (Unicode scalars), not bytes.
        assert_eq!(invoke_one(TextProc::Reverse, vec!["café"]).await, "éfac");
    }
}
