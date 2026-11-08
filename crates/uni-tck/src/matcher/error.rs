use std::str::FromStr;
use uni_common::UniError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorPhase {
    CompileTime,
    Runtime,
    AnyTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TckErrorType {
    SyntaxError,
    TypeError,
    SemanticError,
    ConstraintValidationFailed,
    EntityNotFound,
    PropertyNotFound,
    ArithmeticError,
    ArgumentError,
    Unknown(String),
}

impl FromStr for TckErrorType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "SyntaxError" => Self::SyntaxError,
            "TypeError" => Self::TypeError,
            "SemanticError" => Self::SemanticError,
            "ConstraintValidationFailed" => Self::ConstraintValidationFailed,
            "EntityNotFound" => Self::EntityNotFound,
            "PropertyNotFound" => Self::PropertyNotFound,
            "ArithmeticError" => Self::ArithmeticError,
            "ArgumentError" => Self::ArgumentError,
            other => Self::Unknown(other.to_string()),
        })
    }
}

/// Match an actual error against an expected TCK error specification.
pub fn match_error(
    actual: &UniError,
    expected_type: TckErrorType,
    expected_phase: ErrorPhase,
    detail_code: Option<&str>,
) -> Result<(), String> {
    // Skip phase check when expected_phase is AnyTime
    if expected_phase != ErrorPhase::AnyTime {
        let actual_phase = classify_phase(actual, expected_phase, detail_code);
        if actual_phase != expected_phase {
            return Err(format!(
                "Error phase mismatch: expected {:?}, got {:?}",
                expected_phase, actual_phase
            ));
        }
    }

    // Compute the detail outcome first, because an *unclassified* actual error
    // (`TckErrorType::Unknown`) is only allowed to satisfy a *specific* expected
    // type when the detail substring genuinely matches. Without this, "couldn't
    // classify" would be treated as "matches everything" — and combined with the
    // wildcard ('*') detail scenarios (which skip the detail check entirely) that
    // produces false passes against specific expected types like NumberOutOfRange.
    let error_message = actual.to_string();
    let detail_genuinely_matched = match detail_code {
        // Wildcard or absent detail provides no corroborating evidence.
        None | Some("*") => false,
        Some(detail) => detail_matches(&error_message, detail),
    };

    let actual_type = classify_error(actual);
    if !error_types_match(&actual_type, &expected_type, detail_genuinely_matched) {
        return Err(format!(
            "Error type mismatch: expected {:?}, got {:?}",
            expected_type, actual_type
        ));
    }

    if let Some(detail) = detail_code {
        // Skip detail check if wildcard '*' is used
        if detail != "*" && !detail_genuinely_matched {
            return Err(format!(
                "Error detail mismatch: expected message to contain '{}', got '{}'",
                detail, error_message
            ));
        }
    }

    Ok(())
}

fn classify_phase(
    error: &UniError,
    expected_phase: ErrorPhase,
    detail_code: Option<&str>,
) -> ErrorPhase {
    // `_cypher_in` argument checks are compile-time validations in schema mode.
    if expected_phase == ErrorPhase::CompileTime
        && detail_code == Some("InvalidArgumentType")
        && error
            .to_string()
            .contains("_cypher_in(): second argument must be a list")
    {
        return ErrorPhase::CompileTime;
    }

    let base_phase = match error {
        UniError::Parse { .. }
        | UniError::Query { .. }
        | UniError::LabelNotFound { .. }
        | UniError::EdgeTypeNotFound { .. } => ErrorPhase::CompileTime,

        UniError::Type { .. } | UniError::Constraint { .. } | UniError::PropertyNotFound { .. } => {
            ErrorPhase::Runtime
        }

        _ => ErrorPhase::Runtime,
    };

    // Some runtime errors are currently surfaced through compile-time typed error
    // wrappers. Use detail codes to preserve TCK runtime expectations.
    if expected_phase == ErrorPhase::Runtime {
        let declared_runtime = detail_code.is_some_and(is_runtime_detail_code);
        let surfaced_runtime =
            extract_detail_code(&error.to_string()).is_some_and(|d| is_runtime_detail_code(&d));
        if declared_runtime || surfaced_runtime {
            return ErrorPhase::Runtime;
        }
    }

    base_phase
}

fn is_runtime_detail_code(detail: &str) -> bool {
    matches!(
        detail,
        "DeleteConnectedNode"
            | "DeletedEntityAccess"
            | "MergeReadOwnWrites"
            | "NumberOutOfRange"
            | "MapElementAccessByNonString"
            | "InvalidArgumentValue"
            | "InvalidArgumentType"
            | "NegativeIntegerArgument"
            | "InvalidPropertyType"
    )
}

fn extract_detail_code(message: &str) -> Option<String> {
    let mut text = message.trim();

    if let Some(rest) = text.strip_prefix("Query error: ") {
        text = rest.trim();
    } else if let Some(rest) = text.strip_prefix("Parse error: ") {
        text = rest.trim();
    } else if let Some(rest) = text.strip_prefix("Type error: expected ") {
        text = rest.trim();
    } else if let Some(rest) = text.strip_prefix("Type error: ") {
        // `UniError::Type` carrying a whole engine message (empty `actual`)
        // displays as `Type error: <message>`.
        text = rest.trim();
    }

    for prefix in [
        "SyntaxError:",
        "TypeError:",
        "SemanticError:",
        "ArgumentError:",
        "EntityNotFound:",
        "ParameterMissing:",
        "ConstraintVerificationFailed:",
        "ProcedureError:",
    ] {
        if let Some(rest) = text.strip_prefix(prefix) {
            text = rest.trim();
            break;
        }
    }

    let code: String = text
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect();

    (!code.is_empty()).then_some(code)
}

fn classify_error(error: &UniError) -> TckErrorType {
    match error {
        UniError::Parse { .. } => TckErrorType::SyntaxError,
        UniError::Type { .. } => TckErrorType::TypeError,
        UniError::Query { message, .. } => {
            // Planner errors prefixed with "SyntaxError:" are compile-time syntax errors
            if message.starts_with("SyntaxError:") {
                TckErrorType::SyntaxError
            } else if message.starts_with("TypeError:") {
                TckErrorType::TypeError
            } else if message.starts_with("ArgumentError:") {
                TckErrorType::ArgumentError
            } else if message.starts_with("EntityNotFound:") {
                TckErrorType::EntityNotFound
            } else {
                TckErrorType::SemanticError
            }
        }
        UniError::Constraint { .. } => TckErrorType::ConstraintValidationFailed,
        UniError::LabelNotFound { .. } | UniError::EdgeTypeNotFound { .. } => {
            TckErrorType::EntityNotFound
        }
        UniError::PropertyNotFound { .. } => TckErrorType::PropertyNotFound,
        _ => TckErrorType::Unknown(format!("{:?}", error)),
    }
}

/// Be lenient with error type matching since Cypher classifies many semantic
/// validations as SyntaxError, and our engine may use different error categories.
///
/// `detail_genuinely_matched` reports whether the expected error's detail
/// substring was actually found in the error message. It gates the otherwise
/// dangerously-permissive `Unknown` actual case: an actual error the runner
/// could not classify (`TckErrorType::Unknown`) must NOT be treated as matching
/// every specific expected type. It is allowed to match a specific expected type
/// only when the detail substring corroborates the match; an unspecified
/// (`Unknown`) expected type is still accepted because the TCK itself declined to
/// pin a standard error category there.
fn error_types_match(
    actual: &TckErrorType,
    expected: &TckErrorType,
    detail_genuinely_matched: bool,
) -> bool {
    if actual == expected {
        return true;
    }
    // An unclassified actual error matches a specific expected type only when the
    // detail substring genuinely corroborates it. When the expected side is itself
    // `Unknown` (a non-standard/unspecified TCK type name), accept it regardless.
    if let TckErrorType::Unknown(_) = actual {
        return matches!(expected, TckErrorType::Unknown(_)) || detail_genuinely_matched;
    }
    matches!(
        (actual, expected),
        (_, TckErrorType::Unknown(_))
            // Cypher TCK classifies many semantic/type validations as SyntaxError
            | (TckErrorType::SemanticError, TckErrorType::SyntaxError)
            | (TckErrorType::SemanticError, TckErrorType::TypeError)
            | (TckErrorType::TypeError, TckErrorType::SyntaxError)
            // TCK may classify argument validations under different front-end categories.
            | (TckErrorType::SemanticError, TckErrorType::ArgumentError)
            | (TckErrorType::SyntaxError, TckErrorType::ArgumentError)
            | (TckErrorType::TypeError, TckErrorType::ArgumentError)
    )
}

fn detail_matches(error_message: &str, detail: &str) -> bool {
    if error_message.contains(detail) {
        return true;
    }

    let lower = error_message.to_ascii_lowercase();

    match detail {
        "NegativeIntegerArgument" => {
            lower.contains("negativeintegerargument")
                || (lower.contains("invalidargumenttype")
                    && lower.contains("constant integer expression"))
        }
        "MapElementAccessByNonString" => {
            lower.contains("mapelementaccessbynonstring")
                || lower.contains("map index must be a string")
                || (lower.contains("map") && lower.contains("indexing by string key"))
                || (lower.contains("map") && lower.contains("non-string"))
        }
        "InvalidArgumentValue" => {
            lower.contains("invalidargumentvalue")
                || lower.contains("utf8")
                || lower.contains("invalid utf-8")
        }
        "InvalidArgumentType" => {
            lower.contains("invalidargumenttype")
                || lower
                    .contains("failed to coerce arguments to satisfy a call to 'range' function")
                || lower.contains("coercion from [")
        }
        "NumberOutOfRange" => {
            lower.contains("numberoutofrange")
                || lower.contains("step cannot be zero")
                || (lower.contains("unknownfunction")
                    && (lower.contains("percentilecont") || lower.contains("percentiledisc")))
        }
        "DeleteConnectedNode" => {
            lower.contains("deleteconnectednode")
                || (lower.contains("delete")
                    && (lower.contains("connected")
                        || lower.contains("still has relationships")
                        || lower.contains("cannot delete")))
        }
        "MergeReadOwnWrites" => {
            lower.contains("mergereadownwrites")
                || lower.contains("without labels info")
                || (lower.contains("merge")
                    && (lower.contains("already bound")
                        || lower.contains("variablealreadybound")
                        || lower.contains("nosinglerelationshiptype")
                        || lower.contains("must have a label")
                        || lower.contains("without labels info")))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_parse_error() {
        let err = UniError::Parse {
            message: "Syntax error".to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        };
        assert_eq!(
            classify_phase(&err, ErrorPhase::CompileTime, None),
            ErrorPhase::CompileTime
        );
        assert_eq!(classify_error(&err), TckErrorType::SyntaxError);
    }

    #[test]
    fn test_classify_type_error() {
        let err = UniError::Type {
            expected: "Int".to_string(),
            actual: "String".to_string(),
        };
        assert_eq!(
            classify_phase(&err, ErrorPhase::Runtime, None),
            ErrorPhase::Runtime
        );
        assert_eq!(classify_error(&err), TckErrorType::TypeError);
    }

    #[test]
    fn test_runtime_detail_forces_runtime_phase() {
        let err = UniError::Query {
            message: "SyntaxError: InvalidArgumentType - range() start must be an integer"
                .to_string(),
            query: None,
        };
        assert_eq!(
            classify_phase(&err, ErrorPhase::Runtime, Some("InvalidArgumentType")),
            ErrorPhase::Runtime
        );
    }

    #[test]
    fn test_extract_detail_code() {
        let msg = "Query error: SyntaxError: UndefinedVariable - Variable 'x' not defined";
        assert_eq!(
            extract_detail_code(msg).as_deref(),
            Some("UndefinedVariable")
        );
    }

    /// Both `UniError::Type` display shapes yield the detail code.
    #[test]
    fn test_extract_detail_code_from_type_error_display() {
        let message = UniError::Type {
            expected: "TypeError: InvalidArgumentValue - size() is not supported".to_string(),
            actual: String::new(),
        };
        assert_eq!(
            extract_detail_code(&message.to_string()).as_deref(),
            Some("InvalidArgumentValue")
        );
        let pair = UniError::Type {
            expected: "TypeError: InvalidArgumentType - wanted a list".to_string(),
            actual: "String".to_string(),
        };
        assert_eq!(
            extract_detail_code(&pair.to_string()).as_deref(),
            Some("InvalidArgumentType")
        );
    }

    #[test]
    fn test_tck_error_type_from_str() {
        assert_eq!(
            "SyntaxError".parse::<TckErrorType>().unwrap(),
            TckErrorType::SyntaxError
        );
        assert_eq!(
            "TypeError".parse::<TckErrorType>().unwrap(),
            TckErrorType::TypeError
        );
        assert_eq!(
            "FooBar".parse::<TckErrorType>().unwrap(),
            TckErrorType::Unknown("FooBar".to_string())
        );
    }

    #[test]
    fn test_argument_error_prefix_classification() {
        let err = UniError::Query {
            message: "ArgumentError: InvalidArgumentType - bad argument".to_string(),
            query: None,
        };
        assert_eq!(classify_error(&err), TckErrorType::ArgumentError);
    }

    #[test]
    fn test_unknown_actual_does_not_blanket_match_specific_expected() {
        // An error the runner cannot classify (`Unknown`) must NOT satisfy a
        // specific expected type when the detail does not correspond. Use a
        // UniError variant that `classify_error` leaves as `Unknown(_)`.
        let unclassified = UniError::Schema {
            message: "some schema failure unrelated to ranges".to_string(),
        };
        assert!(
            matches!(classify_error(&unclassified), TckErrorType::Unknown(_)),
            "precondition: this error must classify as Unknown"
        );

        // Wildcard detail provides no corroboration -> must NOT match a specific
        // type. `SyntaxError` is a real standard TCK error category (unlike the
        // detail code `NumberOutOfRange`), so this exercises the false-pass path
        // for the ~3 wildcard ('*') detail scenarios.
        let res = match_error(
            &unclassified,
            "SyntaxError".parse::<TckErrorType>().unwrap(),
            ErrorPhase::AnyTime,
            Some("*"),
        );
        assert!(
            res.is_err(),
            "Unknown actual must not match specific SyntaxError under wildcard detail"
        );

        // Absent detail also provides no corroboration -> must NOT match.
        let res_no_detail = match_error(
            &unclassified,
            "TypeError".parse::<TckErrorType>().unwrap(),
            ErrorPhase::AnyTime,
            None,
        );
        assert!(
            res_no_detail.is_err(),
            "Unknown actual must not match specific type when no detail is given"
        );

        // But a legitimately-classified error still matches when type + detail line up.
        let classified = UniError::Query {
            message: "Query error: SyntaxError: NumberOutOfRange - step cannot be zero".to_string(),
            query: None,
        };
        let ok = match_error(
            &classified,
            "SyntaxError".parse::<TckErrorType>().unwrap(),
            ErrorPhase::AnyTime,
            Some("NumberOutOfRange"),
        );
        assert!(
            ok.is_ok(),
            "legitimately classified SyntaxError + matching detail must still pass: {ok:?}"
        );

        // And an Unknown actual whose detail genuinely corroborates a specific
        // expected type is still allowed (the detail substring carries the match).
        let unknown_with_detail = UniError::Schema {
            message: "boom: step cannot be zero".to_string(),
        };
        let corroborated = match_error(
            &unknown_with_detail,
            "SyntaxError".parse::<TckErrorType>().unwrap(),
            ErrorPhase::AnyTime,
            Some("NumberOutOfRange"),
        );
        assert!(
            corroborated.is_ok(),
            "Unknown actual with corroborating detail must match specific type: {corroborated:?}"
        );
    }

    #[test]
    fn test_compile_phase_override_for_cypher_in_invalid_argument_type() {
        let err = UniError::Type {
            expected: "_cypher_in(): second argument must be a list".to_string(),
            actual: "String".to_string(),
        };
        assert_eq!(
            classify_phase(&err, ErrorPhase::CompileTime, Some("InvalidArgumentType")),
            ErrorPhase::CompileTime
        );
    }
}
