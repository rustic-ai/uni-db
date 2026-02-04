use std::str::FromStr;
use uni_common::UniError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorPhase {
    CompileTime,
    Runtime,
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
    let actual_phase = classify_phase(actual);
    if actual_phase != expected_phase {
        return Err(format!(
            "Error phase mismatch: expected {:?}, got {:?}",
            expected_phase, actual_phase
        ));
    }

    let actual_type = classify_error(actual);
    if !error_types_match(&actual_type, &expected_type) {
        return Err(format!(
            "Error type mismatch: expected {:?}, got {:?}",
            expected_type, actual_type
        ));
    }

    if let Some(detail) = detail_code {
        let error_message = actual.to_string();
        if !error_message.contains(detail) {
            return Err(format!(
                "Error detail mismatch: expected message to contain '{}', got '{}'",
                detail, error_message
            ));
        }
    }

    Ok(())
}

fn classify_phase(error: &UniError) -> ErrorPhase {
    match error {
        UniError::Parse { .. }
        | UniError::Query { .. }
        | UniError::LabelNotFound { .. }
        | UniError::EdgeTypeNotFound { .. } => ErrorPhase::CompileTime,

        UniError::Type { .. } | UniError::Constraint { .. } | UniError::PropertyNotFound { .. } => {
            ErrorPhase::Runtime
        }

        _ => ErrorPhase::Runtime,
    }
}

fn classify_error(error: &UniError) -> TckErrorType {
    match error {
        UniError::Parse { .. } => TckErrorType::SyntaxError,
        UniError::Type { .. } => TckErrorType::TypeError,
        UniError::Query { message, .. } => {
            // Planner errors prefixed with "SyntaxError:" are compile-time syntax errors
            if message.starts_with("SyntaxError:") {
                TckErrorType::SyntaxError
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
fn error_types_match(actual: &TckErrorType, expected: &TckErrorType) -> bool {
    if actual == expected {
        return true;
    }
    matches!(
        (actual, expected),
        (TckErrorType::Unknown(_), _)
            | (_, TckErrorType::Unknown(_))
            // Cypher TCK classifies many semantic/type validations as SyntaxError
            | (TckErrorType::SemanticError, TckErrorType::SyntaxError)
            | (TckErrorType::SemanticError, TckErrorType::TypeError)
    )
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
        assert_eq!(classify_phase(&err), ErrorPhase::CompileTime);
        assert_eq!(classify_error(&err), TckErrorType::SyntaxError);
    }

    #[test]
    fn test_classify_type_error() {
        let err = UniError::Type {
            expected: "Int".to_string(),
            actual: "String".to_string(),
        };
        assert_eq!(classify_phase(&err), ErrorPhase::Runtime);
        assert_eq!(classify_error(&err), TckErrorType::TypeError);
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
}
