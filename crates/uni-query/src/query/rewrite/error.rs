/// Error types for query rewriting operations
use std::fmt;

/// Errors that can occur during query rewriting
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RewriteError {
    /// Function has wrong number of arguments
    ArityMismatch { expected: usize, got: usize },

    /// Function argument arity is out of expected range
    ArityOutOfRange { min: usize, max: usize, got: usize },

    /// Expected a string literal for property name but got dynamic expression
    ExpectedStringLiteral { arg_index: usize },

    /// Expected an entity reference (variable) but got different type
    ExpectedEntityReference { arg_index: usize },

    /// Argument has unexpected type
    TypeError {
        arg_index: usize,
        expected: String,
        got: String,
    },

    /// Rewrite rule is not applicable in current context
    NotApplicable { reason: String },

    /// Internal error during rewrite transformation
    TransformError { message: String },

    /// Missing required context information
    MissingContext { required: String },
}

impl fmt::Display for RewriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RewriteError::ArityMismatch { expected, got } => {
                write!(
                    f,
                    "Function arity mismatch: expected {} arguments, got {}",
                    expected, got
                )
            }
            RewriteError::ArityOutOfRange { min, max, got } => {
                write!(
                    f,
                    "Function arity out of range: expected {}-{} arguments, got {}",
                    min, max, got
                )
            }
            RewriteError::ExpectedStringLiteral { arg_index } => {
                write!(
                    f,
                    "Expected string literal at argument {}, got dynamic expression",
                    arg_index
                )
            }
            RewriteError::ExpectedEntityReference { arg_index } => {
                write!(
                    f,
                    "Expected entity reference at argument {}, got different type",
                    arg_index
                )
            }
            RewriteError::TypeError {
                arg_index,
                expected,
                got,
            } => {
                write!(
                    f,
                    "Type error at argument {}: expected {}, got {}",
                    arg_index, expected, got
                )
            }
            RewriteError::NotApplicable { reason } => {
                write!(f, "Rewrite not applicable: {}", reason)
            }
            RewriteError::TransformError { message } => {
                write!(f, "Transform error: {}", message)
            }
            RewriteError::MissingContext { required } => {
                write!(f, "Missing required context: {}", required)
            }
        }
    }
}

impl std::error::Error for RewriteError {}
