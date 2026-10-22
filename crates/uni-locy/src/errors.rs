use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum LocyError {
    #[error("max iterations ({max}) exceeded for stratum {stratum_id} (rules: {rules})")]
    MaxIterationsExceeded {
        max: usize,
        stratum_id: usize,
        rules: String,
    },

    #[error("evaluation timeout: {elapsed:?} exceeded limit of {limit:?}")]
    Timeout { elapsed: Duration, limit: Duration },

    #[error("MSUM negative value in rule '{rule}', fold '{fold}': {value}")]
    MsumNegativeValue {
        rule: String,
        fold: String,
        value: f64,
    },

    #[error("executor error: {message}")]
    ExecutorError { message: String },

    #[error("evaluation error: {message}")]
    EvaluationError { message: String },

    #[error("type error: {message}")]
    TypeError { message: String },

    #[error("savepoint failed: {message}")]
    SavepointFailed { message: String },

    #[error("query resolution error: {message}")]
    QueryResolutionError { message: String },

    #[error("abduction error: {message}")]
    AbductionError { message: String },

    /// An evaluation the program needed was cut short (its budget ran out,
    /// or a recursive stratum hit `max_iterations`) and partial results
    /// were not allowed: a re-derivation after a write, or a hypothetical
    /// state's. Surfaces as `UniError::LocyIncomplete`, as the program's own
    /// cut-short run does.
    #[error("Locy evaluation incomplete: {detail}")]
    Incomplete {
        detail: Box<uni_common::LocyIncomplete>,
    },
}
