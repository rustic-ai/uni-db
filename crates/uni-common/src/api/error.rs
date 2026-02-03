// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UniError {
    #[error("Database not found: {path}")]
    NotFound { path: PathBuf },

    #[error("Schema error: {message}")]
    Schema { message: String },

    #[error("Parse error: {message}")]
    Parse {
        message: String,
        position: Option<usize>,
        line: Option<usize>,
        column: Option<usize>,
        context: Option<String>,
    },

    #[error("Query error: {message}")]
    Query {
        message: String,
        query: Option<String>,
    },

    #[error("Transaction error: {message}")]
    Transaction { message: String },

    #[error("Transaction conflict: {message}")]
    TransactionConflict { message: String },

    #[error("Transaction already completed")]
    TransactionAlreadyCompleted,

    /// Operation not supported on read-only database
    #[error("Operation '{operation}' not supported on read-only database")]
    ReadOnly { operation: String },

    /// Label not found in schema
    #[error("Label '{label}' not found in schema")]
    LabelNotFound { label: String },

    /// Edge type not found in schema
    #[error("Edge type '{edge_type}' not found in schema")]
    EdgeTypeNotFound { edge_type: String },

    /// Property not found on node/edge
    #[error("Property '{property}' not found on {entity_type} with label '{label}'")]
    PropertyNotFound {
        property: String,
        entity_type: String, // "node" or "edge"
        label: String,
    },

    /// Index not found
    #[error("Index '{index}' not found")]
    IndexNotFound { index: String },

    /// Snapshot not found
    #[error("Snapshot '{snapshot_id}' not found")]
    SnapshotNotFound { snapshot_id: String },

    /// Query memory limit exceeded
    #[error("Query exceeded memory limit of {limit_bytes} bytes")]
    MemoryLimitExceeded { limit_bytes: usize },

    #[error("Database is locked by another process")]
    DatabaseLocked,

    #[error("Operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("Type error: expected {expected}, got {actual}")]
    Type { expected: String, actual: String },

    #[error("Constraint violation: {message}")]
    Constraint { message: String },

    #[error("Storage error: {message}")]
    Storage {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),

    #[error("Invalid identifier '{name}': {reason}")]
    InvalidIdentifier { name: String, reason: String },

    #[error("Label '{label}' already exists")]
    LabelAlreadyExists { label: String },

    #[error("Edge type '{edge_type}' already exists")]
    EdgeTypeAlreadyExists { edge_type: String },

    #[error("Permission denied: {action}")]
    PermissionDenied { action: String },

    #[error("Argument '{arg}' is invalid: {message}")]
    InvalidArgument { arg: String, message: String },
}

pub type Result<T> = std::result::Result<T, UniError>;
