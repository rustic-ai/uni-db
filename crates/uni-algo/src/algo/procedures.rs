// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Algorithm procedure interface for Cypher integration.
//!
//! Procedures are registered with `AlgorithmRegistry` and can be invoked
//! via `CALL algo.name(...)` in Cypher queries.

use anyhow::{Result, anyhow};
use futures::Stream;
use serde_json::Value;
use std::pin::Pin;

/// Procedure signature for documentation and validation.
#[derive(Debug, Clone)]
pub struct ProcedureSignature {
    /// Required arguments: (name, type)
    pub args: Vec<(&'static str, ValueType)>,
    /// Optional arguments: (name, type, default)
    pub optional_args: Vec<(&'static str, ValueType, Value)>,
    /// Output columns: (name, type)
    pub yields: Vec<(&'static str, ValueType)>,
}

impl ProcedureSignature {
    /// Validate arguments against signature and fill defaults for optional args.
    pub fn validate_args(&self, mut args: Vec<Value>) -> Result<Vec<Value>> {
        let req_count = self.args.len();
        let total_count = req_count + self.optional_args.len();

        if args.len() < req_count {
            return Err(anyhow!(
                "Too few arguments. Expected at least {}, got {}",
                req_count,
                args.len()
            ));
        }

        if args.len() > total_count {
            return Err(anyhow!(
                "Too many arguments. Expected at most {}, got {}",
                total_count,
                args.len()
            ));
        }

        // Validate required args
        for (i, (name, ty)) in self.args.iter().enumerate() {
            if !ty.matches(&args[i]) {
                return Err(anyhow!(
                    "Invalid type for argument '{}'. Expected {:?}, got {:?}",
                    name,
                    ty,
                    args[i]
                ));
            }
        }

        // Validate provided optional args and fill defaults for missing ones
        for i in 0..self.optional_args.len() {
            let idx = req_count + i;
            let (name, ty, default) = &self.optional_args[i];

            if idx < args.len() {
                if !ty.matches(&args[idx]) {
                    return Err(anyhow!(
                        "Invalid type for optional argument '{}'. Expected {:?}, got {:?}",
                        name,
                        ty,
                        args[idx]
                    ));
                }
            } else {
                args.push(default.clone());
            }
        }

        Ok(args)
    }
}

/// Value types for procedure signatures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Int,
    Float,
    String,
    Bool,
    List,
    Map,
    Node,
    Relationship,
    Path,
    Any,
}

impl ValueType {
    pub fn matches(&self, val: &Value) -> bool {
        match self {
            ValueType::Int => val.is_i64() || val.is_u64(),
            ValueType::Float => val.is_f64() || val.is_i64() || val.is_u64(),
            ValueType::String => val.is_string(),
            ValueType::Bool => val.is_boolean(),
            ValueType::List => val.is_array(),
            ValueType::Map => val.is_object(),
            ValueType::Node => val.is_string() || val.is_u64(), // VID string or u64
            ValueType::Relationship => val.is_u64() || val.is_object(),
            ValueType::Path => val.is_object(), // Path struct
            ValueType::Any => true,
        }
    }
}

/// Result row from algorithm execution.
#[derive(Debug, Clone)]
pub struct AlgoResultRow {
    /// Column values in order matching `yields`.
    pub values: Vec<Value>,
}

/// Trait for algorithm procedures.
///
/// Implement this to expose an algorithm via `CALL algo.name(...)`.
pub trait AlgoProcedure: Send + Sync {
    /// Procedure name (e.g., "algo.pageRank").
    fn name(&self) -> &str;

    /// Procedure signature for validation and documentation.
    fn signature(&self) -> ProcedureSignature;

    /// Execute the procedure with given arguments.
    ///
    /// Returns a stream of result rows.
    fn execute(
        &self,
        ctx: AlgoContext,
        args: Vec<Value>,
    ) -> Pin<Box<dyn Stream<Item = Result<AlgoResultRow>> + Send + 'static>>;
}

use std::sync::Arc;
use uni_store::runtime::L0Manager;
use uni_store::storage::manager::StorageManager;

/// Execution context for algorithm procedures.
pub struct AlgoContext {
    pub storage: Arc<StorageManager>,
    /// L0 manager for scanning in-memory vertices not yet flushed.
    pub l0_manager: Option<Arc<L0Manager>>,
}

impl AlgoContext {
    /// Create a new algorithm context.
    pub fn new(storage: Arc<StorageManager>, l0_manager: Option<Arc<L0Manager>>) -> Self {
        Self {
            storage,
            l0_manager,
        }
    }
}

// Placeholder procedure implementations will be added in Phase 3.3
