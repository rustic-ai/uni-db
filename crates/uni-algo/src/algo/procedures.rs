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

    /// Execute against a pre-built [`crate::algo::GraphProjection`]
    /// (V2 `(graphRef, config)` form, used by Cypher / Named projection
    /// callers).
    ///
    /// `args[0]` and `args[1]` are placeholder empty arrays — the
    /// projection is supplied directly. Algorithm-specific args start
    /// at position 2.
    ///
    /// Default implementation rejects with `0x823 — algorithm does
    /// not support pre-built projections`. Override on algorithms
    /// whose first args are `(nodeLabels, edgeTypes, …)`; see
    /// [`crate::algo::procedure_template::GenericAlgoProcedure`] for
    /// the generic projection-aware base.
    fn execute_with_projection(
        &self,
        _ctx: AlgoContext,
        _args: Vec<Value>,
        _projection: crate::algo::GraphProjection,
    ) -> Pin<Box<dyn Stream<Item = Result<AlgoResultRow>> + Send + 'static>> {
        use futures::stream::{self, StreamExt};
        let name = self.name().to_owned();
        stream::once(async move {
            Err(anyhow!(
                "Algorithm `{name}` does not support pre-built projections; \
                 use Native graphRef instead"
            ))
        })
        .boxed()
    }

    /// Execute with native-terminal arguments — `(startNode, endNode,
    /// edgeType, …)` shape used by the cypher path family
    /// (`shortest_path`, `astar`, `all_simple_paths`). The algorithm
    /// is responsible for materialising its own
    /// [`crate::algo::GraphProjection`] from the edge-type schema or
    /// per-call inputs; no projection is supplied.
    ///
    /// Dispatched when [`Self::wants_native_terminals`] returns
    /// `true`. Default implementation rejects with `0x824`.
    fn execute_with_native_terminals(
        &self,
        _ctx: AlgoContext,
        _args: Vec<Value>,
    ) -> Pin<Box<dyn Stream<Item = Result<AlgoResultRow>> + Send + 'static>> {
        use futures::stream::{self, StreamExt};
        let name = self.name().to_owned();
        stream::once(async move {
            Err(anyhow!(
                "Algorithm `{name}` does not support native-terminals entry; \
                 override `execute_with_native_terminals` and set \
                 `wants_native_terminals = true`"
            ))
        })
        .boxed()
    }

    /// True if this algorithm consumes `(startNode, endNode, edgeType,
    /// …)` arguments and wants
    /// [`Self::execute_with_native_terminals`] dispatch instead of the
    /// projection-aware [`Self::execute_with_projection`] path. Default
    /// `false` — opt-in for the cypher path family.
    fn wants_native_terminals(&self) -> bool {
        false
    }

    /// Customise the [`crate::algo::ProjectionBuilder`] before
    /// `.build()` is called on the projection-aware dispatch path.
    /// Default enables `include_reverse(true)`. Override to set edge
    /// weights or other algorithm-specific projection knobs.
    fn customize_projection(
        &self,
        builder: crate::algo::ProjectionBuilder,
        _args: &[Value],
    ) -> crate::algo::ProjectionBuilder {
        builder.include_reverse(true)
    }
}

use std::sync::Arc;
use uni_store::runtime::L0Manager;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

/// Execution context for algorithm procedures.
pub struct AlgoContext {
    pub storage: Arc<StorageManager>,
    /// L0 manager for scanning in-memory vertices not yet flushed.
    pub l0_manager: Option<Arc<L0Manager>>,
    /// The calling query's property manager, for property values.
    ///
    /// Inside a transaction `storage` is version-pinned, and a property
    /// manager built over pinned storage hides the transaction's own writes
    /// (it treats the pin as a time-travel snapshot). Cypher reads property
    /// values through a manager over live storage; an algorithm must use the
    /// same one. `None` builds one over `storage`.
    pub property_manager: Option<Arc<PropertyManager>>,
}

impl AlgoContext {
    /// Create a new algorithm context.
    pub fn new(storage: Arc<StorageManager>, l0_manager: Option<Arc<L0Manager>>) -> Self {
        Self {
            storage,
            l0_manager,
            property_manager: None,
        }
    }

    /// Read property values through the calling query's property manager.
    #[must_use]
    pub fn with_property_manager(mut self, property_manager: Option<Arc<PropertyManager>>) -> Self {
        self.property_manager = property_manager;
        self
    }

    /// The property manager algorithms read values through: the calling
    /// query's, or a fresh one over [`Self::storage`].
    #[must_use]
    pub fn property_manager(&self) -> Arc<PropertyManager> {
        self.property_manager.clone().unwrap_or_else(|| {
            Arc::new(PropertyManager::new(
                Arc::clone(&self.storage),
                self.storage.schema_manager_arc(),
                1000,
            ))
        })
    }
}
