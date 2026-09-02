// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Query execution layer for the Uni graph database.
//!
//! This crate provides OpenCypher query parsing, logical planning, and
//! execution against Uni's object-store-backed property graph.
//!
//! # Modules
//!
//! - [`query`] — planner, executor, DataFusion integration, pushdown logic
//! - [`types`] — public value types (`Value`, `Node`, `Edge`, `Path`, etc.)
//!
//! # Quick Start
//!
//! ```rust,ignore
//! let executor = Executor::new(storage);
//! let planner = QueryPlanner::new(schema);
//! let plan = planner.plan(cypher_ast)?;
//! let result = executor.execute_plan(plan, &params).await?;
//! ```

#![recursion_limit = "256"]

pub mod procedures_plugin;
pub mod projection_store;
pub mod query;
pub mod types;

pub use query::df_graph::locy_profile::{
    LocyExecProfile, LocyIterationProfile, LocyRuleProfile, LocyStratumProfile,
};
pub use query::executor::core::{OperatorStats, ProfileOutput};
pub use query::executor::plan_shape;
pub use query::executor::procedure::{
    ProcedureOutput, ProcedureParam, ProcedureRegistry, ProcedureValueType, RegisteredProcedure,
};
pub use query::executor::{CustomFunctionRegistry, CustomScalarFn, Executor, ResultNormalizer};
// M8.6: session-scoped plugin registry plumbing. Host crates wrap their
// per-query execution paths with `scoped_with_session_plugin_registry`;
// the executor consults `current_session_plugin_registry` at the UDF /
// procedure / Locy-aggregate resolution sites.
pub use query::df_udfs_plugin::{
    CURRENT_PRINCIPAL, SESSION_PLUGIN_REGISTRY, current_principal, current_session_plugin_registry,
    maybe_scope_with_principal, scoped_with_principal, scoped_with_session_context,
    scoped_with_session_plugin_registry,
};
pub use query::planner::{
    CostEstimates, ExplainOutput, ForkIndexLookup, FusionKind, IndexUsage, LogicalPlan,
    QueryPlanner, fuse_create_set, resolve_traversal_endpoints, rewrite_for_fork_fusion,
};
pub use types::{
    Edge, ExecuteResult, FromValue, Node, Path, QueryCursor, QueryMetrics, QueryResult,
    QueryWarning, Row, Value,
};
pub use uni_cypher::ast::{Query as CypherQuery, TimeTravelSpec};

/// Validate that a query AST contains only read clauses.
///
/// Rejects any query that contains CREATE, MERGE, DELETE, SET, REMOVE,
/// or schema commands, **including writes nested inside a `CALL { … }`
/// subquery**.
///
/// Procedure calls (`CALL proc(...)`) are not classified here because their
/// read/write nature is registry-dependent; use [`validate_read_only_with`] to
/// also reject write procedures when a classifier is available.
///
/// # Errors
///
/// Returns `Err(message)` describing the first write clause found. Used to
/// enforce read-only access for time-travel queries (`VERSION AS OF` /
/// `TIMESTAMP AS OF`) and for `Session::query`.
pub fn validate_read_only(query: &CypherQuery) -> Result<(), String> {
    validate_read_only_with(query, &|_| false)
}

/// Like [`validate_read_only`], but also rejects procedure calls that
/// `is_write_procedure` classifies as mutating.
///
/// The predicate receives the procedure name (e.g. `"db.create.something"`) and
/// returns `true` if invoking it could mutate the graph or schema. Callers that
/// hold a plugin registry can back it with the registered `ProcedureMode`;
/// callers without one should use [`validate_read_only`], which treats every
/// procedure as read-only (AST-determinable writes are still rejected).
///
/// # Errors
///
/// Returns `Err(message)` describing the first write clause, write subquery, or
/// write procedure found.
pub fn validate_read_only_with(
    query: &CypherQuery,
    is_write_procedure: &dyn Fn(&str) -> bool,
) -> Result<(), String> {
    use uni_cypher::ast::{CallKind, Clause, Query, Statement};

    fn check_statement(
        stmt: &Statement,
        is_write_procedure: &dyn Fn(&str) -> bool,
    ) -> Result<(), String> {
        for clause in &stmt.clauses {
            match clause {
                Clause::Create(_)
                | Clause::Merge(_)
                | Clause::Delete(_)
                | Clause::Set(_)
                | Clause::Remove(_) => {
                    return Err(
                        "Write clauses (CREATE, MERGE, DELETE, SET, REMOVE) are not allowed \
                         in a read-only context"
                            .to_string(),
                    );
                }
                Clause::Call(call) => match &call.kind {
                    // A subquery can itself contain writes; the planner fully
                    // supports them, so the validator must recurse to match.
                    CallKind::Subquery(inner) => check_query(inner, is_write_procedure)?,
                    CallKind::Procedure { procedure, .. } => {
                        if is_write_procedure(procedure) {
                            return Err(format!(
                                "Write procedure CALL {procedure}(...) is not allowed \
                                 in a read-only context"
                            ));
                        }
                    }
                },
                _ => {}
            }
        }
        Ok(())
    }

    fn check_query(q: &Query, is_write_procedure: &dyn Fn(&str) -> bool) -> Result<(), String> {
        match q {
            Query::Single(stmt) => check_statement(stmt, is_write_procedure),
            Query::Union { left, right, .. } => {
                check_query(left, is_write_procedure)?;
                check_query(right, is_write_procedure)
            }
            Query::Explain(inner) => check_query(inner, is_write_procedure),
            Query::TimeTravel { query, .. } => check_query(query, is_write_procedure),
            Query::Schema(cmd) => {
                use uni_cypher::ast::SchemaCommand;
                match cmd.as_ref() {
                    // Read-only schema commands are allowed
                    SchemaCommand::ShowConstraints(_)
                    | SchemaCommand::ShowIndexes(_)
                    | SchemaCommand::ShowDatabase
                    | SchemaCommand::ShowConfig
                    | SchemaCommand::ShowStatistics => Ok(()),
                    // All other schema commands mutate state
                    _ => Err(
                        "Mutating schema commands are not allowed in read-only context".to_string(),
                    ),
                }
            }
        }
    }

    check_query(query, is_write_procedure)
}
