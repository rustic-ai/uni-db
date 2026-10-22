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
    BUILTIN_PROCEDURE_MODES, ProcedureOutput, ProcedureParam, ProcedureRegistry,
    ProcedureValueType, RegisteredProcedure, builtin_procedure_mode,
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
use uni_plugin::traits::procedure::ProcedureMode;

/// Validate that a query AST contains only read clauses.
///
/// Rejects any query that contains CREATE, MERGE, DELETE, SET, REMOVE,
/// or schema commands, **including writes nested inside a `CALL { … }`
/// subquery**, and calls to executor built-in procedures whose mode is not
/// `Read` ([`BUILTIN_PROCEDURE_MODES`]: `uni.admin.compact`,
/// `uni.admin.snapshot.restore`, the `uni.schema.create*`/`drop*` DDL
/// procedures, …).
///
/// Registered plugin procedures are not classified here because their
/// read/write nature is registry-dependent; use [`validate_read_only_with`] to
/// also reject write procedures when a classifier is available.
///
/// # Errors
///
/// Returns `Err(message)` describing the first write clause found. Used to
/// enforce read-only access for time-travel queries (`VERSION AS OF` /
/// `TIMESTAMP AS OF`), session prepared queries and `Session::query`.
pub fn validate_read_only(query: &CypherQuery) -> Result<(), String> {
    validate_read_only_with(query, &|_| false)
}

/// Like [`validate_read_only`], but also rejects procedure calls that
/// `is_write_procedure` classifies as mutating.
///
/// The predicate receives the procedure name (e.g. `"db.create.something"`) and
/// returns `true` if invoking it could mutate the graph or schema. Callers that
/// hold a plugin registry can back it with the registered `ProcedureMode`.
/// An executor built-in procedure is classified by [`BUILTIN_PROCEDURE_MODES`]
/// and never reaches the predicate: the registry does not hold built-ins, so a
/// registry-backed predicate would call every one of them a read.
///
/// # Errors
///
/// Returns `Err(message)` describing the first write clause, write subquery, or
/// write procedure found.
pub fn validate_read_only_with(
    query: &CypherQuery,
    is_write_procedure: &dyn Fn(&str) -> bool,
) -> Result<(), String> {
    check_read_only(query, &|name| match builtin_procedure_mode(name) {
        Some(mode) => !matches!(mode, ProcedureMode::Read),
        None => is_write_procedure(name),
    })
}

/// The most privileged mode among the procedures `query` calls, at any
/// depth (`CALL { … }` subqueries, `UNION` arms, `EXPLAIN`, time travel), or
/// `None` when it calls none.
///
/// Built-ins are classified by [`BUILTIN_PROCEDURE_MODES`]; any other name by
/// `registered_mode`, and an unknown name counts as `Read` (it fails at
/// execution). Ranking, lowest first: `Read`, `Write`, `Schema`, `Dbms`.
/// Used to choose the authorization verb (`dbms`, `schema`) and to route a
/// `Dbms` statement outside a transaction.
#[must_use]
pub fn strongest_procedure_mode(
    query: &CypherQuery,
    registered_mode: &dyn Fn(&str) -> Option<ProcedureMode>,
) -> Option<ProcedureMode> {
    fn rank(mode: ProcedureMode) -> u8 {
        match mode {
            ProcedureMode::Read => 0,
            ProcedureMode::Write => 1,
            ProcedureMode::Schema => 2,
            // `ProcedureMode` is non-exhaustive: a mode added later ranks
            // with the most privileged, failing closed.
            _ => 3,
        }
    }
    let mut strongest: Option<ProcedureMode> = None;
    for_each_procedure(query, &mut |name| {
        let mode = builtin_procedure_mode(name)
            .or_else(|| registered_mode(name))
            .unwrap_or(ProcedureMode::Read);
        if strongest.is_none_or(|s| rank(mode) > rank(s)) {
            strongest = Some(mode);
        }
    });
    strongest
}

/// Whether `query` is an administrative statement: it calls at least one
/// `Dbms`-mode procedure and otherwise only reads (no write clause, no
/// mutating schema command, no `Write`/`Schema` procedure).
///
/// Such a statement acts on the database as a whole and cannot be undone by
/// a rollback, so `Session::run` executes it outside a transaction, as
/// Neo4j runs its administration commands in auto-commit only.
#[must_use]
pub fn is_admin_statement(
    query: &CypherQuery,
    registered_mode: &dyn Fn(&str) -> Option<ProcedureMode>,
) -> bool {
    let mode = |name: &str| builtin_procedure_mode(name).or_else(|| registered_mode(name));
    strongest_procedure_mode(query, registered_mode).is_some_and(|m| {
        !matches!(
            m,
            ProcedureMode::Read | ProcedureMode::Write | ProcedureMode::Schema
        )
    }) && check_read_only(query, &|name| {
        matches!(
            mode(name),
            Some(ProcedureMode::Write | ProcedureMode::Schema)
        )
    })
    .is_ok()
}

/// Calls `f` with the name of every procedure `query` calls, at the depths
/// [`check_read_only`] visits.
fn for_each_procedure(query: &CypherQuery, f: &mut dyn FnMut(&str)) {
    use uni_cypher::ast::{CallKind, Clause, Query};
    match query {
        Query::Single(stmt) => {
            for clause in &stmt.clauses {
                if let Clause::Call(call) = clause {
                    match &call.kind {
                        CallKind::Subquery(inner) => for_each_procedure(inner, f),
                        CallKind::Procedure { procedure, .. } => f(procedure),
                    }
                }
            }
        }
        Query::Union { left, right, .. } => {
            for_each_procedure(left, f);
            for_each_procedure(right, f);
        }
        Query::Explain(inner) => for_each_procedure(inner, f),
        Query::TimeTravel { query, .. } => for_each_procedure(query, f),
        Query::Schema(_) => {}
    }
}

/// The read-only walk behind [`validate_read_only_with`], classifying each
/// procedure by `is_write_procedure` alone.
fn check_read_only(
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

#[cfg(test)]
mod read_only_oracle_tests {
    use super::*;

    fn parse(cypher: &str) -> CypherQuery {
        uni_cypher::parse(cypher).expect("parse")
    }

    /// A built-in's table mode wins over the predicate, so a registry that
    /// cannot see built-ins does not make them reads.
    #[test]
    fn builtin_mutating_procedures_are_not_read_only() {
        for cypher in [
            "CALL uni.admin.snapshot.restore('s')",
            "CALL uni.admin.snapshot.delete('s')",
            "CALL uni.admin.compact() YIELD tables_optimized RETURN tables_optimized",
            "CALL { CALL uni.admin.snapshot.create('x') YIELD snapshot_id \
             RETURN snapshot_id } RETURN 1",
            "RETURN 1 AS a UNION CALL uni.schema.dropLabel('L') YIELD success \
             RETURN success AS a",
            "EXPLAIN CALL uni.admin.verifyEdgeEndpoints()",
        ] {
            let q = parse(cypher);
            assert!(validate_read_only(&q).is_err(), "{cypher}");
            assert!(validate_read_only_with(&q, &|_| false).is_err(), "{cypher}");
        }
        for cypher in [
            "CALL uni.admin.compactionStatus()",
            "CALL uni.admin.snapshot.list()",
        ] {
            assert!(validate_read_only(&parse(cypher)).is_ok(), "{cypher}");
        }
    }

    #[test]
    fn strongest_mode_and_admin_statement() {
        let none = |_: &str| None;
        let q = parse("CALL uni.admin.snapshot.restore('s')");
        assert_eq!(
            strongest_procedure_mode(&q, &none),
            Some(ProcedureMode::Dbms)
        );
        assert!(is_admin_statement(&q, &none));
        let q = parse("CALL uni.schema.createLabel('L', {})");
        assert_eq!(
            strongest_procedure_mode(&q, &none),
            Some(ProcedureMode::Schema)
        );
        assert!(!is_admin_statement(&q, &none));
        // A DBMS call alongside a graph write is not purely administrative.
        let q = parse("CALL uni.admin.compact() YIELD duration_ms CREATE (:T) RETURN 1");
        assert!(!is_admin_statement(&q, &none));
        let q = parse("MATCH (n) RETURN n");
        assert_eq!(strongest_procedure_mode(&q, &none), None);
        assert!(!is_admin_statement(&q, &none));
        // A registered DBMS procedure is administrative too.
        let reg = |n: &str| (n == "my.admin").then_some(ProcedureMode::Dbms);
        assert!(is_admin_statement(&parse("CALL my.admin()"), &reg));
    }
}
