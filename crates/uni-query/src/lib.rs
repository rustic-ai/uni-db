// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod query;
pub mod types;

pub use query::executor::Executor;
pub use query::executor::core::{OperatorStats, ProfileOutput};
pub use query::executor::procedure::{
    ProcedureOutput, ProcedureParam, ProcedureRegistry, ProcedureValueType, RegisteredProcedure,
};
pub use query::planner::{CostEstimates, ExplainOutput, IndexUsage, LogicalPlan, QueryPlanner};
pub use types::{Edge, ExecuteResult, FromValue, Node, Path, QueryCursor, QueryResult, Row, Value};
pub use uni_cypher::ast::{Query as CypherQuery, TimeTravelSpec};
pub use uni_cypher::{
    ParseError, parse as parse_cypher, parse_expression as parse_cypher_expression,
};

/// Parse a Cypher query and extract any time-travel clause.
///
/// Returns `(inner_query_ast, Option<TimeTravelSpec>)`. If the query contains
/// `VERSION AS OF` or `TIMESTAMP AS OF`, the inner query AST is unwrapped
/// from the `TimeTravel` variant so it can be planned directly.
///
/// This avoids double-parsing: the caller receives the parsed AST and can
/// pass it to `QueryPlanner::plan()` without re-parsing a string.
pub fn parse_and_extract_time_travel(
    cypher: &str,
) -> Result<(CypherQuery, Option<TimeTravelSpec>), ParseError> {
    let ast = parse_cypher(cypher)?;
    match ast {
        CypherQuery::TimeTravel { query, spec } => Ok((*query, Some(spec))),
        other => Ok((other, None)),
    }
}

/// Validate that a query AST is read-only (no CREATE, SET, DELETE, MERGE, REMOVE).
///
/// Returns `Ok(())` if the query contains only read clauses, or an error
/// message describing the violation.
pub fn validate_read_only(query: &CypherQuery) -> Result<(), String> {
    use uni_cypher::ast::{Clause, Query, Statement};

    fn check_statement(stmt: &Statement) -> Result<(), String> {
        for clause in &stmt.clauses {
            match clause {
                Clause::Create(_)
                | Clause::Merge(_)
                | Clause::Delete(_)
                | Clause::Set(_)
                | Clause::Remove(_) => {
                    return Err(
                        "Write clauses (CREATE, MERGE, DELETE, SET, REMOVE) are not allowed \
                         with VERSION AS OF / TIMESTAMP AS OF"
                            .to_string(),
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn check_query(q: &Query) -> Result<(), String> {
        match q {
            Query::Single(stmt) => check_statement(stmt),
            Query::Union { left, right, .. } => {
                check_query(left)?;
                check_query(right)
            }
            Query::Explain(inner) => check_query(inner),
            Query::TimeTravel { query, .. } => check_query(query),
            Query::Schema(_) | Query::Transaction(_) => {
                Err("Schema and transaction commands are not allowed \
                 with VERSION AS OF / TIMESTAMP AS OF"
                    .to_string())
            }
        }
    }

    check_query(query)
}
