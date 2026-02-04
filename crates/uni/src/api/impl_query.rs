// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use crate::api::query_builder::QueryBuilder;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::{Result, UniConfig, UniError};
use uni_query::{
    ExecuteResult, ExplainOutput, LogicalPlan, ProfileOutput, QueryCursor, QueryResult, Row,
    Value as ApiValue,
};

/// Convert a parse error into `UniError::Parse`.
fn into_parse_error(e: impl std::fmt::Display) -> UniError {
    UniError::Parse {
        message: e.to_string(),
        position: None,
        line: None,
        column: None,
        context: None,
    }
}

/// Convert an error into the appropriate `UniError` type.
/// Errors starting with "SyntaxError:" are treated as parse/syntax errors.
/// All other errors are query/semantic errors.
fn into_query_error(e: impl std::fmt::Display, cypher: &str) -> UniError {
    let msg = e.to_string();
    // Errors containing "SyntaxError:" prefix should be treated as syntax errors
    // This covers validation errors like VariableTypeConflict, UndefinedVariable, etc.
    if msg.starts_with("SyntaxError:") {
        UniError::Parse {
            message: msg,
            position: None,
            line: None,
            column: None,
            context: Some(cypher.to_string()),
        }
    } else {
        UniError::Query {
            message: msg,
            query: Some(cypher.to_string()),
        }
    }
}

/// Extract projection column names from a LogicalPlan, preserving query order.
/// Returns None if the plan doesn't have projections at the top level.
fn extract_projection_order(plan: &LogicalPlan) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::Project { projections, .. } => Some(
            projections
                .iter()
                .map(|(expr, alias)| alias.clone().unwrap_or_else(|| expr.to_string_repr()))
                .collect(),
        ),
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            let mut names: Vec<String> = group_by.iter().map(|e| e.to_string_repr()).collect();
            names.extend(aggregates.iter().map(|e| e.to_string_repr()));
            Some(names)
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Filter { input, .. } => extract_projection_order(input),
        _ => None,
    }
}

impl Uni {
    /// Explain a Cypher query plan without executing it.
    pub async fn explain(&self, cypher: &str) -> Result<ExplainOutput> {
        let ast = uni_query::parse_cypher(cypher).map_err(into_parse_error)?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone().into());
        planner
            .explain_plan(ast)
            .map_err(|e| into_query_error(e, cypher))
    }

    /// Profile a Cypher query execution.
    pub async fn profile(&self, cypher: &str) -> Result<(QueryResult, ProfileOutput)> {
        let ast = uni_query::parse_cypher(cypher).map_err(into_parse_error)?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone().into());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(self.config.clone());
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let json_params: HashMap<String, serde_json::Value> = HashMap::new(); // TODO: Support params in profile

        // Extract projection order
        let projection_order = extract_projection_order(&logical_plan);

        let (results, profile_output) = executor
            .profile(logical_plan, &json_params)
            .await
            .map_err(|e| into_query_error(e, cypher))?;

        // Convert results to QueryResult
        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let json_val = map.get(col).cloned().unwrap_or(serde_json::Value::Null);
                    values.push(ApiValue::from(json_val));
                }
                Row {
                    columns: columns.clone(),
                    values,
                }
            })
            .collect();

        Ok((
            QueryResult {
                columns,
                rows,
                warnings: Vec::new(),
            },
            profile_output,
        ))
    }

    /// Execute a Cypher query
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.execute_internal(cypher, HashMap::new()).await
    }

    /// Execute query returning a cursor for streaming results
    pub async fn query_cursor(&self, cypher: &str) -> Result<QueryCursor> {
        self.execute_cursor_internal(cypher, HashMap::new()).await
    }

    /// Execute a query with parameters using a builder
    pub fn query_with(&self, cypher: &str) -> QueryBuilder<'_> {
        QueryBuilder::new(self, cypher)
    }

    /// Execute a mutation with parameters using a builder.
    ///
    /// Alias for [`query_with`](Self::query_with) that clarifies intent for
    /// mutation queries. Use `.param()` to bind parameters, then `.execute()`
    /// to run the mutation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::Uni;
    /// # async fn example(db: &Uni) -> uni_db::Result<()> {
    /// db.execute_with("CREATE (p:Person {name: $name, age: $age})")
    ///     .param("name", "Alice")
    ///     .param("age", 30)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute_with(&self, cypher: &str) -> QueryBuilder<'_> {
        self.query_with(cypher)
    }

    /// Execute a modification query (CREATE, SET, DELETE, etc.)
    /// Returns the number of affected rows/elements
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        let result = self.execute_internal(cypher, HashMap::new()).await?;
        Ok(ExecuteResult {
            affected_rows: result.len(),
        })
    }

    pub(crate) async fn execute_cursor_internal(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<QueryCursor> {
        self.execute_cursor_internal_with_config(cypher, params, self.config.clone())
            .await
    }

    pub(crate) async fn execute_cursor_internal_with_config(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
    ) -> Result<QueryCursor> {
        let ast = uni_query::parse_cypher(cypher).map_err(into_parse_error)?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone().into());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let json_params: HashMap<String, serde_json::Value> =
            params.into_iter().map(|(k, v)| (k, v.into())).collect();

        let projection_order = extract_projection_order(&logical_plan);
        let projection_order_for_rows = projection_order.clone();
        let cypher_for_error = cypher.to_string();

        let stream = executor.execute_stream(logical_plan, self.properties.clone(), json_params);

        let row_stream = stream.map(move |batch_res| {
            let results = batch_res.map_err(|e| UniError::Query {
                message: e.to_string(),
                query: Some(cypher_for_error.clone()),
            })?;

            if results.is_empty() {
                return Ok(vec![]);
            }

            // Determine columns for this batch (should be stable for the whole query)
            let columns = if let Some(order) = &projection_order_for_rows {
                Arc::new(order.clone())
            } else {
                let mut cols: Vec<String> = results[0].keys().cloned().collect();
                cols.sort();
                Arc::new(cols)
            };

            let rows = results
                .into_iter()
                .map(|map| {
                    let mut values = Vec::with_capacity(columns.len());
                    for col in columns.iter() {
                        let json_val = map.get(col).cloned().unwrap_or(serde_json::Value::Null);
                        values.push(ApiValue::from(json_val));
                    }
                    Row {
                        columns: columns.clone(),
                        values,
                    }
                })
                .collect();

            Ok(rows)
        });

        // We need columns ahead of time for QueryCursor if possible.
        let columns = if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            Arc::new(vec![])
        };

        Ok(QueryCursor {
            columns,
            stream: Box::pin(row_stream),
        })
    }

    pub(crate) async fn execute_internal(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<QueryResult> {
        self.execute_internal_with_config(cypher, params, self.config.clone())
            .await
    }

    pub(crate) async fn execute_internal_with_config(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
    ) -> Result<QueryResult> {
        // Single parse: extract time-travel clause if present
        let (ast, tt_spec) =
            uni_query::parse_and_extract_time_travel(cypher).map_err(into_parse_error)?;

        if let Some(spec) = tt_spec {
            uni_query::validate_read_only(&ast).map_err(|msg| into_query_error(msg, cypher))?;
            // Resolve to snapshot and execute on pinned instance
            let snapshot_id = self.resolve_time_travel(&spec).await?;
            let pinned = self.at_snapshot(&snapshot_id).await?;
            return pinned
                .execute_ast_internal(ast, cypher, params, config)
                .await;
        }

        self.execute_ast_internal(ast, cypher, params, config).await
    }

    /// Execute a pre-parsed Cypher AST through the planner and executor.
    ///
    /// The `cypher` parameter is the original query string, used only for
    /// error messages.
    async fn execute_ast_internal(
        &self,
        ast: uni_query::CypherQuery,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
    ) -> Result<QueryResult> {
        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone().into());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let json_params: HashMap<String, serde_json::Value> =
            params.into_iter().map(|(k, v)| (k, v.into())).collect();

        let projection_order = extract_projection_order(&logical_plan);

        let results = executor
            .execute(logical_plan, &self.properties, &json_params)
            .await
            .map_err(|e| into_query_error(e, cypher))?;

        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let json_val = map.get(col).cloned().unwrap_or(serde_json::Value::Null);
                    values.push(ApiValue::from(json_val));
                }
                Row {
                    columns: columns.clone(),
                    values,
                }
            })
            .collect();

        Ok(QueryResult {
            columns,
            rows,
            warnings: Vec::new(),
        })
    }

    /// Resolve a time-travel spec to a snapshot ID.
    async fn resolve_time_travel(&self, spec: &uni_query::TimeTravelSpec) -> Result<String> {
        match spec {
            uni_query::TimeTravelSpec::Version(id) => Ok(id.clone()),
            uni_query::TimeTravelSpec::Timestamp(ts_str) => {
                let ts = chrono::DateTime::parse_from_rfc3339(ts_str)
                    .map_err(|e| {
                        into_parse_error(format!("Invalid timestamp '{}': {}", ts_str, e))
                    })?
                    .with_timezone(&chrono::Utc);
                let manifest = self
                    .storage
                    .snapshot_manager()
                    .find_snapshot_at_time(ts)
                    .await
                    .map_err(UniError::Internal)?
                    .ok_or_else(|| UniError::Query {
                        message: format!("No snapshot found at or before {}", ts_str),
                        query: None,
                    })?;
                Ok(manifest.snapshot_id)
            }
        }
    }
}
