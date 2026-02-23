// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use std::collections::HashMap;
use uni_common::Result;
use uni_query::{ExecuteResult, QueryCursor, QueryResult, Value};

/// Builder for constructing and executing Cypher queries.
///
/// Supports parameter binding, timeouts, and resource limits.
///
/// # Examples
///
/// ```no_run
/// # use uni_db::{Uni, Value};
/// # async fn example(db: &Uni) -> uni_db::Result<()> {
/// let results = db.query_with("MATCH (n:Person) WHERE n.age > $min_age RETURN n")
///     .param("min_age", 18)
///     .timeout(std::time::Duration::from_secs(5))
///     .fetch_all()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "query builders do nothing until .fetch_all(), .execute(), or .query_cursor() is called"]
pub struct QueryBuilder<'a> {
    db: &'a Uni,
    cypher: String,
    params: HashMap<String, Value>,
    timeout: Option<std::time::Duration>,
    max_memory: Option<usize>,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(db: &'a Uni, cypher: &str) -> Self {
        Self {
            db,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout: None,
            max_memory: None,
        }
    }

    /// Set maximum execution time for this query.
    /// Overrides the default timeout in `UniConfig`.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Set maximum memory per query in bytes.
    /// Overrides the default limit in `UniConfig`.
    pub fn max_memory(mut self, bytes: usize) -> Self {
        self.max_memory = Some(bytes);
        self
    }

    /// Bind a parameter to the query.
    ///
    /// The parameter name should not include the `$` prefix.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.params.insert(name.to_string(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator or collection.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.params.insert(k.to_string(), v);
        }
        self
    }

    /// Execute the query and fetch all results into memory.
    pub async fn fetch_all(self) -> Result<QueryResult> {
        let mut db_config = self.db.config().clone();
        if let Some(t) = self.timeout {
            db_config.query_timeout = t;
        }
        if let Some(m) = self.max_memory {
            db_config.max_query_memory = m;
        }

        self.db
            .execute_internal_with_config(&self.cypher, self.params, db_config)
            .await
    }

    /// Execute a mutation (CREATE, SET, DELETE, etc.) and return affected row count.
    pub async fn execute(self) -> Result<ExecuteResult> {
        let db = self.db;
        let before = db.get_mutation_count().await;
        let result = self.fetch_all().await?;
        let affected_rows = if result.is_empty() {
            db.get_mutation_count().await.saturating_sub(before)
        } else {
            result.len()
        };
        Ok(ExecuteResult { affected_rows })
    }

    /// Execute the query and return a cursor for streaming results.
    ///
    /// Useful for large result sets to avoid loading everything into memory.
    pub async fn query_cursor(self) -> Result<QueryCursor> {
        let mut db_config = self.db.config().clone();
        if let Some(t) = self.timeout {
            db_config.query_timeout = t;
        }
        if let Some(m) = self.max_memory {
            db_config.max_query_memory = m;
        }

        self.db
            .execute_cursor_internal_with_config(&self.cypher, self.params, db_config)
            .await
    }
}
