// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Result;
use uni_query::{ExecuteResult, QueryResult, Value};

/// Builder for creating query sessions with scoped variables
pub struct SessionBuilder<'a> {
    db: &'a Uni,
    variables: HashMap<String, Value>,
}

impl<'a> SessionBuilder<'a> {
    pub fn new(db: &'a Uni) -> Self {
        Self {
            db,
            variables: HashMap::new(),
        }
    }

    /// Set a session variable
    pub fn set<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.variables.insert(key.into(), value.into());
        self
    }

    /// Build the session (variables become immutable)
    pub fn build(self) -> Session<'a> {
        Session {
            db: self.db,
            variables: Arc::new(self.variables),
        }
    }
}

/// A query session with scoped variables
pub struct Session<'a> {
    db: &'a Uni,
    variables: Arc<HashMap<String, Value>>,
}

impl<'a> Session<'a> {
    /// Execute a query with session variables available
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.query_with(cypher).execute().await
    }

    /// Execute a query with additional parameters
    pub fn query_with(&self, cypher: &str) -> SessionQueryBuilder<'a, '_> {
        SessionQueryBuilder {
            session: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
        }
    }

    /// Execute a mutation
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        self.query_with(cypher).execute_mutation().await
    }

    /// Execute a mutation with additional parameters using a builder.
    ///
    /// Alias for [`query_with`](Self::query_with) that clarifies intent for mutations.
    pub fn execute_with(&self, cypher: &str) -> SessionQueryBuilder<'a, '_> {
        self.query_with(cypher)
    }

    /// Get a session variable value
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.variables.get(key)
    }
}

pub struct SessionQueryBuilder<'a, 'b> {
    session: &'b Session<'a>,
    cypher: String,
    params: HashMap<String, Value>,
}

impl<'a, 'b> SessionQueryBuilder<'a, 'b> {
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    pub async fn execute(self) -> Result<QueryResult> {
        let params = Self::merge_params_internal(self.params, &self.session.variables);
        self.session.db.execute_internal(&self.cypher, params).await
    }

    pub async fn execute_mutation(self) -> Result<ExecuteResult> {
        let params = Self::merge_params_internal(self.params, &self.session.variables);
        let before = self.session.db.get_mutation_count().await;
        let result = self
            .session
            .db
            .execute_internal(&self.cypher, params)
            .await?;
        let affected_rows = if result.is_empty() {
            self.session
                .db
                .get_mutation_count()
                .await
                .saturating_sub(before)
        } else {
            result.len()
        };
        Ok(ExecuteResult { affected_rows })
    }

    fn merge_params_internal(
        mut params: HashMap<String, Value>,
        session_vars: &HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        let session_map: HashMap<String, Value> = session_vars.clone();

        if let Some(Value::Map(existing)) = params.get_mut("session") {
            for (k, v) in session_map {
                existing.entry(k).or_insert(v);
            }
        } else {
            params.insert("session".to_string(), Value::Map(session_map));
        }
        params
    }
}
