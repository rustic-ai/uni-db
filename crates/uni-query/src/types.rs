// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Query result types and value re-exports.
//!
//! Core value types ([`Value`], [`Node`], [`Edge`], [`Path`]) are defined in
//! `uni_common::value` and re-exported here for backward compatibility.
//! Query-specific types ([`Row`], [`QueryResult`], [`QueryCursor`]) remain here.

use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use uni_common::{Result, UniError};

// Re-export core value types from uni-common.
#[doc(inline)]
pub use uni_common::value::{Edge, FromValue, Node, Path, Value};

/// Single result row from a query.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column names shared across all rows in a result set.
    pub columns: Arc<Vec<String>>,
    /// Column values for this row.
    pub values: Vec<Value>,
}

impl Row {
    /// Gets a typed value by column name.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Query` if the column is missing,
    /// or `UniError::Type` if it cannot be converted.
    pub fn get<T: FromValue>(&self, column: &str) -> Result<T> {
        let idx = self
            .columns
            .iter()
            .position(|c| c == column)
            .ok_or_else(|| UniError::Query {
                message: format!("Column '{}' not found", column),
                query: None,
            })?;
        self.get_idx(idx)
    }

    /// Gets a typed value by column index.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Query` if the index is out of bounds,
    /// or `UniError::Type` if it cannot be converted.
    pub fn get_idx<T: FromValue>(&self, index: usize) -> Result<T> {
        if index >= self.values.len() {
            return Err(UniError::Query {
                message: format!("Column index {} out of bounds", index),
                query: None,
            });
        }
        T::from_value(&self.values[index])
    }

    /// Tries to get a typed value, returning `None` on failure.
    pub fn try_get<T: FromValue>(&self, column: &str) -> Option<T> {
        self.get(column).ok()
    }

    /// Gets the raw `Value` by column name.
    pub fn value(&self, column: &str) -> Option<&Value> {
        let idx = self.columns.iter().position(|c| c == column)?;
        self.values.get(idx)
    }

    /// Returns all column-value pairs as a map.
    pub fn as_map(&self) -> HashMap<&str, &Value> {
        let mut map = HashMap::new();
        for (i, col) in self.columns.iter().enumerate() {
            if let Some(val) = self.values.get(i) {
                map.insert(col.as_str(), val);
            }
        }
        map
    }

    /// Converts this row to a JSON object.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self.as_map()).unwrap_or(serde_json::Value::Null)
    }
}

impl std::ops::Index<usize> for Row {
    type Output = Value;
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

/// Warnings emitted during query execution.
///
/// Warnings indicate potential issues but do not prevent the query from
/// completing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum QueryWarning {
    /// An index is unavailable (e.g., still being rebuilt).
    IndexUnavailable {
        /// The label that the index is for.
        label: String,
        /// The name of the unavailable index.
        index_name: String,
        /// Reason the index is unavailable.
        reason: String,
    },
    /// A property filter could not use an index.
    NoIndexForFilter {
        /// The label being filtered.
        label: String,
        /// The property being filtered.
        property: String,
    },
    /// Generic warning message.
    Other(String),
}

impl std::fmt::Display for QueryWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryWarning::IndexUnavailable {
                label,
                index_name,
                reason,
            } => {
                write!(
                    f,
                    "Index '{}' on label '{}' is unavailable: {}",
                    index_name, label, reason
                )
            }
            QueryWarning::NoIndexForFilter { label, property } => {
                write!(
                    f,
                    "No index available for filter on {}.{}, using full scan",
                    label, property
                )
            }
            QueryWarning::Other(msg) => write!(f, "{}", msg),
        }
    }
}

/// Collection of query result rows.
#[derive(Debug)]
pub struct QueryResult {
    /// Column names shared across all rows.
    pub columns: Arc<Vec<String>>,
    /// Result rows.
    pub rows: Vec<Row>,
    /// Warnings emitted during query execution.
    pub warnings: Vec<QueryWarning>,
}

impl QueryResult {
    /// Returns the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if there are no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns all rows.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Consumes the result, returning the rows.
    pub fn into_rows(self) -> Vec<Row> {
        self.rows
    }

    /// Returns an iterator over the rows.
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    /// Returns warnings emitted during execution.
    pub fn warnings(&self) -> &[QueryWarning] {
        &self.warnings
    }

    /// Returns `true` if the query produced any warnings.
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

/// Result of a write operation (CREATE, SET, DELETE, etc.).
#[derive(Debug)]
pub struct ExecuteResult {
    /// Number of entities affected.
    pub affected_rows: usize,
}

/// Cursor-based result streaming for large result sets.
pub struct QueryCursor {
    /// Column names shared across all rows.
    pub columns: Arc<Vec<String>>,
    /// Async stream of row batches.
    pub stream: Pin<Box<dyn Stream<Item = Result<Vec<Row>>> + Send>>,
}

impl QueryCursor {
    /// Returns the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Fetches the next batch of rows.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<Row>>> {
        use futures::StreamExt;
        self.stream.next().await
    }

    /// Consumes all remaining rows into a single vector.
    ///
    /// # Errors
    ///
    /// Returns the first error encountered while streaming.
    pub async fn collect_remaining(mut self) -> Result<Vec<Row>> {
        use futures::StreamExt;
        let mut rows = Vec::new();
        while let Some(batch_res) = self.stream.next().await {
            rows.extend(batch_res?);
        }
        Ok(rows)
    }
}
