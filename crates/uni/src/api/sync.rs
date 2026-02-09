// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use crate::api::query_builder::QueryBuilder;
use crate::api::transaction::Transaction;
use uni_common::core::schema::{DataType, Schema};
use uni_common::{Result, UniError};
use uni_query::{ExecuteResult, QueryResult}; // for execute_internal? No, it's pub(crate).

/// Blocking API wrapper for Uni.
pub struct UniSync {
    inner: Option<Uni>,
    rt: tokio::runtime::Runtime,
}

impl UniSync {
    pub fn new(inner: Uni) -> Result<Self> {
        let rt = tokio::runtime::Runtime::new().map_err(UniError::Io)?;
        Ok(Self {
            inner: Some(inner),
            rt,
        })
    }

    /// Open an in-memory database (blocking)
    pub fn in_memory() -> Result<Self> {
        let rt = tokio::runtime::Runtime::new().map_err(UniError::Io)?;
        let inner = rt.block_on(Uni::in_memory().build())?;
        Ok(Self {
            inner: Some(inner),
            rt,
        })
    }

    fn inner(&self) -> &Uni {
        self.inner.as_ref().expect("UniSync already shut down")
    }

    pub fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.rt.block_on(self.inner().query(cypher))
    }

    pub fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        self.rt.block_on(self.inner().execute(cypher))
    }

    pub fn query_with<'a>(&'a self, cypher: &'a str) -> QueryBuilderSync<'a> {
        QueryBuilderSync {
            inner: self.inner().query_with(cypher),
            rt: &self.rt,
        }
    }

    pub fn schema_meta(&self) -> Schema {
        self.inner().get_schema()
    }

    pub fn schema(&self) -> SchemaBuilderSync<'_> {
        SchemaBuilderSync {
            inner: self.inner().schema(),
            rt: &self.rt,
        }
    }

    pub fn begin(&self) -> Result<TransactionSync<'_>> {
        let tx = self.rt.block_on(self.inner().begin())?;
        Ok(TransactionSync { tx, rt: &self.rt })
    }

    /// Shutdown the database gracefully (blocking).
    ///
    /// Note: This consumes self, which prevents the Drop impl from also
    /// triggering shutdown. Use this for explicit shutdown with error handling.
    pub fn shutdown(mut self) -> Result<()> {
        // Take ownership of the inner Uni to prevent Drop from also running
        if let Some(uni) = self.inner.take() {
            let result = self.rt.block_on(uni.shutdown());

            // Prevent Drop from running by forgetting self
            // (we've already done the cleanup in the async shutdown)
            std::mem::forget(self);

            result
        } else {
            Ok(()) // Already shut down
        }
    }
}

impl Drop for UniSync {
    fn drop(&mut self) {
        if let Some(ref uni) = self.inner {
            uni.shutdown_handle.shutdown_blocking();
            tracing::debug!("UniSync dropped");
        }
    }
}

pub struct TransactionSync<'a> {
    tx: Transaction<'a>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> TransactionSync<'a> {
    pub fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.rt.block_on(self.tx.query(cypher))
    }

    pub fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        self.rt.block_on(self.tx.execute(cypher))
    }

    pub fn commit(self) -> Result<()> {
        self.rt.block_on(self.tx.commit())
    }

    pub fn rollback(self) -> Result<()> {
        self.rt.block_on(self.tx.rollback())
    }
}

pub struct SchemaBuilderSync<'a> {
    inner: crate::api::schema::SchemaBuilder<'a>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> SchemaBuilderSync<'a> {
    pub fn label(self, name: &str) -> LabelBuilderSync<'a> {
        LabelBuilderSync {
            inner: self.inner.label(name),
            rt: self.rt,
        }
    }

    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilderSync<'a> {
        EdgeTypeBuilderSync {
            inner: self.inner.edge_type(name, from, to),
            rt: self.rt,
        }
    }

    pub fn apply(self) -> Result<()> {
        self.rt.block_on(self.inner.apply())
    }
}

pub struct LabelBuilderSync<'a> {
    inner: crate::api::schema::LabelBuilder<'a>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> LabelBuilderSync<'a> {
    pub fn property(mut self, name: &str, data_type: DataType) -> Self {
        self.inner = self.inner.property(name, data_type);
        self
    }

    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self {
        self.inner = self.inner.property_nullable(name, data_type);
        self
    }

    pub fn vector(mut self, name: &str, dimensions: usize) -> Self {
        self.inner = self.inner.vector(name, dimensions);
        self
    }

    pub fn done(self) -> SchemaBuilderSync<'a> {
        SchemaBuilderSync {
            inner: self.inner.done(),
            rt: self.rt,
        }
    }

    pub fn label(self, name: &str) -> LabelBuilderSync<'a> {
        self.done().label(name)
    }

    pub fn apply(self) -> Result<()> {
        self.rt.block_on(self.inner.apply())
    }
}

pub struct EdgeTypeBuilderSync<'a> {
    inner: crate::api::schema::EdgeTypeBuilder<'a>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> EdgeTypeBuilderSync<'a> {
    pub fn property(mut self, name: &str, data_type: DataType) -> Self {
        self.inner = self.inner.property(name, data_type);
        self
    }

    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self {
        self.inner = self.inner.property_nullable(name, data_type);
        self
    }

    pub fn done(self) -> SchemaBuilderSync<'a> {
        SchemaBuilderSync {
            inner: self.inner.done(),
            rt: self.rt,
        }
    }

    pub fn apply(self) -> Result<()> {
        self.rt.block_on(self.inner.apply())
    }
}

pub struct QueryBuilderSync<'a> {
    inner: QueryBuilder<'a>,
    rt: &'a tokio::runtime::Runtime,
}

impl<'a> QueryBuilderSync<'a> {
    pub fn param(mut self, name: &str, value: impl Into<uni_query::Value>) -> Self {
        self.inner = self.inner.param(name, value);
        self
    }

    pub fn fetch_all(self) -> Result<QueryResult> {
        self.rt.block_on(self.inner.fetch_all())
    }
}
