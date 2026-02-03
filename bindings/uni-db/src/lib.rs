// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Python bindings for the Uni embedded graph database.
//!
//! This module provides PyO3-based Python bindings that expose the Uni graph
//! database API to Python applications. It includes both synchronous and
//! asynchronous database management, query execution, schema management,
//! and bulk loading.

pub mod async_api;
pub mod builders;
pub mod convert;
pub mod core;
pub mod sync_api;
pub mod types;

use pyo3::prelude::*;

/// Python module for the Uni embedded graph database.
#[pymodule]
fn _uni_db(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialise the tokio runtime used by pyo3-async-runtimes with an 8 MB
    // worker-thread stack.  The query executor builds deeply nested async state
    // machines that overflow the default 2 MB stack in debug builds.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_stack_size(8 * 1024 * 1024);
    pyo3_async_runtimes::tokio::init(builder);

    // Sync main classes
    m.add_class::<sync_api::Database>()?;
    m.add_class::<builders::DatabaseBuilder>()?;
    m.add_class::<sync_api::Transaction>()?;

    // Sync query
    m.add_class::<builders::QueryBuilder>()?;

    // Schema
    m.add_class::<builders::SchemaBuilder>()?;
    m.add_class::<builders::LabelBuilder>()?;
    m.add_class::<builders::EdgeTypeBuilder>()?;

    // Session
    m.add_class::<builders::SessionBuilder>()?;
    m.add_class::<builders::Session>()?;

    // Bulk loading
    m.add_class::<builders::BulkWriterBuilder>()?;
    m.add_class::<builders::BulkWriter>()?;

    // Async classes
    m.add_class::<async_api::AsyncDatabase>()?;
    m.add_class::<async_api::AsyncDatabaseBuilder>()?;
    m.add_class::<async_api::AsyncTransaction>()?;
    m.add_class::<async_api::AsyncSession>()?;
    m.add_class::<async_api::AsyncSessionBuilder>()?;
    m.add_class::<async_api::AsyncBulkWriter>()?;
    m.add_class::<async_api::AsyncBulkWriterBuilder>()?;
    m.add_class::<async_api::AsyncQueryBuilder>()?;
    m.add_class::<async_api::AsyncSchemaBuilder>()?;
    m.add_class::<async_api::AsyncLabelBuilder>()?;
    m.add_class::<async_api::AsyncEdgeTypeBuilder>()?;

    // Data classes
    m.add_class::<types::LabelInfo>()?;
    m.add_class::<types::PropertyInfo>()?;
    m.add_class::<types::IndexInfo>()?;
    m.add_class::<types::ConstraintInfo>()?;
    m.add_class::<types::BulkStats>()?;
    m.add_class::<types::BulkProgress>()?;

    Ok(())
}
