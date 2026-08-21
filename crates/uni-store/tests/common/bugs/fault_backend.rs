// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A `StorageBackend` wrapper that delegates to an inner backend but can be
//! armed to fail `table_exists` with an error — modeling a transient
//! object-store directory-listing (LIST) failure. Used to show that the
//! `.unwrap_or(false)` sites collapse such a transient error into "table
//! absent" and silently return an empty result.

#![cfg(feature = "lance-backend")]
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use uni_store::backend::traits::{RecordBatchStream, StorageBackend, TableWriteGuard};
use uni_store::backend::types::{FilterExpr, ScanRequest, WriteMode};

pub struct FaultBackend {
    inner: Arc<dyn StorageBackend>,
    fail_table_exists: AtomicBool,
}

impl FaultBackend {
    pub fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fail_table_exists: AtomicBool::new(false),
        }
    }

    pub fn set_fail_table_exists(&self, on: bool) {
        self.fail_table_exists.store(on, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for FaultBackend {
    async fn table_names(&self) -> Result<Vec<String>> {
        self.inner.table_names().await
    }

    async fn table_exists(&self, name: &str) -> Result<bool> {
        if self.fail_table_exists.load(Ordering::SeqCst) {
            return Err(anyhow!("injected transient LIST failure for {name}"));
        }
        self.inner.table_exists(name).await
    }

    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        self.inner.create_table(name, batches).await
    }

    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        self.inner.create_empty_table(name, schema).await
    }

    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        self.inner.open_or_create_table(name, schema).await
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        self.inner.drop_table(name).await
    }

    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>> {
        self.inner.scan(request).await
    }

    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream> {
        self.inner.scan_stream(request).await
    }

    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>> {
        self.inner.get_table_schema(name).await
    }

    async fn count_rows(&self, table_name: &str, filter: Option<&FilterExpr>) -> Result<usize> {
        self.inner.count_rows(table_name, filter).await
    }

    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()> {
        self.inner.write(table_name, batches, mode).await
    }

    async fn delete_rows(&self, table_name: &str, filter: &FilterExpr) -> Result<()> {
        self.inner.delete_rows(table_name, filter).await
    }

    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()> {
        self.inner.replace_table_atomic(name, batches, schema).await
    }

    async fn lock_table_for_write(&self, name: &str) -> TableWriteGuard {
        self.inner.lock_table_for_write(name).await
    }

    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>> {
        self.inner.get_table_version(table_name).await
    }

    async fn rollback_table(&self, table_name: &str, target_version: u64) -> Result<()> {
        self.inner.rollback_table(table_name, target_version).await
    }

    async fn optimize_table(
        &self,
        table_name: &str,
        version_retention: std::time::Duration,
    ) -> Result<uni_store::backend::OptimizeReport> {
        self.inner
            .optimize_table(table_name, version_retention)
            .await
    }

    async fn recover_staging(&self, table_name: &str) -> Result<()> {
        self.inner.recover_staging(table_name).await
    }

    fn base_uri(&self) -> &str {
        self.inner.base_uri()
    }
}
