// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A backend that reads at the read cut.
//!
//! Row and search reads (`scan`, `scan_stream`, `count_rows`, the vector and
//! full-text searches, and a published table's schema) see the table version
//! the published manifest names, through the inner backend's
//! [`StorageBackend::at_table_versions`] view; a table the manifest does not
//! name holds nothing a reader can see. Every other call, writes included,
//! goes to the inner backend at each table's newest version, which is what a
//! writer deciding what to write next must see.
//!
//! See [`crate::storage::read_cut`].

use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use parking_lot::Mutex;

use super::branching::ForkBranching;
use super::traits::{
    RecordBatchStream, RecordedPublish, StagedAppend, StagedFilesHold, StorageBackend,
    TableWriteGuard,
};
use super::types::*;
use crate::storage::read_cut::{Published, ReadCut};
use uni_common::core::schema::TokenizerConfig;

/// A published cut and the backend view that reads at it.
type CutView = (
    Arc<std::collections::BTreeMap<String, u64>>,
    Arc<dyn StorageBackend>,
);

/// See the module docs.
pub struct ReadCutBackend {
    inner: Arc<dyn StorageBackend>,
    cut: Arc<ReadCut>,
    /// The view of the last published cut, rebuilt when the cut moves.
    view: Mutex<Option<CutView>>,
}

/// Where a read of one table goes.
enum Reader {
    /// The table at its published version (or at its newest, with no cut).
    At(Arc<dyn StorageBackend>),
    /// The cut does not name the table: nothing is visible.
    Unpublished,
}

impl ReadCutBackend {
    /// Reads `inner`'s tables at `cut`.
    #[must_use]
    pub fn new(inner: Arc<dyn StorageBackend>, cut: Arc<ReadCut>) -> Self {
        Self {
            inner,
            cut,
            view: Mutex::new(None),
        }
    }

    /// The backend writes and newest-version reads go to.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn StorageBackend> {
        &self.inner
    }

    fn view_of(&self, published: &Published) -> Arc<dyn StorageBackend> {
        let mut held = self.view.lock();
        if let Some((tables, view)) = held.as_ref()
            && Arc::ptr_eq(tables, &published.tables)
        {
            return Arc::clone(view);
        }
        let versions: std::collections::BTreeMap<String, u64> = published
            .tables
            .iter()
            .map(|(t, v)| (t.clone(), *v))
            .collect();
        let view = self
            .inner
            .at_table_versions(&versions)
            .unwrap_or_else(|| Arc::clone(&self.inner));
        *held = Some((Arc::clone(&published.tables), Arc::clone(&view)));
        view
    }

    fn reader(&self, table: &str) -> Reader {
        match self.cut.view() {
            None => Reader::At(Arc::clone(&self.inner)),
            Some(published) if published.tables.contains_key(table) => {
                Reader::At(self.view_of(&published))
            }
            Some(_) => Reader::Unpublished,
        }
    }
}

#[async_trait]
impl StorageBackend for ReadCutBackend {
    async fn table_names(&self) -> Result<Vec<String>> {
        self.inner.table_names().await
    }

    async fn table_exists(&self, name: &str) -> Result<bool> {
        self.inner.table_exists(name).await
    }

    fn check_write_fence(&self) -> Result<()> {
        self.inner.check_write_fence()
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

    async fn notify_table_created(&self, name: &str) {
        self.inner.notify_table_created(name).await;
    }

    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>> {
        // A fork's branch is read at its newest version.
        if request.branch.is_some() {
            return self.inner.scan(request).await;
        }
        match self.reader(&request.table_name) {
            Reader::At(reader) => reader.scan(request).await,
            Reader::Unpublished => Ok(Vec::new()),
        }
    }

    fn filter_batch(&self, batch: &RecordBatch, filter: &FilterExpr) -> Result<Vec<bool>> {
        self.inner.filter_batch(batch, filter)
    }

    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream> {
        if request.branch.is_some() {
            return self.inner.scan_stream(request).await;
        }
        match self.reader(&request.table_name) {
            Reader::At(reader) => reader.scan_stream(request).await,
            Reader::Unpublished => Ok(Box::pin(futures::stream::empty())),
        }
    }

    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>> {
        match self.reader(name) {
            Reader::At(reader) => reader.get_table_schema(name).await,
            // Nothing of it is readable; its newest schema is what a writer
            // appending to it next must match.
            Reader::Unpublished => self.inner.get_table_schema(name).await,
        }
    }

    async fn count_rows(&self, table_name: &str, filter: Option<&FilterExpr>) -> Result<usize> {
        match self.reader(table_name) {
            Reader::At(reader) => reader.count_rows(table_name, filter).await,
            Reader::Unpublished => Ok(0),
        }
    }

    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()> {
        self.inner.write(table_name, batches, mode).await
    }

    async fn merge_insert(
        &self,
        table_name: &str,
        on: &[&str],
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.inner.merge_insert(table_name, on, batches).await
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

    async fn evolve_table_schema(
        &self,
        table_name: &str,
        add: &[arrow_schema::Field],
        relax_nullable: &[String],
    ) -> Result<()> {
        self.inner
            .evolve_table_schema(table_name, add, relax_nullable)
            .await
    }

    fn stages_invisibly(&self) -> bool {
        self.inner.stages_invisibly()
    }

    fn attach_read_cut(&self, cut: Arc<ReadCut>) {
        self.inner.attach_read_cut(cut);
    }

    async fn stage_append(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<StagedAppend> {
        self.inner.stage_append(table_name, batches).await
    }

    async fn publish_staged(&self, table_name: &str, staged: Vec<StagedAppend>) -> Result<()> {
        self.inner.publish_staged(table_name, staged).await
    }

    async fn discard_staged(&self, table_name: &str, staged: Vec<StagedAppend>) -> Result<()> {
        self.inner.discard_staged(table_name, staged).await
    }

    fn hold_staged_files(&self, table_name: &str) -> StagedFilesHold {
        self.inner.hold_staged_files(table_name)
    }

    fn staged_record(
        &self,
        table_name: &str,
        staged: &[StagedAppend],
    ) -> Result<Option<serde_json::Value>> {
        self.inner.staged_record(table_name, staged)
    }

    async fn publish_staged_record(
        &self,
        table_name: &str,
        record: &serde_json::Value,
        published_after: Option<u64>,
    ) -> Result<RecordedPublish> {
        self.inner
            .publish_staged_record(table_name, record, published_after)
            .await
    }

    async fn lock_table_for_write(&self, name: &str) -> TableWriteGuard {
        self.inner.lock_table_for_write(name).await
    }

    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>> {
        match self.reader(table_name) {
            Reader::At(reader) => reader.get_table_version(table_name).await,
            Reader::Unpublished => Ok(None),
        }
    }

    async fn rollback_table(&self, table_name: &str, target_version: u64) -> Result<()> {
        self.inner.rollback_table(table_name, target_version).await
    }

    async fn tag_table_version(&self, table_name: &str, tag: &str, version: u64) -> Result<()> {
        self.inner.tag_table_version(table_name, tag, version).await
    }

    async fn delete_table_tag(&self, table_name: &str, tag: &str) -> Result<()> {
        self.inner.delete_table_tag(table_name, tag).await
    }

    async fn optimize_table(
        &self,
        table_name: &str,
        version_retention: std::time::Duration,
    ) -> Result<OptimizeReport> {
        self.inner
            .optimize_table(table_name, version_retention)
            .await
    }

    async fn recover_staging(&self, table_name: &str) -> Result<()> {
        self.inner.recover_staging(table_name).await
    }

    fn invalidate_cache(&self, table_name: &str) {
        self.inner.invalidate_cache(table_name);
    }

    fn clear_cache(&self) {
        self.inner.clear_cache();
    }

    fn cache_usage(&self) -> Option<BackendCacheUsage> {
        self.inner.cache_usage()
    }

    fn base_uri(&self) -> &str {
        self.inner.base_uri()
    }

    fn branching(&self) -> Option<Arc<dyn ForkBranching>> {
        self.inner.branching()
    }

    fn at_table_versions(
        &self,
        versions: &std::collections::BTreeMap<String, u64>,
    ) -> Option<Arc<dyn StorageBackend>> {
        self.inner.at_table_versions(versions)
    }

    fn supports_vector_search(&self) -> bool {
        self.inner.supports_vector_search()
    }

    fn supports_full_text_search(&self) -> bool {
        self.inner.supports_full_text_search()
    }

    fn supports_scalar_index(&self) -> bool {
        self.inner.supports_scalar_index()
    }

    async fn vector_search(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        metric: DistanceMetric,
        filter: FilterExpr,
        opts: VectorQueryOpts,
        counters: Option<Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        match self.reader(table) {
            Reader::At(reader) => {
                reader
                    .vector_search(table, column, query, k, metric, filter, opts, counters)
                    .await
            }
            Reader::Unpublished => Ok(Vec::new()),
        }
    }

    async fn multivector_search(
        &self,
        table: &str,
        column: &str,
        query: &[Vec<f32>],
        k: usize,
        metric: DistanceMetric,
        filter: FilterExpr,
        opts: VectorQueryOpts,
        counters: Option<Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        match self.reader(table) {
            Reader::At(reader) => {
                reader
                    .multivector_search(table, column, query, k, metric, filter, opts, counters)
                    .await
            }
            Reader::Unpublished => Ok(Vec::new()),
        }
    }

    async fn full_text_search_with_unstored(
        &self,
        table: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: FilterExpr,
        unstored: Vec<(u64, String)>,
        analyzer: Option<&uni_common::core::schema::TokenizerConfig>,
        counters: Option<Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<TextSearch> {
        match self.reader(table) {
            Reader::At(reader) => {
                reader
                    .full_text_search_with_unstored(
                        table, column, query, k, filter, unstored, analyzer, counters,
                    )
                    .await
            }
            Reader::Unpublished => Ok(TextSearch {
                stored: Vec::new(),
                unstored: unstored
                    .into_iter()
                    .filter_map(|(id, text)| {
                        let score = crate::storage::manager::compute_text_relevance(query, &text);
                        (score > 0.0).then_some((id, score))
                    })
                    .collect(),
            }),
        }
    }

    async fn full_text_search(
        &self,
        table: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: FilterExpr,
        counters: Option<Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        match self.reader(table) {
            Reader::At(reader) => {
                reader
                    .full_text_search(table, column, query, k, filter, counters)
                    .await
            }
            Reader::Unpublished => Ok(Vec::new()),
        }
    }

    async fn create_vector_index(
        &self,
        table: &str,
        column: &str,
        name: &str,
        params: VectorIndexParams,
    ) -> Result<()> {
        self.inner
            .create_vector_index(table, column, name, params)
            .await
    }

    async fn create_fts_index(
        &self,
        table: &str,
        columns: &[&str],
        name: Option<&str>,
        tokenizer: &TokenizerConfig,
        with_positions: bool,
    ) -> Result<()> {
        self.inner
            .create_fts_index(table, columns, name, tokenizer, with_positions)
            .await
    }

    async fn create_scalar_index(
        &self,
        table: &str,
        columns: &[&str],
        index_type: ScalarIndexType,
        name: Option<&str>,
    ) -> Result<()> {
        self.inner
            .create_scalar_index(table, columns, index_type, name)
            .await
    }

    async fn drop_index(&self, table: &str, index_name: &str) -> Result<()> {
        self.inner.drop_index(table, index_name).await
    }

    async fn list_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        self.inner.list_indexes(table).await
    }
}
