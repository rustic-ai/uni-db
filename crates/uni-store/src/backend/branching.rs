// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Backend-neutral fork branching abstraction.
//!
//! Forks need copy-on-write isolation over a set of tables: a child branch that
//! reads through to its parent for untouched rows and shadows it for its own
//! writes. Lance provides this natively via branches plus the `base_paths`
//! chain, and until now the fork engine called `backend::lance_branch`'s free
//! functions directly — which made forking structurally unavailable to any
//! other backend and leaked `*.lance` URI construction as far up as
//! `crates/uni/src/api/fork.rs`.
//!
//! [`ForkBranching`] is that capability expressed in backend-neutral terms.
//! Two deliberate differences from the `lance_branch` free functions it
//! replaces:
//!
//! - **Logical table names, not dataset URIs.** Every method takes a table
//!   name; resolving it to physical storage is the implementation's job. This
//!   is what removes the duplicated `format!("{base}/{table}.lance")` from
//!   `BranchedBackend` and from `crates/uni`.
//! - **`Vec<RecordBatch>` + an explicit schema**, matching
//!   [`StorageBackend::replace_table_atomic`]. The free functions took a
//!   generic `R: RecordBatchReader`, which is not object-safe; the schema must
//!   travel separately because an empty batch vector carries none, and branch
//!   creation needs a schema precisely in that case.
//!
//! A backend without copy-on-write branching simply returns `None` from
//! [`StorageBackend::branching`], and forks are unavailable rather than
//! silently incorrect.
//!
//! [`StorageBackend::replace_table_atomic`]: super::traits::StorageBackend::replace_table_atomic
//! [`StorageBackend::branching`]: super::traits::StorageBackend::branching

use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use uni_common::core::schema::TokenizerConfig;

use super::types::{FilterExpr, TextSearch, VectorQueryOpts};

/// Copy-on-write branching over individual tables, as required by the fork
/// engine.
///
/// Obtained from [`StorageBackend::branching`]. All methods take a *logical*
/// table name (`vertices_Person`, `adjacency_KNOWS_out`, …) and a branch name;
/// the implementation owns the mapping to physical storage.
///
/// # Contract
///
/// - **Branch creation need not be atomic.** Lance's is two-phase, so a crash
///   can leave a zombie branch. Implementations must make
///   [`Self::delete_branch`] a force-delete that tolerates a partially created
///   branch, since that is how recovery clears the way for a retry.
/// - **Reads on a branch must fuse.** A scan of `branch` is required to
///   surface both the branch's own rows and those inherited from its parent.
/// - **Versions are opaque monotonic counters.** Callers only ever pass a
///   value previously returned by [`Self::current_version`] or
///   [`Self::current_version_on_branch`] back into a create call.
///
/// [`StorageBackend::branching`]: super::traits::StorageBackend::branching
#[async_trait]
pub trait ForkBranching: Send + Sync + 'static {
    // ========================
    // Branch lifecycle
    // ========================

    /// Create `branch` on `table`, rooted at `parent_version` of the trunk.
    async fn create_branch(&self, table: &str, branch: &str, parent_version: u64) -> Result<()>;

    /// Create `new_branch` rooted at `parent_version` of `parent_branch`
    /// rather than of the trunk — the nested-fork case.
    ///
    /// The resulting read chain must be `new_branch → parent_branch → trunk`.
    /// Rooting a nested child on the trunk instead would skip the parent
    /// fork's writes and lose snapshot isolation against it.
    async fn create_branch_from(
        &self,
        table: &str,
        new_branch: &str,
        parent_branch: &str,
        parent_version: u64,
    ) -> Result<()>;

    /// Force-delete `branch`, tolerating a partially created one.
    ///
    /// Must succeed (or report "already absent") when called on the debris of
    /// a crashed [`Self::create_branch`]; recovery relies on this to retry.
    async fn delete_branch(&self, table: &str, branch: &str) -> Result<()>;

    /// List every branch present on `table`.
    async fn list_branches(&self, table: &str) -> Result<Vec<String>>;

    /// Current version of `table`'s trunk.
    async fn current_version(&self, table: &str) -> Result<u64>;

    /// Current version of `branch` on `table`.
    async fn current_version_on_branch(&self, table: &str, branch: &str) -> Result<u64>;

    /// Whether `table` exists in physical storage at all.
    ///
    /// Distinguishes "never written" from "written and empty", which decides
    /// whether a fork branches an existing table or must materialize one.
    async fn table_exists(&self, table: &str) -> Result<bool>;

    // ========================
    // Tags
    // ========================

    /// Pin `branch`'s current version under `tag`.
    ///
    /// The pin must survive whatever garbage collection the backend performs,
    /// so that tag-then-drop retains an auditable snapshot. Later writes to
    /// the branch do not move the tag.
    async fn create_tag(&self, table: &str, tag: &str, branch: &str) -> Result<()>;

    /// Delete `tag` from `table`.
    async fn delete_tag(&self, table: &str, tag: &str) -> Result<()>;

    /// List `(tag, pinned_version)` for `table`.
    async fn list_tags(&self, table: &str) -> Result<Vec<(String, u64)>>;

    // ========================
    // Writes against a branch
    // ========================

    /// Append `batches` to `branch`.
    async fn write_to_branch(
        &self,
        table: &str,
        branch: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()>;

    /// Whether [`Self::stage_on_branch`] and its siblings are implemented.
    ///
    /// `false` (the default) leaves a fork's bulk load on the protocol that
    /// writes visibly.
    fn stages_on_branch(&self) -> bool {
        false
    }

    /// Write `batches` as data files for `branch` without committing them.
    ///
    /// The branch counterpart of `StorageBackend::stage_append`: no reader of
    /// the branch sees the rows until [`Self::publish_on_branch`] commits the
    /// returned payload. The payload is implementation-private.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch does not exist, the write fails, or the
    /// implementation cannot stage ([`Self::stages_on_branch`] is `false`).
    async fn stage_on_branch(
        &self,
        table: &str,
        branch: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync>> {
        let _ = batches;
        anyhow::bail!("stage_on_branch('{table}', '{branch}'): this backend cannot stage")
    }

    /// Commit payloads from [`Self::stage_on_branch`] on `branch`, as one
    /// commit onto the branch's current version.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails, in which case none of the rows
    /// are visible, or if a payload was not staged by this implementation.
    async fn publish_on_branch(
        &self,
        table: &str,
        branch: &str,
        staged: Vec<Box<dyn std::any::Any + Send + Sync>>,
    ) -> Result<()> {
        let _ = staged;
        anyhow::bail!("publish_on_branch('{table}', '{branch}'): this backend cannot stage")
    }

    /// Delete the files of payloads from [`Self::stage_on_branch`], never
    /// committing them. Best effort, as for `StorageBackend::discard_staged`.
    ///
    /// # Errors
    ///
    /// Returns an error if reclaiming the storage failed.
    async fn discard_on_branch(
        &self,
        table: &str,
        branch: &str,
        staged: Vec<Box<dyn std::any::Any + Send + Sync>>,
    ) -> Result<()> {
        let _ = (table, branch, staged);
        Ok(())
    }

    /// A durable description of payloads from [`Self::stage_on_branch`], from
    /// which [`Self::publish_staged_record_on_branch`] can publish them in a
    /// later process; `None` when they hold no rows.
    ///
    /// The branch counterpart of `StorageBackend::staged_record`.
    ///
    /// # Errors
    ///
    /// Returns an error if a payload was not staged by this implementation,
    /// or the implementation cannot stage.
    fn staged_record_on_branch(
        &self,
        table: &str,
        branch: &str,
        staged: &[&(dyn std::any::Any + Send + Sync)],
    ) -> Result<Option<serde_json::Value>> {
        let _ = staged;
        anyhow::bail!("staged_record_on_branch('{table}', '{branch}'): this backend cannot stage")
    }

    /// Publish the rows `record` describes on `branch`, unless an earlier
    /// attempt already did -- exactly once, as
    /// `StorageBackend::publish_staged_record` does for a table.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails, if `record` was not written by
    /// this implementation, or if only part of the recorded files is present.
    async fn publish_staged_record_on_branch(
        &self,
        table: &str,
        branch: &str,
        record: &serde_json::Value,
    ) -> Result<crate::backend::traits::RecordedPublish> {
        let _ = record;
        anyhow::bail!(
            "publish_staged_record_on_branch('{table}', '{branch}'): this backend cannot stage"
        )
    }

    /// Widen `branch`'s stored schema: add all-null nullable columns, and/or
    /// relax `NOT NULL` on existing ones.
    ///
    /// A branch carries its own manifest and schema copy, so a widening on the
    /// trunk does not reach it. Widening-only, metadata-only: no row is
    /// rewritten and rows inherited from the parent keep their `base_paths`
    /// indirection.
    async fn evolve_branch_schema(
        &self,
        table: &str,
        branch: &str,
        add: &[arrow_schema::Field],
        relax_nullable: &[String],
    ) -> Result<()>;

    /// Delete rows matching `filter` from `branch`.
    ///
    /// A trivially-true filter ([`FilterExpr::Literal`]`(true)`, or an empty
    /// [`FilterExpr::And`]) is rejected rather than treated as "match all".
    /// On a read "no predicate" is harmlessly "every row", but on a delete it
    /// would silently empty the branch — and a caller that produced it by
    /// accident (an unset `Option`, a filter that optimized away) would get
    /// exactly that. Clearing a branch deliberately is
    /// [`Self::replace_branch_tip`] with no batches.
    async fn delete_from_branch(
        &self,
        table: &str,
        branch: &str,
        filter: &FilterExpr,
    ) -> Result<()>;

    /// Upsert `batches` into `branch`, joining on `on`; matched rows are
    /// updated wholesale and unmatched source rows are dropped.
    async fn merge_insert_on_branch(
        &self,
        table: &str,
        branch: &str,
        on: &[&str],
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()>;

    /// Replace `branch`'s entire contents with `batches`.
    async fn replace_branch_tip(
        &self,
        table: &str,
        branch: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()>;

    /// Materialize `table` as **empty** on the trunk, then branch from it.
    ///
    /// For a table the fork is the first to write. The trunk copy must stay
    /// empty and the caller writes the real rows to the branch afterwards:
    /// seeding the trunk with those rows instead would leak fork data into
    /// primary's view, since primary always resolves through the trunk.
    async fn create_empty_table_then_branch(
        &self,
        table: &str,
        branch: &str,
        schema: Arc<ArrowSchema>,
    ) -> Result<()>;

    // ========================
    // Fork-local indexes
    // ========================

    /// Build a scalar index on `column`, local to `branch`.
    async fn create_scalar_index_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        index_name: &str,
    ) -> Result<()>;

    /// Build a vector index on `column`, local to `branch`.
    async fn create_vector_index_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        index_name: &str,
    ) -> Result<()>;

    /// Build a full-text index on `column`, local to `branch`.
    async fn create_fts_index_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        index_name: &str,
        tokenizer: &TokenizerConfig,
    ) -> Result<()>;

    // ========================
    // Search against a branch
    // ========================

    /// Vector kNN against `branch`, fused across the parent chain.
    ///
    /// The requested metric also applies to flat/unindexed branch scans.
    #[allow(clippy::too_many_arguments)]
    async fn vector_search_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        query: &[f32],
        k: usize,
        metric: super::types::DistanceMetric,
        filter: &FilterExpr,
        opts: VectorQueryOpts,
    ) -> Result<Vec<RecordBatch>>;

    /// [`StorageBackend::full_text_search_with_unstored`](crate::backend::traits::StorageBackend::full_text_search_with_unstored)
    /// against `branch`, whose rows and statistics include the parent
    /// chain's.
    #[allow(clippy::too_many_arguments)]
    async fn full_text_search_with_unstored_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: &FilterExpr,
        unstored: Vec<(u64, String)>,
    ) -> Result<TextSearch>;

    /// Full-text search against `branch`, fused across the parent chain.
    #[allow(clippy::too_many_arguments)]
    async fn full_text_search_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: &FilterExpr,
    ) -> Result<Vec<RecordBatch>>;
}
