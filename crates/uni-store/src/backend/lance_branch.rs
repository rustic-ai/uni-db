// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Branch primitives for Lance datasets, used by the fork feature.
//!
//! `lancedb` 0.27.1 does not expose Lance's branch API, so this module
//! descends one layer to operate on `lance::Dataset` directly. It provides
//! free functions for create / delete / list / open-branch and version
//! lookup, used by the fork registry and recovery driver in
//! `crate::fork`.
//!
//! # Idempotence and recovery
//!
//! `lance::Dataset::create_branch` is a two-phase operation (shallow
//! clone of the dataset, then `BranchContents` write) and is **not
//! atomic**. A crash between the two phases leaves a "zombie" branch
//! dataset that prevents re-creating the same branch name. The recovery
//! driver must call `force_delete_branch` before retrying. See
//! `crates/uni-store/src/fork/recovery.rs` for the recovery logic.

// Rust guideline compliant

use std::sync::Arc;

use crate::backend::branching::ForkBranching;
use crate::backend::types::{FilterExpr, TextSearch, VectorQueryOpts};
use anyhow::{Context, Result};
use arrow_schema::{Field, Schema as ArrowSchema};
use lance::Dataset;

/// Open the dataset at `uri` on its main branch.
///
/// Used by Phase 1 fork-creation flows to discover the current version
/// before branching, and by recovery to enumerate live branches.
///
/// # Errors
///
/// Returns an error if the dataset does not exist or cannot be opened.
pub async fn open_dataset(uri: &str) -> Result<Dataset> {
    Dataset::open(uri)
        .await
        .with_context(|| format!("open lance dataset at {uri}"))
}

/// Open the dataset at `uri` on the given branch.
///
/// Returns a `Dataset` whose subsequent reads resolve through the branch
/// chain (`base_paths`) up to primary.
///
/// # Errors
///
/// Returns an error if the dataset or branch cannot be opened.
pub async fn open_branch(uri: &str, branch: &str) -> Result<Dataset> {
    let dataset = open_dataset(uri).await?;
    dataset
        .checkout_branch(branch)
        .await
        .with_context(|| format!("checkout branch {branch} on {uri}"))
}

/// Return the current version number of the dataset's main branch.
///
/// The returned u64 is the parent_version handed to [`create_branch`].
///
/// # Errors
///
/// Returns an error if the dataset cannot be opened.
pub async fn current_version(uri: &str) -> Result<u64> {
    let dataset = open_dataset(uri).await?;
    Ok(dataset.version().version)
}

/// Create a new branch off the dataset's main branch at `parent_version`.
///
/// Phase 1 of fork creation calls this once per Lance dataset that
/// already exists in the schema at fork point. The resulting branch
/// shares fragments with the parent until its first write.
///
/// **Not atomic.** A crash between the shallow-clone and BranchContents
/// phases leaves a zombie branch dataset. Recovery must use
/// `force_delete_branch` to clean up before retry.
///
/// # Errors
///
/// Returns an error if the branch already exists, the parent version
/// does not exist, or the underlying object store call fails.
///
/// # Fault injection
///
/// Setting `UNI_FORK_INJECT_FAIL_AFTER` to a non-negative integer N
/// causes the Nth and subsequent invocations (in this process) to
/// return an error before reaching `create_branch`. Used by the
/// recovery test suite to drive partial-create scenarios. The counter
/// is process-local; no real production path reads this variable.
pub async fn create_branch(uri: &str, branch: &str, parent_version: u64) -> Result<()> {
    inject_fault_create_branch()?;
    let mut dataset = open_dataset(uri).await?;
    dataset
        .create_branch(branch, parent_version, None)
        .await
        .with_context(|| format!("create branch {branch} on {uri} at v{parent_version}"))?;
    Ok(())
}

/// Return the current version number of the named branch (Phase 3).
///
/// Lance versions are per-branch — a branch's tip advances independently
/// of main once writes land on it. Nested-fork creation must read the
/// parent branch's tip, not main's, before calling
/// [`create_branch_from`].
///
/// # Errors
///
/// Returns an error if the dataset or branch cannot be opened.
pub async fn current_version_on_branch(uri: &str, branch: &str) -> Result<u64> {
    let dataset = open_branch(uri, branch).await?;
    Ok(dataset.version().version)
}

/// Create a new branch off another branch (Phase 3, nested forks).
///
/// `parent_branch` must already exist on the dataset. The new branch's
/// `base_paths` resolves: `new_branch → parent_branch → main`, so reads
/// against the new branch chain through both ancestors.
///
/// Same non-idempotence contract as [`create_branch`]: a crash between
/// the shallow-clone and `BranchContents` phases leaves a zombie; the
/// recovery driver must [`delete_branch`] (force-mode) before retry.
///
/// # Errors
///
/// - The dataset or parent branch cannot be opened.
/// - `new_branch` already exists.
/// - The underlying object store call fails.
pub async fn create_branch_from(
    uri: &str,
    new_branch: &str,
    parent_branch: &str,
    parent_version: u64,
) -> Result<()> {
    inject_fault_create_branch()?;
    let mut on_parent = open_branch(uri, parent_branch).await?;
    on_parent
        .create_branch(new_branch, parent_version, None)
        .await
        .with_context(|| {
            format!("create branch {new_branch} off {parent_branch} on {uri} at v{parent_version}")
        })?;
    Ok(())
}

/// Per-process counter for fault injection in `create_branch`. The
/// recovery test crate reads / writes via the helpers below.
#[doc(hidden)]
pub mod fault_injection {
    use std::sync::atomic::{AtomicI64, Ordering};

    pub(super) static CALL_COUNT: AtomicI64 = AtomicI64::new(0);
    pub(super) static DELETE_CALL_COUNT: AtomicI64 = AtomicI64::new(0);

    /// Reset the create-branch call counter. Tests that exercise
    /// `UNI_FORK_INJECT_FAIL_AFTER` should call this at the start to
    /// make assertions deterministic.
    pub fn reset() {
        CALL_COUNT.store(0, Ordering::SeqCst);
    }

    /// Read the current create-branch call count.
    pub fn calls_so_far() -> i64 {
        CALL_COUNT.load(Ordering::SeqCst)
    }

    /// Reset the delete-branch call counter (Phase 3 cascade-recovery
    /// test harness). Pairs with `UNI_FORK_INJECT_FAIL_DELETE_AFTER`.
    pub fn reset_delete() {
        DELETE_CALL_COUNT.store(0, Ordering::SeqCst);
    }

    /// Read the current delete-branch call count.
    pub fn delete_calls_so_far() -> i64 {
        DELETE_CALL_COUNT.load(Ordering::SeqCst)
    }
}

fn inject_fault_create_branch() -> Result<()> {
    let cur = fault_injection::CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let Some(threshold_str) = std::env::var_os("UNI_FORK_INJECT_FAIL_AFTER") else {
        return Ok(());
    };
    let Ok(threshold) = threshold_str
        .into_string()
        .map_err(|_| ())
        .and_then(|s| s.parse::<i64>().map_err(|_| ()))
    else {
        return Ok(());
    };
    if cur >= threshold {
        anyhow::bail!(
            "UNI_FORK_INJECT_FAIL_AFTER triggered at call #{cur} (threshold {threshold})"
        );
    }
    Ok(())
}

/// Phase 3 cascade-recovery fault hook for `delete_branch`. Reads
/// `UNI_FORK_INJECT_FAIL_DELETE_AFTER`; the Nth and subsequent calls
/// (in-process counter `DELETE_CALL_COUNT`) bail before the actual
/// delete runs. Independent from the create-side counter so the two
/// can be armed in the same test.
fn inject_fault_delete_branch() -> Result<()> {
    let cur = fault_injection::DELETE_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let Some(threshold_str) = std::env::var_os("UNI_FORK_INJECT_FAIL_DELETE_AFTER") else {
        return Ok(());
    };
    let Ok(threshold) = threshold_str
        .into_string()
        .map_err(|_| ())
        .and_then(|s| s.parse::<i64>().map_err(|_| ()))
    else {
        return Ok(());
    };
    if cur >= threshold {
        anyhow::bail!(
            "UNI_FORK_INJECT_FAIL_DELETE_AFTER triggered at call #{cur} (threshold {threshold})"
        );
    }
    Ok(())
}

/// Create a scalar BTree index on a branch-checked-out dataset
/// (Phase 5a-impl). The Phase 5a spike confirmed Lance writes the
/// index file branch-locally — main sees zero indexes after this
/// call; only the branch sees the new one.
///
/// Used by `fork_index_builder::build_fork_local_index` to land
/// fork-local BTree / Sorted indexes without touching primary's
/// index set.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - Lance rejects the column (e.g. unsupported type).
/// - Object-store IO failures.
pub async fn create_scalar_index_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    index_name: &str,
) -> Result<()> {
    use lance::index::DatasetIndexExt;
    use lance_index::{IndexType, scalar::ScalarIndexParams};
    let mut on_branch = open_branch(uri, branch).await?;
    on_branch
        .create_index_builder(&[column], IndexType::Scalar, &ScalarIndexParams::default())
        .name(index_name.to_string())
        .replace(true)
        .await
        .with_context(|| {
            format!("create_scalar_index_on_branch({uri}@{branch}, column={column})")
        })?;
    Ok(())
}

/// Phase 5b: vector kNN search against a branch-checked-out dataset.
/// Lance's `base_paths` chain on the branch surfaces both the
/// fork-local rows and the parent-inherited rows in one scan, so a
/// single nearest-K call returns the fused result set.
///
/// Used by `BranchedBackend::vector_search` when the fork has a
/// branch for the target dataset. When the fork has no branch
/// (label never written through the fork), the BranchedBackend
/// delegates to primary's vector_search directly.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - Lance rejects the query (column type mismatch, dimension mismatch).
#[allow(clippy::too_many_arguments)]
pub async fn vector_search_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    query: &[f32],
    k: usize,
    filter: &FilterExpr,
    opts: VectorQueryOpts,
) -> Result<Vec<arrow_array::RecordBatch>> {
    use arrow_array::Float32Array;
    use futures::TryStreamExt;

    let on_branch = open_branch(uri, branch).await?;
    let key = Float32Array::from(query.to_vec());
    let mut scanner = on_branch.scan();
    // Disable scalar-index pushdown: the ANN prefilter would otherwise engage scalar-index
    // discovery, whose `_indices/<id>/page_lookup.lance` is unresolvable across a >1-level
    // fork `base_paths` chain (#106). Result-set neutral — the prefilter still excludes the
    // same rows via a sequential scan.
    scanner.use_scalar_index(false);
    scanner
        .nearest(column, &key, k)
        .map_err(|e| anyhow::anyhow!("vector_search_on_branch nearest({column}, k={k}): {e}"))?;
    if let Some(n) = opts.nprobes {
        scanner.nprobes(n);
    }
    if let Some(r) = opts.refine_factor {
        scanner.refine(r);
    }
    // M6: honor the caller's filter (user predicate + `_deleted = false`
    // + version HWM pin). Prefilter so excluded rows never consume a
    // top-k slot — otherwise a soft-deleted or out-of-version row could
    // shadow a live match and shrink the result below k.
    if !filter.is_trivially_true() {
        let sql = filter.to_sql()?;
        scanner.prefilter(true);
        scanner
            .filter(&sql)
            .map_err(|e| anyhow::anyhow!("vector_search_on_branch filter('{sql}'): {e}"))?;
    }
    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| anyhow::anyhow!("vector_search_on_branch stream: {e}"))?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("vector_search_on_branch collect: {e}"))
}

/// Phase 5b: BM25 full-text search against a branch-checked-out
/// dataset. Same `base_paths` semantics as `vector_search_on_branch`
/// — the chain gives a fused result set.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - Lance rejects the query.
pub async fn full_text_search_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    query: &str,
    k: usize,
    filter: &FilterExpr,
) -> Result<Vec<arrow_array::RecordBatch>> {
    use futures::TryStreamExt;
    use lance_index::scalar::FullTextSearchQuery;
    use lance_index::scalar::inverted::query::MatchQuery;

    let on_branch = open_branch(uri, branch).await?;
    let match_query = MatchQuery::new(query.to_string()).with_column(Some(column.to_string()));
    let fts_query = FullTextSearchQuery {
        query: match_query.into(),
        limit: Some(k as i64),
        wand_factor: None,
    };
    let mut scanner = on_branch.scan();
    // Disable scalar-index pushdown (the prefilter would otherwise need the scalar index,
    // unresolvable across a >1-level fork `base_paths` chain — #106). Only scalar indices are
    // disabled; the FTS inverted index used by `full_text_search` is unaffected.
    scanner.use_scalar_index(false);
    scanner
        .full_text_search(fts_query)
        .map_err(|e| anyhow::anyhow!("full_text_search_on_branch({column}, k={k}): {e}"))?;
    // M6: honor the caller's filter (user predicate + `_deleted = false`
    // + version HWM pin), prefiltered so excluded rows don't take a slot.
    if !filter.is_trivially_true() {
        let sql = filter.to_sql()?;
        scanner.prefilter(true);
        scanner
            .filter(&sql)
            .map_err(|e| anyhow::anyhow!("full_text_search_on_branch filter('{sql}'): {e}"))?;
    }
    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| anyhow::anyhow!("full_text_search_on_branch stream: {e}"))?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("full_text_search_on_branch collect: {e}"))
}

/// Create a Lance vector index on a branch-checked-out dataset (Phase 5b).
/// The Phase 5b spike confirmed Lance writes the vector index file
/// branch-locally — main sees zero indexes after; only the branch
/// sees the new one. The fork's branch reads via `base_paths` then
/// produce results that include parent-inherited rows when relevant.
///
/// Defaults to a 1-partition IVF-Flat with L2 distance — the
/// simplest configuration that always builds. Phase 5b's MVP doesn't
/// expose the IVF/PQ knobs because the auto-build path is opt-in via
/// `Session::build_fork_local_index` and matches whatever the user
/// explicitly asked for.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - Lance rejects the column (e.g. wrong type for vector index —
///   must be `FixedSizeList<Float32, dim>`).
/// - Object-store IO failures.
pub async fn create_vector_index_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    index_name: &str,
) -> Result<()> {
    use lance::index::DatasetIndexExt;
    use lance::index::vector::VectorIndexParams;
    use lance_index::IndexType;
    use lance_linalg::distance::MetricType;

    let mut on_branch = open_branch(uri, branch).await?;
    let params = VectorIndexParams::ivf_flat(1, MetricType::L2);
    on_branch
        .create_index(
            &[column],
            IndexType::Vector,
            Some(index_name.to_string()),
            &params,
            true,
        )
        .await
        .with_context(|| {
            format!("create_vector_index_on_branch({uri}@{branch}, column={column})")
        })?;
    Ok(())
}

/// Create a Lance native FTS / inverted index on a branch-checked-out
/// dataset (Phase 5b).
///
/// Same per-branch semantics as `create_vector_index_on_branch`.
/// Used by `fork_index_builder::build_fork_local_index` for the
/// `FullText` kind.
///
/// `tokenizer` selects the analyzer pipeline (mapped through the shared
/// `fts_analyzer` mapper). Position postings are not stored for branch indexes.
///
/// # Errors
///
/// - The dataset or branch cannot be opened.
/// - The column type is not text.
/// - The tokenizer configuration is invalid (bad N-gram bounds, unsupported
///   stop-word language).
/// - Object-store IO failures.
pub async fn create_fts_index_on_branch(
    uri: &str,
    branch: &str,
    column: &str,
    index_name: &str,
    tokenizer: &uni_common::core::schema::TokenizerConfig,
) -> Result<()> {
    use lance::index::DatasetIndexExt;
    use lance_index::IndexType;

    let mut on_branch = open_branch(uri, branch).await?;
    // Mirrors `IndexManager::create_fts_index`: uses
    // `IndexType::Inverted` (not Scalar) and `InvertedIndexParams`
    // which carries the required `base_tokenizer` config that
    // ScalarIndexParams::for_builtin(Inverted) doesn't set. The tokenizer
    // config is mapped through the shared `fts_analyzer` mapper so fork-local
    // FTS honors the same analyzer pipeline as the main index.
    let fts_params = super::fts_analyzer::to_inverted_params(tokenizer, false)
        .with_context(|| format!("invalid FTS tokenizer for branch index on column={column}"))?;
    on_branch
        .create_index_builder(&[column], IndexType::Inverted, &fts_params)
        .name(index_name.to_string())
        .replace(true)
        .await
        .with_context(|| format!("create_fts_index_on_branch({uri}@{branch}, column={column})"))?;
    Ok(())
}

/// Create a Lance tag pinning `branch`'s current tip (Phase 4a).
///
/// Tags are GC-exempt references — Lance's compaction retention sweep
/// preserves any version referenced by an active tag. This is what
/// lets `Uni::tag_fork` keep a fork's state on disk after the fork
/// itself is dropped (e.g. for audit / regulatory hold).
///
/// The reference resolved at creation time is the branch's current
/// version: subsequent writes on the branch do not "follow" the tag.
///
/// # Errors
///
/// - The dataset or `branch` cannot be opened.
/// - A tag named `tag` already exists (Lance returns `RefConflict`).
/// - Object-store IO fails.
pub async fn create_tag(uri: &str, tag: &str, branch: &str) -> Result<()> {
    let on_branch = open_branch(uri, branch).await?;
    let version = on_branch.version().version;
    let dataset = open_dataset(uri).await?;
    dataset
        .tags()
        .create(tag, (branch, version))
        .await
        .with_context(|| format!("create tag {tag} on {uri} -> {branch}@v{version}"))?;
    Ok(())
}

/// Delete a Lance tag (Phase 4a). Idempotent: a missing tag is treated
/// as success so callers don't have to special-case re-runs.
///
/// # Errors
///
/// Object-store IO failures unrelated to the missing-tag case.
pub async fn delete_tag(uri: &str, tag: &str) -> Result<()> {
    let dataset = open_dataset(uri).await?;
    let existing = dataset
        .tags()
        .list()
        .await
        .with_context(|| format!("list tags on {uri}"))?;
    if !existing.contains_key(tag) {
        return Ok(());
    }
    dataset
        .tags()
        .delete(tag)
        .await
        .with_context(|| format!("delete tag {tag} on {uri}"))
}

/// List all tags on the dataset, returning `(name, pinned_version)`
/// pairs (Phase 4a).
///
/// # Errors
///
/// Object-store IO failures.
pub async fn list_tags(uri: &str) -> Result<Vec<(String, u64)>> {
    let dataset = open_dataset(uri).await?;
    let map = dataset
        .tags()
        .list()
        .await
        .with_context(|| format!("list tags on {uri}"))?;
    Ok(map
        .into_iter()
        .map(|(name, contents)| (name, contents.version))
        .collect())
}

/// Delete a branch (idempotent under the recovery driver — uses force).
///
/// Phase 1 fork-drop and recovery both call this. Force-delete handles
/// zombie branches left by a half-completed `create_branch`. Missing
/// branches are treated as success: the recovery driver may invoke
/// this even when nothing was created, and the spec's drop step 3 is
/// declared idempotent.
///
/// # Errors
///
/// Returns an error only on object-store failures unrelated to the
/// missing-branch case.
pub async fn delete_branch(uri: &str, branch: &str) -> Result<()> {
    inject_fault_delete_branch()?;
    let mut dataset = open_dataset(uri).await?;

    // Cheap pre-check: skip the force-delete if neither BranchContents
    // nor the shallow-clone tree exist. lance::Dataset::force_delete_branch
    // treats truly-missing branches as an error, but the recovery
    // contract requires idempotence — so we filter that case here.
    let branches = dataset
        .list_branches()
        .await
        .with_context(|| format!("list branches on {uri}"))?;
    if !branches.contains_key(branch) {
        // Probe the shallow-clone path to catch zombies that have no
        // BranchContents but still occupy `tree/{branch}`.
        let zombie_uri = format!("{uri}/tree/{branch}");
        if Dataset::open(&zombie_uri).await.is_err() {
            return Ok(());
        }
    }

    dataset
        .force_delete_branch(branch)
        .await
        .with_context(|| format!("force-delete branch {branch} on {uri}"))?;
    Ok(())
}

/// List branch names on the dataset.
///
/// Used by recovery to determine which datasets a partial fork has
/// already branched.
///
/// # Errors
///
/// Returns an error if the dataset cannot be opened.
pub async fn list_branches(uri: &str) -> Result<Vec<String>> {
    let dataset = open_dataset(uri).await?;
    let branches = dataset
        .list_branches()
        .await
        .with_context(|| format!("list branches on {uri}"))?;
    Ok(branches.into_keys().collect())
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 2: branch-targeted write helpers
//
// These wrap `lance::Dataset` write paths against a specific branch,
// keeping the lancedb high-level API for primary while routing fork
// writes one layer below. lancedb 0.27.1 has no branch-write surface
// (`Table::as_branch` is `todo!()`), so descent is the only option.
// ─────────────────────────────────────────────────────────────────────────

/// Append `batches` to the dataset's named branch.
///
/// Opens the dataset on `branch`, then commits an Append. The on-disk
/// dataset path is unchanged; the branch's `BranchContents` advances
/// to a new version. Primary is unaffected.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The append fails (schema mismatch, IO, conflict on commit).
pub async fn write_to_branch<R>(uri: &str, branch: &str, batches: R) -> Result<()>
where
    R: arrow_array::RecordBatchReader + Send + 'static,
{
    let mut on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} for append"))?;
    on_branch
        .append(batches, None)
        .await
        .with_context(|| format!("append to branch {branch} on {uri}"))?;
    Ok(())
}

/// Write `batches` as data files of `branch` without committing them.
///
/// Lance's two-step write against a branch-checked-out dataset, the branch
/// counterpart of the Lance backend's `stage_append`: the files land where an
/// append on the branch would put them, and no manifest references them until
/// [`publish_on_branch`] commits the returned fragments.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The write fails, or produces something other than an append.
pub async fn stage_on_branch(
    uri: &str,
    branch: &str,
    batches: Vec<arrow_array::RecordBatch>,
) -> Result<Vec<lance::table::format::Fragment>> {
    use lance::dataset::transaction::Operation;

    let on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} to stage rows"))?;
    let params = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Append,
        ..Default::default()
    };
    let transaction = lance::dataset::InsertBuilder::new(Arc::new(on_branch))
        .with_params(&params)
        .execute_uncommitted(batches)
        .await
        .with_context(|| format!("stage rows on branch {branch} of {uri}"))?;
    match transaction.operation {
        Operation::Append { fragments } => Ok(fragments),
        other => anyhow::bail!(
            "staging rows on branch {branch} of {uri} produced a {other} operation, \
             expected Append"
        ),
    }
}

/// Commit `fragments` staged by [`stage_on_branch`] onto `branch`'s current
/// version, as one append.
///
/// Re-basing an append onto whatever the branch committed since staging is
/// exact, for the reason the Lance backend's `publish_staged` gives.
///
/// # Errors
///
/// Returns an error if the dataset or branch does not exist or the commit
/// fails, in which case none of the rows are visible.
pub async fn publish_on_branch(
    uri: &str,
    branch: &str,
    fragments: Vec<lance::table::format::Fragment>,
) -> Result<()> {
    use lance::dataset::transaction::{Operation, TransactionBuilder};

    if fragments.is_empty() {
        return Ok(());
    }
    let on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} to publish staged rows"))?;
    let transaction =
        TransactionBuilder::new(on_branch.version().version, Operation::Append { fragments })
            .build();
    lance::dataset::CommitBuilder::new(Arc::new(on_branch))
        .execute(transaction)
        .await
        .with_context(|| format!("publish staged rows to branch {branch} of {uri}"))?;
    Ok(())
}

/// Publish `fragments` staged by [`stage_on_branch`] on `branch` unless a
/// retained version of the branch already references their files.
///
/// Exactly once, by the rule `LanceDbBackend::publish_staged_record` applies
/// to a table: a file referenced by the branch's current manifest, or by the
/// Append transaction of any retained branch version, was published; no
/// version referencing it, all files present, it was not; all files gone, it
/// was published and since rewritten; some gone, neither is exact.
///
/// # Errors
///
/// Returns an error if the branch cannot be opened, the commit fails, or only
/// part of the recorded files is present.
pub async fn publish_recorded_on_branch(
    uri: &str,
    branch: &str,
    fragments: Vec<lance::table::format::Fragment>,
) -> Result<crate::backend::traits::RecordedPublish> {
    use crate::backend::traits::RecordedPublish;

    let paths: std::collections::HashSet<&str> = fragments
        .iter()
        .flat_map(|f| f.files.iter().map(|file| file.path.as_str()))
        .collect();
    if paths.is_empty() {
        return Ok(RecordedPublish::AlreadyPublished);
    }
    let on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} to complete a publish"))?;
    // The branch's own history, from its first version: the bound a primary
    // table records is a primary version and means nothing on a branch.
    if crate::backend::lance::staged_append_committed(&on_branch, &paths, None).await {
        return Ok(RecordedPublish::AlreadyPublished);
    }
    let data_dir = on_branch.data_dir();
    let mut missing = 0usize;
    let mut total = 0usize;
    for fragment in &fragments {
        for file in &fragment.files {
            total += 1;
            let store = on_branch
                .object_store(file.base_id)
                .await
                .with_context(|| format!("open the store of branch {branch} on {uri}"))?;
            let present = store
                .exists(&data_dir.clone().join(file.path.as_str()))
                .await
                .with_context(|| {
                    format!(
                        "check staged file '{}' of branch {branch} on {uri}",
                        file.path
                    )
                })?;
            if !present {
                missing += 1;
            }
        }
    }
    if missing == total {
        tracing::warn!(
            uri,
            branch,
            files = total,
            "recorded staged rows are in no retained branch version and their files are \
             gone; treating them as published and since rewritten"
        );
        return Ok(RecordedPublish::AlreadyPublished);
    }
    if missing > 0 {
        anyhow::bail!(
            "cannot complete the recorded publish to branch {branch} on {uri}: {missing} of \
             {total} staged files are missing and no retained version references them"
        );
    }
    publish_on_branch(uri, branch, fragments).await?;
    Ok(RecordedPublish::Published)
}

/// Delete the files of `fragments` staged by [`stage_on_branch`].
///
/// # Errors
///
/// Returns the first deletion error; the remaining files are still tried.
pub async fn discard_on_branch(
    uri: &str,
    branch: &str,
    fragments: Vec<lance::table::format::Fragment>,
) -> Result<()> {
    if fragments.is_empty() {
        return Ok(());
    }
    let on_branch = open_branch(uri, branch).await?;
    let data_dir = on_branch.data_dir();
    let mut first_error = None;
    for fragment in &fragments {
        for file in &fragment.files {
            let removed = match on_branch.object_store(file.base_id).await {
                Ok(store) => {
                    store
                        .delete(&data_dir.clone().join(file.path.as_str()))
                        .await
                }
                Err(e) => Err(e),
            };
            if let Err(e) = removed {
                first_error.get_or_insert_with(|| {
                    anyhow::anyhow!(
                        "delete staged file '{}' of branch {branch} on {uri}: {e}",
                        file.path
                    )
                });
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// The fragments one [`ForkBranching::stage_on_branch`] call wrote.
struct BranchStagedFragments(Vec<lance::table::format::Fragment>);

/// Collect the fragments of `staged`, rejecting payloads staged elsewhere.
fn branch_staged_fragments(
    table: &str,
    staged: Vec<Box<dyn std::any::Any + Send + Sync>>,
) -> Result<Vec<lance::table::format::Fragment>> {
    let mut fragments = Vec::new();
    for payload in staged {
        let BranchStagedFragments(mut f) =
            *payload.downcast::<BranchStagedFragments>().map_err(|_| {
                anyhow::anyhow!("rows staged for '{table}' were not staged on a branch")
            })?;
        fragments.append(&mut f);
    }
    Ok(fragments)
}

/// Widen a branch's stored schema so it accepts a batch it currently rejects.
///
/// The branch half of [`crate::backend::StorageBackend::evolve_table_schema`].
/// A Lance branch is a shallow *clone* carrying its own manifest and its own
/// copy of the schema, so a widening committed on the trunk does not reach it
/// — and both scan paths project an explicit column list, which hard-errors on
/// a column the branch's manifest has never seen.
///
/// `AllNulls` clones fragment metadata rather than rewriting data, so rows
/// inherited from the parent through `base_paths` keep their indirection and
/// stay readable. Verified by `phase249_spike_add_nullable_column_on_branch`:
/// 3 inherited + 2 branch-local rows survive the widening, main does not gain
/// the column, and a subsequent append carrying the wide schema is accepted.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - A name in `add` already exists on the branch with a different type.
/// - The commit fails.
pub async fn evolve_schema_on_branch(
    uri: &str,
    branch: &str,
    add: &[Field],
    relax_nullable: &[String],
) -> Result<()> {
    use lance::dataset::{ColumnAlteration, NewColumnTransform};

    if add.is_empty() && relax_nullable.is_empty() {
        return Ok(());
    }

    let mut on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} to widen its schema"))?;
    let current: Arc<ArrowSchema> = Arc::new(on_branch.schema().into());

    let mut missing: Vec<Field> = Vec::new();
    for field in add {
        match current.fields().iter().find(|f| f.name() == field.name()) {
            Some(existing) if existing.data_type() == field.data_type() => {}
            Some(existing) => {
                anyhow::bail!(
                    "cannot add column '{}' to branch {branch} of {uri} as {:?}: \
                     it already exists as {:?}",
                    field.name(),
                    field.data_type(),
                    existing.data_type()
                );
            }
            None => missing.push(field.clone().with_nullable(true)),
        }
    }

    if !missing.is_empty() {
        let added = Arc::new(ArrowSchema::new(missing));
        on_branch
            .add_columns(NewColumnTransform::AllNulls(added), None, None)
            .await
            .with_context(|| format!("add column(s) on branch {branch} of {uri}"))?;
    }

    let alterations: Vec<ColumnAlteration> = relax_nullable
        .iter()
        .filter(|name| {
            current
                .fields()
                .iter()
                .any(|f| f.name() == *name && !f.is_nullable())
        })
        .map(|name| ColumnAlteration::new(name.clone()).set_nullable(true))
        .collect();
    if !alterations.is_empty() {
        on_branch
            .alter_columns(&alterations)
            .await
            .with_context(|| format!("relax nullability on branch {branch} of {uri}"))?;
    }
    Ok(())
}

/// Delete rows on the dataset's named branch by SQL predicate.
///
/// Opens the dataset on `branch`, then commits a Delete. Lance encodes
/// deletions as tombstones at the branch tip; primary's main branch
/// is untouched.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The predicate fails to parse / type-check.
pub async fn delete_from_branch(uri: &str, branch: &str, predicate: &str) -> Result<()> {
    let mut on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} for delete"))?;
    on_branch.delete(predicate).await.with_context(|| {
        format!("delete on branch {branch} on {uri} with predicate `{predicate}`")
    })?;
    Ok(())
}

/// Merge-insert (update-only) `batches` into the dataset's named branch.
///
/// Opens the dataset on `branch` and runs Lance's `MergeInsertBuilder`
/// keyed on `on`, applying `WhenMatched::UpdateAll`. Like the primary
/// `LanceDbBackend::merge_insert`, it deliberately
/// does NOT insert unmatched source rows (CREATE goes through the Append
/// path); the partial/tombstone source only updates existing rows. The
/// commit lands on the branch tip; primary's main branch is untouched.
///
/// This is what lets a fork update or soft-delete an *inherited*
/// (base_paths) vertex: the flush emits a partial `_deleted`/`_version`
/// (or touched-column) batch keyed by `_vid`, which matches the inherited
/// row visible through the branch chain and shadows it on the branch.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The merge build/execute fails (schema mismatch, IO, commit conflict).
pub async fn merge_insert_on_branch<R>(
    uri: &str,
    branch: &str,
    on: &[&str],
    batches: R,
) -> Result<()>
where
    R: arrow_array::RecordBatchReader + Send + 'static,
{
    use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};

    let on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} for merge_insert"))?;
    let mut builder = MergeInsertBuilder::try_new(
        Arc::new(on_branch),
        on.iter().map(|s| (*s).to_string()).collect(),
    )
    .with_context(|| format!("merge_insert builder on branch {branch} of {uri}"))?;
    builder.when_matched(WhenMatched::UpdateAll);
    // Update-only: drop unmatched source rows. The flush source is a
    // partial-column batch (e.g. tombstone `_vid`/`_deleted`/`_version`),
    // so inserting unmatched rows would fail on non-nullable target columns
    // and is wrong anyway — CREATE goes through the Append path. The lance
    // `Dataset` builder defaults to InsertAll (unlike the lancedb `Table`),
    // so this must be set explicitly.
    builder.when_not_matched(WhenNotMatched::DoNothing);
    let job = builder
        .try_build()
        .with_context(|| format!("merge_insert build on branch {branch} of {uri}"))?;
    let boxed: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(batches);
    job.execute_reader(boxed)
        .await
        .with_context(|| format!("merge_insert execute on branch {branch} of {uri}"))?;
    Ok(())
}

/// Replace the branch's tip with `batches` (overwrite semantics).
///
/// Used by the rare write-path that wants atomic table-replace on a
/// fork (e.g. compaction artifacts). Implementation: delete-all
/// followed by append, both committed against the branch. Two commits,
/// not one — Lance does not yet expose a single-commit overwrite at
/// the branch level.
///
/// # Errors
///
/// - The dataset or branch does not exist.
/// - The delete or append fails.
pub async fn replace_branch_tip<R>(uri: &str, branch: &str, batches: R) -> Result<()>
where
    R: arrow_array::RecordBatchReader + Send + 'static,
{
    let mut on_branch = open_branch(uri, branch)
        .await
        .with_context(|| format!("open branch {branch} on {uri} for replace"))?;
    on_branch
        .delete("true")
        .await
        .with_context(|| format!("delete-all on branch {branch} on {uri}"))?;
    on_branch
        .append(batches, None)
        .await
        .with_context(|| format!("append on branch {branch} on {uri} after delete-all"))?;
    Ok(())
}

/// Create a new Lance dataset and immediately branch off it.
///
/// Used when a fork's transaction introduces a label or edge type
/// whose Lance dataset doesn't yet exist on primary. The flow:
///
/// 1. `Dataset::write` to `uri` with `initial_batches` (empty or seed).
///    This produces `version=1` on the dataset's main branch.
/// 2. `create_branch(uri, branch_name, version=1)` — the fork's
///    branch starts from primary's empty version 1.
///
/// Phase 2's flush path appends data to the branch *after* this helper
/// returns. Primary's main branch stays at version 1 forever (or until
/// promotion) — its schema file does not list the label, so primary
/// queries can't see this dataset by name.
///
/// # Errors
///
/// - The dataset already exists at `uri`.
/// - Either step fails. On step-2 failure the dataset *is* created on
///   primary but the branch is not; Phase 2 callers compensate by
///   invoking [`delete_branch`] (force-mode) and then retrying the
///   whole flow, since `create_branch` itself is non-idempotent.
pub async fn create_dataset_then_branch<R>(
    uri: &str,
    branch: &str,
    initial_batches: R,
) -> Result<()>
where
    R: arrow_array::RecordBatchReader + Send + 'static,
{
    Dataset::write(initial_batches, uri, None)
        .await
        .with_context(|| format!("create lance dataset at {uri}"))?;
    let parent_v = current_version(uri).await?;
    create_branch(uri, branch, parent_v)
        .await
        .with_context(|| format!("branch {branch} off newly-created dataset at {uri}"))?;
    Ok(())
}

/// [`ForkBranching`] over Lance datasets rooted at a base URI.
///
/// A thin adapter: every method resolves the logical table name to
/// `{base}/{table}.lance` and delegates to this module's free functions, so
/// behavior is identical to the pre-trait direct calls. The point of the
/// indirection is that path resolution now lives here instead of being
/// duplicated in `BranchedBackend` and in `crates/uni/src/api/fork.rs`.
#[derive(Debug, Clone)]
pub struct LanceBranching {
    base_uri: String,
}

/// Whether the local dataset at `dataset` has a committed version.
///
/// A Lance dataset exists once its first manifest is committed under
/// `_versions/`; creating one writes the data files first, so a create cut off
/// midway leaves the directory without one. Mirrors
/// `LanceDirectory::is_committed`, which answers the same question for the
/// primary backend.
fn has_committed_manifest(dataset: &std::path::Path) -> bool {
    std::fs::read_dir(dataset.join("_versions")).is_ok_and(|entries| {
        entries
            .flatten()
            .any(|e| e.file_name().to_string_lossy().ends_with(".manifest"))
    })
}

impl LanceBranching {
    /// Adapter over the Lance datasets under `base_uri`.
    pub fn new(base_uri: impl Into<String>) -> Self {
        Self {
            base_uri: base_uri.into(),
        }
    }

    /// Resolve a logical table name to its dataset URI, matching the layout
    /// `lancedb` uses when it stores tables.
    fn dataset_uri(&self, table: &str) -> String {
        if self.base_uri.ends_with('/') {
            format!("{}{table}.lance", self.base_uri)
        } else {
            format!("{}/{table}.lance", self.base_uri)
        }
    }

    /// Wrap owned batches as the `RecordBatchReader` the free functions take.
    ///
    /// The schema travels separately because an empty `batches` carries none.
    ///
    /// An empty `batches` is normalized to a single zero-row batch rather than
    /// an empty stream. Lance's `Dataset::write` / `append` need at least one
    /// batch to carry the schema, and every pre-trait call site did this
    /// normalization itself — centralizing it here keeps that behavior while
    /// letting the trait express "clear the table" as simply an empty `Vec`.
    fn reader(
        mut batches: Vec<arrow_array::RecordBatch>,
        schema: Arc<arrow_schema::Schema>,
    ) -> impl arrow_array::RecordBatchReader + Send + 'static {
        if batches.is_empty() {
            batches.push(arrow_array::RecordBatch::new_empty(schema.clone()));
        }
        arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema)
    }

    /// SQL text for a delete predicate, rejecting a match-all filter.
    ///
    /// See [`ForkBranching::delete_from_branch`] for why a trivially-true
    /// filter is an error rather than "match all".
    fn delete_predicate(filter: &FilterExpr) -> Result<String> {
        if filter.is_trivially_true() {
            anyhow::bail!(
                "delete_from_branch requires an explicit predicate; a \
                 trivially-true filter would delete every row. Use \
                 replace_branch_tip with no batches to clear a branch \
                 deliberately."
            );
        }
        Ok(filter.to_sql()?)
    }
}

#[async_trait::async_trait]
impl ForkBranching for LanceBranching {
    async fn create_branch(&self, table: &str, branch: &str, parent_version: u64) -> Result<()> {
        create_branch(&self.dataset_uri(table), branch, parent_version).await
    }

    async fn create_branch_from(
        &self,
        table: &str,
        new_branch: &str,
        parent_branch: &str,
        parent_version: u64,
    ) -> Result<()> {
        create_branch_from(
            &self.dataset_uri(table),
            new_branch,
            parent_branch,
            parent_version,
        )
        .await
    }

    async fn delete_branch(&self, table: &str, branch: &str) -> Result<()> {
        delete_branch(&self.dataset_uri(table), branch).await
    }

    async fn list_branches(&self, table: &str) -> Result<Vec<String>> {
        list_branches(&self.dataset_uri(table)).await
    }

    async fn current_version(&self, table: &str) -> Result<u64> {
        current_version(&self.dataset_uri(table)).await
    }

    async fn current_version_on_branch(&self, table: &str, branch: &str) -> Result<u64> {
        current_version_on_branch(&self.dataset_uri(table), branch).await
    }

    async fn table_exists(&self, table: &str) -> Result<bool> {
        let uri = self.dataset_uri(table);
        // Local-fs heuristic, matching `BranchedBackend::dataset_path_exists`:
        // for object stores we answer `true` so the caller proceeds to a real
        // open, which surfaces the accurate error if the table is missing.
        if uri.contains("://") {
            return Ok(true);
        }
        Ok(has_committed_manifest(std::path::Path::new(&uri)))
    }

    async fn create_tag(&self, table: &str, tag: &str, branch: &str) -> Result<()> {
        create_tag(&self.dataset_uri(table), tag, branch).await
    }

    async fn delete_tag(&self, table: &str, tag: &str) -> Result<()> {
        delete_tag(&self.dataset_uri(table), tag).await
    }

    async fn list_tags(&self, table: &str) -> Result<Vec<(String, u64)>> {
        list_tags(&self.dataset_uri(table)).await
    }

    async fn write_to_branch(
        &self,
        table: &str,
        branch: &str,
        batches: Vec<arrow_array::RecordBatch>,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<()> {
        write_to_branch(
            &self.dataset_uri(table),
            branch,
            Self::reader(batches, schema),
        )
        .await
    }

    fn stages_on_branch(&self) -> bool {
        true
    }

    async fn stage_on_branch(
        &self,
        table: &str,
        branch: &str,
        batches: Vec<arrow_array::RecordBatch>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync>> {
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Box::new(BranchStagedFragments(Vec::new())));
        }
        let fragments = stage_on_branch(&self.dataset_uri(table), branch, batches).await?;
        Ok(Box::new(BranchStagedFragments(fragments)))
    }

    async fn publish_on_branch(
        &self,
        table: &str,
        branch: &str,
        staged: Vec<Box<dyn std::any::Any + Send + Sync>>,
    ) -> Result<()> {
        let fragments = branch_staged_fragments(table, staged)?;
        publish_on_branch(&self.dataset_uri(table), branch, fragments).await
    }

    async fn discard_on_branch(
        &self,
        table: &str,
        branch: &str,
        staged: Vec<Box<dyn std::any::Any + Send + Sync>>,
    ) -> Result<()> {
        let fragments = branch_staged_fragments(table, staged)?;
        discard_on_branch(&self.dataset_uri(table), branch, fragments).await
    }

    fn staged_record_on_branch(
        &self,
        table: &str,
        branch: &str,
        staged: &[&(dyn std::any::Any + Send + Sync)],
    ) -> Result<Option<serde_json::Value>> {
        let mut fragments: Vec<&lance::table::format::Fragment> = Vec::new();
        for payload in staged {
            let BranchStagedFragments(f) = payload
                .downcast_ref::<BranchStagedFragments>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "rows staged for '{table}' on branch {branch} were not staged on a branch"
                    )
                })?;
            fragments.extend(f);
        }
        if fragments.is_empty() {
            return Ok(None);
        }
        Ok(Some(serde_json::to_value(&fragments).with_context(
            || format!("record staged rows of '{table}' on branch {branch}"),
        )?))
    }

    async fn publish_staged_record_on_branch(
        &self,
        table: &str,
        branch: &str,
        record: &serde_json::Value,
    ) -> Result<crate::backend::traits::RecordedPublish> {
        let fragments: Vec<lance::table::format::Fragment> = serde_json::from_value(record.clone())
            .with_context(|| {
                format!("read the recorded staged rows of '{table}' on branch {branch}")
            })?;
        publish_recorded_on_branch(&self.dataset_uri(table), branch, fragments).await
    }

    async fn evolve_branch_schema(
        &self,
        table: &str,
        branch: &str,
        add: &[Field],
        relax_nullable: &[String],
    ) -> Result<()> {
        evolve_schema_on_branch(&self.dataset_uri(table), branch, add, relax_nullable).await
    }

    async fn delete_from_branch(
        &self,
        table: &str,
        branch: &str,
        filter: &FilterExpr,
    ) -> Result<()> {
        let predicate = Self::delete_predicate(filter)?;
        delete_from_branch(&self.dataset_uri(table), branch, &predicate).await
    }

    async fn merge_insert_on_branch(
        &self,
        table: &str,
        branch: &str,
        on: &[&str],
        batches: Vec<arrow_array::RecordBatch>,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<()> {
        merge_insert_on_branch(
            &self.dataset_uri(table),
            branch,
            on,
            Self::reader(batches, schema),
        )
        .await
    }

    async fn replace_branch_tip(
        &self,
        table: &str,
        branch: &str,
        batches: Vec<arrow_array::RecordBatch>,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<()> {
        replace_branch_tip(
            &self.dataset_uri(table),
            branch,
            Self::reader(batches, schema),
        )
        .await
    }

    async fn create_empty_table_then_branch(
        &self,
        table: &str,
        branch: &str,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<()> {
        // The trunk copy must stay empty — see the trait docs. `reader`
        // normalizes the empty vec into the one zero-row batch that carries
        // the schema to `Dataset::write`.
        create_dataset_then_branch(
            &self.dataset_uri(table),
            branch,
            Self::reader(Vec::new(), schema),
        )
        .await
    }

    async fn create_scalar_index_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        index_name: &str,
    ) -> Result<()> {
        create_scalar_index_on_branch(&self.dataset_uri(table), branch, column, index_name).await
    }

    async fn create_vector_index_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        index_name: &str,
    ) -> Result<()> {
        create_vector_index_on_branch(&self.dataset_uri(table), branch, column, index_name).await
    }

    async fn create_fts_index_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        index_name: &str,
        tokenizer: &uni_common::core::schema::TokenizerConfig,
    ) -> Result<()> {
        create_fts_index_on_branch(
            &self.dataset_uri(table),
            branch,
            column,
            index_name,
            tokenizer,
        )
        .await
    }

    async fn vector_search_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        query: &[f32],
        k: usize,
        filter: &FilterExpr,
        opts: VectorQueryOpts,
    ) -> Result<Vec<arrow_array::RecordBatch>> {
        vector_search_on_branch(
            &self.dataset_uri(table),
            branch,
            column,
            query,
            k,
            filter,
            opts,
        )
        .await
    }

    async fn full_text_search_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: &FilterExpr,
    ) -> Result<Vec<arrow_array::RecordBatch>> {
        full_text_search_on_branch(&self.dataset_uri(table), branch, column, query, k, filter).await
    }

    async fn full_text_search_with_unstored_on_branch(
        &self,
        table: &str,
        branch: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: &FilterExpr,
        unstored: Vec<(u64, String)>,
    ) -> Result<TextSearch> {
        let sql = if filter.is_trivially_true() {
            None
        } else {
            Some(filter.to_sql()?)
        };
        let on_branch = open_branch(&self.dataset_uri(table), branch).await?;
        super::lance_fts::search_with_unstored(
            Some(&on_branch),
            column,
            query,
            k,
            sql.as_deref(),
            unstored,
            |_| {},
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn test_batch(ids: Vec<u64>, values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    /// Seed a fresh dataset with one batch and return its URI + temp dir guard.
    async fn seed_dataset() -> (TempDir, String) {
        let dir = TempDir::new().unwrap();
        let uri = format!("{}/ds.lance", dir.path().display());
        let batch = test_batch(vec![1, 2, 3], vec![100, 200, 300]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        Dataset::write(reader, &uri, None).await.unwrap();
        (dir, uri)
    }

    #[tokio::test]
    async fn current_version_returns_positive_value() {
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        assert!(v >= 1, "fresh dataset should have version >= 1, got {v}");
    }

    #[tokio::test]
    async fn create_open_list_delete_roundtrip() {
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();

        // Create
        create_branch(&uri, "fork-a", v).await.unwrap();

        // List sees it
        let branches = list_branches(&uri).await.unwrap();
        assert!(
            branches.iter().any(|b| b == "fork-a"),
            "expected fork-a in {branches:?}"
        );

        // Open on the branch and confirm it reads parent rows via base_paths
        let branched = open_branch(&uri, "fork-a").await.unwrap();
        let count = branched.count_rows(None).await.unwrap();
        assert_eq!(count, 3, "branch reads should chain to parent");

        // Delete
        delete_branch(&uri, "fork-a").await.unwrap();
        let branches = list_branches(&uri).await.unwrap();
        assert!(!branches.iter().any(|b| b == "fork-a"));
    }

    #[tokio::test]
    async fn create_branch_not_idempotent_at_same_version() {
        // Documents the contract: re-creating an existing branch errors.
        // Recovery uses force_delete_branch + retry, not idempotent retry.
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        create_branch(&uri, "fork-x", v).await.unwrap();

        let second = create_branch(&uri, "fork-x", v).await;
        assert!(second.is_err(), "second create_branch should fail");

        // After force-delete, recreate succeeds.
        delete_branch(&uri, "fork-x").await.unwrap();
        create_branch(&uri, "fork-x", v).await.unwrap();
    }

    #[tokio::test]
    async fn delete_missing_branch_is_force_safe() {
        // force_delete_branch should not error on a name that never existed;
        // recovery relies on this to clean up partial state idempotently.
        let (_dir, uri) = seed_dataset().await;
        delete_branch(&uri, "never-existed").await.unwrap();
    }

    #[tokio::test]
    async fn current_version_on_branch_tracks_branch_tip() {
        // Phase 3: branch versions advance independently of main.
        let (_dir, uri) = seed_dataset().await;
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "child", v_main).await.unwrap();

        // Initially the branch tip matches the parent version it was
        // forked from (Lance stamps a BranchContents commit on creation).
        let v_branch_initial = current_version_on_branch(&uri, "child").await.unwrap();
        assert!(v_branch_initial >= v_main);

        // Append on the branch — branch tip advances, main does not.
        let batch = test_batch(vec![10, 11], vec![1000, 1100]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        write_to_branch(&uri, "child", reader).await.unwrap();

        let v_branch_after = current_version_on_branch(&uri, "child").await.unwrap();
        let v_main_after = current_version(&uri).await.unwrap();
        assert!(
            v_branch_after > v_branch_initial,
            "branch tip should advance after append"
        );
        assert_eq!(
            v_main_after, v_main,
            "main version must not move when branch is written"
        );
    }

    #[tokio::test]
    async fn create_branch_from_chains_through_parent() {
        // Phase 3: nested branch reads chain through parent branch and main.
        let (_dir, uri) = seed_dataset().await;
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "level1", v_main).await.unwrap();

        // Append on level1 so we can verify level2 reads see those rows.
        let batch = test_batch(vec![10, 11], vec![1000, 1100]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        write_to_branch(&uri, "level1", reader).await.unwrap();

        let v_l1 = current_version_on_branch(&uri, "level1").await.unwrap();
        create_branch_from(&uri, "level2", "level1", v_l1)
            .await
            .unwrap();

        // level2 should see all 5 rows: 3 from main + 2 from level1.
        let on_l2 = open_branch(&uri, "level2").await.unwrap();
        assert_eq!(on_l2.count_rows(None).await.unwrap(), 5);

        // Writes on level1 *after* level2 was created must not appear on level2.
        let batch2 = test_batch(vec![20], vec![2000]);
        let reader2 =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch2)].into_iter(), test_schema());
        write_to_branch(&uri, "level1", reader2).await.unwrap();

        let on_l2_again = open_branch(&uri, "level2").await.unwrap();
        assert_eq!(
            on_l2_again.count_rows(None).await.unwrap(),
            5,
            "level2 must not see writes to level1 that happened after its creation"
        );
    }

    #[tokio::test]
    async fn create_list_delete_tag_roundtrip() {
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        create_branch(&uri, "to-tag", v).await.unwrap();

        // Create
        create_tag(&uri, "audit-2026", "to-tag").await.unwrap();

        // List
        let tags = list_tags(&uri).await.unwrap();
        assert!(
            tags.iter().any(|(n, _)| n == "audit-2026"),
            "tags = {tags:?}"
        );

        // Delete idempotent
        delete_tag(&uri, "audit-2026").await.unwrap();
        let tags2 = list_tags(&uri).await.unwrap();
        assert!(!tags2.iter().any(|(n, _)| n == "audit-2026"));

        // Re-deleting a missing tag is a no-op.
        delete_tag(&uri, "audit-2026").await.unwrap();
    }

    #[tokio::test]
    async fn create_tag_pins_version_at_call_time() {
        // Phase 4a contract: tag captures the branch's tip at create
        // time and does not "follow" subsequent writes. This is what
        // makes a tagged-then-dropped fork safe to retain on disk.
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        create_branch(&uri, "snap-branch", v).await.unwrap();

        // Append a row before tagging.
        let batch = test_batch(vec![10], vec![1000]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        write_to_branch(&uri, "snap-branch", reader).await.unwrap();

        create_tag(&uri, "v1", "snap-branch").await.unwrap();
        let tags = list_tags(&uri).await.unwrap();
        let (_, pinned_v) = tags.iter().find(|(n, _)| n == "v1").unwrap();

        // Append more after tagging; the tag's pinned version must not move.
        let batch2 = test_batch(vec![11], vec![1100]);
        let reader2 =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch2)].into_iter(), test_schema());
        write_to_branch(&uri, "snap-branch", reader2).await.unwrap();

        let tags_after = list_tags(&uri).await.unwrap();
        let (_, pinned_after) = tags_after.iter().find(|(n, _)| n == "v1").unwrap();
        assert_eq!(
            pinned_v, pinned_after,
            "tag must pin to fork-time version, not follow branch tip"
        );
    }

    #[tokio::test]
    async fn parent_writes_after_branch_invisible_to_branch() {
        // Spec §10: snapshot isolation at fork point.
        let (_dir, uri) = seed_dataset().await;
        let v = current_version(&uri).await.unwrap();
        create_branch(&uri, "snap", v).await.unwrap();

        // Append on primary
        let batch = test_batch(vec![4, 5], vec![400, 500]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        let mut primary = Dataset::open(&uri).await.unwrap();
        primary.append(reader, None).await.unwrap();

        // Branch still sees fork-point state (3 rows, not 5)
        let branched = open_branch(&uri, "snap").await.unwrap();
        let count = branched.count_rows(None).await.unwrap();
        assert_eq!(count, 3, "branch must not see post-fork primary writes");

        // Primary saw the new rows
        let primary = Dataset::open(&uri).await.unwrap();
        assert_eq!(primary.count_rows(None).await.unwrap(), 5);
    }

    /// Phase 5b guard: Lance's `Scanner::nearest` on a branch traverses
    /// `base_paths`, so a fork's nearest-K sees its own rows over the
    /// parent's index without bespoke fusion; and a vector index built
    /// against the branch is branch-local -- main does not get it.
    ///
    /// Began as the Phase 5b spike that decided against a
    /// `FusedVectorSearchExec`; it printed its verdict and stayed
    /// `#[ignore]`d, so the semantics `BranchedBackend::vector_search` and
    /// AGENTS.md rely on were never checked again. It now asserts them.
    #[tokio::test]
    async fn phase5b_spike_per_branch_vector() {
        use arrow_array::{Float32Array, RecordBatch as Batch, UInt64Array};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use lance::index::DatasetIndexExt;
        use lance::index::vector::VectorIndexParams;
        use lance_index::IndexType;
        use lance_linalg::distance::MetricType;

        let dir = TempDir::new().unwrap();
        let uri = format!("{}/vec_ds.lance", dir.path().display());

        // Schema: id u64 + vector FixedSizeList<Float32, 4>.
        let vec_field = Field::new("item", DataType::Float32, false);
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(vec_field.clone()), 4),
                false,
            ),
        ]));

        // Helper to build a batch with 4-d vectors.
        let make_batch = |ids: Vec<u64>, vecs: Vec<[f32; 4]>| -> Batch {
            let flat: Vec<f32> = vecs.into_iter().flatten().collect();
            let values = Float32Array::from(flat);
            let list = arrow_array::FixedSizeListArray::new(
                Arc::new(vec_field.clone()),
                4,
                Arc::new(values),
                None,
            );
            Batch::try_new(
                schema.clone(),
                vec![Arc::new(UInt64Array::from(ids)), Arc::new(list)],
            )
            .unwrap()
        };

        // Seed primary with 100 rows, all near origin.
        let primary_batch = {
            let ids: Vec<u64> = (0..100).collect();
            let vecs: Vec<[f32; 4]> = (0..100)
                .map(|i| [(i as f32) * 0.001, 0.0, 0.0, 0.0])
                .collect();
            make_batch(ids, vecs)
        };
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(primary_batch)].into_iter(),
            schema.clone(),
        );
        Dataset::write(reader, &uri, None).await.unwrap();

        // Build a vector index on primary's main branch.
        let mut main_ds = Dataset::open(&uri).await.unwrap();
        let params = VectorIndexParams::ivf_flat(1, MetricType::L2);
        main_ds
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("primary_vec".into()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Branch off main and append 5 fork-only rows clustered far
        // away (so they're easy to distinguish in nearest-N results).
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "fork-vec", v_main).await.unwrap();
        let fork_batch = {
            let ids: Vec<u64> = (1000..1005).collect();
            let vecs: Vec<[f32; 4]> = (0..5)
                .map(|i| [100.0 + (i as f32), 0.0, 0.0, 0.0])
                .collect();
            make_batch(ids, vecs)
        };
        let reader2 =
            arrow_array::RecordBatchIterator::new(vec![Ok(fork_batch)].into_iter(), schema.clone());
        write_to_branch(&uri, "fork-vec", reader2).await.unwrap();

        // Probe 1: query near 100.0 on the FORK branch — does the
        // result include the fork-only rows?
        let on_branch = open_branch(&uri, "fork-vec").await.unwrap();
        let query = Float32Array::from(vec![100.5_f32, 0.0, 0.0, 0.0]);
        let mut scanner = on_branch.scan();
        scanner.nearest("vector", &query, 5).unwrap();
        let stream = scanner.try_into_stream().await.unwrap();
        let batches = futures::TryStreamExt::try_collect::<Vec<_>>(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut ids: Vec<u64> = Vec::new();
        for b in &batches {
            let id_col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                ids.push(id_col.value(i));
            }
        }
        assert_eq!(total, 5, "nearest-5 on the branch: ids={ids:?}");
        assert!(
            ids.iter().all(|i| *i >= 1000),
            "the five fork-only rows are the nearest to [100.5, 0, 0, 0]; the branch \
             scan must see them through base_paths, got ids={ids:?}"
        );

        // Probe 2: try to build a vector index on the fork branch.
        let mut on_branch_mut = open_branch(&uri, "fork-vec").await.unwrap();
        on_branch_mut
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("fork_vec".into()),
                &params,
                true,
            )
            .await
            .expect("a vector index builds against a branch-checked-out dataset");
        let main_after = Dataset::open(&uri).await.unwrap();
        let main_indices = main_after.load_indices().await.unwrap();
        let branch_after = open_branch(&uri, "fork-vec").await.unwrap();
        let branch_indices = branch_after.load_indices().await.unwrap();
        assert!(
            branch_indices
                .iter()
                .any(|i: &lance::table::format::IndexMetadata| i.name == "fork_vec"),
            "the branch sees its own vector index"
        );
        assert!(
            !main_indices
                .iter()
                .any(|i: &lance::table::format::IndexMetadata| i.name == "fork_vec"),
            "a branch's vector index must not leak to main"
        );
    }

    /// Phase 5a guard: `Dataset::create_index_builder` against a
    /// branch-checked-out dataset produces a branch-local scalar index --
    /// the branch sees it, main does not.
    ///
    /// Began as the Phase 5a spike that told `fork_index_builder` which
    /// path was real; it printed its verdict and stayed `#[ignore]`d, so the
    /// semantics the fork drop path relies on (fork-local index files live
    /// under the branch, AGENTS.md) were never checked again. It now asserts
    /// them.
    #[tokio::test]
    async fn phase5a_spike_per_branch_index() {
        use lance::index::DatasetIndexExt;
        use lance_index::{IndexType, scalar::ScalarIndexParams};

        let (_dir, uri) = seed_dataset().await;
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "fork-spike", v_main).await.unwrap();

        // Append fork-only rows on the branch.
        let batch = test_batch(vec![100, 101, 102], vec![1000, 1100, 1200]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        write_to_branch(&uri, "fork-spike", reader).await.unwrap();

        // Probe 1: try to build a scalar BTree on `id` against the branch.
        let mut on_branch = open_branch(&uri, "fork-spike").await.unwrap();
        let scalar_params = ScalarIndexParams::default();
        on_branch
            .create_index_builder(&["id"], IndexType::Scalar, &scalar_params)
            .name("phase5a_spike".to_string())
            .replace(true)
            .await
            .expect("a scalar index builds against a branch-checked-out dataset");
        let main_after = Dataset::open(&uri).await.unwrap();
        let main_indices = main_after.load_indices().await.unwrap();
        let branch_after = open_branch(&uri, "fork-spike").await.unwrap();
        let branch_indices = branch_after.load_indices().await.unwrap();
        assert!(
            branch_indices
                .iter()
                .any(|i: &lance::table::format::IndexMetadata| i.name == "phase5a_spike"),
            "the branch sees its own scalar index"
        );
        assert!(
            !main_indices
                .iter()
                .any(|i: &lance::table::format::IndexMetadata| i.name == "phase5a_spike"),
            "a branch's scalar index must not leak to main"
        );
    }

    /// Spike S1 (#249): can a nullable column be added to a *branch* whose
    /// fragments live in the parent via `base_paths`?
    ///
    /// This is the assumption the whole #249 fork story rests on. Lance's
    /// `add_columns` rebuilds the fragment list from `dataset.get_fragments()`
    /// into an `Operation::Merge`; if inherited base-path references did not
    /// survive that round-trip, forks would need a different strategy
    /// entirely.
    ///
    /// Probes, in order:
    ///   1. the widening succeeds on the branch at all;
    ///   2. main does NOT gain the column (no leak to primary);
    ///   3. the branch reads back every row -- inherited *and* branch-local --
    ///      with the new column NULL;
    ///   4. a subsequent append carrying the wide schema is accepted.
    #[tokio::test]
    async fn phase249_spike_add_nullable_column_on_branch() {
        use futures::TryStreamExt;
        use lance::dataset::NewColumnTransform;

        let (_dir, uri) = seed_dataset().await;
        let v_main = current_version(&uri).await.unwrap();
        create_branch(&uri, "add-col-spike", v_main).await.unwrap();

        // Branch-local rows, so the branch has both inherited and own fragments.
        let batch = test_batch(vec![100, 101], vec![1000, 1100]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        write_to_branch(&uri, "add-col-spike", reader)
            .await
            .unwrap();

        // Probe 1: widen the branch.
        let new_field = Arc::new(ArrowSchema::new(vec![Field::new(
            "added_later",
            DataType::Utf8,
            true,
        )]));
        let mut on_branch = open_branch(&uri, "add-col-spike").await.unwrap();
        let res = on_branch
            .add_columns(NewColumnTransform::AllNulls(new_field), None, None)
            .await;
        match &res {
            Ok(()) => eprintln!("S1 PROBE 1: add_columns on branch OK"),
            Err(e) => eprintln!("S1 PROBE 1: add_columns on branch FAILED: {e}"),
        }
        res.expect("add_columns(AllNulls) on a branch");

        // Probe 2: main must not have gained the column.
        let main_after = Dataset::open(&uri).await.unwrap();
        let main_has = main_after
            .schema()
            .fields
            .iter()
            .any(|f| f.name == "added_later");
        let branch_after = open_branch(&uri, "add-col-spike").await.unwrap();
        let branch_has = branch_after
            .schema()
            .fields
            .iter()
            .any(|f| f.name == "added_later");
        eprintln!("S1 PROBE 2: main_has={main_has} branch_has={branch_has}");
        assert!(!main_has, "column leaked to main");
        assert!(branch_has, "branch did not gain the column");

        // Probe 3: every row still readable, new column NULL throughout.
        let rows = branch_after.count_rows(None).await.unwrap();
        eprintln!("S1 PROBE 3: branch row count = {rows} (expect 5: 3 inherited + 2 local)");
        assert_eq!(rows, 5, "inherited fragments lost by the widening");
        let batches = branch_after
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut seen = 0usize;
        for b in &batches {
            let col = b
                .column_by_name("added_later")
                .expect("added_later present");
            assert_eq!(col.null_count(), b.num_rows(), "new column not all-null");
            seen += b.num_rows();
        }
        assert_eq!(seen, 5, "scan returned fewer rows than count_rows");

        // Probe 4: an append carrying the wide schema is accepted.
        let wide_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("added_later", DataType::Utf8, true),
        ]));
        let wide = RecordBatch::try_new(
            wide_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![200u64])),
                Arc::new(Int64Array::from(vec![2000i64])),
                Arc::new(arrow_array::StringArray::from(vec![Some("x")])),
            ],
        )
        .unwrap();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(wide)].into_iter(), wide_schema);
        let appended = write_to_branch(&uri, "add-col-spike", reader).await;
        eprintln!("S1 PROBE 4: wide append -> {appended:?}");
        appended.expect("append with the widened schema");

        let final_rows = open_branch(&uri, "add-col-spike")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap();
        eprintln!("S1 VERDICT: final branch rows = {final_rows} (expect 6)");
        assert_eq!(final_rows, 6);
    }

    /// Spike S2 (#249): does a scalar index survive a nullable-column add?
    ///
    /// `vertex.rs` creates default BTree indexes on every per-label table, so
    /// if widening dropped or invalidated them the fix would silently
    /// deoptimise every existing store.
    #[tokio::test]
    async fn phase249_spike_add_column_preserves_scalar_index() {
        use lance::dataset::NewColumnTransform;
        use lance::index::DatasetIndexExt;
        use lance_index::{IndexType, scalar::ScalarIndexParams};

        let (_dir, uri) = seed_dataset().await;
        let mut ds = Dataset::open(&uri).await.unwrap();
        ds.create_index_builder(&["id"], IndexType::Scalar, &ScalarIndexParams::default())
            .name("s2_idx".to_string())
            .replace(true)
            .await
            .unwrap();
        let before = Dataset::open(&uri)
            .await
            .unwrap()
            .load_indices()
            .await
            .unwrap();
        eprintln!("S2: indexes before widening = {}", before.len());

        let mut ds = Dataset::open(&uri).await.unwrap();
        ds.add_columns(
            NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![Field::new(
                "added_later",
                DataType::Utf8,
                true,
            )]))),
            None,
            None,
        )
        .await
        .expect("widen a dataset carrying a scalar index");

        let after_ds = Dataset::open(&uri).await.unwrap();
        let after = after_ds.load_indices().await.unwrap();
        eprintln!("S2: indexes after widening  = {}", after.len());
        for i in after.iter() {
            eprintln!("  index name={} uuid={}", i.name, i.uuid);
        }
        // Still queryable through the index path.
        let n = after_ds.count_rows(Some("id = 2".into())).await.unwrap();
        eprintln!(
            "S2 VERDICT: indexes survived={} filtered_count={n}",
            after.len() == before.len()
        );
        assert_eq!(after.len(), before.len(), "widening dropped a scalar index");
        assert_eq!(n, 1);
    }

    /// Spike S4 (#249): `add_columns` builds `Operation::Merge` from the
    /// manifest version captured at open, and has no retry of its own, while
    /// `Merge` conflicts *retryably* with a concurrent `Append`. Establishes
    /// what a stale handle actually does, so the production path knows whether
    /// it must reopen inside the lock and retry.
    #[tokio::test]
    async fn phase249_spike_stale_handle_widening_conflicts() {
        use lance::dataset::NewColumnTransform;

        let (_dir, uri) = seed_dataset().await;
        // Open the handle FIRST, then let an append move the dataset on.
        let mut stale = Dataset::open(&uri).await.unwrap();
        let batch = test_batch(vec![9], vec![900]);
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), test_schema());
        Dataset::write(
            reader,
            &uri,
            Some(lance::dataset::WriteParams {
                mode: lance::dataset::WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let res = stale
            .add_columns(
                NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![Field::new(
                    "added_later",
                    DataType::Utf8,
                    true,
                )]))),
                None,
                None,
            )
            .await;
        eprintln!("S4: widening from a STALE handle -> {res:?}");

        // A freshly-opened handle must always succeed.
        let mut fresh = Dataset::open(&uri).await.unwrap();
        let fresh_res = fresh
            .add_columns(
                NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![Field::new(
                    "added_later2",
                    DataType::Utf8,
                    true,
                )]))),
                None,
                None,
            )
            .await;
        eprintln!("S4 VERDICT: fresh-handle widening -> {fresh_res:?}");
        fresh_res.expect("widening from a freshly opened handle");
    }
}
