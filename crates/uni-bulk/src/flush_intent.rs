// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Crash-atomicity for bulk loads.
//!
//! A bulk load writes many independent Lance datasets — per-label vertex tables,
//! the main vertices table, fwd/bwd edge delta tables, the main edges table —
//! each committed as its own Lance transaction. Lance has no cross-dataset
//! atomic commit, so a crash *between* two of those commits would otherwise
//! leave the tables permanently divergent (an entity in the per-label table but
//! not the main table), with no reconciliation on reopen.
//! [`crate::BulkWriter::abort`] only helps if it is actually reached, which a
//! crash skips.
//!
//! This module makes the *whole bulk load* atomic with a durable intent marker
//! per load plus reconciliation at database open:
//!
//! 1. Each load owns one marker, `catalog/bulk_flush_intents/{load id}.json`,
//!    so any number of loads can be in flight at once and a load only ever
//!    writes or deletes its own.
//! 2. On a backend that stages writes (every Lance primary), nothing is
//!    committed until `commit()` publishes the staged rows. Before the first
//!    table is published the marker records, per table and in publish order,
//!    a durable description of the staged rows (phase `Active`, mode
//!    *publish*). From then on the load is decided: recovery **completes the
//!    publish** — each table's rows are published exactly once, onto the
//!    table's current version — so it never undoes anything, and so never
//!    discards what another writer committed to the same tables, during the
//!    publish or after it.
//! 3. On a backend that cannot stage, rows are visible as they are written, so
//!    the marker records each touched table's pre-load Lance version *before*
//!    its first write (phase `Active`, no publish record) and recovery **rolls
//!    every touched table back** to it, dropping tables the load created. That
//!    undo is by version, so it also discards whatever else reached those
//!    tables meanwhile; no such backend ships with uni.
//! 4. [`crate::BulkWriter::commit`] writes the marker as `Committed` (carrying
//!    the new snapshot id) *after* the manifest is durably saved but *before*
//!    the latest pointer is flipped, then deletes the marker once everything is
//!    finalized. Recovery of a `Committed` marker moves the latest pointer to
//!    the load's snapshot only if no other writer has moved it since.
//!
//! Recovery is idempotent: a marker is deleted only after its reconciliation
//! fully succeeded, and repeating a completed step is a no-op, so a crash
//! during recovery is recovered by the next open. The single marker file of
//! earlier releases (`catalog/bulk_flush_intent.json`) is still reconciled.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use serde::{Deserialize, Serialize};
use uni_common::core::schema::IndexStatus;
use uni_store::backend::traits::RecordedPublish;
use uni_store::storage::manager::StorageManager;
use uni_store::store_utils::{delete_with_timeout, get_with_timeout, put_with_timeout};

const INTENT_TIMEOUT: Duration = Duration::from_secs(30);

/// Directory of the per-load markers.
const INTENT_DIR: &str = "catalog/bulk_flush_intents";

fn intent_path(load_id: &str) -> ObjectStorePath {
    ObjectStorePath::from(format!("{INTENT_DIR}/{load_id}.json"))
}

/// The one marker of releases that kept a single marker per database.
fn legacy_intent_path() -> ObjectStorePath {
    ObjectStorePath::from("catalog/bulk_flush_intent.json")
}

/// Phase of an in-flight bulk load.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum BulkIntentPhase {
    /// Tables are being written or published; the manifest is not yet saved.
    /// Recovery completes a recorded publish, or rolls a visible load back.
    Active,
    /// The load committed (manifest durably saved); only the latest-pointer flip
    /// and/or marker delete may be missing. Recovery rolls forward.
    Committed,
}

/// One table's staged rows, as recorded before the publish began.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StagedTablePublish {
    /// Table the rows are published to.
    pub table: String,
    /// The backend's durable description of the staged rows
    /// (`StorageBackend::staged_record`).
    pub staged: serde_json::Value,
}

/// Durable record of an in-flight bulk load's touched tables and phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BulkFlushIntent {
    pub phase: BulkIntentPhase,
    /// table name -> pre-load Lance version (`None` = table created during the
    /// load). A visible load is rolled back to these; for a recorded publish
    /// they bound the versions searched for an earlier publish of each table.
    pub tables: BTreeMap<String, Option<u64>>,
    /// Target snapshot id, set when transitioning to `Committed` (used to roll
    /// the latest pointer forward during recovery).
    pub snapshot_id: Option<String>,
    /// The staged rows of every table, in publish order; `Some` when the load
    /// stages, in which case recovery completes the publish instead of
    /// rolling back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish: Option<Vec<StagedTablePublish>>,
    /// Vertex labels the load wrote, whose user indexes a completed publish
    /// leaves out of date.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

fn table_map(tables: &HashMap<String, Option<u64>>) -> BTreeMap<String, Option<u64>> {
    tables.iter().map(|(k, v)| (k.clone(), *v)).collect()
}

/// Write/overwrite `load_id`'s marker in the `Active` phase for a load whose
/// writes are visible as they land. Must be called *before* those writes.
pub(crate) async fn write_active(
    store: &Arc<dyn ObjectStore>,
    load_id: &str,
    tables: &HashMap<String, Option<u64>>,
) -> Result<()> {
    let intent = BulkFlushIntent {
        phase: BulkIntentPhase::Active,
        tables: table_map(tables),
        snapshot_id: None,
        publish: None,
        labels: Vec::new(),
    };
    write(store, load_id, &intent).await
}

/// Write `load_id`'s marker in the `Active` phase with the staged rows of
/// every table about to be published. Must be called *before* the first table
/// is published: from then on, recovery completes the publish.
pub(crate) async fn write_publishing(
    store: &Arc<dyn ObjectStore>,
    load_id: &str,
    tables: &HashMap<String, Option<u64>>,
    publish: Vec<StagedTablePublish>,
    labels: Vec<String>,
) -> Result<()> {
    let intent = BulkFlushIntent {
        phase: BulkIntentPhase::Active,
        tables: table_map(tables),
        snapshot_id: None,
        publish: Some(publish),
        labels,
    };
    write(store, load_id, &intent).await
}

/// Promote `load_id`'s marker to `Committed`, recording the target snapshot
/// id. Must be called *after* the snapshot manifest is durably saved and
/// *before* the latest pointer is flipped.
pub(crate) async fn write_committed(
    store: &Arc<dyn ObjectStore>,
    load_id: &str,
    tables: &HashMap<String, Option<u64>>,
    snapshot_id: &str,
) -> Result<()> {
    let intent = BulkFlushIntent {
        phase: BulkIntentPhase::Committed,
        tables: table_map(tables),
        snapshot_id: Some(snapshot_id.to_string()),
        publish: None,
        labels: Vec::new(),
    };
    write(store, load_id, &intent).await
}

async fn write(
    store: &Arc<dyn ObjectStore>,
    load_id: &str,
    intent: &BulkFlushIntent,
) -> Result<()> {
    let json = serde_json::to_vec(intent)?;
    put_with_timeout(
        store,
        &intent_path(load_id),
        Bytes::from(json),
        INTENT_TIMEOUT,
    )
    .await?;
    Ok(())
}

/// Delete `load_id`'s marker (a missing marker is not an error). Never
/// touches another load's.
pub(crate) async fn clear(store: &Arc<dyn ObjectStore>, load_id: &str) -> Result<()> {
    delete_marker(store, &intent_path(load_id)).await
}

async fn delete_marker(store: &Arc<dyn ObjectStore>, path: &ObjectStorePath) -> Result<()> {
    match delete_with_timeout(store, path, INTENT_TIMEOUT).await {
        Ok(()) => Ok(()),
        Err(e) if is_not_found(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn read(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectStorePath,
) -> Result<Option<BulkFlushIntent>> {
    match get_with_timeout(store, path, INTENT_TIMEOUT).await {
        Ok(result) => {
            let bytes = result.bytes().await?;
            let intent: BulkFlushIntent = serde_json::from_slice(&bytes)?;
            Ok(Some(intent))
        }
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Every marker present: the per-load ones, then the legacy single one.
async fn marker_paths(store: &Arc<dyn ObjectStore>) -> Result<Vec<ObjectStorePath>> {
    use futures::TryStreamExt;

    let prefix = ObjectStorePath::from(INTENT_DIR);
    let mut paths: Vec<ObjectStorePath> = store
        .list(Some(&prefix))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await?;
    paths.retain(|p| p.extension() == Some("json"));
    paths.sort();
    paths.push(legacy_intent_path());
    Ok(paths)
}

/// #233 Tier 1: this matched on the error's rendered STRING, so any genuine
/// store failure whose text happens to contain "not found" read as "no marker
/// present" — an interrupted bulk load would then never be reconciled and its
/// half-written tables would stay. `uni-store` already carries the typed
/// discrimination this needs.
fn is_not_found(e: &anyhow::Error) -> bool {
    uni_store::store_utils::is_not_found(e)
}

/// Reconcile every interrupted bulk load at database open. See the module docs
/// for the recovery policy. Idempotent and safe to call on every open (a no-op
/// when no marker is present).
///
/// # Errors
///
/// Returns an error if a marker cannot be read or any load cannot be fully
/// reconciled. Every marker not fully reconciled is kept, so the next open
/// retries it; the others are reconciled and deleted regardless.
pub async fn recover_interrupted_bulk_load(storage: &StorageManager) -> Result<()> {
    let store = storage.store();
    let mut failed = Vec::new();
    for path in marker_paths(&store).await? {
        let Some(intent) = read(&store, &path).await? else {
            continue;
        };
        match reconcile(storage, &intent).await {
            // Only clear the marker once reconciliation fully succeeded, so a
            // failed or interrupted one is retried by the next open.
            Ok(()) => delete_marker(&store, &path).await?,
            Err(e) => {
                tracing::warn!(marker = %path, error = %e, "bulk recovery failed; marker retained for retry");
                failed.push(format!("{path}: {e:#}"));
            }
        }
    }
    if !failed.is_empty() {
        anyhow::bail!(
            "bulk recovery: {} interrupted load(s) not reconciled; their markers are retained \
             for retry: {}",
            failed.len(),
            failed.join("; ")
        );
    }
    Ok(())
}

/// Reconcile one load's marker; the caller deletes it on success.
async fn reconcile(storage: &StorageManager, intent: &BulkFlushIntent) -> Result<()> {
    match (intent.phase, &intent.publish) {
        (BulkIntentPhase::Committed, _) => finish_committed(storage, intent).await,
        (BulkIntentPhase::Active, Some(publish)) => {
            complete_publish(storage, intent, publish).await
        }
        (BulkIntentPhase::Active, None) => roll_back_visible(storage, intent).await,
    }
}

/// The load committed (manifest written): flip the latest pointer to its
/// snapshot — unless another writer has moved the pointer on since, whose
/// snapshot then already supersedes the load's.
async fn finish_committed(storage: &StorageManager, intent: &BulkFlushIntent) -> Result<()> {
    let Some(snapshot_id) = &intent.snapshot_id else {
        return Ok(());
    };
    let snapshots = storage.snapshot_manager();
    let ours = snapshots.load_snapshot(snapshot_id).await?;
    let latest = snapshots.load_latest_snapshot().await?;
    let flip = match &latest {
        None => true,
        Some(latest) if latest.snapshot_id == *snapshot_id => false,
        Some(latest) => ours.parent_snapshot.as_deref() == Some(latest.snapshot_id.as_str()),
    };
    if flip {
        snapshots.set_latest_snapshot(snapshot_id).await?;
    } else if latest
        .as_ref()
        .is_some_and(|l| l.snapshot_id != *snapshot_id)
    {
        tracing::info!(
            snapshot = %snapshot_id,
            "bulk recovery: another writer moved the latest snapshot past this load's; \
             keeping it"
        );
    }
    tracing::warn!(
        tables = intent.tables.len(),
        "Finalized a committed-but-unfinished bulk load on reopen"
    );
    Ok(())
}

/// The load was publishing: publish every table the interrupted attempt did
/// not reach, exactly once, onto each table's current version.
async fn complete_publish(
    storage: &StorageManager,
    intent: &BulkFlushIntent,
    publish: &[StagedTablePublish],
) -> Result<()> {
    let backend = storage.backend();
    let mut published = 0usize;
    for entry in publish {
        let after = intent.tables.get(&entry.table).copied().flatten();
        let outcome = backend
            .publish_staged_record(&entry.table, &entry.staged, after)
            .await?;
        if outcome == RecordedPublish::Published {
            published += 1;
        }
        // Test seam: a crash during recovery, after some tables are published.
        fail::fail_point!("bulk::recover::after-table", |_| Err(anyhow::anyhow!(
            "injected failure during bulk recovery after publishing '{}'",
            entry.table
        )));
    }
    backend.clear_cache();

    // The rows bypassed every user index on their labels; mark those indexes
    // `Stale`, which keeps the planner off them and is itself a rebuild
    // trigger, as an uninterrupted commit's index step would have done.
    let schema = storage.schema_manager();
    let stale: Vec<String> = schema
        .schema()
        .indexes
        .iter()
        .filter(|idx| intent.labels.iter().any(|l| l == idx.label()))
        .map(|idx| idx.name().to_string())
        .collect();
    for name in &stale {
        schema.update_index_metadata(name, |m| {
            m.status = IndexStatus::Stale;
        })?;
    }
    if !stale.is_empty() {
        schema.save().await?;
    }
    tracing::warn!(
        tables = publish.len(),
        published,
        stale_indexes = stale.len(),
        "Completed an interrupted bulk load publish on reopen"
    );
    Ok(())
}

/// A load whose writes were visible as they landed: roll every touched table
/// back to its pre-load version (drop tables created during the load) so the
/// per-label and main tables cannot be left divergent.
async fn roll_back_visible(storage: &StorageManager, intent: &BulkFlushIntent) -> Result<()> {
    let backend = storage.backend();
    let mut failures = 0usize;
    for (table, pre_version) in &intent.tables {
        let result = match pre_version {
            Some(version) => backend.rollback_table(table, *version).await,
            None => backend.drop_table(table).await,
        };
        if let Err(e) = result {
            // A drop/rollback error is only a genuine reconciliation
            // failure if the table is not already in its desired state.
            // A table created during the load (pre_version `None`) that
            // was never actually committed — the load faulted before
            // writing it — is already absent, so its failed `drop_table`
            // is a benign no-op, not divergence. A `Some` pre-version
            // means the table existed before the load and must be
            // restored, so a rollback error there is always genuine.
            let benign =
                pre_version.is_none() && !backend.table_exists(table).await.unwrap_or(true);
            if benign {
                tracing::debug!(table = %table, "bulk recovery: table already absent; nothing to roll back");
            } else {
                failures += 1;
                tracing::warn!(table = %table, error = %e, "bulk recovery: table rollback failed");
            }
        }
    }
    backend.clear_cache();
    tracing::warn!(
        tables = intent.tables.len(),
        failures,
        "Rolled back an interrupted bulk load on reopen"
    );
    // Fail before clearing (the caller keeps the marker), so the next reopen
    // retries — otherwise a divergent table would be abandoned permanently.
    if failures > 0 {
        anyhow::bail!("{failures} table rollback(s) failed");
    }
    Ok(())
}
