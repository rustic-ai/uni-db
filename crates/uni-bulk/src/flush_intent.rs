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
//! 2. Nothing is committed until `commit()` publishes the staged rows (a
//!    backend that cannot stage is refused a bulk load up front,
//!    `UniError::BulkLoadUnsupported`). Before the first
//!    table is published the marker records, per table and in publish order,
//!    a durable description of the staged rows (phase `Active`, mode
//!    *publish*). From then on the load is decided: recovery **completes the
//!    publish** — each table's rows are published exactly once, onto the
//!    table's current version — so it never undoes anything, and so never
//!    discards what another writer committed to the same tables, during the
//!    publish or after it.
//! 3. The single marker of earlier releases, written before loads staged,
//!    records each touched table's pre-load Lance version (phase `Active`, no
//!    publish record); recovery **rolls every touched table back** to it,
//!    dropping tables the load created, as those releases did. That undo is by
//!    version, so it also discards whatever else reached those tables
//!    meanwhile, which is why no load of this release takes that path. A
//!    per-load marker without a publish record could only come from a backend
//!    that cannot stage, which no release ships; recovery refuses it and keeps
//!    it rather than roll anything back.
//! 4. [`crate::BulkWriter::commit`] writes the marker as `Committed` (carrying
//!    the new snapshot id, when the manifest was saved) once every table is
//!    published, and before the latest pointer is flipped. Recovery of a
//!    `Committed` marker publishes nothing; it moves the latest pointer to the
//!    load's snapshot only if no other writer has moved it since.
//! 5. Before a load's marker is deleted -- by `commit` or by recovery -- a
//!    *receipt* (`catalog/bulk_loads_applied/{load id}.json`) is written. The
//!    marker answers "is this load applied?" while it exists and the receipt
//!    answers it afterwards, which is what [`bulk_load_applied`] reads.
//!
//! 6. A fork that stages keeps its markers and receipts in its own catalog
//!    namespace, `catalog/forks/{fork_id}/` (see `Markers`); recovery runs
//!    on the fork's storage when the fork is opened and publishes through the
//!    fork's branches, never touching primary tables.
//! 7. A load whose publish `commit` could not finish (`BulkCommitIncomplete`)
//!    is completed in the background through `publish_records`, hidden from
//!    the handle's readers until it is, and otherwise by the next open.
//!
//! 8. A load's events (see [`crate::events`]) are delivered after it becomes
//!    visible and before its marker is promoted to `Committed`. A marker
//!    recovery finds still `Active` therefore belongs to a load whose events
//!    are not known to have been delivered. Recovery keeps such a marker, as
//!    `Committed` with `events_pending`, and returns the load
//!    ([`RecoveredLoad`]) for the database to deliver once its event
//!    machinery exists; [`finish_recovered_load_events`] then finishes the
//!    marker. A crash, or a shutdown, before that leaves the marker for the
//!    next open, which delivers the events again: at least once, never a
//!    hole. A `Committed` marker written with `events_pending` (the commit's
//!    delivery was interrupted by a shutdown) is returned the same way.
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

/// Directory of the per-load markers, under a catalog root.
const INTENT_DIR: &str = "bulk_flush_intents";

/// Directory of the receipts of applied loads, under a catalog root.
const RECEIPT_DIR: &str = "bulk_loads_applied";

/// Where one database's or one fork's markers and receipts live.
///
/// The primary keeps them under `catalog/`; a fork under its own catalog
/// namespace `catalog/forks/{fork_id}/`, beside its id allocator and snapshot
/// manifests, so dropping the fork removes them with the rest of its
/// artifacts (`uni_store::fork::delete_fork_artifacts`) and the primary's
/// recovery never sees a fork's load. These files are not fork-registry
/// state, so they are not written through `ForkRegistryHandle`.
#[derive(Clone)]
pub(crate) struct Markers {
    store: Arc<dyn ObjectStore>,
    root: String,
    /// Whether the single marker of earlier releases is reconciled too
    /// (primary only; forks never wrote one).
    legacy: bool,
}

impl Markers {
    /// The namespace `storage` reads and writes: its fork's, or the primary's.
    pub(crate) fn for_storage(storage: &StorageManager) -> Self {
        match storage.fork_scope() {
            Some(scope) => Self {
                store: storage.store(),
                root: format!("catalog/forks/{}", scope.fork_id()),
                legacy: false,
            },
            None => Self {
                store: storage.store(),
                root: "catalog".to_string(),
                legacy: true,
            },
        }
    }

    fn intent_dir(&self) -> String {
        format!("{}/{INTENT_DIR}", self.root)
    }

    fn intent_path(&self, load_id: &str) -> ObjectStorePath {
        ObjectStorePath::from(format!("{}/{load_id}.json", self.intent_dir()))
    }

    fn receipt_path(&self, load_id: &str) -> ObjectStorePath {
        ObjectStorePath::from(format!("{}/{RECEIPT_DIR}/{load_id}.json", self.root))
    }

    /// The load id a per-load marker path names; `None` for the legacy marker.
    fn marker_load_id(&self, path: &ObjectStorePath) -> Option<String> {
        if !path
            .as_ref()
            .starts_with(&format!("{}/", self.intent_dir()))
        {
            return None;
        }
        path.filename()?.strip_suffix(".json").map(str::to_string)
    }
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
    /// The `_version` every row of the load carries. A completed publish
    /// raises the snapshot ceiling to it (see [`raise_snapshot_hwm`]); absent
    /// in markers of earlier releases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    /// Whether the published edges' declared endpoints were checked again
    /// after the publish (see `crate::endpoint_check`) and any downgrade
    /// saved. Absent (`false`) until then, and in every earlier release's
    /// marker; recovery then checks them once the WAL is replayed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub endpoints_checked: bool,
    /// Rows the load wrote (vertices plus edges), for the announcement of a
    /// load whose events recovery delivers. Absent in earlier releases'
    /// markers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<usize>,
    /// The load is visible but its events are not known to have been
    /// delivered: the next open delivers them (see [`RecoveredLoad`]) before
    /// the marker is finished.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub events_pending: bool,
}

/// A load whose events recovery leaves to the database to deliver; see
/// [`recover_interrupted_bulk_load`].
///
/// Deliver them through the database's `BulkEventSink` path (announce, then
/// stream the rows [`Self::into_visible`] reads back), then call
/// [`finish_recovered_load_events`]. Until then the load's marker stays, so a
/// crash delivers them again at the next open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredLoad {
    /// The load's id.
    pub load_id: String,
    /// The `_version` every row of the load carries.
    pub write_version: u64,
    /// Labels the load wrote vertices under, sorted.
    pub labels: Vec<String>,
    /// Edge types the load wrote edges of, sorted.
    pub edge_types: Vec<String>,
    /// Rows the load wrote; `0` when its marker did not record them.
    pub rows: usize,
}

impl RecoveredLoad {
    /// The load as its events are delivered: announced as `load_id`, its rows
    /// read back from `storage`, the storage it was recovered on.
    #[must_use]
    pub fn into_visible(self, storage: Arc<StorageManager>) -> crate::events::VisibleLoad {
        let reader = crate::events::LoadEventReader::new(
            storage,
            self.write_version,
            !self.labels.is_empty(),
            !self.edge_types.is_empty(),
        );
        crate::events::VisibleLoad {
            load_id: self.load_id,
            write_version: self.write_version,
            labels: self.labels,
            edge_types: self.edge_types,
            rows: self.rows,
            reader,
        }
    }
}

pub(crate) fn table_map(tables: &HashMap<String, Option<u64>>) -> BTreeMap<String, Option<u64>> {
    tables.iter().map(|(k, v)| (k.clone(), *v)).collect()
}

/// Write `load_id`'s marker in the `Active` phase with the staged rows of
/// every table about to be published. Must be called *before* the first table
/// is published: from then on, recovery completes the publish.
pub(crate) async fn write_publishing(
    markers: &Markers,
    load_id: &str,
    tables: &HashMap<String, Option<u64>>,
    publish: Vec<StagedTablePublish>,
    labels: Vec<String>,
    version: Option<u64>,
    rows: usize,
) -> Result<()> {
    let intent = BulkFlushIntent {
        phase: BulkIntentPhase::Active,
        tables: table_map(tables),
        snapshot_id: None,
        publish: Some(publish),
        labels,
        version,
        endpoints_checked: false,
        rows: Some(rows),
        events_pending: false,
    };
    write(markers, load_id, &intent).await
}

/// Promote `load_id`'s marker to `Committed`, recording the target snapshot
/// id when one was saved, whether the published edges' endpoints were
/// checked again, and -- `pending_rows`, the load's row count, when `Some` --
/// that the load's events are still to be delivered by the next open. Must be called only once every row
/// of the load is visible -- recovery of a `Committed` marker publishes
/// nothing -- and *before* the latest pointer is flipped.
#[expect(
    clippy::too_many_arguments,
    reason = "one argument per marker field; a marker is written in one place"
)]
pub(crate) async fn write_committed(
    markers: &Markers,
    load_id: &str,
    tables: &HashMap<String, Option<u64>>,
    snapshot_id: Option<&str>,
    labels: Vec<String>,
    version: Option<u64>,
    endpoints_checked: bool,
    pending_rows: Option<usize>,
) -> Result<()> {
    let intent = BulkFlushIntent {
        phase: BulkIntentPhase::Committed,
        tables: table_map(tables),
        snapshot_id: snapshot_id.map(str::to_string),
        publish: None,
        labels,
        version,
        endpoints_checked,
        rows: pending_rows,
        events_pending: pending_rows.is_some(),
    };
    write(markers, load_id, &intent).await
}

/// Read `load_id`'s marker back; `None` when it is absent.
pub(crate) async fn read_marker(
    markers: &Markers,
    load_id: &str,
) -> Result<Option<BulkFlushIntent>> {
    read(&markers.store, &markers.intent_path(load_id)).await
}

/// Whether a marker means its load is decided: it will be completed (a
/// recorded publish) or is already finalized (`Committed`), as opposed to an
/// earlier release's visible load that recovery rolls back.
pub(crate) fn marker_applies(intent: &BulkFlushIntent) -> bool {
    match intent.phase {
        BulkIntentPhase::Committed => true,
        BulkIntentPhase::Active => intent.publish.is_some(),
    }
}

/// Record that `load_id` is applied. Written before its marker is deleted, so
/// one of the two always answers [`bulk_load_applied`] from the commit point
/// on.
pub(crate) async fn write_receipt(markers: &Markers, load_id: &str) -> Result<()> {
    #[derive(Serialize)]
    struct Receipt<'a> {
        load_id: &'a str,
        applied_at: chrono::DateTime<chrono::Utc>,
    }
    fail::fail_point!("bulk::receipt::write", |_| Err(anyhow::anyhow!(
        "injected receipt write failure"
    )));
    let json = serde_json::to_vec(&Receipt {
        load_id,
        applied_at: chrono::Utc::now(),
    })?;
    put_with_timeout(
        &markers.store,
        &markers.receipt_path(load_id),
        Bytes::from(json),
        INTENT_TIMEOUT,
    )
    .await?;
    Ok(())
}

/// Whether the bulk load `load_id` is applied: its commit point was reached,
/// so its rows are visible, or will be completed at the next open.
///
/// The marker is read *before* the receipt: finalization writes the receipt
/// and only then deletes the marker, so reading in the opposite order could
/// miss both while a load is being finalized.
///
/// # Errors
///
/// Returns an error if `load_id` is not a load id (a UUID), or if the store
/// cannot be read -- never `false` for an unreadable answer.
pub async fn bulk_load_applied(storage: &StorageManager, load_id: &str) -> Result<bool> {
    let id = uuid::Uuid::parse_str(load_id)
        .map_err(|e| anyhow::anyhow!("'{load_id}' is not a bulk load id: {e}"))?;
    // The canonical hyphenated form names the files.
    let load_id = id.hyphenated().to_string();
    let markers = Markers::for_storage(storage);
    if let Some(intent) = read_marker(&markers, &load_id).await? {
        return Ok(marker_applies(&intent));
    }
    match store_head(&markers.store, &markers.receipt_path(&load_id)).await {
        Ok(()) => Ok(true),
        Err(e) if is_not_found(&e) => Ok(false),
        Err(e) => Err(e),
    }
}

async fn store_head(store: &Arc<dyn ObjectStore>, path: &ObjectStorePath) -> Result<()> {
    use object_store::ObjectStoreExt;
    match tokio::time::timeout(INTENT_TIMEOUT, store.head(path)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => anyhow::bail!("Object store head timed out after {INTENT_TIMEOUT:?} for {path}"),
    }
}

/// Write `intent` as `load_id`'s marker, as a test sets up a store state.
#[cfg(test)]
pub(crate) async fn write_for_test(
    markers: &Markers,
    load_id: &str,
    intent: &BulkFlushIntent,
) -> Result<()> {
    write(markers, load_id, intent).await
}

async fn write(markers: &Markers, load_id: &str, intent: &BulkFlushIntent) -> Result<()> {
    let json = serde_json::to_vec(intent)?;
    put_with_timeout(
        &markers.store,
        &markers.intent_path(load_id),
        Bytes::from(json),
        INTENT_TIMEOUT,
    )
    .await?;
    Ok(())
}

/// Delete `load_id`'s marker (a missing marker is not an error). Never
/// touches another load's.
pub(crate) async fn clear(markers: &Markers, load_id: &str) -> Result<()> {
    delete_marker(&markers.store, &markers.intent_path(load_id)).await
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
async fn marker_paths(markers: &Markers) -> Result<Vec<ObjectStorePath>> {
    use futures::TryStreamExt;

    let prefix = ObjectStorePath::from(markers.intent_dir());
    let mut paths: Vec<ObjectStorePath> = markers
        .store
        .list(Some(&prefix))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await?;
    paths.retain(|p| p.extension() == Some("json"));
    paths.sort();
    if markers.legacy {
        paths.push(legacy_intent_path());
    }
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
/// Returns the loads whose events are not known to have been delivered, in
/// the order their rows were versioned (`write_version`), which is the order
/// they became decided. Their markers are kept: deliver each one's events
/// (see [`RecoveredLoad`]), then call [`finish_recovered_load_events`].
///
/// # Errors
///
/// Returns an error if a marker cannot be read or any load cannot be fully
/// reconciled. Every marker not fully reconciled is kept, so the next open
/// retries it; the others are reconciled and deleted regardless.
pub async fn recover_interrupted_bulk_load(storage: &StorageManager) -> Result<Vec<RecoveredLoad>> {
    let markers = Markers::for_storage(storage);
    let store = markers.store.clone();
    let mut failed = Vec::new();
    let mut recovered = Vec::new();
    for path in marker_paths(&markers).await? {
        let Some(intent) = read(&store, &path).await? else {
            continue;
        };
        let legacy = markers.legacy && path == legacy_intent_path();
        match reconcile(storage, &intent, legacy).await {
            // Only clear the marker once reconciliation fully succeeded, so a
            // failed or interrupted one is retried by the next open. A load it
            // applied gets its receipt first, so `bulk_load_applied` keeps
            // answering once the marker is gone.
            Ok(()) => {
                let load_id = markers.marker_load_id(&path);
                if let Some(load_id) = &load_id
                    && let Some(load) = events_to_deliver(storage, load_id, &intent)
                {
                    // Applied and visible, its events not known delivered:
                    // keep the marker, as `Committed` (recovery of it
                    // publishes nothing), until they are.
                    let kept = BulkFlushIntent {
                        endpoints_checked: intent.endpoints_checked
                            || !needs_endpoint_check(storage, &intent),
                        events_pending: true,
                        rows: Some(load.rows),
                        ..committed_unchecked(&intent)
                    };
                    write(&markers, load_id, &kept).await?;
                    recovered.push(load);
                    continue;
                }
                if let Some(load_id) = &load_id
                    && needs_endpoint_check(storage, &intent)
                {
                    // Published, but its endpoints are checked only once the
                    // WAL is replayed (`check_recovered_bulk_endpoints`): a
                    // label removal still in the WAL is invisible here. The
                    // marker stays, as `Committed`, until then.
                    write(&markers, load_id, &committed_unchecked(&intent)).await?;
                    continue;
                }
                if marker_applies(&intent)
                    && let Some(load_id) = &load_id
                {
                    write_receipt(&markers, load_id).await?;
                }
                delete_marker(&store, &path).await?
            }
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
    recovered.sort_by_key(|load| load.write_version);
    Ok(recovered)
}

/// The load whose events the open must deliver, if `intent` (just reconciled)
/// is one: a recorded publish recovery completed -- `commit` promotes the
/// marker to `Committed` only after delivering the events -- or a `Committed`
/// marker whose delivery was left pending. A marker without the load's
/// version (an earlier release's) cannot have its rows told apart from
/// others', so its events are not delivered.
fn events_to_deliver(
    storage: &StorageManager,
    load_id: &str,
    intent: &BulkFlushIntent,
) -> Option<RecoveredLoad> {
    let pending = match intent.phase {
        BulkIntentPhase::Active => intent.publish.is_some(),
        BulkIntentPhase::Committed => intent.events_pending,
    };
    if !pending {
        return None;
    }
    let Some(write_version) = intent.version else {
        tracing::warn!(
            load_id,
            "bulk recovery: a recovered load's marker does not record its version, so its \
             trigger and CDC events cannot be read back and are not delivered"
        );
        return None;
    };
    let mut labels = intent.labels.clone();
    labels.sort();
    Some(RecoveredLoad {
        load_id: load_id.to_owned(),
        write_version,
        labels,
        edge_types: crate::endpoint_check::edge_types_of_tables(storage, intent.tables.keys()),
        rows: intent.rows.unwrap_or(0),
    })
}

/// Finish the marker of a load returned by [`recover_interrupted_bulk_load`]
/// once its events are delivered: write its receipt and delete it -- or, when
/// its edges' declared endpoints are still to be checked (a read-only open
/// skipped [`check_recovered_bulk_endpoints`]), only record the delivery and
/// leave the marker for that check. A missing marker, or one with no
/// delivery pending, is left as it is.
///
/// # Errors
///
/// Returns an error if the marker cannot be read or written, or the receipt
/// written; the marker is then kept, and the next open delivers the events
/// again.
pub async fn finish_recovered_load_events(storage: &StorageManager, load_id: &str) -> Result<()> {
    let markers = Markers::for_storage(storage);
    let Some(intent) = read_marker(&markers, load_id).await? else {
        return Ok(());
    };
    if intent.phase != BulkIntentPhase::Committed || !intent.events_pending {
        return Ok(());
    }
    if !intent.endpoints_checked {
        let delivered = BulkFlushIntent {
            events_pending: false,
            ..intent
        };
        return write(&markers, load_id, &delivered).await;
    }
    write_receipt(&markers, load_id).await?;
    clear(&markers, load_id).await
}

/// Whether `intent`'s load is applied and wrote edges of a type the catalog
/// records as conforming, whose endpoints have not been checked since the
/// publish.
fn needs_endpoint_check(storage: &StorageManager, intent: &BulkFlushIntent) -> bool {
    marker_applies(intent)
        && !intent.endpoints_checked
        && !crate::endpoint_check::conforming_types(
            storage,
            &crate::endpoint_check::edge_types_of_tables(storage, intent.tables.keys()),
        )
        .is_empty()
}

/// `intent` as the `Committed` marker of a published load whose endpoints
/// are still to be checked: recovery of it publishes nothing.
fn committed_unchecked(intent: &BulkFlushIntent) -> BulkFlushIntent {
    BulkFlushIntent {
        phase: BulkIntentPhase::Committed,
        publish: None,
        endpoints_checked: false,
        ..intent.clone()
    }
}

/// Check the declared endpoints of every recovered bulk load's edges, then
/// finish its marker. Call at open after the WAL is replayed (on a fork, the
/// fork's WAL), with the writer it was replayed into.
///
/// [`recover_interrupted_bulk_load`] completes an interrupted publish before
/// the WAL is replayed, so a label removed from an endpoint by a commit still
/// in the WAL would look present to it. For a load that wrote edges of a type
/// the catalog records as conforming, it therefore leaves the marker as
/// `Committed` and this call does the check: each such type with an endpoint
/// lacking a declared label is recorded `Unverified` (in the catalog; on a
/// fork, whose catalog view is never saved, in the fork's own record, which
/// this call also applies at every open of the fork), then the receipt is
/// written and the marker deleted. The load is applied either way; the check only decides
/// what the planner may assume about the type's edges.
///
/// With `persist` false (a read-only open) the downgrade is made in memory
/// and the markers are left for a writable open.
///
/// # Errors
///
/// Returns an error if a marker cannot be read, or the check or the catalog
/// save fails; the marker is kept for the next open.
pub async fn check_recovered_bulk_endpoints(
    storage: &StorageManager,
    writer: &uni_store::runtime::writer::Writer,
    persist: bool,
) -> Result<()> {
    crate::endpoint_check::apply_fork_downgrades(storage).await?;
    let markers = Markers::for_storage(storage);
    for path in marker_paths(&markers).await? {
        let Some(load_id) = markers.marker_load_id(&path) else {
            continue;
        };
        let Some(intent) = read(&markers.store, &path).await? else {
            continue;
        };
        if intent.phase != BulkIntentPhase::Committed || intent.endpoints_checked {
            continue;
        }
        let types = crate::endpoint_check::conforming_types(
            storage,
            &crate::endpoint_check::edge_types_of_tables(storage, intent.tables.keys()),
        );
        let endpoints =
            crate::endpoint_check::load_edge_endpoints(storage, &types, intent.version).await?;
        let found =
            crate::endpoint_check::downgrade_violating_types(storage, Some(writer), &endpoints)
                .await?;
        if !persist {
            continue;
        }
        crate::endpoint_check::persist_downgrades(storage, &found).await?;
        if intent.events_pending {
            // The load's events are still to be delivered; the delivery
            // finishes the marker (`finish_recovered_load_events`).
            let checked = BulkFlushIntent {
                endpoints_checked: true,
                ..intent
            };
            write(&markers, &load_id, &checked).await?;
        } else {
            write_receipt(&markers, &load_id).await?;
            delete_marker(&markers.store, &path).await?;
        }
        tracing::warn!(
            load_id = %load_id,
            checked = types.len(),
            downgraded = found.len(),
            "bulk recovery: checked the declared endpoints of a recovered load's edges"
        );
    }
    Ok(())
}

/// Reconcile one load's marker; the caller deletes it on success. `legacy`
/// says it is the single marker of an earlier release.
async fn reconcile(storage: &StorageManager, intent: &BulkFlushIntent, legacy: bool) -> Result<()> {
    match (intent.phase, &intent.publish) {
        (BulkIntentPhase::Committed, _) => finish_committed(storage, intent).await,
        (BulkIntentPhase::Active, Some(publish)) => {
            complete_publish(storage, intent, publish).await
        }
        (BulkIntentPhase::Active, None) if legacy => roll_back_visible(storage, intent).await,
        (BulkIntentPhase::Active, None) => anyhow::bail!(
            "a bulk load marker without a publish record: written by a backend that cannot \
             stage, whose loads this release refuses. Rolling its tables back by version would \
             discard every later commit to them, so the marker is kept untouched"
        ),
    }
}

/// The load committed (manifest written): flip the latest pointer to its
/// snapshot — unless another writer has moved the pointer on since, whose
/// snapshot then already supersedes the load's.
async fn finish_committed(storage: &StorageManager, intent: &BulkFlushIntent) -> Result<()> {
    // The commit's own index step may not have finished; the labels' indexes
    // may not cover the load's rows.
    mark_indexes_stale(storage, &intent.labels).await?;
    let Some(snapshot_id) = &intent.snapshot_id else {
        // The commit could not save the load's snapshot.
        if let Some(version) = intent.version {
            raise_snapshot_hwm(storage, version).await?;
        }
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
    let published = publish_records(storage, &intent.tables, publish, |table| {
        // Inert unless the `failpoints` feature is on.
        let _ = table;
        // Test seam: a crash during recovery, after some tables are published.
        fail::fail_point!("bulk::recover::after-table", |_| Err(anyhow::anyhow!(
            "injected failure during bulk recovery after publishing '{table}'"
        )));
        Ok(())
    })
    .await?;

    // The rows bypassed every user index on their labels; mark those indexes
    // `Stale`, which keeps the planner off them and is itself a rebuild
    // trigger, as an uninterrupted commit's index step would have done.
    let stale = mark_indexes_stale(storage, &intent.labels).await?;
    // The interrupted commit never recorded the load's snapshot; without this
    // the open about to run would restart the version counter below the
    // load's version.
    if let Some(version) = intent.version {
        raise_snapshot_hwm(storage, version).await?;
    }
    tracing::warn!(
        tables = publish.len(),
        published,
        stale_indexes = stale,
        "Completed an interrupted bulk load publish on reopen"
    );
    Ok(())
}

/// Make the latest snapshot manifest's `version_high_water_mark` at least
/// `version`, and stop it excluding `version`, writing a new manifest (and
/// moving the latest pointer) when it falls short of either.
///
/// A bulk load stamps its rows with a version reserved from the live counter
/// and writes them straight to storage, so no flush records that version in a
/// manifest. Left below it, the manifest's ceiling (`_version <= hwm`) hides
/// the load from a session pinned to that snapshot, and the next open, which
/// restarts the counter at `hwm + 1`, reuses the load's version for new
/// writes.
///
/// # Errors
///
/// Returns an error if a manifest cannot be read or written.
pub(crate) async fn raise_snapshot_hwm(storage: &StorageManager, version: u64) -> Result<()> {
    let snapshots = storage.snapshot_manager();
    let mut manifest = match snapshots.load_latest_snapshot().await? {
        // A manifest cut while the load was staged covers its version but
        // excludes it; it is replaced like a low one.
        Some(latest)
            if latest.version_high_water_mark >= version
                && !latest.unpublished_bulk_versions.contains(&version) =>
        {
            return Ok(());
        }
        Some(mut latest) => {
            latest.parent_snapshot = Some(latest.snapshot_id.clone());
            latest
        }
        None => uni_common::core::snapshot::SnapshotManifest::new(
            String::new(),
            storage.schema_manager().schema().schema_version,
        ),
    };
    manifest.snapshot_id = uuid::Uuid::new_v4().to_string();
    manifest.name = None;
    manifest.created_at = chrono::Utc::now();
    manifest.version_high_water_mark = manifest.version_high_water_mark.max(version);
    // The load is visible now. Other versions listed stay excluded: this path
    // has no writer to say whether their loads were shown since, and keeping
    // a shown load out of a ceiling cut before it is the conservative side.
    manifest.unpublished_bulk_versions.retain(|v| *v != version);
    snapshots.save_snapshot(&manifest).await?;
    snapshots.set_latest_snapshot(&manifest.snapshot_id).await?;
    Ok(())
}

/// Mark every user index on `labels` `Stale` and save the schema; returns how
/// many were marked.
pub(crate) async fn mark_indexes_stale(
    storage: &StorageManager,
    labels: &[String],
) -> Result<usize> {
    let schema = storage.schema_manager();
    let stale: Vec<String> = schema
        .schema()
        .indexes
        .iter()
        .filter(|idx| labels.iter().any(|l| l == idx.label()))
        .map(|idx| idx.name().to_string())
        .collect();
    for name in &stale {
        schema.update_index_metadata(name, |m| {
            m.status = IndexStatus::Stale;
        })?;
    }
    // Never from a fork: its `SchemaManager` is `primary ⊕ overlay` with the
    // primary's path, so a save would write the fork's merged view over the
    // primary's schema. On a fork the marks stay in memory, which is what the
    // fork's planner reads.
    if !stale.is_empty() && storage.fork_scope().is_none() {
        schema.save().await?;
    }
    Ok(stale.len())
}

/// Publish every recorded table not yet published, exactly once, in the
/// recorded order, onto each table's current version; `after_table` runs after
/// each table. Returns how many tables this call published (the rest were
/// already published). The one path both recovery and an in-process
/// [`crate::BulkWriter::commit`] completion take.
pub(crate) async fn publish_records(
    storage: &StorageManager,
    tables: &BTreeMap<String, Option<u64>>,
    publish: &[StagedTablePublish],
    mut after_table: impl FnMut(&str) -> Result<()>,
) -> Result<usize> {
    let backend = storage.backend();
    let mut published = 0usize;
    for entry in publish {
        let after = tables.get(&entry.table).copied().flatten();
        let outcome = backend
            .publish_staged_record(&entry.table, &entry.staged, after)
            .await?;
        if outcome == RecordedPublish::Published {
            published += 1;
        }
        after_table(&entry.table)?;
    }
    backend.clear_cache();
    Ok(published)
}

/// An earlier release's load whose writes were visible as they landed (the
/// legacy single marker): roll every touched table back to its pre-load
/// version (drop tables created during the load) so the per-label and main
/// tables cannot be left divergent.
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
