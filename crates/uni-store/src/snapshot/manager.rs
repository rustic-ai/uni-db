// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::store_utils::{
    DEFAULT_TIMEOUT, get_with_timeout, is_not_found, list_with_timeout, put_opts_with_timeout,
    put_with_timeout,
};
use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::instrument;
use uni_common::core::fork::ForkId;
use uni_common::core::schema::{IndexDefinition, Schema};
use uni_common::core::snapshot::SnapshotManifest;

/// The physical state an explicit snapshot rewinds the store to.
///
/// A [`SnapshotManifest`] records only the MVCC high-water mark, which a
/// time-travel read filters on. Rewinding needs more, because flushed writes
/// after the snapshot changed rows in place (a delete flips `_deleted` on the
/// stored row) and created tables and index datasets the snapshot never had.
/// So an explicit snapshot also records the storage version of every table and
/// index dataset, each pinned by a tag so version cleanup keeps it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RestorePoint {
    /// The snapshot this point belongs to.
    pub snapshot_id: String,
    /// The tag pinning every recorded version against cleanup.
    pub tag: String,
    /// Storage table name to its version at the snapshot.
    pub tables: BTreeMap<String, u64>,
    /// Index dataset, as a path relative to the store root, to its version at
    /// the snapshot.
    #[serde(default)]
    pub index_datasets: BTreeMap<String, u64>,
    /// The catalog's index definitions at the snapshot, which say whether an
    /// index's stored artifact still matches its definition after the rewind.
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
    /// The whole catalog at the snapshot (labels, edge types, properties,
    /// indexes, constraints, the schemaless edge-type registry), which a
    /// restore reinstates.
    ///
    /// `None` for a restore point recorded before the catalog was: such a
    /// snapshot cannot be restored, since its catalog is unknown.
    #[serde(default)]
    pub catalog: Option<Schema>,
}

/// A restore that has started and not finished, persisted before the restore
/// touches any table so that reopening the store completes it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RestoreIntent {
    /// The snapshot being restored.
    pub snapshot_id: String,
    /// The manifest to publish as `latest` once every table is rewound.
    pub manifest_id: String,
    /// Labels and edge types whose indexes are rebuilt after the rewind.
    #[serde(default)]
    pub rebuild_indexes_for: Vec<String>,
}

/// Reads and writes snapshot manifests + the `latest` pointer.
///
/// A primary manager namespaces under `catalog/`; a fork-scoped manager
/// (`new_for_fork`) namespaces under `catalog/forks/{fork_id}/` so a fork's
/// flush never overwrites the primary's global `catalog/latest` pointer or
/// version/WAL high-water-marks (review C1). The fork namespace mirrors the
/// existing per-fork `catalog/forks/{fork_id}/id_allocator.json`.
pub struct SnapshotManager {
    store: Arc<dyn ObjectStore>,
    /// `Some` for a fork-scoped manager; `None` for the primary.
    fork_id: Option<ForkId>,
    /// The store's ID allocator, whose reserved ranges every saved manifest
    /// records (`next_vid` / `next_eid`). Set once the allocator exists.
    id_allocator: std::sync::OnceLock<Arc<crate::runtime::id_allocator::IdAllocator>>,
    /// Checked before every publish; a lost compare-and-swap trips it.
    fence: parking_lot::RwLock<Option<Arc<crate::lease::WriteFence>>>,
    /// The `latest` pointer as this manager last read or wrote it, for
    /// compare-and-swap.
    latest_version: parking_lot::Mutex<Expected>,
    /// Serializes this manager's compare-and-swap publishes, so two of its
    /// own concurrent writes never race each other into a false conflict.
    publish: tokio::sync::Mutex<()>,
    /// The store's read cut: every manifest saved names the versions it
    /// holds, and moving `latest` to a manifest publishes them. Unset for a
    /// fork, whose manifests name no tables.
    read_cut: std::sync::OnceLock<Arc<crate::storage::read_cut::ReadCut>>,
    /// The cut each manifest saved by this manager named, until `latest`
    /// moves to it.
    /// Each entry also names the published manifest it was stamped on.
    stamped: parking_lot::Mutex<HashMap<String, Stamp>>,
}

/// The published manifest a cut was stamped on, and the cut.
type Stamp = (Option<String>, std::collections::BTreeMap<String, u64>);

/// What a compare-and-swap write expects to replace.
#[derive(Debug, Clone, Default)]
enum Expected {
    /// Never read: overwrite (a manager that has not loaded the object).
    #[default]
    Unknown,
    /// Read, and absent: create only.
    Absent,
    /// Read at this version: replace only that.
    Version(object_store::UpdateVersion),
}

impl SnapshotManager {
    /// Construct the primary (global-namespace) snapshot manager.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            fork_id: None,
            id_allocator: std::sync::OnceLock::new(),
            fence: parking_lot::RwLock::new(None),
            latest_version: parking_lot::Mutex::new(Expected::Unknown),
            publish: tokio::sync::Mutex::new(()),
            read_cut: std::sync::OnceLock::new(),
            stamped: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Records `allocator` as the store's ID allocator, so every manifest
    /// saved from now on carries the end of its reserved ranges. An open
    /// whose allocator file is gone restarts above them. First call wins.
    pub fn set_id_allocator(&self, allocator: Arc<crate::runtime::id_allocator::IdAllocator>) {
        let _ = self.id_allocator.set(allocator);
    }

    /// A manifest stamped on a published cut that another publish has since
    /// replaced (a bulk load's manifest, saved before a flush published) is
    /// stamped again before `latest` moves to it, so it does not take back
    /// the newer versions.
    async fn restamp_if_stale(&self, snapshot_id: &str) -> Result<()> {
        let Some(cut) = self.recording_cut() else {
            return Ok(());
        };
        let current = cut.published().map(|p| p.snapshot_id);
        let stale = self
            .stamped
            .lock()
            .get(snapshot_id)
            .is_some_and(|(base, _)| *base != current);
        if stale {
            let manifest = self.load_snapshot(snapshot_id).await?;
            self.save_snapshot(&manifest).await?;
        }
        Ok(())
    }

    /// Records the store's read cut; see the field.
    pub fn set_read_cut(&self, cut: Arc<crate::storage::read_cut::ReadCut>) {
        let _ = self.read_cut.set(cut);
    }

    /// The read cut, when this handle records one.
    fn recording_cut(&self) -> Option<&Arc<crate::storage::read_cut::ReadCut>> {
        self.read_cut.get().filter(|cut| cut.is_recording())
    }

    /// The fence every publish checks; a lost compare-and-swap trips it.
    pub fn set_write_fence(&self, fence: Arc<crate::lease::WriteFence>) {
        *self.fence.write() = Some(fence);
    }

    fn check_fence(&self) -> Result<()> {
        match self.fence.read().as_ref() {
            Some(fence) => fence.check(),
            None => Ok(()),
        }
    }

    /// Writes `bytes` to `path` only if it still is what `expected` says;
    /// returns the new version. A store without conditional writes (a local
    /// filesystem, guarded by its OS lock instead) is written plainly. A lost
    /// swap trips the fence: another writer is active.
    async fn put_expecting(
        &self,
        path: &ObjectStorePath,
        bytes: Bytes,
        expected: Expected,
    ) -> Result<Expected> {
        let mode = match &expected {
            Expected::Unknown => object_store::PutMode::Overwrite,
            Expected::Absent => object_store::PutMode::Create,
            Expected::Version(v) => object_store::PutMode::Update(v.clone()),
        };
        match put_opts_with_timeout(
            &self.store,
            path,
            bytes.clone(),
            mode.into(),
            DEFAULT_TIMEOUT,
        )
        .await
        {
            Ok(result) => Ok(Expected::Version(object_store::UpdateVersion {
                e_tag: result.e_tag,
                version: result.version,
            })),
            Err(e) => match e.downcast_ref::<object_store::Error>() {
                Some(
                    object_store::Error::NotImplemented { .. }
                    | object_store::Error::NotSupported { .. },
                ) => {
                    put_with_timeout(&self.store, path, bytes, DEFAULT_TIMEOUT).await?;
                    Ok(Expected::Unknown)
                }
                Some(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    // This handle's own earlier attempt may have landed with its
                    // response lost (a retried request then sees itself).
                    if let Ok(current) = self.store.get(path).await {
                        let version = object_store::UpdateVersion {
                            e_tag: current.meta.e_tag.clone(),
                            version: current.meta.version.clone(),
                        };
                        if current.bytes().await.is_ok_and(|held| held == bytes) {
                            return Ok(Expected::Version(version));
                        }
                    }
                    let reason = format!("{path} was changed by another writer");
                    if let Some(fence) = self.fence.read().as_ref() {
                        fence.trip(reason.clone());
                    }
                    anyhow::bail!("{reason}; this handle has stopped writing")
                }
                _ => Err(e),
            },
        }
    }

    /// Construct a fork-scoped snapshot manager.
    ///
    /// All catalog paths are namespaced under `catalog/forks/{fork_id}/`, so a
    /// fork's flush publishes its manifest and `latest` pointer in isolation and
    /// never touches the primary's global catalog (review C1).
    pub fn new_for_fork(store: Arc<dyn ObjectStore>, fork_id: ForkId) -> Self {
        Self {
            store,
            fork_id: Some(fork_id),
            id_allocator: std::sync::OnceLock::new(),
            fence: parking_lot::RwLock::new(None),
            latest_version: parking_lot::Mutex::new(Expected::Unknown),
            publish: tokio::sync::Mutex::new(()),
            read_cut: std::sync::OnceLock::new(),
            stamped: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Whether this manager is fork-scoped (publishes under
    /// `catalog/forks/{fork_id}/` rather than the global `catalog/`).
    pub fn is_fork_scoped(&self) -> bool {
        self.fork_id.is_some()
    }

    /// Catalog namespace prefix: `catalog` for primary, `catalog/forks/{id}`
    /// for a fork-scoped manager.
    fn catalog_prefix(&self) -> String {
        match &self.fork_id {
            Some(id) => format!("catalog/forks/{id}"),
            None => "catalog".to_string(),
        }
    }

    fn manifest_path(&self, snapshot_id: &str) -> ObjectStorePath {
        ObjectStorePath::from(format!(
            "{}/manifests/{}.json",
            self.catalog_prefix(),
            snapshot_id
        ))
    }

    fn latest_ptr_path(&self) -> ObjectStorePath {
        ObjectStorePath::from(format!("{}/latest", self.catalog_prefix()))
    }

    fn named_snapshots_path(&self) -> ObjectStorePath {
        ObjectStorePath::from(format!("{}/named_snapshots.json", self.catalog_prefix()))
    }

    #[instrument(skip(self, manifest), fields(snapshot_id = %manifest.snapshot_id, size_bytes), level = "info")]
    pub async fn save_snapshot(&self, manifest: &SnapshotManifest) -> Result<()> {
        self.check_fence()?;
        let path = self.manifest_path(&manifest.snapshot_id);
        // Every manifest this binary writes records its writer features, also
        // one cloned from a manifest an older binary wrote: an open that finds
        // a latest manifest without them knows an older binary flushed last.
        //
        // It also records the end of the ID allocator's reserved ranges, never
        // lower than what the manifest already carries.
        let marks = self.id_allocator.get().map(|a| a.reserved_hwm());
        let needs_marks = marks.is_some_and(|(v, e)| {
            manifest.next_vid.is_none_or(|m| m < v) || manifest.next_eid.is_none_or(|m| m < e)
        });
        // And the read cut: the versions of every table this handle holds.
        let cut = self
            .recording_cut()
            .map(|cut| cut.stamp(&manifest.snapshot_id));
        let json = if manifest.writer_features.as_slice()
            == uni_common::core::schema::WRITER_FEATURES
            && !needs_marks
            && cut.is_none()
        {
            serde_json::to_string_pretty(manifest)?
        } else {
            let mut stamped = manifest.clone();
            if let Some(tables) = &cut {
                stamped.tables = tables.clone();
            }
            stamped.writer_features = uni_common::core::schema::current_writer_features();
            if let Some((v, e)) = marks {
                stamped.next_vid = Some(stamped.next_vid.unwrap_or(0).max(v));
                stamped.next_eid = Some(stamped.next_eid.unwrap_or(0).max(e));
            }
            serde_json::to_string_pretty(&stamped)?
        };
        tracing::Span::current().record("size_bytes", json.len());
        put_with_timeout(&self.store, &path, Bytes::from(json), DEFAULT_TIMEOUT).await?;
        if let Some(tables) = cut {
            let base = self
                .read_cut
                .get()
                .and_then(|c| c.published())
                .map(|p| p.snapshot_id);
            let mut stamped = self.stamped.lock();
            // Kept only until `latest` moves to them; a manifest that never
            // becomes latest must not stay forever.
            if stamped.len() >= 64 {
                stamped.clear();
            }
            stamped.insert(manifest.snapshot_id.clone(), (base, tables));
        }
        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn load_snapshot(&self, snapshot_id: &str) -> Result<SnapshotManifest> {
        // Try this manager's own namespace first.
        match self
            .load_snapshot_at(&self.manifest_path(snapshot_id))
            .await
        {
            Ok(m) => Ok(m),
            Err(e) if self.fork_id.is_some() => {
                // A fork INHERITS the primary's snapshots: a pin / time-travel
                // can reference a primary-created manifest (e.g. `create_snapshot`
                // writes to the global namespace). Fall back to it. Fork *writes*
                // stay fork-scoped, so this read fallback does not weaken C1.
                let primary_path =
                    ObjectStorePath::from(format!("catalog/manifests/{snapshot_id}.json"));
                self.load_snapshot_at(&primary_path).await.map_err(|_| e)
            }
            Err(e) => Err(e),
        }
    }

    async fn load_snapshot_at(&self, path: &ObjectStorePath) -> Result<SnapshotManifest> {
        let result = get_with_timeout(&self.store, path, DEFAULT_TIMEOUT).await?;
        let bytes = result.bytes().await?;
        let content = String::from_utf8(bytes.to_vec())?;
        let manifest: SnapshotManifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }

    pub async fn list_snapshots(&self) -> Result<Vec<String>> {
        let prefix = ObjectStorePath::from(format!("{}/manifests", self.catalog_prefix()));
        let metas = list_with_timeout(&self.store, Some(&prefix), DEFAULT_TIMEOUT).await?;
        let mut ids = Vec::new();

        for meta in metas {
            if let Some(filename) = meta.location.filename()
                && filename.ends_with(".json")
            {
                ids.push(filename.trim_end_matches(".json").to_string());
            }
        }
        Ok(ids)
    }

    /// Check if any snapshot manifests exist (for detecting database with lost manifest pointer).
    pub async fn has_any_manifests(&self) -> Result<bool> {
        let ids = self.list_snapshots().await?;
        Ok(!ids.is_empty())
    }

    pub async fn load_latest_snapshot(&self) -> Result<Option<SnapshotManifest>> {
        let latest_path = self.latest_ptr_path();
        match get_with_timeout(&self.store, &latest_path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                // The first read sets the version this manager swaps from;
                // later reads must not adopt another writer's pointer.
                {
                    let mut held = self.latest_version.lock();
                    if matches!(*held, Expected::Unknown) {
                        *held = Expected::Version(object_store::UpdateVersion {
                            e_tag: result.meta.e_tag.clone(),
                            version: result.meta.version.clone(),
                        });
                    }
                }
                let bytes = result.bytes().await.map_err(anyhow::Error::from)?;
                let snapshot_id = String::from_utf8(bytes.to_vec())?;
                let snapshot_id = snapshot_id.trim();
                if snapshot_id.is_empty() {
                    return Ok(None);
                }
                Ok(Some(self.load_snapshot(snapshot_id).await?))
            }
            Err(e) if is_not_found(&e) => {
                let mut held = self.latest_version.lock();
                if matches!(*held, Expected::Unknown) {
                    *held = Expected::Absent;
                }
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self), level = "info")]
    pub async fn set_latest_snapshot(&self, snapshot_id: &str) -> Result<()> {
        self.check_fence()?;
        let _publishing = self.publish.lock().await;
        self.set_latest_locked(snapshot_id).await
    }

    /// Publishes this handle's loose commits (see
    /// [`crate::storage::read_cut`]) in a successor of the latest manifest:
    /// its contents with a new id, the current one as its parent. Returns
    /// the successor's id; `None` when no manifest exists yet, so the next
    /// flush publishes them.
    ///
    /// Holds the pointer's lock from reading `latest` to moving it, so a
    /// flush publishing in between cannot be replaced by a successor of the
    /// manifest before it.
    ///
    /// # Errors
    /// A read or write failed, or the fence is tripped.
    pub async fn publish_successor(&self) -> Result<Option<String>> {
        self.check_fence()?;
        let _publishing = self.publish.lock().await;
        let Some(mut manifest) = self.load_latest_snapshot().await? else {
            return Ok(None);
        };
        manifest.parent_snapshot = Some(manifest.snapshot_id.clone());
        manifest.snapshot_id = uuid::Uuid::new_v4().to_string();
        manifest.name = None;
        manifest.created_at = chrono::Utc::now();
        self.save_snapshot(&manifest).await?;
        self.set_latest_locked(&manifest.snapshot_id).await?;
        Ok(Some(manifest.snapshot_id))
    }

    async fn set_latest_locked(&self, snapshot_id: &str) -> Result<()> {
        self.restamp_if_stale(snapshot_id).await?;
        let path = self.latest_ptr_path();
        // A manager that never read the pointer learns where it stands first.
        let expected = {
            let held = self.latest_version.lock().clone();
            match held {
                Expected::Unknown => match self.store.head(&path).await {
                    Ok(meta) => Expected::Version(object_store::UpdateVersion {
                        e_tag: meta.e_tag,
                        version: meta.version,
                    }),
                    Err(object_store::Error::NotFound { .. }) => Expected::Absent,
                    Err(e) => return Err(e.into()),
                },
                known => known,
            }
        };
        let written = self
            .put_expecting(&path, Bytes::from(snapshot_id.to_string()), expected)
            .await?;
        *self.latest_version.lock() = written;
        // Reads now see what the manifest names.
        if let Some(cut) = self.read_cut.get() {
            let stamped = self.stamped.lock().remove(snapshot_id);
            let tables = match stamped {
                Some((_, tables)) => tables,
                None => self.load_snapshot(snapshot_id).await?.tables,
            };
            cut.publish(snapshot_id, tables);
        }
        Ok(())
    }

    pub async fn load_named_snapshots(&self) -> Result<HashMap<String, String>> {
        let path = self.named_snapshots_path();
        match get_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8(bytes.to_vec())?;
                Ok(serde_json::from_str(&content)?)
            }
            // Only a genuine NotFound (no named-snapshots file yet) yields an
            // empty map. A transient/IO error must propagate: `save_named_snapshot`
            // does a read-modify-write over this map, so collapsing it to `{}`
            // here would persist only the new entry and wipe every existing
            // named snapshot.
            Err(e) if is_not_found(&e) => Ok(HashMap::new()),
            Err(e) => Err(e),
        }
    }

    /// The named-snapshot map and the version it was read at.
    async fn load_named_snapshots_versioned(&self) -> Result<(HashMap<String, String>, Expected)> {
        let path = self.named_snapshots_path();
        match get_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let version = Expected::Version(object_store::UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let bytes = result.bytes().await?;
                Ok((serde_json::from_slice(&bytes)?, version))
            }
            Err(e) if is_not_found(&e) => Ok((HashMap::new(), Expected::Absent)),
            Err(e) => Err(e),
        }
    }

    pub async fn save_named_snapshot(&self, name: &str, snapshot_id: &str) -> Result<()> {
        self.check_fence()?;
        let _publishing = self.publish.lock().await;
        let (mut map, expected) = self.load_named_snapshots_versioned().await?;
        map.insert(name.to_string(), snapshot_id.to_string());
        let json = serde_json::to_string_pretty(&map)?;
        self.put_expecting(&self.named_snapshots_path(), Bytes::from(json), expected)
            .await?;
        Ok(())
    }

    pub async fn get_named_snapshot(&self, name: &str) -> Result<Option<String>> {
        let map = self.load_named_snapshots().await?;
        Ok(map.get(name).cloned())
    }

    fn restore_point_path(&self, snapshot_id: &str) -> ObjectStorePath {
        ObjectStorePath::from(format!(
            "{}/restore_points/{}.json",
            self.catalog_prefix(),
            snapshot_id
        ))
    }

    fn compaction_horizon_path(&self) -> ObjectStorePath {
        ObjectStorePath::from(format!("{}/compaction_horizon.json", self.catalog_prefix()))
    }

    /// The version below which semantic compaction has merged history away,
    /// as recorded by earlier passes; 0 when no pass recorded one.
    ///
    /// # Errors
    ///
    /// When the object store read fails for a reason other than absence, or
    /// the record does not parse.
    pub async fn load_compaction_horizon(&self) -> Result<u64> {
        Ok(self
            .load_json::<u64>(&self.compaction_horizon_path())
            .await?
            .unwrap_or(0))
    }

    /// Record the compaction horizon (see [`Self::load_compaction_horizon`]).
    ///
    /// # Errors
    ///
    /// When the object store write fails.
    pub async fn save_compaction_horizon(&self, version: u64) -> Result<()> {
        put_with_timeout(
            &self.store,
            &self.compaction_horizon_path(),
            Bytes::from(serde_json::to_string(&version)?),
            DEFAULT_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    fn restore_intent_path(&self) -> ObjectStorePath {
        ObjectStorePath::from(format!("{}/restore_intent.json", self.catalog_prefix()))
    }

    /// Persist the [`RestorePoint`] of an explicit snapshot.
    ///
    /// # Errors
    ///
    /// When the object store write fails.
    pub async fn save_restore_point(&self, point: &RestorePoint) -> Result<()> {
        let json = serde_json::to_string_pretty(point)?;
        put_with_timeout(
            &self.store,
            &self.restore_point_path(&point.snapshot_id),
            Bytes::from(json),
            DEFAULT_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    /// The [`RestorePoint`] of `snapshot_id`, or `None` when the snapshot has
    /// none: it was published by an ordinary flush, or taken by a version that
    /// did not record one.
    ///
    /// # Errors
    ///
    /// When the object store read fails for a reason other than absence, or
    /// the record does not parse.
    pub async fn load_restore_point(&self, snapshot_id: &str) -> Result<Option<RestorePoint>> {
        self.load_json(&self.restore_point_path(snapshot_id)).await
    }

    /// Remove the [`RestorePoint`] of `snapshot_id`; absent is not an error.
    ///
    /// # Errors
    ///
    /// When the object store delete fails for a reason other than absence.
    pub async fn delete_restore_point(&self, snapshot_id: &str) -> Result<()> {
        self.delete_if_present(&self.restore_point_path(snapshot_id))
            .await
    }

    /// Remove the manifest of `snapshot_id`; absent is not an error.
    ///
    /// The caller must not remove the manifest `latest` names.
    ///
    /// # Errors
    ///
    /// When the object store delete fails for a reason other than absence.
    pub async fn delete_snapshot_manifest(&self, snapshot_id: &str) -> Result<()> {
        self.delete_if_present(&self.manifest_path(snapshot_id))
            .await
    }

    /// Whether this manager's namespace holds a manifest for `snapshot_id`.
    ///
    /// # Errors
    ///
    /// When the object store read fails for a reason other than absence.
    pub async fn snapshot_exists(&self, snapshot_id: &str) -> Result<bool> {
        match self.store.head(&self.manifest_path(snapshot_id)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// The snapshot id `catalog/latest` names, if any.
    ///
    /// # Errors
    ///
    /// When the object store read fails for a reason other than absence.
    pub async fn latest_snapshot_id(&self) -> Result<Option<String>> {
        match get_with_timeout(&self.store, &self.latest_ptr_path(), DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(anyhow::Error::from)?;
                let id = String::from_utf8(bytes.to_vec())?.trim().to_string();
                Ok((!id.is_empty()).then_some(id))
            }
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Remove every name that refers to `snapshot_id`; returns whether any
    /// did.
    ///
    /// # Errors
    ///
    /// When the names cannot be read or written back.
    pub async fn remove_snapshot_names(&self, snapshot_id: &str) -> Result<bool> {
        self.check_fence()?;
        let _publishing = self.publish.lock().await;
        let (mut map, expected) = self.load_named_snapshots_versioned().await?;
        let before = map.len();
        map.retain(|_, id| id != snapshot_id);
        if map.len() == before {
            return Ok(false);
        }
        let json = serde_json::to_string_pretty(&map)?;
        self.put_expecting(&self.named_snapshots_path(), Bytes::from(json), expected)
            .await?;
        Ok(true)
    }

    async fn delete_if_present(&self, path: &ObjectStorePath) -> Result<()> {
        match self.store.delete(path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Record that a restore is under way, before it touches any table.
    ///
    /// # Errors
    ///
    /// When the object store write fails.
    pub async fn save_restore_intent(&self, intent: &RestoreIntent) -> Result<()> {
        let json = serde_json::to_string_pretty(intent)?;
        put_with_timeout(
            &self.store,
            &self.restore_intent_path(),
            Bytes::from(json),
            DEFAULT_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    /// The restore a crash interrupted, if any.
    ///
    /// # Errors
    ///
    /// When the object store read fails for a reason other than absence, or
    /// the record does not parse.
    pub async fn load_restore_intent(&self) -> Result<Option<RestoreIntent>> {
        self.load_json(&self.restore_intent_path()).await
    }

    /// Remove the restore intent once the restore is complete.
    ///
    /// # Errors
    ///
    /// When the object store delete fails for a reason other than absence.
    pub async fn clear_restore_intent(&self) -> Result<()> {
        match self.store.delete(&self.restore_intent_path()).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn load_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &ObjectStorePath,
    ) -> Result<Option<T>> {
        match get_with_timeout(&self.store, path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(serde_json::from_slice(&bytes)?))
            }
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Find the most recent snapshot created at or before the given timestamp.
    pub async fn find_snapshot_at_time(
        &self,
        target: DateTime<Utc>,
    ) -> Result<Option<SnapshotManifest>> {
        let ids = self.list_snapshots().await?;
        let mut best: Option<SnapshotManifest> = None;

        for id in ids {
            // Fail closed: propagate a load error for a listed snapshot rather
            // than silently skipping it. Swallowing the error (the old `if let
            // Ok(m)`) let a corrupt/unreadable newer manifest fall through to an
            // older snapshot, answering a time-travel query from the wrong point
            // in time with no signal (review #3c).
            let m = self.load_snapshot(&id).await?;
            if m.created_at <= target && best.as_ref().is_none_or(|b| m.created_at > b.created_at) {
                best = Some(m);
            }
        }
        Ok(best)
    }
}

/// Make a just-published snapshot durable on local-filesystem stores by
/// fsync'ing the manifest body and the `catalog/latest` pointer (and their
/// parent directories) BEFORE the WAL — the only other durable copy of this
/// flush's data — is truncated (review C4).
///
/// `save_snapshot` / `set_latest_snapshot` write through the object store,
/// which does NOT fsync. Without this barrier a crash after WAL truncation but
/// before the OS flushed those writes would lose the snapshot: recovery could
/// not resolve `catalog/latest`.
///
/// A no-op when `local_root` is `None` (remote/object stores), which provide
/// their own durability on `put`. The two artifacts are fsync'd body-first then
/// pointer, matching the publish order, so a crash mid-barrier never leaves
/// `latest` pointing at a non-durable manifest. Paths mirror the private
/// `SnapshotManager::manifest_path` / `SnapshotManager::latest_ptr_path` helpers.
pub fn fsync_snapshot_pointer(
    local_root: Option<&std::path::Path>,
    fork_id: Option<&ForkId>,
    snapshot_id: &str,
) -> std::io::Result<()> {
    let Some(root) = local_root else {
        return Ok(());
    };
    // Mirror `SnapshotManager::catalog_prefix`: forks live under
    // `catalog/forks/{fork_id}/` so the barrier fsyncs the fork's own
    // manifest + pointer, not the (nonexistent) global paths (review C1).
    let prefix = match fork_id {
        Some(id) => root.join("catalog").join("forks").join(id.to_string()),
        None => root.join("catalog"),
    };
    let manifest = prefix.join("manifests").join(format!("{snapshot_id}.json"));
    let latest = prefix.join("latest");
    crate::runtime::wal::sync_file_and_parent(&manifest)?;
    crate::runtime::wal::sync_file_and_parent(&latest)?;
    Ok(())
}

/// Make primary catalog files durable on a local-filesystem store, as
/// [`fsync_snapshot_pointer`] does for a flush's manifest and pointer.
///
/// `files` are paths relative to `catalog/`. A no-op when `local_root` is
/// `None` (remote stores provide their own durability on `put`).
///
/// # Errors
///
/// When a file or its directory cannot be synced.
pub fn fsync_catalog_files(
    local_root: Option<&std::path::Path>,
    files: &[String],
) -> std::io::Result<()> {
    let Some(root) = local_root else {
        return Ok(());
    };
    for file in files {
        crate::runtime::wal::sync_file_and_parent(&root.join("catalog").join(file))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `latest` pointer is compare-and-swapped: a manager whose view of it
    /// is stale cannot move it, and its fence trips.
    #[tokio::test]
    async fn a_stale_latest_pointer_is_not_overwritten() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let a = SnapshotManager::new(store.clone());
        let fence = crate::lease::WriteFence::new();
        a.set_write_fence(Arc::clone(&fence));
        a.set_latest_snapshot("s1").await.unwrap();
        // Another writer moves it.
        let b = SnapshotManager::new(store.clone());
        assert!(b.load_latest_snapshot().await.is_err()); // s1 has no manifest
        b.set_latest_snapshot("s2").await.unwrap();
        let err = a
            .set_latest_snapshot("s3")
            .await
            .expect_err("a stale writer moved latest");
        assert!(format!("{err:#}").contains("another writer"), "{err:#}");
        assert!(fence.is_tripped());
        let held = store
            .get(&ObjectStorePath::from("catalog/latest"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&held[..], b"s2");
        // A fenced manager refuses to publish at all.
        assert!(a.set_latest_snapshot("s4").await.is_err());
    }

    /// Concurrent publishes through one manager are this handle's own writes,
    /// not another writer's: none of them trips the fence.
    #[tokio::test]
    async fn concurrent_publishes_by_one_manager_do_not_fence_it() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let m = Arc::new(SnapshotManager::new(store.clone()));
        let fence = crate::lease::WriteFence::new();
        m.set_write_fence(Arc::clone(&fence));
        assert!(m.load_latest_snapshot().await.unwrap().is_none());
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let m = Arc::clone(&m);
                tokio::spawn(async move {
                    m.set_latest_snapshot(&format!("s{i}")).await?;
                    m.save_named_snapshot(&format!("n{i}"), &format!("s{i}"))
                        .await
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert!(!fence.is_tripped(), "{:?}", fence.reason_if_tripped());
        assert_eq!(m.load_named_snapshots().await.unwrap().len(), 8);
    }

    /// The pointer a manager read at open is the one it swaps from.
    #[tokio::test]
    async fn a_manager_moves_the_pointer_it_read() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let first = SnapshotManager::new(store.clone());
        let manifest = SnapshotManifest::new("s1".into(), 1);
        first.save_snapshot(&manifest).await.unwrap();
        first.set_latest_snapshot("s1").await.unwrap();
        let reopened = SnapshotManager::new(store.clone());
        assert!(reopened.load_latest_snapshot().await.unwrap().is_some());
        reopened.set_latest_snapshot("s2").await.unwrap();
        reopened.set_latest_snapshot("s3").await.unwrap();
    }

    /// Remote/object stores have no local root — the barrier is a no-op and
    /// must never error (durability is the backend's responsibility there).
    #[test]
    fn test_fsync_snapshot_pointer_noop_for_remote() {
        assert!(fsync_snapshot_pointer(None, None, "snap-1").is_ok());
    }

    /// On a local filesystem the manifest body and `catalog/latest` pointer are
    /// fsync'd in place (review C4).
    #[test]
    fn test_fsync_snapshot_pointer_syncs_local_artifacts() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        let manifests = root.join("catalog").join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::write(manifests.join("snap-1.json"), b"{}").unwrap();
        std::fs::write(root.join("catalog").join("latest"), b"snap-1").unwrap();

        fsync_snapshot_pointer(Some(root), None, "snap-1").unwrap();
    }
}
