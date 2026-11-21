// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! ID allocation for vertices and edges using pure auto-increment counters.
//!
//! VIDs and EIDs are simple auto-incrementing u64 values. Unlike the previous
//! design, they no longer embed label/type information - that's now handled
//! by the VidLabelsIndex and edge tables.

use crate::store_utils::{DEFAULT_TIMEOUT, get_with_timeout, put_with_timeout};
use anyhow::Result;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use uni_common::core::id::{Eid, Vid};

/// Persisted counter manifest - stores the reserved counter ranges.
#[derive(Serialize, Deserialize, Default, Clone)]
struct CounterManifest {
    /// Next VID value that needs to be reserved (end of current batch)
    next_vid_batch: u64,
    /// Next EID value that needs to be reserved (end of current batch)
    next_eid_batch: u64,
}

/// Internal allocator state - tracks current position within reserved batch.
struct AllocatorState {
    manifest: CounterManifest,
    /// ETag and version of the persisted file, for conditional writes. GCS
    /// needs the version (generation); S3 and Azure the ETag.
    manifest_version: Option<UpdateVersion>,
    current_vid: u64,
    current_eid: u64,
}

/// Allocates globally unique VIDs and EIDs using auto-increment counters.
///
/// This allocator uses batch reservation to minimize object store writes:
/// - Reserves a batch of IDs (e.g., 1000) from the object store
/// - Allocates from the local batch until exhausted
/// - Reserves a new batch when needed
pub struct IdAllocator {
    store: Arc<dyn ObjectStore>,
    path: Path,
    state: Mutex<AllocatorState>,
    batch_size: u64,
    allow_unconditional: bool,
    /// Mirrors of the persisted batch ends, readable without the lock.
    reserved_vid: AtomicU64,
    reserved_eid: AtomicU64,
}

/// How [`IdAllocator::open`] starts.
#[derive(Debug, Clone, Copy)]
pub struct AllocatorOptions {
    /// `(vid, eid)` the allocator never starts below, whatever its file says.
    pub floor: (u64, u64),
    /// Whether a store that cannot do conditional writes may be written
    /// unconditionally. Only safe where nothing else writes the file, such as
    /// a local directory.
    pub allow_unconditional: bool,
}

impl Default for AllocatorOptions {
    fn default() -> Self {
        Self {
            floor: (0, 0),
            allow_unconditional: true,
        }
    }
}

impl IdAllocator {
    /// Creates a new ID allocator, loading existing state from object store.
    pub async fn new(store: Arc<dyn ObjectStore>, path: Path, batch_size: u64) -> Result<Self> {
        Self::open(store, path, batch_size, AllocatorOptions::default()).await
    }

    /// Opens the allocator with explicit [`AllocatorOptions`].
    ///
    /// # Errors
    /// The counter file exists but cannot be read.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        path: Path,
        batch_size: u64,
        options: AllocatorOptions,
    ) -> Result<Self> {
        let (manifest, version) = load_manifest(&store, &path).await?;

        // Start allocating from where the last batch ended, or the floor.
        let current_vid = manifest.next_vid_batch.max(options.floor.0);
        let current_eid = manifest.next_eid_batch.max(options.floor.1);

        Ok(Self {
            reserved_vid: AtomicU64::new(manifest.next_vid_batch.max(options.floor.0)),
            reserved_eid: AtomicU64::new(manifest.next_eid_batch.max(options.floor.1)),
            store,
            path,
            state: Mutex::new(AllocatorState {
                manifest,
                manifest_version: version,
                current_vid,
                current_eid,
            }),
            batch_size,
            allow_unconditional: options.allow_unconditional,
        })
    }

    /// Moves the allocator's cursors up to at least `(vid, eid)`.
    pub async fn raise_floor(&self, vid: u64, eid: u64) {
        let mut state = self.state.lock().await;
        state.current_vid = state.current_vid.max(vid);
        state.current_eid = state.current_eid.max(eid);
        // The marks a manifest records must cover the floor too: the counter
        // file may not be rewritten before the next manifest is saved.
        self.reserved_vid.fetch_max(vid, Ordering::AcqRel);
        self.reserved_eid.fetch_max(eid, Ordering::AcqRel);
    }

    /// End of the reserved VID and EID batches: every id this allocator has
    /// handed out, or can hand out without another reservation, is below it.
    #[must_use]
    pub fn reserved_hwm(&self) -> (u64, u64) {
        (
            self.reserved_vid.load(Ordering::Acquire),
            self.reserved_eid.load(Ordering::Acquire),
        )
    }

    /// Allocates a new VID.
    ///
    /// Returns a globally unique, auto-incrementing vertex ID.
    pub async fn allocate_vid(&self) -> Result<Vid> {
        let mut state = self.state.lock().await;

        // Check if we've exhausted our current batch
        if state.current_vid >= state.manifest.next_vid_batch {
            // Reserve a new batch. `checked_add` guards against u64
            // exhaustion (defense-in-depth; ~1.8e19 ids — physically
            // unreachable, but wrapping silently would be a correctness
            // disaster). L13.
            let reserved = state
                .current_vid
                .checked_add(self.batch_size)
                .ok_or_else(|| anyhow::anyhow!("VID space exhausted"))?;
            let prev = state.manifest.next_vid_batch;
            state.manifest.next_vid_batch = reserved;
            if let Err(e) = self.persist_manifest(&mut state).await {
                // Roll back the in-memory advance so a retry re-attempts the
                // DURABLE reservation. Leaving it advanced would let a retry skip
                // the reservation and hand out ids from a batch a crash would lose
                // (the on-disk manifest still points at `prev`) — risking id reuse.
                state.manifest.next_vid_batch = prev;
                return Err(e);
            }
        }

        let vid = Vid::new(state.current_vid);
        state.current_vid += 1;
        Ok(vid)
    }

    /// Allocates multiple VIDs at once.
    pub async fn allocate_vids(&self, count: usize) -> Result<Vec<Vid>> {
        let mut state = self.state.lock().await;
        let needed = count as u64;

        // Check if we need to expand our batch (L13: checked).
        let want = state
            .current_vid
            .checked_add(needed)
            .ok_or_else(|| anyhow::anyhow!("VID space exhausted"))?;
        if want > state.manifest.next_vid_batch {
            // Reserve enough for the request plus a full batch
            let reserved = want
                .checked_add(self.batch_size)
                .ok_or_else(|| anyhow::anyhow!("VID space exhausted"))?;
            let prev = state.manifest.next_vid_batch;
            state.manifest.next_vid_batch = reserved;
            if let Err(e) = self.persist_manifest(&mut state).await {
                // Roll back so a retry re-attempts the durable reservation.
                state.manifest.next_vid_batch = prev;
                return Err(e);
            }
        }

        let vids: Vec<Vid> = (0..count)
            .map(|i| Vid::new(state.current_vid + i as u64))
            .collect();
        state.current_vid += needed;
        Ok(vids)
    }

    /// Allocates a new EID.
    ///
    /// Returns a globally unique, auto-incrementing edge ID.
    pub async fn allocate_eid(&self) -> Result<Eid> {
        let mut state = self.state.lock().await;

        // Check if we've exhausted our current batch (L13: checked).
        if state.current_eid >= state.manifest.next_eid_batch {
            // Reserve a new batch
            let reserved = state
                .current_eid
                .checked_add(self.batch_size)
                .ok_or_else(|| anyhow::anyhow!("EID space exhausted"))?;
            let prev = state.manifest.next_eid_batch;
            state.manifest.next_eid_batch = reserved;
            if let Err(e) = self.persist_manifest(&mut state).await {
                // Roll back so a retry re-attempts the durable reservation.
                state.manifest.next_eid_batch = prev;
                return Err(e);
            }
        }

        let eid = Eid::new(state.current_eid);
        state.current_eid += 1;
        Ok(eid)
    }

    /// Allocates multiple EIDs at once.
    pub async fn allocate_eids(&self, count: usize) -> Result<Vec<Eid>> {
        let mut state = self.state.lock().await;
        let needed = count as u64;

        // Check if we need to expand our batch (L13: checked).
        let want = state
            .current_eid
            .checked_add(needed)
            .ok_or_else(|| anyhow::anyhow!("EID space exhausted"))?;
        if want > state.manifest.next_eid_batch {
            // Reserve enough for the request plus a full batch
            let reserved = want
                .checked_add(self.batch_size)
                .ok_or_else(|| anyhow::anyhow!("EID space exhausted"))?;
            let prev = state.manifest.next_eid_batch;
            state.manifest.next_eid_batch = reserved;
            if let Err(e) = self.persist_manifest(&mut state).await {
                // Roll back so a retry re-attempts the durable reservation.
                state.manifest.next_eid_batch = prev;
                return Err(e);
            }
        }

        let eids: Vec<Eid> = (0..count)
            .map(|i| Eid::new(state.current_eid + i as u64))
            .collect();
        state.current_eid += needed;
        Ok(eids)
    }

    /// Returns the current VID counter value (next VID that would be allocated).
    pub async fn current_vid(&self) -> u64 {
        self.state.lock().await.current_vid
    }

    /// Returns the current EID counter value (next EID that would be allocated).
    pub async fn current_eid(&self) -> u64 {
        self.state.lock().await.current_eid
    }

    /// Snapshot the current high-water-marks for VID and EID.
    ///
    /// Returns `(next_vid, next_eid)` — the values the next
    /// allocations would produce if not constrained by batch
    /// reservation. Used by Phase 2 fork-creation to bootstrap a
    /// fork's allocator above primary's range without going through
    /// disk (the primary and fork allocators may live on different
    /// `ObjectStore` instances, making file-copy bootstrap fragile).
    pub async fn current_hwm(&self) -> (u64, u64) {
        let state = self.state.lock().await;
        (state.current_vid, state.current_eid)
    }

    /// Creates a throwaway in-memory `IdAllocator` that allocates ids *above*
    /// `(vid_hwm, eid_hwm)`, backed by an `InMemory` object store discarded with
    /// the allocator.
    ///
    /// This is the id source for an ephemeral / scratch transaction
    /// (`Session::scratch`): it hands out vids/eids that cannot collide with the
    /// primary's live rows (all `< hwm`, pinned in the scratch's read base), and
    /// it never touches the primary's durable `id_allocator.json` — so thousands
    /// of open-write-discard rollouts advance no global counter and do no catalog
    /// I/O (G8/E2).
    pub async fn in_memory_seeded(vid_hwm: u64, eid_hwm: u64, batch_size: u64) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("scratch_id_allocator.json");
        let manifest = CounterManifest {
            next_vid_batch: vid_hwm,
            next_eid_batch: eid_hwm,
        };
        let bytes = Bytes::from(serde_json::to_vec(&manifest)?);
        put_with_timeout(&store, &path, bytes, DEFAULT_TIMEOUT).await?;
        Self::new(store, path, batch_size).await
    }

    /// Force a checkpoint of the in-memory state to the underlying
    /// object store.
    ///
    /// Used by Phase 2 fork creation to bootstrap a fork's allocator
    /// from primary's *current* HWM. Without this, primary's allocator
    /// has an in-memory state that the on-disk manifest doesn't yet
    /// reflect (the disk file is only updated on batch-boundary
    /// crossings), and the fork would start at VID 0 — colliding with
    /// primary rows visible through the `base_paths` chain.
    ///
    /// Idempotent and safe to call frequently; the persisted manifest
    /// reflects the same state if nothing has changed.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`anyhow::Error`] from `persist_manifest`
    /// (object-store put failure).
    pub async fn checkpoint(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        // Advance the persisted batch HWM to at least the current
        // allocation cursor so reloads start above any allocated VIDs.
        if state.manifest.next_vid_batch < state.current_vid {
            state.manifest.next_vid_batch = state.current_vid;
        }
        if state.manifest.next_eid_batch < state.current_eid {
            state.manifest.next_eid_batch = state.current_eid;
        }
        self.persist_manifest(&mut state).await
    }

    /// Persists the counter manifest with a conditional write.
    ///
    /// The first write creates the file only if it is absent; later writes
    /// replace it only if it is unchanged since this allocator last read or
    /// wrote it. When another allocator created the file first, its batches
    /// are adopted: this allocator moves its cursors above them and retries,
    /// so the two never hand out the same id. A changed file afterwards is
    /// another live writer, and the reservation fails.
    ///
    /// A store without conditional writes is written unconditionally only
    /// when [`AllocatorOptions::allow_unconditional`] says nothing else
    /// writes the file (a local directory).
    async fn persist_manifest(&self, state: &mut AllocatorState) -> Result<()> {
        for _ in 0..3 {
            let bytes = Bytes::from(serde_json::to_vec_pretty(&state.manifest)?);
            let mode = match &state.manifest_version {
                Some(v) => PutMode::Update(v.clone()),
                None => PutMode::Create,
            };
            let opts: PutOptions = mode.into();
            let outcome = tokio::time::timeout(
                DEFAULT_TIMEOUT,
                self.store.put_opts(&self.path, bytes.clone().into(), opts),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Object store put_opts timed out after {:?}",
                    DEFAULT_TIMEOUT
                )
            })?;
            let result = match outcome {
                Ok(result) => result,
                Err(
                    e @ (object_store::Error::NotImplemented { .. }
                    | object_store::Error::NotSupported { .. }),
                ) => {
                    if !self.allow_unconditional {
                        return Err(anyhow::anyhow!(
                            "The object store holding {} does not support the conditional \
                             writes the ID allocator needs to keep ids unique ({e}). For an \
                             S3-compatible service, enable conditional puts (the \
                             `aws_conditional_put` storage option).",
                            self.path
                        ));
                    }
                    put_with_timeout(&self.store, &self.path, bytes, DEFAULT_TIMEOUT).await?
                }
                Err(object_store::Error::AlreadyExists { .. }) => {
                    // Another allocator created the file first: move above
                    // its reservations, keeping the span this one reserved.
                    let (theirs, version) = load_manifest(&self.store, &self.path).await?;
                    if version.is_none() {
                        anyhow::bail!("{} exists but could not be read back", self.path);
                    }
                    shift_above(
                        &mut state.current_vid,
                        &mut state.manifest.next_vid_batch,
                        theirs.next_vid_batch,
                    );
                    shift_above(
                        &mut state.current_eid,
                        &mut state.manifest.next_eid_batch,
                        theirs.next_eid_batch,
                    );
                    state.manifest_version = version;
                    continue;
                }
                Err(object_store::Error::Precondition { .. }) => {
                    // Either another writer changed the file, or this write
                    // landed and only its acknowledgement was lost (a retried
                    // request then sees its own write). Tell them apart by
                    // what the file now holds.
                    let current =
                        get_with_timeout(&self.store, &self.path, DEFAULT_TIMEOUT).await?;
                    let version = UpdateVersion {
                        e_tag: current.meta.e_tag.clone(),
                        version: current.meta.version.clone(),
                    };
                    let held = current.bytes().await?;
                    if held == bytes {
                        state.manifest_version = Some(version);
                        self.reserved_vid
                            .fetch_max(state.manifest.next_vid_batch, Ordering::AcqRel);
                        self.reserved_eid
                            .fetch_max(state.manifest.next_eid_batch, Ordering::AcqRel);
                        return Ok(());
                    }
                    anyhow::bail!(
                        "{} was changed by a concurrent writer: another process holds this \
                         store open for writing",
                        self.path
                    );
                }
                Err(e) => return Err(e.into()),
            };
            state.manifest_version = Some(UpdateVersion {
                e_tag: result.e_tag,
                version: result.version,
            });
            self.reserved_vid
                .fetch_max(state.manifest.next_vid_batch, Ordering::AcqRel);
            self.reserved_eid
                .fetch_max(state.manifest.next_eid_batch, Ordering::AcqRel);
            return Ok(());
        }
        anyhow::bail!(
            "Could not reserve ids in {}: it kept changing under concurrent writers",
            self.path
        )
    }
}

/// Moves a cursor and the end of its reserved batch above `floor`, keeping
/// the span between them.
fn shift_above(cursor: &mut u64, reserved: &mut u64, floor: u64) {
    if *cursor < floor {
        let span = reserved.saturating_sub(*cursor);
        *cursor = floor;
        *reserved = floor.saturating_add(span);
    }
}

/// Reads the counter file: its manifest and the version to update it from.
async fn load_manifest(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<(CounterManifest, Option<UpdateVersion>)> {
    match get_with_timeout(store, path, DEFAULT_TIMEOUT).await {
        Ok(get_result) => {
            let version = UpdateVersion {
                e_tag: get_result.meta.e_tag.clone(),
                version: get_result.meta.version.clone(),
            };
            let bytes = get_result.bytes().await?;
            let manifest: CounterManifest = serde_json::from_slice(&bytes)?;
            Ok((manifest, Some(version)))
        }
        // Typed, not substring: a transient failure whose message merely
        // reads like a missing object (proxy 404, wrapped S3 "bucket not
        // found") would otherwise start the allocator from a defaulted
        // manifest against a populated database and re-issue live VIDs.
        // Same rationale as `fork/registry.rs` and `snapshot/manager.rs`.
        Err(e) if crate::store_utils::is_not_found(&e) => Ok((CounterManifest::default(), None)),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_allocate_vid() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("id_counters.json");
        let allocator = IdAllocator::new(store, path, 100).await.unwrap();

        let vid1 = allocator.allocate_vid().await.unwrap();
        let vid2 = allocator.allocate_vid().await.unwrap();
        let vid3 = allocator.allocate_vid().await.unwrap();

        assert_eq!(vid1.as_u64(), 0);
        assert_eq!(vid2.as_u64(), 1);
        assert_eq!(vid3.as_u64(), 2);
    }

    #[tokio::test]
    async fn test_allocate_eid() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("id_counters.json");
        let allocator = IdAllocator::new(store, path, 100).await.unwrap();

        let eid1 = allocator.allocate_eid().await.unwrap();
        let eid2 = allocator.allocate_eid().await.unwrap();

        assert_eq!(eid1.as_u64(), 0);
        assert_eq!(eid2.as_u64(), 1);
    }

    #[tokio::test]
    async fn test_allocate_many() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("id_counters.json");
        let allocator = IdAllocator::new(store, path, 100).await.unwrap();

        let vids = allocator.allocate_vids(5).await.unwrap();
        assert_eq!(vids.len(), 5);
        for (i, vid) in vids.iter().enumerate() {
            assert_eq!(vid.as_u64(), i as u64);
        }

        // Next allocation should continue from 5
        let next = allocator.allocate_vid().await.unwrap();
        assert_eq!(next.as_u64(), 5);
    }

    #[tokio::test]
    async fn test_persistence() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("id_counters.json");

        // Allocate some IDs
        {
            let allocator = IdAllocator::new(store.clone(), path.clone(), 10)
                .await
                .unwrap();
            for _ in 0..15 {
                allocator.allocate_vid().await.unwrap();
            }
        }

        // Re-open and verify continuation
        {
            let allocator = IdAllocator::new(store, path, 10).await.unwrap();
            // After allocating 15 IDs with batch size 10, we reserved up to 20
            // So next allocation should be 20 (start of new batch after reload)
            let vid = allocator.allocate_vid().await.unwrap();
            assert_eq!(vid.as_u64(), 20);
        }
    }

    /// A store that refuses conditional writes, as a local filesystem's
    /// `PutMode::Update` does.
    #[derive(Debug)]
    struct NoConditional(InMemory);

    impl std::fmt::Display for NoConditional {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "NoConditional")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for NoConditional {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if !matches!(opts.mode, PutMode::Overwrite) {
                return Err(object_store::Error::NotImplemented {
                    operation: "conditional put".into(),
                    implementer: "NoConditional".into(),
                });
            }
            self.0.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.0.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn a_floor_raises_the_cursor_and_never_lowers_it() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("id.json");
        {
            let a = IdAllocator::new(store.clone(), path.clone(), 10)
                .await
                .unwrap();
            a.allocate_vid().await.unwrap();
        }
        // The file says 10; a floor of 500 wins.
        let a = IdAllocator::open(
            store.clone(),
            path.clone(),
            10,
            AllocatorOptions {
                floor: (500, 70),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(a.allocate_vid().await.unwrap().as_u64(), 500);
        assert_eq!(a.allocate_eid().await.unwrap().as_u64(), 70);
        drop(a);
        // A floor below the file is ignored.
        let a = IdAllocator::open(
            store,
            path,
            10,
            AllocatorOptions {
                floor: (3, 3),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(a.allocate_vid().await.unwrap().as_u64() >= 510);
    }

    #[tokio::test]
    async fn raise_floor_moves_a_live_allocator_forward() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = IdAllocator::new(store, Path::from("id.json"), 10)
            .await
            .unwrap();
        a.allocate_vid().await.unwrap();
        a.raise_floor(42, 7).await;
        assert_eq!(a.allocate_vid().await.unwrap().as_u64(), 42);
        assert_eq!(a.allocate_eid().await.unwrap().as_u64(), 7);
        a.raise_floor(1, 1).await;
        assert_eq!(a.allocate_vid().await.unwrap().as_u64(), 43);
    }

    #[tokio::test]
    async fn reserved_hwm_reports_the_end_of_the_reserved_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = IdAllocator::new(store, Path::from("id.json"), 10)
            .await
            .unwrap();
        assert_eq!(a.reserved_hwm(), (0, 0));
        a.allocate_vid().await.unwrap();
        a.allocate_eids(3).await.unwrap();
        assert_eq!(a.reserved_hwm(), (10, 13));
    }

    #[tokio::test]
    async fn two_allocators_racing_to_create_the_file_never_share_ids() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("id.json");
        // Both see no file.
        let a = IdAllocator::new(store.clone(), path.clone(), 10)
            .await
            .unwrap();
        let b = IdAllocator::new(store.clone(), path.clone(), 10)
            .await
            .unwrap();
        let a_ids: Vec<u64> = a
            .allocate_vids(5)
            .await
            .unwrap()
            .iter()
            .map(|v| v.as_u64())
            .collect();
        let b_ids: Vec<u64> = b
            .allocate_vids(5)
            .await
            .unwrap()
            .iter()
            .map(|v| v.as_u64())
            .collect();
        for id in &b_ids {
            assert!(!a_ids.contains(id), "a={a_ids:?} b={b_ids:?}");
        }
        // The file covers both reservations.
        let c = IdAllocator::new(store, path, 10).await.unwrap();
        let next = c.allocate_vid().await.unwrap().as_u64();
        assert!(next > *b_ids.iter().max().unwrap() && next > *a_ids.iter().max().unwrap());
    }

    #[tokio::test]
    async fn a_store_without_conditional_writes_is_refused_unless_allowed() {
        let path = Path::from("id.json");
        let strict = IdAllocator::open(
            Arc::new(NoConditional(InMemory::new())),
            path.clone(),
            10,
            AllocatorOptions {
                allow_unconditional: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let err = strict
            .allocate_vid()
            .await
            .expect_err("strict store wrote unconditionally");
        assert!(format!("{err:#}").contains("conditional"), "{err:#}");

        let lenient = IdAllocator::open(
            Arc::new(NoConditional(InMemory::new())),
            path,
            10,
            AllocatorOptions {
                allow_unconditional: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(lenient.allocate_vid().await.unwrap().as_u64(), 0);
        assert_eq!(lenient.allocate_vids(20).await.unwrap().len(), 20);
    }

    #[tokio::test]
    async fn reserved_hwm_never_reports_below_the_floor() {
        // A floor that came from the manifest or the tables, with the counter
        // file gone: the marks a manifest records must not fall back to 0.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = IdAllocator::open(
            store,
            Path::from("id.json"),
            10,
            AllocatorOptions {
                floor: (500, 70),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(a.reserved_hwm(), (500, 70));
        a.raise_floor(900, 80).await;
        assert_eq!(a.reserved_hwm(), (900, 80));
        a.raise_floor(1, 1).await;
        assert_eq!(a.reserved_hwm(), (900, 80));
    }

    /// Applies writes, but answers the first conditional update with a
    /// precondition failure, as a retried request whose first attempt landed
    /// does.
    #[derive(Debug)]
    struct LostAck {
        inner: InMemory,
        fail_next_update: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for LostAck {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LostAck")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for LostAck {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let is_update = matches!(opts.mode, PutMode::Update(_));
            let result = self.inner.put_opts(location, payload, opts).await?;
            if is_update
                && self
                    .fail_next_update
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "lost acknowledgement".into(),
                });
            }
            Ok(result)
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn an_update_that_landed_but_was_reported_failed_is_adopted() {
        let store = Arc::new(LostAck {
            inner: InMemory::new(),
            fail_next_update: std::sync::atomic::AtomicBool::new(false),
        });
        let a = IdAllocator::new(store.clone(), Path::from("id.json"), 10)
            .await
            .unwrap();
        a.allocate_vid().await.unwrap(); // creates the file
        store
            .fail_next_update
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // The next batch's update lands but reports a precondition failure.
        let ids = a.allocate_vids(15).await.unwrap();
        assert_eq!(ids.first().unwrap().as_u64(), 1);
        // And the allocator keeps working afterwards.
        a.allocate_vids(30).await.unwrap();
    }

    #[tokio::test]
    async fn a_real_concurrent_writer_still_fails_the_reservation() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = IdAllocator::new(store.clone(), Path::from("id.json"), 10)
            .await
            .unwrap();
        a.allocate_vid().await.unwrap();
        let b = IdAllocator::new(store, Path::from("id.json"), 10)
            .await
            .unwrap();
        b.allocate_vid().await.unwrap(); // b moves the file on
        let err = a
            .allocate_vids(20)
            .await
            .expect_err("stale writer reserved ids");
        assert!(
            format!("{err:#}").to_lowercase().contains("concurrent"),
            "{err:#}"
        );
    }
}
