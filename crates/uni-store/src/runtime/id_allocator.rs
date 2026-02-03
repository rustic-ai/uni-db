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
    manifest_version: Option<String>, // ETag for optimistic locking
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
}

impl IdAllocator {
    /// Creates a new ID allocator, loading existing state from object store.
    pub async fn new(store: Arc<dyn ObjectStore>, path: Path, batch_size: u64) -> Result<Self> {
        let (manifest, version) = match get_with_timeout(&store, &path, DEFAULT_TIMEOUT).await {
            Ok(get_result) => {
                let version = get_result.meta.e_tag.clone();
                let bytes = get_result.bytes().await?;
                let manifest: CounterManifest = serde_json::from_slice(&bytes)?;
                (manifest, version)
            }
            Err(e) if e.to_string().contains("not found") => (CounterManifest::default(), None),
            Err(e) => return Err(e),
        };

        // Start allocating from where the last batch ended
        let current_vid = manifest.next_vid_batch;
        let current_eid = manifest.next_eid_batch;

        Ok(Self {
            store,
            path,
            state: Mutex::new(AllocatorState {
                manifest,
                manifest_version: version,
                current_vid,
                current_eid,
            }),
            batch_size,
        })
    }

    /// Allocates a new VID.
    ///
    /// Returns a globally unique, auto-incrementing vertex ID.
    pub async fn allocate_vid(&self) -> Result<Vid> {
        let mut state = self.state.lock().await;

        // Check if we've exhausted our current batch
        if state.current_vid >= state.manifest.next_vid_batch {
            // Reserve a new batch
            state.manifest.next_vid_batch = state.current_vid + self.batch_size;
            self.persist_manifest(&mut state).await?;
        }

        let vid = Vid::new(state.current_vid);
        state.current_vid += 1;
        Ok(vid)
    }

    /// Allocates multiple VIDs at once.
    pub async fn allocate_vids(&self, count: usize) -> Result<Vec<Vid>> {
        let mut state = self.state.lock().await;
        let needed = count as u64;

        // Check if we need to expand our batch
        if state.current_vid + needed > state.manifest.next_vid_batch {
            // Reserve enough for the request plus a full batch
            state.manifest.next_vid_batch = state.current_vid + needed + self.batch_size;
            self.persist_manifest(&mut state).await?;
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

        // Check if we've exhausted our current batch
        if state.current_eid >= state.manifest.next_eid_batch {
            // Reserve a new batch
            state.manifest.next_eid_batch = state.current_eid + self.batch_size;
            self.persist_manifest(&mut state).await?;
        }

        let eid = Eid::new(state.current_eid);
        state.current_eid += 1;
        Ok(eid)
    }

    /// Allocates multiple EIDs at once.
    pub async fn allocate_eids(&self, count: usize) -> Result<Vec<Eid>> {
        let mut state = self.state.lock().await;
        let needed = count as u64;

        // Check if we need to expand our batch
        if state.current_eid + needed > state.manifest.next_eid_batch {
            // Reserve enough for the request plus a full batch
            state.manifest.next_eid_batch = state.current_eid + needed + self.batch_size;
            self.persist_manifest(&mut state).await?;
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

    /// Persists the counter manifest to object store with optimistic locking.
    async fn persist_manifest(&self, state: &mut AllocatorState) -> Result<()> {
        let json = serde_json::to_vec_pretty(&state.manifest)?;
        let bytes = Bytes::from(json);

        // Try conditional put first, fall back to unconditional if not supported
        // (LocalFileSystem doesn't support ETag-based conditional puts)
        let put_result = if let Some(version) = &state.manifest_version {
            let opts: PutOptions = PutMode::Update(UpdateVersion {
                e_tag: Some(version.clone()),
                version: None,
            })
            .into();
            match tokio::time::timeout(
                DEFAULT_TIMEOUT,
                self.store.put_opts(&self.path, bytes.clone().into(), opts),
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(e))
                    if e.to_string().contains("not yet implemented")
                        || e.to_string().contains("not supported") =>
                {
                    // LocalFileSystem doesn't support conditional puts, use regular put
                    put_with_timeout(&self.store, &self.path, bytes, DEFAULT_TIMEOUT).await?
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "Object store put_opts timed out after {:?}",
                        DEFAULT_TIMEOUT
                    ));
                }
            }
        } else {
            // No version yet, try create mode, fall back to regular put
            let opts: PutOptions = PutMode::Create.into();
            match tokio::time::timeout(
                DEFAULT_TIMEOUT,
                self.store.put_opts(&self.path, bytes.clone().into(), opts),
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(object_store::Error::AlreadyExists { .. })) => {
                    // Another process created it, just overwrite
                    put_with_timeout(&self.store, &self.path, bytes, DEFAULT_TIMEOUT).await?
                }
                Ok(Err(e)) if e.to_string().contains("not yet implemented") => {
                    put_with_timeout(&self.store, &self.path, bytes, DEFAULT_TIMEOUT).await?
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "Object store put_opts timed out after {:?}",
                        DEFAULT_TIMEOUT
                    ));
                }
            }
        };

        state.manifest_version = put_result.e_tag;
        Ok(())
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
}
