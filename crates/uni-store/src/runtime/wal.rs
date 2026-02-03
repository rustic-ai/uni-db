// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::store_utils::{
    DEFAULT_TIMEOUT, delete_with_timeout, get_with_timeout, list_with_timeout, put_with_timeout,
};
use anyhow::Result;
use metrics;
use object_store::ObjectStore;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, instrument, warn};
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};
use uni_common::sync::acquire_mutex;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Mutation {
    InsertEdge {
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u16,
        eid: Eid,
        version: u64,
        properties: Properties,
    },
    DeleteEdge {
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u16,
        version: u64,
    },
    InsertVertex {
        vid: Vid,
        properties: Properties,
    },
    DeleteVertex {
        vid: Vid,
    },
}

/// WAL segment with LSN for idempotent recovery
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalSegment {
    /// Log Sequence Number - monotonically increasing per segment
    pub lsn: u64,
    /// Mutations in this segment
    pub mutations: Vec<Mutation>,
}

pub struct WriteAheadLog {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    state: Mutex<WalState>,
}

struct WalState {
    buffer: Vec<Mutation>,
    /// Current LSN counter (incremented per flush)
    next_lsn: u64,
    /// Highest LSN successfully flushed
    flushed_lsn: u64,
}

impl WriteAheadLog {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self {
            store,
            prefix,
            state: Mutex::new(WalState {
                buffer: Vec::new(),
                next_lsn: 1, // Start at 1 so 0 means "no WAL"
                flushed_lsn: 0,
            }),
        }
    }

    /// Initialize WAL state from existing segments (called on startup)
    pub async fn initialize(&self) -> Result<u64> {
        let max_lsn = self.find_max_lsn().await?;
        {
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            state.next_lsn = max_lsn + 1;
            state.flushed_lsn = max_lsn;
        }
        Ok(max_lsn)
    }

    /// Find the maximum LSN in existing WAL segments
    async fn find_max_lsn(&self) -> Result<u64> {
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;
        let mut max_lsn: u64 = 0;

        for meta in metas {
            let get_result = get_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
            let bytes = get_result.bytes().await?;
            if bytes.is_empty() {
                continue;
            }
            let segment: WalSegment = serde_json::from_slice(&bytes)?;
            max_lsn = max_lsn.max(segment.lsn);
        }

        Ok(max_lsn)
    }

    #[instrument(skip(self, mutation), level = "trace")]
    pub fn append(&self, mutation: &Mutation) -> Result<()> {
        let mut state = acquire_mutex(&self.state, "wal_state")?;
        state.buffer.push(mutation.clone());
        metrics::counter!("uni_wal_entries_total").increment(1);
        Ok(())
    }

    /// Flush buffered mutations to a WAL segment. Returns the LSN of the flushed segment.
    #[instrument(skip(self), fields(lsn, mutations_count, size_bytes))]
    pub async fn flush(&self) -> Result<u64> {
        let start = std::time::Instant::now();
        let (batch, lsn) = {
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            if state.buffer.is_empty() {
                return Ok(state.flushed_lsn);
            }
            let lsn = state.next_lsn;
            state.next_lsn += 1;
            (std::mem::take(&mut state.buffer), lsn)
        };

        tracing::Span::current().record("lsn", lsn);
        tracing::Span::current().record("mutations_count", batch.len());

        // Create segment with LSN
        let segment = WalSegment {
            lsn,
            mutations: batch.clone(),
        };

        let json = serde_json::to_vec(&segment)?;
        tracing::Span::current().record("size_bytes", json.len());
        metrics::counter!("uni_wal_bytes_written_total").increment(json.len() as u64);

        // Include LSN in filename for easy ordering and identification
        let filename = format!("{:020}_{}.wal", lsn, Uuid::new_v4());
        let path = self.prefix.child(filename);

        // Attempt to write; restore buffer on failure to prevent data loss
        if let Err(e) = put_with_timeout(&self.store, &path, json.into(), DEFAULT_TIMEOUT).await {
            warn!(lsn, error = %e, "Failed to flush WAL segment, restoring buffer");
            // Restore buffer so data isn't lost on transient failures
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            // Prepend the failed batch to any new mutations that arrived
            let new_mutations = std::mem::take(&mut state.buffer);
            state.buffer = batch;
            state.buffer.extend(new_mutations);
            // Roll back LSN
            state.next_lsn -= 1;
            return Err(e);
        }

        // Update flushed LSN on success
        {
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            state.flushed_lsn = lsn;
        }

        let duration = start.elapsed();
        metrics::histogram!("wal_flush_latency_ms").record(duration.as_millis() as f64);
        metrics::histogram!("uni_wal_flush_duration_seconds").record(duration.as_secs_f64());

        if duration.as_millis() > 100 {
            warn!(
                lsn,
                duration_ms = duration.as_millis(),
                "Slow WAL flush detected"
            );
        } else {
            debug!(
                lsn,
                duration_ms = duration.as_millis(),
                "WAL flush completed"
            );
        }

        Ok(lsn)
    }

    /// Get the highest LSN that has been flushed
    pub fn flushed_lsn(&self) -> u64 {
        self.state
            .lock()
            .expect("WAL state lock poisoned - a thread panicked while holding it")
            .flushed_lsn
    }

    /// Replay WAL segments with LSN > high_water_mark.
    /// Returns mutations from segments that haven't been applied yet.
    #[instrument(skip(self), level = "debug")]
    pub async fn replay_since(&self, high_water_mark: u64) -> Result<Vec<Mutation>> {
        let start = std::time::Instant::now();
        debug!(high_water_mark, "Replaying WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;
        let mut mutations = Vec::new();

        // Collect paths and sort by LSN (filename prefix)
        let mut paths: Vec<_> = metas.into_iter().map(|m| m.location).collect();
        paths.sort(); // Lexicographical sort works for LSN prefix (zero-padded)

        let mut segments_replayed = 0;

        for path in paths {
            let get_result = get_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await?;
            let bytes = get_result.bytes().await?;
            if bytes.is_empty() {
                continue;
            }

            let segment: WalSegment = serde_json::from_slice(&bytes)?;
            if segment.lsn > high_water_mark {
                mutations.extend(segment.mutations);
                segments_replayed += 1;
            }
        }

        info!(
            segments_replayed,
            mutations_count = mutations.len(),
            "WAL replay completed"
        );
        metrics::histogram!("uni_wal_replay_duration_seconds")
            .record(start.elapsed().as_secs_f64());

        Ok(mutations)
    }

    /// Replay all WAL segments.
    pub async fn replay(&self) -> Result<Vec<Mutation>> {
        self.replay_since(0).await
    }

    /// Deletes WAL segments with LSN <= high_water_mark
    #[instrument(skip(self), level = "info")]
    pub async fn truncate_before(&self, high_water_mark: u64) -> Result<()> {
        info!(high_water_mark, "Truncating WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;

        let mut deleted_count = 0;
        for meta in metas {
            // Read segment to check LSN
            let get_result = get_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
            let bytes = get_result.bytes().await?;
            if bytes.is_empty() {
                delete_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
                deleted_count += 1;
                continue;
            }

            let segment: WalSegment = serde_json::from_slice(&bytes)?;
            if segment.lsn <= high_water_mark {
                delete_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
                deleted_count += 1;
            }
        }
        info!(deleted_count, "WAL truncation completed");
        Ok(())
    }

    /// Deletes all WAL files (full truncation)
    #[instrument(skip(self), level = "info")]
    pub async fn truncate(&self) -> Result<()> {
        info!("Truncating all WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;

        let mut deleted_count = 0;
        for meta in metas {
            delete_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
            deleted_count += 1;
        }
        info!(deleted_count, "Full WAL truncation completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_append_replay() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        let mutation = Mutation::InsertVertex {
            vid: Vid::new(1),
            properties: HashMap::new(),
        };

        wal.append(&mutation.clone())?;
        wal.flush().await?;

        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 1);
        if let Mutation::InsertVertex { vid, .. } = &mutations[0] {
            assert_eq!(vid.as_u64(), Vid::new(1).as_u64());
        } else {
            panic!("Wrong mutation type");
        }

        wal.truncate().await?;
        let mutations2 = wal.replay().await?;
        assert_eq!(mutations2.len(), 0);

        Ok(())
    }
}
