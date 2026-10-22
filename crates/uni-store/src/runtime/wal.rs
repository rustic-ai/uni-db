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

/// Parse LSN from WAL segment filename format `{:020}_{uuid}.wal`.
/// Returns None if the filename doesn't match the expected format.
fn parse_lsn_from_filename(path: &Path) -> Option<u64> {
    let filename = path.filename()?;
    if filename.len() < 20 {
        return None;
    }
    // `str::get(..20)` yields `None` (instead of panicking) when byte index 20
    // falls mid-UTF-8-character, so a foreign multibyte filename is skipped.
    filename.get(..20).and_then(|s| s.parse::<u64>().ok())
}

/// Magic prefix of checksummed (v2) WAL segments.
///
/// v2 layout: `UNIWAL2\n<64-hex-char blake3 of payload>\n<payload JSON>`.
/// Segments without the magic are legacy (pre-2.0.7) raw JSON and are still
/// readable; they just have no integrity protection.
const WAL_V2_MAGIC: &[u8] = b"UNIWAL2\n";

/// Length of the hex-encoded blake3 checksum in the v2 header.
const WAL_V2_HASH_HEX_LEN: usize = 64;

/// Wrap a serialized segment payload in the checksummed v2 envelope.
fn encode_segment_envelope(payload_json: &[u8]) -> Vec<u8> {
    let hash = blake3::hash(payload_json);
    let mut out =
        Vec::with_capacity(WAL_V2_MAGIC.len() + WAL_V2_HASH_HEX_LEN + 1 + payload_json.len());
    out.extend_from_slice(WAL_V2_MAGIC);
    out.extend_from_slice(hash.to_hex().as_bytes());
    out.push(b'\n');
    out.extend_from_slice(payload_json);
    out
}

/// Decode a WAL segment from its on-disk bytes, verifying the checksum for
/// v2 envelopes and falling back to legacy raw-JSON parsing otherwise.
///
/// Returns a human-readable corruption description on failure — the caller
/// decides whether that is fatal (corrupt middle segment) or a tolerated
/// torn tail (see [`WriteAheadLog::replay_since`]).
///
/// `pub` + `doc(hidden)` solely so `fuzz/fuzz_targets/wal_decode.rs` can
/// drive it with arbitrary bytes; it is not part of the public API.
#[doc(hidden)]
pub fn decode_segment(bytes: &[u8]) -> std::result::Result<WalSegment, String> {
    if let Some(rest) = bytes.strip_prefix(WAL_V2_MAGIC) {
        if rest.len() < WAL_V2_HASH_HEX_LEN + 1 || rest[WAL_V2_HASH_HEX_LEN] != b'\n' {
            return Err("truncated v2 segment header".to_string());
        }
        let (hex, payload_nl) = rest.split_at(WAL_V2_HASH_HEX_LEN);
        let payload = &payload_nl[1..];
        let expected =
            std::str::from_utf8(hex).map_err(|_| "non-utf8 checksum header".to_string())?;
        let actual = blake3::hash(payload);
        if actual.to_hex().as_str() != expected {
            return Err(format!(
                "checksum mismatch (expected {expected}, computed {})",
                actual.to_hex()
            ));
        }
        serde_json::from_slice(payload).map_err(|e| format!("v2 payload parse: {e}"))
    } else {
        // Legacy (pre-2.0.7) segment: raw JSON, no checksum.
        serde_json::from_slice(bytes).map_err(|e| format!("legacy segment parse: {e}"))
    }
}

/// Test-only fault injection: when set, the next local-store segment fsync is
/// treated as having failed, exercising the clean-abort delete path (review H3)
/// without needing a real disk fault. Process-isolated under nextest.
#[cfg(test)]
pub(crate) static FAIL_NEXT_FSYNC: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Fsync a freshly written file and its parent directory.
///
/// The directory fsync makes the new directory entry itself durable across
/// a crash (pattern borrowed from uni-sidecar's atomic `store_value`).
///
/// `pub(crate)` so the snapshot durability barrier (review C4) can reuse the
/// exact same file+parent fsync the WAL uses for its own segments.
pub(crate) fn sync_file_and_parent(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(unix)]
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Lossless WAL serialization for a property map.
///
/// `uni_common::Value` is `#[serde(untagged)]`, so serializing a `Properties`
/// map straight to serde_json is **lossy**: a `Value::SparseVector` collapses to
/// a `Map`, a `Value::Vector` to a `List`, nested temporals to strings, etc. —
/// the variant cannot be recovered on replay. The WAL is a persistence path, so
/// it must not rely on untagged serde (the exact hazard called out on the
/// `Value` type).
///
/// This module encodes each property value through the explicit, tagged
/// CypherValue codec (`cypher_value_codec`), stored as a base64 string with a
/// sentinel prefix. On read, a value carrying the prefix is CV-decoded (lossless,
/// new format); any other value is a pre-existing legacy segment and is decoded
/// through the old untagged path unchanged (backward compatible — no WAL version
/// bump, and old segments behave exactly as before).
mod cv_props {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;
    use uni_common::{Properties, Value};

    /// Sentinel marking a CV-encoded value. The leading control byte cannot
    /// begin a legacy untagged-JSON string a user could realistically store.
    const CV_PREFIX: &str = "\u{1}uni_cv:";

    pub fn serialize<S: Serializer>(props: &Properties, s: S) -> Result<S::Ok, S::Error> {
        let engine = base64::engine::general_purpose::STANDARD;
        let encoded: HashMap<&String, String> = props
            .iter()
            .map(|(k, v)| {
                let bytes = uni_common::cypher_value_codec::encode(v);
                (k, format!("{CV_PREFIX}{}", engine.encode(bytes)))
            })
            .collect();
        encoded.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Properties, D::Error> {
        let raw: HashMap<String, serde_json::Value> = HashMap::deserialize(d)?;
        let engine = base64::engine::general_purpose::STANDARD;
        let mut out = Properties::with_capacity(raw.len());
        for (k, jv) in raw {
            let value = match &jv {
                serde_json::Value::String(s) if s.starts_with(CV_PREFIX) => {
                    let bytes = engine
                        .decode(&s[CV_PREFIX.len()..])
                        .map_err(serde::de::Error::custom)?;
                    uni_common::cypher_value_codec::decode(&bytes)
                        .map_err(serde::de::Error::custom)?
                }
                // Legacy (pre-CV) segment: an untagged `Value` written directly.
                // Decode through the old path (lossy for SparseVector/Vector, but
                // that is exactly the pre-existing behavior for old segments).
                _ => serde_json::from_value::<Value>(jv).map_err(serde::de::Error::custom)?,
            };
            out.insert(k, value);
        }
        Ok(out)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Mutation {
    InsertEdge {
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        version: u64,
        #[serde(with = "cv_props")]
        properties: Properties,
        /// Edge type name for metadata recovery. Optional for backward compatibility.
        #[serde(default)]
        edge_type_name: Option<String>,
        /// Wall-clock creation time (nanos) as the live write path stamped it.
        /// `None` in segments written before this field existed, which replay
        /// treats exactly as it always did — the timestamp stays unset. See
        /// the note on `InsertVertex::created_at`.
        #[serde(default)]
        created_at: Option<i64>,
        /// Wall-clock update time (nanos). Same compatibility rules as
        /// `created_at`.
        #[serde(default)]
        updated_at: Option<i64>,
    },
    DeleteEdge {
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        version: u64,
    },
    InsertVertex {
        vid: Vid,
        #[serde(with = "cv_props")]
        properties: Properties,
        #[serde(default)]
        labels: Vec<String>,
        /// Wall-clock creation time (nanos) as the live write path stamped it.
        ///
        /// `created_at`/`updated_at` are user-visible (`RETURN n.created_at`),
        /// but nothing else in the record can reconstruct them: a WAL replay
        /// happens at an arbitrary later time, so `now()` at recovery would
        /// invent a value rather than restore one. Carrying the original is the
        /// only honest option, and the alternative — leaving them unset — bakes
        /// a permanent null into L1 at the next flush.
        ///
        /// Externally-tagged serde with `#[serde(default)]`, so segments
        /// written before this field deserialize to `None` and replay exactly
        /// as they did before: unset, as today. Same mechanism as `labels` and
        /// `InsertEdge::edge_type_name`.
        #[serde(default)]
        created_at: Option<i64>,
        /// Wall-clock update time (nanos). Same compatibility rules as
        /// `created_at`.
        #[serde(default)]
        updated_at: Option<i64>,
    },
    DeleteVertex {
        vid: Vid,
        #[serde(default)]
        labels: Vec<String>,
    },
    /// Replaces a vertex's full label set (a `SET n:Label` / `REMOVE n:Label`
    /// that touched no properties). Carries the complete resolved label set so
    /// replay can REPLACE (removals included). Added after the original four
    /// variants — externally-tagged serde_json, so old WAL segments (which never
    /// contain it) deserialize unchanged.
    SetVertexLabels { vid: Vid, labels: Vec<String> },
}

/// WAL segment with LSN for idempotent recovery
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalSegment {
    /// Log Sequence Number - monotonically increasing per segment
    pub lsn: u64,
    /// Mutations in this segment
    pub mutations: Vec<Mutation>,
    /// Writer features of the binary that wrote the segment
    /// (`uni_common::core::schema::WRITER_FEATURES`). Empty in a segment an
    /// older binary wrote.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writer_features: Vec<String>,
}

/// What [`WriteAheadLog::replay_since_with_provenance`] recovered.
#[derive(Debug, Default)]
pub struct WalReplay {
    /// Mutations of every replayed segment, in LSN order.
    pub mutations: Vec<Mutation>,
    /// Replayed segments that carry no writer features, i.e. were written by
    /// a binary that may not enforce declared edge endpoints.
    pub unmarked_segments: usize,
}

/// Borrowed serialization view of [`WalSegment`].
///
/// Field names and order match `WalSegment` exactly, so the serde output is
/// byte-identical; `flush` serializes through this to avoid deep-cloning the
/// whole mutation batch per flush.
#[derive(Serialize, Debug)]
struct WalSegmentRef<'a> {
    lsn: u64,
    mutations: &'a [Mutation],
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    writer_features: &'a [String],
}

pub struct WriteAheadLog {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    /// Filesystem root backing `store` when it is a local store. When set,
    /// every flushed segment is fsync'd (file + parent directory) before the
    /// flush is reported durable — `object_store::LocalFileSystem` does not
    /// fsync on `put`, so without this a power loss can drop acknowledged
    /// commits. `None` for remote stores (the PUT ack is the durability
    /// point there).
    local_root: Option<std::path::PathBuf>,
    /// Recorded in every segment this log writes.
    writer_features: Vec<String>,
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
            local_root: None,
            writer_features: uni_common::core::schema::current_writer_features(),
            state: Mutex::new(WalState {
                buffer: Vec::new(),
                next_lsn: 1, // Start at 1 so 0 means "no WAL"
                flushed_lsn: 0,
            }),
        }
    }

    /// Set the local filesystem root backing the object store, enabling
    /// fsync-on-flush. See the field docs on `local_root`.
    #[must_use]
    pub fn with_local_root(mut self, local_root: Option<std::path::PathBuf>) -> Self {
        self.local_root = local_root;
        self
    }

    /// Write segments without writer features, as a binary that predates
    /// them does. Only for tests that simulate such a binary.
    #[doc(hidden)]
    #[must_use]
    pub fn without_writer_features(mut self) -> Self {
        self.writer_features.clear();
        self
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

    /// Keep LSNs above `floor`, the WAL mark of the latest snapshot manifest.
    ///
    /// [`Self::initialize`] resumes after the highest LSN still on disk, but
    /// a flush truncates every segment at or below the mark it publishes, so
    /// a store shut down cleanly has none: the log restarted at LSN 1 while
    /// the manifest said 3, and recovery (`replay_since(3)`) skipped every
    /// segment written after the reopen -- a crash before the next flush lost
    /// those commits. Raises the next LSN to `floor + 1` and the flushed LSN
    /// to `floor`, so the marks a later flush publishes never go backwards.
    ///
    /// # Errors
    /// When the state lock is poisoned.
    pub fn ensure_lsn_above(&self, floor: u64) -> Result<()> {
        let mut state = acquire_mutex(&self.state, "wal_state")?;
        state.next_lsn = state.next_lsn.max(floor + 1);
        state.flushed_lsn = state.flushed_lsn.max(floor);
        Ok(())
    }

    /// Find the maximum LSN in existing WAL segments by parsing filenames.
    /// Only downloads segments if filename parsing fails (fallback).
    async fn find_max_lsn(&self) -> Result<u64> {
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;
        let mut max_lsn: u64 = 0;

        for meta in metas {
            // Try to parse LSN from filename first (fast path)
            if let Some(lsn) = parse_lsn_from_filename(&meta.location) {
                max_lsn = max_lsn.max(lsn);
            } else {
                // Fallback: download and parse segment if filename doesn't match expected format
                warn!(
                    path = %meta.location,
                    "WAL filename doesn't match expected format, downloading segment"
                );
                let get_result =
                    get_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
                let bytes = get_result.bytes().await?;
                if bytes.is_empty() {
                    continue;
                }
                // This is only a max-LSN probe; a corrupt segment is skipped
                // here (with a warning) and adjudicated by `replay_since`'s
                // tail-vs-middle policy during actual recovery.
                match decode_segment(&bytes) {
                    Ok(segment) => max_lsn = max_lsn.max(segment.lsn),
                    Err(reason) => {
                        warn!(path = %meta.location, reason = %reason,
                            "Skipping corrupt WAL segment during max-LSN probe");
                    }
                }
            }
        }

        Ok(max_lsn)
    }

    #[instrument(skip(self, mutation), level = "trace")]
    pub fn append(&self, mutation: Mutation) -> Result<()> {
        let mut state = acquire_mutex(&self.state, "wal_state")?;
        state.buffer.push(mutation);
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

        // Serialize a borrowed view of the segment (serde output is identical
        // to `WalSegment` — see `wal_segment_ref_serializes_identically`), so
        // the batch is not deep-cloned per flush. `batch` itself stays owned
        // for the restore-on-failure paths below.
        let segment = WalSegmentRef {
            lsn,
            mutations: &batch,
            writer_features: &self.writer_features,
        };

        // Serialize segment; restore buffer on failure
        let json = match serde_json::to_vec(&segment) {
            Ok(j) => j,
            Err(e) => {
                warn!(lsn, error = %e, "Failed to serialize WAL segment, restoring buffer");
                // Restore buffer on serialization failure
                let mut state = acquire_mutex(&self.state, "wal_state")?;
                let new_mutations = std::mem::take(&mut state.buffer);
                state.buffer = batch;
                state.buffer.extend(new_mutations);
                // Don't roll back LSN - gap is harmless and maintains monotonicity
                return Err(e.into());
            }
        };
        // Wrap in the checksummed v2 envelope so torn/corrupt segments are
        // detected at replay instead of surfacing as opaque parse errors.
        let body = encode_segment_envelope(&json);
        tracing::Span::current().record("size_bytes", body.len());
        metrics::counter!("uni_wal_bytes_written_total").increment(body.len() as u64);

        // Include LSN in filename for easy ordering and identification
        let filename = format!("{:020}_{}.wal", lsn, Uuid::new_v4());
        let path = self.prefix.clone().join(filename);

        // Attempt to write; restore buffer on failure to prevent data loss
        if let Err(e) = put_with_timeout(&self.store, &path, body.into(), DEFAULT_TIMEOUT).await {
            warn!(
                lsn,
                error = %e,
                "Failed to flush WAL segment, restoring buffer (LSN gap preserved for monotonicity)"
            );
            // Restore buffer so data isn't lost on transient failures
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            // Prepend the failed batch to any new mutations that arrived
            let new_mutations = std::mem::take(&mut state.buffer);
            state.buffer = batch;
            state.buffer.extend(new_mutations);
            // Don't roll back LSN - gap is harmless and maintains strict monotonicity
            // All WAL consumers use `>` / `<=` comparisons, not equality checks
            return Err(e);
        }

        // Local stores: fsync the segment + its directory before reporting
        // the flush durable. On fsync failure we report failure (durability
        // cannot be guaranteed) AND delete the just-written segment: the bytes
        // are already on disk, so leaving them would let a later crash + replay
        // resurrect a transaction the caller was told had FAILED (ghost commit).
        // `flushed_lsn` is intentionally left un-advanced. (review H3)
        if let Some(root) = &self.local_root {
            let file_path = root.join(path.as_ref());
            #[cfg(test)]
            let synced = if FAIL_NEXT_FSYNC.swap(false, std::sync::atomic::Ordering::SeqCst) {
                Ok(Err(std::io::Error::other("injected fsync failure")))
            } else {
                tokio::task::spawn_blocking(move || sync_file_and_parent(&file_path)).await
            };
            #[cfg(not(test))]
            let synced =
                tokio::task::spawn_blocking(move || sync_file_and_parent(&file_path)).await;
            let fsync_err: Option<anyhow::Error> = match synced {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(e.into()),
                Err(e) => Some(e.into()),
            };
            if let Some(err) = fsync_err {
                warn!(
                    lsn,
                    error = %err,
                    "WAL segment fsync failed — deleting the non-durable segment to avoid a ghost commit on replay"
                );
                // Best-effort clean abort. If the delete ALSO fails the WAL is
                // in an indeterminate state (a non-durable segment may survive
                // and replay); surface that as a hard error rather than a
                // routine flush failure so the caller does not silently retry.
                if let Err(del_err) = delete_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await
                {
                    return Err(anyhow::anyhow!(
                        "WAL segment fsync failed ({err}) and the cleanup delete \
                         of segment at lsn {lsn} also failed ({del_err}); the WAL \
                         may contain a non-durable segment"
                    ));
                }
                return Err(err);
            }
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

    /// Get the highest LSN that has been flushed.
    ///
    /// # Errors
    ///
    /// Returns error if the WAL state lock is poisoned (see issue #18/#150).
    pub fn flushed_lsn(&self) -> Result<u64, uni_common::sync::LockPoisonedError> {
        let guard = uni_common::sync::acquire_mutex(&self.state, "wal_state")?;
        Ok(guard.flushed_lsn)
    }

    /// Replay WAL segments with LSN > high_water_mark.
    /// Returns mutations from segments that haven't been applied yet.
    /// Optimized to skip downloading segments with LSN <= high_water_mark (parsed from filename).
    ///
    /// Corruption policy: a corrupt (bad checksum / unparseable / empty)
    /// segment at the **tail** of the log is the classic torn write from a
    /// crash — it is logged prominently and treated as end-of-WAL, since the
    /// commit it belonged to was never acknowledged. A corrupt segment with
    /// valid segments **after** it is real data loss and fails recovery with
    /// an error naming the file.
    pub async fn replay_since(&self, high_water_mark: u64) -> Result<Vec<Mutation>> {
        Ok(self
            .replay_since_with_provenance(high_water_mark)
            .await?
            .mutations)
    }

    /// [`Self::replay_since`], also counting the replayed segments written
    /// without writer features (see [`WalReplay::unmarked_segments`]).
    ///
    /// # Errors
    /// As [`Self::replay_since`].
    #[instrument(skip(self), level = "debug")]
    pub async fn replay_since_with_provenance(&self, high_water_mark: u64) -> Result<WalReplay> {
        let start = std::time::Instant::now();
        debug!(high_water_mark, "Replaying WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;
        let mut mutations = Vec::new();

        // Collect candidate paths and sort by LSN (filename prefix).
        // Lexicographical sort works for the zero-padded LSN prefix.
        let mut paths: Vec<_> = metas
            .into_iter()
            .map(|m| m.location)
            .filter(|p| {
                // Skip segments identifiable as <= high_water_mark without
                // downloading. Unparseable filenames stay in (legacy safety).
                parse_lsn_from_filename(p).is_none_or(|lsn| lsn > high_water_mark)
            })
            .collect();
        paths.sort();

        let mut segments_replayed = 0;
        let mut unmarked_segments = 0;

        for (idx, path) in paths.iter().enumerate() {
            // A segment can disappear between the listing above and this read.
            // Two benign, unavoidable causes:
            //   * a flush finalizer's `truncate_before` (writer.rs step K) is
            //     deleting segments concurrently. `Drop for Uni` only *signals*
            //     shutdown (`ShutdownHandle::shutdown_blocking` sends on a
            //     channel and returns without joining), so a spawned finalizer
            //     outlives the handle and can still be truncating while the next
            //     open replays.
            //   * an eventually-consistent object store returns an
            //     already-deleted key in a listing.
            //
            // Skipping costs no durability: `truncate_before` only deletes
            // segments whose mutations are already in L1, so a vanished segment
            // carries nothing that is not recovered anyway — and a listing taken
            // microseconds later would simply not have included it. Propagating
            // instead turns the race into a failed database open.
            //
            // ONLY `NotFound` is skipped. Any other read error still fails
            // recovery: treating a transient blip as "skip" would silently drop
            // committed mutations. Pinned by
            // `replay_still_fails_on_non_notfound_read_error`.
            let get_result = match get_with_timeout(&self.store, path, DEFAULT_TIMEOUT).await {
                Ok(result) => result,
                Err(e) if crate::store_utils::is_not_found(&e) => {
                    warn!(
                        path = %path,
                        "WAL segment vanished between listing and read (concurrent \
                         truncation); skipping — its mutations are already durable in L1"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };
            let bytes = get_result.bytes().await?;

            // Empty files and decode failures share one corruption policy.
            let decoded = if bytes.is_empty() {
                Err("empty segment file".to_string())
            } else {
                decode_segment(&bytes)
            };

            let segment = match decoded {
                Ok(segment) => segment,
                Err(reason) => {
                    let is_tail = idx + 1 == paths.len();
                    if is_tail {
                        warn!(
                            path = %path,
                            reason = %reason,
                            "Corrupt tail WAL segment — torn write from a crash; \
                             treating as end of WAL (the commit was never acknowledged)"
                        );
                        break;
                    }
                    return Err(anyhow::anyhow!(
                        "corrupt WAL segment '{path}' ({reason}) with {} later segment(s) \
                         present; refusing to skip — manual inspection required",
                        paths.len() - idx - 1
                    ));
                }
            };

            // Double-check LSN from segment content (handles fallback case)
            if segment.lsn > high_water_mark {
                if !uni_common::core::schema::writer_enforces_endpoints(&segment.writer_features) {
                    unmarked_segments += 1;
                }
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

        Ok(WalReplay {
            mutations,
            unmarked_segments,
        })
    }

    /// Replay all WAL segments.
    pub async fn replay(&self) -> Result<Vec<Mutation>> {
        self.replay_since(0).await
    }

    /// Delete one WAL segment, treating "already absent" as success.
    ///
    /// `truncate_before` and `truncate` list segments and then delete them one
    /// at a time. Between the listing and any given delete, another truncation
    /// can remove the same segment: the flush finalizer runs `truncate_before`
    /// on a spawned task (`writer.rs` step K) which outlives the `Uni` handle,
    /// because `Drop for Uni` only *signals* shutdown
    /// (`ShutdownHandle::shutdown_blocking` sends on a channel and returns
    /// without joining). Reopening the same directory therefore gives two live
    /// truncators, and the loser of the race previously failed its whole flush
    /// with `Object at location ... not found`.
    ///
    /// A delete whose object is already gone has achieved its postcondition, so
    /// treating it as an error is wrong. Only `NotFound` is tolerated — every
    /// other delete error still propagates, so a permissions or I/O fault is
    /// never mistaken for "already truncated".
    ///
    /// Returns `true` when this call is the one that removed the object, so
    /// `deleted_count` stays an honest count rather than a count of attempts.
    async fn delete_segment_if_present(&self, path: &Path) -> Result<bool> {
        match delete_with_timeout(&self.store, path, DEFAULT_TIMEOUT).await {
            Ok(()) => Ok(true),
            Err(e) if crate::store_utils::is_not_found(&e) => {
                debug!(
                    path = %path,
                    "WAL segment already removed by a concurrent truncation; nothing to do"
                );
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Deletes WAL segments with LSN <= high_water_mark by parsing filenames.
    /// Only downloads segments if filename parsing fails (fallback).
    #[instrument(skip(self), level = "info")]
    pub async fn truncate_before(&self, high_water_mark: u64) -> Result<()> {
        info!(high_water_mark, "Truncating WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;

        let mut deleted_count = 0;
        for meta in metas {
            // Try to parse LSN from filename first (fast path)
            let should_delete = if let Some(lsn) = parse_lsn_from_filename(&meta.location) {
                lsn <= high_water_mark
            } else {
                // Fallback: download and parse segment if filename doesn't match expected format
                warn!(
                    path = %meta.location,
                    "WAL filename doesn't match expected format, downloading segment"
                );
                let get_result =
                    get_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
                let bytes = get_result.bytes().await?;
                if bytes.is_empty() {
                    // Empty segments should be deleted
                    true
                } else {
                    match decode_segment(&bytes) {
                        Ok(segment) => segment.lsn <= high_water_mark,
                        Err(reason) => {
                            // Never delete a corrupt segment during
                            // truncation — keep the evidence; replay's
                            // tail-vs-middle policy adjudicates it.
                            warn!(path = %meta.location, reason = %reason,
                                "Keeping corrupt WAL segment during truncation");
                            false
                        }
                    }
                }
            };

            if should_delete && self.delete_segment_if_present(&meta.location).await? {
                deleted_count += 1;
            }
        }
        info!(deleted_count, "WAL truncation completed");
        Ok(())
    }

    /// Check if any WAL segments exist (for detecting database with lost manifest).
    pub async fn has_segments(&self) -> Result<bool> {
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;
        Ok(!metas.is_empty())
    }

    pub async fn truncate(&self) -> Result<()> {
        info!("Truncating all WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;

        let mut deleted_count = 0;
        for meta in metas {
            if self.delete_segment_if_present(&meta.location).await? {
                deleted_count += 1;
            }
        }
        info!(deleted_count, "Full WAL truncation completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
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
            labels: vec![],
            created_at: None,
            updated_at: None,
        };

        wal.append(mutation)?;
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

    #[tokio::test]
    async fn test_lsn_monotonicity() -> Result<()> {
        // Verify that LSN is strictly monotonic even across multiple flushes
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        let mutation1 = Mutation::InsertVertex {
            vid: Vid::new(1),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
        };
        let mutation2 = Mutation::InsertVertex {
            vid: Vid::new(2),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
        };
        let mutation3 = Mutation::InsertVertex {
            vid: Vid::new(3),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
        };

        // First flush
        wal.append(mutation1)?;
        let lsn1 = wal.flush().await?;

        // Second flush
        wal.append(mutation2)?;
        let lsn2 = wal.flush().await?;

        // Third flush
        wal.append(mutation3)?;
        let lsn3 = wal.flush().await?;

        // Verify strict monotonicity
        assert!(lsn2 > lsn1, "LSN2 ({}) should be > LSN1 ({})", lsn2, lsn1);
        assert!(lsn3 > lsn2, "LSN3 ({}) should be > LSN2 ({})", lsn3, lsn2);

        // Verify LSNs are consecutive
        assert_eq!(lsn2, lsn1 + 1);
        assert_eq!(lsn3, lsn2 + 1);

        Ok(())
    }

    /// H3: when a segment's fsync fails, the just-written (non-durable) segment
    /// must be deleted so a later crash + replay cannot resurrect a transaction
    /// the caller was told had failed (ghost commit). `flush()` reports failure.
    #[tokio::test]
    async fn fsync_failure_deletes_segment_no_ghost_commit() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");
        // local_root must be set for the fsync barrier (and thus the H3 path) to run.
        let wal = WriteAheadLog::new(store, prefix).with_local_root(Some(dir.path().to_path_buf()));

        wal.append(Mutation::InsertVertex {
            vid: Vid::new(1),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
        })?;

        // Force the next segment fsync to fail.
        FAIL_NEXT_FSYNC.store(true, std::sync::atomic::Ordering::SeqCst);
        let result = wal.flush().await;
        assert!(
            result.is_err(),
            "flush must report failure when the segment fsync fails"
        );

        // The non-durable segment must have been deleted: replay surfaces nothing.
        let replayed = wal.replay().await?;
        assert!(
            replayed.is_empty(),
            "a segment whose fsync failed must not be replayable (ghost commit); got {} mutations",
            replayed.len()
        );
        Ok(())
    }

    #[test]
    fn test_parse_lsn_from_filename() {
        // Standard format
        let path = Path::from("00000000000000000042_a1b2c3d4.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(42));

        let path = Path::from("00000000000000001234_e5f6a7b8.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(1234));

        // Leading zeros
        let path = Path::from("00000000000000000001_xyz.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(1));

        // Large LSN (within u64 range)
        let path = Path::from("12345678901234567890_uuid.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(12345678901234567890));

        // Invalid formats
        let path = Path::from("invalid.wal");
        assert_eq!(parse_lsn_from_filename(&path), None);

        let path = Path::from("123.wal"); // Too short
        assert_eq!(parse_lsn_from_filename(&path), None);

        let path = Path::from("abcdefghijklmnopqrst_uuid.wal"); // Non-numeric
        assert_eq!(parse_lsn_from_filename(&path), None);

        // Missing underscore separator (but first 20 chars are valid LSN)
        let path = Path::from("00000000000000000100.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(100));

        // Empty path
        let path = Path::from("");
        assert_eq!(parse_lsn_from_filename(&path), None);
    }

    /// Regression for Bug #30: `parse_lsn_from_filename` must not panic on a
    /// filename whose byte 20 falls in the middle of a multi-byte UTF-8 char.
    ///
    /// The length guard uses [`str::len`] (byte length), but the subsequent
    /// `filename[..20]` slice requires byte 20 to be a char boundary. A name of
    /// 19 ASCII bytes plus one 2-byte char ('é', bytes 19..21) has a byte length
    /// of at least 20, passes the guard, then slices mid-'é' and panics today.
    /// The correct behavior is to return `None` for a non-numeric/unparsable name.
    ///
    /// We use [`Path::parse`] (not [`Path::from`]) because `from` percent-encodes
    /// non-ASCII so that `filename()` would be pure ASCII and never reach the
    /// mid-char slice; `parse` preserves the raw multi-byte segment verbatim,
    /// which is exactly what a real on-disk listing can surface.
    #[test]
    fn test_parse_lsn_from_filename_multibyte_no_panic() {
        let name = format!("{}{}.wal", "0".repeat(19), "é"); // byte 20 falls mid-'é'
        let path = Path::parse(name).expect("multi-byte segment is a valid object_store path");
        assert_eq!(parse_lsn_from_filename(&path), None); // RED: panics today; correct = None
    }

    /// Test for Issue #6: WAL initialization should parse LSN from filenames
    /// without downloading all segments
    #[tokio::test]
    async fn test_find_max_lsn_scalability() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        // Create 100 WAL segments with increasing LSNs
        for i in 1..=100 {
            let mutation = Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
                created_at: None,
                updated_at: None,
            };
            wal.append(mutation)?;
            wal.flush().await?;
        }

        // Measure initialization time - should be fast (parsing filenames, not downloading)
        let start = std::time::Instant::now();
        let max_lsn = wal.find_max_lsn().await?;
        let duration = start.elapsed();

        // Verify correctness
        assert_eq!(max_lsn, 100, "Max LSN should be 100");

        // Verify performance - should complete quickly even with many segments
        assert!(
            duration.as_millis() < 1000,
            "find_max_lsn took {}ms, expected < 1000ms (filename parsing should be fast)",
            duration.as_millis()
        );

        Ok(())
    }

    /// Test for Issue #11: LSN gaps are preserved on flush failures (watermark pattern)
    #[tokio::test]
    async fn test_lsn_gaps_preserved_on_flush_failure() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store.clone(), prefix.clone());

        // Flush mutation 1 successfully
        wal.append(Mutation::InsertVertex {
            vid: Vid::new(1),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
        })?;
        let lsn1 = wal.flush().await?;
        assert_eq!(lsn1, 1);

        // Flush mutation 2 successfully
        wal.append(Mutation::InsertVertex {
            vid: Vid::new(2),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
        })?;
        let lsn2 = wal.flush().await?;
        assert_eq!(lsn2, 2);

        // Simulate a scenario where flush might fail by creating a read-only store
        // (In real scenario, network failures would cause this)
        // For now, verify that LSN assignment happens BEFORE write attempt
        // by checking that next_lsn increments even if we don't flush

        // Append mutation 3 but DON'T flush
        wal.append(Mutation::InsertVertex {
            vid: Vid::new(3),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
        })?;

        // Now flush mutation 4
        wal.append(Mutation::InsertVertex {
            vid: Vid::new(4),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
        })?;
        let lsn4 = wal.flush().await?;

        // LSN should be 3 (both mutations 3 and 4 flushed together)
        assert_eq!(lsn4, 3, "LSN should increment monotonically");

        // Verify all mutations can be replayed
        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 4, "All 4 mutations should be replayed");

        Ok(())
    }

    /// Test for Issue #11: Verify LSN watermark pattern - no LSN reuse
    #[tokio::test]
    async fn test_lsn_watermark_no_reuse() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        // Track all LSNs we've seen
        let mut seen_lsns = std::collections::HashSet::new();

        // Perform 50 flushes
        for i in 1..=50 {
            wal.append(Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
                created_at: None,
                updated_at: None,
            })?;
            let lsn = wal.flush().await?;

            // Verify no LSN reuse
            assert!(
                !seen_lsns.contains(&lsn),
                "LSN {} was reused! This violates monotonicity.",
                lsn
            );
            seen_lsns.insert(lsn);

            // Verify LSN is strictly increasing
            assert_eq!(lsn, i, "LSN should be {}, got {}", i, lsn);
        }

        Ok(())
    }

    /// Test for Issue #33: WAL truncation should parse LSN from filenames
    /// without downloading all segments
    #[tokio::test]
    async fn test_truncate_scalability() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        // Create 100 WAL segments
        for i in 1..=100 {
            let mutation = Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
                created_at: None,
                updated_at: None,
            };
            wal.append(mutation)?;
            wal.flush().await?;
        }

        // Truncate segments with LSN <= 50
        let start = std::time::Instant::now();
        wal.truncate_before(50).await?;
        let duration = start.elapsed();

        // Verify only segments 51-100 remain
        let mutations = wal.replay().await?;
        assert_eq!(
            mutations.len(),
            50,
            "Should have 50 mutations remaining (51-100)"
        );

        // Verify performance - should be fast (filename parsing, not downloading)
        assert!(
            duration.as_millis() < 1000,
            "truncate_before took {}ms, expected < 1000ms (filename parsing should be fast)",
            duration.as_millis()
        );

        Ok(())
    }

    /// Test for Issue #6: replay_since should skip old segments by filename
    #[tokio::test]
    async fn test_replay_since_skips_old_segments() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        // Create 100 WAL segments
        for i in 1..=100 {
            let mutation = Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
                created_at: None,
                updated_at: None,
            };
            wal.append(mutation)?;
            wal.flush().await?;
        }

        // Replay only segments with LSN > 90 (should skip 90 segments by filename)
        let start = std::time::Instant::now();
        let mutations = wal.replay_since(90).await?;
        let duration = start.elapsed();

        // Verify only 10 mutations returned (LSN 91-100)
        assert_eq!(mutations.len(), 10, "Should replay only LSNs 91-100");

        // Verify performance - should be fast (skips 90 segments by filename)
        assert!(
            duration.as_millis() < 500,
            "replay_since took {}ms, expected < 500ms (should skip by filename)",
            duration.as_millis()
        );

        Ok(())
    }

    /// Test for Issue #23: Vertex labels preserved through WAL replay
    #[tokio::test]
    async fn test_wal_replay_preserves_vertex_labels() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = Arc::new(WriteAheadLog::new(store, prefix));

        // Append InsertVertex with labels
        wal.append(Mutation::InsertVertex {
            vid: Vid::new(42),
            properties: {
                let mut props = HashMap::new();
                props.insert(
                    "name".to_string(),
                    uni_common::Value::String("Alice".to_string()),
                );
                props
            },
            labels: vec!["Person".to_string(), "User".to_string()],
            created_at: None,
            updated_at: None,
        })?;

        // Flush to WAL
        wal.flush().await?;

        // Replay mutations
        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 1);

        // Verify labels are preserved
        if let Mutation::InsertVertex { vid, labels, .. } = &mutations[0] {
            assert_eq!(vid.as_u64(), 42);
            assert_eq!(labels.len(), 2);
            assert!(labels.contains(&"Person".to_string()));
            assert!(labels.contains(&"User".to_string()));
        } else {
            panic!("Expected InsertVertex mutation");
        }

        Ok(())
    }

    /// Test for Issue #23: DeleteVertex labels preserved through WAL replay
    #[tokio::test]
    async fn test_wal_replay_preserves_delete_vertex_labels() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = Arc::new(WriteAheadLog::new(store, prefix));

        // Append DeleteVertex with labels (needed for tombstone flushing - Issue #76)
        wal.append(Mutation::DeleteVertex {
            vid: Vid::new(99),
            labels: vec!["Person".to_string(), "Admin".to_string()],
        })?;

        // Flush to WAL
        wal.flush().await?;

        // Replay mutations
        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 1);

        // Verify labels are preserved in DeleteVertex
        if let Mutation::DeleteVertex { vid, labels } = &mutations[0] {
            assert_eq!(vid.as_u64(), 99);
            assert_eq!(labels.len(), 2);
            assert!(labels.contains(&"Person".to_string()));
            assert!(labels.contains(&"Admin".to_string()));
        } else {
            panic!("Expected DeleteVertex mutation");
        }

        Ok(())
    }

    /// Test for Issue #28: Edge type name preserved through WAL replay
    #[tokio::test]
    async fn test_wal_replay_preserves_edge_type_name() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = Arc::new(WriteAheadLog::new(store, prefix));

        // Append InsertEdge with edge_type_name
        wal.append(Mutation::InsertEdge {
            src_vid: Vid::new(1),
            dst_vid: Vid::new(2),
            edge_type: 100,
            eid: Eid::new(500),
            version: 1,
            properties: {
                let mut props = HashMap::new();
                props.insert("since".to_string(), uni_common::Value::Int(2020));
                props
            },
            edge_type_name: Some("KNOWS".to_string()),
            created_at: None,
            updated_at: None,
        })?;

        // Flush to WAL
        wal.flush().await?;

        // Replay mutations
        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 1);

        // Verify edge_type_name is preserved
        if let Mutation::InsertEdge {
            eid,
            edge_type_name,
            ..
        } = &mutations[0]
        {
            assert_eq!(eid.as_u64(), 500);
            assert_eq!(edge_type_name.as_deref(), Some("KNOWS"));
        } else {
            panic!("Expected InsertEdge mutation");
        }

        Ok(())
    }

    /// Test for Issue #23: Backward compatibility with old WAL segments (no labels)
    #[tokio::test]
    async fn test_wal_backward_compatibility_labels() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        // Manually create a WAL segment with old format (no labels field)
        let old_format_json = r#"{
            "lsn": 1,
            "mutations": [
                {
                    "InsertVertex": {
                        "vid": 123,
                        "properties": {}
                    }
                }
            ]
        }"#;

        let path = prefix.clone().join("00000000000000000001_test.wal");
        store.put(&path, old_format_json.into()).await?;

        // Create WAL and replay
        let wal = WriteAheadLog::new(store, prefix);
        let mutations = wal.replay().await?;

        // Verify old format deserializes with empty labels (via #[serde(default)])
        assert_eq!(mutations.len(), 1);
        if let Mutation::InsertVertex { vid, labels, .. } = &mutations[0] {
            assert_eq!(vid.as_u64(), 123);
            assert_eq!(
                labels.len(),
                0,
                "Old format should deserialize with empty labels"
            );
        } else {
            panic!("Expected InsertVertex mutation");
        }

        Ok(())
    }

    /// `flush` serializes through the borrowed `WalSegmentRef`; its bytes must
    /// be identical to the owned `WalSegment` so replay (which deserializes
    /// `WalSegment`) is unaffected.
    #[test]
    fn wal_segment_ref_serializes_identically() {
        let mut props = HashMap::new();
        props.insert("p".to_string(), uni_common::Value::Int(7));
        let mutations = vec![
            Mutation::InsertVertex {
                vid: Vid::new(1),
                properties: props,
                labels: vec!["L".to_string()],
                created_at: None,
                updated_at: None,
            },
            Mutation::DeleteEdge {
                eid: Eid::new(2),
                src_vid: Vid::new(1),
                dst_vid: Vid::new(3),
                edge_type: 4,
                version: 5,
            },
        ];
        for features in [
            uni_common::core::schema::current_writer_features(),
            Vec::new(),
        ] {
            let owned = WalSegment {
                lsn: 42,
                mutations: mutations.clone(),
                writer_features: features.clone(),
            };
            let borrowed = WalSegmentRef {
                lsn: 42,
                mutations: &mutations,
                writer_features: &features,
            };
            assert_eq!(
                serde_json::to_vec(&owned).unwrap(),
                serde_json::to_vec(&borrowed).unwrap()
            );
        }
    }

    /// A segment this binary writes records its writer features; one written
    /// without them (an older binary) is counted as unmarked on replay.
    #[tokio::test]
    async fn replay_counts_segments_without_writer_features() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store: Arc<dyn ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(dir.path())?,
        );
        let prefix = Path::from("wal");
        let mutation = || Mutation::DeleteVertex {
            vid: Vid::new(1),
            labels: vec![],
        };
        let marked = WriteAheadLog::new(store.clone(), prefix.clone());
        marked.initialize().await?;
        marked.append(mutation())?;
        marked.flush().await?;
        let replay = marked.replay_since_with_provenance(0).await?;
        assert_eq!((replay.mutations.len(), replay.unmarked_segments), (1, 0));

        let older = WriteAheadLog::new(store.clone(), prefix.clone()).without_writer_features();
        older.initialize().await?;
        older.append(mutation())?;
        older.flush().await?;
        let replay = WriteAheadLog::new(store, prefix)
            .replay_since_with_provenance(0)
            .await?;
        assert_eq!((replay.mutations.len(), replay.unmarked_segments), (2, 1));
        Ok(())
    }

    /// An `ObjectStore` that lists a chosen path but returns `NotFound` when it
    /// is read — modelling the window between `replay_since`'s listing and its
    /// per-segment `get`.
    ///
    /// Two real situations produce exactly this, and neither is a fault:
    ///   * a flush finalizer's `truncate_before` (`writer.rs` step K) deleting a
    ///     segment concurrently — `Drop for Uni` only *signals* shutdown
    ///     (`ShutdownHandle::shutdown_blocking` sends on a channel and returns),
    ///     so a spawned finalizer outlives the handle and can still be deleting
    ///     while the next open replays;
    ///   * an eventually-consistent object store returning a already-deleted key
    ///     in a listing.
    #[derive(Debug)]
    struct VanishingStore {
        inner: Arc<dyn ObjectStore>,
        vanish: Path,
    }

    impl std::fmt::Display for VanishingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "VanishingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for VanishingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
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
            if location == &self.vanish {
                return Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)),
                });
            }
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

    /// Build a WAL with `n` single-mutation segments and return their paths in
    /// LSN order.
    async fn seed_segments(store: &Arc<dyn ObjectStore>, prefix: &Path, n: u64) -> Vec<Path> {
        let wal = WriteAheadLog::new(store.clone(), prefix.clone());
        for i in 1..=n {
            wal.append(Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
                created_at: None,
                updated_at: None,
            })
            .unwrap();
            wal.flush().await.unwrap();
        }
        let mut paths: Vec<Path> = list_with_timeout(store, Some(prefix), DEFAULT_TIMEOUT)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location)
            .collect();
        paths.sort();
        paths
    }

    /// A WAL segment that disappears between the listing and its read must not
    /// fail recovery.
    ///
    /// `replay_since` lists segments and then `get`s each one. A concurrent
    /// `truncate_before` deletes segments whose mutations are already durable in
    /// L1, so a segment vanishing in that window carries nothing that is not
    /// already recovered — and a listing taken microseconds later would simply
    /// not have included it. Propagating the `NotFound` turns that benign,
    /// unavoidable race into a hard open failure.
    ///
    /// Observed in the wild as a load-dependent flake in
    /// `recovery_index_no_rebuild::scalar_recovered_delta_queryable_without_rebuild`:
    /// `Object at location .../wal/00000000000000000002_<uuid>.wal not found`.
    #[tokio::test]
    async fn replay_skips_segment_truncated_between_list_and_read() -> Result<()> {
        let dir = tempdir()?;
        let prefix = Path::from("wal");
        let base: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let paths = seed_segments(&base, &prefix, 3).await;

        // Vanish a NON-tail segment: the tail has its own corrupt-tail policy,
        // so using the tail would pass for the wrong reason.
        let store: Arc<dyn ObjectStore> = Arc::new(VanishingStore {
            inner: base,
            vanish: paths[0].clone(),
        });
        let wal = WriteAheadLog::new(store, prefix);

        let mutations = wal.replay().await?;

        // The two readable segments still replay; only the vanished one is skipped.
        assert_eq!(
            mutations.len(),
            2,
            "a concurrently-truncated segment must be skipped, not fail the replay"
        );
        Ok(())
    }

    /// A store whose `list` hands back entries that it has *already* removed —
    /// the other truncator winning the race in the window between our listing
    /// and our deletes. The `NotFound` the truncation then sees is a real one
    /// raised by `LocalFileSystem`, not a synthesised error.
    #[derive(Debug)]
    struct DeletedAfterListStore {
        inner: Arc<dyn ObjectStore>,
    }

    impl std::fmt::Display for DeletedAfterListStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DeletedAfterListStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DeletedAfterListStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
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
            use futures::{StreamExt, TryStreamExt};
            let inner = self.inner.clone();
            let prefix = prefix.cloned();
            Box::pin(
                futures::stream::once(async move {
                    let metas: Vec<object_store::ObjectMeta> = inner
                        .list(prefix.as_ref())
                        .try_collect()
                        .await
                        .unwrap_or_default();
                    // The competing truncation completes here.
                    for m in &metas {
                        let _ = inner.delete(&m.location).await;
                    }
                    futures::stream::iter(metas.into_iter().map(Ok))
                })
                .flatten(),
            )
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

    /// Truncation must not fail when a segment it listed was already deleted by
    /// a concurrent truncation.
    ///
    /// Two truncators exist whenever a spawned flush finalizer (`writer.rs`
    /// step K) outlives its `Uni` — `Drop for Uni` only signals shutdown — and
    /// the directory is reopened. The loser used to fail the enclosing flush
    /// with `Object at location .../wal/...wal not found`, which is how this
    /// surfaced as a load-dependent flake across the
    /// `recovery_index_no_rebuild` tests.
    ///
    /// Deleting an object that is already absent has achieved its
    /// postcondition, so it is success, not failure.
    #[tokio::test]
    async fn truncate_tolerates_segment_deleted_by_concurrent_truncation() -> Result<()> {
        let dir = tempdir()?;
        let prefix = Path::from("wal");
        let base: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        seed_segments(&base, &prefix, 3).await;

        let store: Arc<dyn ObjectStore> = Arc::new(DeletedAfterListStore { inner: base });
        let wal = WriteAheadLog::new(store, prefix);

        // Every segment this lists has already been removed by the time the
        // delete runs.
        wal.truncate_before(u64::MAX)
            .await
            .expect("truncate_before must tolerate an already-deleted segment");
        wal.truncate()
            .await
            .expect("truncate must tolerate an already-deleted segment");
        Ok(())
    }

    /// Inverse guard: a `NotFound` is the *only* read error that may be skipped.
    /// A transient read failure must still fail recovery loudly, or a blip
    /// silently drops committed mutations.
    #[tokio::test]
    async fn replay_still_fails_on_non_notfound_read_error() -> Result<()> {
        struct FailingStore(Arc<dyn ObjectStore>);

        impl std::fmt::Debug for FailingStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "FailingStore")
            }
        }
        impl std::fmt::Display for FailingStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "FailingStore")
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for FailingStore {
            async fn put_opts(
                &self,
                location: &Path,
                payload: object_store::PutPayload,
                opts: object_store::PutOptions,
            ) -> object_store::Result<object_store::PutResult> {
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
                _location: &Path,
                _options: object_store::GetOptions,
            ) -> object_store::Result<object_store::GetResult> {
                Err(object_store::Error::Generic {
                    store: "FailingStore",
                    source: Box::new(std::io::Error::other("injected transient read failure")),
                })
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

        let dir = tempdir()?;
        let prefix = Path::from("wal");
        let base: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        seed_segments(&base, &prefix, 2).await;

        let store: Arc<dyn ObjectStore> = Arc::new(FailingStore(base));
        let wal = WriteAheadLog::new(store, prefix);

        assert!(
            wal.replay().await.is_err(),
            "a non-NotFound read error must still fail recovery"
        );
        Ok(())
    }
}
