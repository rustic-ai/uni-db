// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Optimistic concurrency control: commit-time conflict detection (SSI/OCC).
//!
//! Commits are already serialized at the Writer's `flush_lock`, which gives the
//! validate phase a natural critical section. Each transaction captures the
//! Writer's commit-sequence at begin (`L0Buffer::occ_read_seq`); at commit it
//! checks its write-set (and, under SSI, read-set) against the write-sets of
//! every transaction that committed since. A conflict aborts the committer with
//! a retryable error.

use std::collections::{HashSet, VecDeque};

use crate::runtime::l0::{L0Buffer, OccReadSet, try_as_crdt};
use crate::runtime::sync::{AtomicU64, Ordering};
use uni_common::core::id::{Eid, Vid};

/// The set of items a transaction wrote, used for conflict detection.
#[derive(Debug, Default, Clone)]
pub struct WriteSet {
    /// Vertices created, updated, or deleted by the transaction.
    pub vertices: HashSet<Vid>,
    /// Edges created, updated, or deleted by the transaction.
    pub edges: HashSet<Eid>,
    /// Key claims survive L0 rotation through the bounded commit history.
    pub vertex_keys: HashSet<Vec<u8>>,
    pub edge_keys: HashSet<Vec<u8>>,
    pub merge_keys: HashSet<Vec<u8>>,
    pub ext_ids: HashSet<String>,
}

impl WriteSet {
    /// Builds a write-set from a transaction's private L0 buffer.
    ///
    /// Item-level granularity: a touched vertex/edge id is a conflict candidate
    /// regardless of which columns were written (the conservative lost-update
    /// rule). The one exception is the CRDT carve-out: a vertex whose write
    /// touched *only* CRDT-mergeable properties — with no delete and no label
    /// change — is excluded, because `L0Buffer::merge_crdt_properties` will
    /// commute those writes at commit. This lets concurrent CRDT-counter
    /// increments to the same vertex both commit (and merge) instead of aborting.
    ///
    /// Mixed CRDT+non-CRDT writes, label changes, and deletes stay conflictable
    /// (their last-writer-wins / structural part can still be lost). Edges are
    /// always conflictable: every live edge write asserts endpoints/type, which
    /// is non-commutative topology that no CRDT carve-out can cover.
    pub fn from_l0(l0: &L0Buffer) -> Self {
        let mut vertices: HashSet<Vid> = HashSet::new();
        for (vid, props) in &l0.vertex_properties {
            if !is_crdt_carveout(l0, vid, props) {
                vertices.insert(*vid);
            }
        }
        // A delete is never commutative with a concurrent CRDT increment.
        vertices.extend(l0.vertex_tombstones.iter().copied());
        // A label-only mutation (`SET n:Label` / `REMOVE n:Label`) is a
        // structural write — not CRDT-commutative — so the vertex is
        // conflictable. `vertex_label_overwrites` flags exactly the vids whose
        // labels were explicitly replaced, so this is precise: a pure-CRDT
        // increment (no label op) is never flagged and stays carved out.
        vertices.extend(l0.vertex_label_overwrites.iter().copied());

        let mut edges: HashSet<Eid> = l0.edge_properties.keys().copied().collect();
        edges.extend(l0.edge_endpoints.keys().copied());
        edges.extend(l0.tombstones.keys().copied());
        Self {
            vertices,
            edges,
            vertex_keys: l0.constraint_index.keys().cloned().collect(),
            edge_keys: l0.edge_constraint_index.keys().cloned().collect(),
            merge_keys: l0.merge_guard_index.keys().cloned().collect(),
            ext_ids: l0.extid_index.keys().cloned().collect(),
        }
    }

    /// Returns `true` when the write-set touches nothing (a read-only commit).
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty()
            && self.edges.is_empty()
            && self.vertex_keys.is_empty()
            && self.edge_keys.is_empty()
            && self.merge_keys.is_empty()
            && self.ext_ids.is_empty()
    }

    fn conflicting_key_kind(&self, other: &Self) -> Option<&'static str> {
        if !self.vertex_keys.is_disjoint(&other.vertex_keys) {
            Some("vertex constraint")
        } else if !self.edge_keys.is_disjoint(&other.edge_keys) {
            Some("edge constraint")
        } else if !self.merge_keys.is_disjoint(&other.merge_keys) {
            Some("MERGE")
        } else if !self.ext_ids.is_disjoint(&other.ext_ids) {
            Some("ext_id")
        } else {
            None
        }
    }

    /// Returns `true` when this and `other` write any common vertex or edge.
    pub fn intersects(&self, other: &WriteSet) -> bool {
        // Iterate the smaller side for cheaper membership checks.
        let (small, large) = if self.vertices.len() <= other.vertices.len() {
            (&self.vertices, &other.vertices)
        } else {
            (&other.vertices, &self.vertices)
        };
        if small.iter().any(|v| large.contains(v)) {
            return true;
        }
        let (small, large) = if self.edges.len() <= other.edges.len() {
            (&self.edges, &other.edges)
        } else {
            (&other.edges, &self.edges)
        };
        small.iter().any(|e| large.contains(e))
    }
}

/// Returns `true` when a vertex write is a pure CRDT-mergeable carve-out.
///
/// A write qualifies when every property is a CRDT value and the write made no
/// label change. Such a write commutes at commit via
/// [`L0Buffer::merge_crdt_properties`], so it is excluded from the write-set to
/// let concurrent CRDT increments to the same vertex both commit. A
/// re-asserted/changed non-empty label set is not CRDT-mergeable, and a pure
/// increment is written with no labels (`&[]`), so it stays eligible. Tombstones
/// are handled by the caller (a delete never commutes with an increment).
///
/// Shared by [`WriteSet::from_l0`] and [`crdt_carveout_overwrite`] so the
/// carve-out decision and its commit-time soundness check stay identical.
fn is_crdt_carveout(l0: &L0Buffer, vid: &Vid, props: &uni_common::Properties) -> bool {
    let label_changed = l0
        .vertex_labels
        .get(vid)
        .is_some_and(|labels| !labels.is_empty());
    let all_crdt = !props.is_empty() && props.values().all(|v| try_as_crdt(v).is_some());
    all_crdt && !label_changed
}

/// A carved-out CRDT write whose committed value is a different CRDT variant.
///
/// The write-set carve-out ([`WriteSet::from_l0`]) drops a pure-CRDT vertex
/// write from conflict detection assuming its merge commutes. That holds only
/// when the committed value is the *same* CRDT variant. For a different variant,
/// `merge_crdt_properties` falls through to a last-writer-wins overwrite — a
/// silent lost update the carve-out would otherwise hide.
#[derive(Debug)]
pub struct CrdtVariantConflict {
    /// The vertex whose carved-out CRDT write would be overwritten.
    pub vid: Vid,
    /// The property whose committed CRDT variant differs from the write.
    pub property: String,
}

impl std::fmt::Display for CrdtVariantConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "carved-out CRDT write to property {:?} would overwrite a different \
             committed CRDT variant (a lost update); aborting",
            self.property
        )
    }
}

/// Detects carved-out CRDT writes that would silently overwrite a committed value.
///
/// The write-set carve-out removes pure-CRDT writes from conflict detection, so
/// this commit-time check (against the merged main L0, under `flush_lock`) is
/// what keeps the carve-out sound when a property's committed value is a
/// *different* CRDT variant than the write — the one case `merge_crdt_properties`
/// would overwrite rather than merge. Returns the first such mismatch, or `None`
/// when every carved-out write merges cleanly. Declared CRDT properties are
/// additionally guarded at write time; this also covers undeclared CRDT-shaped
/// values that bypass that path.
pub fn crdt_carveout_overwrite(tx_l0: &L0Buffer, main: &L0Buffer) -> Option<CrdtVariantConflict> {
    for (vid, props) in &tx_l0.vertex_properties {
        if tx_l0.vertex_tombstones.contains(vid) || !is_crdt_carveout(tx_l0, vid, props) {
            continue;
        }
        let Some(existing_props) = main.vertex_properties.get(vid) else {
            continue;
        };
        for (key, value) in props {
            let (Some(new_crdt), Some(existing_crdt)) = (
                try_as_crdt(value),
                existing_props.get(key).and_then(try_as_crdt),
            ) else {
                continue;
            };
            if new_crdt.type_name() != existing_crdt.type_name() {
                return Some(CrdtVariantConflict {
                    vid: *vid,
                    property: key.clone(),
                });
            }
        }
    }
    None
}

/// Returns `true` when a committed write touched something the read-set saw.
fn read_set_intersects(read_set: &OccReadSet, w: &WriteSet) -> bool {
    read_set.vertices.iter().any(|v| w.vertices.contains(v))
        || read_set.edges.iter().any(|e| w.edges.contains(e))
}

/// Outcome of a commit-time conflict check.
#[derive(Debug)]
pub enum Conflict {
    /// Concurrent transactions claimed the same logical unique/MERGE key.
    Key { seq: u64, kind: &'static str },
    /// A concurrent commit wrote an item this transaction also wrote.
    WriteWrite { seq: u64 },
    /// A concurrent commit wrote an item this transaction read (SSI).
    ReadWrite { seq: u64 },
    /// The commit history was pruned below this transaction's read sequence,
    /// so a potential conflict cannot be ruled out; abort conservatively.
    HistoryTruncated { read_seq: u64, oldest: u64 },
}

impl std::fmt::Display for Conflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Conflict::Key { seq, kind } => {
                write!(f, "{kind} key conflict with commit sequence {seq}")
            }
            Conflict::WriteWrite { seq } => {
                write!(f, "write-write conflict with commit sequence {seq}")
            }
            Conflict::ReadWrite { seq } => {
                write!(f, "read-write antidependency with commit sequence {seq}")
            }
            Conflict::HistoryTruncated { read_seq, oldest } => write!(
                f,
                "commit history truncated below read sequence {read_seq} \
                 (oldest retained {oldest}); aborting conservatively"
            ),
        }
    }
}

/// Bounded log of recently-committed write-sets, keyed by commit sequence.
///
/// Mutated only under the Writer's `flush_lock`, so it needs no internal
/// synchronization beyond the `Mutex` the Writer wraps it in.
#[derive(Debug)]
pub struct CommitRegistry {
    entries: VecDeque<(u64, WriteSet)>,
    capacity: usize,
}

impl CommitRegistry {
    /// Creates a registry retaining at most `capacity` recent commits.
    ///
    /// # Panics
    /// Panics if `capacity` is zero (a programming error — the registry must
    /// retain at least one commit to detect any conflict).
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "CommitRegistry capacity must be non-zero");
        Self {
            entries: VecDeque::new(),
            capacity,
        }
    }

    /// Records a committed write-set under `seq`, pruning to capacity.
    pub fn record(&mut self, seq: u64, write_set: WriteSet) {
        self.entries.push_back((seq, write_set));
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }

    /// Allocates the next commit sequence and records `write_set` under it,
    /// returning the assigned sequence.
    ///
    /// This is the single shared mutation a committer performs on the OCC state.
    /// The production commit path and the loom/shuttle models call exactly this
    /// method, so the checked bump-then-record step cannot drift from production.
    /// Must be called under the Writer's `flush_lock` (the registry `Mutex`
    /// serializes the `record`, and the bump is the sole writer of `seq`).
    pub(crate) fn commit(&mut self, seq: &AtomicU64, write_set: WriteSet) -> u64 {
        let next = seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.record(next, write_set);
        next
    }

    /// Makes every transaction that began before this call fail its next
    /// commit check with [`Conflict::HistoryTruncated`], and every later one
    /// unaffected.
    ///
    /// For a change that rewrites state wholesale rather than through a
    /// write-set (a snapshot restore): a transaction that read before it cannot
    /// have its reads validated against it. Advances the sequence by two and
    /// keeps one empty entry, so the oldest retained sequence is more than one
    /// past every earlier read sequence and equal to every later one. Must be
    /// called under the Writer's `flush_lock`, like [`Self::commit`].
    pub(crate) fn fence(&mut self, seq: &AtomicU64) -> u64 {
        let next = seq.fetch_add(2, Ordering::Relaxed) + 2;
        self.entries.clear();
        self.entries.push_back((next, WriteSet::default()));
        next
    }

    /// Checks a committing transaction against all commits newer than its read
    /// sequence. Returns the first [`Conflict`] found, or `None` if it may commit.
    ///
    /// `read_set` is `Some` only for SSI read-write transactions; passing `None`
    /// performs write-set-only (lost-update) detection.
    pub fn check(
        &self,
        read_seq: u64,
        write_set: &WriteSet,
        read_set: Option<&OccReadSet>,
    ) -> Option<Conflict> {
        // If the oldest retained commit is newer than read_seq+1, commits in the
        // gap were pruned and cannot be checked — abort conservatively (sound:
        // never misses a real conflict, at the cost of rare false aborts).
        if let Some(&(oldest, _)) = self.entries.front()
            && oldest > read_seq.saturating_add(1)
        {
            return Some(Conflict::HistoryTruncated { read_seq, oldest });
        }
        for (seq, committed) in &self.entries {
            if *seq <= read_seq {
                continue;
            }
            if write_set.intersects(committed) {
                return Some(Conflict::WriteWrite { seq: *seq });
            }
            if let Some(kind) = write_set.conflicting_key_kind(committed) {
                return Some(Conflict::Key { seq: *seq, kind });
            }
            if let Some(rs) = read_set
                && read_set_intersects(rs, committed)
            {
                return Some(Conflict::ReadWrite { seq: *seq });
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(vids: &[u64]) -> WriteSet {
        WriteSet {
            vertices: vids.iter().map(|&v| Vid::from(v)).collect(),
            edges: HashSet::new(),
            ..Default::default()
        }
    }

    /// An edge-only write-set (mirror of [`ws`] for the edge conflict path).
    fn es(eids: &[u64]) -> WriteSet {
        WriteSet {
            vertices: HashSet::new(),
            edges: eids.iter().map(|&e| Eid::from(e)).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn fence_aborts_earlier_readers_only() {
        let seq = AtomicU64::new(0);
        let mut reg = CommitRegistry::new(16);
        let before = reg.commit(&seq, ws(&[1]));
        let fenced = reg.fence(&seq);
        assert!(matches!(
            reg.check(before, &ws(&[9]), None),
            Some(Conflict::HistoryTruncated { .. })
        ));
        assert!(reg.check(fenced, &ws(&[9]), None).is_none());
        let after = reg.commit(&seq, ws(&[2]));
        assert!(reg.check(fenced, &ws(&[3]), None).is_none());
        assert!(matches!(
            reg.check(fenced, &ws(&[2]), None),
            Some(Conflict::WriteWrite { seq }) if seq == after
        ));
    }

    #[test]
    fn disjoint_writes_do_not_conflict() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, ws(&[1, 2]));
        assert!(reg.check(0, &ws(&[3, 4]), None).is_none());
    }

    #[test]
    fn overlapping_write_after_read_seq_conflicts() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, ws(&[1, 2]));
        // A tx that began at read_seq 0 and writes vertex 2 must abort.
        assert!(matches!(
            reg.check(0, &ws(&[2]), None),
            Some(Conflict::WriteWrite { seq: 1 })
        ));
    }

    #[test]
    fn commit_at_or_before_read_seq_is_ignored() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, ws(&[1]));
        // A tx that began AFTER commit 1 (read_seq 1) does not conflict with it.
        assert!(reg.check(1, &ws(&[1]), None).is_none());
    }

    #[test]
    fn read_write_antidependency_detected() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, ws(&[5]));
        let mut rs = OccReadSet::default();
        rs.vertices.insert(Vid::from(5));
        assert!(matches!(
            reg.check(0, &ws(&[99]), Some(&rs)),
            Some(Conflict::ReadWrite { seq: 1 })
        ));
    }

    #[test]
    fn truncated_history_aborts_conservatively() {
        let mut reg = CommitRegistry::new(2);
        reg.record(1, ws(&[1]));
        reg.record(2, ws(&[2]));
        reg.record(3, ws(&[3])); // evicts seq 1
        // A tx with read_seq 0 cannot verify against the evicted seq 1.
        assert!(matches!(
            reg.check(0, &ws(&[42]), None),
            Some(Conflict::HistoryTruncated {
                read_seq: 0,
                oldest: 2
            })
        ));
    }

    #[test]
    fn commit_bumps_sequence_and_records() {
        let seq = AtomicU64::new(0);
        let mut reg = CommitRegistry::new(16);
        // Sequences are allocated 1, 2, … (the bump returns the new value).
        assert_eq!(reg.commit(&seq, ws(&[1])), 1);
        assert_eq!(reg.commit(&seq, ws(&[2])), 2);
        assert_eq!(seq.load(Ordering::Relaxed), 2);
        // The recorded write to vertex 1 is now visible to a stale committer.
        assert!(matches!(
            reg.check(0, &ws(&[1]), None),
            Some(Conflict::WriteWrite { seq: 1 })
        ));
    }

    #[test]
    fn intersects_detects_overlapping_edges() {
        // `intersects` must check the edge sets, not just the vertices.
        assert!(es(&[1, 2]).intersects(&es(&[2, 3])));
        assert!(!es(&[1, 2]).intersects(&es(&[3, 4])));
        // Vertices and edges live in separate namespaces: vertex id 1 and edge
        // id 1 must NOT be mistaken for an overlap.
        assert!(!ws(&[1]).intersects(&es(&[1])));
    }

    #[test]
    fn overlapping_edge_write_after_read_seq_conflicts() {
        let mut reg = CommitRegistry::new(16);
        reg.record(1, es(&[10, 11]));
        // A tx that began at read_seq 0 and writes edge 11 must abort.
        assert!(matches!(
            reg.check(0, &es(&[11]), None),
            Some(Conflict::WriteWrite { seq: 1 })
        ));
        // A disjoint edge write does not conflict.
        assert!(reg.check(0, &es(&[12]), None).is_none());
    }

    // ── CRDT carve-out (`from_l0`) ───────────────────────────────────────────

    fn vid(n: u64) -> Vid {
        Vid::from(n)
    }

    /// A property map with a single GCounter CRDT value under `counter`.
    fn crdt_props(actor: &str, n: u64) -> uni_common::Properties {
        let mut gc = uni_crdt::GCounter::new();
        gc.increment(actor, n);
        let v: uni_common::Value = serde_json::to_value(uni_crdt::Crdt::GCounter(gc))
            .unwrap()
            .into();
        uni_common::Properties::from([("counter".to_string(), v)])
    }

    fn int_props(n: i64) -> uni_common::Properties {
        uni_common::Properties::from([("n".to_string(), uni_common::Value::Int(n))])
    }

    /// A property map with a single GSet CRDT value under `counter` — a
    /// *different* CRDT variant than [`crdt_props`]'s GCounter.
    fn gset_props(item: &str) -> uni_common::Properties {
        let mut gs = uni_crdt::GSet::new();
        gs.add(item.to_string());
        let v: uni_common::Value = serde_json::to_value(uni_crdt::Crdt::GSet(gs))
            .unwrap()
            .into();
        uni_common::Properties::from([("counter".to_string(), v)])
    }

    #[test]
    fn crdt_only_write_without_labels_is_carved_out() {
        let mut buf = L0Buffer::new(0, None);
        buf.insert_vertex_with_labels(vid(1), crdt_props("a", 5), &[]);
        // A pure CRDT increment with no label change merges at commit, so it must
        // not be a conflict candidate — this is what lets concurrent increments
        // both commit.
        assert!(!WriteSet::from_l0(&buf).vertices.contains(&vid(1)));
    }

    #[test]
    fn non_crdt_write_without_labels_is_conflictable() {
        let mut buf = L0Buffer::new(0, None);
        buf.insert_vertex_with_labels(vid(1), int_props(1), &[]);
        assert!(WriteSet::from_l0(&buf).vertices.contains(&vid(1)));
    }

    #[test]
    fn crdt_write_with_labels_stays_conflictable() {
        let mut buf = L0Buffer::new(0, None);
        // A label change is not CRDT-mergeable, so even an otherwise pure CRDT
        // write stays a conflict candidate.
        buf.insert_vertex_with_labels(vid(1), crdt_props("a", 5), &["Counter".to_string()]);
        assert!(WriteSet::from_l0(&buf).vertices.contains(&vid(1)));
    }

    #[test]
    fn mixed_crdt_and_lww_write_is_conflictable() {
        let mut buf = L0Buffer::new(0, None);
        let mut props = crdt_props("a", 5);
        props.insert("n".to_string(), uni_common::Value::Int(1));
        buf.insert_vertex_with_labels(vid(1), props, &[]);
        // The LWW `n` can be lost, so the vertex must stay conflictable.
        assert!(WriteSet::from_l0(&buf).vertices.contains(&vid(1)));
    }

    #[test]
    fn plain_map_value_is_not_mistaken_for_crdt() {
        let mut buf = L0Buffer::new(0, None);
        let map = uni_common::Value::Map(std::collections::HashMap::from([(
            "x".to_string(),
            uni_common::Value::Int(1),
        )]));
        buf.insert_vertex_with_labels(
            vid(1),
            uni_common::Properties::from([("data".to_string(), map)]),
            &[],
        );
        // A non-CRDT map is overwritten (LWW) by `merge_crdt_properties`, so it
        // must remain conflictable.
        assert!(WriteSet::from_l0(&buf).vertices.contains(&vid(1)));
    }

    #[test]
    fn tombstoned_vertex_is_conflictable() {
        let mut buf = L0Buffer::new(0, None);
        buf.insert_vertex_with_labels(vid(1), crdt_props("a", 5), &[]);
        buf.delete_vertex(vid(1)).unwrap();
        // Deletion is not commutative with a concurrent increment.
        assert!(WriteSet::from_l0(&buf).vertices.contains(&vid(1)));
    }

    // ── CRDT carve-out soundness (`crdt_carveout_overwrite`) ─────────────────

    #[test]
    fn crdt_carveout_overwrite_detects_variant_mismatch() {
        // main holds a GCounter; a carved-out write puts a GSet under the same
        // property. `merge_crdt_properties` would silently overwrite the GCounter
        // (a lost update the carve-out hid), so this must be flagged.
        let mut main = L0Buffer::new(0, None);
        main.insert_vertex_with_labels(vid(1), crdt_props("a", 5), &[]);
        let mut tx = L0Buffer::new(0, None);
        tx.insert_vertex_with_labels(vid(1), gset_props("x"), &[]);
        let conflict = crdt_carveout_overwrite(&tx, &main).expect("variant mismatch");
        assert_eq!(conflict.vid, vid(1));
        assert_eq!(conflict.property, "counter");
    }

    #[test]
    fn crdt_carveout_overwrite_allows_same_variant() {
        // Same CRDT variant merges commutatively — the carve-out is sound, no abort.
        let mut main = L0Buffer::new(0, None);
        main.insert_vertex_with_labels(vid(1), crdt_props("a", 5), &[]);
        let mut tx = L0Buffer::new(0, None);
        tx.insert_vertex_with_labels(vid(1), crdt_props("b", 7), &[]);
        assert!(crdt_carveout_overwrite(&tx, &main).is_none());
    }

    #[test]
    fn crdt_carveout_overwrite_allows_new_vertex() {
        // No committed value to overwrite — the merge just inserts.
        let main = L0Buffer::new(0, None);
        let mut tx = L0Buffer::new(0, None);
        tx.insert_vertex_with_labels(vid(1), gset_props("x"), &[]);
        assert!(crdt_carveout_overwrite(&tx, &main).is_none());
    }

    #[test]
    fn crdt_carveout_overwrite_ignores_conflictable_writes() {
        // A labelled (non-carved-out) write is already in the write-set and
        // handled by ordinary conflict detection, so it is not re-flagged here.
        let mut main = L0Buffer::new(0, None);
        main.insert_vertex_with_labels(vid(1), crdt_props("a", 5), &[]);
        let mut tx = L0Buffer::new(0, None);
        tx.insert_vertex_with_labels(vid(1), gset_props("x"), &["Counter".to_string()]);
        assert!(crdt_carveout_overwrite(&tx, &main).is_none());
    }

    // ── Registry pruning under a long-lived reader ───────────────────────────

    #[test]
    fn long_lived_reader_within_retained_history_does_not_abort() {
        // Capacity comfortably holds every commit since the reader's snapshot, so
        // a long-lived reader (low read_seq) is not falsely aborted by truncation.
        let mut reg = CommitRegistry::new(16);
        for seq in 1..=5 {
            reg.record(seq, ws(&[seq + 100])); // disjoint vids → no real conflict
        }
        assert!(reg.check(0, &ws(&[1]), None).is_none());
    }

    #[test]
    fn truncated_history_aborts_read_set_txn_conservatively() {
        // A read-write (SSI) transaction whose snapshot predates evicted commits
        // also aborts conservatively, not just write-set-only transactions.
        let mut reg = CommitRegistry::new(2);
        reg.record(1, ws(&[1]));
        reg.record(2, ws(&[2]));
        reg.record(3, ws(&[3])); // evicts seq 1
        let mut rs = OccReadSet::default();
        rs.vertices.insert(Vid::from(7));
        assert!(matches!(
            reg.check(0, &ws(&[42]), Some(&rs)),
            Some(Conflict::HistoryTruncated { .. })
        ));
    }
}

#[cfg(test)]
mod key_history_regressions {
    use super::*;

    #[test]
    fn key_claims_conflict_across_distinct_ids_and_history_truncation() {
        let mut history = CommitRegistry::new(1);
        let mut a = WriteSet::default();
        a.vertex_keys.insert(vec![1]);
        history.record(1, a.clone());
        assert!(matches!(
            history.check(0, &a, None),
            Some(Conflict::Key {
                seq: 1,
                kind: "vertex constraint"
            })
        ));
        assert!(history.check(1, &a, None).is_none());
        let mut edge = WriteSet::default();
        edge.edge_keys.insert(vec![1]);
        assert!(history.check(0, &edge, None).is_none());
        history.record(2, edge);
        assert!(matches!(
            history.check(0, &a, None),
            Some(Conflict::HistoryTruncated { .. })
        ));
    }
}
