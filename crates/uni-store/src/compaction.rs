// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::time::{Duration, SystemTime};

/// Trigger strategy for a compaction run.
#[derive(Debug, Clone, Copy)]
pub enum CompactionTask {
    /// Compact when the number of L1 runs exceeds a threshold.
    ByRunCount,
    /// Compact when the total L1 size exceeds a byte threshold.
    BySize,
    /// Compact when the oldest L1 run exceeds an age threshold.
    ByAge,
}

/// What a single compaction run actually did.
///
/// Every field is measured. That is worth stating because it was not always
/// true: this struct once reported `files_compacted: 1` for a label whose table
/// did not exist, and `bytes_before`/`bytes_after` as a literal `0` on every
/// path, so a caller could not tell a real compaction from a no-op (#172).
///
/// The byte fields are gone rather than fixed. They promised a before/after size
/// delta the storage layer does not cheaply provide, while the numbers it *does*
/// compute — fragments and files merged — were being discarded one layer below.
/// [`Self::bytes_reclaimed`] is what remains, and it means something narrower.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CompactionStats {
    /// Tables whose optimize call ran, including those with nothing to merge.
    ///
    /// This is the **denominator** for every Lance-derived field below. A zero
    /// in `fragments_removed` means "there was nothing to merge" when this is
    /// non-zero, and "no table was visited" when it is zero. Without it the two
    /// are indistinguishable — the same ambiguity that made the old
    /// `files_compacted` useless as either a counter or a denominator, since it
    /// conflated tables touched with files merged.
    pub tables_optimized: usize,
    /// Fragments merged away, summed over every optimized table.
    pub fragments_removed: usize,
    /// Fragments written in their place. A compaction that made progress leaves
    /// this below `fragments_removed`.
    pub fragments_added: usize,
    /// Data files merged away. Counts deletion files too, so it can exceed
    /// `fragments_removed` but never trails it.
    pub files_removed: usize,
    /// Data files written. Equal to `fragments_added`.
    pub files_added: usize,
    /// Bytes freed by pruning dataset versions past the retention window.
    ///
    /// Not "before minus after": no space saved by re-encoding is counted, only
    /// bytes deleted from disk by version cleanup. **Reads `0` for any database
    /// younger than the retention window**, which is every short-lived one — so
    /// a zero here is normal and is not evidence that compaction did nothing.
    /// Use `fragments_removed` for that.
    pub bytes_reclaimed: u64,
    /// Wall-clock duration of the run. Real even for a no-op: it measures the
    /// call, not the merge.
    pub duration: Duration,
    /// Semantic (tier-2) vertex compaction passes that ran.
    ///
    /// The **denominator** for [`Self::crdt_merges`], and separate from
    /// `tables_optimized` because backend optimize runs on paths where semantic
    /// compaction does not. The public `compact`/`compact_label`/
    /// `compact_edge_type` entry points do no semantic pass at all, so this is
    /// `0` there — which is how a caller tells "no CRDT properties were merged"
    /// from "CRDT merges are not measured on this path".
    pub semantic_passes: usize,
    /// CRDT value merges performed during semantic compaction. Meaningful only
    /// when `semantic_passes` is non-zero.
    pub crdt_merges: usize,
}

impl CompactionStats {
    /// Fold one table's optimize report into the run totals.
    ///
    /// Bumps `tables_optimized`, so the denominator counts a table that was
    /// visited and had nothing to do.
    pub(crate) fn absorb(&mut self, report: &crate::backend::OptimizeReport) {
        self.tables_optimized += 1;
        self.fragments_removed += report.fragments_removed;
        self.fragments_added += report.fragments_added;
        self.files_removed += report.files_removed;
        self.files_added += report.files_added;
        self.bytes_reclaimed += report.bytes_reclaimed;
    }
}

/// Snapshot of the current compaction state for observability.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CompactionStatus {
    /// Flush generations since the last completed compaction.
    ///
    /// Incremented per flush and reset to zero by a compaction run, so despite
    /// the name it counts flushes, not compactions.
    pub l1_runs: usize,
    /// Rows across the L1 delta tables as of the last status refresh.
    pub l1_rows: u64,
    /// **An estimate, not a measurement**: `l1_rows * ENTRY_SIZE_ESTIMATE`, a
    /// fixed per-row guess.
    ///
    /// Renamed from `l1_size_bytes` so the size-based compaction trigger is not
    /// read as being driven by real bytes on disk. It is not, and never was.
    /// Measuring it truthfully is possible but costs a footer read per fragment,
    /// and the cheap alternative can legitimately report zero — which would
    /// silently disable the trigger.
    pub l1_estimated_bytes: u64,
    /// Age of the oldest L1 row as of the last status refresh.
    pub oldest_l1_age: Duration,
    pub compaction_in_progress: bool,
    pub compaction_pending: usize,
    pub last_compaction: Option<SystemTime>,
    /// Completed compaction runs.
    pub total_compactions: u64,
    /// Lifetime sum of [`CompactionStats::bytes_reclaimed`].
    ///
    /// Replaces `total_bytes_compacted`, which no code ever wrote — it was
    /// published as an observation while being a permanent zero. The name
    /// changed with it because "bytes compacted" is not what the storage layer
    /// can report; bytes reclaimed by version pruning is.
    pub total_bytes_reclaimed: u64,
    /// Completed status refreshes.
    ///
    /// Denominator for `l1_rows`, `l1_estimated_bytes` and `oldest_l1_age`: the
    /// background loop may not have run yet, and a zero in those means "never
    /// observed" rather than "empty" until this is non-zero.
    pub status_refreshes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::OptimizeReport;

    fn report(n: usize) -> OptimizeReport {
        OptimizeReport {
            fragments_removed: n,
            fragments_added: n + 1,
            files_removed: n + 2,
            files_added: n + 3,
            bytes_reclaimed: n as u64 + 4,
            old_versions_removed: n as u64 + 5,
        }
    }

    #[test]
    fn absorb_carries_every_field() {
        let mut stats = CompactionStats::default();
        stats.absorb(&report(1));
        stats.absorb(&report(10));

        // Each field summed independently. A fold that dropped one would leave
        // that field at zero, which is indistinguishable from "nothing to do" —
        // so every field is checked, not a representative one.
        assert_eq!(stats.tables_optimized, 2);
        assert_eq!(stats.fragments_removed, 11);
        assert_eq!(stats.fragments_added, 13);
        assert_eq!(stats.files_removed, 15);
        assert_eq!(stats.files_added, 17);
        assert_eq!(stats.bytes_reclaimed, 19);
    }

    #[test]
    fn absorb_counts_a_table_that_had_nothing_to_do() {
        let mut stats = CompactionStats::default();
        stats.absorb(&OptimizeReport::default());

        // The denominator is the whole point: a table was visited and found
        // nothing, which must not read the same as no table being visited.
        assert_eq!(stats.tables_optimized, 1);
        assert_eq!(stats.fragments_removed, 0);
        assert_ne!(
            stats.tables_optimized,
            CompactionStats::default().tables_optimized
        );
    }

    #[test]
    fn optimize_report_add_assign_matches_absorb() {
        let mut a = report(1);
        a += report(10);
        assert_eq!(a.fragments_removed, 11);
        assert_eq!(a.fragments_added, 13);
        assert_eq!(a.files_removed, 15);
        assert_eq!(a.files_added, 17);
        assert_eq!(a.bytes_reclaimed, 19);
        assert_eq!(a.old_versions_removed, 21);
    }

    #[test]
    fn is_noop_is_true_only_for_an_all_zero_report() {
        assert!(OptimizeReport::default().is_noop());
        assert!(!report(0).is_noop(), "report(0) is nonzero in five fields");
        let one_field = OptimizeReport {
            fragments_removed: 1,
            ..Default::default()
        };
        assert!(!one_field.is_noop());
    }
}
