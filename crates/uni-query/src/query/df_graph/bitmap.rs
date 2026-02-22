use bitvec::vec::BitVec;
use fxhash::FxHashSet;
use uni_common::core::id::{Eid, Vid};

/// Density threshold: if set bits exceed this fraction of total range, use DenseBitVec.
const DENSITY_THRESHOLD: f64 = 0.125; // 12.5%

/// Build an adaptive filter (dense bitvec or sparse hashset) from raw u64 IDs.
///
/// Uses a density heuristic: if more than 12.5% of the range is populated,
/// a `BitVec` is used for O(1) lookups; otherwise a `FxHashSet` is used.
///
/// Returns `(Option<BitVec>, Option<FxHashSet<u64>>)` — exactly one is `Some`.
fn build_filter(ids: Vec<u64>, max_hint: usize) -> (Option<BitVec>, Option<FxHashSet<u64>>) {
    if ids.is_empty() {
        return (None, Some(FxHashSet::default()));
    }

    let range = max_hint.max(ids.iter().copied().max().unwrap_or(0) as usize + 1);
    let density = ids.len() as f64 / range.max(1) as f64;

    if density > DENSITY_THRESHOLD {
        let mut bv = BitVec::repeat(false, range);
        for &id in &ids {
            let idx = id as usize;
            if idx < bv.len() {
                bv.set(idx, true);
            }
        }
        (Some(bv), None)
    } else {
        (None, Some(ids.into_iter().collect()))
    }
}

/// Check if a raw u64 ID passes a dense bitvec filter.
fn bitvec_contains(bv: &BitVec, raw: u64) -> bool {
    let idx = raw as usize;
    idx < bv.len() && bv[idx]
}

/// Macro to define an ID filter enum with identical structure but typed `contains` method.
macro_rules! define_id_filter {
    (
        $(#[$meta:meta])*
        $filter_name:ident, $id_type:ty, $from_fn:ident
    ) => {
        $(#[$meta])*
        #[derive(Debug)]
        pub enum $filter_name {
            /// No predicate — all IDs are allowed.
            AllAllowed,
            /// Dense bitvec indexed by raw ID.
            DenseBitVec(BitVec),
            /// Sparse hash set for low-cardinality results.
            Sparse(FxHashSet<u64>),
        }

        impl $filter_name {
            /// Check if an ID passes the filter.
            pub fn contains(&self, id: $id_type) -> bool {
                match self {
                    Self::AllAllowed => true,
                    Self::DenseBitVec(bv) => bitvec_contains(bv, id.as_u64()),
                    Self::Sparse(set) => set.contains(&id.as_u64()),
                }
            }

            /// Build a filter from a list of raw u64 IDs.
            ///
            /// Uses a density heuristic to choose between DenseBitVec (>12.5% density)
            /// and Sparse HashSet.
            pub fn $from_fn(ids: Vec<u64>, max_hint: usize) -> Self {
                let (bv, set) = build_filter(ids, max_hint);
                if let Some(bv) = bv {
                    Self::DenseBitVec(bv)
                } else {
                    Self::Sparse(set.unwrap_or_default())
                }
            }
        }
    };
}

define_id_filter!(
    /// Bitmap filter for edge IDs — preselects which edges pass a property predicate.
    EidFilter, Eid, from_eids
);

define_id_filter!(
    /// Bitmap filter for vertex IDs — preselects which target vertices pass a property predicate.
    VidFilter, Vid, from_vids
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eid_all_allowed() {
        let f = EidFilter::AllAllowed;
        assert!(f.contains(Eid::new(0)));
        assert!(f.contains(Eid::new(999)));
        assert!(f.contains(Eid::new(u64::MAX - 1)));
    }

    #[test]
    fn test_eid_dense_bitvec_contains() {
        let f = EidFilter::from_eids(vec![1, 3, 5, 7], 8);
        // DenseBitVec chosen: 4/8 = 50% > 12.5%
        assert!(matches!(f, EidFilter::DenseBitVec(_)));
        assert!(f.contains(Eid::new(1)));
        assert!(f.contains(Eid::new(3)));
        assert!(f.contains(Eid::new(5)));
        assert!(f.contains(Eid::new(7)));
        assert!(!f.contains(Eid::new(0)));
        assert!(!f.contains(Eid::new(2)));
        assert!(!f.contains(Eid::new(4)));
        assert!(!f.contains(Eid::new(6)));
    }

    #[test]
    fn test_eid_dense_bitvec_empty() {
        let f = EidFilter::from_eids(vec![], 100);
        // Empty set → Sparse
        assert!(matches!(f, EidFilter::Sparse(_)));
        assert!(!f.contains(Eid::new(0)));
        assert!(!f.contains(Eid::new(50)));
    }

    #[test]
    fn test_eid_hashset_contains() {
        // Sparse set: 3 out of 1000 = 0.3% < 12.5%
        let f = EidFilter::from_eids(vec![100, 500, 999], 1000);
        assert!(matches!(f, EidFilter::Sparse(_)));
        assert!(f.contains(Eid::new(100)));
        assert!(f.contains(Eid::new(500)));
        assert!(f.contains(Eid::new(999)));
        assert!(!f.contains(Eid::new(0)));
        assert!(!f.contains(Eid::new(101)));
        assert!(!f.contains(Eid::new(998)));
    }

    #[test]
    fn test_eid_from_eids_chooses_dense() {
        // 20 out of 100 = 20% > 12.5% → DenseBitVec
        let eids: Vec<u64> = (0..20).collect();
        let f = EidFilter::from_eids(eids, 100);
        assert!(matches!(f, EidFilter::DenseBitVec(_)));
    }

    #[test]
    fn test_eid_from_eids_chooses_hashset() {
        // 5 out of 10000 = 0.05% < 12.5% → Sparse
        let f = EidFilter::from_eids(vec![10, 100, 1000, 5000, 9999], 10000);
        assert!(matches!(f, EidFilter::Sparse(_)));
    }

    #[test]
    fn test_eid_out_of_range_dense() {
        // DenseBitVec with range 10, querying beyond range returns false
        let f = EidFilter::from_eids(vec![1, 2, 3, 4, 5, 6, 7, 8], 10);
        assert!(matches!(f, EidFilter::DenseBitVec(_)));
        assert!(!f.contains(Eid::new(100)));
    }

    #[test]
    fn test_vid_filter_basic() {
        // AllAllowed
        let f = VidFilter::AllAllowed;
        assert!(f.contains(Vid::new(0)));
        assert!(f.contains(Vid::new(999)));

        // Dense
        let f = VidFilter::from_vids(vec![1, 2, 3, 4], 8);
        assert!(matches!(f, VidFilter::DenseBitVec(_)));
        assert!(f.contains(Vid::new(1)));
        assert!(f.contains(Vid::new(4)));
        assert!(!f.contains(Vid::new(0)));
        assert!(!f.contains(Vid::new(5)));

        // Sparse
        let f = VidFilter::from_vids(vec![100, 9999], 10000);
        assert!(matches!(f, VidFilter::Sparse(_)));
        assert!(f.contains(Vid::new(100)));
        assert!(f.contains(Vid::new(9999)));
        assert!(!f.contains(Vid::new(0)));
    }
}
