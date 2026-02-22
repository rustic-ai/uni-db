use bitvec::vec::BitVec;
use fxhash::FxHashSet;
use uni_common::core::id::{Eid, Vid};

/// Density threshold: if set bits exceed this fraction of total range, use DenseBitVec.
const DENSITY_THRESHOLD: f64 = 0.125; // 12.5%

/// Bitmap filter for edge IDs — preselects which edges pass a property predicate.
#[derive(Debug)]
pub enum EidFilter {
    /// No predicate — all edges are allowed.
    AllAllowed,
    /// Dense bitvec indexed by raw EID.
    DenseBitVec(BitVec),
    /// Sparse hash set for low-cardinality results.
    Sparse(FxHashSet<u64>),
}

impl EidFilter {
    /// Check if an edge passes the filter.
    pub fn contains(&self, eid: Eid) -> bool {
        match self {
            Self::AllAllowed => true,
            Self::DenseBitVec(bv) => {
                let idx = eid.as_u64() as usize;
                if idx < bv.len() { bv[idx] } else { false }
            }
            Self::Sparse(set) => set.contains(&eid.as_u64()),
        }
    }

    /// Build an EidFilter from a list of matching EIDs.
    ///
    /// Uses a density heuristic to choose between DenseBitVec (>12.5% density)
    /// and Sparse HashSet.
    pub fn from_eids(eids: Vec<u64>, max_eid_hint: usize) -> Self {
        if eids.is_empty() {
            return Self::Sparse(FxHashSet::default());
        }

        let range = max_eid_hint.max(eids.iter().copied().max().unwrap_or(0) as usize + 1);
        let density = eids.len() as f64 / range.max(1) as f64;

        if density > DENSITY_THRESHOLD {
            let mut bv = BitVec::repeat(false, range);
            for &eid in &eids {
                let idx = eid as usize;
                if idx < bv.len() {
                    bv.set(idx, true);
                }
            }
            Self::DenseBitVec(bv)
        } else {
            Self::Sparse(eids.into_iter().collect())
        }
    }
}

/// Bitmap filter for vertex IDs — preselects which target vertices pass a property predicate.
#[derive(Debug)]
pub enum VidFilter {
    /// No predicate — all vertices are allowed.
    AllAllowed,
    /// Dense bitvec indexed by raw VID.
    DenseBitVec(BitVec),
    /// Sparse hash set for low-cardinality results.
    Sparse(FxHashSet<u64>),
}

impl VidFilter {
    /// Check if a vertex passes the filter.
    pub fn contains(&self, vid: Vid) -> bool {
        match self {
            Self::AllAllowed => true,
            Self::DenseBitVec(bv) => {
                let idx = vid.as_u64() as usize;
                if idx < bv.len() { bv[idx] } else { false }
            }
            Self::Sparse(set) => set.contains(&vid.as_u64()),
        }
    }

    /// Build a VidFilter from a list of matching VIDs.
    pub fn from_vids(vids: Vec<u64>, max_vid_hint: usize) -> Self {
        if vids.is_empty() {
            return Self::Sparse(FxHashSet::default());
        }

        let range = max_vid_hint.max(vids.iter().copied().max().unwrap_or(0) as usize + 1);
        let density = vids.len() as f64 / range.max(1) as f64;

        if density > DENSITY_THRESHOLD {
            let mut bv = BitVec::repeat(false, range);
            for &vid in &vids {
                let idx = vid as usize;
                if idx < bv.len() {
                    bv.set(idx, true);
                }
            }
            Self::DenseBitVec(bv)
        } else {
            Self::Sparse(vids.into_iter().collect())
        }
    }
}

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
