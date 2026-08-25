//! Property tests for `uni-sparse-vector` (test set A from the #95 plan):
//! codec round-trips, constructor robustness, and a reference cross-check of the
//! merge-join `sparse_dot` against a naive hash-map computation.

use proptest::collection::vec;
use proptest::prelude::*;
use std::collections::HashMap;
use uni_sparse_vector::SparseVector;
use uni_sparse_vector::encode::{decode_slice, encode};
use uni_sparse_vector::ops::{l2_norm, prune_top_k, sparse_dot};

/// Finite, bounded weights so generated vectors are always valid.
fn weight() -> impl Strategy<Value = f32> {
    -1.0e6f32..1.0e6f32
}

/// Arbitrary `(term_id, weight)` pairs. Term ids drawn from a bounded range so
/// independently-generated vectors overlap often enough to exercise dot scoring.
fn pairs() -> impl Strategy<Value = Vec<(u32, f32)>> {
    vec((0u32..2_000u32, weight()), 0..64)
}

/// A valid sparse vector built through `from_pairs` (sorts + sums duplicates).
fn sparse_vector() -> impl Strategy<Value = SparseVector> {
    pairs().prop_map(|p| SparseVector::from_pairs(p).expect("finite weights -> valid"))
}

/// Naive reference dot product via a hash map — independent of the merge-join.
fn reference_dot(a: &SparseVector, b: &SparseVector) -> f32 {
    let map: HashMap<u32, f32> = a.iter().collect();
    b.iter()
        .filter_map(|(t, w)| map.get(&t).map(|aw| aw * w))
        .sum()
}

/// Cases per property under miri.
///
/// Matches the `PROPTEST_CASES: "16"` the miri CI job sets, which never
/// actually arrives — see [`miri_safe_config`].
const MIRI_CASES: u32 = 16;

/// Proptest config that survives miri's isolation.
///
/// Two separate things break under `-Zmiri-isolation` (which the codec crates
/// run with deliberately, so a new syscall dependency surfaces rather than
/// passing quietly), and both are silent:
///
/// **1. The target aborted before checking anything.** Proptest resolves
/// `file!()` — always a relative path — through `std::env::current_dir` to
/// locate `.proptest-regressions`, and does so in `load_persisted_failures2` at
/// *runner startup*, before a single case runs. Under isolation that is a
/// blocked `getcwd`, so the whole target died having verified nothing. It had
/// been failing that way since this file was added, which is why the lane's
/// recorded timings never included these properties: they never ran.
///
/// Disabling persistence under miri turns off only the reading and writing of
/// `.proptest-regressions`. All eight properties still run — the point of
/// keeping this crate in the lane — and a failing seed is still printed.
///
/// **2. `PROPTEST_CASES` does not reach proptest under isolation.** Miri hides
/// the environment, so `std::env::var("PROPTEST_CASES")` is `Err(NotPresent)`
/// and `ProptestConfig::default()` falls back to proptest's built-in 256 rather
/// than the 16 the CI job asks for. Measured: 16 cases takes ~90 s for
/// `--lib --tests`; letting it default to 256 takes over 20 minutes, against
/// that job's 20-minute budget. So the count is set here explicitly instead of
/// through an env var that cannot be read.
///
/// Outside miri both fields keep their normal behaviour, including honouring
/// `PROPTEST_CASES`.
fn miri_safe_config() -> ProptestConfig {
    ProptestConfig {
        failure_persistence: if cfg!(miri) {
            None
        } else {
            ProptestConfig::default().failure_persistence
        },
        cases: if cfg!(miri) {
            MIRI_CASES
        } else {
            ProptestConfig::default().cases
        },
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(miri_safe_config())]

    #[test]
    fn from_pairs_yields_valid_sorted_unique(p in pairs()) {
        let sv = SparseVector::from_pairs(p).unwrap();
        // Strictly ascending (sorted + unique).
        for w in sv.indices().windows(2) {
            prop_assert!(w[0] < w[1]);
        }
        prop_assert_eq!(sv.indices().len(), sv.values().len());
        // Reconstructing via the strict ctor must succeed on from_pairs output.
        prop_assert!(SparseVector::new(sv.indices().to_vec(), sv.values().to_vec()).is_ok());
    }

    #[test]
    fn encode_decode_roundtrip(sv in sparse_vector()) {
        let bytes = encode(&sv);
        let decoded = decode_slice(&bytes).unwrap();
        prop_assert_eq!(sv, decoded);
    }

    #[test]
    fn encoded_length_is_exact(sv in sparse_vector()) {
        prop_assert_eq!(encode(&sv).len(), 4 + sv.len() * 8);
    }

    #[test]
    fn truncating_any_byte_is_rejected(sv in sparse_vector()) {
        let bytes = encode(&sv);
        if !bytes.is_empty() {
            let mut t = bytes.clone();
            t.truncate(bytes.len() - 1);
            // A one-byte-short buffer can never be a well-formed payload.
            prop_assert!(decode_slice(&t).is_err());
        }
    }

    #[test]
    fn dot_matches_reference(a in sparse_vector(), b in sparse_vector()) {
        let merge = sparse_dot(&a, &b);
        let reference = reference_dot(&a, &b);
        // Floating-point summation order differs; allow a relative tolerance.
        let tol = 1e-2 + 1e-4 * reference.abs();
        prop_assert!((merge - reference).abs() <= tol,
            "merge={merge} reference={reference}");
    }

    #[test]
    fn dot_is_commutative(a in sparse_vector(), b in sparse_vector()) {
        prop_assert_eq!(sparse_dot(&a, &b), sparse_dot(&b, &a));
    }

    #[test]
    fn self_dot_equals_l2_norm_squared(sv in sparse_vector()) {
        let dot = sparse_dot(&sv, &sv);
        let norm_sq = l2_norm(&sv).powi(2);
        let tol = 1e-1 + 1e-4 * norm_sq.abs();
        prop_assert!((dot - norm_sq).abs() <= tol, "dot={dot} norm_sq={norm_sq}");
    }

    #[test]
    fn prune_is_valid_subset(sv in sparse_vector(), k in 0usize..80usize) {
        let pruned = prune_top_k(&sv, k);
        prop_assert_eq!(pruned.len(), k.min(sv.len()));
        // Every kept (term, weight) must appear in the original.
        let original: HashMap<u32, f32> = sv.iter().collect();
        for (t, w) in pruned.iter() {
            prop_assert_eq!(original.get(&t).copied(), Some(w));
        }
        // Invariant preserved.
        for win in pruned.indices().windows(2) {
            prop_assert!(win[0] < win[1]);
        }
    }
}
