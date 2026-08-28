// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Single source of truth for parsing vector-index options into a
//! [`VectorIndexType`] + [`DistanceMetric`].
//!
//! ALL index-creation entry points use these helpers so dense vectors, native
//! multi-vectors, and MUVERA behave **identically** regardless of path:
//! - the Cypher DDL `CREATE VECTOR INDEX ... OPTIONS {type:'...', ...}` (`planner.rs`),
//! - the `uni.schema.createIndex(...)` procedure (`executor::ddl_procedures`), and
//! - the Python binding config map (`bindings/uni-db/src/core.rs`).
//!
//! (The typed Rust builder `VectorAlgo` in the `uni` crate maps directly to the same
//! `VectorIndexType`.) Lives in `uni-common` — the only crate every surface depends on.
//! Keeping the mapping here prevents the paths from drifting (they previously had
//! different default ANN types: `ivf_pq` vs `hnsw`).

use anyhow::Result;

use crate::core::schema::{DistanceMetric, VectorIndexType};
use crate::muvera::DEFAULT_FDE_SEED;

/// Raw, already-typed vector-index options collected from either entry point. Each
/// field is the user-supplied value or `None` (→ the canonical default below).
#[derive(Debug, Default, Clone)]
pub struct VectorIndexOpts<'a> {
    /// The ANN/index subtype name (`flat`, `ivf_pq`, `hnsw_sq`, `muvera`, …). For the
    /// DDL path this is `OPTIONS.type`; for the procedure it is the `algorithm` field.
    pub type_name: Option<&'a str>,
    pub partitions: Option<u32>,
    pub m: Option<u32>,
    pub ef_construction: Option<u32>,
    pub sub_vectors: Option<u32>,
    pub num_bits: Option<u8>,
    // MUVERA-only knobs.
    pub k_sim: Option<u32>,
    pub reps: Option<u32>,
    pub d_proj: Option<u32>,
    pub seed: Option<u64>,
    /// The single-vector ANN type built over the MUVERA FDE column.
    pub inner: Option<&'a str>,
}

/// Sentinel in `VectorIndexType::{IvfPq, HnswPq}.num_sub_vectors` meaning "not
/// specified by the caller; resolve from the column's dimensionality".
///
/// Resolved by `resolve_auto_sub_vectors` before the index definition is
/// persisted, so a stored schema never contains it. `0` is safe as a sentinel:
/// it is not a legal user value, and `IndexManager`'s divisibility check already
/// guards `sub != 0`.
pub const AUTO_SUB_VECTORS: u32 = 0;

/// PQ sub-vector count to use when the caller did not specify one.
///
/// PQ encodes each vector as `sub_vectors` bytes (at the default 8 bits per
/// sub-vector), so the compression ratio is `dim * 4 / sub_vectors`. A fixed
/// default is therefore *dimension-blind*: 16 gives 32x at 128-d but 192x at
/// 768-d and 384x at 1536-d, i.e. recall degrades with exactly the widths modern
/// embedding models use. Measured on SIFT-1M and GIST-1M, recall@10 without a
/// refine pass falls 0.878 (8x) -> 0.558 (32x) -> 0.300 (240x); see
/// `docs/perf/refine-policy-2026-08-26.md`.
///
/// This targets **<= ~32x** compression while respecting two hard constraints:
///
/// * `sub_vectors` must **divide** `dim` — `IndexManager` rejects an index where
///   `dim >= sub && dim % sub != 0`, so a plain `dim / 8` would make index
///   creation fail outright for many dimensions (e.g. 100).
/// * small dimensions keep returning 16, preserving the deliberate `sub > dim`
///   case that lets an index be declared on an empty table.
///
/// Returns 16 when `dim` is unknown, below 128, or has no divisor in range (a
/// prime), which leaves today's behaviour untouched in each of those cases.
#[must_use]
pub fn default_sub_vectors(dim: Option<usize>) -> u32 {
    const FALLBACK: u32 = 16;
    let Some(dim) = dim else {
        return FALLBACK;
    };
    // Below this the fixed 16 is already <= 32x (or intentionally exceeds dim).
    if dim < 128 {
        return FALLBACK;
    }
    // Smallest divisor of `dim` that is >= dim/8 => the least compression at or
    // above ~32x that PQ can actually encode. Capped at dim/2 so a prime falls
    // back rather than degenerating to `sub == dim` (no compression at all).
    let lower = dim.div_ceil(8);
    (lower..=dim / 2)
        .find(|c| dim.is_multiple_of(*c))
        .map_or(FALLBACK, |c| c as u32)
}

/// Default `refine_factor` for a quantized index, derived from how hard it
/// compresses. `None` for index types that store full-precision vectors.
///
/// A refine pass over-fetches candidates and re-scores them against the original
/// vectors, which is what recovers the recall quantization costs. The heavier the
/// compression, the more candidates it takes: measured recall@10 without refine
/// runs 0.878 at 8x, 0.558 at 32x and 0.300 at 240x, and the refine needed to
/// reach ~0.99 grows with it (see `docs/perf/refine-policy-2026-08-26.md`).
///
/// Bounds are deliberate. The floor of 8 covers the mildest quantization at
/// negligible cost; the ceiling of 32 stops a legacy index at extreme
/// compression from turning every query into a near-exhaustive rescan — such an
/// index wants rebuilding at a dimension-aware `sub_vectors`, and no query-time
/// knob fully rescues it.
#[must_use]
pub fn default_refine_factor(index_type: &VectorIndexType, dim: Option<usize>) -> Option<u32> {
    // Floor chosen from measurement, not roundness: at the 32x this codebase
    // now targets, refine=5 measured 0.90 recall@10 and refine=8 measured 0.944,
    // both short of the 0.95 bar; 12 clears it with margin. See
    // `docs/perf/refine-policy-2026-08-26.md`.
    const MIN: u32 = 12;
    const MAX: u32 = 32;

    let ratio = match index_type {
        VectorIndexType::IvfPq {
            num_sub_vectors, ..
        }
        | VectorIndexType::HnswPq {
            num_sub_vectors, ..
        } => {
            // Unresolved sentinel or unknown dim: assume the ~32x this codebase
            // now targets rather than skipping the default entirely.
            match (dim, *num_sub_vectors) {
                (Some(d), sub) if sub != AUTO_SUB_VECTORS && sub != 0 => (d * 4) as u32 / sub,
                _ => 32,
            }
        }
        // Scalar and RaBitQ quantization are deliberately left alone. They are
        // far milder than PQ (f32 -> ~1 byte per dimension, ~4x), and the recall
        // ceiling this default exists to lift was measured on **PQ only** — an
        // HNSW-SQ index already reaches 0.98 recall through `ef_search` alone.
        // Defaulting a refine pass here would flatten that curve (it re-scores
        // exactly, so recall stops responding to `ef_search`) on the strength of
        // no measurement, which is the mistake this whole change exists to undo.
        // Revisit with data, not by analogy.
        VectorIndexType::IvfSq { .. }
        | VectorIndexType::IvfRq { .. }
        | VectorIndexType::HnswSq { .. } => return None,
        // The inner ANN over the derived FDE column is what actually gets
        // probed, so `dim` here must already BE the FDE width — callers that can
        // reach `fde_spec_for_config` pass it. Its width depends on `d_proj` and,
        // when that is 0, on the source column, neither of which is reachable
        // from this crate; a caller that cannot compute it passes `None` and the
        // PQ arm assumes the ~32x this codebase now targets.
        VectorIndexType::Muvera { inner, .. } => {
            return default_refine_factor(inner, dim);
        }
        // Flat, IvfFlat, HnswFlat and anything else store full precision: a
        // refine pass would re-score against the same values it already ranked.
        _ => return None,
    };

    Some((ratio / 4).clamp(MIN, MAX))
}

/// Map a single-vector ANN type name to a [`VectorIndexType`], defaulting to `IvfPq`.
/// Shared by the outer index type and the MUVERA `inner` type.
fn ann_type(o: &VectorIndexOpts, t: Option<&str>) -> VectorIndexType {
    match t {
        Some("flat") => VectorIndexType::Flat,
        Some("ivf_flat") => VectorIndexType::IvfFlat {
            num_partitions: o.partitions.unwrap_or(256),
        },
        Some("ivf_sq") => VectorIndexType::IvfSq {
            num_partitions: o.partitions.unwrap_or(256),
        },
        Some("ivf_rq") => VectorIndexType::IvfRq {
            num_partitions: o.partitions.unwrap_or(256),
            num_bits: o.num_bits,
        },
        Some("hnsw_flat") => VectorIndexType::HnswFlat {
            m: o.m.unwrap_or(16),
            ef_construction: o.ef_construction.unwrap_or(200),
            num_partitions: o.partitions,
        },
        Some("hnsw" | "hnsw_sq") => VectorIndexType::HnswSq {
            m: o.m.unwrap_or(16),
            ef_construction: o.ef_construction.unwrap_or(200),
            num_partitions: o.partitions,
        },
        Some("hnsw_pq") => VectorIndexType::HnswPq {
            m: o.m.unwrap_or(16),
            ef_construction: o.ef_construction.unwrap_or(200),
            num_sub_vectors: o.sub_vectors.unwrap_or(AUTO_SUB_VECTORS),
            num_partitions: o.partitions,
        },
        // None / unknown → IVF_PQ (the canonical default for BOTH paths).
        _ => VectorIndexType::IvfPq {
            num_partitions: o.partitions.unwrap_or(256),
            num_sub_vectors: o.sub_vectors.unwrap_or(AUTO_SUB_VECTORS),
            bits_per_subvector: o.num_bits.unwrap_or(8),
        },
    }
}

/// Build a [`VectorIndexType`] from raw options. `type:'muvera'` produces a MUVERA index
/// whose `inner` ANN (over the derived FDE column) is itself parsed via the private
/// `ann_type` helper.
///
/// NOTE: the MUVERA defaults below (`k_sim=4, reps=20, d_proj=16`) are reasonable starting
/// points, NOT values validated for recall on a specific corpus. FDE recall is
/// corpus-dependent; tune these per corpus and confirm recall@k with the bench harness
/// `crates/uni-store/examples/multivec_recall_real.rs` (real ColBERT corpus) before relying
/// on the first-stage retrieval quality.
pub fn build_vector_index_type(o: &VectorIndexOpts) -> VectorIndexType {
    match o.type_name {
        Some("muvera") => VectorIndexType::Muvera {
            k_sim: o.k_sim.unwrap_or(4),
            reps: o.reps.unwrap_or(20),
            d_proj: o.d_proj.unwrap_or(16),
            seed: o.seed.unwrap_or(DEFAULT_FDE_SEED),
            inner: Box::new(ann_type(o, o.inner)),
        },
        other => ann_type(o, other),
    }
}

/// Parse a vector distance-metric name; errors on an unknown value. `None` → `Cosine`
/// (the ColBERT/vector default). Shared by both paths so the error text matches.
pub fn parse_vector_metric(s: Option<&str>) -> Result<DistanceMetric> {
    match s.map(|m| m.to_ascii_lowercase()).as_deref() {
        Some("l2" | "euclidean") => Ok(DistanceMetric::L2),
        Some("dot") => Ok(DistanceMetric::Dot),
        Some("l1" | "manhattan") => Ok(DistanceMetric::L1),
        Some("hamming") => Ok(DistanceMetric::Hamming),
        Some("jaccard") => Ok(DistanceMetric::Jaccard),
        Some("cosine") | None => Ok(DistanceMetric::Cosine),
        Some(other) => Err(anyhow::anyhow!(
            "Unknown vector index metric '{other}' \
             (expected cosine, l2, dot, l1, hamming, or jaccard)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(type_name: Option<&str>) -> VectorIndexOpts<'_> {
        VectorIndexOpts {
            type_name,
            ..Default::default()
        }
    }

    #[test]
    fn default_sub_vectors_targets_32x_and_always_divides() {
        // Unknown / small dims keep today's 16. dim 8 is the case pinned by
        // `vector_index_options_test.rs`, and is the deliberate `sub > dim`
        // allowance that lets an index be declared on an empty table.
        assert_eq!(default_sub_vectors(None), 16);
        assert_eq!(default_sub_vectors(Some(8)), 16);
        assert_eq!(default_sub_vectors(Some(127)), 16);

        // At and above 128 the ratio is held near 32x.
        for (dim, want) in [
            (128usize, 16u32), // unchanged: 128*4/16 == 32x
            (256, 32),
            (384, 48),
            (768, 96),
            (960, 120),
            (1536, 192),
        ] {
            assert_eq!(default_sub_vectors(Some(dim)), want, "dim {dim}");
            assert!(
                (dim as u32).is_multiple_of(want),
                "dim {dim}: {want} must divide it"
            );
            assert!(
                (dim * 4) as u32 / want <= 32,
                "dim {dim}: compression must stay <= 32x"
            );
        }

        // A dimension with no divisor in [dim/8, dim/2] falls back rather than
        // degenerating to `sub == dim`, which would not compress at all.
        assert_eq!(default_sub_vectors(Some(1031)), 16); // prime

        // For every dim this rule *acts on* (>= 128), the result must be
        // encodable: it divides `dim`, or it is the 16 fallback.
        //
        // The fallback is deliberate rather than a gap. A dim with no divisor in
        // range (17, 1031, ...) has no good PQ split, and 16 then surfaces
        // IndexManager's existing "num_sub_vectors must divide the embedding
        // dimension" error — a loud, actionable failure. Substituting the
        // largest divisor instead would silently build a 1-byte-per-vector index
        // with dismal recall, which is the failure mode this whole change exists
        // to remove. Note this is unchanged from the previous fixed default.
        for dim in 128usize..4096 {
            let sub = default_sub_vectors(Some(dim)) as usize;
            assert!(
                dim.is_multiple_of(sub) || sub == 16,
                "dim {dim}: sub_vectors {sub} must divide dim or be the 16 fallback"
            );
            if dim.is_multiple_of(sub) {
                assert!(
                    (dim * 4) / sub <= 32,
                    "dim {dim}: sub_vectors {sub} leaves compression above 32x"
                );
            }
        }
    }

    #[test]
    fn default_is_ivf_pq_for_both_paths() {
        // None and unknown names both default to IVF_PQ (the canonical default).
        assert!(matches!(
            build_vector_index_type(&opts(None)),
            VectorIndexType::IvfPq { .. }
        ));
        assert!(matches!(
            build_vector_index_type(&opts(Some("nonsense"))),
            VectorIndexType::IvfPq { .. }
        ));
    }

    #[test]
    fn named_types_map() {
        assert!(matches!(
            build_vector_index_type(&opts(Some("flat"))),
            VectorIndexType::Flat
        ));
        assert!(matches!(
            build_vector_index_type(&opts(Some("hnsw"))),
            VectorIndexType::HnswSq { .. }
        ));
    }

    #[test]
    fn muvera_defaults_and_inner() {
        let o = VectorIndexOpts {
            type_name: Some("muvera"),
            inner: Some("flat"),
            ..Default::default()
        };
        match build_vector_index_type(&o) {
            VectorIndexType::Muvera {
                k_sim,
                reps,
                d_proj,
                seed,
                inner,
            } => {
                assert_eq!((k_sim, reps, d_proj), (4, 20, 16));
                assert_eq!(seed, DEFAULT_FDE_SEED);
                assert!(matches!(*inner, VectorIndexType::Flat));
            }
            other => panic!("expected Muvera, got {other:?}"),
        }
        // Default inner is IVF_PQ.
        assert!(matches!(
            build_vector_index_type(&opts(Some("muvera"))),
            VectorIndexType::Muvera { inner, .. } if matches!(*inner, VectorIndexType::IvfPq { .. })
        ));
    }

    #[test]
    fn metric_parsing() {
        assert_eq!(parse_vector_metric(None).unwrap(), DistanceMetric::Cosine);
        assert_eq!(parse_vector_metric(Some("L2")).unwrap(), DistanceMetric::L2);
        assert_eq!(
            parse_vector_metric(Some("dot")).unwrap(),
            DistanceMetric::Dot
        );
        assert_eq!(parse_vector_metric(Some("l1")).unwrap(), DistanceMetric::L1);
        assert_eq!(
            parse_vector_metric(Some("hamming")).unwrap(),
            DistanceMetric::Hamming
        );
        assert_eq!(
            parse_vector_metric(Some("jaccard")).unwrap(),
            DistanceMetric::Jaccard
        );
        assert!(parse_vector_metric(Some("no_such_metric")).is_err());
    }
}
