// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Per-row dependency tracking for Phase C C0 `TopKProofs`.
//!
//! A row's PROB column carries a [`crate::top_k_proofs::TopKTag`] —
//! a small set of [`Proof`s](crate::top_k_proofs::Proof). Each proof
//! depends on a set of base random variables (the underlying
//! probabilistic facts that supported the derivation). The DNF formed
//! by ORing each proof's conjunction of base RVs is the row's full
//! probability expression; its weight is computed by inclusion-exclusion.
//!
//! This module ships the compact representation: [`BaseRv`] identifies
//! a base random variable opaquely (the runtime resolves it from a
//! stable hash of the base fact); [`BaseRvSet`] is a small bitset / Vec
//! union for ≤ 64 RVs; [`DependencyDnf`] is the disjunction of clauses.
//!
//! See `crates/uni-locy/src/top_k_proofs.rs` for the semiring
//! integration, and impl plan §3.0 (decision D-7) for the design
//! rationale.

use std::collections::HashMap;

/// Identifier for a base probabilistic fact. Two `Proof`s sharing the
/// same `BaseRv` reference the SAME random variable: their truth
/// values are not independent, and any DNF containing both must use
/// inclusion-exclusion to avoid double-counting.
///
/// The identifier is opaque. The runtime is responsible for the
/// `BaseRv → row` mapping (typically a stable hash over the base
/// fact's primary key).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BaseRv(pub u32);

/// Compact set of [`BaseRv`]s.
///
/// Two representations:
/// * `Inline { bits, base }`: a 64-bit bitset over the range
///   `[base, base + 64)`. Constant-time membership / union /
///   intersect on the common path where all RVs in a proof fit in
///   one contiguous window.
/// * `Vec(v)`: sorted, deduplicated fallback for RVs that span more
///   than 64 IDs apart. Uses `O(n + m)` merge / intersect.
///
/// The set is always sorted and deduplicated in either form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BaseRvSet {
    Inline { bits: u64, base: u32 },
    Vec(Vec<u32>),
}

impl Default for BaseRvSet {
    fn default() -> Self {
        Self::empty()
    }
}

impl BaseRvSet {
    pub fn empty() -> Self {
        Self::Inline { bits: 0, base: 0 }
    }

    pub fn single(rv: BaseRv) -> Self {
        Self::Inline {
            bits: 1,
            base: rv.0,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Inline { bits, .. } => *bits == 0,
            Self::Vec(v) => v.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Inline { bits, .. } => bits.count_ones() as usize,
            Self::Vec(v) => v.len(),
        }
    }

    pub fn contains(&self, rv: BaseRv) -> bool {
        match self {
            Self::Inline { bits, base } => {
                let offset = rv.0.wrapping_sub(*base);
                offset < 64 && (*bits >> offset) & 1 == 1
            }
            Self::Vec(v) => v.binary_search(&rv.0).is_ok(),
        }
    }

    pub fn insert(&mut self, rv: BaseRv) {
        if self.contains(rv) {
            return;
        }
        match self {
            Self::Inline { bits, base } => {
                if *bits == 0 {
                    *base = rv.0;
                    *bits = 1;
                    return;
                }
                // Fits within the current window?
                if rv.0 >= *base && rv.0 - *base < 64 {
                    *bits |= 1u64 << (rv.0 - *base);
                    return;
                }
                // Spill to Vec.
                let mut all: Vec<u32> = self.iter().map(|r| r.0).collect();
                all.push(rv.0);
                all.sort_unstable();
                all.dedup();
                *self = Self::Vec(all);
            }
            Self::Vec(v) => {
                let pos = v.partition_point(|x| *x < rv.0);
                v.insert(pos, rv.0);
            }
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = BaseRv> + '_> {
        match self {
            Self::Inline { bits, base } => {
                let bits = *bits;
                let base = *base;
                Box::new(
                    (0u32..64)
                        .filter_map(move |i| ((bits >> i) & 1 == 1).then_some(BaseRv(base + i))),
                )
            }
            Self::Vec(v) => Box::new(v.iter().map(|x| BaseRv(*x))),
        }
    }

    /// Union of two sets. Result is canonicalized (inline when small,
    /// Vec when sparse).
    pub fn union(a: &Self, b: &Self) -> Self {
        let mut out = a.clone();
        for rv in b.iter() {
            out.insert(rv);
        }
        out
    }

    /// Returns true if `a` and `b` share at least one `BaseRv`. Used
    /// by `TopKProofs::plus` to decide whether pruning would cross a
    /// dependency edge.
    pub fn intersect_any(a: &Self, b: &Self) -> bool {
        match (a, b) {
            (Self::Inline { bits: ba, base: ea }, Self::Inline { bits: bb, base: eb }) => {
                if *ea == *eb {
                    return (*ba & *bb) != 0;
                }
                // Different windows — fall back to iteration. Avoids
                // computing window-shift constants for the rare cross-
                // window case.
                a.iter().any(|rv| b.contains(rv))
            }
            _ => a.iter().any(|rv| b.contains(rv)),
        }
    }

    /// True iff `a ⊆ b`. Order-preserving check.
    pub fn is_subset_of(a: &Self, b: &Self) -> bool {
        a.iter().all(|rv| b.contains(rv))
    }
}

/// Disjunctive normal form over [`BaseRv`]s. Each `BaseRvSet` is a
/// conjunction (`AND`); the DNF is the OR over those conjunctions.
///
/// The probability of a DNF is the disjoint-sum over its clauses,
/// computed via inclusion-exclusion:
///
/// ```text
/// P(DNF) = Σ_S⊆clauses, S≠∅ (−1)^(|S|+1) · P(∩ clauses in S)
/// ```
///
/// where `P(∩ clauses in S) = ∏ weight(rv) for rv in ∪ clauses in S`
/// (each base RV's probability appears at most once in the product
/// regardless of how many clauses reference it — that's the whole
/// point of inclusion-exclusion).
///
/// Inclusion-exclusion is exponential in `clauses.len()`. `TopKProofs`
/// bounds clause count at `K` (typically 4–16) so each `weight` call
/// materializes at most `2^K` subsets — fine for query-output paths.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependencyDnf {
    pub clauses: Vec<BaseRvSet>,
}

impl DependencyDnf {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn single_clause(clause: BaseRvSet) -> Self {
        Self {
            clauses: vec![clause],
        }
    }

    pub fn or_with(&mut self, other: &Self) {
        self.clauses.extend(other.clauses.iter().cloned());
    }

    /// Conjunction: every clause of self AND'd with every clause of
    /// other. The Cartesian product over clause sets.
    pub fn and_with(&mut self, other: &Self) {
        let mut new_clauses = Vec::with_capacity(self.clauses.len() * other.clauses.len());
        for a in &self.clauses {
            for b in &other.clauses {
                new_clauses.push(BaseRvSet::union(a, b));
            }
        }
        self.clauses = new_clauses;
    }

    /// Compute the exact probability of this DNF given per-RV
    /// independence-mode weights. `base_weights[rv]` is the marginal
    /// probability of the base random variable; missing entries are
    /// treated as 1.0 (the RV is always true — degenerate).
    ///
    /// Returns 0.0 for an empty DNF (no clauses can hold) and 1.0 for
    /// a DNF whose only clause is empty (the trivially-true proof).
    pub fn weight(&self, base_weights: &HashMap<BaseRv, f64>) -> f64 {
        if self.clauses.is_empty() {
            return 0.0;
        }
        let n = self.clauses.len();
        // 2^n − 1 non-empty subsets. We cap n at 24 here defensively
        // (16M iterations) to prevent runaway with adversarially-sized
        // top-k. Callers should keep K small.
        if n > 24 {
            // Conservative fallback: independence-mode noisy-OR over
            // per-clause weights. Documented in callers as a degraded
            // path.
            let mut complement = 1.0;
            for clause in &self.clauses {
                complement *= 1.0 - clause_weight(clause, base_weights);
            }
            return 1.0 - complement;
        }
        let mut acc = 0.0f64;
        for mask in 1u32..(1u32 << n) {
            // Union of the selected clauses.
            let mut union = BaseRvSet::empty();
            let mut bits = mask;
            while bits != 0 {
                let i = bits.trailing_zeros() as usize;
                union = BaseRvSet::union(&union, &self.clauses[i]);
                bits &= bits - 1;
            }
            let p = clause_weight(&union, base_weights);
            let popcount = mask.count_ones() as i32;
            let sign = if popcount % 2 == 1 { 1.0 } else { -1.0 };
            acc += sign * p;
        }
        // Clamp tiny negatives from floating-point error. The exact
        // value is provably in [0, 1].
        acc.clamp(0.0, 1.0)
    }
}

fn clause_weight(clause: &BaseRvSet, base_weights: &HashMap<BaseRv, f64>) -> f64 {
    if clause.is_empty() {
        return 1.0;
    }
    let mut p = 1.0;
    for rv in clause.iter() {
        // A missing marginal would silently read as weight 1.0 — "certain" —
        // which is the `locy_aggregates` shape. It is fenced off here because
        // the sole caller interns a weight for every RV it puts in a clause,
        // but this fn is `pub` and re-exported, so a future caller could break
        // that. Cheap insurance rather than a behaviour change: debug builds
        // and tests catch it, release keeps the existing default.
        debug_assert!(
            base_weights.contains_key(&rv),
            "clause_weight: no marginal for {rv:?}; a missing weight silently \
             reads as certainty"
        );
        let w = base_weights.get(&rv).copied().unwrap_or(1.0);
        p *= w;
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rvset(rvs: &[u32]) -> BaseRvSet {
        let mut s = BaseRvSet::empty();
        for r in rvs {
            s.insert(BaseRv(*r));
        }
        s
    }

    fn weights(pairs: &[(u32, f64)]) -> HashMap<BaseRv, f64> {
        pairs.iter().map(|(r, w)| (BaseRv(*r), *w)).collect()
    }

    #[test]
    fn rvset_empty_and_single() {
        let e = BaseRvSet::empty();
        assert!(e.is_empty());
        assert_eq!(e.len(), 0);
        let s = BaseRvSet::single(BaseRv(7));
        assert!(!s.is_empty());
        assert_eq!(s.len(), 1);
        assert!(s.contains(BaseRv(7)));
        assert!(!s.contains(BaseRv(8)));
    }

    #[test]
    fn rvset_insert_within_window() {
        let mut s = BaseRvSet::empty();
        s.insert(BaseRv(3));
        s.insert(BaseRv(5));
        s.insert(BaseRv(3)); // dup
        assert_eq!(s.len(), 2);
        assert!(s.contains(BaseRv(3)));
        assert!(s.contains(BaseRv(5)));
        let collected: Vec<u32> = s.iter().map(|r| r.0).collect();
        assert_eq!(collected, vec![3, 5]);
    }

    #[test]
    fn rvset_insert_outside_window_spills_to_vec() {
        let mut s = BaseRvSet::empty();
        s.insert(BaseRv(0));
        s.insert(BaseRv(100));
        assert!(matches!(s, BaseRvSet::Vec(_)));
        assert_eq!(s.len(), 2);
        assert!(s.contains(BaseRv(0)));
        assert!(s.contains(BaseRv(100)));
    }

    #[test]
    fn rvset_union_and_intersect() {
        let a = rvset(&[1, 3, 5]);
        let b = rvset(&[3, 5, 7]);
        let u = BaseRvSet::union(&a, &b);
        assert_eq!(u.len(), 4);
        for r in [1, 3, 5, 7] {
            assert!(u.contains(BaseRv(r)));
        }
        assert!(BaseRvSet::intersect_any(&a, &b));
        let c = rvset(&[2, 4]);
        assert!(!BaseRvSet::intersect_any(&a, &c));
    }

    #[test]
    fn rvset_subset() {
        let a = rvset(&[1, 3]);
        let b = rvset(&[1, 3, 5]);
        assert!(BaseRvSet::is_subset_of(&a, &b));
        assert!(!BaseRvSet::is_subset_of(&b, &a));
    }

    #[test]
    fn dnf_empty_is_zero() {
        let d = DependencyDnf::new();
        assert_eq!(d.weight(&HashMap::new()), 0.0);
    }

    #[test]
    fn dnf_single_empty_clause_is_one() {
        // A DNF whose only clause is the empty conjunction is the
        // tautology: P(true) = 1.0.
        let d = DependencyDnf::single_clause(BaseRvSet::empty());
        assert_eq!(d.weight(&HashMap::new()), 1.0);
    }

    #[test]
    fn dnf_single_rv_clause_returns_rv_weight() {
        let d = DependencyDnf::single_clause(rvset(&[1]));
        let w = weights(&[(1, 0.42)]);
        assert!((d.weight(&w) - 0.42).abs() < 1e-12);
    }

    #[test]
    fn dnf_independent_clauses_match_noisy_or() {
        // P(A ∨ B) = 1 − (1−P(A))(1−P(B)) when A and B don't share RVs.
        let d = DependencyDnf {
            clauses: vec![rvset(&[1]), rvset(&[2])],
        };
        let w = weights(&[(1, 0.3), (2, 0.5)]);
        let expected = 1.0 - (1.0 - 0.3) * (1.0 - 0.5);
        assert!(
            (d.weight(&w) - expected).abs() < 1e-12,
            "got {}",
            d.weight(&w)
        );
    }

    #[test]
    fn dnf_shared_rv_corrects_for_overlap() {
        // P({A,B} ∨ {A,C}) using inclusion-exclusion:
        //   = P(A∧B) + P(A∧C) − P(A∧B∧C)
        //   = P(A)P(B) + P(A)P(C) − P(A)P(B)P(C)
        // Compare to naive noisy-OR which assumes independence and
        // over-counts the shared A.
        let d = DependencyDnf {
            clauses: vec![rvset(&[1, 2]), rvset(&[1, 3])],
        };
        let w = weights(&[(1, 0.5), (2, 0.4), (3, 0.6)]);
        let p_a = 0.5;
        let p_b = 0.4;
        let p_c = 0.6;
        let expected = p_a * p_b + p_a * p_c - p_a * p_b * p_c;
        assert!((d.weight(&w) - expected).abs() < 1e-12);
        // Naive independence (wrong) would compute 1 − (1−0.2)(1−0.3)
        // = 0.44; exact answer is 0.5 · 0.4 + 0.5 · 0.6 − 0.5 · 0.4 · 0.6
        // = 0.20 + 0.30 − 0.12 = 0.38.
        assert!((d.weight(&w) - 0.38).abs() < 1e-12);
    }

    #[test]
    fn dnf_and_with_distributes() {
        // (A ∨ B) ∧ (C ∨ D) = (A∧C) ∨ (A∧D) ∨ (B∧C) ∨ (B∧D)
        let mut a = DependencyDnf {
            clauses: vec![rvset(&[1]), rvset(&[2])],
        };
        let b = DependencyDnf {
            clauses: vec![rvset(&[3]), rvset(&[4])],
        };
        a.and_with(&b);
        assert_eq!(a.clauses.len(), 4);
    }

    #[test]
    fn dnf_or_with_concatenates() {
        let mut a = DependencyDnf::single_clause(rvset(&[1]));
        let b = DependencyDnf::single_clause(rvset(&[2]));
        a.or_with(&b);
        assert_eq!(a.clauses.len(), 2);
    }
}
