// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Sets of code points as sorted, disjoint, non-adjacent inclusive ranges.
//!
//! Every single-code-point matcher (a literal, a class, a property, `.`, a
//! predefined class) compiles to one of these, with case-insensitivity
//! already folded in, so matching is one membership test on the code point
//! read at the current position (spec SUR-1). The universe is
//! `0..=0x10FFFF`, surrogates included: a lone surrogate is a code point a
//! set may contain (spec PROP-8, SUR-1).

/// The largest code point.
pub(crate) const MAX_CP: u32 = 0x10FFFF;

/// A set of code points.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct CodeSet {
    ranges: Vec<(u32, u32)>,
}

impl CodeSet {
    /// The empty set.
    pub(crate) fn empty() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Every code point.
    pub(crate) fn all() -> Self {
        Self {
            ranges: vec![(0, MAX_CP)],
        }
    }

    /// The set holding `cp` alone.
    pub(crate) fn single(cp: u32) -> Self {
        Self {
            ranges: vec![(cp, cp)],
        }
    }

    /// The set `lo..=hi` (empty when `lo > hi`).
    pub(crate) fn range(lo: u32, hi: u32) -> Self {
        if lo > hi {
            Self::empty()
        } else {
            Self {
                ranges: vec![(lo, hi.min(MAX_CP))],
            }
        }
    }

    /// A set from arbitrary (possibly overlapping, unsorted) ranges.
    pub(crate) fn from_ranges(mut ranges: Vec<(u32, u32)>) -> Self {
        ranges.retain(|&(a, b)| a <= b);
        ranges.sort_unstable();
        let mut out: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
        for (a, b) in ranges {
            match out.last_mut() {
                Some(last) if a <= last.1.saturating_add(1) => {
                    if b > last.1 {
                        last.1 = b;
                    }
                }
                _ => out.push((a, b)),
            }
        }
        Self { ranges: out }
    }

    /// A set from a static range table (already sorted and disjoint, but
    /// possibly adjacent).
    pub(crate) fn from_table(table: &[(u32, u32)]) -> Self {
        Self::from_ranges(table.to_vec())
    }

    /// A set from code points.
    pub(crate) fn from_cps(cps: impl IntoIterator<Item = u32>) -> Self {
        Self::from_ranges(cps.into_iter().map(|c| (c, c)).collect())
    }

    /// The ranges, sorted and disjoint.
    pub(crate) fn ranges(&self) -> &[(u32, u32)] {
        &self.ranges
    }

    /// The code point when the set holds exactly one.
    pub(crate) fn as_single(&self) -> Option<u32> {
        match self.ranges.as_slice() {
            [(a, b)] if a == b => Some(*a),
            _ => None,
        }
    }

    /// Whether `cp` is in the set.
    #[inline]
    pub(crate) fn contains(&self, cp: u32) -> bool {
        let r = &self.ranges;
        if r.len() <= 4 {
            return r.iter().any(|&(a, b)| a <= cp && cp <= b);
        }
        // Index of the first range whose start is past `cp`.
        let i = r.partition_point(|&(a, _)| a <= cp);
        i > 0 && cp <= r[i - 1].1
    }

    /// The union of two sets.
    pub(crate) fn union(&self, other: &CodeSet) -> CodeSet {
        let mut v = self.ranges.clone();
        v.extend_from_slice(&other.ranges);
        Self::from_ranges(v)
    }

    /// The complement within `0..=0x10FFFF`.
    pub(crate) fn negate(&self) -> CodeSet {
        let mut out = Vec::with_capacity(self.ranges.len() + 1);
        let mut next = 0u32;
        for &(a, b) in &self.ranges {
            if a > next {
                out.push((next, a - 1));
            }
            next = b + 1;
        }
        if next <= MAX_CP {
            out.push((next, MAX_CP));
        }
        CodeSet { ranges: out }
    }

    /// The intersection of two sets.
    pub(crate) fn intersect(&self, other: &CodeSet) -> CodeSet {
        let (a, b) = (&self.ranges, &other.ranges);
        let (mut i, mut j) = (0, 0);
        let mut out = Vec::new();
        while i < a.len() && j < b.len() {
            let lo = a[i].0.max(b[j].0);
            let hi = a[i].1.min(b[j].1);
            if lo <= hi {
                out.push((lo, hi));
            }
            if a[i].1 < b[j].1 {
                i += 1;
            } else {
                j += 1;
            }
        }
        CodeSet { ranges: out }
    }

    /// The set without `other`.
    pub(crate) fn difference(&self, other: &CodeSet) -> CodeSet {
        self.intersect(&other.negate())
    }

    /// FNV-1a over the range ends as little-endian `u32`s: the fingerprint
    /// `GenUcdTables.java` records for the sets Java's `Pattern` matches.
    #[cfg(test)]
    pub(crate) fn fingerprint(&self) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &(a, b) in &self.ranges {
            for v in [a, b] {
                for byte in v.to_le_bytes() {
                    h ^= u64::from(byte);
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
        }
        h
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_algebra() {
        let a = CodeSet::from_ranges(vec![(5, 10), (1, 3), (4, 4), (20, 30)]);
        assert_eq!(a.ranges(), &[(1, 10), (20, 30)]);
        assert!(a.contains(1) && a.contains(10) && !a.contains(11) && a.contains(25));
        let n = a.negate();
        assert_eq!(n.ranges(), &[(0, 0), (11, 19), (31, MAX_CP)]);
        assert_eq!(n.negate(), a);
        let b = CodeSet::range(8, 22);
        assert_eq!(a.intersect(&b).ranges(), &[(8, 10), (20, 22)]);
        assert_eq!(a.difference(&b).ranges(), &[(1, 7), (23, 30)]);
        assert_eq!(a.union(&b).ranges(), &[(1, 30)]);
        assert!(CodeSet::range(3, 2).ranges().is_empty());
        assert_eq!(CodeSet::single(7).as_single(), Some(7));
    }
}
