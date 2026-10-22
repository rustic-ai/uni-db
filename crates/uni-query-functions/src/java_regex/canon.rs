// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bracket classes and properties under the inline `(?c)` flag.
//!
//! The spec (FLG-6) records only that `(?c)` leaves literals alone. The
//! rest was established by black-box probes of JDK 21 (recorded in
//! `PROVENANCE.md`): under `(?c)` a bracket class or a `\p{…}`/`\P{…}`
//! property — not a literal, `.`, `\X`, `\R` or a predefined class like
//! `\w` — matches as follows at a position:
//!
//! * take the grapheme cluster starting there (computed fresh, as `\X`
//!   does);
//! * if it is one code point, test that code point;
//! * otherwise try its prefixes of two or more code points, longest first:
//!   a prefix matches when its NFC is a single code point in the class.
//!   Shorter prefixes are alternatives the matcher backtracks into; a
//!   one-code-point prefix is never tried.

use std::collections::HashMap;
use std::sync::OnceLock;

use super::grapheme::{cluster_end, cp_at};
use super::ucd;
use super::ucd_tables as t;

const S_BASE: u32 = 0xAC00;
const L_BASE: u32 = 0x1100;
const V_BASE: u32 = 0x1161;
const T_BASE: u32 = 0x11A7;
const L_COUNT: u32 = 19;
const V_COUNT: u32 = 21;
const T_COUNT: u32 = 28;
const N_COUNT: u32 = V_COUNT * T_COUNT;
const S_COUNT: u32 = L_COUNT * N_COUNT;

/// Appends the full canonical decomposition of `cp`.
fn decompose(cp: u32, out: &mut Vec<u32>) {
    if (S_BASE..S_BASE + S_COUNT).contains(&cp) {
        let s = cp - S_BASE;
        out.push(L_BASE + s / N_COUNT);
        out.push(V_BASE + (s % N_COUNT) / T_COUNT);
        if !s.is_multiple_of(T_COUNT) {
            out.push(T_BASE + s % T_COUNT);
        }
        return;
    }
    match t::DECOMPOSITIONS.binary_search_by_key(&cp, |&(c, _, _)| c) {
        Ok(i) => out.extend_from_slice(t::DECOMPOSITIONS[i].1),
        Err(_) => out.push(cp),
    }
}

/// The NFD of `cps`: decomposed, then canonically ordered.
fn nfd(cps: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(cps.len() + 2);
    for &c in cps {
        decompose(c, &mut out);
    }
    // Canonical ordering: a stable sort of each run of non-starters.
    let mut i = 0;
    while i < out.len() {
        if ucd::ccc(out[i]) == 0 {
            i += 1;
            continue;
        }
        let start = i;
        while i < out.len() && ucd::ccc(out[i]) != 0 {
            i += 1;
        }
        out[start..i].sort_by_key(|&c| ucd::ccc(c));
    }
    out
}

/// Every code point that is its own NFC and decomposes, by its NFD.
fn composites() -> &'static HashMap<Vec<u32>, u32> {
    static CELL: OnceLock<HashMap<Vec<u32>, u32>> = OnceLock::new();
    CELL.get_or_init(|| {
        t::DECOMPOSITIONS
            .iter()
            .filter(|&&(cp, d, nfc)| nfc == cp && d.len() >= 2)
            .map(|&(cp, d, _)| (d.to_vec(), cp))
            .collect()
    })
}

/// The single code point `cps` normalizes to under NFC, if it is one.
fn nfc_single(cps: &[u32]) -> Option<u32> {
    let d = nfd(cps);
    // Hangul syllables compose algorithmically.
    if let [l, v, rest @ ..] = d.as_slice()
        && (L_BASE..L_BASE + L_COUNT).contains(l)
        && (V_BASE..V_BASE + V_COUNT).contains(v)
    {
        let lv = S_BASE + ((l - L_BASE) * V_COUNT + (v - V_BASE)) * T_COUNT;
        return match rest {
            [] => Some(lv),
            [t] if (T_BASE + 1..T_BASE + T_COUNT).contains(t) => Some(lv + (t - T_BASE)),
            _ => None,
        };
    }
    composites().get(&d).copied()
}

/// The ways a `(?c)` class can match at `pos`: `(end, code point tested)`,
/// in the order they are tried. A single-code-point cluster gives its own
/// code point; a longer one gives each composing prefix, longest first.
pub(crate) fn candidates(text: &[u16], pos: usize) -> Vec<(usize, u32)> {
    let end = cluster_end(text, pos);
    let mut cps = Vec::new();
    let mut ends = Vec::new();
    let mut i = pos;
    while i < end {
        let (c, w) = cp_at(text, i);
        cps.push(c);
        i += w;
        ends.push(i);
    }
    if cps.len() == 1 {
        return vec![(ends[0], cps[0])];
    }
    (2..=cps.len())
        .rev()
        .filter_map(|k| nfc_single(&cps[..k]).map(|c| (ends[k - 1], c)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> Vec<u16> {
        s.encode_utf16().collect()
    }

    #[test]
    fn composed_prefixes() {
        assert_eq!(candidates(&u("e\u{301}"), 0), vec![(2, 0xE9)]);
        assert_eq!(candidates(&u("e\u{301}\u{301}"), 0), vec![(2, 0xE9)]);
        assert_eq!(
            candidates(&u("e\u{302}\u{301}"), 0),
            vec![(3, 0x1EBF), (2, 0xEA)]
        );
        assert_eq!(
            candidates(&u("a\u{302}\u{323}"), 0),
            vec![(3, 0x1EAD), (2, 0xE2)]
        );
        assert_eq!(
            candidates(&u("\u{1100}\u{1161}\u{11A8}"), 0),
            vec![(3, 0xAC01), (2, 0xAC00)]
        );
        assert_eq!(candidates(&u("\u{915}\u{93C}"), 0), vec![]);
        assert_eq!(candidates(&u("x\u{301}"), 0), vec![]);
        assert_eq!(candidates(&u("\u{212B}"), 0), vec![(1, 0x212B)]);
    }
}
