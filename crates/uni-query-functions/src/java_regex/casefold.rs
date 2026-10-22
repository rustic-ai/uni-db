// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Case-insensitive closures of literals and ranges (spec CI-1…CI-3) and
//! the case-insensitive back-reference comparison (BR-6).
//!
//! U(c) and L(c) are `Character.toUpperCase/toLowerCase(int)` from the
//! generated tables. With UNICODE_CASE:
//! * a literal `p` matches `t` iff `t == p`, or `p` is cased (U(p) ≠ p or
//!   L(p) ≠ p) and U(t) = U(p) or L(U(t)) = L(U(p)) (CI-2);
//! * a range `[a-b]` matches `t` iff `t`, U(t) or L(U(t)) lies in it (CI-3).
//!
//! Without UNICODE_CASE only ASCII letters fold (CI-1, CI-3).

use super::charset::CodeSet;
use super::ucd::{case_relevant, lower, upper};

fn ascii_other_case(cp: u32) -> Option<u32> {
    match cp {
        0x41..=0x5A => Some(cp + 0x20),
        0x61..=0x7A => Some(cp - 0x20),
        _ => None,
    }
}

/// Whether `t` matches the literal `p` with UNICODE_CASE (CI-2).
pub(crate) fn unicode_char_matches(p: u32, t: u32) -> bool {
    if t == p {
        return true;
    }
    let (up, lp) = (upper(p), lower(p));
    if up == p && lp == p {
        return false;
    }
    let ut = upper(t);
    ut == up || lower(ut) == lower(up)
}

/// The code points a literal `cp` matches under CASE_INSENSITIVE.
pub(crate) fn char_closure(cp: u32, unicode: bool) -> CodeSet {
    if !unicode {
        return match ascii_other_case(cp) {
            Some(o) => CodeSet::from_cps([cp, o]),
            None => CodeSet::single(cp),
        };
    }
    let up = upper(cp);
    if up == cp && lower(cp) == cp {
        return CodeSet::single(cp);
    }
    let mut out = vec![cp];
    for &(t, ut, lut) in case_relevant() {
        if ut == up || lut == lower(up) {
            out.push(t);
        }
    }
    for t in [up, lower(up)] {
        if unicode_char_matches(cp, t) {
            out.push(t);
        }
    }
    CodeSet::from_cps(out)
}

/// The code points a literal inside a run of two or more literals matches
/// under CASE_INSENSITIVE with UNICODE_CASE: `t == p`, U(t) = U(p) or
/// L(U(t)) = L(U(p)), with no guard on `p` being cased.
pub(crate) fn char_closure_unguarded(cp: u32) -> CodeSet {
    let up = upper(cp);
    let key = lower(up);
    let mut out = vec![cp, up, key];
    for &(t, ut, lut) in case_relevant() {
        if ut == up || lut == key {
            out.push(t);
        }
    }
    out.retain(|&t| t == cp || upper(t) == up || lower(upper(t)) == key);
    CodeSet::from_cps(out)
}

/// The code points a range `lo..=hi` matches under CASE_INSENSITIVE.
pub(crate) fn range_closure(lo: u32, hi: u32, unicode: bool) -> CodeSet {
    let base = CodeSet::range(lo, hi);
    let inside = |c: u32| lo <= c && c <= hi;
    if !unicode {
        let extra = (0x41..=0x5A)
            .chain(0x61..=0x7A)
            .filter(|&t| ascii_other_case(t).is_some_and(inside));
        return base.union(&CodeSet::from_cps(extra));
    }
    let extra = case_relevant()
        .iter()
        .filter(|&&(_, ut, lut)| inside(ut) || inside(lut))
        .map(|&(t, _, _)| t);
    base.union(&CodeSet::from_cps(extra))
}

/// Whether two code points are equal for a case-insensitive
/// back-reference with UNICODE_CASE (BR-6): L(U(c)) = L(U(d)).
pub(crate) fn unicode_ref_eq(a: u32, b: u32) -> bool {
    a == b || lower(upper(a)) == lower(upper(b))
}

/// Whether two UTF-16 units are equal for a case-insensitive
/// back-reference without UNICODE_CASE (BR-6): ASCII letters fold.
pub(crate) fn ascii_ref_eq(a: u16, b: u16) -> bool {
    a == b || ascii_other_case(u32::from(a)) == Some(u32::from(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(cps: &[u32]) -> CodeSet {
        CodeSet::from_cps(cps.iter().copied())
    }

    /// Spec CI-2 examples.
    #[test]
    fn ci2_literal_closures() {
        let (k, kk, kelvin) = (0x6B, 0x4B, 0x212A);
        for p in [k, kk, kelvin] {
            assert_eq!(char_closure(p, true), s(&[k, kk, kelvin]), "{p:x}");
        }
        assert_eq!(char_closure(0xDF, true), s(&[0xDF]));
        assert_eq!(char_closure(0x1E9E, true), s(&[0xDF, 0x1E9E]));
        assert_eq!(char_closure(0x3C2, true), s(&[0x3A3, 0x3C3, 0x3C2]));
        assert_eq!(char_closure(0x10400, true), s(&[0x10400, 0x10428]));
        assert_eq!(char_closure(0x10D0, true), s(&[0x10D0, 0x1C90]));
        assert_eq!(char_closure(0xE9, false), s(&[0xE9]));
        assert_eq!(char_closure(0x6B, false), s(&[0x4B, 0x6B]));
    }

    /// Spec CI-3 examples.
    #[test]
    fn ci3_range_closures() {
        let r = |a: char, b: char| range_closure(a as u32, b as u32, true);
        assert_eq!(r('K', 'K'), s(&[0x4B, 0x6B]));
        assert_eq!(r('k', 'k'), s(&[0x4B, 0x6B, 0x212A]));
        assert_eq!(r('I', 'I'), s(&[0x49, 0x69, 0x131]));
        assert_eq!(r('\u{DF}', '\u{DF}'), s(&[0xDF, 0x1E9E]));
        assert_eq!(r('\u{1E9E}', '\u{1E9E}'), s(&[0x1E9E]));
        let az = r('a', 'z');
        for c in [0x41, 0x5A, 0x17F, 0x212A, 0x130, 0x131] {
            assert!(az.contains(c), "{c:x}");
        }
        let big = r('A', 'Z');
        assert!(big.contains(0x17F) && big.contains(0x131));
        assert!(!big.contains(0x212A) && !big.contains(0x130));
        let ascii = range_closure(u32::from('Z'), u32::from('a'), false);
        assert_eq!(
            ascii,
            s(&[0x41, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F, 0x60, 0x61, 0x7A])
        );
    }
}
