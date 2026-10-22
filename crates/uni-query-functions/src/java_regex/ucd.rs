// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Lookups over the generated Unicode tables (`ucd_tables.rs`, taken from
//! JDK 21's public `Character` API, and `ucd_extra.rs`, from UCD 15.0 and a
//! black-box scan of JDK 21's `\X`).
//!
//! Java pins its regex semantics to one Unicode version (15.0 for JDK 21,
//! spec §0.3), so nothing here consults the Rust toolchain's own Unicode
//! data.

use std::sync::OnceLock;

use super::charset::CodeSet;
use super::ucd_extra as extra;
use super::ucd_tables as t;

/// Java `Character.getType` codes, by the two-letter category name.
pub(crate) const CATEGORIES: &[(&str, u8)] = &[
    ("Cn", 0),
    ("Lu", 1),
    ("Ll", 2),
    ("Lt", 3),
    ("Lm", 4),
    ("Lo", 5),
    ("Mn", 6),
    ("Me", 7),
    ("Mc", 8),
    ("Nd", 9),
    ("Nl", 10),
    ("No", 11),
    ("Zs", 12),
    ("Zl", 13),
    ("Zp", 14),
    ("Cc", 15),
    ("Cf", 16),
    ("Co", 18),
    ("Cs", 19),
    ("Pd", 20),
    ("Ps", 21),
    ("Pe", 22),
    ("Pc", 23),
    ("Po", 24),
    ("Sm", 25),
    ("Sc", 26),
    ("Sk", 27),
    ("So", 28),
    ("Pi", 29),
    ("Pf", 30),
];

pub(crate) const GC_CN: u8 = 0;
pub(crate) const GC_LU: u8 = 1;
pub(crate) const GC_LL: u8 = 2;
pub(crate) const GC_LT: u8 = 3;
pub(crate) const GC_MN: u8 = 6;
pub(crate) const GC_ND: u8 = 9;

/// Grapheme_Cluster_Break values, as `ucd_extra::GCB` encodes them.
pub(crate) mod gcb {
    pub(crate) const OTHER: u8 = 0;
    pub(crate) const CR: u8 = 1;
    pub(crate) const LF: u8 = 2;
    pub(crate) const CONTROL: u8 = 3;
    pub(crate) const EXTEND: u8 = 4;
    pub(crate) const ZWJ: u8 = 5;
    pub(crate) const RI: u8 = 6;
    pub(crate) const PREPEND: u8 = 7;
    pub(crate) const SPACING_MARK: u8 = 8;
    pub(crate) const L: u8 = 9;
    pub(crate) const V: u8 = 10;
    pub(crate) const T: u8 = 11;
    pub(crate) const LV: u8 = 12;
    pub(crate) const LVT: u8 = 13;
}

/// The value of a by-range table at `cp`, if a range holds it.
fn lookup3<V: Copy>(table: &[(u32, u32, V)], cp: u32) -> Option<V> {
    let i = table.partition_point(|&(a, _, _)| a <= cp);
    if i == 0 {
        return None;
    }
    let (_, b, v) = table[i - 1];
    (cp <= b).then_some(v)
}

/// Whether a range table holds `cp`.
pub(crate) fn in_table(table: &[(u32, u32)], cp: u32) -> bool {
    let i = table.partition_point(|&(a, _)| a <= cp);
    i > 0 && cp <= table[i - 1].1
}

/// `Character.getType(cp)`.
pub(crate) fn gc(cp: u32) -> u8 {
    lookup3(t::GENERAL_CATEGORY, cp).unwrap_or(GC_CN)
}

/// `Character.UnicodeBlock.of(cp)`, as an index into `BLOCK_NAMES`.
pub(crate) fn block(cp: u32) -> Option<u16> {
    lookup3(t::BLOCK, cp)
}

/// The Grapheme_Cluster_Break class `\X` uses for `cp`.
pub(crate) fn grapheme_class(cp: u32) -> u8 {
    lookup3(extra::GCB, cp).unwrap_or(gcb::OTHER)
}

/// Whether `cp` is Extended_Pictographic.
pub(crate) fn is_ext_pict(cp: u32) -> bool {
    in_table(t::EXTENDED_PICTOGRAPHIC, cp)
}

/// The canonical combining class.
pub(crate) fn ccc(cp: u32) -> u8 {
    lookup3(t::COMBINING_CLASS, cp).unwrap_or(0)
}

fn map_lookup(table: &[(u32, u32)], cp: u32) -> u32 {
    match table.binary_search_by_key(&cp, |&(a, _)| a) {
        Ok(i) => table[i].1,
        Err(_) => cp,
    }
}

/// `Character.toUpperCase(int)`: U(c) in the spec's notation.
pub(crate) fn upper(cp: u32) -> u32 {
    map_lookup(t::TO_UPPER, cp)
}

/// `Character.toLowerCase(int)`: L(c) in the spec's notation.
pub(crate) fn lower(cp: u32) -> u32 {
    map_lookup(t::TO_LOWER, cp)
}

/// Whether `cp` is an ASCII letter, a digit or `_`.
pub(crate) fn is_ascii_word(cp: u32) -> bool {
    cp < 0x80 && (cp as u8).is_ascii_alphanumeric() || cp == u32::from(b'_')
}

/// The code points whose general category is one of `codes`.
pub(crate) fn gc_set(codes: &[u8]) -> CodeSet {
    CodeSet::from_ranges(
        t::GENERAL_CATEGORY
            .iter()
            .filter(|&&(_, _, v)| codes.contains(&v))
            .map(|&(a, b, _)| (a, b))
            .collect(),
    )
}

/// A lazily built set, cached for the process.
macro_rules! cached_set {
    ($name:ident, $build:expr) => {
        pub(crate) fn $name() -> &'static CodeSet {
            static CELL: OnceLock<CodeSet> = OnceLock::new();
            CELL.get_or_init(|| $build)
        }
    };
}

cached_set!(alphabetic, CodeSet::from_table(t::ALPHABETIC));
cached_set!(white_space, CodeSet::from_table(extra::WHITE_SPACE));
cached_set!(join_control, CodeSet::from_table(extra::JOIN_CONTROL));
cached_set!(nd, gc_set(&[GC_ND]));
cached_set!(
    lowercase_or_uppercase_or_lt,
    CodeSet::from_table(t::LOWERCASE)
        .union(&CodeSet::from_table(t::UPPERCASE))
        .union(&gc_set(&[GC_LT]))
);
cached_set!(cased_letter, gc_set(&[GC_LU, GC_LL, GC_LT]));
// `\w` with UNICODE_CHARACTER_CLASS (spec PRE-2).
cached_set!(
    unicode_word,
    alphabetic()
        .union(&gc_set(&[GC_MN, 7, 8, GC_ND, 23]))
        .union(join_control())
);
// `\p{Graph}` with UNICODE_CHARACTER_CLASS (spec PRE-5).
cached_set!(
    unicode_graph,
    white_space().union(&gc_set(&[15, 19, GC_CN])).negate()
);
// `\p{Blank}` with UNICODE_CHARACTER_CLASS (spec PRE-5).
cached_set!(
    unicode_blank,
    white_space()
        .difference(&gc_set(&[13, 14]))
        .difference(&CodeSet::from_cps([0x0A, 0x0B, 0x0C, 0x0D, 0x85]))
);

/// Case-relevant code points: those `toUpperCase` or `toLowerCase` change,
/// and their images, each with `(U(c), L(U(c)))`, sorted by code point.
///
/// A code point outside this list maps to itself under both functions, so
/// case closures (spec CI-2, CI-3) only need to look here.
pub(crate) fn case_relevant() -> &'static [(u32, u32, u32)] {
    static CELL: OnceLock<Vec<(u32, u32, u32)>> = OnceLock::new();
    CELL.get_or_init(|| {
        let mut cps: Vec<u32> = Vec::new();
        for &(a, b) in t::TO_UPPER.iter().chain(t::TO_LOWER.iter()) {
            cps.push(a);
            cps.push(b);
        }
        cps.sort_unstable();
        cps.dedup();
        cps.into_iter()
            .map(|c| {
                let u = upper(c);
                (c, u, lower(u))
            })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_lookups() {
        assert_eq!(gc(u32::from('A')), GC_LU);
        assert_eq!(gc(0x378), GC_CN);
        assert_eq!(gc(0xD800), 19);
        assert_eq!(upper(u32::from('a')), u32::from('A'));
        assert_eq!(lower(0x212A), u32::from('k'));
        assert_eq!(upper(0xDF), 0xDF);
        assert_eq!(grapheme_class(0x0D), gcb::CR);
        assert_eq!(grapheme_class(0x301), gcb::EXTEND);
        assert_eq!(grapheme_class(0xD800), gcb::CONTROL);
        assert_eq!(ccc(0x301), 230);
        assert!(is_ascii_word(u32::from('_')) && !is_ascii_word(0xE9));
        assert_eq!(
            white_space()
                .ranges()
                .iter()
                .map(|&(a, b)| b - a + 1)
                .sum::<u32>(),
            25
        );
    }
}
