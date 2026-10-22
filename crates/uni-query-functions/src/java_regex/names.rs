// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `\N{name}` resolution (spec LIT-8): the behaviour of the public
//! `Character.codePointOf(String)`.
//!
//! `ucd_names.bin` holds every name `Character.getName` returns from its
//! name table (control characters included, under their Unicode 1.0
//! names); every other assigned code point is named by its block and
//! hexadecimal value (`CJK UNIFIED IDEOGRAPHS 4E00`). The lookup trims
//! characters up to U+0020 from both ends and upper-cases the name as
//! `String.toUpperCase(Locale.ROOT)` does (so `ı` and `ſ` spell `I` and
//! `S`); both were confirmed by black-box probes.

use std::collections::HashMap;
use std::sync::OnceLock;

use super::ucd;
use super::ucd_tables as t;

static NAMES_BIN: &[u8] = include_bytes!("ucd_names.bin");

struct Names {
    by_name: HashMap<String, u32>,
    named: std::collections::HashSet<u32>,
}

fn names() -> &'static Names {
    static CELL: OnceLock<Names> = OnceLock::new();
    CELL.get_or_init(|| {
        let d = NAMES_BIN;
        let u32_at = |i: usize| u32::from_le_bytes([d[i], d[i + 1], d[i + 2], d[i + 3]]);
        let mut at = 0;
        let nwords = u32_at(at) as usize;
        at += 4;
        let mut words = Vec::with_capacity(nwords);
        for _ in 0..nwords {
            let n = d[at] as usize;
            words.push(std::str::from_utf8(&d[at + 1..at + 1 + n]).unwrap_or(""));
            at += 1 + n;
        }
        let count = u32_at(at) as usize;
        at += 4;
        let mut by_name = HashMap::with_capacity(count);
        let mut named = std::collections::HashSet::with_capacity(count);
        for _ in 0..count {
            let cp = u32::from(d[at]) | u32::from(d[at + 1]) << 8 | u32::from(d[at + 2]) << 16;
            let k = d[at + 3] as usize;
            at += 4;
            let mut name = String::new();
            for i in 0..k {
                let tok = u16::from_le_bytes([d[at], d[at + 1]]);
                at += 2;
                if i > 0 {
                    name.push(if tok & 0x8000 != 0 { '-' } else { ' ' });
                }
                name.push_str(words[usize::from(tok & 0x7FFF)]);
            }
            by_name.insert(name, cp);
            named.insert(cp);
        }
        Names { by_name, named }
    })
}

/// `s.toUpperCase(Locale.ROOT)` when the result is ASCII, `None` otherwise
/// (no name holds a non-ASCII character).
pub(crate) fn ascii_upper_root(s: &str) -> Option<String> {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii() {
            out.push(c.to_ascii_uppercase());
        } else {
            let mapped = t::UPPER_TO_ASCII
                .binary_search_by_key(&(c as u32), |&(cp, _)| cp)
                .ok()?;
            out.push_str(t::UPPER_TO_ASCII[mapped].1);
        }
    }
    Some(out)
}

/// `s.toLowerCase(Locale.ROOT)` when the result is ASCII, `None` otherwise.
pub(crate) fn ascii_lower_root(s: &str) -> Option<String> {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii() {
            out.push(c.to_ascii_lowercase());
        } else {
            let mapped = t::LOWER_TO_ASCII
                .binary_search_by_key(&(c as u32), |&(cp, _)| cp)
                .ok()?;
            out.push_str(t::LOWER_TO_ASCII[mapped].1);
        }
    }
    Some(out)
}

/// The code point `Character.codePointOf(name)` returns, if any.
pub(crate) fn code_point_of(name: &str) -> Option<u32> {
    let trimmed = name.trim_matches(|c: char| c <= ' ');
    let up = ascii_upper_root(trimmed)?;
    let n = names();
    if let Some(&cp) = n.by_name.get(&up) {
        return Some(cp);
    }
    // The generated form: block name, a space, upper-case hex without
    // leading zeros, for an assigned code point without a table name.
    let (block_part, hex) = up.rsplit_once(' ')?;
    if hex.is_empty() || hex.len() > 6 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let cp = u32::from_str_radix(hex, 16).ok()?;
    if cp > 0x10FFFF || format!("{cp:X}") != hex {
        return None;
    }
    if ucd::gc(cp) == ucd::GC_CN || n.named.contains(&cp) {
        return None;
    }
    let block = ucd::block(cp)?;
    let generated = t::BLOCK_NAMES[usize::from(block)].replace('_', " ");
    (generated == block_part).then_some(cp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_resolve_as_code_point_of() {
        assert_eq!(names().by_name.len(), t::NAME_COUNT);
        assert_eq!(code_point_of("white smiling face"), Some(0x263A));
        assert_eq!(code_point_of(" LATIN SMALL LETTER A "), Some(0x61));
        assert_eq!(code_point_of("\tLATIN SMALL LETTER A"), Some(0x61));
        assert_eq!(code_point_of("\u{2003}LATIN SMALL LETTER A"), None);
        assert_eq!(code_point_of("LAT\u{131}N SMALL LETTER A"), Some(0x61));
        assert_eq!(code_point_of("CJK UNIFIED IDEOGRAPHS 4E00"), Some(0x4E00));
        assert_eq!(code_point_of("cjk unified ideographs 4e00"), Some(0x4E00));
        assert_eq!(code_point_of("CJK UNIFIED IDEOGRAPHS 04E00"), None);
        assert_eq!(code_point_of("CJK UNIFIED IDEOGRAPH-4E00"), None);
        assert_eq!(code_point_of("HANGUL SYLLABLES AC00"), Some(0xAC00));
        assert_eq!(code_point_of("HANGUL SYLLABLE GA"), None);
        assert_eq!(code_point_of("HIGH SURROGATES D800"), Some(0xD800));
        assert_eq!(code_point_of("GREEK AND COPTIC 378"), None);
        assert_eq!(code_point_of("BASIC LATIN 41"), None);
        assert_eq!(code_point_of("NULL"), Some(0));
        assert_eq!(code_point_of("LINE FEED (LF)"), Some(0x0A));
        assert_eq!(code_point_of("LINE FEED"), None);
        assert_eq!(code_point_of("GRINNING FACE"), Some(0x1F600));
        assert_eq!(code_point_of(""), None);
    }
}
