// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Extended grapheme clusters for `\X`, `\b{g}` and `(?c)` classes
//! (spec GC-1, GC-2, WB-4).
//!
//! The cluster is computed from its first code point with no context
//! before it (GC-2), by the UAX #29 rules of Unicode 15.0 (GB3–GB13, no
//! GB9c), over the Grapheme_Cluster_Break classes JDK 21 applies (an
//! unpaired surrogate is Control, as are most unassigned code points; see
//! `ucd_extra.rs`).

use super::ucd::{self, gcb};

/// The code point at unit index `i` and its length in units (spec SUR-1:
/// a surrogate pair is read forwards only).
#[inline]
pub(crate) fn cp_at(text: &[u16], i: usize) -> (u32, usize) {
    let u = text[i];
    if (0xD800..0xDC00).contains(&u)
        && let Some(&lo) = text.get(i + 1)
        && (0xDC00..0xE000).contains(&lo)
    {
        return (
            0x10000 + ((u32::from(u) - 0xD800) << 10) + (u32::from(lo) - 0xDC00),
            2,
        );
    }
    (u32::from(u), 1)
}

/// The unit index where the cluster starting at `start` ends
/// (`start < text.len()`).
pub(crate) fn cluster_end(text: &[u16], start: usize) -> usize {
    let (c0, w0) = cp_at(text, start);
    let mut prev = ucd::grapheme_class(c0);
    let mut i = start + w0;
    // Regional indicators in the current run, for GB12/GB13.
    let mut ri_run = usize::from(prev == gcb::RI);
    // GB11: 1 after Extended_Pictographic Extend*, 2 after a following ZWJ.
    let mut emoji = u8::from(ucd::is_ext_pict(c0));
    while i < text.len() {
        let (c, w) = cp_at(text, i);
        let cur = ucd::grapheme_class(c);
        let joins = if prev == gcb::CR {
            // GB3, GB4.
            cur == gcb::LF
        } else if matches!(prev, gcb::CONTROL | gcb::LF)
            || matches!(cur, gcb::CONTROL | gcb::CR | gcb::LF)
        {
            // GB4, GB5.
            false
        } else {
            // GB6, GB7, GB8 (Hangul syllables).
            (prev == gcb::L && matches!(cur, gcb::L | gcb::V | gcb::LV | gcb::LVT))
                || (matches!(prev, gcb::LV | gcb::V) && matches!(cur, gcb::V | gcb::T))
                || (matches!(prev, gcb::LVT | gcb::T) && cur == gcb::T)
                // GB9, GB9a, GB9b.
                || matches!(cur, gcb::EXTEND | gcb::ZWJ | gcb::SPACING_MARK)
                || prev == gcb::PREPEND
                // GB11.
                || (emoji == 2 && ucd::is_ext_pict(c))
                // GB12, GB13.
                || (prev == gcb::RI && cur == gcb::RI && ri_run % 2 == 1)
        };
        if !joins {
            break;
        }
        ri_run = if cur == gcb::RI { ri_run + 1 } else { 0 };
        emoji = match (ucd::is_ext_pict(c), emoji, cur) {
            (true, _, _) | (false, 1, gcb::EXTEND) => 1,
            (false, 1, gcb::ZWJ) => 2,
            _ => 0,
        };
        prev = cur;
        i += w;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf16(s: &str) -> Vec<u16> {
        s.encode_utf16().collect()
    }

    /// Every line of the Unicode 15.0 GraphemeBreakTest.txt (spec GC-1).
    #[test]
    fn grapheme_break_test_15_0() {
        let data = include_str!("../../testdata/GraphemeBreakTest-15.0.0.txt");
        let mut checked = 0;
        for line in data.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            // `÷ 0020 × 0308 ÷`: code points with the break decisions.
            let mut text = Vec::new();
            let mut breaks = Vec::new();
            for tok in line.split_whitespace() {
                match tok {
                    "÷" => breaks.push(text.len()),
                    "×" => {}
                    hex => {
                        let cp = u32::from_str_radix(hex, 16).unwrap_or(0);
                        match char::from_u32(cp) {
                            Some(c) => {
                                let mut buf = [0u16; 2];
                                text.extend_from_slice(c.encode_utf16(&mut buf));
                            }
                            None => text.push(cp as u16),
                        }
                    }
                }
            }
            let mut got = vec![0];
            let mut i = 0;
            while i < text.len() {
                i = cluster_end(&text, i);
                got.push(i);
            }
            assert_eq!(got, breaks, "{line}");
            checked += 1;
        }
        assert_eq!(checked, 602);
    }

    #[test]
    fn clusters() {
        let t = utf16("e\u{301}\r\n\u{1F468}\u{200D}\u{1F469}");
        assert_eq!(cluster_end(&t, 0), 2);
        assert_eq!(cluster_end(&t, 2), 4);
        assert_eq!(cluster_end(&t, 4), t.len());
        // From the ZWJ, GB11 has no Extended_Pictographic before it (GC-2).
        assert_eq!(cluster_end(&t, 6), 7);
    }
}
