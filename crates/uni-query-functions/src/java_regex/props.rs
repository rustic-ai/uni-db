// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Property names inside `\p{…}` / `\P{…}` and the code point sets they
//! denote (spec PRE-4, PRE-5, PROP-1…PROP-7, SCR-1, BLK-1, CI-6).
//!
//! Resolution order (PROP-3): a `key=value` form; an `In` prefix (a block);
//! an `Is` prefix (a binary or POSIX-Unicode property compared after
//! upper-casing, then a script, then a case-sensitive name); otherwise a
//! case-sensitive name (a category, `LC`, `LD`, `L1`, `all`, a POSIX name,
//! a `java…` name). With `U`, POSIX names other than `ASCII` are also found
//! after upper-casing (observed: `(?U)\p{lower}` compiles, `\p{lower}` does
//! not). Under `i`, the case-related sets widen (CI-6).

use super::charset::CodeSet;
use super::names::{ascii_lower_root, ascii_upper_root};
use super::ucd::{self, CATEGORIES, gc_set};
use super::ucd_tables as t;

/// How a property is being read: the flags in force at it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PropFlags {
    /// CASE_INSENSITIVE (spec CI-6: `i` alone is enough).
    pub(crate) ci: bool,
    /// UNICODE_CHARACTER_CLASS.
    pub(crate) unicode_class: bool,
}

/// The general-category groups and their member codes.
fn group_codes(name: &str) -> Option<&'static [u8]> {
    Some(match name {
        "L" => &[1, 2, 3, 4, 5],
        "M" => &[6, 7, 8],
        "N" => &[9, 10, 11],
        "Z" => &[12, 13, 14],
        "C" => &[15, 16, 18, 19, 0],
        "P" => &[20, 21, 22, 23, 24, 29, 30],
        "S" => &[25, 26, 27, 28],
        "LC" => &[1, 2, 3],
        _ => return None,
    })
}

/// The POSIX names (spec PRE-4).
const POSIX: &[&str] = &[
    "Lower", "Upper", "ASCII", "Alpha", "Digit", "Alnum", "Punct", "Graph", "Print", "Blank",
    "Cntrl", "XDigit", "Space",
];

/// A POSIX class in ASCII mode (spec PRE-4).
fn posix_ascii(name: &str, ci: bool) -> CodeSet {
    let r = |a: u8, b: u8| (u32::from(a), u32::from(b));
    let ranges: Vec<(u32, u32)> = match name {
        "Lower" if ci => vec![r(b'A', b'Z'), r(b'a', b'z')],
        "Upper" if ci => vec![r(b'A', b'Z'), r(b'a', b'z')],
        "Lower" => vec![r(b'a', b'z')],
        "Upper" => vec![r(b'A', b'Z')],
        "ASCII" => vec![(0, 0x7F)],
        "Alpha" => vec![r(b'A', b'Z'), r(b'a', b'z')],
        "Digit" => vec![r(b'0', b'9')],
        "Alnum" => vec![r(b'0', b'9'), r(b'A', b'Z'), r(b'a', b'z')],
        "Punct" => vec![(0x21, 0x2F), (0x3A, 0x40), (0x5B, 0x60), (0x7B, 0x7E)],
        "Graph" => vec![(0x21, 0x7E)],
        "Print" => vec![(0x20, 0x7E)],
        "Blank" => vec![(0x20, 0x20), (0x09, 0x09)],
        "Cntrl" => vec![(0, 0x1F), (0x7F, 0x7F)],
        "XDigit" => vec![r(b'0', b'9'), r(b'A', b'F'), r(b'a', b'f')],
        "Space" => vec![(0x09, 0x0D), (0x20, 0x20)],
        _ => vec![],
    };
    CodeSet::from_ranges(ranges)
}

/// The Unicode definition of a binary / POSIX-Unicode name, given upper
/// case (spec PROP-5, PRE-5), widened under `i` (CI-6).
fn unicode_binary(upper: &str, ci: bool) -> Option<CodeSet> {
    let case_widened = || ucd::lowercase_or_uppercase_or_lt().clone();
    Some(match upper {
        "ALPHABETIC" | "ALPHA" => ucd::alphabetic().clone(),
        "IDEOGRAPHIC" => CodeSet::from_table(t::IDEOGRAPHIC),
        "LETTER" => gc_set(group_codes("L")?),
        "LOWERCASE" | "LOWER" if ci => case_widened(),
        "UPPERCASE" | "UPPER" if ci => case_widened(),
        "TITLECASE" if ci => case_widened(),
        "LOWERCASE" | "LOWER" => CodeSet::from_table(t::LOWERCASE),
        "UPPERCASE" | "UPPER" => CodeSet::from_table(t::UPPERCASE),
        "TITLECASE" => gc_set(&[ucd::GC_LT]),
        "PUNCTUATION" | "PUNCT" => gc_set(group_codes("P")?),
        "CONTROL" | "CNTRL" => gc_set(&[15]),
        "WHITE_SPACE" | "WHITESPACE" | "SPACE" => ucd::white_space().clone(),
        "DIGIT" => ucd::nd().clone(),
        "HEX_DIGIT" | "HEXDIGIT" | "XDIGIT" => {
            ucd::nd().union(&CodeSet::from_table(super::ucd_extra::HEX_DIGIT))
        }
        "JOIN_CONTROL" | "JOINCONTROL" => ucd::join_control().clone(),
        "NONCHARACTER_CODE_POINT" | "NONCHARACTERCODEPOINT" => {
            CodeSet::from_table(super::ucd_extra::NONCHARACTER)
        }
        "ASSIGNED" => gc_set(&[ucd::GC_CN]).negate(),
        "EMOJI" => CodeSet::from_table(t::EMOJI),
        "EMOJI_PRESENTATION" => CodeSet::from_table(t::EMOJI_PRESENTATION),
        "EMOJI_MODIFIER" => CodeSet::from_table(t::EMOJI_MODIFIER),
        "EMOJI_MODIFIER_BASE" => CodeSet::from_table(t::EMOJI_MODIFIER_BASE),
        "EMOJI_COMPONENT" => CodeSet::from_table(t::EMOJI_COMPONENT),
        "EXTENDED_PICTOGRAPHIC" => CodeSet::from_table(t::EXTENDED_PICTOGRAPHIC),
        "ALNUM" => ucd::alphabetic().union(ucd::nd()),
        "GRAPH" => ucd::unicode_graph().clone(),
        "PRINT" => ucd::unicode_graph()
            .union(ucd::unicode_blank())
            .difference(&gc_set(&[15])),
        "BLANK" => ucd::unicode_blank().clone(),
        "WORD" => ucd::unicode_word().clone(),
        _ => return None,
    })
}

/// The POSIX names `U` resolves after upper-casing (all but `ASCII`).
const POSIX_UNICODE_UPPER: &[&str] = &[
    "LOWER", "UPPER", "ALPHA", "DIGIT", "ALNUM", "PUNCT", "GRAPH", "PRINT", "BLANK", "CNTRL",
    "XDIGIT", "SPACE",
];

/// A `java…` name (spec PROP-6).
fn java_prop(name: &str, ci: bool) -> Option<CodeSet> {
    let table: &[(u32, u32)] = match name {
        "javaLowerCase" | "javaUpperCase" | "javaTitleCase" if ci => {
            return Some(ucd::lowercase_or_uppercase_or_lt().clone());
        }
        "javaLowerCase" => t::LOWERCASE,
        "javaUpperCase" => t::UPPERCASE,
        "javaTitleCase" => t::JAVA_TITLE_CASE,
        "javaDigit" => t::JAVA_DIGIT,
        "javaDefined" => t::JAVA_DEFINED,
        "javaLetter" => t::JAVA_LETTER,
        "javaLetterOrDigit" => t::JAVA_LETTER_OR_DIGIT,
        "javaJavaIdentifierStart" => t::JAVA_IDENTIFIER_START,
        "javaJavaIdentifierPart" => t::JAVA_IDENTIFIER_PART,
        "javaUnicodeIdentifierStart" => t::UNICODE_IDENTIFIER_START,
        "javaUnicodeIdentifierPart" => t::UNICODE_IDENTIFIER_PART,
        "javaIdentifierIgnorable" => t::IDENTIFIER_IGNORABLE,
        "javaSpaceChar" => t::SPACE_CHAR,
        "javaWhitespace" => t::JAVA_WHITESPACE,
        "javaISOControl" => t::ISO_CONTROL,
        "javaMirrored" => t::MIRRORED,
        "javaAlphabetic" => t::ALPHABETIC,
        "javaIdeographic" => t::IDEOGRAPHIC,
        _ => return None,
    };
    Some(CodeSet::from_table(table))
}

/// A POSIX name with its `U`-dependent meaning (spec PRE-4, PRE-5, CI-6).
fn posix(name: &str, f: PropFlags) -> CodeSet {
    if f.unicode_class && name != "ASCII" {
        let upper = name.to_ascii_uppercase();
        return unicode_binary(&upper, f.ci).unwrap_or_default();
    }
    posix_ascii(name, f.ci)
}

/// A case-sensitive name: a category or group, `LC`, `LD`, `L1`, `all`, a
/// POSIX name or a `java…` name (spec PROP-3 step 3; also the values
/// `gc=` accepts).
fn plain_name(name: &str, f: PropFlags) -> Option<CodeSet> {
    if let Some(&(_, code)) = CATEGORIES.iter().find(|(n, _)| *n == name) {
        if f.ci && matches!(code, 1..=3) {
            return Some(ucd::cased_letter().clone());
        }
        return Some(gc_set(&[code]));
    }
    if let Some(codes) = group_codes(name) {
        return Some(gc_set(codes));
    }
    match name {
        "LD" => return Some(gc_set(group_codes("L")?).union(ucd::nd())),
        "L1" => return Some(CodeSet::range(0, 0xFF)),
        "all" => return Some(CodeSet::all()),
        _ => {}
    }
    if POSIX.contains(&name) {
        return Some(posix(name, f));
    }
    java_prop(name, f.ci)
}

/// `Character.UnicodeScript.forName(name)`, as a script index.
fn script_for_name(name: &str) -> Option<u8> {
    let up = ascii_upper_root(name)?;
    if let Some(i) = t::SCRIPT_NAMES.iter().position(|s| *s == up) {
        return u8::try_from(i).ok();
    }
    let (_, target) = t::SCRIPT_ALIASES.iter().find(|(a, _)| *a == up)?;
    t::SCRIPT_NAMES
        .iter()
        .position(|s| s == target)
        .and_then(|i| u8::try_from(i).ok())
}

fn script_set(index: u8) -> CodeSet {
    CodeSet::from_ranges(
        t::SCRIPT
            .iter()
            .filter(|&&(_, _, v)| v == index)
            .map(|&(a, b, _)| (a, b))
            .collect(),
    )
}

/// `Character.UnicodeBlock.forName(name)`, as a block index.
fn block_for_name(name: &str) -> Option<u16> {
    let up = ascii_upper_root(name)?;
    let i = t::BLOCK_KEYS
        .binary_search_by(|(k, _)| (*k).cmp(up.as_str()))
        .ok()?;
    Some(t::BLOCK_KEYS[i].1)
}

fn block_set(index: u16) -> CodeSet {
    CodeSet::from_ranges(
        t::BLOCK
            .iter()
            .filter(|&&(_, _, v)| v == index)
            .map(|&(a, b, _)| (a, b))
            .collect(),
    )
}

/// The set `\p{name}` denotes, or the `PatternSyntaxException` description.
pub(crate) fn resolve(name: &str, f: PropFlags) -> Result<CodeSet, String> {
    let unknown_name = || format!("Unknown character property name {{{name}}}");
    if let Some((key, value)) = name.split_once('=') {
        let unknown = || {
            format!(
                "Unknown Unicode property {{name=<{}>, value=<{value}>}}",
                key.to_lowercase()
            )
        };
        let found = match ascii_lower_root(key).as_deref() {
            Some("gc" | "general_category") => plain_name(value, f),
            Some("sc" | "script") => script_for_name(value).map(script_set),
            Some("blk" | "block") => block_for_name(value).map(block_set),
            _ => None,
        };
        return found.ok_or_else(unknown);
    }
    if let Some(rest) = name.strip_prefix("In") {
        return block_for_name(rest).map(block_set).ok_or_else(unknown_name);
    }
    if let Some(rest) = name.strip_prefix("Is") {
        if let Some(set) = ascii_upper_root(rest).and_then(|up| unicode_binary(&up, f.ci)) {
            return Ok(set);
        }
        if let Some(s) = script_for_name(rest) {
            return Ok(script_set(s));
        }
        return plain_name(rest, f).ok_or_else(unknown_name);
    }
    if let Some(set) = plain_name(name, f) {
        return Ok(set);
    }
    if f.unicode_class
        && let Some(up) = ascii_upper_root(name)
        && POSIX_UNICODE_UPPER.contains(&up.as_str())
    {
        return Ok(unicode_binary(&up, f.ci).unwrap_or_default());
    }
    Err(unknown_name())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every property spelling `GenUcdTables.java` fingerprinted resolves
    /// to exactly the set JDK 21's `Pattern` matches (spec PROP-*, PRE-*,
    /// CI-6).
    #[test]
    fn property_sets_match_java_fingerprints() {
        let mut bad = Vec::new();
        for &(flags, name, want) in t::PATTERN_FINGERPRINTS {
            let f = PropFlags {
                ci: flags.contains("(?i)"),
                unicode_class: flags.contains("(?U)"),
            };
            let set = match name {
                "\\w" => ucd::unicode_word().clone(),
                "\\d" => ucd::nd().clone(),
                "\\s" => ucd::white_space().clone(),
                _ => match resolve(name, f) {
                    Ok(s) => s,
                    Err(e) => {
                        bad.push(format!("{flags}{name}: {e}"));
                        continue;
                    }
                },
            };
            if set.fingerprint() != want {
                bad.push(format!("{flags}\\p{{{name}}}: fingerprint differs"));
            }
        }
        assert!(
            bad.is_empty(),
            "{} mismatches:\n{}",
            bad.len(),
            bad.join("\n")
        );
    }

    #[test]
    fn property_name_resolution() {
        let f = PropFlags {
            ci: false,
            unicode_class: false,
        };
        assert!(resolve("IsLatin", f).is_ok());
        assert!(resolve("Isalphabet\u{131}c", f).is_ok());
        assert!(resolve("IsALPHABET\u{130}C", f).is_err());
        assert!(resolve("Latin", f).is_err());
        assert!(resolve("lower", f).is_err());
        assert!(resolve("gc=javaLetter", f).is_ok());
        assert!(resolve("gc=IsL", f).is_err());
        assert!(resolve("bl\u{212A}=Greek", f).is_ok());
        assert_eq!(
            resolve("g\u{212A}=Lu", f).unwrap_err(),
            "Unknown Unicode property {name=<gk>, value=<Lu>}"
        );
        assert!(resolve("InGreek", f).is_ok());
        assert!(resolve("InGreek_and_Coptic", f).is_err());
        assert!(resolve("L1", f).is_ok() && resolve("InL1", f).is_err());
        let u = PropFlags {
            ci: false,
            unicode_class: true,
        };
        assert!(resolve("lower", u).is_ok());
        assert!(resolve("ascii", u).is_err());
        assert!(resolve("word", u).is_err());
    }
}
