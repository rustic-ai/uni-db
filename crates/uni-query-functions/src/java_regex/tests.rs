// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests: the oracle gate (answers recorded from JDK 21) and unit tests per
//! spec section, each citing the rule it checks.

use super::*;

/// Decodes the oracle escape format (`\\ \t \n \r \x{HEX}`); `None` when
/// the text holds an unpaired surrogate (not a Rust string).
fn unescape(s: &str) -> Option<String> {
    let mut out = String::new();
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match it.next()? {
            '\\' => out.push('\\'),
            't' => out.push('\t'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            'x' => {
                it.next(); // '{'
                let hex: String = it.by_ref().take_while(|&c| c != '}').collect();
                out.push(char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?);
            }
            _ => return None,
        }
    }
    Some(out)
}

/// The inverse of [`unescape`], escaping everything outside printable
/// ASCII (as `JavaRegexOracle.escape` does).
fn escape(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            ' '..='~' => out.push(c),
            _ => out.push_str(&format!("\\x{{{:X}}}", c as u32)),
        }
    }
    out
}

/// The outcome of one oracle case.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, PartialOrd, Ord)]
enum Outcome {
    Agree,
    Disagree,
    /// Java overflowed its stack or timed out; any answer is accepted.
    JavaGaveUp,
    /// The subject holds an unpaired surrogate.
    Unrepresentable,
}

fn check_case(pattern: &str, subject: &str, want: &str) -> (Outcome, String) {
    let (Some(p), Some(s)) = (unescape(pattern), unescape(subject)) else {
        return (Outcome::Unrepresentable, String::new());
    };
    if want == "stackoverflow" || want == "timeout" {
        return (Outcome::JavaGaveUp, String::new());
    }
    let got = match CypherRegex::new(&p) {
        Err(e) => {
            if want == "error" {
                return (Outcome::Agree, String::new());
            }
            // `JavaRegexOracle -Doracle.detail=true` records the
            // description and index too.
            format!("error:{}@{}", escape(e.description()), e.index())
        }
        Ok(re) => match re.is_match(&s) {
            Ok(b) => b.to_string(),
            Err(MatchError::JavaException { exception, .. }) => {
                let class = exception.split(':').next().unwrap_or("").to_string();
                format!("exception:{class}")
            }
            Err(e) => format!("limit ({e})"),
        },
    };
    if got == want {
        (Outcome::Agree, got)
    } else {
        (Outcome::Disagree, got)
    }
}

/// Runs an oracle file; returns the outcome counts and the disagreements.
fn run_oracle(data: &str) -> (std::collections::BTreeMap<Outcome, usize>, Vec<String>) {
    let lines: Vec<&str> = data
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let chunk = lines.len().div_ceil(threads).max(1);
    let results: Vec<(Outcome, String)> = std::thread::scope(|scope| {
        let handles: Vec<_> = lines
            .chunks(chunk)
            .map(|part| {
                scope.spawn(move || {
                    part.iter()
                        .map(|line| {
                            let f: Vec<&str> = line.split('\t').collect();
                            if f.len() < 3 {
                                return (Outcome::Disagree, format!("bad line: {line}"));
                            }
                            let (o, got) = check_case(f[0], f[1], f[2]);
                            (o, format!("{}\t{}\twant {}\tgot {got}", f[0], f[1], f[2]))
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap_or_default())
            .collect()
    });
    let mut counts = std::collections::BTreeMap::new();
    let mut bad = Vec::new();
    for (o, msg) in results {
        *counts.entry(o).or_insert(0) += 1;
        if o == Outcome::Disagree {
            bad.push(msg);
        }
    }
    (counts, bad)
}

/// The curated oracle cases (`testdata/java_regex_oracle.tsv`), and any
/// generated oracle files named in `UNI_JAVA_REGEX_ORACLE` (separated by
/// `:`), all agree with JDK 21.
#[test]
fn answers_match_java() {
    let mut files = vec![(
        "testdata/java_regex_oracle.tsv".to_string(),
        include_str!("../../testdata/java_regex_oracle.tsv").to_string(),
    )];
    if let Ok(list) = std::env::var("UNI_JAVA_REGEX_ORACLE") {
        for path in list.split(':').filter(|p| !p.is_empty()) {
            let data = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
            files.push((path.to_string(), data));
        }
    }
    let mut failures = Vec::new();
    for (name, data) in &files {
        let (counts, bad) = run_oracle(data);
        eprintln!("{name}: {counts:?}");
        if !bad.is_empty() {
            let limit = std::env::var("UNI_JAVA_REGEX_SHOW")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(40);
            for b in bad.iter().take(limit) {
                eprintln!("  {b}");
            }
            failures.push(format!("{name}: {} disagreements", bad.len()));
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}

/// Debugging aid: compiles and matches the cases in `UNI_JAVA_REGEX_PROBE`
/// (a file of `pattern<TAB>subject` lines, oracle escapes) and prints the
/// answers.
#[test]
fn probe_cases() {
    let Ok(path) = std::env::var("UNI_JAVA_REGEX_PROBE") else {
        return;
    };
    let data = std::fs::read_to_string(&path).unwrap_or_default();
    for line in data.lines().filter(|l| !l.is_empty()) {
        let f: Vec<&str> = line.split('\t').collect();
        let (Some(p), Some(s)) = (unescape(f[0]), unescape(f.get(1).copied().unwrap_or(""))) else {
            continue;
        };
        let limit = std::env::var("UNI_JAVA_REGEX_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BACKTRACK_LIMIT);
        let t = std::time::Instant::now();
        let got = match CypherRegex::with_backtrack_limit(&p, limit) {
            Err(e) => format!("error: {}", e.description()),
            Ok(re) => format!("{:?} route={:?}", re.is_match(&s), re.route()),
        };
        eprintln!("{line}  =>  {got} ({:?})", t.elapsed());
    }
}

/// Debugging aid: answers the probes in `UNI_JAVA_REGEX_JPROBE` in the
/// input and output format of `scripts/java_regex_oracle/JavaRegexProbe.java`
/// (modes `C`, `M`, `G`; `@{HEX}` code points), so the two outputs can be
/// compared line by line. Lines whose pattern holds a lone surrogate are
/// printed with `SKIP`.
#[test]
fn jprobe_cases() {
    let Ok(path) = std::env::var("UNI_JAVA_REGEX_JPROBE") else {
        return;
    };
    fn unesc(s: &str) -> Vec<u16> {
        let mut out = Vec::new();
        let mut rest = s;
        while let Some(at) = rest.find("@{") {
            out.extend(rest[..at].encode_utf16());
            let close = rest[at..].find('}').map_or(rest.len(), |c| at + c);
            let v = u32::from_str_radix(&rest[at + 2..close], 16).unwrap_or(0);
            if v <= 0xFFFF {
                out.push(v as u16);
            } else {
                out.extend(
                    char::from_u32(v)
                        .unwrap_or('?')
                        .encode_utf16(&mut [0; 2])
                        .iter(),
                );
            }
            rest = &rest[(close + 1).min(rest.len())..];
        }
        out.extend(rest.encode_utf16());
        out
    }
    fn show(u: &[u16]) -> String {
        let mut b = String::from("\"");
        for &c in u {
            if (0x20..0x7f).contains(&c) {
                b.push(c as u8 as char);
            } else {
                b.push_str(&format!("\\u{c:04X}"));
            }
        }
        b.push('"');
        b
    }
    let data = std::fs::read_to_string(&path).unwrap_or_default();
    let limit = std::env::var("UNI_JAVA_REGEX_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_BACKTRACK_LIMIT);
    let mut out = String::new();
    for line in data
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let f: Vec<&str> = line.split('\t').collect();
        let mode = f[0];
        let p = unesc(f.get(1).copied().unwrap_or(""));
        let s = unesc(f.get(2).copied().unwrap_or(""));
        let r = match String::from_utf16(&p) {
            Err(_) => "SKIP".to_string(),
            Ok(p) => match CypherRegex::with_backtrack_limit(&p, limit) {
                Err(e) => format!(
                    "PSE {} @{}",
                    show(&e.description().encode_utf16().collect::<Vec<_>>()),
                    e.index()
                ),
                Ok(re) => match mode {
                    "C" => format!("OK groups={}", re.group_count()),
                    _ => match re.groups(&s) {
                        Err(e) => format!("LIMIT {e}"),
                        Ok(None) => "false".to_string(),
                        Ok(Some(g)) if mode == "G" => {
                            let mut r = "true".to_string();
                            for (k, &(a, b)) in g.iter().enumerate().skip(1) {
                                if a < 0 {
                                    r.push_str(&format!(" g{k}=unset"));
                                } else {
                                    r.push_str(&format!(
                                        " g{k}=[{a},{b}]{}",
                                        show(&s[a as usize..b as usize])
                                    ));
                                }
                            }
                            r
                        }
                        Ok(Some(_)) => "true".to_string(),
                    },
                },
            },
        };
        out.push_str(&format!("{line}  =>  {r}\n"));
    }
    if let Ok(o) = std::env::var("UNI_JAVA_REGEX_JPROBE_OUT") {
        std::fs::write(o, out).unwrap_or_else(|e| panic!("{e}"));
    } else {
        eprint!("{out}");
    }
}

// ---------------------------------------------------------------------
// Unit tests per spec section. Each case is (pattern, subject, answer),
// with `None` for a pattern Java refuses; the answers are the spec's
// examples (black-box probes of JDK 21) unless noted.
// ---------------------------------------------------------------------

fn check(cases: &[(&str, &str, Option<bool>)]) {
    let mut bad = Vec::new();
    for &(p, s, want) in cases {
        let got = CypherRegex::new(p).ok().map(|re| re.is_match(s));
        let ok = match (want, &got) {
            (None, None) => true,
            (Some(w), Some(Ok(g))) => w == *g,
            _ => false,
        };
        if !ok {
            bad.push(format!("{p:?} on {s:?}: want {want:?}, got {got:?}"));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

const T: Option<bool> = Some(true);
const F: Option<bool> = Some(false);
const E: Option<bool> = None;

/// ERR-1…ERR-3: error categories and Java's message format.
#[test]
fn err_messages() {
    let e = CypherRegex::new(")").unwrap_err();
    assert_eq!(e.to_string(), "Unmatched closing ')'\n)");
    let e = CypherRegex::new("a)").unwrap_err();
    assert_eq!(e.to_string(), "Unmatched closing ')' near index 0\na)\n^");
    let e = CypherRegex::new("(").unwrap_err();
    assert_eq!(e.description(), "Unclosed group");
    assert_eq!(e.to_string(), "Unclosed group near index 1\n(");
    for (p, d) in [
        ("\\y", "Illegal/unsupported escape sequence"),
        ("\\", "Unescaped trailing backslash"),
        ("[a", "Unclosed character class"),
        ("a{", "Illegal repetition"),
        ("a{2", "Unclosed counted closure"),
        ("a{3,2}", "Illegal repetition range"),
        ("*", "Dangling meta character '*'"),
        (
            "(?<1>a)",
            "capturing group name does not start with a Latin letter",
        ),
        ("(?<a_>a)", "named capturing group is missing trailing '>'"),
        (
            "(?<a>x)(?<a>y)",
            "Named capturing group <a> is already defined",
        ),
        ("\\k<a>", "named capturing group <a> does not exist"),
        ("\\p{Foo}", "Unknown character property name {Foo}"),
        (
            "\\p{gc=Foo}",
            "Unknown Unicode property {name=<gc>, value=<Foo>}",
        ),
        ("\\N{FOO}", "Unknown character name [FOO]"),
        (
            "(?<=(?:ab)+)",
            "Look-behind group does not have an obvious maximum length",
        ),
        ("[&&]", "Bad class syntax"),
        ("[[a]b&&]", "Bad intersection syntax"),
    ] {
        assert_eq!(CypherRegex::new(p).unwrap_err().description(), d, "{p}");
    }
}

/// ERR-3: Java's error descriptions, indices and order, which black-box
/// probes of JDK 21 settle (an independent cross-check of 50 375 refused
/// patterns found the module's indices off by one in most cases). Indices
/// count code points of the pattern after `\Q…\E` removal.
#[test]
fn err3_indices_and_order() {
    let cases: &[(&str, &str, i32)] = &[
        ("{", "Illegal repetition", 1),
        ("a{,2}", "Illegal repetition", 2),
        ("(?x)a{ 3,1}", "Illegal repetition", 6),
        ("a{3,2}", "Illegal repetition range", 5),
        ("😀{3,1}+", "Illegal repetition range", 5),
        ("a{2147483648", "Illegal repetition range", 11),
        ("a{21474836470}", "Illegal repetition range", 12),
        ("a{1,99999999999}", "Illegal repetition range", 13),
        ("a{1x}", "Unclosed counted closure", 3),
        ("(?x)a* * b", "Dangling meta character '*'", 8),
        ("a** b", "Dangling meta character '*'", 2),
        ("(?$=a)", "Unknown group type", 2),
        ("(?@)", "Unknown group type", 2),
        ("(?#c)", "Unknown inline modifier", 2),
        (
            "(?<a>x)(?<a>y)",
            "Named capturing group <a> is already defined",
            11,
        ),
        ("\\k<ab>", "named capturing group <ab> does not exist", 5),
        ("\\px", "Unknown character property name {x}", 2),
        ("\\p😀", "Unknown character property name {😀}", 2),
        ("\\p", "Unknown character property name {\u{0}}", 2),
        ("\\p{}", "Empty character family", 3),
        ("\\p{Foo}", "Unknown character property name {Foo}", 6),
        (
            "\\p{gc=}",
            "Unknown Unicode property {name=<gc>, value=<>}",
            6,
        ),
        ("(?x)\\p{L#}", "Unclosed character family", 10),
        ("\\c", "Illegal control escape sequence", 1),
        ("(?x)\\c ", "Unexpected internal error", 7),
        ("\\x{", "Illegal hexadecimal escape sequence", 2),
        ("\\x{}", "Illegal hexadecimal escape sequence", 2),
        ("\\x{ 41}", "Illegal hexadecimal escape sequence", 2),
        ("\\x{4G}", "Unclosed hexadecimal escape sequence", 4),
        ("\\x{110000}", "Hexadecimal codepoint is too big", 8),
        ("\\x{1F600a", "Hexadecimal codepoint is too big", 8),
        ("\\N{", "Unclosed character name escape sequence", 3),
        ("\\N{abc", "Unclosed character name escape sequence", 5),
        ("(?x)\\N{a ", "Unclosed character name escape sequence", 9),
        (
            "(?x)\\N{LATIN SMALL LETTER A#}",
            "Unclosed character name escape sequence",
            29,
        ),
        ("\\N{}", "Unknown character name []", 3),
        ("\\b{g", "Illegal/unsupported escape sequence", 4),
        ("\\b{gx}", "Illegal/unsupported escape sequence", 4),
        ("\\b{x}", "Illegal repetition", 3),
        ("[z-a]", "Illegal character range", 3),
        ("[a-\\d]", "Illegal character range", 4),
        ("[a-\\Q]", "Illegal character range", 4),
        ("[a-", "Illegal character range", 3),
        ("[A-\\P{L}]", "Illegal/unsupported escape sequence", 4),
        ("[a-\\p{Foo}]", "Illegal/unsupported escape sequence", 4),
        ("[&&]", "Bad class syntax", 2),
        ("[[a]b&&]", "Bad intersection syntax", 6),
        // Characters whose case closure reaches past U+00FF are not
        // collected with the class's single characters (-2: compiles).
        ("[😀a&&]", "Bad intersection syntax", 4),
        ("[\\x{100}a&&]", "Bad intersection syntax", 10),
        ("(?iu)[ka&&]", "Bad intersection syntax", 9),
        ("[a😀&&]", "", -2),
        ("[\\p{L}😀&&]", "", -2),
        ("[\\x{FF}a&&]", "", -2),
        // A trailing backslash escapes the end of the pattern.
        ("\\", "Unescaped trailing backslash", 1),
        ("(\\", "Unclosed group", 3),
        ("[a\\", "Unclosed character class", 3),
        ("[a-\\", "Illegal character range", 4),
        // `\Q…\E` removal: ASCII punctuation escaped, a leading digit as
        // `\x3d`, everything else as it is.
        ("\\Q()\\E(", "Unclosed group", 5),
        ("\\Q1\\E(", "Unclosed group", 5),
        ("\\Q12\\E(", "Unclosed group", 6),
        ("\\Qa1\\E(", "Unclosed group", 3),
        ("\\Q😀\\E(", "Unclosed group", 2),
        ("\\N{w00\\Q4}", "Unknown character name [w00\\x34\\]", 11),
        ("b*(\\1|a){2,\\Q3}", "Unclosed counted closure", 11),
        // Look-behind lengths are checked as soon as the body ends.
        (
            "(?<!\\1)",
            "Look-behind group does not have an obvious maximum length",
            5,
        ),
        (
            "(?<!\\1",
            "Look-behind group does not have an obvious maximum length",
            5,
        ),
        (
            "(?<!\\1)\\q",
            "Look-behind group does not have an obvious maximum length",
            5,
        ),
        (
            "((?<!\\1)",
            "Look-behind group does not have an obvious maximum length",
            6,
        ),
        (
            "(?<=(?:a|b)*)",
            "Look-behind group does not have an obvious maximum length",
            11,
        ),
        (
            "(?<=a)(?<=\\1)",
            "Look-behind group does not have an obvious maximum length",
            11,
        ),
        (
            "\\G{0,3}+(?<!(1+?){2}?){3,1}+$?",
            "Look-behind group does not have an obvious maximum length",
            20,
        ),
        (")", "Unmatched closing ')'", -1),
        ("😀)", "Unmatched closing ')'", 0),
        ("[😀-a]", "Illegal character range", 3),
    ];
    let mut bad = Vec::new();
    for &(p, d, i) in cases {
        let got = match CypherRegex::new(p) {
            Ok(_) => (String::new(), -2),
            Err(e) => (e.description().to_string(), e.index()),
        };
        if got != (d.to_string(), i) {
            bad.push(format!(
                "{p:?}: want {d:?} @{i}, got {:?} @{}",
                got.0, got.1
            ));
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

/// LIT-1…LIT-10: literals and escapes.
#[test]
fn lit_literals_and_escapes() {
    check(&[
        ("]", "]", T),
        ("}", "}", T),
        ("a{x}", "a{x}", E),
        ("\\!\\\"\\#\\-", "!\"#-", T),
        ("\\é", "é", T),
        ("\\😀", "😀", T),
        ("\\E", "", E),
        ("\\g", "g", E),
        ("\\a\\e\\f\\n\\r\\t", "\u{7}\u{1b}\u{c}\n\r\t", T),
        ("\\07\\077\\0377", "\u{7}?\u{ff}", T),
        ("\\0400", " 0", T),
        ("\\08", "", E),
        ("\\x41\\x{1F600}\\x{00000000000000000041}", "A😀A", T),
        ("\\x{110000}", "", E),
        ("\\x{41", "", E),
        ("\\uD83D\\uDE00+", "😀😀", T),
        ("\\x{D83D}\\x{DE00}", "😀", F),
        ("\\uu0041", "", E),
        ("\\N{white smiling face}", "\u{263A}", T),
        ("\\N{CJK UNIFIED IDEOGRAPHS 4E00}", "\u{4E00}", T),
        ("\\N{CJK UNIFIED IDEOGRAPH-4E00}", "", E),
        ("\\N{LINE FEED}", "", E),
        ("\\cA\\ca\\c?", "\u{1}!\u{7f}", T),
        ("\\c", "", E),
        ("\\Qa.*\\E", "a.*", T),
        ("\\Qab\\E+", "abb", T),
        ("a\\Q\\E+", "aa", T),
        ("\\Q\\E*", "", E),
        ("\\Q\\E{2}", "", T),
        ("(?i)\\Qab\\E", "AB", T),
        ("[\\Qa-c\\E]", "b", F),
        ("[\\Qa-c\\E]", "-", T),
        // Quoting is removed before parsing: `\u004\Qa` is `J`,
        // `a*\Q\E+` the possessive `a*+` (black-box probes).
        ("\\u004\\Qa", "J", T),
        ("a*\\Q\\E+a", "aa", F),
    ]);
}

/// CLS-1…CLS-11: character classes.
#[test]
fn cls_classes() {
    check(&[
        ("[]a]", "]", T),
        ("[^]a]", "]", F),
        ("[]", "", E),
        ("[[]", "", E),
        ("[\\[]", "[", T),
        ("[.$^*+?(){}|]", "|", T),
        ("[\\b]", "", E),
        ("[z-a]", "", E),
        ("[\\x{1F600}-\\x{1F64F}]", "\u{1F610}", T),
        ("[a-c-e]", "-", T),
        ("[a-c-e]", "d", F),
        ("[\\d-z]", "-", T),
        ("[a--]", "", E),
        ("[--/]", ".", T),
        ("[a-\\d]", "", E),
        ("[a-[b]]", "-", T),
        ("[a-d[m-p]]", "n", T),
        ("[a-c&&b-dx]", "x", F),
        ("[a-c&&b-dx]", "b", T),
        ("[a-z&&b-y&&c-x]", "b", F),
        ("[a&b-d]", "&", T),
        ("[a&&]", "a", T),
        ("[&&a]", "a", T),
        ("[a&&&&b]", "a", F),
        ("[a&&&b]", "&", T),
        ("[&&&a]", "a", E),
        // Members after `&&` and single characters (black-box probes).
        ("[a&&[b]&c]", "a", T),
        ("[[a][b]&&]", "a", F),
        ("[[a][b]&&]", "b", T),
        ("[^a[b]]", "b", F),
        ("[^a[^b]]", "b", T),
        ("[^a&&b]", "a", T),
        ("(?x)[a b]", "b", T),
        ("(?x)[a#b\n]", "b", F),
        ("(?x)[^ a]", "b", T),
        ("(?x)[^ a]", "a", F),
        ("(?x)[ ^a]", "^", T),
        ("[\\v-z]", "x", T),
    ]);
}

/// PRE-1…PRE-5, PROP-1…PROP-8, SCR-1, BLK-1: predefined classes and
/// properties (the full sets are checked against Java's fingerprints in
/// `props::tests`).
#[test]
fn prop_predefined_and_properties() {
    check(&[
        ("\\d\\s\\w", "5 _", T),
        ("\\w", "é", F),
        ("(?U)\\w", "é", T),
        ("\\h", "\u{A0}", T),
        ("\\v", "\u{85}", T),
        (".", "\u{2028}", F),
        (".", "\u{B}", T),
        ("(?d).", "\r", T),
        ("(?s).", "\n", T),
        ("\\pLu", "Au", T),
        ("\\px", "", E),
        ("\\p{}", "", E),
        ("\\p{L", "", E),
        ("[^\\P{L}]", "a", T),
        ("\\p{GC=Lu}", "A", T),
        ("\\p{gc=lu}", "", E),
        ("\\p{IsLatin}", "\u{212A}", T),
        ("\\p{Latin}", "", E),
        ("\\p{IsOld_Italic}", "\u{10300}", T),
        ("\\p{IsOldItalic}", "", E),
        ("\\p{InGreek}", "\u{3B1}", T),
        ("\\p{InGreek_and_Coptic}", "", E),
        ("\\p{InBASIC_LATIN}", "a", T),
        ("\\p{IsHex_Digit}", "\u{1040}", T),
        ("\\p{IsEmojiPresentation}", "", E),
        ("\\p{all}", "\u{10FFFF}", T),
        ("\\p{All}", "", E),
        ("\\p{IsLower}", "é", T),
        ("\\p{IsjavaDigit}", "1", T),
        ("\\p{javadigit}", "", E),
        ("\\p{Cn}", "\u{378}", T),
    ]);
}

/// ANC-1…ANC-7, WB-1…WB-4: anchors and boundaries.
#[test]
fn anc_anchors_and_boundaries() {
    check(&[
        ("^", "", T),
        ("(?m)^", "", F),
        ("(?m)^$", "", F),
        ("(?m)a\n^", "a\n", F),
        ("a$", "a\n", F),
        ("a$\n", "a\n", T),
        ("a$\r\n", "a\r\n", T),
        ("a$\n", "a\r\n", F),
        ("(?m)$", "", T),
        ("(?m)a$\r\nb", "a\r\nb", T),
        ("(?m)a\r$\nb", "a\r\nb", F),
        ("a\\Ab", "ab", F),
        ("\\Ga", "a", T),
        ("a\\Gb", "ab", F),
        ("^*a", "a", T),
        ("\\b", "", F),
        ("\\B", "", T),
        ("\\B{1}", "", T),
        ("\\b{x}", "", E),
        ("a\\b-", "a-", T),
        ("é\\b", "é", F),
        ("(?U)é\\b", "é", T),
        // WB-3: a non-spacing mark after a letter is a word character.
        ("e\\x{301}\\b", "e\u{301}", T),
        ("e\\b\\x{301}", "e\u{301}", F),
        ("!\\x{301}\\b", "!\u{301}", F),
        ("\\b{g}", "", T),
        ("e\\b{g}\\x{301}", "e\u{301}", F),
    ]);
}

/// `\b{g}` reads from `last` (see `backtrack`), black-box probes.
#[test]
fn wb4_grapheme_boundary_last() {
    check(&[
        ("a\\b{g}b", "ab", T),
        ("ab\\b{g}\\x{301}", "ab\u{301}", T),
        (".{1}\\b{g}bc", "abc", F),
        ("a*\\b{g}bc", "abc", T),
        ("(?=a)a\\b{g}bc", "abc", F),
        ("a(?<=a)\\b{g}bc", "abc", T),
    ]);
    let err = CypherRegex::new("(?=ab)a\\b{g}b")
        .unwrap()
        .is_match("ab")
        .unwrap_err();
    assert!(
        matches!(&err, MatchError::JavaException { exception, .. }
        if exception.starts_with("java.lang.StringIndexOutOfBoundsException")),
        "{err}"
    );
}

/// LBRK-1, GC-1…GC-3: `\R` and `\X`.
#[test]
fn lbrk_gc_linebreak_and_grapheme() {
    check(&[
        ("\\R\\n", "\r\n", T),
        ("(\\R)\\n", "\r\n", T),
        ("\\R\\R", "\r\n", T),
        ("\\R{2}", "\r\n", F),
        ("\\R?\\n", "\r\n", F),
        ("\\R{1,2}", "\r\n", T),
        ("(?:\\R)+\\n", "\r\n", F),
        ("(?:\\R)?\\n", "\r\n", T),
        ("(?:\\R|x)+\\n", "\r\n", T),
        ("(?>\\R)\\n", "\r\n", F),
        ("\\X", "e\u{301}", T),
        ("e\\X", "e\u{301}", T),
        ("\\X\\x{301}", "e\u{301}", F),
        ("\\X\\n", "\r\n", F),
        ("\\X", "", F),
        ("\\X", "\u{1F468}\u{200D}\u{1F469}", T),
        ("\\x{1F468}\\X", "\u{1F468}\u{200D}\u{1F469}", F),
        ("[\\X]", "", E),
    ]);
}

/// QNT-1…QNT-8: quantifiers.
#[test]
fn qnt_quantifiers() {
    check(&[
        ("a{02}", "aa", T),
        ("a{,3}", "", E),
        ("a{ 2}", "", E),
        ("(?x)a{1, 2}", "aa", T),
        ("(?x)a{ 2}", "", E),
        ("a{2147483648}", "", E),
        ("a*+a", "aaa", F),
        ("(?:ab|a)++b", "ab", F),
        ("a**", "", E),
        ("a{2}*", "", E),
        ("a{2}{3}", "aa", T),
        ("a{2}{3}", "aaaaaa", F),
        ("{2}", "", T),
        ("(?i){2}a", "a", T),
        ("\\b{2}a", "a", T),
        ("(?=a){2}a", "a", T),
        ("a{2147483647}", "a", F),
        ("a{0,2147483647}", "aaa", T),
        ("(?:a|b){2147483647}", "ab", F),
        ("(a?){3}", "a", T),
        ("(?:a|\\G){2}", "a", F),
        ("(?:a|){2}", "a", T),
        // Possessive loops match each iteration once (black-box probe).
        ("((b*)b){2}+a", "bba", F),
    ]);
}

/// GRP-1…GRP-3, FLG-1…FLG-6, XM-1, XM-2: groups, flags, COMMENTS mode.
#[test]
fn grp_flg_groups_and_flags() {
    check(&[
        ("(?P<n>a)", "", E),
        ("(?#c)", "", E),
        ("(?|a)", "", E),
        ("()", "", T),
        ("(?<=)", "", T),
        ("(?<a1>x)\\k<a1>", "xx", T),
        ("(?)a(?-)(?i-)(?ii)(?i-i)", "a", T),
        ("(?i--i)", "", E),
        ("(?l)", "", E),
        ("a(?i)b", "aB", T),
        ("a(?i)b", "AB", F),
        ("(?i)a|b", "B", T),
        ("x(?i)|y", "Y", T),
        ("((?i)a)|b", "B", F),
        ("(?:a(?i))b", "aB", F),
        ("(?i:a)b", "AB", F),
        ("(?iU)é", "É", T),
        ("(?iU)(?-u)é", "É", F),
        ("(a)(?i)\\1", "aA", T),
        ("(?s:.)a", "\na", T),
        ("(?c)\\x{e5}", "a\u{30A}", F),
        ("(?x)a b # c\n c", "abc", T),
        ("(?x)a\\ b", "a b", T),
        ("(?x)a#c\u{2028}b", "a\u{2028}b", T),
        ("(?x)(? :a)", "a", T),
        ("(?x)(? <=a)", "", E),
        ("(?x)\\p{ L}", "a", T),
        ("(?x)\\p{Is Latin}", "", E),
    ]);
}

/// LA-1, LB-1…LB-6: look-around.
#[test]
fn lb_lookaround() {
    check(&[
        ("(?<=a)b", "ab", F),
        ("a(?<=a)b", "ab", T),
        ("(a)(?<=\\1)", "", E),
        ("(?<=(?:ab)*)", "", E),
        ("(?<=(?:abc)+)", "", F),
        ("(?<=a*)", "", T),
        ("(?<=\\X*)", "", T),
        ("(?<=(?=(?:ab)*))", "", T),
        ("(?<=(?:ab){1073741824})", "", E),
        ("a(?<=a{2147483647}bc)", "a", F),
        ("(?<=a+b+)a", "a", F),
        ("\\x{1F600}(?<=^\\x{1F600})", "😀", F),
        (".(?<=^.)", "😀", F),
        ("😀(?<=^😀)", "😀", T),
        ("(?:.(?<=^.))😀?", "😀", T),
        // The code-point window needs the supplementary character after the
        // look-behind (black-box probe).
        ("(?:😀)?.(?<=^.)", "😀", F),
        // Code-point starts never fall inside a pair (black-box probes).
        ("(?x)a.(?<=\\uDE00) #😀", "a😀", F),
        ("a.(?<=\\uDE00)", "a😀", T),
        ("(?s).*(?<=ab*|cb*)", "ab", F),
        ("(?x)(?s).*(?<=x*x*) #😀", "xxxx", T),
        ("\\x{1F600}(?<=.)x", "😀x", T),
        ("\\x{1F600}(?<=\\x{1F600})x", "😀x", F),
        ("\\x{1F600}(?<=\\uDE00)x", "😀x", T),
        ("(?<=\\X)x", "ax", F),
        ("(?<!\\X)x", "x", T),
        ("a(?<=\\X)x", "ax", F),
        (".(?<=(?iu)^𐐀)x", "𐐨x", T),
        (".(?<=(?i)^𐐀)x", "𐐨x", F),
        // A code-point window with fewer code points before the position
        // than its minimum still runs its body from the input start, so a
        // committed capture leaks out of a failed negative look-behind
        // (JDK 21 probes, from the cross-check's `(?<!(𐐨?)++T)\1`).
        ("(?<!(𐐨?)++T)\\1", "", T),
        ("(?<!(𐐨?)++TT)\\1", "", T),
        ("(?<!(.?)++TT)\\1𐐨{0}", "", T),
        ("(?<!(\\x{10428}?)++T)\\1", "", F),
        ("(?<!(a?)++T)\\1", "", F),
        ("(?<!(𐐨?)T)\\1", "", F),
        ("(?<=(𐐨?)++T)\\1", "T", F),
    ]);
}

/// LB-1: a plain or capturing group repeated by a greedy or lazy counted
/// quantifier other than `?` needs a body that matches one way, as the
/// loop strategy judges it, even with a count of zero (JDK 21 probes; the
/// cross-check found the first two).
#[test]
fn lb1_repeated_group_bodies() {
    check(&[
        ("(?<!((?>a?)){0})", "", E),
        ("(?<=((?>a?)){0})", "", E),
        ("(?<!(?:(?>a?)){0})", "", E),
        ("(?<=((?>a?)){2})", "", E),
        ("(?<=(a?){1})", "", E),
        ("(?<=(?:a|b){2})", "", E),
        ("(?<=(?:a?+){2})", "", E),
        ("(?<=(?:\\X){2})", "", E),
        ("(?<=(a{1,2}){2}?)", "", E),
        ("(?<!((?>a?))*)", "", E),
        // These compile; the answers on the empty string are Java's.
        ("(?<!(?>a?){0})", "", F),
        ("(?<=(?>a?){2})", "", T),
        ("(?<!((?>a)){0})", "", F),
        ("(?<!((?>a?)){0,1})", "", F),
        ("(?<!((?>a?))?)", "", F),
        ("(?<=(a?){2}+)", "", T),
        ("(?<=(?:\\R){2})", "", F),
        ("(?<=(?:a{2}){2})", "", F),
        ("(?<=(?:(?=a|b)c){2})", "", F),
        ("(?<=(?:(?>a)){2})", "", F),
    ]);
}

/// BR-1…BR-8: back-references.
#[test]
fn br_backreferences() {
    check(&[
        ("(a)\\10", "aa0", T),
        ("(a)\\2", "a", F),
        ("\\9", "", F),
        ("(\\2b|(a))+", "aab", T),
        ("\\k<a>(?<a>x)", "", E),
        ("(a)?\\1", "", F),
        ("(a\\1)", "a", F),
        ("(a\\1?)", "a", T),
        ("(a|b\\1)+", "aba", T),
        ("((a)\\2)+", "aaaa", T),
        ("(?:x(\\1?a))+", "xaxaa", T),
        ("(.)\\1", "😀😀", T),
        ("(?i)(é)\\1", "éÉ", F),
        ("(?iu)(ß)\\1", "ßẞ", T),
        ("(?iu)(k)\\1", "k\u{212A}", T),
        ("(?iu)(ß)\\1", "ßSS", F),
        ("(a)\\1++a", "aaa", F),
    ]);
}

/// CAP-1…CAP-5: captures, commitment and the zero-width quirk.
#[test]
fn cap_captures() {
    check(&[
        ("(?:(a)x|\\1)", "a", F),
        ("(?:(a)x|a)\\1", "aa", F),
        ("(?:(?=(.))x|\\1a+)", "aa", T),
        ("(?:(?>(a))x|\\1)", "a", T),
        ("(?:(a)*+x|\\1)", "a", T),
        ("(?:(?!(a))x|\\1)", "a", T),
        ("(?:(?!(a)b)x|\\1)", "a", F),
        ("(?:((?=(a)))x|\\2)", "a", T),
        ("(?:(?=(a))b)?a\\1", "aa", T),
        ("(?=(a))\\1", "a", T),
        ("(?!(a))\\1", "a", F),
        ("(\\b)*a\\1", "a", F),
        ("(x?)*a\\1", "a", T),
        ("(\\b)+a\\1", "a", T),
        ("(\\b)??\\1a", "a", T),
        // CAP-5 drops only the capture of the group the loop repeats; groups
        // inside an empty iteration keep theirs (JDK 21 probes; the
        // independent cross-check found the first five).
        ("(?:())*\\1", "", T),
        ("(?:(\\b))*a\\1", "a", T),
        ("(?:()){0,3}a\\1", "a", T),
        ("(?:(?=a)())*a\\1", "a", T),
        ("(?:(\\G))*a\\1", "a", T),
        ("((\\b))*a\\2", "a", T),
        ("((\\b))*a\\1", "a", F),
        ("(?:()b?)*a\\1", "a", T),
        ("(?:())*?\\1", "", F),
        ("(?:()){0}\\1", "", F),
        ("(?:())*+\\1", "", T),
    ]);
}

/// CI-1…CI-6: case-insensitive matching.
#[test]
fn ci_case_insensitive() {
    check(&[
        ("(?i)é", "É", F),
        ("(?i)k", "\u{212A}", F),
        ("(?iu)\u{212A}", "k", T),
        ("(?iu)ß", "ẞ", F),
        ("(?iu)ẞ", "ß", T),
        // Two or more adjacent literals drop the guard (black-box probe).
        ("(?iu)aß", "aẞ", T),
        ("(?iu)a(?:ß)", "aẞ", F),
        ("(?iu)[K-K]", "\u{212A}", F),
        ("(?iu)[k-k]", "\u{212A}", T),
        ("(?iu)[a-z]", "\u{130}", T),
        ("(?iu)[A-Z]", "\u{130}", F),
        ("(?i)[Z-a]", "z", T),
        ("(?i)[^K]", "k", F),
        ("(?iu)\\w", "\u{212A}", F),
        ("(?i)\\p{Lu}", "a", T),
        ("(?i)\\P{Lu}", "a", F),
        ("(?i)\\p{Lower}", "\u{C9}", F),
        ("(?iU)\\p{Lower}", "\u{C9}", T),
    ]);
}

/// SUR-1…SUR-4: surrogates.
#[test]
fn sur_surrogates() {
    check(&[
        ("\\uD83D", "😀", F),
        ("[\\uD83D]", "😀", F),
        ("[\\uD800-\\uDFFF]+", "😀", F),
        (".\\uDE00", "😀", F),
        ("..", "😀", F),
        ("😀{2}", "😀😀", T),
        ("[^a]{2}", "😀", F),
    ]);
}

/// `(?c)`: classes and properties match composed prefixes of the grapheme
/// cluster (black-box probes; see `canon`).
#[test]
fn ceq_inline_canonical_equivalence() {
    check(&[
        ("(?c)[\\x{e9}]", "e\u{301}", T),
        ("(?c)[e]\\x{301}", "e\u{301}", F),
        ("(?c)\\x{e9}", "e\u{301}", F),
        ("(?c).", "e\u{301}", F),
        ("(?c)\\p{L}", "e\u{302}\u{301}", T),
        ("(?c)[^a]\\x{301}", "e\u{302}\u{301}", T),
        ("(?c)[^a]+\\x{301}", "e\u{302}\u{301}", F),
        ("(?c)(?:[^a])+\\x{301}", "e\u{302}\u{301}", T),
        ("(?c)[\\x{c5}]", "\u{212B}", F),
        ("(?c)[\\x{ac00}]", "\u{1100}\u{1161}", T),
        ("(?c)[\\x{1ea1}]\\x{302}", "a\u{323}\u{302}", T),
        ("(?c)\\w", "e\u{301}", F),
        ("(?s).*(?<=(?c)[e])", "e", F),
    ]);
}

/// RES-1…RES-3 and the limits of this implementation.
#[test]
fn res_limits() {
    // RES-3: nesting past the limit is Java's compile-time stack overflow.
    let deep = "(".repeat(parse::MAX_NESTING + 1) + &")".repeat(parse::MAX_NESTING + 1);
    assert_eq!(
        CypherRegex::new(&deep).unwrap_err().description(),
        "Stack overflow during pattern compilation"
    );
    let ok = "(".repeat(1000) + &")".repeat(1000);
    assert!(CypherRegex::new(&ok).is_ok());
    // A deep loop is answered while its backtrack stack fits the memory
    // budget (Java overflows its default stack long before), and fails
    // with the recursion limit, never overflows, past it.
    let re = CypherRegex::new("(?:a|b)*(?=)").unwrap();
    assert_eq!(re.is_match(&"ab".repeat(100_000)), Ok(true));
    assert!(matches!(
        re.is_match(&"ab".repeat(1_200_000)),
        Err(MatchError::RecursionLimit { .. })
    ));
    // The cross-check's `(?>a|b)*` on a million characters.
    let re = CypherRegex::new("(?>a|b)*").unwrap();
    assert_eq!(re.is_match(&"ab".repeat(500_000)), Ok(true));
    // Loops Java runs without recursion.
    for p in ["a*", "(?:ab)*", "(a)*(?=)", "(?:a{2})*(?=)", "[ab]*(?=)"] {
        let re = CypherRegex::new(p).unwrap();
        let s = if p.contains("ab)") {
            "ab".repeat(300_000)
        } else {
            "a".repeat(600_000)
        };
        assert_eq!(re.is_match(&s), Ok(true), "{p}");
    }
    // The backtrack limit: catastrophic backtracking is refused, and the
    // message carries the pattern and limit.
    let cat = "(?:(a+)+)+b(?=)";
    let re = CypherRegex::new(cat).unwrap();
    assert_eq!(re.is_match(&"a".repeat(12)), Ok(false));
    let err = re.is_match(&"a".repeat(40)).unwrap_err();
    assert_eq!(
        parse_backtrack_limit_message(&format!("wrapped: {err} (more)")),
        Some((cat.to_string(), DEFAULT_BACKTRACK_LIMIT))
    );
    let small = CypherRegex::with_backtrack_limit(cat, 50).unwrap();
    assert!(matches!(
        small.is_match(&"a".repeat(12)),
        Err(MatchError::BacktrackLimit { limit: 50, .. })
    ));
    // The same shape without look-around runs on the automaton.
    let lin = CypherRegex::with_backtrack_limit("((a+)+)+b", 50).unwrap();
    assert_eq!(lin.route(), Route::Linear);
    assert_eq!(lin.is_match(&"a".repeat(12)), Ok(false));
    // `(a|aa)*c\1` answers within the default budget at 25 characters.
    let re = CypherRegex::new("(a|aa)*c\\1").unwrap();
    assert_eq!(re.is_match(&"a".repeat(25)), Ok(false));
    // Forward work is charged too (four units per step), so a refused
    // match costs a bounded amount of work per step: a scan with no
    // resumption at all runs into a small limit.
    let scan = "b".repeat(100_000);
    let re = CypherRegex::with_backtrack_limit("b*+c", 1_000).unwrap();
    assert!(matches!(
        re.is_match(&scan),
        Err(MatchError::BacktrackLimit { .. })
    ));
    assert_eq!(CypherRegex::new("b*+c").unwrap().is_match(&scan), Ok(false));
    let re = CypherRegex::with_backtrack_limit("(a*)\\1c", 1_000).unwrap();
    assert!(matches!(
        re.is_match(&"a".repeat(10_000)),
        Err(MatchError::BacktrackLimit { .. })
    ));
}

/// The linear route answers exactly as the backtracking matcher does.
#[test]
fn linear_route_agrees_with_backtracking() {
    let patterns = [
        "a|ab",
        "(a|ab)(c|bcd)",
        "x*",
        "(?i)caf\\x{e9}",
        "(?iu)caf\\x{e9}",
        "\\w+@\\w+\\.com",
        "^\\d{3}-\\d{4}$",
        "[a-z&&[^aeiou]]+",
        "(?:ab|a)*b?",
        "\\p{L}+\\s?",
        "a{2,5}?",
        ".*",
        "(?s).*",
        "\\R",
        "[\\x{1F600}-\\x{1F64F}]+",
        "(?:)*",
        "(?:a|)*b",
    ];
    let subjects = [
        "", "a", "ab", "abc", "abcd", "café", "CAFÉ", "123-4567", "bcd", "aab", "\r\n", "a\r\nb",
        "😀😁", "x@y.com", "abab b", "\n",
    ];
    for p in patterns {
        let re = CypherRegex::new(p).unwrap();
        assert_eq!(re.route(), Route::Linear, "{p}");
        for s in subjects {
            assert_eq!(re.is_match(s), re.is_match_backtracking(s), "{p} on {s:?}");
        }
    }
    for p in [
        "a\\b", "(?=a)a", "(a)\\1", "a++", "\\X", "(?m)^a", "\\R+", "(?c)[a]",
    ] {
        assert_eq!(
            CypherRegex::new(p).unwrap().route(),
            Route::Backtracking,
            "{p}"
        );
    }
}

/// Throughput per route (run on demand, with optimizations:
/// `cargo nextest run -p uni-query-functions --lib --run-ignored only
/// -E 'test(throughput)' --no-capture`).
#[test]
#[ignore]
fn throughput() {
    let sentence = "The quick brown fox jumps over the lazy dog near the river bank 12345 ";
    let short = sentence.repeat(2);
    let long = sentence.repeat(150);
    let email = format!("{}user.name@example.com", "x".repeat(40));
    let lower = long.to_lowercase();
    let cases: &[(&str, &str)] = &[
        (".*lazy.*", &short),
        (".*lazy.*", &long),
        ("(?i).*LAZY DOG.*", &long),
        ("[\\w.]+@\\w+\\.com", &email),
        ("(?:\\w+\\s?)+\\d+\\s", &short),
        ("\\p{L}[\\p{L}\\s\\d]*", &long),
        ("(?=.*fox).*", &long),
        (".*\\b(\\w+) \\w+ \\1\\b.*", &short),
        ("(?:\\b\\w+\\b\\s*)+", &short),
        ("(?:\\w+\\s)+(?<=dog )\\w.*", &short),
        ("\\X+", &long),
        ("(?:[a-z]+|\\s|\\d)*+", &lower),
        ("(?:[a-z]+|\\s|\\d)*", &lower),
    ];
    eprintln!(
        "{:<32} {:>8} {:>13} {:>12}",
        "pattern", "bytes", "route", "MB/s"
    );
    for &(p, s) in cases {
        let re = CypherRegex::new(p).unwrap();
        let start = std::time::Instant::now();
        let mut iters = 0u64;
        while start.elapsed() < std::time::Duration::from_millis(300) {
            for _ in 0..16 {
                let _ = std::hint::black_box(re.is_match(std::hint::black_box(s)));
            }
            iters += 16;
        }
        let secs = start.elapsed().as_secs_f64();
        let mbps = (s.len() as f64 * iters as f64) / secs / 1e6;
        eprintln!(
            "{:<32} {:>8} {:>13} {:>12.1}",
            p,
            s.len(),
            format!("{:?}", re.route()),
            mbps
        );
    }
}

/// Large flat patterns compile in reasonable time without deep recursion
/// (spec RES-3: Java accepts 100 000 literal characters and 100 000
/// top-level alternatives).
#[test]
fn large_flat_patterns() {
    let t = std::time::Instant::now();
    let lits = "a".repeat(100_000);
    assert_eq!(CypherRegex::new(&lits).unwrap().is_match(&lits), Ok(true));
    let ci = format!("(?i){}", "ab".repeat(50_000));
    assert_eq!(
        CypherRegex::new(&ci)
            .unwrap()
            .is_match(&"AB".repeat(50_000)),
        Ok(true)
    );
    let alts = vec!["a"; 100_000].join("|");
    assert_eq!(CypherRegex::new(&alts).unwrap().is_match("a"), Ok(true));
    let ranges: String = (0..10_000u32)
        .filter_map(|i| char::from_u32(0x4E00 + 2 * i))
        .map(|c| format!("{c}-{c}"))
        .collect();
    let class = format!("[{ranges}]");
    assert_eq!(
        CypherRegex::new(&class).unwrap().is_match("\u{4E02}"),
        Ok(true)
    );
    let chars: String = (0..20_000u32)
        .filter_map(|i| char::from_u32(0x4E00 + i))
        .collect();
    assert_eq!(
        CypherRegex::new(&format!("[{chars}]"))
            .unwrap()
            .is_match("\u{4E05}"),
        Ok(true)
    );
    // 10 000 `&&` operands nest the class parser: refused like Java's stack
    // overflow, not a crash.
    let ands = format!("[a{}]", "&&a".repeat(10_000));
    assert_eq!(
        CypherRegex::new(&ands).unwrap_err().description(),
        "Stack overflow during pattern compilation"
    );
    assert!(
        t.elapsed() < std::time::Duration::from_secs(60),
        "{:?}",
        t.elapsed()
    );
}

/// Wall time of matches that run into the backtrack limit, on 1 KB
/// subjects (run on demand, with optimizations: `cargo nextest run
/// --cargo-profile release -p uni-query-functions --lib --run-ignored only
/// -E 'test(limit_timing)' --no-capture`; `UNI_JAVA_REGEX_LIMIT` sets the
/// limit, default [`DEFAULT_BACKTRACK_LIMIT`]).
#[test]
#[ignore]
fn limit_timing() {
    let limit = std::env::var("UNI_JAVA_REGEX_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_BACKTRACK_LIMIT);
    let a = "a".repeat(1000);
    let x = "x".repeat(1000);
    let words = "ab ".repeat(333);
    let head_a = format!("{}{}", "a".repeat(30), "b".repeat(970));
    let head_e = format!("{}{}", "a".repeat(30), "\u{e9}".repeat(970));
    let head_m = format!("{}{}", "a".repeat(30), "\u{301}".repeat(970));
    let ref_e = format!(
        "{}{}{}",
        "\u{e9}".repeat(485),
        "a".repeat(30),
        "\u{c9}".repeat(485)
    );
    let cases: &[(&str, &str)] = &[
        // Exponential searches.
        ("(?:(a+)+)+b(?=)", &a),
        ("(a+)+b\\1", &a),
        ("(a|aa)+c\\1", &a),
        ("(?:a|a)*b(?=)", &a),
        ("(.*a){20}x(?=)", &a),
        ("(\\w+\\s?)*$(?<=x)", &words),
        ("(?:(a*)*)*\\1b", &a),
        ("(a|a?)+\\1b", &a),
        ("(?:a|(?=a))*b(?=)", &a),
        ("(?:x?x?)*y(?=)", &x),
        ("(a*)*\\1\\1b", &a),
        ("(?:a|a)*(?<=a{0,200})b", &a),
        ("(?:\\X|a)*b", &a),
        ("(?:(?i)a|A)*b(?=)", &a),
        // Each resumption followed by a scan of the rest of the input.
        ("(?:a|a)*(?=b*+c)", &head_a),
        ("(?:a|a)*b*+c", &head_a),
        ("(a|a)*\\1b*+c", &head_a),
        // The same with costlier per-character work.
        ("(?:a|a)*\\p{L}*+c", &head_e),
        ("(?:a|a)*\\X*+c", &head_e),
        ("(?:a|a)*(?c)[\\x{e9}]*+c", &head_e),
        ("(?:a|a)*(?iu)\\x{e9}*+c", &head_e),
        ("(?:a|a)*(?:\\b|\\x{e9})*+c", &head_e),
        ("(?:a|a)*(?:\\b{g}\\x{e9})*+c", &head_e),
        ("(?:a|a)*(?<=\\x{e9}{0,900}+)c", &head_e),
        ("(?iu)(\\x{e9}{485})(?:a|a)*\\1c", &ref_e),
        ("(?iu)(\\x{e9}{485})(?:a|a)*\\1c", &ref_e),
        ("(?:a|a)*(?:\\x{301})*+c", &head_m),
        ("(?:a|a)*(?:\\b\\x{301})*+c", &head_m),
        ("(?:a|a)*\\X++c", &head_m),
    ];
    eprintln!("limit {limit}");
    for &(p, s) in cases {
        let re = CypherRegex::with_backtrack_limit(p, limit).unwrap();
        let t = std::time::Instant::now();
        let r = re.is_match(s);
        let ms = t.elapsed().as_secs_f64() * 1e3;
        let r = match r {
            Ok(b) => b.to_string(),
            Err(MatchError::BacktrackLimit { .. }) => "backtrack limit".to_string(),
            Err(e) => format!("{e}").chars().take(40).collect(),
        };
        eprintln!("{p:<28} {:>5} {:?} {ms:>9.1} ms  {r}", s.len(), re.route());
    }
}

/// Time and backtrack-stack size of long inputs whose loops keep a choice
/// point per iteration (run on demand like [`limit_timing`]).
#[test]
#[ignore]
fn stack_budget() {
    use std::sync::atomic::Ordering;
    let cases: &[(&str, &str, usize)] = &[
        ("(?>a|b)*", "ab", 50_000),
        ("(?>a|b)*", "ab", 250_000),
        ("(?>a|b)*", "ab", 500_000),
        ("(?>a|b)*", "ab", 1_000_000),
        ("(?>a|b)*", "ab", 2_000_000),
        // `(?=)` keeps these off the linear route.
        ("(?:a|b)*(?=)", "ab", 250_000),
        ("(?:a|b)*(?=)", "ab", 500_000),
        ("(?:a|b)*(?=)", "ab", 1_000_000),
        ("(a|b)*(?=)", "ab", 500_000),
        ("(?:a|b)*?(?=)", "ab", 500_000),
        ("(?:a|ab)*(?=)", "ab", 1_000_000),
        ("(?:ab)*(?=)", "ab", 1_000_000),
        ("(?:a|b)*+", "ab", 1_000_000),
    ];
    for &(p, unit, n) in cases {
        let s = unit.repeat(n / unit.len());
        let re = CypherRegex::new(p).unwrap();
        backtrack::PEAK_FRAMES.store(0, Ordering::Relaxed);
        let t = std::time::Instant::now();
        let r = re.is_match(&s);
        let ms = t.elapsed().as_secs_f64() * 1e3;
        let peak = backtrack::PEAK_FRAMES.load(Ordering::Relaxed);
        let r = match r {
            Ok(b) => b.to_string(),
            Err(e) => format!("{e}").chars().take(30).collect(),
        };
        eprintln!(
            "{p:<12} {n:>9} chars {ms:>9.1} ms  peak {peak:>8} records ({:>5.1} MB)  {r}",
            (peak * backtrack::FRAME_BYTES) as f64 / 1e6
        );
    }
}
