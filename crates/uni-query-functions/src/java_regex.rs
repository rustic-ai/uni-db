// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cypher's `=~` with Neo4j's semantics: `java.util.regex` syntax and
//! defaults, matched against the whole string.
//!
//! openCypher 9 dropped `=~`, so Neo4j is the reference. Neo4j compiles the
//! pattern with `Pattern.compile(p)` (no flags) and answers
//! `matcher(s).matches()`. This module reproduces that on top of the `regex`
//! crate: the pattern is parsed into a `regex-syntax` AST, and the AST is
//! translated node by node into an HIR that means what the Java pattern
//! means, then wrapped between `\A` and `\z`. The text of the pattern is
//! never rewritten.
//!
//! What the translation changes relative to the `regex` crate's own reading:
//!
//! - `\w`, `\d`, `\s`, `\b`, `\B` and the POSIX `\p{Lower}`-style classes
//!   are ASCII unless `(?U)` (`UNICODE_CHARACTER_CLASS`) is on.
//! - `(?i)` folds ASCII letters only unless `(?u)` (`UNICODE_CASE`) or `(?U)`
//!   is on, and then folds with Java's `toUpperCase`/`toLowerCase` rules,
//!   including the differences between a lone character, a run of literal
//!   characters and a character inside a class.
//! - `.` excludes all of Java's line terminators: `\n`, `\r`, U+0085, U+2028
//!   and U+2029.
//! - `\v` is Java's vertical-whitespace class, `\<` and `\>` are the literal
//!   characters, and `[[:alpha:]]` is a class of the characters `:alpha`.
//! - `\p{..}` names resolve the way Java resolves them (`Is`/`sc=`/`gc=`
//!   prefixes, case rules, POSIX names), and case-insensitive `\p{Lu}`-style
//!   classes match every cased letter, as in Java.
//!
//! Java constructs the `regex` crate cannot express are rejected with an
//! invalid-pattern error rather than answered differently: possessive and
//! stacked quantifiers, lookaround, backreferences, atomic groups, `\Q…\E`,
//! Unicode blocks, `$` (and multi-line `^`) where it is followed by more
//! pattern, and Rust-only syntax Java rejects. Two input-dependent cases are
//! refused at match time: multi-line `^`/`$` over an input containing U+0085,
//! U+2028 or U+2029, and an ASCII word boundary over an input with a
//! combining mark after a letter or digit (Java counts such a mark as a word
//! character).

use std::collections::HashMap;
use std::sync::OnceLock;

use regex_syntax::ast::{
    self, Assertion, AssertionKind, Ast, ClassPerlKind, ClassSet, ClassSetBinaryOpKind,
    ClassSetItem, ClassUnicodeKind, ClassUnicodeOpKind, Flag, FlagsItemKind, GroupKind,
    HexLiteralKind, LiteralKind, RepetitionKind, RepetitionRange, SpecialLiteralKind,
};
use regex_syntax::hir::{self, Class, ClassUnicode, ClassUnicodeRange, Hir, Look};

/// A compiled `=~` pattern.
#[derive(Debug, Clone)]
pub struct CypherRegex {
    pattern: String,
    regex: regex_automata::meta::Regex,
    guards: Guards,
}

/// Input conditions under which the translation is not exact, so a match
/// over such input is refused rather than answered.
#[derive(Debug, Clone, Copy, Default)]
struct Guards {
    /// `^`/`$` became `\r`/`\n` line anchors, which do not know Java's
    /// other line terminators (U+0085, U+2028, U+2029).
    unicode_terminators: bool,
    /// A multi-line `^` may be reached at the end of the input, where Java's
    /// never matches but a line anchor does after a final terminator (or on
    /// an empty input).
    caret_at_end: bool,
    /// A `$` with more pattern after it became a line anchor; Java's matches
    /// only before a *final* line terminator.
    inner_terminators: bool,
    /// An ASCII word boundary is used; Java treats a combining mark that
    /// follows a letter or digit as a word character.
    combining_marks: bool,
}

const JAVA_TERMINATORS: [char; 5] = ['\n', '\r', '\u{85}', '\u{2028}', '\u{2029}'];

impl Guards {
    /// Why `text` cannot be answered exactly, if it cannot.
    fn refusal(&self, text: &str) -> Option<&'static str> {
        if self.unicode_terminators && text.contains(['\u{85}', '\u{2028}', '\u{2029}']) {
            return Some(
                "multi-line ^ or $ is not supported over input containing U+0085, \
                 U+2028 or U+2029",
            );
        }
        if self.caret_at_end && (text.is_empty() || text.ends_with(JAVA_TERMINATORS)) {
            return Some(
                "a multi-line ^ that may match at the end is not supported over empty \
                 input or input ending in a line terminator",
            );
        }
        if self.inner_terminators {
            let body = text
                .strip_suffix("\r\n")
                .or_else(|| text.strip_suffix(['\n', '\r']))
                .unwrap_or(text);
            if body.contains(JAVA_TERMINATORS) {
                return Some(
                    "$ followed by more pattern is only supported over input whose \
                     only line terminator is a final \\n, \\r or \\r\\n",
                );
            }
        }
        if self.combining_marks && marked_word_char(text) {
            return Some(
                "\\b or \\B is not supported over input with a combining mark after a \
                 letter or digit",
            );
        }
        None
    }
}

impl CypherRegex {
    /// Compiles `pattern` with Java's syntax and defaults, anchored to match
    /// the whole input.
    ///
    /// # Errors
    ///
    /// Returns a message describing why the pattern is invalid, or why it
    /// uses a Java construct that cannot be evaluated exactly.
    pub fn new(pattern: &str) -> Result<Self, String> {
        let ast = ast::parse::ParserBuilder::new()
            .build()
            .parse(pattern)
            .map_err(|e| e.to_string())?;
        let mut tr = Translator {
            pattern,
            guards: Guards::default(),
        };
        let mut flags = JFlags::default();
        let body = tr.translate(
            &ast,
            &mut flags,
            Pos {
                head: true,
                tail: true,
                rest_nonnull: false,
            },
        )?;
        let anchored = Hir::concat(vec![Hir::look(Look::Start), body, Hir::look(Look::End)]);
        // Built from the HIR directly: printing it and re-parsing the text
        // is not faithful (a repetition of a repetition prints as `x{2}?`,
        // which reads back as a lazy quantifier).
        let regex = regex_automata::meta::Regex::builder()
            .build_from_hir(&anchored)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            pattern: pattern.to_string(),
            regex,
            guards: tr.guards,
        })
    }

    /// Whether `text` matches the pattern in full.
    ///
    /// # Errors
    ///
    /// Returns a message when the answer depends on input the translation
    /// cannot evaluate exactly (see the module documentation).
    pub fn is_match(&self, text: &str) -> Result<bool, String> {
        if let Some(why) = self.guards.refusal(text) {
            return Err(format!("pattern '{}': {why}", self.pattern));
        }
        Ok(self.regex.is_match(text))
    }
}

/// Whether `text` has a nonspacing mark directly after a letter or digit.
fn marked_word_char(text: &str) -> bool {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"[\p{L}\p{Nd}]\p{Mn}").expect("static pattern compiles"))
        .is_match(text)
}

/// The Java pattern flags that change meaning (`x` only changes parsing,
/// which the parser has already done).
#[derive(Debug, Clone, Copy, Default)]
struct JFlags {
    /// `i`: `CASE_INSENSITIVE`.
    ci: bool,
    /// `u`: `UNICODE_CASE`.
    ucase: bool,
    /// `U`: `UNICODE_CHARACTER_CLASS`.
    uclass: bool,
    /// `m`: `MULTILINE`.
    multiline: bool,
    /// `s`: `DOTALL`.
    dotall: bool,
    /// `x`: `COMMENTS` (the parser has applied it; kept for `#` in classes).
    verbose: bool,
}

/// Where a node sits relative to the whole pattern, for the anchors whose
/// Java meaning can only be expressed at the ends of the match.
#[derive(Debug, Clone, Copy)]
struct Pos {
    /// Everything before the node can only match the empty string.
    head: bool,
    /// Everything after the node can only match the empty string.
    tail: bool,
    /// Something after the node must consume at least one character.
    rest_nonnull: bool,
}

struct Translator<'p> {
    pattern: &'p str,
    guards: Guards,
}

type R<T> = Result<T, String>;

fn class_of(chars: impl IntoIterator<Item = char>) -> ClassUnicode {
    ClassUnicode::new(chars.into_iter().map(|c| ClassUnicodeRange::new(c, c)))
}

fn class_range(a: char, b: char) -> ClassUnicode {
    ClassUnicode::new([ClassUnicodeRange::new(a, b)])
}

fn union(mut a: ClassUnicode, b: &ClassUnicode) -> ClassUnicode {
    a.union(b);
    a
}

fn negated(mut a: ClassUnicode) -> ClassUnicode {
    a.negate();
    a
}

/// The class a `regex`-crate pattern such as `\p{Lu}` denotes.
fn rust_class(pattern: &str) -> ClassUnicode {
    match regex_syntax::ParserBuilder::new().build().parse(pattern) {
        Ok(h) => match h.into_kind() {
            hir::HirKind::Class(Class::Unicode(c)) => c,
            hir::HirKind::Literal(hir::Literal(bytes)) => std::str::from_utf8(&bytes)
                .map(|s| class_of(s.chars()))
                .unwrap_or_else(|_| ClassUnicode::empty()),
            _ => ClassUnicode::empty(),
        },
        // `\p{Cs}`: surrogates are not Unicode scalar values.
        Err(_) => ClassUnicode::empty(),
    }
}

fn ascii_class(spec: &[(char, char)]) -> ClassUnicode {
    ClassUnicode::new(spec.iter().map(|&(a, b)| ClassUnicodeRange::new(a, b)))
}

fn ascii_word() -> ClassUnicode {
    ascii_class(&[('0', '9'), ('A', 'Z'), ('_', '_'), ('a', 'z')])
}

fn ascii_space() -> ClassUnicode {
    ascii_class(&[('\t', '\r'), (' ', ' ')])
}

fn ascii_alpha() -> ClassUnicode {
    ascii_class(&[('A', 'Z'), ('a', 'z')])
}

fn ascii_punct() -> ClassUnicode {
    ascii_class(&[('!', '/'), (':', '@'), ('[', '`'), ('{', '~')])
}

/// Java's line terminators: what `.` does not match without `(?s)`.
fn line_terminators() -> ClassUnicode {
    class_of(['\n', '\r', '\u{85}', '\u{2028}', '\u{2029}'])
}

/// Java's `\v`.
fn vertical_whitespace() -> ClassUnicode {
    ascii_class(&[('\n', '\r'), ('\u{85}', '\u{85}'), ('\u{2028}', '\u{2029}')])
}

/// `Character.isLowerCase || isUpperCase || isTitleCase`: what a
/// case-insensitive case property matches in Java.
fn cased_letters() -> ClassUnicode {
    let mut c = rust_class(r"\p{Lowercase}");
    c.union(&rust_class(r"\p{Uppercase}"));
    c.union(&rust_class(r"\p{Lt}"));
    c
}

/// Java's `GRAPH()`: not a separator, control, surrogate or unassigned.
fn unicode_graph() -> ClassUnicode {
    negated(rust_class(r"[\p{Zs}\p{Zl}\p{Zp}\p{Cc}\p{Cn}]"))
}

/// Java's `BLANK()`: a space separator or a tab.
fn unicode_blank() -> ClassUnicode {
    union(rust_class(r"\p{Zs}"), &class_of(['\t']))
}

// ---------------------------------------------------------------------------
// Java's simple case mappings
// ---------------------------------------------------------------------------

/// `Character.toUpperCase(int)`: the simple (one-to-one) uppercase mapping.
///
/// `char::to_uppercase` is the full mapping; where that expands to several
/// characters the simple mapping is the character itself, except for the
/// Greek letters with ypogegrammeni, which map to their prosgegrammeni form.
fn java_upper(c: char) -> char {
    let cp = c as u32;
    let simple = match cp {
        0x1F80..=0x1F87 | 0x1F90..=0x1F97 | 0x1FA0..=0x1FA7 => Some(cp + 8),
        0x1FB3 => Some(0x1FBC),
        0x1FC3 => Some(0x1FCC),
        0x1FF3 => Some(0x1FFC),
        _ => None,
    };
    if let Some(u) = simple {
        return char::from_u32(u).unwrap_or(c);
    }
    let mut up = c.to_uppercase();
    match (up.next(), up.next()) {
        (Some(u), None) => u,
        _ => c,
    }
}

/// `Character.toLowerCase(int)`: the simple lowercase mapping. The only
/// character whose full mapping expands is U+0130, whose simple mapping is
/// `i`.
fn java_lower(c: char) -> char {
    if c == '\u{130}' {
        return 'i';
    }
    let mut low = c.to_lowercase();
    match (low.next(), low.next()) {
        (Some(l), None) => l,
        _ => c,
    }
}

/// Characters whose `toLowerCase(toUpperCase(ch))` is not themselves,
/// grouped by that key.
fn fold_groups() -> &'static HashMap<char, Vec<char>> {
    static MAP: OnceLock<HashMap<char, Vec<char>>> = OnceLock::new();
    MAP.get_or_init(|| {
        let mut map: HashMap<char, Vec<char>> = HashMap::new();
        for cp in 0..=0x10FFFF_u32 {
            if let Some(ch) = char::from_u32(cp) {
                let key = java_lower(java_upper(ch));
                if key != ch {
                    map.entry(key).or_default().push(ch);
                }
            }
        }
        map
    })
}

/// Characters with a non-trivial `toUpperCase` or `toLowerCase(toUpperCase)`.
fn cased_chars() -> &'static [char] {
    static LIST: OnceLock<Vec<char>> = OnceLock::new();
    LIST.get_or_init(|| {
        (0..=0x10FFFF_u32)
            .filter_map(char::from_u32)
            .filter(|&ch| {
                let up = java_upper(ch);
                up != ch || java_lower(up) != ch
            })
            .collect()
    })
}

/// Java's `SingleU(lower)`: `lower` and every `ch` with
/// `toLowerCase(toUpperCase(ch)) == lower`.
fn unicode_fold_set(key: char) -> ClassUnicode {
    let mut chars = vec![key];
    if let Some(more) = fold_groups().get(&key) {
        chars.extend(more.iter().copied());
    }
    class_of(chars)
}

// ---------------------------------------------------------------------------
// Translation
// ---------------------------------------------------------------------------

fn repetition_bounds(kind: &RepetitionKind) -> (u32, Option<u32>) {
    match kind {
        RepetitionKind::ZeroOrOne => (0, Some(1)),
        RepetitionKind::ZeroOrMore => (0, None),
        RepetitionKind::OneOrMore => (1, None),
        RepetitionKind::Range(RepetitionRange::Exactly(n)) => (*n, Some(*n)),
        RepetitionKind::Range(RepetitionRange::AtLeast(n)) => (*n, None),
        RepetitionKind::Range(RepetitionRange::Bounded(m, n)) => (*m, Some(*n)),
    }
}

/// An assertion that, in Java, is a literal character (`\<`, `\>`).
fn is_angle(a: &Assertion) -> bool {
    matches!(
        a.kind,
        AssertionKind::WordBoundaryStartAngle | AssertionKind::WordBoundaryEndAngle
    )
}

/// The node can only match the empty string.
fn empty_only(a: &Ast) -> bool {
    match a {
        Ast::Empty(_) | Ast::Flags(_) => true,
        Ast::Assertion(x) => !is_angle(x),
        Ast::Literal(_)
        | Ast::Dot(_)
        | Ast::ClassUnicode(_)
        | Ast::ClassPerl(_)
        | Ast::ClassBracketed(_) => false,
        Ast::Repetition(r) => repetition_bounds(&r.op.kind).1 == Some(0) || empty_only(&r.ast),
        Ast::Group(g) => empty_only(&g.ast),
        Ast::Alternation(x) => x.asts.iter().all(empty_only),
        Ast::Concat(x) => x.asts.iter().all(empty_only),
    }
}

/// The node can match the empty string.
fn nullable(a: &Ast) -> bool {
    match a {
        Ast::Empty(_) | Ast::Flags(_) => true,
        Ast::Assertion(x) => !is_angle(x),
        Ast::Literal(_)
        | Ast::Dot(_)
        | Ast::ClassUnicode(_)
        | Ast::ClassPerl(_)
        | Ast::ClassBracketed(_) => false,
        Ast::Repetition(r) => repetition_bounds(&r.op.kind).0 == 0 || nullable(&r.ast),
        Ast::Group(g) => nullable(&g.ast),
        Ast::Alternation(x) => x.asts.iter().any(nullable),
        Ast::Concat(x) => x.asts.iter().all(nullable),
    }
}

/// A node Java parses as one character of a literal run: a literal (other
/// than `\v`, which is a class in Java) or `\<` / `\>`.
fn is_run_char(a: &Ast) -> bool {
    match a {
        Ast::Literal(l) => !matches!(
            l.kind,
            LiteralKind::Special(SpecialLiteralKind::VerticalTab)
        ),
        Ast::Assertion(x) => is_angle(x),
        _ => false,
    }
}

impl Translator<'_> {
    fn text(&self, span: &ast::Span) -> &str {
        &self.pattern[span.start.offset..span.end.offset]
    }

    fn translate(&mut self, a: &Ast, flags: &mut JFlags, pos: Pos) -> R<Hir> {
        match a {
            Ast::Empty(_) => Ok(Hir::empty()),
            Ast::Flags(sf) => {
                apply_flags(&sf.flags, flags)?;
                Ok(Hir::empty())
            }
            Ast::Literal(l) => Ok(Hir::class(Class::Unicode(self.literal(l, *flags, false)?))),
            Ast::Dot(_) => Ok(if flags.dotall {
                Hir::dot(hir::Dot::AnyChar)
            } else {
                Hir::class(Class::Unicode(negated(line_terminators())))
            }),
            Ast::Assertion(x) => self.assertion(x, *flags, pos),
            Ast::ClassUnicode(c) => Ok(Hir::class(Class::Unicode(self.property(c, *flags)?))),
            Ast::ClassPerl(c) => Ok(Hir::class(Class::Unicode(perl(c, *flags)))),
            Ast::ClassBracketed(c) => Ok(Hir::class(Class::Unicode(self.bracketed(c, *flags)?))),
            Ast::Repetition(r) => {
                if matches!(*r.ast, Ast::Repetition(_)) {
                    return Err(
                        if matches!(r.op.kind, RepetitionKind::OneOrMore) && r.greedy {
                            "possessive quantifiers (*+, ++, ?+, {n,m}+) are not supported"
                                .to_string()
                        } else {
                            "a quantifier cannot follow another quantifier".to_string()
                        },
                    );
                }
                if self.text(&r.op.span).starts_with("{,") {
                    return Err("a repetition needs a lower bound: {,n} is not valid".to_string());
                }
                let (min, max) = repetition_bounds(&r.op.kind);
                let repeats = max.is_none_or(|m| m > 1) && !empty_only(&r.ast);
                let inner = Pos {
                    head: pos.head && !repeats,
                    tail: pos.tail && !repeats,
                    rest_nonnull: pos.rest_nonnull,
                };
                let sub = self.translate(&r.ast, flags, inner)?;
                Ok(Hir::repetition(hir::Repetition {
                    min,
                    max,
                    greedy: r.greedy,
                    sub: Box::new(sub),
                }))
            }
            Ast::Group(g) => {
                let saved = *flags;
                match &g.kind {
                    GroupKind::CaptureIndex(_) => {}
                    GroupKind::CaptureName {
                        starts_with_p,
                        name,
                    } => {
                        if *starts_with_p {
                            return Err("named groups are written (?<name>...), not (?P<name>...)"
                                .to_string());
                        }
                        let mut cs = name.name.chars();
                        let valid = cs.next().is_some_and(|c| c.is_ascii_alphabetic())
                            && cs.all(|c| c.is_ascii_alphanumeric());
                        if !valid {
                            return Err(format!(
                                "group name '{}' must be an ASCII letter followed by \
                                 letters or digits",
                                name.name
                            ));
                        }
                    }
                    GroupKind::NonCapturing(f) => apply_flags(f, flags)?,
                }
                let h = self.translate(&g.ast, flags, pos);
                *flags = saved;
                h
            }
            Ast::Alternation(x) => {
                // Flags set in one branch stay set in the following ones,
                // in Java as in the parser.
                let mut branches = Vec::with_capacity(x.asts.len());
                for b in &x.asts {
                    branches.push(self.translate(b, flags, pos)?);
                }
                Ok(Hir::alternation(branches))
            }
            Ast::Concat(x) => {
                let items = &x.asts;
                let run_char: Vec<bool> = items.iter().map(is_run_char).collect();
                let mut out = Vec::with_capacity(items.len());
                for (i, item) in items.iter().enumerate() {
                    let item_pos = Pos {
                        head: pos.head && items[..i].iter().all(empty_only),
                        tail: pos.tail && items[i + 1..].iter().all(empty_only),
                        rest_nonnull: pos.rest_nonnull
                            || items[i + 1..].iter().any(|a| !nullable(a)),
                    };
                    let in_run = run_char[i]
                        && ((i > 0 && run_char[i - 1]) || run_char.get(i + 1) == Some(&true));
                    let h = match item {
                        Ast::Literal(l) if in_run => {
                            Hir::class(Class::Unicode(self.literal(l, *flags, true)?))
                        }
                        _ => self.translate(item, flags, item_pos)?,
                    };
                    out.push(h);
                }
                Ok(Hir::concat(out))
            }
        }
    }

    /// A literal character outside a class. `in_run` is a character in a run
    /// of two or more literals, which Java matches as a slice and folds
    /// differently from a lone character.
    fn literal(&self, l: &ast::Literal, flags: JFlags, in_run: bool) -> R<ClassUnicode> {
        check_literal_syntax(l)?;
        if matches!(
            l.kind,
            LiteralKind::Special(SpecialLiteralKind::VerticalTab)
        ) {
            return Ok(vertical_whitespace());
        }
        Ok(literal_set(l.c, flags, in_run))
    }

    fn assertion(&mut self, a: &Assertion, flags: JFlags, pos: Pos) -> R<Hir> {
        let look = match a.kind {
            AssertionKind::WordBoundaryStartAngle => {
                return Ok(Hir::class(Class::Unicode(literal_set('<', flags, false))));
            }
            AssertionKind::WordBoundaryEndAngle => {
                return Ok(Hir::class(Class::Unicode(literal_set('>', flags, false))));
            }
            AssertionKind::StartText => Look::Start,
            AssertionKind::EndText => Look::End,
            AssertionKind::StartLine if !flags.multiline => Look::Start,
            AssertionKind::StartLine => {
                // Java's multi-line `^` matches after a line terminator but
                // never at the end of the input (not even an empty one).
                // Where something must still be consumed, the end is out of
                // reach and `\r`/`\n` line anchors say the rest.
                if pos.tail {
                    // Nothing can follow, so `^` would sit at the end of the
                    // input, where Java's multi-line `^` never matches.
                    return Ok(Hir::fail());
                }
                // Where the rest must consume a character, the end is out of
                // reach; otherwise the input decides.
                if !pos.rest_nonnull {
                    self.guards.caret_at_end = true;
                }
                if pos.head {
                    Look::Start
                } else {
                    self.guards.unicode_terminators = true;
                    Look::StartCRLF
                }
            }
            AssertionKind::EndLine => {
                if pos.tail {
                    // Nothing follows, so the match ends here: the end of
                    // the input, which `$` matches in both modes.
                    Look::End
                } else if flags.multiline {
                    self.guards.unicode_terminators = true;
                    Look::EndCRLF
                } else {
                    // Java's `$` also matches before a final line terminator.
                    // Over input whose only terminator is a final `\n`, `\r`
                    // or `\r\n`, that is exactly where a line anchor matches.
                    self.guards.inner_terminators = true;
                    Look::EndCRLF
                }
            }
            AssertionKind::WordBoundary | AssertionKind::NotWordBoundary => {
                let negate = matches!(a.kind, AssertionKind::NotWordBoundary);
                if flags.uclass {
                    if negate {
                        Look::WordUnicodeNegate
                    } else {
                        Look::WordUnicode
                    }
                } else {
                    self.guards.combining_marks = true;
                    if negate {
                        Look::WordAsciiNegate
                    } else {
                        Look::WordAscii
                    }
                }
            }
            AssertionKind::WordBoundaryStart
            | AssertionKind::WordBoundaryEnd
            | AssertionKind::WordBoundaryStartHalf
            | AssertionKind::WordBoundaryEndHalf => {
                return Err(format!("{} is not supported", self.text(&a.span)));
            }
        };
        Ok(Hir::look(look))
    }

    /// `\p{..}` / `\P{..}` / `\pX`, resolved the way `Pattern.family` does.
    fn property(&self, c: &ast::ClassUnicode, flags: JFlags) -> R<ClassUnicode> {
        let set = match &c.kind {
            ClassUnicodeKind::OneLetter(ch) => plain_property(&ch.to_string(), flags),
            ClassUnicodeKind::Named(name) => named_property(name, flags),
            ClassUnicodeKind::NamedValue { op, name, value } => {
                if *op != ClassUnicodeOpKind::Equal {
                    None
                } else {
                    match name.to_ascii_lowercase().as_str() {
                        "sc" | "script" => script(value),
                        "gc" | "general_category" => java_category(value, flags.ci),
                        "blk" | "block" => {
                            return Err("Unicode blocks are not supported".to_string());
                        }
                        _ => None,
                    }
                }
            }
        };
        let Some(set) = set.transpose()? else {
            return Err(format!("unknown character property {}", self.text(&c.span)));
        };
        Ok(if c.negated { negated(set) } else { set })
    }

    fn bracketed(&self, c: &ast::ClassBracketed, flags: JFlags) -> R<ClassUnicode> {
        let set = self.class_set(&c.kind, flags)?;
        Ok(if c.negated { negated(set) } else { set })
    }

    fn class_set(&self, s: &ClassSet, flags: JFlags) -> R<ClassUnicode> {
        match s {
            ClassSet::Item(item) => self.class_item(item, flags),
            ClassSet::BinaryOp(op) => match op.kind {
                ClassSetBinaryOpKind::Intersection => {
                    // Java ignores a missing operand: `[a&&]` is `[a]`.
                    let is_empty =
                        |s: &ClassSet| matches!(s, ClassSet::Item(ClassSetItem::Empty(_)));
                    if is_empty(&op.lhs) && is_empty(&op.rhs) {
                        return Err("a class must not be empty".to_string());
                    }
                    if is_empty(&op.rhs) {
                        return self.class_set(&op.lhs, flags);
                    }
                    if is_empty(&op.lhs) {
                        return self.class_set(&op.rhs, flags);
                    }
                    let mut l = self.class_set(&op.lhs, flags)?;
                    let r = self.class_set(&op.rhs, flags)?;
                    l.intersect(&r);
                    Ok(l)
                }
                ClassSetBinaryOpKind::Difference | ClassSetBinaryOpKind::SymmetricDifference => {
                    Err(format!(
                        "class operator in {} is not Java syntax (only && is)",
                        self.text(&op.span)
                    ))
                }
            },
        }
    }

    fn class_item(&self, item: &ClassSetItem, flags: JFlags) -> R<ClassUnicode> {
        match item {
            ClassSetItem::Empty(_) => Ok(ClassUnicode::empty()),
            ClassSetItem::Literal(l) => {
                check_literal_syntax(l)?;
                if matches!(
                    l.kind,
                    LiteralKind::Special(SpecialLiteralKind::VerticalTab)
                ) {
                    return Ok(vertical_whitespace());
                }
                // In `(?x)` mode Java reads a verbatim `#` inside a class as
                // the start of a comment; the parser keeps it as a literal.
                if flags.verbose && l.c == '#' && matches!(l.kind, LiteralKind::Verbatim) {
                    return Err("# inside a class starts a comment in (?x) mode".to_string());
                }
                Ok(class_single(l.c, flags))
            }
            ClassSetItem::Range(r) => {
                check_literal_syntax(&r.start)?;
                check_literal_syntax(&r.end)?;
                Ok(class_range_ci(r.start.c, r.end.c, flags))
            }
            ClassSetItem::Ascii(a) => {
                // Java has no POSIX bracket expressions: `[:alpha:]` is a
                // nested class of the characters `:`, `a`, `l`, `p`, `h`.
                let text = self.text(&a.span);
                let inner = text
                    .strip_prefix('[')
                    .and_then(|t| t.strip_suffix(']'))
                    .unwrap_or(text);
                let mut set = ClassUnicode::empty();
                for ch in inner.chars() {
                    set.union(&class_single(ch, flags));
                }
                Ok(set)
            }
            ClassSetItem::Unicode(u) => self.property(u, flags),
            ClassSetItem::Perl(p) => Ok(perl(p, flags)),
            ClassSetItem::Bracketed(b) => self.bracketed(b, flags),
            ClassSetItem::Union(u) => {
                let mut set = ClassUnicode::empty();
                for it in &u.items {
                    set.union(&self.class_item(it, flags)?);
                }
                Ok(set)
            }
        }
    }
}

/// Literal escapes the parser accepts that Java does not.
fn check_literal_syntax(l: &ast::Literal) -> R<()> {
    match l.kind {
        LiteralKind::HexBrace(HexLiteralKind::UnicodeShort | HexLiteralKind::UnicodeLong)
        | LiteralKind::HexFixed(HexLiteralKind::UnicodeLong) => {
            Err("\\u{..} and \\U escapes are not Java syntax; use \\uXXXX or \\x{..}".to_string())
        }
        _ => Ok(()),
    }
}

fn apply_flags(f: &ast::Flags, flags: &mut JFlags) -> R<()> {
    let mut on = true;
    for item in &f.items {
        match &item.kind {
            FlagsItemKind::Negation => on = false,
            FlagsItemKind::Flag(flag) => match flag {
                Flag::CaseInsensitive => flags.ci = on,
                Flag::MultiLine => flags.multiline = on,
                Flag::DotMatchesNewLine => flags.dotall = on,
                Flag::Unicode => flags.ucase = on,
                // Java's `U` is UNICODE_CHARACTER_CLASS, which implies
                // UNICODE_CASE (and clearing it clears both).
                Flag::SwapGreed => {
                    flags.uclass = on;
                    flags.ucase = on;
                }
                Flag::IgnoreWhitespace => flags.verbose = on,
                Flag::CRLF => return Err("(?R) is not a Java flag".to_string()),
            },
        }
    }
    Ok(())
}

/// A literal character outside a class (Java's `single` / slice nodes).
fn literal_set(c: char, flags: JFlags, in_run: bool) -> ClassUnicode {
    if !flags.ci {
        return class_of([c]);
    }
    if flags.ucase {
        let up = java_upper(c);
        let low = java_lower(up);
        // A lone character with no case pair is matched exactly; in a run
        // (`SliceU`) every character compares by `toLowerCase(toUpperCase)`.
        if up == low && !in_run {
            return class_of([c]);
        }
        return unicode_fold_set(low);
    }
    if c.is_ascii_alphabetic() {
        class_of([c.to_ascii_lowercase(), c.to_ascii_uppercase()])
    } else {
        class_of([c])
    }
}

/// A single character inside a class (`bitsOrSingle`).
fn class_single(c: char, flags: JFlags) -> ClassUnicode {
    let special = matches!(
        c as u32,
        0xff | 0xb5 | 0x49 | 0x69 | 0x53 | 0x73 | 0x4b | 0x6b | 0xc5 | 0xe5
    );
    if (c as u32) < 256 && !(flags.ci && flags.ucase && special) {
        if !flags.ci {
            return class_of([c]);
        }
        if c.is_ascii() {
            return class_of([c, c.to_ascii_uppercase(), c.to_ascii_lowercase()]);
        }
        if flags.ucase {
            return class_of([c, java_lower(c), java_upper(c)]);
        }
        return class_of([c]);
    }
    literal_set(c, flags, false)
}

/// A range inside a class (`Range` / `CIRange` / `CIRangeU`).
fn class_range_ci(a: char, b: char, flags: JFlags) -> ClassUnicode {
    let mut set = class_range(a, b);
    if !flags.ci {
        return set;
    }
    let inr = |ch: char| a <= ch && ch <= b;
    if flags.ucase {
        let extra = cased_chars().iter().copied().filter(|&ch| {
            let up = java_upper(ch);
            inr(up) || inr(java_lower(up))
        });
        set.union(&class_of(extra));
    } else {
        let extra = (0u8..128)
            .map(char::from)
            .filter(|&ch| inr(ch.to_ascii_uppercase()) || inr(ch.to_ascii_lowercase()));
        set.union(&class_of(extra));
    }
    set
}

/// `\d`, `\s`, `\w` and their negations.
fn perl(c: &ast::ClassPerl, flags: JFlags) -> ClassUnicode {
    let set = match (c.kind.clone(), flags.uclass) {
        (ClassPerlKind::Digit, false) => ascii_class(&[('0', '9')]),
        (ClassPerlKind::Space, false) => ascii_space(),
        (ClassPerlKind::Word, false) => ascii_word(),
        (ClassPerlKind::Digit, true) => rust_class(r"\p{Nd}"),
        (ClassPerlKind::Space, true) => rust_class(r"\p{White_Space}"),
        (ClassPerlKind::Word, true) => rust_class(r"\w"),
    };
    if c.negated { negated(set) } else { set }
}

/// `\p{name}` without `=`: `In…` blocks, `Is…` properties, scripts and
/// categories, otherwise POSIX names (under `(?U)`) and categories.
fn named_property(name: &str, flags: JFlags) -> Option<R<ClassUnicode>> {
    if name.starts_with("In") {
        return Some(Err(
            "Unicode blocks (\\p{In…}) are not supported".to_string()
        ));
    }
    if let Some(rest) = name.strip_prefix("Is") {
        return unicode_property(rest, flags.ci)
            .map(Ok)
            .or_else(|| posix_unicode(rest, flags.ci).map(Ok))
            .or_else(|| java_category(rest, flags.ci))
            .or_else(|| script(rest));
    }
    plain_property(name, flags)
}

fn plain_property(name: &str, flags: JFlags) -> Option<R<ClassUnicode>> {
    if flags.uclass
        && let Some(set) = posix_unicode(name, flags.ci)
    {
        return Some(Ok(set));
    }
    java_category(name, flags.ci)
}

/// `CharPredicates.getUnicodePredicate`.
fn unicode_property(name: &str, ci: bool) -> Option<ClassUnicode> {
    let case = |p: &str| if ci { cased_letters() } else { rust_class(p) };
    Some(match name.to_uppercase().as_str() {
        "ALPHABETIC" => rust_class(r"\p{Alphabetic}"),
        "ASSIGNED" => negated(rust_class(r"\p{Cn}")),
        "CONTROL" => rust_class(r"\p{Cc}"),
        "EMOJI" => rust_class(r"\p{Emoji}"),
        "EMOJI_PRESENTATION" => rust_class(r"\p{Emoji_Presentation}"),
        "EMOJI_MODIFIER" => rust_class(r"\p{Emoji_Modifier}"),
        "EMOJI_MODIFIER_BASE" => rust_class(r"\p{Emoji_Modifier_Base}"),
        "EMOJI_COMPONENT" => rust_class(r"\p{Emoji_Component}"),
        "EXTENDED_PICTOGRAPHIC" => rust_class(r"\p{Extended_Pictographic}"),
        "HEXDIGIT" | "HEX_DIGIT" => rust_class(r"[\p{Nd}\p{Hex_Digit}]"),
        "IDEOGRAPHIC" => rust_class(r"\p{Ideographic}"),
        "JOINCONTROL" | "JOIN_CONTROL" => rust_class(r"\p{Join_Control}"),
        "LETTER" => rust_class(r"\p{L}"),
        "LOWERCASE" => case(r"\p{Lowercase}"),
        "UPPERCASE" => case(r"\p{Uppercase}"),
        "TITLECASE" => case(r"\p{Lt}"),
        "NONCHARACTERCODEPOINT" | "NONCHARACTER_CODE_POINT" => {
            rust_class(r"\p{Noncharacter_Code_Point}")
        }
        "PUNCTUATION" => rust_class(r"\p{P}"),
        "WHITESPACE" | "WHITE_SPACE" => rust_class(r"\p{White_Space}"),
        "WORD" => rust_class(r"\w"),
        _ => return None,
    })
}

/// `CharPredicates.getPosixPredicate`: the Unicode POSIX classes, by
/// case-insensitive name.
fn posix_unicode(name: &str, ci: bool) -> Option<ClassUnicode> {
    Some(match name.to_uppercase().as_str() {
        "ALPHA" => rust_class(r"\p{Alphabetic}"),
        "LOWER" if ci => cased_letters(),
        "LOWER" => rust_class(r"\p{Lowercase}"),
        "UPPER" if ci => cased_letters(),
        "UPPER" => rust_class(r"\p{Uppercase}"),
        "SPACE" => rust_class(r"\p{White_Space}"),
        "PUNCT" => rust_class(r"\p{P}"),
        "XDIGIT" => rust_class(r"[\p{Nd}\p{Hex_Digit}]"),
        "ALNUM" => rust_class(r"[\p{Alphabetic}\p{Nd}]"),
        "CNTRL" => rust_class(r"\p{Cc}"),
        "DIGIT" => rust_class(r"\p{Nd}"),
        "BLANK" => unicode_blank(),
        "GRAPH" => unicode_graph(),
        "PRINT" => {
            let mut p = union(unicode_graph(), &unicode_blank());
            p.difference(&rust_class(r"\p{Cc}"));
            p
        }
        _ => return None,
    })
}

/// `CharPredicates.forProperty`: general categories and the ASCII POSIX
/// classes, by exact name.
fn java_category(name: &str, ci: bool) -> Option<R<ClassUnicode>> {
    const CATEGORIES: &[&str] = &[
        "Cn", "Lu", "Ll", "Lt", "Lm", "Lo", "Mn", "Me", "Mc", "Nd", "Nl", "No", "Zs", "Zl", "Zp",
        "Cc", "Cf", "Co", "Cs", "Pd", "Ps", "Pe", "Pc", "Po", "Sm", "Sc", "Sk", "So", "Pi", "Pf",
        "L", "M", "N", "Z", "C", "P", "S",
    ];
    let set = match name {
        "Lu" | "Ll" | "Lt" if ci => rust_class(r"[\p{Lu}\p{Ll}\p{Lt}]"),
        n if CATEGORIES.contains(&n) => rust_class(&format!(r"\p{{{n}}}")),
        "LC" => rust_class(r"[\p{Lu}\p{Ll}\p{Lt}]"),
        "LD" => rust_class(r"[\p{L}\p{Nd}]"),
        "L1" => class_range('\0', '\u{ff}'),
        "all" => class_range('\0', char::MAX),
        "ASCII" => class_range('\0', '\x7f'),
        "Alnum" => ascii_class(&[('0', '9'), ('A', 'Z'), ('a', 'z')]),
        "Alpha" => ascii_alpha(),
        "Blank" => class_of([' ', '\t']),
        "Cntrl" => ascii_class(&[('\0', '\x1f'), ('\x7f', '\x7f')]),
        "Digit" => ascii_class(&[('0', '9')]),
        "Graph" => ascii_class(&[('!', '~')]),
        "Lower" if ci => ascii_alpha(),
        "Lower" => ascii_class(&[('a', 'z')]),
        "Print" => ascii_class(&[(' ', '~')]),
        "Punct" => ascii_punct(),
        "Space" => ascii_space(),
        "Upper" if ci => ascii_alpha(),
        "Upper" => ascii_class(&[('A', 'Z')]),
        "XDigit" => ascii_class(&[('0', '9'), ('A', 'F'), ('a', 'f')]),
        n if n.starts_with("java") => {
            return Some(Err(format!("\\p{{{n}}} is not supported")));
        }
        _ => return None,
    };
    Some(Ok(set))
}

/// `Character.UnicodeScript.forName`.
fn script(name: &str) -> Option<R<ClassUnicode>> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    regex_syntax::ParserBuilder::new()
        .build()
        .parse(&format!(r"\p{{sc={name}}}"))
        .ok()
        .and_then(|h| match h.into_kind() {
            hir::HirKind::Class(Class::Unicode(c)) => Some(Ok(c)),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decodes the oracle files' escapes: `\\`, `\t`, `\n`, `\r`, `\x{HEX}`.
    fn unescape(s: &str) -> String {
        let mut out = String::new();
        let mut it = s.chars().peekable();
        while let Some(c) = it.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            match it.next() {
                Some('\\') => out.push('\\'),
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('x') => {
                    assert_eq!(it.next(), Some('{'), "bad escape in {s:?}");
                    let hex: String = it.by_ref().take_while(|&c| c != '}').collect();
                    let cp = u32::from_str_radix(&hex, 16).expect("hex escape");
                    out.push(char::from_u32(cp).expect("scalar value"));
                }
                other => panic!("bad escape \\{other:?} in {s:?}"),
            }
        }
        out
    }

    /// Every case in `testdata/java_regex_oracle.tsv` answers what Java's
    /// `Pattern.compile(p).matcher(s).matches()` answered (recorded by
    /// `scripts/java_regex_oracle/regenerate.sh`), or is refused with an
    /// error where the case is marked `reject`. Set `UNI_JAVA_REGEX_ORACLE`
    /// to check another oracle file in the same format.
    #[test]
    fn answers_match_java() {
        let text = match std::env::var("UNI_JAVA_REGEX_ORACLE") {
            Ok(path) => std::fs::read_to_string(path).expect("oracle file"),
            Err(_) => include_str!("../testdata/java_regex_oracle.tsv").to_string(),
        };
        let mut failures = Vec::new();
        let mut checked = 0;
        for line in text.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let f: Vec<&str> = line.split('\t').collect();
            let (pattern, subject, java) = (unescape(f[0]), unescape(f[1]), f[2]);
            let reject = f.get(3) == Some(&"reject");
            let ours = match CypherRegex::new(&pattern) {
                Ok(re) => match re.is_match(&subject) {
                    Ok(true) => "true".to_string(),
                    Ok(false) => "false".to_string(),
                    Err(e) => format!("error ({e})"),
                },
                Err(e) => format!("error ({e})"),
            };
            checked += 1;
            let ok = if reject {
                ours.starts_with("error")
            } else {
                ours.split(' ').next() == Some(java)
            };
            if !ok {
                failures.push(format!(
                    "{:?} =~ {:?}: java {java}{}, uni-db {ours}",
                    subject,
                    pattern,
                    if reject { " (marked reject)" } else { "" }
                ));
            }
        }
        assert!(checked > 0, "no cases read");
        assert!(
            failures.is_empty(),
            "{} of {checked} cases differ from Java:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[test]
    fn simple_case_mappings_match_java_samples() {
        assert_eq!(java_upper('\u{df}'), '\u{df}');
        assert_eq!(java_lower('\u{1e9e}'), '\u{df}');
        assert_eq!(java_upper('\u{1fb3}'), '\u{1fbc}');
        assert_eq!(java_lower('\u{1fbc}'), '\u{1fb3}');
        assert_eq!(java_lower('\u{130}'), 'i');
        assert_eq!(java_upper('\u{131}'), 'I');
        assert_eq!(java_upper('\u{17f}'), 'S');
    }
}
