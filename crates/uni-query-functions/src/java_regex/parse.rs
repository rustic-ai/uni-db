// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Java pattern syntax to [`Node`]s (spec §1–§4, §7, §8, §10).
//!
//! The parser reads code points and reports errors with Java's
//! `PatternSyntaxException` descriptions (spec Appendix C) and indices.
//! Black-box probes settle what ERR-3 leaves open: an index counts code
//! points of the pattern after `\Q…\E` removal (`😀{3,1}` reports index 5),
//! errors are raised in the order Java reads the pattern (a look-behind
//! whose length has no maximum as soon as its body ends), and most errors
//! found after reading a construct point at its last character.
//!
//! COMMENTS mode (XM-1, XM-2): most reads go through [`Parser::peek`],
//! which skips whitespace and `#` comments while `x` is on; a few are raw,
//! as observed: the character after `\`, the character after `(?`, the
//! first character after the `{` of a counted quantifier or of `\b{`, the
//! text of `\N{…}`, and a property name after its first character.
//! `\Q…\E` is removed before parsing (see [`remove_quoting`]).

use std::collections::HashMap;
use std::sync::Arc;

use super::analyze;
use super::ast::{Assertion, Flags, INF, Node, Parsed, RefCase, RepMode, SetNode};
use super::casefold;
use super::charset::{CodeSet, MAX_CP};
use super::names;
use super::props::{self, PropFlags};
use super::ucd;

/// Nesting deeper than this is refused with Java's compile-time
/// stack-overflow description (spec RES-3). Java's own limit depends on
/// its thread stack and JIT state and varies between runs (black-box, JDK
/// 21 default stack: 1 000 nested groups compiled in every run, 1 200 to
/// 3 000 in some runs and not in others); up to this depth patterns compile
/// as they do on a Java stack large enough for them.
pub(crate) const MAX_NESTING: usize = 2000;

/// A `PatternSyntaxException`: its description and index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SyntaxError {
    pub(crate) description: String,
    pub(crate) index: i32,
}

/// What an escape denotes.
enum Esc {
    /// One code point (a range endpoint inside a class).
    Cp(u32),
    /// A predefined class (not case-folded, spec CI-5).
    Predef(CodeSet),
    /// A property, already resolved under the flags in force.
    Prop(CodeSet),
    /// Anything else: an anchor, a back-reference, `\R`, `\X`.
    Node(Node),
}

const fn ch(c: char) -> u32 {
    c as u32
}

fn is_x_space(c: u32) -> bool {
    matches!(c, 0x20 | 0x09 | 0x0A | 0x0B | 0x0C | 0x0D)
}

fn hex_val(c: u32) -> Option<u32> {
    char::from_u32(c).and_then(|c| c.to_digit(16))
}

fn is_ascii_digit(c: Option<u32>) -> bool {
    matches!(c, Some(d) if (ch('0')..=ch('9')).contains(&d))
}

/// `[ \t\n\x0B\f\r]`.
fn ascii_space() -> CodeSet {
    CodeSet::from_ranges(vec![(0x09, 0x0D), (0x20, 0x20)])
}

/// `\h` (spec PRE-1).
fn horizontal_space() -> CodeSet {
    CodeSet::from_ranges(vec![
        (0x09, 0x09),
        (0x20, 0x20),
        (0xA0, 0xA0),
        (0x1680, 0x1680),
        (0x180E, 0x180E),
        (0x2000, 0x200A),
        (0x202F, 0x202F),
        (0x205F, 0x205F),
        (0x3000, 0x3000),
    ])
}

/// `\v` (spec PRE-1).
fn vertical_space() -> CodeSet {
    CodeSet::from_ranges(vec![(0x0A, 0x0D), (0x85, 0x85), (0x2028, 0x2029)])
}

/// `.` under `flags` (spec PRE-3).
pub(crate) fn dot_set(flags: Flags) -> CodeSet {
    if flags.has(Flags::S) {
        CodeSet::all()
    } else if flags.has(Flags::D) {
        CodeSet::single(0x0A).negate()
    } else {
        CodeSet::from_cps([0x0A, 0x0D, 0x85, 0x2028, 0x2029]).negate()
    }
}

struct Parser {
    cps: Vec<u32>,
    pos: usize,
    flags: Flags,
    groups: usize,
    names: HashMap<String, usize>,
    /// Case closures already computed: `(code point, kind)`, kind 0 for
    /// ASCII-only, 1 for UNICODE_CASE, 2 for a literal run.
    closures: std::cell::RefCell<HashMap<(u32, u8), CodeSet>>,
    depth: usize,
}

/// Parses `pattern` as `Pattern.compile(pattern)` (no flags) would.
pub(crate) fn parse(pattern: &str) -> Result<Parsed, SyntaxError> {
    let cps = remove_quoting(pattern.chars().map(|c| c as u32).collect());
    let last_supplementary = cps.iter().rposition(|&c| c > 0xFFFF);
    let mut p = Parser {
        cps,
        pos: 0,
        flags: Flags::default(),
        groups: 0,
        names: HashMap::new(),
        closures: std::cell::RefCell::new(HashMap::new()),
        depth: 0,
    };
    let root = p.parse_expr()?;
    if p.pos > p.cps.len() {
        return Err(p.err_at("Unescaped trailing backslash", p.cps.len() as i32));
    }
    if p.pos < p.cps.len() {
        // Only an unmatched `)` stops the top-level expression early.
        return Err(p.err_at("Unmatched closing ')'", p.unit(p.pos) as i32 - 1));
    }
    Ok(Parsed {
        root,
        groups: p.groups,
        last_supplementary,
    })
}

/// Replaces every `\Q…\E` (or `\Q` to the end) by its text, before
/// anything else is parsed (spec LIT-10): ASCII letters and digits and
/// non-ASCII characters as they are, except that a digit right after `\Q`
/// becomes `\x3d`, and every other ASCII character escaped with `\`. Error
/// indices count positions in the result, which black-box probes expose:
/// `\Q()\E(` reports its unclosed group at 5, `\Q1\E(` at 5, `\Q12\E(`
/// at 6, `\Qa1\E(`, `\Qé\E(` and `\Q😀\E(` at 3, 2 and 2, and
/// `\N{w00\Q4}` names `w00\x34\`.
///
/// Java does this as a separate pass, which black-box probes expose:
/// `\u004\Qa` compiles as `\u004a`, `a*\Q\E+` is the possessive `a*+`, and
/// `[\Q\E]` is `[]` (unclosed).
fn remove_quoting(cps: Vec<u32>) -> Vec<u32> {
    let bs = ch('\\');
    if !cps.windows(2).any(|w| w[0] == bs && w[1] == ch('Q')) {
        return cps;
    }
    let mut out = Vec::with_capacity(cps.len() + 8);
    let mut i = 0;
    while i < cps.len() {
        let c = cps[i];
        if c != bs || i + 1 >= cps.len() {
            out.push(c);
            i += 1;
            continue;
        }
        if cps[i + 1] != ch('Q') {
            out.extend_from_slice(&cps[i..i + 2]);
            i += 2;
            continue;
        }
        i += 2;
        let first = i;
        while i < cps.len() {
            if cps[i] == bs && cps.get(i + 1) == Some(&ch('E')) {
                i += 2;
                break;
            }
            let q = cps[i];
            if i == first && (ch('0')..=ch('9')).contains(&q) {
                // `\x3N`, so the digit does not extend a preceding
                // back-reference or octal escape.
                out.extend_from_slice(&[bs, ch('x'), ch('3'), q]);
            } else {
                if q < 0x80 && !(q as u8).is_ascii_alphanumeric() {
                    out.push(bs);
                }
                out.push(q);
            }
            i += 1;
        }
    }
    out
}

impl Parser {
    /// The index Java reports for a code-point position (positions are
    /// code points already; one past the end is possible after a trailing
    /// `\`).
    fn unit(&self, cp_index: usize) -> usize {
        cp_index
    }

    fn err_at(&self, description: impl Into<String>, index: i32) -> SyntaxError {
        SyntaxError {
            description: description.into(),
            index,
        }
    }

    /// An error at the current position.
    fn err(&self, description: impl Into<String>) -> SyntaxError {
        self.err_at(description, self.unit(self.pos) as i32)
    }

    /// An error at the code point just read (Java reports most errors
    /// found after reading a construct's last character there: the `}` of
    /// `a{3,2}` or `\p{Foo}`, the end of a range `[z-a]`).
    fn err_before(&self, description: impl Into<String>) -> SyntaxError {
        self.err_at(description, self.unit(self.pos) as i32 - 1)
    }

    fn raw(&self) -> Option<u32> {
        self.cps.get(self.pos).copied()
    }

    fn raw_at(&self, i: usize) -> Option<u32> {
        self.cps.get(i).copied()
    }

    /// Skips COMMENTS-mode whitespace and comments (spec XM-1).
    fn skip_ws(&mut self) {
        if !self.flags.has(Flags::X) {
            return;
        }
        let unix = self.flags.has(Flags::D);
        while let Some(c) = self.raw() {
            if is_x_space(c) {
                self.pos += 1;
            } else if c == ch('#') {
                // A comment runs up to a line terminator, which it leaves
                // in place (`(?x)a#c\u2028b` matches "a\u2028b").
                self.pos += 1;
                while let Some(c) = self.raw() {
                    let terminator = if unix {
                        c == 0x0A
                    } else {
                        matches!(c, 0x0A | 0x0D | 0x85 | 0x2028 | 0x2029)
                    };
                    if terminator {
                        break;
                    }
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    /// The next code point, after COMMENTS-mode whitespace.
    fn peek(&mut self) -> Option<u32> {
        self.skip_ws();
        self.raw()
    }

    fn enter(&mut self) -> Result<(), SyntaxError> {
        self.depth += 1;
        if self.depth > MAX_NESTING {
            return Err(self.err("Stack overflow during pattern compilation"));
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Expressions, sequences, quantifiers
    // ------------------------------------------------------------------

    fn parse_expr(&mut self) -> Result<Node, SyntaxError> {
        let mut alts = vec![self.parse_seq()?];
        while self.peek() == Some(ch('|')) {
            self.pos += 1;
            alts.push(self.parse_seq()?);
        }
        Ok(if alts.len() == 1 {
            alts.pop().unwrap_or(Node::Empty)
        } else {
            Node::Alt(alts)
        })
    }

    fn parse_seq(&mut self) -> Result<Node, SyntaxError> {
        #[derive(PartialEq)]
        enum St {
            Start,
            Atom,
            Quant,
        }
        let mut items: Vec<Node> = Vec::new();
        // For each item: the literal code point it is, with the run of
        // adjacent literals it belongs to (a flag group starts a new run).
        let mut lits: Vec<Option<(u32, usize, bool)>> = Vec::new();
        let mut run = 0usize;
        let mut st = St::Start;
        while let Some(c) = self.peek() {
            let mut lit_cp = None;
            match char::from_u32(c).unwrap_or('\0') {
                '|' | ')' => break,
                q @ ('*' | '+' | '?') => {
                    if st != St::Atom {
                        // Reported just before what follows it, COMMENTS-mode
                        // whitespace skipped (black-box: `(?x)a* * b` at 8).
                        self.pos += 1;
                        self.skip_ws();
                        return Err(self.err_before(format!("Dangling meta character '{q}'")));
                    }
                    self.pos += 1;
                    let (min, max) = match q {
                        '*' => (0, INF),
                        '+' => (1, INF),
                        _ => (0, 1),
                    };
                    let mode = self.parse_suffix();
                    let body = items.pop().unwrap_or(Node::Empty);
                    lits.pop();
                    items.push(Node::Repeat {
                        body: Box::new(body),
                        min,
                        max,
                        mode,
                    });
                    lits.push(None);
                    st = St::Quant;
                }
                '{' => {
                    let (min, max, mode) = self.parse_closure()?;
                    if st == St::Atom {
                        let body = items.pop().unwrap_or(Node::Empty);
                        lits.pop();
                        items.push(Node::Repeat {
                            body: Box::new(body),
                            min,
                            max,
                            mode,
                        });
                        lits.push(None);
                    } else {
                        // A counted quantifier with no atom repeats the
                        // empty string (spec QNT-4, QNT-5).
                        items.push(Node::Repeat {
                            body: Box::new(Node::Empty),
                            min,
                            max,
                            mode,
                        });
                        lits.push(None);
                    }
                    st = St::Quant;
                }
                '(' => match self.parse_group()? {
                    Some(n) => {
                        items.push(n);
                        st = St::Atom;
                    }
                    None => {
                        st = St::Start;
                        run += 1;
                    }
                },
                '[' => {
                    let set = self.parse_class()?;
                    items.push(self.set_node(set, self.flags.has(Flags::C)));
                    st = St::Atom;
                }
                '.' => {
                    self.pos += 1;
                    items.push(Node::Set(SetNode {
                        set: Arc::new(dot_set(self.flags)),
                        canon: false,
                    }));
                    st = St::Atom;
                }
                '^' => {
                    self.pos += 1;
                    items.push(Node::Assert(Assertion::Caret {
                        multiline: self.flags.has(Flags::M),
                        unix: self.flags.has(Flags::D),
                    }));
                    st = St::Atom;
                }
                '$' => {
                    self.pos += 1;
                    items.push(Node::Assert(Assertion::Dollar {
                        multiline: self.flags.has(Flags::M),
                        unix: self.flags.has(Flags::D),
                    }));
                    st = St::Atom;
                }
                '\\' => {
                    self.pos += 1;
                    match self.parse_escape(false)? {
                        Esc::Cp(cp) => {
                            items.push(self.literal(cp));
                            lit_cp = Some(cp);
                            st = St::Atom;
                        }
                        Esc::Predef(set) => {
                            items.push(self.set_node(set, false));
                            st = St::Atom;
                        }
                        Esc::Prop(set) => {
                            items.push(self.set_node(set, self.flags.has(Flags::C)));
                            st = St::Atom;
                        }
                        Esc::Node(n) => {
                            items.push(n);
                            st = St::Atom;
                        }
                    }
                }
                _ => {
                    self.pos += 1;
                    items.push(self.literal(c));
                    lit_cp = Some(c);
                    st = St::Atom;
                }
            }
            if lits.len() < items.len() {
                let unicode_ci = self.flags.has(Flags::I) && self.flags.has(Flags::UCASE);
                lits.push(lit_cp.map(|cp| (cp, run, unicode_ci)));
            }
        }
        self.fold_literal_runs(&mut items, &lits);
        Ok(match items.len() {
            0 => Node::Empty,
            1 => items.pop().unwrap_or(Node::Empty),
            _ => Node::Seq(items),
        })
    }

    /// Two or more adjacent literals (a "slice") compare case-insensitively
    /// without the cased-pattern guard of a lone literal (spec CI-2): under
    /// `(?iu)`, `ß` alone matches only `ß`, but `aß` also matches `aẞ`
    /// (black-box probes; quantifying the last literal, or a flag group,
    /// ends the run; flags cannot change inside one).
    fn fold_literal_runs(&self, items: &mut [Node], lits: &[Option<(u32, usize, bool)>]) {
        let mut i = 0;
        while i < items.len() {
            let Some((_, r, unicode_ci)) = lits[i] else {
                i += 1;
                continue;
            };
            let mut j = i + 1;
            while j < items.len() && matches!(lits[j], Some((_, r2, _)) if r2 == r) {
                j += 1;
            }
            if j - i >= 2 && unicode_ci {
                for k in i..j {
                    if let Some((cp, _, _)) = lits[k] {
                        let set = self.closure(cp, 2, || casefold::char_closure_unguarded(cp));
                        items[k] = self.set_node(set, false);
                    }
                }
            }
            i = j;
        }
    }

    /// `?` (reluctant) or `+` (possessive) after a quantifier.
    fn parse_suffix(&mut self) -> RepMode {
        match self.peek() {
            Some(c) if c == ch('?') => {
                self.pos += 1;
                RepMode::Lazy
            }
            Some(c) if c == ch('+') => {
                self.pos += 1;
                RepMode::Possessive
            }
            _ => RepMode::Greedy,
        }
    }

    /// Digits of a count (the first already checked to be a digit). A
    /// count past `Integer.MAX_VALUE` is refused at the digit that makes it
    /// so (`a{2147483648` and `a{21474836470}` at index 11 and 12).
    fn parse_count(&mut self) -> Result<u64, SyntaxError> {
        let mut v: u64 = 0;
        while is_ascii_digit(self.peek()) {
            let d = u64::from(self.raw().unwrap_or(0) - ch('0'));
            v = v * 10 + d;
            if v > i32::MAX as u64 {
                return Err(self.err("Illegal repetition range"));
            }
            self.pos += 1;
        }
        Ok(v)
    }

    /// `{n}`, `{n,}`, `{n,m}` with its suffix (spec QNT-1). Error indices
    /// (black-box probes): "Illegal repetition" one past the `{`,
    /// "Illegal repetition range" at the `}` (`a{3,2}` at 5), "Unclosed
    /// counted closure" where the `}` was expected.
    fn parse_closure(&mut self) -> Result<(u32, u32, RepMode), SyntaxError> {
        let brace = self.pos;
        self.pos += 1;
        if !is_ascii_digit(self.raw()) {
            return Err(self.err_at("Illegal repetition", self.unit(brace) as i32 + 1));
        }
        let min = self.parse_count()?;
        let mut max = min;
        let mut unbounded = false;
        if self.peek() == Some(ch(',')) {
            self.pos += 1;
            if is_ascii_digit(self.peek()) {
                max = self.parse_count()?;
            } else {
                unbounded = true;
            }
        }
        if self.peek() != Some(ch('}')) {
            return Err(self.err("Unclosed counted closure"));
        }
        self.pos += 1;
        if !unbounded && min > max {
            return Err(self.err_before("Illegal repetition range"));
        }
        let mode = self.parse_suffix();
        let max = if unbounded { INF } else { max as u32 };
        Ok((min as u32, max, mode))
    }

    // ------------------------------------------------------------------
    // Groups and flags
    // ------------------------------------------------------------------

    /// Parses a group; `None` for a bare flag group `(?flags)`, whose
    /// flags stay in force to the end of the enclosing group (FLG-2).
    fn parse_group(&mut self) -> Result<Option<Node>, SyntaxError> {
        self.enter()?;
        let r = stacker::maybe_grow(64 * 1024, 1024 * 1024, || self.parse_group_inner());
        self.depth -= 1;
        r
    }

    fn parse_group_inner(&mut self) -> Result<Option<Node>, SyntaxError> {
        let open = self.pos;
        self.pos += 1; // '('
        let saved = self.flags;
        if self.peek() != Some(ch('?')) {
            self.groups += 1;
            let cap = self.groups;
            let body = self.parse_group_body(saved)?;
            return Ok(Some(Node::Group {
                cap: Some(cap),
                body: Box::new(body),
            }));
        }
        self.pos += 1; // '?'
        let c = self.raw().unwrap_or(0);
        let node = match char::from_u32(c).unwrap_or('\0') {
            ':' => {
                self.pos += 1;
                Node::Group {
                    cap: None,
                    body: Box::new(self.parse_group_body(saved)?),
                }
            }
            '=' | '!' => {
                self.pos += 1;
                Node::Look {
                    behind: false,
                    negate: c == ch('!'),
                    body: Box::new(self.parse_group_body(saved)?),
                    at: open,
                }
            }
            '>' => {
                self.pos += 1;
                Node::Atomic(Box::new(self.parse_group_body(saved)?))
            }
            '<' => {
                self.pos += 1;
                match self.peek() {
                    Some(c) if c == ch('=') || c == ch('!') => {
                        self.pos += 1;
                        let body = self.parse_expr()?;
                        // Java checks the length as soon as the body ends,
                        // before its `)` and anything after it (spec LB-1;
                        // `(?<!\1` and `(?<!\1)\q` report it, at the index
                        // before the `)` or the end).
                        if analyze::lookbehind_window(&body).is_none() {
                            self.skip_ws();
                            return Err(self.err_before(
                                "Look-behind group does not have an obvious maximum length",
                            ));
                        }
                        Node::Look {
                            behind: true,
                            negate: c == ch('!'),
                            body: Box::new(self.close_group(body, saved)?),
                            at: open,
                        }
                    }
                    _ => {
                        let name = self.parse_group_name()?;
                        if self.names.contains_key(&name) {
                            return Err(self.err_before(format!(
                                "Named capturing group <{name}> is already defined"
                            )));
                        }
                        self.groups += 1;
                        let cap = self.groups;
                        self.names.insert(name, cap);
                        Node::Group {
                            cap: Some(cap),
                            body: Box::new(self.parse_group_body(saved)?),
                        }
                    }
                }
            }
            // Black-box: these two are refused as a group type, every other
            // unknown character as a flag.
            '$' | '@' => return Err(self.err("Unknown group type")),
            _ => {
                if self.parse_flags()? {
                    // `(?flags)`: the flags persist in the enclosing group.
                    return Ok(None);
                }
                Node::Group {
                    cap: None,
                    body: Box::new(self.parse_group_body(saved)?),
                }
            }
        };
        Ok(Some(node))
    }

    /// The body of a group up to its `)`; the flags revert afterwards.
    fn parse_group_body(&mut self, saved: Flags) -> Result<Node, SyntaxError> {
        let body = self.parse_expr()?;
        self.close_group(body, saved)
    }

    /// Reads the `)` after a group body.
    fn close_group(&mut self, body: Node, saved: Flags) -> Result<Node, SyntaxError> {
        if self.peek() != Some(ch(')')) {
            return Err(self.err("Unclosed group"));
        }
        self.pos += 1;
        self.flags = saved;
        Ok(body)
    }

    /// Flag letters after `(?`; true for `(?flags)`, false for `(?flags:`.
    fn parse_flags(&mut self) -> Result<bool, SyntaxError> {
        let mut negate = false;
        loop {
            let c = self.peek().unwrap_or(0);
            let bit = match char::from_u32(c).unwrap_or('\0') {
                'i' => Flags::I,
                'd' => Flags::D,
                'm' => Flags::M,
                's' => Flags::S,
                'u' => Flags::UCASE,
                'x' => Flags::X,
                'U' => Flags::UCLASS | Flags::UCASE,
                'c' => Flags::C,
                '-' if !negate => {
                    negate = true;
                    self.pos += 1;
                    continue;
                }
                ')' => {
                    self.pos += 1;
                    return Ok(true);
                }
                ':' => {
                    self.pos += 1;
                    return Ok(false);
                }
                _ => return Err(self.err("Unknown inline modifier")),
            };
            if negate {
                self.flags.0 &= !bit;
            } else {
                self.flags.0 |= bit;
            }
            self.pos += 1;
        }
    }

    /// A group name up to and including its `>` (spec GRP-2).
    fn parse_group_name(&mut self) -> Result<String, SyntaxError> {
        let mut name = String::new();
        match self.peek() {
            Some(c) if c < 0x80 && (c as u8).is_ascii_alphabetic() => {}
            _ => return Err(self.err("capturing group name does not start with a Latin letter")),
        }
        while let Some(c) = self.peek() {
            if c < 0x80 && (c as u8).is_ascii_alphanumeric() {
                name.push(c as u8 as char);
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.peek() != Some(ch('>')) {
            return Err(self.err("named capturing group is missing trailing '>'"));
        }
        self.pos += 1;
        Ok(name)
    }

    // ------------------------------------------------------------------
    // Literals and escapes
    // ------------------------------------------------------------------

    fn set_node(&self, set: CodeSet, canon: bool) -> Node {
        Node::Set(SetNode {
            set: Arc::new(set),
            canon,
        })
    }

    /// The case closure of one literal code point under the current flags.
    fn char_set(&self, cp: u32) -> CodeSet {
        if self.flags.has(Flags::I) {
            let unicode = self.flags.has(Flags::UCASE);
            self.closure(cp, u8::from(unicode), || {
                casefold::char_closure(cp, unicode)
            })
        } else {
            CodeSet::single(cp)
        }
    }

    fn closure(&self, cp: u32, kind: u8, compute: impl FnOnce() -> CodeSet) -> CodeSet {
        self.closures
            .borrow_mut()
            .entry((cp, kind))
            .or_insert_with(compute)
            .clone()
    }

    fn range_set(&self, lo: u32, hi: u32) -> CodeSet {
        if self.flags.has(Flags::I) {
            casefold::range_closure(lo, hi, self.flags.has(Flags::UCASE))
        } else {
            CodeSet::range(lo, hi)
        }
    }

    fn literal(&self, cp: u32) -> Node {
        self.set_node(self.char_set(cp), false)
    }

    /// Hex digits read through `peek` (COMMENTS mode skips between them).
    fn hex_digits(&mut self, n: usize) -> Option<u32> {
        let mut v = 0;
        for _ in 0..n {
            let d = self.peek().and_then(hex_val)?;
            v = v * 16 + d;
            self.pos += 1;
        }
        Some(v)
    }

    /// The escape after a `\` (already consumed).
    fn parse_escape(&mut self, in_class: bool) -> Result<Esc, SyntaxError> {
        let Some(c) = self.raw() else {
            // A trailing `\` escapes the end of the pattern as if it were a
            // U+0000 one past it (black-box): `(\` is an unclosed group at
            // 3, `[a-\` an illegal range at 4, and only at the top level is
            // it reported as such (see `parse`).
            self.pos += 1;
            return Ok(Esc::Cp(0));
        };
        let letter_at = self.pos;
        self.pos += 1;
        let illegal = |p: &Self| {
            p.err_at(
                "Illegal/unsupported escape sequence",
                p.unit(letter_at) as i32,
            )
        };
        let uclass = self.flags.has(Flags::UCLASS);
        let esc = match char::from_u32(c).unwrap_or('\0') {
            '0' => {
                let Some(d1) = self.peek().filter(|&d| (ch('0')..=ch('7')).contains(&d)) else {
                    return Err(self.err("Illegal octal escape sequence"));
                };
                self.pos += 1;
                let mut v = d1 - ch('0');
                if let Some(d2) = self.peek().filter(|&d| (ch('0')..=ch('7')).contains(&d)) {
                    self.pos += 1;
                    v = v * 8 + (d2 - ch('0'));
                    if d1 <= ch('3')
                        && let Some(d3) = self.peek().filter(|&d| (ch('0')..=ch('7')).contains(&d))
                    {
                        self.pos += 1;
                        v = v * 8 + (d3 - ch('0'));
                    }
                }
                Esc::Cp(v)
            }
            '1'..='9' => {
                if in_class {
                    return Err(illegal(self));
                }
                let mut n = (c - ch('0')) as usize;
                while let Some(d) = self.peek().filter(|&d| is_ascii_digit(Some(d))) {
                    let next = n * 10 + (d - ch('0')) as usize;
                    if next > self.groups {
                        break;
                    }
                    n = next;
                    self.pos += 1;
                }
                Esc::Node(self.backref(n))
            }
            'a' => Esc::Cp(0x07),
            'e' => Esc::Cp(0x1B),
            'f' => Esc::Cp(0x0C),
            'n' => Esc::Cp(0x0A),
            'r' => Esc::Cp(0x0D),
            't' => Esc::Cp(0x09),
            'c' => match self.peek() {
                Some(x) => {
                    self.pos += 1;
                    Esc::Cp(x ^ 0x40)
                }
                // At the end, at the `c`; in COMMENTS mode with only
                // whitespace or a comment left, Java reports an internal
                // error at the end (black-box: `(?x)\c ` and `(?x)\c#x`).
                None if self.pos > letter_at + 1 => {
                    return Err(self.err("Unexpected internal error"));
                }
                None => {
                    return Err(self.err_at("Illegal control escape sequence", letter_at as i32));
                }
            },
            'x' => {
                if self.peek() == Some(ch('{')) {
                    self.pos += 1;
                    // Black-box indices: no first digit is reported just
                    // before it (`\x{}` and `\x{G}` at 2), a code point
                    // past U+10FFFF at the digit that makes it so
                    // (`\x{1100000` at 8), a missing `}` where it was
                    // expected.
                    if !self.peek().is_some_and(|d| hex_val(d).is_some()) {
                        return Err(self.err_before("Illegal hexadecimal escape sequence"));
                    }
                    let mut v: u64 = 0;
                    loop {
                        match self.peek() {
                            Some(d) if hex_val(d).is_some() => {
                                v = v * 16 + u64::from(hex_val(d).unwrap_or(0));
                                if v > u64::from(MAX_CP) {
                                    return Err(self.err("Hexadecimal codepoint is too big"));
                                }
                                self.pos += 1;
                            }
                            Some(d) if d == ch('}') => {
                                self.pos += 1;
                                break;
                            }
                            _ => return Err(self.err("Unclosed hexadecimal escape sequence")),
                        }
                    }
                    Esc::Cp(v as u32)
                } else {
                    match self.hex_digits(2) {
                        Some(v) => Esc::Cp(v),
                        None => return Err(self.err("Illegal hexadecimal escape sequence")),
                    }
                }
            }
            'u' => {
                let Some(v) = self.hex_digits(4) else {
                    return Err(self.err("Illegal Unicode escape sequence"));
                };
                // A `\u` high surrogate followed directly by a `\u` low
                // surrogate is one supplementary code point (LIT-7).
                if (0xD800..0xDC00).contains(&v)
                    && self.raw() == Some(ch('\\'))
                    && self.raw_at(self.pos + 1) == Some(ch('u'))
                {
                    let save = self.pos;
                    self.pos += 2;
                    match self.hex_digits(4) {
                        Some(lo) if (0xDC00..0xE000).contains(&lo) => {
                            Esc::Cp(0x10000 + ((v - 0xD800) << 10) + (lo - 0xDC00))
                        }
                        _ => {
                            self.pos = save;
                            Esc::Cp(v)
                        }
                    }
                } else {
                    Esc::Cp(v)
                }
            }
            'N' => {
                if self.peek() != Some(ch('{')) {
                    return Err(self.err("Illegal character name escape sequence"));
                }
                self.pos += 1;
                let start = self.pos;
                // The name is the text up to the first `}` that COMMENTS
                // mode does not skip: a `#` comment can hide it, while the
                // name keeps its spaces and comments (black-box:
                // `(?x)\N{LATIN SMALL LETTER A}` is `a`,
                // `(?x)\N{LATIN SMALL LETTER A#}` is unclosed).
                loop {
                    let before = self.pos;
                    match self.peek() {
                        Some(c) if c == ch('}') => break,
                        Some(_) => self.pos += 1,
                        None => {
                            // Black-box indices: the last character, or
                            // the end when COMMENTS mode skipped the tail
                            // (`\N{abc` at 5, `(?x)\N{a ` and `(?x)\N{a#b`
                            // at the end), never before the name (`\N{`
                            // at 3).
                            let len = self.cps.len();
                            let at = if self.pos > before {
                                len
                            } else {
                                len.saturating_sub(1)
                            };
                            return Err(self.err_at(
                                "Unclosed character name escape sequence",
                                at.max(start) as i32,
                            ));
                        }
                    }
                }
                let name: String = self.cps[start..self.pos]
                    .iter()
                    .filter_map(|&c| char::from_u32(c))
                    .collect();
                self.pos += 1;
                match names::code_point_of(&name) {
                    Some(cp) => Esc::Cp(cp),
                    None => {
                        return Err(self.err_before(format!("Unknown character name [{name}]")));
                    }
                }
            }
            'd' => Esc::Predef(if uclass {
                ucd::nd().clone()
            } else {
                CodeSet::range(0x30, 0x39)
            }),
            'D' => Esc::Predef(
                if uclass {
                    ucd::nd().clone()
                } else {
                    CodeSet::range(0x30, 0x39)
                }
                .negate(),
            ),
            's' => Esc::Predef(if uclass {
                ucd::white_space().clone()
            } else {
                ascii_space()
            }),
            'S' => Esc::Predef(
                if uclass {
                    ucd::white_space().clone()
                } else {
                    ascii_space()
                }
                .negate(),
            ),
            'w' => Esc::Predef(if uclass {
                ucd::unicode_word().clone()
            } else {
                ascii_word()
            }),
            'W' => Esc::Predef(
                if uclass {
                    ucd::unicode_word().clone()
                } else {
                    ascii_word()
                }
                .negate(),
            ),
            'h' => Esc::Predef(horizontal_space()),
            'H' => Esc::Predef(horizontal_space().negate()),
            'v' => Esc::Predef(vertical_space()),
            'V' => Esc::Predef(vertical_space().negate()),
            'p' | 'P' => Esc::Prop(self.parse_property(c == ch('P'))?),
            'b' if !in_class => {
                // `\b{g}`; any other `{` is left for the quantifier parser,
                // which refuses it (spec WB-1).
                let save = self.pos;
                if self.peek() == Some(ch('{')) {
                    self.pos += 1;
                    if self.raw() == Some(ch('g')) {
                        self.pos += 1;
                        if self.peek() == Some(ch('}')) {
                            self.pos += 1;
                            return Ok(Esc::Node(Node::Assert(Assertion::Grapheme)));
                        }
                        // `\b{g` not closed (black-box: `\b{gx}` at 4).
                        return Err(self.err("Illegal/unsupported escape sequence"));
                    }
                }
                self.pos = save;
                Esc::Node(Node::Assert(Assertion::Word {
                    unicode: uclass,
                    negate: false,
                }))
            }
            'B' if !in_class => Esc::Node(Node::Assert(Assertion::Word {
                unicode: uclass,
                negate: true,
            })),
            'A' if !in_class => Esc::Node(Node::Assert(Assertion::StartInput)),
            'G' if !in_class => Esc::Node(Node::Assert(Assertion::LastMatch)),
            'z' if !in_class => Esc::Node(Node::Assert(Assertion::EndInput)),
            'Z' if !in_class => Esc::Node(Node::Assert(Assertion::EndZ {
                unix: self.flags.has(Flags::D),
            })),
            'R' if !in_class => Esc::Node(Node::LineBreak),
            'X' if !in_class => Esc::Node(Node::Grapheme),
            'k' if !in_class => {
                if self.peek() != Some(ch('<')) {
                    return Err(self.err("\\k is not followed by '<' for named capturing group"));
                }
                self.pos += 1;
                let name = self.parse_group_name()?;
                match self.names.get(&name) {
                    Some(&g) => Esc::Node(self.backref(g)),
                    None => {
                        return Err(self
                            .err_before(format!("named capturing group <{name}> does not exist")));
                    }
                }
            }
            _ if c < 0x80 && (c as u8).is_ascii_alphanumeric() => return Err(illegal(self)),
            _ => Esc::Cp(c),
        };
        Ok(esc)
    }

    fn backref(&self, group: usize) -> Node {
        let case = if !self.flags.has(Flags::I) {
            RefCase::Sensitive
        } else if self.flags.has(Flags::UCASE) {
            RefCase::Unicode
        } else {
            RefCase::Ascii
        };
        Node::BackRef { group, case }
    }

    /// `\p…` / `\P…` after the letter (spec PROP-1).
    fn parse_property(&mut self, negate: bool) -> Result<CodeSet, SyntaxError> {
        let name: String = match self.peek() {
            None => return Err(self.err("Unknown character property name {\u{0}}")),
            Some(c) if c == ch('{') => {
                self.pos += 1;
                if self.peek() == Some(ch('}')) {
                    self.pos += 1;
                    return Err(self.err_before("Empty character family"));
                }
                let start = self.pos;
                // Up to the first `}` COMMENTS mode does not skip, as for
                // `\N{…}` (black-box: `(?x)\p{L#}` is unclosed).
                loop {
                    match self.peek() {
                        Some(c) if c == ch('}') => break,
                        Some(_) => self.pos += 1,
                        None => return Err(self.err("Unclosed character family")),
                    }
                }
                let name = self.cps[start..self.pos]
                    .iter()
                    .filter_map(|&c| char::from_u32(c))
                    .collect();
                self.pos += 1;
                name
            }
            Some(c) => {
                self.pos += 1;
                char::from_u32(c).map(String::from).unwrap_or_default()
            }
        };
        let f = PropFlags {
            ci: self.flags.has(Flags::I),
            unicode_class: self.flags.has(Flags::UCLASS),
        };
        // At the `}` or the one-letter name (`\p{Foo}` at 6, `\px` at 2).
        let set = props::resolve(&name, f).map_err(|d| self.err_before(d))?;
        Ok(if negate { set.negate() } else { set })
    }

    // ------------------------------------------------------------------
    // Character classes
    // ------------------------------------------------------------------

    fn parse_class(&mut self) -> Result<CodeSet, SyntaxError> {
        self.parse_class_level(true)
    }

    /// A bracket class (`outer`: at its `[`), or the members after `&&`
    /// up to the closing `]`, which is left in place.
    fn parse_class_level(&mut self, outer: bool) -> Result<CodeSet, SyntaxError> {
        self.enter()?;
        let r = stacker::maybe_grow(64 * 1024, 1024 * 1024, || self.parse_class_body(outer));
        self.depth -= 1;
        r
    }

    /// Spec CLS-1…CLS-11, CI-3, CI-4, refined by black-box probes.
    ///
    /// Members are read one by one. Single characters (those whose case
    /// closure lies below U+0100) collect into one set
    /// (`singles`) that the rest of the class refers to as it is at the end;
    /// every other member (a range, a predefined class, a property, a
    /// nested class) is unioned into `prev` and remembered as `curr`, which
    /// a single character resets. `&&` first folds the singles in, then
    /// reads its right operand: nested classes, and anything else read as a
    /// class body to the closing `]`, all unioned. With no right operand,
    /// `curr` takes its place. This reproduces, e.g.,
    /// `[a&&[b]&c]` = {a, c, &}, `[[a][b]&&]` = {b}, `[[a]b&&]` refused
    /// ("Bad intersection syntax") and `[&&]` refused ("Bad class syntax").
    fn parse_class_body(&mut self, outer: bool) -> Result<CodeSet, SyntaxError> {
        let unclosed = |p: &Self| p.err_before("Unclosed character class");
        let mut negate = false;
        if outer {
            self.pos += 1; // '['
            if self.raw() == Some(ch('^')) {
                negate = true;
                self.pos += 1;
            }
        }
        let mut singles: Vec<(u32, u32)> = Vec::new();
        let mut has_singles = false;
        let mut prev: Option<ClassExpr> = None;
        let mut curr: Option<ClassExpr> = None;
        let mut first = outer;
        let union = |prev: &mut Option<ClassExpr>, e: ClassExpr| {
            *prev = Some(match prev.take() {
                Some(p) => p.union(&e),
                None => e,
            });
        };
        loop {
            let Some(c) = self.peek() else {
                return Err(unclosed(self));
            };
            if c == ch(']') && !first {
                if outer {
                    self.pos += 1;
                }
                break;
            }
            first = false;
            if c == ch('[') {
                let nested = ClassExpr::set(self.parse_class_level(true)?);
                curr = Some(nested.clone());
                union(&mut prev, nested);
                continue;
            }
            if c == ch('&') {
                let save = self.pos;
                self.pos += 1;
                if self.peek() == Some(ch('&')) {
                    self.pos += 1;
                    if has_singles {
                        if prev.is_none() {
                            prev = Some(ClassExpr::singles());
                            curr = Some(ClassExpr::singles());
                        } else {
                            union(&mut prev, ClassExpr::singles());
                        }
                        has_singles = false;
                    }
                    let mut right: Option<ClassExpr> = None;
                    loop {
                        match self.peek() {
                            None => return Err(unclosed(self)),
                            Some(d) if d == ch(']') || d == ch('&') => break,
                            Some(d) if d == ch('[') => {
                                let n = ClassExpr::set(self.parse_class_level(true)?);
                                union(&mut right, n);
                            }
                            Some(_) => {
                                let n = ClassExpr::set(self.parse_class_level(false)?);
                                union(&mut right, n);
                            }
                        }
                    }
                    if right.is_some() {
                        curr = right.clone();
                    }
                    match (prev.take(), right) {
                        (None, None) => {
                            return Err(self.err_at("Bad class syntax", self.unit(save) as i32 + 1));
                        }
                        (None, Some(r)) => prev = Some(r),
                        (Some(p), _) => {
                            let Some(cu) = curr.clone() else {
                                return Err(self.err_before("Bad intersection syntax"));
                            };
                            prev = Some(p.intersect(&cu));
                        }
                    }
                    continue;
                }
                // A single `&` is an ordinary character.
                self.pos = save;
            }
            // `\v` is the vertical-tab character when a `-` follows it.
            let vt_start = c == ch('\\')
                && self.raw_at(self.pos + 1) == Some(ch('v'))
                && self.raw_at(self.pos + 2) == Some(ch('-'));
            let start_cp = if vt_start {
                self.pos += 2;
                0x0B
            } else if c == ch('\\') {
                self.pos += 1;
                match self.parse_escape(true)? {
                    Esc::Cp(cp) => cp,
                    Esc::Predef(set) | Esc::Prop(set) => {
                        let e = ClassExpr::set(set);
                        curr = Some(e.clone());
                        union(&mut prev, e);
                        continue;
                    }
                    Esc::Node(_) => {
                        return Err(self.err("Illegal/unsupported escape sequence"));
                    }
                }
            } else {
                self.pos += 1;
                c
            };
            // A range `x-y`, unless `-` is followed by `[` or `]`.
            if self.peek() == Some(ch('-')) {
                let dash = self.pos;
                self.pos += 1;
                match self.peek() {
                    None => return Err(self.err("Illegal character range")),
                    Some(d) if d == ch(']') || d == ch('[') => {
                        // `[a-]`, `[a-[b]]`: the dash is read next.
                        self.pos = dash;
                    }
                    Some(d) => {
                        let end_cp = if d == ch('\\') {
                            self.pos += 1;
                            if self.raw() == Some(ch('v')) {
                                self.pos += 1;
                                0x0B
                            } else if matches!(self.raw(), Some(c) if c == ch('p') || c == ch('P'))
                            {
                                // Refused before the name is read
                                // (black-box: `[a-\p{Foo}]` at 4).
                                return Err(self.err("Illegal/unsupported escape sequence"));
                            } else {
                                match self.parse_escape(true)? {
                                    Esc::Cp(cp) => cp,
                                    _ => return Err(self.err_before("Illegal character range")),
                                }
                            }
                        } else {
                            self.pos += 1;
                            d
                        };
                        if end_cp < start_cp {
                            return Err(self.err_before("Illegal character range"));
                        }
                        let e = ClassExpr::set(self.range_set(start_cp, end_cp));
                        curr = Some(e.clone());
                        union(&mut prev, e);
                        continue;
                    }
                }
            }
            // Only a character whose case closure lies below U+0100 joins
            // the singles; any other one is a member of its own
            // (black-box: `[😀a&&]`, `[\x{100}a&&]` and `(?iu)[ka&&]` are
            // refused like `[[a]b&&]`, `[a😀&&]` and `[\p{L}😀&&]` compile).
            let set = self.char_set(start_cp);
            if set.ranges().last().is_some_and(|&(_, hi)| hi >= 0x100) {
                let e = ClassExpr::set(set);
                curr = Some(e.clone());
                union(&mut prev, e);
                continue;
            }
            singles.extend_from_slice(set.ranges());
            has_singles = true;
            curr = None;
        }
        if has_singles {
            union(&mut prev, ClassExpr::singles());
        }
        let singles = CodeSet::from_ranges(singles);
        let set = prev.map(|e| e.eval(&singles)).unwrap_or_default();
        Ok(if negate { set.negate() } else { set })
    }
}

/// A class expression over the class's single characters `S`, which are
/// only known once the whole class has been read: `(S ∩ p) ∪ q`. Unions
/// and intersections of such expressions keep that shape, so no tree (and
/// no recursion) is needed however long the class is; unions only append
/// ranges, normalized when an intersection or the result needs them.
#[derive(Clone, Debug)]
struct ClassExpr {
    p: Vec<(u32, u32)>,
    q: Vec<(u32, u32)>,
}

impl ClassExpr {
    fn set(q: CodeSet) -> Self {
        Self {
            p: Vec::new(),
            q: q.ranges().to_vec(),
        }
    }

    fn singles() -> Self {
        Self {
            p: CodeSet::all().ranges().to_vec(),
            q: Vec::new(),
        }
    }

    fn union(mut self, o: &Self) -> Self {
        self.p.extend_from_slice(&o.p);
        self.q.extend_from_slice(&o.q);
        self
    }

    fn intersect(self, o: &Self) -> Self {
        let (p1, q1) = (CodeSet::from_ranges(self.p), CodeSet::from_ranges(self.q));
        let (p2, q2) = (
            CodeSet::from_ranges(o.p.clone()),
            CodeSet::from_ranges(o.q.clone()),
        );
        let p = p1
            .intersect(&p2)
            .union(&p1.intersect(&q2))
            .union(&p2.intersect(&q1));
        Self {
            p: p.ranges().to_vec(),
            q: q1.intersect(&q2).ranges().to_vec(),
        }
    }

    fn eval(self, singles: &CodeSet) -> CodeSet {
        singles
            .intersect(&CodeSet::from_ranges(self.p))
            .union(&CodeSet::from_ranges(self.q))
    }
}

/// `[a-zA-Z_0-9]`.
fn ascii_word() -> CodeSet {
    CodeSet::from_ranges(vec![(0x30, 0x39), (0x41, 0x5A), (0x5F, 0x5F), (0x61, 0x7A)])
}
