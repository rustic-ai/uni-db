// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The parsed form of a Java pattern.
//!
//! Every single-code-point matcher is a [`Node::Set`] whose set already has
//! the case-insensitivity rules of the flags at its position folded in
//! (spec CI-1…CI-6, FLG-5), so later stages never look at `i`/`u` for them.
//! Anchors carry the `m`/`d`/`U` flags read at their position for the same
//! reason.

use std::sync::Arc;

use super::charset::CodeSet;

/// Inline flags (spec FLG-1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Flags(pub(crate) u16);

impl Flags {
    /// CASE_INSENSITIVE.
    pub(crate) const I: u16 = 1;
    /// MULTILINE.
    pub(crate) const M: u16 = 2;
    /// DOTALL.
    pub(crate) const S: u16 = 4;
    /// UNIX_LINES.
    pub(crate) const D: u16 = 8;
    /// COMMENTS.
    pub(crate) const X: u16 = 16;
    /// UNICODE_CHARACTER_CLASS.
    pub(crate) const UCLASS: u16 = 32;
    /// UNICODE_CASE.
    pub(crate) const UCASE: u16 = 64;
    /// CANON_EQ, as the inline `c` enables it (spec FLG-6 and the
    /// black-box findings in the module docs).
    pub(crate) const C: u16 = 128;

    pub(crate) fn has(self, f: u16) -> bool {
        self.0 & f != 0
    }
}

/// How a quantifier backtracks (spec QNT-2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RepMode {
    Greedy,
    Lazy,
    Possessive,
}

/// An unbounded maximum.
pub(crate) const INF: u32 = u32::MAX;

/// Zero-width assertions (spec ANC-*, WB-*).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Assertion {
    /// `^`.
    Caret { multiline: bool, unix: bool },
    /// `$`.
    Dollar { multiline: bool, unix: bool },
    /// `\A`.
    StartInput,
    /// `\z`.
    EndInput,
    /// `\Z`.
    EndZ { unix: bool },
    /// `\G`.
    LastMatch,
    /// `\b` (`negate` for `\B`).
    Word { unicode: bool, negate: bool },
    /// `\b{g}`.
    Grapheme,
}

/// How a back-reference compares (spec BR-5, BR-6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefCase {
    Sensitive,
    Ascii,
    Unicode,
}

/// A single-code-point matcher.
#[derive(Clone, Debug)]
pub(crate) struct SetNode {
    pub(crate) set: Arc<CodeSet>,
    /// A bracket class or property under `(?c)`: matched against composed
    /// prefixes of the grapheme cluster at the position (see `canon.rs`).
    pub(crate) canon: bool,
}

/// A pattern node.
#[derive(Clone, Debug)]
pub(crate) enum Node {
    Empty,
    Set(SetNode),
    Seq(Vec<Node>),
    Alt(Vec<Node>),
    /// A group: capturing when `cap` is set.
    Group {
        cap: Option<usize>,
        body: Box<Node>,
    },
    Atomic(Box<Node>),
    Look {
        behind: bool,
        negate: bool,
        body: Box<Node>,
        /// Code-point index of the `(` in the pattern.
        at: usize,
    },
    Repeat {
        body: Box<Node>,
        min: u32,
        max: u32,
        mode: RepMode,
    },
    Assert(Assertion),
    BackRef {
        group: usize,
        case: RefCase,
    },
    /// `\R`.
    LineBreak,
    /// `\X`.
    Grapheme,
}

/// A parsed pattern.
#[derive(Debug)]
pub(crate) struct Parsed {
    pub(crate) root: Node,
    /// Capturing groups, numbered from 1.
    pub(crate) groups: usize,
    /// Code-point index of the last literal supplementary character in the
    /// pattern string (a UTF-16 surrogate pair), if any (spec LB-2's K).
    pub(crate) last_supplementary: Option<usize>,
}
