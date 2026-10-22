// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The linear-time route: patterns whose Java answer is plain language
//! membership run on `regex-automata`.
//!
//! For `matches()` Java's backtracking explores every way to match, so the
//! answer is "does some path consume the whole input" — the regular
//! language's membership test — unless a construct prunes paths. The route
//! is taken only when nothing in the pattern does:
//!
//! * no look-around, atomic group, possessive quantifier, back-reference,
//!   `\X`, `\b`/`\B`/`\b{g}`, `(?c)` class, and no anchor other than a
//!   leading `^`/`\A`/`\G` and a trailing `$`/`\z`/`\Z` without MULTILINE
//!   (those hold trivially for a whole-input match);
//! * no `\R` under a quantifier (a quantified `\R` never gives back the
//!   `\n` of `\r\n`, spec LBRK-1).
//!
//! Under those conditions the remaining Java rules cannot change the answer:
//! a loop iteration that matches empty ends the loop (QNT-8), but an empty
//! iteration never changes the position; iterations of a deterministic loop
//! are atomic, but such a body (one code-point matcher after another) has
//! only one way to match. Every single-code-point matcher is already a code
//! point set with Java's case rules applied, so the automaton gets exact
//! classes and no case-insensitivity of its own. Input from a Rust string
//! has no unpaired surrogates, so surrogate code points are dropped from the
//! classes, and no position inside a pair is reachable without
//! look-behind or back-references.

use regex_automata::meta::Regex;
use regex_automata::{Anchored, Input};
use regex_syntax::hir::{Class, ClassUnicode, ClassUnicodeRange, Hir, Look, Repetition};

use super::ast::{Assertion, INF, Node};
use super::charset::CodeSet;

/// A compiled linear-time matcher.
#[derive(Debug)]
pub(crate) struct Linear {
    re: Regex,
}

impl Linear {
    /// Whether all of `text` matches.
    pub(crate) fn is_match(&self, text: &str) -> bool {
        self.re.is_match(Input::new(text).anchored(Anchored::Yes))
    }
}

/// The linear matcher for `root`, when the pattern qualifies.
pub(crate) fn build(root: &Node) -> Option<Linear> {
    let mut items: Vec<&Node> = match root {
        Node::Seq(v) => v.iter().collect(),
        n => vec![n],
    };
    while let Some(Node::Assert(
        Assertion::Caret {
            multiline: false, ..
        }
        | Assertion::StartInput
        | Assertion::LastMatch,
    )) = items.first()
    {
        items.remove(0);
    }
    while let Some(Node::Assert(
        Assertion::Dollar {
            multiline: false, ..
        }
        | Assertion::EndInput
        | Assertion::EndZ { .. },
    )) = items.last()
    {
        items.pop();
    }
    let mut subs = Vec::with_capacity(items.len() + 1);
    for n in items {
        subs.push(translate(n, false)?);
    }
    subs.push(Hir::look(Look::End));
    let hir = Hir::concat(subs);
    let config = Regex::config()
        .nfa_size_limit(Some(4 << 20))
        .hybrid_cache_capacity(1 << 20)
        .dfa_size_limit(Some(1 << 20));
    let re = Regex::builder()
        .configure(config)
        .build_from_hir(&hir)
        .ok()?;
    Some(Linear { re })
}

fn class(set: &CodeSet) -> Hir {
    let surrogates = CodeSet::range(0xD800, 0xDFFF);
    let ranges = set
        .difference(&surrogates)
        .ranges()
        .iter()
        .filter_map(|&(a, b)| {
            Some(ClassUnicodeRange::new(
                char::from_u32(a)?,
                char::from_u32(b)?,
            ))
        })
        .collect::<Vec<_>>();
    Hir::class(Class::Unicode(ClassUnicode::new(ranges)))
}

fn translate(n: &Node, in_loop: bool) -> Option<Hir> {
    stacker::maybe_grow(64 * 1024, 1024 * 1024, || {
        Some(match n {
            Node::Empty => Hir::empty(),
            Node::Set(s) if !s.canon => class(&s.set),
            Node::Seq(v) => Hir::concat(
                v.iter()
                    .map(|x| translate(x, in_loop))
                    .collect::<Option<Vec<_>>>()?,
            ),
            Node::Alt(v) => Hir::alternation(
                v.iter()
                    .map(|x| translate(x, in_loop))
                    .collect::<Option<Vec<_>>>()?,
            ),
            Node::Group { body, .. } => translate(body, in_loop)?,
            Node::Repeat {
                body,
                min,
                max,
                mode,
            } => {
                if *mode == super::ast::RepMode::Possessive {
                    return None;
                }
                let sub = translate(body, true)?;
                Hir::repetition(Repetition {
                    min: *min,
                    max: (*max != INF).then_some(*max),
                    greedy: true,
                    sub: Box::new(sub),
                })
            }
            Node::LineBreak if !in_loop => Hir::alternation(vec![
                Hir::literal(*b"\r\n"),
                class(&CodeSet::from_ranges(vec![
                    (0x0A, 0x0D),
                    (0x85, 0x85),
                    (0x2028, 0x2029),
                ])),
            ]),
            _ => return None,
        })
    })
}
