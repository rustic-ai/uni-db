// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Structural facts about a parsed pattern: which repetition strategy a
//! quantifier gets, look-behind windows, and Java's compile-time check that
//! a look-behind has an "obvious maximum length".

use super::ast::{INF, Node, RepMode};

/// How a quantifier iterates (see `backtrack.rs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoopKind {
    /// `X?` / `X{0,1}` on a group: an alternation between the body and
    /// nothing; the body is not atomic.
    Optional,
    /// A loop whose iterations are each matched atomically (the first way
    /// the body matches is the only one tried), with iterations given back
    /// one at a time. Java uses it for a quantified single atom and for a
    /// group whose body is deterministic.
    Deterministic,
    /// A loop whose iterations continue into the rest of the pattern, so
    /// every way the body can match is explored.
    General,
}

/// Whether a node can match in only one way at a position, as Java judges
/// it when choosing a loop strategy: no alternation and no variable-count
/// quantifier outside look-arounds, no `(?c)` class (each of which Java
/// treats as an alternation) and no `\X`. Back-references, `\R` and
/// anchors count as deterministic.
///
/// Evidence (black-box, `PROVENANCE.md`): `(?:\R)+\n` fails on `\r\n`
/// while `(?:\R|x)+\n` and `(?:\Rx?)+\n` match; `(?:(?=a|b)\R)+\n` fails;
/// `(?:(?>a|)\R)+\n` matches; `(?:x{1}\R)+\n` fails.
pub(crate) fn deterministic(node: &Node) -> bool {
    stacker::maybe_grow(64 * 1024, 1024 * 1024, || match node {
        Node::Set(s) => !s.canon,
        Node::Empty
        | Node::Assert(_)
        | Node::BackRef { .. }
        | Node::LineBreak
        | Node::Look { .. } => true,
        Node::Grapheme => false,
        Node::Seq(v) => v.iter().all(deterministic),
        Node::Alt(_) => false,
        Node::Group { body, .. } | Node::Atomic(body) => deterministic(body),
        Node::Repeat { body, min, max, .. } => min == max && deterministic(body),
    })
}

/// Whether a quantified node is a group (as opposed to a single atom; a
/// look-around counts as an atom).
fn is_group(node: &Node) -> bool {
    matches!(node, Node::Group { .. } | Node::Atomic(_))
}

/// The strategy for `body{min,max}`; possessive quantifiers always match
/// each iteration atomically (`((b*)b){2}+a` fails on `bba`).
pub(crate) fn loop_kind(body: &Node, min: u32, max: u32, possessive: bool) -> LoopKind {
    if possessive {
        LoopKind::Deterministic
    } else if is_group(body) {
        if min == 0 && max == 1 {
            LoopKind::Optional
        } else if deterministic(body) {
            LoopKind::Deterministic
        } else {
            LoopKind::General
        }
    } else {
        LoopKind::Deterministic
    }
}

/// Java's length estimate for a look-behind body, in 32-bit arithmetic as
/// Java does it: the minimum and maximum lengths (each single-code-point
/// matcher counts 1, `\R` 1 to 2, zero-width items 0), the running value
/// Java checks for overflow, and whether the maximum is still valid.
///
/// Observed (black-box, `PROVENANCE.md`; spec LB-1 left this partially
/// characterized). The body is a chain of items, plain and capturing groups
/// being transparent:
/// * single characters and `x?` add without a check (`a{2147483647}b`
///   compiles, its maximum wrapping negative);
/// * a counted quantifier adds `body max × count` (wrapping) and is refused
///   when adding it makes the running value *decrease*
///   (`a{2147483647}b{1}` and `(?:ab){1073741824}` are refused, `a*b*c{1}`
///   compiles);
/// * an unbounded quantifier counts `Integer.MAX_VALUE` as its count; a
///   greedy one on a single character adds without a check (`a*b*`
///   compiles), on a group or when lazy or possessive it is checked like a
///   counted one (`a*b*?` is refused; `(?:abc)+` compiles since `3 × MAX`
///   wraps positive, `(?:ab)+` does not);
/// * a non-possessive loop other than `?` over a plain or capturing group
///   whose body can match in more than one way (as [`deterministic`]
///   judges it, atomic groups inside included) is refused, even with a
///   count of zero; a quantified atomic group `(?>…){n}` is not;
/// * an alternation or an optional group `(…)?` is a branch: its width is
///   the largest of its alternatives' and −1 (`(?<=ab*|cb*)`, whose
///   alternatives both wrap to `Integer.MIN_VALUE`, has an empty window;
///   `(?:x*abc|y*d)++` is refused), the rest of its chain is checked from
///   zero, and the branch and the rest are added without a check
///   (`(?:x|y)z{2147483647}` and `\H+(?:b)?\W{2}` compile,
///   `(?:b)?\H+\W{2}` does not);
/// * the body of `(?>…)`, `(?>…)?` and `(…)?+` continues the running value
///   but ends its own chain (`a+(?:bc{1})?+` compiles, `a+(?:b{1}c)?+` does
///   not; `(?>(?:x|y)z{2147483647})` compiles, `(?>(?:x|y))z{2147483647}`
///   does not).
///
/// The wrapped totals are what Java's look-behind uses for its window
/// (spec LB-2, LB-3).
#[derive(Clone, Copy)]
struct Est {
    min: i32,
    max: i32,
    chk: i32,
    valid: bool,
}

impl Est {
    fn new() -> Self {
        Self {
            min: 0,
            max: 0,
            chk: 0,
            valid: true,
        }
    }

    /// Adds `n` to the maximum without a check.
    fn add(&mut self, n: i32) {
        self.max = self.max.wrapping_add(n);
        self.chk = self.chk.wrapping_add(n);
    }

    /// Adds `n` to the maximum, refusing when the running value decreases.
    fn add_checked(&mut self, n: i32) {
        let c = self.chk.wrapping_add(n);
        if c < self.chk {
            self.valid = false;
        } else {
            self.chk = c;
            self.max = self.max.wrapping_add(n);
        }
    }
}

/// The items of a chain: sequences and plain or capturing groups flattened.
fn chain<'a>(node: &'a Node, out: &mut Vec<&'a Node>) {
    match node {
        Node::Seq(v) => v.iter().for_each(|n| chain(n, out)),
        Node::Group { body, .. } => chain(body, out),
        n => out.push(n),
    }
}

/// A fresh estimate of one node as its own chain.
fn study_fresh(node: &Node) -> Est {
    let mut items = Vec::new();
    chain(node, &mut items);
    let mut e = Est::new();
    study_chain(&items, &mut e);
    e
}

/// `(lo, hi, valid)` when the item is a branch: an alternation, or `(…)?`
/// on a plain group.
fn branch(node: &Node) -> Option<(i32, i32, bool)> {
    match node {
        Node::Alt(v) => {
            let mut lo = i32::MAX;
            let mut hi = -1;
            let mut valid = true;
            for b in v {
                let sub = study_fresh(b);
                valid &= sub.valid;
                lo = lo.min(sub.min);
                hi = hi.max(sub.max);
            }
            Some((lo, hi, valid))
        }
        Node::Repeat {
            body,
            min: 0,
            max: 1,
            mode,
        } if matches!(**body, Node::Group { .. }) && *mode != RepMode::Possessive => {
            let sub = study_fresh(body);
            Some((0, sub.max.max(0), sub.valid))
        }
        _ => None,
    }
}

fn study_chain(items: &[&Node], e: &mut Est) {
    stacker::maybe_grow(64 * 1024, 1024 * 1024, || {
        for (k, n) in items.iter().enumerate() {
            if let Some((lo, hi, valid)) = branch(n) {
                let mut rest = Est::new();
                study_chain(&items[k + 1..], &mut rest);
                e.valid &= valid && rest.valid;
                e.min = e.min.wrapping_add(lo).wrapping_add(rest.min);
                e.max = e.max.wrapping_add(hi).wrapping_add(rest.max);
                e.chk = e.chk.wrapping_add(hi).wrapping_add(rest.chk);
                return;
            }
            study_item(n, e);
        }
    });
}

fn study_item(node: &Node, e: &mut Est) {
    match node {
        // A `(?c)` class counts like `\X`: at least one, at most zero
        // (`.*(?<=(?c)[e])` never matches, `e(?<=(?c)[e]|x)` does).
        Node::Set(s) if s.canon => e.min = e.min.wrapping_add(1),
        Node::Set(_) => {
            e.min = e.min.wrapping_add(1);
            e.add(1);
        }
        Node::LineBreak => {
            e.min = e.min.wrapping_add(1);
            e.add(2);
        }
        // `\X` counts as at least one and at most zero units, so a
        // look-behind never runs a body that needs one (spec LB-5).
        Node::Grapheme => e.min = e.min.wrapping_add(1),
        Node::Empty | Node::Assert(_) | Node::Look { .. } => {}
        Node::BackRef { .. } => e.valid = false,
        Node::Seq(_) | Node::Group { .. } | Node::Alt(_) => {
            let mut items = Vec::new();
            chain(node, &mut items);
            study_chain(&items, e);
        }
        Node::Atomic(body) => {
            // In line, in a chain of its own.
            let mut items = Vec::new();
            chain(body, &mut items);
            study_chain(&items, e);
        }
        Node::Repeat {
            body,
            min,
            max,
            mode,
        } => {
            if matches!(**body, Node::Empty) {
                return;
            }
            let group = is_group(body);
            let possessive = *mode == RepMode::Possessive;
            if group && (possessive || matches!(**body, Node::Atomic(_))) && *min == 0 && *max == 1
            {
                // `(…)?+`, `(?>…)?`: the body continues the running value
                // in a chain of its own.
                let inner = match &**body {
                    Node::Atomic(b) => &**b,
                    b => b,
                };
                let mut items = Vec::new();
                chain(inner, &mut items);
                let min_before = e.min;
                study_chain(&items, e);
                e.min = min_before;
                return;
            }
            let sub = study_fresh(body);
            if !sub.valid {
                e.valid = false;
                return;
            }
            if *min == 0 && *max == 1 {
                e.add(sub.max);
                return;
            }
            let cmin = i32::try_from(*min).unwrap_or(i32::MAX);
            e.min = e.min.wrapping_add(sub.min.wrapping_mul(cmin));
            // A plain or capturing group repeated by a greedy or lazy
            // counted quantifier needs a body that matches one way, by the
            // same test as the loop strategy (an atomic group inside is
            // judged by its body): `(?<=(?:(?>a?)){0})`, `(?<=((?>a?)){2})`
            // and `(?<=(?:a?+){2})` are refused, while `(?<=(?>a?){2})`,
            // `(?<=(?:\R){2})` and `(?<=(?:(?=a|b)c){2})` compile.
            if matches!(**body, Node::Group { .. }) && !possessive && !deterministic(body) {
                e.valid = false;
                return;
            }
            let cmax = if *max == INF {
                i32::MAX
            } else {
                i32::try_from(*max).unwrap_or(i32::MAX)
            };
            let total = sub.max.wrapping_mul(cmax);
            if *max == INF && !group && *mode == RepMode::Greedy && sub.max <= 1 {
                e.add(total);
            } else {
                e.add_checked(total);
            }
        }
    }
}

/// Java's `(min, max)` look-behind lengths, or `None` when Java refuses
/// the body ("does not have an obvious maximum length", spec LB-1). The
/// lengths are in UTF-16 units, or in code points when the pattern holds a
/// supplementary character at or after the look-behind (see `backtrack`).
pub(crate) fn lookbehind_window(body: &Node) -> Option<(i32, i32)> {
    let e = study_fresh(body);
    e.valid.then_some((e.min, e.max))
}
