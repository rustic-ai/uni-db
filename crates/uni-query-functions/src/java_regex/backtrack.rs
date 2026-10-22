// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The backtracking matcher: a program of instructions run over the input's
//! UTF-16 units with an explicit backtrack stack (no recursion), bounded by
//! a step budget and a stack budget.
//!
//! # Semantics it implements
//!
//! * Positions are UTF-16 unit indices; every single-code-point matcher
//!   reads the code point at the position, pairing surrogates forwards only
//!   (SUR-1), so a look-behind start or a back-reference can leave the
//!   position inside a pair (LB-4, BR-5, SUR-3).
//! * Captures (CAP-1…CAP-4): a group records `[start, end]` when it closes,
//!   so a back-reference inside the group sees the previous iteration's
//!   value (BR-4). Every capture change pushes an undo record, so
//!   backtracking restores captures (CAP-2). A look-around, an atomic group
//!   or a possessive quantifier whose body succeeds *commits*: the records
//!   made inside are dropped together with the choice points, so later
//!   backtracking does not undo those captures (CAP-3).
//! * Three loop strategies (see [`LoopKind`]; black-box findings beyond the
//!   spec are in `PROVENANCE.md`):
//!   - *general* (a group whose body can match in more than one way): each
//!     iteration continues into the rest of the pattern; an iteration that
//!     consumes nothing ends the loop (QNT-8);
//!   - *deterministic* (a quantified single atom, a group whose body has one
//!     way to match, any possessive quantifier): each iteration is matched
//!     atomically — its choice points are dropped but its capture records
//!     kept, so giving the iteration back restores captures. An empty
//!     iteration once the minimum is reached ends the loop without being
//!     counted, and the capture of the group the loop repeats is not
//!     recorded (CAP-5; groups inside it keep theirs); below the minimum
//!     it counts. This is what makes `\R{1,2}\n` fail on `\r\n` (LBRK-1);
//!     a possessive loop records the empty iteration and never gives an
//!     iteration back;
//!   - *optional*: `(X)?` on a group is an alternation between `X` and
//!     nothing.
//! * Look-behind (LB-2, LB-3): start positions from `i - min` down to
//!   `i - max` (or 0), nearest first, in Java's 32-bit arithmetic over the
//!   lengths `analyze::lookbehind_window` computes; the body must end
//!   exactly at `i`. When the pattern holds a supplementary character at or
//!   after the look-behind, the lengths count code points: starts step back
//!   one code point at a time from `min` code points before `i` (or the
//!   input start, when there are fewer) to `max` code points before it
//!   (none when `max` is negative), so they never fall inside a surrogate
//!   pair (black-box probes refining LB-2's K).
//! * `\b{g}` (WB-4) as JDK 21 behaves for `matches()`, established by
//!   black-box probes: true at the start and end of the input, false inside
//!   a surrogate pair; elsewhere true iff the position is at or past the end
//!   of the grapheme cluster that starts at `last`. `last` is 0 initially
//!   and moves to the end of the body of every look-ahead (positive or
//!   negative), atomic group and possessive-loop iteration whose body
//!   matches, and to the end of every matched iteration of a deterministic
//!   loop other than a greedy `x*`/`x+` on one code-point matcher. When
//!   `last` is the input length Java throws
//!   `StringIndexOutOfBoundsException`, reported as
//!   [`ExecError::JavaException`].
//!
//! # Limits
//!
//! * `step_limit`: every resumption at an alternative (a choice point,
//!   a shorter `(?c)` prefix, a given-back loop iteration, the next
//!   look-behind start) is one step, and so is every [`WORK_PER_STEP`]
//!   units of forward work (instructions run, and code units a
//!   back-reference compares or `\X`, `\b`, `\b{g}`, `(?c)` and a
//!   look-behind window scan). Past the limit the match fails with
//!   [`ExecError::StepLimit`]. Charging forward work bounds the time of a
//!   refused match: a resumption followed by a scan of the rest of the
//!   input (`(?:a|a)*(?=b*+c)`) would otherwise cost O(n) per step.
//! * [`MAX_FRAMES`]: records on the backtrack stack (pending choice
//!   points, capture and loop undo records), bounding memory at about
//!   64 MB; past it the match fails with [`ExecError::RecursionLimit`].
//!   Java overflows its thread stack far earlier on some shapes (RES-1,
//!   `(?:a|b)*` past about 1 500 characters); here they are answered up to
//!   the budget (RES-2), e.g. `(?>a|b)*` on a million characters.

use std::sync::Arc;

use super::analyze::{LoopKind, lookbehind_window, loop_kind};
use super::ast::{Assertion, INF, Node, Parsed, RefCase, RepMode};
use super::canon;
use super::casefold;
use super::charset::CodeSet;
use super::grapheme::{cluster_end, cp_at};
use super::parse::SyntaxError;
use super::ucd;

/// Backtrack-stack records allowed at once (16 bytes each).
pub(crate) const MAX_FRAMES: usize = 4_000_000;

/// The size of one backtrack-stack record.
#[cfg(test)]
pub(crate) const FRAME_BYTES: usize = std::mem::size_of::<Frame>();

/// The most records the backtrack stack has held (for measurements).
#[cfg(test)]
pub(crate) static PEAK_FRAMES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Units of forward work charged as one step (see the module docs).
pub(crate) const WORK_PER_STEP: usize = 4;

const NONE: u32 = u32::MAX;

#[derive(Clone, Debug)]
enum Inst {
    /// One BMP code point that is not a surrogate.
    Char(u16),
    Set(u32),
    Canon(u32),
    /// Try the next instruction, then the target.
    Split(u32),
    Jmp(u32),
    CapOpen(u32),
    CapClose(u32),
    Assert(Assertion),
    BackRef(u32, RefCase),
    LineBreak,
    Grapheme,
    LookStart(u32),
    LookEnd(u32),
    AtomicStart(u32),
    /// Ends an atomic group; `true` when it moves `last` (an atomic group
    /// does, the commit after a possessive quantifier does not).
    AtomicEnd(u32, bool),
    GInit(u32),
    GTest(u32),
    GEnd(u32),
    DInit(u32),
    DTry(u32),
    DEnd(u32),
    Match,
}

#[derive(Clone, Copy, Debug)]
struct LookInfo {
    behind: bool,
    negate: bool,
    /// Java's 32-bit window lengths (spec LB-2), possibly wrapped: UTF-16
    /// units, or code points when `wide`.
    min_w: i32,
    max_w: i32,
    /// The pattern holds a supplementary character at or after the
    /// look-behind: start positions are counted in code points.
    wide: bool,
    body: u32,
    end: u32,
}

#[derive(Clone, Copy, Debug)]
struct LoopInfo {
    min: u32,
    max: u32,
    greedy: bool,
    possessive: bool,
    /// Whether each matched iteration moves `last` (see the module docs).
    sets_last: bool,
    /// The capturing group the loop repeats directly (`(X)*`), whose
    /// capture an empty iteration does not record (CAP-5).
    own_cap: Option<u32>,
    /// The test (`GTest`/`DTry`) instruction.
    test: u32,
    body: u32,
    exit: u32,
}

/// A compiled pattern for the backtracking matcher.
#[derive(Debug)]
pub(crate) struct Program {
    insts: Vec<Inst>,
    sets: Vec<Arc<CodeSet>>,
    looks: Vec<LookInfo>,
    loops: Vec<LoopInfo>,
    n_slots: usize,
    groups: usize,
}

impl Program {
    /// The number of capturing groups.
    #[cfg(test)]
    pub(crate) fn group_count(&self) -> usize {
        self.groups
    }
}

/// Why a match stopped without an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExecError {
    StepLimit,
    RecursionLimit,
    /// A Java runtime exception Java's matcher throws on this input.
    JavaException(String),
}

struct Compiler {
    insts: Vec<Inst>,
    sets: Vec<Arc<CodeSet>>,
    set_ids: std::collections::HashMap<CodeSet, u32>,
    looks: Vec<LookInfo>,
    loops: Vec<LoopInfo>,
    n_slots: usize,
    last_supplementary: Option<usize>,
}

/// Compiles a parsed pattern; refuses look-behinds Java refuses (LB-1).
pub(crate) fn compile(parsed: &Parsed) -> Result<Program, SyntaxError> {
    let mut c = Compiler {
        insts: Vec::new(),
        sets: Vec::new(),
        set_ids: std::collections::HashMap::new(),
        looks: Vec::new(),
        loops: Vec::new(),
        n_slots: 0,
        last_supplementary: parsed.last_supplementary,
    };
    c.node(&parsed.root)?;
    c.insts.push(Inst::Match);
    Ok(Program {
        insts: c.insts,
        sets: c.sets,
        looks: c.looks,
        loops: c.loops,
        n_slots: c.n_slots,
        groups: parsed.groups,
    })
}

impl Compiler {
    fn pc(&self) -> u32 {
        self.insts.len() as u32
    }

    fn set_index(&mut self, set: &Arc<CodeSet>) -> u32 {
        if let Some(&i) = self.set_ids.get(&**set) {
            return i;
        }
        let i = self.sets.len() as u32;
        self.sets.push(Arc::clone(set));
        self.set_ids.insert((**set).clone(), i);
        i
    }

    fn node(&mut self, n: &Node) -> Result<(), SyntaxError> {
        stacker::maybe_grow(64 * 1024, 1024 * 1024, || self.node_inner(n))
    }

    fn node_inner(&mut self, n: &Node) -> Result<(), SyntaxError> {
        match n {
            Node::Empty => {}
            Node::Set(s) => {
                if s.canon {
                    let i = self.set_index(&s.set);
                    self.insts.push(Inst::Canon(i));
                } else if let Some(cp) = s
                    .set
                    .as_single()
                    .filter(|&c| c < 0x10000 && !(0xD800..0xE000).contains(&c))
                {
                    self.insts.push(Inst::Char(cp as u16));
                } else {
                    let i = self.set_index(&s.set);
                    self.insts.push(Inst::Set(i));
                }
            }
            Node::Seq(v) => {
                for x in v {
                    self.node(x)?;
                }
            }
            Node::Alt(v) => {
                let mut jumps = Vec::new();
                for (i, x) in v.iter().enumerate() {
                    if i + 1 < v.len() {
                        let split = self.insts.len();
                        self.insts.push(Inst::Split(0));
                        self.node(x)?;
                        jumps.push(self.insts.len());
                        self.insts.push(Inst::Jmp(0));
                        let next = self.pc();
                        self.insts[split] = Inst::Split(next);
                    } else {
                        self.node(x)?;
                    }
                }
                let end = self.pc();
                for j in jumps {
                    self.insts[j] = Inst::Jmp(end);
                }
            }
            Node::Group { cap, body } => match cap {
                Some(g) => {
                    self.insts.push(Inst::CapOpen(*g as u32));
                    self.node(body)?;
                    self.insts.push(Inst::CapClose(*g as u32));
                }
                None => self.node(body)?,
            },
            Node::Atomic(body) => {
                let slot = self.new_slot();
                self.insts.push(Inst::AtomicStart(slot));
                self.node(body)?;
                self.insts.push(Inst::AtomicEnd(slot, true));
            }
            Node::Look {
                behind,
                negate,
                body,
                at,
            } => {
                let wide = *behind && self.last_supplementary.is_some_and(|p| p >= *at);
                let (min_w, max_w) = if *behind {
                    lookbehind_window(body).ok_or_else(|| SyntaxError {
                        description: "Look-behind group does not have an obvious maximum length"
                            .to_string(),
                        index: -1,
                    })?
                } else {
                    (0, 0)
                };
                let li = self.looks.len();
                self.looks.push(LookInfo {
                    behind: *behind,
                    negate: *negate,
                    min_w,
                    max_w,
                    wide,
                    body: 0,
                    end: 0,
                });
                self.insts.push(Inst::LookStart(li as u32));
                self.looks[li].body = self.pc();
                self.node(body)?;
                self.insts.push(Inst::LookEnd(li as u32));
                self.looks[li].end = self.pc();
            }
            Node::Assert(a) => self.insts.push(Inst::Assert(*a)),
            Node::BackRef { group, case } => self.insts.push(Inst::BackRef(*group as u32, *case)),
            Node::LineBreak => self.insts.push(Inst::LineBreak),
            Node::Grapheme => self.insts.push(Inst::Grapheme),
            Node::Repeat {
                body,
                min,
                max,
                mode,
            } => self.repeat(body, *min, *max, *mode)?,
        }
        Ok(())
    }

    fn new_slot(&mut self) -> u32 {
        self.n_slots += 1;
        (self.n_slots - 1) as u32
    }

    fn repeat(
        &mut self,
        body: &Node,
        min: u32,
        max: u32,
        mode: RepMode,
    ) -> Result<(), SyntaxError> {
        if matches!(body, Node::Empty) || max == 0 {
            // Nothing to repeat, or zero times; look-behinds inside are
            // still checked as Java checks them.
            if max == 0 {
                self.check_only(body)?;
            }
            return Ok(());
        }
        let possessive = mode == RepMode::Possessive;
        let slot = if possessive {
            let s = self.new_slot();
            self.insts.push(Inst::AtomicStart(s));
            Some(s)
        } else {
            None
        };
        let greedy = mode != RepMode::Lazy;
        match loop_kind(body, min, max, possessive) {
            LoopKind::Optional => {
                if greedy {
                    let split = self.insts.len();
                    self.insts.push(Inst::Split(0));
                    self.node(body)?;
                    let end = self.pc();
                    self.insts[split] = Inst::Split(end);
                } else {
                    let split = self.insts.len();
                    self.insts.push(Inst::Split(0));
                    let jmp = self.insts.len();
                    self.insts.push(Inst::Jmp(0));
                    let body_pc = self.pc();
                    self.node(body)?;
                    let end = self.pc();
                    self.insts[split] = Inst::Split(body_pc);
                    self.insts[jmp] = Inst::Jmp(end);
                }
            }
            kind => {
                let l = self.loops.len() as u32;
                let det = kind == LoopKind::Deterministic;
                // Java's greedy `x*` / `x+` on one code-point matcher does not
                // move `last`; every other deterministic loop does, after each
                // matched iteration (black-box probes, `PROVENANCE.md`).
                let plain_char_loop = matches!(body, Node::Set(s) if !s.canon)
                    && mode == RepMode::Greedy
                    && max == INF
                    && min <= 1;
                self.loops.push(LoopInfo {
                    min,
                    max,
                    greedy,
                    possessive,
                    sets_last: det && !plain_char_loop,
                    own_cap: match body {
                        Node::Group { cap: Some(g), .. } => Some(*g as u32),
                        _ => None,
                    },
                    test: 0,
                    body: 0,
                    exit: 0,
                });
                self.insts
                    .push(if det { Inst::DInit(l) } else { Inst::GInit(l) });
                let test = self.pc();
                self.insts
                    .push(if det { Inst::DTry(l) } else { Inst::GTest(l) });
                let body_pc = self.pc();
                self.node(body)?;
                self.insts
                    .push(if det { Inst::DEnd(l) } else { Inst::GEnd(l) });
                let exit = self.pc();
                let info = &mut self.loops[l as usize];
                info.test = test;
                info.body = body_pc;
                info.exit = exit;
            }
        }
        if let Some(s) = slot {
            self.insts.push(Inst::AtomicEnd(s, false));
        }
        Ok(())
    }

    /// Checks look-behinds in a node that is not compiled.
    fn check_only(&mut self, n: &Node) -> Result<(), SyntaxError> {
        let saved = (
            self.insts.len(),
            self.looks.len(),
            self.loops.len(),
            self.n_slots,
        );
        self.node(n)?;
        self.insts.truncate(saved.0);
        self.looks.truncate(saved.1);
        self.loops.truncate(saved.2);
        self.n_slots = saved.3;
        Ok(())
    }
}

/// One backtrack-stack record.
#[derive(Clone, Copy, Debug)]
enum Frame {
    // Undo records.
    Cap {
        g: u32,
        s: i32,
        e: i32,
    },
    Tmp {
        g: u32,
        v: i32,
    },
    Loop {
        l: u32,
        count: u32,
        start: u32,
    },
    // Choice points and barriers.
    Choice {
        pc: u32,
        pos: u32,
    },
    GLazy {
        l: u32,
        pos: u32,
    },
    Look {
        li: u32,
        i: u32,
        next: u32,
    },
    Atomic,
    DSoft {
        l: u32,
        pos: u32,
        count: u32,
    },
    DLazy {
        l: u32,
        pos: u32,
        count: u32,
    },
    Canon {
        pc: u32,
        pos: u32,
        idx: u32,
    },
    LineAlt {
        pc: u32,
        pos: u32,
    },
    /// A deterministic-loop iteration to give back (not counted towards
    /// the choice budget).
    DGive {
        l: u32,
        pos: u32,
        count: u32,
    },
}

impl Frame {
    fn is_undo(self) -> bool {
        matches!(
            self,
            Frame::Cap { .. } | Frame::Tmp { .. } | Frame::Loop { .. }
        )
    }
}

struct Vm<'a> {
    prog: &'a Program,
    text: &'a [u16],
    pos: usize,
    caps: Vec<(i32, i32)>,
    tmp: Vec<i32>,
    count: Vec<u32>,
    start: Vec<u32>,
    bar: Vec<u32>,
    dbar: Vec<u32>,
    /// `last` when the current deterministic-loop iteration started.
    dlast: Vec<usize>,
    last: usize,
    stack: Vec<Frame>,
    steps: usize,
    /// Forward work not yet charged as a step.
    work: usize,
    step_limit: usize,
}

/// Whether `prog` matches all of `text`.
pub(crate) fn is_match(prog: &Program, text: &[u16], step_limit: usize) -> Result<bool, ExecError> {
    new_vm(prog, text, step_limit).run()
}

/// The group spans after a successful match (`(-1, -1)` for unset groups;
/// index 0 unused), or `None` when there is no match. For probes.
#[cfg(test)]
pub(crate) fn match_groups(
    prog: &Program,
    text: &[u16],
    step_limit: usize,
) -> Result<Option<Vec<(i32, i32)>>, ExecError> {
    let mut vm = new_vm(prog, text, step_limit);
    Ok(vm.run()?.then(|| vm.caps.clone()))
}

fn new_vm<'a>(prog: &'a Program, text: &'a [u16], step_limit: usize) -> Vm<'a> {
    Vm {
        prog,
        text,
        pos: 0,
        caps: vec![(-1, -1); prog.groups + 1],
        tmp: vec![-1; prog.groups + 1],
        count: vec![0; prog.loops.len()],
        start: vec![0; prog.loops.len()],
        bar: vec![0; prog.looks.len() + prog.n_slots],
        dbar: vec![0; prog.loops.len()],
        dlast: vec![0; prog.loops.len()],
        last: 0,
        stack: Vec::new(),
        steps: 0,
        work: 0,
        step_limit,
    }
}

fn is_line_terminator(u: u16) -> bool {
    matches!(u, 0x0A | 0x0D | 0x85 | 0x2028 | 0x2029)
}

impl Vm<'_> {
    fn push(&mut self, f: Frame) -> Result<(), ExecError> {
        if self.stack.len() >= MAX_FRAMES {
            return Err(ExecError::RecursionLimit);
        }
        self.stack.push(f);
        #[cfg(test)]
        PEAK_FRAMES.fetch_max(self.stack.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Charges `n` units of forward work against the step budget.
    #[inline]
    fn charge(&mut self, n: usize) -> Result<(), ExecError> {
        self.work += n;
        if self.work >= WORK_PER_STEP {
            self.steps += self.work / WORK_PER_STEP;
            self.work %= WORK_PER_STEP;
            if self.steps > self.step_limit {
                return Err(ExecError::StepLimit);
            }
        }
        Ok(())
    }

    /// Drops every record from `idx` up (a commit).
    fn truncate(&mut self, idx: usize) {
        self.stack.truncate(idx);
    }

    /// Drops the choice points above `idx`, keeping undo records.
    fn soft_cut(&mut self, idx: usize) {
        let mut w = idx + 1;
        for r in idx + 1..self.stack.len() {
            let f = self.stack[r];
            if f.is_undo() {
                self.stack[w] = f;
                w += 1;
            }
        }
        self.stack.truncate(w);
    }

    /// Removes the record at `idx`, keeping the (undo) records above it.
    fn remove_at(&mut self, idx: usize) {
        self.stack.remove(idx);
    }

    fn undo(&mut self, f: Frame) {
        match f {
            Frame::Cap { g, s, e } => self.caps[g as usize] = (s, e),
            Frame::Tmp { g, v } => self.tmp[g as usize] = v,
            Frame::Loop { l, count, start } => {
                self.count[l as usize] = count;
                self.start[l as usize] = start;
            }
            _ => {}
        }
    }

    fn save_loop(&mut self, l: u32) -> Result<(), ExecError> {
        let f = Frame::Loop {
            l,
            count: self.count[l as usize],
            start: self.start[l as usize],
        };
        self.push(f)
    }

    /// Pops records until a choice point resumes; its program counter, or
    /// `None` when the match has failed. Every resumption is one step of
    /// the backtrack budget.
    fn backtrack(&mut self) -> Result<Option<usize>, ExecError> {
        let resumed = self.backtrack_to_choice()?;
        if resumed.is_some() {
            self.steps += 1;
            if self.steps > self.step_limit {
                return Err(ExecError::StepLimit);
            }
        }
        Ok(resumed)
    }

    fn backtrack_to_choice(&mut self) -> Result<Option<usize>, ExecError> {
        while let Some(f) = self.stack.pop() {
            if f.is_undo() {
                self.undo(f);
                continue;
            }
            match f {
                Frame::Choice { pc, pos } => {
                    self.pos = pos as usize;
                    return Ok(Some(pc as usize));
                }
                Frame::GLazy { l, pos } => {
                    self.pos = pos as usize;
                    self.save_loop(l)?;
                    self.start[l as usize] = pos;
                    return Ok(Some(self.prog.loops[l as usize].body as usize));
                }
                Frame::Look { li, i, next } => {
                    let info = self.prog.looks[li as usize];
                    if info.behind && next != NONE {
                        let lo = self.lookbehind_floor(info, i);
                        if info.wide {
                            self.charge(i as usize - lo.max(0) as usize)?;
                        }
                        let f = Frame::Look {
                            li,
                            i,
                            next: if i64::from(next) > lo {
                                self.lookbehind_step(info, next)
                            } else {
                                NONE
                            },
                        };
                        self.push(f)?;
                        self.bar[li as usize] = (self.stack.len() - 1) as u32;
                        self.pos = next as usize;
                        return Ok(Some(info.body as usize));
                    }
                    if info.negate {
                        self.pos = i as usize;
                        return Ok(Some(info.end as usize));
                    }
                }
                Frame::Atomic => {}
                Frame::DSoft { l, pos, count } => {
                    let info = self.prog.loops[l as usize];
                    self.pos = pos as usize;
                    self.count[l as usize] = count;
                    if info.greedy && count >= info.min {
                        return Ok(Some(info.exit as usize));
                    }
                }
                Frame::DGive { l, pos, count } => {
                    let info = self.prog.loops[l as usize];
                    self.pos = pos as usize;
                    self.count[l as usize] = count;
                    if count >= info.min {
                        return Ok(Some(info.exit as usize));
                    }
                }
                Frame::DLazy { l, pos, count } => {
                    let info = self.prog.loops[l as usize];
                    self.pos = pos as usize;
                    self.count[l as usize] = count;
                    self.push(Frame::DSoft { l, pos, count })?;
                    self.dbar[l as usize] = (self.stack.len() - 1) as u32;
                    self.dlast[l as usize] = self.last;
                    return Ok(Some(info.body as usize));
                }
                Frame::Canon { pc, pos, idx } => {
                    self.pos = pos as usize;
                    if self.canon_from(pc, idx as usize)? {
                        return Ok(Some(pc as usize + 1));
                    }
                }
                Frame::LineAlt { pc, pos } => {
                    self.pos = pos as usize;
                    return Ok(Some(pc as usize));
                }
                Frame::Cap { .. } | Frame::Tmp { .. } | Frame::Loop { .. } => {}
            }
        }
        Ok(None)
    }

    /// The farthest look-behind start: `i - max` in Java's 32-bit
    /// arithmetic, not before the input start (spec LB-3); in code points
    /// when the window is `wide`, with a negative maximum counting as zero.
    fn lookbehind_floor(&self, info: LookInfo, i: u32) -> i64 {
        if info.wide {
            let steps = u32::try_from(info.max_w).unwrap_or(0);
            return walk_back(self.text, i as usize, steps).0 as i64;
        }
        i64::from((i as i32).wrapping_sub(info.max_w)).max(0)
    }

    /// The nearest look-behind start, `i - min` (Java's 32-bit arithmetic);
    /// a start past `i` cannot end at `i`. When `wide`, `min` code points
    /// back, stopping at the input start when there are fewer: the body
    /// then still runs there (`(?<!(.?)++TT)\1𐐨{0}` matches the empty
    /// string, the capture leaking out of the failed body).
    fn lookbehind_first(&self, info: LookInfo, i: u32) -> i64 {
        if info.wide {
            let steps = u32::try_from(info.min_w).unwrap_or(0);
            return walk_back(self.text, i as usize, steps).0 as i64;
        }
        i64::from((i as i32).wrapping_sub(info.min_w)).min(i64::from(i))
    }

    /// The look-behind start after `s`, one unit or one code point back.
    fn lookbehind_step(&self, info: LookInfo, s: u32) -> u32 {
        if info.wide {
            walk_back(self.text, s as usize, 1).0 as u32
        } else {
            s - 1
        }
    }

    /// Matches the `(?c)` class at `pc` from its candidate `idx` on.
    fn canon_from(&mut self, pc: u32, idx: usize) -> Result<bool, ExecError> {
        let Inst::Canon(si) = self.prog.insts[pc as usize] else {
            return Ok(false);
        };
        if self.pos >= self.text.len() {
            return Ok(false);
        }
        let set = &self.prog.sets[si as usize];
        let cands = canon::candidates(self.text, self.pos);
        // Composing candidates costs several times a plain read.
        let scanned = cands.last().map_or(0, |&(end, _)| end - self.pos);
        self.charge(4 * scanned)?;
        let mut hits = cands
            .iter()
            .enumerate()
            .skip(idx)
            .filter(|(_, (_, c))| set.contains(*c));
        let Some((j, &(end, _))) = hits.next() else {
            return Ok(false);
        };
        if hits.next().is_some() {
            let f = Frame::Canon {
                pc,
                pos: self.pos as u32,
                idx: (j + 1) as u32,
            };
            self.push(f)?;
        }
        self.pos = end;
        Ok(true)
    }

    fn run(&mut self) -> Result<bool, ExecError> {
        let prog = self.prog;
        let text = self.text;
        let n = text.len();
        let mut pc = 0usize;
        loop {
            self.charge(1)?;
            let ok = match &prog.insts[pc] {
                Inst::Char(c) => {
                    if self.pos < n && text[self.pos] == *c {
                        self.pos += 1;
                        pc += 1;
                        true
                    } else {
                        false
                    }
                }
                Inst::Set(i) => {
                    if self.pos < n {
                        let (cp, w) = cp_at(text, self.pos);
                        if prog.sets[*i as usize].contains(cp) {
                            self.pos += w;
                            pc += 1;
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
                Inst::Canon(_) => {
                    if self.canon_from(pc as u32, 0)? {
                        pc += 1;
                        true
                    } else {
                        false
                    }
                }
                Inst::Split(t) => {
                    self.push(Frame::Choice {
                        pc: *t,
                        pos: self.pos as u32,
                    })?;
                    pc += 1;
                    true
                }
                Inst::Jmp(t) => {
                    pc = *t as usize;
                    true
                }
                Inst::CapOpen(g) => {
                    let g = *g as usize;
                    self.push(Frame::Tmp {
                        g: g as u32,
                        v: self.tmp[g],
                    })?;
                    self.tmp[g] = self.pos as i32;
                    pc += 1;
                    true
                }
                Inst::CapClose(g) => {
                    let g = *g as usize;
                    let (s, e) = self.caps[g];
                    self.push(Frame::Cap { g: g as u32, s, e })?;
                    self.caps[g] = (self.tmp[g], self.pos as i32);
                    pc += 1;
                    true
                }
                Inst::Assert(a) => {
                    if self.assert(*a)? {
                        pc += 1;
                        true
                    } else {
                        false
                    }
                }
                Inst::BackRef(g, case) => {
                    if self.backref(*g as usize, *case)? {
                        pc += 1;
                        true
                    } else {
                        false
                    }
                }
                Inst::LineBreak => {
                    if self.pos < n {
                        let u = text[self.pos];
                        if u == 0x0D && self.pos + 1 < n && text[self.pos + 1] == 0x0A {
                            self.push(Frame::LineAlt {
                                pc: (pc + 1) as u32,
                                pos: (self.pos + 1) as u32,
                            })?;
                            self.pos += 2;
                            pc += 1;
                            true
                        } else if matches!(u, 0x0A..=0x0D | 0x85 | 0x2028 | 0x2029) {
                            self.pos += 1;
                            pc += 1;
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
                Inst::Grapheme => {
                    if self.pos < n {
                        // Cluster segmentation costs about twice a read.
                        let end = cluster_end(text, self.pos);
                        self.charge(2 * (end - self.pos))?;
                        self.pos = end;
                        pc += 1;
                        true
                    } else {
                        false
                    }
                }
                Inst::LookStart(li) => {
                    let li = *li;
                    let info = prog.looks[li as usize];
                    let i = self.pos as u32;
                    if info.behind {
                        let first = self.lookbehind_first(info, i);
                        let lo = self.lookbehind_floor(info, i);
                        if info.wide {
                            self.charge(i as usize - lo.max(0) as usize)?;
                        }
                        if first < lo {
                            // No start position fits.
                            if info.negate {
                                pc = info.end as usize;
                                true
                            } else {
                                false
                            }
                        } else {
                            let next = if first > lo {
                                self.lookbehind_step(info, first as u32)
                            } else {
                                NONE
                            };
                            self.push(Frame::Look { li, i, next })?;
                            self.bar[li as usize] = (self.stack.len() - 1) as u32;
                            self.pos = first as usize;
                            pc = info.body as usize;
                            true
                        }
                    } else {
                        self.push(Frame::Look { li, i, next: NONE })?;
                        self.bar[li as usize] = (self.stack.len() - 1) as u32;
                        pc = info.body as usize;
                        true
                    }
                }
                Inst::LookEnd(li) => {
                    let info = prog.looks[*li as usize];
                    let idx = self.bar[*li as usize] as usize;
                    let Frame::Look { i, .. } = self.stack[idx] else {
                        unreachable!("look-around barrier");
                    };
                    if info.behind && self.pos != i as usize {
                        false
                    } else {
                        let end = self.pos;
                        self.truncate(idx);
                        if !info.behind {
                            self.last = end;
                        }
                        self.pos = i as usize;
                        if info.negate {
                            false
                        } else {
                            pc = info.end as usize;
                            true
                        }
                    }
                }
                Inst::AtomicStart(s) => {
                    self.push(Frame::Atomic)?;
                    self.bar[*s as usize + prog.looks.len()] = (self.stack.len() - 1) as u32;
                    pc += 1;
                    true
                }
                Inst::AtomicEnd(s, moves_last) => {
                    let idx = self.bar[*s as usize + prog.looks.len()] as usize;
                    self.truncate(idx);
                    if *moves_last {
                        self.last = self.pos;
                    }
                    pc += 1;
                    true
                }
                Inst::GInit(l) => {
                    self.save_loop(*l)?;
                    self.count[*l as usize] = 0;
                    pc += 1;
                    true
                }
                Inst::GTest(l) => {
                    let l = *l;
                    let info = prog.loops[l as usize];
                    let c = self.count[l as usize];
                    if c < info.min || (c < info.max && info.greedy) {
                        if c >= info.min {
                            self.push(Frame::Choice {
                                pc: info.exit,
                                pos: self.pos as u32,
                            })?;
                        }
                        self.save_loop(l)?;
                        self.start[l as usize] = self.pos as u32;
                        pc = info.body as usize;
                    } else if c < info.max {
                        self.push(Frame::GLazy {
                            l,
                            pos: self.pos as u32,
                        })?;
                        pc = info.exit as usize;
                    } else {
                        pc = info.exit as usize;
                    }
                    true
                }
                Inst::GEnd(l) => {
                    let l = *l;
                    let info = prog.loops[l as usize];
                    if self.pos == self.start[l as usize] as usize {
                        pc = info.exit as usize;
                    } else {
                        self.save_loop(l)?;
                        self.count[l as usize] += 1;
                        pc = info.test as usize;
                    }
                    true
                }
                Inst::DInit(l) => {
                    self.save_loop(*l)?;
                    self.count[*l as usize] = 0;
                    pc += 1;
                    true
                }
                Inst::DTry(l) => {
                    let l = *l;
                    let info = prog.loops[l as usize];
                    let c = self.count[l as usize];
                    let pos = self.pos as u32;
                    if c < info.min || (info.greedy && c < info.max) {
                        self.push(Frame::DSoft { l, pos, count: c })?;
                        self.dbar[l as usize] = (self.stack.len() - 1) as u32;
                        self.dlast[l as usize] = self.last;
                        pc = info.body as usize;
                    } else if c < info.max {
                        self.push(Frame::DLazy { l, pos, count: c })?;
                        pc = info.exit as usize;
                    } else {
                        pc = info.exit as usize;
                    }
                    true
                }
                Inst::DEnd(l) => {
                    let l = *l;
                    let info = prog.loops[l as usize];
                    let idx = self.dbar[l as usize] as usize;
                    let Frame::DSoft {
                        pos: p0, count: c0, ..
                    } = self.stack[idx]
                    else {
                        unreachable!("deterministic-loop barrier");
                    };
                    self.soft_cut(idx);
                    if info.sets_last {
                        self.last = self.pos;
                    }
                    if self.pos == p0 as usize {
                        if c0 >= info.min {
                            if info.possessive {
                                self.remove_at(idx);
                                self.count[l as usize] = c0 + 1;
                                pc = info.exit as usize;
                                true
                            } else {
                                // The empty iteration does not record the
                                // capture of the group the loop repeats
                                // (CAP-5: `(\b)*a\1` fails on `a`); groups
                                // inside it keep theirs (`(?:(\b))*a\1`
                                // and `((\b))*a\2` match `a`).
                                let mut kept = Vec::new();
                                while self.stack.len() > idx + 1 {
                                    let Some(f) = self.stack.pop() else { break };
                                    match f {
                                        Frame::Cap { g, .. } | Frame::Tmp { g, .. }
                                            if Some(g) == info.own_cap =>
                                        {
                                            self.undo(f);
                                        }
                                        _ => kept.push(f),
                                    }
                                }
                                self.truncate(idx);
                                self.stack.extend(kept.into_iter().rev());
                                self.count[l as usize] = c0;
                                if info.greedy {
                                    pc = info.exit as usize;
                                    true
                                } else {
                                    false
                                }
                            }
                        } else {
                            // Below the minimum an empty iteration counts.
                            // When it changed nothing (captures, `last`),
                            // every remaining required one would be the same.
                            let unchanged = self.last == self.dlast[l as usize]
                                && self.stack[idx + 1..].iter().all(|f| match *f {
                                    Frame::Cap { g, s, e } => self.caps[g as usize] == (s, e),
                                    _ => true,
                                });
                            self.remove_at(idx);
                            self.count[l as usize] = if unchanged { info.min } else { c0 + 1 };
                            pc = info.test as usize;
                            true
                        }
                    } else {
                        if info.greedy {
                            self.stack[idx] = Frame::DGive {
                                l,
                                pos: p0,
                                count: c0,
                            };
                        } else {
                            self.remove_at(idx);
                        }
                        self.count[l as usize] = c0.saturating_add(1);
                        pc = info.test as usize;
                        true
                    }
                }
                Inst::Match => {
                    if self.pos == n {
                        return Ok(true);
                    }
                    false
                }
            };
            if !ok {
                match self.backtrack()? {
                    Some(p) => pc = p,
                    None => return Ok(false),
                }
            }
        }
    }

    fn assert(&mut self, a: Assertion) -> Result<bool, ExecError> {
        let t = self.text;
        let n = t.len();
        let p = self.pos;
        Ok(match a {
            Assertion::Caret { multiline, unix } => {
                if !multiline {
                    p == 0
                } else if p == 0 {
                    n > 0
                } else if p == n {
                    false
                } else {
                    let c = t[p - 1];
                    if unix {
                        c == 0x0A
                    } else {
                        matches!(c, 0x0A | 0x85 | 0x2028 | 0x2029) || (c == 0x0D && t[p] != 0x0A)
                    }
                }
            }
            Assertion::Dollar { multiline, unix } => {
                if !multiline {
                    end_z(t, p, unix)
                } else if p == n {
                    true
                } else if unix {
                    t[p] == 0x0A
                } else {
                    is_line_terminator(t[p]) && !(t[p] == 0x0A && p > 0 && t[p - 1] == 0x0D)
                }
            }
            Assertion::StartInput | Assertion::LastMatch => p == 0,
            Assertion::EndInput => p == n,
            Assertion::EndZ { unix } => end_z(t, p, unix),
            Assertion::Word { unicode, negate } => {
                let (b, scanned) = word_boundary(t, p, unicode);
                self.charge(scanned)?;
                b != negate
            }
            Assertion::Grapheme => {
                if p == 0 || p == n {
                    true
                } else if (0xD800..0xDC00).contains(&t[p - 1]) && (0xDC00..0xE000).contains(&t[p]) {
                    // Never inside a surrogate pair.
                    false
                } else if self.last >= n {
                    return Err(ExecError::JavaException(format!(
                        "java.lang.StringIndexOutOfBoundsException: Index {} out of bounds for length {n}",
                        self.last
                    )));
                } else {
                    let end = cluster_end(t, self.last);
                    self.charge(2 * (end - self.last))?;
                    p >= end
                }
            }
        })
    }

    fn backref(&mut self, g: usize, case: RefCase) -> Result<bool, ExecError> {
        let Some(&(s, e)) = self.caps.get(g) else {
            return Ok(false);
        };
        if s < 0 {
            return Ok(false);
        }
        let (s, e) = (s as usize, e as usize);
        // A Unicode case-insensitive comparison costs about twice a plain one.
        let weight = if case == RefCase::Unicode { 2 } else { 1 };
        self.charge(weight * (e - s))?;
        Ok(self.compare_ref(s, e, case))
    }

    /// Whether the text at the position repeats `[s, e)`; advances past it.
    fn compare_ref(&mut self, s: usize, e: usize, case: RefCase) -> bool {
        let t = self.text;
        let cap = &t[s..e];
        let p = self.pos;
        match case {
            RefCase::Sensitive | RefCase::Ascii => {
                if p + cap.len() > t.len() {
                    return false;
                }
                let ok = if case == RefCase::Sensitive {
                    t[p..p + cap.len()] == *cap
                } else {
                    cap.iter()
                        .zip(&t[p..p + cap.len()])
                        .all(|(&a, &b)| casefold::ascii_ref_eq(a, b))
                };
                if ok {
                    self.pos += cap.len();
                }
                ok
            }
            RefCase::Unicode => {
                let mut i = 0;
                let mut q = p;
                while i < cap.len() {
                    if q >= t.len() {
                        return false;
                    }
                    let (a, wa) = cp_at(cap, i);
                    let (b, wb) = cp_at(t, q);
                    if !casefold::unicode_ref_eq(a, b) {
                        return false;
                    }
                    i += wa;
                    q += wb;
                }
                self.pos = q;
                true
            }
        }
    }
}

/// The position `steps` code points before `i` (pairs read backwards), and
/// how many steps were taken before reaching the input start.
fn walk_back(t: &[u16], mut i: usize, steps: u32) -> (usize, u32) {
    let mut done = 0;
    while done < steps && i > 0 {
        i -= if i >= 2
            && (0xDC00..0xE000).contains(&t[i - 1])
            && (0xD800..0xDC00).contains(&t[i - 2])
        {
            2
        } else {
            1
        };
        done += 1;
    }
    (i, done)
}

/// `$` without MULTILINE and `\Z` (spec ANC-3).
fn end_z(t: &[u16], p: usize, unix: bool) -> bool {
    let n = t.len();
    if p == n {
        return true;
    }
    if unix {
        return p + 1 == n && t[p] == 0x0A;
    }
    match n - p {
        1 => is_line_terminator(t[p]) && !(t[p] == 0x0A && p > 0 && t[p - 1] == 0x0D),
        2 => t[p] == 0x0D && t[p + 1] == 0x0A,
        _ => false,
    }
}

/// Whether the code point (or unit) is a word character for `\b`.
/// Whether `\b` holds at `p`, and how many code units were scanned.
fn word_boundary(t: &[u16], p: usize, unicode: bool) -> (bool, usize) {
    let n = t.len();
    if unicode {
        // WB-2: the code point at `p` and the one ending at `p`.
        let right =
            p < n && ucd::unicode_word().contains(cp_at(t, p).0) && !is_surrogate_cp(cp_at(t, p).0);
        let left = p > 0 && {
            let lo = t[p - 1];
            let cp =
                if (0xDC00..0xE000).contains(&lo) && p >= 2 && (0xD800..0xDC00).contains(&t[p - 2])
                {
                    0x10000 + ((u32::from(t[p - 2]) - 0xD800) << 10) + (u32::from(lo) - 0xDC00)
                } else {
                    u32::from(lo)
                };
            !is_surrogate_cp(cp) && ucd::unicode_word().contains(cp)
        };
        return (left != right, 2);
    }
    // WB-3.
    let (right, a) = if p < n {
        ascii_word_at(t, p, cp_at(t, p).0)
    } else {
        (false, 0)
    };
    let (left, b) = if p > 0 {
        ascii_word_at(t, p - 1, u32::from(t[p - 1]))
    } else {
        (false, 0)
    };
    (left != right, a + b)
}

fn is_surrogate_cp(c: u32) -> bool {
    (0xD800..0xE000).contains(&c)
}

/// WB-3's `isWordAt(q, c)`, and how many code units it scanned.
fn ascii_word_at(t: &[u16], q: usize, c: u32) -> (bool, usize) {
    if ucd::is_ascii_word(c) {
        return (true, 1);
    }
    if ucd::gc(c) != ucd::GC_MN {
        return (false, 1);
    }
    let mut j = q;
    while j > 0 {
        j -= 1;
        let u = u32::from(t[j]);
        if ucd::gc(u) == ucd::GC_MN {
            continue;
        }
        let g = ucd::gc(u);
        return (matches!(g, 1..=5) || g == ucd::GC_ND, q - j + 1);
    }
    (false, q + 1)
}
