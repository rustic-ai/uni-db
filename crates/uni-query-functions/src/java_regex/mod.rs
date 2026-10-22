// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Java 21 `java.util.regex` matching for Cypher's `=~`.
//!
//! `s =~ p` is `Pattern.compile(p).matcher(s).matches()`: Java's syntax,
//! Java's defaults (ASCII `\w`/`\b`, ASCII-only `(?i)` unless `(?u)`, `.`
//! excluding Java's line terminators), Unicode 15.0 data, and a match that
//! must span the whole input. This module is a clean-room implementation of
//! the behaviour described in `docs/specs/java_regex_behaviour.md` (rule ids
//! such as `LB-3` below refer to it), extended by black-box probes of JDK 21
//! where the spec is silent; `PROVENANCE.md` lists the sources.
//!
//! # Design
//!
//! * `parse` turns the pattern into an `ast::Node` tree, reporting
//!   Java's `PatternSyntaxException` descriptions and indices, in the order
//!   Java finds the errors (spec §1–§4, §7, §8, §10; ERR-3's indices as
//!   black-box probes settle them). `\Q…\E` is removed first, as Java does (LIT-10). Every
//!   single-code-point matcher becomes a code point set with the case rules
//!   of its position folded in (`casefold`, CI-1…CI-6; `props` for
//!   `\p{…}`, PRE-*, PROP-*, SCR-1, BLK-1; `names` for `\N{…}`, LIT-8),
//!   evaluated against the JDK 21 tables in `ucd_tables.rs` and
//!   `ucd_extra.rs`.
//! * `analyze` decides each quantifier's strategy and computes Java's
//!   look-behind length estimate (LB-1, LB-2).
//! * `linear`: patterns whose answer is plain language membership run on
//!   `regex-automata` in linear time ([`Route::Linear`]).
//! * `backtrack`: everything else — look-around, back-references, atomic
//!   groups, possessive quantifiers, `\b`, `\R`, `\X`, `\b{g}`, `(?c)` —
//!   runs on a backtracking matcher over UTF-16 units with an explicit stack
//!   ([`Route::Backtracking`]; SUR-1…SUR-4, CAP-1…CAP-5, QNT-8, LB-3, BR-*),
//!   bounded by the backtrack limit and a stack budget. `grapheme`
//!   computes clusters (GC-1, GC-2) and `canon` the `(?c)` class matches.
//!
//! Where the specification is silent or partial (its §17), the behaviour was
//! settled by black-box probes of JDK 21; `PROVENANCE.md` lists the findings
//! and the code cites the deciding probes.
//!
//! # Limits
//!
//! * A backtracking match that takes more than the backtrack limit
//!   (default [`DEFAULT_BACKTRACK_LIMIT`]) of steps fails with
//!   [`MatchError::BacktrackLimit`]; Java has no such limit. A step is a
//!   resumption at a choice point or four units of forward work
//!   (`backtrack::WORK_PER_STEP`), so the limit bounds time: measured in a
//!   release build on 1 KB subjects, a match refused at the default limit
//!   takes 60–320 ms, the costliest per step being `(?c)` classes, `\X`
//!   and Unicode case-insensitive back-references.
//! * More than `backtrack::MAX_FRAMES` backtrack-stack records (16 bytes
//!   each, about 64 MB) fail with [`MatchError::RecursionLimit`] ("Regex
//!   recursion limit exceeded"). Java throws `StackOverflowError` on many
//!   such inputs much earlier (RES-1, e.g. `(?:a|b)*` past about 1 500
//!   characters with its default stack); here they are answered up to the
//!   budget instead (the divergence RES-2 recommends): `(?>a|b)*` and
//!   `(?:a|b)*(?=)` answer on a million characters (48 and 56 MB of stack).
//! * Pattern nesting deeper than `parse::MAX_NESTING` (2 000) is refused
//!   with Java's "Stack overflow during pattern compilation" (RES-3). Java's
//!   own limit depends on its thread stack and on JIT state: with the
//!   default stack, one JDK 21 run refused 1 300 nested groups yet accepted
//!   3 000 later. Patterns up to 2 000 levels are answered as Java answers
//!   them when its stack suffices.
//! * Where Java's matcher itself throws on an input (`\b{g}` after a
//!   look-ahead that reached the end, see `backtrack`), the match fails
//!   with [`MatchError::JavaException`] naming Java's exception.

mod analyze;
mod ast;
pub(crate) mod backtrack;
mod canon;
mod casefold;
mod charset;
mod grapheme;
mod linear;
mod names;
pub(crate) mod parse;
mod props;
mod ucd;
mod ucd_extra;
#[allow(dead_code)]
mod ucd_tables;

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;

/// The default for [`CypherRegex::with_backtrack_limit`]: steps a
/// backtracking match may take (see the module docs).
pub const DEFAULT_BACKTRACK_LIMIT: usize = 10_000_000;

/// Which matcher answers a pattern.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// `regex-automata`, linear in the input.
    Linear,
    /// The backtracking matcher, bounded by the backtrack limit.
    Backtracking,
}

/// A pattern Java refuses: the `PatternSyntaxException` it throws.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatternError {
    description: String,
    index: i32,
    pattern: String,
}

impl PatternError {
    /// Java's `getDescription()`.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Java's `getIndex()` (best effort; −1 when unknown).
    pub fn index(&self) -> i32 {
        self.index
    }
}

impl fmt::Display for PatternError {
    /// Java's `getMessage()` format (spec ERR-2).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)?;
        if self.index >= 0 {
            write!(f, " near index {}", self.index)?;
        }
        write!(f, "\n{}", self.pattern)?;
        let len = self.pattern.encode_utf16().count() as i32;
        if self.index >= 0 && self.index < len {
            write!(f, "\n{}^", " ".repeat(self.index as usize))?;
        }
        Ok(())
    }
}

impl std::error::Error for PatternError {}

/// Why a match has no answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchError {
    /// The backtracking matcher took more than `limit` steps.
    BacktrackLimit { pattern: String, limit: usize },
    /// The match needed more backtrack stack than allowed.
    RecursionLimit { pattern: String },
    /// Java's matcher throws this exception on the input.
    JavaException { pattern: String, exception: String },
}

const LIMIT_PREFIX: &str = "Regex backtrack limit exceeded: limit=";

impl fmt::Display for MatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Machine-readable: `parse_backtrack_limit_message` reads the
            // pattern back by its byte length.
            MatchError::BacktrackLimit { pattern, limit } => write!(
                f,
                "{LIMIT_PREFIX}{limit} pattern_bytes={}: {pattern}",
                pattern.len()
            ),
            MatchError::RecursionLimit { pattern } => write!(
                f,
                "Regex recursion limit exceeded: pattern '{pattern}' needs more than {} \
                 backtrack-stack records on this input",
                backtrack::MAX_FRAMES
            ),
            MatchError::JavaException { pattern, exception } => write!(
                f,
                "Regex match failed: Java's matcher throws {exception} for pattern '{pattern}' \
                 on this input"
            ),
        }
    }
}

impl std::error::Error for MatchError {}

/// The pattern and limit a [`MatchError::BacktrackLimit`] message carries,
/// found anywhere in `msg` (error layers may wrap it).
pub fn parse_backtrack_limit_message(msg: &str) -> Option<(String, usize)> {
    let at = msg.find(LIMIT_PREFIX)?;
    let rest = &msg[at + LIMIT_PREFIX.len()..];
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let limit: usize = rest[..digits].parse().ok()?;
    let rest = rest[digits..].strip_prefix(" pattern_bytes=")?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let len: usize = rest[..digits].parse().ok()?;
    let rest = rest[digits..].strip_prefix(": ")?;
    let pattern = rest.get(..len)?;
    Some((pattern.to_string(), limit))
}

/// A compiled Cypher `=~` pattern.
#[derive(Clone, Debug)]
pub struct CypherRegex {
    pattern: String,
    limit: usize,
    linear: Option<Arc<linear::Linear>>,
    program: Arc<backtrack::Program>,
}

impl CypherRegex {
    /// Compiles `pattern` with the default backtrack limit.
    ///
    /// # Errors
    ///
    /// Returns the `PatternSyntaxException` Java would throw.
    pub fn new(pattern: &str) -> Result<Self, PatternError> {
        Self::with_backtrack_limit(pattern, DEFAULT_BACKTRACK_LIMIT)
    }

    /// Compiles `pattern`; a backtracking match may take at most `limit`
    /// steps (see the module docs).
    ///
    /// # Errors
    ///
    /// Returns the `PatternSyntaxException` Java would throw.
    pub fn with_backtrack_limit(pattern: &str, limit: usize) -> Result<Self, PatternError> {
        let to_err = |e: parse::SyntaxError| PatternError {
            description: e.description,
            index: e.index,
            pattern: pattern.to_string(),
        };
        let parsed = parse::parse(pattern).map_err(to_err)?;
        let program = backtrack::compile(&parsed).map_err(to_err)?;
        let linear = linear::build(&parsed.root).map(Arc::new);
        Ok(Self {
            pattern: pattern.to_string(),
            limit,
            linear,
            program: Arc::new(program),
        })
    }

    /// The pattern as written.
    pub fn as_str(&self) -> &str {
        &self.pattern
    }

    /// Which matcher answers this pattern.
    pub fn route(&self) -> Route {
        if self.linear.is_some() {
            Route::Linear
        } else {
            Route::Backtracking
        }
    }

    /// Whether `text` matches in full, as Java's `matches()` answers.
    ///
    /// # Errors
    ///
    /// Returns a [`MatchError`] when the backtracking matcher exceeds its
    /// limits, or when Java's matcher throws on this input.
    pub fn is_match(&self, text: &str) -> Result<bool, MatchError> {
        if let Some(l) = &self.linear {
            return Ok(l.is_match(text));
        }
        let units: Vec<u16> = text.encode_utf16().collect();
        backtrack::is_match(&self.program, &units, self.limit).map_err(|e| match e {
            backtrack::ExecError::StepLimit => MatchError::BacktrackLimit {
                pattern: self.pattern.clone(),
                limit: self.limit,
            },
            backtrack::ExecError::RecursionLimit => MatchError::RecursionLimit {
                pattern: self.pattern.clone(),
            },
            backtrack::ExecError::JavaException(exception) => MatchError::JavaException {
                pattern: self.pattern.clone(),
                exception,
            },
        })
    }

    /// The group spans of a backtracking match (for probes).
    #[cfg(test)]
    pub(crate) fn groups(&self, units: &[u16]) -> Result<Option<Vec<(i32, i32)>>, MatchError> {
        backtrack::match_groups(&self.program, units, self.limit).map_err(|_| {
            MatchError::RecursionLimit {
                pattern: self.pattern.clone(),
            }
        })
    }

    /// The number of capturing groups.
    #[cfg(test)]
    pub(crate) fn group_count(&self) -> usize {
        self.program.group_count()
    }

    /// [`Self::is_match`] on the backtracking matcher even when the linear
    /// route exists (for differential tests).
    #[cfg(test)]
    pub(crate) fn is_match_backtracking(&self, text: &str) -> Result<bool, MatchError> {
        let units: Vec<u16> = text.encode_utf16().collect();
        backtrack::is_match(&self.program, &units, self.limit).map_err(|_| {
            MatchError::RecursionLimit {
                pattern: self.pattern.clone(),
            }
        })
    }
}
