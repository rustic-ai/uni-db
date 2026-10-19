//! Naive-Datalog reference oracle for differential-testing the Locy engine.
//!
//! Locy is a Datalog dialect, and Datalog's *naive fixpoint* (re-evaluate every
//! rule over every fact until nothing changes) is trivially correct and
//! semantically identical to the optimized semi-naive + `LeftAnti` algorithm the
//! production engine runs. This crate implements that naive evaluator as a
//! reference oracle: generated monotone-core programs are run through both the
//! engine and the oracle, and their derived fact sets must match exactly. A
//! mismatch is, by construction, an engine bug.
//!
//! # Independence invariant
//!
//! The oracle's entire value is sharing *zero* evaluation code with the engine.
//! This crate's **library** therefore depends only on [`uni_common`] (for the
//! shared [`uni_common::Value`] type) and `proptest`. The engine (`uni-db`) is a
//! dev-dependency, reachable only from `tests/`. Do not add `uni-db`, `uni-locy`,
//! or `uni-query` to `[dependencies]` — the boundary is the guarantee.
//!
//! # Scope
//!
//! The oracle covers plain rules, `IS` references, stratified `IS NOT` and
//! `YIELD`, and evaluates every rule as a bag before finalizing it by its kind:
//!
//! * a plain rule is a **set**;
//! * a `FOLD` rule aggregates the **bag** of its rows per key (`COUNT(*)`,
//!   `COUNT`, `SUM`/`MSUM`, `MIN`/`MMIN`, `MAX`/`MMAX`, `MCOUNT`, `MNOR`,
//!   `MPROD`), so parallel edges contribute once each; recursively, a
//!   self-reference reads the children's **folded** values (#162);
//! * an `ALONG` rule keeps **one fact per derivation path** (#159), equal
//!   values included — two paths that diverge below the first hop are two
//!   facts;
//! * a `BEST BY` rule keeps the best row per key (shortest or longest path);
//! * a PROB rule carries a probability (as a fixed-point integer), and an
//!   `IS NOT` between two PROB rules multiplies by the complement of the
//!   referenced facts' noisy-OR instead of anti-joining;
//! * a positive `IS` between two PROB rules multiplies in the referenced
//!   fact's probability, unless the clause's PROB expression already reads
//!   it (one `ProbFactor` per multiplied reference). Where no two proofs
//!   share an edge, that is the possible-worlds probability, which a unit
//!   test checks by enumerating every world of small graphs.
//!
//! Per-path and recursive-sum rules terminate only on acyclic input, so the
//! generator gives them the edges to a greater id. `DERIVE`, `ASSUME`,
//! `ABDUCE` and `HAVING` are out of scope.
//!
//! [`generator::random_program_strategy`] draws random programs over a random
//! multigraph (self-loops, parallel and identical edges, two relationship
//! types, integer weights) that exercise all of the above.
//!
//! [`surface`] holds each program's text as structured data, so a test can
//! render it under a renaming or another conjunct order (a metamorphic
//! relation) and map the oracle's answer through the same renaming.
//!
//! # Binding
//!
//! A clause body is an order-independent natural join: at each `IS` position
//! a bound variable tests and an unbound one binds, so the order of the `WHERE`
//! conditions never changes an answer. [`check_program`] computes the compile
//! errors the engine must raise for a program — an ambiguous bare column read,
//! an unbound `IS NOT` KEY, a node/scalar kind mismatch — before evaluation.

// Rust guideline compliant
pub mod eval;
pub mod generator;
pub mod ir;
pub mod surface;

#[doc(inline)]
pub use eval::{OracleError, check_program};

#[cfg(test)]
mod tests {
    /// P0 smoke test: proves the crate compiles and its test target runs.
    #[test]
    fn crate_links() {}
}
