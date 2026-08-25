//! Metamorphic query-correctness oracles (G2 / Track B).
//!
//! These tests attack the *silent-wrong-answer* class — pushdown drops, lost
//! edges, three-valued-logic bugs — without any reference engine, by checking
//! algebraic relations a correct engine must satisfy:
//!
//! - [`tlp`] — Ternary Logic Partitioning: the result of a query equals the
//!   multiset union of its three predicate partitions (TRUE / FALSE / NULL).
//! - [`norec`] — No-Optimization Reference Comparison: a predicate yields the
//!   same answer whether or not the optimizer can push it into the scan.
//!
//! Both consume the [`seed`] fixture and the `diff` row-bag comparator, and
//! build queries via `querygen`.
//!
//! # Case budgets
//!
//! Every oracle has two tiers, mirroring `uni-locy-oracle`'s soak pattern: a
//! non-ignored smoke tier (PR gate) and an `#[ignore]` soak tier (nightly
//! volume). Both read the case count from `METAMORPHIC_CASES`, with a small
//! smoke default and a larger soak default; the nightly job sets the env var
//! high and runs `--run-ignored ignored-only`.
//!
//! # Shared seed database
//!
//! [`drive`] builds **one** read-only seed database and reuses it across every
//! generated case. The metamorphic queries never write (no `tx()`), the seed is
//! immutable, and node/edge ids are frozen at write time, so all cases see
//! identical, deterministic state — and amortizing the build (the dominating
//! per-case cost) is what makes the ≥100k nightly volume feasible.

pub mod dense;
pub mod dqp;
pub mod multi;
pub mod norec;
pub mod seed;
pub mod sparse;
pub mod structural;
pub mod tlp;

use std::future::Future;
use std::pin::Pin;

use proptest::prelude::Strategy;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use uni_cypher::ast::Query;
use uni_db::Uni;

use crate::diff::{RowBag, bag};
use crate::querygen::render::render;

/// A boxed future borrowing the shared db and the current case for `'a`.
type CheckFut<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>;

/// Drives a metamorphic law over `strategy` against one shared read-only seed db.
///
/// Builds the seed and a current-thread runtime once, then runs a proptest
/// [`TestRunner`] of `cases` iterations, calling `check(&db, &case)` per case.
/// The boxed-future closure lets each case borrow the shared `&db` across its
/// `.await`s; boxing is negligible against query-execution cost.
///
/// # Panics
///
/// Panics (failing the test) if the seed cannot be built or any case violates
/// the law — proptest shrinks and reports the minimal failing case, whose error
/// carries the rendered queries and the bag symmetric difference.
pub fn drive<S, F>(cases: u32, strategy: S, check: F)
where
    S: Strategy,
    F: for<'a> Fn(&'a Uni, &'a S::Value) -> CheckFut<'a>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let db = rt.block_on(seed::build_seed()).expect("build seed db");
    let mut runner = TestRunner::new(Config {
        cases,
        ..Config::default()
    });
    runner
        .run(&strategy, |case| {
            rt.block_on(check(&db, &case))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            Ok(())
        })
        .expect("metamorphic law violated");
}

/// Default smoke-tier case count (fast PR gate).
const SMOKE_CASES: u32 = 64;
/// Default soak-tier case count (nightly volume; the nightly job overrides it).
const SOAK_CASES: u32 = 256;

/// Smoke-tier case count: `METAMORPHIC_CASES` or [`SMOKE_CASES`].
pub fn smoke_cases() -> u32 {
    env_cases(SMOKE_CASES)
}

/// Soak-tier case count: `METAMORPHIC_CASES` or [`SOAK_CASES`].
pub fn soak_cases() -> u32 {
    env_cases(SOAK_CASES)
}

fn env_cases(default: u32) -> u32 {
    std::env::var("METAMORPHIC_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Renders `q` and runs it against `db`, returning its result as a row bag.
///
/// # Errors
///
/// Returns any error from query execution.
pub async fn run_bag(db: &Uni, q: &Query) -> anyhow::Result<RowBag> {
    let cypher = render(q);
    let result = db.session().query(&cypher).await?;
    Ok(bag(&result))
}

/// Renders a single-row, single-column aggregate query and returns its scalar
/// value as `f64`.
///
/// A NULL scalar (e.g. `sum` over an empty partition) reads as `0.0`, matching
/// the additive reconciliation the aggregate-TLP law expects.
///
/// # Errors
///
/// Returns an error if the query fails or does not produce exactly one cell.
///
/// # Panics
///
/// Panics if the single cell is non-numeric — an aggregate query is expected to
/// yield a number, so anything else is a generator/engine bug, not a runtime
/// condition.
pub async fn run_scalar(db: &Uni, q: &Query) -> anyhow::Result<f64> {
    let cypher = render(q);
    let result = db.session().query(&cypher).await?;
    let rows = result.rows();
    anyhow::ensure!(
        rows.len() == 1 && rows[0].values().len() == 1,
        "expected a single scalar cell from `{cypher}`, got {} rows",
        rows.len()
    );
    let v = &rows[0].values()[0];
    if v.is_null() {
        return Ok(0.0);
    }
    if let Some(i) = v.as_i64() {
        return Ok(i as f64);
    }
    if let Some(f) = v.as_f64() {
        return Ok(f);
    }
    panic!("aggregate scalar is non-numeric in `{cypher}`: {v:?}");
}
