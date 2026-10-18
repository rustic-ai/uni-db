// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Differential tests: the production Locy engine must agree, row for row, with
//! this crate's naive reference oracle.
//!
//! Each case is generated once and fed to both sides — the engine evaluates the
//! Locy program text over the seeded graph; the oracle evaluates the equivalent
//! IR — and their derived relations are compared. A divergence is, by
//! construction, an engine bug (see rustic-ai/uni-db#94 for one such find).
//!
//! The harness returns the engine's **raw** rows (duplicates preserved) so the
//! tests can witness set-semantics breakage directly — the historical `IS NOT`
//! bug emitted a bag, and deduping in the harness would have masked it.

// Rust guideline compliant

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use proptest::prelude::*;
use uni_common::{LocyIncompleteReason, Value};
use uni_db::{Uni, UniError};

use uni_locy_oracle::eval::{Relation, evaluate};
use uni_locy_oracle::generator::{
    build_complement, build_layered_dag, build_union, expected_closure_size,
};
use uni_locy_oracle::ir::{Generated, Tuple, WireKind};

// Submodules live in `tests/differential/` but join this one test binary
// (the crate cap is 3 integration-test binaries; see `docs/test_layout.md`).
#[path = "differential/assume.rs"]
mod assume;
#[path = "differential/goal.rs"]
mod goal;
#[path = "differential/random.rs"]
mod random;
#[path = "differential/relations.rs"]
mod relations;

/// Recovers the seeded integer id from a node `YIELD KEY` column value.
///
/// Strict: a node column must come back as a node. An integer is not
/// accepted in its place, even one equal to the expected id — seeded ids equal
/// VIDs, so tolerating it would hide a VID where a node belongs (and the
/// random-program harness, [`random::decode`], is strict the other way too).
///
/// # Panics
/// Panics on any value other than a node carrying an integer `id`.
fn key_to_id(value: &Value) -> i64 {
    match random::decode(WireKind::Node, value, &HashMap::new()) {
        Ok(id) => id,
        Err(e) => panic!("node KEY column: {e:?}"),
    }
}

/// Runs a generated program through the engine, returning relation `rel`'s **raw**
/// rows as id-tuples (duplicates preserved, so callers can check set semantics).
async fn engine_rows(generated: &Generated, rel: &str) -> Result<Vec<Tuple>> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(&generated.base_graph_cypher).await?;
    tx.commit().await?;

    let result = db.session().locy(&generated.program_text).await?;
    let key_cols = &generated.key_schema[rel];

    let rows = result.derived.get(rel).map_or_else(Vec::new, |rows| {
        rows.iter()
            .map(|row| {
                key_cols
                    .iter()
                    .map(|c| key_to_id(&row[c]))
                    .collect::<Tuple>()
            })
            .collect()
    });
    Ok(rows)
}

/// Returns relation `rel` from the oracle as a set (empty if the rule derived nothing).
fn oracle_facts(generated: &Generated, rel: &str) -> Relation {
    evaluate(&generated.oracle_rules)
        .remove(rel)
        .unwrap_or_default()
}

/// Set view of raw rows.
fn as_set(rows: &[Tuple]) -> HashSet<Tuple> {
    rows.iter().cloned().collect()
}

/// Compares one relation between engine and oracle; returns a failure note if they diverge.
async fn rel_divergence(generated: &Generated, rel: &str) -> Result<Option<String>> {
    let raw = engine_rows(generated, rel).await?;
    let set = as_set(&raw);
    let oracle = oracle_facts(generated, rel);
    if raw.len() != set.len() {
        return Ok(Some(format!(
            "{rel}: engine returned duplicates (raw={}, distinct={})",
            raw.len(),
            set.len(),
        )));
    }
    if set != oracle {
        return Ok(Some(format!(
            "{rel}: engine≠oracle (engine={} oracle={})",
            set.len(),
            oracle.len(),
        )));
    }
    Ok(None)
}

// ── P3: fixed cases spanning the 300-fact dedup threshold ────────────────────

/// Engine output equals the oracle on curated shapes, including the historical
/// `IS NOT`-bug closure `(4, 15) = 1350` and cases on both sides of 300.
///
/// Checks all three witnesses (no duplicates, closed-form size, oracle agreement)
/// and runs every case before asserting, so a regression reports the full
/// divergence picture rather than only the first failure.
#[tokio::test]
async fn engine_matches_oracle_fixed_cases() -> Result<()> {
    let cases = [(2, 3), (3, 5), (3, 10), (4, 15), (6, 5)];
    let mut failures = Vec::new();

    for &(stages, width) in &cases {
        let g = build_layered_dag(stages, width);
        let raw = engine_rows(&g, "reaches").await?;
        let set = as_set(&raw);
        let oracle = oracle_facts(&g, "reaches");

        if raw.len() != set.len() {
            failures.push(format!(
                "({stages},{width}): engine returned duplicates (raw={}, distinct={})",
                raw.len(),
                set.len(),
            ));
        }
        if set != oracle {
            failures.push(format!(
                "({stages},{width}): engine≠oracle (engine={} oracle={} closed-form={})",
                set.len(),
                oracle.len(),
                expected_closure_size(stages, width),
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "engine↔oracle divergence:\n{}",
        failures.join("\n"),
    );
    Ok(())
}

// ── P4: threshold-straddling property test ───────────────────────────────────

proptest! {
    // PR-time budget: modest case count and graph size so the gate stays fast.
    // The higher-volume nightly run lives in the `*_soak` test below (driven by
    // the `ORACLE_SOAK_CASES` env var).
    #![proptest_config(ProptestConfig { cases: 40, ..ProptestConfig::default() })]

    /// Randomized closures straddling the 300-fact dedup-strategy switch.
    ///
    /// The `(stages, width)` bounds make `expected_closure_size` land on both
    /// sides of 300. Three independent witnesses fail the moment the engine's
    /// `LeftAnti` path stops being set-correct, on either side of the threshold:
    /// no duplicate rows, the closed-form cardinality, and exact agreement with
    /// the naive oracle.
    #[test]
    fn closure_is_set_across_dedup_threshold(stages in 2usize..6, width in 5usize..18) {
        let g = build_layered_dag(stages, width);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let raw = rt.block_on(engine_rows(&g, "reaches")).expect("engine eval");
        let set = as_set(&raw);
        let expected = expected_closure_size(stages, width);

        // W1: set semantics — the engine returns no duplicate rows.
        prop_assert_eq!(raw.len(), set.len(), "duplicate rows at ({},{})", stages, width);
        // W2: closed-form cardinality (independent of the oracle).
        prop_assert_eq!(set.len(), expected, "wrong closure size at ({},{})", stages, width);
        // W3: row-for-row agreement with the naive oracle.
        prop_assert_eq!(set, oracle_facts(&g, "reaches"), "oracle disagreement at ({},{})", stages, width);
    }
}

// ── P5: completeness guard — never silent-short ──────────────────────────────

/// Seeds the standard graph and returns a ready database for a builder call.
async fn seeded_db(generated: &Generated) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(&generated.base_graph_cypher).await?;
    tx.commit().await?;
    Ok(db)
}

/// An over-budget evaluation must raise an explicit error, never return short rows.
///
/// For a program of known cardinality run under a 1ns timeout, the result must be
/// either exact (it happened to finish) or an explicit [`UniError::LocyIncomplete`]
/// — a silently-short `Ok` fails the exact-count assertion, which is the bug
/// (gap G14). The engine ships this behavior; this is the regression net.
#[tokio::test]
async fn incomplete_eval_errors_never_silent_short() -> Result<()> {
    let g = build_layered_dag(5, 15);
    let expected = expected_closure_size(5, 15);
    let db = seeded_db(&g).await?;

    let outcome = db
        .session()
        .locy_with(&g.program_text)
        .timeout(Duration::from_nanos(1))
        .run()
        .await;

    match outcome {
        Ok(result) => {
            let key_cols = &g.key_schema["reaches"];
            let n = result.derived.get("reaches").map_or(0, |rows| {
                rows.iter()
                    .map(|row| {
                        key_cols
                            .iter()
                            .map(|c| key_to_id(&row[c]))
                            .collect::<Tuple>()
                    })
                    .collect::<HashSet<_>>()
                    .len()
            });
            assert_eq!(n, expected, "completed but short — silent partial (G14)!");
        }
        Err(UniError::LocyIncomplete { detail }) => {
            assert!(
                matches!(
                    detail.reason,
                    LocyIncompleteReason::Timeout | LocyIncompleteReason::IterationLimit
                ),
                "unexpected incompleteness reason: {:?}",
                detail.reason,
            );
        }
        Err(e) => panic!("unexpected error (expected exact rows or LocyIncomplete): {e}"),
    }
    Ok(())
}

/// Under a generous budget the same program completes with the exact closure —
/// proving any partial above is caused by the budget, not the program.
#[tokio::test]
async fn generous_timeout_completes_exactly() -> Result<()> {
    let g = build_layered_dag(5, 15);
    let raw = engine_rows(&g, "reaches").await?;
    assert_eq!(as_set(&raw), oracle_facts(&g, "reaches"));
    assert_eq!(as_set(&raw).len(), expected_closure_size(5, 15));
    Ok(())
}

/// With `allow_partial(true)`, a tight budget returns `Ok` whose result is
/// explicitly flagged incomplete — proving the partial path is reachable and
/// labeled (the negative control for the guard above).
#[tokio::test]
async fn allow_partial_surfaces_incomplete_flag() -> Result<()> {
    let g = build_layered_dag(5, 15);
    let db = seeded_db(&g).await?;

    let result = db
        .session()
        .locy_with(&g.program_text)
        .timeout(Duration::from_nanos(1))
        .allow_partial(true)
        .run()
        .await?;

    assert!(
        result.timed_out(),
        "allow_partial result should be flagged as timed out",
    );
    let incomplete = result
        .incomplete
        .as_ref()
        .expect("incomplete diagnostics should be present");
    assert!(matches!(
        incomplete.reason,
        LocyIncompleteReason::Timeout | LocyIncompleteReason::IterationLimit
    ));
    Ok(())
}

// ── P6: stratified IS NOT complement + multi-clause unions ───────────────────

/// The stratified `IS NOT` complement matches the oracle — including shapes whose
/// closure exceeds 300, the exact regime of the historical `IS NOT` bug.
///
/// This is the track's headline: the bug returned 0 complement rows above the
/// threshold; here the engine and the naive oracle must agree on both `reaches`
/// and `unreached`, and the oracle catches any divergence at *any* scale,
/// independent of the 300-fact trigger.
#[tokio::test]
async fn engine_matches_oracle_complement() -> Result<()> {
    // (4,12) closure=864 and (5,12) closure=1440 are over the 300 threshold.
    let cases = [(3, 6), (3, 10), (4, 12), (5, 12)];
    let mut failures = Vec::new();

    for &(stages, width) in &cases {
        let g = build_complement(stages, width);
        for rel in ["reaches", "unreached"] {
            if let Some(note) = rel_divergence(&g, rel).await? {
                failures.push(format!("({stages},{width}) {note}"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "complement divergence:\n{}",
        failures.join("\n"),
    );
    Ok(())
}

/// Multi-clause union (`EDGE` ∪ reverse `EDGE2`) matches the oracle.
#[tokio::test]
async fn engine_matches_oracle_union() -> Result<()> {
    let cases = [(2, 4), (3, 6), (4, 8)];
    let mut failures = Vec::new();

    for &(stages, width) in &cases {
        let g = build_union(stages, width);
        if let Some(note) = rel_divergence(&g, "linked").await? {
            failures.push(format!("({stages},{width}) {note}"));
        }
    }

    assert!(
        failures.is_empty(),
        "union divergence:\n{}",
        failures.join("\n"),
    );
    Ok(())
}

// ── P7: high-volume soak (nightly only) ──────────────────────────────────────

/// High-volume agreement soak: thousands of generated programs, engine vs oracle.
///
/// `#[ignore]` (nightly only) and named `*_soak` so the nightly workflow's
/// `--run-ignored ignored-only -E 'test(/soak/)'` filter selects it. Volume is
/// driven by `ORACLE_SOAK_CASES` (default 10_000) — a dedicated env var, kept
/// separate from the shared `PROPTEST_CASES` so it cannot perturb other crates'
/// soak tests in the same nightly run. Bounds include over-threshold closures so
/// the dedup-strategy switch is stressed at volume.
///
/// On the volume: each case pays a full `Uni::in_memory()` instantiation
/// (~tens of ms), which dominates over graph size — so the engine-diff rate is
/// the limiter, not the oracle. 10_000 cases completes in ~10–15 min, well
/// inside the soak profile's per-test kill window; pushing toward the plan's
/// aspirational ≥100k would exceed it (and so never complete) without amortizing
/// the database build across cases or parallelizing — a deliberate follow-up.
/// The oracle's own correctness is validated at higher volume, cheaply, by the
/// closed-form unit tests in `eval.rs` (no engine in the loop).
#[test]
#[ignore = "soak: high-volume nightly run; volume set by ORACLE_SOAK_CASES"]
fn closure_is_set_soak() {
    use proptest::test_runner::{Config, TestCaseError, TestRunner};

    let cases: u32 = std::env::var("ORACLE_SOAK_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut runner = TestRunner::new(Config {
        cases,
        ..Config::default()
    });

    runner
        .run(&(2usize..5, 2usize..9), |(stages, width)| {
            let g = build_layered_dag(stages, width);
            let raw = rt
                .block_on(engine_rows(&g, "reaches"))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            let set = as_set(&raw);
            prop_assert_eq!(
                raw.len(),
                set.len(),
                "duplicate rows ({},{})",
                stages,
                width
            );
            prop_assert_eq!(
                set.len(),
                expected_closure_size(stages, width),
                "wrong size ({},{})",
                stages,
                width
            );
            prop_assert_eq!(
                set,
                oracle_facts(&g, "reaches"),
                "oracle disagreement ({},{})",
                stages,
                width
            );
            Ok(())
        })
        .expect("soak: engine diverged from oracle");
}
