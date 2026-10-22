// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A program's budget covers its re-derivations, and a cut-short evaluation
//! is reported, never silent.
//!
//! - Every re-derivation (the facts derived again after a write, an ASSUME
//!   body's hypothetical facts) got the program's whole `timeout` afresh, so
//!   a program of several writes ran several timeouts long and returned `Ok`.
//!   They now share what remains of the program's budget.
//! - Under `allow_partial`, a re-derivation stopped by `max_iterations`
//!   returned its partial facts with `incomplete` unset, at the top level and
//!   in an ASSUME body: a short answer reported as complete. It now reports
//!   `incomplete`, naming the rule. Without `allow_partial` it is the typed
//!   `LocyIncomplete` the program's own run raises, not a string.
//! - Under `allow_partial`, an operator's deadline (rather than the check
//!   between strata or iterations) raised a hard error; it now reports
//!   `incomplete` as the other checks do.
//!
//! Timing cases compare against a duration measured in the same test, with
//! wide margins, rather than fixed thresholds.

// Rust guideline compliant

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use uni_db::locy::CommandResult;
use uni_db::{LocyIncompleteReason, Uni, UniError};

/// `n` items in five categories.
async fn items(n: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:Item {{name: 'item_' + toString(i), category: i % 5}})",
        n - 1
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// A non-recursive cross join of the items in the same category.
const PAIRS: &str = "CREATE RULE pairs AS MATCH (a:Item), (b:Item) \
    WHERE a.category = b.category AND a.name <> b.name YIELD KEY a, KEY b\n";

/// A write `pairs` reads: one more item, so the facts are derived again.
fn add_item(i: usize) -> String {
    format!("CREATE (:Item {{name: 'extra_{i}', category: 0}})")
}

/// The fastest of three runs of `program`, unbounded.
async fn fastest(db: &Uni, program: &str) -> Result<Duration> {
    let mut best = Duration::MAX;
    for _ in 0..3 {
        let started = Instant::now();
        db.session().locy(program).await?;
        best = best.min(started.elapsed());
    }
    Ok(best)
}

/// Eight writes, each followed by a QUERY that derives `pairs` again: about
/// nine derivations' work. With a timeout of three derivations, the program
/// runs out of budget part way, at the top level and in an ASSUME body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rederivations_share_the_programs_timeout() -> Result<()> {
    let db = items(300).await?;
    let one = fastest(&db, &format!("{PAIRS}QUERY pairs RETURN count(*) AS c")).await?;
    let timeout = one * 3;
    let rounds: String = (0..8)
        .map(|i| format!("{}\nQUERY pairs RETURN count(*) AS c\n", add_item(i)))
        .collect();
    for (what, program) in [
        ("top level", format!("{PAIRS}{rounds}")),
        (
            "ASSUME body",
            format!(
                "{PAIRS}ASSUME {{ CREATE (:Item {{name: 'h', category: 1}}) }} THEN {{ {rounds} }}"
            ),
        ),
    ] {
        let started = Instant::now();
        let result = db
            .session()
            .locy_with(&program)
            .timeout(timeout)
            .run()
            .await;
        let wall = started.elapsed();
        match result {
            Err(UniError::LocyIncomplete { detail }) => {
                assert_eq!(detail.reason, LocyIncompleteReason::Timeout, "{what}");
            }
            Err(UniError::Timeout { .. }) => {}
            other => bail!(
                "{what}: one derivation {one:?}, timeout {timeout:?}, ran {wall:?}: expected a timeout, got {other:?}"
            ),
        }
        // Allow_partial reports the same cut instead.
        let partial = db
            .session()
            .locy_with(&program)
            .timeout(timeout)
            .allow_partial(true)
            .run()
            .await?;
        let detail = partial
            .incomplete
            .as_ref()
            .unwrap_or_else(|| panic!("{what}: allow_partial must report the cut"));
        assert_eq!(detail.reason, LocyIncompleteReason::Timeout, "{what}");
    }
    Ok(())
}

/// A chain `c0 -> c1 -> ... -> c239` missing the edge `c59 -> c60`; `reach`
/// walks it from `c0`, one node per iteration.
async fn chain() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(0, 239) AS i CREATE (:C {i: i})")
        .await?;
    tx.execute("MATCH (a:C), (b:C) WHERE b.i = a.i + 1 AND a.i <> 59 CREATE (a)-[:NEXT]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

const REACH: &str = "\
CREATE RULE reach AS MATCH (a:C)-[:NEXT]->(b:C) WHERE a.i = 0 YIELD KEY b\n\
CREATE RULE reach AS MATCH (m:C)-[:NEXT]->(b:C) WHERE m IS reach YIELD KEY b\n";

const JOIN: &str = "MATCH (a:C {i: 59}), (b:C {i: 60}) CREATE (a)-[:NEXT]->(b)";

const Q_REACH: &str = "QUERY reach RETURN b.i AS i";

/// Joining the chain makes `reach` need 239 iterations; with
/// `max_iterations = 100` the re-derivation after the write is cut short.
/// Without `allow_partial` that is the typed `LocyIncomplete`; with it, the
/// QUERY answers from the partial facts and `incomplete` names `reach`.
#[tokio::test]
async fn a_rederivation_cut_by_max_iterations_is_reported() -> Result<()> {
    let db = chain().await?;
    // The control: before the write, 59 nodes, well inside the limit.
    let control = db
        .session()
        .locy_with(&format!("{REACH}{Q_REACH}"))
        .max_iterations(100)
        .run()
        .await?;
    assert!(control.incomplete.is_none());
    match control.command_results.as_slice() {
        [CommandResult::Query(rows)] => assert_eq!(rows.len(), 59),
        other => bail!("unexpected {other:?}"),
    }

    for (what, program) in [
        ("top level", format!("{REACH}{JOIN}\n{Q_REACH}")),
        (
            "ASSUME body",
            format!("{REACH}ASSUME {{ {JOIN} }} THEN {{ {Q_REACH} }}"),
        ),
    ] {
        let err = db
            .session()
            .locy_with(&program)
            .max_iterations(100)
            .run()
            .await;
        match err {
            Err(UniError::LocyIncomplete { detail }) => {
                assert_eq!(
                    detail.reason,
                    LocyIncompleteReason::IterationLimit,
                    "{what}"
                );
                assert!(
                    detail.incomplete_rules.iter().any(|r| r == "reach"),
                    "{what}: {detail:?}"
                );
            }
            other => bail!("{what}: expected LocyIncomplete, got {other:?}"),
        }
        let partial = db
            .session()
            .locy_with(&program)
            .max_iterations(100)
            .allow_partial(true)
            .run()
            .await?;
        let detail = partial
            .incomplete
            .as_ref()
            .unwrap_or_else(|| panic!("{what}: a cut-short re-derivation must be reported"));
        assert_eq!(
            detail.reason,
            LocyIncompleteReason::IterationLimit,
            "{what}"
        );
        assert!(
            detail.incomplete_rules.iter().any(|r| r == "reach"),
            "{what}: {detail:?}"
        );
    }
    Ok(())
}

/// One non-recursive stratum over 600 items: about 72k facts. Most of an
/// unbounded run is spent outside every operator: deduplicating the facts,
/// converting and hydrating them, and answering the QUERY. With a fifth of
/// that as the timeout the program fails near its deadline instead of
/// running to completion. Under `allow_partial` it reports `incomplete`
/// instead of raising and answers no command past the deadline; the facts
/// it derived are still converted and hydrated to be returned, which is
/// not bounded by the deadline, so its time is not asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operator_deadline_is_enforced_and_reported_under_allow_partial() -> Result<()> {
    let db = items(600).await?;
    let program = format!("{PAIRS}QUERY pairs RETURN count(*) AS c");
    let one = fastest(&db, &program).await?;
    let timeout = one / 5;
    for allow_partial in [false, true] {
        let started = Instant::now();
        let result = db
            .session()
            .locy_with(&program)
            .timeout(timeout)
            .allow_partial(allow_partial)
            .run()
            .await;
        let wall = started.elapsed();
        assert!(
            allow_partial || wall < one / 2,
            "one run {one:?}, timeout {timeout:?}, stopped after {wall:?}"
        );
        match (allow_partial, result) {
            (false, Err(UniError::Timeout { .. } | UniError::LocyIncomplete { .. })) => {}
            (true, Ok(r)) => {
                let detail = r.incomplete.as_ref().expect("the cut is reported");
                assert_eq!(detail.reason, LocyIncompleteReason::Timeout);
                assert!(
                    r.command_results.is_empty(),
                    "the QUERY is not answered past the deadline: {:?}",
                    r.command_results
                );
            }
            (_, other) => bail!("allow_partial={allow_partial}: unexpected {other:?}"),
        }
    }
    Ok(())
}

/// The whole chain joined: `reach` walks 239 nodes, one per iteration. With
/// a fifth of an unbounded run as the timeout, the cut lands inside the
/// fixpoint: in an operator's deadline check, or in the check between
/// iterations, whichever notices first. Either way, under `allow_partial`
/// the cut is reported, not raised (an operator's deadline used to raise
/// `Timeout` there).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_inside_the_fixpoint_is_reported_under_allow_partial() -> Result<()> {
    let db = chain().await?;
    let tx = db.session().tx().await?;
    tx.execute(JOIN).await?;
    tx.commit().await?;
    let program = format!("{REACH}{Q_REACH}");
    let one = fastest(&db, &program).await?;
    let timeout = one / 5;
    for _ in 0..3 {
        let partial = db
            .session()
            .locy_with(&program)
            .timeout(timeout)
            .allow_partial(true)
            .run()
            .await;
        match partial {
            Ok(r) => {
                let detail = r.incomplete.as_ref().unwrap_or_else(|| {
                    panic!("one run {one:?}, timeout {timeout:?}: no cut reported")
                });
                assert_eq!(detail.reason, LocyIncompleteReason::Timeout);
                assert!(
                    detail.incomplete_rules.iter().any(|r| r == "reach"),
                    "{detail:?}"
                );
            }
            Err(e) => bail!("one run {one:?}, timeout {timeout:?}: allow_partial raised {e:?}"),
        }
        let strict = db
            .session()
            .locy_with(&program)
            .timeout(timeout)
            .run()
            .await;
        assert!(
            matches!(
                strict,
                Err(UniError::Timeout { .. } | UniError::LocyIncomplete { .. })
            ),
            "without allow_partial: {strict:?}"
        );
    }
    Ok(())
}
