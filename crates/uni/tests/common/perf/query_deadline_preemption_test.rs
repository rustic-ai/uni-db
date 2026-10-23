// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `query_timeout` must *preempt* execution, not merely report on it (#207).
//!
//! Every other timeout test in this crate uses `Duration::from_nanos(1)`, so the
//! deadline has already elapsed before execution begins and the outer
//! `tokio::time::timeout` in `impl_query.rs` catches it. Those tests stay green
//! with every operator-level `check_timeout()` deleted, which is how the
//! deadline came to be missing from `GraphExecutionContext` entirely without
//! anything going red.
//!
//! The discriminating signal is therefore **how much work ran**, not the error
//! variant: before the fix these queries also return `UniError::Timeout`, just
//! after running to completion. Asserting only the variant reproduces the
//! original blind spot.
//!
//! # On the bounds
//!
//! The bounds are on the **process CPU time** spent across the query
//! ([`super::cpu_clock`]), set roughly an order of magnitude above the observed
//! passing values and an order of magnitude below the failing ones.
//!
//! They were wall-clock ceilings, and those measured the machine as well as the
//! query: beside a full workspace run one of them held the session 602 ms
//! against a 600 ms ceiling, and the search test below took 856 ms against
//! 600 ms with 44 CPU-bound processes competing for 22 cores, both with the
//! mechanism under test intact. The deadline is wall-clock, so under contention
//! a preempted query gets *fewer* cycles before it fires and its CPU time goes
//! down, while an unpreempted one still does its whole natural runtime of work
//! (seconds): the separation a wall-clock bound loses. Wall time is still
//! reported in every failure message. The bounds rely on nextest's
//! process-per-test model; see [`super::cpu_clock`] for `cargo test`.
//!
//! Deliberately no "these may flake, check the machine" note here. An earlier
//! version of `deadline_overrun_does_not_scale_with_fixture_size` carried one,
//! and its assertion — a *ratio* of overruns past the budget — became less
//! stable the better the fix worked, since overrun tends to zero and the ratio
//! becomes noise over noise. The hedge would have taught the next reader to
//! discount a red result that was in fact reporting a badly built assertion.
//! If one of these goes red, treat it as a real signal and measure a failure
//! rate before concluding anything about the machine.

use std::time::Duration;

use anyhow::Result;
use uni_db::Uni;

use super::cpu_clock::{Spent, measure};

/// A comprehension whose pattern variables are all fresh cannot anchor, so it
/// compiles to `PatternComprehensionSubqueryExpr` — one sub-plan execution on a
/// scoped thread per outer row, none of it interruptible before #207. Shape
/// borrowed from `crates/uni/examples/pc_perrow_probe.rs`.
const PER_ROW_FALLBACK: &str = "MATCH (n:P) \
     RETURN n.idx AS i, size([(a:P)-[:KNOWS]->(b:P) WHERE a.idx > n.idx | 1]) AS s";

/// The budget under test. Short enough that it expires *during* execution
/// rather than before it — which is the whole difference from the existing
/// `from_nanos(1)` tests.
const BUDGET: Duration = Duration::from_millis(50);

async fn build_chain(n: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (idx INT)").await?;
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P").await?;
    for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:P {{idx:{i}}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await?;
    }
    tx.execute("MATCH (a:P), (b:P) WHERE b.idx = a.idx + 1 CREATE (a)-[:KNOWS]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// Run the fallback under `BUDGET`, returning what the query spent. Asserts
/// the query did time out, so the caller only reasons about cost.
async fn held_for(db: &Uni, label: &str) -> Result<Spent> {
    let (res, spent) = measure(
        db.session()
            .query_with(PER_ROW_FALLBACK)
            .timeout(BUDGET)
            .fetch_all(),
    )
    .await;

    let err = res
        .err()
        .unwrap_or_else(|| panic!("{label}: the fixture is too small to outlast {BUDGET:?}"));
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "{label}: expected UniError::Timeout, got {err:?}"
    );
    eprintln!("{label}: cpu {:?}, wall {:?}", spent.cpu, spent.wall);
    Ok(spent)
}

/// The session must be released within a bounded overrun of the deadline.
///
/// Before #207 this query held the session for its full natural runtime — the
/// per-row sub-plan loop runs inside one `poll_next`, so neither the outer
/// `tokio::time::timeout` nor the cancellation race can interrupt it, and the
/// deadline is only *observed* once the work finishes.
#[tokio::test]
async fn a_mid_execution_deadline_releases_the_session_promptly() -> Result<()> {
    let db = build_chain(900).await?;
    let spent = held_for(&db, "n=900").await?;

    // The natural runtime of this fixture is seconds of work; one sub-plan
    // evaluation past a 50 ms budget is a few milliseconds. 600 ms of CPU sits
    // far above the fixed cost and far below the unpreempted work.
    assert!(
        spent.cpu < Duration::from_millis(600),
        "the query used {:?} of CPU ({:?} wall) against a {BUDGET:?} budget — \
         the deadline was reported but never enforced",
        spent.cpu,
        spent.wall
    );
    Ok(())
}

/// Overrun must not grow with the data.
///
/// The scaling is the clearest statement of the defect: #207 measured 1.4x
/// overrun at N=400 and 29x at N=2000 for the same budget. Once the deadline is
/// checked between rows the overrun is one sub-plan evaluation regardless of how
/// many rows remain, so the two sizes cost the same.
#[tokio::test]
async fn deadline_overrun_does_not_scale_with_fixture_size() -> Result<()> {
    let small = held_for(&build_chain(300).await?, "n=300").await?;
    let large = held_for(&build_chain(900).await?, "n=900").await?;

    // Both sizes must clear the *same* fixed ceiling. That is the non-scaling
    // claim, and it is the stable way to state it.
    //
    // This asserted a ratio of overruns past the budget, and that metric defeats
    // itself: as the fix works, overrun tends to zero and the ratio becomes
    // noise over noise. It failed a full-suite run at
    // `55.26ms -> 86.27ms` — a 6.9x "growth" between two timings that are both
    // excellent — while the absolute numbers were an order of magnitude inside
    // any sane bound. A test whose flakiness grows as the defect shrinks is
    // measuring the wrong thing.
    //
    // The margin here is wide in both directions: unpreempted, these fixtures
    // hold the session 837ms and 7.01s (measured against a neutralised fix), so
    // the ceiling fails hard when the defect is present and passes with roughly
    // a 10x cushion when it is not.
    const CEILING: Duration = Duration::from_millis(500);
    for (label, held) in [("n=300", small), ("n=900", large)] {
        assert!(
            held.cpu < CEILING,
            "{label}: used {:?} of CPU ({:?} wall) against a {BUDGET:?} budget \
             (ceiling {CEILING:?}); per-row work is not being bounded",
            held.cpu,
            held.wall
        );
    }
    Ok(())
}

/// `Session::cancel()` must interrupt a query that is already running.
///
/// The existing cancellation tests all pre-cancel the token, so they are
/// satisfied by the outer `CancelScope` race and never exercise a cooperative
/// checkpoint. `test_concurrent_query_cancellation_isolation` documents the gap
/// from the other side: it explicitly accepts a cancelled query "racing to
/// completion" as valid.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_running_query_interrupts_it() -> Result<()> {
    let db = build_chain(900).await?;
    let token = tokio_util::sync::CancellationToken::new();

    let (res, spent) = measure(async {
        let handle = {
            let token = token.clone();
            let session = db.session();
            tokio::spawn(async move {
                session
                    .query_with(PER_ROW_FALLBACK)
                    // No timeout: cancellation alone must stop this.
                    .cancellation_token(token)
                    .fetch_all()
                    .await
            })
        };

        // Let execution get properly under way before pulling the plug, so
        // this cannot pass by the pre-cancelled route the other tests take.
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
        handle.await.expect("query task panicked")
    })
    .await;
    eprintln!("cancel n=900: cpu {:?}, wall {:?}", spent.cpu, spent.wall);

    let err = res.err().expect("a cancelled query must not succeed");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got {err:?}"
    );
    assert!(
        spent.cpu < Duration::from_millis(900),
        "the query used {:?} of CPU ({:?} wall) after a cancel at 50 ms — the \
         token never reached the per-row loop",
        spent.cpu,
        spent.wall
    );
    Ok(())
}

/// Work that never pauses: nested `UNWIND`s counted by an aggregate, which
/// drains its whole input inside one poll. About 4.5 s unbounded in a debug
/// build.
const UNYIELDING: &str = "UNWIND range(1, 5000) AS a UNWIND range(1, 5000) AS b \
     WITH count(*) AS c RETURN c";

/// CPU-time ceiling for the tests below: an order of magnitude above what they
/// use with the budget enforced, and well below the unbounded runtime.
const UNYIELDING_CEILING: Duration = Duration::from_millis(600);

/// A database with `test.unyielding()` declared, whose body is [`UNYIELDING`].
async fn with_unyielding_procedure() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CALL uni.plugin.declareProcedure('test.unyielding', '{UNYIELDING}', 'READ', '[]', '[]')"
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// Run `query` against [`BUDGET`] and return what it spent.
async fn timed_out_after(db: &Uni, query: &str) -> Result<Spent> {
    let (res, spent) = measure(db.session().query_with(query).timeout(BUDGET).fetch_all()).await;
    let err = res
        .err()
        .expect("the query must not finish within its budget");
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "expected UniError::Timeout, got {err:?}"
    );
    eprintln!("{query}: cpu {:?}, wall {:?}", spent.cpu, spent.wall);
    Ok(spent)
}

/// UNWIND makes its rows without pulling input for each one, so under an
/// aggregate the whole expansion runs inside one poll. No `DeadlineGuardExec`
/// sits beneath the aggregate either, because the subtree holds an operator
/// of ours. Only UNWIND itself can stop it.
#[tokio::test]
async fn an_unwind_under_an_aggregate_stops_at_the_deadline() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let spent = timed_out_after(&db, UNYIELDING).await?;
    assert!(
        spent.cpu < UNYIELDING_CEILING,
        "used {:?} of CPU ({:?} wall) against a {BUDGET:?} budget",
        spent.cpu,
        spent.wall
    );
    Ok(())
}

/// A declared procedure's body runs as an inner query, inside the caller's poll
/// and under `block_in_place`, so neither the caller's `tokio::time::timeout`
/// nor its cancellation race can interrupt it. Its own checks must spend the
/// caller's budget: the inner executor used to start a fresh
/// `query_timeout` of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_procedure_body_stops_at_the_callers_deadline() -> Result<()> {
    let db = with_unyielding_procedure().await?;
    let spent = timed_out_after(&db, "CALL test.unyielding()").await?;
    assert!(
        spent.cpu < UNYIELDING_CEILING,
        "the procedure body used {:?} of CPU ({:?} wall) against a {BUDGET:?} budget",
        spent.cpu,
        spent.wall
    );
    Ok(())
}

/// As above, for cancellation: the inner query used to run without the
/// caller's token, so a cancelled call finished its body and then returned
/// success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_declared_procedure_interrupts_its_body() -> Result<()> {
    let db = with_unyielding_procedure().await?;
    let token = tokio_util::sync::CancellationToken::new();

    let (res, spent) = measure(async {
        let handle = {
            let token = token.clone();
            let session = db.session();
            tokio::spawn(async move {
                session
                    .query_with("CALL test.unyielding()")
                    .cancellation_token(token)
                    .fetch_all()
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
        handle.await.expect("query task panicked")
    })
    .await;
    eprintln!(
        "cancel procedure: cpu {:?}, wall {:?}",
        spent.cpu, spent.wall
    );
    let err = res.err().expect("a cancelled call must not succeed");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got {err:?}"
    );
    assert!(
        spent.cpu < UNYIELDING_CEILING,
        "the call used {:?} of CPU ({:?} wall) after a cancel at 50 ms — the \
         cancellation never reached the procedure body",
        spent.cpu,
        spent.wall
    );
    Ok(())
}

/// A Cypher graph projection runs `nodeQuery` as an inner query too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cypher_graph_projection_stops_at_the_callers_deadline() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let query = format!(
        "CALL uni.algo.pageRank({{nodeQuery: '{UNYIELDING} AS id', \
         edgeQuery: 'MATCH (a)-[r]->(b) RETURN id(a) AS source, id(b) AS target'}}, {{}}) \
         YIELD nodeId, score RETURN nodeId, score"
    );
    let spent = timed_out_after(&db, &query).await?;
    assert!(
        spent.cpu < UNYIELDING_CEILING,
        "the projection used {:?} of CPU ({:?} wall) against a {BUDGET:?} budget",
        spent.cpu,
        spent.wall
    );
    Ok(())
}

/// A vector or full-text search over unflushed rows is linear in them.
///
/// Both merged each unflushed candidate into the results by a linear search of
/// a list that grew with every append, so 40,000 rows took 2.2 s in a debug
/// build, against about 0.1 s now. The ceiling, on CPU time, sits between the
/// two; as a wall-clock ceiling it failed at 856 ms beside 44 CPU-bound
/// competitors with the search still linear.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_search_over_unflushed_rows_is_linear_in_them() -> Result<()> {
    const ROWS: usize = 40_000;
    const CEILING: Duration = Duration::from_millis(600);
    // Keep every row in L0: a flush would move them to Lance and off this path.
    let db = Uni::in_memory()
        .config(uni_common::config::UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(1, {ROWS}) AS i \
         CREATE (:Doc {{emb: [toFloat(i), 1.0, 2.0, 3.0], body: 'alpha ' + toString(i)}})"
    ))
    .await?;
    tx.commit().await?;

    for (name, query) in [
        (
            "vector",
            "CALL uni.vector.query('Doc', 'emb', [1.0, 1.0, 2.0, 3.0], 10) \
             YIELD node RETURN count(*) AS c",
        ),
        (
            "fts",
            "CALL uni.fts.query('Doc', 'body', 'alpha', 10) YIELD node RETURN count(*) AS c",
        ),
    ] {
        let (rows, spent) = measure(db.session().query(query)).await;
        let rows = rows?;
        eprintln!("{name}: cpu {:?}, wall {:?}", spent.cpu, spent.wall);
        assert_eq!(rows.rows()[0].get::<i64>("c")?, 10, "{name}");
        assert!(
            spent.cpu < CEILING,
            "{name} search over {ROWS} unflushed rows used {:?} of CPU ({:?} wall)",
            spent.cpu,
            spent.wall
        );
    }
    Ok(())
}
