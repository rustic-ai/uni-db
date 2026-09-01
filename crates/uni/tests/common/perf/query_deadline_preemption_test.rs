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
//! The discriminating signal is therefore **wall time**, not the error variant:
//! before the fix these queries also return `UniError::Timeout`, just long after
//! the budget expired. Asserting only the variant reproduces the original blind
//! spot.
//!
//! # Flakiness
//!
//! These tests assert elapsed time and are sensitive to machine load and to
//! test parallelism. The bounds are deliberately many times the expected value
//! — the defect they guard against overshoots by 20x and up, so a loose bound
//! still discriminates. If they flake, suspect the box before the code: check
//! `uptime` first.

use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::Uni;

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

/// Run the fallback under `BUDGET`, returning how long the session was actually
/// held. Asserts the query did time out, so the caller only reasons about time.
async fn held_for(db: &Uni, label: &str) -> Result<Duration> {
    let started = Instant::now();
    let res = db
        .session()
        .query_with(PER_ROW_FALLBACK)
        .timeout(BUDGET)
        .fetch_all()
        .await;
    let elapsed = started.elapsed();

    let err = res
        .err()
        .unwrap_or_else(|| panic!("{label}: the fixture is too small to outlast {BUDGET:?}"));
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "{label}: expected UniError::Timeout, got {err:?}"
    );
    Ok(elapsed)
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
    let elapsed = held_for(&db, "n=900").await?;

    // The natural runtime of this fixture is well over a second; one sub-plan
    // evaluation past a 50 ms budget is a few milliseconds. 600 ms sits far
    // above the fixed cost and far below the unpreempted time.
    assert!(
        elapsed < Duration::from_millis(600),
        "held the session for {elapsed:?} against a {BUDGET:?} budget — the \
         deadline was reported but never enforced"
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

    // Tripling the rows must not triple the overshoot. Compared as overrun past
    // the budget rather than raw elapsed, since the budget itself is a constant
    // floor that would dilute the ratio.
    let overrun = |d: Duration| d.saturating_sub(BUDGET).as_secs_f64().max(0.001);
    let growth = overrun(large) / overrun(small);
    assert!(
        growth < 3.0,
        "overrun grew {growth:.1}x when the fixture grew 3x \
         ({small:?} -> {large:?}); the deadline is not bounding per-row work"
    );
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

    let started = Instant::now();
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

    // Let execution get properly under way before pulling the plug, so this
    // cannot pass by the pre-cancelled route the other tests take.
    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();

    let res = handle.await.expect("query task panicked");
    let elapsed = started.elapsed();

    let err = res.err().expect("a cancelled query must not succeed");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got {err:?}"
    );
    assert!(
        elapsed < Duration::from_millis(900),
        "cancellation took {elapsed:?} to take effect — the token never reached \
         the per-row loop"
    );
    Ok(())
}
