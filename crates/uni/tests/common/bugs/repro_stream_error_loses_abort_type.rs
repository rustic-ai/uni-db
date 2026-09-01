// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `into_stream_error` (crates/uni/src/api/impl_query.rs).
//!
//! The cursor path maps executor errors with `into_stream_error`, which was the
//! `into_execution_error` classification *minus* its cancellation and timeout
//! arms. The omission was documented and, at the time, correct: the cursor's own
//! guard in `build_guarded_cursor` raises both around the stream and raises them
//! already typed, so nothing else could produce one.
//!
//! That reasoning depended on a fact stated nowhere near it —
//! `GraphExecutionContext` carried no deadline and no cancellation token, so no
//! operator-level `check_timeout` could fire. The moment #207 plumbed them
//! through, an operator aborting *inside* a `poll` surfaced its error through
//! the stream instead of the guard, fell to the catch-all arm, and a genuine
//! abort was reported as `UniError::Query { message: "Query timed out" }`.
//!
//! That is not cosmetic. The bindings map `UniError::Timeout` to
//! `UniTimeoutError` and `UniError::Cancelled` to `UniCancelledError`; a caller
//! catching the documented exception would have missed both and seen an opaque
//! query failure instead.
//!
//! FIXED: `into_stream_error` now carries the same cancellation and timeout arms
//! as `into_execution_error`, and takes the effective `query_timeout` so the
//! typed error can report the budget it blew.
//!
//! The two tests below split by which arm they reach:
//!
//! * the timeout arm is reachable with an already-elapsed budget — the first
//!   operator checkpoint fires before the outer `timeout_at` gets a turn;
//! * the cancellation arm needs the token to flip *mid-flight*. A pre-cancelled
//!   token is taken by the `biased` `cancel.cancelled()` branch of the guard's
//!   `select!` before the stream is ever polled, which is why every existing
//!   cancellation test passes without exercising this path at all.

use std::time::Duration;

use anyhow::Result;
use uni_db::{Uni, UniError};

/// Enough rows that execution is still in progress when the token flips.
const ROWS: usize = 900;

/// Fresh pattern variables prevent anchoring, so this takes the per-row
/// subquery fallback: one blocking sub-plan execution per outer row, all inside
/// a single `poll_next`. Slow by construction, which is what gives the token a
/// poll to flip during.
const SLOW_UNANCHORED: &str = "MATCH (n:Node) \
     RETURN n.idx AS i, size([(a:Node)-[:LINK]->(b:Node) WHERE a.idx > n.idx | 1]) AS s";

async fn fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Node (idx INT)").await?;
    tx.execute("CREATE EDGE TYPE LINK FROM Node TO Node")
        .await?;
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(300) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:Node {{idx:{i}}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await?;
    }
    tx.execute("MATCH (a:Node), (b:Node) WHERE b.idx = a.idx + 1 CREATE (a)-[:LINK]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// Drain a cursor to exhaustion, returning the first streamed error.
async fn drain(mut cursor: uni_query::QueryCursor) -> Option<UniError> {
    while let Some(batch) = cursor.next_batch().await {
        if let Err(e) = batch {
            return Some(e);
        }
    }
    None
}

/// An operator-raised timeout must keep the typed variant on the cursor path.
#[tokio::test]
async fn an_operator_raised_timeout_on_a_cursor_stays_typed() -> Result<()> {
    let db = fixture().await?;

    let cursor = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .cursor()
        .await?;

    let err = drain(cursor).await.expect("an elapsed budget must reject");
    assert!(
        matches!(err, UniError::Timeout { .. }),
        "an abort raised inside an operator was reported as {err:?} rather than \
         UniError::Timeout — `into_stream_error` has lost the timeout arm"
    );
    Ok(())
}

/// An operator-raised cancellation must keep the typed variant on the cursor
/// path.
///
/// Reaching the operator arm at all takes care, and the obvious way to write
/// this test does not do it. Cancelling before the drain leaves
/// `cancel.cancelled()` already ready when the guard's `biased` `select!` first
/// polls, so the guard wins and raises its own typed error — the test then
/// passes with the arm under test deleted. Verified: written that way it stayed
/// green against a stripped `into_stream_error`.
///
/// What discriminates is flipping the token *during* a poll. The query below
/// takes the per-row pattern-comprehension fallback, which blocks on a scoped
/// thread inside one `poll_next`; the guard's `select!` is parked inside that
/// poll and cannot react to the token either. So the operator's own checkpoint
/// observes the cancellation first, the stream resolves with its error, and
/// that branch of the `select!` completes — landing in `into_stream_error`.
#[tokio::test(flavor = "multi_thread")]
async fn an_operator_raised_cancellation_on_a_cursor_stays_typed() -> Result<()> {
    let db = fixture().await?;
    let token = tokio_util::sync::CancellationToken::new();

    let cursor = db
        .session()
        .query_with(SLOW_UNANCHORED)
        .cancellation_token(token.clone())
        .cursor()
        .await?;

    let drainer = tokio::spawn(drain(cursor));
    // Long enough that execution is inside the blocking poll, short enough that
    // it has not finished. The fixture takes far longer than this to complete.
    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();

    let err = drainer
        .await
        .expect("drain task panicked")
        .expect("a cancelled cursor must reject");
    assert!(
        matches!(err, UniError::Cancelled),
        "an abort raised inside an operator was reported as {err:?} rather than \
         UniError::Cancelled — `into_stream_error` has lost the cancellation arm"
    );
    Ok(())
}
