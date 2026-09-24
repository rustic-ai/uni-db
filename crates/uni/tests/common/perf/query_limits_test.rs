// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::time::{Duration, Instant};
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_query_timeout() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("Node").apply().await?;

    // Create some data
    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;

    // This query should be very fast, but let's set an extremely short timeout
    let res = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .fetch_all()
        .await;

    // The typed variant, not a stringly-typed `Query`: an elapsed deadline is
    // exactly the case a dedicated error class exists for, and Python maps it
    // to `UniTimeoutError`.
    let err = res.err().expect("a 1ns timeout must reject");
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "expected UniError::Timeout, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_query_memory_limit() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("Node").apply().await?;

    // Create some data
    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;

    // Set an extremely small memory limit
    let res = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .max_memory(100) // 100 bytes
        .fetch_all()
        .await;

    assert!(res.is_err());
    let err_msg = res.err().unwrap().to_string();
    // This used to assert the post-hoc message, "Query exceeded memory limit",
    // which is produced *after* the rows are materialized. `GraphScanExec` now
    // reserves the batch it builds, so the pool refuses first and names the
    // operator that asked (#242). The rejection is the same; the mechanism moved
    // earlier, which is the point of the change, so the assertion follows it
    // rather than being relaxed to accept either.
    assert!(
        err_msg.contains("GraphScanExec"),
        "expected the scan's own reservation to refuse: {err_msg}"
    );

    Ok(())
}

/// The transaction Cypher builder honours `.max_memory()`.
///
/// It was the only one of {session, tx} x {Cypher, Locy} without the knob,
/// which left the shape that needs a ceiling most -- a long-running read inside
/// a write transaction -- with no way to set one. The assertion follows
/// `test_query_memory_limit` in requiring the operator's own reservation to
/// refuse, rather than accepting the post-hoc materialized-result message: a
/// ceiling that only bites after the rows exist is a report, not a limit.
#[tokio::test]
async fn test_tx_query_builder_honours_max_memory() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Node").apply().await?;

    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;

    let session = db.session();
    let tx = session.tx().await?;
    let res = tx
        .query_with("MATCH (n:Node) RETURN n")
        .max_memory(100)
        .fetch_all()
        .await;

    assert!(res.is_err(), "a 100-byte ceiling must refuse this query");
    let err_msg = res.err().unwrap().to_string();
    assert!(
        err_msg.contains("GraphScanExec"),
        "expected the scan's own reservation to refuse: {err_msg}"
    );

    Ok(())
}

/// Control for the above: the same query under a workable ceiling must succeed.
///
/// Without it, a build that refused every transaction read would satisfy the
/// test above.
#[tokio::test]
async fn test_tx_query_builder_max_memory_leaves_a_fitting_query_alone() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Node").apply().await?;

    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;

    let session = db.session();
    let tx = session.tx().await?;
    let rows = tx
        .query_with("MATCH (n:Node) RETURN n")
        .max_memory(256 * 1024 * 1024)
        .fetch_all()
        .await?;

    assert_eq!(rows.rows().len(), 100);
    Ok(())
}

/// `TxQueryBuilder::profile()` -- the read-path counterpart of
/// `ExecuteBuilder::profile()`, which only ever covered writes.
#[tokio::test]
async fn test_tx_query_builder_profiles_a_read() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Node").apply().await?;

    let tx = db.session().tx().await?;
    for _ in 0..10 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;

    let session = db.session();
    let tx = session.tx().await?;
    let (result, profile) = tx
        .query_with("MATCH (n:Node) RETURN count(n) AS c")
        .profile()
        .await?;

    let count: i64 = result.rows()[0].get("c")?;
    assert_eq!(count, 10);
    assert!(
        !profile.runtime_stats.is_empty(),
        "a profile with no operator stats is not a profile"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Cursor parity — the streaming path must enforce the same limits
// ---------------------------------------------------------------------------
//
// `QueryBuilder::cursor` advertises `.timeout()`, `.max_memory()` and
// `.cancellation_token()`, but enforcement lived entirely in the materializing
// path: `execute_plan_internal` wraps execution in `tokio::time::timeout` and
// calls `enforce_memory_limit`, while `execute_cursor_internal_with_config`
// did neither and never received the token at all. Every limit the builder
// accepted was silently inert once `.cursor()` was the terminal.
//
// The cooperative `GraphContext::check_timeout` is not a substitute: it only
// fires where an operator happens to call it, and no scan/join/traverse plan
// exercised here reaches one. `test_concurrent_query_cancellation_isolation`
// documents the same weakness from the other side — it accepts a cancelled
// query "racing to completion" as a valid outcome.

/// Drain a cursor to exhaustion, returning the first streamed error.
async fn drain_cursor(mut cursor: uni_query::QueryCursor) -> Option<uni_db::UniError> {
    while let Some(batch) = cursor.next_batch().await {
        if let Err(e) = batch {
            return Some(e);
        }
    }
    None
}

async fn seeded_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Node").apply().await?;
    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn test_query_memory_limit_applies_to_cursor() -> Result<()> {
    let db = seeded_db().await?;

    // Identical query and limit to `test_query_memory_limit`, which rejects.
    let cursor = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .max_memory(100)
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "cursor streamed every row under a 100-byte ceiling; `fetch_all` \
         rejects the same query with the same limit",
    );
    // Parity is what this test is for, so it follows `fetch_all` to the pool's
    // message rather than staying on the post-hoc one (#242). Both terminals
    // must be refused by the same mechanism, or the streaming path would once
    // again be bounded differently from the materializing one.
    assert!(
        err.to_string().contains("GraphScanExec"),
        "expected the same reservation refusal `fetch_all` produces, got: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_query_timeout_applies_to_cursor() -> Result<()> {
    let db = seeded_db().await?;

    // Identical query and timeout to `test_query_timeout`, which rejects.
    let cursor = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "cursor ran to completion under a 1ns timeout; `fetch_all` rejects \
         the same query with the same timeout",
    );
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "expected the timeout error `fetch_all` produces, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_cancellation_token_aborts_a_cursor() -> Result<()> {
    let db = seeded_db().await?;

    // Pre-cancelled: the outcome must be deterministic, not a race.
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let cursor = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .cancellation_token(token)
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "cursor streamed every row despite an already-cancelled token; \
         `QueryBuilder::cursor` never read `self.cancellation_token`",
    );
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_cursor_tolerates_polling_past_exhaustion() -> Result<()> {
    // The limit guard wraps the row stream in `stream::unfold`, which panics
    // outright if polled after it has yielded `None`. Two supported call
    // patterns do exactly that, and neither is exotic:
    //
    //   * an empty result set — the very first poll is also the last;
    //   * `fetch_one()` on a drained cursor, which polls again to confirm
    //     exhaustion and is how Python's cursor reports "no more rows".
    //
    // The pre-guard `map`/`flat_map` chain tolerated both, so the guard has to
    // be `.fuse()`d. Without it the panic crosses the pyo3 boundary as a hard
    // abort rather than a Python exception.
    let db = seeded_db().await?;

    // Empty result: exhausted immediately, then polled once more.
    let mut empty = db
        .session()
        .query_with("MATCH (n:Node) WHERE n.missing = 'nope' RETURN n")
        .cursor()
        .await?;
    while let Some(batch) = empty.next_batch().await {
        batch?;
    }
    assert!(
        empty.next_batch().await.is_none(),
        "re-polling an exhausted empty cursor must stay None"
    );

    // Non-empty result, drained and then over-polled twice.
    let mut full = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .cursor()
        .await?;
    let mut seen = 0usize;
    while let Some(batch) = full.next_batch().await {
        seen += batch?.len();
    }
    assert_eq!(seen, 100, "cursor must stream every seeded row");
    assert!(full.next_batch().await.is_none());
    assert!(full.next_batch().await.is_none());

    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction cursor — the same limits, on the other surface
// ---------------------------------------------------------------------------
//
// `TxQueryBuilder` accepts `.timeout()` and `.cancellation_token()`, and its
// `execute`/`fetch_all` terminals wrap the future in `tokio::time::timeout`.
// `cursor_inner` passed neither down, so a transaction cursor ran unbounded —
// the same defect the session cursor had, in the copy of the cursor-building
// code that lives next to it.
//
// Both surfaces now render an elapsed deadline as `UniError::Timeout`. They
// used to disagree — the session produced `Query { "Query timed out" }` — so
// the same condition surfaced as two different classes depending on which
// terminal the caller reached for.

async fn seeded_db_with_config(config: uni_db::UniConfig) -> Result<Uni> {
    let db = Uni::in_memory().config(config).build().await?;
    db.schema().label("Node").apply().await?;
    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn test_tx_cursor_honours_builder_timeout() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let cursor = tx
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "transaction cursor ran to completion under a 1ns timeout; the same \
         builder's `fetch_all` honours it",
    );
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "expected UniError::Timeout, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_cursor_honours_cancellation_token() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let cursor = tx
        .query_with("MATCH (n:Node) RETURN n")
        .cancellation_token(token)
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "transaction cursor streamed every row despite an already-cancelled \
         token; `cursor_inner` never read it",
    );
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_cursor_enforces_configured_memory_limit() -> Result<()> {
    // The ceiling here comes from `UniConfig` rather than the builder. That is
    // now a choice rather than the only option -- `TxQueryBuilder` has gained
    // `.max_memory()` -- and the config route is worth keeping covered, since
    // it is what a caller who never touches the builder relies on.
    let mut config = uni_db::UniConfig::default();
    config.max_query_memory = 100;
    let db = seeded_db_with_config(config).await?;
    let session = db.session();
    let tx = session.tx().await?;

    let cursor = tx.query_with("MATCH (n:Node) RETURN n").cursor().await?;

    let err = drain_cursor(cursor)
        .await
        .expect("transaction cursor ignored the configured memory ceiling");
    // Follows the mechanism, not the message: `GraphScanExec` now reserves the
    // batch it builds, so the pool refuses before the rows exist rather than the
    // post-hoc check measuring them afterwards (#242). Both terminals moved
    // together, which is what this pair exists to check -- an asymmetry here
    // would mean one of them is still bounded only after the fact.
    assert!(
        err.to_string().contains("GraphScanExec"),
        "expected the scan's own reservation to refuse, got: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_fetch_all_enforces_configured_memory_limit() -> Result<()> {
    // Guards against fixing only the cursor: if the ceiling applied to the
    // streaming terminal but not the materializing one, the tx surface would
    // gain exactly the asymmetry this work exists to remove.
    let mut config = uni_db::UniConfig::default();
    config.max_query_memory = 100;
    let db = seeded_db_with_config(config).await?;
    let session = db.session();
    let tx = session.tx().await?;

    let res = tx.query_with("MATCH (n:Node) RETURN n").fetch_all().await;

    let err = res
        .err()
        .expect("transaction fetch_all ignored the configured memory ceiling")
        .to_string();
    // Follows the mechanism, not the message: `GraphScanExec` now reserves the
    // batch it builds, so the pool refuses before the rows exist rather than the
    // post-hoc check measuring them afterwards (#242). Both terminals moved
    // together, which is what this pair exists to check -- an asymmetry here
    // would mean one of them is still bounded only after the fact.
    assert!(
        err.to_string().contains("GraphScanExec"),
        "expected the scan's own reservation to refuse, got: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_cursor_streams_normally_without_limits() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let mut cursor = tx.query_with("MATCH (n:Node) RETURN n").cursor().await?;
    let mut seen = 0usize;
    while let Some(batch) = cursor.next_batch().await {
        seen += batch?.len();
    }
    assert_eq!(
        seen, 100,
        "an unconstrained tx cursor must stream every row"
    );
    assert!(cursor.next_batch().await.is_none());

    Ok(())
}

// ---------------------------------------------------------------------------
// Cancellation must reach the materializing terminals too
// ---------------------------------------------------------------------------
//
// Both cursors now abort on a cancelled token, but `fetch_all` on either
// surface does not: the token is handed to the executor, and the executor's
// only cooperative checkpoint (`GraphContext::check_timeout`) is never reached
// by a scan/join/traverse plan. So the surfaces were inconsistent in one
// direction before this work and the other direction after it.
//
// These pin the intended contract: a cancelled scope aborts the statement on
// every terminal, regardless of plan shape.

#[tokio::test]
async fn test_cancellation_token_aborts_fetch_all() -> Result<()> {
    let db = seeded_db().await?;

    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let res = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .cancellation_token(token)
        .fetch_all()
        .await;

    let err = res
        .err()
        .expect("session fetch_all ran to completion under an already-cancelled token");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_cancellation_token_aborts_fetch_all() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let res = tx
        .query_with("MATCH (n:Node) RETURN n")
        .cancellation_token(token)
        .fetch_all()
        .await;

    let err = res
        .err()
        .expect("transaction fetch_all ran to completion under an already-cancelled token");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_transaction_cancel_aborts_its_own_queries() -> Result<()> {
    // `Transaction::cancel()` cancels `Transaction.cancellation_token`, a child
    // of the session's token. That token was never handed to an executor, so
    // cancelling a transaction affected nothing in flight -- the whole point of
    // the API. No builder token here: the transaction's own scope must be
    // enough.
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    tx.cancel();

    let res = tx.query_with("MATCH (n:Node) RETURN n").fetch_all().await;
    let err = res
        .err()
        .expect("query ran to completion after `Transaction::cancel()`");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_transaction_cancel_aborts_its_own_cursor() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    tx.cancel();

    let cursor = tx.query_with("MATCH (n:Node) RETURN n").cursor().await?;
    let err = drain_cursor(cursor)
        .await
        .expect("cursor streamed every row after `Transaction::cancel()`");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_uncancelled_transaction_is_unaffected() -> Result<()> {
    // Guards the inverse: wiring the transaction's scope into execution must
    // not make ordinary transactional queries fail.
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let rows = tx
        .query_with("MATCH (n:Node) RETURN n")
        .fetch_all()
        .await?
        .into_rows();
    assert_eq!(rows.len(), 100);

    let mut cursor = tx.query_with("MATCH (n:Node) RETURN n").cursor().await?;
    let mut seen = 0usize;
    while let Some(batch) = cursor.next_batch().await {
        seen += batch?.len();
    }
    assert_eq!(seen, 100);

    Ok(())
}

/// `LocyBuilder::cancellation_token` was write-only.
///
/// The setter existed on both the session and transaction Locy builders, the
/// Python bindings called it (`builders.rs`), and nothing ever read the field:
/// `LocyEngine` carried no cancellation state at all, so every Cypher statement
/// the evaluation ran — clause bodies, DERIVE mutations, trailing reads — ran
/// unguarded. A caller who cancelled observed the program run to completion.
///
/// Pre-cancelled so the outcome is deterministic rather than a race.
#[tokio::test]
async fn test_cancellation_token_aborts_a_locy_program() -> Result<()> {
    let db = seeded_db().await?;

    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let result = db
        .session()
        .locy_with("CREATE RULE r AS MATCH (n:Node) YIELD KEY n")
        .cancellation_token(token)
        .run()
        .await;

    let err = result.expect_err(
        "the Locy program ran to completion despite an already-cancelled token; \
         `LocyBuilder::cancellation_token` was never read",
    );
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

/// Inverse guard: an uncancelled Locy program still returns its rows.
///
/// Wiring a scope into every Locy execution path must not make ordinary
/// evaluation abort.
#[tokio::test]
async fn test_uncancelled_locy_program_still_runs() -> Result<()> {
    let db = seeded_db().await?;

    let result = db
        .session()
        .locy_with("CREATE RULE r AS MATCH (n:Node) YIELD KEY n")
        .run()
        .await?;

    assert!(
        result.into_inner().stats.derived_nodes > 0,
        "the program should derive at least one fact"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// `max_query_memory` must bound execution, not only the result set — #185
// ---------------------------------------------------------------------------
//
// `enforce_memory_limit` runs *after* `executor.execute(...)` and measures the
// finished rows. A query that returns a handful of rows passed it while peak
// RSS reached tens of gigabytes on the way there, because the limit never
// reached DataFusion: the `SessionContext` was built with `SessionContext::new()`
// and therefore with DataFusion's default unbounded memory pool, which never
// refuses a reservation and never spills.
//
// It is now built with a `GreedyMemoryPool` sized from `max_query_memory`, so
// operators that reserve through the pool are bounded. Two limits are honest
// and deliberate: the pool sits on the shared session template, so it is a
// budget across concurrent queries rather than strictly per query; and an
// operator that allocates an Arrow buffer directly without reserving (the
// `MutableArrayData` path behind #184) is still unbounded.

/// The premise the pool choice rests on: a disk manager *is* configured.
///
/// Two comments once justified `GreedyMemoryPool` over `FairSpillPool` on the
/// claim that no disk-spill path existed, so neither pool could spill (#238).
/// The claim was false when written — #202's evidence is an `ExternalSorter`
/// asking for 5.1 GB on LDBC IC9 with a disk manager available throughout — and
/// the reasoning that replaced it depends on the opposite fact being true.
///
/// That fact is a *dependency default*, not something this repo controls. If
/// DataFusion ever ships `Disabled` as the default, the reasoning on
/// `memory_bounded_runtime` silently becomes wrong again and the old comment
/// becomes right by accident. This is the cheapest way to be told.
#[test]
fn disk_manager_default_is_a_real_directory() {
    use datafusion::execution::disk_manager::DiskManagerMode;

    assert!(
        matches!(DiskManagerMode::default(), DiskManagerMode::OsTmpDirectory),
        "DataFusion's default disk manager is what makes spilling possible; \
         the GreedyMemoryPool reasoning in `memory_bounded_runtime` and on the \
         session template assumes it, and #238 exists because an earlier \
         comment assumed the reverse"
    );
}

/// A database whose only unusual setting is a small query-memory ceiling.
///
/// # Prefer `.max_memory()` on the query
///
/// A database-wide ceiling applies to the fixture's own writes as well as to
/// the query under test, which conflates two costs that have nothing to do with
/// each other. That stayed invisible while the seeding path reserved almost
/// nothing; once #261 made `GraphUnwindExec` and `MutationExec` account for what
/// they hold, two tests here began failing in their `UNWIND range(...) CREATE`
/// seeding — reporting a defect in a fixture rather than in the operator each
/// was written to guard.
///
/// Use this only where the ceiling is genuinely a property of the database (the
/// result-size estimator, the "modest query is unaffected" control). Where the
/// subject is one query, put the ceiling on that query.
async fn db_with_memory_limit(bytes: usize) -> Result<Uni> {
    let mut config = uni_db::UniConfig::default();
    config.max_query_memory = bytes;
    Ok(Uni::in_memory().config(config).build().await?)
}

/// A first-party operator reserves what it materializes (#242).
///
/// `VidLookupJoinExec` replaced `HashJoinExec` on this shape by deliberate
/// plan-shape choice. The one it replaced is pool-accounted and spillable; it
/// was neither, so the choice narrowed the pool's coverage on purpose, for
/// unrelated reasons. Across the whole workspace there were zero `try_grow`
/// sites, which is why multi-GB peaks in graph operators never tripped a pool
/// that was configured and working the entire time — every failure it ever
/// produced came from a stock DataFusion operator.
///
/// **The build side has to be large.** The operator's whole point is that a
/// small, scattered build set becomes a handful of indexed lookups, and in that
/// regime it materializes almost nothing — an earlier version of this test used
/// 50 sources and passed a 64 KiB ceiling honestly, because 50 probe rows really
/// do fit. Memory only matters once the distinct-vid set is big enough that the
/// probe fetches a large slice of the table, and past `MAX_VIDS_PER_CHUNK` it is
/// also concatenated, which holds the chunks and the combined batch at once.
#[tokio::test]
async fn a_vid_lookup_join_reserves_what_it_materializes() -> Result<()> {
    const ROWS: usize = 20_000;
    // The ceiling has to clear the build-side scan and still fall below the
    // join's probe materialization. Both sides are scans now that
    // `GraphScanExec` reserves too, and the build side is deliberately the
    // narrow one -- `linked_vid` only -- while the probe carries long strings,
    // so there is a wide band between them.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Target")
        .property("name", uni_db::DataType::String)
        .done()
        .label("Source")
        .property_nullable("linked_vid", uni_db::DataType::Int64)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:Target {{name: \
         'a-name-long-enough-that-twenty-thousand-of-them-are-megabytes-' + toString(i)}})",
        ROWS - 1
    ))
    .await?;
    tx.commit().await?;

    // One Source per Target: the distinct-vid set is the whole table, which is
    // both the regime where this operator materializes and, being over
    // `MAX_VIDS_PER_CHUNK`, the one that concatenates.
    let tx = session.tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:Source {{linked_vid: i}})",
        ROWS - 1
    ))
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // `count(b.name)`, not `b.name`: one row out, so the post-hoc result-size
    // check cannot see this query at all and the only thing that can refuse it
    // is the execution-time pool.
    //
    // This test used to return the names and sit at a 4 MiB ceiling. That worked
    // only while the join's charge was inflated by `get_array_memory_size`
    // summing shared buffers; once the charge became honest the two costs landed
    // within half a megabyte of each other and the result-size check won the
    // race, rejecting the query with a message that names no operator. Removing
    // the confound is better than re-tuning around it.
    //
    // Swept on this fixture, 20 000 rows:
    //
    // | ceiling | outcome |
    // |---|---|
    // | 1 MiB | refused, `GraphScanExec` — below what the scan itself needs |
    // | 2–3 MiB | **refused, `VidLookupJoinExec`** |
    // | 4 MiB and up | OK |
    let res = session
        .query_with(
            "MATCH (a:Source) MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN count(b.name) AS c",
        )
        .max_memory(3 * 1024 * 1024)
        .fetch_all()
        .await;

    match res {
        Err(e) => {
            let msg = e.to_string();
            // Naming the operator is what makes this discriminating. An earlier
            // version accepted any message containing "memory" and passed with
            // the reservations removed, because the post-hoc result-size check
            // rejected this query at that ceiling too. Two mechanisms, one
            // indistinguishable assertion. The pool names the consumer that
            // asked; the result-size check cannot.
            assert!(
                msg.contains("VidLookupJoinExec"),
                "the refusal must come from the join's own reservation, not from \
                 some other limit that happens to reject this query: {msg}"
            );
        }
        Ok(rows) => panic!(
            "a 3 MiB ceiling accepted a join that materialized the whole probe \
             side (counted {:?}); the operator is allocating outside the pool \
             again",
            rows.rows()[0].values()[0]
        ),
    }
    Ok(())
}

/// The discriminating shape from #185: **one row out**, a large intermediate.
/// The post-hoc result-size check cannot see this query at all — one integer
/// is far below any ceiling — so if it is rejected, the rejection came from
/// the execution-time pool.
#[tokio::test]
async fn max_query_memory_bounds_execution_not_just_results() -> Result<()> {
    // The ceiling goes on the query, not the database: the seeding below writes
    // 40 000 rows and has no business being measured against a limit written
    // for a hash table.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("W")
        .property("k", uni_db::DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "UNWIND range(0, 40000) AS i CREATE (:W {k: 'key-that-is-long-enough-to-matter-' + toString(i)})",
    )
    .await?;
    tx.commit().await?;

    // A *grouped* aggregate, because that is what reserves through the pool:
    // `count(DISTINCT x)` with no grouping keys uses a plain accumulator that
    // allocates its hash set directly. The inner aggregate builds 40k groups;
    // the outer collapses them so only one row is ever returned, which is what
    // keeps the post-hoc result-size check out of the picture.
    // 256 KiB: far below the distinct-value hash table this builds.
    let res = db
        .session()
        .query_with("MATCH (n:W) WITH n.k AS k, count(*) AS per RETURN count(k) AS c")
        .max_memory(256 * 1024)
        .fetch_all()
        .await;

    match res {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("memory") || msg.contains("Resources") || msg.contains("resources"),
                "expected a memory-exhaustion error, got: {msg}"
            );
        }
        Ok(rows) => panic!(
            "a 256 KiB ceiling accepted a 40k-distinct-value aggregation returning {:?}; \
             the limit is still measuring the result set rather than execution",
            rows.rows()[0].values()[0]
        ),
    }
    Ok(())
}

/// The same ceiling must not reject an ordinary query. Guards against "fixed"
/// meaning "everything now fails".
#[tokio::test]
async fn a_modest_query_is_unaffected_by_the_execution_pool() -> Result<()> {
    let db = db_with_memory_limit(256 * 1024).await?;
    db.schema()
        .label("S")
        .property("k", uni_db::DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(0, 50) AS i CREATE (:S {k: toString(i)})")
        .await?;
    tx.commit().await?;

    let rows = db
        .session()
        .query("MATCH (n:S) RETURN count(*) AS c")
        .await?;
    assert_eq!(rows.rows()[0].values()[0], uni_db::Value::Int(51));
    Ok(())
}

/// The result-size estimator has to count heap bytes.
///
/// It was `size_of_val(v) + 64` — the size of the `Value` enum's discriminant,
/// a constant — so a row holding a megabyte string was charged the same as a
/// row holding a small integer, and the "byte" limit was really a row count.
/// One row of ~1 MB must exceed a 64 KiB ceiling.
#[tokio::test]
async fn the_memory_estimator_counts_heap_bytes() -> Result<()> {
    let db = db_with_memory_limit(64 * 1024).await?;

    let res = db
        .session()
        .query("RETURN reduce(s = '', x IN range(0, 4000) | s + '0123456789abcdefghij') AS big")
        .await;

    let err = res
        .err()
        .expect("one ~80 KB string must exceed a 64 KiB ceiling; a per-value constant would not");
    assert!(
        err.to_string().contains("Query exceeded memory limit"),
        "expected the result-size limit, got: {err}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// A transaction statement must be time-bounded, like its session twin
// ---------------------------------------------------------------------------
//
// The session paths wrapped execution in `tokio::time::timeout`; the two
// transaction paths raced only the cancellation scope, so a statement run
// inside a transaction had no wall-clock bound at all.

#[tokio::test]
async fn a_transaction_statement_honours_query_timeout() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let res = tx
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .fetch_all()
        .await;

    let err = res
        .err()
        .expect("a 1ns timeout must reject a transaction statement on the materializing terminal");
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "expected UniError::Timeout, got: {err:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// A whole-node group key must not materialise the whole node — #196
// ---------------------------------------------------------------------------
//
// `WITH p, count(*)` needs only the entity's *identity*: grouping cannot depend
// on a property the query never reads. The analysis marked a bare group-key
// variable `"*"` anyway, which pulled the full schema — `_all_props` and
// `overflow_json` included — into the scan, and the physical group key then
// appends every `{v}.`-prefixed column beside the entity struct. The node is
// hashed and copied per group, twice over.
//
// At LDBC SF1 that made
// `MATCH (p:Person)-[:KNOWS]-() WITH p, count(*) RETURN p.id` request 1.76 GB
// against a 1 GiB pool and abort the bench during parameter derivation, for a
// query that reads one property.
//
// The test below is that shape at a scale the suite can afford: wide payload
// properties the query never touches, a ceiling sized so materialising them
// would exceed it, and a single property actually read.

/// Adding a property the query never reads must not change what the aggregate
/// costs.
///
/// Measured on this fixture, 20 001 groups, before and after the fix:
///
/// | `pad` length | before | after |
/// |---|---|---|
/// | 4 chars   |  9.8 MB | 4.4 MB |
/// | 256 chars | 65.5 MB | 4.4 MB |
///
/// The ceiling below sits above the constant cost and far below the 256-char
/// figure, so this passes only if the group key is insensitive to the payload.
/// Asserting insensitivity rather than an absolute number is deliberate: the
/// first version of this test asserted a ceiling, and a ceiling cannot tell a
/// materialised payload from an aggregate that is simply large. Both arms run
/// at the same limit for the same reason.
#[tokio::test]
async fn an_unread_property_does_not_change_what_a_group_key_costs() -> Result<()> {
    async fn run(pad_len: usize) -> Result<()> {
        // 16 MiB: above the ~4.4 MB the 20k groups genuinely need, well below
        // the ~65 MB the same query cost when the payload rode along.
        let db = db_with_memory_limit(16 * 1024 * 1024).await?;
        db.schema()
            .label("G")
            .property("tag", uni_db::DataType::String)
            .property("pad", uni_db::DataType::String)
            .apply()
            .await?;
        let pad = "x".repeat(pad_len);
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            "UNWIND range(0, 20000) AS i CREATE (:G {{tag: 'tag-' + toString(i % 50), \
             pad: '{pad}' + toString(i)}})"
        ))
        .await?;
        tx.commit().await?;

        db.session()
            // No ORDER BY: sorting 20k rows reserves through the same pool and
            // would make this a test of the sorter instead of the group key.
            // The outer aggregate collapses the groups to one row, which also
            // keeps the post-hoc result-size check out of the picture.
            .query("MATCH (p:G) WITH p, count(*) AS c RETURN count(c) AS n")
            .await
            .map(|_| ())
            .map_err(|e| {
                anyhow::anyhow!(
                    "grouping by a whole node exhausted the ceiling with pad_len={pad_len}: {e}. \
                     The query reads only `tag`; a property it never mentions must not be \
                     materialised into the group key."
                )
            })
    }

    // The narrow arm establishes the ceiling is workable at all; the wide arm
    // is the one that fails when the payload is carried.
    run(4).await?;
    run(256).await?;
    Ok(())
}

/// The control: the same shape where the node *is* returned whole must still
/// work, and must still carry its properties. Narrowing a group key that is
/// genuinely returned would be a wrong answer, not a smaller one.
#[tokio::test]
async fn a_group_key_returned_whole_still_carries_its_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("G")
        .property("tag", uni_db::DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(0, 5) AS i CREATE (:G {tag: 'tag-' + toString(i)})")
        .await?;
    tx.commit().await?;

    let rows = db
        .session()
        .query("MATCH (p:G) WITH p, count(*) AS c RETURN p ORDER BY p.tag LIMIT 1")
        .await?;

    match &rows.rows()[0].values()[0] {
        uni_db::Value::Node(n) => assert_eq!(
            n.properties.get("tag"),
            Some(&uni_db::Value::String("tag-0".to_string())),
            "a group key returned whole lost its properties"
        ),
        other => panic!("expected a Node, got {other:?}"),
    }
    Ok(())
}

/// The scan reserves the whole-result batch it builds (#242).
///
/// `GraphScanExec` builds one `RecordBatch` for the entire result and then
/// hands it out in slices, so it is the largest single allocation this system
/// makes and it was invisible to the pool. The reservation is on the *stream*,
/// not inside the scan future, because the batch stays resident across every
/// poll that follows — the slices are zero-copy views onto it.
///
/// The assertion names the operator for the reason the join test does: at a
/// ceiling low enough to reject this query, the post-hoc result-size check
/// rejects it too, and an assertion that accepts any memory-shaped message
/// passes with the reservation removed.
#[tokio::test]
async fn a_graph_scan_reserves_the_batch_it_builds() -> Result<()> {
    // On the query, not the database — the 20 000-row seed below is not what
    // this test is about, and a database-wide ceiling makes it refuse first.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("W")
        .property("k", uni_db::DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "UNWIND range(0, 20000) AS i CREATE (:W {k: \
         'a-string-long-enough-to-add-up-across-twenty-thousand-rows-' + toString(i)})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    match db
        .session()
        .query_with("MATCH (n:W) RETURN n.k AS k")
        .max_memory(256 * 1024)
        .fetch_all()
        .await
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("GraphScanExec"),
                "the refusal must come from the scan's own reservation: {msg}"
            );
        }
        Ok(rows) => panic!(
            "a 256 KiB ceiling accepted a scan materializing {} rows; the scan \
             is allocating outside the pool again",
            rows.rows().len()
        ),
    }
    Ok(())
}

/// Vertices enough to span more than one scan chunk.
///
/// The chunk is the session's `batch_size`, 8192 by default, so this is three
/// chunks and a remainder. At or below one chunk nothing chunks and the test
/// could not tell the two apart.
const CHUNKED_SCAN_ROWS: i64 = 25_000;

/// Build a store of [`CHUNKED_SCAN_ROWS`] vertices and the literal id list that
/// selects all of them.
async fn store_and_id_list() -> Result<(Uni, String)> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("CH")
        .property("k", uni_db::DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:CH {{k: \
         'a-string-long-enough-to-add-up-across-twenty-five-thousand-rows-' + toString(i)}})",
        CHUNKED_SCAN_ROWS - 1
    ))
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let ids = db.session().query("MATCH (n:CH) RETURN id(n) AS v").await?;
    let list = ids
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            uni_db::Value::Int(i) => i.to_string(),
            other => panic!("id() returned {other:?}"),
        })
        .collect::<Vec<_>>()
        .join(",");
    Ok((db, list))
}

/// A vid-filtered scan builds one chunk at a time, not the whole result (#214).
///
/// `GraphScanExec` built one `RecordBatch` for the entire result and then
/// handed it out in zero-copy slices, so slicing bounded what went downstream
/// at once but not what the scan held — every slice pins the parent's buffers.
/// Scanning a chunk of vids at a time gives each output batch its own storage.
///
/// Chunking by *vid* is what makes this safe: every version of a vid falls in
/// one chunk, so the MVCC dedup still sees all the candidates it must choose
/// between, and the L0 overlay is scoped to the same vid set. Chunking by
/// arriving storage batch would break both and return stale rows.
///
/// Discriminating, and measured: the scan needs 2.2 MB for this result whole.
/// With chunking disabled the query fails at this ceiling with
/// `Failed to allocate additional 2.2 MB for GraphScanExec`; chunked it
/// succeeds, because a chunk is a third of that. The count is asserted exactly
/// so a boundary that dropped or repeated a row fails too — `min(cursor +
/// slice, len)` and the resume offset are otherwise unexercised.
#[tokio::test]
async fn a_vid_filtered_scan_is_bounded_by_one_chunk() -> Result<()> {
    // Below the 2.2 MB the unchunked scan reserves, above one chunk's share.
    const CEILING: usize = 1 << 20;

    let (db, id_list) = store_and_id_list().await?;

    // Aggregated on purpose: returning the rows themselves would trip the
    // cursor's result-size check at this ceiling first, and an assertion that
    // accepts any memory-shaped failure passes with the chunking removed.
    let rows = db
        .session()
        .query_with(&format!(
            "MATCH (n:CH) WHERE id(n) IN [{id_list}] RETURN count(n.k) AS c"
        ))
        .max_memory(CEILING)
        .fetch_all()
        .await?;

    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(CHUNKED_SCAN_ROWS),
        "chunked scan lost or repeated rows at a chunk boundary"
    );
    Ok(())
}

/// `id(n) IN [...]` selects exactly those nodes.
///
/// A guard on the rewrite that makes the case above reachable at all:
/// `rewrite_id_to_vid` did not recurse into `Expr::In`, so `id(n) IN [...]`
/// stayed a function call and the multi-VID Lance pushdown could never match
/// it — while `id(n) = x` reached it, `=` being a `BinaryOp`. Teaching the
/// rewrite a new shape is what risks wrong rows, so the answer is pinned here
/// rather than only the memory behavior above.
#[tokio::test]
async fn id_in_a_literal_list_selects_exactly_those_nodes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Pick")
        .property("k", uni_db::DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(0, 9) AS i CREATE (:Pick {k: i})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let all = db
        .session()
        .query("MATCH (n:Pick) WHERE n.k IN [2, 5, 7] RETURN id(n) AS v ORDER BY v")
        .await?;
    let wanted: Vec<i64> = all
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            uni_db::Value::Int(i) => *i,
            other => panic!("id() returned {other:?}"),
        })
        .collect();
    assert_eq!(wanted.len(), 3, "fixture must select three nodes");

    let list = wanted
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let picked = db
        .session()
        .query(&format!(
            "MATCH (n:Pick) WHERE id(n) IN [{list}] RETURN n.k AS k ORDER BY k"
        ))
        .await?;
    let got: Vec<uni_db::Value> = picked
        .rows()
        .iter()
        .map(|r| r.values()[0].clone())
        .collect();
    assert_eq!(
        got,
        vec![
            uni_db::Value::Int(2),
            uni_db::Value::Int(5),
            uni_db::Value::Int(7)
        ],
        "id() IN a literal list returned the wrong nodes"
    );
    Ok(())
}

/// A full-label scan is bounded by one `_vid` range, not by the result (#214).
///
/// Phase 2 bounded a scan restricted to a vid list. A plain `MATCH (n:L)` has
/// no list to chunk, so it read the label whole; this walks it in `_vid`
/// ranges instead. The range is the unit because `_vid` is the MVCC dedup key:
/// every version of a vid lands in exactly one range, so the highest-`_version`
/// choice still sees all its candidates. Chunking on anything else — arriving
/// storage batch, row offset — would serve superseded rows.
///
/// Discriminating, and measured: whole, this scan reserves 2.2 MB and fails at
/// this ceiling with `Failed to allocate additional 2.2 MB for GraphScanExec`.
/// Walked, each range reserves 751 KB and it succeeds. The count is asserted
/// exactly, so a range walk that skipped or repeated a stretch fails too.
#[tokio::test]
async fn a_full_label_scan_is_bounded_by_one_range() -> Result<()> {
    // Above one range's 751 KB, below the 2.2 MB the whole result needs.
    const CEILING: usize = 1 << 20;

    let (db, _ids) = store_and_id_list().await?;

    // Aggregated so the scan's own reservation is the binding constraint;
    // returning the rows would trip the cursor's result-size check first, and
    // an assertion that accepts any memory-shaped failure passes with the walk
    // removed.
    let rows = db
        .session()
        .query_with("MATCH (n:CH) RETURN count(n.k) AS c")
        .max_memory(CEILING)
        .fetch_all()
        .await?;

    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(CHUNKED_SCAN_ROWS),
        "the range walk lost or repeated rows"
    );
    Ok(())
}

/// A label whose vids begin past the start of the range walk is still read.
///
/// Vids are global, so one label's rows can sit anywhere in the space and a
/// walk from zero meets empty ranges before reaching them. Emptiness is
/// ambiguous — past the end, or a gap — and a walk that guesses "end" returns
/// a silently truncated result rather than failing.
///
/// Discriminating on exactly that: with the gap case treated as the end this
/// returns **0 rows**, not an error. `Target`'s 12 000 rows are also above the
/// 8 192 gate, which is what puts this fixture on the range walk at all — at
/// 5 000 it took the ordinary unchunked path and passed with the gap handling
/// deleted.
#[tokio::test]
async fn a_label_whose_vids_start_late_is_walked_past_the_gap() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    for label in ["Other", "Target"] {
        db.schema()
            .label(label)
            .property("k", uni_db::DataType::Int)
            .apply()
            .await?;
    }
    let tx = db.session().tx().await?;
    // `Other` first, so every `Target` vid is above the walk's first ranges.
    tx.execute("UNWIND range(0, 19999) AS i CREATE (:Other {k: i})")
        .await?;
    tx.execute("UNWIND range(0, 11999) AS i CREATE (:Target {k: i})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let rows = db
        .session()
        .query("MATCH (n:Target) RETURN count(n) AS c")
        .await?;
    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(12_000),
        "the walk stopped at an empty range instead of crossing the gap"
    );
    Ok(())
}

/// The range walk is gated: a label that fits in one output batch is still read
/// in a single scan (#214).
///
/// Chunking unconditionally is not free — the traversal measured ~6% against a
/// control when it chunked a result that did not need it, which is why #214
/// asks for the gate and not just the walk. A skipped gate is invisible in the
/// results, so this asserts the observable that separates the two strategies:
/// `scans_reported`, which counts completed Lance scans.
///
/// The sizing call itself does not pollute that count — `count_rows` answers
/// from fragment metadata and never builds a `ScanRequest`, so it never reaches
/// the scan-stats callback.
///
/// Discriminating in both directions, which is why both sizes are measured in
/// one test: delete the gate and the small label's count rises to the large
/// one's shape; delete the walk and the large label's falls to the small one's.
/// A single-size assertion would pass for one of those.
#[tokio::test]
async fn the_range_walk_is_skipped_for_a_label_that_fits_one_batch() -> Result<()> {
    // Well under the 8 192 default batch size, so the gate must decline.
    const SMALL_ROWS: i64 = 500;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Small")
        .property("k", uni_db::DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:Small {{k: i}})",
        SMALL_ROWS - 1
    ))
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let small = db
        .session()
        .query("MATCH (n:Small) RETURN count(n) AS c")
        .await?;
    assert_eq!(small.rows()[0].values()[0], uni_db::Value::Int(SMALL_ROWS));
    let small_scans = small.metrics().scans_reported;

    // The same query over a label well above the gate, as the contrast.
    let (big_db, _ids) = store_and_id_list().await?;
    let big = big_db
        .session()
        .query("MATCH (n:CH) RETURN count(n) AS c")
        .await?;
    assert_eq!(
        big.rows()[0].values()[0],
        uni_db::Value::Int(CHUNKED_SCAN_ROWS)
    );
    let big_scans = big.metrics().scans_reported;

    eprintln!(
        "#214 gate: {SMALL_ROWS} rows -> {small_scans} scans, \
         {CHUNKED_SCAN_ROWS} rows -> {big_scans} scans"
    );

    // Exactly one, not merely "fewer than the large label". Measured: with the
    // gate removed this is 2 — the walk reads its first range and then pays the
    // `ConfirmingEnd` probe to learn there is nothing above it. A `>` comparison
    // against the large label passes either way, because the large label needs
    // more ranges whether or not the small one was gated. The exact count is
    // what makes this fail when the gate goes.
    assert_eq!(
        small_scans, 1,
        "a label that fits one output batch must be read in a single scan; \
         {small_scans} means the range walk engaged and paid for ranges the \
         result did not need"
    );
    assert!(
        big_scans > small_scans,
        "the large label reported {big_scans} scans against the small label's \
         {small_scans}: the walk did not engage at all"
    );
    Ok(())
}

/// A variable-length expansion is bounded by one row-chunk, not by the input
/// batch (#241).
///
/// `VarLengthStreamState` accumulates a `Vec<VarLengthExpansion>` carrying a
/// node path and an edge path *per enumerated path*, so its size is paths times
/// path length and is bounded by neither the input nor any table. It used to
/// build that set for a whole input batch at once.
///
/// Chunking the *materialization*, which is what the single-hop sibling does,
/// would not have helped: the whole set exists before materialization begins.
/// The input rows are what had to be chunked.
///
/// # The ceiling is chosen from measurement, not guessed
///
/// On this fixture the query enumerates 111 100 paths from 10 source rows.
/// Measured by tightening the pool until it refuses:
///
/// * one row-chunk asks for **1697 KB**;
/// * the whole 10-row batch asks for **15.2 MB**.
///
/// 8 MB sits between them, so this passes only while the expansion is chunked.
/// Verified discriminating: restoring `rows_per_chunk` to `slice_size` fails it
/// with `Failed to allocate additional 15.2 MB`.
///
/// The same contrast at depth 6 is 184.8 MB against 1846.3 MB — a ratio of
/// 9.99 on 10 rows, which is the bound moving from per-batch to per-row. Depth
/// 4 is used here because it shows the same thing in two seconds.
#[tokio::test]
async fn a_variable_length_expansion_is_bounded_by_one_row_chunk() -> Result<()> {
    /// Above one row-chunk's 1697 KB, below the whole batch's ~17 MB.
    const CEILING: usize = 8 * 1024 * 1024;
    const WIDTH: i64 = 10;
    const LAYERS: i64 = 7;
    const EXPECTED_PATHS: i64 = 111_100;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("layer", uni_db::DataType::Int)
        .apply()
        .await?;
    db.schema().edge_type("E", &["N"], &["N"]).apply().await?;

    // A layered mesh, every node in layer i pointing at every node in i+1, so
    // path count is multiplicative in depth while the graph stays at 70 nodes.
    let tx = db.session().tx().await?;
    for layer in 0..LAYERS {
        tx.query_with("UNWIND range(0, $w - 1) AS i CREATE (:N {layer: $l})")
            .param("w", uni_db::Value::Int(WIDTH))
            .param("l", uni_db::Value::Int(layer))
            .fetch_all()
            .await?;
    }
    for layer in 0..LAYERS - 1 {
        tx.query_with("MATCH (a:N {layer: $l}), (b:N {layer: $n}) CREATE (a)-[:E]->(b)")
            .param("l", uni_db::Value::Int(layer))
            .param("n", uni_db::Value::Int(layer + 1))
            .fetch_all()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    // `p` is bound, which is what selects full path enumeration over the
    // endpoint-only BFS. Without it the traversal never builds the expansion
    // set at all and this would pass with the chunking deleted.
    let rows = db
        .session()
        .query_with("MATCH p = (a:N {layer: 0})-[:E*1..4]->(b:N) RETURN count(p) AS c")
        .max_memory(CEILING)
        .fetch_all()
        .await?;

    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(EXPECTED_PATHS),
        "the row-chunked expansion lost or repeated paths"
    );
    Ok(())
}

/// The schemaless variable-length expansion is both accounted and bounded
/// (#241, second arm).
///
/// `GraphVariableLengthTraverseMainExec` reserved only its adjacency map, so
/// the expansion set — a node path and an edge path per enumerated path, and
/// this operator's dominant allocation — was invisible to the query pool.
/// Measured before the change: 106 MB of expansions passed an 8 MB ceiling
/// without the pool noticing, because nothing ever asked it.
///
/// Two assertions, because the two halves fail differently and a single
/// ceiling cannot catch both:
///
/// * **accounted** — a 1 MB ceiling must now be *refused*. Before, no ceiling
///   could refuse this query at all: unaccounted memory cannot be rejected, so
///   the test that catches missing accounting is one that demands a failure.
/// * **bounded** — an 8 MB ceiling must *pass*. One row-chunk asks 1697 KB;
///   the whole 10-row batch asks ~15 MB. Given accounting, this fails unless
///   the expansion is chunked.
///
/// Verified discriminating in both directions: dropping the reservation makes
/// the first assertion fail, and restoring `rows_per_chunk` to `slice_size`
/// makes the second fail.
///
/// # Sensitive to machine load, and why the timeout is explicit
///
/// Enumerating 111 100 paths takes ~17 s on an idle 22-core box, against
/// `UniConfig::query_timeout`'s 30 s default — under 2x of headroom. Saturate
/// the machine and the query crosses the deadline: measured **9 failures in 12
/// runs** with 20 spinners running, every one of them
/// `UniError::Timeout`, not a memory assertion.
///
/// That failure is doubly misleading. The second query surfaces it through `?`,
/// so the test reports a bare "Operation timed out"; and had the *first* query
/// timed out instead, the error would have failed the `contains` check for the
/// operator name, reading exactly like a lost reservation.
///
/// So both queries carry an explicit generous timeout. This test asserts what
/// the pool accounts for, not how fast the query is — it must not be able to
/// fail on latency. Keep the bound finite so a genuine hang still ends.
#[tokio::test]
async fn a_schemaless_variable_length_expansion_is_accounted_and_bounded() -> Result<()> {
    const WIDTH: i64 = 10;
    const LAYERS: i64 = 7;
    const EXPECTED_PATHS: i64 = 111_100;
    const QUERY: &str = "MATCH p = (a:N {layer: 0})-[:E*1..4]->(b:N) RETURN count(p) AS c";
    /// Far above the ~17 s idle cost, so contention cannot reach it, while
    /// still bounding a real hang.
    const NOT_A_LATENCY_TEST: std::time::Duration = std::time::Duration::from_secs(600);

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("layer", uni_db::DataType::Int)
        .apply()
        .await?;
    // `E` is deliberately NOT declared: an undeclared edge type is what routes
    // the pattern through `GraphVariableLengthTraverseMainExec` rather than its
    // schema'd twin.
    let tx = db.session().tx().await?;
    for layer in 0..LAYERS {
        tx.query_with("UNWIND range(0, $w - 1) AS i CREATE (:N {layer: $l})")
            .param("w", uni_db::Value::Int(WIDTH))
            .param("l", uni_db::Value::Int(layer))
            .fetch_all()
            .await?;
    }
    for layer in 0..LAYERS - 1 {
        tx.query_with("MATCH (a:N {layer: $l}), (b:N {layer: $n}) CREATE (a)-[:E]->(b)")
            .param("l", uni_db::Value::Int(layer))
            .param("n", uni_db::Value::Int(layer + 1))
            .fetch_all()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    // Accounted: a ceiling below one row-chunk must be refused, naming this
    // operator. An unaccounted expansion would sail past any ceiling.
    let refused = db
        .session()
        .query_with(QUERY)
        .max_memory(1024 * 1024)
        .timeout(NOT_A_LATENCY_TEST)
        .fetch_all()
        .await;
    let err = refused
        .err()
        .expect("a 1 MB ceiling must refuse a 111k-path expansion")
        .to_string();
    assert!(
        err.contains("GraphVariableLengthTraverseMainExec"),
        "the refusal must come from the schemaless VLP operator, so the \
         expansion set is what the pool saw; got: {err}"
    );

    // Bounded: above one row-chunk but below the whole batch.
    let rows = db
        .session()
        .query_with(QUERY)
        .max_memory(8 * 1024 * 1024)
        .timeout(NOT_A_LATENCY_TEST)
        .fetch_all()
        .await?;
    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(EXPECTED_PATHS),
        "the row-chunked expansion lost or repeated paths"
    );
    Ok(())
}

/// A schemaless single-hop traversal accounts for the batch it expands, not
/// just the input it expanded from (#242).
///
/// `GraphTraverseMainExec` reserved `buffered_bytes + adjacency` on entering
/// `Processing` and then returned `expand_batch(...)` directly, so its largest
/// allocation — the expanded fan-out batch — never reached the pool. It looked
/// accounted: it registers a `MemoryConsumer`, holds a `MemoryReservation`, and
/// refuses a small enough ceiling. It simply refused on the wrong quantity.
///
/// Measured on a 500x400 fixture (200 000 edges): the pool saw 11.7 MB of
/// adjacency and input while the expansion added a further **9.9 MB** it never
/// saw, so a 13 MB ceiling passed a query whose true peak was 21.6 MB.
///
/// # Why the assertion is a required *failure*
///
/// An unaccounted allocation passes every ceiling, so no passing query can
/// witness it. The only assertion that catches this is one that demands a
/// refusal at a ceiling above what the operator used to reserve — and names the
/// operator, so the refusal is attributable rather than incidental.
///
/// Verified discriminating: dropping the output term from the reservation makes
/// the first assertion fail, because the query then succeeds.
#[tokio::test]
async fn a_schemaless_traversal_accounts_for_the_batch_it_expands() -> Result<()> {
    const SOURCES: i64 = 200;
    const TARGETS: i64 = 100;
    const EDGES: i64 = SOURCES * TARGETS;
    const QUERY: &str = "MATCH (a:Src)-[:E]->(b:Dst) RETURN count(*) AS c";

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Src")
        .property("k", uni_db::DataType::Int)
        .label("Dst")
        .property("k", uni_db::DataType::Int)
        .apply()
        .await?;
    // `E` is never declared: that is what routes a single hop through
    // `GraphTraverseMainExec` rather than its schema'd twin.
    let tx = db.session().tx().await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Src {k: i})")
        .param("n", uni_db::Value::Int(SOURCES))
        .fetch_all()
        .await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Dst {k: i})")
        .param("n", uni_db::Value::Int(TARGETS))
        .fetch_all()
        .await?;
    tx.query("MATCH (a:Src), (b:Dst) CREATE (a)-[:E]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Above what the operator used to reserve, below its true peak. Before the
    // fix this ceiling passed; now the expansion is part of the ask.
    let refused = db
        .session()
        .query_with(QUERY)
        .max_memory(2 * 1024 * 1024)
        .fetch_all()
        .await;
    let err = refused
        .err()
        .expect("the expanded batch must be part of the reservation")
        .to_string();
    assert!(
        err.contains("GraphTraverseMainExec"),
        "the refusal must name the traversal, so it is the expansion that was \
         refused rather than something incidental; got: {err}"
    );

    // With room for both, the answer is unchanged.
    let rows = db
        .session()
        .query_with(QUERY)
        .max_memory(64 * 1024 * 1024)
        .fetch_all()
        .await?;
    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(EDGES),
        "accounting must not change the result"
    );
    Ok(())
}
/// `VidLookupJoinExec` accounts for the structures it derives, not only the
/// batches it holds (#242).
///
/// The operator reserves its build batches and probe chunks carefully — it even
/// reserves the `concat_batches` peak, noting the chunks and the combined batch
/// are live at once. What it did not reserve were the two structures it derives
/// from them: `vid_set: HashSet<u64>` and the probe index
/// `HashMap<u64, Vec<usize>>`, built `with_capacity(rows)`. Rust-side, not
/// Arrow, and outside every `get_array_memory_size` it was summing.
///
/// # The window is measured, not guessed
///
/// On this 60 000-row join the derived structures add **3.7 MB** on top of
/// ~17.8 MB of accounted batches. Sweeping ceilings with and without the
/// reservation:
///
/// Re-swept after the join's charge stopped double-counting shared buffers
/// (see `BatchFootprint`): the honest figure is several times smaller, so the
/// band moved down and the old 20 MB ceiling now fits the query comfortably.
///
/// | ceiling | outcome |
/// |---|---|
/// | 1 MiB | refused, `GraphScanExec` |
/// | 2–6 MiB | **refused, `VidLookupJoinExec`** |
/// | 8 MiB and up | OK |
///
/// So 20 MB is the only kind of ceiling that can witness this, and it is why
/// the assertion below is a required *failure*: an unaccounted structure passes
/// every ceiling, so no successful query can prove it was ever charged for.
///
/// Verified discriminating: removing either `try_grow` makes this query succeed
/// at 20 MB and the test fail.
#[tokio::test]
async fn a_vid_lookup_join_accounts_for_its_derived_index() -> Result<()> {
    /// Large enough that the derived index clears the noise around the batch
    /// bytes; below this the structures are a few hundred KB and no ceiling
    /// separates the two cases.
    const N: i64 = 60_000;
    const QUERY: &str =
        "MATCH (a:Source) MATCH (b:Target) WHERE id(b) = a.linked_vid RETURN count(*) AS c";

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Source")
        .property("linked_vid", uni_db::DataType::Int)
        .label("Target")
        .property("name", uni_db::DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Target {name: 't' + toString(i)})")
        .param("n", uni_db::Value::Int(N))
        .fetch_all()
        .await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Source {linked_vid: i})")
        .param("n", uni_db::Value::Int(N))
        .fetch_all()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Above the accounted batches, below batches + derived structures.
    let refused = db
        .session()
        .query_with(QUERY)
        .max_memory(6 * 1024 * 1024)
        .fetch_all()
        .await;
    let err = refused
        .err()
        .expect("the derived index must be part of the reservation")
        .to_string();
    assert!(
        err.contains("VidLookupJoinExec"),
        "the refusal must name the join, so it is the derived structures that \
         were refused rather than something incidental; got: {err}"
    );

    // With room for both, the answer is unchanged.
    let rows = db
        .session()
        .query_with(QUERY)
        .max_memory(64 * 1024 * 1024)
        .fetch_all()
        .await?;
    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(N),
        "accounting must not change the result"
    );
    Ok(())
}

/// A chunking single-hop traversal accounts for the expansion set it retains
/// (#242).
///
/// `GraphTraverseExec` reserves the batch it is slicing, and frees that
/// reservation when a batch is handed straight downstream — correctly, since it
/// no longer holds it. But when the expansion exceeds `slice_size` it takes the
/// `Chunking` path instead, retaining the whole `Vec<Expansion>` *and* its input
/// batch across every `MaterializingChunk` round-trip, and neither was
/// accounted. The reservations on the other two paths cover the emitted batch,
/// which is a different object.
///
/// # Measured
///
/// On a 300x300 fixture (90 000 expansions) the retained set is **2.8 MB**.
/// Without the reservation the query passes a **1 MB** ceiling while holding
/// it; with the reservation it is refused at 1 MB and 2 MB and passes at 4 MB.
///
/// The assertion is a required *failure* for the usual reason: an unaccounted
/// allocation passes every ceiling, so only a demanded refusal can witness it.
///
/// Verified discriminating: removing the `try_resize` on the `Chunking`
/// transition makes this query succeed at 2 MB and the test fail.
#[tokio::test]
async fn a_chunking_traversal_accounts_for_its_retained_expansions() -> Result<()> {
    const S: i64 = 300;
    const T: i64 = 300;
    const EXPANSIONS: i64 = S * T;
    const QUERY: &str = "MATCH (a:S)-[r:R]->(b:T) RETURN count(b.k) AS c";

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("S")
        .property("k", uni_db::DataType::Int)
        .label("T")
        .property("k", uni_db::DataType::Int)
        .done()
        .edge_type("R", &["S"], &["T"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:S {k: i})")
        .param("n", uni_db::Value::Int(S))
        .fetch_all()
        .await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:T {k: i})")
        .param("n", uni_db::Value::Int(T))
        .fetch_all()
        .await?;
    tx.query("MATCH (a:S), (b:T) CREATE (a)-[:R]->(b)").await?;
    tx.commit().await?;
    db.flush().await?;

    // Below the retained expansion set. Unaccounted, this ceiling passed.
    let refused = db
        .session()
        .query_with(QUERY)
        .max_memory(2 * 1024 * 1024)
        .fetch_all()
        .await;
    let err = refused
        .err()
        .expect("the retained expansion set must be part of the reservation")
        .to_string();
    assert!(
        err.contains("GraphTraverseExec"),
        "the refusal must name the traversal; got: {err}"
    );

    // With room, the answer is unchanged.
    let rows = db
        .session()
        .query_with(QUERY)
        .max_memory(32 * 1024 * 1024)
        .fetch_all()
        .await?;
    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(EXPANSIONS),
        "accounting must not change the result"
    );
    Ok(())
}

/// A full-label scan walks its ranges in O(log rows) round trips, not one per
/// output batch (#214 follow-up).
///
/// The range walk shipped tuned on **rows** — each range aimed at one
/// `batch_size` worth. That is the same thing as a memory bound only for a row
/// of average width, and it cost a full scan dearly. Measured on LDBC SF1
/// `Message` (3 055 774 rows), `RETURN count(n)`:
///
/// | | scans | time |
/// |---|---|---|
/// | before the walk existed | 1 | 1.16 s |
/// | walk tuned on rows | 374 | 8.20 s |
/// | walk tuned on bytes | 11 | 1.18 s |
///
/// One Lance round trip per 8192 rows is 374 of them on that table. Tuning on
/// bytes, against a share of the query's own budget, lets a narrow projection
/// take ranges hundreds of times wider for the same peak — the width doubles
/// from `batch_size` until a range fills the budget, so the count is
/// logarithmic in the table rather than linear.
///
/// # Why this guard is here
///
/// The regression reached `main`. The #214 acceptance checked that the gate
/// *skips* for a small result, and that the walk is correct — neither of which
/// a large result exercises, and the cost only appears when the gate fires.
/// This asserts the round-trip count directly, at a size where the two tunings
/// differ: 200 000 narrow rows is ~24 ranges tuned on rows and ~5 tuned on
/// bytes.
#[tokio::test]
async fn a_full_label_scan_does_not_pay_a_round_trip_per_batch() -> Result<()> {
    const ROWS: i64 = 200_000;
    /// Comfortably above the ~5 the doubling ramp needs, far below the ~24 a
    /// row-tuned walk would take.
    const MAX_SCANS: u64 = 12;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Wide")
        .property("k", uni_db::DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Wide {k: i})")
        .param("n", uni_db::Value::Int(ROWS))
        .fetch_all()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:Wide) RETURN count(n) AS c")
        .await?;
    assert_eq!(
        r.rows()[0].values()[0],
        uni_db::Value::Int(ROWS),
        "the range walk lost or repeated rows"
    );
    let scans = r.metrics().scans_reported;
    eprintln!("#214 round trips: {ROWS} rows -> {scans} scans");
    assert!(
        scans > 0,
        "no scan was reported at all, so this measured nothing"
    );
    assert!(
        scans <= MAX_SCANS,
        "a full scan of {ROWS} rows issued {scans} Lance round trips; the walk \
         is tuned on rows again rather than on a share of the query budget, \
         which cost 7x on a 3M-row table"
    );
    Ok(())
}

/// `ORDER BY … LIMIT n` keeps n rows in the sort, not the whole input (#213).
///
/// The physical planner builds `SortExec` directly and DataFusion's
/// `LimitPushdown` never runs — `QueryPlanner::plan` returns the hand-built plan
/// with no physical-optimizer pass — so the fetch has to be pushed explicitly.
/// Without it, LDBC IC9 sorts 2.87M rows to return 20.
///
/// # The observable
///
/// Results are identical either way, and both survive a memory ceiling — one
/// spills, one does not — so neither rows nor a ceiling can witness this. What
/// does is `OperatorStats::actual_rows` on the sort itself: rows *produced by
/// that operator*. A fetch-less sort emits all N and the limit trims after; a
/// fetch-set sort emits n.
///
/// # Why the fetch is set at all
///
/// It was tried during #202 and measured as a regression — IC2 died at
/// `TopK[0]` with 977.4 MB, because `TopK` cannot spill where `ExternalSorter`
/// can. Re-measured at SF1 after #202/#214/#241 bounded the producers, that
/// failure does not reproduce: 977 MB was one giant input batch, not `k` rows.
/// IC2 and IC9 now complete with the fetch at 1 GiB and at 256 MB and are ~10%
/// faster warm. `TopK`'s non-spillability is unchanged and remains the standing
/// risk; a large `k` over a large input was measured as no worse.
///
/// Verified discriminating: dropping the `with_fetch` push makes the sort report
/// all ROWS rows instead of LIMIT.
#[tokio::test]
async fn an_ordered_limit_keeps_only_n_rows_in_the_sort() -> Result<()> {
    const ROWS: i64 = 20_000;
    const LIMIT: usize = 10;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Sorted")
        .property("k", uni_db::DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Sorted {k: i})")
        .param("n", uni_db::Value::Int(ROWS))
        .fetch_all()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let session = db.session();
    let (result, profile) = session
        .query_with("MATCH (n:Sorted) RETURN n.k AS k ORDER BY k DESC LIMIT 10")
        .profile()
        .await?;

    assert_eq!(result.rows().len(), LIMIT, "the limit must still apply");
    assert_eq!(
        result.rows()[0].values()[0],
        uni_db::Value::Int(ROWS - 1),
        "descending order must still be correct"
    );

    let sorts: Vec<&uni_query::query::executor::core::OperatorStats> = profile
        .runtime_stats
        .iter()
        .filter(|s| s.operator.contains("Sort"))
        .collect();
    assert!(
        !sorts.is_empty(),
        "no sort operator ran, so this measured nothing; operators were {:?}",
        profile
            .runtime_stats
            .iter()
            .map(|s| &s.operator)
            .collect::<Vec<_>>()
    );
    for s in &sorts {
        assert!(
            s.actual_rows <= LIMIT,
            "`{}` produced {} rows for a LIMIT {LIMIT}: the fetch was not pushed \
             into the sort, so the whole input is being sorted and discarded",
            s.operator,
            s.actual_rows
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// #261 — operators that hold memory the query pool never sees
// ---------------------------------------------------------------------------
//
// #242 covered operators whose reservation was smaller than what they held.
// This is the other half: operators that never reserve at all. The issue lists
// sixteen and then refuses to treat the list as a work queue, because "a static
// list is a starting point for measurement, not a work queue".
//
// **Step 1 was a census of the real corpus.** `crates/uni/examples/operator_census.rs`
// plans all fourteen LDBC SNB interactive-complex queries against SF1 and counts
// physical operators. Three of the sixteen appear:
//
// | operator | occurrences | queries |
// |---|---|---|
// | `OptionalFilterExec` | 6 | IC1 x4, IC5, IC10 |
// | `GraphShortestPathExec` | 3 | IC1, IC13, IC14 |
// | `GraphUnwindExec` | 3 | IC6, IC9, IC14 |
//
// The other thirteen appear in none of the fourteen plans. That is a finding
// about priority, not about correctness: they are still unaccounted, and the
// tests further down cover them, but these three are the ones tied to a query
// whose peak is on record.
//
// **Step 2 was the attribution, and none of the sixteen is the answer.** Each IC
// query was then *run* against SF1 under a 1 GiB per-query ceiling — per query,
// because a database-wide one dies in parameter derivation, where
// `GraphTraverseExec` asks 614.7 MB of a 409.9 MB remainder.
//
// Thirteen of the fourteen complete. The one that refuses is **IC14**, and the
// refusal names `GraphTraverseExec` asking **4.2 GB** — an operator that already
// reserved before this change, and not one of the sixteen.
//
// IC14 was also the obvious attribution for `GraphShortestPathExec`: it is
// `allShortestPaths`, and `docs/proposals/ldbc_findings_remediation_2026-08-27.md`
// records it as "executes; killed by hand after 111 min" at "19.2 GB and
// climbing". The pool refuses it long before the enumeration, on a different
// operator entirely. #261 warns in as many words that this repository has twice
// attributed an LDBC peak to a mechanism the query did not use; this would have
// been the third.
//
// Two caveats, both against reading this as "the corpus is fine": four of the
// fourteen (IC2, IC7, IC8, IC9) returned **zero rows**, so their parameters
// select nothing and they measure nothing — the same class of problem as #227.
// And the doc's 29–45 GB figures are process-wide `VmHWM` across a whole bench
// child, never per-query peaks, so they were not comparable with a per-query
// ceiling to begin with.
//
// So these fixes are preventive, not a repair of a measured failure: they turn a
// class of silent overrun into a refusal that names the operator. The evidence
// for each is its own fixture below, where the cost is constructed and measured
// — never an LDBC number it would have been convenient to claim.
//
// **The assertion is a required failure.** An unaccounted allocation passes
// every ceiling, so no successful query can witness one. Each test sets
// `max_memory` below what the operator holds and requires a refusal *naming the
// operator*, because the post-hoc result-size check rejects these same queries
// at these same ceilings for an unrelated reason, and an assertion that accepts
// any memory error cannot tell the two apart.

/// `allShortestPaths` enumerates every shortest path into memory at once.
///
/// `compute_all_shortest_paths` finds the target by layered BFS and then walks
/// `predecessors` backwards, pushing a **cloned partial path per branch** onto a
/// DFS stack and collecting every complete one into a `Vec<Vec<Vid>>`. The
/// number of shortest paths between two vertices is the product of the
/// predecessor counts along the layers, so it is combinatorial in the graph, not
/// linear in it — and nothing bounded or counted it.
///
/// The fixture makes that explicit at a size the suite can afford: three middle
/// layers of `WIDTH` nodes, fully connected layer to layer, so every path
/// `S -> L1 -> L2 -> L3 -> T` is a shortest path and there are `WIDTH.pow(3)` of
/// them. At `WIDTH = 40` that is 122 vertices and 3 280 edges producing **64 000
/// paths** — a graph small enough to build in a test, holding an intermediate
/// three orders of magnitude larger than itself.
///
/// The ceiling is applied **per query**, not to the database: an earlier version
/// configured the whole `Uni` and the *fixture build* hit the limit first, in
/// `HashJoinInput`, so the test failed without the operator under test ever
/// running. Both arms also carry a generous explicit timeout, so a refusal can
/// never be a disguised `Operation timed out`.
#[tokio::test]
async fn a_shortest_path_search_accounts_for_the_paths_it_enumerates() -> Result<()> {
    const WIDTH: usize = 40;
    const QUERY: &str = "MATCH (s:P {tag: 'S'}), (t:P {tag: 'T'}) \
                         MATCH p = allShortestPaths((s)-[:R*]-(t)) \
                         RETURN count(p) AS c";

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("tag", uni_db::DataType::String)
        .done()
        .edge_type("R", &["P"], &["P"])
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:P {tag: 'S'}) CREATE (:P {tag: 'T'})")
        .await?;
    for layer in 1..=3 {
        tx.execute(&format!(
            "UNWIND range(0, {}) AS i CREATE (:P {{tag: 'L{layer}'}})",
            WIDTH - 1
        ))
        .await?;
    }
    tx.execute("MATCH (s:P {tag: 'S'}), (a:P {tag: 'L1'}) CREATE (s)-[:R]->(a)")
        .await?;
    tx.execute("MATCH (a:P {tag: 'L1'}), (b:P {tag: 'L2'}) CREATE (a)-[:R]->(b)")
        .await?;
    tx.execute("MATCH (b:P {tag: 'L2'}), (c:P {tag: 'L3'}) CREATE (b)-[:R]->(c)")
        .await?;
    tx.execute("MATCH (c:P {tag: 'L3'}), (t:P {tag: 'T'}) CREATE (c)-[:R]->(t)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // 1 MiB: above everything the 122-vertex graph itself needs, and reached
    // after roughly a quarter of the enumeration, so the refusal arrives early
    // rather than at the very end of a long walk.
    match session
        .query_with(QUERY)
        .max_memory(1024 * 1024)
        .timeout(Duration::from_secs(300))
        .fetch_all()
        .await
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("GraphShortestPathExec"),
                "the refusal must name the operator that asked, not come from \
                 some other ceiling this query also exceeds: {msg}"
            );
        }
        Ok(rows) => panic!(
            "a 1 MiB ceiling accepted an enumeration of {:?} shortest paths; the \
             operator is still allocating outside the pool",
            rows.rows()[0].values()[0]
        ),
    }

    // The control. `count(p)` returns one row, so nothing but the enumeration
    // itself can be what the tight ceiling rejected — and at a ceiling that
    // fits it, the answer must still be WIDTH^3.
    let rows = session
        .query_with(QUERY)
        .max_memory(512 * 1024 * 1024)
        .timeout(Duration::from_secs(300))
        .fetch_all()
        .await?;
    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int((WIDTH * WIDTH * WIDTH) as i64),
        "the reservation must not change the answer"
    );
    Ok(())
}

/// `OPTIONAL MATCH ... WHERE` buffers one batch per unmatched source group.
///
/// `OptionalFilterStream` cannot decide a group's NULL-recovery row until the
/// input is exhausted, because a group that fails in this batch may pass in a
/// later one. So it holds `passed_keys` for every group that ever passed, and
/// `pending_null` — a **one-row `RecordBatch` per group that has not** — to
/// end-of-stream. One-row Arrow batches are dominated by per-column buffer
/// overhead rather than by their single row, so a wide schema makes each one
/// cost far more than the row it carries.
///
/// This is a semantically required barrier: unlike an unwind, it cannot be
/// chunked without changing the answer. Reserving is therefore the whole of the
/// fix — it converts a silent overrun into a clean refusal, which is what #261
/// says such an operator is owed.
#[tokio::test]
async fn an_optional_filter_accounts_for_the_null_rows_it_buffers() -> Result<()> {
    const ROWS: usize = 60_000;
    // Every source row fails the predicate, so **every** group lands in
    // `pending_null` and none is ever cancelled — the operator's worst case, and
    // the one an ordinary OPTIONAL MATCH reaches whenever the optional side
    // rarely matches.
    const QUERY: &str = "MATCH (a:Src) OPTIONAL MATCH (a)-[:E]->(b:Dst) \
                         WHERE b.k < 0 RETURN count(a) AS c";

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Src")
        .property("k", uni_db::DataType::Int64)
        .done()
        .label("Dst")
        .property("k", uni_db::DataType::Int64)
        .done()
        .edge_type("E", &["Src"], &["Dst"])
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:Src {{k: i}})",
        ROWS - 1
    ))
    .await?;
    // **One** shared target, not a handful: the operator's cost is in the number
    // of *source* groups, so extra edges buy this test nothing.
    //
    // Four targets is also what exposed the buffer-sharing defect described on
    // `BatchFootprint`. The seeding refused at 968 MB against the shipped 1 GiB
    // default — and went on refusing at 966 MB with a quarter of the edges and
    // at 1021 MB with a sixth of the rows. A charge that does not move when its
    // input is cut by four is not measuring its input, which is what sent the
    // investigation to the measure rather than to the fixture.
    tx.execute("CREATE (:Dst {k: 0})").await?;
    tx.execute("MATCH (a:Src), (b:Dst) CREATE (a)-[:E]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    match session
        .query_with(QUERY)
        .max_memory(8 * 1024 * 1024)
        .timeout(Duration::from_secs(300))
        .fetch_all()
        .await
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("OptionalFilterExec"),
                "the refusal must name the operator that asked: {msg}"
            );
        }
        Ok(rows) => panic!(
            "an 8 MiB ceiling accepted {ROWS} buffered NULL-recovery rows \
             (returned {:?}); the operator is still allocating outside the pool",
            rows.rows()[0].values()[0]
        ),
    }

    let rows = session
        .query_with(QUERY)
        .max_memory(512 * 1024 * 1024)
        .timeout(Duration::from_secs(300))
        .fetch_all()
        .await?;
    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(ROWS as i64),
        "the reservation must not change the answer"
    );
    Ok(())
}

/// UNWIND accounts for the list it is part way through.
///
/// This operator is the one case on #261's list where the *bounding* step had
/// already happened: #241 chunked the output to `chunk_size x columns`, which is
/// why its peak is not the fan-out. What it still retains across polls is the
/// input batch and the remainder of the single list being expanded — and #184's
/// shape is exactly one enormous collected list, which chunking cannot split
/// because every element is legitimately live.
///
/// # Why there is no `collect()` here
///
/// The first version of this test built the list with
/// `MATCH (n:U) WITH collect(n.k) AS big UNWIND big AS y`, on the theory that
/// the aggregate would hold the list Arrow-encoded at a few bytes an element
/// while the unwind held its remainder as `Vec<Value>` at several times that,
/// leaving a band between them. There is no band: `AggregateStream` grew to
/// **5.5 MB under a 6 MiB ceiling and to 15.0 MB under a 16 MiB one**, on the
/// same 300 000 values. It expands to fill whatever it is given, so it refuses
/// first at every ceiling and the operator under test never runs.
///
/// `UNWIND range(...)` produces the same one-huge-list shape with nothing else
/// in the plan, so the only consumer that can refuse is the one being tested.
#[tokio::test]
async fn an_unwind_accounts_for_the_list_it_is_expanding() -> Result<()> {
    const N: i64 = 2_000_000;
    let query = format!("UNWIND range(0, {}) AS x RETURN count(x) AS c", N - 1);

    let db = Uni::in_memory().build().await?;
    let session = db.session();

    match session
        .query_with(&query)
        .max_memory(8 * 1024 * 1024)
        .timeout(Duration::from_secs(300))
        .fetch_all()
        .await
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("GraphUnwindExec"),
                "the refusal must name the operator that asked: {msg}"
            );
        }
        Ok(rows) => panic!(
            "an 8 MiB ceiling accepted an unwind of {N} elements (returned {:?}); \
             the operator is still allocating outside the pool",
            rows.rows()[0].values()[0]
        ),
    }

    let rows = session
        .query_with(&query)
        .max_memory(512 * 1024 * 1024)
        .timeout(Duration::from_secs(300))
        .fetch_all()
        .await?;
    assert_eq!(
        rows.rows()[0].values()[0],
        uni_db::Value::Int(N),
        "the reservation must not change the answer"
    );
    Ok(())
}

/// A traversal reserves the target read it shares across its output chunks.
///
/// Hydration reads the whole expansion set once, in `_vid` order, so that each
/// `_vid IN (...)` scan covers a narrow key range instead of the whole table.
/// That read then stays resident for the entire chunking loop while the chunks
/// gather from it -- a second thing the traversal holds, beside the expansion
/// set and input it already reserved, and the pool has to see it.
///
/// **The expansion set must exceed one output slice**, or the traversal takes
/// its single-batch arm, hydrates once inside one call, and there is no shared
/// read to charge for. Hence one hub with more edges than `batch_size`.
///
/// `count(t.name)`, not `t.name`: one row out, so the post-hoc result-size check
/// cannot see this query and the only thing that can refuse it is the pool.
///
/// Like the scan's own reservation, this charge is taken after the read exists,
/// so it bounds how long an over-budget result survives rather than preventing
/// its construction.
#[tokio::test]
async fn a_traversal_reserves_the_target_read_it_shares() -> Result<()> {
    const TARGETS: usize = 20_000;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Hub")
        .property("id", uni_db::DataType::Int64)
        .done()
        .label("Leaf")
        .property("name", uni_db::DataType::String)
        .done()
        .edge_type("TO", &["Hub"], &["Leaf"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Hub {id: 0})").await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:Leaf {{name: \
         'a-name-long-enough-that-twenty-thousand-of-them-are-megabytes-' + toString(i)}})",
        TARGETS - 1
    ))
    .await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    tx.execute("MATCH (h:Hub), (l:Leaf) CREATE (h)-[:TO]->(l)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = session
        .query_with("MATCH (h:Hub)-[:TO]->(t:Leaf) RETURN count(t.name) AS c")
        .max_memory(1024 * 1024)
        .fetch_all()
        .await;

    match res {
        Err(e) => {
            let msg = e.to_string();
            // Naming the operator is what makes this discriminating: a message
            // that merely mentions memory passes with the charge removed, since
            // some other consumer refuses this query at a tight ceiling too.
            assert!(
                msg.contains("GraphTraverseExec"),
                "the refusal must come from the traversal's own reservation, not \
                 from some other limit that happens to reject this query: {msg}"
            );
        }
        Ok(rows) => panic!(
            "a 1 MiB ceiling accepted a traversal holding {TARGETS} hydrated \
             targets (counted {:?}); the shared read is being held outside the \
             pool again",
            rows.rows()[0].values()[0]
        ),
    }

    // The control: the same query, the same shared read, a ceiling that fits.
    // Without it a charge that always refuses would pass the assertion above.
    let ok = session
        .query_with("MATCH (h:Hub)-[:TO]->(t:Leaf) RETURN count(t.name) AS c")
        .max_memory(64 * 1024 * 1024)
        .fetch_all()
        .await?;
    assert_eq!(ok.rows().len(), 1, "the control query must answer");

    Ok(())
}

// ---------------------------------------------------------------------------
// Issue #285 / #284 — bounding variable-length path enumeration.
// ---------------------------------------------------------------------------

/// Builds a small strongly-connected graph: `n` entities, each with `out`
/// outgoing edges. Cycles are the point — trail counts explode with them, and
/// mutual cross-holdings are ordinary in the corporate-ownership data this came
/// from.
async fn cyclic_graph(db: &Uni, n: usize, out: usize) {
    db.schema()
        .label("Entity")
        .property("uid", DataType::String)
        .done()
        .edge_type("OWNS", &["Entity"], &["Entity"])
        .done()
        .apply()
        .await
        .unwrap();

    let tx = db.session().tx().await.unwrap();
    for i in 0..n {
        tx.execute_with("CREATE (:Entity {uid: $u})")
            .param("u", format!("e{i}"))
            .run()
            .await
            .unwrap();
    }
    for i in 0..n {
        for k in 0..out {
            let dst = (i * 7 + k * 13 + 1) % n;
            tx.execute_with(
                "MATCH (a:Entity {uid: $a}), (b:Entity {uid: $b}) CREATE (a)-[:OWNS]->(b)",
            )
            .param("a", format!("e{i}"))
            .param("b", format!("e{dst}"))
            .run()
            .await
            .unwrap();
        }
    }
    tx.commit().await.unwrap();
}

/// An unbounded variable-length path over a cyclic graph must fail against its
/// memory limit rather than growing until the OS intervenes (issue #285).
///
/// The BFS is cheap and terminates; the cost is the enumeration that follows
/// it, which keeps a node path and an edge path per trail. The stream reserved
/// for that set only *after* building it, so the pool could never refuse it —
/// which is also why `max_memory` overshot by 12x (issue #284). The budget
/// inside the enumeration is what makes the limit bind.
///
/// The bounded arm is the control: same graph, same query shape, an upper hop
/// bound. It must still succeed, or this test would pass on a build that simply
/// refused all variable-length queries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unbounded_variable_length_path_is_bounded_by_max_memory() {
    let db = Uni::in_memory().build().await.unwrap();
    cyclic_graph(&db, 30, 3).await;

    let bounded = db
        .session()
        .query_with("MATCH p=(a:Entity)-[:OWNS*1..4]->(b:Entity) RETURN count(p) AS n")
        .max_memory(256 * 1024 * 1024)
        .fetch_all()
        .await;
    let bounded = bounded.expect("control: a hop-bounded path query must still succeed");
    let counted: i64 = bounded.rows()[0].get("n").unwrap();
    assert!(
        counted > 0,
        "control: the bounded query found no paths, so the graph is not cyclic \
         enough for the unbounded arm to mean anything"
    );

    let err = db
        .session()
        .query_with("MATCH p=(a:Entity)-[:OWNS*]->(b:Entity) RETURN count(p) AS n")
        .max_memory(64 * 1024 * 1024)
        .fetch_all()
        .await
        .expect_err("an unbounded path query over a cyclic graph must hit a limit");

    // Which limit stops it changed with #285, and the change was the point.
    //
    // This used to exhaust the pool, because the operator built a source
    // vertex's entire path set before it could emit anything. It now
    // enumerates in bounded batches, so memory stays inside the budget and the
    // deadline is what stops it. Counting every path of an unbounded pattern
    // over a cyclic graph is unbounded *work*, so a clock bound is the honest
    // one; the assertion is that some declared limit stops it, which is the
    // guarantee that matters. That `max_memory` still binds on this operator
    // is asserted directly by `locy_max_memory_bounds_the_evaluation` (which
    // trips inside `GraphVariableLengthTraverse[enumeration]`) and by
    // `a_chunking_traversal_accounts_for_its_retained_expansions`.
    let msg = err.to_string();
    let lowered = msg.to_lowercase();
    assert!(
        msg.contains("Resources exhausted")
            || lowered.contains("memory")
            || lowered.contains("timed out"),
        "the failure must name a declared limit, not surface as something else: {msg}"
    );
}

/// Issue #285, the other half: the same unbounded pattern under a `LIMIT`
/// must *answer*, not hit a limit at all.
///
/// Before the enumeration could be paused, a `LIMIT` bought nothing — the
/// operator built every path a source vertex owned inside a single poll, so
/// `LIMIT 5` cost exactly what no limit cost and died the same way. Measured on
/// a 852-entity sanctions-shaped graph: a 30s timeout before, ~4s and five rows
/// after.
///
/// This is the arm that would catch the fix regressing. The arm above only says
/// the query is *stopped* by something, which a build with no laziness at all
/// still satisfies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unbounded_path_query_under_a_limit_answers() {
    let db = Uni::in_memory().build().await.unwrap();
    cyclic_graph(&db, 30, 3).await;

    for limit in [1usize, 7, 40] {
        let rows = db
            .session()
            .query_with(&format!(
                "MATCH p=(a:Entity)-[:OWNS*]->(b:Entity) RETURN length(p) AS hops LIMIT {limit}"
            ))
            .max_memory(64 * 1024 * 1024)
            .fetch_all()
            .await
            .unwrap_or_else(|e| panic!("unbounded pattern with LIMIT {limit} must answer: {e}"));
        assert_eq!(
            rows.rows().len(),
            limit,
            "LIMIT {limit} over an unbounded pattern returned the wrong row count"
        );
    }
}

/// Issue #284: `locy_with` had no `max_memory`, while `query_with` did.
///
/// Locy has always run through the same DataFusion planner and so has always
/// been bounded by the database-level `max_query_memory` — what it lacked was
/// the per-evaluation knob. A setter that writes a field nothing reads would
/// satisfy an API-surface check while changing nothing, which has happened on
/// this builder before (its `cancellation_token` did exactly that), so this
/// asserts the bound actually binds: the error must name the pool size that was
/// asked for, not the database default.
///
/// The bound is 2 MB rather than the 32 MB this was first written with. Since
/// #285 the variable-length operator enumerates paths in bounded batches
/// instead of building a source vertex's whole path set at once, so this shape
/// no longer *reaches* 32 MB — it is the same knob, asked at a size the work
/// still exceeds. What the assertion checks is unchanged: the figure in the
/// error is the one this call asked for, not the database default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locy_max_memory_bounds_the_evaluation() {
    let db = Uni::in_memory().build().await.unwrap();
    cyclic_graph(&db, 30, 3).await;

    let err = db
        .session()
        .locy_with("MATCH p=(a:Entity)-[:OWNS*]->(b:Entity) RETURN count(p) AS n")
        .max_memory(2 * 1024 * 1024)
        .run()
        .await
        .expect_err("an unbounded path query must hit the configured memory bound");

    let msg = err.to_string();
    assert!(
        msg.contains("2.0 MB"),
        "the evaluation was bounded by something other than the requested 2 MB \
         — a setter that is not read would fail exactly here: {msg}"
    );
}

/// Control for the above: the same limit on a program that fits must succeed,
/// so the test above cannot pass merely because the limit breaks everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locy_max_memory_leaves_a_fitting_program_alone() {
    let db = Uni::in_memory().build().await.unwrap();
    cyclic_graph(&db, 30, 3).await;

    let result = db
        .session()
        .locy_with("MATCH (a:Entity) RETURN count(a) AS n")
        .max_memory(32 * 1024 * 1024)
        .run()
        .await;
    assert!(
        result.is_ok(),
        "a program that fits within the bound must still run: {:?}",
        result.err()
    );
}

/// Issue #283: `locy_with(..).timeout(..)` did not bound execution.
///
/// The Locy budget was consulted only between strata, between fixpoint
/// iterations and in the SLG loop. A program that crossed none of those
/// boundaries ran to completion and reported nothing — a complete result and no
/// error, which is worse than a slow one. The budget also never reached the
/// operators: `LocyEngine` handed the executor the *database* config, so the
/// deadline every operator checks was `db.query_timeout` and a 2s Locy budget
/// was invisible to all of them.
///
/// This drives work that lives inside a graph operator — path enumeration — so
/// the deadline has somewhere to be observed. The wall-clock bound is generous
/// relative to the 2s budget because the check is amortized across a stride of
/// enumerated paths; it is still far below the several seconds the same query
/// takes unbounded, which the control establishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locy_timeout_interrupts_work_inside_an_operator() {
    let db = Uni::in_memory().build().await.unwrap();
    cyclic_graph(&db, 30, 3).await;

    let started = Instant::now();
    let err = db
        .session()
        .locy_with("MATCH p=(a:Entity)-[:OWNS*]->(b:Entity) RETURN count(p) AS n")
        .timeout(Duration::from_secs(2))
        .run()
        .await
        .expect_err("a 2s budget must stop this, not describe it afterwards");
    let elapsed = started.elapsed();

    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("timed out") || msg.to_lowercase().contains("timeout"),
        "expected a timeout, got: {msg}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the budget was reported rather than enforced: {elapsed:?}"
    );
}

/// Issue #283: a timeout must stop a query, not describe it afterwards.
///
/// A pipeline-breaking operator — here the aggregate under `count(*)` —
/// consumes its whole input inside one `poll_next`, so the per-batch check in
/// the collecting loop above it runs once at the start and once after the work
/// is over. `tokio::time::timeout` cannot preempt it either, because a
/// CPU-bound span that never yields never lets the timer run. What was left was
/// an `Instant::now() > deadline` test after the rows existed: a report, not a
/// limit. `DeadlineGuardExec` puts a checkpoint on the pull path beneath the
/// breaker, where the polling still repeats.
///
/// The wall-clock assertion is what makes this test mean anything — the query
/// returned the right answer before, just twenty seconds late. The control
/// establishes the query is genuinely slow, so a fast failure is enforcement
/// rather than the fixture being trivial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_is_enforced_under_a_pipeline_breaking_operator() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Entity")
        .property("uid", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    for i in 0..600 {
        tx.execute_with("CREATE (:Entity {uid: $u})")
            .param("u", format!("e{i}"))
            .run()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    // Three-way cartesian under an aggregate: 600^3 rows to count.
    const SLOW: &str = "MATCH (a:Entity),(b:Entity),(c:Entity) RETURN count(*) AS n";

    // The guard is actually in this plan. Without this, a fast failure could
    // come from anywhere and the test would still be green.
    let session = db.session();
    crate::plan_shape::assert_plan_uses(&session, SLOW, "DeadlineGuardExec").await;

    let started = Instant::now();
    let err = db
        .session()
        .query_with(SLOW)
        .timeout(Duration::from_secs(2))
        .fetch_all()
        .await
        .expect_err("a 2s timeout must stop this query");
    let elapsed = started.elapsed();

    assert!(
        err.to_string().to_lowercase().contains("time"),
        "expected a timeout, got: {err}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the deadline was reported rather than enforced: {elapsed:?}"
    );
}

/// Issue #283, the Locy half: `locy_with(..).timeout(..)` on the same shape.
///
/// Two things kept this unbounded after the Cypher side was fixed. No guard was
/// inserted, because `ReadSetRecordingExec` sits over every clause body under
/// SSI and the subtree test refused to insert anywhere beneath one. And the
/// Locy budget never reached the operators at all, which the engine's
/// `executor_config` now handles.
///
/// The `query_with` sibling above covers the Cypher path; this pins that the
/// Locy path is bounded on the *same* query, since it was the one that reported
/// nothing whatsoever — a complete result after sixteen seconds against a
/// two-second budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_locy_deadline_is_enforced_on_a_pipeline_breaking_plan() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Entity")
        .property("uid", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    for i in 0..600 {
        tx.execute_with("CREATE (:Entity {uid: $u})")
            .param("u", format!("e{i}"))
            .run()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let started = Instant::now();
    let err = db
        .session()
        .locy_with("MATCH (a:Entity),(b:Entity),(c:Entity) RETURN count(*) AS n")
        .timeout(Duration::from_secs(2))
        .run()
        .await
        .expect_err("a 2s budget must stop this, not return a complete result");
    let elapsed = started.elapsed();

    assert!(
        err.to_string().to_lowercase().contains("time"),
        "expected a timeout, got: {err}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the budget was ignored rather than enforced: {elapsed:?}"
    );
}

/// A database whose `query_timeout` is far below what a three-way cartesian
/// over `n` entities takes, for the #289 tests.
async fn short_default_timeout_db(n: usize, query_timeout: Duration) -> Uni {
    let config = uni_db::UniConfig {
        query_timeout,
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await.unwrap();
    db.schema()
        .label("Entity")
        .property("uid", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    for i in 0..n {
        tx.execute_with("CREATE (:Entity {uid: $u})")
            .param("u", format!("e{i}"))
            .run()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db
}

const CARTESIAN: &str = "MATCH (a:Entity),(b:Entity),(c:Entity) RETURN count(*) AS n";

/// Issue #289: an explicit Locy timeout above the database's `query_timeout`
/// was silently discarded.
///
/// `executor_config` combined the two with `min`, because `LocyConfig::timeout`
/// was a plain 300s default that could not be told apart from a value the
/// caller asked for. So `locy_with(q).timeout(120s)` ran under the database's
/// 30s and died there, while `query_with(q).timeout(120s)` was honoured.
///
/// The database default here is 250ms so the program crosses it quickly. The
/// first arm is the control: with no Locy timeout the database value must still
/// bind — that is the behaviour `min` existed to protect, and it is also what
/// proves the program genuinely outlasts 250ms, so the explicit arms completing
/// is the timeout being honoured rather than the fixture being trivial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_explicit_locy_timeout_above_the_database_default_is_honoured() {
    let db = short_default_timeout_db(250, Duration::from_millis(250)).await;
    let session = db.session();

    let err = session
        .locy_with(CARTESIAN)
        .run()
        .await
        .expect_err("with no Locy timeout the 250ms database default must still bind");
    assert!(
        matches!(err, uni_db::UniError::Timeout { timeout_ms: 250 }),
        "expected the database's 250ms timeout, got: {err:?}"
    );

    let via_builder = session
        .locy_with(CARTESIAN)
        .timeout(Duration::from_secs(120))
        .run()
        .await;
    assert!(
        via_builder.is_ok(),
        "`.timeout(120s)` was capped at the database default: {:?}",
        via_builder.err()
    );

    // The same request through a whole `LocyConfig`, the other documented way
    // to set it — a fix that honoured only the builder setter would fail here.
    let via_config = session
        .locy_with(CARTESIAN)
        .with_config(uni_db::locy::LocyConfig {
            timeout: Some(Duration::from_secs(120)),
            ..Default::default()
        })
        .run()
        .await;
    assert!(
        via_config.is_ok(),
        "`LocyConfig {{ timeout: Some(120s) }}` was capped at the database default: {:?}",
        via_config.err()
    );
}

/// Issue #289: a Locy program refused for cost raised the same `UniError::Query`
/// as a program that was simply wrong, so only the message text told them
/// apart. Each refusal must carry the variant the Cypher path raises for the
/// same condition, with the budget that was actually applied.
///
/// The last arm is the control: an ordinary broken program must stay
/// `UniError::Query`, or this would pass on a mapper that typed everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locy_cost_refusals_are_typed_and_distinct_from_broken_programs() {
    let db = short_default_timeout_db(250, Duration::from_secs(30)).await;
    let err = db
        .session()
        .locy_with(CARTESIAN)
        .timeout(Duration::from_millis(300))
        .run()
        .await
        .expect_err("a 300ms budget must stop the cartesian");
    assert!(
        matches!(err, uni_db::UniError::Timeout { timeout_ms: 300 }),
        "a Locy timeout must be UniError::Timeout carrying its budget, got: {err:?}"
    );

    let db = Uni::in_memory().build().await.unwrap();
    cyclic_graph(&db, 30, 3).await;
    let session = db.session();

    let err = session
        .locy_with("MATCH p=(a:Entity)-[:OWNS*]->(b:Entity) RETURN count(p) AS n")
        .max_memory(2 * 1024 * 1024)
        .run()
        .await
        .expect_err("an unbounded path query must hit the 2 MB pool");
    assert!(
        matches!(
            err,
            uni_db::UniError::MemoryLimitExceeded { limit_bytes, .. } if limit_bytes == 2 * 1024 * 1024
        ),
        "a refused pool reservation must be MemoryLimitExceeded(2 MB), got: {err:?}"
    );

    // `max_derived_bytes` is a different budget from the pool — one derived
    // relation's retained facts — raised by the fixpoint rather than an
    // operator, so it is classified on a separate arm.
    let err = session
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:Entity)-[:OWNS]->(b:Entity) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:Entity)-[:OWNS]->(mid:Entity) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .with_config(uni_db::locy::LocyConfig {
            max_derived_bytes: 1024,
            ..Default::default()
        })
        .run()
        .await
        .expect_err("30 x 30 reachable pairs cannot fit in 1 KiB of derived facts");
    assert!(
        matches!(
            err,
            uni_db::UniError::MemoryLimitExceeded {
                limit_bytes: 1024,
                ..
            }
        ),
        "a derived relation over max_derived_bytes must be MemoryLimitExceeded(1 KiB), \
         got: {err:?}"
    );

    let err = session
        .locy("MATCH (e:Entity) RETURN nosuchfn(e.uid) AS x LIMIT 1")
        .await
        .expect_err("an unknown function is a broken program");
    assert!(
        matches!(err, uni_db::UniError::Query { .. }),
        "a broken program must stay UniError::Query, got: {err:?}"
    );
}

/// Issue #289, the Cypher half: `UniError::MemoryLimitExceeded` existed and the
/// bindings mapped it to `UniMemoryLimitExceededError`, but nothing constructed
/// it — both the pool refusal and the result-size estimate were
/// `UniError::Query`. The two Cypher mechanisms are pinned separately because
/// they are raised at different layers.
#[tokio::test]
async fn cypher_memory_refusals_are_typed() -> Result<()> {
    let db = seeded_db().await?;
    let err = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .max_memory(100)
        .fetch_all()
        .await
        .expect_err("a 100-byte pool cannot hold a scan batch");
    assert!(
        matches!(
            err,
            uni_db::UniError::MemoryLimitExceeded {
                limit_bytes: 100,
                ..
            }
        ),
        "a refused pool reservation must be MemoryLimitExceeded, got: {err:?}"
    );

    let db = db_with_memory_limit(64 * 1024).await?;
    let err = db
        .session()
        .query("RETURN reduce(s = '', x IN range(0, 4000) | s + '0123456789abcdefghij') AS big")
        .await
        .expect_err("an ~80 KB result must exceed a 64 KiB ceiling");
    assert!(
        matches!(
            err,
            uni_db::UniError::MemoryLimitExceeded { limit_bytes, .. } if limit_bytes == 64 * 1024
        ),
        "the result-size estimate must be MemoryLimitExceeded, got: {err:?}"
    );
    Ok(())
}
