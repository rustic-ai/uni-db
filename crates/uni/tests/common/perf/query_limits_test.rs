// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::time::Duration;
use uni_db::Uni;

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
    // `TxQueryBuilder` has no `.max_memory()`, so the ceiling comes from
    // `UniConfig`. It was inert on both tx terminals.
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
    let db = db_with_memory_limit(4 * 1024 * 1024).await?;
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

    let res = session
        .query("MATCH (a:Source) MATCH (b:Target) WHERE id(b) = a.linked_vid RETURN b.name AS bn")
        .await;

    match res {
        Err(e) => {
            let msg = e.to_string();
            // Naming the operator is what makes this discriminating. An earlier
            // version accepted any message containing "memory" and passed with
            // the reservations removed, because the post-hoc result-size check
            // rejects this query at this ceiling too — 20k rows of names exceed
            // it on their own. Two mechanisms, one indistinguishable assertion.
            // The pool names the consumer that asked; the result-size check
            // cannot.
            assert!(
                msg.contains("VidLookupJoinExec"),
                "the refusal must come from the join's own reservation, not from \
                 some other limit that happens to reject this query: {msg}"
            );
        }
        Ok(rows) => panic!(
            "a 4 MiB ceiling accepted a join that materialized {} rows; the \
             operator is allocating outside the pool again",
            rows.rows().len()
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
    // 256 KiB: comfortably above what the seeding writes need, far below the
    // distinct-value hash table built below.
    let db = db_with_memory_limit(256 * 1024).await?;
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
    let res = db
        .session()
        .query("MATCH (n:W) WITH n.k AS k, count(*) AS per RETURN count(k) AS c")
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
    let db = db_with_memory_limit(256 * 1024).await?;
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

    match db.session().query("MATCH (n:W) RETURN n.k AS k").await {
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
