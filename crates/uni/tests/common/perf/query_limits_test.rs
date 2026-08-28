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
    assert!(err_msg.contains("Query exceeded memory limit"));

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
    assert!(
        err.to_string().contains("Query exceeded memory limit"),
        "expected the memory-limit error `fetch_all` produces, got: {err}"
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
    assert!(
        err.to_string().contains("Query exceeded memory limit"),
        "expected the memory-limit error, got: {err}"
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
    assert!(
        err.to_string().contains("Query exceeded memory limit"),
        "expected the memory-limit error, got: {err}"
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

/// A database whose only unusual setting is a small query-memory ceiling.
async fn db_with_memory_limit(bytes: usize) -> Result<Uni> {
    let mut config = uni_db::UniConfig::default();
    config.max_query_memory = bytes;
    Ok(Uni::in_memory().config(config).build().await?)
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
