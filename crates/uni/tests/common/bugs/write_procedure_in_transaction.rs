// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A declared `WRITE` procedure's writes belong to the calling transaction.
//!
//! The procedure body runs as an inner query. That inner executor was built
//! with the writer but not with the transaction's private L0, so its
//! mutations went straight into the shared main L0: they survived
//! `rollback()`, were visible to other sessions before `commit()`, and were
//! invisible to the transaction's own later reads. This held for a
//! standalone `CALL` and for a per-row `CALL` inside a query.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(write_procedure_in_transaction)'

// Rust guideline compliant

use std::sync::Arc;

use anyhow::Result;
use uni_db::{Session, Uni};
use uni_plugin::traits::connector::Principal;
use uni_plugin::{Capability, CapabilitySet};

/// A database with `t.mark()` (creates one `:Marker`) and `t.bump()` (adds
/// one to the `:Counter {k: 'a'}` node's `n`), and a session allowed to call them.
async fn with_write_procedures() -> Result<(Uni, Session)> {
    let db = Uni::in_memory().build().await?;
    let mut caps = CapabilitySet::new();
    caps.insert(Capability::ProcedureWrites);
    let writer = Arc::new(Principal {
        id: "admin".to_owned(),
        groups: vec!["admin".to_owned()],
        capabilities: caps,
    });
    let session = db.session().with_principal(writer);
    let tx = session.tx().await?;
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.mark', 'CREATE (:Marker)', 'WRITE', '[]', '[]')",
    )
    .await?;
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.bump', \
         'MATCH (c:Counter {k: \"a\"}) SET c.n = c.n + 1', 'WRITE', '[]', '[]')",
    )
    .await?;
    tx.execute("CREATE (:Counter {k: 'a', n: 0})").await?;
    tx.commit().await?;
    Ok((db, session))
}

/// Markers visible to a fresh session (committed state).
async fn committed_markers(db: &Uni) -> Result<i64> {
    let res = db
        .session()
        .query("MATCH (m:Marker) RETURN count(m) AS n")
        .await?;
    Ok(res.rows()[0].get::<i64>("n")?)
}

/// The standalone and the per-row form of the same write.
const CALLS: [&str; 2] = [
    "CALL t.mark() YIELD row_json RETURN row_json",
    "UNWIND [1, 2] AS x CALL t.mark() YIELD row_json RETURN x",
];

/// Markers each form of [`CALLS`] creates.
const MADE: [i64; 2] = [1, 2];

/// Requires the multi-thread runtime: the declared procedure bridges
/// sync→async with `block_in_place`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_procedure_in_transaction_rollback_discards_writes() -> Result<()> {
    let (db, session) = with_write_procedures().await?;
    for call in CALLS {
        let tx = session.tx().await?;
        tx.query(call).await?;
        tx.rollback();
        assert_eq!(committed_markers(&db).await?, 0, "rolled back: {call}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_procedure_in_transaction_reads_its_own_writes_and_isolates_them() -> Result<()> {
    let (db, session) = with_write_procedures().await?;
    let mut total = 0;
    for (call, made) in CALLS.into_iter().zip(MADE) {
        let tx = session.tx().await?;
        tx.query(call).await?;
        let inside = tx.query("MATCH (m:Marker) RETURN count(m) AS n").await?;
        assert_eq!(
            inside.rows()[0].get::<i64>("n")?,
            total + made,
            "the transaction sees its own procedure writes: {call}"
        );
        assert_eq!(
            committed_markers(&db).await?,
            total,
            "other sessions must not see uncommitted writes: {call}"
        );
        // A later procedure call in the same transaction reads them too.
        tx.query("CALL t.mark() YIELD row_json RETURN row_json")
            .await?;
        tx.commit().await?;
        total += made + 1;
        assert_eq!(committed_markers(&db).await?, total, "committed: {call}");
    }
    Ok(())
}

/// Session::run auto-commits a write procedure, as it does any write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_procedure_in_transaction_autocommit_persists() -> Result<()> {
    let (db, session) = with_write_procedures().await?;
    session
        .run("CALL t.mark() YIELD row_json RETURN row_json")
        .await?;
    assert_eq!(committed_markers(&db).await?, 1);
    Ok(())
}

/// Two transactions that both read-modify-write the same node through a
/// procedure conflict exactly as two that do it with plain Cypher: the
/// second commit is refused and no increment is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_procedure_in_transaction_conflicts_like_plain_writes() -> Result<()> {
    let bodies = [
        "CALL t.bump() YIELD row_json RETURN row_json",
        "MATCH (c:Counter {k: 'a'}) SET c.n = c.n + 1",
    ];
    let mut outcomes = Vec::new();
    for body in bodies {
        let (db, session) = with_write_procedures().await?;
        // One write transaction per session, so the second one needs its own.
        let other = db
            .session()
            .with_principal(Arc::clone(session.principal().expect("set above")));
        let t1 = session.tx().await?;
        let t2 = other.tx().await?;
        t1.query(body).await?;
        t2.query(body).await?;
        let first = t1.commit().await.is_ok();
        let second = t2.commit().await.is_ok();
        let n = db
            .session()
            .query("MATCH (c:Counter {k: 'a'}) RETURN c.n AS n")
            .await?
            .rows()[0]
            .get::<i64>("n")?;
        // Either the second commit is refused, or both increments landed.
        assert!(
            (first && !second && n == 1) || (first && second && n == 2),
            "{body}: first={first} second={second} n={n}"
        );
        outcomes.push((first, second, n));
    }
    assert_eq!(
        outcomes[0], outcomes[1],
        "a procedure write must conflict as the same plain write does"
    );
    Ok(())
}
