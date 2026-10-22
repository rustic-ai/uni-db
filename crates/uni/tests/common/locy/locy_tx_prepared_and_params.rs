// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A program prepared in a transaction runs as `tx.locy` does, and a
//! program's `$params` reach every command and the `tx.apply` replay.
//!
//! `tx.prepare_locy` built a session-style program: it read neither the
//! transaction's uncommitted writes nor its pinned snapshot, ignored its
//! cancellation scope, and collected its writes instead of writing them into
//! the transaction. Each case below runs the same program through `tx.locy`
//! and through the prepared program, in the same transaction state, and the
//! two must agree.
//!
//! A Locy Cypher command ran with no parameters once the program had written
//! (they are dispatched after the first write, outside the inline loop), so a
//! write stored NULL and a read failed with "Unresolved parameter"; and a
//! session program's writes were replayed by `tx.apply` with no parameters
//! either. The parameters are now passed to every command and stored with
//! the `DerivedFactSet` for the replay.

// Rust guideline compliant

use anyhow::Result;
use uni_db::locy::CommandResult;
use uni_db::{Uni, UniError, Value};

const READ_N: &str = "CREATE RULE r AS MATCH (n:N) YIELD KEY n, n.v AS v\nQUERY r RETURN v";

/// The rows a program's single QUERY returned, sorted `v` values.
fn query_values(result: &uni_db::locy::LocyResult) -> Vec<Value> {
    let rows = result
        .command_results
        .iter()
        .rev()
        .find_map(|c| match c {
            CommandResult::Query(rows) => Some(rows.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let mut values: Vec<Value> = rows
        .iter()
        .map(|r| r.get("v").cloned().unwrap_or(Value::Null))
        .collect();
    values.sort_by_key(|v| format!("{v:?}"));
    values
}

async fn count(db: &Uni, cypher: &str) -> Result<i64> {
    let rows = db.session().query(cypher).await?;
    Ok(rows.rows()[0].get::<i64>("c")?)
}

/// A tx-prepared program reads the transaction's own uncommitted write.
#[tokio::test]
async fn tx_prepared_program_reads_the_transactions_writes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {v: 7})").await?;

    let oracle = query_values(&tx.locy(READ_N).await?);
    assert_eq!(oracle, vec![Value::Int(7)]);
    let prepared = tx.prepare_locy(READ_N).await?;
    assert_eq!(query_values(&prepared.execute(&[]).await?), oracle);
    tx.rollback();
    Ok(())
}

/// A tx-prepared program's writes, DERIVE and Cypher alike, land in the
/// transaction: visible to it, invisible outside until commit, gone on
/// rollback, kept on commit.
#[tokio::test]
async fn tx_prepared_program_writes_into_the_transaction() -> Result<()> {
    const PROGRAM: &str = "\
CREATE RULE link AS MATCH (a:P)-[:KNOWS]->(b:P) DERIVE (a)-[:LINK]->(b)\n\
DERIVE link\n\
CREATE (:W {v: 1})";
    let db = Uni::in_memory().build().await?;
    let seed = db.session().tx().await?;
    seed.execute("CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})")
        .await?;
    seed.commit().await?;

    for commit in [false, true] {
        // The oracle, `tx.locy`, then the prepared program, each in a fresh
        // transaction.
        for prepared in [false, true] {
            let tx = db.session().tx().await?;
            let result = if prepared {
                tx.prepare_locy(PROGRAM).await?.execute(&[]).await?
            } else {
                tx.locy(PROGRAM).await?
            };
            assert!(
                result.derived_fact_set.is_none(),
                "prepared={prepared}: a transaction program writes, it does not collect"
            );
            let in_tx = tx
                .query("MATCH (:P)-[l:LINK]->(:P) RETURN count(l) AS c")
                .await?
                .rows()[0]
                .get::<i64>("c")?;
            let w_in_tx =
                tx.query("MATCH (w:W) RETURN count(w) AS c").await?.rows()[0].get::<i64>("c")?;
            assert_eq!(
                (in_tx, w_in_tx),
                (1, 1),
                "prepared={prepared}: the writes are in the tx"
            );
            assert_eq!(
                count(&db, "MATCH (:P)-[l:LINK]->(:P) RETURN count(l) AS c").await?,
                0,
                "prepared={prepared}: uncommitted writes are invisible outside the tx"
            );
            if commit {
                tx.commit().await?;
                assert_eq!(
                    count(&db, "MATCH (:P)-[l:LINK]->(:P) RETURN count(l) AS c").await?,
                    1,
                    "prepared={prepared}: committed"
                );
                assert_eq!(count(&db, "MATCH (w:W) RETURN count(w) AS c").await?, 1);
                // Undo for the next arm.
                let undo = db.session().tx().await?;
                undo.execute("MATCH (:P)-[l:LINK]->(:P) DELETE l").await?;
                undo.execute("MATCH (w:W) DELETE w").await?;
                undo.commit().await?;
            } else {
                tx.rollback();
                assert_eq!(
                    count(&db, "MATCH (:P)-[l:LINK]->(:P) RETURN count(l) AS c").await?,
                    0,
                    "prepared={prepared}: rolled back"
                );
                assert_eq!(count(&db, "MATCH (w:W) RETURN count(w) AS c").await?, 0);
            }
        }
    }
    Ok(())
}

/// A tx-prepared program reads the transaction's pinned snapshot: a commit
/// by another session after the transaction's first read is invisible to it,
/// as it is to `tx.locy`.
#[tokio::test]
async fn tx_prepared_program_reads_the_transactions_snapshot() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let seed = db.session().tx().await?;
    seed.execute("CREATE (:N {v: 1})").await?;
    seed.commit().await?;

    let tx = db.session().tx().await?;
    let prepared = tx.prepare_locy(READ_N).await?;
    assert_eq!(query_values(&tx.locy(READ_N).await?), vec![Value::Int(1)]);

    let other = db.session().tx().await?;
    other.execute("CREATE (:N {v: 2})").await?;
    other.commit().await?;

    let oracle = query_values(&tx.locy(READ_N).await?);
    assert_eq!(oracle, vec![Value::Int(1)], "tx.locy reads its snapshot");
    assert_eq!(query_values(&prepared.execute(&[]).await?), oracle);
    tx.rollback();
    Ok(())
}

/// A tx-prepared program runs under the transaction's cancellation scope:
/// once the transaction is cancelled it is refused as `tx.locy` is.
#[tokio::test]
async fn tx_prepared_program_honours_the_transactions_cancellation() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {v: 7})").await?;
    let prepared = tx.prepare_locy(READ_N).await?;
    tx.cancel();
    let oracle = tx.locy(READ_N).await;
    assert!(
        matches!(oracle, Err(UniError::Cancelled)),
        "tx.locy after cancel: {oracle:?}"
    );
    let got = prepared.execute(&[]).await;
    assert!(
        matches!(got, Err(UniError::Cancelled)),
        "the prepared program after cancel: {got:?}"
    );
    Ok(())
}

/// A session-prepared program is unchanged: it collects its writes for
/// `tx.apply` and reads live committed state.
#[tokio::test]
async fn session_prepared_program_still_collects() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let prepared = db
        .session()
        .prepare_locy("CREATE (:W {v: $x})\nCREATE RULE r AS MATCH (n:W) YIELD KEY n, n.v AS v\nQUERY r RETURN v")
        .await?;
    let result = prepared.execute(&[("x", Value::Int(3))]).await?;
    assert_eq!(query_values(&result), vec![Value::Int(3)]);
    assert_eq!(count(&db, "MATCH (w:W) RETURN count(w) AS c").await?, 0);
    let dfs = result
        .derived_fact_set
        .clone()
        .expect("the write is collected");
    let tx = db.session().tx().await?;
    tx.apply(dfs).await?;
    tx.commit().await?;
    let v = db
        .session()
        .query("MATCH (w:W) RETURN w.v AS v")
        .await?
        .rows()[0]
        .get::<i64>("v")?;
    assert_eq!(v, 3, "the replay used the program's parameters");
    Ok(())
}

/// A Cypher write after the program's first write stores the parameter's
/// value, in a transaction.
#[tokio::test]
async fn tx_cypher_write_after_a_write_reads_params() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.locy_with("CREATE (:T {v: 0})\nCREATE (:T {v: $x})")
        .param("x", 5)
        .run()
        .await?;
    tx.commit().await?;
    let rows = db
        .session()
        .query("MATCH (t:T) RETURN t.v AS v ORDER BY v")
        .await?;
    let values: Vec<Value> = rows
        .rows()
        .iter()
        .map(|r| r.value("v").cloned().unwrap_or(Value::Null))
        .collect();
    assert_eq!(values, vec![Value::Int(0), Value::Int(5)]);
    Ok(())
}

/// A Cypher read after a write resolves the parameter, at session and
/// transaction level. The QUERY between them ends the write's statement.
#[tokio::test]
async fn cypher_read_after_a_write_reads_params() -> Result<()> {
    const PROGRAM: &str = "CREATE RULE t AS MATCH (t:T) YIELD KEY t, t.v AS v\n\
        CREATE (:T {v: $x})\n\
        QUERY t RETURN v\n\
        MATCH (t:T) WHERE t.v = $x RETURN t.v AS v";
    let db = Uni::in_memory().build().await?;
    let session = db.session().locy_with(PROGRAM).param("x", 5).run().await?;
    let tx = db.session().tx().await?;
    let in_tx = tx.locy_with(PROGRAM).param("x", 5).run().await?;
    tx.rollback();
    for (what, result) in [("session", &session), ("tx", &in_tx)] {
        match result.command_results.as_slice() {
            [
                CommandResult::Cypher(w),
                CommandResult::Query(q),
                CommandResult::Cypher(rows),
            ] => {
                assert!(w.is_empty(), "{what}: {w:?}");
                assert_eq!(q.len(), 1, "{what}: {q:?}");
                assert_eq!(q[0].get("v"), Some(&Value::Int(5)), "{what}");
                assert_eq!(rows.len(), 1, "{what}: {rows:?}");
                assert_eq!(rows[0].get("v"), Some(&Value::Int(5)), "{what}");
            }
            other => panic!("{what}: unexpected results {other:?}"),
        }
    }
    Ok(())
}

/// A session program's Cypher write is replayed by `tx.apply` with the
/// program's parameters, after a DERIVE as well as on its own.
#[tokio::test]
async fn apply_replays_with_the_programs_params() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let seed = db.session().tx().await?;
    seed.execute("CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})")
        .await?;
    seed.commit().await?;
    let result = db
        .session()
        .locy_with(
            "CREATE RULE link AS MATCH (a:P)-[:KNOWS]->(b:P) DERIVE (a)-[:LINK]->(b)\n\
             DERIVE link\n\
             CREATE (:T {v: $x, s: $s})",
        )
        .param("x", 5)
        .param("s", "five")
        .run()
        .await?;
    let dfs = result
        .derived_fact_set
        .clone()
        .expect("the writes are collected");
    let tx = db.session().tx().await?;
    let applied = tx.apply(dfs).await?;
    assert_eq!(applied.facts_applied, 2);
    tx.commit().await?;
    let rows = db
        .session()
        .query("MATCH (t:T) RETURN t.v AS v, t.s AS s")
        .await?;
    assert_eq!(rows.rows().len(), 1);
    assert_eq!(rows.rows()[0].get::<i64>("v")?, 5);
    assert_eq!(rows.rows()[0].get::<String>("s")?, "five");
    assert_eq!(
        count(&db, "MATCH (:P)-[l:LINK]->(:P) RETURN count(l) AS c").await?,
        1
    );
    Ok(())
}
