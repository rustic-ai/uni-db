// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #249: declaring a new property on a label that already has flushed data
//! leaves the Lance dataset unchanged, so the next write to that label fails.
//!
//! ```text
//! Internal error: Write to 'vertices_A' (Append) failed: ...
//! ```
//!
//! `.apply()` records the property in uni's schema and never reaches storage;
//! `get_arrow_schema` then emits a Field for every declared property, so the
//! flush batch carries a column the existing dataset has never had.
//!
//! These tests pin the *observed* behaviour, whatever it is: each asserts the
//! outcome it measured rather than the outcome the issue predicts, so a fix
//! that adds a column-migration path turns them red and they can be updated.

// Rust guideline compliant

use std::collections::HashMap;

use tempfile::TempDir;
use uni_db::{DataType, Result, Uni, Value};

fn rows(n: i64) -> Vec<HashMap<String, Value>> {
    (0..n)
        .map(|i| {
            let mut m = HashMap::new();
            m.insert("name".to_string(), Value::String(format!("name-{i}")));
            m.insert("num".to_string(), Value::Int(i));
            m
        })
        .collect()
}

/// Step 1 of the issue's repro: declare `A`, bulk-load rows, flush.
async fn seeded(dir: &TempDir) -> Result<Uni> {
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .property("num", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.bulk_insert_vertices_labeled(&["A"], rows(500)).await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// (a) `.apply()` of the late property, then a CREATE that uses it.
#[tokio::test]
async fn issue_249_create_after_late_property() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;

    // (a) does the schema change itself succeed?
    let applied = db
        .schema()
        .label("A")
        .property_nullable("added_later", DataType::String)
        .done()
        .apply()
        .await;
    eprintln!("ISSUE249 apply() -> {applied:?}");
    assert!(applied.is_ok(), "apply() of the late property");
    let flushed = db.flush().await;
    eprintln!("ISSUE249 flush-after-apply -> {flushed:?}");

    let s = db.session();
    let tx = s.tx().await?;
    let created = tx
        .execute("CREATE (:A {name: 'late', num: 99999, added_later: 'x'})")
        .await;
    eprintln!("ISSUE249 CREATE -> {created:?}");
    let committed = if created.is_ok() {
        let c = tx.commit().await.map(|_| ());
        eprintln!("ISSUE249 commit -> {c:?}");
        c
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    let post_flush = db.flush().await;
    eprintln!("ISSUE249 flush-after-create -> {post_flush:?}");

    // (e) do reads on the label still work?
    let read = s.query("MATCH (n:A) RETURN count(n) AS c").await;
    eprintln!(
        "ISSUE249 read -> {:?}",
        read.as_ref().map(|r| r.rows().len())
    );
    if let Ok(r) = &read {
        eprintln!(
            "ISSUE249 read count = {:?}",
            r.rows().first().and_then(|x| x.get::<i64>("c").ok())
        );
    }
    let read_new = s
        .query("MATCH (n:A) RETURN n.added_later AS v LIMIT 3")
        .await;
    eprintln!("ISSUE249 read-new-prop ok? {}", read_new.is_ok());
    if let Err(e) = &read_new {
        eprintln!("ISSUE249 read-new-prop err: {e}");
    }

    let failed = created.is_err() || committed.is_err() || post_flush.is_err();
    eprintln!("ISSUE249 SUMMARY any-write-failed = {failed}");
    assert!(
        failed,
        "issue #249 expects a write failure after a late property; none observed"
    );

    db.shutdown().await?;
    Ok(())
}

/// The bulk-API arm of the same repro.
#[tokio::test]
async fn issue_249_bulk_insert_after_late_property() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;
    db.schema()
        .label("A")
        .property_nullable("added_later", DataType::String)
        .done()
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String("late".into()));
    m.insert("num".to_string(), Value::Int(99999));
    m.insert("added_later".to_string(), Value::String("x".into()));
    let inserted = tx.bulk_insert_vertices_labeled(&["A"], vec![m]).await;
    eprintln!("ISSUE249 bulk insert -> {inserted:?}");
    let committed = if inserted.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    eprintln!("ISSUE249 bulk commit -> {committed:?}");
    let post_flush = db.flush().await;
    eprintln!("ISSUE249 bulk flush -> {post_flush:?}");
    assert!(
        inserted.is_err() || committed.is_err() || post_flush.is_err(),
        "bulk arm: no write failure observed"
    );
    db.shutdown().await?;
    Ok(())
}

/// (b) does the wedge survive a close and reopen?
#[tokio::test]
async fn issue_249_survives_reopen() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;
    db.schema()
        .label("A")
        .property_nullable("added_later", DataType::String)
        .done()
        .apply()
        .await?;
    db.shutdown().await?;

    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    let s = db.session();
    let tx = s.tx().await?;
    let created = tx
        .execute("CREATE (:A {name: 'late', num: 1, added_later: 'x'})")
        .await;
    eprintln!("ISSUE249 reopen CREATE -> {created:?}");
    let committed = if created.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    eprintln!("ISSUE249 reopen commit -> {committed:?}");
    let post_flush = db.flush().await;
    eprintln!("ISSUE249 reopen flush -> {post_flush:?}");
    let read = s.query("MATCH (n:A) RETURN count(n) AS c").await;
    eprintln!("ISSUE249 reopen read ok? {}", read.is_ok());

    assert!(
        created.is_err() || committed.is_err() || post_flush.is_err(),
        "reopen arm: no write failure observed"
    );
    db.shutdown().await?;
    Ok(())
}

/// (c) a SET that does not touch the new property, and
/// (d) a SET that does — plus a CREATE that omits the new property entirely.
#[tokio::test]
async fn issue_249_partial_writes() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir).await?;
    db.schema()
        .label("A")
        .property_nullable("added_later", DataType::String)
        .done()
        .apply()
        .await?;
    let s = db.session();

    // (c) SET on an old column only.
    let tx = s.tx().await?;
    let r = tx
        .execute("MATCH (n:A) WHERE n.num = 0 SET n.name = 'renamed'")
        .await;
    eprintln!("ISSUE249 SET-old -> {r:?}");
    let c = if r.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    eprintln!("ISSUE249 SET-old commit -> {c:?}");
    let f = db.flush().await;
    eprintln!("ISSUE249 SET-old flush -> {f:?}");
    eprintln!(
        "ISSUE249 SUMMARY set_old_ok = {}",
        r.is_ok() && c.is_ok() && f.is_ok()
    );

    // (d) SET on the new column.
    let tx = s.tx().await?;
    let r2 = tx
        .execute("MATCH (n:A) WHERE n.num = 1 SET n.added_later = 'y'")
        .await;
    eprintln!("ISSUE249 SET-new -> {r2:?}");
    let c2 = if r2.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    eprintln!("ISSUE249 SET-new commit -> {c2:?}");
    let f2 = db.flush().await;
    eprintln!("ISSUE249 SET-new flush -> {f2:?}");
    eprintln!(
        "ISSUE249 SUMMARY set_new_ok = {}",
        r2.is_ok() && c2.is_ok() && f2.is_ok()
    );

    // CREATE that omits the new property entirely.
    let tx = s.tx().await?;
    let r3 = tx.execute("CREATE (:A {name: 'plain', num: 4242})").await;
    eprintln!("ISSUE249 CREATE-without-new-prop -> {r3:?}");
    let c3 = if r3.is_ok() {
        tx.commit().await.map(|_| ())
    } else {
        {
            tx.rollback();
            Ok(())
        }
    };
    let f3 = db.flush().await;
    eprintln!("ISSUE249 CREATE-without commit -> {c3:?} flush -> {f3:?}");
    eprintln!(
        "ISSUE249 SUMMARY create_without_new_prop_ok = {}",
        r3.is_ok() && c3.is_ok() && f3.is_ok()
    );

    // (e) reads.
    let read = s.query("MATCH (n:A) RETURN count(n) AS c").await;
    eprintln!("ISSUE249 SUMMARY read_ok = {}", read.is_ok());
    if let Ok(r) = &read {
        eprintln!(
            "ISSUE249 read count = {:?}",
            r.rows().first().and_then(|x| x.get::<i64>("c").ok())
        );
    } else if let Err(e) = &read {
        eprintln!("ISSUE249 read err: {e}");
    }

    // Observed, pinned so a migration fix turns these red:
    // (c) a SET that touches only pre-existing columns is accepted by the
    //     query but its flush is rejected all the same — L0 re-materialises
    //     the whole declared property set for the row.
    assert!(r.is_ok(), "(c) SET on an old column is accepted");
    assert!(f.is_err(), "(c) but its flush is rejected");
    // (d) a SET that touches the new column fails earlier, in the query.
    assert!(r2.is_err(), "(d) SET on the new column fails");
    // A CREATE that never mentions the new property fails too.
    assert!(
        r3.is_err() || c3.is_err() || f3.is_err(),
        "a CREATE omitting the new property also fails"
    );
    // (e) reads that do not project the new column still work.
    assert!(read.is_ok(), "(e) reads on the label still work");

    db.shutdown().await?;
    Ok(())
}
