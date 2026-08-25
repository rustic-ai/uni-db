// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A label-anchored `DETACH DELETE` of a multi-label vertex is undone by the
//! next flush.
//!
//! Found while building the compaction crash matrix — the multi-label scenario
//! failed, and the crash turned out to be irrelevant: the vertex is already
//! resurrected before any compaction runs.
//!
//! Measured, on `(:Person:Staff {name: 'carol'})` deleted through the `:Person`
//! anchor:
//!
//! | delete form | after flush |
//! |---|---|
//! | `MATCH (c:Person {name: 'carol'}) DETACH DELETE c` | **visible via `:Staff`** |
//! | `MATCH (c {name: 'carol'}) DETACH DELETE c` | correct |
//! | `MATCH (c:Person:Staff {name: 'carol'}) DETACH DELETE c` | correct |
//! | `MATCH (c:Person {name: 'carol'}) DETACH DELETE c`, **no flush** | correct |
//!
//! The last row localizes it: the delete is correct in memory, so this is not a
//! planner or matching bug. It is the flush writing the tombstone into only the
//! *matched* label's per-label table rather than into every label the vertex
//! carries, so the unwritten anchors still hold a live row.
//!
//! Distinct from #181 (flush resurrecting a detached edge) and #182 (a delete
//! before the first flush resurrected by the next one): both of those are
//! single-label, and their fixes are in the edge and merge-insert paths.
//!
//! The correct-behaviour test is `#[ignore]`d rather than deleted or inverted:
//! asserting the buggy observable would turn green and quietly encode the
//! defect as intended behaviour, and leaving it un-ignored would redden CI for
//! a defect this change does not fix.

#![cfg(feature = "lance-backend")]

use anyhow::Result;
use uni_db::Uni;

// Rust guideline compliant

async fn two_label_graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE LABEL Staff (badge STRING)").await?;
    tx.execute("CREATE (:Person:Staff {name: 'alice', badge: 'a1'})")
        .await?;
    tx.execute("CREATE (:Person:Staff {name: 'carol', badge: 'c1'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

async fn names_via(db: &Uni, anchor: &str) -> Result<Vec<String>> {
    let rows = db
        .session()
        .query(&format!(
            "MATCH (n:{anchor}) RETURN n.name AS name ORDER BY name"
        ))
        .await?;
    Ok(rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap_or_default())
        .collect())
}

/// The defect. Un-ignore when the flush writes tombstones for every label a
/// deleted vertex carries.
#[tokio::test]
#[ignore = "known defect: a label-anchored DETACH DELETE of a multi-label vertex \
            is undone by the next flush for the vertex's other labels"]
async fn a_label_anchored_delete_survives_a_flush() -> Result<()> {
    let db = two_label_graph().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (c:Person {name: 'carol'}) DETACH DELETE c")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(names_via(&db, "Person").await?, vec!["alice".to_string()]);
    assert_eq!(
        names_via(&db, "Staff").await?,
        vec!["alice".to_string()],
        "carol was deleted through the :Person anchor but is still visible through \
         :Staff after a flush"
    );
    Ok(())
}

/// The control that localizes it: identical delete, no flush, correct answer.
/// Without this the defect could be read as a matching or planner bug.
#[tokio::test]
async fn a_label_anchored_delete_is_correct_before_the_flush() -> Result<()> {
    let db = two_label_graph().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (c:Person {name: 'carol'}) DETACH DELETE c")
        .await?;
    tx.commit().await?;

    assert_eq!(names_via(&db, "Person").await?, vec!["alice".to_string()]);
    assert_eq!(
        names_via(&db, "Staff").await?,
        vec!["alice".to_string()],
        "the delete is correct in memory; the resurrection is introduced by the flush"
    );
    Ok(())
}

/// The forms that do work today, pinned so a future fix cannot regress them
/// while repairing the anchored form.
#[tokio::test]
async fn unanchored_and_fully_anchored_deletes_survive_a_flush() -> Result<()> {
    for pattern in [
        "MATCH (c {name: 'carol'}) DETACH DELETE c",
        "MATCH (c:Person:Staff {name: 'carol'}) DETACH DELETE c",
    ] {
        let db = two_label_graph().await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute(pattern).await?;
        tx.commit().await?;
        db.flush().await?;

        for anchor in ["Person", "Staff"] {
            assert_eq!(
                names_via(&db, anchor).await?,
                vec!["alice".to_string()],
                "`{pattern}` must delete carol for the {anchor} anchor too"
            );
        }
    }
    Ok(())
}
