// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An anchored match must report every label its vertex carries.
//!
//! Found while building the compaction crash matrix: a vertex deleted through
//! one of its labels came back through another. The crash turned out to be
//! irrelevant — it reproduced with a plain `flush()`.
//!
//! **Root cause.** An anchored vertex scan never requested the stored `_labels`
//! column, so `build_labels_column_for_known_label`
//! (`uni-query/src/query/df_graph/scan.rs`) fabricated `[label]`. Its docstring
//! said the fallback was for "legacy data"; because the projection never asked,
//! the legacy path became the only path. Unflushed vertices were correct
//! because the L0 overlay restored the true set — which is why every symptom
//! below needed a flush first.
//!
//! The fabricated set was not merely displayed. The executor reads it back via
//! `extract_labels_from_node` and writes it, so a truncated read became a
//! durable write. Four manifestations, one cause, all pinned here:
//!
//! | statement, on a flushed `(:Person:Staff)` matched by `:Person` | was |
//! |---|---|
//! | `DETACH DELETE n` | tombstone written only to `Person`; vertex live via `:Staff` |
//! | `SET n:Manager` | label set REPLACED with `[Person, Manager]` — `:Staff` silently deleted |
//! | `REMOVE n:Person` | resolved `remaining = []` — the vertex lost every label |
//! | `SET n.badge = …` | plain property update rewrote the label set as `[Person]` |
//! | `RETURN labels(n)` | `["Person"]` — a wrong answer with no error |
//!
//! Distinct from #181 (flush resurrecting a detached edge) and #182 (a delete
//! before the first flush): both single-label, both fixed elsewhere.
//!
//! The two "correct before the flush" and "other delete forms" tests are kept
//! as controls — they localize the defect to the flush boundary and to the
//! anchored form, and would have caught a fix that merely papered over the
//! delete path.

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

/// The originally-reported symptom.
#[tokio::test]
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

/// `labels(n)` under an anchored match. The UDF reads the `_labels` column the
/// scan produced, so a fabricated set is returned to the user verbatim.
#[tokio::test]
async fn labels_function_reports_every_label_after_a_flush() -> Result<()> {
    let db = two_label_graph().await?;
    let rows = db
        .session()
        .query("MATCH (n:Person {name: 'alice'}) RETURN labels(n) AS l")
        .await?;
    assert_eq!(rows.len(), 1);
    let mut labels: Vec<String> = rows.rows()[0].get::<Vec<String>>("l").unwrap_or_default();
    labels.sort();
    assert_eq!(
        labels,
        vec!["Person".to_string(), "Staff".to_string()],
        "labels(n) under a :Person anchor reports only the anchored label"
    );
    Ok(())
}

/// Adding a label through one anchor must not drop the others. `SET n:Label`
/// REPLACES the label set with whatever the scan reported, so a truncated read
/// becomes a durable write.
#[tokio::test]
async fn setting_a_label_through_one_anchor_keeps_the_others() -> Result<()> {
    let db = two_label_graph().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:Person {name: 'alice'}) SET n:Manager")
        .await?;
    tx.commit().await?;

    // Before any flush: SET n:Label takes effect in L0 immediately, so this is
    // where the loss first becomes observable.
    assert!(
        names_via(&db, "Staff")
            .await?
            .contains(&"alice".to_string()),
        "alice lost :Staff when :Manager was added through the :Person anchor"
    );
    db.flush().await?;
    assert!(
        names_via(&db, "Staff")
            .await?
            .contains(&"alice".to_string()),
        "the flush made the lost :Staff durable"
    );

    let rows = session
        .query("MATCH (n:Manager {name: 'alice'}) RETURN labels(n) AS l")
        .await?;
    assert_eq!(rows.len(), 1, "alice is reachable through the new :Manager");
    let mut labels: Vec<String> = rows.rows()[0].get::<Vec<String>>("l").unwrap_or_default();
    labels.sort();
    assert_eq!(
        labels,
        vec![
            "Manager".to_string(),
            "Person".to_string(),
            "Staff".to_string()
        ]
    );
    Ok(())
}

/// Removing one label must remove exactly that one.
#[tokio::test]
async fn removing_a_label_through_one_anchor_keeps_the_others() -> Result<()> {
    let db = two_label_graph().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:Person {name: 'alice'}) REMOVE n:Person")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        names_via(&db, "Person").await?,
        vec!["carol".to_string()],
        "alice must be gone from the label that was removed"
    );
    assert!(
        names_via(&db, "Staff")
            .await?
            .contains(&"alice".to_string()),
        "REMOVE n:Person also stripped :Staff"
    );
    Ok(())
}

/// The least obvious manifestation: an ordinary property update. Nothing in the
/// statement mentions labels, yet the truncated `_labels` the scan produced is
/// written back as the vertex's whole label set.
#[tokio::test]
async fn a_plain_property_update_keeps_every_label() -> Result<()> {
    let db = two_label_graph().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:Person {name: 'alice'}) SET n.badge = 'a2'")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    assert!(
        names_via(&db, "Staff")
            .await?
            .contains(&"alice".to_string()),
        "a plain property update under the :Person anchor dropped :Staff"
    );
    let rows = session
        .query("MATCH (n:Staff {badge: 'a2'}) RETURN n.name AS name")
        .await?;
    assert_eq!(
        rows.len(),
        1,
        "the updated property must be readable through the other anchor"
    );
    Ok(())
}
