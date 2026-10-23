// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Label writes reaching one vertex through more than one binding.
//!
//! `SET n:L` and `REMOVE n:L` computed the vertex's new label set from the
//! label list carried by the row's binding, then *replaced* the stored set
//! with it. A binding is a snapshot taken when the row was matched, so when a
//! second binding of the same vertex — another variable in the same row, or
//! the same variable in a later row — wrote after the first, it rebuilt the
//! set from labels that no longer held and silently discarded the first
//! write: `SET a:X SET b:Y` left `[A, Y]`, `REMOVE a:B REMOVE b:C` left
//! `[A, B]`.
//!
//! A property write through such a binding had the mirror defect: it passed
//! the binding's stale labels along with the properties, and the buffer
//! merged them back in, resurrecting a label removed a moment earlier.
//!
//! openCypher binds a node by reference: every write to it takes effect, and
//! every binding of it observes the result.

use anyhow::Result;
use uni_db::{DataType, Uni};

/// One `A` vertex carrying `labels`, optionally flushed to storage so the
/// write starts from a vertex that is in no L0 buffer.
async fn one_vertex(labels: &str, flushed: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property_nullable("x", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!("CREATE (:{labels} {{x: 0}})")).await?;
    tx.commit().await?;
    if flushed {
        db.flush().await?;
    }
    Ok(db)
}

async fn run(db: &Uni, cypher: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    Ok(())
}

/// The sorted labels of the only vertex in the database.
async fn labels_of_only(db: &Uni) -> Result<Vec<String>> {
    let r = db
        .session()
        .query("MATCH (n) RETURN labels(n) AS l")
        .await?;
    assert_eq!(r.rows().len(), 1, "expected exactly one vertex");
    let mut l: Vec<String> = r.rows()[0].get("l")?;
    l.sort();
    Ok(l)
}

async fn x_of_only(db: &Uni) -> Result<i64> {
    let r = db.session().query("MATCH (n) RETURN n.x AS x").await?;
    Ok(r.rows()[0].get::<i64>("x")?)
}

const SAME: &str = "MATCH (a:A), (b:A) WHERE id(a) = id(b)";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_write_set_through_two_bindings_keeps_both() -> Result<()> {
    for flushed in [false, true] {
        let db = one_vertex("A", flushed).await?;
        run(&db, &format!("{SAME} SET a:X SET b:Y")).await?;
        assert_eq!(
            labels_of_only(&db).await?,
            ["A", "X", "Y"],
            "flushed={flushed}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_write_remove_through_two_bindings_removes_both() -> Result<()> {
    for flushed in [false, true] {
        let db = one_vertex("A:B:C", flushed).await?;
        run(&db, &format!("{SAME} REMOVE a:B REMOVE b:C")).await?;
        assert_eq!(labels_of_only(&db).await?, ["A"], "flushed={flushed}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_write_set_then_remove_through_two_bindings() -> Result<()> {
    let db = one_vertex("A:B", false).await?;
    run(&db, &format!("{SAME} SET a:X REMOVE b:B")).await?;
    assert_eq!(labels_of_only(&db).await?, ["A", "X"]);
    Ok(())
}

/// The same variable in two rows: each row's binding was taken before
/// either row wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_write_across_rows_of_one_statement() -> Result<()> {
    for flushed in [false, true] {
        let db = one_vertex("A", flushed).await?;
        run(
            &db,
            "MATCH (a:A) UNWIND [1, 2] AS i \
             FOREACH (_ IN CASE WHEN i = 1 THEN [1] ELSE [] END | SET a:X) \
             FOREACH (_ IN CASE WHEN i = 2 THEN [1] ELSE [] END | SET a:Y)",
        )
        .await?;
        assert_eq!(
            labels_of_only(&db).await?,
            ["A", "X", "Y"],
            "flushed={flushed}"
        );

        let db = one_vertex("A:B:C", flushed).await?;
        run(
            &db,
            "MATCH (a:A) UNWIND [1, 2] AS i \
             FOREACH (_ IN CASE WHEN i = 1 THEN [1] ELSE [] END | REMOVE a:B) \
             FOREACH (_ IN CASE WHEN i = 2 THEN [1] ELSE [] END | REMOVE a:C)",
        )
        .await?;
        assert_eq!(labels_of_only(&db).await?, ["A"], "flushed={flushed}");
    }
    Ok(())
}

/// `MERGE … ON MATCH SET` after an earlier clause relabelled the vertex.
///
/// A guard, not a reproduction: MERGE binds the vertex afresh after the
/// earlier `SET`, so its binding was never stale. It pins that the label
/// read now taken from the L0 chain keeps this case right.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_write_merge_on_match_after_set() -> Result<()> {
    for flushed in [false, true] {
        let db = one_vertex("A", flushed).await?;
        run(&db, "MATCH (b:A) SET b:Y MERGE (a:A) ON MATCH SET a:X").await?;
        assert_eq!(
            labels_of_only(&db).await?,
            ["A", "X", "Y"],
            "flushed={flushed}"
        );
    }
    Ok(())
}

/// A property write through a binding taken before a label removal must not
/// put the removed label back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_write_property_set_does_not_resurrect_removed_label() -> Result<()> {
    for flushed in [false, true] {
        for set in ["SET b.x = 7", "SET b += {x: 7}", "SET b = {x: 7}"] {
            let db = one_vertex("A:B:C", flushed).await?;
            run(&db, &format!("{SAME} REMOVE a:B {set}")).await?;
            assert_eq!(
                (labels_of_only(&db).await?, x_of_only(&db).await?),
                (vec!["A".to_owned(), "C".to_owned()], 7),
                "flushed={flushed} {set}"
            );
        }
    }
    Ok(())
}

/// Every binding of the relabelled vertex projects its final label set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_write_every_binding_projects_final_labels() -> Result<()> {
    let db = one_vertex("A:B", false).await?;
    let tx = db.session().tx().await?;
    let r = tx
        .query(&format!(
            "{SAME} SET a:X REMOVE b:B RETURN labels(a) AS la, labels(b) AS lb, a, b"
        ))
        .await?;
    tx.commit().await?;
    assert_eq!(r.rows().len(), 1);
    let mut la: Vec<String> = r.rows()[0].get("la")?;
    let mut lb: Vec<String> = r.rows()[0].get("lb")?;
    la.sort();
    lb.sort();
    assert_eq!(
        (la, lb),
        (vec!["A".into(), "X".into()], vec!["A".into(), "X".into()])
    );
    for var in ["a", "b"] {
        let node: uni_db::Node = r.rows()[0].get(var)?;
        let mut l = node.labels.clone();
        l.sort();
        assert_eq!(l, ["A", "X"], "projected node {var}");
    }
    Ok(())
}
