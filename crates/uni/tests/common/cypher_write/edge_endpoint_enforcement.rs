// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Declared edge endpoints are enforced when an edge is written.
//!
//! `edge_type("R2", &["A"], &["B"])` declares that an `R2` runs from a node
//! carrying `A` to a node carrying `B`. Until wave P1 nothing checked it: a
//! `CREATE (:A)-[:R2]->(:A)` was stored, and the planner, which narrows
//! `MATCH (a:A)-[:R2]->(b)` by the declaration, then could not find it. The
//! writer now refuses such an edge with the typed `EdgeEndpointViolation` on
//! every path that creates one, and refuses a `REMOVE` of a label that would
//! leave an existing edge outside its declaration.

// Rust guideline compliant

use std::io::Write as _;

use anyhow::Result;
use uni_db::{DataType, Session, Uni, UniError};

async fn setup() -> Result<(Uni, Session)> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property_nullable("v", DataType::String)
        .label("B")
        .property_nullable("v", DataType::String)
        .label("S")
        .property_nullable("v", DataType::String)
        .edge_type("R2", &["A"], &["B"])
        .edge_type("FREE", &[], &[])
        .apply()
        .await?;
    let s = db.session();
    Ok((db, s))
}

async fn run(s: &Session, stmt: &str) -> uni_db::Result<()> {
    let tx = s.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

/// Assert `r` is the endpoint refusal for `endpoint`.
fn assert_violation(r: uni_db::Result<()>, endpoint: &str, what: &str) {
    match r {
        Err(UniError::EdgeEndpointViolation {
            edge_type,
            endpoint: got,
            allowed,
            ..
        }) => {
            assert_eq!(edge_type, "R2", "{what}");
            assert_eq!(got, endpoint, "{what}");
            let want = if endpoint == "source" { "A" } else { "B" };
            assert_eq!(allowed, vec![want.to_string()], "{what}");
        }
        other => panic!("{what}: expected EdgeEndpointViolation({endpoint}), got {other:?}"),
    }
}

async fn r2_count(s: &Session) -> Result<i64> {
    Ok(s.query("MATCH ()-[r:R2]->() RETURN count(r) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?)
}

#[tokio::test]
async fn create_and_merge_enforce_declared_endpoints() -> Result<()> {
    let (_db, s) = setup().await?;
    run(&s, "CREATE (:A {v: 'a'})-[:R2]->(:B {v: 'b'})").await?;
    run(&s, "CREATE (:A:S {v: 'as'})-[:R2]->(:B:S {v: 'bs'})").await?;
    assert_eq!(r2_count(&s).await?, 2, "declared endpoints are accepted");

    assert_violation(
        run(&s, "CREATE (:A {v: 'x'})-[:R2]->(:A {v: 'y'})").await,
        "target",
        "CREATE wrong target",
    );
    assert_violation(
        run(&s, "CREATE (:S {v: 'x'})-[:R2]->(:B {v: 'y'})").await,
        "source",
        "CREATE wrong source",
    );
    assert_violation(
        run(&s, "CREATE (:A {v: 'x'})-[:R2]->()").await,
        "target",
        "CREATE unlabeled target",
    );
    assert_violation(
        run(&s, "CREATE (:B {v: 'x'})<-[:R2]-(:S {v: 'y'})").await,
        "source",
        "CREATE right-to-left",
    );
    run(&s, "CREATE (:A {v: 'm1'}), (:A {v: 'm2'})").await?;
    assert_violation(
        run(
            &s,
            "MATCH (a:A {v: 'm1'}), (b:A {v: 'm2'}) MERGE (a)-[:R2]->(b)",
        )
        .await,
        "target",
        "MERGE between bound nodes",
    );
    assert_violation(
        run(&s, "MERGE (a:A {v: 'm3'})-[:R2]->(b:S {v: 'm4'})").await,
        "target",
        "MERGE of a whole pattern",
    );
    assert_eq!(r2_count(&s).await?, 2, "no refused edge was stored");

    // An edge type declared without endpoint labels accepts anything.
    run(&s, "CREATE (:S)-[:FREE]->()").await?;
    Ok(())
}

#[tokio::test]
async fn bulk_insert_and_copy_from_enforce_declared_endpoints() -> Result<()> {
    let (_db, s) = setup().await?;
    run(&s, "CREATE (:A {v: 'a'}), (:A {v: 'a2'}), (:B {v: 'b'})").await?;
    let ids = s
        .query("MATCH (n) RETURN n.v AS v, id(n) AS id ORDER BY v")
        .await?;
    let id = |name: &str| -> uni_common::core::id::Vid {
        let row = ids
            .rows()
            .iter()
            .find(|r| r.get::<String>("v").ok().as_deref() == Some(name))
            .expect("row");
        uni_common::core::id::Vid::new(row.get::<i64>("id").expect("id") as u64)
    };

    let tx = s.tx().await?;
    let r = tx
        .bulk_insert_edges(
            "R2",
            vec![(id("a"), id("a2"), uni_common::Properties::new())],
        )
        .await;
    assert_violation(r, "target", "bulk_insert_edges");
    drop(tx);

    let tx = s.tx().await?;
    tx.bulk_insert_edges(
        "R2",
        vec![(id("a"), id("b"), uni_common::Properties::new())],
    )
    .await?;
    tx.commit().await?;
    assert_eq!(r2_count(&s).await?, 1);

    let dir = tempfile::tempdir()?;
    let csv = dir.path().join("r2.csv");
    {
        let mut f = std::fs::File::create(&csv)?;
        writeln!(f, "src,dst")?;
        writeln!(f, "{},{}", id("a2").as_u64(), id("a").as_u64())?;
    }
    assert_violation(
        run(&s, &format!("COPY R2 FROM '{}'", csv.display())).await,
        "target",
        "COPY FROM",
    );
    assert_eq!(r2_count(&s).await?, 1);
    Ok(())
}

#[tokio::test]
async fn remove_label_refuses_to_orphan_a_declared_edge() -> Result<()> {
    let (_db, s) = setup().await?;
    run(&s, "CREATE (:A:S {v: 'a'})-[:R2]->(:B:S {v: 'b'})").await?;
    assert_violation(
        run(&s, "MATCH (n:A {v: 'a'}) REMOVE n:A").await,
        "source",
        "REMOVE the source's declared label",
    );
    assert_violation(
        run(&s, "MATCH (n:B {v: 'b'}) REMOVE n:B").await,
        "target",
        "REMOVE the target's declared label",
    );
    // Labels the declaration does not need can go.
    run(&s, "MATCH (n:A {v: 'a'}) REMOVE n:S").await?;
    run(&s, "MATCH (n:B {v: 'b'}) REMOVE n:S").await?;
    // After a flush the edge lives in storage; the check still sees it.
    s.flush().await?;
    assert_violation(
        run(&s, "MATCH (n:A {v: 'a'}) REMOVE n:A").await,
        "source",
        "REMOVE after flush",
    );
    // Once the edge is gone the label may be removed.
    run(&s, "MATCH ()-[r:R2]->() DELETE r").await?;
    run(&s, "MATCH (n:A {v: 'a'}) REMOVE n:A").await?;
    // An edge created and the label removed in one statement is refused too.
    assert_violation(
        run(&s, "CREATE (a:A {v: 'c'})-[:R2]->(:B {v: 'd'}) REMOVE a:A").await,
        "source",
        "CREATE then REMOVE in one statement",
    );
    Ok(())
}
