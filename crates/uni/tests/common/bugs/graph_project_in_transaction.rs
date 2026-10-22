// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.graph.project` inside a transaction.
//!
//! The projection catalog is per database and not transactional (as in Neo4j
//! GDS): a projection is visible to every session as soon as it is built and is
//! not undone by a rollback. Built inside a transaction with uncommitted
//! writes, it captured those writes -- other sessions saw them before commit,
//! the projection outlived a rollback, and `pageRank` over it returned vertex
//! ids that never existed. A projection may therefore not be built from a
//! transaction holding uncommitted writes; that is refused with a typed error.
//! A transaction without writes builds from its own snapshot.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(graph_project_in_transaction)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniError, Value};

const NATIVE: &str = "{nodeLabels: ['Person'], edgeTypes: ['KNOWS']}";
const CYPHER: &str = "{nodeQuery: 'MATCH (n:Person) RETURN id(n) AS id', \
                      edgeQuery: 'MATCH (a:Person)-[:KNOWS]->(b:Person) \
                      RETURN id(a) AS source, id(b) AS target'}";

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'a'})").await?;
    tx.commit().await?;
    Ok(db)
}

fn project(graph_ref: &str) -> String {
    format!(
        "CALL uni.graph.project('g', {graph_ref}, {{}}) \
         YIELD node_count, edge_count RETURN node_count, edge_count"
    )
}

async fn exists(db: &Uni) -> Result<bool> {
    let r = db
        .session()
        .query("CALL uni.graph.exists('g') YIELD exists RETURN exists")
        .await?;
    Ok(r.rows()[0].value("exists") == Some(&Value::Bool(true)))
}

#[tokio::test]
async fn graph_project_in_transaction_with_uncommitted_writes_is_refused() -> Result<()> {
    for (kind, graph_ref) in [("native", NATIVE), ("cypher", CYPHER)] {
        let db = open().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Person {name: 'b'})-[:KNOWS]->(:Person {name: 'c'})")
            .await?;
        let err = tx.query(&project(graph_ref)).await.expect_err(&format!(
            "{kind}: projecting over uncommitted writes must fail"
        ));
        match &err {
            UniError::UncommittedWritesInTransaction { operation, .. } => {
                assert_eq!(operation, "uni.graph.project", "{kind}");
            }
            other => panic!("{kind}: expected UncommittedWritesInTransaction, got {other:?}"),
        }
        assert!(
            !exists(&db).await?,
            "{kind}: no projection may be registered"
        );
        tx.rollback();
        assert!(!exists(&db).await?, "{kind}: nothing survives the rollback");
    }
    Ok(())
}

#[tokio::test]
async fn graph_project_in_clean_transaction_builds_from_its_snapshot() -> Result<()> {
    for (kind, graph_ref) in [("native", NATIVE), ("cypher", CYPHER)] {
        let db = open().await?;
        let tx = db.session().tx().await?;
        // Pin the transaction's snapshot with a read, then commit a vertex from
        // another session that the snapshot must not see.
        let seen = tx.query("MATCH (n:Person) RETURN count(n) AS c").await?;
        assert_eq!(seen.rows()[0].value("c"), Some(&Value::Int(1)), "{kind}");
        let other = db.session().tx().await?;
        other.execute("CREATE (:Person {name: 'late'})").await?;
        other.commit().await?;

        let r = tx.query(&project(graph_ref)).await?;
        assert_eq!(
            r.rows()[0].value("node_count"),
            Some(&Value::Int(1)),
            "{kind}: the projection is built from the transaction's snapshot"
        );
        tx.commit().await?;
        // The catalog is not transactional: the projection is visible to
        // every session once built.
        assert!(exists(&db).await?, "{kind}: the projection is registered");
        let pr = db
            .session()
            .query("CALL uni.algo.pageRank({name: 'g'}, {}) YIELD nodeId RETURN nodeId")
            .await?;
        assert_eq!(
            pr.rows().len(),
            1,
            "{kind}: pageRank over the snapshot projection"
        );
    }
    Ok(())
}
