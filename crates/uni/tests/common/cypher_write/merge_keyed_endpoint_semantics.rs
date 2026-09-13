// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! What `MERGE (a)-[:R]->(b:L {k: v})` means, pinned before it is made fast.
//!
//! MERGE is **whole-pattern** match-or-create. The tempting optimisation for
//! this shape — resolve `b` by its key once for the batch, then reuse the vid —
//! is wrong, and wrong in a way that returns plausible answers: a node with
//! `k = v` that exists but is *not* a neighbour of `a` must **not** be reused.
//! The pattern misses, and a brand-new `b` is created.
//!
//! That follows from `execute_create_pattern`, which reuses a node only when it
//! is already bound in the row and otherwise creates one. These tests make it an
//! assertion rather than an implementation detail, so the optimisation cannot
//! quietly change it.
//!
//! They pass today. That is the point: they describe the contract the fast path
//! has to preserve, so they are written first and must keep passing after.

use anyhow::Result;
use uni_db::{Uni, Value};

async fn db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING, tag STRING)")
        .await?;
    tx.execute("CREATE EDGE TYPE R FROM E TO E").await?;
    tx.commit().await?;
    Ok(db)
}

async fn count(db: &Uni, cypher: &str) -> Result<i64> {
    Ok(db.session().query(cypher).await?.rows()[0].get("n")?)
}

/// A keyed endpoint that exists but is not a neighbour is NOT reused.
///
/// The whole pattern misses, so MERGE creates a second node with the same key.
/// Reusing the existing one would look right — one node, one edge — and be
/// wrong.
#[tokio::test]
async fn an_unlinked_node_with_the_same_key_is_not_reused() -> Result<()> {
    let db = db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {uid: 'a'}), (:E {uid: 'x', tag: 'pre-existing'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:E {uid: 'a'}) MERGE (a)-[:R]->(b:E {uid: 'x'})")
        .await?;
    tx.commit().await?;

    assert_eq!(
        count(&db, "MATCH (n:E {uid: 'x'}) RETURN count(n) AS n").await?,
        2,
        "the pre-existing unlinked `x` was reused; MERGE matches whole patterns, \
         so a miss must create its own node"
    );
    assert_eq!(
        count(
            &db,
            "MATCH (:E {uid: 'a'})-[:R]->(b:E {uid: 'x'}) RETURN count(b) AS n"
        )
        .await?,
        1,
        "the created `x` is not linked to `a`"
    );
    // And the original is untouched — it kept its tag and gained no edge.
    assert_eq!(
        count(
            &db,
            "MATCH (n:E {uid: 'x'}) WHERE n.tag = 'pre-existing' RETURN count(n) AS n"
        )
        .await?,
        1,
        "the pre-existing node was mutated"
    );
    Ok(())
}

/// The same key from two different sources yields two nodes.
///
/// Each row is its own whole-pattern miss, so batching key resolution must not
/// collapse them.
#[tokio::test]
async fn the_same_key_from_two_sources_creates_two_nodes() -> Result<()> {
    let db = db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {uid: 'a1'}), (:E {uid: 'a2'})")
        .await?;
    tx.commit().await?;

    let batch = Value::List(vec![
        Value::Map([("src".to_string(), Value::String("a1".into()))].into()),
        Value::Map([("src".to_string(), Value::String("a2".into()))].into()),
    ]);
    let tx = db.session().tx().await?;
    tx.execute_with(
        "UNWIND $batch AS r \
         MATCH (a:E {uid: r.src}) \
         MERGE (a)-[:R]->(b:E {uid: 'shared'})",
    )
    .param("batch", batch)
    .run()
    .await?;
    tx.commit().await?;

    assert_eq!(
        count(&db, "MATCH (n:E {uid: 'shared'}) RETURN count(n) AS n").await?,
        2,
        "two sources merging the same key must each create their own node"
    );
    assert_eq!(
        count(&db, "MATCH ()-[e:R]->() RETURN count(e) AS n").await?,
        2
    );
    Ok(())
}

/// The same row twice creates one node and one edge.
///
/// Intra-batch dedup: the second row must see what the first created. This is
/// the property a batched key snapshot breaks if it is not folded forward.
#[tokio::test]
async fn the_same_row_twice_creates_one_node_and_one_edge() -> Result<()> {
    let db = db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {uid: 'a'})").await?;
    tx.commit().await?;

    let row = || Value::Map([("src".to_string(), Value::String("a".into()))].into());
    let tx = db.session().tx().await?;
    tx.execute_with(
        "UNWIND $batch AS r \
         MATCH (a:E {uid: r.src}) \
         MERGE (a)-[:R]->(b:E {uid: 'dup'})",
    )
    .param("batch", Value::List(vec![row(), row()]))
    .run()
    .await?;
    tx.commit().await?;

    assert_eq!(
        count(&db, "MATCH (n:E {uid: 'dup'}) RETURN count(n) AS n").await?,
        1,
        "the second row did not see the node the first created"
    );
    assert_eq!(
        count(&db, "MATCH ()-[e:R]->() RETURN count(e) AS n").await?,
        1,
        "the second row did not see the edge the first created"
    );
    Ok(())
}

/// Re-running the same statement matches instead of creating again.
///
/// Covers the flushed/committed side, where the candidate comes from persisted
/// storage rather than from L0.
#[tokio::test]
async fn re_running_matches_rather_than_creating() -> Result<()> {
    let db = db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {uid: 'a'})").await?;
    tx.commit().await?;

    for _ in 0..2 {
        let tx = db.session().tx().await?;
        tx.execute("MATCH (a:E {uid: 'a'}) MERGE (a)-[:R]->(b:E {uid: 'once'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
    }

    assert_eq!(
        count(&db, "MATCH (n:E {uid: 'once'}) RETURN count(n) AS n").await?,
        1,
        "the second run created a duplicate instead of matching"
    );
    assert_eq!(
        count(&db, "MATCH ()-[e:R]->() RETURN count(e) AS n").await?,
        1
    );
    Ok(())
}

/// ON CREATE SET applies to the created endpoint, and not on a later match.
#[tokio::test]
async fn on_create_set_applies_once() -> Result<()> {
    let db = db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {uid: 'a'})").await?;
    tx.commit().await?;

    for tag in ["first", "second"] {
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            "MATCH (a:E {{uid: 'a'}}) MERGE (a)-[:R]->(b:E {{uid: 'oc'}}) \
             ON CREATE SET b.tag = '{tag}'"
        ))
        .await?;
        tx.commit().await?;
    }

    let tag: String = db
        .session()
        .query("MATCH (n:E {uid: 'oc'}) RETURN n.tag AS t")
        .await?
        .rows()[0]
        .get("t")?;
    assert_eq!(
        tag, "first",
        "ON CREATE SET fired on a match, or the second run created a new node"
    );
    Ok(())
}
