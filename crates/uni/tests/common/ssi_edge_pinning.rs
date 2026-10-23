// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A transaction reads edges and properties as of its snapshot.
//!
//! A transaction's pinned `StorageManager` shares the live `AdjacencyManager`
//! and `PropertyManager`. Traversal filtered nothing by version and property
//! reads used live L1 visibility and a shared cache, so a transaction saw an
//! edge committed after it began — and through it, a vertex outside its
//! snapshot — kept seeing an edge deleted after it began, and read edge
//! properties updated and flushed by another transaction. Reads through both
//! managers now filter by the pin's row visibility (high-water mark plus
//! excluded bulk versions), and pinned property reads bypass the shared
//! cache.
//!
//! Each case runs with the other transaction's write left in L0 and flushed
//! to L1, the two tiers the pin covers.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(ssi_edge_pinning)'

use anyhow::Result;
use uni_db::{DataType, QueryResult, Uni};

fn count(rows: QueryResult) -> Result<i64> {
    Ok(rows.rows()[0].get::<i64>("n")?)
}

fn ids(rows: QueryResult) -> Result<Vec<i64>> {
    Ok(rows
        .rows()
        .iter()
        .map(|row| row.get::<i64>("n"))
        .collect::<std::result::Result<_, _>>()?)
}

/// `P {name}` vertices `a` and `b`, a `KNOWS` edge type with a `w` property.
async fn knows_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .property("v", DataType::Int64)
        .edge_type("KNOWS", &["P"], &["P"])
        .property("w", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {name: 'a', v: 1}), (:P {name: 'b', v: 1})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// An edge committed after a transaction began is not traversed by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_edge_committed_after_begin_is_invisible() -> Result<()> {
    let mut seen = Vec::new();
    for flush in [false, true] {
        let db = knows_db().await?;
        let tx1 = db.session().tx().await?;
        let q = "MATCH (:P {name: 'a'})-[r:KNOWS]->(:P) RETURN count(r) AS n";
        assert_eq!(count(tx1.query(q).await?)?, 0, "fixture");

        let tx2 = db.session().tx().await?;
        tx2.execute("MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) CREATE (a)-[:KNOWS {w: 1}]->(b)")
            .await?;
        tx2.commit().await?;
        if flush {
            db.flush().await?;
        }

        seen.push((flush, count(tx1.query(q).await?)?));
        tx1.rollback();
    }
    assert_eq!(seen, [(false, 0), (true, 0)], "(flush, edges tx1 sees)");
    Ok(())
}

/// A post-snapshot edge does not lead a transaction to a vertex outside its
/// snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_post_snapshot_edge_does_not_reach_a_post_snapshot_vertex() -> Result<()> {
    let db = knows_db().await?;
    let tx1 = db.session().tx().await?;
    let tx2 = db.session().tx().await?;
    tx2.execute("MATCH (a:P {name: 'a'}) CREATE (a)-[:KNOWS {w: 1}]->(:P {name: 'c', v: 1})")
        .await?;
    tx2.commit().await?;

    let direct = count(
        tx1.query("MATCH (c:P {name: 'c'}) RETURN count(c) AS n")
            .await?,
    )?;
    let via_edge = count(
        tx1.query("MATCH (:P {name: 'a'})-[:KNOWS]->(c) RETURN count(c) AS n")
            .await?,
    )?;
    tx1.rollback();
    assert_eq!(direct, 0, "control: c is outside tx1's snapshot");
    assert_eq!(via_edge, 0, "c reached through a post-snapshot edge");
    Ok(())
}

/// The other edge read paths: an unanchored relationship scan, an edge
/// property, and an edge deleted after begin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_scans_properties_and_deletes_are_pinned() -> Result<()> {
    let mut wrong = Vec::new();
    for flush in [false, true] {
        let db = knows_db().await?;
        let tx = db.session().tx().await?;
        tx.execute(
            "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) \
             CREATE (c:P {name: 'c', v: 1}), (a)-[:KNOWS {w: 1}]->(b), (b)-[:KNOWS {w: 5}]->(c)",
        )
        .await?;
        tx.commit().await?;
        db.flush().await?;

        let scan = "MATCH ()-[r:KNOWS]->() RETURN id(r) AS n ORDER BY n";
        let prop = "MATCH (:P {name: 'a'})-[r:KNOWS]->(:P {name: 'b'}) RETURN r.w AS n";
        let out_of_b = "MATCH (:P {name: 'b'})-[r:KNOWS]->() RETURN count(r) AS n";
        let tx1 = db.session().tx().await?;
        let before = (
            ids(tx1.query(scan).await?)?,
            count(tx1.query(prop).await?)?,
            count(tx1.query(out_of_b).await?)?,
        );

        let tx2 = db.session().tx().await?;
        tx2.execute("MATCH (a:P {name: 'a'}), (c:P {name: 'c'}) CREATE (a)-[:KNOWS {w: 9}]->(c)")
            .await?;
        tx2.execute("MATCH (:P {name: 'a'})-[r:KNOWS]->(:P {name: 'b'}) SET r.w = 2")
            .await?;
        tx2.execute("MATCH (:P {name: 'b'})-[r:KNOWS]->() DELETE r")
            .await?;
        tx2.commit().await?;
        if flush {
            db.flush().await?;
        }

        let after = (
            ids(tx1.query(scan).await?)?,
            count(tx1.query(prop).await?)?,
            count(tx1.query(out_of_b).await?)?,
        );
        tx1.rollback();
        assert_eq!((before.0.len(), before.1, before.2), (2, 1, 1), "fixture");
        if after != before {
            wrong.push(format!(
                "flush={flush}: (scan ids, w, edges out of b) {before:?} -> {after:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    Ok(())
}

/// Reads through the shared property manager — a vertex reached by
/// traversal or by scan, an edge property, and a schemaless edge type — keep
/// their snapshot values when another transaction updates and flushes them,
/// even after a live read has refilled the shared cache with the new values.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn property_reads_are_pinned_across_a_flush() -> Result<()> {
    let db = knows_db().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) \
         CREATE (a)-[:KNOWS {w: 1}]->(b), (a)-[:LOOSE {w: 1}]->(b)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let reads = [
        (
            "vertex via traversal",
            "MATCH (:P {name: 'a'})-[:KNOWS]->(b) RETURN b.v AS n",
        ),
        ("vertex via scan", "MATCH (b:P {name: 'b'}) RETURN b.v AS n"),
        (
            "edge property",
            "MATCH (:P {name: 'a'})-[r:KNOWS]->() RETURN r.w AS n",
        ),
        (
            "schemaless edge property",
            "MATCH (:P {name: 'a'})-[r:LOOSE]->() RETURN r.w AS n",
        ),
        (
            "schemaless edge count",
            "MATCH (:P {name: 'a'})-[r:LOOSE]->() RETURN count(r) AS n",
        ),
    ];
    let tx1 = db.session().tx().await?;
    let mut before = Vec::new();
    for (_, q) in reads {
        before.push(count(tx1.query(q).await?)?);
    }
    assert_eq!(before, [1, 1, 1, 1, 1], "fixture");

    let tx2 = db.session().tx().await?;
    tx2.execute("MATCH (b:P {name: 'b'}) SET b.v = 2").await?;
    tx2.execute("MATCH ()-[r:KNOWS]->() SET r.w = 2").await?;
    tx2.execute("MATCH ()-[r:LOOSE]->() SET r.w = 2").await?;
    tx2.execute("MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) CREATE (a)-[:LOOSE {w: 7}]->(b)")
        .await?;
    tx2.commit().await?;
    db.flush().await?;
    for (_, q) in reads {
        db.session().query(q).await?;
    }

    let mut wrong = Vec::new();
    for ((name, q), was) in reads.iter().zip(&before) {
        let now = count(tx1.query(q).await?)?;
        if now != *was {
            wrong.push(format!("{name}: {was} -> {now}"));
        }
    }
    tx1.rollback();
    assert!(wrong.is_empty(), "post-snapshot values leaked: {wrong:?}");
    Ok(())
}
