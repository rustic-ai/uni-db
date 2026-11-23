// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for the `created_at(n)` / `updated_at(n)` Cypher functions —
//! system-managed timestamps surfacing the L0 buffer's per-row
//! `_created_at` / `_updated_at` columns.

use anyhow::Result;
use std::time::Duration;
use tempfile::tempdir;
use uni_common::Value;
use uni_db::UniBuilder;

fn extract_nanos(v: &Value) -> Option<i64> {
    use uni_common::value::TemporalValue;
    if let Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch, ..
    })
    | Value::Temporal(TemporalValue::LocalDateTime {
        nanos_since_epoch, ..
    }) = v
    {
        Some(*nanos_since_epoch)
    } else {
        None
    }
}

#[tokio::test]
async fn test_created_at_on_node_after_create() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:Person) RETURN created_at(n) AS c, updated_at(n) AS u")
        .await?;
    assert_eq!(res.len(), 1);
    let c = res.rows()[0].value("c").unwrap();
    let u = res.rows()[0].value("u").unwrap();
    let c_ns = extract_nanos(c).expect("created_at should be a DateTime");
    let u_ns = extract_nanos(u).expect("updated_at should be a DateTime");
    assert!(c_ns > 0);
    // For a fresh node, created_at and updated_at are set within the same
    // tx — same wall-clock millisecond in practice, but a CREATE may
    // perform several internal L0 writes (label assignment + property set)
    // each with its own `Utc::now()` call, so they can differ by sub-ms.
    let delta_ns = (u_ns - c_ns).abs();
    assert!(
        delta_ns < 1_000_000_000,
        "fresh node: created_at ({c_ns}) and updated_at ({u_ns}) should be within 1s",
    );
    Ok(())
}

#[tokio::test]
async fn test_updated_at_advances_after_set() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    tx.execute("CREATE (:Person {name: 'Bob', age: 30})")
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:Person {name: 'Bob'}) RETURN created_at(n) AS c, updated_at(n) AS u")
        .await?;
    let c1 = extract_nanos(res.rows()[0].value("c").unwrap()).unwrap();
    let u1 = extract_nanos(res.rows()[0].value("u").unwrap()).unwrap();

    // Sleep to make sure the wall clock advances enough to observe.
    tokio::time::sleep(Duration::from_millis(5)).await;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Person {name: 'Bob'}) SET n.age = 31")
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:Person {name: 'Bob'}) RETURN created_at(n) AS c, updated_at(n) AS u")
        .await?;
    let c2 = extract_nanos(res.rows()[0].value("c").unwrap()).unwrap();
    let u2 = extract_nanos(res.rows()[0].value("u").unwrap()).unwrap();

    assert_eq!(c1, c2, "created_at must not change after SET");
    assert!(
        u2 > u1,
        "updated_at must advance after SET (was {u1}, now {u2})"
    );
    assert!(
        u2 > c2,
        "updated_at should be later than created_at after a SET"
    );
    Ok(())
}

#[tokio::test]
async fn test_edge_created_updated_at() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person")
        .await?;
    tx.execute("CREATE (:Person {name: 'A'})").await?;
    tx.execute("CREATE (:Person {name: 'B'})").await?;
    tx.execute(
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) \
         CREATE (a)-[:KNOWS {since: 2020}]->(b)",
    )
    .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query(
            "MATCH (:Person)-[r:KNOWS]->(:Person) \
             RETURN created_at(r) AS c, updated_at(r) AS u",
        )
        .await?;
    assert_eq!(res.len(), 1);
    let c_val = res.rows()[0].value("c").unwrap();
    let u_val = res.rows()[0].value("u").unwrap();
    let c =
        extract_nanos(c_val).unwrap_or_else(|| panic!("edge created_at not DateTime: {c_val:?}"));
    let u =
        extract_nanos(u_val).unwrap_or_else(|| panic!("edge updated_at not DateTime: {u_val:?}"));
    assert!(c > 0);
    let delta_ns = (u - c).abs();
    assert!(
        delta_ns < 1_000_000_000,
        "fresh edge: created_at ({c}) and updated_at ({u}) should be within 1s",
    );

    // Updating an edge property should bump updated_at but not created_at.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:KNOWS]->() SET r.since = 2021")
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query(
            "MATCH (:Person)-[r:KNOWS]->(:Person) \
             RETURN created_at(r) AS c, updated_at(r) AS u",
        )
        .await?;
    let c2 = extract_nanos(res.rows()[0].value("c").unwrap()).unwrap();
    let u2 = extract_nanos(res.rows()[0].value("u").unwrap()).unwrap();
    assert_eq!(c, c2);
    assert!(u2 > u);
    Ok(())
}

#[tokio::test]
async fn test_filter_by_created_at() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Item (sku STRING)").await?;
    tx.execute("CREATE (:Item {sku: 'A'})").await?;
    tx.commit().await?;

    // Anchor with a cutoff captured between two writes.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let cutoff_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    tokio::time::sleep(Duration::from_millis(5)).await;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {sku: 'B'})").await?;
    tx.commit().await?;

    let res = db
        .session()
        .query_with("MATCH (n:Item) WHERE created_at(n) > $cutoff RETURN n.sku AS sku")
        .param(
            "cutoff",
            Value::Temporal(uni_common::value::TemporalValue::DateTime {
                nanos_since_epoch: cutoff_ns,
                offset_seconds: 0,
                timezone_name: None,
            }),
        )
        .fetch_all()
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res.rows()[0].get::<String>("sku")?, "B");
    Ok(())
}

#[tokio::test]
async fn test_tx_local_created_at_visible() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE (:Person {name: 'Eve'})").await?;
    // Read created_at *before* commit — should already be set.
    let res = tx
        .query("MATCH (n:Person {name: 'Eve'}) RETURN created_at(n) AS c")
        .await?;
    let c = extract_nanos(res.rows()[0].value("c").unwrap())
        .expect("tx-local created_at must be visible");
    assert!(c > 0);
    tx.commit().await?;
    Ok(())
}

/// `(created_at, updated_at)` of the single row `query` returns as `c` and `u`.
async fn read_stamps(db: &uni_db::Uni, query: &str) -> Result<(i64, i64)> {
    let res = db.session().query(query).await?;
    assert_eq!(res.len(), 1, "{query}: expected one row");
    let c = res.rows()[0].value("c").unwrap();
    let u = res.rows()[0].value("u").unwrap();
    let c_ns = extract_nanos(c).unwrap_or_else(|| panic!("{query}: created_at is {c:?}"));
    let u_ns = extract_nanos(u).unwrap_or_else(|| panic!("{query}: updated_at is {u:?}"));
    Ok((c_ns, u_ns))
}

/// Checks a freshly inserted row's stamps before and after a flush: both set,
/// `updated_at >= created_at`, and the flush changes neither.
async fn assert_fresh_stamps(db: &uni_db::Uni, query: &str) -> Result<()> {
    let (c, u) = read_stamps(db, query).await?;
    assert!(c > 0, "{query}: created_at must be set");
    assert!(
        u >= c,
        "{query}: updated_at ({u}) must not be earlier than created_at ({c}) on insert"
    );
    db.flush().await?;
    let (c2, u2) = read_stamps(db, query).await?;
    assert_eq!(
        (c, u),
        (c2, u2),
        "{query}: a flush must not change the stamps"
    );
    Ok(())
}

#[tokio::test]
async fn test_insert_stamps_are_ordered_declared_label() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    // Time passes inside the transaction before it commits.
    tokio::time::sleep(Duration::from_millis(5)).await;
    tx.commit().await?;
    assert_fresh_stamps(
        &db,
        "MATCH (n:Person) RETURN created_at(n) AS c, updated_at(n) AS u",
    )
    .await
}

#[tokio::test]
async fn test_stamps_on_schemaless_label() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Loose {name: 'x'})").await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    tx.commit().await?;
    assert_fresh_stamps(
        &db,
        "MATCH (n:Loose) RETURN created_at(n) AS c, updated_at(n) AS u",
    )
    .await?;

    // A SET after the flush advances updated_at.
    let (_, u1) = read_stamps(
        &db,
        "MATCH (n:Loose) RETURN created_at(n) AS c, updated_at(n) AS u",
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Loose) SET n.name = 'y'").await?;
    tx.commit().await?;
    let (c2, u2) = read_stamps(
        &db,
        "MATCH (n:Loose) RETURN created_at(n) AS c, updated_at(n) AS u",
    )
    .await?;
    assert!(c2 <= u2, "updated_at must not be earlier than created_at");
    assert!(u2 > u1, "updated_at must advance after SET");
    Ok(())
}

#[tokio::test]
async fn test_edge_stamps_survive_flush() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person")
        .await?;
    tx.execute("CREATE (:Person {name: 'A'})-[:KNOWS {since: 2020}]->(:Person {name: 'B'})")
        .await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    tx.commit().await?;
    assert_fresh_stamps(
        &db,
        "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN created_at(r) AS c, updated_at(r) AS u",
    )
    .await
}

#[tokio::test]
async fn test_schemaless_edge_stamps_survive_flush() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Loose {name: 'A'})-[:LINKS {w: 1}]->(:Loose {name: 'B'})")
        .await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    tx.commit().await?;
    assert_fresh_stamps(
        &db,
        "MATCH (:Loose)-[r:LINKS]->(:Loose) RETURN created_at(r) AS c, updated_at(r) AS u",
    )
    .await
}

/// A `SET` on a vertex that is already flushed keeps its `created_at`, read
/// before the update is flushed and after; `updated_at` advances (C36).
async fn assert_set_after_flush_keeps_created_at(db: &uni_db::Uni, label: &str) -> Result<()> {
    let q = format!("MATCH (n:{label}) RETURN created_at(n) AS c, updated_at(n) AS u");
    let tx = db.session().tx().await?;
    tx.execute(&format!("CREATE (:{label} {{name: 'a'}})"))
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let (c0, u0) = read_stamps(db, &q).await?;

    tokio::time::sleep(Duration::from_millis(5)).await;
    let tx = db.session().tx().await?;
    tx.execute(&format!("MATCH (n:{label}) SET n.name = 'b'"))
        .await?;
    tx.commit().await?;

    let (c1, u1) = read_stamps(db, &q).await?;
    assert_eq!(c0, c1, "{label}: created_at changed by a SET (unflushed)");
    assert!(u1 > u0, "{label}: updated_at must advance after SET");

    db.flush().await?;
    let (c2, u2) = read_stamps(db, &q).await?;
    assert_eq!(c0, c2, "{label}: created_at changed by a SET (re-flushed)");
    assert_eq!(u1, u2, "{label}: a flush must not change updated_at");

    // A second update after the re-flush still keeps the original stamp.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let tx = db.session().tx().await?;
    tx.execute(&format!("MATCH (n:{label}) SET n.name = 'c'"))
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let (c3, _) = read_stamps(db, &q).await?;
    assert_eq!(c0, c3, "{label}: created_at changed by a second SET");
    Ok(())
}

#[tokio::test]
async fn test_set_on_flushed_vertex_keeps_created_at_declared() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.commit().await?;
    assert_set_after_flush_keeps_created_at(&db, "Person").await
}

#[tokio::test]
async fn test_set_on_flushed_vertex_keeps_created_at_schemaless() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    assert_set_after_flush_keeps_created_at(&db, "Loose").await
}

/// `MERGE ... ON MATCH SET`, `REMOVE` and `SET n += map` on a flushed vertex
/// keep its `created_at` as well (C36).
#[tokio::test]
async fn test_other_updates_on_flushed_vertex_keep_created_at() -> Result<()> {
    let dir = tempdir()?;
    let db = UniBuilder::new(dir.path().to_str().unwrap().to_string())
        .build()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {name: 'a', x: 1})").await?;
    tx.commit().await?;
    db.flush().await?;
    let q = "MATCH (n:Doc) RETURN created_at(n) AS c, updated_at(n) AS u";
    let (c0, _) = read_stamps(&db, q).await?;
    for stmt in [
        "MERGE (n:Doc {name: 'a'}) ON MATCH SET n.x = 2",
        "MATCH (n:Doc) REMOVE n.x",
        "MATCH (n:Doc) SET n += {y: 3}",
    ] {
        tokio::time::sleep(Duration::from_millis(2)).await;
        let tx = db.session().tx().await?;
        tx.execute(stmt).await?;
        tx.commit().await?;
        let (c, u) = read_stamps(&db, q).await?;
        assert_eq!(c0, c, "{stmt}: created_at changed (unflushed)");
        assert!(u > c, "{stmt}: updated_at must follow created_at");
        db.flush().await?;
        let (c, _) = read_stamps(&db, q).await?;
        assert_eq!(c0, c, "{stmt}: created_at changed (flushed)");
    }
    Ok(())
}
