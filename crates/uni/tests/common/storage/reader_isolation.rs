// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for reader isolation: data visibility through create, flush, delete lifecycle.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_reader_isolation_lifecycle() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // 1. Insert via transaction (data in L0, no flush)
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    // 2. Query should see committed data
    let result = db
        .session()
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name")
        .await?;
    assert_eq!(result.len(), 1, "Should find Alice after commit");
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");

    // 3. Flush to storage
    db.flush().await?;

    // 4. Query should still see data after flush
    let result = db
        .session()
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name")
        .await?;
    assert_eq!(result.len(), 1, "Should find Alice after flush");

    // 5. Delete via transaction
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:Person {name: 'Alice'}) DELETE n")
        .await?;
    tx.commit().await?;

    // 6. Query should NOT see deleted data (tombstone in L0)
    let result = db
        .session()
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .await?;
    assert_eq!(result.len(), 0, "Should NOT find Alice (L0 tombstone)");

    // 7. Flush deletion to storage
    db.flush().await?;

    // 8. Still should not see deleted data
    let result = db
        .session()
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .await?;
    assert_eq!(
        result.len(),
        0,
        "Should NOT find Alice (deleted in storage)"
    );

    Ok(())
}

/// Issue #282 — a second handle on the same store read every committed vertex
/// and none of the committed edges, silently.
///
/// Two handles share nothing in-process: the second builds its own
/// `StorageManager`, `AdjacencyManager` and L0, and sees the first handle's
/// committed-but-unflushed writes only by replaying the WAL beyond the manifest
/// it read. Recovery restored those edges into the L0 buffer but not into the
/// adjacency overlay, which is the only place the traversal read path looks for
/// an unflushed edge — so `MATCH (a)-[r]->(b)` answered zero rows while every
/// vertex came back. Same root cause as issue #281, reached without a crash.
///
/// The vertex assertions are the control: they hold on the buggy build, so a
/// regression that broke cross-handle visibility outright would fail here with
/// a different message rather than passing this test for the wrong reason.
#[tokio::test]
async fn second_handle_sees_committed_edges() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = dir.path().join("store");
    let path = store.to_string_lossy().to_string();

    let writer = Uni::open(&path).build().await?;
    writer
        .schema()
        .label("Entity")
        .property("name", DataType::String)
        .done()
        .edge_type("OWNS", &["Entity"], &["Entity"])
        .property("pct", DataType::Float)
        .done()
        .apply()
        .await?;

    // Vertices and edges in separate transactions, as in the report.
    let session = writer.session();
    let tx = session.tx().await?;
    for name in ["N6", "N8", "N1"] {
        tx.execute_with("CREATE (:Entity {name: $n})")
            .param("n", name)
            .run()
            .await?;
    }
    tx.commit().await?;

    let tx = session.tx().await?;
    for (owner, asset, pct) in [("N6", "N8", 11.64_f64), ("N8", "N1", 59.24)] {
        tx.execute_with(
            "MATCH (o:Entity {name: $o}), (a:Entity {name: $a}) \
             CREATE (o)-[:OWNS {pct: $p}]->(a)",
        )
        .param("o", owner)
        .param("a", asset)
        .param("p", pct)
        .run()
        .await?;
    }
    tx.commit().await?;

    // No flush and no shutdown: the edges are WAL-durable and nothing more,
    // which is the window the bug lives in. Asserting the writer's own view
    // first keeps a failure here from being blamed on the reader.
    let (w_vertices, w_edges) = survey(&session).await?;
    assert_eq!(
        (w_vertices, w_edges),
        (3, 2),
        "writing handle lost its own committed data"
    );

    // Second handle, same path, still open alongside the first. It is
    // read-only: a second writable handle is refused (issue #348).
    let reader = Uni::open(&path).read_only().build().await?;
    let reader_session = reader.session();
    let (r_vertices, r_edges) = survey(&reader_session).await?;

    assert_eq!(
        r_vertices, 3,
        "control: the second handle did not replay committed vertices either, \
         so this run says nothing about the edge path"
    );
    assert_eq!(
        r_edges, 2,
        "second handle replayed all {w_vertices} committed vertices but {r_edges} \
         of {w_edges} committed edges (issue #282)"
    );

    let pairs = reader_session
        .query("MATCH (a:Entity)-[:OWNS]->(b:Entity) RETURN a.name AS a, b.name AS b")
        .await?;
    let mut seen: Vec<(String, String)> = pairs
        .rows()
        .iter()
        .map(|r| Ok((r.get::<String>("a")?, r.get::<String>("b")?)))
        .collect::<Result<_>>()?;
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("N6".to_string(), "N8".to_string()),
            ("N8".to_string(), "N1".to_string()),
        ],
        "the second handle recovered the right number of edges but not the right ones"
    );

    Ok(())
}

/// Counts committed vertices and `OWNS` edges visible to `session`.
async fn survey(session: &uni_db::Session) -> Result<(i64, i64)> {
    let vertices = session
        .query("MATCH (e:Entity) RETURN count(*) AS n")
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    let edges = session
        .query("MATCH (:Entity)-[:OWNS]->(:Entity) RETURN count(*) AS n")
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    Ok((vertices, edges))
}
