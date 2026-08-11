// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #168 — a read-only handle that has once traversed an edge type never
//! observes edges of that type created afterwards, even after the writer has
//! committed and flushed.
//!
//! Nodes from the same handle, in the same session, stay fresh. So a reader can
//! observe that a vertex was added and not observe the edge that connects it.
//!
//! Candidate root cause: `AdjacencyManager` caches a warmed CSR in `main_csr`,
//! keyed only by `(edge_type, direction)`, and `has_csr` is a bare presence
//! check with no version comparison — the type's own doc comment states "Data
//! flush never invalidates or rebuilds the CSR". That is sound for the
//! in-process writer, whose commits go straight into `active_overlay` via
//! `insert_edge`, so it never needs to re-read. It does not hold for a second,
//! independent handle: that handle's overlay never receives those calls, and
//! its only route to new edges is a re-warm that `has_csr` permanently blocks.
//!
//! Node reads are unaffected because every scan reopens the Lance dataset and
//! so picks up the newest manifest. Edges are the only path that materialises
//! and then permanently caches a *derived* structure.
//!
//! The discriminating variable is stated by the issue and pinned below: a
//! handle that never traversed the edge type, or was warmed only by a *node*
//! query, reads correctly. Only the prior edge read pins it.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// A store with one `(:A)-[:E]->(:B)` edge, committed and flushed.
async fn seed(path: &str) -> Result<()> {
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("A")
        .property("n", DataType::String)
        .label("B")
        .property("n", DataType::String)
        .edge_type("E", &["A"], &["B"])
        .property("w", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {n: 'a1'})").await?;
    tx.execute("CREATE (:B {n: 'b1'})").await?;
    tx.execute("MATCH (a:A {n: 'a1'}), (b:B {n: 'b1'}) CREATE (a)-[:E {w: 1}]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    db.shutdown().await?;
    Ok(())
}

/// Adds a second `(:A)-[:E]->(:B)` pair through an independent writer handle,
/// then commits and flushes so the write is durable and visible to any reader.
async fn write_second_edge(path: &str) -> Result<()> {
    let db = Uni::open(path).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {n: 'a2'})").await?;
    tx.execute("CREATE (:B {n: 'b2'})").await?;
    tx.execute("MATCH (a:A {n: 'a2'}), (b:B {n: 'b2'}) CREATE (a)-[:E {w: 2}]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    db.shutdown().await?;
    Ok(())
}

const EDGE_Q: &str = "MATCH (a:A)-[e:E]->(b:B) RETURN e.w AS w";
const NODE_Q: &str = "MATCH (a:A) RETURN a.n AS n";

/// The reported case: warm the reader on an **edge** query, then write.
#[tokio::test]
async fn readonly_handle_warmed_on_edges_observes_later_edges() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    let warm = reader.session().query(EDGE_Q).await?;
    assert_eq!(warm.len(), 1, "precondition: the seeded edge is visible");

    write_second_edge(path).await?;

    // Nodes stay fresh on the very same handle — this is the asymmetry that
    // makes the staleness invisible to a caller.
    let nodes = reader.session().query(NODE_Q).await?;
    assert_eq!(nodes.len(), 2, "precondition: node reads observe the write");

    let edges = reader.session().query(EDGE_Q).await?;
    assert_eq!(
        edges.len(),
        2,
        "a read-only handle warmed on an edge query must observe edges committed \
         and flushed afterwards; got {} (nodes were correctly {})",
        edges.len(),
        nodes.len()
    );
    Ok(())
}

/// A fresh session from the same handle must not inherit the staleness — the
/// issue reports it does, which localises the cache below the session.
#[tokio::test]
async fn readonly_handle_new_session_observes_later_edges() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    reader.session().query(EDGE_Q).await?;
    write_second_edge(path).await?;

    let edges = reader.session().query(EDGE_Q).await?;
    assert_eq!(
        edges.len(),
        2,
        "a new session from the same handle must observe the flushed edge"
    );
    Ok(())
}

/// `refresh()` looks like the invalidation hook and is documented as picking up
/// "the latest committed version from storage". If the caching is deliberate,
/// this is the method that must clear it.
#[tokio::test]
async fn refresh_clears_the_staleness() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    let mut session = reader.session();
    session.query(EDGE_Q).await?;
    write_second_edge(path).await?;

    assert!(
        !session.is_pinned(),
        "precondition: nothing reports the session as pinned"
    );
    session.refresh().await?;
    let edges = session.query(EDGE_Q).await?;
    assert_eq!(
        edges.len(),
        2,
        "refresh() must return the session to the live database state"
    );
    Ok(())
}

/// Control A: a handle warmed only by a **node** query reads edges correctly.
#[tokio::test]
async fn readonly_handle_warmed_on_nodes_observes_later_edges() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    reader.session().query(NODE_Q).await?;
    write_second_edge(path).await?;

    let edges = reader.session().query(EDGE_Q).await?;
    assert_eq!(edges.len(), 2, "node-warmed control must read edges fresh");
    Ok(())
}

/// Control B: a handle that read nothing before the write is correct.
#[tokio::test]
async fn readonly_handle_never_warmed_observes_later_edges() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    write_second_edge(path).await?;

    let edges = reader.session().query(EDGE_Q).await?;
    assert_eq!(edges.len(), 2, "never-warmed control must read edges fresh");
    Ok(())
}
