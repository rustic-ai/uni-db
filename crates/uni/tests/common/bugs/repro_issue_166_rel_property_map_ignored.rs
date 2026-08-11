// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #166 — an inline property map on an **anonymous** relationship
//! pattern is parsed and then discarded, so `-[:E {tag: 'keep'}]->` matches
//! every `E` edge.
//!
//! The map on a *node* pattern is honoured, which is what makes this hard to
//! see: the syntax is uniform, the behaviour is not. The failure is silent and
//! fails **open** — a filter that selects nothing returns a plausible,
//! fully-populated answer that is merely too large.
//!
//! The discriminating variable is whether the relationship pattern **binds a
//! variable**, not which language issued the query: `[r:E {tag:'keep'}]`
//! filters correctly and `[:E {tag:'keep'}]` does not. Cypher and Locy share
//! the planner path (`locy_planner` calls `QueryPlanner::plan_pattern`), so
//! both inherit the same omission from one site.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Two `E` edges between the same pair, tagged `keep` and `drop`.
async fn fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("n", DataType::String)
        .label("B")
        .property("n", DataType::String)
        .edge_type("E", &["A"], &["B"])
        .property("tag", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {n: 'a1'})").await?;
    tx.execute("CREATE (:B {n: 'b1'})").await?;
    tx.execute("MATCH (a:A {n: 'a1'}), (b:B {n: 'b1'}) CREATE (a)-[:E {tag: 'keep'}]->(b)")
        .await?;
    tx.execute("MATCH (a:A {n: 'a1'}), (b:B {n: 'b1'}) CREATE (a)-[:E {tag: 'drop'}]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// The reported case: anonymous relationship, inline property map.
#[tokio::test]
async fn anonymous_relationship_property_map_filters() -> Result<()> {
    let db = fixture().await?;
    let rows = db
        .session()
        .query("MATCH (a:A)-[:E {tag: 'keep'}]->(b:B) RETURN a.n AS n")
        .await?;
    assert_eq!(
        rows.len(),
        1,
        "an inline property map on an anonymous relationship must filter; \
         got {} rows, so the map was discarded and every E edge matched",
        rows.len()
    );
    Ok(())
}

/// Control A: the same map on a **named** relationship already works. This is
/// the line that localises the defect to the missing anonymous-variable
/// fallback rather than to property-map evaluation as such.
#[tokio::test]
async fn named_relationship_property_map_filters() -> Result<()> {
    let db = fixture().await?;
    let rows = db
        .session()
        .query("MATCH (a:A)-[r:E {tag: 'keep'}]->(b:B) RETURN a.n AS n")
        .await?;
    assert_eq!(rows.len(), 1, "named-relationship control must filter");
    Ok(())
}

/// Control B: the same construct on a **node** pattern. Nodes always get a
/// synthesized anonymous variable, so their filter is built unconditionally.
#[tokio::test]
async fn node_property_map_filters() -> Result<()> {
    let db = fixture().await?;
    let rows = db
        .session()
        .query("MATCH (a:A {n: 'a1'}) RETURN a.n AS n")
        .await?;
    assert_eq!(rows.len(), 1, "node-pattern control must filter");
    Ok(())
}

/// The same anonymous pattern issued through Locy. Shares `plan_pattern` with
/// Cypher, so this is a second observation of one defect rather than a second
/// defect — pinned so a fix that reached only one front-end would be caught.
#[tokio::test]
async fn locy_anonymous_relationship_property_map_filters() -> Result<()> {
    let db = fixture().await?;
    let result = db
        .session()
        .locy(
            "CREATE RULE probe_inline AS\n\
             MATCH (a:A)-[:E {tag: 'keep'}]->(b:B)\n\
             YIELD KEY a, KEY b, 1 AS hit",
        )
        .await?;
    let facts = result.derived.get("probe_inline").map_or(0, Vec::len);
    assert_eq!(
        facts, 1,
        "Locy must honour the inline relationship property map; got {facts} facts"
    );
    Ok(())
}

/// Control C: Locy filtering through an explicit `WHERE` on a bound
/// relationship — the documented workaround, which must keep working.
#[tokio::test]
async fn locy_where_on_bound_relationship_filters() -> Result<()> {
    let db = fixture().await?;
    let result = db
        .session()
        .locy(
            "CREATE RULE probe_where AS\n\
             MATCH (a:A)-[r:E]->(b:B)\n\
             WHERE r.tag = 'keep'\n\
             YIELD KEY a, KEY b, 1 AS hit",
        )
        .await?;
    let facts = result.derived.get("probe_where").map_or(0, Vec::len);
    assert_eq!(facts, 1, "WHERE-on-bound-relationship control must filter");
    Ok(())
}
