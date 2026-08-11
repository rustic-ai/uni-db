// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #166 family — other places a relationship's inline property map might
//! be dropped, found by asking where else the #166 shape occurs.
//!
//! #166 itself was a `MATCH` fixed-length traversal whose edge property filter
//! was gated on the relationship binding a variable. These probe the same
//! question in three other planner paths: MERGE's match phase, multi-hop
//! quantified path patterns, and `shortestPath`.
//!
//! Every probe routes the filtered and unfiltered answers to **different target
//! nodes**, so a dropped predicate shows up as an extra row rather than as the
//! same row arrived at differently. A probe that cannot distinguish the two
//! outcomes is worse than no probe: it reports "fixed" for free.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Two disjoint two-hop routes out of `a1`, one tagged `keep` and one `drop`:
///
/// ```text
/// a1 -[:E {tag:'keep'}]-> b_keep -[:L]-> a_keep
/// a1 -[:E {tag:'drop'}]-> b_drop -[:L]-> a_drop
/// ```
///
/// Any query asking for `keep` must reach only the `_keep` nodes.
async fn fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("n", DataType::String)
        .label("B")
        .property("n", DataType::String)
        .edge_type("E", &["A"], &["B"])
        .property("tag", DataType::String)
        .edge_type("L", &["B"], &["A"])
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for n in ["a1", "a_keep", "a_drop"] {
        tx.execute(&format!("CREATE (:A {{n: '{n}'}})")).await?;
    }
    for n in ["b_keep", "b_drop"] {
        tx.execute(&format!("CREATE (:B {{n: '{n}'}})")).await?;
    }
    for (tag, b) in [("keep", "b_keep"), ("drop", "b_drop")] {
        tx.execute(&format!(
            "MATCH (a:A {{n: 'a1'}}), (b:B {{n: '{b}'}}) \
             CREATE (a)-[:E {{tag: '{tag}'}}]->(b)"
        ))
        .await?;
    }
    for (b, a) in [("b_keep", "a_keep"), ("b_drop", "a_drop")] {
        tx.execute(&format!(
            "MATCH (b:B {{n: '{b}'}}), (a:A {{n: '{a}'}}) CREATE (b)-[:L]->(a)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// Endpoint names for rows that actually produced a path.
///
/// `shortestPath` keeps a row per input pair and sets the path to NULL when no
/// path exists — deliberate, and pinned by
/// `cypher_shortest_path::test_all_shortest_paths_no_path`. So the endpoint
/// column alone cannot tell "reached" from "not reached"; the path length can.
const REACHED: &str =
    "MATCH p = {sp} WHERE length(p) IS NOT NULL RETURN {ret}.n AS n, length(p) AS hops";

fn names(rows: &uni_db::QueryResult, col: &str) -> Vec<String> {
    let mut v: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>(col).ok())
        .collect();
    v.sort();
    v.dedup();
    v
}

/// MERGE's match phase must honour an anonymous relationship's property map.
///
/// If it does not, MERGE treats the existing `tag: 'keep'` edge as a match for
/// `{tag: 'fresh'}` and skips the write — the wrong direction for a statement
/// whose whole purpose is "create it if it isn't there".
#[tokio::test]
async fn merge_honours_anonymous_relationship_property_map() -> Result<()> {
    let db = fixture().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (a:A {n: 'a1'}), (b:B {n: 'b_keep'}) \
         MERGE (a)-[:E {tag: 'fresh'}]->(b)",
    )
    .await?;
    tx.commit().await?;

    let rows = db
        .session()
        .query("MATCH (a:A)-[r:E]->(b:B) WHERE r.tag = 'fresh' RETURN r.tag AS tag")
        .await?;
    assert_eq!(
        rows.len(),
        1,
        "MERGE must create the `tag: 'fresh'` edge; finding {} means its match \
         phase ignored the property map and matched the existing `keep` edge",
        rows.len()
    );
    Ok(())
}

/// A multi-hop quantified path pattern must apply a per-hop property map.
///
/// Discriminating because the two hops lead to different `A` nodes: filtered
/// reaches `a_keep` alone, unfiltered reaches both.
///
/// The trailing `(t:A)` is the pattern's endpoint. A quantified pattern's inner
/// variables are GQL group variables — lists with one element per iteration —
/// so `z` no longer names the node the traversal lands on. The quantifier is
/// kept at `{1,1}` deliberately: dropping it would route the pattern away from
/// the QPP planning path and stop exercising the per-hop maps these tests exist
/// for.
#[tokio::test]
async fn qpp_multi_hop_honours_relationship_property_map() -> Result<()> {
    let db = fixture().await?;
    let rows = db
        .session()
        .query("MATCH ((x:A)-[:E {tag: 'keep'}]->(y:B)-[:L]->(z:A)){1,1}(t:A) RETURN t.n AS n")
        .await?;

    assert_eq!(
        names(&rows, "n"),
        vec!["a_keep".to_string()],
        "a multi-hop QPP must apply the per-hop property map"
    );
    Ok(())
}

/// `shortestPath` must apply a relationship property map.
///
/// Discriminating because the two `E` edges end at different `B` nodes, so an
/// unfiltered search yields a shortest path to each.
#[tokio::test]
async fn shortest_path_honours_relationship_property_map() -> Result<()> {
    let db = fixture().await?;
    let rows = db
        .session()
        .query(
            &REACHED
                .replace(
                    "{sp}",
                    "shortestPath((a:A {n: 'a1'})-[:E {tag: 'keep'}]->(b:B))",
                )
                .replace("{ret}", "b"),
        )
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["b_keep".to_string()],
        "shortestPath must apply the relationship property map"
    );
    Ok(())
}

/// `allShortestPaths` must apply the map too.
///
/// Its backward reconstruction walks a `predecessors` map built during the
/// forward BFS, so gating the forward pass should be sufficient — asserted
/// rather than assumed.
#[tokio::test]
async fn all_shortest_paths_honours_relationship_property_map() -> Result<()> {
    let db = fixture().await?;
    let rows = db
        .session()
        .query(
            &REACHED
                .replace(
                    "{sp}",
                    "allShortestPaths((a:A {n: 'a1'})-[:E {tag: 'keep'}]->(b:B))",
                )
                .replace("{ret}", "b"),
        )
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["b_keep".to_string()],
        "allShortestPaths must apply the relationship property map"
    );
    Ok(())
}

/// The relationship carried in the returned path must itself satisfy the map.
///
/// Gating the BFS alone is not enough: after a path is found, each consecutive
/// pair is re-resolved to a concrete edge by taking the *first* neighbour
/// matching the destination. With two `E` edges between the **same** pair, the
/// walk is admitted via `keep` while the materialized relationship can come
/// back as `drop` — a path that contradicts the query that produced it. Needs
/// its own fixture: the shared one routes `keep` and `drop` to different nodes,
/// so it cannot exercise this at all.
#[tokio::test]
async fn shortest_path_returns_the_matching_parallel_edge() -> Result<()> {
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
    // Both edges join the SAME pair; only the tag distinguishes them. The
    // order matters: with `keep` inserted first, an ungated re-resolution
    // returns `drop`, so this probe can tell the two outcomes apart. Reversing
    // it makes the test pass whether or not the gate is there.
    for tag in ["keep", "drop"] {
        tx.execute(&format!(
            "MATCH (a:A {{n: 'a1'}}), (b:B {{n: 'b1'}}) CREATE (a)-[:E {{tag: '{tag}'}}]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;

    // `head([r IN relationships(p) | r.tag])` rather than
    // `UNWIND relationships(p)`, which yields no rows here — a separate,
    // pre-existing limitation unrelated to the property map.
    let rows = db
        .session()
        .query(
            "MATCH p = shortestPath((a:A {n: 'a1'})-[:E {tag: 'keep'}]->(b:B)) \
             RETURN head([r IN relationships(p) | r.tag]) AS n",
        )
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["keep".to_string()],
        "the relationship materialized into the path must be the one that \
         satisfied the property map, not a parallel edge between the same pair"
    );
    Ok(())
}

/// `shortestPath` must respect an upper hop bound.
///
/// `a1 -[:E]-> b_keep -[:L]-> a_keep` is two hops, so a `*1..1` bound admits no
/// path at all. The bound was parsed and then discarded before this — the
/// planner computed `max_hops` and the physical planner dropped it on the
/// floor, so the search was always unbounded.
#[tokio::test]
async fn shortest_path_respects_max_hops() -> Result<()> {
    let db = fixture().await?;

    let two_hops = db
        .session()
        .query(
            &REACHED
                .replace("{sp}", "shortestPath((a:A {n: 'a1'})-[:E|L*1..2]->(z:A))")
                .replace("{ret}", "z"),
        )
        .await?;
    assert!(
        names(&two_hops, "n").contains(&"a_keep".to_string()),
        "precondition: a_keep is reachable from a1 within two hops"
    );

    let one_hop = names(
        &db.session()
            .query(
                &REACHED
                    .replace("{sp}", "shortestPath((a:A {n: 'a1'})-[:E|L*1..1]->(z:A))")
                    .replace("{ret}", "z"),
            )
            .await?,
        "n",
    );
    assert!(
        !one_hop.contains(&"a_keep".to_string()) && !one_hop.contains(&"a_drop".to_string()),
        "a one-hop bound must not reach a two-hop target; got {one_hop:?}"
    );
    Ok(())
}

/// A lower hop bound above 1 is refused rather than silently ignored.
///
/// Honouring it means not returning on first sight of the target and relaxing
/// the visited-set semantics, which changes BFS termination. Until then an
/// error beats a path that quietly violates the bound the user wrote.
#[tokio::test]
async fn shortest_path_refuses_min_hops_above_one() -> Result<()> {
    let db = fixture().await?;
    let err = db
        .session()
        .query("MATCH p = shortestPath((a:A {n: 'a1'})-[:E|L*2..3]->(z:A)) RETURN z.n AS n")
        .await
        .expect_err("a minimum hop bound above 1 must be refused, not ignored");
    let msg = err.to_string();
    assert!(
        msg.contains("minimum") || msg.contains("min_hops") || msg.contains("not support"),
        "the refusal must say what is unsupported: {msg}"
    );
    Ok(())
}

/// A variable-length pattern must apply an anonymous relationship's property
/// map, which it does through `edge_filter_expr` rather than a `Filter` node.
///
/// Pinned because the black book states it, and because the VLP branch is the
/// one place that already had a synthesized-name fallback — making it the
/// obvious thing to assume works without checking.
#[tokio::test]
async fn variable_length_honours_relationship_property_map() -> Result<()> {
    let db = fixture().await?;
    let rows = db
        .session()
        .query("MATCH (a:A {n: 'a1'})-[:E*1..1 {tag: 'keep'}]->(b:B) RETURN b.n AS n")
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["b_keep".to_string()],
        "a variable-length pattern must apply the relationship property map"
    );
    Ok(())
}

/// A zero-length path is only legal when the lower bound allows it.
///
/// `shortestPath` short-circuits when source and target resolve to the same
/// vertex. That is right for `*0..`, and wrong for any pattern demanding at
/// least one hop — which is every pattern without an explicit `0` lower bound,
/// since `min_hops` defaults to 1.
#[tokio::test]
async fn shortest_path_zero_length_requires_min_hops_zero() -> Result<()> {
    let db = fixture().await?;

    let with_zero = names(
        &db.session()
            .query(
                &REACHED
                    .replace("{sp}", "shortestPath((a:A {n: 'a1'})-[:E|L*0..2]->(z:A))")
                    .replace("{ret}", "z"),
            )
            .await?,
        "n",
    );
    assert!(
        with_zero.contains(&"a1".to_string()),
        "a *0.. bound must admit the zero-length self path; got {with_zero:?}"
    );

    let without_zero = names(
        &db.session()
            .query(
                &REACHED
                    .replace("{sp}", "shortestPath((a:A {n: 'a1'})-[:E|L*1..2]->(z:A))")
                    .replace("{ret}", "z"),
            )
            .await?,
        "n",
    );
    assert!(
        !without_zero.contains(&"a1".to_string()),
        "a *1.. bound must not admit the zero-length self path; got {without_zero:?}"
    );
    Ok(())
}

/// Two-hop QPP fixture with a `keep` chain long enough to iterate twice, and a
/// `drop` chain that must never be reached when the maps ask for `keep`:
///
/// ```text
/// a1 -E{keep}-> bk1 -L{keep}-> ak1 -E{keep}-> bk2 -L{keep}-> ak2
/// a1 -E{drop}-> bd1 -L{drop}-> ad1
/// ```
///
/// Both `E` and `L` carry `tag`, so a map can be placed on either hop
/// independently — that is what distinguishes a genuinely per-hop predicate
/// from one applied uniformly to the whole pattern.
async fn qpp_fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("n", DataType::String)
        .label("B")
        .property("n", DataType::String)
        .edge_type("E", &["A"], &["B"])
        .property("tag", DataType::String)
        .edge_type("L", &["B"], &["A"])
        .property("tag", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for n in ["a1", "ak1", "ak2", "ad1"] {
        tx.execute(&format!("CREATE (:A {{n: '{n}'}})")).await?;
    }
    for n in ["bk1", "bk2", "bd1"] {
        tx.execute(&format!("CREATE (:B {{n: '{n}'}})")).await?;
    }
    for (a, tag, b) in [
        ("a1", "keep", "bk1"),
        ("ak1", "keep", "bk2"),
        ("a1", "drop", "bd1"),
    ] {
        tx.execute(&format!(
            "MATCH (a:A {{n: '{a}'}}), (b:B {{n: '{b}'}}) \
             CREATE (a)-[:E {{tag: '{tag}'}}]->(b)"
        ))
        .await?;
    }
    for (b, tag, a) in [
        ("bk1", "keep", "ak1"),
        ("bk2", "keep", "ak2"),
        ("bd1", "drop", "ad1"),
    ] {
        tx.execute(&format!(
            "MATCH (b:B {{n: '{b}'}}), (a:A {{n: '{a}'}}) \
             CREATE (b)-[:L {{tag: '{tag}'}}]->(a)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// A map on the **second** hop only.
///
/// The sharpest probe in this file: a predicate applied uniformly to the whole
/// pattern — or indexed with an off-by-one — cannot produce this answer, because
/// hop 1 (`E`) must stay unconstrained while hop 2 (`L`) is constrained.
#[tokio::test]
async fn qpp_applies_a_map_on_the_second_hop_only() -> Result<()> {
    let db = qpp_fixture().await?;
    let rows = db
        .session()
        .query("MATCH ((x:A)-[:E]->(y:B)-[:L {tag: 'keep'}]->(z:A)){1,1}(t:A) RETURN t.n AS n")
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["ak1".to_string(), "ak2".to_string()],
        "a map on hop 2 must constrain hop 2 and leave hop 1 open"
    );
    Ok(())
}

/// Maps on both hops at once.
///
/// Paired on purpose. The negative half alone is a weak probe — an emptiness
/// assertion also passes when the implementation is broken towards matching
/// *nothing*, which is exactly what a single filter shared across hops does
/// here (hop 2's `L` edges are absent from hop 1's `E` filter). The positive
/// half is what makes the pair discriminating in both directions.
#[tokio::test]
async fn qpp_applies_maps_on_both_hops() -> Result<()> {
    let db = qpp_fixture().await?;

    let agreeing = db
        .session()
        .query(
            "MATCH ((x:A)-[:E {tag: 'keep'}]->(y:B)-[:L {tag: 'keep'}]->(z:A)){1,1}(t:A) \
             RETURN t.n AS n",
        )
        .await?;
    assert_eq!(
        names(&agreeing, "n"),
        vec!["ak1".to_string(), "ak2".to_string()],
        "keep-then-keep must still traverse both keep hops"
    );

    let conflicting = db
        .session()
        .query(
            "MATCH ((x:A)-[:E {tag: 'keep'}]->(y:B)-[:L {tag: 'drop'}]->(z:A)){1,1}(t:A) \
             RETURN t.n AS n",
        )
        .await?;
    assert!(
        names(&conflicting, "n").is_empty(),
        "no path is keep-then-drop; got {:?}",
        names(&conflicting, "n")
    );
    Ok(())
}

/// Two iterations, so the per-hop slots must cycle correctly past the first.
#[tokio::test]
async fn qpp_applies_maps_across_iterations() -> Result<()> {
    let db = qpp_fixture().await?;
    let rows = db
        .session()
        .query(
            "MATCH ((x:A)-[:E {tag: 'keep'}]->(y:B)-[:L {tag: 'keep'}]->(z:A)){2,2}(t:A) \
             RETURN t.n AS n",
        )
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["ak2".to_string()],
        "two iterations of the keep chain must land on ak2"
    );
    Ok(())
}

/// An inline property map on an **intermediate node** must filter too.
///
/// Silently dropped before this: the step loop read `node.labels.first()` and
/// never `node.properties`.
#[tokio::test]
async fn qpp_applies_intermediate_node_property_map() -> Result<()> {
    let db = qpp_fixture().await?;
    let rows = db
        .session()
        .query("MATCH ((x:A)-[:E]->(y:B {n: 'bk1'})-[:L]->(z:A)){1,1}(t:A) RETURN t.n AS n")
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["ak1".to_string()],
        "only the path through bk1 may survive an intermediate node map"
    );
    Ok(())
}

/// The same, against flushed data.
///
/// Guards the trap that #135 and #141 hit: the synchronous per-vertex property
/// accessor reads the L0 chain only, so a flushed vertex looks property-less
/// and an unguarded implementation admits it.
#[tokio::test]
async fn qpp_intermediate_node_property_map_after_flush() -> Result<()> {
    let db = qpp_fixture().await?;
    db.flush().await?;
    let rows = db
        .session()
        .query("MATCH ((x:A)-[:E]->(y:B {n: 'bk1'})-[:L]->(z:A)){1,1}(t:A) RETURN t.n AS n")
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["ak1".to_string()],
        "an intermediate node map must hold after the data is flushed"
    );
    Ok(())
}

/// An intermediate node map filters even when the node carries no label.
///
/// The labelled read projects to the label's table; without a label the
/// label-free batch read resolves per-label and then the schemaless main table,
/// so there is no reason to refuse the pattern.
#[tokio::test]
async fn qpp_applies_intermediate_node_map_without_a_label() -> Result<()> {
    let db = qpp_fixture().await?;
    let rows = db
        .session()
        .query("MATCH ((x:A)-[:E]->(y {n: 'bk1'})-[:L]->(z:A)){1,1}(t:A) RETURN t.n AS n")
        .await?;
    assert_eq!(
        names(&rows, "n"),
        vec!["ak1".to_string()],
        "an unlabelled intermediate node map must still filter"
    );
    Ok(())
}
