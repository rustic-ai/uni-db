//! Regression tests for VLP (Variable-Length Path) match bugs.
//!
//! Bug 1: Empty interval VLP panics ([*2..1], [*..0])
//! Bug 2: VLP edge property filter not applied during BFS
//! Bug 5: VLP uniqueness/dedup issues on long chains

use anyhow::Result;
use uni_db::{DataType, Uni};

// =============================================================================
// Bug 1: Empty Interval VLP Panic
// =============================================================================

#[tokio::test]
async fn test_vlp_empty_interval_min_gt_max() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema().edge_type("R", &[], &[]).apply().await?;
    db.execute("CREATE (a:N {name: 'A'})-[:R]->(b:N {name: 'B'})")
        .await?;

    // [*2..1] means min_hops=2, max_hops=1 — empty interval, must not panic
    let result = db.query("MATCH (a:N)-[:R*2..1]->(b) RETURN b").await?;
    assert_eq!(
        result.rows().len(),
        0,
        "Empty interval [*2..1] should return 0 rows"
    );

    Ok(())
}

#[tokio::test]
async fn test_vlp_empty_interval_star_to_zero() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema().edge_type("R", &[], &[]).apply().await?;
    db.execute("CREATE (a:N {name: 'A'})-[:R]->(b:N {name: 'B'})")
        .await?;

    // [*..0] means min_hops=1 (default), max_hops=0 — empty interval, must not panic
    let result = db.query("MATCH (a:N)-[:R*..0]->(b) RETURN b").await?;
    assert_eq!(
        result.rows().len(),
        0,
        "Empty interval [*..0] should return 0 rows"
    );

    Ok(())
}

#[tokio::test]
async fn test_vlp_empty_interval_exact_zero_regression() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema().edge_type("R", &[], &[]).apply().await?;
    db.execute("CREATE (a:N {name: 'A'})-[:R]->(b:N {name: 'B'})")
        .await?;

    // [*0..1] means min_hops=0, max_hops=1 — valid interval, regression guard
    // Starting from A: 0-hop gives A, 1-hop gives B → 2 results from source A
    // Starting from B: 0-hop gives B, no outgoing R → 1 result from source B
    // Total: 3 rows
    let result = db
        .query("MATCH (a:N)-[:R*0..1]->(b) RETURN a.name, b.name ORDER BY a.name, b.name")
        .await?;
    assert!(
        result.rows().len() >= 2,
        "Valid interval [*0..1] should return results including zero-length paths"
    );

    Ok(())
}

// =============================================================================
// Bug 2: VLP Edge Property Filter Not Applied During BFS
// =============================================================================

#[tokio::test]
async fn test_vlp_edge_property_filter_single_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("W", &[], &[])
        .property("year", DataType::Int64)
        .apply()
        .await?;

    db.execute(
        r#"
        CREATE (a:P {name: 'A'}), (b:P {name: 'B'}), (c:P {name: 'C'})
        CREATE (a)-[:W {year: 1988}]->(b)
        CREATE (b)-[:W {year: 2000}]->(c)
    "#,
    )
    .await?;

    // VLP with edge property filter — only edges with year=1988 should be traversed
    let result = db
        .query("MATCH (a:P {name: 'A'})-[:W* {year: 1988}]->(x) RETURN x.name")
        .await?;
    assert_eq!(
        result.rows().len(),
        1,
        "VLP edge property filter should only traverse edges matching year=1988"
    );
    assert_eq!(result.rows()[0].get::<String>("x.name")?, "B");

    Ok(())
}

#[tokio::test]
async fn test_vlp_edge_property_filter_no_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("W", &[], &[])
        .property("year", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (a:P {name: 'A'})-[:W {year: 2020}]->(b:P {name: 'B'})")
        .await?;

    // No edge has year=9999
    let result = db
        .query("MATCH (a:P {name: 'A'})-[:W* {year: 9999}]->(x) RETURN x.name")
        .await?;
    assert_eq!(result.rows().len(), 0, "No edges match year=9999");

    Ok(())
}

#[tokio::test]
async fn test_vlp_edge_property_filter_multi_hop() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("K", &[], &[])
        .property("s", DataType::Int64)
        .apply()
        .await?;

    db.execute(
        r#"
        CREATE (a:P {name: 'A'}), (b:P {name: 'B'}), (c:P {name: 'C'})
        CREATE (a)-[:K {s: 10}]->(b)
        CREATE (b)-[:K {s: 10}]->(c)
    "#,
    )
    .await?;

    // All edges have s=10, so multi-hop VLP should follow all
    let result = db
        .query("MATCH (a:P {name: 'A'})-[:K*1..3 {s: 10}]->(x) RETURN x.name ORDER BY x.name")
        .await?;
    assert_eq!(
        result.rows().len(),
        2,
        "Multi-hop VLP with matching edge filter should find B and C"
    );

    Ok(())
}

// =============================================================================
// Bug 5: VLP Uniqueness / Dedup Issues
// =============================================================================

#[tokio::test]
async fn test_vlp_long_chain_single_result() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("X")
        .property("id", DataType::Int64)
        .property("var", DataType::String)
        .apply()
        .await?;
    db.schema().edge_type("N", &[], &[]).apply().await?;

    // Create 5-node chain: 0→1→2→3→4 where node 4 has var='end'
    db.execute(
        r#"
        CREATE (n0:X {id: 0, var: 'mid'}), (n1:X {id: 1, var: 'mid'}), (n2:X {id: 2, var: 'mid'}), (n3:X {id: 3, var: 'mid'}), (n4:X {id: 4, var: 'end'})
        CREATE (n0)-[:N]->(n1)
        CREATE (n1)-[:N]->(n2)
        CREATE (n2)-[:N]->(n3)
        CREATE (n3)-[:N]->(n4)
    "#,
    )
    .await?;

    // Only one path from 0 to the node with var='end'
    let result = db
        .query("MATCH (a:X {id: 0})-[:N*]->(b:X {var: 'end'}) RETURN b.id")
        .await?;
    assert_eq!(
        result.rows().len(),
        1,
        "Long chain should have exactly 1 path to the end node"
    );
    assert_eq!(result.rows()[0].get::<i64>("b.id")?, 4);

    Ok(())
}

#[tokio::test]
async fn test_vlp_chain_all_reachable() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("X")
        .property("id", DataType::Int64)
        .apply()
        .await?;
    db.schema().edge_type("N", &[], &[]).apply().await?;

    // Create 5-node chain: 0→1→2→3→4
    db.execute(
        r#"
        CREATE (n0:X {id: 0}), (n1:X {id: 1}), (n2:X {id: 2}), (n3:X {id: 3}), (n4:X {id: 4})
        CREATE (n0)-[:N]->(n1)
        CREATE (n1)-[:N]->(n2)
        CREATE (n2)-[:N]->(n3)
        CREATE (n3)-[:N]->(n4)
    "#,
    )
    .await?;

    // From node 0, should reach all 4 other nodes
    let result = db
        .query("MATCH (a:X {id: 0})-[:N*]->(b) RETURN b.id ORDER BY b.id")
        .await?;
    assert_eq!(
        result.rows().len(),
        4,
        "Should reach exactly 4 nodes from source"
    );
    assert_eq!(result.rows()[0].get::<i64>("b.id")?, 1);
    assert_eq!(result.rows()[1].get::<i64>("b.id")?, 2);
    assert_eq!(result.rows()[2].get::<i64>("b.id")?, 3);
    assert_eq!(result.rows()[3].get::<i64>("b.id")?, 4);

    Ok(())
}

#[tokio::test]
async fn test_vlp_mixed_directions_simple() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("M")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema().edge_type("R", &[], &[]).apply().await?;

    // A→B, C→B (B has both incoming edges)
    db.execute(
        r#"
        CREATE (a:M {name: 'A'}), (b:M {name: 'B'}), (c:M {name: 'C'})
        CREATE (a)-[:R]->(b)
        CREATE (c)-[:R]->(b)
    "#,
    )
    .await?;

    // Undirected VLP from A — should reach B (direct) and C (via B)
    let result = db
        .query("MATCH (a:M {name: 'A'})-[:R*1..2]-(x) RETURN DISTINCT x.name ORDER BY x.name")
        .await?;
    // A→B (1 hop), A→B←C (2 hops) → reach B and C
    assert_eq!(
        result.rows().len(),
        2,
        "Should find B and C via undirected traversal"
    );

    Ok(())
}

#[tokio::test]
async fn test_vlp_zero_one_hop_count() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Z")
        .property("id", DataType::Int64)
        .apply()
        .await?;
    db.schema().edge_type("E", &[], &[]).apply().await?;

    // 3-node chain: 0→1→2
    db.execute(
        r#"
        CREATE (a:Z {id: 0}), (b:Z {id: 1}), (c:Z {id: 2})
        CREATE (a)-[:E]->(b)
        CREATE (b)-[:E]->(c)
    "#,
    )
    .await?;

    // [*0..1] from node 0: zero-hop=self(0), one-hop=1 → 2 results
    let result = db
        .query("MATCH (a:Z {id: 0})-[:E*0..1]->(b) RETURN b.id ORDER BY b.id")
        .await?;
    assert_eq!(result.rows().len(), 2, "Zero+one hop should give 2 results");
    assert_eq!(result.rows()[0].get::<i64>("b.id")?, 0); // zero-hop self
    assert_eq!(result.rows()[1].get::<i64>("b.id")?, 1); // one-hop neighbor

    Ok(())
}

// =============================================================================
// TCK Match4[5] exact reproduction — "Variable length relationships with property predicate"
// Uses multi-label nodes (Artist:A, Artist:B, Artist:C) to match TCK setup
// =============================================================================

#[tokio::test]
async fn test_tck_match4_5_vlp_edge_property_predicate() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // TCK creates multi-label nodes and edges with properties — no schema
    db.execute(
        "CREATE (a:Artist:A), (b:Artist:B), (c:Artist:C) \
         CREATE (a)-[:WORKED_WITH {year: 1987}]->(b), \
                (b)-[:WORKED_WITH {year: 1988}]->(c)",
    )
    .await?;

    // Only B->C edge has year=1988, so only one path should match: B->C
    let result = db
        .query("MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b:Artist) RETURN count(*) AS cnt")
        .await?;

    let cnt = result.rows()[0].get::<i64>("cnt")?;
    assert_eq!(
        cnt, 1,
        "Only B->C edge has year=1988, so exactly one path should match"
    );

    Ok(())
}

/// Schemaless version — same as above but without schema definitions.
/// This tests the GraphVariableLengthTraverseMainExec code path.
#[tokio::test]
async fn test_tck_match4_5_schemaless_vlp_edge_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:X {name: 'a'}), (b:X {name: 'b'}), (c:X {name: 'c'}) \
         CREATE (a)-[:R {val: 10}]->(b), \
                (b)-[:R {val: 20}]->(c)",
    )
    .await?;

    // Only a->b edge has val=10
    let result = db
        .query("MATCH (a:X)-[:R* {val: 10}]->(b:X) RETURN count(*) AS cnt")
        .await?;

    let cnt = result.rows()[0].get::<i64>("cnt")?;
    assert_eq!(
        cnt, 1,
        "Only a->b edge has val=10, exactly one path matches"
    );

    Ok(())
}

// =============================================================================
// TCK Match4[4] — "Matching longer variable length paths"
// The TCK uses UNWIND+collect to build a 22-node chain.
// We test both the exact TCK setup and a simplified version.
// =============================================================================

/// Simplified version: explicitly create a long chain, test VLP endpoint-to-endpoint
#[tokio::test]
async fn test_tck_match4_4_long_chain_simple() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Build a 22-node chain: start -> n1 -> n2 -> ... -> n20 -> end
    db.execute(
        "CREATE (s {var: 'start'}), \
         (n1 {var: 1}), (n2 {var: 2}), (n3 {var: 3}), (n4 {var: 4}), \
         (n5 {var: 5}), (n6 {var: 6}), (n7 {var: 7}), (n8 {var: 8}), \
         (n9 {var: 9}), (n10 {var: 10}), (n11 {var: 11}), (n12 {var: 12}), \
         (n13 {var: 13}), (n14 {var: 14}), (n15 {var: 15}), (n16 {var: 16}), \
         (n17 {var: 17}), (n18 {var: 18}), (n19 {var: 19}), (n20 {var: 20}), \
         (e {var: 'end'}) \
         CREATE (s)-[:T]->(n1), (n1)-[:T]->(n2), (n2)-[:T]->(n3), (n3)-[:T]->(n4), \
         (n4)-[:T]->(n5), (n5)-[:T]->(n6), (n6)-[:T]->(n7), (n7)-[:T]->(n8), \
         (n8)-[:T]->(n9), (n9)-[:T]->(n10), (n10)-[:T]->(n11), (n11)-[:T]->(n12), \
         (n12)-[:T]->(n13), (n13)-[:T]->(n14), (n14)-[:T]->(n15), (n15)-[:T]->(n16), \
         (n16)-[:T]->(n17), (n17)-[:T]->(n18), (n18)-[:T]->(n19), (n19)-[:T]->(n20), \
         (n20)-[:T]->(e)",
    )
    .await?;

    let result = db
        .query("MATCH (n {var: 'start'})-[:T*]->(m {var: 'end'}) RETURN m")
        .await?;
    assert_eq!(
        result.rows().len(),
        1,
        "22-node chain: exactly 1 path from start to end"
    );

    Ok(())
}

/// Exact TCK Match4[4] setup using UNWIND + collect + list operations
#[tokio::test]
async fn test_tck_match4_4_exact_unwind_collect() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Exact TCK setup
    db.execute(
        "CREATE (a {var: 'start'}), (b {var: 'end'}) \
         WITH * \
         UNWIND range(1, 20) AS i \
         CREATE (n {var: i}) \
         WITH a, b, [a] + collect(n) + [b] AS nodeList \
         UNWIND range(0, size(nodeList) - 2, 1) AS i \
         WITH nodeList[i] AS n1, nodeList[i+1] AS n2 \
         CREATE (n1)-[:T]->(n2)",
    )
    .await?;

    let result = db
        .query("MATCH (n {var: 'start'})-[:T*]->(m {var: 'end'}) RETURN m")
        .await?;
    assert_eq!(
        result.rows().len(),
        1,
        "TCK Match4[4]: UNWIND chain should yield exactly 1 path from start to end"
    );

    Ok(())
}

// =============================================================================
// TCK Match4[7] — "Matching variable length patterns including a bound relationship"
// =============================================================================

#[tokio::test]
async fn test_tck_match4_7_bound_relationship_vlp() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (n0:Node), (n1:Node), (n2:Node), (n3:Node), \
         (n0)-[:EDGE]->(n1), (n1)-[:EDGE]->(n2), (n2)-[:EDGE]->(n3)",
    )
    .await?;

    // First match all edges, then use bound relationship r in a complex pattern
    let result = db
        .query(
            "MATCH ()-[r:EDGE]-() \
             MATCH p = (n)-[*0..1]-()-[r]-()-[*0..1]-(m) \
             RETURN count(p) AS c",
        )
        .await?;

    let c = result.rows()[0].get::<i64>("c")?;
    assert_eq!(c, 32, "TCK Match4[7]: expected count(p) = 32");

    Ok(())
}

// =============================================================================
// TCK Match3[24] — "Matching twice with duplicate relationship types on same relationship"
// Regression guard: bound edge re-verification must still match.
// =============================================================================

#[tokio::test]
async fn test_tck_match3_24_rebound_edge_re_verification() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (:A)-[:T]->(:B)").await?;

    let result = db
        .query(
            "MATCH (a1)-[r:T]->() \
             WITH r, a1 \
             MATCH (a1)-[r:T]->(b2) \
             RETURN a1, r, b2",
        )
        .await?;

    assert_eq!(
        result.rows().len(),
        1,
        "TCK Match3[24]: re-verifying bound edge should return exactly 1 row"
    );

    Ok(())
}

// =============================================================================
// TCK Match8[2] — "Counting rows after MATCH, MERGE, OPTIONAL MATCH"
// =============================================================================

#[tokio::test]
async fn test_tck_match8_2_merge_optional_count() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:A), (b:B) CREATE (a)-[:T1]->(b), (b)-[:T2]->(a)")
        .await?;

    // Diagnostic: check MERGE cross-product step by step
    let r1 = db.query("MATCH (a) RETURN count(*) AS c").await?;
    let c1 = r1.rows()[0].get::<i64>("c")?;
    eprintln!("Step 1 - MATCH (a): {c1}");

    let r2 = db.query("MATCH (a) MERGE (b) RETURN count(*) AS c").await?;
    let c2 = r2.rows()[0].get::<i64>("c")?;
    eprintln!("Step 2 - MATCH (a) MERGE (b): {c2}");

    let r3 = db
        .query("MATCH (a) MERGE (b) WITH * RETURN count(*) AS c")
        .await?;
    let c3 = r3.rows()[0].get::<i64>("c")?;
    eprintln!("Step 3 - MATCH (a) MERGE (b) WITH *: {c3}");

    // Diagnostic: EXPLAIN to see logical plan
    let explain = db
        .explain("MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)--(b) RETURN count(*)")
        .await?;
    eprintln!("EXPLAIN:\n{}", explain.plan_text);

    // Test with unbound target — to verify optional traverse works in general
    let r_unbound = db
        .query("MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)-[r]-(x) RETURN count(*) AS c")
        .await?;
    let c_unbound = r_unbound.rows()[0].get::<i64>("c")?;
    eprintln!("Unbound target OPTIONAL: {c_unbound}");

    let result = db
        .query("MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)--(b) RETURN count(*)")
        .await?;

    let cnt = result.rows()[0].get::<i64>("count(*)")?;
    eprintln!("Full query: {cnt}");

    // Also check without WITH *
    let r_no_with = db
        .query("MATCH (a) MERGE (b) OPTIONAL MATCH (a)--(b) RETURN count(*) AS c")
        .await?;
    let c_no_with = r_no_with.rows()[0].get::<i64>("c")?;
    eprintln!("Without WITH *: {c_no_with}");
    assert_eq!(cnt, 6, "TCK Match8[2]: expected 6 rows");

    Ok(())
}
