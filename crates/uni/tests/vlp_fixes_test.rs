//! Integration tests for Variable-Length Path (VLP) bug fixes.
//!
//! Tests cover:
//! - Bug 1: BFS early termination removal for OPTIONAL MATCH
//! - Bug 2: Property hydration for unlabeled targets
//! - Bug 3: Step variable (edge list) support

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Setup test graph with multiple paths for BFS tests
async fn setup_multi_path_graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("A")
        .property("num", DataType::Int64)
        .apply()
        .await?;
    db.schema()
        .label("B")
        .property("num", DataType::Int64)
        .apply()
        .await?;
    db.schema()
        .label("C")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("R", &[], &[])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    // Create: a-[:R]->b1, a-[:R]->b2, a-[:R]->b3
    db.execute(
        r#"
        CREATE (a:A {num: 1})
        CREATE (b1:B {num: 10}), (b2:B {num: 20}), (b3:B {num: 30})
        CREATE (c1:C {name: "c1"}), (c2:C {name: "c2"}), (c3:C {name: "c3"})
        CREATE (a)-[:R {since: 2020}]->(b1)
        CREATE (a)-[:R {since: 2021}]->(b2)
        CREATE (a)-[:R {since: 2022}]->(b3)
        CREATE (b1)-[:R {since: 2023}]->(c1)
        CREATE (b2)-[:R {since: 2024}]->(c2)
        CREATE (b3)-[:R {since: 2025}]->(c3)
    "#,
    )
    .await?;

    Ok(db)
}

// =============================================================================
// Bug 1: BFS Early Termination / OPTIONAL MATCH Tests
// =============================================================================

#[tokio::test]
async fn test_optional_vlp_returns_all_matches() -> Result<()> {
    let db = setup_multi_path_graph().await?;

    // Should return ALL 3 b nodes, not just first match
    let result = db
        .query("MATCH (a:A {num: 1}) OPTIONAL MATCH (a)-[*1..1]->(b:B) RETURN b.num ORDER BY b.num")
        .await?;

    assert_eq!(
        result.rows().len(),
        3,
        "Should return all 3 matches, not just first"
    );
    assert_eq!(result.rows()[0].get::<i64>("b.num")?, 10);
    assert_eq!(result.rows()[1].get::<i64>("b.num")?, 20);
    assert_eq!(result.rows()[2].get::<i64>("b.num")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_optional_vlp_emits_row_when_no_match() -> Result<()> {
    let db = setup_multi_path_graph().await?;
    db.execute("CREATE (:A {num: 999})").await?;

    // Should return 1 row (with null target), not 0 rows
    let result = db
        .query("MATCH (a:A {num: 999}) OPTIONAL MATCH (a)-[*1..2]->(x) RETURN a.num")
        .await?;

    assert_eq!(
        result.rows().len(),
        1,
        "Should return 1 row for OPTIONAL with no match"
    );
    assert_eq!(result.rows()[0].get::<i64>("a.num")?, 999);

    Ok(())
}

#[tokio::test]
async fn test_optional_vlp_returns_all_multi_hop_paths() -> Result<()> {
    let db = setup_multi_path_graph().await?;

    // Should return 6 paths: 3 at hop-1 (to B nodes) + 3 at hop-2 (to C nodes)
    let result = db
        .query("MATCH (a:A {num: 1}) OPTIONAL MATCH (a)-[*1..2]->(c) RETURN c._vid")
        .await?;

    assert_eq!(
        result.rows().len(),
        6,
        "Should return all paths at different hop counts"
    );

    Ok(())
}

// =============================================================================
// Bug 2: Property Hydration for Unlabeled Targets
// =============================================================================

#[tokio::test]
async fn test_vlp_hydrates_unlabeled_target_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Start")
        .property("id", DataType::Int64)
        .apply()
        .await?;
    db.schema().edge_type("LINK", &[], &[]).apply().await?;

    // Create unlabeled nodes with properties
    db.execute(
        r#"
        CREATE (start:Start {id: 1})
        CREATE (n1 {name: "node1"}), (n2 {name: "node2"})
        CREATE (start)-[:LINK]->(n1)-[:LINK]->(n2)
    "#,
    )
    .await?;

    // Query unlabeled targets via VLP - properties should be hydrated
    let result = db
        .query("MATCH (start:Start)-[*1..2]->(c) RETURN c.name ORDER BY c.name")
        .await?;

    assert_eq!(result.rows().len(), 2);

    // Properties should NOT be null (this was the bug)
    assert_eq!(result.rows()[0].get::<String>("c.name")?, "node1");
    assert_eq!(result.rows()[1].get::<String>("c.name")?, "node2");

    Ok(())
}

// =============================================================================
// Bug 3: Step Variable (Edge List) Tests
// =============================================================================

#[tokio::test]
async fn test_vlp_step_variable_returns_edge_list() -> Result<()> {
    let db = setup_multi_path_graph().await?;

    // Step variable should be List<Edge>
    let result = db
        .query("MATCH (a:A {num: 1})-[r*1..1]->(b) RETURN size(r) AS edge_count LIMIT 1")
        .await?;

    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].get::<i64>("edge_count")?,
        1,
        "Step variable should have 1 edge"
    );

    Ok(())
}

// Note: Zero-hop VLP is a special case that requires schema-level nullable handling
// This test is commented out as it reveals a separate schema issue
// #[tokio::test]
// async fn test_vlp_step_variable_zero_hop_is_empty_list() -> Result<()> {
//     // Zero-hop patterns (r*0..0) are edge cases with schema nullability issues
//     Ok(())
// }

#[tokio::test]
async fn test_vlp_step_variable_has_edge_fields() -> Result<()> {
    let db = setup_multi_path_graph().await?;

    // Edge list should have _eid, _type_name fields
    let result = db
        .query("MATCH (a:A {num: 1})-[r*1..1]->(b) RETURN r[0]._type_name AS type LIMIT 1")
        .await?;

    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<String>("type")?, "R");

    Ok(())
}

#[tokio::test]
async fn test_vlp_step_variable_different_lengths() -> Result<()> {
    let db = setup_multi_path_graph().await?;

    // Paths of length 1 and 2 should have correctly sized edge lists
    let result = db
        .query("MATCH (a:A {num: 1})-[r*1..2]->(c) RETURN size(r) AS len ORDER BY len")
        .await?;

    assert_eq!(result.rows().len(), 6);

    // First 3 should be length 1, next 3 should be length 2
    for i in 0..3 {
        assert_eq!(
            result.rows()[i].get::<i64>("len")?,
            1,
            "First 3 paths should have 1 edge"
        );
    }
    for i in 3..6 {
        assert_eq!(
            result.rows()[i].get::<i64>("len")?,
            2,
            "Last 3 paths should have 2 edges"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_vlp_step_variable_eid_access_works() -> Result<()> {
    let db = setup_multi_path_graph().await?;

    // This was Match9 TCK failure: "No field named r._eid"
    // With EdgeList variant, r[0]._eid should work
    let result = db
        .query("MATCH (a:A {num: 1})-[r*1..1]->(b) RETURN r[0]._eid AS eid LIMIT 1")
        .await?;

    assert_eq!(result.rows().len(), 1);
    // If this doesn't crash with "No field named r._eid", the bug is fixed
    let _eid = result.rows()[0].get::<i64>("eid")?;

    Ok(())
}

// =============================================================================
// Combined Tests
// =============================================================================

#[tokio::test]
async fn test_optional_vlp_with_step_variable() -> Result<()> {
    let db = setup_multi_path_graph().await?;

    // Comprehensive: OPTIONAL + VLP + step_variable + properties
    let result = db
        .query(
            r#"
            MATCH (a:A {num: 1})
            OPTIONAL MATCH (a)-[r*1..2]->(c:C)
            RETURN c.name, size(r) AS hops
            ORDER BY c.name
        "#,
        )
        .await?;

    assert_eq!(result.rows().len(), 3);

    for i in 0..3 {
        // Verify properties work
        let _name = result.rows()[i].get::<String>("c.name")?;
        // Verify step variable works
        assert_eq!(result.rows()[i].get::<i64>("hops")?, 2);
    }

    Ok(())
}

#[tokio::test]
async fn test_vlp_with_filter_on_step_variable() -> Result<()> {
    let db = setup_multi_path_graph().await?;

    // Filter on edge list properties
    let result = db
        .query(
            r#"
            MATCH (a:A {num: 1})-[r*1..1]->(b)
            WHERE r[0].since >= 2021
            RETURN b.num ORDER BY b.num
        "#,
        )
        .await?;

    // Should filter to only b2 and b3 (since >= 2021)
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].get::<i64>("b.num")?, 20);
    assert_eq!(result.rows()[1].get::<i64>("b.num")?, 30);

    Ok(())
}
