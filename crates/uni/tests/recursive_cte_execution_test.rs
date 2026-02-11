// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

// ---------------------------------------------------------------------------
// Helper: build a common graph for hierarchy tests
// ---------------------------------------------------------------------------

/// Creates an in-memory DB with `Item` nodes (Int32 `id`) and `CHILD` edges.
/// Builds a linear chain: 0 → 1 → 2.
async fn setup_linear_chain() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    db.execute("CREATE (n0:Item {id: 0})").await?;
    db.execute("CREATE (n1:Item {id: 1})").await?;
    db.execute("CREATE (n2:Item {id: 2})").await?;

    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 1}), (b:Item {id: 2}) CREATE (a)-[:CHILD]->(b)")
        .await?;

    Ok(db)
}

// ===========================================================================
// Happy Path Tests
// ===========================================================================

/// Original test: linear chain 0 → 1 → 2, start at root 0.
#[tokio::test]
async fn test_recursive_cte_linear_chain() -> Result<()> {
    let db = setup_linear_chain().await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    assert_eq!(result.len(), 3);
    let rows = result.rows();
    assert_eq!(rows[0].get::<i32>("id")?, 0);
    assert_eq!(rows[1].get::<i32>("id")?, 1);
    assert_eq!(rows[2].get::<i32>("id")?, 2);

    Ok(())
}

/// Branching tree: root has multiple children (fan-out).
///
/// ```text
///       0
///      / \
///     1   2
///    / \
///   3   4
/// ```
#[tokio::test]
async fn test_recursive_cte_branching_tree() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    for i in 0..5 {
        db.execute(&format!("CREATE (:Item {{id: {}}})", i)).await?;
    }

    // 0 → 1, 0 → 2, 1 → 3, 1 → 4
    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 2}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 1}), (b:Item {id: 3}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 1}), (b:Item {id: 4}) CREATE (a)-[:CHILD]->(b)")
        .await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    assert_eq!(result.len(), 5);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);

    Ok(())
}

/// Deeper chain: 0 → 1 → 2 → 3 → 4 → 5 → 6 (7 levels).
#[tokio::test]
async fn test_recursive_cte_deep_chain() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    let depth = 7;
    for i in 0..depth {
        db.execute(&format!("CREATE (:Item {{id: {}}})", i)).await?;
    }
    for i in 0..depth - 1 {
        db.execute(&format!(
            "MATCH (a:Item {{id: {}}}), (b:Item {{id: {}}}) CREATE (a)-[:CHILD]->(b)",
            i,
            i + 1
        ))
        .await?;
    }

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    assert_eq!(result.len(), depth as usize);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, (0..depth).collect::<Vec<i32>>());

    Ok(())
}

/// Diamond/DAG: A→B, A→C, B→D, C→D.
/// Tests that D is not duplicated despite two paths reaching it.
///
/// ```text
///     0
///    / \
///   1   2
///    \ /
///     3
/// ```
#[tokio::test]
async fn test_recursive_cte_diamond_deduplication() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    for i in 0..4 {
        db.execute(&format!("CREATE (:Item {{id: {}}})", i)).await?;
    }

    // 0 → 1, 0 → 2, 1 → 3, 2 → 3
    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 2}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 1}), (b:Item {id: 3}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 2}), (b:Item {id: 3}) CREATE (a)-[:CHILD]->(b)")
        .await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    // Node 3 should appear exactly once despite two paths
    assert_eq!(result.len(), 4);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![0, 1, 2, 3]);

    Ok(())
}

/// Cycle: 0 → 1 → 2 → 0. CTE should terminate and not loop forever.
#[tokio::test]
async fn test_recursive_cte_cycle_terminates() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    for i in 0..3 {
        db.execute(&format!("CREATE (:Item {{id: {}}})", i)).await?;
    }

    // 0 → 1 → 2 → 0 (cycle)
    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 1}), (b:Item {id: 2}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 2}), (b:Item {id: 0}) CREATE (a)-[:CHILD]->(b)")
        .await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    // All 3 nodes reachable, but no infinite loop
    assert_eq!(result.len(), 3);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![0, 1, 2]);

    Ok(())
}

/// Self-loop: node 0 has an edge to itself (0 → 0).
/// CTE should detect the cycle and terminate.
#[tokio::test]
async fn test_recursive_cte_self_loop() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    db.execute("CREATE (:Item {id: 0})").await?;
    db.execute("CREATE (:Item {id: 1})").await?;

    // 0 → 0 (self-loop) and 0 → 1
    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 0}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    assert_eq!(result.len(), 2);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![0, 1]);

    Ok(())
}

/// Multiple anchor roots: start from two different nodes simultaneously.
#[tokio::test]
async fn test_recursive_cte_multiple_anchor_roots() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int64)
        .property("is_root", DataType::Bool)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    // Two separate trees: 0 → 1 and 10 → 11
    db.execute("CREATE (:Item {id: 0, is_root: true})").await?;
    db.execute("CREATE (:Item {id: 1, is_root: false})").await?;
    db.execute("CREATE (:Item {id: 10, is_root: true})").await?;
    db.execute("CREATE (:Item {id: 11, is_root: false})")
        .await?;

    db.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    db.execute("MATCH (a:Item {id: 10}), (b:Item {id: 11}) CREATE (a)-[:CHILD]->(b)")
        .await?;

    // Anchor returns both roots via boolean flag
    let result = db
        .query(
            "
        WITH RECURSIVE reachable AS (
            MATCH (root:Item) WHERE root.is_root = true RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN reachable
            RETURN child
        )
        MATCH (n:Item) WHERE n IN reachable
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    assert_eq!(result.len(), 4);
    let ids: Vec<i64> = result
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![0, 1, 10, 11]);

    Ok(())
}

/// Disconnected graph: CTE should only find reachable nodes from anchor.
///
/// Graph: 0 → 1 → 2, and isolated node 99.
#[tokio::test]
async fn test_recursive_cte_disconnected_graph() -> Result<()> {
    let db = setup_linear_chain().await?;

    // Add an isolated node
    db.execute("CREATE (:Item {id: 99})").await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    // Only 0, 1, 2 — not 99
    assert_eq!(result.len(), 3);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![0, 1, 2]);

    Ok(())
}

/// Start from a mid-chain node: CTE should only find descendants, not ancestors.
#[tokio::test]
async fn test_recursive_cte_mid_chain_start() -> Result<()> {
    let db = setup_linear_chain().await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 1}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    // Only 1 and 2 (not 0)
    assert_eq!(result.len(), 2);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2]);

    Ok(())
}

/// Start from a leaf node: no recursive results, only the anchor.
#[tokio::test]
async fn test_recursive_cte_leaf_start() -> Result<()> {
    let db = setup_linear_chain().await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 2}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    // Only 2 (leaf has no children)
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i32>("id")?, 2);

    Ok(())
}

/// CTE result used with COUNT aggregation.
#[tokio::test]
async fn test_recursive_cte_with_count() -> Result<()> {
    let db = setup_linear_chain().await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN count(n) AS cnt
    ",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let cnt: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(cnt, 3);

    Ok(())
}

/// Multiple edge types: uses MANAGES and REPORTS_TO in the same graph.
/// CTE follows only one edge type.
#[tokio::test]
async fn test_recursive_cte_specific_edge_type() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("MANAGES", &["Person"], &["Person"])
        .edge_type("MENTORS", &["Person"], &["Person"])
        .apply()
        .await?;

    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.execute("CREATE (:Person {name: 'Bob'})").await?;
    db.execute("CREATE (:Person {name: 'Carol'})").await?;
    db.execute("CREATE (:Person {name: 'Dave'})").await?;

    // MANAGES chain: Alice → Bob → Carol
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:MANAGES]->(b)",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:MANAGES]->(b)",
    )
    .await?;

    // MENTORS: Alice → Dave (different relationship)
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) CREATE (a)-[:MENTORS]->(b)",
    )
    .await?;

    // CTE follows only MANAGES
    let result = db
        .query(
            "
        WITH RECURSIVE team AS (
            MATCH (root:Person {name: 'Alice'}) RETURN root
            UNION
            MATCH (mgr:Person)-[:MANAGES]->(report:Person)
            WHERE mgr IN team
            RETURN report
        )
        MATCH (p:Person) WHERE p IN team
        RETURN p.name AS name ORDER BY name
    ",
        )
        .await?;

    assert_eq!(result.len(), 3);
    let names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap())
        .collect();
    // Alice, Bob, Carol — not Dave (MENTORS, not MANAGES)
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);

    Ok(())
}

/// String properties: verify CTE works with non-integer data types.
#[tokio::test]
async fn test_recursive_cte_string_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Category")
        .property("name", DataType::String)
        .property("level", DataType::Int32)
        .edge_type("SUBCATEGORY", &["Category"], &["Category"])
        .apply()
        .await?;

    db.execute("CREATE (:Category {name: 'Root', level: 0})")
        .await?;
    db.execute("CREATE (:Category {name: 'Electronics', level: 1})")
        .await?;
    db.execute("CREATE (:Category {name: 'Phones', level: 2})")
        .await?;

    db.execute("MATCH (a:Category {name: 'Root'}), (b:Category {name: 'Electronics'}) CREATE (a)-[:SUBCATEGORY]->(b)")
        .await?;
    db.execute("MATCH (a:Category {name: 'Electronics'}), (b:Category {name: 'Phones'}) CREATE (a)-[:SUBCATEGORY]->(b)")
        .await?;

    let result = db
        .query(
            "
        WITH RECURSIVE cats AS (
            MATCH (root:Category {name: 'Root'}) RETURN root
            UNION
            MATCH (parent:Category)-[:SUBCATEGORY]->(child:Category)
            WHERE parent IN cats
            RETURN child
        )
        MATCH (c:Category) WHERE c IN cats
        RETURN c.name AS name, c.level AS level ORDER BY level
    ",
        )
        .await?;

    assert_eq!(result.len(), 3);
    let names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap())
        .collect();
    assert_eq!(names, vec!["Root", "Electronics", "Phones"]);

    let levels: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("level").unwrap())
        .collect();
    assert_eq!(levels, vec![0, 1, 2]);

    Ok(())
}

/// Float properties: verify CTE works with Float64 data.
#[tokio::test]
async fn test_recursive_cte_float_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Station")
        .property("name", DataType::String)
        .property("distance", DataType::Float64)
        .edge_type("NEXT", &["Station"], &["Station"])
        .apply()
        .await?;

    db.execute("CREATE (:Station {name: 'A', distance: 0.0})")
        .await?;
    db.execute("CREATE (:Station {name: 'B', distance: 1.5})")
        .await?;
    db.execute("CREATE (:Station {name: 'C', distance: 3.7})")
        .await?;

    db.execute("MATCH (a:Station {name: 'A'}), (b:Station {name: 'B'}) CREATE (a)-[:NEXT]->(b)")
        .await?;
    db.execute("MATCH (a:Station {name: 'B'}), (b:Station {name: 'C'}) CREATE (a)-[:NEXT]->(b)")
        .await?;

    let result = db
        .query(
            "
        WITH RECURSIVE route AS (
            MATCH (start:Station {name: 'A'}) RETURN start
            UNION
            MATCH (prev:Station)-[:NEXT]->(next:Station)
            WHERE prev IN route
            RETURN next
        )
        MATCH (s:Station) WHERE s IN route
        RETURN s.name AS name, s.distance AS dist ORDER BY dist
    ",
        )
        .await?;

    assert_eq!(result.len(), 3);
    let names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap())
        .collect();
    assert_eq!(names, vec!["A", "B", "C"]);

    let dists: Vec<f64> = result
        .rows()
        .iter()
        .map(|r| r.get::<f64>("dist").unwrap())
        .collect();
    assert!((dists[0] - 0.0).abs() < f64::EPSILON);
    assert!((dists[1] - 1.5).abs() < f64::EPSILON);
    assert!((dists[2] - 3.7).abs() < f64::EPSILON);

    Ok(())
}

/// CTE result used with LIMIT.
#[tokio::test]
async fn test_recursive_cte_with_limit() -> Result<()> {
    let db = setup_linear_chain().await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id LIMIT 2
    ",
        )
        .await?;

    assert_eq!(result.len(), 2);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![0, 1]);

    Ok(())
}

/// CTE result used with SKIP.
#[tokio::test]
async fn test_recursive_cte_with_skip() -> Result<()> {
    let db = setup_linear_chain().await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id SKIP 1
    ",
        )
        .await?;

    assert_eq!(result.len(), 2);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2]);

    Ok(())
}

// ===========================================================================
// Empty / No-Result Tests
// ===========================================================================

/// Anchor matches no nodes: CTE result should be empty.
#[tokio::test]
async fn test_recursive_cte_anchor_no_match() -> Result<()> {
    let db = setup_linear_chain().await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 999}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    assert_eq!(result.len(), 0);

    Ok(())
}

/// Anchor matches nodes but they have no outgoing edges of the specified type.
#[tokio::test]
async fn test_recursive_cte_no_recursive_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    // Just isolated nodes, no edges
    db.execute("CREATE (:Item {id: 0})").await?;
    db.execute("CREATE (:Item {id: 1})").await?;

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    // Only the anchor node (no edges to follow)
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i32>("id")?, 0);

    Ok(())
}

// ===========================================================================
// Complex Topology Tests
// ===========================================================================

/// Reverse traversal: follow edges backwards using `<-[:CHILD]-`.
#[tokio::test]
async fn test_recursive_cte_reverse_traversal() -> Result<()> {
    let db = setup_linear_chain().await?;

    // Start at leaf (2), follow CHILD edges backwards to find ancestors
    let result = db
        .query(
            "
        WITH RECURSIVE ancestors AS (
            MATCH (leaf:Item {id: 2}) RETURN leaf
            UNION
            MATCH (child:Item)<-[:CHILD]-(parent:Item)
            WHERE child IN ancestors
            RETURN parent
        )
        MATCH (n:Item) WHERE n IN ancestors
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    assert_eq!(result.len(), 3);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![0, 1, 2]);

    Ok(())
}

/// Complex graph with multiple edge types and labels.
/// Organization hierarchy with different departments.
#[tokio::test]
async fn test_recursive_cte_multi_label_graph() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Dept")
        .property("name", DataType::String)
        .edge_type("HAS_SUB", &["Dept"], &["Dept"])
        .apply()
        .await?;

    db.execute("CREATE (:Dept {name: 'Company'})").await?;
    db.execute("CREATE (:Dept {name: 'Engineering'})").await?;
    db.execute("CREATE (:Dept {name: 'Backend'})").await?;
    db.execute("CREATE (:Dept {name: 'Frontend'})").await?;
    db.execute("CREATE (:Dept {name: 'Sales'})").await?;

    db.execute("MATCH (a:Dept {name: 'Company'}), (b:Dept {name: 'Engineering'}) CREATE (a)-[:HAS_SUB]->(b)")
        .await?;
    db.execute(
        "MATCH (a:Dept {name: 'Company'}), (b:Dept {name: 'Sales'}) CREATE (a)-[:HAS_SUB]->(b)",
    )
    .await?;
    db.execute("MATCH (a:Dept {name: 'Engineering'}), (b:Dept {name: 'Backend'}) CREATE (a)-[:HAS_SUB]->(b)")
        .await?;
    db.execute("MATCH (a:Dept {name: 'Engineering'}), (b:Dept {name: 'Frontend'}) CREATE (a)-[:HAS_SUB]->(b)")
        .await?;

    // Find all sub-departments under Engineering
    let result = db
        .query(
            "
        WITH RECURSIVE subdepts AS (
            MATCH (root:Dept {name: 'Engineering'}) RETURN root
            UNION
            MATCH (parent:Dept)-[:HAS_SUB]->(child:Dept)
            WHERE parent IN subdepts
            RETURN child
        )
        MATCH (d:Dept) WHERE d IN subdepts
        RETURN d.name AS name ORDER BY name
    ",
        )
        .await?;

    assert_eq!(result.len(), 3);
    let names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap())
        .collect();
    // Backend, Engineering, Frontend (alphabetical) — not Company or Sales
    assert_eq!(names, vec!["Backend", "Engineering", "Frontend"]);

    Ok(())
}

/// Large fan-out: one node with many children.
#[tokio::test]
async fn test_recursive_cte_wide_fan_out() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    let fan_out = 20;
    db.execute("CREATE (:Item {id: 0})").await?;
    for i in 1..=fan_out {
        db.execute(&format!("CREATE (:Item {{id: {}}})", i)).await?;
        db.execute(&format!(
            "MATCH (a:Item {{id: 0}}), (b:Item {{id: {}}}) CREATE (a)-[:CHILD]->(b)",
            i
        ))
        .await?;
    }

    let result = db
        .query(
            "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ",
        )
        .await?;

    assert_eq!(result.len(), fan_out + 1);
    let ids: Vec<i32> = result
        .rows()
        .iter()
        .map(|r| r.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(ids, (0..=fan_out as i32).collect::<Vec<i32>>());

    Ok(())
}
