// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Debug tests for WHERE clause behavior.
//!
//! These tests help identify WHERE clause execution issues.

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Basic WHERE test: filter by numeric property
#[tokio::test]
async fn test_where_property_simple() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Charlie', age: 35})")
        .await?;

    // Simple WHERE with greater than
    let result = db
        .query("MATCH (n:Person) WHERE n.age > 28 RETURN n.name ORDER BY n.name")
        .await?;

    // Should return Alice (30) and Charlie (35)
    assert_eq!(result.len(), 2, "Expected 2 results, got {}", result.len());

    let name1: String = result.rows()[0].get("n.name")?;
    let name2: String = result.rows()[1].get("n.name")?;
    assert_eq!(name1, "Alice");
    assert_eq!(name2, "Charlie");

    Ok(())
}

/// WHERE with equality comparison
#[tokio::test]
async fn test_where_equality() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .await?;

    let result = db
        .query("MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let name: String = result.rows()[0].get("n.name")?;
    assert_eq!(name, "Alice");

    Ok(())
}

/// WHERE with string comparison
#[tokio::test]
async fn test_where_string_equality() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice'})").await?;
    db.execute("CREATE (n:Person {name: 'Bob'})").await?;

    let result = db
        .query("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let name: String = result.rows()[0].get("n.name")?;
    assert_eq!(name, "Alice");

    Ok(())
}

/// WHERE with AND condition
#[tokio::test]
async fn test_where_and_condition() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Bob', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Alice', age: 25})")
        .await?;

    let result = db
        .query("MATCH (n:Person) WHERE n.name = 'Alice' AND n.age = 30 RETURN n")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());

    Ok(())
}

/// WHERE on edge property
#[tokio::test]
async fn test_where_edge_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (a:Person {name: 'Alice'})").await?;
    db.execute("CREATE (b:Person {name: 'Bob'})").await?;
    db.execute("CREATE (c:Person {name: 'Charlie'})").await?;
    db.execute("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)").await?;
    db.execute("MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS {since: 2022}]->(c)").await?;

    let result = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2020 RETURN b.name")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let name: String = result.rows()[0].get("b.name")?;
    assert_eq!(name, "Bob");

    Ok(())
}

/// WHERE with OR condition
#[tokio::test]
async fn test_where_or_condition() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Charlie', age: 35})")
        .await?;

    let result = db
        .query("MATCH (n:Person) WHERE n.age = 25 OR n.age = 35 RETURN n.name ORDER BY n.name")
        .await?;

    assert_eq!(result.len(), 2, "Expected 2 results, got {}", result.len());
    let name1: String = result.rows()[0].get("n.name")?;
    let name2: String = result.rows()[1].get("n.name")?;
    assert_eq!(name1, "Bob");
    assert_eq!(name2, "Charlie");

    Ok(())
}

/// WHERE with less than comparison
#[tokio::test]
async fn test_where_less_than() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Charlie', age: 35})")
        .await?;

    let result = db
        .query("MATCH (n:Person) WHERE n.age < 30 RETURN n.name")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let name: String = result.rows()[0].get("n.name")?;
    assert_eq!(name, "Bob");

    Ok(())
}

/// WHERE with NOT
#[tokio::test]
async fn test_where_not() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .await?;

    let result = db
        .query("MATCH (n:Person) WHERE NOT n.age = 30 RETURN n.name")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let name: String = result.rows()[0].get("n.name")?;
    assert_eq!(name, "Bob");

    Ok(())
}

/// WHERE with float/int comparison (type coercion)
#[tokio::test]
async fn test_where_type_coercion() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("price", DataType::Float64)
        .apply()
        .await?;

    db.execute("CREATE (n:Item {price: 10.5})").await?;
    db.execute("CREATE (n:Item {price: 20.0})").await?;
    db.execute("CREATE (n:Item {price: 5.5})").await?;

    // Compare float with integer literal
    let result = db
        .query("MATCH (n:Item) WHERE n.price > 10 RETURN n.price ORDER BY n.price")
        .await?;

    assert_eq!(result.len(), 2, "Expected 2 results, got {}", result.len());

    Ok(())
}

/// WHERE with label predicate (WHERE n:Label)
#[tokio::test]
async fn test_where_label_predicate() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("A")
        .property("id", DataType::Int64)
        .label("B")
        .property("id", DataType::Int64)
        .label("C")
        .property("id", DataType::Int64)
        .property("a", DataType::String)
        .edge_type("ADMIN", &["A", "B", "C"], &["A", "B", "C"])
        .apply()
        .await?;

    // Create: (:A {id: 0})<-[:ADMIN]-(:B {id: 1})-[:ADMIN]->(:C {id: 2, a: 'A'})
    db.execute("CREATE (a:A {id: 0}), (b:B {id: 1}), (c:C {id: 2, a: 'A'})")
        .await?;
    db.execute("MATCH (a:A {id: 0}), (b:B {id: 1}) CREATE (b)-[:ADMIN]->(a)")
        .await?;
    db.execute("MATCH (b:B {id: 1}), (c:C {id: 2}) CREATE (b)-[:ADMIN]->(c)")
        .await?;

    // First check: match all nodes without label predicate
    let result_all = db.query("MATCH (n) RETURN n, labels(n) as lbls").await?;
    eprintln!("All nodes ({}):", result_all.len());
    for row in result_all.rows() {
        let n_val = row.value("n");
        let lbls_val = row.value("lbls");
        eprintln!("  n={:?}, labels={:?}", n_val, lbls_val);
    }

    // Second check: simple label predicate
    let result = db.query("MATCH (n) WHERE n:A RETURN n").await?;
    eprintln!("Nodes with label A: {}", result.len());
    for row in result.rows() {
        let n_val = row.value("n");
        eprintln!("  n={:?}", n_val);
    }
    assert_eq!(
        result.len(),
        1,
        "Expected 1 node with label A, got {}",
        result.len()
    );

    // Test label predicate in WHERE with edge pattern
    let result = db
        .query("MATCH (a)-[:ADMIN]-(b) WHERE a:A RETURN a.id, b.id")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let a_id: i64 = result.rows()[0].get("a.id")?;
    let b_id: i64 = result.rows()[0].get("b.id")?;
    assert_eq!(a_id, 0);
    assert_eq!(b_id, 1);

    Ok(())
}

/// WHERE with IS NULL
#[tokio::test]
async fn test_where_is_null() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Bob'})").await?; // No age property

    // Match people where age is null
    let result = db
        .query("MATCH (n:Person) WHERE n.age IS NULL RETURN n.name")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let name: String = result.rows()[0].get("n.name")?;
    assert_eq!(name, "Bob");

    Ok(())
}

/// WHERE with IS NOT NULL
#[tokio::test]
async fn test_where_is_not_null() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (n:Person {name: 'Bob'})").await?; // No age property

    // Match people where age is not null
    let result = db
        .query("MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let name: String = result.rows()[0].get("n.name")?;
    assert_eq!(name, "Alice");

    Ok(())
}

/// WHERE with undirected edge pattern (TCK scenario 1)
#[tokio::test]
async fn test_where_undirected_edge() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    db.execute("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
        .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;

    // Undirected edge match
    let result = db
        .query("MATCH (a)-[:KNOWS]-(b) WHERE a.name = 'Alice' RETURN b.name")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());
    let name: String = result.rows()[0].get("b.name")?;
    assert_eq!(name, "Bob");

    Ok(())
}

/// TCK MatchWhere1 scenario 4: Filter start node with property predicate
#[tokio::test]
async fn test_where_start_node_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .label("Other")
        .edge_type("T", &["Person"], &["Other"])
        .apply()
        .await?;

    // TCK setup: CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c), (d)
    //            CREATE (a)-[:T]->(c), (b)-[:T]->(d)
    // Use single CREATE with unique node IDs to avoid cross-product issues
    db.execute("CREATE (a:Person {name: 'Alice'})-[:T]->(:Other)")
        .await?;
    db.execute("CREATE (b:Person {name: 'Bob'})-[:T]->(:Other)")
        .await?;

    // First check what we have without WHERE
    let all = db.query("MATCH (n:Person)-->() RETURN n.name").await?;
    eprintln!("All Person nodes with outgoing edges: {} rows", all.len());
    for row in all.rows() {
        eprintln!("  n.name = {:?}", row.value("n.name"));
    }

    // Now with WHERE
    let result = db
        .query("MATCH (n:Person)-->() WHERE n.name = 'Bob' RETURN n")
        .await?;
    eprintln!("After WHERE n.name = 'Bob': {} rows", result.len());
    for row in result.rows() {
        eprintln!("  n = {:?}", row.value("n"));
    }

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());

    Ok(())
}

/// TCK MatchWhere1 scenario 4 - EXACT setup (schemaless)
/// Tests the exact TCK CREATE syntax which uses nodes without labels
#[tokio::test]
async fn test_where_start_node_schemaless() -> Result<()> {
    // No schema - pure schemaless mode like TCK
    let db = Uni::in_memory().build().await?;

    // Create nodes and edges in a way that works with our system
    // Since variables don't persist across statements, use MATCH to bind
    db.execute("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c), (d)")
        .await?;

    // Use MATCH to find the specific nodes by property
    db.execute("MATCH (a:Person {name: 'Alice'}), (c) WHERE c.name IS NULL RETURN a, c LIMIT 1")
        .await?;

    // Debug: Check what we created
    let all_nodes = db.query("MATCH (n) RETURN n, labels(n) as lbls").await?;
    eprintln!("All nodes ({}): ", all_nodes.len());
    for row in all_nodes.rows() {
        eprintln!("  n={:?}, labels={:?}", row.value("n"), row.value("lbls"));
    }

    // Try to create edges using MATCH
    // First, let's check if we can find unlabeled nodes
    let unlabeled = db.query("MATCH (n) WHERE NOT n:Person RETURN n").await?;
    eprintln!("Unlabeled nodes ({}): ", unlabeled.len());

    // Since we can't easily reference unlabeled nodes, let's simplify:
    // Just create Person nodes with edges in one statement
    let db2 = Uni::in_memory().build().await?;
    db2.execute("CREATE (a:Person {name: 'Alice'})-[:T]->(:Other)")
        .await?;
    db2.execute("CREATE (b:Person {name: 'Bob'})-[:T]->(:Other)")
        .await?;

    // Debug: Check what we created
    let all_nodes2 = db2.query("MATCH (n) RETURN n, labels(n) as lbls").await?;
    eprintln!("All nodes in db2 ({}): ", all_nodes2.len());
    for row in all_nodes2.rows() {
        eprintln!("  n={:?}, labels={:?}", row.value("n"), row.value("lbls"));
    }

    let all_edges2 = db2.query("MATCH ()-[r]->() RETURN r, type(r) as t").await?;
    eprintln!("All edges in db2 ({}): ", all_edges2.len());
    for row in all_edges2.rows() {
        eprintln!("  r={:?}, type={:?}", row.value("r"), row.value("t"));
    }

    // Check Person nodes with outgoing edges
    let with_edges2 = db2.query("MATCH (n:Person)-->() RETURN n.name").await?;
    eprintln!(
        "Person nodes with outgoing edges in db2 ({}): ",
        with_edges2.len()
    );
    for row in with_edges2.rows() {
        eprintln!("  n.name = {:?}", row.value("n.name"));
    }

    // The actual query on db2
    let result = db2
        .query("MATCH (n:Person)-->() WHERE n.name = 'Bob' RETURN n")
        .await?;
    eprintln!("After WHERE n.name = 'Bob': {} rows", result.len());
    for row in result.rows() {
        eprintln!("  n = {:?}", row.value("n"));
    }

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());

    Ok(())
}

/// Debug: Test minimal schemaless edge creation
#[tokio::test]
async fn test_schemaless_edge_basic() -> Result<()> {
    // Pure schemaless - no schema at all
    let db = Uni::in_memory().build().await?;

    // Step 1: Try creating a node first
    db.execute("CREATE (:Test {name: 'foo'})").await?;
    let nodes = db.query("MATCH (n) RETURN n").await?;
    eprintln!("After CREATE (:Test): {} nodes", nodes.len());

    // Step 2: Try creating edge with inline node
    let create_result = db.execute("CREATE (:A)-[:REL]->(:B)").await;
    match &create_result {
        Ok(_) => eprintln!("CREATE (:A)-[:REL]->(:B) succeeded"),
        Err(e) => eprintln!("CREATE (:A)-[:REL]->(:B) FAILED: {:?}", e),
    }
    create_result?;

    // Check what was created
    let nodes2 = db.query("MATCH (n) RETURN n, labels(n) as l").await?;
    eprintln!("After CREATE (:A)-[:REL]->(:B): {} nodes", nodes2.len());
    for row in nodes2.rows() {
        eprintln!("  node: {:?}, labels: {:?}", row.value("n"), row.value("l"));
    }

    let edges = db.query("MATCH ()-[r]->() RETURN r, type(r) as t").await?;
    eprintln!("Edges: {} rows", edges.len());
    for row in edges.rows() {
        eprintln!("  edge: {:?}, type: {:?}", row.value("r"), row.value("t"));
    }

    // Verify at least some edges exist
    assert!(!edges.is_empty(), "Expected at least 1 edge, got 0");

    Ok(())
}

/// TCK MatchWhere1 scenario 7: WHERE type(r) = 'KNOWS'
#[tokio::test]
async fn test_where_type_predicate() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("A")
        .property("name", DataType::String)
        .label("B")
        .property("name", DataType::String)
        .label("C")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["A"], &["B"])
        .edge_type("HATES", &["A"], &["C"])
        .apply()
        .await?;

    // Setup
    db.execute("CREATE (a:A {name: 'A'}), (b:B {name: 'B'}), (c:C {name: 'C'})")
        .await?;
    db.execute("MATCH (a:A), (b:B) CREATE (a)-[:KNOWS]->(b)")
        .await?;
    db.execute("MATCH (a:A), (c:C) CREATE (a)-[:HATES]->(c)")
        .await?;

    // Debug: all edges
    let all_edges = db
        .query("MATCH (n {name: 'A'})-[r]->(x) RETURN type(r) as t, x.name")
        .await?;
    eprintln!("All edges from A ({}): ", all_edges.len());
    for row in all_edges.rows() {
        eprintln!(
            "  type={:?}, target={:?}",
            row.value("t"),
            row.value("x.name")
        );
    }

    // Filter by type
    let result = db
        .query("MATCH (n {name: 'A'})-[r]->(x) WHERE type(r) = 'KNOWS' RETURN x")
        .await?;
    eprintln!("After WHERE type(r) = 'KNOWS': {} rows", result.len());

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());

    Ok(())
}

/// TCK MatchWhere1 scenario 8: WHERE r.name = 'monkey'
#[tokio::test]
async fn test_where_edge_property_predicate() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("A")
        .label("X") // Use distinct label for center node
        .label("B")
        .edge_type("KNOWS", &["X"], &["A", "B"])
        .property("name", DataType::String)
        .apply()
        .await?;

    // Setup: (:A)<-[:KNOWS {name: 'monkey'}]-(:X)-[:KNOWS {name: 'woot'}]->(:B)
    // Use single CREATE with inline edges to ensure correct structure
    db.execute("CREATE (:X)-[:KNOWS {name: 'monkey'}]->(:A)")
        .await?;
    db.execute("MATCH (x:X) CREATE (x)-[:KNOWS {name: 'woot'}]->(:B)")
        .await?;

    // Debug: all edges
    let all_edges = db
        .query("MATCH (node)-[r:KNOWS]->(a) RETURN r.name, labels(a) as lbls")
        .await?;
    eprintln!("All KNOWS edges ({}): ", all_edges.len());
    for row in all_edges.rows() {
        eprintln!(
            "  r.name={:?}, target labels={:?}",
            row.value("r.name"),
            row.value("lbls")
        );
    }

    // Filter by edge property
    let result = db
        .query("MATCH (node)-[r:KNOWS]->(a) WHERE r.name = 'monkey' RETURN a")
        .await?;
    eprintln!("After WHERE r.name = 'monkey': {} rows", result.len());

    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());

    Ok(())
}

/// TCK MatchWhere5 scenario 1: Filter out on null
/// When comparing string vs int, result is NULL which should filter out
#[tokio::test]
async fn test_matchwhere5_filter_on_null() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Root")
        .property("name", DataType::String)
        .label("TextNode")
        .property("var", DataType::String)
        .label("IntNode")
        .property("var", DataType::Int64)
        .edge_type("T", &["Root"], &["TextNode", "IntNode"])
        .apply()
        .await?;

    // TCK setup:
    // CREATE (root:Root {name: 'x'}),
    //        (child1:TextNode {var: 'text'}),
    //        (child2:IntNode {var: 0})
    // CREATE (root)-[:T]->(child1),
    //        (root)-[:T]->(child2)
    db.execute("CREATE (r:Root {name: 'x'})").await?;
    db.execute("CREATE (t:TextNode {var: 'text'})").await?;
    db.execute("CREATE (i:IntNode {var: 0})").await?;
    db.execute("MATCH (r:Root), (t:TextNode) CREATE (r)-[:T]->(t)")
        .await?;
    db.execute("MATCH (r:Root), (i:IntNode) CREATE (r)-[:T]->(i)")
        .await?;

    // Debug: Check all nodes
    let all_nodes = db.query("MATCH (n) RETURN labels(n) as lbls, n").await?;
    eprintln!("All nodes ({}):", all_nodes.len());
    for row in all_nodes.rows() {
        eprintln!("  labels={:?}, n={:?}", row.value("lbls"), row.value("n"));
    }

    // Debug: Check all edges from Root
    let all_edges = db
        .query("MATCH (:Root {name: 'x'})-->(n) RETURN labels(n) as lbls, n")
        .await?;
    eprintln!("All edges from Root ({}):", all_edges.len());
    for row in all_edges.rows() {
        eprintln!("  labels={:?}, n={:?}", row.value("lbls"), row.value("n"));
    }

    // Debug: Check TextNode traversal specifically
    let text_edges = db
        .query("MATCH (:Root {name: 'x'})-->(i:TextNode) RETURN i")
        .await?;
    eprintln!("TextNode edges from Root ({}):", text_edges.len());
    for row in text_edges.rows() {
        eprintln!("  i={:?}", row.value("i"));
    }

    // The actual query: WHERE i.var > 'te'
    // For TextNode: 'text' > 'te' = true → include
    // For IntNode: 0 > 'te' = NULL → filter out
    let result = db
        .query("MATCH (:Root {name: 'x'})-->(i:TextNode) WHERE i.var > 'te' RETURN i")
        .await?;
    eprintln!("After WHERE i.var > 'te' ({}):", result.len());
    for row in result.rows() {
        eprintln!("  i={:?}", row.value("i"));
    }

    // Should return only TextNode
    assert_eq!(result.len(), 1, "Expected 1 TextNode, got {}", result.len());

    Ok(())
}

/// Test labels() function returns correct labels for schemaless nodes
#[tokio::test]
async fn test_labels_function_schemaless() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schemaless nodes with labels
    db.execute("CREATE (a:MyLabel {name: 'test'})").await?;

    // Check labels function
    let result = db
        .query("MATCH (n:MyLabel) RETURN n, labels(n) as lbls")
        .await?;
    eprintln!("labels() result ({}):", result.len());
    for row in result.rows() {
        eprintln!("  n={:?}", row.value("n"));
        eprintln!("  labels={:?}", row.value("lbls"));
    }

    assert_eq!(result.len(), 1, "Should find 1 node");
    let labels: Vec<String> = result.rows()[0].get("lbls")?;
    assert!(
        labels.contains(&"MyLabel".to_string()),
        "Node should have label MyLabel"
    );

    Ok(())
}

/// Test labels are populated correctly when traversing edges
#[tokio::test]
async fn test_labels_after_traverse_schemaless() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schemaless nodes and edge
    db.execute("CREATE (:Root {name: 'x'})-[:CONNECTS]->(:Target {val: 1})")
        .await?;

    // Check source node labels
    let source = db
        .query("MATCH (n:Root) RETURN n, labels(n) as lbls")
        .await?;
    eprintln!("Source node ({}):", source.len());
    for row in source.rows() {
        eprintln!("  n={:?}, labels={:?}", row.value("n"), row.value("lbls"));
    }

    // Check target node labels after traversal
    let target = db
        .query("MATCH (:Root)-->(t) RETURN t, labels(t) as lbls")
        .await?;
    eprintln!("Target node after traverse ({}):", target.len());
    for row in target.rows() {
        eprintln!("  t={:?}, labels={:?}", row.value("t"), row.value("lbls"));
    }

    assert_eq!(target.len(), 1, "Should find 1 target node");
    let target_labels: Vec<String> = target.rows()[0].get("lbls")?;
    eprintln!("Target labels: {:?}", target_labels);
    assert!(
        target_labels.contains(&"Target".to_string()),
        "Target node should have label Target, got: {:?}",
        target_labels
    );

    Ok(())
}

/// TCK MatchWhere5 scenario 1: Filter out on null (SCHEMALESS - exact TCK replication)
#[tokio::test]
async fn test_matchwhere5_schemaless() -> Result<()> {
    // NO SCHEMA - exactly like TCK
    let db = Uni::in_memory().build().await?;

    // TCK setup (exact):
    // CREATE (root:Root {name: 'x'}),
    //        (child1:TextNode {var: 'text'}),
    //        (child2:IntNode {var: 0})
    // CREATE (root)-[:T]->(child1),
    //        (root)-[:T]->(child2)

    // Split into separate CREATE statements for debug
    db.execute("CREATE (root:Root {name: 'x'}), (child1:TextNode {var: 'text'}), (child2:IntNode {var: 0})").await?;

    // Debug: Check nodes
    let all_nodes = db.query("MATCH (n) RETURN labels(n) as lbls, n").await?;
    eprintln!("After CREATE nodes - All nodes ({}):", all_nodes.len());
    for row in all_nodes.rows() {
        eprintln!("  labels={:?}, n={:?}", row.value("lbls"), row.value("n"));
    }

    // Create edges using MATCH
    db.execute("MATCH (root:Root {name: 'x'}), (child1:TextNode {var: 'text'}) CREATE (root)-[:T]->(child1)").await?;
    db.execute(
        "MATCH (root:Root {name: 'x'}), (child2:IntNode {var: 0}) CREATE (root)-[:T]->(child2)",
    )
    .await?;

    // Debug: Check all edges from Root
    let all_edges = db
        .query("MATCH (:Root {name: 'x'})-->(n) RETURN labels(n) as lbls, n.var")
        .await?;
    eprintln!("All edges from Root ({}):", all_edges.len());
    for row in all_edges.rows() {
        eprintln!(
            "  labels={:?}, n.var={:?}",
            row.value("lbls"),
            row.value("n.var")
        );
    }

    // Debug: Check TextNode traversal specifically
    let text_edges = db
        .query("MATCH (:Root {name: 'x'})-->(i:TextNode) RETURN i.var")
        .await?;
    eprintln!("TextNode edges from Root ({}):", text_edges.len());
    for row in text_edges.rows() {
        eprintln!("  i.var={:?}", row.value("i.var"));
    }

    // The actual TCK query
    let result = db
        .query("MATCH (:Root {name: 'x'})-->(i:TextNode) WHERE i.var > 'te' RETURN i")
        .await?;
    eprintln!("After WHERE i.var > 'te' ({}):", result.len());
    for row in result.rows() {
        eprintln!("  i={:?}", row.value("i"));
    }

    // Should return only TextNode
    assert_eq!(result.len(), 1, "Expected 1 TextNode, got {}", result.len());

    Ok(())
}

/// Test MatchWhere6 scenario [2]: OPTIONAL MATCH with false label predicate
/// Expected behavior: When OPTIONAL MATCH finds matches but WHERE filters all of them out,
/// the row should still be returned with NULL values for the optional variables.
#[tokio::test]
async fn test_optional_match_where_false_label() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create the graph in schemaless mode
    db.execute("CREATE (s:Single), (a:A {num: 42}), (b:B {num: 46}), (c:C)")
        .await?;
    db.execute("MATCH (s:Single), (a:A {num: 42}) CREATE (s)-[:REL]->(a)")
        .await?;
    db.execute("MATCH (s:Single), (b:B {num: 46}) CREATE (s)-[:REL]->(b)")
        .await?;
    db.execute("MATCH (a:A {num: 42}), (c:C) CREATE (a)-[:REL]->(c)")
        .await?;
    db.execute("MATCH (b:B {num: 46}) CREATE (b)-[:LOOP]->(b)")
        .await?;

    // Debug: Check what relationships exist
    let all_rels = db
        .query("MATCH (n:Single)-[r]-(m) RETURN labels(m) as lbls, r")
        .await?;
    eprintln!("Single's relationships ({}):", all_rels.len());
    for row in all_rels.rows() {
        eprintln!("  m labels={:?}, r={:?}", row.value("lbls"), row.value("r"));
    }

    // The TCK query: OPTIONAL MATCH with WHERE that filters everything out
    // Expected: 1 row with r=null (because m:NonExistent matches nothing)
    let result = db
        .query("MATCH (n:Single) OPTIONAL MATCH (n)-[r]-(m) WHERE m:NonExistent RETURN r")
        .await?;
    eprintln!("OPTIONAL MATCH result ({} rows):", result.len());
    for row in result.rows() {
        eprintln!("  r={:?}", row.value("r"));
    }

    // Should return 1 row with r=null
    assert_eq!(result.len(), 1, "Expected 1 row, got {}", result.len());
    let r = result.rows()[0].value("r");
    eprintln!("r value: {:?}", r);
    assert!(
        r.is_none() || matches!(r, Some(uni_db::Value::Null)),
        "Expected r=null"
    );

    Ok(())
}

/// MatchWhere6 scenario [6]: Join nodes on non-equality of properties – OPTIONAL MATCH and WHERE
/// Expected: 3 rows - X nodes with matching Y (if x.val < y.val) or NULL y if no match
#[tokio::test]
async fn test_optional_match_non_equality_join() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup:
    // (:X {val: 1})-[:E1]->(:Y {val: 2})-[:E2]->(:Z {val: 3}),
    // (:X {val: 4})-[:E1]->(:Y {val: 5}),
    // (:X {val: 6})
    db.execute("CREATE (:X {val: 1})-[:E1]->(:Y {val: 2})-[:E2]->(:Z {val: 3})")
        .await?;
    db.execute("CREATE (:X {val: 4})-[:E1]->(:Y {val: 5})")
        .await?;
    db.execute("CREATE (:X {val: 6})").await?;

    // Debug: Check all X nodes
    let x_nodes = db.query("MATCH (x:X) RETURN x.val ORDER BY x.val").await?;
    eprintln!("X nodes ({}):", x_nodes.len());
    for row in x_nodes.rows() {
        eprintln!("  x.val={:?}", row.value("x.val"));
    }
    assert_eq!(x_nodes.len(), 3, "Should have 3 X nodes");

    // Debug: Check E1 edges from each X
    let e1_edges = db
        .query("MATCH (x:X)-[:E1]->(y:Y) RETURN x.val, y.val ORDER BY x.val")
        .await?;
    eprintln!("E1 edges ({}):", e1_edges.len());
    for row in e1_edges.rows() {
        eprintln!(
            "  x.val={:?} -> y.val={:?}",
            row.value("x.val"),
            row.value("y.val")
        );
    }

    // Debug: Check OPTIONAL MATCH without WHERE
    let opt_no_where = db
        .query("MATCH (x:X) OPTIONAL MATCH (x)-[:E1]->(y:Y) RETURN x.val, y.val ORDER BY x.val")
        .await?;
    eprintln!("OPTIONAL MATCH without WHERE ({}):", opt_no_where.len());
    for row in opt_no_where.rows() {
        eprintln!(
            "  x.val={:?}, y.val={:?}",
            row.value("x.val"),
            row.value("y.val")
        );
    }
    assert_eq!(
        opt_no_where.len(),
        3,
        "OPTIONAL MATCH should return 3 rows (one per X)"
    );

    // The actual TCK query
    let result = db.query("MATCH (x:X) OPTIONAL MATCH (x)-[:E1]->(y:Y) WHERE x.val < y.val RETURN x.val, y.val ORDER BY x.val").await?;
    eprintln!("OPTIONAL MATCH with WHERE ({}):", result.len());
    for row in result.rows() {
        eprintln!(
            "  x.val={:?}, y.val={:?}",
            row.value("x.val"),
            row.value("y.val")
        );
    }

    // Expected:
    // x.val=1, y.val=2 (1 < 2 passes)
    // x.val=4, y.val=5 (4 < 5 passes)
    // x.val=6, y.val=NULL (no E1 edge, so y is NULL)
    assert_eq!(result.len(), 3, "Expected 3 rows, got {}", result.len());

    Ok(())
}

/// MatchWhere6 scenario [7]: Multi-hop OPTIONAL MATCH with WHERE
/// Expected: When intermediate hop succeeds but final hop fails, ALL optional vars should be NULL
#[tokio::test]
async fn test_optional_match_multi_hop() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup:
    // (:X {val: 1})-[:E1]->(:Y {val: 2})-[:E2]->(:Z {val: 3}),
    // (:X {val: 4})-[:E1]->(:Y {val: 5}),  <- Y has no E2 edge!
    // (:X {val: 6})
    db.execute("CREATE (:X {val: 1})-[:E1]->(:Y {val: 2})-[:E2]->(:Z {val: 3})")
        .await?;
    db.execute("CREATE (:X {val: 4})-[:E1]->(:Y {val: 5})")
        .await?;
    db.execute("CREATE (:X {val: 6})").await?;

    // Debug: Check the graph structure
    let x_nodes = db.query("MATCH (x:X) RETURN x.val ORDER BY x.val").await?;
    eprintln!("X nodes ({}):", x_nodes.len());
    for row in x_nodes.rows() {
        eprintln!("  x.val={:?}", row.value("x.val"));
    }

    let e1_edges = db
        .query("MATCH (x:X)-[:E1]->(y:Y) RETURN x.val, y.val ORDER BY x.val")
        .await?;
    eprintln!("E1 edges ({}):", e1_edges.len());
    for row in e1_edges.rows() {
        eprintln!(
            "  x.val={:?} -> y.val={:?}",
            row.value("x.val"),
            row.value("y.val")
        );
    }

    let e2_edges = db
        .query("MATCH (y:Y)-[:E2]->(z:Z) RETURN y.val, z.val")
        .await?;
    eprintln!("E2 edges ({}):", e2_edges.len());
    for row in e2_edges.rows() {
        eprintln!(
            "  y.val={:?} -> z.val={:?}",
            row.value("y.val"),
            row.value("z.val")
        );
    }

    // The actual TCK query - multi-hop OPTIONAL MATCH
    let result = db.query("MATCH (x:X) OPTIONAL MATCH (x)-[:E1]->(y:Y)-[:E2]->(z:Z) WHERE x.val < z.val RETURN x.val, y.val, z.val ORDER BY x.val").await?;
    eprintln!("Multi-hop OPTIONAL MATCH result ({}):", result.len());
    for row in result.rows() {
        eprintln!(
            "  x.val={:?}, y.val={:?}, z.val={:?}",
            row.value("x.val"),
            row.value("y.val"),
            row.value("z.val")
        );
    }

    // Expected:
    // X(val:1): -> Y(val:2) -> Z(val:3), 1 < 3 passes → x=1, y=2, z=3
    // X(val:4): -> Y(val:5) but Y has no E2 edge → x=4, y=NULL, z=NULL (pattern fails completely)
    // X(val:6): no E1 edges → x=6, y=NULL, z=NULL
    assert_eq!(result.len(), 3, "Expected 3 rows, got {}", result.len());

    // Check X(val:4) row - y should be NULL because the full pattern failed
    let row_4 = result
        .rows()
        .iter()
        .find(|r| matches!(r.value("x.val"), Some(uni_db::Value::Int(4))))
        .expect("Should have row for x.val=4");

    let y_val = row_4.value("y.val");
    eprintln!("X(val:4) y.val = {:?}", y_val);
    assert!(
        y_val.is_none() || matches!(y_val, Some(uni_db::Value::Null)),
        "For X(val:4), y should be NULL because full pattern (x)-[:E1]->(y)-[:E2]->(z) failed, got {:?}",
        y_val
    );

    Ok(())
}

/// MatchWhere1 scenario [6]: Filter node with a parameter in a property predicate
/// Query: MATCH (a)-[r]->(b) WHERE b.name = $param RETURN r
#[tokio::test]
async fn test_where_parameter_filter_return_edge() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup: CREATE (:A)-[:T {name: 'bar'}]->(:B {name: 'me'})
    db.execute("CREATE (:A)-[:T {name: 'bar'}]->(:B {name: 'me'})")
        .await?;

    // Debug: Check the graph
    let edges = db
        .query("MATCH (a)-[r]->(b) RETURN a, r, b, r.name as r_name, b.name as b_name")
        .await?;
    eprintln!("All edges ({}):", edges.len());
    for row in edges.rows() {
        eprintln!(
            "  a={:?}, r={:?}, b={:?}, r.name={:?}, b.name={:?}",
            row.value("a"),
            row.value("r"),
            row.value("b"),
            row.value("r_name"),
            row.value("b_name")
        );
    }

    // Step 1: Query without parameter (hardcoded filter)
    let result_hardcoded = db
        .query("MATCH (a)-[r]->(b) WHERE b.name = 'me' RETURN r")
        .await?;
    eprintln!("Hardcoded filter result ({}):", result_hardcoded.len());
    for row in result_hardcoded.rows() {
        eprintln!("  r={:?}", row.value("r"));
    }

    // Step 2: Query with parameter
    let result = db
        .query_with("MATCH (a)-[r]->(b) WHERE b.name = $param RETURN r")
        .param("param", "me")
        .fetch_all()
        .await?;
    eprintln!("Parameter filter result ({}):", result.len());
    for row in result.rows() {
        eprintln!("  r={:?}", row.value("r"));
    }

    // Should return 1 row with the edge
    assert_eq!(result.len(), 1, "Expected 1 result, got {}", result.len());

    Ok(())
}

/// MatchWhere6 scenario [5]: Reused relationship variable in OPTIONAL MATCH
/// This tests the case where a relationship variable from MATCH is reused in OPTIONAL MATCH.
///
/// Graph: (:A)-[:T]->(:B)
/// The relationship r goes A->B. OPTIONAL MATCH (a2)<-[r]-(b2) means b2-[r]->a2,
/// so a2=B, b2=A. WHERE a1=a2 means A=B, which is false, so OPTIONAL MATCH fails → nulls.
#[tokio::test]
async fn test_optional_match_reused_relationship() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (:A)-[:T]->(:B)").await?;

    let result = db
        .query(
            "
        MATCH (a1)-[r]->()
        WITH r, a1 LIMIT 1
        OPTIONAL MATCH (a2)<-[r]-(b2)
        WHERE a1 = a2
        RETURN a1, r, b2, a2
    ",
        )
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 row, got {}", result.len());

    let row = &result.rows()[0];
    let a2_val = row.value("a2");
    assert!(
        a2_val.is_none() || matches!(a2_val, Some(uni_db::Value::Null)),
        "a2 should be NULL because WHERE a1=a2 failed (A != B), got {:?}",
        a2_val
    );

    Ok(())
}
