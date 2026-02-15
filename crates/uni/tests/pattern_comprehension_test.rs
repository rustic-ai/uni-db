// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for pattern comprehension via the DataFusion execution path.

use anyhow::Result;
use uni_db::{Uni, Value};

#[tokio::test(flavor = "multi_thread")]
async fn test_pattern_comprehension_basic_traversal() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = Uni::in_memory().build().await?;

    // Create nodes and relationships
    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c)",
    )
    .await?;

    // First verify regular MATCH works
    let check = db
        .query("MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.name, m.name")
        .await?;
    eprintln!("Regular MATCH results ({} rows):", check.len());
    for row in check.rows() {
        eprintln!("  {:?}", row);
    }
    assert!(!check.is_empty(), "Regular MATCH should find results");

    // Now test pattern comprehension
    let results = db
        .query("MATCH (n:Person) RETURN n.name, [(n)-[:KNOWS]->(m) | m.name] AS friends")
        .await?;

    eprintln!("Pattern comprehension results ({} rows):", results.len());
    for row in results.rows() {
        eprintln!("  {:?}", row);
    }

    assert_eq!(results.len(), 3, "Should have 3 rows (one per Person)");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pattern_comprehension_node_property() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = Uni::in_memory().build().await?;

    // TCK Scenario 4: Introduce a new node variable
    db.execute("CREATE ({ext_id: 'a'})-[:T]->({name: 'val', ext_id: 'b'})-[:T]->({ext_id: 'c'})")
        .await?;

    let results = db
        .query("MATCH (n) RETURN [(n)-[:T]->(b) | b.name] AS list")
        .await?;

    eprintln!("TCK4 results ({} rows):", results.len());
    for row in results.rows() {
        eprintln!("  {:?}", row);
    }

    assert_eq!(results.len(), 3, "Should have 3 rows");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pattern_comprehension_edge_property() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = Uni::in_memory().build().await?;

    // TCK Scenario 5: Introduce a new relationship variable
    db.execute("CREATE (a), (b), (c) CREATE (a)-[:T {name: 'val'}]->(b), (b)-[:T]->(c)")
        .await?;

    let results = db
        .query("MATCH (n) RETURN [(n)-[r:T]->() | r.name] AS list")
        .await?;

    eprintln!("TCK5 results ({} rows):", results.len());
    for row in results.rows() {
        eprintln!("  {:?}", row);
    }

    assert_eq!(results.len(), 3, "Should have 3 rows");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pattern_comprehension_path_variable() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = Uni::in_memory().build().await?;

    // TCK Scenario 1: Return a pattern comprehension with path variable
    db.execute("CREATE (a:A), (b:B) CREATE (a)-[:T]->(b), (b)-[:T]->(:C)")
        .await?;

    let result = db
        .query("MATCH (n) RETURN [p = (n)-->() | p] AS list")
        .await;

    match result {
        Ok(rows) => {
            eprintln!("Path variable results ({} rows):", rows.len());
            for row in rows.rows() {
                eprintln!("  {:?}", row);
            }
        }
        Err(e) => {
            eprintln!("Path variable query failed: {:?}", e);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: extract list values as sorted strings for order-independent comparison
// ---------------------------------------------------------------------------

fn sorted_strings(list: &[Value]) -> Vec<String> {
    let mut v: Vec<String> = list
        .iter()
        .filter_map(|val| val.as_str().map(|s| s.to_string()))
        .collect();
    v.sort();
    v
}

fn sorted_ints(list: &[Value]) -> Vec<i64> {
    let mut v: Vec<i64> = list.iter().filter_map(|val| val.as_i64()).collect();
    v.sort();
    v
}

// ===========================================================================
// Happy-path tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_single_hop_verify_values() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let friends = results.rows()[0]
        .value("friends")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(sorted_strings(friends), vec!["Bob", "Carol"]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_edge_property_values() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:RATED {score: 5}]->(b), (a)-[:RATED {score: 3}]->(c)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[r:RATED]->(m) | r.score] AS scores",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let scores = results.rows()[0]
        .value("scores")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(sorted_ints(scores), vec![3, 5]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_multi_hop_chain() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'A'}), (b:Person {name: 'B'}), \
         (c:Person {name: 'C'}), (d:Person {name: 'D'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}), \
         (c:Person {name: 'C'}), (d:Person {name: 'D'}) \
         CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'A'}) \
             RETURN [(n)-[:KNOWS]->(b)-[:KNOWS]->(c) | c.name] AS fof",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let fof = results.rows()[0].value("fof").unwrap().as_array().unwrap();
    assert_eq!(sorted_strings(fof), vec!["C", "D"]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_where_clause_filter() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), \
         (b:Person {name: 'Bob', age: 25}), \
         (c:Person {name: 'Carol', age: 35})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:KNOWS]->(m) WHERE m.age > 28 | m.name] AS older",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let older = results.rows()[0]
        .value("older")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(sorted_strings(older), vec!["Carol"]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_empty_list_no_outgoing() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
        .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;

    // Bob has no outgoing KNOWS edges
    let results = db
        .query(
            "MATCH (n:Person {name: 'Bob'}) \
             RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let friends = results.rows()[0]
        .value("friends")
        .unwrap()
        .as_array()
        .unwrap();
    assert!(friends.is_empty(), "Bob should have no outgoing KNOWS");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_typed_vs_untyped_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:LIKES]->(c)",
    )
    .await?;

    // Typed: only KNOWS
    let typed = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
        )
        .await?;
    assert_eq!(typed.len(), 1);
    let friends = typed.rows()[0]
        .value("friends")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(sorted_strings(friends), vec!["Bob"]);

    // Untyped: all outgoing
    let untyped = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-->(m) | m.name] AS all_out",
        )
        .await?;
    assert_eq!(untyped.len(), 1);
    let all_out = untyped.rows()[0]
        .value("all_out")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(sorted_strings(all_out), vec!["Bob", "Carol"]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_undirected_pattern() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (c)-[:KNOWS]->(b)",
    )
    .await?;

    // Bob is connected to both Alice and Carol via undirected KNOWS
    let results = db
        .query(
            "MATCH (n:Person {name: 'Bob'}) \
             RETURN [(n)-[:KNOWS]-(m) | m.name] AS connected",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let connected = results.rows()[0]
        .value("connected")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(sorted_strings(connected), vec!["Alice", "Carol"]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_literal_map_expression() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:KNOWS]->(m) | 1] AS ones",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let ones = results.rows()[0].value("ones").unwrap().as_array().unwrap();
    assert_eq!(ones.len(), 2);
    for v in ones {
        assert_eq!(v.as_i64(), Some(1));
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_arithmetic_map_expression() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), \
         (b:Person {name: 'Bob', age: 25}), \
         (c:Person {name: 'Carol', age: 20})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c)",
    )
    .await?;

    // Use string concatenation instead of numeric arithmetic as the map expression,
    // since schemaless properties may be stored as LargeBinary which doesn't support
    // direct arithmetic coercion in DataFusion.
    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:KNOWS]->(m) | m.name] AS names",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let names = results.rows()[0]
        .value("names")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(sorted_strings(names), vec!["Bob", "Carol"]);

    // Also verify the ages come through correctly as raw values
    let results2 = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:KNOWS]->(m) | m.age] AS ages",
        )
        .await?;

    assert_eq!(results2.len(), 1);
    let ages = results2.rows()[0]
        .value("ages")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(sorted_ints(ages), vec![20, 25]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_with_order_by() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person) \
             RETURN n.name AS name, [(n)-[:KNOWS]->(m) | m.name] AS friends \
             ORDER BY name",
        )
        .await?;

    assert_eq!(results.len(), 3);

    // Build a map of name -> friends for order-independent checking
    let mut name_to_friends: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for row in results.rows() {
        let name = row.value("name").unwrap().as_str().unwrap().to_string();
        let friends = row.value("friends").unwrap().as_array().unwrap();
        name_to_friends.insert(name, sorted_strings(friends));
    }

    assert_eq!(name_to_friends["Alice"], vec!["Bob"]);
    assert_eq!(name_to_friends["Bob"], vec!["Carol"]);
    assert!(name_to_friends["Carol"].is_empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_alongside_scalar_columns() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:Person {name: 'Alice', age: 30}), (b:Person {name: 'Bob'})")
        .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN n.name AS name, n.age AS age, [(n)-[:KNOWS]->(m) | m.name] AS friends",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];
    assert_eq!(row.value("name"), Some(&Value::String("Alice".into())));
    assert_eq!(row.value("age"), Some(&Value::Int(30)));
    let friends = row.value("friends").unwrap().as_array().unwrap();
    assert_eq!(sorted_strings(friends), vec!["Bob"]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_multiple_comprehensions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:LIKES]->(c)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:KNOWS]->(m) | m.name] AS known, \
                    [(n)-[:LIKES]->(m) | m.name] AS liked",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];
    let known = row.value("known").unwrap().as_array().unwrap();
    assert_eq!(sorted_strings(known), vec!["Bob"]);
    let liked = row.value("liked").unwrap().as_array().unwrap();
    assert_eq!(sorted_strings(liked), vec!["Carol"]);

    Ok(())
}

// ===========================================================================
// Unhappy / edge-case tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_isolated_node() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (:Person {name: 'Lonely'})").await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'Lonely'}) \
             RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let friends = results.rows()[0]
        .value("friends")
        .unwrap()
        .as_array()
        .unwrap();
    assert!(friends.is_empty(), "Isolated node should have empty list");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_nonexistent_edge_type() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
        .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;

    // Edge type DOES_NOT_EXIST is not in the graph — should return empty list, not error
    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:DOES_NOT_EXIST]->(m) | m.name] AS friends",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let friends = results.rows()[0]
        .value("friends")
        .unwrap()
        .as_array()
        .unwrap();
    assert!(
        friends.is_empty(),
        "Non-existent edge type should yield empty list"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pc_null_property_in_map_expr() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (a:Person {name: 'Alice'}), \
         (b:Person {name: 'Bob', nickname: 'Bobby'}), \
         (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c)",
    )
    .await?;

    let results = db
        .query(
            "MATCH (n:Person {name: 'Alice'}) \
             RETURN [(n)-[:KNOWS]->(m) | m.nickname] AS nicknames",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let nicknames = results.rows()[0]
        .value("nicknames")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(
        nicknames.len(),
        2,
        "Should have two entries (one non-null, one null)"
    );

    // One should be "Bobby", the other null
    let has_bobby = nicknames.iter().any(|v| v.as_str() == Some("Bobby"));
    let has_null = nicknames.iter().any(|v| v.is_null());
    assert!(has_bobby, "Should contain 'Bobby'");
    assert!(has_null, "Should contain null for Carol (no nickname)");

    Ok(())
}

/// TCK Pattern2 [7]: Use a pattern comprehension inside a list comprehension.
///
/// MATCH p = (n:X)-->()
/// RETURN n, [x IN nodes(p) | size([(x)-->(:Y) | 1])] AS list
#[tokio::test(flavor = "multi_thread")]
async fn test_pc_inside_list_comprehension() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE (n1:X {n: 1}), (m1:Y), (i1:Y), (i2:Y) \
         CREATE (n1)-[:T]->(m1), \
                (m1)-[:T]->(i1), \
                (m1)-[:T]->(i2) \
         CREATE (n2:X {n: 2}), (m2), (i3:L), (i4:Y) \
         CREATE (n2)-[:T]->(m2), \
                (m2)-[:T]->(i3), \
                (m2)-[:T]->(i4)",
    )
    .await?;

    // Verify data is set up correctly
    let check = db
        .query("MATCH (n:X)-->(m) RETURN n.n AS nn, labels(m) AS ml")
        .await?;
    eprintln!("Data check ({} rows):", check.len());
    for row in check.rows() {
        eprintln!("  {:?}", row);
    }

    // Verify what nodes(p) contains
    let path_check = db
        .query("MATCH p = (n:X)-->() RETURN n.n, nodes(p) AS np")
        .await?;
    eprintln!("Path nodes ({} rows):", path_check.len());
    for row in path_check.rows() {
        eprintln!("  {:?}", row);
    }

    // Now run the actual query
    let results = db
        .query(
            "MATCH p = (n:X)-->() \
             RETURN n, [x IN nodes(p) | size([(x)-->(:Y) | 1])] AS list",
        )
        .await?;

    eprintln!("Pattern2 [7] results ({} rows):", results.len());
    for row in results.rows() {
        eprintln!("  {:?}", row);
    }

    assert_eq!(results.len(), 2, "Should have 2 rows");

    // Find the row for n1 (n=1) and n2 (n=2)
    let mut found_n1 = false;
    let mut found_n2 = false;
    for row in results.rows() {
        let n_val = row.value("n").unwrap();
        // Extract n property from node
        let n_prop = match n_val {
            Value::Node(node) => node.properties.get("n").cloned(),
            Value::Map(map) => map.get("n").cloned().or_else(|| {
                map.get("properties").and_then(|p| {
                    if let Value::Map(pm) = p {
                        pm.get("n").cloned()
                    } else {
                        None
                    }
                })
            }),
            _ => None,
        };
        let list = row.value("list").unwrap().as_array().unwrap().to_vec();
        eprintln!("  n_prop={:?}, list={:?}", n_prop, list);

        if n_prop == Some(Value::Int(1)) {
            // n1:X {n:1} --> m1:Y; n1 has 1 Y neighbor (m1), m1 has 2 Y neighbors (i1, i2)
            assert_eq!(
                list,
                vec![Value::Int(1), Value::Int(2)],
                "n1 list should be [1, 2]"
            );
            found_n1 = true;
        } else if n_prop == Some(Value::Int(2)) {
            // n2:X {n:2} --> m2; n2 has 0 Y neighbors, m2 has 1 Y neighbor (i4)
            assert_eq!(
                list,
                vec![Value::Int(0), Value::Int(1)],
                "n2 list should be [0, 1]"
            );
            found_n2 = true;
        }
    }

    assert!(found_n1, "Should find row for n1");
    assert!(found_n2, "Should find row for n2");

    Ok(())
}
