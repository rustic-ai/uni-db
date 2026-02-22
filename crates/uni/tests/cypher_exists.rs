// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::unival;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

/// Helper: create a test environment with Person schema + KNOWS + LIKES edge types.
async fn setup_person_graph(
    extra_edge_types: &[&str],
    extra_props: &[&str],
) -> anyhow::Result<(
    Arc<StorageManager>,
    Arc<RwLock<Writer>>,
    Executor,
    PropertyManager,
    QueryPlanner,
)> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_path_buf();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    for prop in extra_props {
        schema_manager.add_property("Person", prop, DataType::String, false)?;
    }
    schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    for et in extra_edge_types {
        schema_manager.add_edge_type(et, vec!["Person".to_string()], vec!["Person".to_string()])?;
    }
    schema_manager.save().await?;
    let schema = Arc::new(schema_manager.schema().clone());

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            Arc::new(schema_manager),
        )
        .await?,
    );
    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), storage.schema_manager_arc(), 0)
            .await
            .unwrap(),
    ));
    let prop_manager = PropertyManager::new(storage.clone(), storage.schema_manager_arc(), 1024);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema);

    // Leak temp_dir so it doesn't get cleaned up during the test
    std::mem::forget(temp_dir);

    Ok((storage, writer, executor, prop_manager, planner))
}

#[tokio::test]
async fn test_cypher_exists() -> anyhow::Result<()> {
    let (_storage, _writer, executor, prop_manager, planner) = setup_person_graph(&[], &[]).await?;
    let params = HashMap::new();

    // Create data: Alice->Bob, Charlie
    let query = "CREATE (a:Person {name: 'Alice'}) CREATE (b:Person {name: 'Bob'}) CREATE (c:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(b)";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // EXISTS query: Find people who know someone
    let query = "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(:Person) } RETURN p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("p.name"), Some(&unival!("Alice")));

    // NOT EXISTS query: Find people who know no one
    let query = "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(:Person) } RETURN p.name ORDER BY p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 2);
    assert_eq!(res[0].get("p.name"), Some(&unival!("Bob")));
    assert_eq!(res[1].get("p.name"), Some(&unival!("Charlie")));

    Ok(())
}

/// Test EXISTS with correlated property access: WHERE b.city = a.city across scopes.
#[tokio::test]
async fn test_exists_correlated_property() -> anyhow::Result<()> {
    let (_storage, _writer, executor, prop_manager, planner) =
        setup_person_graph(&[], &["city"]).await?;
    let params = HashMap::new();

    // Alice(NYC) -> Bob(NYC), Alice -> Charlie(SF)
    let create = "\
        CREATE (a:Person {name: 'Alice', city: 'NYC'}) \
        CREATE (b:Person {name: 'Bob', city: 'NYC'}) \
        CREATE (c:Person {name: 'Charlie', city: 'SF'}) \
        CREATE (a)-[:KNOWS]->(b) \
        CREATE (a)-[:KNOWS]->(c)";
    let ast = uni_query::parse_cypher(create)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // Correlated EXISTS: find people who know someone in the same city
    let query = "MATCH (a:Person) \
                 WHERE EXISTS { (a)-[:KNOWS]->(b:Person) WHERE b.city = a.city } \
                 RETURN a.name ORDER BY a.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // Alice knows Bob (same city NYC), so Alice matches
    assert_eq!(res.len(), 1, "Expected 1 result, got {:?}", res);
    assert_eq!(res[0].get("a.name"), Some(&unival!("Alice")));

    Ok(())
}

/// Test that EXISTS with mutation clauses (SET) is rejected.
#[tokio::test]
async fn test_exists_mutation_rejected() -> anyhow::Result<()> {
    let (_storage, _writer, executor, prop_manager, planner) = setup_person_graph(&[], &[]).await?;
    let params = HashMap::new();

    let create = "CREATE (a:Person {name: 'Alice'}) CREATE (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)";
    let ast = uni_query::parse_cypher(create)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // EXISTS with SET should error
    let query = "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(m) SET m.name = 'fail' } RETURN p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast);
    let result = match plan {
        Ok(plan) => executor.execute(plan, &prop_manager, &params).await,
        Err(e) => Err(e),
    };
    assert!(
        result.is_err(),
        "EXISTS with mutation should error, got: {:?}",
        result
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("InvalidClauseComposition")
            || err_msg.contains("updating clauses")
            || err_msg.contains("Cannot use"),
        "Error should mention clause composition, got: {}",
        err_msg
    );

    Ok(())
}

/// Test EXISTS with full MATCH subquery form (explicit MATCH + RETURN).
#[tokio::test]
async fn test_exists_full_subquery() -> anyhow::Result<()> {
    let (_storage, _writer, executor, prop_manager, planner) = setup_person_graph(&[], &[]).await?;
    let params = HashMap::new();

    let create = "CREATE (a:Person {name: 'Alice'}) CREATE (b:Person {name: 'Bob'}) CREATE (c:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(b)";
    let ast = uni_query::parse_cypher(create)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // Full subquery form: EXISTS { MATCH ... RETURN ... }
    let query =
        "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->() RETURN true } RETURN p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 1, "Expected 1 result, got {:?}", res);
    assert_eq!(res[0].get("p.name"), Some(&unival!("Alice")));

    Ok(())
}

/// Test negated EXISTS with pattern predicate form.
#[tokio::test]
async fn test_exists_negated_pattern() -> anyhow::Result<()> {
    let (_storage, _writer, executor, prop_manager, planner) = setup_person_graph(&[], &[]).await?;
    let params = HashMap::new();

    let create = "\
        CREATE (a:Person {name: 'Alice'}) \
        CREATE (b:Person {name: 'Bob'}) \
        CREATE (c:Person {name: 'Charlie'}) \
        CREATE (a)-[:KNOWS]->(b)";
    let ast = uni_query::parse_cypher(create)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // NOT EXISTS with simple pattern predicate
    let query =
        "MATCH (p:Person) WHERE NOT EXISTS { (p)-[:KNOWS]->() } RETURN p.name ORDER BY p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 2, "Expected 2 results, got {:?}", res);
    assert_eq!(res[0].get("p.name"), Some(&unival!("Bob")));
    assert_eq!(res[1].get("p.name"), Some(&unival!("Charlie")));

    Ok(())
}

/// Test EXISTS with relationship type filter inside the pattern predicate.
#[tokio::test]
async fn test_exists_with_rel_type_filter() -> anyhow::Result<()> {
    let (_storage, _writer, executor, prop_manager, planner) =
        setup_person_graph(&["LIKES"], &[]).await?;
    let params = HashMap::new();

    let create = "\
        CREATE (a:Person {name: 'Alice'}) \
        CREATE (b:Person {name: 'Bob'}) \
        CREATE (c:Person {name: 'Charlie'}) \
        CREATE (a)-[:KNOWS]->(b) \
        CREATE (b)-[:LIKES]->(c)";
    let ast = uni_query::parse_cypher(create)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // Only check for KNOWS relationship (not LIKES)
    let query = "MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->() } RETURN p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // Only Alice has KNOWS
    assert_eq!(res.len(), 1, "Expected 1 result, got {:?}", res);
    assert_eq!(res[0].get("p.name"), Some(&unival!("Alice")));

    Ok(())
}

/// Test EXISTS with relationship variable and WHERE on type(r).
#[tokio::test]
async fn test_exists_with_rel_variable() -> anyhow::Result<()> {
    let (_storage, _writer, executor, prop_manager, planner) =
        setup_person_graph(&["LIKES"], &[]).await?;
    let params = HashMap::new();

    let create = "\
        CREATE (a:Person {name: 'Alice'}) \
        CREATE (b:Person {name: 'Bob'}) \
        CREATE (a)-[:KNOWS]->(b) \
        CREATE (a)-[:LIKES]->(b)";
    let ast = uni_query::parse_cypher(create)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // EXISTS with relationship variable + type filter
    let query =
        "MATCH (p:Person) WHERE EXISTS { (p)-[r]->() WHERE type(r) = 'LIKES' } RETURN p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 1, "Expected 1 result, got {:?}", res);
    assert_eq!(res[0].get("p.name"), Some(&unival!("Alice")));

    Ok(())
}
