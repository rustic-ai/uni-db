// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_db::unival;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_exists() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
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
    let params = HashMap::new();

    // 2. Create data: Alice->Bob, Charlie
    let query = "CREATE (a:Person {name: 'Alice'}) CREATE (b:Person {name: 'Bob'}) CREATE (c:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(b)";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. EXISTS query: Find people who know someone
    let query = "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(:Person) } RETURN p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // Should match Alice
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("p.name"), Some(&unival!("Alice")));

    // 4. NOT EXISTS query: Find people who know no one
    let query = "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(:Person) } RETURN p.name ORDER BY p.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // Should match Bob, Charlie
    assert_eq!(res.len(), 2);
    // Assuming Bob and Charlie. Order by name -> Bob, Charlie.
    assert_eq!(res[0].get("p.name"), Some(&unival!("Bob")));
    assert_eq!(res[1].get("p.name"), Some(&unival!("Charlie")));

    Ok(())
}
