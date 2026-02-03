// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::SchemaManager;
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_create() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    let _knows_type =
        schema_manager.add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    // Initialize Writer
    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // Test 1: CREATE (n:Person)
    println!("--- Test 1: CREATE (n:Person) ---");
    let sql = "CREATE (n:Person {name: 'Alice'})";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    // Verify in Writer L0
    {
        let w = writer.read().await;
        let l0_arc = w.l0_manager.get_current();
        let l0 = l0_arc.read();
        assert_eq!(l0.graph.vertex_count(), 1);
        let _vid = l0.graph.vertices().next().unwrap();
        // With the new pure auto-increment model, labels are tracked via VidLabelsIndex
        // not embedded in the VID itself
        assert_eq!(l0.vertex_properties.len(), 1);
    }

    // Flush L0 to make it visible for subsequent MATCH?
    // Executor uses StorageManager to MATCH. StorageManager reads L1/L2.
    // L0 is not yet fully integrated into read path for MATCH unless we pass it.
    // But StorageManager doesn't take L0.
    // So we must flush to verify via MATCH.
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // Test 2: MATCH (a:Person) CREATE (a)-[:KNOWS]->(b:Person)
    println!("--- Test 2: MATCH ... CREATE ---");
    // We have 1 person. Let's create another connected to it.
    let sql = "MATCH (a:Person) CREATE (a)-[:KNOWS]->(b:Person)";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    // Verify
    {
        let mut w = writer.write().await;
        // Should have 1 new node (b) and 1 new edge.
        let l0_arc = w.l0_manager.get_current();
        {
            let l0 = l0_arc.read();
            assert_eq!(l0.graph.edge_count(), 1);
        }
        w.flush_to_l1(None).await?;
    }

    // Test 3: CREATE Chain (x:Person)-[:KNOWS]->(y:Person)
    println!("--- Test 3: CREATE Chain ---");
    let sql = "CREATE (x:Person)-[:KNOWS]->(y:Person)";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    {
        let w = writer.read().await;
        let l0_arc = w.l0_manager.get_current();
        let l0 = l0_arc.read();
        assert_eq!(l0.graph.vertex_count(), 2);
        assert_eq!(l0.graph.edge_count(), 1);
    }

    Ok(())
}
