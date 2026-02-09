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
async fn test_reader_isolation_lifecycle() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    // Executor with Writer (enables L0 access)
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // 2. Insert into L0 (No Flush)
    {
        let mut w = writer.write().await;
        let v1 = w.next_vid().await?;
        let mut p1 = HashMap::new();
        p1.insert("name".to_string(), unival!("Alice"));
        w.insert_vertex_with_labels(v1, p1, vec!["Person".to_string()])
            .await?;
    }

    // 3. Query (Should see L0 data)
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(results.len(), 1, "Should find Alice in L0");
    assert_eq!(results[0].get("n.name"), Some(&unival!("Alice")));

    // 4. Flush to Storage
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // 5. Query (Should see Storage data)
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(results.len(), 1, "Should find Alice in Storage");
    assert_eq!(results[0].get("n.name"), Some(&unival!("Alice")));

    // 6. Delete in L0 (No Flush)
    // We need the VID.
    let alice_vid = {
        // Cheating a bit: we know it's VID(0, 0) but let's be robust
        // Scan to get VID
        let vids = executor
            .execute(
                planner.plan(uni_query::parse_cypher("MATCH (n:Person) RETURN n")?)?,
                &prop_mgr,
                &HashMap::new(),
            )
            .await?;
        // Extract VID from the node object
        let n_obj = vids[0].get("n").unwrap().as_object().unwrap();
        let vid_val = n_obj.get("_vid").unwrap();
        uni_db::core::id::Vid::new(vid_val.as_u64().unwrap())
    };

    {
        let mut w = writer.write().await;
        w.delete_vertex(alice_vid, None).await?;
    }

    // 7. Query (Should NOT see Alice due to L0 tombstone)
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(
        results.len(),
        0,
        "Should NOT find Alice (masked by L0 tombstone)"
    );

    // 8. Flush (Commit deletion)
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // 9. Query (Should NOT see Alice in Storage)
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    println!("DEBUG: Plan for final query: {:?}", plan);
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;
    println!("DEBUG: Results for final query: {:?}", results);

    assert_eq!(
        results.len(),
        0,
        "Should NOT find Alice (deleted in Storage)"
    );

    Ok(())
}
