// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::UniConfig;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::unival;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_case_expression() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int64, true)?;
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
        Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            0,
            UniConfig::default(),
            None,
            None,
        )
        .await
        .unwrap(),
    ));

    {
        let mut w = writer.write().await;

        // Alice: 10
        let vid1 = Vid::new(1);
        let mut props1 = HashMap::new();
        props1.insert(
            "name".to_string(),
            uni_common::Value::String("Alice".to_string()),
        );
        props1.insert("age".to_string(), uni_common::Value::Int(10));
        w.insert_vertex_with_labels(vid1, props1, vec!["Person".to_string()])
            .await?;

        // Bob: 20
        let vid2 = Vid::new(2);
        let mut props2 = HashMap::new();
        props2.insert(
            "name".to_string(),
            uni_common::Value::String("Bob".to_string()),
        );
        props2.insert("age".to_string(), uni_common::Value::Int(20));
        w.insert_vertex_with_labels(vid2, props2, vec!["Person".to_string()])
            .await?;

        // Charlie: 30
        let vid3 = Vid::new(3);
        let mut props3 = HashMap::new();
        props3.insert(
            "name".to_string(),
            uni_common::Value::String("Charlie".to_string()),
        );
        props3.insert("age".to_string(), uni_common::Value::Int(30));
        w.insert_vertex_with_labels(vid3, props3, vec!["Person".to_string()])
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // 1. Generic CASE
    // MATCH (n:Person) RETURN n.name, CASE WHEN n.age < 15 THEN 'Child' WHEN n.age < 25 THEN 'Teen' ELSE 'Adult' END ORDER BY n.name
    let cypher1 = "MATCH (n:Person) RETURN n.name, CASE WHEN n.age < 15 THEN 'Child' WHEN n.age < 25 THEN 'Teen' ELSE 'Adult' END ORDER BY n.name";

    let query_ast = uni_query::parse_cypher(cypher1)?;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results[0].get("n.name"), Some(&unival!("Alice"))); // Child
    let val1 = results[0]
        .values()
        .find(|v| v.as_str() == Some("Child"))
        .cloned();
    assert_eq!(val1, Some(unival!("Child")));

    assert_eq!(results[1].get("n.name"), Some(&unival!("Bob"))); // Teen
    let val2 = results[1]
        .values()
        .find(|v| v.as_str() == Some("Teen"))
        .cloned();
    assert_eq!(val2, Some(unival!("Teen")));

    assert_eq!(results[2].get("n.name"), Some(&unival!("Charlie"))); // Adult
    let val3 = results[2]
        .values()
        .find(|v| v.as_str() == Some("Adult"))
        .cloned();
    assert_eq!(val3, Some(unival!("Adult")));

    // 2. Simple CASE
    // MATCH (n:Person) RETURN n.name, CASE n.name WHEN 'Alice' THEN 1 WHEN 'Bob' THEN 2 ELSE 3 END ORDER BY n.name
    let cypher2 = "MATCH (n:Person) RETURN n.name, CASE n.name WHEN 'Alice' THEN 1 WHEN 'Bob' THEN 2 ELSE 3 END ORDER BY n.name";
    let query_ast = uni_query::parse_cypher(cypher2)?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // Alice -> 1
    assert_eq!(results[0].get("n.name"), Some(&unival!("Alice")));
    let val1 = results[0].values().find(|v| v.as_u64() == Some(1)).cloned();
    assert_eq!(val1, Some(unival!(1)));

    // Bob -> 2
    assert_eq!(results[1].get("n.name"), Some(&unival!("Bob")));
    let val2 = results[1].values().find(|v| v.as_u64() == Some(2)).cloned();
    assert_eq!(val2, Some(unival!(2)));

    // Charlie -> 3
    assert_eq!(results[2].get("n.name"), Some(&unival!("Charlie")));
    let val3 = results[2].values().find(|v| v.as_u64() == Some(3)).cloned();
    assert_eq!(val3, Some(unival!(3)));

    Ok(())
}
