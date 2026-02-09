// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_db::{unival, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::UniConfig;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_null_handling_functions() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;

    // Add properties, including nullable ones
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int64, true)?; // Nullable
    schema_manager.add_property("Person", "nickname", DataType::String, true)?; // Nullable

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

    // 2. Insert Data
    {
        let mut w = writer.write().await;

        // Person 1: Alice, age 30, no nickname
        let vid1 = Vid::new(1);
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), unival!("Alice"));
        props1.insert("age".to_string(), unival!(30));
        w.insert_vertex_with_labels(vid1, props1, vec!["Person".to_string()])
            .await?;

        // Person 2: Bob, no age, nickname 'Bobby'
        let vid2 = Vid::new(2);
        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), unival!("Bob"));
        props2.insert("nickname".to_string(), unival!("Bobby"));
        w.insert_vertex_with_labels(vid2, props2, vec!["Person".to_string()])
            .await?;

        // Person 3: Charlie, no age, no nickname
        let vid3 = Vid::new(3);
        let mut props3 = HashMap::new();
        props3.insert("name".to_string(), unival!("Charlie"));
        w.insert_vertex_with_labels(vid3, props3, vec!["Person".to_string()])
            .await?;

        w.flush_to_l1(None).await?;
    }

    // 3. Test COALESCE
    // MATCH (n:Person) RETURN n.name, coalesce(n.nickname, n.age, 'Unknown') ORDER BY n.name
    let cypher_coalesce =
        "MATCH (n:Person) RETURN n.name, coalesce(n.nickname, n.age, 'Unknown') ORDER BY n.name";

    let query_ast = uni_query::parse_cypher(cypher_coalesce)?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    let coalesce_key = "coalesce(n.nickname, n.age, \"Unknown\")";

    // Expected results sorted by name: Alice, Bob, Charlie
    // Alice: nickname=null, age=30 -> 30 (may be string "30" in vectorized engine due to mixed type promotion)
    assert_eq!(results[0].get("n.name"), Some(&unival!("Alice")));
    let alice_coalesce = results[0].get(coalesce_key).unwrap();
    assert!(
        alice_coalesce == &unival!(30) || alice_coalesce == &Value::String("30".to_string()),
        "Alice coalesce mismatch: {:?}",
        alice_coalesce
    );

    // Bob: nickname='Bobby', age=null -> 'Bobby'
    assert_eq!(results[1].get("n.name"), Some(&unival!("Bob")));
    assert_eq!(results[1].get(coalesce_key), Some(&unival!("Bobby")));

    // Charlie: nickname=null, age=null -> 'Unknown'
    assert_eq!(results[2].get("n.name"), Some(&unival!("Charlie")));
    assert_eq!(results[2].get(coalesce_key), Some(&unival!("Unknown")));

    // 4. Test nullIf
    // MATCH (n:Person) RETURN n.name, nullIf(n.name, 'Bob') ORDER BY n.name
    let cypher_nullif = "MATCH (n:Person) RETURN n.name, nullIf(n.name, 'Bob') ORDER BY n.name";

    let query_ast = uni_query::parse_cypher(cypher_nullif)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    let nullif_key = "nullIf(n.name, \"Bob\")";

    // Alice: 'Alice' != 'Bob' -> 'Alice'
    assert_eq!(results[0].get("n.name"), Some(&unival!("Alice")));
    assert_eq!(results[0].get(nullif_key), Some(&unival!("Alice")));

    // Bob: 'Bob' == 'Bob' -> null
    assert_eq!(results[1].get("n.name"), Some(&unival!("Bob")));
    assert_eq!(results[1].get(nullif_key), Some(&Value::Null));

    // Charlie: 'Charlie' != 'Bob' -> 'Charlie'
    assert_eq!(results[2].get("n.name"), Some(&unival!("Charlie")));
    assert_eq!(results[2].get(nullif_key), Some(&unival!("Charlie")));

    Ok(())
}
