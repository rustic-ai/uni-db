// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use tempfile::tempdir;
use uni_db::api::Uni;
use uni_db::core::schema::{DataType, SchemaManager};

#[tokio::test]
async fn test_collection_types_storage_and_query() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Node")?;

    // List<String>
    schema_manager.add_property(
        "Node",
        "tags",
        DataType::List(Box::new(DataType::String)),
        false,
    )?;

    // Map<String, Int64>
    schema_manager.add_property(
        "Node",
        "counters",
        DataType::Map(Box::new(DataType::String), Box::new(DataType::Int64)),
        false,
    )?;

    schema_manager.save().await?;

    // 2. Initialize Uni
    let db = Uni::open(path.to_string_lossy().to_string())
        .build()
        .await?;

    // 3. Insert Data via Cypher (Testing parser/planner/executor for collections)
    db.execute("CREATE (n:Node {tags: ['alpha', 'beta', 'gamma'], counters: {a: 10, b: 20}})")
        .await?;
    db.flush().await?;

    // 4. Query Data
    // Basic Fetch
    let query_fetch = "MATCH (n:Node) RETURN n.tags, n.counters";
    let res = db.query(query_fetch).await?;

    // Rows are uni_db::Value. Convert to JSON for easier assertion.
    let fetched_tags: Vec<String> =
        serde_json::from_value(res.rows[0].value("n.tags").unwrap().clone().into())?;
    assert_eq!(fetched_tags, vec!["alpha", "beta", "gamma"]);

    let fetched_counters: HashMap<String, i64> =
        serde_json::from_value(res.rows[0].value("n.counters").unwrap().clone().into())?;
    assert_eq!(fetched_counters.get("a"), Some(&10));
    assert_eq!(fetched_counters.get("b"), Some(&20));

    // 5. Test Functions
    // size(), head(), tail() on List
    let query_list_funcs = "MATCH (n:Node) RETURN size(n.tags), head(n.tags), size(tail(n.tags))";
    let res_list = db.query(query_list_funcs).await?;

    let size_tags: i64 = serde_json::from_value(res_list.rows[0].values[0].clone().into())?;
    assert_eq!(size_tags, 3);

    let head_tags: String = serde_json::from_value(res_list.rows[0].values[1].clone().into())?;
    assert_eq!(head_tags, "alpha");

    let size_tail: i64 = serde_json::from_value(res_list.rows[0].values[2].clone().into())?;
    assert_eq!(size_tail, 2);

    // size(), keys() on Map
    let query_map_funcs = "MATCH (n:Node) RETURN size(n.counters), keys(n.counters)";
    let res_map = db.query(query_map_funcs).await?;

    let size_map: i64 = serde_json::from_value(res_map.rows[0].values[0].clone().into())?;
    assert_eq!(size_map, 2);

    let keys_map: Vec<String> = serde_json::from_value(res_map.rows[0].values[1].clone().into())?;
    assert!(keys_map.contains(&"a".to_string()));
    assert!(keys_map.contains(&"b".to_string()));

    Ok(())
}
