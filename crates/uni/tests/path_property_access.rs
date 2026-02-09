// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::UniConfig;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::unival;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_path_property_access() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    let knows_edge = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;

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
    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        UniConfig::default(),
        None,
        None,
    )
    .await
    .unwrap();

    // 2. Insert Data
    // A -> B
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);

    let mut props_a = HashMap::new();
    props_a.insert("name".to_string(), unival!("Alice"));
    writer
        .insert_vertex_with_labels(vid_a, props_a, vec!["Person".to_string()])
        .await?;

    let mut props_b = HashMap::new();
    props_b.insert("name".to_string(), unival!("Bob"));
    writer
        .insert_vertex_with_labels(vid_b, props_b, vec!["Person".to_string()])
        .await?;

    writer
        .insert_edge(vid_a, vid_b, knows_edge, Eid::new(10), HashMap::new())
        .await?;

    // Keep data in L0 to verify L0 lookup in vectorized engine
    // writer.flush_to_l1().await?;

    // 3. Test Property Access on Path Node: nodes(p)[0].name
    // We need a named path variable to use nodes() function.
    // The relationship variable (r) in VLP binds to a list of edges, not a Path.
    // Use p = ... syntax to create a named path variable.
    let cypher =
        "MATCH p = (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN nodes(p)[0].name, nodes(p)[1].name";

    let query_ast = uni_query::parse_cypher(cypher)?;

    // Set RUST_LOG for debugging
    // std::env::set_var("RUST_LOG", "debug");
    // let _ = env_logger::builder().is_test(true).try_init();

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor =
        Executor::new_with_writer(storage.clone(), Arc::new(tokio::sync::RwLock::new(writer)));

    // We need to access writer to get L0 manager for prop manager?
    // Executor::new_with_writer handles context creation.
    // But we need to pass a property manager.
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("nodes(p)[0].name"), Some(&unival!("Alice")));
    assert_eq!(results[0].get("nodes(p)[1].name"), Some(&unival!("Bob")));

    Ok(())
}
