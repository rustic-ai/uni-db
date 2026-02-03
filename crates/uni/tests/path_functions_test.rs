// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::UniConfig;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_path_functions() -> anyhow::Result<()> {
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
    // A -> B -> C
    let names = ["A", "B", "C"];
    let mut vids = Vec::new();

    {
        let mut w = writer.write().await;

        for (i, name) in names.iter().enumerate() {
            let vid = Vid::new(i as u64);
            vids.push(vid);
            let mut props = HashMap::new();
            props.insert("name".to_string(), json!(name));
            w.insert_vertex_with_labels(vid, props, vec!["Person".to_string()])
                .await?;
        }

        // Edges with specific EIDs
        // A(0) -> B(1), eid=10
        w.insert_edge(vids[0], vids[1], knows_edge, Eid::new(10), HashMap::new())
            .await?;
        // B(1) -> C(2), eid=11
        w.insert_edge(vids[1], vids[2], knows_edge, Eid::new(11), HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    // 3. Test Path Functions
    // MATCH (n:Person)-[p:KNOWS*2..2]->(m:Person) WHERE n.name = 'A'
    // RETURN length(p), nodes(p), relationships(p)
    // Test simpler first - just return p
    let cypher = "MATCH (n:Person)-[p:KNOWS*2..2]->(m:Person) WHERE n.name = 'A' RETURN p";

    let query_ast = uni_query::parse_cypher(cypher)?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);

    // Debug: print all keys and the value of 'p'
    eprintln!("Result keys: {:?}", results[0].keys().collect::<Vec<_>>());
    if let Some(p_val) = results[0].get("p") {
        eprintln!("Value of 'p': {:?}", p_val);
        eprintln!(
            "Type of 'p': {}",
            if p_val.is_object() {
                "Object"
            } else if p_val.is_array() {
                "Array"
            } else {
                "Other"
            }
        );
    } else {
        eprintln!("'p' not found in results!");
    }

    // Check if p is a proper path object
    let p = results[0].get("p").expect("p should be in results");
    assert!(p.is_object(), "p should be an object (Path)");

    let p_obj = p.as_object().unwrap();
    eprintln!("Fields in p: {:?}", p_obj.keys().collect::<Vec<_>>());

    assert!(p_obj.contains_key("nodes"), "p should have 'nodes' field");
    assert!(
        p_obj.contains_key("relationships"),
        "p should have 'relationships' field"
    );

    let nodes = p_obj.get("nodes").unwrap().as_array().unwrap();
    let rels = p_obj.get("relationships").unwrap().as_array().unwrap();

    assert_eq!(nodes.len(), 3);
    assert_eq!(rels.len(), 2);

    // Nodes are objects with _id field
    let node0_id = nodes[0]
        .get("_id")
        .unwrap()
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let node1_id = nodes[1]
        .get("_id")
        .unwrap()
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let node2_id = nodes[2]
        .get("_id")
        .unwrap()
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();

    assert_eq!(node0_id, vids[0].as_u64());
    assert_eq!(node1_id, vids[1].as_u64());
    assert_eq!(node2_id, vids[2].as_u64());

    // Relationships are objects with _id field
    let rel0_id = rels[0]
        .get("_id")
        .unwrap()
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let rel1_id = rels[1]
        .get("_id")
        .unwrap()
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let eid10 = Eid::new(10).as_u64();
    let eid11 = Eid::new(11).as_u64();
    assert_eq!(rel0_id, eid10);
    assert_eq!(rel1_id, eid11);

    Ok(())
}
