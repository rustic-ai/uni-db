// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::Value;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::unival;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_shortest_path_match() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
    schema_manager.add_property("Node", "name", DataType::String, false)?;
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

    // 2. Insert Data (Chain: A -> B -> C -> D)
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);
    let vid_d = Vid::new(3);

    {
        let mut w = writer.write().await;
        for (vid, name) in [(vid_a, "A"), (vid_b, "B"), (vid_c, "C"), (vid_d, "D")] {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String(name.to_string()));
            w.insert_vertex_with_labels(vid, props, vec!["Node".to_string()])
                .await?;
        }

        w.insert_edge(vid_a, vid_b, link_edge, Eid::new(0), HashMap::new())
            .await?;
        w.insert_edge(vid_b, vid_c, link_edge, Eid::new(1), HashMap::new())
            .await?;
        w.insert_edge(vid_c, vid_d, link_edge, Eid::new(2), HashMap::new())
            .await?;

        // Add a long shortcut just to test shortestPath
        // A -> D (but let's say we want to find the one through LINK)
        // Wait, if I add A -> D directly, shortestPath will be 1 hop.

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 3. Test shortestPath((a)-[*]->(b))
    let cypher = "MATCH (a:Node {name: 'A'}), (b:Node {name: 'D'}) MATCH p = shortestPath((a)-[:LINK*]->(b)) RETURN length(p) as len";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("len"), Some(&unival!(3)));

    Ok(())
}

/// Test allShortestPaths with a diamond graph where there are two shortest paths.
///
/// Graph:
///   A -> B -> D
///   A -> C -> D
///
/// Both A->B->D and A->C->D are shortest paths of length 2.
#[tokio::test]
async fn test_all_shortest_paths_diamond() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
    schema_manager.add_property("Node", "name", DataType::String, false)?;
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

    // Diamond: A -> B -> D, A -> C -> D
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);
    let vid_d = Vid::new(3);

    {
        let mut w = writer.write().await;
        for (vid, name) in [(vid_a, "A"), (vid_b, "B"), (vid_c, "C"), (vid_d, "D")] {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String(name.to_string()));
            w.insert_vertex_with_labels(vid, props, vec!["Node".to_string()])
                .await?;
        }

        w.insert_edge(vid_a, vid_b, link_edge, Eid::new(0), HashMap::new())
            .await?;
        w.insert_edge(vid_a, vid_c, link_edge, Eid::new(1), HashMap::new())
            .await?;
        w.insert_edge(vid_b, vid_d, link_edge, Eid::new(2), HashMap::new())
            .await?;
        w.insert_edge(vid_c, vid_d, link_edge, Eid::new(3), HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // allShortestPaths should return 2 paths (A->B->D and A->C->D)
    let cypher = "MATCH (a:Node {name: 'A'}), (d:Node {name: 'D'}) \
                  MATCH p = allShortestPaths((a)-[:LINK*]->(d)) \
                  RETURN length(p) as len ORDER BY len";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(
        results.len(),
        2,
        "Expected 2 shortest paths in diamond graph"
    );
    // Both paths should be length 2
    assert_eq!(results[0].get("len"), Some(&unival!(2)));
    assert_eq!(results[1].get("len"), Some(&unival!(2)));

    Ok(())
}

/// Test allShortestPaths returns only shortest (not longer) paths.
///
/// Graph:
///   A -> B -> C (length 2)
///   A -> C      (length 1, shortest)
///
/// allShortestPaths should return only A->C (length 1).
#[tokio::test]
async fn test_all_shortest_paths_only_shortest() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
    schema_manager.add_property("Node", "name", DataType::String, false)?;
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

    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);

    {
        let mut w = writer.write().await;
        for (vid, name) in [(vid_a, "A"), (vid_b, "B"), (vid_c, "C")] {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String(name.to_string()));
            w.insert_vertex_with_labels(vid, props, vec!["Node".to_string()])
                .await?;
        }

        w.insert_edge(vid_a, vid_b, link_edge, Eid::new(0), HashMap::new())
            .await?;
        w.insert_edge(vid_b, vid_c, link_edge, Eid::new(1), HashMap::new())
            .await?;
        w.insert_edge(vid_a, vid_c, link_edge, Eid::new(2), HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // allShortestPaths should return only the direct A->C path (length 1)
    let cypher = "MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
                  MATCH p = allShortestPaths((a)-[:LINK*]->(c)) \
                  RETURN length(p) as len";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1, "Expected 1 shortest path (direct edge)");
    assert_eq!(results[0].get("len"), Some(&unival!(1)));

    Ok(())
}

/// Test allShortestPaths when no path exists.
#[tokio::test]
async fn test_all_shortest_paths_no_path() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let _link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
    schema_manager.add_property("Node", "name", DataType::String, false)?;
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

    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);

    {
        let mut w = writer.write().await;
        for (vid, name) in [(vid_a, "A"), (vid_b, "B")] {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String(name.to_string()));
            w.insert_vertex_with_labels(vid, props, vec!["Node".to_string()])
                .await?;
        }
        // No edges between A and B
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher = "MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) \
                  MATCH p = allShortestPaths((a)-[:LINK*]->(b)) \
                  RETURN length(p) as len";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // When no path exists, the exec returns a null-path row (consistent with shortestPath)
    assert_eq!(results.len(), 1, "Expected 1 result with null path");
    assert_eq!(results[0].get("len"), Some(&Value::Null));

    Ok(())
}

/// Test allShortestPaths with source == target.
#[tokio::test]
async fn test_all_shortest_paths_same_node() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let _link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
    schema_manager.add_property("Node", "name", DataType::String, false)?;
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

    let vid_a = Vid::new(0);

    {
        let mut w = writer.write().await;
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("A".to_string()));
        w.insert_vertex_with_labels(vid_a, props, vec!["Node".to_string()])
            .await?;
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher = "MATCH (a:Node {name: 'A'}) \
                  MATCH p = allShortestPaths((a)-[:LINK*0..]->(a)) \
                  RETURN length(p) as len";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // source == target with *0.. should return 1 result with length 0
    assert_eq!(results.len(), 1, "Expected 1 result for self-path");
    assert_eq!(results[0].get("len"), Some(&unival!(0)));

    Ok(())
}
