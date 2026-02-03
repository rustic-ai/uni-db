// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_pagerank_dangling_nodes() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
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

    // Graph: A -> B. B is a sink (no outgoing edges).
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);

    {
        let mut w = writer.write().await;
        w.insert_vertex_with_labels(vid_a, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(vid_b, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_edge(vid_a, vid_b, link_edge, Eid::new(0), HashMap::new())
            .await?;
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Run PageRank
    let cypher =
        "CALL uni.algo.pageRank(['Node'], ['LINK']) YIELD nodeId, score RETURN nodeId, score";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);
    // Even with dangling nodes, PageRank should return scores.
    // The exact relationship between scores depends on damping factor and dangling node handling.
    let mut scores = HashMap::new();
    for row in results {
        let vid = row.get("nodeId").unwrap().as_u64().unwrap();
        let score = row.get("score").unwrap().as_f64().unwrap();
        scores.insert(vid, score);
    }

    // Verify both nodes have valid scores
    let score_a = *scores.get(&vid_a.as_u64()).unwrap();
    let score_b = *scores.get(&vid_b.as_u64()).unwrap();
    assert!(score_a > 0.0, "Node A should have positive PageRank score");
    assert!(score_b > 0.0, "Node B should have positive PageRank score");
    // B should receive at least as much as A since A points to B
    assert!(
        score_b >= score_a,
        "B should have score >= A since A points to B"
    );

    Ok(())
}

#[tokio::test]
async fn test_shortest_path_unreachable() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
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

    // Graph: A -> B, C -> D. Disconnected components.
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);
    let vid_d = Vid::new(3);

    {
        let mut w = writer.write().await;
        for &v in &[vid_a, vid_b, vid_c, vid_d] {
            w.insert_vertex_with_labels(v, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }
        w.insert_edge(vid_a, vid_b, link_edge, Eid::new(0), HashMap::new())
            .await?;
        w.insert_edge(vid_c, vid_d, link_edge, Eid::new(1), HashMap::new())
            .await?;
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Run Shortest Path A -> C (Unreachable)
    let cypher = format!(
        "CALL uni.algo.shortestPath('{}', '{}', ['LINK']) YIELD length RETURN length",
        vid_a, vid_c
    );
    let query_ast = uni_query::parse_cypher(&cypher)?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // Should return no rows
    assert_eq!(results.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_wcc_singletons() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let _link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
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

    // Graph: A, B, C. No edges.
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);

    {
        let mut w = writer.write().await;
        for &v in &[vid_a, vid_b, vid_c] {
            w.insert_vertex_with_labels(v, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Run WCC
    let cypher = "CALL uni.algo.wcc(['Node'], ['LINK']) YIELD nodeId, componentId RETURN nodeId, componentId";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 3);

    // Each node should be in its own component
    let mut components = std::collections::HashSet::new();
    for row in results {
        components.insert(row.get("componentId").unwrap().as_u64().unwrap());
    }
    assert_eq!(components.len(), 3);

    Ok(())
}

#[tokio::test]
async fn test_louvain_disconnected() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
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

    // Graph: Clique A-B and Clique C-D. Disconnected.
    let vids: Vec<Vid> = (0..4).map(Vid::new).collect();

    {
        let mut w = writer.write().await;
        for &v in &vids {
            w.insert_vertex_with_labels(v, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }
        // Clique 1
        w.insert_edge(vids[0], vids[1], link_edge, Eid::new(0), HashMap::new())
            .await?;
        w.insert_edge(vids[1], vids[0], link_edge, Eid::new(1), HashMap::new())
            .await?;
        // Clique 2
        w.insert_edge(vids[2], vids[3], link_edge, Eid::new(2), HashMap::new())
            .await?;
        w.insert_edge(vids[3], vids[2], link_edge, Eid::new(3), HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Run Louvain
    let cypher = "CALL uni.algo.louvain(['Node'], ['LINK']) YIELD nodeId, communityId RETURN nodeId, communityId";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 4);

    let mut comms = HashMap::new();
    for row in results {
        let nid = row.get("nodeId").unwrap().as_u64().unwrap();
        let cid = row.get("communityId").unwrap().as_u64().unwrap();
        comms.insert(nid, cid);
    }

    // 0 and 1 should be same. 2 and 3 should be same.
    assert_eq!(comms.get(&vids[0].as_u64()), comms.get(&vids[1].as_u64()));
    assert_eq!(comms.get(&vids[2].as_u64()), comms.get(&vids[3].as_u64()));
    // Communities should be different
    assert_ne!(comms.get(&vids[0].as_u64()), comms.get(&vids[2].as_u64()));

    Ok(())
}

#[tokio::test]
async fn test_algo_grid_graph() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
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

    // 10x10 Grid
    let width = 10;
    let height = 10;
    let n = width * height;

    {
        let mut w = writer.write().await;
        for i in 0..n {
            w.insert_vertex_with_labels(
                Vid::new(i as u64),
                HashMap::new(),
                vec!["Node".to_string()],
            )
            .await?;
        }

        // Add edges (Grid structure)
        let mut edge_id = 0;
        for y in 0..height {
            for x in 0..width {
                let u = y * width + x;

                // Right neighbor
                if x + 1 < width {
                    let v = y * width + (x + 1);
                    w.insert_edge(
                        Vid::new(u as u64),
                        Vid::new(v as u64),
                        link_edge,
                        Eid::new(edge_id),
                        HashMap::new(),
                    )
                    .await?;
                    edge_id += 1;
                    // Undirected for simple grid movement
                    w.insert_edge(
                        Vid::new(v as u64),
                        Vid::new(u as u64),
                        link_edge,
                        Eid::new(edge_id),
                        HashMap::new(),
                    )
                    .await?;
                    edge_id += 1;
                }

                // Down neighbor
                if y + 1 < height {
                    let v = (y + 1) * width + x;
                    w.insert_edge(
                        Vid::new(u as u64),
                        Vid::new(v as u64),
                        link_edge,
                        Eid::new(edge_id),
                        HashMap::new(),
                    )
                    .await?;
                    edge_id += 1;
                    w.insert_edge(
                        Vid::new(v as u64),
                        Vid::new(u as u64),
                        link_edge,
                        Eid::new(edge_id),
                        HashMap::new(),
                    )
                    .await?;
                    edge_id += 1;
                }
            }
        }
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Test Shortest Path from Top-Left (0,0) to Bottom-Right (9,9)
    // Distance should be (width-1) + (height-1) = 9 + 9 = 18 steps.
    let start_node = Vid::new(0);
    let end_node = Vid::new((n - 1) as u64);

    let cypher = format!(
        "CALL uni.algo.shortestPath('{}', '{}', ['LINK']) YIELD length RETURN length",
        start_node, end_node
    );
    let query_ast = uni_query::parse_cypher(&cypher)?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let len = results[0].get("length").unwrap().as_i64().unwrap();
    assert_eq!(len, 18);

    Ok(())
}

#[tokio::test]
async fn test_shortest_path_self() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let _link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
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
        w.insert_vertex_with_labels(vid_a, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher = format!(
        "CALL uni.algo.shortestPath('{}', '{}', ['LINK']) YIELD length RETURN length",
        vid_a, vid_a
    );
    let query_ast = uni_query::parse_cypher(&cypher)?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let len = results[0].get("length").unwrap().as_i64().unwrap();
    assert_eq!(len, 0);

    Ok(())
}
