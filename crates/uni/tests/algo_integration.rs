// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_db::unival;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::id::Vid;
use uni_db::core::schema::SchemaManager;
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_pagerank_procedure() -> anyhow::Result<()> {
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

    // 2. Insert Data (Triangle: A -> B, B -> C, C -> A)
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);

    {
        let mut w = writer.write().await;
        w.insert_vertex_with_labels(vid_a, HashMap::new(), vec!["Person".to_string()])
            .await?;
        w.insert_vertex_with_labels(vid_b, HashMap::new(), vec!["Person".to_string()])
            .await?;
        w.insert_vertex_with_labels(vid_c, HashMap::new(), vec!["Person".to_string()])
            .await?;

        let eid1 = w.next_eid(knows_edge).await?;
        w.insert_edge(vid_a, vid_b, knows_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(knows_edge).await?;
        w.insert_edge(vid_b, vid_c, knows_edge, eid2, HashMap::new())
            .await?;
        let eid3 = w.next_eid(knows_edge).await?;
        w.insert_edge(vid_c, vid_a, knows_edge, eid3, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 3. Run PageRank
    let cypher =
        "CALL uni.algo.pageRank(['Person'], ['KNOWS']) YIELD nodeId, score RETURN nodeId, score";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 3);
    // In a symmetric triangle, all nodes should have same score (1/3 = 0.333...)
    for row in results {
        let score = row.get("score").unwrap().as_f64().unwrap();
        assert!((score - 0.333).abs() < 0.1);
    }

    Ok(())
}

#[tokio::test]
async fn test_wcc_procedure() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
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

    // 2. Insert Data (Two components: {A, B} and {C})
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);

    {
        let mut w = writer.write().await;
        w.insert_vertex_with_labels(vid_a, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(vid_b, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(vid_c, HashMap::new(), vec!["Node".to_string()])
            .await?;

        let eid = w.next_eid(link_edge).await?;
        w.insert_edge(vid_a, vid_b, link_edge, eid, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 3. Run WCC
    let cypher = "CALL uni.algo.wcc(['Node'], ['LINK']) YIELD nodeId, componentId RETURN nodeId, componentId";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 3);

    let mut components = HashMap::new();
    for row in results {
        let node_id = row.get("nodeId").unwrap().as_u64().unwrap();
        let component_id = row.get("componentId").unwrap().as_u64().unwrap();
        components.insert(node_id, component_id);
    }

    assert_eq!(
        components.get(&vid_a.as_u64()),
        components.get(&vid_b.as_u64())
    );
    assert_ne!(
        components.get(&vid_a.as_u64()),
        components.get(&vid_c.as_u64())
    );

    Ok(())
}

#[tokio::test]
async fn test_shortest_path_procedure() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
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

    // 2. Insert Data (Chain: A -> B -> C)
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);

    {
        let mut w = writer.write().await;
        w.insert_vertex_with_labels(vid_a, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(vid_b, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(vid_c, HashMap::new(), vec!["Node".to_string()])
            .await?;

        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(vid_a, vid_b, link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(vid_b, vid_c, link_edge, eid2, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 3. Run Shortest Path
    let cypher = format!(
        "CALL uni.algo.shortestPath('{}', '{}', ['LINK']) YIELD length RETURN length",
        vid_a, vid_c
    );
    let query_ast = uni_query::parse_cypher(&cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("length"), Some(&unival!(2)));

    Ok(())
}

#[tokio::test]
async fn test_louvain_procedure() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
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

    // 2. Insert Data (Two cliques connected by one edge)
    // Clique 1: 0, 1, 2
    // Clique 2: 3, 4, 5
    // Connection: 2 -> 3
    let vids: Vec<Vid> = (0..6).map(Vid::new).collect();

    {
        let mut w = writer.write().await;
        for &vid in &vids {
            w.insert_vertex_with_labels(vid, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }

        // Clique 1
        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[0], vids[1], link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[1], vids[2], link_edge, eid2, HashMap::new())
            .await?;
        let eid3 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[2], vids[0], link_edge, eid3, HashMap::new())
            .await?;

        // Clique 2
        let eid4 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[3], vids[4], link_edge, eid4, HashMap::new())
            .await?;
        let eid5 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[4], vids[5], link_edge, eid5, HashMap::new())
            .await?;
        let eid6 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[5], vids[3], link_edge, eid6, HashMap::new())
            .await?;

        // Connection
        let eid7 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[2], vids[3], link_edge, eid7, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 3. Run Louvain
    let cypher = "CALL uni.algo.louvain(['Node'], ['LINK']) YIELD nodeId, communityId RETURN nodeId, communityId";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 6);

    let mut communities = HashMap::new();
    for row in results {
        let node_id = row.get("nodeId").unwrap().as_u64().unwrap();
        let community_id = row.get("communityId").unwrap().as_u64().unwrap();
        communities.insert(node_id, community_id);
    }

    // Nodes in same clique should likely have same community
    assert_eq!(
        communities.get(&vids[0].as_u64()),
        communities.get(&vids[1].as_u64())
    );
    assert_eq!(
        communities.get(&vids[3].as_u64()),
        communities.get(&vids[4].as_u64())
    );

    Ok(())
}

#[tokio::test]
async fn test_scc_procedure() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
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

    // 2. Insert Data (Cycle: A -> B, B -> C, C -> A, and D -> A)
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let vid_c = Vid::new(2);
    let vid_d = Vid::new(3);

    {
        let mut w = writer.write().await;
        for &vid in &[vid_a, vid_b, vid_c, vid_d] {
            w.insert_vertex_with_labels(vid, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }

        // Cycle {A, B, C}
        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(vid_a, vid_b, link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(vid_b, vid_c, link_edge, eid2, HashMap::new())
            .await?;
        let eid3 = w.next_eid(link_edge).await?;
        w.insert_edge(vid_c, vid_a, link_edge, eid3, HashMap::new())
            .await?;

        // D -> A (not in cycle)
        let eid4 = w.next_eid(link_edge).await?;
        w.insert_edge(vid_d, vid_a, link_edge, eid4, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 3. Run SCC
    let cypher = "CALL uni.algo.scc(['Node'], ['LINK']) YIELD nodeId, componentId RETURN nodeId, componentId";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 4);

    let mut components = HashMap::new();
    for row in results {
        let node_id = row.get("nodeId").unwrap().as_u64().unwrap();
        let component_id = row.get("componentId").unwrap().as_u64().unwrap();
        components.insert(node_id, component_id);
    }

    // A, B, C should be in the same component
    assert_eq!(
        components.get(&vid_a.as_u64()),
        components.get(&vid_b.as_u64())
    );
    assert_eq!(
        components.get(&vid_a.as_u64()),
        components.get(&vid_c.as_u64())
    );

    // D should be in a different component
    assert_ne!(
        components.get(&vid_a.as_u64()),
        components.get(&vid_d.as_u64())
    );

    Ok(())
}

#[tokio::test]
async fn test_label_propagation_procedure() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
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

    // 2. Insert Data (Two cliques connected by one edge)
    // Clique 1: 0, 1, 2
    // Clique 2: 3, 4, 5
    // Connection: 2 -> 3
    let vids: Vec<Vid> = (0..6).map(Vid::new).collect();

    {
        let mut w = writer.write().await;
        for &vid in &vids {
            w.insert_vertex_with_labels(vid, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }

        // Clique 1 (Triangle)
        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[0], vids[1], link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[1], vids[2], link_edge, eid2, HashMap::new())
            .await?;
        let eid3 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[2], vids[0], link_edge, eid3, HashMap::new())
            .await?;

        // Clique 2 (Triangle)
        let eid4 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[3], vids[4], link_edge, eid4, HashMap::new())
            .await?;
        let eid5 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[4], vids[5], link_edge, eid5, HashMap::new())
            .await?;
        let eid6 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[5], vids[3], link_edge, eid6, HashMap::new())
            .await?;

        // Connection
        let eid7 = w.next_eid(link_edge).await?;
        w.insert_edge(vids[2], vids[3], link_edge, eid7, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 3. Run Label Propagation
    let cypher = "CALL uni.algo.labelPropagation(['Node'], ['LINK']) YIELD nodeId, communityId RETURN nodeId, communityId";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 6);

    let mut communities = HashMap::new();
    for row in results {
        let node_id = row.get("nodeId").unwrap().as_u64().unwrap();
        let community_id = row.get("communityId").unwrap().as_u64().unwrap();
        communities.insert(node_id, community_id);
    }

    // Nodes in same clique should have same community
    assert_eq!(
        communities.get(&vids[0].as_u64()),
        communities.get(&vids[1].as_u64())
    );
    assert_eq!(
        communities.get(&vids[3].as_u64()),
        communities.get(&vids[4].as_u64())
    );

    Ok(())
}

#[tokio::test]
async fn test_betweenness_procedure() -> anyhow::Result<()> {
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

    // Star Graph: Center 0 connected to 1, 2, 3, 4
    let center = Vid::new(0);
    let leaves: Vec<Vid> = (1..5).map(Vid::new).collect();

    {
        let mut w = writer.write().await;
        w.insert_vertex_with_labels(center, HashMap::new(), vec!["Node".to_string()])
            .await?;
        for &leaf in &leaves {
            w.insert_vertex_with_labels(leaf, HashMap::new(), vec!["Node".to_string()])
                .await?;
            let eid = w.next_eid(link_edge).await?;
            w.insert_edge(center, leaf, link_edge, eid, HashMap::new())
                .await?;
        }
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher =
        "CALL uni.algo.betweenness(['Node'], ['LINK']) YIELD nodeId, score RETURN nodeId, score";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 5);
    for row in results {
        let score = row.get("score").unwrap().as_f64().unwrap();
        assert_eq!(score, 0.0);
    }

    Ok(())
}

#[tokio::test]
async fn test_node_similarity_procedure() -> anyhow::Result<()> {
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

    // Nodes 0 and 1 share neighbor 2.
    // 0 -> 2, 1 -> 2.
    // Similarity(0, 1) should be > 0.
    let v0 = Vid::new(0);
    let v1 = Vid::new(1);
    let v2 = Vid::new(2);

    {
        let mut w = writer.write().await;
        w.insert_vertex_with_labels(v0, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(v1, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(v2, HashMap::new(), vec!["Node".to_string()])
            .await?;

        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(v0, v2, link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(v1, v2, link_edge, eid2, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher = "CALL uni.algo.nodeSimilarity(['Node'], ['LINK']) YIELD node1, node2, similarity RETURN node1, node2, similarity";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // Should find pair (0, 1) or (1, 0)
    assert!(!results.is_empty());
    let row = &results[0];
    let sim = row.get("similarity").unwrap().as_f64().unwrap();
    assert!(sim > 0.0);

    Ok(())
}

#[tokio::test]
async fn test_closeness_procedure() -> anyhow::Result<()> {
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

    // Line: 0 -> 1 -> 2
    let v0 = Vid::new(0);
    let v1 = Vid::new(1);
    let v2 = Vid::new(2);

    {
        let mut w = writer.write().await;
        w.insert_vertex_with_labels(v0, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(v1, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(v2, HashMap::new(), vec!["Node".to_string()])
            .await?;

        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(v0, v1, link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(v1, v2, link_edge, eid2, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher =
        "CALL uni.algo.closeness(['Node'], ['LINK']) YIELD nodeId, score RETURN nodeId, score";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 3);
    let mut scores = HashMap::new();
    for row in results {
        let id = row.get("nodeId").unwrap().as_u64().unwrap();
        let s = row.get("score").unwrap().as_f64().unwrap();
        scores.insert(id, s);
    }

    assert!((scores[&v0.as_u64()] - 0.666).abs() < 0.01);
    assert!((scores[&v1.as_u64()] - 2.0).abs() < 0.01);
    assert_eq!(scores[&v2.as_u64()], 0.0);

    Ok(())
}

#[tokio::test]
async fn test_triangle_count_procedure() -> anyhow::Result<()> {
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

    // Triangle: 0-1, 1-2, 2-0
    let v0 = Vid::new(0);
    let v1 = Vid::new(1);
    let v2 = Vid::new(2);

    {
        let mut w = writer.write().await;
        w.insert_vertex_with_labels(v0, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(v1, HashMap::new(), vec!["Node".to_string()])
            .await?;
        w.insert_vertex_with_labels(v2, HashMap::new(), vec!["Node".to_string()])
            .await?;

        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(v0, v1, link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(v1, v2, link_edge, eid2, HashMap::new())
            .await?;
        let eid3 = w.next_eid(link_edge).await?;
        w.insert_edge(v2, v0, link_edge, eid3, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher = "CALL uni.algo.triangleCount(['Node'], ['LINK']) YIELD nodeId, triangleCount RETURN nodeId, triangleCount";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    for row in results {
        let count = row.get("triangleCount").unwrap().as_u64().unwrap();
        assert_eq!(count, 1);
    }

    Ok(())
}

#[tokio::test]
async fn test_kcore_procedure() -> anyhow::Result<()> {
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

    // Graph: 0-1, 1-2, 2-0 (Triangle, k-core 2), and 3 connected to 0 (k-core 1)
    let v0 = Vid::new(0);
    let v1 = Vid::new(1);
    let v2 = Vid::new(2);
    let v3 = Vid::new(3);

    {
        let mut w = writer.write().await;
        for &v in &[v0, v1, v2, v3] {
            w.insert_vertex_with_labels(v, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }

        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(v0, v1, link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(v1, v2, link_edge, eid2, HashMap::new())
            .await?;
        let eid3 = w.next_eid(link_edge).await?;
        w.insert_edge(v2, v0, link_edge, eid3, HashMap::new())
            .await?;
        let eid4 = w.next_eid(link_edge).await?;
        w.insert_edge(v3, v0, link_edge, eid4, HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher = "CALL uni.algo.kCore(['Node'], ['LINK']) YIELD nodeId, coreNumber RETURN nodeId, coreNumber";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    let mut cores = HashMap::new();
    for row in results {
        let id = row.get("nodeId").unwrap().as_u64().unwrap();
        let c = row.get("coreNumber").unwrap().as_u64().unwrap();
        cores.insert(id, c);
    }

    assert_eq!(cores[&v0.as_u64()], 2);
    assert_eq!(cores[&v1.as_u64()], 2);
    assert_eq!(cores[&v2.as_u64()], 2);
    assert_eq!(cores[&v3.as_u64()], 1);

    Ok(())
}

#[tokio::test]
async fn test_random_walk_procedure() -> anyhow::Result<()> {
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

    // 0 -> 1 -> 2
    let v0 = Vid::new(0);
    let v1 = Vid::new(1);
    let v2 = Vid::new(2);

    {
        let mut w = writer.write().await;
        for &v in &[v0, v1, v2] {
            w.insert_vertex_with_labels(v, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }
        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(v0, v1, link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(v1, v2, link_edge, eid2, HashMap::new())
            .await?;
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher = "CALL uni.algo.randomWalk(['Node'], ['LINK'], 2, 1) YIELD path RETURN path";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 3);

    for row in results {
        let path = row.get("path").unwrap().as_array().unwrap();
        assert!(!path.is_empty());
        assert!(path.len() <= 3);
    }

    Ok(())
}

#[tokio::test]
async fn test_apsp_procedure() -> anyhow::Result<()> {
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

    // 0 -> 1 -> 2
    let v0 = Vid::new(0);
    let v1 = Vid::new(1);
    let v2 = Vid::new(2);

    {
        let mut w = writer.write().await;
        for &v in &[v0, v1, v2] {
            w.insert_vertex_with_labels(v, HashMap::new(), vec!["Node".to_string()])
                .await?;
        }
        let eid1 = w.next_eid(link_edge).await?;
        w.insert_edge(v0, v1, link_edge, eid1, HashMap::new())
            .await?;
        let eid2 = w.next_eid(link_edge).await?;
        w.insert_edge(v1, v2, link_edge, eid2, HashMap::new())
            .await?;
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    let cypher = "CALL uni.algo.allPairsShortestPath(['Node'], ['LINK']) YIELD sourceNodeId, targetNodeId, distance RETURN sourceNodeId, targetNodeId, distance";
    let query_ast = uni_query::parse_cypher(cypher)?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // Pairs: (0,1,1), (0,2,2), (1,2,1). Total 3.
    assert_eq!(results.len(), 3);

    Ok(())
}
