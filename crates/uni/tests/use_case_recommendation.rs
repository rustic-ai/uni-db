// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use uni_db::Uni;
use uni_db::Value;

#[tokio::test]
async fn test_recommendation_use_case() -> anyhow::Result<()> {
    // 1. Setup
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().to_str().unwrap();
    let schema_path = temp_dir.path().join("schema.json");

    let schema_json = r#"{
      "schema_version": 1,
      "labels": {
        "User": { "id": 1, "created_at": "2024-01-01T00:00:00Z", "state": "Active" },
        "Product": { "id": 2, "created_at": "2024-01-01T00:00:00Z", "state": "Active" },
        "Category": { "id": 3, "created_at": "2024-01-01T00:00:00Z", "state": "Active" }
      },
      "edge_types": {
        "VIEWED": { "id": 1, "src_labels": ["User"], "dst_labels": ["Product"], "state": "Active" },
        "PURCHASED": { "id": 2, "src_labels": ["User"], "dst_labels": ["Product"], "state": "Active" },
        "IN_CATEGORY": { "id": 3, "src_labels": ["Product"], "dst_labels": ["Category"], "state": "Active" }
      },
      "properties": {
        "Product": {
          "name": { "type": "String", "nullable": false, "added_in": 1, "state": "Active" },
          "price": { "type": "Float64", "nullable": false, "added_in": 1, "state": "Active" },
          "embedding": { "type": { "Vector": { "dimensions": 4 } }, "nullable": false, "added_in": 1, "state": "Active" }
        }
      },
      "indexes": [
        {
          "type": "Vector",
          "name": "product_embedding",
          "label": "Product",
          "property": "embedding",
          "index_type": { "IvfPq": { "num_partitions": 10, "num_sub_vectors": 2, "bits_per_subvector": 8 } },
          "metric": "Cosine",
          "embedding_config": null
        }
      ]
    }"#;
    std::fs::write(&schema_path, schema_json)?;

    let db = Uni::open(path).schema_file(&schema_path).build().await?;

    // 2. Data Ingestion
    // Products: P1 (Shoes, [1,0,0,0]), P2 (Socks, [0.9,0.1,0,0])
    let p1_vec: Vec<f32> = vec![1.0, 0.0, 0.0, 0.0];
    let p2_vec: Vec<f32> = vec![0.9, 0.1, 0.0, 0.0];

    let products = vec![
        HashMap::from([
            (
                "name".to_string(),
                Value::String("Running Shoes".to_string()),
            ),
            ("price".to_string(), Value::Float(100.0)),
            ("embedding".to_string(), Value::Vector(p1_vec.clone())),
        ]),
        HashMap::from([
            ("name".to_string(), Value::String("Socks".to_string())),
            ("price".to_string(), Value::Float(10.0)),
            ("embedding".to_string(), Value::Vector(p2_vec.clone())),
        ]),
    ];
    let product_vids = db.bulk_insert_vertices("Product", products).await?;
    let p1 = product_vids[0];
    let _p2 = product_vids[1];

    // Users: U1, U2, U3
    let users = vec![HashMap::new(), HashMap::new(), HashMap::new()];
    let user_vids = db.bulk_insert_vertices("User", users).await?;
    let u1 = user_vids[0];
    let u2 = user_vids[1];
    let u3 = user_vids[2];

    // Edges: U1 bought P1, U2 bought P1, U3 bought P1 (Popular item)
    let purchased = vec![
        (u1, p1, HashMap::new()),
        (u2, p1, HashMap::new()),
        (u3, p1, HashMap::new()),
    ];
    db.bulk_insert_edges("PURCHASED", purchased).await?;

    db.flush().await?;

    // 3. Hybrid Query (Simulated via 2 queries due to Parser limitation)

    // A. Vector Search
    let search_vec: Vec<f32> = vec![1.0, 0.0, 0.0, 0.0];
    let vec_query = "CALL uni.vector.query('Product', 'embedding', $vec, 10) YIELD node, distance RETURN node._vid AS vid, distance";
    let vec_result = db
        .query_with(vec_query)
        .param("vec", Value::Vector(search_vec))
        .fetch_all()
        .await?;

    // Verify we got results
    assert!(!vec_result.rows.is_empty());

    // Assume P1 is top result (dist 0)
    let p1_node_vid_str: String = String::try_from(&vec_result.rows[0].values[0]).unwrap();
    // Verify it matches P1
    assert_eq!(p1_node_vid_str, p1.to_string());

    println!("P1 VID: {}", p1.as_u64());

    // Debug Graph Query
    let debug_graph = "MATCH (u:User)-[:PURCHASED]->(p:Product) RETURN u._vid, p._vid";
    let debug_res = db.query_with(debug_graph).fetch_all().await?;
    println!("Debug Graph: {} rows", debug_res.rows.len());
    for row in &debug_res.rows {
        println!("  Row: {:?}", row.values);
    }

    // B. Graph Scoring (Collaborative)
    // MATCH (other_user:User)-[:PURCHASED]->(product) WHERE product._vid = $pid RETURN COUNT(other_user)
    let graph_query = "MATCH (other_user:User)-[:PURCHASED]->(product) WHERE product._vid = $pid RETURN COUNT(other_user) as count";
    let graph_result = db
        .query_with(graph_query)
        .param("pid", Value::Int(p1.as_u64() as i64))
        .fetch_all()
        .await?;

    assert_eq!(graph_result.rows.len(), 1);
    let count: i64 = i64::try_from(&graph_result.rows[0].values[0]).unwrap();
    assert_eq!(count, 3);

    Ok(())
}
