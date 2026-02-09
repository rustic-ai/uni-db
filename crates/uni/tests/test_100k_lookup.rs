// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Test for 100k property lookup bug

use std::collections::HashMap;
use std::time::Instant;
use uni_common::core::id::Vid;
use uni_db::unival;

const SCHEMA_JSON: &str = r#"{
    "schema_version": 1,
    "labels": {
        "Person": {
            "id": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "state": "Active"
        }
    },
    "edge_types": {
        "KNOWS": {
            "id": 1,
            "src_labels": ["Person"],
            "dst_labels": ["Person"],
            "state": "Active"
        }
    },
    "properties": {
        "Person": {
            "name": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" },
            "age": { "type": "Int32", "nullable": true, "added_in": 1, "state": "Active" },
            "embedding": { "type": { "Vector": { "dimensions": 128 } }, "nullable": true, "added_in": 1, "state": "Active" }
        }
    },
    "indexes": []
}"#;

#[tokio::test]
#[ignore = "Performance optimization for large datasets pending"]
async fn test_100k_property_lookup() {
    let test_start = Instant::now();
    eprintln!("\n=== TEST START ===\n");

    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    let schema_path = path.join("schema.json");
    tokio::fs::write(&schema_path, SCHEMA_JSON).await.unwrap();

    let db_open_start = Instant::now();
    let db = uni_db::Uni::open(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    eprintln!("DB open: {:?}", db_open_start.elapsed());

    let schema_load_start = Instant::now();
    db.load_schema(&schema_path).await.unwrap();
    eprintln!("Schema load: {:?}\n", schema_load_start.elapsed());

    // Create 10k vertices to test (smaller scale for faster debugging)
    let num_vertices = 10_000;
    let batch_size = 5_000;

    eprintln!(
        "Creating {} vertices in batches of {}",
        num_vertices, batch_size
    );

    let vertex_creation_start = Instant::now();
    let mut all_vids: Vec<Vid> = Vec::new();

    for batch_start in (0..num_vertices).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_vertices);

        let prep_start = Instant::now();
        let mut props = Vec::new();
        for i in batch_start..batch_end {
            let embedding: Vec<f32> = (0..128).map(|x| (x + i) as f32).collect();
            let mut p: HashMap<String, uni_db::Value> = HashMap::new();
            p.insert("name".to_string(), unival!(format!("Person_{}", i)));
            p.insert("age".to_string(), unival!((i % 100) as i32));
            p.insert("embedding".to_string(), unival!(embedding));
            props.push(p);
        }
        let prep_time = prep_start.elapsed();

        let insert_start = Instant::now();
        let vids = db.bulk_insert_vertices("Person", props).await.unwrap();
        let insert_time = insert_start.elapsed();

        all_vids.extend(vids);
        eprintln!(
            "  Batch {} - {}: prep={:?}, insert={:?}",
            batch_start, batch_end, prep_time, insert_time
        );
    }

    eprintln!(
        "Created {} vertices total in {:?}\n",
        all_vids.len(),
        vertex_creation_start.elapsed()
    );

    // Check properties BEFORE creating edges
    let query_start = Instant::now();
    let pre_edge_sample = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 3")
        .await
        .unwrap();
    eprintln!(
        "BEFORE EDGES - Sample names: {:?} (query time: {:?})",
        pre_edge_sample.rows,
        query_start.elapsed()
    );

    eprintln!("\nNow creating edges...");

    // Create 30k edges (same ratio as benchmark: 3 edges per vertex)
    let num_edges = 30_000usize;
    let edge_batch_size = 10_000usize;

    let edge_creation_start = Instant::now();
    for batch_start in (0..num_edges).step_by(edge_batch_size) {
        let batch_end = (batch_start + edge_batch_size).min(num_edges);

        let edge_prep_start = Instant::now();
        let edges: Vec<(Vid, Vid, HashMap<String, uni_db::Value>)> = (batch_start..batch_end)
            .map(|i| {
                let src = all_vids[i % all_vids.len()];
                let dst = all_vids[(i * 7 + 13) % all_vids.len()]; // pseudo-random dest
                (src, dst, HashMap::new())
            })
            .collect();
        let edge_prep_time = edge_prep_start.elapsed();

        let edge_insert_start = Instant::now();
        db.bulk_insert_edges("KNOWS", edges).await.unwrap();
        let edge_insert_time = edge_insert_start.elapsed();

        eprintln!(
            "  Edge batch {} - {}: prep={:?}, insert={:?}",
            batch_start, batch_end, edge_prep_time, edge_insert_time
        );
    }
    eprintln!(
        "Created {} edges total in {:?}\n",
        num_edges,
        edge_creation_start.elapsed()
    );

    // Check properties AFTER edges, BEFORE flush
    let query_start = Instant::now();
    let post_edge_sample = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 3")
        .await
        .unwrap();
    eprintln!(
        "AFTER EDGES, BEFORE FLUSH - Sample names: {:?} (query time: {:?})",
        post_edge_sample.rows,
        query_start.elapsed()
    );

    let flush_start = Instant::now();
    eprintln!("\nFlushing...");
    db.flush().await.unwrap();
    eprintln!("Flush completed in {:?}\n", flush_start.elapsed());

    // Check properties AFTER flush
    let query_start = Instant::now();
    let post_flush_sample = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 3")
        .await
        .unwrap();
    eprintln!(
        "AFTER FLUSH - Sample names: {:?} (query time: {:?})",
        post_flush_sample.rows,
        query_start.elapsed()
    );

    eprintln!("\n=== DIAGNOSTIC QUERIES ===\n");

    // Test count
    let query_start = Instant::now();
    let count = db
        .query("MATCH (n:Person) RETURN count(n) as cnt")
        .await
        .unwrap();
    eprintln!(
        "Count query: {:?} (time: {:?})",
        count.rows.first(),
        query_start.elapsed()
    );

    // Diagnostic: Check if properties are accessible at all
    let query_start = Instant::now();
    let sample = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 5")
        .await
        .unwrap();
    eprintln!(
        "Sample names (LIMIT 5): {:?} (time: {:?})",
        sample.rows,
        query_start.elapsed()
    );

    // Check if Person_0 exists without filter
    let query_start = Instant::now();
    let all_names = db
        .query("MATCH (n:Person) WHERE n.name IS NOT NULL RETURN n.name")
        .await
        .unwrap();
    eprintln!(
        "Total rows with name: {} (time: {:?})",
        all_names.rows.len(),
        query_start.elapsed()
    );

    // Check if we can find Person_0 in the returned names
    let has_person_0 = all_names.rows.iter().any(|row| {
        row.values
            .iter()
            .any(|v| matches!(v, uni_db::Value::String(s) if s == "Person_0"))
    });
    eprintln!("Person_0 exists in full scan: {}", has_person_0);

    // Try a simple equality filter without the edge pattern
    let query_start = Instant::now();
    let simple_lookup = db
        .query("MATCH (n:Person) WHERE n.name = 'Person_0' RETURN n.name")
        .await
        .unwrap();
    eprintln!(
        "Simple lookup Person_0: {} rows (time: {:?})",
        simple_lookup.rows.len(),
        query_start.elapsed()
    );

    eprintln!("\n=== TARGETED LOOKUPS ===\n");

    // Test lookups at different positions
    let lookup_tests_start = Instant::now();
    for target in [0, 100, 1000, 5000, 9999] {
        let query = format!(
            "MATCH (n:Person) WHERE n.name = 'Person_{}' RETURN n.name",
            target
        );
        let query_start = Instant::now();
        let result = db.query(&query).await.unwrap();
        let query_time = query_start.elapsed();
        eprintln!(
            "Lookup Person_{}: {} rows (time: {:?})",
            target,
            result.rows.len(),
            query_time
        );
        assert_eq!(result.rows.len(), 1, "Expected 1 row for Person_{}", target);
    }
    eprintln!(
        "\nAll lookup tests completed in {:?}",
        lookup_tests_start.elapsed()
    );

    eprintln!("\n=== TEST SUMMARY ===");
    eprintln!("Total test time: {:?}", test_start.elapsed());
}
