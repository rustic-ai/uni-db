// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Test to isolate COUNT performance issue

use std::collections::HashMap;
use std::time::Instant;
use uni_db::Value;

const SCHEMA_JSON: &str = r#"{
    "schema_version": 1,
    "labels": {
        "Person": {
            "id": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "state": "Active"
        }
    },
    "edge_types": {},
    "properties": {
        "Person": {
            "name": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" }
        }
    },
    "indexes": []
}"#;

#[tokio::test]
#[ignore = "slow performance test — run with --run-ignored or --ignored"]
async fn test_count_scaling() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    let schema_path = path.join("schema.json");
    tokio::fs::write(&schema_path, SCHEMA_JSON).await.unwrap();

    let db = uni_db::Uni::open(path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    db.load_schema(&schema_path).await.unwrap();

    eprintln!("\n=== COUNT PERFORMANCE TEST ===\n");

    // Test COUNT at different data scales
    for scale in [100, 500, 1000, 2000, 5000] {
        eprintln!("Testing with {} vertices:", scale);

        // Create vertices
        let create_start = Instant::now();
        let mut props = Vec::new();
        for i in 0..scale {
            let mut p: HashMap<String, Value> = HashMap::new();
            p.insert("name".to_string(), Value::String(format!("Person_{}", i)));
            props.push(p);
        }
        db.bulk_insert_vertices("Person", props).await.unwrap();
        eprintln!("  Create: {:?}", create_start.elapsed());

        // Test COUNT query
        let count_start = Instant::now();
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            db.query("MATCH (n:Person) RETURN count(n) as cnt"),
        )
        .await
        {
            Ok(Ok(result)) => {
                eprintln!(
                    "  COUNT: {:?} - result: {:?}",
                    count_start.elapsed(),
                    result.rows.first()
                );
            }
            Ok(Err(e)) => {
                eprintln!("  COUNT: ERROR - {}", e);
            }
            Err(_) => {
                eprintln!("  COUNT: TIMEOUT (>5s)");
            }
        }

        // Test simple scan with LIMIT
        let scan_start = Instant::now();
        let scan_result = db
            .query("MATCH (n:Person) RETURN n.name LIMIT 3")
            .await
            .unwrap();
        eprintln!(
            "  Scan LIMIT 3: {:?} - {} rows",
            scan_start.elapsed(),
            scan_result.rows.len()
        );

        // Clear for next test
        db.query("MATCH (n:Person) DETACH DELETE n").await.unwrap();
        eprintln!();
    }
}

#[tokio::test]
async fn test_count_vs_scan_all() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    let schema_path = path.join("schema.json");
    tokio::fs::write(&schema_path, SCHEMA_JSON).await.unwrap();

    let db = uni_db::Uni::open(path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    db.load_schema(&schema_path).await.unwrap();

    eprintln!("\n=== COUNT vs SCAN ALL ===\n");

    let num_vertices = 1000;

    // Create vertices
    let mut props = Vec::new();
    for i in 0..num_vertices {
        let mut p: HashMap<String, Value> = HashMap::new();
        p.insert("name".to_string(), Value::String(format!("Person_{}", i)));
        props.push(p);
    }
    db.bulk_insert_vertices("Person", props).await.unwrap();
    eprintln!("Created {} vertices\n", num_vertices);

    // Test COUNT
    let count_start = Instant::now();
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        db.query("MATCH (n:Person) RETURN count(n) as cnt"),
    )
    .await
    {
        Ok(Ok(result)) => {
            eprintln!("COUNT time: {:?}", count_start.elapsed());
            eprintln!("COUNT result: {:?}", result.rows.first());
        }
        Ok(Err(e)) => {
            eprintln!("COUNT ERROR: {}", e);
        }
        Err(_) => {
            eprintln!("COUNT TIMEOUT (>5s)");
        }
    }

    // Test full scan (to compare)
    let scan_start = Instant::now();
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        db.query("MATCH (n:Person) RETURN n.name"),
    )
    .await
    {
        Ok(Ok(result)) => {
            eprintln!("\nFull scan time: {:?}", scan_start.elapsed());
            eprintln!("Full scan rows: {}", result.rows.len());
        }
        Ok(Err(e)) => {
            eprintln!("\nFull scan ERROR: {}", e);
        }
        Err(_) => {
            eprintln!("\nFull scan TIMEOUT (>5s)");
        }
    }

    // Test scan without property projection
    let scan_no_prop_start = Instant::now();
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        db.query("MATCH (n:Person) RETURN n"),
    )
    .await
    {
        Ok(Ok(result)) => {
            eprintln!("\nScan (RETURN n) time: {:?}", scan_no_prop_start.elapsed());
            eprintln!("Scan (RETURN n) rows: {}", result.rows.len());
        }
        Ok(Err(e)) => {
            eprintln!("\nScan (RETURN n) ERROR: {}", e);
        }
        Err(_) => {
            eprintln!("\nScan (RETURN n) TIMEOUT (>5s)");
        }
    }
}
