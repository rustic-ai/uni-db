// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Test to demonstrate bulk insert performance improvement

use std::collections::HashMap;
use std::time::Instant;
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
    "edge_types": {},
    "properties": {
        "Person": {
            "name": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" },
            "email": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" }
        }
    },
    "constraints": [],
    "indexes": []
}"#;

#[tokio::test]
#[ignore = "slow performance test — run with --run-ignored or --ignored"]
async fn test_bulk_insert_performance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    let schema_path = path.join("schema.json");
    tokio::fs::write(&schema_path, SCHEMA_JSON).await.unwrap();

    let db = uni_db::Uni::open(path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    db.load_schema(&schema_path).await.unwrap();

    eprintln!("\n=== BULK INSERT PERFORMANCE TEST ===\n");

    // Test at different scales
    for scale in [100, 500, 1000, 2000, 5000] {
        eprintln!("Testing bulk insert with {} vertices:", scale);

        let create_start = Instant::now();
        let mut props = Vec::new();
        for i in 0..scale {
            let mut p: HashMap<String, uni_db::Value> = HashMap::new();
            p.insert("name".to_string(), unival!(format!("Person_{}", i)));
            p.insert(
                "email".to_string(),
                unival!(format!("person_{}@example.com", i)),
            );
            props.push(p);
        }

        db.bulk_insert_vertices("Person", props).await.unwrap();
        let insert_elapsed = create_start.elapsed();

        let flush_start = Instant::now();
        db.flush().await.unwrap();
        let flush_elapsed = flush_start.elapsed();

        let total = insert_elapsed + flush_elapsed;
        eprintln!("  Insert: {:?}", insert_elapsed);
        eprintln!("  Flush:  {:?}", flush_elapsed);
        eprintln!("  Total:  {:?}", total);
        eprintln!(
            "  Per vertex (insert): {:.2}ms",
            insert_elapsed.as_millis() as f64 / scale as f64
        );
        eprintln!(
            "  Per vertex (total):  {:.2}ms",
            total.as_millis() as f64 / scale as f64
        );
        eprintln!();

        // Clean up for next test and flush to reset L0
        db.query("MATCH (n:Person) DETACH DELETE n").await.unwrap();
        db.flush().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "slow performance test — run with --run-ignored or --ignored"]
async fn test_bulk_insert_with_constraints() {
    let schema_with_constraints = r#"{
        "schema_version": 1,
        "labels": {
            "User": {
                "id": 1,
                "created_at": "2024-01-01T00:00:00Z",
                "state": "Active"
            }
        },
        "edge_types": {},
        "properties": {
            "User": {
                "email": { "type": "String", "nullable": false, "added_in": 1, "state": "Active" },
                "age": { "type": "Int64", "nullable": true, "added_in": 1, "state": "Active" }
            }
        },
        "constraints": [
            {
                "name": "unique_email",
                "target": { "Label": "User" },
                "constraint_type": { "Unique": { "properties": ["email"] } },
                "enabled": true,
                "created_at": "2024-01-01T00:00:00Z"
            }
        ],
        "indexes": []
    }"#;

    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    let schema_path = path.join("schema.json");
    tokio::fs::write(&schema_path, schema_with_constraints)
        .await
        .unwrap();

    let db = uni_db::Uni::open(path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    db.load_schema(&schema_path).await.unwrap();

    eprintln!("\n=== BULK INSERT WITH UNIQUE CONSTRAINT ===\n");

    for scale in [100, 500, 1000, 2000] {
        eprintln!(
            "Testing bulk insert with {} vertices (with unique constraint):",
            scale
        );

        let create_start = Instant::now();
        let mut props = Vec::new();
        for i in 0..scale {
            let mut p: HashMap<String, uni_db::Value> = HashMap::new();
            p.insert(
                "email".to_string(),
                unival!(format!("user_{}@example.com", i)),
            );
            p.insert("age".to_string(), unival!(25 + (i % 50)));
            props.push(p);
        }

        db.bulk_insert_vertices("User", props).await.unwrap();
        let insert_elapsed = create_start.elapsed();

        let flush_start = Instant::now();
        db.flush().await.unwrap();
        let flush_elapsed = flush_start.elapsed();

        let total = insert_elapsed + flush_elapsed;
        eprintln!("  Insert: {:?}", insert_elapsed);
        eprintln!("  Flush:  {:?}", flush_elapsed);
        eprintln!("  Total:  {:?}", total);
        eprintln!(
            "  Per vertex (insert): {:.2}ms",
            insert_elapsed.as_millis() as f64 / scale as f64
        );
        eprintln!(
            "  Per vertex (total):  {:.2}ms",
            total.as_millis() as f64 / scale as f64
        );
        eprintln!();

        // Verify all vertices were created
        let result = db
            .query("MATCH (n:User) RETURN count(n) as cnt")
            .await
            .unwrap();
        if let uni_db::Value::Int(count) = &result.rows[0].values[0] {
            assert_eq!(*count, scale as i64);
        } else {
            panic!("Expected Int value");
        }

        // Clean up for next test and flush to reset L0
        db.query("MATCH (n:User) DETACH DELETE n").await.unwrap();
        db.flush().await.unwrap();
    }

    eprintln!("✓ All constraint checks passed!");
}
