// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use uni_db::Uni;
use uni_db::Value;

#[tokio::test]
async fn test_fraud_detection_use_case() -> anyhow::Result<()> {
    // 1. Setup
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().to_str().unwrap();
    let schema_path = temp_dir.path().join("schema.json");

    let schema_json = r#"{
      "schema_version": 1,
      "labels": {
        "User": { "id": 1, "created_at": "2024-01-01T00:00:00Z", "state": "Active" },
        "Device": { "id": 2, "created_at": "2024-01-01T00:00:00Z", "state": "Active" },
        "IP": { "id": 3, "created_at": "2024-01-01T00:00:00Z", "state": "Active" }
      },
      "edge_types": {
        "SENT_MONEY": { "id": 1, "src_labels": ["User"], "dst_labels": ["User"], "state": "Active" },
        "USED_DEVICE": { "id": 2, "src_labels": ["User"], "dst_labels": ["Device"], "state": "Active" },
        "USED_IP": { "id": 3, "src_labels": ["User"], "dst_labels": ["IP"], "state": "Active" }
      },
      "properties": {
        "SENT_MONEY": {
          "amount": { "type": "Float64", "nullable": false, "added_in": 1, "state": "Active" },
          "ts": { "type": "Int64", "nullable": false, "added_in": 1, "state": "Active" }
        },
        "User": {
          "risk_score": { "type": "Float32", "nullable": true, "added_in": 1, "state": "Active" }
        }
      },
      "indexes": []
    }"#;
    std::fs::write(&schema_path, schema_json)?;

    let db = Uni::open(path).schema_file(&schema_path).build().await?;

    // 2. Data Ingestion
    // Labels: User=1, Device=2, IP=3
    // Edge Types: SENT_MONEY=1, USED_DEVICE=2, USED_IP=3

    // Insert Users: A, B, C, D (fraudster)
    let users = vec![
        HashMap::from([("risk_score".to_string(), Value::Float(0.1))]), // User A
        HashMap::from([("risk_score".to_string(), Value::Float(0.2))]), // User B
        HashMap::from([("risk_score".to_string(), Value::Float(0.3))]), // User C
        HashMap::from([("risk_score".to_string(), Value::Float(0.9))]), // User D (Fraudster)
    ];
    let user_vids = db.bulk_insert_vertices("User", users).await?;
    let user_a = user_vids[0];
    let user_b = user_vids[1];
    let user_c = user_vids[2];
    let user_d = user_vids[3];

    // Insert Device: D1
    let devices = vec![HashMap::new()];
    let device_vids = db.bulk_insert_vertices("Device", devices).await?;
    let device_d1 = device_vids[0];

    // Insert Edges: Payment Ring A -> B -> C -> A
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let edges_sent_money = vec![
        (
            user_a,
            user_b,
            HashMap::from([
                ("amount".to_string(), Value::Float(5000.0)),
                ("ts".to_string(), Value::Int(current_time)),
            ]),
        ),
        (
            user_b,
            user_c,
            HashMap::from([
                ("amount".to_string(), Value::Float(5000.0)),
                ("ts".to_string(), Value::Int(current_time + 10)),
            ]),
        ),
        (
            user_c,
            user_a,
            HashMap::from([
                ("amount".to_string(), Value::Float(5000.0)),
                ("ts".to_string(), Value::Int(current_time + 20)),
            ]),
        ),
    ];
    db.bulk_insert_edges("SENT_MONEY", edges_sent_money).await?;

    // Insert Edges: Shared Device A -> D1, B -> D1, D -> D1
    let edges_used_device = vec![
        (user_a, device_d1, HashMap::new()),
        (user_d, device_d1, HashMap::new()), // User A shares device with Fraudster D
    ];
    db.bulk_insert_edges("USED_DEVICE", edges_used_device)
        .await?;

    // 3. Real-time Detection Query (Cycle Detection)
    // MATCH (a:User)-[t1:SENT_MONEY]->(b:User)-[t2:SENT_MONEY]->(c:User)-[t3:SENT_MONEY]->(a)
    // WHERE t1.ts > $threshold AND ...
    // RETURN ...

    let threshold = current_time - 1000; // 1 second ago

    let query_cycle = "
        MATCH (a:User)-[t1:SENT_MONEY]->(b:User)-[t2:SENT_MONEY]->(c:User)-[t3:SENT_MONEY]->(a)
        WHERE t1.ts > $threshold AND t2.ts > $threshold AND t3.ts > $threshold
        RETURN a._vid, b._vid, c._vid, t1.amount
    ";

    let result_cycle = db
        .query_with(query_cycle)
        .param("threshold", Value::Int(threshold))
        .fetch_all()
        .await?;

    // Cypher returns all rotations of the cycle: A->B->C->A, B->C->A->B, C->A->B->C
    assert_eq!(
        result_cycle.rows.len(),
        3,
        "Should detect cycle (3 rotations)"
    );

    // 4. Identity Resolution Query
    // MATCH (sender:User)-[:USED_DEVICE]->(shared_resource)<-[:USED_DEVICE]-(other:User)
    // WHERE other.risk_score > 0.8
    // RETURN count(other) as suspicious_links

    // Note: Documentation had `[:USED_DEVICE|USED_IP]` but we simplified to `[:USED_DEVICE]` in fix.
    // Also, we need to bind `sender` to `user_a`.
    // In a real app, we would probably filter by sender ID: `MATCH (sender:User {id: $sender_id}) ...`
    // But our User schema only has `risk_score` property in this example (and implicit _vid).
    // So let's filter by `ID(sender) = $sender_vid`.

    let query_identity = "
        MATCH (sender:User)-[:USED_DEVICE]->(shared_resource)<-[:USED_DEVICE]-(other:User)
        WHERE sender._vid = $sender_vid AND other.risk_score > 0.8
        RETURN count(other) as suspicious_links
    ";

    // Cast user_a (Vid is u64) to Value
    let sender_vid_val = Value::Int(user_a.as_u64() as i64);

    let result_identity = db
        .query_with(query_identity)
        .param("sender_vid", sender_vid_val)
        .fetch_all()
        .await?;

    assert_eq!(result_identity.rows.len(), 1);
    let count: i64 = i64::try_from(&result_identity.rows[0].values[0]).unwrap();
    assert_eq!(count, 1, "Should find 1 suspicious link (User D)");

    Ok(())
}
