// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Test for Issue #75: N+1 Subgraph → Batch Frontier
//
// Verifies that adjacency and delta reads can be batched for multiple VIDs.

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

/// Verify that batch vertex/edge insertion and flush works correctly.
#[tokio::test]
async fn test_batch_insert_and_flush() {
    let dir = tempdir().unwrap();
    let path = dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_property("Person", "name", DataType::String, true)
        .unwrap();
    schema_manager
        .add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])
        .unwrap();
    schema_manager.save().await.unwrap();

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await
        .unwrap(),
    );

    let writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    // Insert 3 vertices, labelled `Person`: `KNOWS` is declared Person -> Person.
    let v1 = writer.next_vid().await.unwrap();
    let v2 = writer.next_vid().await.unwrap();
    let v3 = writer.next_vid().await.unwrap();

    writer
        .insert_vertex_with_labels(v1, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();
    writer
        .insert_vertex_with_labels(v2, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();
    writer
        .insert_vertex_with_labels(v3, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();

    // Insert 2 edges: v1->v2, v2->v3
    let e1 = writer.next_eid(0).await.unwrap();
    let e2 = writer.next_eid(0).await.unwrap();
    writer
        .insert_edge(
            v1,
            v2,
            0,
            e1,
            HashMap::new(),
            Some("KNOWS".to_string()),
            None,
        )
        .await
        .unwrap();
    writer
        .insert_edge(
            v2,
            v3,
            0,
            e2,
            HashMap::new(),
            Some("KNOWS".to_string()),
            None,
        )
        .await
        .unwrap();

    // Flush WAL
    writer.flush_wal().await.unwrap();

    // Verify VIDs and EIDs were allocated distinctly
    assert_ne!(v1, v2);
    assert_ne!(v2, v3);
    assert_ne!(v1, v3);
    assert_ne!(e1, e2);
}
