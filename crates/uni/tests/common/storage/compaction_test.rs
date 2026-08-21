// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::SchemaManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::compaction::Compactor;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_compaction_l1_to_l2() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
    let compactor = Compactor::new(storage.clone());

    // 2. Write to L1 via Writer (simulating flush)
    // We need to initialize ID allocator for writer
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);
    let vid_c = Vid::new(3);

    // Insert A -> B
    let eid1 = writer.next_eid(1).await?;
    writer
        .insert_edge(vid_a, vid_b, 1, eid1, HashMap::new(), None, None)
        .await?;

    // Insert A -> C
    let eid2 = writer.next_eid(1).await?;
    writer
        .insert_edge(vid_a, vid_c, 1, eid2, HashMap::new(), None, None)
        .await?;

    // Flush to L1
    writer.flush_to_l1(None).await?;

    // 3. Verify L1 has data and L2 is empty
    let delta_ds = storage.delta_dataset("knows", "fwd")?;
    let deltas = delta_ds
        .scan_all_backend(storage.backend(), &schema_manager.schema())
        .await?;
    assert_eq!(deltas.len(), 2);

    let adj_ds = storage.adjacency_dataset("knows", "Person", "fwd")?;
    let l2_data = adj_ds
        .read_adjacency_backend(storage.backend(), vid_a)
        .await?;
    assert!(l2_data.is_none());

    // 4. Run Compaction
    let _ = compactor
        .compact_adjacency("knows", "Person", "fwd")
        .await?;

    // 5. Verify L2 has data
    let l2_data = adj_ds
        .read_adjacency_backend(storage.backend(), vid_a)
        .await?;
    assert!(l2_data.is_some());
    let (neighbors, eids) = l2_data.unwrap();

    // Should have B and C
    assert_eq!(neighbors.len(), 2);
    assert!(neighbors.contains(&vid_b));
    assert!(neighbors.contains(&vid_c));
    assert!(eids.contains(&eid1));
    assert!(eids.contains(&eid2));

    // 6. Test Updates (Delete + Insert new)
    // Delete A -> B
    writer.delete_edge(eid1, vid_a, vid_b, 1, None).await?;

    // Insert B -> C (different source, check multi-row)
    let eid3 = writer.next_eid(1).await?;
    writer
        .insert_edge(vid_b, vid_c, 1, eid3, HashMap::new(), None, None)
        .await?;

    writer.flush_to_l1(None).await?;

    // Compact again
    let _ = compactor
        .compact_adjacency("knows", "Person", "fwd")
        .await?;

    // 7. Verify L2 Updates
    // A should only have C
    let l2_data_a = adj_ds
        .read_adjacency_backend(storage.backend(), vid_a)
        .await?
        .unwrap();
    assert_eq!(l2_data_a.0.len(), 1);
    assert_eq!(l2_data_a.0[0], vid_c);

    // B should have C
    let l2_data_b = adj_ds
        .read_adjacency_backend(storage.backend(), vid_b)
        .await?
        .unwrap();
    assert_eq!(l2_data_b.0.len(), 1);
    assert_eq!(l2_data_b.0[0], vid_c);

    Ok(())
}

#[tokio::test]
async fn test_compaction_vertices_crdt() -> anyhow::Result<()> {
    use uni_crdt::{Crdt, GCounter};
    use uni_db::Value;
    use uni_db::core::schema::{CrdtType, DataType};

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("CounterNode")?;
    schema_manager.add_property(
        "CounterNode",
        "visits",
        DataType::Crdt(CrdtType::GCounter),
        false,
    )?;
    schema_manager.add_property("CounterNode", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
    let compactor = Compactor::new(storage.clone());
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    let vid = writer.next_vid().await?; // Label 1 = CounterNode

    // 2. Write Version 1: Initial GCounter + Name
    let mut gc1 = GCounter::new();
    gc1.increment("actor_A", 10);

    let props1 = HashMap::from([
        (
            "visits".to_string(),
            serde_json::to_value(Crdt::GCounter(gc1))?.into(),
        ),
        ("name".to_string(), Value::String("Version1".to_string())),
    ]);
    writer
        .insert_vertex_with_labels(vid, props1, &["CounterNode".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // 3. Write Version 2: Update GCounter (partial) + Update Name
    let mut gc2 = GCounter::new();
    gc2.increment("actor_B", 5);

    let props2 = HashMap::from([
        (
            "visits".to_string(),
            serde_json::to_value(Crdt::GCounter(gc2))?.into(),
        ),
        ("name".to_string(), Value::String("Version2".to_string())),
    ]);
    writer
        .insert_vertex_with_labels(vid, props2, &["CounterNode".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Verify before compaction: table has 2 rows
    let table_name = uni_db::store::backend::table_names::vertex_table_name("CounterNode");
    let count = storage.backend().count_rows(&table_name, None).await?;
    assert_eq!(count, 2);

    // 4. Run Compaction
    compactor.compact_vertices("CounterNode").await?;

    // 5. Verify after compaction
    let count_compacted = storage.backend().count_rows(&table_name, None).await?;

    // Should be 1 row (latest state)
    assert_eq!(count_compacted, 1);

    // Verify Properties
    // Use PropertyManager to fetch (it should read from storage)
    // Actually, we can just read the row directly to verify compaction logic
    // But PropertyManager is the standard way.
    let prop_manager = uni_db::runtime::property_manager::PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        100,
    );
    let name_val = prop_manager.get_vertex_prop(vid, "name").await?;
    assert_eq!(name_val, Value::String("Version2".to_string())); // LWW -> Newest wins

    let visits_val = prop_manager.get_vertex_prop(vid, "visits").await?;
    let crdt: Crdt = serde_json::from_value(visits_val.into())?;

    if let Crdt::GCounter(gc) = crdt {
        // Merged: actor_A=10 (from v1), actor_B=5 (from v2) => total 15
        assert_eq!(gc.value(), 15);
    } else {
        panic!("Expected GCounter");
    }

    Ok(())
}

#[tokio::test]
async fn test_compaction_procedures() -> anyhow::Result<()> {
    use uni_db::query::executor::Executor;

    use uni_db::query::planner::QueryPlanner;
    use uni_db::runtime::property_manager::PropertyManager;

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
    let writer = Arc::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    );
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(schema_manager.schema());

    // 1. uni.admin.compactionStatus()
    {
        let sql = "CALL uni.admin.compactionStatus() YIELD l1_runs, in_progress, pending RETURN l1_runs, in_progress, pending";
        let query = uni_cypher::parse(sql)?;
        let plan = planner.plan(query)?;
        let results = executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;
        assert_eq!(results.len(), 1);

        let row = &results[0]; // Vec<HashMap>
        assert!(row.contains_key("l1_runs"));
        assert!(row.contains_key("in_progress"));
    }

    // 2. uni.admin.compact()
    //
    // This fixture has an empty schema, so `compact()` visits no tables and
    // every count is legitimately zero. That makes it a wiring test, not a
    // discrimination test: it proves the procedure yields the measured fields
    // as Ints. Real-vs-no-op discrimination needs data and lives in
    // `compaction_granular_test` and `dqp::transition_probe`.
    {
        let sql = "CALL uni.admin.compact() YIELD tables_optimized, fragments_removed, duration_ms \
                   RETURN tables_optimized, fragments_removed, duration_ms";
        let query = uni_cypher::parse(sql)?;
        let plan = planner.plan(query)?;
        let results = executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;
        assert_eq!(results.len(), 1);

        let row = &results[0];
        for key in ["tables_optimized", "fragments_removed", "duration_ms"] {
            match row.get(key) {
                Some(uni_db::Value::Int(_)) => {}
                other => panic!("{key} missing or not an Int: {other:?}"),
            }
        }
        // Nothing in the schema, so nothing to visit.
        assert_eq!(row.get("tables_optimized"), Some(&uni_db::Value::Int(0)));
    }

    Ok(())
}
