// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use tempfile::tempdir;
use uni_crdt::{Crdt, GCounter};
use uni_db::Uni;
use uni_db::core::id::Vid;
use uni_db::core::schema::{CrdtType, DataType};
use uni_db::runtime::property_manager::PropertyManager;

#[tokio::test]
async fn test_crdt_merge_on_write() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema with GCounter CRDT
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    db.schema()
        .label("CounterNode")
        .property("counter", DataType::Crdt(CrdtType::GCounter))
        .done()
        .apply()
        .await?;

    let schema_manager = db.schema_manager();
    let _label_id = schema_manager
        .schema()
        .labels
        .get("CounterNode")
        .unwrap()
        .id;
    let vid = Vid::new(1);

    // 2. Write initial value: GCounter { actor1: 10 }
    let mut gc1 = GCounter::new();
    gc1.increment("actor1", 10);
    let val1 = serde_json::to_value(Crdt::GCounter(gc1))?;

    let mut props1 = HashMap::new();
    props1.insert("counter".to_string(), val1.into());

    // Use internal writer to simulate direct writes
    // Note: Uni::execute uses writer internally
    let writer_lock = db.writer().unwrap();
    {
        let mut writer = writer_lock.write().await;
        writer
            .insert_vertex_with_labels(vid, props1, vec!["CounterNode".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;
    }

    // 3. Write second value: GCounter { actor2: 20 }
    // This should MERGE with existing value, resulting in { actor1: 10, actor2: 20 }
    let mut gc2 = GCounter::new();
    gc2.increment("actor2", 20);
    let val2 = serde_json::to_value(Crdt::GCounter(gc2))?;

    let mut props2 = HashMap::new();
    props2.insert("counter".to_string(), val2.into());

    {
        let mut writer = writer_lock.write().await;
        writer
            .insert_vertex_with_labels(vid, props2, vec!["CounterNode".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;
    }

    // 4. Read back and verify merge
    // Use PropertyManager directly to bypass potential higher-level query caching logic if any
    let storage = db.storage();
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let result = prop_manager.get_vertex_prop(vid, "counter").await?;

    println!("Result: {:?}", result);
    let result_crdt: Crdt = serde_json::from_value(result.into())?;

    if let Crdt::GCounter(gc) = result_crdt {
        assert_eq!(gc.value(), 30); // 10 + 20
    } else {
        panic!("Expected GCounter result");
    }

    Ok(())
}
