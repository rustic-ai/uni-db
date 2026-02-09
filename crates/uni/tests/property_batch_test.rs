// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_db::unival;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::UniConfig;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::runtime::QueryContext;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_property_batch_loading() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int32, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );
    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        UniConfig::default(),
        None,
        None,
    )
    .await
    .unwrap();

    // 2. Insert Data (L2/L1)
    let vid0 = Vid::new(0);
    let vid1 = Vid::new(1);
    let vid2 = Vid::new(2);

    let mut props0 = HashMap::new();
    props0.insert("name".to_string(), unival!("Alice"));
    props0.insert("age".to_string(), unival!(30));
    writer
        .insert_vertex_with_labels(vid0, props0, vec!["Person".to_string()])
        .await?;

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), unival!("Bob"));
    props1.insert("age".to_string(), unival!(40));
    writer
        .insert_vertex_with_labels(vid1, props1, vec!["Person".to_string()])
        .await?;

    // Flush to storage
    writer.flush_to_l1(None).await?;

    // 3. Insert L0 Data
    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), unival!("Charlie"));
    props2.insert("age".to_string(), unival!(20));
    writer
        .insert_vertex_with_labels(vid2, props2, vec!["Person".to_string()])
        .await?;

    // Update vid0 in L0
    let mut props0_update = HashMap::new();
    props0_update.insert("age".to_string(), unival!(31)); // Birthday!
    props0_update.insert("name".to_string(), unival!("Alice")); // Must provide mandatory field
    writer
        .insert_vertex_with_labels(vid0, props0_update, vec!["Person".to_string()])
        .await?;

    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // 4. Batch Load (Mixed L1/L2 and L0)
    let vids = vec![vid0, vid1, vid2];
    let fields = vec!["name", "age"];

    // Get Context from Writer's L0
    let l0_arc = writer.l0_manager.get_current();
    let ctx = QueryContext::new(l0_arc);

    let results = prop_manager
        .get_batch_vertex_props(&vids, &fields, Some(&ctx))
        .await?;

    assert_eq!(results.len(), 3);

    // Verify vid0 (Merged: Alice from Storage, Age 31 from L0)
    let p0 = results.get(&vid0).unwrap();
    // In L0 insert_vertex overwrites properties for that VID in L0 map.
    // If we only inserted "age", "name" might be missing in L0 if we didn't copy it?
    // L0 implementation of insert_vertex: `self.vertex_properties.insert(vid, properties);`
    // It replaces the map entry.
    // So "name" is gone from L0 view if we didn't include it.
    // But `get_batch_vertex_props` logic:
    // 1. Fetch from storage (Alice, 30)
    // 2. Overlay L0.
    // If L0 has (Age 31), it overwrites Age.
    // But wait, `insert_vertex` in `Writer` calls `l0.insert_vertex`.
    // `L0Buffer::insert_vertex`: `self.vertex_properties.insert(vid, properties);`
    // It REPLACES the entry.
    // So L0 only has "age": 31.
    // Overlay logic:
    // `if let Some(l0_props) = l0.vertex_properties.get(&vid) { ... for (k, v) in l0_props { ... entry.insert(...) } }`
    // It merges individual properties into the result map.
    // So Name should be Alice (from Storage), Age should be 31 (from L0).
    // Let's verify.
    // Wait, does `l0.insert_vertex` keep old properties? No.
    // Does the system enforce partial updates?
    // `Writer::insert_vertex` is typically for full updates or new vertices.
    // If I wanted partial update, I should have read-modify-write or L0 should support partials.
    // Currently L0 stores `Properties` map.
    // The merge logic in `get_batch` does key-by-key merge.

    // However, if L0 has (Age: 31), and Storage has (Name: Alice, Age: 30).
    // Result will be (Name: Alice, Age: 31). Correct.

    assert_eq!(p0.get("name"), Some(&unival!("Alice")));
    assert_eq!(p0.get("age"), Some(&unival!(31)));

    // Verify vid1 (Pure Storage)
    let p1 = results.get(&vid1).unwrap();
    assert_eq!(p1.get("name"), Some(&unival!("Bob")));
    assert_eq!(p1.get("age"), Some(&unival!(40)));

    // Verify vid2 (Pure L0)
    let p2 = results.get(&vid2).unwrap();
    assert_eq!(p2.get("name"), Some(&unival!("Charlie")));
    assert_eq!(p2.get("age"), Some(&unival!(20)));

    Ok(())
}
