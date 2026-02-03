// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_crdt::{Crdt, VCRegister, VectorClock};
use uni_store::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn test_vector_clock_integration() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    let label_id = schema_manager.add_label("Person")?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
        .await
        .unwrap();
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let vid = writer.next_vid().await?;

    // 3. Write Initial State (Actor A)
    let mut vc1 = VectorClock::new();
    vc1.increment("A");

    let reg1 = VCRegister {
        value: serde_json::to_value("Initial")?,
        clock: vc1.clone(),
    };

    let props1 = HashMap::from([
        (
            "vclock".to_string(),
            serde_json::to_value(Crdt::VectorClock(vc1.clone()))?,
        ),
        (
            "state".to_string(),
            serde_json::to_value(Crdt::VCRegister(reg1))?,
        ),
    ]);

    writer
        .insert_vertex_with_labels(vid, props1, vec!["Person".to_string()])
        .await?;
    writer.flush_to_l1(None).await?;

    // 4. Concurrent Update (Actor B) - Causal History: {A:1} -> {A:1, B:1}
    // We simulate reading the old state first (A:1)
    let mut vc2 = vc1.clone();
    vc2.increment("B");

    let reg2 = VCRegister {
        value: serde_json::to_value("UpdateB")?,
        clock: vc2.clone(),
    };

    let props2 = HashMap::from([
        (
            "vclock".to_string(),
            serde_json::to_value(Crdt::VectorClock(vc2.clone()))?,
        ),
        (
            "state".to_string(),
            serde_json::to_value(Crdt::VCRegister(reg2))?,
        ),
    ]);

    // Insert into L0 (this will be merged with L1 on read)
    writer
        .insert_vertex_with_labels(vid, props2, vec!["Person".to_string()])
        .await?;

    // 5. Verify Read (Should see UpdateB because {A:1, B:1} > {A:1})
    // Create QueryContext with Writer's L0 buffer so PropertyManager can see uncommitted data
    let l0_buffer = writer.l0_manager.get_current();
    let query_ctx = QueryContext::new(l0_buffer);
    let read_val = prop_manager
        .get_vertex_prop_with_ctx(vid, "state", Some(&query_ctx))
        .await?;
    let read_crdt: Crdt = serde_json::from_value(read_val.clone())?;

    if let Crdt::VCRegister(reg) = read_crdt {
        println!("DEBUG: reg value: {:?} clock: {:?}", reg.value, reg.clock);
        assert_eq!(reg.value, serde_json::to_value("UpdateB")?);
        assert_eq!(reg.clock.get("A"), 1);
        assert_eq!(reg.clock.get("B"), 1);
    } else {
        panic!("Expected VCRegister");
    }

    // 6. Test Concurrent Conflict (Actor C updates from Initial)
    // Causal History: {A:1} -> {A:1, C:1}
    // This is concurrent with {A:1, B:1}
    let mut vc3 = vc1.clone();
    vc3.increment("C");

    let reg3 = VCRegister {
        value: serde_json::to_value("UpdateC")?,
        clock: vc3.clone(),
    };

    let props3 = HashMap::from([(
        "state".to_string(),
        serde_json::to_value(Crdt::VCRegister(reg3))?,
    )]);

    writer
        .insert_vertex_with_labels(vid, props3, vec!["Person".to_string()])
        .await?;

    // 7. Verify Read (Merge of UpdateB and UpdateC)
    // Clocks merge: {A:1, B:1, C:1}
    // Value: Tie-break logic in VCRegister (currently keeps self if concurrent, depends on merge order)
    // L0 has UpdateC. Storage (conceptually) has UpdateB (if flushed, but here B is in L0 too?).
    // Actually, Writer overwrites L0. So L0 now has UpdateC?
    // Wait, Writer logic:
    // `entry.insert(k, v)` overwrites unless it merges.
    // My Writer logic attempts merge:
    // `new_crdt.merge(&existing_crdt)`
    // So UpdateC merges into UpdateB in L0.

    // Refresh the query context with current L0
    let l0_buffer_conflict = writer.l0_manager.get_current();
    let query_ctx_conflict = QueryContext::new(l0_buffer_conflict);
    let read_val_conflict = prop_manager
        .get_vertex_prop_with_ctx(vid, "state", Some(&query_ctx_conflict))
        .await?;
    let read_crdt_conflict: Crdt = serde_json::from_value(read_val_conflict)?;

    if let Crdt::VCRegister(reg) = read_crdt_conflict {
        // Clock should be merged
        assert_eq!(reg.clock.get("A"), 1);
        assert_eq!(reg.clock.get("B"), 1);
        assert_eq!(reg.clock.get("C"), 1);

        // Value depends on which one was "self" in merge.
        // insert_vertex calls `new_crdt.merge(existing)`.
        // new=UpdateC, existing=UpdateB.
        // UpdateC vs UpdateB -> Concurrent.
        // VCRegister merge implementation:
        // Concurrent => merge clocks, keep self.value.
        // So it should keep UpdateC.
        assert_eq!(reg.value, serde_json::to_value("UpdateC")?);
    } else {
        panic!("Expected VCRegister");
    }

    Ok(())
}
