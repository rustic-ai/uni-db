// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{ListBuilder, UInt64Builder};
use arrow_array::{RecordBatch, UInt64Array};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::runtime::Direction;
use uni_db::runtime::l0::L0Buffer;
use uni_db::storage::delta::{L1Entry, Op};
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_subgraph_loading_merge_logic() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _person_label_id = schema_manager.add_label("Person")?;
    let knows_type_id =
        schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(storage_str, schema_manager.clone()).await?;
    let lancedb_store = storage.lancedb_store();

    // VIDs
    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);
    let vid_c = Vid::new(3);
    let vid_d = Vid::new(4);

    // EIDs
    let eid_ab = Eid::new(1);
    let eid_ac = Eid::new(2);
    let eid_ad = Eid::new(3);

    // 2. Setup L2 (Adjacency): A -> B
    // We need to write to `adj_knows_Person_fwd` (assuming A is Person)
    // Actually mapping is `adjacency_dataset("knows", "Person", "fwd")`
    let adj_ds = storage.adjacency_dataset("knows", "Person", "fwd")?;

    // Build Batch for L2
    // src_vid: [A], neighbors: [[B]], edge_ids: [[eid_ab]]
    let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
    let mut edge_ids_builder = ListBuilder::new(UInt64Builder::new());

    // Row 0
    neighbors_builder.values().append_value(vid_b.as_u64());
    neighbors_builder.append(true);

    edge_ids_builder.values().append_value(eid_ab.as_u64());
    edge_ids_builder.append(true);

    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![vid_a.as_u64()])),
            Arc::new(neighbors_builder.finish()),
            Arc::new(edge_ids_builder.finish()),
        ],
    )?;

    adj_ds.write_chunk_lancedb(lancedb_store, batch).await?;

    // Warm the adjacency cache for L2 data
    use uni_db::storage::direction::Direction as CacheDir;
    storage
        .warm_adjacency(knows_type_id, CacheDir::Outgoing, None)
        .await?;

    // 3. Setup L1 (Delta): Insert A -> C, Delete A -> B
    let delta_ds = storage.delta_dataset("knows", "fwd")?;

    let entries = vec![
        L1Entry {
            src_vid: vid_a,
            dst_vid: vid_c,
            eid: eid_ac,
            op: Op::Insert,
            version: 10,
            properties: HashMap::new(),
            created_at: None,
            updated_at: None,
        },
        L1Entry {
            src_vid: vid_a,
            dst_vid: vid_b,
            eid: eid_ab,
            op: Op::Delete,
            version: 10,
            properties: HashMap::new(),
            created_at: None,
            updated_at: None,
        },
    ];

    let batch = delta_ds.build_record_batch(&entries, &schema_manager.schema())?;
    delta_ds.write_run_lancedb(lancedb_store, batch).await?;

    // 4. Setup L0 (Buffer): Insert A -> D, Delete A -> C
    let mut l0 = L0Buffer::new(20, None);
    // Insert A -> D (ver 21)
    l0.insert_edge(vid_a, vid_d, knows_type_id, eid_ad, HashMap::new())?;

    // Delete A -> C (ver 22)
    l0.delete_edge(eid_ac, vid_a, vid_c, knows_type_id)?;

    // 5. Run load_subgraph
    let graph = storage
        .load_subgraph(
            &[vid_a],
            &[knows_type_id],
            1,
            Direction::Outgoing,
            Some(&l0),
        )
        .await?;

    // 6. Verification
    // Graph should contain edges from A.
    // A->B deleted (L1).
    // A->C deleted (L0 tombstone).
    // A->D exists (L0 insert).

    let mut neighbors = Vec::new();
    // WorkingGraph is SimpleGraph.
    for edge in graph.edges() {
        let u = edge.src_vid;
        let v = edge.dst_vid;
        let eid = edge.eid;

        println!("Edge: {:?} -> {:?} (Eid: {:?})", u, v, eid);
        if u == vid_a {
            neighbors.push(v);
        }
    }

    assert_eq!(neighbors.len(), 1, "Should have exactly 1 neighbor");
    assert_eq!(neighbors[0], vid_d, "Neighbor should be D");

    Ok(())
}
