// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, UInt64Builder};
use arrow_array::{
    LargeBinaryArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::Value;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_query_integration() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _person_label_id = schema_manager.add_label("Person")?;
    let _knows_type_id =
        schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let lancedb_store = storage.lancedb_store();

    // VIDs
    // A (0)
    // B (1)
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);

    // EID
    let eid_ab = Eid::new(1);

    // 2. Write Data
    // 2a. Vertices
    let vertex_ds = storage.vertex_dataset("Person")?;
    // We need to write _vid, _uid, _deleted, _version, name
    // _vid: [0, 1]
    // name: ["Alice", "Bob"]

    let arrow_schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    let vids = UInt64Array::from(vec![vid_a.as_u64(), vid_b.as_u64()]);
    let names = StringArray::from(vec!["Alice", "Bob"]);
    // _uid, _deleted, _version can be null or defaults
    // But Lance requires matching schema.
    // get_arrow_schema returns fields in specific order.
    // _vid, _uid, _deleted, _version, name (sorted)

    let mut uid_builder = FixedSizeBinaryBuilder::new(32);
    let dummy_uid = vec![0u8; 32];
    uid_builder.append_value(&dummy_uid).unwrap();
    uid_builder.append_value(&dummy_uid).unwrap();
    let uids = uid_builder.finish();

    let deleted = arrow_array::BooleanArray::from(vec![false, false]);
    let versions = UInt64Array::from(vec![1, 1]);

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(vids),
            Arc::new(uids),                                     // _uid
            Arc::new(deleted),                                  // _deleted
            Arc::new(versions),                                 // _version
            Arc::new(StringArray::from(vec![None::<&str>; 2])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
                for _ in 0..2 {
                    lb.values().append_value("Person");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 2]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 2]).with_timezone("UTC")), // _updated_at
            Arc::new(names),                                          // name
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 2])), // overflow_json
        ],
    )?;

    vertex_ds
        .write_batch_lancedb(lancedb_store, batch, &schema_manager.schema())
        .await?;

    // 2b. Adjacency (A->B)
    let adj_ds = storage.adjacency_dataset("knows", "Person", "fwd")?;
    let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
    let mut edge_ids_builder = ListBuilder::new(UInt64Builder::new());

    // Row 0 (A): neighbors [B], eids [eid_ab]
    neighbors_builder.values().append_value(vid_b.as_u64());
    neighbors_builder.append(true);
    edge_ids_builder.values().append_value(eid_ab.as_u64());
    edge_ids_builder.append(true);

    // Row 1 (B): empty
    neighbors_builder.append(true);
    edge_ids_builder.append(true);

    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![vid_a.as_u64(), vid_b.as_u64()])),
            Arc::new(neighbors_builder.finish()),
            Arc::new(edge_ids_builder.finish()),
        ],
    )?;

    adj_ds.write_chunk_lancedb(lancedb_store, batch).await?;

    // Warm the adjacency cache and adjacency manager
    use uni_db::storage::direction::Direction;
    let knows_edge_type_id = schema_manager
        .schema()
        .edge_type_id_by_name("knows")
        .unwrap();
    storage
        .warm_adjacency(knows_edge_type_id, Direction::Outgoing, None)
        .await?;

    // 3. Execute Query
    let sql = "MATCH (a:Person)-[:knows]->(b:Person) RETURN a.name";

    let query_ast = uni_query::parse_cypher(sql)?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &std::collections::HashMap::new())
        .await?;

    println!("Results: {:?}", results);

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("a.name"),
        Some(&Value::String("Alice".to_string()))
    );

    Ok(())
}
