// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::unival;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_edge_export_failure() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.add_property("KNOWS", "since", DataType::Int32, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    );

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(schema_manager.schema());

    // 2. Insert Data
    {
        let w: &uni_store::Writer = writer.as_ref();
        // Alice (VID 0), Bob (VID 1)
        w.insert_vertex_with_labels(
            uni_db::common::core::id::Vid::new(0),
            HashMap::new(),
            &["Person".to_string()],
            None,
        )
        .await?;
        w.insert_vertex_with_labels(
            uni_db::common::core::id::Vid::new(1),
            HashMap::new(),
            &["Person".to_string()],
            None,
        )
        .await?;

        // Edge Alice -> Bob
        let eid = uni_db::common::core::id::Eid::new(0); // Type 1 (KNOWS)
        let mut props = HashMap::new();
        props.insert("since".to_string(), unival!(2022));
        w.insert_edge(
            uni_db::common::core::id::Vid::new(0),
            uni_db::common::core::id::Vid::new(1),
            1,
            eid,
            props,
            None,
            None,
        )
        .await?;
        w.flush_to_l1(None).await?; // Flush to ensure it's in storage (optional for export logic depending on implementation)
    }

    // 3. Try Export Edges to Parquet
    let export_file = path.join("knows_export.parquet");
    let cypher = format!("COPY KNOWS TO '{}'", export_file.to_str().unwrap());

    let res = executor
        .execute(
            planner.plan(uni_cypher::parse(&cypher)?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res[0].get("count").unwrap(), &unival!(1));

    // Verify Exported Parquet File
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(&export_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut count = 0;
    let mut found_correct_edge = false;
    for batch_result in reader {
        let batch = batch_result?;
        count += batch.num_rows();

        // The export carries the edge's identity and its user properties —
        // never the storage table's columns (`src_vid`, `type`, `props_json`).
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["_eid", "_src", "_dst", "since"]);

        use arrow_array::Array;
        if batch.num_rows() > 0 {
            let u64_at = |name: &str| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .unwrap()
                    .value(0)
            };
            let since = uni_store::storage::arrow_convert::arrow_to_value(
                batch.column_by_name("since").unwrap().as_ref(),
                0,
                None,
            )?;
            if u64_at("_src") == 0 && u64_at("_dst") == 1 && since == unival!(2022) {
                found_correct_edge = true;
            }
        }
    }

    assert_eq!(count, 1, "Expected 1 edge in export");
    assert!(
        found_correct_edge,
        "Expected to find Alice->Bob edge with since=2022"
    );
    Ok(())
}
