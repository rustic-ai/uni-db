// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::storage::vertex::VertexDataset;
use uni_db::unival;

#[tokio::test]
async fn test_vertex_serialization_nulls() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let schema_manager = SchemaManager::load(&dir.path().join("schema.json")).await?;
    schema_manager.add_label("Node")?;
    schema_manager.add_property("Node", "name", DataType::String, true)?;
    schema_manager.save().await?;
    let schema = schema_manager.schema();

    let ds = VertexDataset::new(path, "Node", 1);

    // Create vertices with nulls
    let mut props = HashMap::new();
    props.insert("name".to_string(), uni_db::Value::Null);

    let vid = Vid::new(0);
    let vertices = vec![(vid, vec!["Node".to_string()], props)];
    let deleted = vec![false];
    let versions = vec![1];

    let batch = ds.build_record_batch(&vertices, &deleted, &versions, &schema)?;

    assert_eq!(batch.num_rows(), 1);
    let name_col = batch.column_by_name("name").unwrap();
    assert!(name_col.is_null(0));

    Ok(())
}

#[tokio::test]
async fn test_vertex_large_properties() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let schema_manager = SchemaManager::load(&dir.path().join("schema.json")).await?;
    schema_manager.add_label("Node")?;
    schema_manager.add_property("Node", "data", DataType::String, false)?;
    schema_manager.save().await?;
    let schema = schema_manager.schema();

    let ds = VertexDataset::new(path, "Node", 1);

    let large_str = "a".repeat(100_000);
    let mut props = HashMap::new();
    props.insert("data".to_string(), unival!(large_str));

    let vid = Vid::new(0);
    let vertices = vec![(vid, vec!["Node".to_string()], props)];
    let deleted = vec![false];
    let versions = vec![1];

    let batch = ds.build_record_batch(&vertices, &deleted, &versions, &schema)?;

    assert_eq!(batch.num_rows(), 1);
    let data_col = batch
        .column_by_name("data")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(data_col.value(0).len(), 100_000);

    Ok(())
}
