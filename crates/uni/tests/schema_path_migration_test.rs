// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use tempfile::tempdir;
use uni_db::Uni;
use uni_db::core::schema::{DataType, SchemaManager};

#[tokio::test]
async fn test_legacy_schema_path_is_migrated_to_catalog() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("db");
    std::fs::create_dir_all(&db_path)?;

    let legacy_schema_path = db_path.join("schema.json");
    let canonical_schema_path = db_path.join("catalog/schema.json");

    // Seed legacy schema location used by older builds.
    let schema_manager = SchemaManager::load(&legacy_schema_path).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    assert!(legacy_schema_path.exists());
    assert!(!canonical_schema_path.exists());

    // Opening the DB should migrate schema to catalog/schema.json.
    let db = Uni::open(db_path.to_str().unwrap()).build().await?;
    assert!(db.label_exists("Person").await?);
    assert!(canonical_schema_path.exists());

    let migrated = SchemaManager::load(&canonical_schema_path).await?;
    assert!(migrated.schema().labels.contains_key("Person"));

    db.shutdown().await?;
    Ok(())
}
