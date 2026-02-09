// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::unival;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_csv_import() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int32, false)?;

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

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 2. Prepare CSV files
    let person_csv = path.join("people.csv");
    {
        let mut file = std::fs::File::create(&person_csv)?;
        writeln!(file, "name,age")?;
        writeln!(file, "Alice,30")?;
        writeln!(file, "Bob,25")?;
    }

    // 3. Import People
    let cypher = format!("COPY Person FROM '{}'", person_csv.to_str().unwrap());
    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(&cypher)?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res[0].get("count").unwrap(), &unival!(2));

    // 4. Verify People
    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(
                "MATCH (n:Person) RETURN n.name ORDER BY n.name",
            )?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].get("n.name"), Some(&unival!("Alice")));
    assert_eq!(res[1].get("n.name"), Some(&unival!("Bob")));

    // 5. Import Edges
    // We need VIDs. Alice is 0, Bob is 1 (simple auto-increment)
    let alice_vid = "0";
    let bob_vid = "1";

    let knows_csv = path.join("knows.csv");
    {
        let mut file = std::fs::File::create(&knows_csv)?;
        writeln!(file, "src,dst,since")?;
        writeln!(file, "{},{},2020", alice_vid, bob_vid)?;
    }

    let cypher = format!(
        "COPY KNOWS FROM '{}' WITH {{ src_col: 'src', dst_col: 'dst' }}",
        knows_csv.to_str().unwrap()
    );
    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(&cypher)?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res[0].get("count").unwrap(), &unival!(1));

    // 6. Verify Edges
    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name, r.since",
            )?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("a.name"), Some(&unival!("Alice")));
    assert_eq!(res[0].get("b.name"), Some(&unival!("Bob")));
    assert_eq!(res[0].get("r.since"), Some(&unival!(2020)));

    // 7. Export People
    let export_csv = path.join("people_export.csv");
    let cypher = format!("COPY Person TO '{}'", export_csv.to_str().unwrap());
    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(&cypher)?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res[0].get("count").unwrap(), &unival!(2));

    // Verify Exported File
    let mut rdr = csv::ReaderBuilder::new().from_path(&export_csv)?;
    let headers = rdr.headers()?.clone();
    let name_idx = headers.iter().position(|h| h == "name").unwrap();

    let mut exported_names = Vec::new();
    for result in rdr.records() {
        let record = result?;
        exported_names.push(record.get(name_idx).unwrap().to_string());
    }
    exported_names.sort();
    assert_eq!(exported_names, vec!["Alice", "Bob"]);

    // 8. Import Parquet
    let person_parquet = path.join("people.parquet");
    {
        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType as ArrowDataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("age", ArrowDataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
                Arc::new(Int32Array::from(vec![40, 45])),
            ],
        )?;

        let file = std::fs::File::create(&person_parquet)?;
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
    }

    let cypher = format!("COPY Person FROM '{}'", person_parquet.to_str().unwrap());
    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(&cypher)?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res[0].get("count").unwrap(), &unival!(2));

    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(
                "MATCH (n:Person) WHERE n.name = 'Charlie' RETURN n.age",
            )?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("n.age"), Some(&unival!(40)));

    Ok(())
}

#[tokio::test]
async fn test_parquet_import() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
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

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 2. Create Parquet file with test data
    let person_parquet = path.join("people.parquet");
    {
        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType as ArrowDataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("age", ArrowDataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
                Arc::new(Int32Array::from(vec![40, 45])),
            ],
        )?;

        let file = std::fs::File::create(&person_parquet)?;
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
    }

    // 3. Import from Parquet with explicit format
    let cypher = format!(
        "COPY Person FROM '{}' WITH {{format: 'parquet'}}",
        person_parquet.to_str().unwrap()
    );
    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(&cypher)?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res[0].get("count").unwrap(), &unival!(2));

    // 4. Verify imported data
    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(
                "MATCH (n:Person) WHERE n.name = 'Charlie' RETURN n.age",
            )?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("n.age"), Some(&unival!(40)));

    let res = executor
        .execute(
            planner.plan(uni_query::parse_cypher(
                "MATCH (n:Person) WHERE n.name = 'Dave' RETURN n.age",
            )?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("n.age"), Some(&unival!(45)));

    Ok(())
}
