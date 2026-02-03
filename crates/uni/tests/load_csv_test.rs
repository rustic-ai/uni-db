// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::io::Write;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_load_csv_simple() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create a temporary CSV file
    let mut file = tempfile::NamedTempFile::new()?;
    writeln!(file, "name,age")?;
    writeln!(file, "Alice,30")?;
    writeln!(file, "Bob,25")?;
    let path = file.path().to_str().unwrap().to_string();

    // Query using LOAD CSV
    let query = format!(
        "LOAD CSV WITH HEADERS FROM 'file://{}' AS row RETURN row.name AS name, row.age AS age ORDER BY name",
        path
    );
    let results = db.query(&query).await?;

    assert_eq!(results.len(), 2);
    assert_eq!(results.rows[0].get::<String>("name")?, "Alice");
    assert_eq!(results.rows[0].get::<String>("age")?, "30");
    assert_eq!(results.rows[1].get::<String>("name")?, "Bob");
    assert_eq!(results.rows[1].get::<String>("age")?, "25");

    Ok(())
}

#[tokio::test]
async fn test_load_csv_create_nodes() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    // Create a temporary CSV file
    let mut file = tempfile::NamedTempFile::new()?;
    writeln!(file, "Alice,30")?;
    writeln!(file, "Bob,25")?;
    let path = file.path().to_str().unwrap().to_string();

    // Query using LOAD CSV to create nodes
    let query = format!(
        "LOAD CSV FROM 'file://{}' AS row CREATE (:Person {{name: row[0], age: toInteger(row[1])}})",
        path
    );
    db.execute(&query).await?;

    // Verify
    let results = db
        .query("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .await?;
    assert_eq!(results.len(), 2);
    assert_eq!(results.rows[0].get::<String>("name")?, "Alice");
    assert_eq!(results.rows[1].get::<String>("name")?, "Bob");

    Ok(())
}
