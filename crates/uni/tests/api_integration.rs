// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni};

#[tokio::test]
async fn test_api_transactions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Account")
        .property("balance", DataType::Int64)
        .apply()
        .await?;

    // 1. Successful transaction
    let tx = db.begin().await?;
    tx.execute("CREATE (:Account {balance: 100})").await?;
    tx.execute("CREATE (:Account {balance: 200})").await?;
    tx.commit().await?;

    let result = db
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("total")?, 300);

    // 2. Rollback transaction
    let tx = db.begin().await?;
    tx.execute("CREATE (:Account {balance: 500})").await?;
    // Data should be visible inside transaction
    let res_inner = tx
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(res_inner.rows()[0].get::<i64>("total")?, 800);

    tx.rollback().await?;

    // Data should NOT be visible after rollback
    let res_outer = db
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(res_outer.rows()[0].get::<i64>("total")?, 300);

    // 3. Closure-based transaction
    db.transaction(|tx| {
        Box::pin(async move {
            tx.execute("CREATE (:Account {balance: 1000})").await?;
            Ok(())
        })
    })
    .await?;

    let result = db
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("total")?, 1300);

    Ok(())
}

#[tokio::test]
async fn test_api_schema_and_property_query() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. Define Schema
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int32)
        .index("name", IndexType::Scalar(ScalarType::BTree))
        .label("Movie")
        .property("title", DataType::String)
        .edge_type("ACTED_IN", &["Person"], &["Movie"])
        .property("role", DataType::String)
        .apply()
        .await?;

    // 2. Insert Data using Cypher
    db.execute("CREATE (:Person {name: 'Tom Hanks', age: 68})")
        .await?;
    db.execute("CREATE (:Movie {title: 'Cast Away'})").await?;
    db.execute(
        "
        MATCH (p:Person {name: 'Tom Hanks'}), (m:Movie {title: 'Cast Away'})
        CREATE (p)-[:ACTED_IN {role: 'Chuck Noland'}]->(m)
    ",
    )
    .await?;

    // 3. Query properties
    let result = db
        .query("MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, p.age, r.role, m.title")
        .await?;
    assert_eq!(result.len(), 1);

    let row = &result.rows()[0];
    assert_eq!(row.get::<String>("p.name")?, "Tom Hanks");
    assert_eq!(row.get::<i32>("p.age")?, 68);
    assert_eq!(row.get::<String>("r.role")?, "Chuck Noland");
    assert_eq!(row.get::<String>("m.title")?, "Cast Away");

    Ok(())
}

#[tokio::test]
async fn test_api_query_flow() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema implicitly? No, need schema first for properties
    // Or we can rely on "schemaless" if supported?
    // Current Uni requires schema for properties.
    // For now, let's create a label using internal schema manager until Phase 3 (Schema API).
    // Accessing internal schema manager is possible via db.schema (it's pub(crate)).
    // Wait, integration tests are outside the crate, so they can't access pub(crate).
    // I need to use the Schema API or hacks.
    // But Schema API is Phase 3.

    // Test basic queries that don't require schema setup.

    // Test 1: Simple scalar return
    let result = db.query("RETURN 1 AS num, 'hello' AS str").await?;
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];
    let num: i64 = row.get("num")?;
    let s: String = row.get("str")?;
    assert_eq!(num, 1);
    assert_eq!(s, "hello");

    // Test 2: List and Map
    let result = db.query("RETURN [1, 2, 3] AS list, {a: 1} AS map").await?;
    let row = &result.rows()[0];
    // Lists come back as Value::List
    let list: Vec<i64> = row.get("list")?;
    assert_eq!(list, vec![1, 2, 3]);

    // Test 3: Params
    let result = db
        .query_with("RETURN $x AS x")
        .param("x", 42)
        .fetch_all()
        .await?;
    let x: i64 = result.rows()[0].get("x")?;
    assert_eq!(x, 42);

    Ok(())
}

#[tokio::test]
async fn test_parameterized_create_vertex() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute_with("CREATE (p:Person {name: $name, age: $age})")
        .param("name", "Alice")
        .param("age", 30)
        .execute()
        .await?;

    let result = db
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("age")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_parameterized_create_edge() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (p:Person {name: 'Alice'})").await?;
    db.execute("CREATE (p:Person {name: 'Bob'})").await?;

    db.execute_with(
        "MATCH (a:Person {name: $src}), (b:Person {name: $dst}) CREATE (a)-[:KNOWS {since: $since}]->(b)",
    )
    .param("src", "Alice")
    .param("dst", "Bob")
    .param("since", 2024)
    .execute()
    .await?;

    let result = db
        .query("MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) RETURN k.since AS since")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("since")?, 2024);

    Ok(())
}

#[tokio::test]
async fn test_parameterized_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        .await?;

    db.execute_with("MATCH (p:Person {name: $name}) SET p.age = $new_age")
        .param("name", "Alice")
        .param("new_age", 31)
        .execute()
        .await?;

    let result = db
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("age")?, 31);

    Ok(())
}

#[tokio::test]
async fn test_parameterized_delete() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        .await?;

    db.execute_with("MATCH (p:Person {name: $name}) DELETE p")
        .param("name", "Alice")
        .execute()
        .await?;

    let result = db.query("MATCH (p:Person) RETURN p.name AS name").await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "Bob");

    Ok(())
}
