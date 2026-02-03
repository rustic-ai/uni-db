// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end test for quantifier expressions with the new parser

use anyhow::Result;
use uni_db::Uni;

async fn create_test_db() -> Result<Uni> {
    use uni_db::DataType;

    let db = Uni::in_memory().build().await?;

    // Schema definition for Person label
    db.schema()
        .label("Person")
        .property_nullable("name", DataType::String)
        .property_nullable("tags", DataType::Json) // Mixed types: integers or strings
        .property_nullable("scores", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("values", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("items", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("errors", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("numbers", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("data", DataType::Json) // Nested list [[1,2], [3,4]]
        .apply()
        .await?;

    Ok(db)
}

#[tokio::test]
async fn test_quantifier_all_e2e() -> Result<()> {
    let db = create_test_db().await?;

    // Create test data
    db.execute("CREATE (p:Person {name: 'Alice', tags: [1, 2, 3], scores: [85, 90, 95]})")
        .await?;
    db.execute("CREATE (p:Person {name: 'Bob', tags: [0, -1, 5], scores: [60, 70, 80]})")
        .await?;
    db.flush().await?;

    // Test ALL quantifier - should return Alice (all tags > 0)
    let result = db
        .query("MATCH (p:Person) WHERE ALL(x IN p.tags WHERE x > 0) RETURN p.name")
        .await?;

    assert_eq!(result.len(), 1);
    let name: String = result.rows()[0].get("p.name")?;
    assert_eq!(name, "Alice");

    Ok(())
}

#[tokio::test]
async fn test_quantifier_any_e2e() -> Result<()> {
    let db = create_test_db().await?;

    db.execute("CREATE (p:Person {name: 'Alice', values: [1, 2, 3]})")
        .await?;
    db.execute("CREATE (p:Person {name: 'Bob', values: [10, 20, 30]})")
        .await?;
    db.flush().await?;

    // Test ANY quantifier - should return Bob (has values >= 20)
    let result = db
        .query(
            "MATCH (p:Person) WHERE ANY(x IN p.values WHERE x >= 20) RETURN p.name ORDER BY p.name",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let name: String = result.rows()[0].get("p.name")?;
    assert_eq!(name, "Bob");

    Ok(())
}

#[tokio::test]
async fn test_quantifier_single_e2e() -> Result<()> {
    let db = create_test_db().await?;

    db.execute("CREATE (p:Person {name: 'Charlie', items: [5]})")
        .await?;
    db.execute("CREATE (p:Person {name: 'David', items: [5, 5, 5]})")
        .await?;
    db.flush().await?;

    // Test SINGLE quantifier - should return Charlie (exactly one 5)
    let result = db
        .query("MATCH (p:Person) WHERE SINGLE(x IN p.items WHERE x = 5) RETURN p.name")
        .await?;

    assert_eq!(result.len(), 1);
    let name: String = result.rows()[0].get("p.name")?;
    assert_eq!(name, "Charlie");

    Ok(())
}

#[tokio::test]
async fn test_quantifier_none_e2e() -> Result<()> {
    let db = create_test_db().await?;

    db.execute("CREATE (p:Person {name: 'Eve', errors: [1, 2, 3]})")
        .await?;
    db.execute("CREATE (p:Person {name: 'Frank', errors: [-1, -2]})")
        .await?;
    db.flush().await?;

    // Test NONE quantifier - should return Eve (no negative values)
    let result = db
        .query("MATCH (p:Person) WHERE NONE(x IN p.errors WHERE x < 0) RETURN p.name")
        .await?;

    assert_eq!(result.len(), 1);
    let name: String = result.rows()[0].get("p.name")?;
    assert_eq!(name, "Eve");

    Ok(())
}

#[tokio::test]
async fn test_quantifier_in_return_e2e() -> Result<()> {
    let db = create_test_db().await?;

    db.execute("CREATE (p:Person {name: 'Grace', numbers: [10, 20, 30]})")
        .await?;
    db.flush().await?;

    // Test quantifier in RETURN clause
    let result = db
        .query("MATCH (p:Person {name: 'Grace'}) RETURN ALL(x IN p.numbers WHERE x >= 10) AS all_valid")
        .await?;

    assert_eq!(result.len(), 1);
    let all_valid: bool = result.rows()[0].get("all_valid")?;
    assert!(all_valid);

    Ok(())
}

#[tokio::test]
async fn test_quantifier_with_literal_list_e2e() -> Result<()> {
    let db = create_test_db().await?;

    // Test quantifier with literal list (no data needed)
    let result = db
        .query("RETURN ALL(x IN [1, 2, 3, 4, 5] WHERE x > 0) AS result")
        .await?;

    assert_eq!(result.len(), 1);
    let res: bool = result.rows()[0].get("result")?;
    assert!(res);

    Ok(())
}

#[tokio::test]
async fn test_quantifier_empty_list_e2e() -> Result<()> {
    let db = create_test_db().await?;

    db.execute("CREATE (p:Person {name: 'Empty', items: []})")
        .await?;
    db.flush().await?;

    // ALL on empty list should be true (vacuous truth)
    let result_all = db
        .query("MATCH (p:Person {name: 'Empty'}) RETURN ALL(x IN p.items WHERE x > 0) AS result")
        .await?;

    let res: bool = result_all.rows()[0].get("result")?;
    assert!(res);

    // ANY on empty list should be false
    let result_any = db
        .query("MATCH (p:Person {name: 'Empty'}) RETURN ANY(x IN p.items WHERE x > 0) AS result")
        .await?;

    let res: bool = result_any.rows()[0].get("result")?;
    assert!(!res);

    // NONE on empty list should be true
    let result_none = db
        .query("MATCH (p:Person {name: 'Empty'}) RETURN NONE(x IN p.items WHERE x > 0) AS result")
        .await?;

    let res: bool = result_none.rows()[0].get("result")?;
    assert!(res);

    Ok(())
}

#[tokio::test]
async fn test_array_indexing_e2e() -> Result<()> {
    let db = create_test_db().await?;

    db.execute("CREATE (p:Person {name: 'Helen', tags: ['a', 'b', 'c', 'd']})")
        .await?;
    db.flush().await?;

    // Test array indexing
    let result = db
        .query("MATCH (p:Person {name: 'Helen'}) RETURN p.tags[0] AS first, p.tags[2] AS third")
        .await?;

    assert_eq!(result.len(), 1);
    let first: String = result.rows()[0].get("first")?;
    let third: String = result.rows()[0].get("third")?;
    assert_eq!(first, "a");
    assert_eq!(third, "c");

    Ok(())
}

#[tokio::test]
async fn test_array_slicing_e2e() -> Result<()> {
    let db = create_test_db().await?;

    db.execute("CREATE (p:Person {name: 'Ivan', numbers: [10, 20, 30, 40, 50]})")
        .await?;
    db.flush().await?;

    // Test array slicing
    let result = db
        .query("MATCH (p:Person {name: 'Ivan'}) RETURN p.numbers[1..3] AS slice")
        .await?;

    assert_eq!(result.len(), 1);
    let slice: Vec<i64> = result.rows()[0].get("slice")?;
    assert_eq!(slice, vec![20, 30]);

    Ok(())
}

#[tokio::test]
async fn test_combined_quantifier_and_array_ops_e2e() -> Result<()> {
    let db = create_test_db().await?;

    db.execute("CREATE (p:Person {name: 'Jane', data: [[1, 2], [3, 4], [5, 6]]})")
        .await?;
    db.flush().await?;

    // Test quantifier with array operations
    let result = db
        .query(
            "MATCH (p:Person {name: 'Jane'})
             WHERE ALL(row IN p.data WHERE row[0] > 0)
             RETURN p.name",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let name: String = result.rows()[0].get("p.name")?;
    assert_eq!(name, "Jane");

    Ok(())
}
