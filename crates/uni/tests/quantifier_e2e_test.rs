// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end test for quantifier expressions with the new parser

use anyhow::Result;
use uni_db::Uni;
use uni_query::Value;

async fn create_test_db() -> Result<Uni> {
    use uni_db::DataType;

    let db = Uni::in_memory().build().await?;

    // Schema definition for Person label
    db.schema()
        .label("Person")
        .property_nullable("name", DataType::String)
        .property_nullable("tags", DataType::CypherValue) // Mixed types: integers or strings
        .property_nullable("scores", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("values", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("items", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("errors", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("numbers", DataType::List(Box::new(DataType::Int64)))
        .property_nullable("data", DataType::CypherValue) // Nested list [[1,2], [3,4]]
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

// ============================================================================
// Helper functions for literal-list quantifier tests (Phases 1-7)
// ============================================================================

/// Evaluate a RETURN expression and return the single Value.
async fn eval(db: &Uni, cypher: &str) -> Value {
    let result = db.query(cypher).await.unwrap();
    result.rows[0].value("result").unwrap().clone()
}

/// Assert that a RETURN expression yields a boolean value.
async fn assert_bool(db: &Uni, cypher: &str, expected: bool) {
    let val = eval(db, cypher).await;
    assert_eq!(
        val,
        Value::Bool(expected),
        "Expected Bool({expected}) for: {cypher}"
    );
}

/// Assert that a RETURN expression yields null.
async fn assert_null(db: &Uni, cypher: &str) {
    let val = eval(db, cypher).await;
    assert_eq!(val, Value::Null, "Expected Null for: {cypher}");
}

// ============================================================================
// Phase 1: Empty List Semantics
// TCK: Quantifier1-4 scenario [1]
// ============================================================================

#[tokio::test]
async fn test_empty_list_all_quantifier() {
    let db = Uni::in_memory().build().await.unwrap();

    // ALL on empty list → true (vacuous truth)
    assert_bool(&db, "RETURN all(x IN [] WHERE x > 0) AS result", true).await;
    assert_bool(&db, "RETURN all(x IN [] WHERE true) AS result", true).await;
    assert_bool(&db, "RETURN all(x IN [] WHERE false) AS result", true).await;
}

#[tokio::test]
async fn test_empty_list_any_quantifier() {
    let db = Uni::in_memory().build().await.unwrap();

    // ANY on empty list → false
    assert_bool(&db, "RETURN any(x IN [] WHERE x > 0) AS result", false).await;
    assert_bool(&db, "RETURN any(x IN [] WHERE true) AS result", false).await;
    assert_bool(&db, "RETURN any(x IN [] WHERE false) AS result", false).await;
}

#[tokio::test]
async fn test_empty_list_none_quantifier() {
    let db = Uni::in_memory().build().await.unwrap();

    // NONE on empty list → true
    assert_bool(&db, "RETURN none(x IN [] WHERE x > 0) AS result", true).await;
    assert_bool(&db, "RETURN none(x IN [] WHERE true) AS result", true).await;
    assert_bool(&db, "RETURN none(x IN [] WHERE false) AS result", true).await;
}

#[tokio::test]
async fn test_empty_list_single_quantifier() {
    let db = Uni::in_memory().build().await.unwrap();

    // SINGLE on empty list → false
    assert_bool(&db, "RETURN single(x IN [] WHERE x > 0) AS result", false).await;
    assert_bool(&db, "RETURN single(x IN [] WHERE true) AS result", false).await;
    assert_bool(&db, "RETURN single(x IN [] WHERE false) AS result", false).await;
}

// ============================================================================
// Phase 2: Basic Data Type Coverage
// TCK: Quantifier1-4 scenarios [2]-[7], [13]-[14]
// ============================================================================

#[tokio::test]
async fn test_quantifier_booleans() {
    let db = Uni::in_memory().build().await.unwrap();

    // ALL booleans true → requires all elements to satisfy predicate
    assert_bool(&db, "RETURN all(x IN [true, true] WHERE x) AS result", true).await;
    assert_bool(
        &db,
        "RETURN all(x IN [true, false] WHERE x) AS result",
        false,
    )
    .await;

    // ANY boolean → at least one true
    assert_bool(
        &db,
        "RETURN any(x IN [true, false] WHERE x) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [false, false] WHERE x) AS result",
        false,
    )
    .await;

    // NONE boolean → no element satisfies
    assert_bool(
        &db,
        "RETURN none(x IN [false, false] WHERE x) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN none(x IN [true, false] WHERE x) AS result",
        false,
    )
    .await;

    // SINGLE boolean → exactly one true
    assert_bool(
        &db,
        "RETURN single(x IN [true, false] WHERE x) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [true, true] WHERE x) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_quantifier_integers() {
    let db = Uni::in_memory().build().await.unwrap();

    // Equality predicates
    assert_bool(
        &db,
        "RETURN all(x IN [2, 2, 2] WHERE x = 2) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN all(x IN [1, 2, 3] WHERE x = 2) AS result",
        false,
    )
    .await;

    // Comparison predicates
    assert_bool(
        &db,
        "RETURN any(x IN [1, 2, 3, 4] WHERE x > 3) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [1, 2, 3] WHERE x > 10) AS result",
        false,
    )
    .await;

    // SINGLE with comparison
    assert_bool(
        &db,
        "RETURN single(x IN [1, 2, 3, 4] WHERE x = 3) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [3, 3, 4] WHERE x = 3) AS result",
        false,
    )
    .await;

    // NONE with comparison
    assert_bool(
        &db,
        "RETURN none(x IN [1, 2, 3] WHERE x < 0) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN none(x IN [1, -2, 3] WHERE x < 0) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_quantifier_floats() {
    let db = Uni::in_memory().build().await.unwrap();

    assert_bool(
        &db,
        "RETURN all(x IN [1.1, 2.2, 3.3] WHERE x > 0.0) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [1.1, 2.2, 3.5] WHERE x = 3.5) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN none(x IN [1.1, 2.2, 3.5] WHERE x < 0.0) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [1.1, 2.2, 3.5] WHERE x > 3.0) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_quantifier_strings() {
    let db = Uni::in_memory().build().await.unwrap();

    // size(x) = 3
    assert_bool(
        &db,
        "RETURN all(x IN ['abc', 'def', 'ghi'] WHERE size(x) = 3) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN all(x IN ['abc', 'de'] WHERE size(x) = 3) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN ['abc', 'de'] WHERE size(x) = 3) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN ['abc', 'de'] WHERE size(x) = 3) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN none(x IN ['ab', 'de'] WHERE size(x) = 3) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_quantifier_nested_lists() {
    let db = Uni::in_memory().build().await.unwrap();

    // size on nested lists
    assert_bool(
        &db,
        "RETURN all(x IN [[1, 2, 3], [4, 5, 6]] WHERE size(x) = 3) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN none(x IN [['a'], ['b']] WHERE size(x) = 3) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_quantifier_maps() {
    let db = Uni::in_memory().build().await.unwrap();

    // Map property access in predicate
    assert_bool(
        &db,
        "RETURN all(x IN [{a: 2}, {a: 2}] WHERE x.a = 2) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [{a: 2}, {a: 4}] WHERE x.a = 2) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [{a: 2}, {a: 4}] WHERE x.a = 2) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN none(x IN [{a: 3}, {a: 4}] WHERE x.a = 2) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_quantifier_static_predicates() {
    let db = Uni::in_memory().build().await.unwrap();

    // WHERE true on non-empty list
    assert_bool(&db, "RETURN all(x IN [1, 2, 3] WHERE true) AS result", true).await;
    assert_bool(&db, "RETURN any(x IN [1, 2, 3] WHERE true) AS result", true).await;
    assert_bool(
        &db,
        "RETURN none(x IN [1, 2, 3] WHERE true) AS result",
        false,
    )
    .await;
    // SINGLE(WHERE true) on 3-element list → false (3 > 1)
    assert_bool(
        &db,
        "RETURN single(x IN [1, 2, 3] WHERE true) AS result",
        false,
    )
    .await;
    // SINGLE(WHERE true) on 1-element list → true
    assert_bool(&db, "RETURN single(x IN [42] WHERE true) AS result", true).await;

    // WHERE false on non-empty list
    assert_bool(
        &db,
        "RETURN all(x IN [1, 2, 3] WHERE false) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [1, 2, 3] WHERE false) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN none(x IN [1, 2, 3] WHERE false) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [1, 2, 3] WHERE false) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_quantifier_single_element_list() {
    let db = Uni::in_memory().build().await.unwrap();

    // Single-element list edge cases
    assert_bool(&db, "RETURN all(x IN [5] WHERE x = 5) AS result", true).await;
    assert_bool(&db, "RETURN all(x IN [5] WHERE x = 6) AS result", false).await;
    assert_bool(&db, "RETURN any(x IN [5] WHERE x = 5) AS result", true).await;
    assert_bool(&db, "RETURN any(x IN [5] WHERE x = 6) AS result", false).await;
    assert_bool(&db, "RETURN single(x IN [5] WHERE x = 5) AS result", true).await;
    assert_bool(&db, "RETURN single(x IN [5] WHERE x = 6) AS result", false).await;
    assert_bool(&db, "RETURN none(x IN [5] WHERE x = 5) AS result", false).await;
    assert_bool(&db, "RETURN none(x IN [5] WHERE x = 6) AS result", true).await;
}

// ============================================================================
// Phase 3: Null Semantics — Three-Valued Logic (CRITICAL)
// TCK: Quantifier1-4 scenarios [10]
// ============================================================================

#[tokio::test]
async fn test_none_null_semantics() {
    let db = Uni::in_memory().build().await.unwrap();

    // none(x IN [null] WHERE x=2): predicate is null → null propagates → null
    assert_null(&db, "RETURN none(x IN [null] WHERE x = 2) AS result").await;

    // none(x IN [2, null] WHERE x=2): 2=2→true → short-circuit false
    assert_bool(
        &db,
        "RETURN none(x IN [2, null] WHERE x = 2) AS result",
        false,
    )
    .await;

    // none(x IN [0, null] WHERE x=2): 0=2→false, null=2→null → null
    assert_null(&db, "RETURN none(x IN [0, null] WHERE x = 2) AS result").await;

    // none(x IN [0, 1] WHERE x=2): 0=2→false, 1=2→false → true
    assert_bool(&db, "RETURN none(x IN [0, 1] WHERE x = 2) AS result", true).await;

    // none(x IN [2, 1] WHERE x=2): 2=2→true → false
    assert_bool(&db, "RETURN none(x IN [2, 1] WHERE x = 2) AS result", false).await;

    // Multiple nulls, no true: still null
    assert_null(&db, "RETURN none(x IN [null, null] WHERE x = 2) AS result").await;

    // null at different positions
    assert_null(&db, "RETURN none(x IN [1, null, 3] WHERE x = 2) AS result").await;

    // Definite true overrides null
    assert_bool(
        &db,
        "RETURN none(x IN [1, null, 2] WHERE x = 2) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_single_null_semantics() {
    let db = Uni::in_memory().build().await.unwrap();

    // single(x IN [null] WHERE x=2): predicate null → null
    assert_null(&db, "RETURN single(x IN [null] WHERE x = 2) AS result").await;

    // single(x IN [2, null] WHERE x=2): 1 true + null → null (could be 2nd true)
    assert_null(&db, "RETURN single(x IN [2, null] WHERE x = 2) AS result").await;

    // single(x IN [0, null] WHERE x=2): 0 true + null → null (could be 1 true)
    assert_null(&db, "RETURN single(x IN [0, null] WHERE x = 2) AS result").await;

    // single(x IN [2] WHERE x=2): exactly 1 true, no nulls → true
    assert_bool(&db, "RETURN single(x IN [2] WHERE x = 2) AS result", true).await;

    // single(x IN [2, 2] WHERE x=2): 2 true → false (definite)
    assert_bool(
        &db,
        "RETURN single(x IN [2, 2] WHERE x = 2) AS result",
        false,
    )
    .await;

    // single(x IN [2, 2, null] WHERE x=2): ≥2 true → false (even with null)
    assert_bool(
        &db,
        "RETURN single(x IN [2, 2, null] WHERE x = 2) AS result",
        false,
    )
    .await;

    // single(x IN [0, 1] WHERE x=2): 0 true, no nulls → false
    assert_bool(
        &db,
        "RETURN single(x IN [0, 1] WHERE x = 2) AS result",
        false,
    )
    .await;

    // single(x IN [0, 2, 1] WHERE x=2): exactly 1 true, no nulls → true
    assert_bool(
        &db,
        "RETURN single(x IN [0, 2, 1] WHERE x = 2) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_any_null_semantics() {
    let db = Uni::in_memory().build().await.unwrap();

    // any(x IN [null] WHERE x=2): null → null
    assert_null(&db, "RETURN any(x IN [null] WHERE x = 2) AS result").await;

    // any(x IN [2, null] WHERE x=2): 2=2→true → short-circuit true
    assert_bool(
        &db,
        "RETURN any(x IN [2, null] WHERE x = 2) AS result",
        true,
    )
    .await;

    // any(x IN [0, null] WHERE x=2): 0=2→false, null=2→null → null
    assert_null(&db, "RETURN any(x IN [0, null] WHERE x = 2) AS result").await;

    // any(x IN [0, 1] WHERE x=2): all false → false
    assert_bool(&db, "RETURN any(x IN [0, 1] WHERE x = 2) AS result", false).await;

    // any(x IN [0, 2] WHERE x=2): found true → true
    assert_bool(&db, "RETURN any(x IN [0, 2] WHERE x = 2) AS result", true).await;

    // Multiple nulls, no true → null
    assert_null(&db, "RETURN any(x IN [null, null] WHERE x = 2) AS result").await;

    // True overrides nulls
    assert_bool(
        &db,
        "RETURN any(x IN [null, 2, null] WHERE x = 2) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_all_null_semantics() {
    let db = Uni::in_memory().build().await.unwrap();

    // all(x IN [null] WHERE x=2): null → null
    assert_null(&db, "RETURN all(x IN [null] WHERE x = 2) AS result").await;

    // all(x IN [2, null] WHERE x=2): 2=2→true, null=2→null → null
    assert_null(&db, "RETURN all(x IN [2, null] WHERE x = 2) AS result").await;

    // all(x IN [0, null] WHERE x=2): 0=2→false → short-circuit false
    assert_bool(
        &db,
        "RETURN all(x IN [0, null] WHERE x = 2) AS result",
        false,
    )
    .await;

    // all(x IN [2, 2] WHERE x=2): all true → true
    assert_bool(&db, "RETURN all(x IN [2, 2] WHERE x = 2) AS result", true).await;

    // all(x IN [0, 1] WHERE x=2): has false → false
    assert_bool(&db, "RETURN all(x IN [0, 1] WHERE x = 2) AS result", false).await;

    // Multiple nulls, no false → null
    assert_null(
        &db,
        "RETURN all(x IN [2, null, null] WHERE x = 2) AS result",
    )
    .await;

    // False overrides nulls
    assert_bool(
        &db,
        "RETURN all(x IN [null, 0, null] WHERE x = 2) AS result",
        false,
    )
    .await;
}

// ============================================================================
// Phase 4: IS NULL / IS NOT NULL Predicates
// TCK: Quantifier1-4 scenarios [11]-[12]
// ============================================================================

#[tokio::test]
async fn test_none_is_null_predicate() {
    let db = Uni::in_memory().build().await.unwrap();

    // IS NULL on null element → true (collapses three-valued to two-valued)
    // none(x IN [null] WHERE x IS NULL) → predicate true → false
    assert_bool(
        &db,
        "RETURN none(x IN [null] WHERE x IS NULL) AS result",
        false,
    )
    .await;

    // none(x IN [1, null] WHERE x IS NULL) → null IS NULL → true → false
    assert_bool(
        &db,
        "RETURN none(x IN [1, null] WHERE x IS NULL) AS result",
        false,
    )
    .await;

    // none(x IN [1, 2] WHERE x IS NULL) → all false → true
    assert_bool(
        &db,
        "RETURN none(x IN [1, 2] WHERE x IS NULL) AS result",
        true,
    )
    .await;

    // IS NOT NULL
    // none(x IN [null] WHERE x IS NOT NULL) → false → true (no true pred)
    assert_bool(
        &db,
        "RETURN none(x IN [null] WHERE x IS NOT NULL) AS result",
        true,
    )
    .await;

    // none(x IN [1, null] WHERE x IS NOT NULL) → 1 IS NOT NULL → true → false
    assert_bool(
        &db,
        "RETURN none(x IN [1, null] WHERE x IS NOT NULL) AS result",
        false,
    )
    .await;

    // none(x IN [1, 2] WHERE x IS NOT NULL) → all true → false
    assert_bool(
        &db,
        "RETURN none(x IN [1, 2] WHERE x IS NOT NULL) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_single_is_null_predicate() {
    let db = Uni::in_memory().build().await.unwrap();

    // single(x IN [null] WHERE x IS NULL) → exactly 1 true → true
    assert_bool(
        &db,
        "RETURN single(x IN [null] WHERE x IS NULL) AS result",
        true,
    )
    .await;

    // single(x IN [0, null] WHERE x IS NULL) → exactly 1 null element → true
    assert_bool(
        &db,
        "RETURN single(x IN [0, null] WHERE x IS NULL) AS result",
        true,
    )
    .await;

    // single(x IN [null, null] WHERE x IS NULL) → 2 true → false
    assert_bool(
        &db,
        "RETURN single(x IN [null, null] WHERE x IS NULL) AS result",
        false,
    )
    .await;

    // single(x IN [1, 2] WHERE x IS NULL) → 0 true → false
    assert_bool(
        &db,
        "RETURN single(x IN [1, 2] WHERE x IS NULL) AS result",
        false,
    )
    .await;

    // IS NOT NULL
    assert_bool(
        &db,
        "RETURN single(x IN [null] WHERE x IS NOT NULL) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [1, null] WHERE x IS NOT NULL) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [1, 2] WHERE x IS NOT NULL) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_any_is_null_predicate() {
    let db = Uni::in_memory().build().await.unwrap();

    // any(x IN [null] WHERE x IS NULL) → true
    assert_bool(
        &db,
        "RETURN any(x IN [null] WHERE x IS NULL) AS result",
        true,
    )
    .await;

    // any(x IN [1, null] WHERE x IS NULL) → true
    assert_bool(
        &db,
        "RETURN any(x IN [1, null] WHERE x IS NULL) AS result",
        true,
    )
    .await;

    // any(x IN [1, 2] WHERE x IS NULL) → false
    assert_bool(
        &db,
        "RETURN any(x IN [1, 2] WHERE x IS NULL) AS result",
        false,
    )
    .await;

    // IS NOT NULL
    assert_bool(
        &db,
        "RETURN any(x IN [null, null] WHERE x IS NOT NULL) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [null, 1] WHERE x IS NOT NULL) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [1, 2] WHERE x IS NOT NULL) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_all_is_null_predicate() {
    let db = Uni::in_memory().build().await.unwrap();

    // all(x IN [null] WHERE x IS NULL) → true
    assert_bool(
        &db,
        "RETURN all(x IN [null] WHERE x IS NULL) AS result",
        true,
    )
    .await;

    // all(x IN [null, null] WHERE x IS NULL) → true
    assert_bool(
        &db,
        "RETURN all(x IN [null, null] WHERE x IS NULL) AS result",
        true,
    )
    .await;

    // all(x IN [1, null] WHERE x IS NULL) → false (1 IS NULL → false)
    assert_bool(
        &db,
        "RETURN all(x IN [1, null] WHERE x IS NULL) AS result",
        false,
    )
    .await;

    // all(x IN [1, 2] WHERE x IS NULL) → false
    assert_bool(
        &db,
        "RETURN all(x IN [1, 2] WHERE x IS NULL) AS result",
        false,
    )
    .await;

    // IS NOT NULL
    assert_bool(
        &db,
        "RETURN all(x IN [null] WHERE x IS NOT NULL) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN all(x IN [null, null] WHERE x IS NOT NULL) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN all(x IN [1, 2] WHERE x IS NOT NULL) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN all(x IN [1, null] WHERE x IS NOT NULL) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_mixed_null_is_null_multiple_elements() {
    let db = Uni::in_memory().build().await.unwrap();

    // Three-element lists with mixed null/non-null
    assert_bool(
        &db,
        "RETURN none(x IN [null, null, null] WHERE x IS NULL) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN all(x IN [null, null, null] WHERE x IS NULL) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [1, null, 2] WHERE x IS NULL) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN single(x IN [1, null, null] WHERE x IS NULL) AS result",
        false,
    )
    .await;
    assert_bool(
        &db,
        "RETURN any(x IN [1, 2, 3] WHERE x IS NULL) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_is_null_on_empty_list() {
    let db = Uni::in_memory().build().await.unwrap();

    // Empty list with IS NULL/IS NOT NULL predicates
    assert_bool(&db, "RETURN all(x IN [] WHERE x IS NULL) AS result", true).await;
    assert_bool(&db, "RETURN any(x IN [] WHERE x IS NULL) AS result", false).await;
    assert_bool(&db, "RETURN none(x IN [] WHERE x IS NULL) AS result", true).await;
    assert_bool(
        &db,
        "RETURN single(x IN [] WHERE x IS NULL) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_is_null_on_all_non_null_list() {
    let db = Uni::in_memory().build().await.unwrap();

    // All non-null elements with IS NULL → no matches
    assert_bool(
        &db,
        "RETURN none(x IN [1, 2, 3] WHERE x IS NULL) AS result",
        true,
    )
    .await;
    assert_bool(
        &db,
        "RETURN all(x IN [1, 2, 3] WHERE x IS NOT NULL) AS result",
        true,
    )
    .await;
}

// ============================================================================
// Phase 5: Nested Quantifiers
// TCK: Quantifier5-8 scenarios [1]-[2]
// ============================================================================

#[tokio::test]
async fn test_nested_quantifier_on_nested_lists() {
    let db = Uni::in_memory().build().await.unwrap();

    // none(x IN [['abc'],['abc','def']] WHERE none(y IN x WHERE y='abc'))
    // Inner: none(y IN ['abc'] WHERE y='abc') → false (found 'abc')
    // Inner: none(y IN ['abc','def'] WHERE y='abc') → false (found 'abc')
    // Outer: none(x IN [false, false] WHERE ...) → true (no true)
    assert_bool(
        &db,
        "RETURN none(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'abc')) AS result",
        true,
    )
    .await;

    // all(x IN [['abc'],['abc','def']] WHERE any(y IN x WHERE y='abc'))
    // Inner: any(y IN ['abc'] WHERE y='abc') → true
    // Inner: any(y IN ['abc','def'] WHERE y='abc') → true
    // Outer: all(x IN [true, true]) → true
    assert_bool(
        &db,
        "RETURN all(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'abc')) AS result",
        true,
    )
    .await;

    // single(x IN [[1,2,3], [4,5,6], [7,8,9]] WHERE all(y IN x WHERE y > 5))
    // Inner: all([1,2,3] > 5) → false
    // Inner: all([4,5,6] > 5) → false
    // Inner: all([7,8,9] > 5) → true
    // Outer: single(false, false, true) → true
    assert_bool(
        &db,
        "RETURN single(x IN [[1, 2, 3], [4, 5, 6], [7, 8, 9]] WHERE all(y IN x WHERE y > 5)) AS result",
        true,
    )
    .await;

    // any(x IN [[1], [2,3], [4,5,6]] WHERE single(y IN x WHERE y > 0))
    // Inner: single([1] > 0) → true (exactly 1)
    // Outer short-circuits → true
    assert_bool(
        &db,
        "RETURN any(x IN [[1], [2, 3], [4, 5, 6]] WHERE single(y IN x WHERE y > 0)) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_nested_quantifier_on_same_list() {
    let db = Uni::in_memory().build().await.unwrap();

    // single(x IN [1,2,3,4,5,6,7,8,9] WHERE none(y IN [1,2,3,4,5,6,7,8,9] WHERE x < y))
    // Only x=9 has no y > 9 → exactly 1 → true
    assert_bool(
        &db,
        "WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list RETURN single(x IN list WHERE none(y IN list WHERE x < y)) AS result",
        true,
    )
    .await;

    // single(x IN [1,2,3,4,5,6,7,8,9] WHERE none(y IN [1,2,3,4,5,6,7,8,9] WHERE y < x))
    // Only x=1 has no y < 1 → exactly 1 → true
    assert_bool(
        &db,
        "WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list RETURN single(x IN list WHERE none(y IN list WHERE y < x)) AS result",
        true,
    )
    .await;
}

#[tokio::test]
async fn test_nested_quantifier_all_any() {
    let db = Uni::in_memory().build().await.unwrap();

    // all(x IN [[1,2],[3,4]] WHERE any(y IN x WHERE y % 2 = 0))
    // Inner: any([1,2] even) → true (2 is even)
    // Inner: any([3,4] even) → true (4 is even)
    // Outer: all(true, true) → true
    assert_bool(
        &db,
        "RETURN all(x IN [[1, 2], [3, 4]] WHERE any(y IN x WHERE y % 2 = 0)) AS result",
        true,
    )
    .await;

    // all(x IN [[1,3],[5,7]] WHERE any(y IN x WHERE y % 2 = 0))
    // Inner: any([1,3] even) → false
    // Outer short-circuits → false
    assert_bool(
        &db,
        "RETURN all(x IN [[1, 3], [5, 7]] WHERE any(y IN x WHERE y % 2 = 0)) AS result",
        false,
    )
    .await;
}

#[tokio::test]
async fn test_nested_quantifier_with_nulls() {
    let db = Uni::in_memory().build().await.unwrap();

    // Nested quantifier where inner sees null
    // any(x IN [[null, 1], [2, 3]] WHERE all(y IN x WHERE y > 0))
    // Inner: all([null,1] > 0) → null (null=2→null, 1>0→true, has null no false → null)
    // Inner: all([2,3] > 0) → true
    // Outer: any(null, true) → true (found true)
    assert_bool(
        &db,
        "RETURN any(x IN [[null, 1], [2, 3]] WHERE all(y IN x WHERE y > 0)) AS result",
        true,
    )
    .await;

    // none(x IN [[null, 1], [2, 3]] WHERE all(y IN x WHERE y > 0))
    // Inner: all([null,1]) → null, all([2,3]) → true
    // Outer: none(null, true) → false (found true)
    assert_bool(
        &db,
        "RETURN none(x IN [[null, 1], [2, 3]] WHERE all(y IN x WHERE y > 0)) AS result",
        false,
    )
    .await;
}

// ============================================================================
// Phase 6: Equivalence Identities
// TCK: Quantifier5-8 scenarios [3]-[5]
// ============================================================================

#[tokio::test]
async fn test_none_equivalences() {
    let db = Uni::in_memory().build().await.unwrap();

    // none(P) = NOT any(P)
    // none(P) = all(NOT P)
    // Test with predicate x = 2 on [1, 2, 3, 4, 5, 6, 7, 8, 9]
    let list = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

    // none(x=2) should be false (2 is in the list)
    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x = 2) = (NOT any(x IN {list} WHERE x = 2)) AS result"
        ),
        true,
    )
    .await;

    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x = 2) = all(x IN {list} WHERE NOT (x = 2)) AS result"
        ),
        true,
    )
    .await;

    // With predicate x % 2 = 0 (evens)
    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x % 2 = 0) = (NOT any(x IN {list} WHERE x % 2 = 0)) AS result"
        ),
        true,
    )
    .await;

    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x % 2 = 0) = all(x IN {list} WHERE NOT (x % 2 = 0)) AS result"
        ),
        true,
    )
    .await;

    // With predicate x > 100 (none satisfy → none=true)
    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x > 100) = (NOT any(x IN {list} WHERE x > 100)) AS result"
        ),
        true,
    )
    .await;

    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x > 100) = all(x IN {list} WHERE NOT (x > 100)) AS result"
        ),
        true,
    )
    .await;
}

#[tokio::test]
async fn test_any_equivalences() {
    let db = Uni::in_memory().build().await.unwrap();

    let list = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

    // any(P) = NOT none(P)
    assert_bool(
        &db,
        &format!(
            "RETURN any(x IN {list} WHERE x = 2) = (NOT none(x IN {list} WHERE x = 2)) AS result"
        ),
        true,
    )
    .await;

    // any(P) = NOT all(NOT P)
    assert_bool(
        &db,
        &format!(
            "RETURN any(x IN {list} WHERE x = 2) = (NOT all(x IN {list} WHERE NOT (x = 2))) AS result"
        ),
        true,
    )
    .await;

    // With x % 3 = 0
    assert_bool(
        &db,
        &format!(
            "RETURN any(x IN {list} WHERE x % 3 = 0) = (NOT none(x IN {list} WHERE x % 3 = 0)) AS result"
        ),
        true,
    )
    .await;

    assert_bool(
        &db,
        &format!(
            "RETURN any(x IN {list} WHERE x % 3 = 0) = (NOT all(x IN {list} WHERE NOT (x % 3 = 0))) AS result"
        ),
        true,
    )
    .await;

    // With x > 100 (no match → any=false)
    assert_bool(
        &db,
        &format!(
            "RETURN any(x IN {list} WHERE x > 100) = (NOT none(x IN {list} WHERE x > 100)) AS result"
        ),
        true,
    )
    .await;
}

#[tokio::test]
async fn test_all_equivalences() {
    let db = Uni::in_memory().build().await.unwrap();

    let list = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

    // all(P) = none(NOT P)
    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x > 0) = none(x IN {list} WHERE NOT (x > 0)) AS result"
        ),
        true,
    )
    .await;

    // all(P) = NOT any(NOT P)
    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x > 0) = (NOT any(x IN {list} WHERE NOT (x > 0))) AS result"
        ),
        true,
    )
    .await;

    // With x < 7 (not all satisfy → all=false)
    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x < 7) = none(x IN {list} WHERE NOT (x < 7)) AS result"
        ),
        true,
    )
    .await;

    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x < 7) = (NOT any(x IN {list} WHERE NOT (x < 7))) AS result"
        ),
        true,
    )
    .await;

    // With x >= 3 (some satisfy → all=false)
    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x >= 3) = none(x IN {list} WHERE NOT (x >= 3)) AS result"
        ),
        true,
    )
    .await;

    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x >= 3) = (NOT any(x IN {list} WHERE NOT (x >= 3))) AS result"
        ),
        true,
    )
    .await;
}

#[tokio::test]
async fn test_equivalence_identities_cross_check() {
    let db = Uni::in_memory().build().await.unwrap();

    // Cross-verify: for multiple predicates, all 4 equivalences hold simultaneously
    let list = "[1, 2, 3, 4, 5]";

    // Predicate: x = 3
    // none(x=3) should be false, any(x=3) should be true
    assert_bool(
        &db,
        &format!("RETURN none(x IN {list} WHERE x = 3) AS result"),
        false,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN any(x IN {list} WHERE x = 3) AS result"),
        true,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN all(x IN {list} WHERE x = 3) AS result"),
        false,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN single(x IN {list} WHERE x = 3) AS result"),
        true,
    )
    .await;

    // Predicate: x > 0 (all satisfy)
    assert_bool(
        &db,
        &format!("RETURN none(x IN {list} WHERE x > 0) AS result"),
        false,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN any(x IN {list} WHERE x > 0) AS result"),
        true,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN all(x IN {list} WHERE x > 0) AS result"),
        true,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN single(x IN {list} WHERE x > 0) AS result"),
        false,
    )
    .await;

    // Predicate: x > 100 (none satisfy)
    assert_bool(
        &db,
        &format!("RETURN none(x IN {list} WHERE x > 100) AS result"),
        true,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN any(x IN {list} WHERE x > 100) AS result"),
        false,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN all(x IN {list} WHERE x > 100) AS result"),
        false,
    )
    .await;
    assert_bool(
        &db,
        &format!("RETURN single(x IN {list} WHERE x > 100) AS result"),
        false,
    )
    .await;
}

// ============================================================================
// Phase 7: Size-Based Equivalences + Implication
// TCK: Quantifier5-8 scenario [5]/[3]/[6], Quantifier7[3]
// ============================================================================

#[tokio::test]
async fn test_none_size_equivalence() {
    let db = Uni::in_memory().build().await.unwrap();

    // none(P) = (size([x IN L WHERE P | x]) = 0)
    let list = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

    // Predicate x > 100 → none true, filter size = 0
    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x > 100) = (size([x IN {list} WHERE x > 100 | x]) = 0) AS result"
        ),
        true,
    )
    .await;

    // Predicate x = 5 → none false, filter size = 1
    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x = 5) = (size([x IN {list} WHERE x = 5 | x]) = 0) AS result"
        ),
        true,
    )
    .await;

    // Predicate x % 2 = 0 → none false, filter size = 4
    assert_bool(
        &db,
        &format!(
            "RETURN none(x IN {list} WHERE x % 2 = 0) = (size([x IN {list} WHERE x % 2 = 0 | x]) = 0) AS result"
        ),
        true,
    )
    .await;
}

#[tokio::test]
async fn test_single_size_equivalence() {
    let db = Uni::in_memory().build().await.unwrap();

    // single(P) = (size([x IN L WHERE P | x]) = 1)
    let list = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

    // Predicate x = 5 → single true, filter size = 1
    assert_bool(
        &db,
        &format!(
            "RETURN single(x IN {list} WHERE x = 5) = (size([x IN {list} WHERE x = 5 | x]) = 1) AS result"
        ),
        true,
    )
    .await;

    // Predicate x % 2 = 0 → single false, filter size = 4
    assert_bool(
        &db,
        &format!(
            "RETURN single(x IN {list} WHERE x % 2 = 0) = (size([x IN {list} WHERE x % 2 = 0 | x]) = 1) AS result"
        ),
        true,
    )
    .await;

    // Predicate x > 100 → single false, filter size = 0
    assert_bool(
        &db,
        &format!(
            "RETURN single(x IN {list} WHERE x > 100) = (size([x IN {list} WHERE x > 100 | x]) = 1) AS result"
        ),
        true,
    )
    .await;
}

#[tokio::test]
async fn test_any_size_equivalence() {
    let db = Uni::in_memory().build().await.unwrap();

    // any(P) = (size([x IN L WHERE P | x]) > 0)
    let list = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

    // Predicate x = 5 → any true, filter size = 1
    assert_bool(
        &db,
        &format!(
            "RETURN any(x IN {list} WHERE x = 5) = (size([x IN {list} WHERE x = 5 | x]) > 0) AS result"
        ),
        true,
    )
    .await;

    // Predicate x > 100 → any false, filter size = 0
    assert_bool(
        &db,
        &format!(
            "RETURN any(x IN {list} WHERE x > 100) = (size([x IN {list} WHERE x > 100 | x]) > 0) AS result"
        ),
        true,
    )
    .await;

    // Predicate x % 3 = 0 → any true, filter size = 3
    assert_bool(
        &db,
        &format!(
            "RETURN any(x IN {list} WHERE x % 3 = 0) = (size([x IN {list} WHERE x % 3 = 0 | x]) > 0) AS result"
        ),
        true,
    )
    .await;
}

#[tokio::test]
async fn test_all_size_equivalence() {
    let db = Uni::in_memory().build().await.unwrap();

    // all(P) = (size([x IN L WHERE P | x]) = size(L))
    let list = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

    // Predicate x > 0 → all true, filter size = 9 = size(list)
    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x > 0) = (size([x IN {list} WHERE x > 0 | x]) = size({list})) AS result"
        ),
        true,
    )
    .await;

    // Predicate x < 7 → all false, filter size = 6 ≠ 9
    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x < 7) = (size([x IN {list} WHERE x < 7 | x]) = size({list})) AS result"
        ),
        true,
    )
    .await;

    // Predicate x > 100 → all false, filter size = 0 ≠ 9
    assert_bool(
        &db,
        &format!(
            "RETURN all(x IN {list} WHERE x > 100) = (size([x IN {list} WHERE x > 100 | x]) = size({list})) AS result"
        ),
        true,
    )
    .await;
}

#[tokio::test]
async fn test_implication_single_all_implies_any() {
    let db = Uni::in_memory().build().await.unwrap();

    let list = "[1, 2, 3, 4, 5, 6, 7, 8, 9]";

    // (single(P) OR all(P)) implies any(P)
    // In Cypher, implication A → B is equivalent to (NOT A) OR B
    // So: NOT (single(P) OR all(P)) OR any(P)

    // Predicate x = 5 → single=true, all=false, any=true
    // (true OR false) → true implies true → true
    assert_bool(
        &db,
        &format!(
            "RETURN (NOT (single(x IN {list} WHERE x = 5) OR all(x IN {list} WHERE x = 5)) OR any(x IN {list} WHERE x = 5)) AS result"
        ),
        true,
    )
    .await;

    // Predicate x > 0 → single=false, all=true, any=true
    // (false OR true) → true implies true → true
    assert_bool(
        &db,
        &format!(
            "RETURN (NOT (single(x IN {list} WHERE x > 0) OR all(x IN {list} WHERE x > 0)) OR any(x IN {list} WHERE x > 0)) AS result"
        ),
        true,
    )
    .await;

    // Predicate x > 100 → single=false, all=false, any=false
    // (false OR false) → false; NOT false → true; true OR false → true (vacuously true)
    assert_bool(
        &db,
        &format!(
            "RETURN (NOT (single(x IN {list} WHERE x > 100) OR all(x IN {list} WHERE x > 100)) OR any(x IN {list} WHERE x > 100)) AS result"
        ),
        true,
    )
    .await;

    // Predicate x % 2 = 0 → single=false, all=false, any=true
    // (false OR false) → false; NOT false → true; true OR true → true
    assert_bool(
        &db,
        &format!(
            "RETURN (NOT (single(x IN {list} WHERE x % 2 = 0) OR all(x IN {list} WHERE x % 2 = 0)) OR any(x IN {list} WHERE x % 2 = 0)) AS result"
        ),
        true,
    )
    .await;

    // Predicate x = 1 → single=true, all=false, any=true
    // (true OR false) → true implies true → true
    assert_bool(
        &db,
        &format!(
            "RETURN (NOT (single(x IN {list} WHERE x = 1) OR all(x IN {list} WHERE x = 1)) OR any(x IN {list} WHERE x = 1)) AS result"
        ),
        true,
    )
    .await;
}
