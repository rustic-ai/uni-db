use uni_db::Uni;
use uni_query::Value;

#[tokio::test]
async fn test_dynamic_map_access() {
    let db = Uni::in_memory().build().await.unwrap();

    // Static literal access (should already work via dot access, but let's test bracket)
    let result = db.query("RETURN {a: 1}['a'] AS val").await.unwrap();
    assert_eq!(result.rows[0].value("val").unwrap(), &Value::Int(1));

    // Dynamic access via expression
    let result = db
        .query("WITH 'a' AS key RETURN {a: 42}[key] AS val")
        .await
        .unwrap();
    assert_eq!(result.rows[0].value("val").unwrap(), &Value::Int(42));

    // Forced DataFusion path via scan
    db.query("CREATE (:Person {name: 'Alice'})").await.unwrap();

    // Check direct property access
    let result = db
        .query("MATCH (n:Person) RETURN n.name AS val")
        .await
        .unwrap();
    assert_eq!(
        result.rows[0].value("val").unwrap(),
        &Value::String("Alice".to_string())
    );

    // Check bracket access
    let result = db
        .query("MATCH (n:Person) RETURN n['name'] AS val")
        .await
        .unwrap();
    assert_eq!(
        result.rows[0].value("val").unwrap(),
        &Value::String("Alice".to_string())
    );
}

#[tokio::test]
async fn test_keys_function_structural() {
    let db = Uni::in_memory().build().await.unwrap();

    // keys() on Map literal
    let result = db.query("RETURN keys({a: 1, b: 2}) AS k").await.unwrap();
    let val = result.rows[0].value("k").unwrap();
    if let Value::List(l) = val {
        assert!(l.contains(&Value::String("a".to_string())));
        assert!(l.contains(&Value::String("b".to_string())));
    } else {
        panic!("Expected List, got {:?}", val);
    }
}

#[tokio::test]
async fn test_keys_function_node() {
    let db = Uni::in_memory().build().await.unwrap();

    db.query("CREATE (n:Person {name: 'Alice', age: 30})")
        .await
        .unwrap();
    let result = db
        .query("MATCH (n:Person) RETURN keys(n) AS k")
        .await
        .unwrap();
    let val = result.rows[0].value("k").unwrap();

    if let Value::List(l) = val {
        println!("Keys: {:?}", l);
        // Should contain name and age
        assert_eq!(l.len(), 2);
        // Order should be sorted (age, name)
        assert_eq!(l[0], Value::String("age".to_string()));
        assert_eq!(l[1], Value::String("name".to_string()));
    } else {
        panic!("Expected List, got {:?}", val);
    }
}

#[tokio::test]
async fn test_dynamic_list_access() {
    let db = Uni::in_memory().build().await.unwrap();

    // Positive index
    let result = db.query("RETURN [1, 2, 3][1] AS val").await.unwrap();
    assert_eq!(result.rows[0].value("val").unwrap(), &Value::Int(2));

    // Negative index (Cypher: -1 is last)
    let result = db.query("RETURN [1, 2, 3][-1] AS val").await.unwrap();
    assert_eq!(result.rows[0].value("val").unwrap(), &Value::Int(3));

    // Out of bounds (Cypher: returns Null)
    let result = db.query("RETURN [1, 2, 3][10] AS val").await.unwrap();
    assert_eq!(result.rows[0].value("val").unwrap(), &Value::Null);
}
