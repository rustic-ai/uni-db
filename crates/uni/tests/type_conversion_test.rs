use uni_db::Uni;
use uni_query::Value;

#[tokio::test]
async fn test_to_integer_conversion() {
    let db = Uni::in_memory().build().await.unwrap();

    // Test float to integer
    let result = db.query("RETURN toInteger(2.9) AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Int(2)); // Cypher toInteger(float) truncates

    // Test string to integer
    let result = db.query("RETURN toInteger('42') AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Int(42));
}

#[tokio::test]
async fn test_to_integer_mixed_list() {
    let db = Uni::in_memory().build().await.unwrap();
    // TCK TypeConversion2 #3
    // UNWIND [2, 2.9, '3'] AS x RETURN toInteger(x) AS val
    let result = db
        .query("UNWIND [2, 2.9, '3'] AS x RETURN toInteger(x) AS val")
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].value("val").unwrap(), &Value::Int(2));
    assert_eq!(result.rows[1].value("val").unwrap(), &Value::Int(2));
    assert_eq!(result.rows[2].value("val").unwrap(), &Value::Int(3));
}

#[tokio::test]
async fn test_to_boolean_invalid_types() {
    let db = Uni::in_memory().build().await.unwrap();
    // TCK TypeConversion1 #5
    // RETURN toBoolean([true]) should return error
    let result = db.query("RETURN toBoolean([true]) AS val").await;
    assert!(result.is_err());
    let err = result.err().unwrap().to_string();
    assert!(err.contains("InvalidArgumentValue"));
}
