use uni_db::Uni;
use uni_query::Value;

#[tokio::test]
async fn test_numeric_equality() {
    let db = Uni::in_memory().build().await.unwrap();

    // TCK Comparison1 #1
    let result = db.query("RETURN 1 = 1.0 AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Bool(true));

    let result = db.query("RETURN 1.0 = 1 AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Bool(true));
}

#[tokio::test]
async fn test_cross_type_equality() {
    let db = Uni::in_memory().build().await.unwrap();

    // Per CIP2016-06-14: Eq on incompatible types returns false (not null) when neither
    // operand is null — we *know* they are not equal, so null ("unknown") is wrong.
    // Null propagates only when an operand IS null (handled separately).
    let result = db.query("RETURN 1 = '1' AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Bool(false));

    // NotEq on incompatible types → true (definitively not equal).
    let result = db.query("RETURN 1 <> '1' AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Bool(true));

    // Ordering on incompatible types → null (order is undefined).
    let result = db.query("RETURN 1 < '1' AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Null);

    // Null operand still propagates null.
    let result = db.query("RETURN null = 1 AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Null);
}

#[tokio::test]
async fn test_list_comparison() {
    let db = Uni::in_memory().build().await.unwrap();

    // TCK Comparison2 Scenario 1
    let result = db.query("RETURN [1, 2] < [1, 3] AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Bool(true));

    let result = db.query("RETURN [1, 2] = [1, 2.0] AS val").await.unwrap();
    let val = result.rows[0].value("val").unwrap();
    assert_eq!(val, &Value::Bool(true));
}
