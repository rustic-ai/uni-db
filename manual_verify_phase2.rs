use uni_db::Uni;

#[tokio::main]
async fn main() {
    let db = Uni::in_memory().build().await.unwrap();
    
    println!("1. Verify Mixed Numeric Equality: RETURN 1 = 1.0");
    let result = db.query("RETURN 1 = 1.0 AS eq").await.unwrap();
    println!("Result: {:?}", result.rows[0].value("eq").unwrap());
    
    println!("
2. Verify List Comparison: RETURN [1, 2] < [1, 3]");
    let result = db.query("RETURN [1, 2] < [1, 3] AS lt").await.unwrap();
    println!("Result: {:?}", result.rows[0].value("lt").unwrap());
    
    println!("
3. Verify NOT null: RETURN NOT null");
    let result = db.query("RETURN NOT null AS n").await.unwrap();
    println!("Result: {:?}", result.rows[0].value("n").unwrap());
    
    println!("
4. Verify toInteger on mixed list: UNWIND [2, 2.9, '3'] AS x RETURN toInteger(x) AS i");
    let result = db.query("UNWIND [2, 2.9, '3'] AS x RETURN toInteger(x) AS i").await.unwrap();
    for row in result.rows {
        println!("Result: {:?}", row.value("i").unwrap());
    }
    
    println!("
5. Verify Error message: RETURN NOT 1");
    let result = db.query("RETURN NOT 1").await;
    match result {
        Ok(_) => println!("Result: SUCCESS (Unexpected!)"),
        Err(e) => println!("Result: ERROR: {}", e),
    }
}
