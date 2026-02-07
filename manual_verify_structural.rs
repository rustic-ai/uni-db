use uni_db::Uni;
use uni_query::Value;

#[tokio::main]
async fn main() {
    let db = Uni::in_memory().build().await.unwrap();
    
    println!("1. Verify Dynamic Map Access: CREATE (n {foo: 'bar'}) RETURN n['foo']");
    db.query("CREATE (:Person {foo: 'bar'})").await.unwrap();
    let result = db.query("MATCH (n:Person) RETURN n['foo'] AS val").await.unwrap();
    println!("Result: {:?}", result.rows[0].value("val").unwrap());
    
    println!("
2. Verify keys(n) on node with system properties: MATCH (n) RETURN keys(n)");
    let result = db.query("MATCH (n:Person) RETURN keys(n) AS k").await.unwrap();
    println!("Result: {:?}", result.rows[0].value("k").unwrap());
    
    println!("
3. Verify Schemaless Structural Return: MATCH (n) RETURN n");
    let result = db.query("MATCH (n:Person) RETURN n AS node").await.unwrap();
    println!("Result: {:?}", result.rows[0].value("node").unwrap());
}
