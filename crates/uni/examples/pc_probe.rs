//! Pattern-comprehension shape matrix. Kept: the map-literal and
//! map-projection forms are still open (see
//! `pattern_comprehension_entity_test`'s two `#[ignore]`d cases).
use uni_db::Uni;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P").await?;
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'}), (:P {name:'c'})")
        .await?;
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await?;
    tx.commit().await?;
    for (tag, q) in [
        (
            "anchored (works today)",
            "MATCH (n:P) RETURN n.name AS n, [(n)-[:KNOWS]->(b) | b.name] AS l",
        ),
        (
            "uncorrelated, no outer MATCH",
            "RETURN [(a:P)-[:KNOWS]->(b:P) | a.name] AS l",
        ),
        (
            "uncorrelated, with outer MATCH",
            "MATCH (n:P) RETURN n.name AS n, [(a:P)-[:KNOWS]->(b:P) | a.name] AS l",
        ),
        (
            "correlated non-equality",
            "MATCH (n:P) RETURN n.name AS n, [(a:P)-[:KNOWS]->(b:P) WHERE a.name > n.name | a.name] AS l",
        ),
        (
            "uncorrelated inside size()",
            "MATCH (n:P) RETURN n.name AS n, size([(a:P)-[:KNOWS]->(b:P) | 1]) AS c",
        ),
        // --- the Phase A premise: do UNANCHORED pattern PREDICATES work today? ---
        (
            "PRED anchored",
            "MATCH (n:P) RETURN n.name AS n, (n)-[:KNOWS]->(:P) AS f",
        ),
        (
            "PRED unanchored, no outer",
            "RETURN (:P)-[:KNOWS]->(:P) AS f",
        ),
        (
            "PRED unanchored, fresh vars",
            "MATCH (n:P) RETURN n.name AS n, ((a:P)-[:KNOWS]->(b:P)) AS f",
        ),
        (
            "PRED unanchored in WHERE",
            "MATCH (n:P) WHERE (:P)-[:KNOWS]->(:P) RETURN n.name AS n",
        ),
        (
            "COLLECT subquery",
            "MATCH (n:P) RETURN n.name AS n, COLLECT { MATCH (a:P)-[:KNOWS]->(b:P) RETURN a.name } AS l",
        ),
    ] {
        match db.session().query(q).await {
            Ok(r) => println!(
                "\n### {tag}\n    OK  {:?}",
                r.rows()
                    .iter()
                    .map(|x| x.values().to_vec())
                    .collect::<Vec<_>>()
            ),
            Err(e) => println!("\n### {tag}\n    ERR {e}"),
        }
    }
    Ok(())
}
