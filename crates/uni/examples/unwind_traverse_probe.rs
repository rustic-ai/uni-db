//! Does traversal from an UNWIND-produced entity behave like traversal from a
//! scan-bound one? LDBC IC6 aborts the process on a 14 TB allocation in the
//! former shape and answers correctly in the latter.
//!
//! Kept: #184's pruning half is fixed, but the unbounded `MutableArrayData`
//! allocation underneath it is not, so this stays the instrument for it.
use uni_db::Uni;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (name STRING)").await?;
    tx.execute("CREATE LABEL Post (title STRING)").await?;
    tx.execute("CREATE EDGE TYPE HAS_CREATOR FROM Post TO P")
        .await?;
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await?;
    tx.execute("CREATE (:Post {title:'p1'}), (:Post {title:'p2'}), (:Post {title:'p3'})")
        .await?;
    // a has 2 posts, b has 1 → 3 in total.
    tx.execute("MATCH (a:P {name:'a'}), (p:Post {title:'p1'}) CREATE (p)-[:HAS_CREATOR]->(a)")
        .await?;
    tx.execute("MATCH (a:P {name:'a'}), (p:Post {title:'p2'}) CREATE (p)-[:HAS_CREATOR]->(a)")
        .await?;
    tx.execute("MATCH (b:P {name:'b'}), (p:Post {title:'p3'}) CREATE (p)-[:HAS_CREATOR]->(b)")
        .await?;
    tx.commit().await?;

    for (label, q) in [
        (
            "baseline: scan-bound",
            "MATCH (f:P) MATCH (f)<-[:HAS_CREATOR]-(post:Post) RETURN count(post)",
        ),
        (
            "WITH DISTINCT",
            "MATCH (f:P) WITH DISTINCT f MATCH (f)<-[:HAS_CREATOR]-(post:Post) RETURN count(post)",
        ),
        (
            "collect+UNWIND",
            "MATCH (f:P) WITH collect(DISTINCT f) AS fs UNWIND fs AS f MATCH (f)<-[:HAS_CREATOR]-(post:Post) RETURN count(post)",
        ),
        (
            "UNWIND round-trip",
            "MATCH (f:P) WITH collect(DISTINCT f) AS fs UNWIND fs AS f RETURN count(f)",
        ),
        (
            "size() of collect",
            "MATCH (f:P) WITH collect(DISTINCT f) AS fs RETURN size(fs)",
        ),
    ] {
        match db.session().query(q).await {
            Ok(r) => println!(
                "{label:<22} -> {:?}",
                r.rows().first().map(|x| x.values().to_vec())
            ),
            Err(e) => println!("{label:<22} -> ERROR {e}"),
        }
    }
    println!("expected 3 for every count, 2 for size()");
    Ok(())
}
