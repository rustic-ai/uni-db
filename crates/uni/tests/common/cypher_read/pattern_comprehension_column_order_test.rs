//! A multi-hop pattern comprehension mixing edge and vertex properties.
//!
//! `PatternComprehensionExecExpr` builds its inner batch one step at a time —
//! this step's vertex properties, then this step's edge properties — while
//! `build_inner_schema` used to declare *every* step's vertex properties and
//! then every step's edge properties. The two agree as long as at most one step
//! contributes a property, which is what every existing test did. With an edge
//! property on an early hop and a vertex property on a later one they diverge,
//! and because every property column is `LargeBinary`,
//! `RecordBatch::try_new` accepts the mismatch rather than rejecting it:
//!
//! ```text
//! [(n)-[r:R1]->(m:P)-[:R2]->(x:Q) | r.since + '/' + x.tag]  ->  "TAGGED/YEAR"
//! ```
//!
//! The values were swapped, silently. Nothing errored and no shorter query
//! could show it, because with one property column there is nothing to swap —
//! which is why the two-property form is the one worth pinning.

use uni_db::{Uni, Value};

/// `a -[:R1 {since:'YEAR'}]-> b -[:R2]-> (q:Q {tag:'TAGGED'})`.
async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE LABEL Q (tag STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE R1 (since STRING) FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE EDGE TYPE R2 FROM P TO Q").await.unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("CREATE (:Q {tag:'TAGGED'})").await.unwrap();
    tx.execute("MATCH (a:P {name:'a'}), (b:P {name:'b'}) CREATE (a)-[:R1 {since:'YEAR'}]->(b)")
        .await
        .unwrap();
    tx.execute("MATCH (b:P {name:'b'}), (q:Q) CREATE (b)-[:R2]->(q)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

fn first_list_item(v: &Value) -> String {
    match v {
        Value::List(items) => match items.first() {
            Some(Value::String(s)) => s.clone(),
            other => panic!("expected a string item, got {other:?}"),
        },
        other => panic!("expected a list, got {other:?}"),
    }
}

/// The discriminating shape: both properties in the same comprehension, one
/// from an edge on hop 1 and one from a node on hop 2.
#[tokio::test]
async fn edge_and_vertex_properties_across_hops_do_not_swap() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (n:P {name:'a'}) \
             RETURN [(n)-[r:R1]->(m:P)-[:R2]->(x:Q) | r.since + '/' + x.tag] AS both",
        )
        .await
        .unwrap();
    assert_eq!(first_list_item(&r.rows()[0].values()[0]), "YEAR/TAGGED");
}

/// The same two properties projected as a list, so a swap shows up as the
/// wrong element rather than a concatenation.
#[tokio::test]
async fn edge_and_vertex_properties_keep_their_positions() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (n:P {name:'a'}) \
             RETURN [(n)-[r:R1]->(m:P)-[:R2]->(x:Q) | r.since] AS since, \
                    [(n)-[r:R1]->(m:P)-[:R2]->(x:Q) | x.tag] AS tag",
        )
        .await
        .unwrap();
    assert_eq!(first_list_item(&r.rows()[0].values()[0]), "YEAR");
    assert_eq!(first_list_item(&r.rows()[0].values()[1]), "TAGGED");
}

/// Two vertex properties on different hops — the same ordering contract from
/// the other side.
#[tokio::test]
async fn vertex_properties_on_two_hops_keep_their_positions() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (n:P {name:'a'}) \
             RETURN [(n)-[:R1]->(m:P)-[:R2]->(x:Q) | m.name + '/' + x.tag] AS both",
        )
        .await
        .unwrap();
    assert_eq!(first_list_item(&r.rows()[0].values()[0]), "b/TAGGED");
}
