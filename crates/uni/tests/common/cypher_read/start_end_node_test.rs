//! `startNode(r)` / `endNode(r)` on a relationship bound by a MATCH traversal.
//!
//! These are standard openCypher functions and the queries below are valid. They
//! work when the relationship comes from `MERGE`/`CREATE` and fail when it comes
//! from a `MATCH` traversal:
//!
//! ```text
//! MATCH ()-[e:KNOWS]->() RETURN startNode(e).name
//! Schema error: No field named e.
//! Valid fields are "_anon_0._vid", …, "e._eid", "e._type".
//! ```
//!
//! The relationship *is* bound — `e._eid` and `e._type` are right there in the
//! schema — but the planner reaches for a bare `e` column the traversal never
//! produces. It is not the property access: `startNode(e)` alone and
//! `id(startNode(e))` fail identically.
//!
//! The openCypher TCK exercises these functions in exactly one scenario,
//! `Merge5` [11] (`clauses/merge/Merge5.feature:219`), where the relationship is
//! bound by `MERGE`. That scenario passes. So the suite covers the feature in the
//! one context where it works and cannot see the context where it does not — the
//! same single-context blind spot that hid unanchored pattern comprehensions.
//!
//! Tracked as #187. The tests below are `#[ignore]`d rather than deleted: they
//! state the intended behaviour, run on demand with `--run-ignored all`, and turn
//! green when the gap is closed.
//!
//! LDBC SNB Interactive IC14 correlates with
//! `a.id = startNode(r).id AND b.id = endNode(r).id` over relationships collected
//! from a matched path, so it cannot execute until this is fixed — independently
//! of anything to do with pattern-comprehension anchoring.

use uni_db::{Uni, Value};

/// `a` -[:KNOWS]-> `b`.
async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// The context the TCK covers, kept as the control: this passes today, so a
/// failure here would mean a regression rather than the known gap.
#[tokio::test]
async fn start_and_end_node_work_on_a_merge_bound_relationship() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    let rows = tx
        .query_with(
            "CREATE (x {id: 2}), (y {id: 1}) MERGE (x)-[r:R]-(y) \
             RETURN startNode(r).id AS s, endNode(r).id AS e",
        )
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(rows.rows()[0].values()[0], Value::Int(2));
    assert_eq!(rows.rows()[0].values()[1], Value::Int(1));
}

#[tokio::test]
#[ignore = "#187: startNode/endNode fail on a MATCH-bound relationship"]
async fn start_node_property_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() RETURN startNode(e).name AS s, endNode(e).name AS t")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::String("b".to_string()));
}

/// Not a property-access problem: returning the node itself fails the same way.
#[tokio::test]
#[ignore = "#187: startNode/endNode fail on a MATCH-bound relationship"]
async fn start_node_whole_entity_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() RETURN startNode(e) AS s")
        .await
        .unwrap();
    assert!(matches!(r.rows()[0].values()[0], Value::Node(_)));
}

/// Nor is it about materializing properties: `id()` of the endpoint fails too.
#[tokio::test]
#[ignore = "#187: startNode/endNode fail on a MATCH-bound relationship"]
async fn id_of_start_node_on_a_match_bound_relationship() {
    let db = fixture().await;
    let direct = db
        .session()
        .query("MATCH (n:P {name:'a'}) RETURN id(n)")
        .await
        .unwrap();
    let via_edge = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() RETURN id(startNode(e))")
        .await
        .unwrap();
    assert_eq!(via_edge.rows()[0].values()[0], direct.rows()[0].values()[0]);
}

/// Carrying the relationship through a `WITH` does not help; the error only
/// changes shape.
#[tokio::test]
#[ignore = "#187: startNode/endNode fail on a MATCH-bound relationship"]
async fn start_node_after_a_with_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() WITH e AS rel RETURN startNode(rel).name AS s")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
}
