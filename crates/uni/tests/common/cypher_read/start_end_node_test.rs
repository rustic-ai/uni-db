//! `startNode(r)` / `endNode(r)` on a relationship bound by a MATCH traversal.
//!
//! These are standard openCypher functions and the queries below are valid.
//! They used to fail whenever the relationship came from a `MATCH` traversal
//! rather than from `MERGE`/`CREATE`:
//!
//! ```text
//! MATCH ()-[e:KNOWS]->() RETURN startNode(e).name
//! Schema error: No field named e.
//! Valid fields are "_anon_0._vid", …, "e._eid", "e._type".
//! ```
//!
//! That error message is also the answer. #187 proposed that the endpoint VIDs
//! were already on the edge columns and only needed resolving; the schema it
//! prints says otherwise — there is no `e._src_vid`. What *is* there is
//! `_anon_0` and `_anon_1`: the traversal's own endpoint variables. For a
//! single-hop traversal in a known direction, `startNode(e)` is not a value to
//! compute at all, it is a variable already in scope, so the planner rewrites
//! it to that variable (`resolve_traversal_endpoints`). Doing it in the logical
//! plan rather than at DataFusion translation time is what keeps
//! `startNode(e).name` narrowing to one column instead of materialising the
//! whole endpoint.
//!
//! The openCypher TCK exercises these functions in exactly one scenario,
//! `Merge5` [11] (`clauses/merge/Merge5.feature:219`), where the relationship is
//! bound by `MERGE`. That scenario passed throughout. So the suite covered the
//! feature in the one context where it worked and could not see the context
//! where it did not — the same single-context blind spot that hid unanchored
//! pattern comprehensions. The MERGE case is kept below as a control, so a
//! failure there is a regression rather than the old gap.
//!
//! Two shapes are still open, each `#[ignore]`d against the issue that tracks
//! it. Once a `WITH` drops the endpoint variables from scope the relationship
//! value is all that is left, and turning its `_src_vid` back into a node with
//! properties needs a lookup against the vertex table that a scalar UDF cannot
//! do — the remaining work under #187. An undirected relationship is #188: the
//! rewrite below is sound only because a directed single hop makes the start
//! node statically knowable, and `-[e]-` makes it a per-row fact instead.

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

/// The one shape still open. A `WITH` narrows scope to `rel`, so the endpoint
/// variables the rewrite would resolve to are gone by the time `startNode` is
/// called, and only a vertex lookup could recover them.
#[tokio::test]
#[ignore = "#187 remainder: a WITH drops the endpoint variables, and resolving \
            the relationship's _src_vid back to a node needs a vertex lookup"]
async fn start_node_after_a_with_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() WITH e AS rel RETURN startNode(rel).name AS s")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
}

/// Direction is what makes the rewrite sound, so it is worth a test of its own:
/// `<-[e]-` traverses against the arrow, so the relationship's start node is the
/// traversal's *target*, not its source. Getting this backwards would swap the
/// two endpoints silently — a wrong answer, not an error.
#[tokio::test]
async fn start_and_end_node_follow_the_arrow_not_the_traversal() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (y:P)<-[e:KNOWS]-(x:P) RETURN startNode(e).name AS s, endNode(e).name AS t")
        .await
        .unwrap();
    // The edge is a->b. Read backwards from b, the start is still a.
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::String("b".to_string()));
}

/// The same edge reached through an undirected pattern — still open, and
/// deliberately so.
///
/// Which end of an undirected relationship is the start is a per-row fact, so
/// the static rewrite cannot fire and the query still fails to plan. It would
/// be easy to make it *plan*: hand the UDF `e._src_vid` and let it fall back to
/// its minimal `{_vid}` node when it cannot match a node argument. That is the
/// wrong trade. `id(startNode(e))` would start working and `startNode(e).name`
/// would start returning NULL — turning a loud error into a silent wrong
/// answer, which is the one direction this codebase's ordering principle says
/// never to move in. Closing it properly means resolving the endpoint per row
/// against both candidate variables, with their properties materialised.
#[tokio::test]
#[ignore = "#188: an undirected relationship's start node is a per-row fact; \
            resolving it needs a runtime endpoint match"]
async fn start_and_end_node_on_an_undirected_match() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (x:P {name:'b'})-[e:KNOWS]-(y:P) RETURN id(startNode(e)) AS s, id(endNode(e)) AS t")
        .await
        .unwrap();
    let a = db
        .session()
        .query("MATCH (n:P {name:'a'}) RETURN id(n) AS v")
        .await
        .unwrap();
    let b = db
        .session()
        .query("MATCH (n:P {name:'b'}) RETURN id(n) AS v")
        .await
        .unwrap();
    // Walked from `b`, but the edge is still a->b.
    assert_eq!(r.rows()[0].values()[0], a.rows()[0].values()[0]);
    assert_eq!(r.rows()[0].values()[1], b.rows()[0].values()[0]);
}

/// A variable-length step variable holds a *list* of relationships, so there is
/// no single pair of endpoints to rewrite to. The pass must leave it alone
/// rather than resolve it to the pattern's outer endpoints, which would be the
/// wrong nodes for every hop but the last.
#[tokio::test]
async fn variable_length_step_variable_is_not_rewritten() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (x:P {name:'a'})-[e:KNOWS*1..2]->(y:P) RETURN size(e) AS hops")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(1));
}
