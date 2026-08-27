//! Graph entities compare by identity, including inside a list.
//!
//! `n IN [n]` returned **false** — the same node, in the same row, was not a
//! member of a one-element list containing itself. `n = n` was true throughout,
//! which is why this survived: the planner lowers `=` on nodes to a VID
//! comparison, so only the paths routed through `cypher_eq` (`IN`, list
//! membership) saw the structural comparison that `#[derive(PartialEq)]` on
//! `Node` provides — vid *and* labels *and* the full property map.
//!
//! Found by LDBC SNB Interactive IC3, which filters `WHERE country IN
//! [countryX, countryY]` and `WHERE NOT city IN cities`. Both silently matched
//! nothing, so the query returned zero rows against a graph that demonstrably
//! contained answers — no error, just an empty result.

use uni_db::Uni;
use uni_db::Value;

async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL Country (name STRING, note STRING)")
        .await
        .unwrap();
    tx.execute("CREATE (:Country {name: 'Egypt', note: 'a'})")
        .await
        .unwrap();
    tx.execute("CREATE (:Country {name: 'Chile', note: 'b'})")
        .await
        .unwrap();
    tx.execute("CREATE (:Country {name: 'Nepal', note: 'c'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

fn one_int(db_rows: &uni_db::QueryResult) -> i64 {
    match db_rows.rows()[0].values()[0] {
        Value::Int(i) => i,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

/// The minimal shape: a node is a member of a list containing itself.
#[tokio::test]
async fn a_node_is_in_a_list_containing_itself() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (c:Country) WITH c, [c] AS lst WHERE c IN lst RETURN count(c)")
        .await
        .unwrap();
    assert_eq!(one_int(&r), 3, "every node must be a member of [itself]");
}

/// Membership against a list built in a different part of the query, which is
/// where the two sides can be hydrated with different property sets.
#[tokio::test]
async fn node_membership_in_a_collected_list() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (c:Country) WHERE c.name IN ['Egypt', 'Chile'] \
             WITH collect(c) AS cs \
             MATCH (d:Country) WHERE d IN cs RETURN count(d)",
        )
        .await
        .unwrap();
    assert_eq!(one_int(&r), 2);
}

/// The IC3 shape: a list *literal* of two bound nodes.
#[tokio::test]
async fn node_membership_in_a_two_element_list_literal() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (x:Country {name: 'Egypt'}), (y:Country {name: 'Chile'}) \
             WITH x, y \
             MATCH (d:Country) WHERE d IN [x, y] RETURN count(d)",
        )
        .await
        .unwrap();
    assert_eq!(one_int(&r), 2);
}

/// The negation, which IC3 also relies on (`NOT city IN cities`).
#[tokio::test]
async fn node_non_membership_is_the_complement() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (c:Country) WHERE c.name IN ['Egypt', 'Chile'] \
             WITH collect(c) AS cs \
             MATCH (d:Country) WHERE NOT d IN cs RETURN count(d)",
        )
        .await
        .unwrap();
    assert_eq!(one_int(&r), 1, "Nepal is the only non-member");
}

/// Distinct nodes must still compare unequal — the fix compares by id, and this
/// is the guard against it collapsing everything to equal.
#[tokio::test]
async fn distinct_nodes_are_not_members_of_each_other() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (x:Country {name: 'Egypt'}) WITH x \
             MATCH (d:Country) WHERE d IN [x] RETURN count(d)",
        )
        .await
        .unwrap();
    assert_eq!(one_int(&r), 1, "only Egypt itself may match");
}

/// Edges carry the same identity rule.
#[tokio::test]
async fn an_edge_is_in_a_list_containing_itself() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS (since INT) FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name: 'a'}), (:P {name: 'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (a:P {name:'a'}), (b:P {name:'b'}) CREATE (a)-[:KNOWS {since: 1}]->(b)")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() WITH e, [e] AS lst WHERE e IN lst RETURN count(e)")
        .await
        .unwrap();
    assert_eq!(one_int(&r), 1);
}
