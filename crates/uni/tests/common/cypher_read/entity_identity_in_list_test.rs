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

/// A per-row entity list must not be hoisted as a batch constant, even when a
/// constant list is in scope on the same row.
///
/// `invoke_cypher_udf` re-decodes an argument only when its bytes differ from
/// the row before, because per-row argument decoding is 99% of that UDF's cost
/// on LDBC IC5 (#245). The saving is sound only if the byte comparison is
/// exact: a false match makes the previous row's value stand in for this one.
///
/// The shape here is the one that check has to get right and that
/// `a_node_is_in_a_list_containing_itself` does not reach: a collected list —
/// genuinely constant, and the argument the hoist exists for — held in scope
/// alongside a per-row list, with the predicate reading the per-row one. A
/// check that answered "constant" from the wrong column, or from a length
/// comparison alone, would test every row against row 0's list.
///
/// Confirmed discriminating: with the memo's byte comparison weakened to a
/// length comparison, this returns 1 instead of 3. Only entity-valued lists
/// reach that path — a list of strings takes a different lowering and cannot
/// exercise it, which is why this test uses nodes.
#[tokio::test]
async fn a_per_row_list_is_not_hoisted_as_constant() {
    let db = fixture().await;

    let r = db
        .session()
        .query(
            "MATCH (c:Country) WITH collect(c) AS xs \
             MATCH (d:Country) WITH d, xs, [d] AS own \
             WHERE d IN own RETURN count(d)",
        )
        .await
        .unwrap();
    assert_eq!(
        one_int(&r),
        3,
        "each row must be tested against its own list, not row 0's"
    );

    // The complement, so a hoist that happened to pick a list every row matches
    // is not mistaken for correctness: here no row is a member of its own
    // single-element list, and a wrongly-hoisted argument would report one.
    let r = db
        .session()
        .query(
            "MATCH (c:Country) WITH collect(c) AS xs \
             MATCH (d:Country)-[]->() WITH d, xs, [d] AS own \
             WHERE NOT d IN own RETURN count(d)",
        )
        .await
        .unwrap();
    assert_eq!(one_int(&r), 0);
}

/// The constant case the hoist exists for still answers correctly, on both
/// sides of the 1 KiB interning threshold.
///
/// Below the threshold the list argument is a msgpack blob repeated per row;
/// above it, a 9-byte handle into the global registry. Both are one long run,
/// so both are decoded once and must read the same on every row.
#[tokio::test]
async fn a_collected_list_is_read_the_same_on_every_row() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (idx INT, pad STRING)")
        .await
        .unwrap();
    // 60 elements, each padded well past 1 KiB in total, so `collect()` interns.
    let pad = "y".repeat(64);
    for i in 0..60 {
        tx.execute(&format!("CREATE (:P {{idx: {i}, pad: '{pad}'}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    // The lower half is collected; every P is then tested against it, so a list
    // read correctly on every row yields exactly half.
    let r = db
        .session()
        .query(
            "MATCH (c:P) WHERE c.idx < 30 WITH collect(c) AS xs \
             MATCH (p:P) WHERE p IN xs RETURN count(p)",
        )
        .await
        .unwrap();
    assert_eq!(one_int(&r), 30);

    // The same shape with a short list, below the interning threshold.
    let r = db
        .session()
        .query(
            "MATCH (c:P) WHERE c.idx < 3 WITH collect(c.idx) AS xs \
             MATCH (p:P) WHERE p.idx IN xs RETURN count(p)",
        )
        .await
        .unwrap();
    assert_eq!(one_int(&r), 3);
}

/// A null between two equal values must not let the second reuse the first.
///
/// The run-length memo in `invoke_cypher_udf` (#245) skips re-decoding an
/// argument whose bytes match the previous row. A null row still overwrites the
/// argument slot with `Value::Null`, so the remembered bytes have to be cleared
/// with it — otherwise the pattern `X, null, X` leaves the third row reading the
/// `Null` the second one wrote.
///
/// Found by openCypher TCK `WithWhere2` scenario [1], which regressed to a wrong
/// answer on the first version of the memo: `(a:A)` has no `id`, so the property
/// column carries exactly this shape.
#[tokio::test]
async fn a_null_row_between_two_equal_rows_clears_the_memo() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (a:A), (b:B {id: 1}), (c:C {id: 2}), (d:D)")
        .await
        .unwrap();
    tx.execute(
        "MATCH (a:A), (b:B), (c:C), (d:D) \
         CREATE (a)-[:T]->(b), (a)-[:T]->(c), (a)-[:T]->(d), \
                (b)-[:T]->(c), (b)-[:T]->(d), (c)-[:T]->(d)",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let r = db
        .session()
        .query(
            "MATCH (a)--(b)--(c)--(d)--(a), (b)--(d) \
             WITH a, c, d WHERE a.id = 1 AND c.id = 2 RETURN d",
        )
        .await
        .unwrap();
    let mut labels: Vec<String> = r
        .rows()
        .iter()
        .map(|row| format!("{:?}", row.values()[0]))
        .collect();
    labels.sort();
    assert_eq!(labels.len(), 2, "expected two rows, got {labels:?}");
}

/// `id()` answers for an entity in either encoding.
///
/// Both functions matched only `Value::Map`, so a native `Value::Node` — which
/// `pattern_comprehension` produces directly — returned NULL: an entity present
/// in the row reading as "not an entity", with no error (#233, #234).
/// `elementId()` had a second gap of its own — it required `_vid` to be
/// `as_u64`-able, so a map carrying the id only as `_id` also fell through — and
/// is fixed the same way, but cannot be asserted here: `ELEMENTID` is known to
/// `function_props` and `expr_eval` and never registered as a UDF, so a query
/// using it fails at planning with "UDF 'elementid' is not registered". That is
/// its own defect, of the shape #176 describes.
///
/// Routed through `Value::entity_vid`, the one definition of vertex identity —
/// which is also the consolidation #234 asks for, one call site at a time.
///
/// `id(null)` stays NULL, which is Cypher's rule and not the defect.
///
/// **This test does not cover the arm it was written for, and says so rather
/// than implying otherwise.** Neutralising `read.rs`'s `id()` back to its
/// map-only form leaves it green: these queries resolve `id()` through the
/// `entity_identity` UDF in `df_udfs`, not through the row-oriented executor.
/// So it stands as a behaviour guard on the reachable path, and the `read.rs`
/// arm's fix is unverified for want of a query shape that reaches it.
#[tokio::test]
async fn id_accepts_every_entity_encoding() {
    let db = fixture().await;

    // Scan-produced and comprehension-produced bindings of the same node reach
    // these functions under different encodings; both must answer.
    let r = db
        .session()
        .query(
            "MATCH (c:Country {name:'Egypt'}) \
             RETURN id(c) AS direct, \
                    [x IN [c] | id(x)] AS through_comprehension",
        )
        .await
        .unwrap();
    let row = &r.rows()[0];
    let direct = match row.values()[0] {
        Value::Int(i) => i,
        ref other => panic!("id() must return an integer, got {other:?}"),
    };
    let through = match &row.values()[1] {
        Value::List(items) => match items.first() {
            Some(Value::Int(i)) => *i,
            other => panic!("id() inside a comprehension returned {other:?}"),
        },
        other => panic!("expected a list, got {other:?}"),
    };
    assert_eq!(
        direct, through,
        "the same node must have the same id under both encodings"
    );
    // Cypher's rule for a null argument is preserved.
    let r = db.session().query("RETURN id(null) AS a").await.unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}
