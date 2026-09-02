//! A collected list large enough to be interned must still behave like a list.
//!
//! `WITH collect(x) AS xs` above a fan-out replicated the whole list onto every
//! row — `rows × list_size` bytes, which OOM-killed LDBC IC3 and IC12 at SF1.
//! A list past a size threshold now travels as a 9-byte handle into a per-query
//! registry, resolved transparently by `cypher_value_codec::decode`.
//!
//! These tests are about the half that is easy to get wrong. Interning changes
//! the *bytes a row carries*, so anything that reads those bytes — a predicate,
//! a function, a returned result, a write — must be unable to tell. Two failure
//! modes are specifically hunted here:
//!
//! 1. **A wrong answer**, if a handle compares or decodes differently from the
//!    list it names. Every test therefore crosses the threshold and asserts the
//!    exact answer, with a sub-threshold twin where the distinction matters.
//! 2. **An escape** — a handle outliving the query scope that owns it, which is
//!    the only way it can dangle. Resolution fails loudly rather than yielding
//!    null, so an escape shows up as an error containing "no longer live"
//!    rather than as silence. The tests below drive a handle at each boundary
//!    it could cross: a returned result, a persisted property, and a second
//!    session reading what the first wrote.

use uni_db::{Uni, Value};

/// Comfortably past `COLLECT_INTERN_MIN_BYTES` (1 KiB) once encoded.
const BIG: usize = 200;
/// Comfortably under it, so the same query stays on the inline path.
const SMALL: usize = 2;

/// `n` cities, and `rows` people each living in one of them, chained by KNOWS
/// so a traversal fans out.
async fn fixture(rows: usize, cities: usize) -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (idx INT)").await.unwrap();
    tx.execute("CREATE LABEL City (idx INT, name STRING)")
        .await
        .unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE EDGE TYPE LIVES FROM P TO City")
        .await
        .unwrap();
    for chunk in (0..=rows).collect::<Vec<_>>().chunks(500) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:P {{idx:{i}}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await.unwrap();
    }
    for chunk in (0..cities).collect::<Vec<_>>().chunks(500) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:City {{idx:{i}, name:'c{i}'}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await.unwrap();
    }
    tx.execute("MATCH (a:P), (b:P) WHERE b.idx = a.idx + 1 CREATE (a)-[:KNOWS]->(b)")
        .await
        .unwrap();
    tx.execute(&format!(
        "MATCH (b:P), (c:City) WHERE c.idx = b.idx % {cities} CREATE (b)-[:LIVES]->(c)"
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    db
}

async fn count_of(db: &Uni, q: &str) -> i64 {
    let r = db
        .session()
        .query(q)
        .await
        .unwrap_or_else(|e| panic!("{q}\n  failed: {e}"));
    match &r.rows()[0].values()[0] {
        Value::Int(n) => *n,
        other => panic!("expected a count, got {other:?}"),
    }
}

/// The shape the interning exists for: the list is read by a predicate on every
/// fan-out row. An interned list must select exactly what an inline one does.
#[tokio::test]
async fn membership_over_an_interned_list_answers_as_it_did_inline() {
    let q = "MATCH (c:City) WITH collect(c) AS cities \
             MATCH (a:P)-[:KNOWS]->(b:P)-[:LIVES]->(city:City) \
             WHERE city IN cities \
             RETURN count(*) AS n";

    // Interned: every one of the 400 rows matches, because every city is in
    // the list.
    let db = fixture(400, BIG).await;
    assert_eq!(count_of(&db, q).await, 400);

    // The identical query below the threshold takes the inline path and must
    // agree — this pairing is what makes the assertion above meaningful.
    let db = fixture(400, SMALL).await;
    assert_eq!(count_of(&db, q).await, 400);
}

/// IC3 uses the negated form, and a membership bug that cancels itself out
/// under `IN` would show up here.
#[tokio::test]
async fn negated_membership_over_an_interned_list_is_the_complement() {
    let db = fixture(400, BIG).await;
    let matched = count_of(
        &db,
        "MATCH (c:City) WITH collect(c) AS cities \
         MATCH (a:P)-[:KNOWS]->(b:P)-[:LIVES]->(city:City) \
         WHERE NOT city IN cities \
         RETURN count(*) AS n",
    )
    .await;
    assert_eq!(
        matched, 0,
        "every city is in the list, so NOT IN selects none"
    );

    // A list that excludes half the cities must exclude exactly half the rows,
    // so this is not passing merely because the predicate is inert.
    let half = count_of(
        &db,
        &format!(
            "MATCH (c:City) WHERE c.idx < {} WITH collect(c) AS cities \
             MATCH (a:P)-[:KNOWS]->(b:P)-[:LIVES]->(city:City) \
             WHERE NOT city IN cities \
             RETURN count(*) AS n",
            BIG / 2
        ),
    )
    .await;
    assert_eq!(
        half, 200,
        "half the cities excluded, half the rows selected"
    );
}

/// Entity identity, not structural equality, is what `IN` over collected nodes
/// means — and it is decided on decoded values, so interning must not perturb
/// it.
#[tokio::test]
async fn entity_identity_survives_interning() {
    let db = fixture(400, BIG).await;
    let n = count_of(
        &db,
        "MATCH (c:City) WITH collect(c) AS cities \
         MATCH (a:P)-[:KNOWS]->(b:P)-[:LIVES]->(city:City) \
         WHERE city IN cities AND city.idx = 0 \
         RETURN count(*) AS n",
    )
    .await;
    assert_eq!(n, 2, "the two people living in city 0");
}

/// Functions other than the membership predicate read the same bytes.
#[tokio::test]
async fn size_and_indexing_of_an_interned_list() {
    let db = fixture(10, BIG).await;
    let r = db
        .session()
        .query(
            "MATCH (c:City) WITH collect(c.name) AS names \
             RETURN size(names) AS n, names[0] AS first",
        )
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(BIG as i64));
    assert!(
        matches!(&r.rows()[0].values()[1], Value::String(s) if s.starts_with('c')),
        "got {:?}",
        r.rows()[0].values()[1]
    );
}

/// A returned list leaves the engine, and the scope that owns the handle dies
/// with the query — so this fails with "no longer live" unless the result was
/// materialized at the boundary.
#[tokio::test]
async fn an_interned_list_can_be_returned() {
    let db = fixture(10, BIG).await;
    let r = db
        .session()
        .query("MATCH (c:City) WITH collect(c.name) AS names RETURN names")
        .await
        .expect("a returned collected list must not carry a handle out of the engine");
    let Value::List(names) = &r.rows()[0].values()[0] else {
        panic!("expected a list, got {:?}", r.rows()[0].values()[0]);
    };
    assert_eq!(names.len(), BIG);
    assert!(names.contains(&Value::String("c0".to_string())));
}

/// The same list carried onto every row of a fan-out and returned there. This
/// is the case where the interning is doing the most work, so it is the one
/// most likely to surface a handle in a place nothing materialized it.
#[tokio::test]
async fn an_interned_list_can_be_returned_on_every_fanned_out_row() {
    let db = fixture(20, BIG).await;
    let r = db
        .session()
        .query(
            "MATCH (c:City) WITH collect(c.name) AS names \
             MATCH (a:P)-[:KNOWS]->(b:P) \
             RETURN size(names) AS n, names AS carried",
        )
        .await
        .expect("a carried list must materialize on the way out");
    assert_eq!(r.rows().len(), 20);
    for row in r.rows() {
        assert_eq!(row.values()[0], Value::Int(BIG as i64));
        let Value::List(names) = &row.values()[1] else {
            panic!("expected a list, got {:?}", row.values()[1]);
        };
        assert_eq!(names.len(), BIG);
    }
}

/// Persistence is the boundary that would be *silent* corruption rather than a
/// loud error: an id written to disk means nothing to a later process. Write an
/// interned list into a property, then read it back through a fresh session.
#[tokio::test]
async fn an_interned_list_written_to_a_property_survives_a_new_session() {
    let db = fixture(10, BIG).await;
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL Bag (names LIST<STRING>)")
        .await
        .unwrap();
    tx.execute("MATCH (c:City) WITH collect(c.name) AS names CREATE (:Bag {names: names})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    let r = db
        .session()
        .query("MATCH (b:Bag) RETURN b.names AS names")
        .await
        .expect("a persisted collected list must not be a handle");
    let Value::List(names) = &r.rows()[0].values()[0] else {
        panic!("expected a list, got {:?}", r.rows()[0].values()[0]);
    };
    assert_eq!(names.len(), BIG, "the whole list must have been persisted");
    assert!(names.contains(&Value::String("c0".to_string())));
}

/// An empty collect and a null-bearing list are the edges where a threshold
/// check could pick the wrong path.
#[tokio::test]
async fn empty_and_small_collects_are_unaffected() {
    let db = fixture(10, SMALL).await;
    let empty = db
        .session()
        .query("MATCH (c:City) WHERE c.idx > 9999 WITH collect(c) AS none RETURN size(none) AS n")
        .await
        .unwrap();
    assert_eq!(empty.rows()[0].values()[0], Value::Int(0));

    let small = db
        .session()
        .query("MATCH (c:City) WITH collect(c.name) AS names RETURN size(names) AS n")
        .await
        .unwrap();
    assert_eq!(small.rows()[0].values()[0], Value::Int(SMALL as i64));
}
